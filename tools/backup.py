"""
Backup of the data that can't be fetched again: lakehouse/ and inbox/kvk/ (risk R14 in
docs/privacy_and_risks.md). CBS figures can be downloaded again, but the KVK download link
only serves the current file, so old KVK snapshots exist nowhere else.

One backup is one archive, backup_<UTC time>.tar.gz, in the folder backup.path
(config.yaml), with a manifest.json inside: per Delta table its version and number of
files. The newest backup.keep archives are kept, older ones are removed, but only after
the new archive was verified.

Consistent while other work runs
--------------------------------
Delta writes the data files of a commit first and the commit itself (in _delta_log)
last. The backup therefore archives every _delta_log first and the data files after:
every commit in the archive then has its data files in it too. Files written later are
in the archive as unused extras, which Delta ignores. Monthly, Dagster runs the backup in
the maintenance job, which never overlaps another writing run (dagster.yaml).

Restore
-------
`restore` unpacks into a new, empty folder and never over the live lakehouse: check the
result, stop Dagster, then swap the folders by hand (INSTRUCTIONS.md, section 7).

The backup folder is on the same disk by default (backups/, not in git). Point
backup.path at another disk or a synchronised folder for a real backup; in the cloud
design, storage with soft delete and zone redundancy replaces this tool.

Usage
-----
    python -m tools.backup create             # make, verify and rotate a backup
    python -m tools.backup list               # the backups, newest first
    python -m tools.backup verify [ARCHIVE]   # check an archive (default: the newest)
    python -m tools.backup restore ARCHIVE --to DIR   # unpack into an empty folder
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pipeline.common.config import project_path, settings

LAKEHOUSE = project_path(settings["paths"]["lakehouse"])
INBOX_KVK = project_path(settings["paths"]["inbox"]) / "kvk"
BACKUP_DIR = project_path(settings["backup"]["path"])
KEEP = int(settings["backup"]["keep"])
PREFIX = "backup_"

log = logging.getLogger("backup")


def _files(root: Path) -> list[Path]:
    """Every file under root, _delta_log files first (see the module docstring); staging
    folders (half-finished writes) are left out."""
    files = [p for p in root.rglob("*") if p.is_file() and "_staging" not in p.parts]
    return sorted(files, key=lambda p: ("_delta_log" not in p.parts, str(p)))


def _delta_tables(root: Path) -> list[Path]:
    return sorted(p.parent for p in root.rglob("_delta_log")
                  if p.is_dir() and "_staging" not in p.parts)


def _manifest(lakehouse: Path) -> dict:
    """Per Delta table (path relative to the lakehouse): its version and number of data
    files, read before the archive is written."""
    from deltalake import DeltaTable
    tables = {}
    for path in _delta_tables(lakehouse):
        dt = DeltaTable(str(path))
        tables[str(path.relative_to(lakehouse))] = {"version": dt.version(),
                                                    "files": len(dt.file_uris())}
    return {"created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "tables": tables}


def create(backup_dir: Path | None = None, keep: int | None = None) -> Path:
    """Write a new archive, verify it, then remove all but the newest `keep` archives.
    Returns the new archive's path."""
    backup_dir, keep = backup_dir or BACKUP_DIR, KEEP if keep is None else keep
    backup_dir.mkdir(parents=True, exist_ok=True)
    manifest = _manifest(LAKEHOUSE) if LAKEHOUSE.exists() else {"tables": {}}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = backup_dir / f"{PREFIX}{stamp}.tar.gz"
    partial = target.with_suffix(".partial")        # renamed only when complete
    with tarfile.open(partial, "w:gz", compresslevel=1) as tar:  # Parquet is compressed already
        for name, root in (("lakehouse", LAKEHOUSE), ("inbox/kvk", INBOX_KVK)):
            if not root.exists():
                continue
            for f in _files(root):
                try:
                    tar.add(f, arcname=f"{name}/{f.relative_to(root)}")
                except FileNotFoundError:         # removed while the backup ran (vacuum)
                    log.warning("skipped %s: removed during the backup", f)
        data = json.dumps(manifest, indent=2).encode()
        info = tarfile.TarInfo("manifest.json")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    partial.replace(target)
    problems = verify(target)
    if problems:
        raise RuntimeError(f"backup {target.name} failed verification: {problems}")
    for old in backups(backup_dir)[keep:]:
        old.unlink()
        log.info("removed old backup %s", old.name)
    return target


def backups(backup_dir: Path | None = None) -> list[Path]:
    """The archives in the backup folder, newest first."""
    return sorted((backup_dir or BACKUP_DIR).glob(f"{PREFIX}*.tar.gz"), reverse=True)


def _extract(tar: tarfile.TarFile, to: Path) -> None:
    # the "data" filter refuses absolute paths, links out of the folder and device files
    # (Python 3.10.12+ / 3.12); without it, plain extraction of our own archive
    if hasattr(tarfile, "data_filter"):
        tar.extractall(to, filter="data")
    else:  # pragma: no cover
        tar.extractall(to)


def verify(archive: Path) -> list[str]:
    """Unpack the archive into a temporary folder and check that every Delta table in the
    manifest opens, at least at the manifest's version, with all its data files present.
    Returns the problems found (empty: the backup is usable)."""
    from deltalake import DeltaTable
    problems = []
    with tempfile.TemporaryDirectory(prefix="backup_verify_") as tmp, \
            tarfile.open(archive) as tar:
        _extract(tar, Path(tmp))
        manifest_file = Path(tmp) / "manifest.json"
        if not manifest_file.exists():
            return ["no manifest.json in the archive"]
        manifest = json.loads(manifest_file.read_text())
        for name, expected in manifest["tables"].items():
            path = Path(tmp) / "lakehouse" / name
            try:
                dt = DeltaTable(str(path))
            except Exception as e:  # noqa: BLE001  (any reason a table can't be opened)
                problems.append(f"{name}: does not open ({e})")
                continue
            if dt.version() < expected["version"]:
                problems.append(f"{name}: version {dt.version()}, expected {expected['version']}")
            missing = [f for f in dt.file_uris()
                       if not Path(f.removeprefix("file://")).exists()]
            if missing:
                problems.append(f"{name}: {len(missing)} data files missing, e.g. {missing[0]}")
    return problems


def restore(archive: Path, to: Path) -> Path:
    """Unpack an archive into `to`, which must be new or empty (never the live lakehouse).
    Returns `to`; the lakehouse is then at <to>/lakehouse, the KVK uploads at <to>/inbox/kvk."""
    live, target = LAKEHOUSE.resolve(), to.resolve()
    if target == live or target in live.parents or live in target.parents:
        raise ValueError(f"restore into a new folder, not over the lakehouse: {to}")
    if to.exists() and any(to.iterdir()):
        raise ValueError(f"{to} is not empty")
    to.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive) as tar:
        _extract(tar, to)
    return to


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Back up lakehouse/ and inbox/kvk/.")
    sub = ap.add_subparsers(dest="task", required=True)
    sub.add_parser("create", help="make, verify and rotate a backup")
    sub.add_parser("list", help="the backups, newest first")
    v = sub.add_parser("verify", help="check an archive (default: the newest)")
    v.add_argument("archive", nargs="?", type=Path)
    r = sub.add_parser("restore", help="unpack an archive into an empty folder")
    r.add_argument("archive", type=Path)
    r.add_argument("--to", type=Path, required=True)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.task == "create":
        path = create()
        log.info("wrote %s (%.0f MB), verified", path, path.stat().st_size / 1e6)
    elif args.task == "list":
        for path in backups():
            print(f"{path}  {path.stat().st_size / 1e6:.0f} MB")
    elif args.task == "verify":
        archive = args.archive or next(iter(backups()), None)
        if archive is None:
            sys.exit(f"no backups in {BACKUP_DIR}")
        problems = verify(archive)
        print(f"{archive}: " + ("OK" if not problems else "; ".join(problems)))
        sys.exit(1 if problems else 0)
    elif args.task == "restore":
        print(f"restored into {restore(args.archive, args.to)}")
