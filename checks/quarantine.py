"""Quarantine: batches that failed their checks are kept for inspection, not ingested.

    lakehouse/quarantine/<source>/<UTC timestamp>_<name>/
        report.json     what was checked, which checks failed, where the data came from
        records.json    the records as received (or a copy of the rejected file)

The same content (by SHA-256) is quarantined only once per source and name, so a source
that keeps failing on every run doesn't fill the folder with copies.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path


def _safe(name: str) -> str:
    return re.sub(r"[^0-9A-Za-z_.-]+", "_", name).strip("_")[:120]


def content_sha256(records: list[dict] | None = None, file: Path | None = None) -> str:
    h = hashlib.sha256()
    if file is not None:
        with Path(file).open("rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
    else:
        h.update(json.dumps(records, sort_keys=True, ensure_ascii=False).encode())
    return h.hexdigest()


def list_entries(root: Path, source: str | None = None) -> list[dict]:
    """Reports of all quarantined batches (newest last), each with its folder."""
    pattern = f"{source}/*/report.json" if source else "*/*/report.json"
    out = []
    for path in sorted(Path(root).glob(pattern)):
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            report = {"name": path.parent.name, "unreadable": True}
        out.append({**report, "folder": str(path.parent)})
    return out


def quarantine(root: Path, source: str, name: str, report: dict,
               records: list[dict] | None = None, file: Path | None = None) -> Path | None:
    """Save a failed batch. Returns its folder, or None when this exact content is
    already in quarantine for the same source and name."""
    sha = content_sha256(records, file)
    for entry in list_entries(root, source):
        if entry.get("name") == name and entry.get("content_sha256") == sha:
            return None

    now = datetime.now(timezone.utc)
    folder = Path(root) / source / f"{now:%Y%m%dT%H%M%S%fZ}_{_safe(name)}"
    folder.mkdir(parents=True)
    if file is not None:
        shutil.copy2(file, folder / Path(file).name)
    else:
        (folder / "records.json").write_text(
            json.dumps(records, ensure_ascii=False, indent=1), encoding="utf-8")
    full = {"source": source, "name": name, "quarantined_at": now.isoformat(),
            "content_sha256": sha, **report}
    (folder / "report.json").write_text(json.dumps(full, ensure_ascii=False, indent=2,
                                                   default=str), encoding="utf-8")
    return folder
