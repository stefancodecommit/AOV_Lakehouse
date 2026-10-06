"""Quarantine: batches that failed their checks are kept for inspection, not ingested.

    lakehouse/quarantine/<source>/<UTC timestamp>_<name>/
        report.json     what was checked, which checks failed, where the data came from
        records.json    the records as received (or a copy of the rejected file)

The same content (by SHA-256) is quarantined only once per source and name, so a source
that keeps failing on every run doesn't fill the folder with copies. cleanup() removes
entries that are resolved or older than a retention period (run monthly by
maintenance.py); a problem that still exists is quarantined again by the next run.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _safe(name: str) -> str:
    """A name that is safe as part of a folder name (no slashes or spaces, max 120 characters)."""
    return re.sub(r"[^0-9A-Za-z_.-]+", "_", name).strip("_")[:120]


def content_sha256(records: list[dict] | None = None, file: Path | None = None) -> str:
    """Fingerprint of a batch (its records as JSON) or a file, to quarantine the same
    content only once."""
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


def cleanup(root: Path, is_resolved, max_age_days: int, now: datetime | None = None,
            dry_run: bool = False) -> list[tuple[str, str]]:
    """Remove quarantine entries that are resolved or older than max_age_days.
    `is_resolved(entry)` returns a reason (str) when the problem is solved, else None.
    Returns (folder, reason) for every removed (or, with dry_run, removable) entry."""
    now = now or datetime.now(timezone.utc)
    removed = []
    for entry in list_entries(root):
        reason = is_resolved(entry)
        if reason is None:
            try:
                age = now - datetime.fromisoformat(entry["quarantined_at"])
            except (KeyError, TypeError, ValueError):
                continue                       # unreadable report: leave it for a person
            if age > timedelta(days=max_age_days):
                reason = f"older than {max_age_days} days ({age.days} days)"
        if reason:
            removed.append((entry["folder"], reason))
            if not dry_run:
                shutil.rmtree(entry["folder"])
    return removed
