"""File helpers shared by the checks and by bronze ingestion (pipeline/bronze/kvk.py and
reference.py): a file's SHA-256 fingerprint and header names compared without case or
punctuation. Here, not in pipeline/, because the checks need them too and checks/ doesn't
import from pipeline/.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path


def sha256(path: Path) -> str:
    """SHA-256 of a file: its fingerprint, used to recognise a file that was already
    loaded (or already quarantined). Read in 1 MB chunks, so memory stays flat."""
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def norm(name) -> str:
    """Compare headers without case or punctuation: 'Datum aanvang' -> 'datumaanvang',
    'SBI2025 titel dd 20260101' -> 'sbi2025titeldd20260101'. Not text (a header pandas
    read as a number) is turned into text first."""
    return re.sub(r"[^a-z0-9]", "", str(name).lower())
