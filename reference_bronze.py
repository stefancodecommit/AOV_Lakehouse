"""
Bronze ingestion for reference files: the CBS crosswalk ("schakelschema") from
SBI 2008 to SBI 2025.

Why it's needed
---------------
The CBS tables used here classify sectors with SBI 2008, while the KVK file mixes
SBI 2008 and SBI 2025 codes (Oct 2026: 1.42M only-2025 vs 0.50M only-2008 codes).
The crosswalk links the two. It is many-to-many: one SBI 2008 code can be split over
several SBI 2025 codes, and several SBI 2008 codes can merge into one.

How "load only once" works
--------------------------
* The SHA-256 of the xlsx file is recorded in `bronze/reference/_batch_log`. A file
  whose hash is already logged is skipped, whatever its name or where it came from.
* A file is written in ONE Delta commit, tagged with a new batch_id, and only then
  logged. Rows of a batch that was written but never logged (a crash in between) are
  removed before the next write, so a retry never doubles them.
* A new CBS version of the file (other hash) becomes a new batch; silver uses the
  latest batch only. Bronze keeps every version.

Checks before writing (the file is rejected, nothing written, if one fails)
-------------------------------------------------------------------------
* a sheet whose name starts with "Schakeltabel", with the expected columns
* every SBI code is 4 to 6 digits, and none looks like it lost a leading zero
  (0111 stored as the number 111: '0' + code starts with its 2-digit column)
* no (SBI 2008, SBI 2025) pair appears twice
Other mismatches between a code and its 2-digit column are warnings: v2.0 has five rows
where the SBI 2025 2-digit column was copied from the SBI 2008 side (e.g. 71111 / 74).
Silver takes the division from the code itself.

Bronze tables
-------------
lakehouse/bronze/reference/sbi2008_sbi2025   crosswalk rows (all text) + _batch_id,
                                             _ingested_at, _source_file, _source_url,
                                             _source_sha256, _source_version, _row_number
lakehouse/bronze/reference/_batch_log        batch_id, dataset, ingested_at, source_file,
                                             source_url, file_sha256, source_version,
                                             row_count, status

Usage
-----
    python reference_bronze.py                      # download from CBS and load if new
    python reference_bronze.py --file schakelschema-sbi2008sbi2025-v20.xlsx
    python reference_bronze.py --url <newer version's URL>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

CROSSWALK_URL = ("https://www.cbs.nl/-/media/cbs/onze-diensten/methoden/classificaties/"
                 "documents/2026/schakelschema-sbi2008sbi2025-v20.xlsx")
DOWNLOAD_DIR = Path("inbox/reference")
REF_ROOT = Path("lakehouse/bronze/reference")
DATA_TABLE = REF_ROOT / "sbi2008_sbi2025"
LOG_TABLE = REF_ROOT / "_batch_log"
DATASET = "sbi2008_sbi2025"
SHEET_PREFIX = "Schakeltabel"

# bronze column -> pattern for the header in the file, matched after lower-casing and
# removing everything that isn't a letter or digit. The SBI 2025 title header carries
# a date ("SBI2025 titel dd 20260101") that may change in a later version.
COLUMNS = {
    "sbi2008": r"sbi2008",
    "sbi2008_2digit": r"sbi20082digit",
    "sbi2008_title": r"sbi2008omschrijving",
    "sbi2025": r"sbi2025",
    "sbi2025_2digit": r"sbi20252digit",
    "sbi2025_title": r"sbi2025titel.*",
    "link_description": r"omschrijvingschakeling",
}
CODE_RE = re.compile(r"\d{4,6}")

log = logging.getLogger("reference_bronze")


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- read & check
def read_crosswalk(path: Path) -> tuple[pd.DataFrame, str, list[str], list[str]]:
    """Returns (rows with bronze column names, all text; version from the sheet name;
    errors; warnings). Rows are empty when the file can't be used."""
    sheets = pd.ExcelFile(path).sheet_names
    sheet = next((s for s in sheets if s.startswith(SHEET_PREFIX)), None)
    if sheet is None:
        return pd.DataFrame(), "", [f"No sheet starting with '{SHEET_PREFIX}': {sheets}"], []
    version = sheet[len(SHEET_PREFIX):].strip()          # 'Schakeltabel v2.0' -> 'v2.0'

    raw = pd.read_excel(path, sheet_name=sheet, dtype=str, keep_default_na=False)
    raw = raw.apply(lambda c: c.str.strip())
    rename, errors = {}, []
    for col, pattern in COLUMNS.items():
        match = next((h for h in raw.columns if re.fullmatch(pattern, _norm(h))), None)
        if match is None:
            errors.append(f"Column missing: {col} (header pattern {pattern!r})")
        else:
            rename[match] = col
    if errors:
        return pd.DataFrame(), version, errors, []
    df = raw.rename(columns=rename)[list(COLUMNS)]
    df = df[(df != "").any(axis=1)].reset_index(drop=True)  # trailing empty rows
    return (df, version, *check_crosswalk(df))


def check_crosswalk(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Returns (errors, warnings)."""
    errors, warnings = [], []
    if df.empty:
        return ["The crosswalk sheet has no rows"], []
    for v in ("2008", "2025"):
        code, two = df[f"sbi{v}"], df[f"sbi{v}_2digit"]
        bad = ~code.str.fullmatch(CODE_RE.pattern)
        if bad.any():
            errors.append(f"{bad.sum()} SBI {v} codes are not 4-6 digits, e.g. "
                          f"{code[bad].head(3).tolist()}")
        lost_zero = (code.str[:2] != two) & (("0" + code).str[:2] == two)
        if lost_zero.any():
            errors.append(f"{lost_zero.sum()} SBI {v} codes seem to have lost a leading zero, "
                          f"e.g. {code[lost_zero].head(3).tolist()}")
        mismatch = ~bad & ~lost_zero & (code.str[:2] != two)
        if mismatch.any():
            pairs = df.loc[mismatch, [f"sbi{v}", f"sbi{v}_2digit"]].head(3).values.tolist()
            warnings.append(f"{mismatch.sum()} SBI {v} codes don't start with their 2-digit "
                            f"column, e.g. {pairs}")
    dup = df.duplicated(["sbi2008", "sbi2025"])
    if dup.any():
        pairs = df.loc[dup, ["sbi2008", "sbi2025"]].head(3).values.tolist()
        errors.append(f"{dup.sum()} duplicate (SBI 2008, SBI 2025) pairs, e.g. {pairs}")
    return errors, warnings


# --------------------------------------------------------------------------- delta io
def ingested_hashes() -> dict[str, str]:
    """file_sha256 -> batch_id of every successful batch."""
    if not (LOG_TABLE / "_delta_log").exists():
        return {}
    from deltalake import DeltaTable
    rows = DeltaTable(str(LOG_TABLE)).to_pyarrow_table(
        columns=["file_sha256", "batch_id", "status"]).to_pylist()
    return {r["file_sha256"]: r["batch_id"] for r in rows if r["status"] == "success"}


def remove_orphan_rows() -> None:
    """Delete rows of a batch that was written but never logged (crash in between)."""
    if not (DATA_TABLE / "_delta_log").exists():
        return
    from deltalake import DeltaTable
    good = set(ingested_hashes().values())
    dt = DeltaTable(str(DATA_TABLE))
    present = set(dt.to_pyarrow_table(columns=["_batch_id"]).column("_batch_id").unique().to_pylist())
    for orphan in present - good:
        dt.delete(f"_batch_id = '{orphan}'")
        log.warning("Removed rows of unlogged batch %s", orphan)


def write_batch(df: pd.DataFrame, batch_id: str, now: datetime, source_file: str,
                source_url: str | None, sha: str, version: str) -> int:
    import pyarrow as pa
    from deltalake import write_deltalake

    remove_orphan_rows()
    n = len(df)
    schema = pa.schema([pa.field(c, pa.string()) for c in COLUMNS] + [
        ("_batch_id", pa.string()), ("_ingested_at", pa.timestamp("us", tz="UTC")),
        ("_source_file", pa.string()), ("_source_url", pa.string()),
        ("_source_sha256", pa.string()), ("_source_version", pa.string()),
        ("_row_number", pa.int64())])
    table = pa.table({**{c: df[c].tolist() for c in COLUMNS},
                      "_batch_id": [batch_id] * n, "_ingested_at": [now] * n,
                      "_source_file": [source_file] * n, "_source_url": [source_url] * n,
                      "_source_sha256": [sha] * n, "_source_version": [version] * n,
                      "_row_number": list(range(1, n + 1))}, schema=schema)
    write_deltalake(str(DATA_TABLE), table, mode="append")   # one commit per batch
    return n


def write_log(batch_id: str, now: datetime, source_file: str, source_url: str | None,
              sha: str, version: str, rows: int) -> None:
    import pyarrow as pa
    from deltalake import write_deltalake
    table = pa.table({
        "batch_id": [batch_id], "dataset": [DATASET], "ingested_at": [now],
        "source_file": [source_file], "source_url": [source_url], "file_sha256": [sha],
        "source_version": [version], "row_count": [rows], "status": ["success"],
    }, schema=pa.schema([
        ("batch_id", pa.string()), ("dataset", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")), ("source_file", pa.string()),
        ("source_url", pa.string()), ("file_sha256", pa.string()),
        ("source_version", pa.string()), ("row_count", pa.int64()), ("status", pa.string())]))
    write_deltalake(str(LOG_TABLE), table, mode="append")


# --------------------------------------------------------------------------- main
def ingest(path: Path, source_url: str | None = None) -> dict:
    """Check and load one crosswalk file. Returns a report with status
    INGESTED, SKIPPED (this exact file is already in bronze) or FAILED."""
    path = Path(path)
    sha = _sha256(path)
    if (batch := ingested_hashes().get(sha)):
        return {"status": "SKIPPED", "reason": f"This exact file was already ingested as batch {batch}",
                "batch_id": batch, "file_sha256": sha}

    df, version, errors, warnings = read_crosswalk(path)
    if errors:
        return {"status": "FAILED", "errors": errors, "warnings": warnings, "file_sha256": sha}

    now = datetime.now(timezone.utc)
    batch_id = f"{DATASET}_{now:%Y%m%dT%H%M%S%fZ}_{sha[:8]}"
    rows = write_batch(df, batch_id, now, path.name, source_url, sha, version)
    write_log(batch_id, now, path.name, source_url, sha, version, rows)
    return {"status": "INGESTED", "batch_id": batch_id, "rows": rows, "version": version,
            "sbi2008_codes": int(df["sbi2008"].nunique()),
            "sbi2025_codes": int(df["sbi2025"].nunique()), "warnings": warnings,
            "file_sha256": sha}


def download(url: str, dest_dir: Path = DOWNLOAD_DIR) -> Path:
    import requests
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / url.rstrip("/").rsplit("/", 1)[-1]
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    return dest


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load the CBS SBI 2008 -> SBI 2025 crosswalk into bronze.")
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--file", help="a local copy of the crosswalk xlsx")
    src.add_argument("--url", default=CROSSWALK_URL, help="download from this URL (default: v2.0)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.file:
        file, url = Path(args.file).expanduser(), None
    else:
        file, url = download(args.url), args.url
        log.info("Downloaded %s", file)
    report = ingest(file, url)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    raise SystemExit(1 if report["status"] == "FAILED" else 0)
