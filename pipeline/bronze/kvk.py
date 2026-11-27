"""
Bronze ingestion for the "KVK Handelsregister Open Dataset Basis Bedrijfsgegevens":
verify an uploaded file, let a person confirm, write it to the KVK bronze Delta Lake.

Two ways in, both through the same verification and the same human gate:
- the KVK intake agent (pipeline/bronze/kvk_agent.py): a local LLM guides the person and
  calls the functions here as its tools; the person types YES at the keyboard
- Dagster (orchestration/definitions.py, asset kvk_bronze): ingest_confirmed_file, the
  person types the exact file name in the launchpad
and the command line below (no LLM). This module has no LLM code: the agent depends on
it, not the other way round (split from kvk_agent.py on 2026-11-27).

Flow
----
1. A person saves the KVK zip in the inbox (inbox/kvk/, the folder Docker shares with
   the computer), or gives its path.
2. verify() checks the file with checks/kvk.py:
     - a readable zip with a CSV inside (or a CSV directly)
     - SHA-256 not ingested before (the same file is never loaded twice)
     - semicolon-separated header with the expected columns
     - every row has the right number of fields (max 0.1% malformed)
     - value checks on actief, rechtsvorm, postcode region and start date (warnings)
   A rejected file is copied to lakehouse/quarantine/kvk/ with its report.
3. Only if verification PASSED for that exact file (same hash) can ingest() run, and only
   after `confirm` says yes: confirm_at_keyboard() (type YES), or the typed file name in
   Dagster.
4. ingest() writes the rows to bronze in ONE Delta commit, tagged with a new batch_id,
   then records the batch in the batch log.

Bronze tables
-------------
lakehouse/bronze/kvk/basis_bedrijfsgegevens   raw CSV rows (all text) +
                                              _batch_id, _ingested_at, _source_file,
                                              _source_sha256, _row_number
lakehouse/bronze/kvk/_batch_log               batch_id, ingested_at, source_file,
                                              file_sha256, csv_member, row_count,
                                              column_count, verified_at,
                                              verification_report, confirmed_by, status,
                                              file_date (KVK's export date, from the zip;
                                              empty for a loose CSV)

Usage
-----
    python -m pipeline.bronze.kvk --inbox  # the newest new file in inbox/kvk/ (type YES)
    python -m pipeline.bronze.kvk --file ~/Downloads/kvk-open-dataset-basis-bedrijfsgegevens.zip
    python -m pipeline.bronze.kvk_agent    # the same, guided by the agent (Ollama)
"""
from __future__ import annotations

import argparse
import getpass
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from checks.kvk import check_file
from checks.kvk import iter_rows as _iter_rows
from checks.kvk import sha256 as _sha256
from pipeline.common.config import project_path, settings

# --------------------------------------------------------------------------- config
_CFG = settings["kvk"]
KVK_ROOT = project_path(settings["paths"]["lakehouse"]) / "bronze" / "kvk"
DATA_TABLE = KVK_ROOT / "basis_bedrijfsgegevens"
LOG_TABLE = KVK_ROOT / "_batch_log"
QUARANTINE_ROOT = project_path(settings["paths"]["lakehouse"]) / "quarantine"
INBOX = project_path(settings["paths"]["inbox"]) / "kvk"     # uploads land here (Docker volume)

DATASET = "KVK Handelsregister Open Dataset Basis Bedrijfsgegevens"
DATASET_PAGE = _CFG["dataset_page"]
DOWNLOAD_URL = _CFG["download_url"]
LICENSE = "CC BY 4.0 (source: KVK)"

MAX_MALFORMED_SHARE = float(_CFG["max_malformed_share"])  # reject above this share
BATCH_ROWS = 200_000               # rows per Arrow batch while writing (memory, not commits)

# Logical column -> accepted header spellings (see config.yaml), compared after
# lower-casing and removing everything that isn't a letter or digit.
REQUIRED_COLUMNS = {k: set(v) for k, v in _CFG["required_columns"].items()}
OPTIONAL_COLUMNS = {k: set(v) for k, v in _CFG["optional_columns"].items()}


# --------------------------------------------------------------------------- state
@dataclass
class Verification:
    """The result of verifying a KVK file, kept in VERIFIED until it is loaded. A load
    is refused when the file's SHA-256 no longer matches: it changed after the check."""
    path: str
    sha256: str
    passed: bool
    csv_member: str | None
    encoding: str | None
    header: list[str]
    row_count: int
    report: dict
    verified_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    file_date: date | None = None


VERIFIED: dict[str, Verification] = {}   # resolved file path -> latest verification


# --------------------------------------------------------------------------- files and the batch log
def resolve_path(file_path: str) -> Path:
    """A file path as typed or dragged into the terminal -> an absolute Path.
    A bare file name that doesn't exist here is looked up in the inbox."""
    # dragging a file into a terminal often adds quotes or escaped spaces
    cleaned = file_path.strip().strip("'\"").replace("\\ ", " ")
    path = Path(cleaned).expanduser()
    if not path.exists() and path.name == cleaned and (INBOX / cleaned).exists():
        path = INBOX / cleaned                       # a bare file name: look in the inbox
    return path.resolve()


def inbox_files() -> list[dict]:
    """Zip and CSV files in the inbox, newest first, marked when already ingested."""
    if not INBOX.is_dir():
        return []
    done = ingested_hashes()
    files = sorted((f for f in INBOX.iterdir()
                    if f.is_file() and f.suffix.lower() in {".zip", ".csv"}),
                   key=lambda f: f.stat().st_mtime, reverse=True)
    out = []
    for f in files:
        batch = done.get(_sha256(f))
        out.append({"file": f.name, "size_mb": round(f.stat().st_size / 1e6, 1),
                    "modified": datetime.fromtimestamp(f.stat().st_mtime, timezone.utc).isoformat(),
                    "already_ingested_as": batch})
    return out


def newest_new_inbox_file() -> str | None:
    """Name of the newest file in the inbox that isn't ingested yet, or None."""
    return next((f["file"] for f in inbox_files() if not f["already_ingested_as"]), None)


def by_snapshot_date(batches):
    """Batch-log rows sorted oldest snapshot first, with a `snapshot_day` column: KVK's file
    date where known, else the upload day. The newest snapshot is the last row.

    Not by load time: an older file found later in the inbox (KVK always uses the same
    file name, so a second download becomes '... (1).zip') would otherwise become
    "the newest" and silver, gold and the freshness check would go back in time. Ties
    (the same file date) go to the later load."""
    import pandas as pd
    df = batches.copy()
    loaded = pd.to_datetime(df["ingested_at"], utc=True).dt.date
    file_day = df["file_date"] if "file_date" in df else pd.Series(None, index=df.index)
    df["snapshot_day"] = file_day.where(file_day.notna(), loaded).map(pd.Timestamp)
    return df.sort_values(["snapshot_day", "ingested_at"]).reset_index(drop=True)


def newest_snapshot_date() -> date | None:
    """KVK file date (or upload day) of the newest snapshot in bronze, or None."""
    if not (LOG_TABLE / "_delta_log").exists():
        return None
    from deltalake import DeltaTable
    log = DeltaTable(str(LOG_TABLE)).to_pandas()
    log = log[log["status"] == "success"]
    return None if log.empty else by_snapshot_date(log)["snapshot_day"].iloc[-1].date()


def ingested_hashes() -> dict[str, str]:
    """file_sha256 -> batch_id of every successful batch."""
    if not (LOG_TABLE / "_delta_log").exists():
        return {}
    from deltalake import DeltaTable
    log = DeltaTable(str(LOG_TABLE)).to_pyarrow_table(
        columns=["file_sha256", "batch_id", "status"]).to_pylist()
    return {r["file_sha256"]: r["batch_id"] for r in log if r["status"] == "success"}


def list_batches() -> list[dict]:
    """The batches in the batch log, oldest first: batch_id, ingested_at (ISO text),
    source_file, row_count, status."""
    if not (LOG_TABLE / "_delta_log").exists():
        return []
    from deltalake import DeltaTable
    rows = DeltaTable(str(LOG_TABLE)).to_pyarrow_table(
        columns=["batch_id", "ingested_at", "source_file", "row_count", "status"]).to_pylist()
    for r in rows:
        r["ingested_at"] = r["ingested_at"].isoformat()
    return sorted(rows, key=lambda r: r["ingested_at"])


# --------------------------------------------------------------------------- verify, confirm, ingest
def verify(file_path: str) -> dict:
    """Verify a KVK file (zip or csv) before it may be ingested; returns the report
    (status PASSED or FAILED and the reasons). The result is kept in VERIFIED, which
    ingest() requires. A rejected file goes to quarantine with its report."""
    path = resolve_path(file_path)
    check = check_file(path, REQUIRED_COLUMNS, OPTIONAL_COLUMNS, MAX_MALFORMED_SHARE,
                       ingested=ingested_hashes())
    newest = newest_snapshot_date() if check.passed else None
    if newest and check.file_date and check.file_date < newest:
        # allowed (bronze keeps history), but it won't change silver: say so
        check.warnings.append(f"this file ({check.file_date}) is older than the newest "
                              f"snapshot in bronze ({newest}): it is kept as history, but "
                              "silver and the dashboard keep using the newest one")
    report = check.report()
    if not check.passed and path.is_file() and "already_ingested" not in check.details:
        # a rejected file is kept with its report; a file that is already in bronze isn't
        from checks.quarantine import quarantine
        folder = quarantine(QUARANTINE_ROOT, "kvk", path.name, report, file=path)
        report["quarantine"] = str(folder) if folder else "already in quarantine"
    VERIFIED[str(path)] = Verification(str(path), check.sha256, check.passed, check.csv_member,
                                       check.encoding, check.header, check.row_count, report,
                                       file_date=check.file_date)
    return report


def confirm_at_keyboard(v: Verification, batch_id: str) -> bool:
    """Human gate: reads from the real keyboard, never from a language model."""
    print("\n" + "=" * 70)
    print(f"  Write {v.row_count:,} rows from {Path(v.path).name}")
    print(f"  to {DATA_TABLE} as batch {batch_id}?")
    print("=" * 70)
    try:
        return input("  Type YES to confirm: ").strip() == "YES"
    except EOFError:
        return False


def ingest(path: Path, confirm, confirmed_by: str) -> dict:
    """Write a verified file to bronze after `confirm(verification, batch_id)` says yes.

    `confirm` is passed in, so each way in brings its own human gate (the keyboard YES
    for the agent and the command line, the typed file name for Dagster) while the
    checks below are the same for all. Refused: a file not verified, verified as FAILED,
    changed since verification (other SHA-256), or already in bronze."""
    v = VERIFIED.get(str(path))
    if v is None:
        return {"status": "REFUSED", "reason": "File was not verified. Run verify_dataset first."}
    if not v.passed:
        return {"status": "REFUSED", "reason": "Verification FAILED for this file.",
                "errors": v.report["errors"]}
    if _sha256(path) != v.sha256:
        VERIFIED.pop(str(path))
        return {"status": "REFUSED", "reason": "File changed since verification. Verify again."}
    if (batch := ingested_hashes().get(v.sha256)):
        return {"status": "REFUSED", "reason": f"Already ingested as batch {batch}"}

    now = datetime.now(timezone.utc)
    batch_id = f"kvk_{now:%Y%m%dT%H%M%SZ}_{v.sha256[:8]}"
    if not confirm(v, batch_id):
        return {"status": "CANCELLED", "reason": "User did not type YES. Nothing written."}

    rows = write_batch(v, batch_id, now)
    write_log(v, batch_id, now, rows, confirmed_by)
    VERIFIED.pop(str(path))
    return {"status": "INGESTED", "batch_id": batch_id, "rows": rows,
            "ingested_at": now.isoformat(), "table": str(DATA_TABLE)}


def ingest_confirmed_file(file_name: str, typed_confirmation: str, confirmed_by: str) -> dict:
    """Verify and ingest one file without a keyboard, for Dagster (orchestration/).

    The person launching the run must type the exact file name as `typed_confirmation`;
    that plays the role of typing YES. The agent never gets this function as a tool, so
    the language model can never call it (a test checks that)."""
    path = resolve_path(file_name)
    report = verify(str(path))
    if report["status"] != "PASSED":
        return {"status": "FAILED", "report": report}
    if typed_confirmation.strip() != path.name:
        VERIFIED.pop(str(path), None)
        return {"status": "CANCELLED",
                "reason": f"Confirmation {typed_confirmation!r} is not the file name {path.name!r}. "
                          "Nothing written."}
    return ingest(path, lambda v, batch_id: True, confirmed_by)


# --------------------------------------------------------------------------- delta writes
def _clean_name(name: str, seen: set[str]) -> str:
    """CSV header -> a valid, unique Delta column name: 'Datum aanvang' -> 'Datum_aanvang'."""
    base = re.sub(r"[^0-9A-Za-z_]", "_", name.strip()) or "column"
    out, i = base, 2
    while out in seen:
        out, i = f"{base}_{i}", i + 1
    seen.add(out)
    return out


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
        print(f"Removed rows of unlogged batch {orphan}")


def write_batch(v: Verification, batch_id: str, now: datetime) -> int:
    """Stream the CSV into the bronze table as one Delta commit. Returns rows written."""
    import pyarrow as pa
    from deltalake import write_deltalake

    remove_orphan_rows()
    seen: set[str] = set()
    cols = [_clean_name(h, seen) for h in v.header]
    schema = pa.schema([pa.field(c, pa.string()) for c in cols] + [
        pa.field("_batch_id", pa.string()), pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("_source_file", pa.string()), pa.field("_source_sha256", pa.string()),
        pa.field("_row_number", pa.int64())])
    written = 0

    def batches():
        """Yield the CSV rows as Arrow batches of BATCH_ROWS rows (keeps memory flat)."""
        nonlocal written
        reader = _iter_rows(Path(v.path), v.csv_member, v.encoding)
        next(reader)  # header
        buf: list[list[str]] = []
        for row in reader:
            if len(row) != len(cols):
                continue  # malformed rows were counted (<= 0.1%) during verification
            buf.append(row)
            if len(buf) >= BATCH_ROWS:
                yield _to_batch(buf)
                buf = []
        if buf:
            yield _to_batch(buf)

    def _to_batch(buf):
        """Rows of text -> one Arrow record batch with the lineage columns added."""
        nonlocal written
        n = len(buf)
        arrays = [pa.array([r[i] for r in buf], pa.string()) for i in range(len(cols))]
        arrays += [pa.array([batch_id] * n, pa.string()),
                   pa.array([now] * n, pa.timestamp("us", tz="UTC")),
                   pa.array([Path(v.path).name] * n, pa.string()),
                   pa.array([v.sha256] * n, pa.string()),
                   pa.array(range(written + 1, written + n + 1), pa.int64())]
        written += n
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    # One call = one Delta commit: a batch is either fully in bronze or not at all.
    # schema_mode="merge" lets KVK add columns later without breaking older batches.
    write_deltalake(str(DATA_TABLE), reader, mode="append", schema_mode="merge")
    return written


def write_log(v: Verification, batch_id: str, now: datetime, rows: int,
              confirmed_by: str | None = None) -> None:
    """Record a written batch in bronze/kvk/_batch_log, with its hash and verification
    report. Written after the data, so an unlogged batch is recognisable as unfinished
    (remove_orphan_rows deletes it before the next write)."""
    import pyarrow as pa
    from deltalake import write_deltalake

    table = pa.table({
        "batch_id": [batch_id], "ingested_at": [now], "source_file": [Path(v.path).name],
        "file_sha256": [v.sha256], "csv_member": [v.csv_member], "row_count": [rows],
        "column_count": [len(v.header)], "verified_at": [v.verified_at],
        "verification_report": [json.dumps(v.report, ensure_ascii=False)],
        "confirmed_by": [confirmed_by or getpass.getuser()], "status": ["success"],
        "file_date": [v.file_date],
    }, schema=pa.schema([
        ("batch_id", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("source_file", pa.string()), ("file_sha256", pa.string()), ("csv_member", pa.string()),
        ("row_count", pa.int64()), ("column_count", pa.int64()),
        ("verified_at", pa.timestamp("us", tz="UTC")), ("verification_report", pa.string()),
        ("confirmed_by", pa.string()), ("status", pa.string()), ("file_date", pa.date32())]))
    # "merge": a column added to the log later is added to the table (empty for old rows)
    write_deltalake(str(LOG_TABLE), table, mode="append", schema_mode="merge")


# --------------------------------------------------------------------------- command line
def run_direct(file_path: str) -> None:
    """Verify, ask YES at the keyboard, ingest: the agent's gates, without an LLM."""
    report = verify(file_path)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if report["status"] != "PASSED":
        sys.exit(1)
    result = ingest(resolve_path(file_path), confirm_at_keyboard, getpass.getuser())
    print(json.dumps(result, indent=2))


def main() -> None:
    """Command line: verify and ingest one KVK file (after typing YES)."""
    ap = argparse.ArgumentParser(description="Get the KVK open dataset into the KVK bronze layer "
                                             "(for a guided chat: python -m pipeline.bronze.kvk_agent).")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", help="verify and ingest this file "
                                    "(a bare file name is looked up in the inbox)")
    src.add_argument("--inbox", action="store_true",
                     help="verify and ingest the newest new file in the inbox")
    args = ap.parse_args()
    if args.inbox:
        newest = newest_new_inbox_file()
        if newest is None:
            sys.exit(f"No new KVK file in {INBOX}. Download it from {DOWNLOAD_URL}")
        run_direct(newest)
    else:
        run_direct(args.file)


if __name__ == "__main__":
    main()
