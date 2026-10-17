"""
Maintenance tasks, run monthly by Dagster (orchestration/, never at the same time as an
ingestion) or by hand.

Quarantine cleanup
------------------
Removes quarantine entries that are no longer needed:
  * resolved: the same CBS table/entity/period was ingested successfully after it was
    quarantined, or a newer KVK / reference batch was loaded after a file was rejected
  * older than quarantine.retention_days (config.yaml). If the problem still exists,
    the next ingestion run quarantines the batch again, so nothing goes unnoticed.

KVK retention
-------------
Each KVK upload is a full snapshot (~22 MB). Kept: the newest snapshot, plus the first
snapshot of every calendar month (so trends over time stay possible). The rows of other
batches are deleted from bronze; their batch-log entry stays, marked "retired", so the
history of what was loaded remains. Silver reads only "success" batches: its next run
deletes the retired snapshots from kvk_company too (they are never the latest one).

Personal data in the KVK upload log (risk R10)
----------------------------------------------
The batch log records who confirmed each load (confirmed_by: a username). After
retention.confirmed_by_days that name is replaced by "removed"; the row itself stays,
because the history of what was loaded is still needed. Older rows can also hold the
full path of the uploaded file in verification_report (it contains the home folder, so
the username); that is shortened to the file name, for every row. New rows store the
file name only (checks/kvk.py).

Delta optimize and vacuum
-------------------------
Every overwrite or delete leaves the old Parquet files on disk (Delta keeps them for
"time travel"). `optimize` merges small files into larger ones; `vacuum` deletes files
that no version newer than delta.vacuum_retention_hours still needs.

Usage
-----
    python -m tools.maintenance quarantine --dry-run  # list what would be removed
    python -m tools.maintenance quarantine            # remove it
    python -m tools.maintenance kvk-retention [--dry-run]
    python -m tools.maintenance personal-data [--dry-run]  # retention in the KVK upload log
    python -m tools.maintenance delta [--dry-run]  # optimize + vacuum every Delta table
    python -m tools.maintenance all [--dry-run]    # all of the above
The monthly Dagster job runs all of the above, then also removes old Dagster runs and
makes a backup (tools/backup.py).
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from checks.quarantine import cleanup
from pipeline.common.config import project_path, settings

LAKEHOUSE = project_path(settings["paths"]["lakehouse"])
QUARANTINE_ROOT = LAKEHOUSE / "quarantine"
KVK_ROOT = LAKEHOUSE / "bronze" / "kvk"
BATCH_LOGS = {                                   # source -> its log of successful loads
    "cbs": LAKEHOUSE / "bronze" / "_ingestion_log",
    "kvk": LAKEHOUSE / "bronze" / "kvk" / "_batch_log",
    "reference": LAKEHOUSE / "bronze" / "reference" / "_batch_log",
}

log = logging.getLogger("maintenance")


def _read_log(path) -> pd.DataFrame:
    """A batch or ingestion log as a DataFrame (successful rows only); empty if missing."""
    if not (path / "_delta_log").exists():
        return pd.DataFrame()
    from deltalake import DeltaTable
    df = DeltaTable(str(path)).to_pandas()
    if "status" in df:
        df = df[df["status"] == "success"]
    return df


def make_resolver(logs: dict | None = None):
    """is_resolved(entry) -> reason or None, reading each batch log once."""
    logs = {src: _read_log(path) for src, path in BATCH_LOGS.items()} if logs is None else logs

    def is_resolved(entry: dict) -> str | None:
        """Reason why this quarantine entry is resolved, or None if it isn't."""
        df = logs.get(entry.get("source"), pd.DataFrame())
        if df.empty or "quarantined_at" not in entry:
            return None
        later = df[df["ingested_at"] > pd.Timestamp(datetime.fromisoformat(entry["quarantined_at"]))]
        if entry["source"] == "cbs":
            same = ((later["table_id"] == entry.get("table_id"))
                    & (later["entity"] == entry.get("entity"))
                    & ((later["period"] == entry.get("period"))
                       | (later["period"].isna() & (entry.get("period") is None))))
            later = later[same]
            key = "request_key"
        else:
            key = "batch_id"
        if later.empty:
            return None
        return f"resolved: {later.sort_values('ingested_at')[key].iloc[-1]} ingested later"

    return is_resolved


def kvk_retention(dry_run: bool = False, path: Path | None = None) -> list[str]:
    """Delete the rows of KVK batches that are neither the newest nor the first of their
    month, and mark them "retired" in the batch log. Returns the retired batch_ids."""
    from deltalake import DeltaTable
    root = path or KVK_ROOT
    data, batch_log = root / "basis_bedrijfsgegevens", root / "_batch_log"
    if not (batch_log / "_delta_log").exists():
        return []
    batches = _read_log(batch_log).sort_values("ingested_at")
    if batches.empty:
        return []
    month = batches["ingested_at"].dt.strftime("%Y-%m")
    keep = set(batches.groupby(month)["batch_id"].first()) | {batches["batch_id"].iloc[-1]}
    retire = [b for b in batches["batch_id"] if b not in keep]
    if retire and not dry_run:
        ids = ", ".join(f"'{b}'" for b in retire)
        if (data / "_delta_log").exists():
            DeltaTable(str(data)).delete(f"_batch_id IN ({ids})")
        DeltaTable(str(batch_log)).update(new_values={"status": "retired"},
                                          predicate=f"batch_id IN ({ids})")
    return retire


REMOVED = "removed"


def clear_personal_data(dry_run: bool = False, path: Path | None = None,
                        now: datetime | None = None) -> list[str]:
    """Remove confirmed_by from KVK batch-log rows older than retention.confirmed_by_days
    and shorten file paths in verification_report to the file name. Returns the batch_ids
    of the rows changed. The log is small, so it is rewritten in one commit."""
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    batch_log = path or KVK_ROOT / "_batch_log"
    if not (batch_log / "_delta_log").exists():
        return []
    days = int(settings["retention"]["confirmed_by_days"])
    cutoff = (pd.Timestamp(now) if now else pd.Timestamp.now(tz="UTC")) - pd.Timedelta(days=days)
    table = DeltaTable(str(batch_log)).to_pyarrow_table()
    rows = table.to_pylist()
    changed = []
    for row in rows:
        before = (row["confirmed_by"], row["verification_report"])
        if row["ingested_at"] is not None and pd.Timestamp(row["ingested_at"]) < cutoff:
            row["confirmed_by"] = REMOVED
        try:
            report = json.loads(row["verification_report"] or "{}")
        except ValueError:
            report = {}
        if isinstance(report.get("file"), str) and Path(report["file"]).name != report["file"]:
            report["file"] = Path(report["file"]).name
            row["verification_report"] = json.dumps(report, ensure_ascii=False)
        if (row["confirmed_by"], row["verification_report"]) != before:
            changed.append(row["batch_id"])
    if changed and not dry_run:
        write_deltalake(str(batch_log), pa.Table.from_pylist(rows, schema=table.schema),
                        mode="overwrite")
    return changed


def delta_tables(root: Path | None = None) -> list[Path]:
    """Every Delta table in the lakehouse (folders with a _delta_log), staging excluded."""
    root = root or LAKEHOUSE
    return sorted(p.parent for p in root.rglob("_delta_log")
                  if p.is_dir() and "_staging" not in p.parts)


def optimize_and_vacuum(dry_run: bool = False, root: Path | None = None) -> dict[str, int]:
    """Compact small files and remove files older versions no longer need, per table.
    Returns {table: number of files vacuumed (or, with dry_run, that would be)}."""
    from deltalake import DeltaTable
    hours = int(settings["delta"]["vacuum_retention_hours"])
    out = {}
    for path in delta_tables(root):
        dt = DeltaTable(str(path))
        if not dry_run:
            dt.optimize.compact()
        # vacuum lists every file the log marks as removed, also ones already deleted
        # by an earlier vacuum; count only the files that are still on disk
        stale = [f for f in dt.vacuum(retention_hours=hours, dry_run=True,
                                      enforce_retention_duration=False)
                 if (path / f).exists()]
        if stale and not dry_run:
            dt.vacuum(retention_hours=hours, dry_run=False, enforce_retention_duration=False)
        out[str(path.relative_to(root or LAKEHOUSE))] = len(stale)
    return out


def clean_quarantine(dry_run: bool = False) -> list[tuple[str, str]]:
    """Run the quarantine cleanup with the configured retention; returns (folder, reason) pairs."""
    days = int(settings["quarantine"]["retention_days"])
    return cleanup(QUARANTINE_ROOT, make_resolver(), days, dry_run=dry_run)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Lakehouse maintenance tasks.")
    ap.add_argument("task", choices=["quarantine", "kvk-retention", "personal-data", "delta",
                                     "all"])
    ap.add_argument("--dry-run", action="store_true", help="only list what would be done")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    verb = "Would" if args.dry_run else "Did"

    if args.task in ("quarantine", "all"):
        removed = clean_quarantine(dry_run=args.dry_run)
        for folder, reason in removed:
            log.info("%s remove %s (%s)", verb, folder, reason)
        log.info("Quarantine: %s remove %d entries", verb.lower(), len(removed))
    if args.task in ("kvk-retention", "all"):
        retired = kvk_retention(dry_run=args.dry_run)
        log.info("KVK retention: %s retire %d batches %s", verb.lower(), len(retired), retired)
    if args.task in ("personal-data", "all"):
        cleared = clear_personal_data(dry_run=args.dry_run)
        log.info("KVK upload log: %s clear personal data in %d rows %s", verb.lower(),
                 len(cleared), cleared)
    if args.task in ("delta", "all"):
        files = optimize_and_vacuum(dry_run=args.dry_run)
        log.info("Delta: %s vacuum %d old files in %d tables", verb.lower(),
                 sum(files.values()), len(files))
