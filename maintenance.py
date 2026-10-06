"""
Maintenance tasks, meant to run monthly (later as a Dagster job, never at the same time
as an ingestion).

Quarantine cleanup
------------------
Removes quarantine entries that are no longer needed:
  * resolved: the same CBS table/entity/period was ingested successfully after it was
    quarantined, or a newer KVK / reference batch was loaded after a file was rejected
  * older than quarantine.retention_days (config.yaml). If the problem still exists,
    the next ingestion run quarantines the batch again, so nothing goes unnoticed.

Still to add (TODO.md): KVK retention, Delta optimize and vacuum.

Usage
-----
    python maintenance.py quarantine --dry-run   # list what would be removed
    python maintenance.py quarantine             # remove it
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

import pandas as pd

from checks.quarantine import cleanup
from config import project_path, settings

LAKEHOUSE = project_path(settings["paths"]["lakehouse"])
QUARANTINE_ROOT = LAKEHOUSE / "quarantine"
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


def clean_quarantine(dry_run: bool = False) -> list[tuple[str, str]]:
    """Run the quarantine cleanup with the configured retention; returns (folder, reason) pairs."""
    days = int(settings["quarantine"]["retention_days"])
    return cleanup(QUARANTINE_ROOT, make_resolver(), days, dry_run=dry_run)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Lakehouse maintenance tasks.")
    sub = ap.add_subparsers(dest="task", required=True)
    q = sub.add_parser("quarantine", help="remove resolved and old quarantine entries")
    q.add_argument("--dry-run", action="store_true", help="only list what would be removed")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    removed = clean_quarantine(dry_run=args.dry_run)
    verb = "Would remove" if args.dry_run else "Removed"
    for folder, reason in removed:
        log.info("%s %s (%s)", verb, folder, reason)
    log.info("%s %d quarantine entries", verb, len(removed))
