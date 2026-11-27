"""The batch log of a file source in bronze: shared by the KVK file (kvk.py) and the SBI
crosswalk (reference.py).

Both load a whole file as one batch and follow the same two rules:
* load once: a file whose SHA-256 is in the batch log (status 'success') is not loaded
  again, whatever its name (ingested_hashes)
* data first, log second: a batch is written in ONE Delta commit and only then logged.
  Rows of a batch that was written but never logged (a crash in between) are removed
  before the next write, so a retry never doubles them (remove_orphan_rows)

The tables are passed in, so each source keeps its own log and data table. Their log
tables need at least: file_sha256, batch_id, status; the data tables: _batch_id.
"""
from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger("bronze_batchlog")


def _exists(table: Path) -> bool:
    return (table / "_delta_log").exists()


def ingested_hashes(log_table: Path) -> dict[str, str]:
    """file_sha256 -> batch_id of every successful batch in `log_table` ({} without a log)."""
    if not _exists(log_table):
        return {}
    from deltalake import DeltaTable
    rows = DeltaTable(str(log_table)).to_pyarrow_table(
        columns=["file_sha256", "batch_id", "status"]).to_pylist()
    return {r["file_sha256"]: r["batch_id"] for r in rows if r["status"] == "success"}


def remove_orphan_rows(data_table: Path, log_table: Path) -> list[str]:
    """Delete the rows of every batch in `data_table` that `log_table` doesn't list as
    successful (written, but the run stopped before logging it). Returns their batch_ids."""
    if not _exists(data_table):
        return []
    from deltalake import DeltaTable
    good = set(ingested_hashes(log_table).values())
    dt = DeltaTable(str(data_table))
    present = set(dt.to_pyarrow_table(columns=["_batch_id"]).column("_batch_id").unique().to_pylist())
    orphans = sorted(present - good)
    for orphan in orphans:
        dt.delete(f"_batch_id = '{orphan}'")
        log.warning("Removed rows of unlogged batch %s from %s", orphan, data_table.name)
    return orphans
