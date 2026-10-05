"""
Run log: one row per pipeline run in the Delta table lakehouse/_run_log.

Every table a run publishes carries the run's `run_id` column, so any silver (and later
gold) row can be joined to the run that produced it:

    SELECT ... FROM silver.fct_absence_rate f JOIN _run_log r USING (run_id)

Columns
-------
run_id        e.g. silver_cbs_20261005T163000123456Z
pipeline      silver_cbs (later also silver_kvk, gold, ...)
started_at    UTC
finished_at   UTC
status        success | failed
error         what went wrong (failed runs)
tables        JSON: {table: {"rows": n, "status": "published" | "skipped"}}
inputs        JSON: {input table: Delta version read}, e.g. bronze tables
code_version  git commit of the code that ran ("-dirty" with uncommitted changes)
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from config import PROJECT_DIR, project_path, settings

RUN_LOG = project_path(settings["paths"]["lakehouse"]) / "_run_log"


def new_run_id(pipeline: str, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{pipeline}_{now:%Y%m%dT%H%M%S%fZ}"


def code_version() -> str | None:
    """Short git commit of the project, with -dirty when there are uncommitted changes."""
    try:
        commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=PROJECT_DIR,
                                capture_output=True, text=True, timeout=5, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"],
                               cwd=PROJECT_DIR, capture_output=True, text=True, timeout=5).stdout
        return commit + ("-dirty" if dirty.strip() else "")
    except (OSError, subprocess.SubprocessError):
        return None


def schema():
    import pyarrow as pa
    return pa.schema([
        ("run_id", pa.string()), ("pipeline", pa.string()),
        ("started_at", pa.timestamp("us", tz="UTC")), ("finished_at", pa.timestamp("us", tz="UTC")),
        ("status", pa.string()), ("error", pa.string()), ("tables", pa.string()),
        ("inputs", pa.string()), ("code_version", pa.string())])


def write_run(run_id: str, pipeline: str, started_at: datetime, status: str,
              tables: dict | None = None, inputs: dict | None = None,
              error: str | None = None, path: Path | None = None) -> None:
    import pyarrow as pa
    from deltalake import write_deltalake
    row = pa.table({
        "run_id": [run_id], "pipeline": [pipeline], "started_at": [started_at],
        "finished_at": [datetime.now(timezone.utc)], "status": [status], "error": [error],
        "tables": [json.dumps(tables or {}, sort_keys=True)],
        "inputs": [json.dumps(inputs or {}, sort_keys=True)],
        "code_version": [code_version()]}, schema=schema())
    write_deltalake(str(path or RUN_LOG), row, mode="append")


def read_runs(path: Path | None = None):
    """The run log as a DataFrame, oldest first (empty when nothing has run yet)."""
    import pandas as pd
    path = path or RUN_LOG
    if not (path / "_delta_log").exists():
        return pd.DataFrame(columns=schema().names)
    from deltalake import DeltaTable
    return DeltaTable(str(path)).to_pandas().sort_values("started_at").reset_index(drop=True)
