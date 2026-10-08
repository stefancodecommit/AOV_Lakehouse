"""
Publishing a silver (later also gold) run: staging, swap and the run log.

A run builds all its tables, writes them to <layer>/_staging/<run_id>/, and only when
every table was staged replaces the live tables, each with a `run_id` column. A run that
fails leaves the live tables untouched. Every run, successful or not, is a row in
lakehouse/_run_log (runlog.py).

Delta can't replace several tables in one commit: a crash while publishing could leave
tables of two runs. RunFailed then names the tables already replaced, and
inspect_lakehouse.py reports tables whose run_id isn't the latest successful run.
"""
from __future__ import annotations

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger("publish")


class RunFailed(RuntimeError):
    pass


def to_arrow(df: pd.DataFrame):
    """DataFrame -> Arrow table with the types silver stores (strings, UTC timestamps).
    Categories (used to save memory) are stored as their plain values, so a table's
    schema doesn't depend on how a DataFrame was built in memory."""
    import pyarrow as pa
    table = pa.Table.from_pandas(df, preserve_index=False)
    fields = []
    for f in table.schema:
        t = f.type
        if pa.types.is_dictionary(t):
            t = t.value_type         # pandas categories: store the plain values
        if pa.types.is_null(t) or pa.types.is_large_string(t):
            t = pa.string()          # all-null columns get a real type
        elif pa.types.is_timestamp(t):
            t = pa.timestamp("us", tz="UTC")
        fields.append(pa.field(f.name, t))
    return table.cast(pa.schema(fields), safe=False)


def _overwrite(path: Path, table) -> None:
    """Replace a Delta table with `table` in one commit (the old version stays in the
    Delta history until vacuum). schema_mode='overwrite' allows columns to change."""
    from deltalake import write_deltalake
    write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")


def write_table(path: Path, df: pd.DataFrame) -> None:
    """Write a DataFrame as a Delta table, replacing what was there."""
    _overwrite(path, to_arrow(df))


def stage_and_publish(root: Path, run_id: str, tables: dict[str, pd.DataFrame],
                      optional: set[str] = frozenset()) -> dict:
    """Write every table to root/_staging/<run_id>/, then replace the live tables under
    root. Nothing live changes unless all tables were staged. Returns per-table results."""
    from deltalake import DeltaTable
    results: dict[str, dict] = {}
    empty_required = [n for n, df in tables.items() if df.empty and n not in optional]
    if empty_required:
        raise RunFailed(f"required tables came out empty: {', '.join(empty_required)}")

    staging = root / "_staging" / run_id
    try:
        # 1. stage every table; the live tables are not touched yet
        for name, df in tables.items():
            if df.empty:
                results[name] = {"rows": 0, "status": "skipped"}
                log.warning("%-26s empty, skipped (optional table)", name)
                continue
            write_table(staging / name, df.assign(run_id=run_id))
            results[name] = {"rows": len(df), "status": "staged"}

        # 2. all staged: replace the live tables one by one
        for name, r in results.items():
            if r["status"] != "staged":
                continue
            # copy the staged table as written, so live gets exactly what was staged
            _overwrite(root / name, DeltaTable(str(staging / name)).to_pyarrow_table())
            r["status"] = "published"
            log.info("%-26s %8d rows -> %s", name, r["rows"], root / name)
    except Exception as e:
        done = [n for n, r in results.items() if r["status"] == "published"]
        if done:   # a crash while publishing: say exactly which tables are new
            raise RunFailed(f"publish incomplete after {len(done)} tables ({', '.join(done)}): "
                            f"{type(e).__name__}: {e}") from e
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        if staging.parent.exists() and not any(staging.parent.iterdir()):
            staging.parent.rmdir()
    return results


def run_pipeline(pipeline: str, build, root: Path, run_log: Path,
                 optional: set[str] = frozenset()) -> str:
    """Build, stage, publish and log one run. `build(inputs)` returns {table: DataFrame}
    and records the Delta version of every table it reads in `inputs`. Returns the
    run_id; raises (after logging the failed run) when something went wrong."""
    import runlog
    started = datetime.now(timezone.utc)
    run_id = runlog.new_run_id(pipeline, started)
    inputs: dict = {}
    tables: dict = {}
    try:
        built = build(inputs)
        tables = {n: {"rows": len(df), "status": "built"} for n, df in built.items()}
        tables = stage_and_publish(root, run_id, built, optional)
    except Exception as e:
        runlog.write_run(run_id, pipeline, started, "failed", tables, inputs,
                         error=f"{type(e).__name__}: {e}", path=run_log)
        log.error("Run %s failed: %s", run_id, e)
        raise
    runlog.write_run(run_id, pipeline, started, "success", tables, inputs, path=run_log)
    log.info("Run %s published %d tables", run_id,
             sum(r["status"] == "published" for r in tables.values()))
    return run_id
