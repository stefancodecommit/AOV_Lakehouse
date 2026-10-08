"""
Overview of everything in the lakehouse: one line per Delta table, then a list of problems.

Per table: number of rows, Delta version, first and last period, latest ingest time,
and extras where they apply (KVK batches, unknown keys in silver facts).

Problems reported:
  - configured CBS bronze endpoints without a table (not ingested yet, or failed)
  - expected silver tables that are missing
  - empty tables
  - silver fact rows whose company characteristic is unknown in the code list
  - batches or files in quarantine (failed their checks; see report.json in each folder)

  - silver/gold tables whose run_id isn't the latest successful run of their pipeline
    (silver_cbs, silver_kvk, gold; a publish that stopped halfway), or a latest run
    that failed

Warnings (shown, but not counted as problems): CBS tables that CBS has stopped or hasn't
modified for longer than their frequency allows, from the newest TableInfos in bronze.

Usage
-----
    python -m tools.inspect_lakehouse                 # all layers
    python -m tools.inspect_lakehouse --layer silver  # one layer
    python -m tools.inspect_lakehouse --root /data/lakehouse
Exit code 1 when problems are found, so it can also be used in scripts.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PERIOD_COLUMNS = ("period_key", "_period", "Perioden")
INGEST_COLUMNS = ("ingested_at", "_ingested_at", "data_as_of")
EXPECTED_GOLD = ["gold_absence_by_sector", "gold_absence_by_size", "gold_absence_profile",
                 "gold_business_context", "gold_kvk_context"]
EXPECTED_SILVER = [
    "dim_period", "dim_sector", "dim_company_size", "dim_region", "dim_survey_measure",
    "map_characteristic", "map_sbi2008_sbi2025", "fct_absence_rate", "fct_absence_survey",
    "fct_business_count", "fct_establishment_count", "kvk_company",
]


def find_tables(root: Path) -> list[Path]:
    """Every folder under root that is a Delta table (has a _delta_log folder)."""
    if not root.exists():
        return []
    return sorted(p.parent for p in root.rglob("_delta_log") if p.is_dir())


def _min_max(dataset, column):
    """Smallest and largest non-empty value of one column, e.g. the first and last period."""
    import pyarrow.compute as pc
    col = dataset.to_table(columns=[column]).column(column).drop_null()
    if len(col) == 0:
        return None, None
    try:
        mm = pc.min_max(col)
        return mm["min"].as_py(), mm["max"].as_py()
    except Exception:  # types without min_max support in older pyarrow
        values = col.to_pylist()
        return min(values), max(values)


def summarize(path: Path) -> dict:
    """Facts about one Delta table for the overview: rows, version, first/last period,
    latest ingest, and notes (batches, unknown keys, provisional rows). It reads only
    the columns it needs, so even the 1.9M-row KVK table is quick."""
    from deltalake import DeltaTable
    import pyarrow.compute as pc

    dt = DeltaTable(str(path))
    ds = dt.to_pyarrow_dataset()
    columns = set(ds.schema.names)
    info = {"path": path, "rows": ds.count_rows(), "version": dt.version(), "extra": []}

    if period_col := next((c for c in PERIOD_COLUMNS if c in columns), None):
        info["first_period"], info["last_period"] = _min_max(ds, period_col)
    if ingest_col := next((c for c in INGEST_COLUMNS if c in columns), None):
        _, latest = _min_max(ds, ingest_col)
        info["latest_ingest"] = latest

    if "_batch_id" in columns:
        batches = ds.to_table(columns=["_batch_id"]).column("_batch_id").unique()
        info["extra"].append(f"{len(batches)} batch(es)")
    if "member_type" in columns and info["rows"]:
        mt = ds.to_table(columns=["member_type"]).column("member_type")
        unknown = pc.sum(pc.equal(mt, "unknown")).as_py() or 0
        info["unknown_keys"] = unknown
        if unknown:
            info["extra"].append(f"{unknown} unknown key rows")
    if "period_status" in columns and info["rows"]:
        st = ds.to_table(columns=["period_status"]).column("period_status")
        # anything with a status other than final: Voorlopig and NaderVoorlopig
        provisional = pc.sum(pc.not_equal(st.drop_null(), "Definitief")).as_py() or 0
        if provisional:
            info["extra"].append(f"{provisional} provisional rows")
    return info


def _fmt(value) -> str:
    """Value for the overview table: '-' for none, timestamps to the minute."""
    if value is None:
        return "-"
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def expected_bronze_names() -> list[str]:
    """Bronze table names the configuration should produce (to spot missing tables)."""
    try:
        from pipeline.bronze import cbs as cbs_bronze
        return sorted({e.name for e in cbs_bronze.ENDPOINTS})
    except Exception:  # running outside the project folder
        return []


def pipeline_layer(pipeline: str) -> str:
    """Layer a pipeline publishes to: 'gold' for gold, 'silver' for silver_cbs/silver_kvk."""
    return "gold" if pipeline.startswith("gold") else "silver"


def silver_run_problems(root: Path, pipeline: str = "silver_cbs") -> tuple[list[str], list[str]]:
    """(summary lines, problems) from the run log and the run_id of each table the
    pipeline's latest successful run published (silver or gold)."""
    import pyarrow.compute as pc
    from deltalake import DeltaTable
    from pipeline.common import runlog
    runs = runlog.read_runs(root / "_run_log")
    runs = runs[runs["pipeline"] == pipeline]
    if runs.empty:
        return ["(no runs logged)"], []
    lines, problems = [], []
    for r in runs.tail(5).itertuples():
        lines.append(f"{r.run_id:<44}{r.status:<9}{str(r.started_at)[:19]}  "
                     f"{(r.code_version or ''):<16}{(r.error or '')[:60]}")
    last = runs.iloc[-1]
    if last["status"] != "success":
        problems.append(f"_run_log: latest {pipeline} run {last['run_id']} failed: {last['error']}")
    ok = runs[runs["status"] == "success"]
    if ok.empty:
        return lines, problems
    latest = ok.iloc[-1]
    for name, info in json.loads(latest["tables"]).items():
        path = root / pipeline_layer(pipeline) / name
        if info.get("status") != "published" or not (path / "_delta_log").exists():
            continue
        ids = DeltaTable(str(path)).to_pyarrow_dataset().to_table(columns=["run_id"]).column("run_id")
        found = set(pc.unique(ids).to_pylist())
        if found != {latest["run_id"]}:
            problems.append(f"{pipeline_layer(pipeline)}/{name}: run_id {sorted(map(str, found))} is not the latest "
                            f"successful run {latest['run_id']} (publish incomplete?)")
    return lines, problems


def lifecycle_warnings(bronze: Path) -> list[str]:
    """Lifecycle warnings from the newest TableInfos snapshot of each CBS table."""
    import json
    from deltalake import DeltaTable
    from checks import warnings
    from checks.cbs import check_table_lifecycle
    out = []
    for path in sorted(bronze.glob("*_tableinfos")):
        df = DeltaTable(str(path)).to_pandas(columns=["_table_id", "_ingested_at", "raw_json"])
        if df.empty:
            continue
        newest = df.sort_values("_ingested_at").iloc[-1]
        for w in warnings(check_table_lifecycle(json.loads(newest["raw_json"]))):
            out.append(f"{newest['_table_id']}: {w.detail}")
    return out


def main() -> int:
    """Print the overview per layer, quarantine, runs, warnings and problems.
    Returns 1 when there are problems, so scripts (and later Dagster) can react."""
    ap = argparse.ArgumentParser(description="Overview of the lakehouse Delta tables.")
    from pipeline.common.config import project_path, settings
    ap.add_argument("--root", default=str(project_path(settings["paths"]["lakehouse"])),
                    help="lakehouse folder (default: paths.lakehouse in config.yaml)")
    ap.add_argument("--layer", choices=["bronze", "silver", "gold"], help="only this layer")
    args = ap.parse_args()

    root = Path(args.root)
    layers = [args.layer] if args.layer else ["bronze", "silver", "gold"]
    problems: list[str] = []

    for layer in layers:
        tables = find_tables(root / layer)
        print(f"\n=== {layer} ({len(tables)} tables)")
        if not tables:
            print("  (none)")
        else:
            print(f"  {'table':<44}{'rows':>10}{'ver':>5}  {'periods':<21}{'latest ingest':<17} notes")
        for path in tables:
            name = str(path.relative_to(root / layer))
            try:
                t = summarize(path)
            except Exception as e:
                problems.append(f"{layer}/{name}: could not be read ({type(e).__name__}: {e})")
                print(f"  {name:<44}  ERROR {e}")
                continue
            periods = (f"{_fmt(t.get('first_period'))} .. {_fmt(t.get('last_period'))}"
                       if t.get("first_period") else "-")
            print(f"  {name:<44}{t['rows']:>10,}{t['version']:>5}  {periods:<21}"
                  f"{_fmt(t.get('latest_ingest')):<17} {', '.join(t['extra'])}")
            if t["rows"] == 0:
                problems.append(f"{layer}/{name}: table is empty")
            if t.get("unknown_keys"):
                problems.append(f"{layer}/{name}: {t['unknown_keys']} rows with keys missing "
                                "from the code list (map_characteristic member_type = 'unknown')")

        present = {str(p.relative_to(root / layer)) for p in tables}
        if layer == "bronze":
            missing = [n for n in expected_bronze_names() if n not in present]
            for n in missing:
                problems.append(f"bronze/{n}: configured in pipeline/bronze/cbs.py but no table yet")
        expected = {"silver": EXPECTED_SILVER, "gold": EXPECTED_GOLD}.get(layer, [])
        if tables:
            for n in expected:
                if n not in present:
                    problems.append(f"{layer}/{n}: expected but missing")

    if not args.layer or args.layer == "bronze":
        from checks.quarantine import list_entries
        entries = list_entries(root / "quarantine")
        print(f"\n=== quarantine ({len(entries)} batches)")
        if not entries:
            print("  (none)")
        for e in entries:
            failed = [c["name"] for c in e.get("checks", []) if not c.get("passed")] or e.get("errors", [])
            print(f"  {e.get('source', '?')}/{e.get('name', '?'):<46}{str(e.get('quarantined_at', ''))[:16]:<17} "
                  f"{'; '.join(map(str, failed))[:80]}")
            problems.append(f"quarantine/{e.get('source', '?')}/{e.get('name', '?')}: failed its checks "
                            f"(see {e['folder']}/report.json)")

    if not args.layer or args.layer in ("silver", "gold"):
        from pipeline.common import runlog
        pipelines = sorted(set(runlog.read_runs(root / "_run_log")["pipeline"]))
        for pipeline in [p for p in pipelines if not args.layer or pipeline_layer(p) == args.layer]:
            lines, run_problems = silver_run_problems(root, pipeline)
            print(f"\n=== {pipeline} runs (latest 5, from _run_log)")
            for line in lines:
                print(f"  {line}")
            problems += run_problems

    if not args.layer or args.layer == "bronze":
        found = lifecycle_warnings(root / "bronze")
        print(f"\n=== warnings ({len(found)})")
        for w in found or ["(none)"]:
            print(f"  {w}")

    print("\n=== problems")
    if problems:
        for p in problems:
            print(f"  - {p}")
    else:
        print("  none")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
