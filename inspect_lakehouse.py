"""
Overview of everything in the lakehouse: one line per Delta table, then a list of problems.

Per table: number of rows, Delta version, first and last period, latest ingest time,
and extras where they apply (KVK batches, unknown keys in silver facts).

Problems reported:
  - configured CBS bronze endpoints without a table (not ingested yet, or failed)
  - expected silver tables that are missing
  - empty tables
  - silver fact rows whose company characteristic is unknown in the code list

Usage
-----
    python inspect_lakehouse.py                  # all layers
    python inspect_lakehouse.py --layer silver   # one layer
    python inspect_lakehouse.py --root /data/lakehouse
Exit code 1 when problems are found, so it can also be used in scripts.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PERIOD_COLUMNS = ("period_key", "_period", "Perioden")
INGEST_COLUMNS = ("ingested_at", "_ingested_at")
EXPECTED_SILVER = [
    "dim_period", "dim_sector", "dim_company_size", "dim_region", "dim_survey_measure",
    "map_characteristic", "fct_absence_rate", "fct_absence_survey",
    "fct_business_count", "fct_establishment_count",
]


def find_tables(root: Path) -> list[Path]:
    """Every folder under root that is a Delta table (has a _delta_log folder)."""
    if not root.exists():
        return []
    return sorted(p.parent for p in root.rglob("_delta_log") if p.is_dir())


def _min_max(dataset, column):
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
        provisional = pc.sum(pc.equal(st, "Voorlopig")).as_py() or 0
        if provisional:
            info["extra"].append(f"{provisional} provisional rows")
    return info


def _fmt(value) -> str:
    if value is None:
        return "-"
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def expected_bronze_names() -> list[str]:
    try:
        import cbs_bronze
        return sorted({e.name for e in cbs_bronze.ENDPOINTS})
    except Exception:  # running outside the project folder
        return []


def main() -> int:
    ap = argparse.ArgumentParser(description="Overview of the lakehouse Delta tables.")
    ap.add_argument("--root", default="lakehouse", help="lakehouse folder (default: lakehouse)")
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
                problems.append(f"bronze/{n}: configured in cbs_bronze.py but no table yet")
        if layer == "silver" and tables:
            for n in EXPECTED_SILVER:
                if n not in present:
                    problems.append(f"silver/{n}: expected but missing")

    print("\n=== problems")
    if problems:
        for p in problems:
            print(f"  - {p}")
    else:
        print("  none")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
