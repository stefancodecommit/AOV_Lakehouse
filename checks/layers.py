"""Checks on a whole layer after it was published (silver CBS, silver KVK, gold).

Dagster runs these as blocking asset checks: a failing error-level check stops the next
layer from being built on top. They take the tables as DataFrames, so pytest can run
them on sample data too.
"""
from __future__ import annotations

import pandas as pd

from checks import CheckResult

# table -> columns that must identify a row (no two rows with the same values)
SILVER_CBS_KEYS = {
    "dim_period": ["period_key"],
    "dim_sector": ["sector_key"],
    "dim_company_size": ["size_key"],
    "dim_region": ["region_key"],
    "dim_survey_measure": ["source_table", "measure_key"],
    "map_characteristic": ["source_table", "characteristic_key"],
    "fct_absence_rate": ["period_key", "characteristic_key"],
    "fct_absence_survey": ["source_table", "period_key", "characteristic_key", "measure_key"],
    "fct_business_count": ["period_key", "sector_key", "measure_key"],
    "fct_establishment_count": ["period_key", "sector_key", "region_key", "measure_key"],
}
GOLD_KEYS = {
    "gold_absence_by_sector": ["period_key", "sector_key"],
    "gold_absence_by_size": ["source_table", "period_key", "member_type", "size_key"],
    "gold_absence_profile": ["period_key", "characteristic_key", "measure_key"],
    "gold_business_context": ["period_key", "sector_key", "size_band"],
    "gold_kvk_context": ["legal_form", "sbi2008_section"],
}
SBI_SECTIONS = set("ABCDEFGHIJKLMNOPQRSTU") | {"unknown"}


def not_empty(tables: dict[str, pd.DataFrame], required) -> CheckResult:
    empty = sorted(n for n in required if tables.get(n) is None or tables[n].empty)
    return CheckResult("tables_not_empty", not empty,
                       f"empty or missing: {', '.join(empty)}" if empty
                       else f"{len(list(required))} tables have rows", empty)


def one_run(tables: dict[str, pd.DataFrame]) -> CheckResult:
    """All tables of a layer come from the same run (a publish didn't stop halfway)."""
    ids = {n: sorted(df["run_id"].dropna().unique()) for n, df in tables.items()
           if not df.empty and "run_id" in df}
    distinct = sorted({i for v in ids.values() for i in v})
    return CheckResult("one_run_id", len(distinct) <= 1,
                       f"run_ids: {distinct}" if len(distinct) > 1 else f"run_id {distinct}",
                       [{"table": n, "run_ids": v} for n, v in ids.items()][:5]
                       if len(distinct) > 1 else [])


def unique_keys(tables: dict[str, pd.DataFrame], keys: dict[str, list[str]]) -> CheckResult:
    dups = {}
    for name, cols in keys.items():
        df = tables.get(name)
        if df is None or df.empty or not set(cols) <= set(df.columns):
            continue
        n = int(df.duplicated(cols).sum())
        if n:
            dups[name] = n
    return CheckResult("unique_keys", not dups,
                       f"duplicate keys: {dups}" if dups else "every row has a unique key",
                       [{"table": n, "duplicates": c} for n, c in dups.items()])


def keys_in(df: pd.DataFrame, col: str, dim: pd.DataFrame, dim_col: str,
            name: str) -> CheckResult:
    """Every non-empty `col` value of a fact table exists in its dimension."""
    if df is None or df.empty or dim is None or dim.empty or col not in df:
        return CheckResult(name, True, "nothing to compare")
    missing = sorted(set(df[col].dropna()) - set(dim[dim_col]))
    return CheckResult(name, not missing,
                       f"{len(missing)} keys missing from the dimension" if missing
                       else f"all {col} values exist in the dimension", missing[:5])


def check_silver_cbs(t: dict[str, pd.DataFrame]) -> list[CheckResult]:
    results = [not_empty(t, SILVER_CBS_KEYS), one_run(t), unique_keys(t, SILVER_CBS_KEYS)]
    for fact in ("fct_absence_rate", "fct_absence_survey", "fct_business_count",
                 "fct_establishment_count"):
        f = t.get(fact)
        results.append(keys_in(f, "period_key", t.get("dim_period"), "period_key",
                               f"{fact}_periods_in_dim_period"))
        results.append(keys_in(f, "sector_key", t.get("dim_sector"), "sector_key",
                               f"{fact}_sectors_in_dim_sector"))
        if f is not None and "size_key" in f:
            results.append(keys_in(f, "size_key", t.get("dim_company_size"), "size_key",
                                   f"{fact}_sizes_in_dim_company_size"))
    unknown = sum(int((df["member_type"] == "unknown").sum()) for df in t.values()
                  if not df.empty and "member_type" in df)
    results.append(CheckResult("no_unknown_characteristics", not unknown,
                               f"{unknown} fact rows with a key missing from the CBS code list"
                               if unknown else "every key is in a CBS code list",
                               severity="warning"))
    return results


def check_silver_kvk(df: pd.DataFrame) -> list[CheckResult]:
    t = {"kvk_company": df}
    results = [not_empty(t, ["kvk_company"]), one_run(t)]
    if df.empty:
        return results
    latest = int(df["is_latest"].astype(bool).sum())
    results.append(CheckResult("latest_snapshot_present", latest > 0,
                               f"{latest:,} rows in the latest snapshot"))
    bad = sorted(set(df["sbi2008_section"].dropna()) - SBI_SECTIONS)
    results.append(CheckResult("valid_sbi_sections", not bad,
                               f"unknown section codes: {bad}" if bad
                               else "every section is A-U", bad[:5]))
    return results


def check_gold(t: dict[str, pd.DataFrame], optional=("gold_kvk_context",)) -> list[CheckResult]:
    required = [n for n in GOLD_KEYS if n not in optional]
    return [not_empty(t, required), one_run(t), unique_keys(t, GOLD_KEYS)]
