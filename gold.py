"""
Gold layer: tables shaped for the dashboard, around the main question "which company
characteristics (sector, size class) go together with higher sickness absence?".

Tables (lakehouse/gold/)
------------------------
gold_absence_by_sector   quarterly, 80072NED: absence % per sector (every CBS level:
                         section, section group, division, ...; sbi_level says which)
                         and the national total; difference from the total in
                         percentage points; rank within the same level per quarter;
                         change versus the same quarter a year earlier; rolling
                         four-quarter average
gold_absence_by_size     quarterly absence % per size class (80072NED, persons employed)
                         and yearly share of employees absent per size class (86009NED,
                         employees, with 95% bounds), side by side: the units differ
gold_absence_profile     yearly, 86009NED: every survey measure per sector and size class
                         with its 95% interval, the national value, whether the national
                         value lies outside the interval, and a rank within each topic
                         (e.g. the most common causes)
gold_business_context    quarterly, 81589NED: number of businesses per sector and size band
                         ("all" = CBS's total). CBS rounds these counts to multiples of 5,
                         so the bands can differ from the total by a few businesses (up to
                         15 in the data); a band is null when one of its classes is missing
gold_kvk_context         latest KVK snapshot: active companies per SBI 2008 section and
                         legal form, median company age; groups under 10 companies flagged

Every row carries period_status / is_provisional (provisional figures are shown by
default, marked; the dashboard can switch to final figures only) and data_as_of (when
the newest data behind the row was ingested). Comparisons that mix provisional and
final figures are flagged (*_mixes_status). Because provisional periods are always the
most recent ones, a final row's comparisons only use final figures, so filtering on
is_provisional = False gives a consistent "final only" view.

Gold refuses to build from silver CBS tables of different runs (a publish that stopped
halfway). Publishing, staging, run_id and the run log work as in silver (publish.py,
pipeline "gold"); the run log's inputs include the silver run_ids that were read.

Usage
-----
    python gold.py            # rebuild all gold tables
    python gold.py --preview  # build and print a summary, write nothing
"""
from __future__ import annotations

import argparse
import logging

import numpy as np
import pandas as pd

from config import project_path, settings

LAKEHOUSE = project_path(settings["paths"]["lakehouse"])
SILVER_ROOT = LAKEHOUSE / "silver"
GOLD_ROOT = LAKEHOUSE / "gold"
RUN_LOG = LAKEHOUSE / "_run_log"
PIPELINE = "gold"
OPTIONAL_TABLES = {"gold_kvk_context"}      # empty until silver KVK exists
SMALL_GROUP = 10                            # KVK re-identification rule
FINAL = "Definitief"
SILVER_CBS_TABLES = ["dim_period", "dim_sector", "dim_company_size", "dim_survey_measure",
                     "fct_absence_rate", "fct_absence_survey", "fct_business_count"]

log = logging.getLogger("gold")


# =========================================================================== reading
def read_silver(names: list[str], inputs: dict | None = None,
                optional: set[str] = frozenset()) -> dict[str, pd.DataFrame]:
    """Silver tables by name. Records version and run_id in `inputs`; raises when the
    silver CBS tables come from different runs."""
    from deltalake import DeltaTable
    from publish import RunFailed
    out, run_ids = {}, {}
    for name in names:
        path = SILVER_ROOT / name
        if not (path / "_delta_log").exists():
            if name in optional:
                out[name] = pd.DataFrame()
                continue
            raise RunFailed(f"silver table missing: {name} (run the silver pipelines first)")
        dt = DeltaTable(str(path))
        df = dt.to_pandas()
        ids = sorted(df["run_id"].dropna().unique()) if "run_id" in df else []
        run_ids[name] = ids
        if inputs is not None:
            inputs[f"silver/{name}"] = dt.version()
            inputs[f"silver/{name}#run_id"] = ",".join(ids)
        out[name] = df
    cbs = {tuple(run_ids[n]) for n in names if n in SILVER_CBS_TABLES and n in run_ids}
    if len(cbs) > 1:
        raise RunFailed(f"silver CBS tables come from different runs {sorted(cbs)}; "
                        "rerun cbs_silver.py first")
    return out


# =========================================================================== helpers
def _status_cols(status: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({"period_status": status, "is_provisional": status != FINAL})


def _periods(dim_period: pd.DataFrame) -> pd.DataFrame:
    p = dim_period[["period_key", "period_label", "year", "quarter", "start_date"]].copy()
    p["year"] = p["year"].astype("int64")
    return p


def add_comparisons(df: pd.DataFrame, group: list[str], value: str, *, quarterly: bool,
                    rank_within: list[str], national: pd.DataFrame | None = None,
                    rank_mask: pd.Series | None = None) -> pd.DataFrame:
    """Add national value and difference, rank, same period last year and (quarterly)
    a rolling four-quarter average. `df` needs period_key, year, quarter (quarterly),
    period_status and the `group` columns."""
    df = df.copy()
    if national is not None:
        df = df.merge(national, on="period_key", how="left")
        df["diff_from_national_pp"] = (df[value] - df["national_value"]).round(2)

    # rank: 1 = highest, within the same period and level; aggregates aren't ranked
    mask = rank_mask if rank_mask is not None else pd.Series(True, index=df.index)
    ranked = df[mask & df[value].notna()]
    df["rank"] = ranked.groupby(["period_key", *rank_within])[value].rank(
        ascending=False, method="min").astype("Int64")
    df["rank_of"] = ranked.groupby(["period_key", *rank_within])[value].transform("size").astype("Int64")

    # same period one year earlier
    prev = df[[*group, "year", *(["quarter"] if quarterly else []), value, "period_status"]].copy()
    prev["year"] = prev["year"] + 1
    prev = prev.rename(columns={value: "last_year_value", "period_status": "last_year_status"})
    df = df.merge(prev, on=[*group, "year", *(["quarter"] if quarterly else [])], how="left")
    df["change_vs_last_year_pp"] = (df[value] - df["last_year_value"]).round(2)
    df["yoy_mixes_status"] = df["last_year_status"].notna() & (
        (df["period_status"] == FINAL) != (df["last_year_status"] == FINAL))

    if quarterly:
        # rolling four-quarter average: only when all four consecutive quarters exist
        df["_q"] = df["year"] * 4 + df["quarter"] - 1
        df = df.sort_values([*group, "_q"])
        parts = []
        for _, g in df.groupby(group, sort=False, dropna=False):
            q = g.set_index("_q")
            full = q.reindex(range(int(q.index.min()), int(q.index.max()) + 1))
            roll = full[value].rolling(4, min_periods=4).mean()
            prov = (full["period_status"] != FINAL).astype(float).rolling(4, min_periods=4).max()
            g = g.assign(rolling_4q_value=roll.reindex(q.index).round(2).to_numpy(),
                         rolling_4q_has_provisional=(prov.reindex(q.index) > 0).to_numpy())
            parts.append(g)
        df = pd.concat(parts).drop(columns="_q")
        df.loc[df["rolling_4q_value"].isna(), "rolling_4q_has_provisional"] = False
    return df.reset_index(drop=True)


# =========================================================================== tables
def build_absence_by_sector(t: dict) -> pd.DataFrame:
    rate, sectors = t["fct_absence_rate"], t["dim_sector"]
    df = rate[rate["member_type"].isin(["sector", "total"])].merge(
        sectors[["sector_key", "sbi_code", "sbi_level", "sector_name"]], on="sector_key", how="left")
    df = df.merge(_periods(t["dim_period"]), on="period_key", how="left")
    national = (df[df["member_type"] == "total"][["period_key", "absence_pct"]]
                .rename(columns={"absence_pct": "national_value"}))
    df = add_comparisons(df, ["sector_key"], "absence_pct", quarterly=True,
                         rank_within=["sbi_level"], national=national,
                         rank_mask=df["member_type"] == "sector")
    df = pd.concat([df, _status_cols(df["period_status"])[["is_provisional"]]], axis=1)
    cols = ["period_key", "period_label", "year", "quarter", "period_status", "is_provisional",
            "sector_key", "sbi_code", "sbi_level", "sector_name", "absence_pct",
            "national_value", "diff_from_national_pp", "rank", "rank_of", "last_year_value",
            "last_year_status", "change_vs_last_year_pp", "yoy_mixes_status",
            "rolling_4q_value", "rolling_4q_has_provisional", "ingested_at"]
    return (df[cols].rename(columns={"national_value": "national_pct", "rank": "rank_in_level",
                                     "rank_of": "sectors_in_level", "last_year_value": "last_year_pct",
                                     "rolling_4q_value": "rolling_4q_pct", "ingested_at": "data_as_of"})
            .sort_values(["period_key", "sbi_level", "sector_key"]).reset_index(drop=True))


def _survey_headline(t: dict) -> str:
    m = t["dim_survey_measure"]
    m = m[(m["source_table"] == "86009NED") & (m["measure_name"] == "aandeel_werknemers_die_hebben_verzuimd")]
    if m.empty:
        raise ValueError("86009NED measure 'aandeel_werknemers_die_hebben_verzuimd' not found")
    return m["measure_key"].iloc[0]


def build_absence_by_size(t: dict) -> pd.DataFrame:
    sizes, periods = t["dim_company_size"], _periods(t["dim_period"])
    size_cols = ["size_key", "size_range", "size_unit", "lower_bound", "size_band"]

    # quarterly absence % (persons employed)
    rate = t["fct_absence_rate"]
    q = rate[rate["member_type"].isin(["size", "total"])].copy()
    q["value"], q["lower_95"], q["upper_95"] = q["absence_pct"], np.nan, np.nan
    q["size_is_aggregate"] = False
    q["source_table"], q["measure"] = "80072NED", "absence_pct"

    # yearly share of employees absent (employees), with 95% bounds
    s = t["fct_absence_survey"]
    s = s[(s["source_table"] == "86009NED") & (s["measure_key"] == _survey_headline(t))
          & s["member_type"].isin(["size", "total"])].copy()
    s["size_is_aggregate"] = s["size_is_aggregate"].astype("boolean").fillna(False).astype(bool)
    s["measure"] = "share_employees_absent"

    parts = []
    for part, quarterly in ((q, True), (s, False)):
        part = part.merge(periods, on="period_key", how="left")
        national = (part[part["member_type"] == "total"][["period_key", "value"]]
                    .rename(columns={"value": "national_value"}))
        part = part.assign(group_key=part["size_key"].fillna("total"))
        part = add_comparisons(part, ["group_key"], "value", quarterly=quarterly,
                               rank_within=[], national=national,
                               rank_mask=(part["member_type"] == "size") & ~part["size_is_aggregate"])
        parts.append(part)
    df = pd.concat(parts, ignore_index=True).merge(sizes[size_cols], on="size_key", how="left")
    df["is_provisional"] = df["period_status"] != FINAL
    df["period_type"] = np.where(df["source_table"] == "80072NED", "quarter", "year")
    cols = ["source_table", "measure", "period_type", "period_key", "period_label", "year",
            "quarter", "period_status", "is_provisional", "member_type", *size_cols,
            "size_is_aggregate", "value", "lower_95", "upper_95", "national_value",
            "diff_from_national_pp", "rank", "rank_of", "last_year_value", "last_year_status",
            "change_vs_last_year_pp", "yoy_mixes_status", "rolling_4q_value",
            "rolling_4q_has_provisional", "ingested_at"]
    df["rolling_4q_has_provisional"] = (df["rolling_4q_has_provisional"].astype("boolean")
                                        .fillna(False).astype(bool))
    return (df[cols].rename(columns={"rank": "rank_in_source", "rank_of": "classes_ranked",
                                     "ingested_at": "data_as_of"})
            .sort_values(["source_table", "period_key", "lower_bound"], na_position="first")
            .reset_index(drop=True))


def build_absence_profile(t: dict) -> pd.DataFrame:
    s = t["fct_absence_survey"]
    s = s[s["source_table"] == "86009NED"].copy()
    m = t["dim_survey_measure"]
    m = m[m["source_table"] == "86009NED"][["measure_key", "title", "topic_group", "unit"]]
    df = s.merge(m, on="measure_key", how="left")
    national = (df[df["member_type"] == "total"][["period_key", "measure_key", "value"]]
                .rename(columns={"value": "national_value"}))
    df = df.merge(national, on=["period_key", "measure_key"], how="left")
    has_ci = df["lower_95"].notna() & df["upper_95"].notna() & df["national_value"].notna()
    above = has_ci & (df["lower_95"] > df["national_value"])
    below = has_ci & (df["upper_95"] < df["national_value"])
    df["differs_from_national"] = (above | below) & (df["member_type"] != "total")
    df["direction"] = np.select([above, below], ["above", "below"], default=None)
    df.loc[df["member_type"] == "total", "direction"] = None
    df["diff_from_national"] = (df["value"] - df["national_value"]).round(2)
    df["rank_in_topic"] = (df[df["value"].notna()]
                           .groupby(["period_key", "characteristic_key", "topic_group"])["value"]
                           .rank(ascending=False, method="min").astype("Int64"))
    df = df.merge(t["dim_sector"][["sector_key", "sbi_code", "sbi_level", "sector_name"]],
                  on="sector_key", how="left")
    df = df.merge(t["dim_company_size"][["size_key", "size_range", "size_unit", "lower_bound"]],
                  on="size_key", how="left")
    df = df.merge(_periods(t["dim_period"])[["period_key", "year"]], on="period_key", how="left")
    df["is_provisional"] = df["period_status"] != FINAL
    df["size_is_aggregate"] = df["size_is_aggregate"].astype("boolean")
    cols = ["period_key", "year", "period_status", "is_provisional", "member_type",
            "characteristic_key", "sector_key", "sbi_code", "sbi_level", "sector_name",
            "size_key", "size_range", "size_unit", "lower_bound", "size_is_aggregate",
            "measure_key", "measure_name", "title", "topic_group", "unit", "value",
            "lower_95", "upper_95", "national_value", "diff_from_national",
            "differs_from_national", "direction", "rank_in_topic", "ingested_at"]
    return (df[cols].rename(columns={"title": "measure_title", "ingested_at": "data_as_of"})
            .sort_values(["period_key", "member_type", "characteristic_key", "measure_key"])
            .reset_index(drop=True))


def build_business_context(t: dict) -> pd.DataFrame:
    b = t["fct_business_count"]
    total = b[b["breakdown"] == "total"].assign(size_band="all")
    sized = b[(b["breakdown"] == "size") & (b["size_is_aggregate"] == False)]  # noqa: E712
    keys = ["period_key", "sector_key", "size_band"]
    agg = {"businesses": ("count", "sum"), "period_status": ("period_status", "first"),
           "data_as_of": ("ingested_at", "max"), "missing_counts": ("count", lambda c: c.isna().sum())}
    df = pd.concat([total.groupby(keys).agg(**agg), sized.groupby(keys).agg(**agg)]).reset_index()
    df["businesses"] = df["businesses"].where(df["missing_counts"] == 0).astype("Int64")
    df = df.merge(t["dim_sector"][["sector_key", "sbi_code", "sbi_level", "sector_name"]],
                  on="sector_key", how="left")
    df = df.merge(_periods(t["dim_period"]), on="period_key", how="left")
    df["is_provisional"] = df["period_status"] != FINAL
    cols = ["period_key", "period_label", "year", "quarter", "period_status", "is_provisional",
            "sector_key", "sbi_code", "sbi_level", "sector_name", "size_band", "businesses",
            "data_as_of"]
    return df[cols].sort_values(["period_key", "sector_key", "size_band"]).reset_index(drop=True)


def build_kvk_context(t: dict) -> pd.DataFrame:
    k = t.get("kvk_company", pd.DataFrame())
    if k.empty:
        return pd.DataFrame()
    k = k[k["is_latest"] & (k["is_active"] == True)].copy()  # noqa: E712
    snap = pd.to_datetime(k["snapshot_date"])
    start = pd.to_datetime(k["start_date"])
    k["age_years"] = ((snap - start).dt.days / 365.25).where(start.notna())
    k["section"] = k["sbi2008_section"].fillna("unknown")
    rows = []
    for legal_form, g in [("all", k), *k.groupby("legal_form")]:
        stats = g.groupby("section").agg(
            active_companies=("section", "size"),
            companies_with_age=("age_years", "count"),
            median_age_years=("age_years", "median"))
        rows.append(stats.reset_index().assign(legal_form=legal_form))
    df = pd.concat(rows, ignore_index=True)
    names = t["dim_sector"]
    names = names[names["sbi_level"] == "section"][["sbi_code", "sector_name"]]
    df = df.merge(names, left_on="section", right_on="sbi_code", how="left").drop(columns="sbi_code")
    df.loc[df["section"] == "unknown", "sector_name"] = "Sector not determined (see sbi_mapping)"
    df["median_age_years"] = df["median_age_years"].round(1)
    df["is_small_group"] = df["active_companies"] < SMALL_GROUP
    df["snapshot_date"] = k["snapshot_date"].iloc[0]
    df["batch_id"] = k["batch_id"].iloc[0]
    cols = ["snapshot_date", "batch_id", "section", "sector_name", "legal_form",
            "active_companies", "companies_with_age", "median_age_years", "is_small_group"]
    return (df[cols].rename(columns={"section": "sbi2008_section"})
            .sort_values(["legal_form", "sbi2008_section"]).reset_index(drop=True))


def build_all(t: dict) -> dict[str, pd.DataFrame]:
    return {
        "gold_absence_by_sector": build_absence_by_sector(t),
        "gold_absence_by_size": build_absence_by_size(t),
        "gold_absence_profile": build_absence_profile(t),
        "gold_business_context": build_business_context(t),
        "gold_kvk_context": build_kvk_context(t),
    }


def read_inputs(inputs: dict | None = None) -> dict:
    return read_silver([*SILVER_CBS_TABLES, "kvk_company"], inputs, optional={"kvk_company"})


def run() -> str:
    from publish import run_pipeline
    return run_pipeline(PIPELINE, lambda inputs: build_all(read_inputs(inputs)),
                        GOLD_ROOT, RUN_LOG, OPTIONAL_TABLES)


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the gold tables from silver.")
    ap.add_argument("--preview", action="store_true", help="build and summarise, write nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.preview:
        for name, df in build_all(read_inputs()).items():
            log.info("%-24s %8d rows  columns: %s", name, len(df), ", ".join(df.columns))
        return
    try:
        run()
    except Exception:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
