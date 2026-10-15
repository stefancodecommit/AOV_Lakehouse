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
gold_risk_index          for the insurer's data analyst: per sector and size class, the
                         relative absence risk (4-quarter average ÷ national), a band
                         (lowest–highest quarter of the last 8), an assessment (higher /
                         around / lower than national), a trend, the number of businesses
                         (and how many are run by natural persons, i.e. self-employed), and
                         one sentence that explains it. Two variants: all figures, and
                         final figures only (basis)
gold_risk_index_history  the quarterly relative risk behind it, per sector and size class

Every row carries period_status / is_provisional (provisional figures are shown by
default, marked; the dashboard can switch to final figures only) and data_as_of (when
the newest data behind the row was ingested). Comparisons that mix provisional and
final figures are flagged (*_mixes_status). Because provisional periods are always the
most recent ones, a final row's comparisons only use final figures, so filtering on
is_provisional = False gives a consistent "final only" view.

Gold refuses to build from silver CBS tables of different runs (a publish that stopped
halfway). Publishing, staging, run_id and the run log work as in silver (pipeline/common/publish.py,
pipeline "gold"); the run log's inputs include the silver run_ids that were read.

Usage
-----
    python -m pipeline.gold.build            # rebuild all gold tables
    python -m pipeline.gold.build --preview  # build and print a summary, write nothing
"""
from __future__ import annotations

import argparse
import logging

import numpy as np
import pandas as pd

from pipeline.common.config import project_path, settings

LAKEHOUSE = project_path(settings["paths"]["lakehouse"])
SILVER_ROOT = LAKEHOUSE / "silver"
GOLD_ROOT = LAKEHOUSE / "gold"
RUN_LOG = LAKEHOUSE / "_run_log"
PIPELINE = "gold"
OPTIONAL_TABLES = {"gold_kvk_context"}      # empty until silver KVK exists
SMALL_GROUP = 10                            # KVK re-identification rule
RISK = settings["gold"]["risk_index"]       # window, minimum quarters, trend threshold
FINAL = "Definitief"
SILVER_CBS_TABLES = ["dim_period", "dim_sector", "dim_company_size", "dim_survey_measure",
                     "fct_absence_rate", "fct_absence_survey", "fct_business_count"]

# Tables gold reads only partly. kvk_company has 1.9M rows per snapshot; gold needs
# seven columns of the latest snapshot only. Reading all of it doubled gold's memory
# use, which made it run out of memory in Docker.
PARTIAL_READS = {
    "kvk_company": {
        "columns": ["snapshot_date", "batch_id", "is_latest", "is_active", "start_date",
                    "legal_form", "sbi2008_section", "run_id"],
        "filters": [("is_latest", "=", True)],
    },
}

log = logging.getLogger("gold")


# =========================================================================== reading
def read_silver(names: list[str], inputs: dict | None = None,
                optional: set[str] = frozenset()) -> dict[str, pd.DataFrame]:
    """Silver tables by name. Records version and run_id in `inputs`; raises when the
    silver CBS tables come from different runs.

    Different run_ids mean a silver publish stopped halfway (see pipeline/common/publish.py): some
    tables are new and some old. Combining them could pair, for example, a new fact
    table with an old dimension, so gold refuses and asks for a silver rerun."""
    from deltalake import DeltaTable
    from pipeline.common.publish import RunFailed
    out, run_ids = {}, {}
    for name in names:
        path = SILVER_ROOT / name
        if not (path / "_delta_log").exists():
            if name in optional:
                out[name] = pd.DataFrame()
                continue
            raise RunFailed(f"silver table missing: {name} (run the silver pipelines first)")
        dt = DeltaTable(str(path))
        if name in PARTIAL_READS:      # only the columns and rows gold uses (memory)
            df = dt.to_pyarrow_table(**PARTIAL_READS[name]).to_pandas()
        else:
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
                        "rerun pipeline/silver/cbs.py first")
    return out


# =========================================================================== helpers
def _status_cols(status: pd.Series) -> pd.DataFrame:
    """period_status as given, plus is_provisional (anything but 'Definitief')."""
    return pd.DataFrame({"period_status": status, "is_provisional": status != FINAL})


def _periods(dim_period: pd.DataFrame) -> pd.DataFrame:
    """The period columns gold adds to every row (label, year, quarter, start date)."""
    p = dim_period[["period_key", "period_label", "year", "quarter", "start_date"]].copy()
    p["year"] = p["year"].astype("int64")
    return p


def add_comparisons(df: pd.DataFrame, group: list[str], value: str, *, quarterly: bool,
                    rank_within: list[str], national: pd.DataFrame | None = None,
                    rank_mask: pd.Series | None = None) -> pd.DataFrame:
    """Add national value and difference, rank, same period last year and (quarterly)
    a rolling four-quarter average. `df` needs period_key, year, quarter (quarterly),
    period_status and the `group` columns.

    Shared by the sector and size tables so both get exactly the same comparisons.
    Absence is seasonal (higher in winter), which is why the comparison is with the
    same quarter a year earlier and with a four-quarter average, never with the
    previous quarter. Every comparison also says whether it mixes provisional and
    final figures (yoy_mixes_status, rolling_4q_has_provisional).

    group:       columns that identify one series (e.g. sector_key)
    rank_within: extra columns that define who is ranked together (e.g. sbi_level)
    national:    period_key -> national_value, for the difference from the total
    rank_mask:   rows that take part in the ranking (e.g. not the total itself)"""
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

    # same period one year earlier: shift every row one year forward and join it back
    prev = df[[*group, "year", *(["quarter"] if quarterly else []), value, "period_status"]].copy()
    prev["year"] = prev["year"] + 1
    prev = prev.rename(columns={value: "last_year_value", "period_status": "last_year_status"})
    df = df.merge(prev, on=[*group, "year", *(["quarter"] if quarterly else [])], how="left")
    df["change_vs_last_year_pp"] = (df[value] - df["last_year_value"]).round(2)
    df["yoy_mixes_status"] = df["last_year_status"].notna() & (
        (df["period_status"] == FINAL) != (df["last_year_status"] == FINAL))

    if quarterly:
        # rolling four-quarter average: only when all four consecutive quarters exist.
        # _q numbers quarters consecutively (2025 Q4 -> 2026 Q1 is +1); reindexing to the
        # full range turns a missing quarter into an empty row, so a window across a gap
        # has fewer than 4 values and gives no average instead of a misleading one.
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
    """gold_absence_by_sector: 80072NED absence % per sector and quarter, compared with
    the national total, last year and a rolling four-quarter average.

    All CBS sector levels are kept (section C, division 47, section group B-F, ...);
    ranks are only within one level, because comparing a section with one of its own
    divisions makes no sense. The dashboard shows sections by default."""
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
    """Key of the 86009NED measure used per size class: the share of employees who were
    absent at least once in the year (the survey has no absence percentage)."""
    m = t["dim_survey_measure"]
    m = m[(m["source_table"] == "86009NED") & (m["measure_name"] == "aandeel_werknemers_die_hebben_verzuimd")]
    if m.empty:
        raise ValueError("86009NED measure 'aandeel_werknemers_die_hebben_verzuimd' not found")
    return m["measure_key"].iloc[0]


def build_absence_by_size(t: dict) -> pd.DataFrame:
    """gold_absence_by_size: absence per size class from two sources, side by side.

    - 80072NED, quarterly: absence % per class of persons employed (1-9, 10-99, 100+)
    - 86009NED, yearly: share of employees absent per class of employees, with 95% bounds
    The units and measures differ, so the rows are never mixed: `source_table` and
    `measure` say which is which. Survey classes that contain others (1-9 contains 1-4
    and 5-9) are kept but not ranked."""
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
    # empty for the total row; the total is not a size class, so not an aggregate
    s["size_is_aggregate"] = s["size_is_aggregate"].astype("boolean").fillna(False).astype(bool)
    s["measure"] = "share_employees_absent"

    parts = []
    for part, quarterly in ((q, True), (s, False)):
        part = part.merge(periods, on="period_key", how="left")
        national = (part[part["member_type"] == "total"][["period_key", "value"]]
                    .rename(columns={"value": "national_value"}))
        # the total has no size_key; give it one so it forms its own series
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
    """gold_absence_profile: every 86009NED survey measure per sector and size class.

    For each value: its 95% interval, the national value, and differs_from_national.
    That flag is only set when the whole interval lies above or below the national
    value; a point estimate that differs but whose interval includes the national
    value may be sampling noise (question 5 in docs/PROJECT.md). rank_in_topic ranks the
    measures inside one topic group, e.g. which complaint is most common."""
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
    """gold_business_context: number of businesses (81589NED) per sector, quarter and
    size band, plus the CBS total as band 'all'.

    Bands are sums of the detailed size classes; aggregate classes (0-50, 0-250) are
    left out so no business is counted twice. If one class of a band is missing the
    band is left empty rather than undercounted."""
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
    """gold_kvk_context: from the latest KVK snapshot, active companies per SBI 2008
    section and legal form ('all' = BV and NV together), with median company age.

    Companies whose section couldn't be determined are kept as section 'unknown', so the
    totals still add up. is_small_group marks groups under 10 companies (the KVK
    re-identification rule); the dashboard hides them by default."""
    k = t.get("kvk_company", pd.DataFrame())
    if k.empty:
        return pd.DataFrame()
    # latest snapshot only: summing snapshots would count each company once per upload
    k = k[k["is_latest"] & (k["is_active"] == True)].copy()  # noqa: E712
    snap = pd.to_datetime(k["snapshot_date"])
    start = pd.to_datetime(k["start_date"])
    # age on the snapshot day; companies without a valid start date get no age
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


# =========================================================================== risk index
# A white-box model: every number is simple arithmetic the analyst can redo by hand.
#   relative risk  = the group's 4-quarter average absence ÷ the national 4-quarter average
#                    (1.00 = national; four quarters, so winter peaks don't distort it)
#   band           = the lowest and highest quarterly relative risk in the window. CBS gives
#                    no margin of error for 80072NED, so the band shows how stable it is
#   assessment     = band wholly above 1 -> higher; wholly below -> lower; else around national
#   trend          = slope of a straight line through the window's quarterly relative risks,
#                    per year. Dividing by the national figure of the same quarter removes
#                    the season, so quarters can be compared directly
def _relative_risk(t: dict) -> pd.DataFrame:
    """Per quarter, sector and size class: absence %, the national absence % of the same
    quarter, and their ratio (the quarterly relative risk)."""
    rate = t["fct_absence_rate"]
    national = (rate[rate["member_type"] == "total"][["period_key", "absence_pct"]]
                .rename(columns={"absence_pct": "national_pct"}))
    df = rate[rate["member_type"].isin(["sector", "size"])].merge(national, on="period_key")
    df = df[df["absence_pct"].notna() & (df["national_pct"] > 0)].copy()
    df["relative_risk"] = (df["absence_pct"] / df["national_pct"]).round(3)
    df["group_type"] = df["member_type"]
    df["group_key"] = df["sector_key"].where(df["group_type"] == "sector", df["size_key"])
    df = df.merge(t["dim_sector"][["sector_key", "sbi_code", "sbi_level", "sector_name"]],
                  on="sector_key", how="left")
    df = df.merge(t["dim_company_size"][["size_key", "size_range"]], on="size_key", how="left")
    df["group_name"] = df["sector_name"].where(df["group_type"] == "sector",
                                               df["size_range"] + " persons employed")
    df["sbi_code"] = df["sbi_code"].where(df["group_type"] == "sector")
    df["sbi_level"] = df["sbi_level"].where(df["group_type"] == "sector")
    df = df.merge(_periods(t["dim_period"]), on="period_key", how="left")
    df["is_provisional"] = df["period_status"] != FINAL
    df["_q"] = df["year"] * 4 + df["quarter"] - 1       # quarters numbered consecutively
    return df


def build_risk_index_history(t: dict) -> pd.DataFrame:
    """gold_risk_index_history: the quarterly relative risk per sector and size class."""
    df = _relative_risk(t)
    cols = ["period_key", "period_label", "year", "quarter", "period_status", "is_provisional",
            "group_type", "group_key", "sbi_code", "sbi_level", "group_name", "absence_pct",
            "national_pct", "relative_risk", "ingested_at"]
    return (df[cols].rename(columns={"ingested_at": "data_as_of"})
            .sort_values(["group_type", "group_key", "period_key"]).reset_index(drop=True))


def _business_counts(t: dict) -> pd.DataFrame:
    """Per SBI code and level, from CBS 81589NED's latest quarter: all businesses, those
    run by natural persons (sole traders, partnerships: the self-employed), and those with
    one person employed. The KVK open data has no sole traders, so these come from CBS."""
    b = t["fct_business_count"]
    if b.empty:
        return pd.DataFrame(columns=["sbi_code", "sbi_level", "businesses", "businesses_natural_persons",
                                     "businesses_one_person", "businesses_period"])
    b = b[b["period_key"] == b["period_key"].max()]
    picks = {"businesses": b["breakdown"] == "total",
             "businesses_natural_persons": (b["breakdown"] == "legal_form")
             & (b["legal_form"] == "Natuurlijke personen"),
             "businesses_one_person": (b["breakdown"] == "size") & (b["size_key"] == "WP_1_2")}
    out = None
    for name, mask in picks.items():
        part = b[mask].groupby("sector_key")["count"].first().rename(name)
        out = part.to_frame() if out is None else out.join(part, how="outer")
    out = out.reset_index().merge(t["dim_sector"][["sector_key", "sbi_code", "sbi_level"]],
                                  on="sector_key", how="left")
    out["businesses_period"] = b["period_key"].iloc[0]
    for c in picks:
        out[c] = out[c].astype("Int64")
    return out.drop(columns="sector_key").drop_duplicates(["sbi_code", "sbi_level"])


def _explain(r) -> str:
    """One sentence for the analyst: the numbers of the row in words."""
    name = f"{r.sbi_code} {r.group_name}" if r.group_type == "sector" else f"Size {r.group_name}"
    if r.assessment == "too few quarters":
        return f"{name}: too few quarters with figures to assess ({r.quarters_in_band})."
    pct = (r.relative_risk - 1) * 100
    text = (f"{name}: {abs(pct):.0f}% {'more' if pct >= 0 else 'less'} absence than nationally "
            f"over the last 4 quarters ({r.absence_4q_pct:.1f}% vs {r.national_4q_pct:.1f}%); ")
    band = f"{r.band_low:.2f}–{r.band_high:.2f}"
    text += {"higher": f"above national in all {r.quarters_in_band} quarters ({band})",
             "lower": f"below national in all {r.quarters_in_band} quarters ({band})",
             "around national": f"sometimes above, sometimes below national ({band})"}[r.assessment]
    slope = round(r.trend_per_year, 2) + 0.0      # + 0.0: no "-0.00" for a tiny negative slope
    text += f"; {r.trend} ({slope:+.2f} per year)."
    if r.provisional_quarters:
        text += f" {r.provisional_quarters} of {r.quarters_in_band} quarters provisional."
    if pd.notna(r.businesses):
        text += f" {r.businesses:,} businesses"
        if pd.notna(r.businesses_natural_persons) and r.businesses:
            text += f", {r.businesses_natural_persons / r.businesses:.0%} run by natural persons"
        text += f" (CBS {r.businesses_period})."
    return text


def build_risk_index(t: dict, settings_: dict | None = None) -> pd.DataFrame:
    """gold_risk_index: one row per sector and size class, for two bases: 'all' (the
    latest quarter, provisional figures included) and 'final' (the latest final quarter,
    final figures only), so the dashboard's "final figures only" switch works here too."""
    s = settings_ or RISK
    window, minimum, rising = int(s["window_quarters"]), int(s["min_quarters"]), float(s["rising_per_year"])
    history = _relative_risk(t)
    counts = _business_counts(t)
    rows = []
    for basis, part in (("all", history), ("final", history[~history["is_provisional"]])):
        if part.empty:
            continue
        latest = part["_q"].max()
        period = part.loc[part["_q"] == latest, ["period_key", "period_label"]].iloc[0]
        for (group_type, group_key), g in part[part["_q"] > latest - window].groupby(
                ["group_type", "group_key"]):
            last4 = g[g["_q"] > latest - 4]
            n = len(g)
            full_year = len(last4) == 4
            row = {"basis": basis, "period_key": period["period_key"],
                   "period_label": period["period_label"], "group_type": group_type,
                   "group_key": group_key, "sbi_code": g["sbi_code"].iloc[0],
                   "sbi_level": g["sbi_level"].iloc[0], "group_name": g["group_name"].iloc[0],
                   "absence_4q_pct": round(last4["absence_pct"].mean(), 2) if full_year else np.nan,
                   "national_4q_pct": round(last4["national_pct"].mean(), 2) if full_year else np.nan,
                   "band_low": g["relative_risk"].min(), "band_high": g["relative_risk"].max(),
                   "quarters_in_band": n,
                   "provisional_quarters": int(g["is_provisional"].sum()),
                   "data_as_of": g["ingested_at"].max()}
            row["relative_risk"] = (round(row["absence_4q_pct"] / row["national_4q_pct"], 3)
                                    if full_year else np.nan)
            if n < minimum or not full_year:
                row.update(assessment="too few quarters", trend_per_year=np.nan, trend="unknown")
            else:
                row["assessment"] = ("higher" if row["band_low"] > 1 else
                                     "lower" if row["band_high"] < 1 else "around national")
                slope = np.polyfit(g["_q"], g["relative_risk"], 1)[0] * 4    # per year
                row["trend_per_year"] = round(float(slope), 3)
                row["trend"] = ("rising" if slope > rising else
                                "falling" if slope < -rising else "stable")
            rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.merge(counts, on=["sbi_code", "sbi_level"], how="left")
    for c in ("businesses", "businesses_natural_persons", "businesses_one_person"):
        df[c] = df[c].astype("Int64")
    df["explanation"] = [_explain(r) for r in df.itertuples()]
    cols = ["basis", "period_key", "period_label", "group_type", "group_key", "sbi_code",
            "sbi_level", "group_name", "absence_4q_pct", "national_4q_pct", "relative_risk",
            "band_low", "band_high", "quarters_in_band", "provisional_quarters", "assessment",
            "trend_per_year", "trend", "businesses", "businesses_natural_persons",
            "businesses_one_person", "businesses_period", "explanation", "data_as_of"]
    return (df[cols].sort_values(["basis", "group_type", "relative_risk"],
                                 ascending=[True, True, False], na_position="last")
            .reset_index(drop=True))


def build_all(t: dict) -> dict[str, pd.DataFrame]:
    """Build all gold tables from the silver tables in `t` (as read by read_inputs)."""
    return {
        "gold_absence_by_sector": build_absence_by_sector(t),
        "gold_absence_by_size": build_absence_by_size(t),
        "gold_absence_profile": build_absence_profile(t),
        "gold_business_context": build_business_context(t),
        "gold_kvk_context": build_kvk_context(t),
        "gold_risk_index": build_risk_index(t),
        "gold_risk_index_history": build_risk_index_history(t),
    }


def read_inputs(inputs: dict | None = None) -> dict:
    """Read every silver table gold needs; kvk_company is optional (empty without silver KVK)."""
    return read_silver([*SILVER_CBS_TABLES, "kvk_company"], inputs, optional={"kvk_company"})


def run() -> str:
    """Build, stage, publish and log one gold run (pipeline/common/publish.py). Returns the run_id."""
    from pipeline.common.publish import run_pipeline
    return run_pipeline(PIPELINE, lambda inputs: build_all(read_inputs(inputs)),
                        GOLD_ROOT, RUN_LOG, OPTIONAL_TABLES)


def main() -> None:
    """Command line: build and publish gold, or only summarise with --preview."""
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
