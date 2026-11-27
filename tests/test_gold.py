"""Gold: comparisons, the gold tables, and an end-to-end run from bronze to gold."""
import json
import sys

import pandas as pd
import pytest

from pipeline.common import paths, publish, runlog
from pipeline.gold import build as g
from pipeline.silver import cbs as s
from test_delta import bronze_delta  # noqa: F401  (fixture)
from tools import inspect_lakehouse

F, V = "Definitief", "Voorlopig"


def quarters(values, statuses=None, start=(2024, 1), group="A"):
    """A quarterly series for one group, as add_comparisons expects."""
    y, q = start
    rows = []
    for i, v in enumerate(values):
        yy, qq = y + (q - 1 + i) // 4, (q - 1 + i) % 4 + 1
        rows.append({"group": group, "period_key": f"{yy}KW{qq:02d}", "year": yy, "quarter": qq,
                     "value": v, "period_status": (statuses or [F] * len(values))[i]})
    return pd.DataFrame(rows)


# =========================================================================== comparisons
def test_year_on_year_and_rolling_average():
    df = g.add_comparisons(quarters([4.0, 5.0, 6.0, 7.0, 4.4, 5.5]), ["group"], "value",
                           quarterly=True, rank_within=[]).set_index("period_key")
    assert pd.isna(df.loc["2024KW04", "last_year_value"])
    assert df.loc["2025KW01", "last_year_value"] == 4.0
    assert df.loc["2025KW01", "change_vs_last_year_pp"] == pytest.approx(0.4)
    assert pd.isna(df.loc["2024KW03", "rolling_4q_value"])          # only 3 quarters yet
    assert df.loc["2024KW04", "rolling_4q_value"] == 5.5
    assert df.loc["2025KW02", "rolling_4q_value"] == pytest.approx((6 + 7 + 4.4 + 5.5) / 4, abs=0.01)


def test_rolling_average_needs_four_consecutive_quarters():
    df = quarters([4.0, 5.0, 6.0, 7.0, 8.0])
    df = df[df["period_key"] != "2024KW02"]                         # a gap
    out = g.add_comparisons(df, ["group"], "value", quarterly=True,
                            rank_within=[]).set_index("period_key")
    assert out["rolling_4q_value"].isna().all()


def test_provisional_is_flagged_in_comparisons_and_final_rows_stay_clean():
    df = g.add_comparisons(quarters([4, 5, 6, 7, 4.5, 5.5], [F, F, F, F, F, V]), ["group"],
                           "value", quarterly=True, rank_within=[]).set_index("period_key")
    assert df.loc["2025KW02", "yoy_mixes_status"]                   # provisional vs final
    assert df.loc["2025KW02", "rolling_4q_has_provisional"]
    final = df[df["period_status"] == F]
    assert not final["yoy_mixes_status"].any() and not final["rolling_4q_has_provisional"].any()


def test_rank_within_period_and_level_skips_masked_rows():
    df = pd.concat([quarters([5.0], group="A"), quarters([7.0], group="B"),
                    quarters([6.0], group="T")], ignore_index=True)
    df["level"] = "section"
    out = g.add_comparisons(df, ["group"], "value", quarterly=True, rank_within=["level"],
                            rank_mask=df["group"] != "T").set_index("group")
    assert [out.loc["B", "rank"], out.loc["A", "rank"]] == [1, 2]
    assert pd.isna(out.loc["T", "rank"]) and out.loc["A", "rank_of"] == 2


# =========================================================================== tables
@pytest.fixture
def silver_tables(silver):
    """The conftest silver tables plus run_id, shaped like what gold reads."""
    return {name: df.assign(run_id="silver_cbs_x") for name, df in silver.items()}


def test_absence_by_sector(silver_tables):
    df = g.build(g.build_absence_by_sector, silver_tables).set_index(["period_key", "sector_key"])
    total, agri = df.loc[("2025KW01", "T001081")], df.loc[("2025KW01", "301000")]
    assert agri.national_pct == total.absence_pct
    assert agri.diff_from_national_pp == pytest.approx(agri.absence_pct - total.absence_pct)
    assert agri.sbi_level == "section" and agri.rank_in_level == 1
    assert pd.isna(total.rank_in_level) and agri.is_provisional       # sample data: Voorlopig


def test_absence_by_size_keeps_units_apart(silver_tables):
    df = g.build(g.build_absence_by_size, silver_tables)
    assert set(df["measure"]) == {"absence_pct", "share_employees_absent"}
    survey = df[df["source_table"] == "86009NED"]
    assert set(survey["size_unit"].dropna()) == {"employees"}
    assert survey["lower_95"].notna().all() and survey["period_type"].eq("year").all()
    assert set(df[df["source_table"] == "80072NED"]["size_unit"].dropna()) == {"persons_employed"}


def test_absence_profile_flags_values_outside_the_national_interval():
    sv = pd.DataFrame([
        # total, and three sectors: interval above, below, and around the national value
        ("T001081", "total", None, 50.0, 49.0, 51.0),
        ("A", "sector", "A", 55.0, 53.0, 57.0),
        ("B", "sector", "B", 45.0, 43.0, 47.0),
        ("C", "sector", "C", 50.5, 48.0, 53.0)],
        columns=["characteristic_key", "member_type", "sector_key", "value", "lower_95", "upper_95"])
    sv = sv.assign(source_table="86009NED", period_key="2025JJ00", period_status=F,
                   measure_key="M_1", measure_name="m", size_key=None, size_is_aggregate=None,
                   ingested_at=pd.Timestamp("2026-10-05", tz="UTC"))
    t = {"fct_absence_survey": sv,
         "dim_survey_measure": pd.DataFrame([("86009NED", "M_1", "m", "M", "Topic", "%")],
                                            columns=["source_table", "measure_key", "measure_name",
                                                     "title", "topic_group", "unit"]),
         "dim_sector": pd.DataFrame({"sector_key": ["A", "B", "C"], "sbi_code": ["A", "B", "C"],
                                     "sbi_level": "section", "sector_name": ["a", "b", "c"]}),
         "dim_company_size": pd.DataFrame(columns=["size_key", "size_range", "size_unit", "lower_bound"]),
         "dim_period": pd.DataFrame({"period_key": ["2025JJ00"], "period_label": ["2025"],
                                     "year": [2025], "quarter": [None], "start_date": [None]})}
    df = g.build(g.build_absence_profile, t).set_index("characteristic_key")
    assert df.loc["A", "differs_from_national"] and df.loc["A", "direction"] == "above"
    assert df.loc["B", "differs_from_national"] and df.loc["B", "direction"] == "below"
    assert not df.loc["C", "differs_from_national"] and df.loc["C", "direction"] is None
    assert not df.loc["T001081", "differs_from_national"]
    assert df.loc["A", "rank_in_topic"] == 1


def test_business_context_bands_and_missing_classes(silver_tables):
    df = g.build(g.build_business_context, silver_tables)
    row = df[(df["sector_key"] == "T001081")].set_index("size_band")["businesses"]
    b = silver_tables["fct_business_count"]
    tb = b[(b.sector_key == "T001081")]
    assert row["all"] == tb[tb.breakdown == "total"]["count"].iloc[0]
    small = tb[(tb.breakdown == "size") & (tb.size_band == "<10") & (tb.size_is_aggregate == False)]  # noqa: E712
    assert row["<10"] == small["count"].sum()

    t = dict(silver_tables)
    t["fct_business_count"] = b.assign(count=b["count"].where(
        ~((b.sector_key == "T001081") & (b.size_band == "<10")), None))
    row = g.build(g.build_business_context, t).set_index(["sector_key", "size_band"])["businesses"]
    assert pd.isna(row[("T001081", "<10")])                          # one class missing


def test_kvk_context(silver_tables):
    kvk = pd.DataFrame({
        "is_active": [True] * 12 + [False],
        "snapshot_date": [pd.Timestamp("2026-10-01").date()] * 13, "batch_id": "b",
        "start_date": [pd.Timestamp("2016-10-01").date()] * 11 + [None, None],
        "legal_form": ["BV"] * 11 + ["NV"] * 2,
        "sbi2008_section": ["A"] * 11 + [None] * 2})
    t = {**silver_tables, "kvk_company": kvk}
    df = g.build(g.build_kvk_context, t).set_index(["legal_form", "sbi2008_section"])
    assert df.loc[("all", "A"), "active_companies"] == 11
    assert df.loc[("all", "A"), "median_age_years"] == 10.0
    assert not df.loc[("all", "A"), "is_small_group"]
    assert df.loc[("all", "unknown"), "is_small_group"]             # 1 company (inactive excluded)
    assert pd.isna(df.loc[("all", "unknown"), "active_companies"])   # small: no numbers in gold
    assert df.loc[("all", "unknown"), "is_suppressed"]
    assert df.loc[("all", "A"), "sector_name"] == "Landbouw, bosbouw en visserij"


def test_kvk_context_hides_what_subtraction_would_reveal(silver_tables):
    # section A: 20 BV + 5 NV. Hiding only NV would leave all (25) - BV (20) = NV (5),
    # so BV is hidden too. Section C: 30 BV + 12 NV, nothing small, nothing hidden.
    # Section B: 15 BV, of which 3 with a start date: count shown, median age not.
    rows = [("A", "BV")] * 20 + [("A", "NV")] * 5 + [("C", "BV")] * 30 + [("C", "NV")] * 12 \
        + [("B", "BV")] * 15
    kvk = pd.DataFrame({
        "is_active": True, "snapshot_date": pd.Timestamp("2026-10-01").date(), "batch_id": "b",
        "start_date": [pd.Timestamp("2016-10-01").date()] * 70 + [None] * 12,
        "legal_form": [f for _, f in rows], "sbi2008_section": [s for s, _ in rows]})
    df = g.build(g.build_kvk_context, {**silver_tables, "kvk_company": kvk}).set_index(
        ["legal_form", "sbi2008_section"])
    assert df.loc[("NV", "A"), "is_small_group"] and df.loc[("NV", "A"), "is_suppressed"]
    assert not df.loc[("BV", "A"), "is_small_group"] and df.loc[("BV", "A"), "is_suppressed"]
    assert pd.isna(df.loc[("BV", "A"), "active_companies"])
    assert df.loc[("all", "A"), "active_companies"] == 25                # the total stays
    assert not df.loc[("all", "A"), "is_suppressed"]
    assert not df.xs("C", level="sbi2008_section")["is_suppressed"].any()
    assert df.loc[("all", "B"), "active_companies"] == 15
    assert df.loc[("all", "B"), "companies_with_age"] == 3
    assert pd.isna(df.loc[("all", "B"), "median_age_years"])             # median of 3 ages
    # whatever is shown, no hidden count can be worked out as 'all' minus the rest
    for _, part in df.reset_index().groupby("sbi2008_section"):
        forms = part[part["legal_form"] != "all"]
        assert forms["is_suppressed"].sum() != 1 or part.loc[
            part["legal_form"] == "all", "is_suppressed"].all()


def test_kvk_context_empty_without_silver_kvk(silver_tables):
    """Missing, without rows, or only inactive companies: the same table, with its
    columns and no rows (a missing table used to come back without columns, and only
    inactive companies made it crash)."""
    inactive = g.no_rows("kvk_company").astype(object)
    inactive.loc[0] = ["2026-10-01", "b1", False, "2000-01-01", "BV", "A", "silver_kvk_1"]
    for kvk in (None, g.no_rows("kvk_company"), inactive):
        t = silver_tables if kvk is None else {**silver_tables, "kvk_company": kvk}
        df = g.build(g.build_kvk_context, t)
        assert df.empty and list(df.columns) == g.KVK_CONTEXT_COLUMNS


# =========================================================================== end to end
def test_gold_run_from_bronze_to_gold(bronze_delta, monkeypatch, tmp_path_factory):
    monkeypatch.setattr(sys, "argv", ["pipeline/silver/cbs.py"])
    silver_run = s.run()
    run_id = g.run()
    runs = runlog.read_runs(paths.lakehouse().run_log)
    row = runs[runs.run_id == run_id].iloc[0]
    tables = json.loads(row.tables)
    assert row.status == "success"
    assert tables["gold_kvk_context"]["status"] == "skipped"            # no silver KVK here
    assert all(tables[n]["status"] == "published" for n in g.OPTIONAL_TABLES ^ set(tables))
    inputs = json.loads(row.inputs)
    assert inputs["silver/fct_absence_rate#run_id"] == silver_run      # which silver run
    assert inspect_lakehouse.silver_run_problems(paths.lakehouse().root, "gold")[1] == []

    # run(lakehouse=...): silver and gold built in a copy read and write only the copy
    import shutil
    copy = tmp_path_factory.mktemp("copy") / "lakehouse"
    shutil.copytree(paths.lakehouse().root, copy)
    copy_silver, copy_gold = s.run(lakehouse=copy), g.run(lakehouse=copy)
    assert list(runlog.read_runs(paths.lakehouse().run_log).run_id) == [silver_run, run_id]   # untouched
    copy_runs = runlog.read_runs(copy / "_run_log")
    assert list(copy_runs.run_id) == [silver_run, run_id, copy_silver, copy_gold]
    assert set(copy_runs.status) == {"success"}
    copy_inputs = json.loads(copy_runs.iloc[-1].inputs)
    assert copy_inputs["silver/fct_absence_rate#run_id"] == copy_silver
    assert inspect_lakehouse.silver_run_problems(copy, "gold")[1] == []


def test_gold_refuses_silver_tables_from_different_runs(bronze_delta):
    s.run()
    from deltalake import DeltaTable
    rate = DeltaTable(str(paths.lakehouse().silver / "fct_absence_rate")).to_pandas()
    publish.write_table(paths.lakehouse().silver / "fct_absence_rate", rate.assign(run_id="silver_cbs_old"))
    with pytest.raises(publish.RunFailed, match="come from different runs"):
        g.run()
    assert runlog.read_runs(paths.lakehouse().run_log).iloc[-1].status == "failed"


def test_gold_fails_when_silver_is_missing():
    with pytest.raises(publish.RunFailed, match="silver table missing"):
        g.run()


def test_gold_reads_only_the_kvk_columns_and_rows_it_needs():
    """kvk_company is large (1.9M rows): gold reads only the seven columns it uses, only
    the active companies (as build_kvk_context counts them: a missing is_active is not
    active), and text as categories."""
    rows = pd.DataFrame({"snapshot_date": [pd.Timestamp("2026-10-01").date()] * 3,
                         "batch_id": ["new"] * 3,
                         "is_active": pd.array([True, False, None], dtype="boolean"),
                         "start_date": [None] * 3,
                         "legal_form": ["BV"] * 3, "sbi2008_section": ["A", "B", "C"],
                         "sbi_mapping": ["1:1"] * 3, "run_id": "silver_kvk_1"})
    publish.write_table(paths.lakehouse().silver / "kvk_company", rows)
    inputs = {}
    df = g.read_silver(["kvk_company"], inputs)["kvk_company"]
    assert "sbi_mapping" not in df and set(g.PARTIAL_READS["kvk_company"]["columns"]) == set(df)
    assert list(df["sbi2008_section"]) == ["A"]                       # the active one only
    assert str(df["legal_form"].dtype) == "category"
    assert inputs["silver/kvk_company#run_id"] == "silver_kvk_1"      # the run check still works
    assert g.build_kvk_context(kvk_company=df, dim_sector=pd.DataFrame(
        {"sbi_code": ["A"], "sbi_level": ["section"], "sector_name": ["Landbouw"]})
    )["active_companies"].dropna().empty                              # 1 company: suppressed


# =========================================================================== risk index
SETTINGS = {"window_quarters": 8, "min_quarters": 4, "rising_per_year": 0.05}


def risk_inputs(series: dict[str, list], statuses=None, start=(2024, 1), counts=True) -> dict:
    """Silver tables for the risk index: 'total' is the national series, other keys are
    sectors (sector_key = SBI code, level 'section'), or a size class for key 'WP_1_10'."""
    rows, periods = [], []
    for key, values in series.items():
        q = quarters(values, statuses, start, key)
        for r in q.itertuples():
            member = "total" if key == "total" else ("size" if key.startswith("WP_") else "sector")
            rows.append({"period_key": r.period_key, "member_type": member,
                         "sector_key": key if member == "sector" else None,
                         "size_key": key if member == "size" else None,
                         "absence_pct": r.value, "period_status": r.period_status,
                         "ingested_at": pd.Timestamp("2026-10-01", tz="UTC")})
            periods.append({"period_key": r.period_key, "period_label": r.period_key,
                            "year": r.year, "quarter": r.quarter,
                            "start_date": pd.Timestamp(r.year, 3 * r.quarter - 2, 1)})
    sectors = [k for k in series if k != "total" and not k.startswith("WP_")]
    business = pd.DataFrame([
        {"period_key": "2026KW03", "period_status": F, "sector_key": k, "breakdown": b,
         "legal_form": lf, "size_key": sk, "count": n}
        for k in sectors for b, lf, sk, n in [("total", None, None, 1000),
                                               ("legal_form", "Natuurlijke personen", None, 800),
                                               ("size", None, "WP_1_2", 600)]]) if counts else \
        pd.DataFrame(columns=["period_key", "period_status", "sector_key", "breakdown",
                              "legal_form", "size_key", "count"])
    return {"fct_absence_rate": pd.DataFrame(rows),
            "dim_period": pd.DataFrame(periods).drop_duplicates("period_key"),
            "dim_sector": pd.DataFrame({"sector_key": sectors, "sbi_code": sectors,
                                        "sbi_level": "section", "sector_name": sectors}),
            "dim_company_size": pd.DataFrame({"size_key": ["WP_1_10"], "size_range": ["1-9"]}),
            "fct_business_count": business}


def risk(series, statuses=None, basis="all", **kw) -> pd.DataFrame:
    """The risk index for one basis ('all' or 'final'); basis=None: both."""
    df = g.build(g.build_risk_index, risk_inputs(series, statuses, **kw), settings_=SETTINGS)
    return df if basis is None else df[df["basis"] == basis].reset_index(drop=True)


def test_relative_risk_band_and_assessment():
    nat = [5.0] * 8
    df = risk({"total": nat, "Q": [7.0, 7.5, 7.0, 7.5, 7.0, 7.5, 7.0, 7.5],
               "I": [3.0] * 8, "P": [4.8, 5.2] * 4}).set_index("group_key")
    q = df.loc["Q"]
    assert q.relative_risk == pytest.approx(7.25 / 5.0)         # 4-quarter averages
    assert (q.band_low, q.band_high) == (1.4, 1.5)              # lowest and highest quarter
    assert q.assessment == "higher" and q.quarters_in_band == 8
    assert df.loc["I"].assessment == "lower" and df.loc["I"].relative_risk == pytest.approx(0.6)
    assert df.loc["P"].assessment == "around national"          # 0.96 and 1.04: band spans 1


def test_trend_removes_the_season_and_uses_the_threshold():
    # national and sector share the same winter peak: relative risk is flat -> stable
    season = [6.0, 5.0, 4.5, 5.5] * 2
    flat = risk({"total": season, "A": [v * 1.2 for v in season]}).set_index("group_key").loc["A"]
    assert flat.trend == "stable" and flat.trend_per_year == pytest.approx(0, abs=1e-9)
    # relative risk rising 0.02 per quarter = 0.08 per year -> rising
    up = risk({"total": [5.0] * 8, "B": [5.0 * (1 + 0.02 * i) for i in range(8)]}).iloc[0]
    assert up.trend == "rising" and up.trend_per_year == pytest.approx(0.08, abs=1e-3)
    down = risk({"total": [5.0] * 8, "C": [5.0 * (1 - 0.02 * i) for i in range(8)]}).iloc[0]
    assert down.trend == "falling"


def test_window_and_too_few_quarters():
    # 12 quarters: only the last 8 count for the band
    df = risk({"total": [5.0] * 12, "A": [10.0] * 4 + [6.0] * 8}).iloc[0]
    assert df.band_high == pytest.approx(1.2) and df.quarters_in_band == 8
    few = risk({"total": [5.0] * 3, "A": [6.0] * 3}).iloc[0]
    assert few.assessment == "too few quarters" and pd.isna(few.relative_risk)
    assert "too few quarters" in few.explanation


def test_final_basis_uses_only_final_quarters():
    statuses = [F] * 6 + [V, V]
    df = risk({"total": [5.0] * 8, "A": [5.0] * 6 + [9.0, 9.0]}, statuses, basis=None).set_index("basis")
    assert df.loc["all", "period_key"] == "2025KW04" and df.loc["all", "provisional_quarters"] == 2
    assert df.loc["final", "period_key"] == "2025KW02" and df.loc["final", "provisional_quarters"] == 0
    assert df.loc["final", "band_high"] == pytest.approx(1.0)  # the provisional 9.0s left out


def test_business_counts_and_explanation():
    df = risk({"total": [5.0] * 8, "A": [7.0] * 8, "WP_1_10": [2.5] * 8}).set_index("group_key")
    a = df.loc["A"]
    assert (a.businesses, a.businesses_natural_persons, a.businesses_one_person) == (1000, 800, 600)
    assert a.explanation.startswith("A A: 40% more absence than nationally")
    assert "80% run by natural persons" in a.explanation and "+0.00 per year" in a.explanation
    size = df.loc["WP_1_10"]
    assert size.group_type == "size" and pd.isna(size.businesses)   # size units differ: no count
    assert size.explanation.startswith("Size 1-9 persons employed: 50% less absence")
    assert risk({"total": [5.0] * 8, "A": [7.0] * 8}, counts=False).businesses.isna().all()


def test_final_basis_takes_the_latest_final_business_counts():
    t = risk_inputs({"total": [5.0] * 8, "A": [7.0] * 8})
    final = t["fct_business_count"]
    newer = final.assign(period_key="2026KW04", period_status=V, count=final["count"] + 5)
    t["fct_business_count"] = pd.concat([final, newer], ignore_index=True)
    df = g.build(g.build_risk_index, t, settings_=SETTINGS).set_index("basis")
    assert (df.loc["all", "businesses"], df.loc["all", "businesses_period"]) == (1005, "2026KW04")
    assert (df.loc["final", "businesses"], df.loc["final", "businesses_period"]) == (1000, "2026KW03")
    assert "(CBS 2026KW03)" in df.loc["final", "explanation"]


def test_business_counts_follow_the_sector_key_not_the_sbi_code():
    # two CBS aggregates (no SBI code, level "aggregate") and one SBI code with two keys:
    # each row must get its own sector's count
    t = risk_inputs({"total": [5.0] * 8, "X1": [6.0] * 8, "X2": [7.0] * 8,
                     "K1": [6.5] * 8, "K2": [5.5] * 8})
    t["dim_sector"] = pd.DataFrame({"sector_key": ["X1", "X2", "K1", "K2"],
                                    "sbi_code": [None, None, "56102", "56102"],
                                    "sbi_level": ["aggregate", "aggregate", "subclass", "subclass"],
                                    "sector_name": ["B-N excl. K", "A-U excl. O", "a", "b"]})
    totals = {"X1": 100, "X2": 200, "K1": 300, "K2": 400}
    b = t["fct_business_count"]
    b.loc[b["breakdown"] == "total", "count"] = b["sector_key"].map(totals)
    df = g.build(g.build_risk_index, t, settings_=SETTINGS)
    got = df[df["basis"] == "all"].set_index("group_key")["businesses"].to_dict()
    assert got == totals


def test_risk_index_history_is_the_quarterly_ratio(silver_tables):
    h = g.build(g.build_risk_index_history, silver_tables)
    assert set(h["group_type"]) <= {"sector", "size"}
    row = h.iloc[0]
    assert row.relative_risk == pytest.approx(row.absence_pct / row.national_pct, abs=1e-3)
    assert not h.duplicated(["period_key", "group_type", "group_key"]).any()
