"""Gold: comparisons, the five tables, and an end-to-end run from bronze to gold."""
import json
import sys

import pandas as pd
import pytest

import cbs_silver as s
import gold as g
import inspect_lakehouse
import publish
import runlog
from test_delta import bronze_delta  # noqa: F401  (fixture)

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
    df = g.build_absence_by_sector(silver_tables).set_index(["period_key", "sector_key"])
    total, agri = df.loc[("2025KW01", "T001081")], df.loc[("2025KW01", "301000")]
    assert agri.national_pct == total.absence_pct
    assert agri.diff_from_national_pp == pytest.approx(agri.absence_pct - total.absence_pct)
    assert agri.sbi_level == "section" and agri.rank_in_level == 1
    assert pd.isna(total.rank_in_level) and agri.is_provisional       # sample data: Voorlopig


def test_absence_by_size_keeps_units_apart(silver_tables):
    df = g.build_absence_by_size(silver_tables)
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
    df = g.build_absence_profile(t).set_index("characteristic_key")
    assert df.loc["A", "differs_from_national"] and df.loc["A", "direction"] == "above"
    assert df.loc["B", "differs_from_national"] and df.loc["B", "direction"] == "below"
    assert not df.loc["C", "differs_from_national"] and df.loc["C", "direction"] is None
    assert not df.loc["T001081", "differs_from_national"]
    assert df.loc["A", "rank_in_topic"] == 1


def test_business_context_bands_and_missing_classes(silver_tables):
    df = g.build_business_context(silver_tables)
    row = df[(df["sector_key"] == "T001081")].set_index("size_band")["businesses"]
    b = silver_tables["fct_business_count"]
    tb = b[(b.sector_key == "T001081")]
    assert row["all"] == tb[tb.breakdown == "total"]["count"].iloc[0]
    small = tb[(tb.breakdown == "size") & (tb.size_band == "<10") & (tb.size_is_aggregate == False)]  # noqa: E712
    assert row["<10"] == small["count"].sum()

    t = dict(silver_tables)
    t["fct_business_count"] = b.assign(count=b["count"].where(
        ~((b.sector_key == "T001081") & (b.size_band == "<10")), None))
    row = g.build_business_context(t).set_index(["sector_key", "size_band"])["businesses"]
    assert pd.isna(row[("T001081", "<10")])                          # one class missing


def test_kvk_context(silver_tables):
    kvk = pd.DataFrame({
        "is_latest": [True] * 13 + [False], "is_active": [True] * 12 + [False, True],
        "snapshot_date": [pd.Timestamp("2026-10-01").date()] * 14, "batch_id": "b",
        "start_date": [pd.Timestamp("2016-10-01").date()] * 11 + [None, None, None],
        "legal_form": ["BV"] * 11 + ["NV"] * 3,
        "sbi2008_section": ["A"] * 11 + [None] * 3})
    t = {**silver_tables, "kvk_company": kvk}
    df = g.build_kvk_context(t).set_index(["legal_form", "sbi2008_section"])
    assert df.loc[("all", "A"), "active_companies"] == 11
    assert df.loc[("all", "A"), "median_age_years"] == 10.0
    assert not df.loc[("all", "A"), "is_small_group"]
    assert df.loc[("all", "unknown"), "active_companies"] == 1      # inactive/old rows excluded
    assert df.loc[("all", "unknown"), "is_small_group"]
    assert df.loc[("all", "A"), "sector_name"] == "Landbouw, bosbouw en visserij"


def test_kvk_context_empty_without_silver_kvk(silver_tables):
    assert g.build_kvk_context(silver_tables).empty


# =========================================================================== end to end
def test_gold_run_from_bronze_to_gold(bronze_delta, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["cbs_silver.py"])
    silver_run = s.run()
    run_id = g.run()
    runs = runlog.read_runs(g.RUN_LOG)
    row = runs[runs.run_id == run_id].iloc[0]
    tables = json.loads(row.tables)
    assert row.status == "success"
    assert tables["gold_kvk_context"]["status"] == "skipped"            # no silver KVK here
    assert all(tables[n]["status"] == "published" for n in g.OPTIONAL_TABLES ^ set(tables))
    inputs = json.loads(row.inputs)
    assert inputs["silver/fct_absence_rate#run_id"] == silver_run      # which silver run
    assert inspect_lakehouse.silver_run_problems(g.LAKEHOUSE, "gold")[1] == []


def test_gold_refuses_silver_tables_from_different_runs(bronze_delta):
    s.run()
    from deltalake import DeltaTable
    rate = DeltaTable(str(s.SILVER_ROOT / "fct_absence_rate")).to_pandas()
    publish.write_table(s.SILVER_ROOT / "fct_absence_rate", rate.assign(run_id="silver_cbs_old"))
    with pytest.raises(publish.RunFailed, match="come from different runs"):
        g.run()
    assert runlog.read_runs(g.RUN_LOG).iloc[-1].status == "failed"


def test_gold_fails_when_silver_is_missing():
    with pytest.raises(publish.RunFailed, match="silver table missing"):
        g.run()
