"""Bronze and silver CBS against real Delta tables in tmp_path (the autouse
isolated_lakehouse fixture points every lakehouse path there). HTTP stays faked,
except in the `live` test at the bottom, which is skipped unless run with -m live."""
import json
import sys

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable

from pipeline.bronze import cbs as b
from pipeline.silver import cbs as s
from pipeline.bronze import reference as r
from conftest import CROSSWALK_BATCH, INGESTED_AT, sample_bronze, sample_crosswalk, table_info
from test_bronze_logic import periods

Endpoint = b.Endpoint
ABSENCE = Endpoint("80072NED", "TypedDataSet", split_by_period=True)
DP_80072 = [{"Type": "Dimension", "Key": "BedrijfskenmerkenSBI2008"},
            {"Type": "TimeDimension", "Key": "Perioden"},
            {"Type": "Topic", "Key": "Ziekteverzuimpercentage_1", "Datatype": "Float"}]


def period_request(period, status, ep=ABSENCE):
    return b.Request(endpoint=ep, key=f"{ep.table_id}/{ep.entity}?Perioden={period}#status={status}",
                     params={"$filter": f"Perioden eq '{period}'"}, period=period, period_status=status)


def records(period, *values):
    return [{"ID": i, "BedrijfskenmerkenSBI2008": "T001081", "Perioden": period,
             "Ziekteverzuimpercentage_1": v} for i, v in enumerate(values)]


def bronze_rows(ep=ABSENCE):
    return DeltaTable(str(b.BRONZE_ROOT / ep.name)).to_pandas().sort_values(["_period", "_record_index"])


# =========================================================================== bronze writes
def test_revised_period_replaces_its_rows_and_keeps_other_periods():
    b.write_bronze(period_request("2025KW04", "Definitief"), records("2025KW04", 5.6))
    b.write_bronze(period_request("2026KW01", "Voorlopig"), records("2026KW01", 6.0, 6.1))
    b.write_bronze(period_request("2026KW01", "Definitief"), records("2026KW01", 6.2))

    df = bronze_rows()
    assert list(df._period) == ["2025KW04", "2026KW01"]
    assert list(df._period_status) == ["Definitief", "Definitief"]
    assert [json.loads(r)["Ziekteverzuimpercentage_1"] for r in df.raw_json] == [5.6, 6.2]


def test_writing_the_same_request_twice_gives_no_duplicates():
    """A crash between write_bronze and log_success means the next run writes again."""
    req = period_request("2026KW01", "Voorlopig")
    b.write_bronze(req, records("2026KW01", 6.0, 6.1))
    b.write_bronze(req, records("2026KW01", 6.0, 6.1))
    assert len(bronze_rows()) == 2


def test_non_split_request_is_replaced_by_request_key():
    ep = Endpoint("86009NED", "Bedrijfskenmerken")
    (req,) = b.plan_requests(None, ep)
    b.write_bronze(req, [{"Key": "T001081", "Title": "old"}])
    b.write_bronze(req, [{"Key": "T001081", "Title": "new"}, {"Key": "301000", "Title": "A"}])
    df = bronze_rows(ep)
    assert [json.loads(r)["Title"] for r in df.raw_json] == ["new", "A"]
    assert df._period.isna().all()


def test_bronze_schema_and_lineage():
    req = period_request("2026KW01", "Voorlopig")
    b.write_bronze(req, records("2026KW01", 6.0))
    dt = DeltaTable(str(b.BRONZE_ROOT / ABSENCE.name))
    schema = pa.schema(dt.schema().to_arrow())
    fixed = b.bronze_schema()
    assert pa.schema(list(schema)[:len(fixed)]) == fixed          # lineage + raw_json first
    assert schema.names[len(fixed):] == ["ID", "BedrijfskenmerkenSBI2008", "Perioden",
                                         "Ziekteverzuimpercentage_1"]   # then the fields
    row = dt.to_pandas().iloc[0]
    assert row._request_key == req.key and row._table_id == "80072NED"
    assert row._source_url == f"{ABSENCE.url}?%24filter=Perioden+eq+%272026KW01%27"
    assert str(row._ingested_at.tz) == "UTC"


# =========================================================================== structured bronze
def bronze_types(ep=ABSENCE):
    dt = DeltaTable(str(b.BRONZE_ROOT / ep.name))
    return {f.name: str(f.type) for f in pa.schema(dt.schema().to_arrow())}


def test_fields_are_columns_with_values_as_received_and_raw_json_kept():
    recs = [{"ID": 1, "BedrijfskenmerkenSBI2008": "307500 ", "Perioden": "2026KW01",
             "Ziekteverzuimpercentage_1": 6.2}]
    b.write_bronze(period_request("2026KW01", "Voorlopig"), recs)
    row = bronze_rows().iloc[0]
    assert row["BedrijfskenmerkenSBI2008"] == "307500 "            # padding kept: silver strips
    assert row["Ziekteverzuimpercentage_1"] == 6.2
    assert json.loads(row["raw_json"]) == recs[0]


def test_types_come_from_dataproperties():
    """5 and 5.4 in one field: without DataProperties the first batch would be int64."""
    b.write_bronze(period_request("2025KW04", "Definitief"), records("2025KW04", 5), DP_80072)
    b.write_bronze(period_request("2026KW01", "Voorlopig"), records("2026KW01", 5.4), DP_80072)
    assert bronze_types()["Ziekteverzuimpercentage_1"] == "double"
    assert list(bronze_rows()["Ziekteverzuimpercentage_1"]) == [5.0, 5.4]


def test_a_later_batch_cannot_change_a_column_type():
    b.write_bronze(period_request("2025KW04", "Definitief"), records("2025KW04", 5.5))
    b.write_bronze(period_request("2026KW01", "Voorlopig"), records("2026KW01", 6))  # int now
    assert bronze_types()["Ziekteverzuimpercentage_1"] == "double"


def test_an_all_empty_field_is_text_and_a_later_number_still_fits():
    """Like CategoryGroupID in 81644NED's RegioS: empty in every record, so its type is
    unknown. Bronze makes it text, not Arrow's null type: that would become Delta's
    "void", which isn't in the Delta protocol and other engines may refuse. A number
    that arrives later is stored as text; raw_json keeps the number."""
    b.write_bronze(period_request("2025KW04", "Definitief"),
                   [{**r, "CategoryGroupID": None} for r in records("2025KW04", 5.5)])
    b.write_bronze(period_request("2026KW01", "Voorlopig"),
                   [{**r, "CategoryGroupID": 12} for r in records("2026KW01", 6.0)])
    assert bronze_types()["CategoryGroupID"] == "string"
    df = bronze_rows()
    assert df["CategoryGroupID"].isna().iloc[0] and df["CategoryGroupID"].iloc[1] == "12"
    assert json.loads(df["raw_json"].iloc[1])["CategoryGroupID"] == 12


def test_a_field_cbs_adds_later_becomes_a_new_column():
    b.write_bronze(period_request("2025KW04", "Definitief"), records("2025KW04", 5.5))
    newer = [{**r, "Nieuw_2": 7} for r in records("2026KW01", 6.0)]
    b.write_bronze(period_request("2026KW01", "Voorlopig"), newer)
    df = bronze_rows()
    assert "Nieuw_2" in df and df["Nieuw_2"].isna().iloc[0] and df["Nieuw_2"].iloc[1] == 7


def test_nested_values_and_clashing_names_are_kept():
    ep = Endpoint("86009NED", "DataProperties")
    (req,) = b.plan_requests(None, ep)
    b.write_bronze(req, [{"odata.type": "Cbs.OData.Topic", "Key": "X", "_period": "odd",
                          "Extra": {"a": 1}, "Flag": True}])
    row = bronze_rows(ep).iloc[0]
    assert row["odata.type"] == "Cbs.OData.Topic"                  # exact field name
    assert row["field__period"] == "odd" and row["_period"] is None  # no clash with lineage
    assert json.loads(row["Extra"]) == {"a": 1}
    assert bronze_types(ep)["Flag"] == "bool" and row["Flag"] == True  # noqa: E712


def write_old_format(req, recs):
    """A bronze table as written before bronze had a column per field."""
    from deltalake import write_deltalake
    table = b.bronze_table(req, recs, b.datetime.now(b.timezone.utc))
    write_deltalake(str(b.BRONZE_ROOT / req.endpoint.name),
                    table.select(b.bronze_schema().names), mode="append")


def test_silver_reads_old_and_new_bronze_the_same():
    recs = records("2026KW01", 6.0, 5.4)
    write_old_format(period_request("2026KW01", "Voorlopig"), recs)
    old = s.read_bronze("80072NED", "TypedDataSet")
    b.rebuild_columns()
    new = s.read_bronze("80072NED", "TypedDataSet")
    pd.testing.assert_frame_equal(old, new)
    assert "ID" not in new and new.BedrijfskenmerkenSBI2008.iloc[0] == "T001081"


def test_rebuild_columns_keeps_rows_and_lineage_and_uses_bronze_dataproperties():
    dp = Endpoint("80072NED", "DataProperties")
    (dp_req,) = b.plan_requests(None, dp)
    b.write_bronze(dp_req, DP_80072)
    write_old_format(period_request("2026KW01", "Voorlopig"), records("2026KW01", 6))
    before = bronze_rows()[b.bronze_schema().names]
    assert b.rebuild_columns(dry_run=True) == ["bronze/80072ned_dataproperties: 3 rows",
                                               "bronze/80072ned_typeddataset: 1 rows"]
    assert "Perioden" not in bronze_rows()                         # dry run changes nothing
    b.rebuild_columns()
    after = bronze_rows()
    pd.testing.assert_frame_equal(after[before.columns], before)
    assert bronze_types()["Ziekteverzuimpercentage_1"] == "double"  # 6 is an int in the JSON
    assert b.done_keys() == set()                                   # the log is left alone


# =========================================================================== ingestion log
def test_done_keys_is_empty_without_a_log():
    assert b.done_keys() == set()


def test_done_keys_reads_back_what_log_success_wrote():
    reqs = [period_request("2025KW04", "Definitief"), period_request("2026KW01", "Voorlopig")]
    for r in reqs:
        b.log_success(r, row_count=39, pages=1)
    assert b.done_keys() == {r.key for r in reqs}
    log = DeltaTable(str(b.LOG_TABLE)).to_pandas()
    assert list(log.row_count) == [39, 39] and set(log.period) == {"2025KW04", "2026KW01"}


# =========================================================================== run() end to end
@pytest.fixture
def cbs_api(monkeypatch):
    """Fake CBS API for one split endpoint; the Delta writes and the log are real."""
    state = {"periods": periods("2025KW04") + periods("2026KW01", status="Voorlopig"),
             "value": 6.0, "data_calls": 0}
    monkeypatch.setattr(b, "ENDPOINTS", [ABSENCE])
    monkeypatch.setattr(b, "make_session", lambda: None)

    def fetch_all(session, url, params):
        if url.endswith("/Perioden"):
            return state["periods"], 1
        if url.endswith("/DataProperties"):
            return DP_80072, 1
        if url.endswith("/TableInfos"):
            return [table_info()], 1
        state["data_calls"] += 1
        period = params["$filter"].split("'")[1]
        return records(period, state["value"]), 1

    monkeypatch.setattr(b, "fetch_all", fetch_all)
    return state


def test_run_twice_then_revision(cbs_api):
    b.run(None, False, False)
    assert cbs_api["data_calls"] == 2 and len(bronze_rows()) == 2

    b.run(None, False, False)                      # nothing new: log is read from Delta
    assert cbs_api["data_calls"] == 2

    cbs_api["periods"] = periods("2025KW04", "2026KW01")   # CBS finalises 2026KW01
    cbs_api["value"] = 6.3
    b.run(None, False, False)
    assert cbs_api["data_calls"] == 3
    df = bronze_rows()
    assert list(df._period_status) == ["Definitief", "Definitief"]
    assert json.loads(df.raw_json.iloc[1])["Ziekteverzuimpercentage_1"] == 6.3
    data_keys = [k for k in b.done_keys() if "TypedDataSet" in k]
    assert len(data_keys) == 3                     # old provisional key stays in the log


# =========================================================================== TableInfos
def test_table_infos_snapshot_only_when_cbs_changes_it(cbs_api, monkeypatch, caplog):
    info = {"value": table_info()}
    real_fetch = b.fetch_all
    monkeypatch.setattr(b, "fetch_all", lambda s, url, params: (
        ([info["value"]], 1) if url.endswith("/TableInfos") else real_fetch(s, url, params)))
    infos = Endpoint("80072NED", "TableInfos")

    b.run(None, False, False)
    b.run(None, False, False)                      # unchanged: nothing new stored
    assert len(bronze_rows(infos)) == 1

    info["value"] = table_info(modified="2026-10-01T02:00:00")
    b.run(None, False, False)                      # CBS updated the table: second version
    df = bronze_rows(infos)
    assert sorted(json.loads(r)["Modified"] for r in df.raw_json) == [
        "2026-09-15T02:00:00", "2026-10-01T02:00:00"]

    info["value"] = table_info("Stopgezet", "2024-04-17T02:00:00", "Gediscontinueerd")
    caplog.clear()
    b.run(None, False, False)
    assert any("CBS has stopped this table" in r.message for r in caplog.records)
    assert len(bronze_rows(infos)) == 3


# =========================================================================== silver end to end
@pytest.fixture
def bronze_delta():
    """The conftest sample tables written to real bronze Delta tables."""
    for (table_id, entity), raw in sample_bronze().items():
        ep = Endpoint(table_id, entity)
        status = raw._period_status.iloc[0]
        req = b.Request(endpoint=ep, key=f"{table_id}/{entity}", params={}, period_status=status)
        b.write_bronze(req, [json.loads(row) for row in raw.raw_json])
    ref = sample_crosswalk()
    r.write_batch(ref[list(r.COLUMNS)], CROSSWALK_BATCH, INGESTED_AT, "v20.xlsx", None, "sha", "v2.0")
    r.write_log(CROSSWALK_BATCH, INGESTED_AT, "v20.xlsx", None, "sha", "v2.0", len(ref))


def silver_table(name):
    return DeltaTable(str(s.SILVER_ROOT / name))


def test_silver_end_to_end_matches_the_in_memory_build(bronze_delta, silver, monkeypatch):
    """Delta round trip changes nothing but the lineage timestamp (bronze sets its own)."""
    monkeypatch.setattr(sys, "argv", ["pipeline/silver/cbs.py"])
    s.main()

    for name, expected in silver.items():
        if expected.empty:
            assert not (s.SILVER_ROOT / name).exists(), name
            continue
        got = silver_table(name).to_pandas()
        assert list(got.columns) == list(expected.columns) + ["run_id"], name
        assert len(got) == len(expected), name
        keep = [c for c in expected.columns if c != "ingested_at"]
        pd.testing.assert_frame_equal(
            got[keep].astype(object).where(got[keep].notna(), None).reset_index(drop=True),
            expected[keep].astype(object).where(expected[keep].notna(), None).reset_index(drop=True),
            check_dtype=False, obj=name)


def test_silver_types_after_reading_back(bronze_delta, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["pipeline/silver/cbs.py"])
    s.main()

    def types(name):
        return {f.name: f.type for f in pa.schema(silver_table(name).schema().to_arrow())}

    utc = pa.timestamp("us", tz="UTC")
    period = types("dim_period")
    assert period["start_date"] == pa.date32() and period["end_date"] == pa.date32()
    assert period["year"] == pa.int64() and period["quarter"] == pa.int64()

    for name in ("fct_business_count", "fct_establishment_count"):
        t = types(name)
        assert t["count"] == pa.int64() and t["size_is_aggregate"] == pa.bool_(), name
        assert t["ingested_at"] == utc, name
    for name in ("fct_absence_rate", "fct_absence_survey"):
        assert types(name)["ingested_at"] == utc, name
    assert types("fct_absence_rate")["absence_pct"] == pa.float64()
    size = types("dim_company_size")
    assert size["lower_bound"] == pa.int64() and size["upper_bound_excl"] == pa.int64()


def test_silver_rerun_replaces_tables(bronze_delta, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["pipeline/silver/cbs.py"])
    s.main()
    first = silver_table("fct_absence_rate").to_pandas()
    s.main()
    dt = silver_table("fct_absence_rate")
    assert dt.version() == 1 and len(dt.to_pandas()) == len(first)


# =========================================================================== live (opt-in)
@pytest.mark.live
def test_live_cbs_periods_and_status():
    """Catches changes in the CBS API or its period lists. Run with: pytest -m live"""
    session = b.make_session()
    ps, pages = b.fetch_all(session, f"{b.BASE_URL}/81589NED/Perioden", {})
    assert pages == 1 and len(ps) > 0
    selected = b.select_periods(ps, 2020)
    assert selected, "no quarters from 2020 on"
    for p in selected:
        assert (p.get("Status") or "").strip() in {"Definitief", "NaderVoorlopig", "Voorlopig"}, p
        assert s.parse_period(p["Key"].strip())["period_type"] == "quarter", p


# =========================================================================== scope cleanup
def test_out_of_scope_periods_rule():
    ep = Endpoint("80072NED", "TypedDataSet", split_by_period=True, start_year=2026)
    cbs = periods("2025KW04", "2025JJ00", "2026KW01")
    # 2025KW04: before the start year -> out; 2019KW01: CBS no longer lists it -> kept
    assert b.out_of_scope_periods(ep, {"2025KW04", "2026KW01", "2019KW01"}, cbs) == {"2025KW04"}
    yearly = Endpoint("80072NED", "TypedDataSet", split_by_period=True, start_year=2025,
                      period_types=("JJ",))
    assert b.out_of_scope_periods(yearly, {"2025KW04", "2025JJ00"}, cbs) == {"2025KW04"}


def test_cleanup_scope_after_a_later_start_year(cbs_api, monkeypatch):
    b.run(None, False, False)                      # 2025KW04 and 2026KW01 in bronze
    later = Endpoint("80072NED", "TypedDataSet", split_by_period=True, start_year=2026)
    monkeypatch.setattr(b, "ENDPOINTS", [later])

    planned = b.cleanup_scope(dry_run=True)
    assert planned == ["remove 1 periods from bronze/80072ned_typeddataset: 2025KW04"]
    assert len(bronze_rows()) == 2                 # dry run changed nothing

    assert b.cleanup_scope(dry_run=False) == planned
    assert list(bronze_rows()._period) == ["2026KW01"]
    assert not any("2025KW04" in k for k in b.done_keys())
    assert b.cleanup_scope(dry_run=False) == []    # nothing left to do

    calls = cbs_api["data_calls"]
    b.run(None, False, False)                      # and ingestion doesn't fetch it again
    assert cbs_api["data_calls"] == calls


def test_cleanup_scope_removes_tables_no_longer_configured(cbs_api, monkeypatch):
    b.run(None, False, False)
    b.write_bronze(b.plan_requests(None, Endpoint("80072NED", "Perioden"))[0],
                   [{"Key": "2026KW01"}])
    b.log_success(b.plan_requests(None, Endpoint("80072NED", "Perioden"))[0], 1, 1)
    monkeypatch.setattr(b, "ENDPOINTS", [Endpoint("80072NED", "Perioden")])

    actions = b.cleanup_scope(dry_run=False)
    assert sorted(actions) == ["remove table bronze/80072ned_tableinfos (not in config.yaml)",
                               "remove table bronze/80072ned_typeddataset (not in config.yaml)"]
    assert not (b.BRONZE_ROOT / "80072ned_typeddataset").exists()
    assert (b.BRONZE_ROOT / "80072ned_perioden").exists()
    assert b.done_keys() == {"80072NED/Perioden"}


def test_cleanup_scope_leaves_kvk_and_reference_alone(tmp_path):
    from deltalake import write_deltalake
    import pyarrow as pa
    write_deltalake(str(b.BRONZE_ROOT / "kvk" / "basis_bedrijfsgegevens"), pa.table({"a": [1]}))
    write_deltalake(str(b.BRONZE_ROOT / "reference" / "sbi2008_sbi2025"), pa.table({"a": [1]}))
    assert b.cleanup_scope(session=object(), dry_run=False) == []
    assert (b.BRONZE_ROOT / "kvk" / "basis_bedrijfsgegevens").exists()


# =========================================================================== silver runs and run log
from tools import inspect_lakehouse  # noqa: E402
from pipeline.common import publish  # noqa: E402
from pipeline.common import runlog  # noqa: E402


def run_ids(name):
    return set(silver_table(name).to_pandas(columns=["run_id"])["run_id"])


def test_successful_run_is_logged_and_joinable(bronze_delta):
    run_id = s.run()
    runs = runlog.read_runs(s.RUN_LOG)
    assert list(runs.run_id) == [run_id] and list(runs.status) == ["success"]
    row = runs.iloc[0]
    assert row.pipeline == "silver_cbs" and row.finished_at >= row.started_at

    tables = json.loads(row.tables)
    assert tables["fct_absence_rate"]["status"] == "published"
    for name, info in tables.items():
        assert run_ids(name) == {run_id}, name                 # every row carries the run_id
        assert len(silver_table(name).to_pandas()) == info["rows"], name
    inputs = json.loads(row.inputs)
    assert inputs["bronze/80072ned_typeddataset"] == 0          # Delta version that was read
    assert inputs["bronze/reference/sbi2008_sbi2025#batch_id"] == CROSSWALK_BATCH

    joined = silver_table("fct_absence_rate").to_pandas().merge(runs, on="run_id")
    assert len(joined) == tables["fct_absence_rate"]["rows"]
    assert not (s.SILVER_ROOT / "_staging").exists()           # staging cleaned up


def test_failed_build_leaves_live_tables_untouched(bronze_delta, monkeypatch):
    first = s.run()
    monkeypatch.setattr(s, "build_all", lambda **kw: (_ for _ in ()).throw(ValueError("boom")))
    with pytest.raises(ValueError):
        s.run()
    runs = runlog.read_runs(s.RUN_LOG)
    assert list(runs.status) == ["success", "failed"]
    assert runs.iloc[1].error == "ValueError: boom"
    assert run_ids("fct_absence_rate") == {first}
    assert silver_table("fct_absence_rate").version() == 0


def test_empty_required_table_fails_before_anything_is_written(bronze_delta, monkeypatch):
    real = s.build_all
    monkeypatch.setattr(s, "build_all", lambda **kw: {**real(**kw), "dim_region": pd.DataFrame()})
    with pytest.raises(publish.RunFailed, match="required tables came out empty: dim_region"):
        s.run()
    assert not (s.SILVER_ROOT / "fct_absence_rate").exists()
    assert runlog.read_runs(s.RUN_LOG).iloc[0].status == "failed"


def test_empty_optional_table_is_skipped(bronze_delta, monkeypatch):
    real = s.build_all
    monkeypatch.setattr(s, "build_all",
                        lambda **kw: {**real(**kw), "map_sbi2008_sbi2025": pd.DataFrame()})
    s.run()
    row = runlog.read_runs(s.RUN_LOG).iloc[0]
    assert row.status == "success"
    assert json.loads(row.tables)["map_sbi2008_sbi2025"] == {"rows": 0, "status": "skipped"}


def test_staging_failure_leaves_live_tables_untouched(bronze_delta, monkeypatch):
    first = s.run()
    real = publish.write_table

    def fail_on_fifth(path, df, calls=[]):
        calls.append(path)
        if len(calls) == 5:
            raise OSError("disk full")
        real(path, df)

    monkeypatch.setattr(publish, "write_table", fail_on_fifth)
    with pytest.raises(OSError):
        s.run()
    for name in ("dim_period", "fct_absence_rate", "map_sbi2008_sbi2025"):
        assert run_ids(name) == {first}, name
    assert not (s.SILVER_ROOT / "_staging").exists()


def test_publish_stopping_halfway_is_logged_and_detected(bronze_delta, monkeypatch):
    first = s.run()
    real = publish._overwrite

    def fail_on_third(path, table, calls=[]):
        if "_staging" not in str(path):           # count only writes to live tables
            calls.append(path)
            if len(calls) == 3:
                raise OSError("power cut")
        real(path, table)

    monkeypatch.setattr(publish, "_overwrite", fail_on_third)
    with pytest.raises(publish.RunFailed, match="publish incomplete after 2 tables"):
        s.run()
    runs = runlog.read_runs(s.RUN_LOG)
    assert runs.iloc[1].status == "failed" and "power cut" in runs.iloc[1].error

    _, problems = inspect_lakehouse.silver_run_problems(s.LAKEHOUSE)
    assert any("latest silver_cbs run" in p and "failed" in p for p in problems)
    mixed = [p for p in problems if "is not the latest successful run" in p]
    assert len(mixed) == 2                         # the two tables already replaced
    assert first in mixed[0]

    monkeypatch.setattr(publish, "_overwrite", real)
    s.run()                                        # a clean rerun fixes it
    assert inspect_lakehouse.silver_run_problems(s.LAKEHOUSE)[1] == []


# =========================================================================== inspect_lakehouse
def test_inspect_counts_every_provisional_status(tmp_path):
    """Both Voorlopig and NaderVoorlopig are provisional; only Definitief is final."""
    publish.write_table(tmp_path / "t", pd.DataFrame({
        "period_key": ["2025KW01", "2025KW02", "2025KW03", "2025KW04"],
        "period_status": ["Definitief", "NaderVoorlopig", "Voorlopig", None]}))
    info = inspect_lakehouse.summarize(tmp_path / "t")
    assert "2 provisional rows" in info["extra"]
