"""Bronze and silver CBS against real Delta tables in tmp_path (the autouse
isolated_lakehouse fixture points every lakehouse path there). HTTP stays faked,
except in the `live` test at the bottom, which is skipped unless run with -m live."""
import json
import sys

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable

import cbs_bronze as b
import cbs_silver as s
import reference_bronze as r
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
    assert pa.schema(dt.schema().to_arrow()) == b.bronze_schema()
    row = dt.to_pandas().iloc[0]
    assert row._request_key == req.key and row._table_id == "80072NED"
    assert row._source_url == f"{ABSENCE.url}?%24filter=Perioden+eq+%272026KW01%27"
    assert str(row._ingested_at.tz) == "UTC"


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
    monkeypatch.setattr(sys, "argv", ["cbs_silver.py"])
    s.main()

    for name, expected in silver.items():
        if expected.empty:
            assert not (s.SILVER_ROOT / name).exists(), name
            continue
        got = silver_table(name).to_pandas()
        assert list(got.columns) == list(expected.columns), name
        assert len(got) == len(expected), name
        keep = [c for c in expected.columns if c != "ingested_at"]
        pd.testing.assert_frame_equal(
            got[keep].astype(object).where(got[keep].notna(), None).reset_index(drop=True),
            expected[keep].astype(object).where(expected[keep].notna(), None).reset_index(drop=True),
            check_dtype=False, obj=name)


def test_silver_types_after_reading_back(bronze_delta, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["cbs_silver.py"])
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
    monkeypatch.setattr(sys, "argv", ["cbs_silver.py"])
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
