"""Bronze CBS: period selection, request keys, pagination and the skip/fetch rules.
Offline: HTTP and Delta writes are replaced by in-memory fakes."""
import json

import pytest
import requests

import cbs_bronze as b
import cbs_silver as s
from conftest import DP_81589, table_info, topic_keys

Endpoint = b.Endpoint


def periods(*keys, status="Definitief"):
    # CBS pads period keys too, e.g. '2024KW03 '
    return [{"Key": f"{k} ", "Title": k, "Status": status} for k in keys]


# =========================================================================== period selection
def test_select_periods_prefers_quarters_from_start_year():
    ps = periods("2019KW04", "2019JJ00", "2020KW01", "2020JJ00", "2024KW04", "2025KW02", "2026MM01")
    assert [p["Key"].strip() for p in b.select_periods(ps, 2020)] == ["2020KW01", "2024KW04", "2025KW02"]


def test_select_periods_falls_back_to_years_without_quarters():
    ps = periods("2018JJ00", "2019JJ00", "2020JJ00", "2025JJ00")
    assert [p["Key"].strip() for p in b.select_periods(ps, 2020)] == ["2020JJ00", "2025JJ00"]


def test_select_periods_returns_nothing_for_unpreferred_types():
    assert b.select_periods(periods("2024MM01", "2024MM02"), 2020) == []


def test_select_periods_custom_start_year_and_preference():
    ps = periods("2023MM12", "2024MM01", "2024KW01")
    got = b.select_periods(ps, 2024, preference=("MM",))
    assert [p["Key"].strip() for p in got] == ["2024MM01"]


# =========================================================================== endpoints & keys
def test_endpoint_name_and_url():
    assert Endpoint("86009NED", "TypedDataSet").name == "86009ned_typeddataset"
    assert Endpoint("81589NED").name == "81589ned_root"
    assert Endpoint("81589NED").url == f"{b.BASE_URL}/81589NED"
    assert Endpoint("81589NED", "Perioden").url == f"{b.BASE_URL}/81589NED/Perioden"


def test_split_requests_one_per_period_with_status_in_key(monkeypatch):
    monkeypatch.setattr(b, "fetch_all", lambda session, url, params: (
        periods("2019KW04", "2020KW01") + periods("2020KW02", status="Voorlopig"), 1))
    reqs = b.plan_requests(None, Endpoint("80072NED", "TypedDataSet", split_by_period=True))
    assert [r.period for r in reqs] == ["2020KW01", "2020KW02"]
    assert reqs[0].key == "80072NED/TypedDataSet?Perioden=2020KW01#status=Definitief"
    assert reqs[1].key == "80072NED/TypedDataSet?Perioden=2020KW02#status=Voorlopig"
    assert reqs[0].params == {"$filter": "Perioden eq '2020KW01'"}


def test_status_change_gives_a_new_request_key(monkeypatch):
    ep = Endpoint("80072NED", "TypedDataSet", split_by_period=True)
    monkeypatch.setattr(b, "fetch_all", lambda *a: (periods("2025KW01", status="Voorlopig"), 1))
    provisional = b.plan_requests(None, ep)[0].key
    monkeypatch.setattr(b, "fetch_all", lambda *a: (periods("2025KW01", status="Definitief"), 1))
    final = b.plan_requests(None, ep)[0].key
    assert provisional != final


def test_non_split_request_key_and_params():
    (req,) = b.plan_requests(None, Endpoint("86009NED", "Bedrijfskenmerken"))
    assert req.key == "86009NED/Bedrijfskenmerken"
    assert req.params == {} and req.period is None
    (req,) = b.plan_requests(None, Endpoint("81644NED", "TypedDataSet", "ge", "2025JJ00", top=100))
    assert req.params == {"$filter": "Perioden ge '2025JJ00'", "$top": "100"}


# =========================================================================== pagination
class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    """Returns the given payloads in order and records every call."""
    def __init__(self, *payloads):
        self.payloads, self.calls = list(payloads), []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return FakeResponse(self.payloads.pop(0))


def test_fetch_all_follows_next_link():
    session = FakeSession({"value": [{"a": 1}], "odata.nextLink": "https://next/page2"},
                          {"value": [{"a": 2}]})
    records, pages = b.fetch_all(session, "https://x/TypedDataSet", {"$filter": "f"})
    assert records == [{"a": 1}, {"a": 2}] and pages == 2
    assert session.calls[0] == ("https://x/TypedDataSet", {"$filter": "f", "$format": "json"})
    assert session.calls[1] == ("https://next/page2", None)  # nextLink already has the params


def test_fetch_all_uses_skip_when_a_full_page_has_no_next_link(monkeypatch):
    monkeypatch.setattr(b, "PAGE_SIZE", 2)
    session = FakeSession({"value": [1, 2]}, {"value": [3, 4]}, {"value": [5]})
    records, pages = b.fetch_all(session, "https://x", {})
    assert records == [1, 2, 3, 4, 5] and pages == 3
    assert [c[1].get("$skip") for c in session.calls] == [None, "2", "4"]


def test_fetch_all_does_not_page_past_a_user_top(monkeypatch):
    monkeypatch.setattr(b, "PAGE_SIZE", 2)
    session = FakeSession({"value": [1, 2]})
    records, pages = b.fetch_all(session, "https://x", {"$top": "2"})
    assert records == [1, 2] and pages == 1


# =========================================================================== run(): skip / fetch rules
@pytest.fixture
def pipeline(monkeypatch):
    """Two small endpoints with fake HTTP and an in-memory ingestion log.
    state["fetched"] records every API call, including the Perioden lookup that
    plans the per-period requests (that small call is made on every run), except the
    DataProperties lookups for the checks, which go to state["dataprops"], and the
    TableInfos lookups (every run), which go to state["tableinfos"]."""
    state = {"log": set(), "fetched": [], "written": [], "fail": set(), "dataprops": [],
             "records": None, "tableinfos": [], "info": table_info()}
    eps = [Endpoint("81589NED", "TypedDataSet", split_by_period=True),
           Endpoint("81589NED", "Perioden")]
    monkeypatch.setattr(b, "ENDPOINTS", eps)
    monkeypatch.setattr(b, "make_session", lambda: None)
    monkeypatch.setattr(b, "done_keys", lambda: set(state["log"]))

    def fetch_all(session, url, params):
        if url.endswith("/DataProperties"):
            state["dataprops"].append(url)
            return DP_81589_FULL, 1
        if url.endswith("/TableInfos"):
            state["tableinfos"].append(url)
            return [state["info"]], 1
        state["fetched"].append((url, params.get("$filter")))
        if url.endswith("/Perioden"):
            return periods("2024KW04", "2025KW01"), 1
        if params.get("$filter") in state["fail"]:
            raise requests.ConnectionError("boom")
        period = params["$filter"].split("'")[1] if params.get("$filter") else None
        if state["records"] is not None:
            return state["records"](period), 1
        return [business_record(period)], 1

    monkeypatch.setattr(b, "fetch_all", fetch_all)
    monkeypatch.setattr(b, "write_bronze",
                        lambda req, recs, dataprops=None: state["written"].append(req.key))
    monkeypatch.setattr(b, "log_success", lambda req, n, pages: state["log"].add(req.key))
    return state


PLAN_CALL = (f"{b.BASE_URL}/81589NED/Perioden", None)


# DataProperties as CBS returns them: dimensions too, and a Datatype per measure
DP_81589_FULL = ([{"Type": "Dimension", "Key": "BedrijfstakkenBranchesSBI2008"},
                  {"Type": "TimeDimension", "Key": "Perioden"}]
                 + [{**d, "Datatype": "Long"} for d in DP_81589])


def business_record(period, **override):
    """A valid 81589NED record: both dimensions and every measure of DP_81589."""
    rec = {"ID": 0, "BedrijfstakkenBranchesSBI2008": "T001081", "Perioden": period,
           **{k: 100 for k in topic_keys(DP_81589)}}
    return {**rec, **override}


def test_first_run_fetches_every_request_and_logs_it(pipeline):
    b.run(only=None, force=False, dry_run=False)
    # planning lookup + two periods + the Perioden code-list endpoint itself
    assert len(pipeline["fetched"]) == 4
    # those three requests + the first TableInfos snapshot
    assert len(pipeline["log"]) == 4 and pipeline["log"] == set(pipeline["written"])
    assert sum("TableInfos" in k for k in pipeline["written"]) == 1


def test_second_run_only_makes_the_planning_call(pipeline):
    b.run(None, False, False)
    pipeline["fetched"].clear()
    b.run(None, False, False)
    assert pipeline["fetched"] == [PLAN_CALL] and len(pipeline["written"]) == 4


def test_failed_request_is_not_logged_and_is_retried(pipeline):
    pipeline["fail"].add("Perioden eq '2025KW01'")
    b.run(None, False, False)
    assert not any("2025KW01" in k for k in pipeline["log"])
    pipeline["fail"].clear()
    pipeline["fetched"].clear()
    b.run(None, False, False)
    assert pipeline["fetched"] == [
        PLAN_CALL, (f"{b.BASE_URL}/81589NED/TypedDataSet", "Perioden eq '2025KW01'")]


def test_force_fetches_again(pipeline):
    b.run(None, False, False)
    pipeline["fetched"].clear()
    b.run(None, force=True, dry_run=False)
    assert len(pipeline["fetched"]) == 4


def test_dry_run_fetches_and_writes_nothing(pipeline):
    b.run(None, False, dry_run=True)
    assert pipeline["fetched"] == [PLAN_CALL] and pipeline["written"] == []


def test_only_filters_by_endpoint_name(pipeline):
    b.run(only="81589NED_PERIODEN", force=False, dry_run=False)
    assert pipeline["fetched"] == [PLAN_CALL] and len(pipeline["written"]) == 1


# =========================================================================== run(): checks & quarantine
def quarantine_folders():
    return sorted(p.name for p in (b.QUARANTINE_ROOT / "cbs").glob("*") if p.is_dir())


def test_batch_failing_a_check_is_quarantined_not_written_and_retried(pipeline):
    pipeline["records"] = lambda period: [business_record(period, TotaalBedrijven_1="n/a")]
    b.run(None, False, False)
    # the Perioden code list passes; both data periods fail
    assert [k for k in pipeline["written"] if "TableInfos" not in k] == ["81589NED/Perioden"]
    assert not any("TypedDataSet" in k for k in pipeline["log"])
    folders = quarantine_folders()
    assert len(folders) == 2 and folders[0].endswith("_81589ned_typeddataset_2024KW04")

    report = json.loads((b.QUARANTINE_ROOT / "cbs" / folders[0] / "report.json").read_text())
    assert report["request_key"] == "81589NED/TypedDataSet?Perioden=2024KW04#status=Definitief"
    failed = [c["name"] for c in report["checks"] if not c["passed"]]
    assert failed == ["measures_numeric_or_null"]
    records = json.loads((b.QUARANTINE_ROOT / "cbs" / folders[0] / "records.json").read_text())
    assert records[0]["TotaalBedrijven_1"] == "n/a"

    pipeline["records"] = None                     # CBS fixes the data
    b.run(None, False, False)
    assert sum("TypedDataSet" in k for k in pipeline["written"]) == 2


def test_same_failing_batch_is_quarantined_once(pipeline):
    pipeline["records"] = lambda period: []        # a published period without records
    b.run(None, False, False)
    b.run(None, False, False)
    assert len(quarantine_folders()) == 2          # one per period, not one per run


@pytest.mark.parametrize("bad, check", [
    (lambda p: [business_record("2019KW01")], "period_matches_request"),
    (lambda p: [{k: v for k, v in business_record(p).items() if k != "TotaalBedrijven_1"}],
     "has_all_columns"),
    (lambda p: [business_record(p), business_record(p, ID=1)], "unique_dimension_keys"),
    (lambda p: [], "not_empty"),
])
def test_each_check_stops_a_batch(pipeline, bad, check):
    pipeline["records"] = bad
    b.run(None, False, False)
    folder = b.QUARANTINE_ROOT / "cbs" / quarantine_folders()[0]
    report = json.loads((folder / "report.json").read_text())
    assert check in [c["name"] for c in report["checks"] if not c["passed"]]
    assert not any("TypedDataSet" in k for k in pipeline["written"])


def test_dataprops_fetched_once_per_table_and_only_with_new_data(pipeline):
    b.run(None, False, False)
    assert pipeline["dataprops"] == [f"{b.BASE_URL}/81589NED/DataProperties"]
    pipeline["dataprops"].clear()
    b.run(None, False, False)                      # nothing new
    assert pipeline["dataprops"] == []


# =========================================================================== configuration
def test_endpoints_are_unique():
    keys = [(e.table_id, e.entity, e.period_op, e.period_value) for e in b.ENDPOINTS]
    assert len(keys) == len(set(keys))


def test_all_typed_datasets_are_split_by_period():
    for e in b.ENDPOINTS:
        if e.entity == "TypedDataSet":
            assert e.split_by_period and e.top is None, e.name


def test_bronze_has_every_table_silver_reads():
    """Silver reads these bronze tables; each must be configured in bronze."""
    configured = {(e.table_id, e.entity) for e in b.ENDPOINTS}
    needed = {(t, dim) for t, dim in s.CHARACTERISTIC_DIMS.items()}
    needed |= {(t, "TypedDataSet") for t in s.CHARACTERISTIC_DIMS}
    needed |= {(t, "DataProperties") for t in s.CHARACTERISTIC_DIMS}
    needed |= {(t, "CategoryGroups") for t in ("80072NED", *s.SURVEY_TABLES)}
    needed |= {("81644NED", "RegioS")}
    assert needed - configured == set()


def test_silver_reads_the_same_folder_names_bronze_writes():
    for e in b.ENDPOINTS:
        assert e.name == f"{e.table_id}_{e.entity or 'root'}".lower()
