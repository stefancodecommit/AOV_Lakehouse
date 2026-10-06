"""Monthly quarantine cleanup: checks.quarantine.cleanup and maintenance.make_resolver."""
import json
from datetime import datetime, timedelta, timezone

import pandas as pd

import cbs_bronze as b
import maintenance as m
from checks.quarantine import cleanup, list_entries, quarantine
from test_delta import ABSENCE, DP_80072, records
from conftest import table_info
from test_bronze_logic import periods

NOW = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)


def entry_at(root, name, when, **report):
    """Quarantine an entry and backdate it to `when`."""
    folder = quarantine(root, report.pop("source", "cbs"), name, report, records=[{"n": name}])
    path = folder / "report.json"
    data = json.loads(path.read_text())
    data["quarantined_at"] = when.isoformat()
    path.write_text(json.dumps(data))
    return folder


def names(root):
    return sorted(e["name"] for e in list_entries(root))


# =========================================================================== cleanup()
def test_cleanup_removes_resolved_and_old_keeps_recent(tmp_path):
    entry_at(tmp_path, "recent", NOW - timedelta(days=10))
    entry_at(tmp_path, "old", NOW - timedelta(days=91))
    entry_at(tmp_path, "fixed", NOW - timedelta(days=1))
    resolved = lambda e: "resolved: fixed" if e["name"] == "fixed" else None
    removed = cleanup(tmp_path, resolved, 90, now=NOW)
    assert sorted(r for _, r in removed) == ["older than 90 days (91 days)", "resolved: fixed"]
    assert names(tmp_path) == ["recent"]


def test_cleanup_dry_run_removes_nothing(tmp_path):
    entry_at(tmp_path, "old", NOW - timedelta(days=200))
    assert len(cleanup(tmp_path, lambda e: None, 90, now=NOW, dry_run=True)) == 1
    assert names(tmp_path) == ["old"]


def test_cleanup_keeps_unreadable_reports(tmp_path):
    folder = entry_at(tmp_path, "broken", NOW - timedelta(days=500))
    (folder / "report.json").write_text("{not json")
    assert cleanup(tmp_path, lambda e: None, 90, now=NOW) == []
    assert folder.exists()


# =========================================================================== resolver
CBS_LOG = pd.DataFrame({
    "request_key": ["80072NED/TypedDataSet?Perioden=2026KW01#status=Definitief",
                    "80072NED/Perioden"],
    "table_id": ["80072NED", "80072NED"], "entity": ["TypedDataSet", "Perioden"],
    "period": ["2026KW01", None],
    "ingested_at": pd.to_datetime(["2026-10-03", "2026-10-03"], utc=True)})


def cbs_entry(period="2026KW01", entity="TypedDataSet", when="2026-10-01T00:00:00+00:00"):
    return {"source": "cbs", "table_id": "80072NED", "entity": entity, "period": period,
            "quarantined_at": when}


def test_cbs_entry_resolved_when_same_period_ingested_later():
    resolve = m.make_resolver({"cbs": CBS_LOG})
    assert resolve(cbs_entry()).startswith("resolved: 80072NED/TypedDataSet?Perioden=2026KW01")
    assert resolve(cbs_entry(entity="Perioden", period=None)) is not None


def test_cbs_entry_not_resolved_by_other_period_or_earlier_ingest():
    resolve = m.make_resolver({"cbs": CBS_LOG})
    assert resolve(cbs_entry(period="2026KW02")) is None
    assert resolve(cbs_entry(when="2026-10-04T00:00:00+00:00")) is None


def test_file_sources_resolved_by_a_later_batch():
    kvk = pd.DataFrame({"batch_id": ["kvk_1"], "status": ["success"],
                        "ingested_at": pd.to_datetime(["2026-10-03"], utc=True)})
    resolve = m.make_resolver({"kvk": kvk})
    assert resolve({"source": "kvk", "quarantined_at": "2026-10-01T00:00:00+00:00"}) == \
        "resolved: kvk_1 ingested later"
    assert resolve({"source": "kvk", "quarantined_at": "2026-10-04T00:00:00+00:00"}) is None
    assert resolve({"source": "reference", "quarantined_at": "2026-10-01T00:00:00+00:00"}) is None


# =========================================================================== end to end
def test_quarantined_period_is_cleaned_up_after_cbs_fixes_it(monkeypatch):
    """Bad batch -> quarantine; CBS fixes it -> ingested; cleanup removes the entry."""
    state = {"value": "5,4"}
    monkeypatch.setattr(b, "ENDPOINTS", [ABSENCE])
    monkeypatch.setattr(b, "make_session", lambda: None)

    def fetch_all(session, url, params):
        if url.endswith("/Perioden"):
            return periods("2026KW01"), 1
        if url.endswith("/DataProperties"):
            return DP_80072, 1
        if url.endswith("/TableInfos"):
            return [table_info()], 1
        return records("2026KW01", state["value"]), 1

    monkeypatch.setattr(b, "fetch_all", fetch_all)
    b.run(None, False, False)
    assert len(list_entries(m.QUARANTINE_ROOT)) == 1
    assert m.clean_quarantine() == []              # not resolved, not old: stays

    state["value"] = 5.4
    b.run(None, False, False)
    removed = m.clean_quarantine()
    assert len(removed) == 1 and removed[0][1].startswith("resolved: 80072NED/TypedDataSet")
    assert list_entries(m.QUARANTINE_ROOT) == []



# =========================================================================== KVK retention
def kvk_batches(days):
    """KVK bronze with one tiny batch per ingest day, all logged as success."""
    import pyarrow as pa
    from deltalake import write_deltalake
    for i, day in enumerate(days):
        ts = pd.Timestamp(day, tz="UTC")
        write_deltalake(str(m.KVK_ROOT / "basis_bedrijfsgegevens"), pa.table({
            "Actief": ["J", "N"], "_batch_id": [f"b{i}"] * 2,
            "_ingested_at": pa.array([ts] * 2, pa.timestamp("us", tz="UTC"))}), mode="append")
        write_deltalake(str(m.KVK_ROOT / "_batch_log"), pa.table({
            "batch_id": [f"b{i}"], "status": ["success"], "file_sha256": [f"sha{i}"],
            "ingested_at": pa.array([ts], pa.timestamp("us", tz="UTC"))}), mode="append")


def test_kvk_retention_keeps_newest_and_first_of_each_month():
    from deltalake import DeltaTable
    kvk_batches(["2026-08-03", "2026-08-20", "2026-09-01", "2026-09-15", "2026-10-02"])
    assert m.kvk_retention(dry_run=True) == ["b1", "b3"]
    assert len(DeltaTable(str(m.KVK_ROOT / "basis_bedrijfsgegevens")).to_pandas()) == 10

    assert m.kvk_retention() == ["b1", "b3"]
    rows = DeltaTable(str(m.KVK_ROOT / "basis_bedrijfsgegevens")).to_pandas()
    assert sorted(rows["_batch_id"].unique()) == ["b0", "b2", "b4"]
    log = DeltaTable(str(m.KVK_ROOT / "_batch_log")).to_pandas().set_index("batch_id")
    assert log.loc["b1", "status"] == "retired" and log.loc["b4", "status"] == "success"
    assert m.kvk_retention() == []                   # nothing left to do


def test_kvk_retention_keeps_the_newest_even_when_it_is_not_first_in_its_month():
    kvk_batches(["2026-10-01", "2026-10-20"])
    assert m.kvk_retention(dry_run=True) == []


def test_kvk_retention_without_kvk_data():
    assert m.kvk_retention() == []


# =========================================================================== Delta maintenance
def test_optimize_and_vacuum_removes_files_old_versions_no_longer_need(monkeypatch):
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    monkeypatch.setitem(m.settings, "delta", {"vacuum_retention_hours": 0})
    path = m.LAKEHOUSE / "silver" / "t"
    for i in range(3):                               # three overwrites: two stale files
        write_deltalake(str(path), pa.table({"v": [i]}), mode="overwrite")
    assert m.optimize_and_vacuum(dry_run=True) == {"silver/t": 2}
    assert m.optimize_and_vacuum() == {"silver/t": 2}
    assert DeltaTable(str(path)).to_pandas()["v"].tolist() == [2]   # current data intact
    assert m.optimize_and_vacuum(dry_run=True) == {"silver/t": 0}


def test_delta_tables_skips_staging():
    import pyarrow as pa
    from deltalake import write_deltalake
    write_deltalake(str(m.LAKEHOUSE / "silver" / "a"), pa.table({"v": [1]}))
    write_deltalake(str(m.LAKEHOUSE / "silver" / "_staging" / "run" / "b"), pa.table({"v": [1]}))
    assert [p.name for p in m.delta_tables()] == ["a"]
