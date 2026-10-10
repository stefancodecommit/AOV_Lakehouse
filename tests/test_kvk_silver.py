"""Silver KVK: kvk_company from bronze KVK and the SBI crosswalk."""
import json

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from tools import inspect_lakehouse
from pipeline.silver import kvk as k
from pipeline.common import publish
from pipeline.common import runlog
from checks import layers

# A small crosswalk with each case (real codes from v2.0 where possible):
#   01110, 64210 (2025) <- one 2008 code           1:1
#   66300, 46900, 47741: in both lists, so the number alone doesn't say the version
#     (candidates: the code itself + its 2008 sources; same division 66 / 47, or not: 46+52)
#   0111, 6420: only in the 2008 list                already SBI 2008
CROSSWALK = pd.DataFrame([
    ("0111", "01110"), ("66300", "66300"), ("66301", "66300"),
    ("46900", "46900"), ("52290", "46900"), ("47741", "47741"), ("47742", "47741"),
    ("6420", "64210")], columns=["sbi2008_code", "sbi2025_code"])


def classify(*values):
    return k.classify_main_activities(list(values), CROSSWALK).set_index("main_activity_codes")


# =========================================================================== SBI mapping
@pytest.mark.parametrize("value, version, mapping, candidates, code, division, section", [
    ("01110", "2025", "1:1", 1, "0111", "01", "A"),
    ("64210", "2025", "1:1", 1, "6420", "64", "K"),
    ("66300", "both", "ambiguous_version", 2, None, "66", "K"),
    ("46900", "both", "ambiguous_version", 2, None, None, None),     # 46 and 52
    ("6420", "2008", "sbi2008", 1, "6420", "64", "K"),
    ("47741", "both", "ambiguous_version", 2, None, "47", "G"),
    ("0001", "unknown", "unknown_code", 0, None, None, None),
])
def test_single_main_code(value, version, mapping, candidates, code, division, section):
    r = classify(value).loc[value]
    assert (r.sbi_version, r.sbi_mapping, r.sbi2008_candidates) == (version, mapping, candidates)
    assert (r.sbi2008_code, r.sbi2008_division, r.sbi2008_section) == (code, division, section)
    assert not r.has_multiple_main_activities and r.main_activity_count == 1


def test_merge_into_one_2025_code():
    crosswalk = pd.DataFrame([("66220", "66300"), ("66290", "66300")],
                             columns=["sbi2008_code", "sbi2025_code"])
    r = k.classify_main_activities(["66300"], crosswalk).iloc[0]
    assert (r.sbi_version, r.sbi_mapping, r.sbi2008_candidates) == ("2025", "merge", 2)
    assert r.sbi2008_code is None and r.sbi2008_division == "66"   # both sources share 66


def test_six_digit_kvk_subcode_is_flagged_not_truncated():
    r = classify("620102").loc["620102"]
    assert r.is_kvk_subcode and r.sbi_mapping == "unknown_code" and r.sbi2008_division is None


def test_several_main_codes():
    same = classify("01110,0111").loc["01110,0111"]
    assert same.has_multiple_main_activities and same.main_activity_count == 2
    assert same.sbi_version == "mixed" and same.sbi2008_code == "0111"   # one candidate
    differ = classify("01110,6420").loc["01110,6420"]
    assert differ.sbi2008_candidates == 2 and differ.sbi2008_division is None
    unknown = classify("01110,0001").loc["01110,0001"]
    assert unknown.sbi_mapping == "unknown_code" and unknown.sbi2008_section is None


def test_division_section_table_covers_every_sbi2008_division():
    assert k.DIVISION_SECTION["47"] == "G" and k.DIVISION_SECTION["99"] == "U"
    assert len(k.DIVISION_SECTION) == 88 and "04" not in k.DIVISION_SECTION


# =========================================================================== kvk_company
COLUMNS = ["Datum_aanvang", "Actief", "Insolventie", "Rechtsvorm", "Postcode_regio",
           "SBI_activiteiten", "Hoofdactiviteiten", "Lidstaat"]
ROWS = [
    ["19720516", "J", "", "BV", "89", "64210,68203", "64210", "NL"],
    ["20060721", "N", "FAIL", "NV", "", "47741", "47741", "NL"],
    ["2006x721", "X", "", "BV", "92", "", "", "NL"],
]


def bronze(batches=(("kvk_b1", "2026-10-01"),), rows=ROWS):
    frames = []
    for batch_id, day in batches:
        df = pd.DataFrame(rows, columns=COLUMNS)
        df["_batch_id"] = batch_id
        df["_ingested_at"] = pd.Timestamp(day, tz="UTC")
        df["_row_number"] = range(1, len(df) + 1)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def test_kvk_company_types_and_flags():
    df = k.build_kvk_company(bronze(), CROSSWALK)
    first, second, third = df.iloc[0], df.iloc[1], df.iloc[2]
    assert str(first.start_date) == "1972-05-16" and not first.has_invalid_start_date
    assert third.start_date is None and third.has_invalid_start_date     # "2006x721"
    assert [first.is_active, second.is_active] == [True, False] and pd.isna(third.is_active)
    assert pd.isna(first.insolvency_code) and second.insolvency_code == "FAIL"
    assert pd.isna(second.postcode_region)                                # "" -> null
    assert first.sbi2008_code == "6420" and first.sbi2008_section == "K"
    assert second.sbi_mapping == "ambiguous_version"
    assert third.has_no_main_activity and third.sbi_mapping == "none"
    assert third.main_activity_count == 0 and third.sbi2008_candidates == 0
    assert list(df.source_row_number) == [1, 2, 3]


def test_is_latest_marks_the_newest_batch_only():
    df = k.build_kvk_company(bronze((("kvk_old", "2026-09-01"), ("kvk_new", "2026-10-01"))),
                             CROSSWALK)
    assert df.groupby("batch_id").is_latest.all().to_dict() == {"kvk_new": True, "kvk_old": False}
    assert str(df.loc[df.is_latest, "snapshot_date"].iloc[0]) == "2026-10-01"


# =========================================================================== end to end on Delta
@pytest.fixture
def kvk_lakehouse():
    """Bronze KVK (one logged batch, one orphan) and the silver crosswalk, as Delta."""
    df = bronze((("kvk_b1", "2026-10-01"), ("kvk_orphan", "2026-10-02")))
    write_deltalake(str(k.KVK_BRONZE / "basis_bedrijfsgegevens"), pa.Table.from_pandas(df))
    write_deltalake(str(k.KVK_BRONZE / "_batch_log"), pa.table({
        "batch_id": ["kvk_b1"], "status": ["success"],
        "ingested_at": pa.array([pd.Timestamp("2026-10-01", tz="UTC")],
                                pa.timestamp("us", tz="UTC"))}))
    publish.write_table(k.SILVER_ROOT / "map_sbi2008_sbi2025", CROSSWALK)


def test_run_publishes_kvk_company_with_run_id_and_log(kvk_lakehouse):
    run_id = k.run()
    df = DeltaTable(str(k.SILVER_ROOT / "kvk_company")).to_pandas()
    assert len(df) == 3 and set(df.batch_id) == {"kvk_b1"}         # orphan batch left out
    assert set(df.run_id) == {run_id}
    row = runlog.read_runs(k.RUN_LOG).iloc[-1]
    assert row.pipeline == "silver_kvk" and row.status == "success"
    assert json.loads(row.tables) == {"kvk_company": {
        "rows": 3, "status": "published", "mode": "full", "reason": "no kvk_company table yet",
        "added": ["kvk_b1"], "removed": []}}
    assert set(json.loads(row.inputs)) == {"bronze/kvk/_batch_log",
                                          "bronze/kvk/basis_bedrijfsgegevens",
                                          "silver/map_sbi2008_sbi2025",
                                          "crosswalk_fingerprint", "logic_version"}
    assert inspect_lakehouse.silver_run_problems(k.LAKEHOUSE, "silver_kvk")[1] == []


def test_types_after_reading_back(kvk_lakehouse):
    k.run()
    schema = {f.name: f.type for f in pa.schema(
        DeltaTable(str(k.SILVER_ROOT / "kvk_company")).schema().to_arrow())}
    assert schema["start_date"] == pa.date32() and schema["snapshot_date"] == pa.date32()
    assert schema["is_active"] == pa.bool_() and schema["is_latest"] == pa.bool_()
    assert schema["main_activity_count"] == pa.int64()
    assert schema["sbi2008_section"] == pa.string()


def test_missing_bronze_fails_the_run_and_is_logged():
    with pytest.raises(publish.RunFailed, match="no KVK batches in bronze"):
        k.run()
    assert runlog.read_runs(k.RUN_LOG).iloc[-1].status == "failed"


def test_reader_returns_a_fixed_order_and_only_needed_columns(kvk_lakehouse):
    df = k.read_kvk_bronze()
    assert list(df["_row_number"]) == [1, 2, 3]
    assert "SBI_activiteiten" not in df and "_source_file" not in df   # not needed: saves memory
    assert str(df["_batch_id"].dtype) == "category"


def test_categories_are_written_as_plain_text():
    table = publish.to_arrow(pd.DataFrame({"batch": pd.Categorical(["a", "b", "a"])}))
    assert table.schema.field("batch").type == pa.string()


# =========================================================================== incremental runs
def plan(wanted, present, ok=("r1",), fingerprint="fp", last=None, full=False):
    last = {"crosswalk_fingerprint": "fp", "logic_version": k.LOGIC_VERSION} if last is None else last
    return k.plan_sync(list(wanted), present, set(ok), fingerprint, last, full)


def test_plan_full_rebuild_when_a_rule_for_all_snapshots_changed():
    present = {"b1": {"r1"}}
    assert plan(["b1"], {}).reason == "no kvk_company table yet"
    assert plan(["b1"], present, last={}).reason == "the last run is from before incremental updates"
    assert plan(["b1"], present, fingerprint="new").reason == "the SBI crosswalk changed"
    assert plan(["b1"], present, last={"crosswalk_fingerprint": "fp", "logic_version": 0}).full
    assert plan(["b1"], present, full=True).reason == "requested (--full)"
    assert k.plan_sync(["b1"], present, {"r1"}, "fp", None).reason == "no successful run to continue from"
    p = plan(["b1", "b2"], {}, )
    assert p.full and p.add == ["b1", "b2"] and p.latest == "b2"


def test_plan_incremental_adds_new_removes_retired_and_redoes_failed():
    assert plan(["b1"], {"b1": {"r1"}}).reason == "nothing new"
    p = plan(["b1", "b2"], {"b1": {"r1"}})
    assert (p.full, p.add, p.remove, p.latest) == (False, ["b2"], [], "b2")
    p = plan(["b2"], {"b1": {"r1"}, "b2": {"r1"}})                 # b1 retired by maintenance
    assert (p.add, p.remove, p.latest) == ([], ["b1"], "b2")
    p = plan(["b1", "b2"], {"b1": {"r1"}, "b2": {"r_failed"}})      # crashed after writing b2
    assert (p.add, p.remove) == (["b2"], ["b2"])
    p = plan(["b1"], {"b1": {"r1", "r2"}})                          # half old, half new
    assert (p.add, p.remove) == (["b1"], ["b1"])


def upload(batch_id, day, rows=ROWS):
    """A KVK upload as the bronze agent writes it: rows plus a success entry in the log."""
    df = bronze(((batch_id, day),), rows)
    write_deltalake(str(k.KVK_BRONZE / "basis_bedrijfsgegevens"), pa.Table.from_pandas(df), mode="append")
    write_deltalake(str(k.KVK_BRONZE / "_batch_log"), pa.table({
        "batch_id": [batch_id], "status": ["success"],
        "ingested_at": pa.array([pd.Timestamp(day, tz="UTC")], pa.timestamp("us", tz="UTC"))}),
        mode="append")


def silver_kvk():
    return (DeltaTable(str(k.SILVER_ROOT / "kvk_company")).to_pandas()
            .sort_values(["batch_id", "source_row_number"], ignore_index=True))


def same_as_full_rebuild():
    """The incremental table equals a full rebuild (apart from which run made the rows)."""
    incremental = silver_kvk().drop(columns="run_id")
    k.run(full=True)
    pd.testing.assert_frame_equal(incremental, silver_kvk().drop(columns="run_id"))


def last_run():
    row = runlog.read_runs(k.RUN_LOG).iloc[-1]
    return row.status, json.loads(row.tables)["kvk_company"]


@pytest.fixture
def crosswalk():
    publish.write_table(k.SILVER_ROOT / "map_sbi2008_sbi2025", CROSSWALK)


def test_a_new_upload_only_builds_the_new_snapshot(crosswalk, monkeypatch):
    upload("kvk_2026_09", "2026-09-01")
    first = k.run()
    upload("kvk_2026_10", "2026-10-01", rows=ROWS[:2])
    reads = []
    real = k.read_kvk_bronze
    monkeypatch.setattr(k, "read_kvk_bronze", lambda versions=None, batches=None:
                        reads.append(batches) or real(versions, batches))
    second = k.run()
    assert reads == [["kvk_2026_10"]]                        # memory: one snapshot read
    _, info = last_run()
    assert (info["mode"], info["added"], info["rows"]) == ("incremental", ["kvk_2026_10"], 5)
    df = silver_kvk()
    assert df.groupby("batch_id").is_latest.all().to_dict() == {"kvk_2026_09": False,
                                                                "kvk_2026_10": True}
    assert df.groupby("batch_id").run_id.unique().map(list).to_dict() == {
        "kvk_2026_09": [first], "kvk_2026_10": [second]}     # old rows keep their run
    assert inspect_lakehouse.silver_run_problems(k.LAKEHOUSE, "silver_kvk")[1] == []
    assert [r.passed for r in layers.check_silver_kvk(df)] == [True] * 4
    same_as_full_rebuild()


def test_nothing_new_changes_nothing(crosswalk):
    upload("kvk_2026_09", "2026-09-01")
    k.run()
    version = DeltaTable(str(k.SILVER_ROOT / "kvk_company")).version()
    k.run()
    assert last_run()[1]["status"] == "unchanged"
    assert DeltaTable(str(k.SILVER_ROOT / "kvk_company")).version() == version


def test_a_retired_batch_is_removed(crosswalk):
    upload("kvk_2026_08", "2026-08-01")
    upload("kvk_2026_09", "2026-09-01")
    k.run()
    DeltaTable(str(k.KVK_BRONZE / "_batch_log")).update(
        new_values={"status": "retired"}, predicate="batch_id = 'kvk_2026_08'")
    k.run()
    assert last_run()[1]["removed"] == ["kvk_2026_08"]
    assert set(silver_kvk().batch_id) == {"kvk_2026_09"}
    same_as_full_rebuild()


def test_a_crash_halfway_is_finished_by_the_next_run(crosswalk, monkeypatch):
    upload("kvk_2026_09", "2026-09-01")
    k.run()
    upload("kvk_2026_10", "2026-10-01")
    def crash(*a, **kw):
        raise RuntimeError("crash after the new snapshot was appended")
    # its own context: monkeypatch.undo() would also undo conftest's isolated lakehouse
    with monkeypatch.context() as m, pytest.raises(RuntimeError):
        m.setattr(DeltaTable, "update", crash)
        k.run()
    # the new rows are there, but from a failed run, and the flag hasn't moved yet
    assert last_run()[0] == "failed"
    assert silver_kvk().groupby("batch_id").is_latest.all().to_dict() == {
        "kvk_2026_09": True, "kvk_2026_10": False}
    assert inspect_lakehouse.silver_run_problems(k.LAKEHOUSE, "silver_kvk")[1]
    k.run()
    _, info = last_run()
    assert (info["added"], info["removed"]) == (["kvk_2026_10"], ["kvk_2026_10"])
    assert inspect_lakehouse.silver_run_problems(k.LAKEHOUSE, "silver_kvk")[1] == []
    same_as_full_rebuild()


def test_a_changed_crosswalk_rebuilds_every_snapshot(crosswalk):
    upload("kvk_2026_09", "2026-09-01")
    upload("kvk_2026_10", "2026-10-01")
    k.run()
    publish.write_table(k.SILVER_ROOT / "map_sbi2008_sbi2025", CROSSWALK)   # same content
    k.run()
    assert last_run()[1]["status"] == "unchanged"
    publish.write_table(k.SILVER_ROOT / "map_sbi2008_sbi2025", CROSSWALK.iloc[1:])
    k.run()
    _, info = last_run()
    assert (info["mode"], info["reason"]) == ("full", "the SBI crosswalk changed")
    assert info["added"] == ["kvk_2026_09", "kvk_2026_10"]
    assert not (k.SILVER_ROOT / "_staging").exists()


def test_check_one_run_per_snapshot():
    df = pd.DataFrame({"batch_id": ["a", "a", "b"], "run_id": ["r1", "r2", "r1"]})
    r = layers.one_run_per_snapshot(df)
    assert not r.passed and r.examples == ["a"]
    assert layers.one_run_per_snapshot(df.iloc[1:]).passed


def test_present_snapshots_are_read_from_the_delta_log_not_the_data(crosswalk, monkeypatch):
    upload("kvk_2026_09", "2026-09-01")
    upload("kvk_2026_10", "2026-10-01")
    run_id = k.run()
    import pyarrow.parquet as pq
    def no_data(*a, **kw):
        raise AssertionError("table_batches read data files")
    with monkeypatch.context() as m:
        m.setattr(pq, "read_table", no_data)
        found = k.table_batches(k.SILVER_ROOT / "kvk_company")
    assert found == {"kvk_2026_09": {run_id}, "kvk_2026_10": {run_id}}
    assert (k.SILVER_ROOT / "kvk_company" / "batch_id=kvk_2026_10").is_dir()   # one folder each


def test_present_snapshots_of_a_file_with_several_batches_are_read_from_the_file():
    """An older, unpartitioned table with two snapshots in one file."""
    write_deltalake(str(k.SILVER_ROOT / "kvk_company"), pa.table({
        "batch_id": ["b1", "b1", "b2"], "run_id": ["r1", "r1", "r2"]}))
    assert k.table_batches(k.SILVER_ROOT / "kvk_company") == {"b1": {"r1"}, "b2": {"r2"}}


def test_a_full_rebuild_writes_the_newest_snapshot_first_and_is_finished_after_a_crash(
        crosswalk, monkeypatch):
    for month in ("08", "09", "10"):
        upload(f"kvk_2026_{month}", f"2026-{month}-01")
    k.run()
    written = []
    real = k._write_snapshot
    def crash_after_first(path, batch_id, *a, **kw):
        if written:
            raise RuntimeError("stopped after the first snapshot")
        written.append(batch_id)
        return real(path, batch_id, *a, **kw)
    with monkeypatch.context() as m, pytest.raises(RuntimeError):
        m.setattr(k, "_write_snapshot", crash_after_first)
        k.run(full=True)
    # stopped halfway: only the newest snapshot, flagged latest (gold reads only that one)
    assert written == ["kvk_2026_10"]
    df = silver_kvk()
    assert set(df.batch_id) == {"kvk_2026_10"} and df.is_latest.all()
    k.run()                                     # finishes: rebuilds what is missing or failed
    assert set(silver_kvk().batch_id) == {"kvk_2026_08", "kvk_2026_09", "kvk_2026_10"}
    assert inspect_lakehouse.silver_run_problems(k.LAKEHOUSE, "silver_kvk")[1] == []
    same_as_full_rebuild()
