"""Silver KVK: kvk_company from bronze KVK and the SBI crosswalk."""
import json
from datetime import date

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from checks import layers
from pipeline.common import config, paths, publish, runlog
from pipeline.silver import kvk as k
from tools import inspect_lakehouse

# A small crosswalk with each case (real codes from v2.0 where possible):
#   01110, 64210 (2025) <- one 2008 code           1:1
#   66300, 46900, 47741: in both lists, so the number alone doesn't say the version
#     (candidates: the code itself + its 2008 sources; same division 66 / 47, or not: 46+52)
#   0111, 6420: only in the 2008 list                already SBI 2008
CROSSWALK = pd.DataFrame([
    ("0111", "01110"), ("66300", "66300"), ("66301", "66300"),
    ("46900", "46900"), ("52290", "46900"), ("47741", "47741"), ("47742", "47741"),
    ("6420", "64210")], columns=["sbi2008_code", "sbi2025_code"])
# as silver CBS writes it: with the bronze batch the crosswalk came from
SILVER_CROSSWALK = CROSSWALK.assign(batch_id="ref_v20")


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


# =========================================================================== end to end on Delta
@pytest.fixture
def kvk_lakehouse():
    """Bronze KVK (one logged batch, one orphan) and the silver crosswalk, as Delta."""
    df = bronze((("kvk_b1", "2026-10-01"), ("kvk_orphan", "2026-10-02")))
    write_deltalake(str(paths.lakehouse().kvk_bronze / "basis_bedrijfsgegevens"), pa.Table.from_pandas(df))
    write_deltalake(str(paths.lakehouse().kvk_bronze / "_batch_log"), pa.table({
        "batch_id": ["kvk_b1"], "status": ["success"],
        "ingested_at": pa.array([pd.Timestamp("2026-10-01", tz="UTC")],
                                pa.timestamp("us", tz="UTC")),
        "file_date": pa.array([None], pa.date32())}))        # no file date: upload date used
    publish.write_table(paths.lakehouse().silver / "map_sbi2008_sbi2025", SILVER_CROSSWALK)


def test_run_publishes_kvk_company_with_run_id_and_log(kvk_lakehouse):
    run_id = k.run()
    df = DeltaTable(str(paths.lakehouse().silver / "kvk_company")).to_pandas()
    assert len(df) == 3 and set(df.batch_id) == {"kvk_b1"}         # orphan batch left out
    assert set(df.run_id) == {run_id}
    row = runlog.read_runs(paths.lakehouse().run_log).iloc[-1]
    assert row.pipeline == "silver_kvk" and row.status == "success"
    assert json.loads(row.tables) == {"kvk_company": {"rows": 3, "status": "published"}}
    inputs = json.loads(row.inputs)
    assert set(inputs) == {"bronze/kvk/_batch_log", "bronze/kvk/_batch_log#batch_id",
                           "bronze/kvk/basis_bedrijfsgegevens", "silver/map_sbi2008_sbi2025",
                           "silver/map_sbi2008_sbi2025#batch_id"}
    assert inputs["bronze/kvk/_batch_log#batch_id"] == "kvk_b1"
    assert inputs["silver/map_sbi2008_sbi2025#batch_id"] == "ref_v20"
    assert inspect_lakehouse.silver_run_problems(paths.lakehouse().root, "silver_kvk")[1] == []


def test_types_after_reading_back(kvk_lakehouse):
    k.run()
    schema = {f.name: f.type for f in pa.schema(
        DeltaTable(str(paths.lakehouse().silver / "kvk_company")).schema().to_arrow())}
    assert schema["start_date"] == pa.date32() and schema["snapshot_date"] == pa.date32()
    assert schema["is_active"] == pa.bool_() and "is_latest" not in schema
    assert schema["main_activity_count"] == pa.int64()
    assert schema["sbi2008_section"] == pa.string()


def test_missing_bronze_fails_the_run_and_is_logged():
    with pytest.raises(publish.RunFailed, match="no KVK batches in bronze"):
        k.run()
    assert runlog.read_runs(paths.lakehouse().run_log).iloc[-1].status == "failed"


def test_reader_returns_a_fixed_order_and_only_needed_columns(kvk_lakehouse):
    df = k.read_kvk_bronze()
    assert list(df["_row_number"]) == [1, 2, 3]
    assert "SBI_activiteiten" not in df and "_source_file" not in df   # not needed: saves memory
    assert str(df["_batch_id"].dtype) == "category"


def test_categories_are_written_as_plain_text():
    table = publish.to_arrow(pd.DataFrame({"batch": pd.Categorical(["a", "b", "a"])}))
    assert table.schema.field("batch").type == pa.string()


# =========================================================================== only the newest upload
def upload(batch_id, day, rows=ROWS, file_date=None):
    """A KVK upload as the bronze agent writes it: rows plus a success entry in the log.
    `file_date`: KVK's export date; None = no file date (the upload day is used)."""
    df = bronze(((batch_id, day),), rows)
    write_deltalake(str(paths.lakehouse().kvk_bronze / "basis_bedrijfsgegevens"), pa.Table.from_pandas(df), mode="append")
    write_deltalake(str(paths.lakehouse().kvk_bronze / "_batch_log"), pa.table({
        "batch_id": [batch_id], "status": ["success"],
        "ingested_at": pa.array([pd.Timestamp(day, tz="UTC")], pa.timestamp("us", tz="UTC")),
        "file_date": pa.array([file_date], pa.date32())}), mode="append")


def silver_kvk():
    return (DeltaTable(str(paths.lakehouse().silver / "kvk_company")).to_pandas()
            .sort_values(["batch_id", "source_row_number"], ignore_index=True))


@pytest.fixture
def crosswalk():
    publish.write_table(paths.lakehouse().silver / "map_sbi2008_sbi2025", SILVER_CROSSWALK)


def test_a_run_builds_only_the_newest_upload(crosswalk, monkeypatch, tmp_path_factory):
    for month in ("08", "09"):
        upload(f"kvk_2026_{month}", f"2026-{month}-01")
    upload("kvk_2026_10", "2026-10-01", rows=ROWS[:2])
    reads = []
    real = k.read_kvk_bronze
    monkeypatch.setattr(k, "read_kvk_bronze", lambda versions=None, batches=None, root=None:
                        reads.append(batches) or real(versions, batches, root))
    run_id = k.run()
    assert reads == [["kvk_2026_10"]]                     # memory: one snapshot read
    df = silver_kvk()
    assert set(df.batch_id) == {"kvk_2026_10"} and len(df) == 2 and set(df.run_id) == {run_id}
    assert all(r.passed for r in layers.check_silver_kvk(df))
    assert inspect_lakehouse.silver_run_problems(paths.lakehouse().root, "silver_kvk")[1] == []

    # run(lakehouse=...): a copy with a newer upload of its own is read and written; the
    # configured lakehouse isn't
    import shutil
    copy = tmp_path_factory.mktemp("copy") / "lakehouse"
    shutil.copytree(paths.lakehouse().root, copy)
    assert k.unchanged_since(copy) == run_id              # the copy's log, batch and table
    # another crosswalk batch in the copy only: the copy must be rebuilt, the original not
    # (so unchanged_since reads the crosswalk of the lakehouse it is given)
    publish.write_table(copy / "silver" / "map_sbi2008_sbi2025",
                        SILVER_CROSSWALK.assign(batch_id="ref_v21", run_id="silver_cbs_2"))
    assert k.unchanged_since(copy) is None and k.unchanged_since() == run_id
    with monkeypatch.context() as m:              # upload() writes to the configured lakehouse
        m.setitem(config.settings["paths"], "lakehouse", str(copy))
        upload("kvk_2026_11", "2026-11-01", rows=ROWS[:1])
    assert k.unchanged_since(copy) is None and k.unchanged_since() == run_id
    copy_run = k.run(lakehouse=copy)
    assert reads[-1] == ["kvk_2026_11"]                    # the copy's newest upload
    assert list(runlog.read_runs(paths.lakehouse().run_log).run_id) == [run_id]              # untouched
    assert set(silver_kvk().run_id) == {run_id}
    assert list(runlog.read_runs(copy / "_run_log").run_id) == [run_id, copy_run]
    copy_df = DeltaTable(str(copy / "silver" / "kvk_company")).to_pandas()
    assert set(copy_df.batch_id) == {"kvk_2026_11"} and set(copy_df.run_id) == {copy_run}
    assert k.unchanged_since(copy) == copy_run


def test_a_new_upload_replaces_the_snapshot(crosswalk):
    upload("kvk_2026_09", "2026-09-01")
    k.run()
    upload("kvk_2026_10", "2026-10-01", rows=ROWS[:2])
    second = k.run()
    df = silver_kvk()
    assert set(df.batch_id) == {"kvk_2026_10"} and set(df.run_id) == {second}


def test_an_older_file_loaded_later_does_not_replace_the_newest_snapshot(crosswalk):
    # November's file is loaded first; October's, found later in the inbox, after it
    upload("kvk_nov", "2026-11-13", rows=ROWS[:2], file_date=date(2026, 11, 12))
    upload("kvk_oct", "2026-11-20", file_date=date(2026, 10, 1))
    k.run()
    assert set(silver_kvk().batch_id) == {"kvk_nov"}


def test_batches_that_did_not_succeed_are_not_used(crosswalk):
    upload("kvk_2026_09", "2026-09-01")
    upload("kvk_2026_10", "2026-10-01")
    DeltaTable(str(paths.lakehouse().kvk_bronze / "_batch_log")).update(
        new_values={"status": "retired"}, predicate="batch_id = 'kvk_2026_10'")
    k.run()
    assert set(silver_kvk().batch_id) == {"kvk_2026_09"}


def test_a_failed_publish_leaves_the_live_table_untouched(crosswalk, monkeypatch):
    upload("kvk_2026_09", "2026-09-01")
    first = k.run()
    upload("kvk_2026_10", "2026-10-01")
    real = publish._overwrite

    def power_cut(path, data):
        if "_staging" not in str(path):           # fail only the write to the live table
            raise OSError("power cut")
        real(path, data)

    with monkeypatch.context() as m, pytest.raises(OSError):
        m.setattr(publish, "_overwrite", power_cut)
        k.run()
    assert runlog.read_runs(paths.lakehouse().run_log).iloc[-1].status == "failed"
    df = silver_kvk()
    assert set(df.batch_id) == {"kvk_2026_09"} and set(df.run_id) == {first}
    assert not (paths.lakehouse().silver / "_staging").exists()
    k.run()                                        # a clean rerun publishes the new upload
    assert set(silver_kvk().batch_id) == {"kvk_2026_10"}


# =========================================================================== skip when nothing changed
def kvk_version():
    return DeltaTable(str(paths.lakehouse().silver / "kvk_company")).version()


def test_a_run_with_the_same_inputs_rebuilds_nothing(crosswalk):
    """A new CBS quarter rebuilds silver CBS, and Dagster then asks for silver KVK too;
    with the same upload, crosswalk and code there is nothing to rebuild."""
    upload("kvk_2026_10", "2026-10-01")
    first = k.run()
    version, runs = kvk_version(), len(runlog.read_runs(paths.lakehouse().run_log))
    # silver CBS rewrites the crosswalk (a new Delta version) with the same content
    publish.write_table(paths.lakehouse().silver / "map_sbi2008_sbi2025", SILVER_CROSSWALK)
    assert k.run() == first
    assert kvk_version() == version                          # nothing written
    assert len(runlog.read_runs(paths.lakehouse().run_log)) == runs          # and no run logged
    assert inspect_lakehouse.silver_run_problems(paths.lakehouse().root, "silver_kvk")[1] == []


def test_force_rebuilds_anyway(crosswalk):
    upload("kvk_2026_10", "2026-10-01")
    first = k.run()
    assert k.run(force=True) != first
    assert set(silver_kvk().run_id) != {first}


def test_a_new_crosswalk_batch_rebuilds(crosswalk):
    upload("kvk_2026_10", "2026-10-01")
    first = k.run()
    publish.write_table(paths.lakehouse().silver / "map_sbi2008_sbi2025",
                        SILVER_CROSSWALK.assign(batch_id="ref_v21"))
    assert k.run() != first


@pytest.mark.parametrize("now", ["def5678",            # other code
                                 "abc1234-dirty",      # uncommitted changes
                                 "unknown",            # Docker image built without the version
                                 None])                # no git, no CODE_VERSION
def test_changed_or_unknown_code_rebuilds(crosswalk, monkeypatch, now):
    upload("kvk_2026_10", "2026-10-01")
    first = k.run()
    monkeypatch.setattr(runlog, "code_version", lambda: now)
    assert k.run() != first


def test_a_table_that_no_longer_holds_the_run_is_rebuilt(crosswalk):
    upload("kvk_2026_10", "2026-10-01")
    first = k.run()
    publish.write_table(paths.lakehouse().silver / "kvk_company", silver_kvk().assign(run_id="other"))
    second = k.run()
    assert second != first and set(silver_kvk().run_id) == {second}


def test_a_run_logged_without_the_crosswalk_batch_is_rebuilt_once(crosswalk):
    """Runs logged before 2026-11-26 don't say which crosswalk batch they read."""
    upload("kvk_2026_10", "2026-10-01")
    first = k.run()
    runs = runlog.read_runs(paths.lakehouse().run_log)
    inputs = json.loads(runs.iloc[-1]["inputs"])
    del inputs[k.CROSSWALK_BATCH]
    runs.loc[runs.index[-1], "inputs"] = json.dumps(inputs)
    write_deltalake(str(paths.lakehouse().run_log), pa.Table.from_pandas(runs, schema=runlog.schema(),
                                                         preserve_index=False), mode="overwrite")
    second = k.run()
    assert second != first
    assert k.run() == second                               # from then on: skipped


def test_an_older_partitioned_table_is_replaced_by_an_unpartitioned_one(crosswalk):
    """Until 2026-11 kvk_company kept every snapshot, partitioned by batch_id."""
    write_deltalake(str(paths.lakehouse().silver / "kvk_company"), pa.table({
        "batch_id": ["old1", "old2"], "is_latest": [False, True], "run_id": ["r1", "r2"]}),
        partition_by=["batch_id"])
    upload("kvk_2026_10", "2026-10-01")
    k.run()
    dt = DeltaTable(str(paths.lakehouse().silver / "kvk_company"))
    assert dt.metadata().partition_columns == []
    assert set(silver_kvk().batch_id) == {"kvk_2026_10"} and "is_latest" not in silver_kvk()


def test_check_one_snapshot():
    df = pd.DataFrame({"batch_id": ["a", "a", "b"], "run_id": ["r1", "r1", "r1"]})
    r = layers.one_snapshot(df)
    assert not r.passed and r.examples == ["a", "b"]
    assert layers.one_snapshot(df.iloc[:2]).passed


def test_snapshot_date_is_kvks_file_date_with_its_source():
    b = bronze(batches=(("kvk_b1", "2026-10-05"), ("kvk_b2", "2026-11-04")))
    # b1's file date is known (2026-10-01), b2's isn't (a loose CSV): upload day, flagged
    b["_file_date"] = b["_batch_id"].map({"kvk_b1": date(2026, 10, 1)}).astype(object)
    df = k.build_kvk_company(b, CROSSWALK).drop_duplicates("batch_id").set_index("batch_id")
    assert str(df.loc["kvk_b1", "snapshot_date"]) == "2026-10-01"
    assert df.loc["kvk_b1", "snapshot_date_source"] == "kvk_file"
    assert str(df.loc["kvk_b2", "snapshot_date"]) == "2026-11-04"
    assert df.loc["kvk_b2", "snapshot_date_source"] == "upload"
    # bronze rows without the column at all: the upload day everywhere
    assert set(k.build_kvk_company(bronze(), CROSSWALK)["snapshot_date_source"]) == {"upload"}


# =========================================================================== memory: categories
def test_categories_from_bronze_give_the_same_table_as_plain_text():
    """read_kvk_bronze reads text as categories (memory); the result must not depend on it."""
    plain = bronze()
    as_read = plain.astype({c: "category" for c in COLUMNS})
    a = publish.to_arrow(k.build_kvk_company(plain, CROSSWALK))
    b = publish.to_arrow(k.build_kvk_company(as_read, CROSSWALK))
    assert a.equals(b)


def test_text_columns_stay_categories():
    """One value per distinct value, not per row: the cleaned columns and the SBI columns
    of 1.9M companies have 1 to ~8,400 distinct values each (memory, NFR-7)."""
    df = k.build_kvk_company(bronze(), CROSSWALK)
    for c in ("legal_form", "postcode_region", "insolvency_code", "main_activity_codes",
              "sbi_mapping", "sbi_version", "sbi2008_code", "sbi2008_section"):
        assert str(df[c].dtype) == "category", c


def test_a_file_without_main_activities_marks_every_company_as_without_one():
    """The main-activity column is optional (config.yaml); without it there is nothing
    to classify. Until 2026-11-26 this stopped the run (an empty classification)."""
    df = k.build_kvk_company(bronze().drop(columns="Hoofdactiviteiten"), CROSSWALK)
    assert df["has_no_main_activity"].all() and set(df["sbi_mapping"]) == {"none"}
    assert (df["main_activity_count"] == 0).all() and df["sbi2008_section"].isna().all()
