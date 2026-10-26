"""Bronze KVK: verification of an uploaded file. Offline: the batch log points at tmp_path."""
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from pipeline.bronze import kvk_agent as k

# Header and rows as in the real KVK file of 2026-10-01
REAL_HEADER = ('"Datum aanvang";"Actief";"Insolventie";"Rechtsvorm";"Postcode regio";'
               '"SBI activiteiten";"Hoofdactiviteiten";"Lidstaat"')
REAL_ROWS = [
    '"19720516";"J";"";"BV";"89";"64210,68203";"64210";"NL"',
    '"20060721";"J";"";"BV";"92";"47741,47742,47789,68203";"47741";"NL"',
    '"19540329";"N";"FAIL";"NV";"79";"6420";"6420";"NL"',
]


@pytest.fixture
def kvk_zip(tmp_path, monkeypatch):
    monkeypatch.setattr(k, "LOG_TABLE", tmp_path / "_batch_log")
    path = tmp_path / "kvk.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("kvk.csv", "\r\n".join([REAL_HEADER, *REAL_ROWS]) + "\r\n")
    return path


def test_verify_recognises_every_column_of_the_real_header(kvk_zip):
    report = json.loads(k.verify_dataset(str(kvk_zip)))
    assert report["status"] == "PASSED", report["errors"]
    assert report["file"] == kvk_zip.name             # no folder: it holds a username (R10)
    assert report["row_count"] == 3
    assert report["warnings"] == []
    assert report["recognised_columns"] == {
        "start_date": "Datum aanvang", "active": "Actief", "legal_form": "Rechtsvorm",
        "postcode_region": "Postcode regio", "insolvency": "Insolventie",
        "sbi": "SBI activiteiten", "main_activity": "Hoofdactiviteiten",
        "member_state": "Lidstaat",
    }


def test_rejected_file_is_quarantined_with_its_report(tmp_path):
    bad = tmp_path / "kvk.csv"
    bad.write_text("just;one\n1;2\n")                 # required columns missing
    report = json.loads(k.verify_dataset(str(bad)))
    assert report["status"] == "FAILED"
    folder = Path(report["quarantine"])
    assert folder.parent == k.QUARANTINE_ROOT / "kvk"
    assert (folder / "kvk.csv").read_bytes() == bad.read_bytes()
    saved = json.loads((folder / "report.json").read_text())
    assert saved["status"] == "FAILED" and any("Required column missing" in e for e in saved["errors"])
    # verifying the same file again doesn't add a second copy
    assert json.loads(k.verify_dataset(str(bad)))["quarantine"] == "already in quarantine"


def test_passed_file_is_not_quarantined(kvk_zip):
    report = json.loads(k.verify_dataset(str(kvk_zip)))
    assert report["status"] == "PASSED" and "quarantine" not in report
    assert not k.QUARANTINE_ROOT.exists()


# =========================================================================== inbox
def inbox_zip(name, rows=REAL_ROWS, age=0):
    import os
    k.INBOX.mkdir(parents=True, exist_ok=True)
    path = k.INBOX / name
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("kvk.csv", "\r\n".join([REAL_HEADER, *rows]) + "\r\n")
    os.utime(path, (1_790_000_000 - age, 1_790_000_000 - age))
    return path


def test_empty_or_missing_inbox():
    listing = json.loads(k.list_inbox())
    assert listing["files"] == [] and listing["new_files"] == []
    assert "get_dataset_info" in listing["next_step"]
    assert k.newest_new_inbox_file() is None


def test_list_inbox_tells_the_agent_which_file_is_new():
    inbox_zip("kvk.zip")
    listing = json.loads(k.list_inbox())
    assert listing["new_files"] == ["kvk.zip"] and "verify_dataset" in listing["next_step"]


def test_inbox_lists_newest_first_and_marks_ingested_files(monkeypatch):
    old = inbox_zip("old.zip", age=86_400)
    inbox_zip("new.zip", rows=REAL_ROWS[:2])
    (k.INBOX / "notes.txt").write_text("ignored")
    monkeypatch.setattr(k, "ingested_hashes", lambda: {k._sha256(old): "kvk_batch_1"})
    files = json.loads(k.list_inbox())["files"]
    assert [f["file"] for f in files] == ["new.zip", "old.zip"]
    assert [f["already_ingested_as"] for f in files] == [None, "kvk_batch_1"]


def test_newest_new_file_skips_ingested_ones(monkeypatch):
    inbox_zip("old.zip", age=86_400)
    new = inbox_zip("new.zip", rows=REAL_ROWS[:2])
    assert k.newest_new_inbox_file() == "new.zip"
    monkeypatch.setattr(k, "ingested_hashes", lambda: {k._sha256(new): "kvk_batch_2"})
    assert k.newest_new_inbox_file() == "old.zip"


def test_bare_file_name_is_looked_up_in_the_inbox(tmp_path, monkeypatch):
    path = inbox_zip("kvk.zip")
    monkeypatch.chdir(tmp_path)                    # not the inbox
    assert k._resolve("kvk.zip") == path.resolve()
    assert json.loads(k.verify_dataset("kvk.zip"))["status"] == "PASSED"
    assert k._resolve("'/somewhere/else.zip'") == Path("/somewhere/else.zip")



# =========================================================================== Dagster entry point
def test_dagster_ingest_needs_the_exact_file_name_as_confirmation():
    inbox_zip("kvk.zip")
    wrong = k.ingest_confirmed_file("kvk.zip", "yes", confirmed_by="dagster:test")
    assert wrong["status"] == "CANCELLED" and not k.DATA_TABLE.exists()

    ok = k.ingest_confirmed_file("kvk.zip", "kvk.zip", confirmed_by="dagster:test")
    assert ok["status"] == "INGESTED" and ok["rows"] == 3
    from deltalake import DeltaTable
    log = DeltaTable(str(k.LOG_TABLE)).to_pandas()
    assert list(log["confirmed_by"]) == ["dagster:test"]
    again = k.ingest_confirmed_file("kvk.zip", "kvk.zip", confirmed_by="dagster:test")
    assert again["status"] == "FAILED"                # already ingested


def test_dagster_entry_point_is_not_an_agent_tool():
    """The model must never be able to skip the human confirmation."""
    assert "ingest_confirmed_file" not in k.TOOLS and "_ingest" not in k.TOOLS
    assert set(k.TOOLS) == {"get_dataset_info", "list_inbox", "verify_dataset",
                            "ingest_verified_dataset", "list_batches"}


# =========================================================================== KVK's own file date
def dated_zip(path, when=(2026, 10, 1, 23, 34, 22), rows=REAL_ROWS):
    """A KVK zip whose CSV carries the export time `when`, as in the real file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(zipfile.ZipInfo("kvk.csv", date_time=when),
                   "\r\n".join([REAL_HEADER, *rows]) + "\r\n")
    return path


def test_the_file_date_comes_from_the_zip(tmp_path):
    report = json.loads(k.verify_dataset(str(dated_zip(tmp_path / "kvk.zip"))))
    assert report["file_date"] == "2026-10-01"
    assert report["newest_start_date"] == "2006-07-21" and report["warnings"] == []


def test_no_file_date_in_a_loose_csv_or_an_undated_zip(tmp_path):
    csv = tmp_path / "kvk.csv"
    csv.write_text("\r\n".join([REAL_HEADER, *REAL_ROWS]) + "\r\n")
    assert "file_date" not in json.loads(k.verify_dataset(str(csv)))
    undated = json.loads(k.verify_dataset(str(dated_zip(tmp_path / "u.zip", (1980, 1, 1, 0, 0, 0)))))
    assert "file_date" not in undated and "no file date in the zip" in undated["warnings"][0]


def test_a_start_date_after_the_file_date_makes_it_doubtful(tmp_path):
    rows = [*REAL_ROWS, '"20261015";"J";"";"BV";"30";"6420";"6420";"NL"']
    report = json.loads(k.verify_dataset(str(dated_zip(tmp_path / "kvk.zip", rows=rows))))
    assert report["status"] == "PASSED"                    # a warning, not a rejection
    assert any("start dates after the file date" in w for w in report["warnings"])


def test_the_batch_log_records_the_file_date():
    from deltalake import DeltaTable
    dated_zip(k.INBOX / "kvk.zip")
    assert k.ingest_confirmed_file("kvk.zip", "kvk.zip", confirmed_by="t")["status"] == "INGESTED"
    log = DeltaTable(str(k.LOG_TABLE)).to_pandas()
    assert str(log["file_date"].iloc[0]) == "2026-10-01"


def test_backfill_fills_old_batches_from_the_same_file_in_the_inbox():
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    path = dated_zip(k.INBOX / "kvk.zip")
    # a log as written before file_date existed: no such column
    write_deltalake(str(k.LOG_TABLE), pa.table({
        "batch_id": ["old", "gone"], "file_sha256": [k._sha256(path), "no-such-file"],
        "status": ["success", "success"]}))
    assert k.backfill_file_dates() == {"old": "2026-10-01"}
    log = DeltaTable(str(k.LOG_TABLE)).to_pandas().set_index("batch_id")
    assert str(log.loc["old", "file_date"]) == "2026-10-01" and pd.isna(log.loc["gone", "file_date"])
    assert k.backfill_file_dates() == {}                   # nothing left that can be filled
    # a later load appends to the log that now has the column
    dated_zip(k.INBOX / "new.zip", (2026, 11, 2, 22, 0, 0), rows=REAL_ROWS[:2])
    assert k.ingest_confirmed_file("new.zip", "new.zip", confirmed_by="t")["status"] == "INGESTED"
    assert len(DeltaTable(str(k.LOG_TABLE)).to_pandas()) == 3
