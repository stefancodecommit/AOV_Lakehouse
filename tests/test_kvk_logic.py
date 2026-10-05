"""Bronze KVK: verification of an uploaded file. Offline: the batch log points at tmp_path."""
import json
import zipfile
from pathlib import Path

import pytest

import kvk_bronze_agent as k

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
