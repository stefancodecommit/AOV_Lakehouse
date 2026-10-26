"""checks/: the CBS record checks and the quarantine folder, without the pipeline."""
import json

import pytest

from datetime import datetime, timezone

from checks import all_passed, failures, warnings
from checks.cbs import (check_metadata, check_table_lifecycle, check_typed_dataset,
                        table_structure)
from conftest import table_info
from checks.kvk import check_file, check_rows, match_columns
from checks.quarantine import list_entries, quarantine

DP = [{"Type": "Dimension", "Key": "Bedrijfskenmerken"},
      {"Type": "Dimension", "Key": "Marges"},
      {"Type": "TimeDimension", "Key": "Perioden"},
      {"Type": "TopicGroup", "Key": None},
      {"Type": "Topic", "Key": "Aandeel_1", "Datatype": "Double"},
      {"Type": "Topic", "Key": "Aantal_2", "Datatype": "Long"},
      {"Type": "Topic", "Key": "Toelichting_3", "Datatype": "String"}]


def rec(key="T001081 ", marge="MW00000", period="2025JJ00", **values):
    return {"ID": 0, "Bedrijfskenmerken": key, "Marges": marge, "Perioden": period,
            "Aandeel_1": 4.5, "Aantal_2": 12, "Toelichting_3": "x", **values}


def failed(results):
    return {r.name for r in failures(results)}


# =========================================================================== CBS checks
def test_table_structure():
    dims, measures = table_structure(DP)
    assert dims == ["Bedrijfskenmerken", "Marges", "Perioden"]
    assert measures == {"Aandeel_1": "Double", "Aantal_2": "Long", "Toelichting_3": "String"}


def test_valid_batch_passes_every_check():
    results = check_typed_dataset([rec(), rec(marge="MOG0095"), rec("301000")], DP, "2025JJ00")
    assert all_passed(results)
    assert [r.name for r in results] == ["not_empty", "has_all_columns", "period_matches_request",
                                         "measures_numeric_or_null", "unique_dimension_keys"]


def test_nulls_are_fine_and_cbs_padding_is_ignored():
    assert all_passed(check_typed_dataset(
        [rec(Aandeel_1=None, Aantal_2=None), rec(period="2025JJ00  ", marge="MOG0095")],
        DP, "2025JJ00"))


def test_empty_batch_fails():
    assert failed(check_typed_dataset([], DP, "2025JJ00")) == {"not_empty"}


def test_missing_column_fails_with_counts():
    r = rec()
    del r["Aantal_2"]
    results = check_typed_dataset([r, rec("301000")], DP, "2025JJ00")
    assert failed(results) == {"has_all_columns"}
    (bad,) = failures(results)
    assert bad.examples == [{"column": "Aantal_2", "records_missing": 1}]


def test_other_period_fails():
    results = check_typed_dataset([rec(), rec("301000", period="2024JJ00")], DP, "2025JJ00")
    assert failed(results) == {"period_matches_request"}
    assert failures(results)[0].examples == [{"period": "2024JJ00", "records": 1}]


def test_period_is_not_checked_without_a_requested_period():
    names = [r.name for r in check_typed_dataset([rec()], DP, None)]
    assert "period_matches_request" not in names


@pytest.mark.parametrize("value", ["4.5", True, [1], {"v": 1}])
def test_non_numeric_measure_fails(value):
    assert failed(check_typed_dataset([rec(Aandeel_1=value)], DP, "2025JJ00")) == \
        {"measures_numeric_or_null"}


def test_string_measures_are_not_checked_for_numbers():
    assert all_passed(check_typed_dataset([rec(Toelichting_3="tekst")], DP, "2025JJ00"))


def test_duplicate_dimension_keys_fail_even_with_padding():
    results = check_typed_dataset([rec("T001081 "), rec("T001081")], DP, "2025JJ00")
    assert failed(results) == {"unique_dimension_keys"}


def test_unknown_structure_fails():
    assert "structure_known" in failed(check_typed_dataset([rec()], [], "2025JJ00"))


def test_metadata_only_needs_records():
    assert all_passed(check_metadata([{"Key": "T001081", "Title": "Totaal"}]))
    assert failed(check_metadata([])) == {"not_empty"}


# =========================================================================== quarantine
def test_quarantine_writes_report_and_records(tmp_path):
    folder = quarantine(tmp_path, "cbs", "80072ned_typeddataset_2026KW01",
                        {"checks": [{"name": "not_empty", "passed": False}]}, records=[{"a": 1}])
    assert folder.parent == tmp_path / "cbs" and folder.name.endswith("_80072ned_typeddataset_2026KW01")
    report = json.loads((folder / "report.json").read_text())
    assert report["source"] == "cbs" and report["checks"][0]["name"] == "not_empty"
    assert json.loads((folder / "records.json").read_text()) == [{"a": 1}]
    assert [e["name"] for e in list_entries(tmp_path)] == ["80072ned_typeddataset_2026KW01"]


def test_same_content_is_quarantined_once_other_content_again(tmp_path):
    assert quarantine(tmp_path, "cbs", "x", {}, records=[{"a": 1}]) is not None
    assert quarantine(tmp_path, "cbs", "x", {}, records=[{"a": 1}]) is None
    assert quarantine(tmp_path, "cbs", "x", {}, records=[{"a": 2}]) is not None
    assert quarantine(tmp_path, "cbs", "y", {}, records=[{"a": 1}]) is not None
    assert len(list_entries(tmp_path, "cbs")) == 3


def test_quarantine_a_file(tmp_path):
    src = tmp_path / "upload.zip"
    src.write_bytes(b"not really a zip")
    folder = quarantine(tmp_path / "q", "kvk", "upload.zip", {"status": "FAILED"}, file=src)
    assert (folder / "upload.zip").read_bytes() == b"not really a zip"
    assert quarantine(tmp_path / "q", "kvk", "upload.zip", {}, file=src) is None


# =========================================================================== KVK file checks
REQUIRED = {"start_date": {"datumaanvang"}, "active": {"actief"},
            "legal_form": {"rechtsvorm"}, "postcode_region": {"postcoderegio"}}
OPTIONAL = {"main_activity": {"hoofdactiviteiten"}}
HEADER = '"Datum aanvang";"Actief";"Rechtsvorm";"Postcode regio";"Hoofdactiviteiten"'


def kvk_zip(tmp_path, *rows, name="kvk.zip", member="kvk.csv"):
    import zipfile
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(member, "\r\n".join([HEADER, *rows]) + "\r\n")
    return path


def test_kvk_valid_file_passes(tmp_path):
    c = check_file(kvk_zip(tmp_path, '"20060721";"J";"BV";"92";"47741"'), REQUIRED, OPTIONAL, 0.001)
    assert c.passed and c.row_count == 1 and c.csv_member == "kvk.csv"
    assert c.report()["recognised_columns"]["main_activity"] == "Hoofdactiviteiten"


def test_kvk_value_problems_are_warnings_not_errors(tmp_path):
    c = check_file(kvk_zip(tmp_path, '"2006/07/21";"X";"VOF";"";"47741"'), REQUIRED, OPTIONAL, 0.001)
    assert c.passed
    assert [w.split(":")[0] for w in c.warnings] == [
        "active_not_J_N", "legal_form_not_BV_NV", "postcode_region_not_2_digits",
        "start_date_unparsed"]


def test_kvk_too_many_malformed_rows_fail(tmp_path):
    c = check_file(kvk_zip(tmp_path, '"20060721";"J";"BV";"92";"47741"', '"too";"few"'),
                   REQUIRED, OPTIONAL, 0.001)
    assert c.errors == ["1 of 2 rows have the wrong number of fields"]


@pytest.mark.parametrize("make, error", [
    (lambda p: kvk_zip(p, member="readme.txt"), "No CSV in zip"),
    (lambda p: (p / "kvk.xlsx").write_bytes(b"x") and p / "kvk.xlsx", "neither a zip nor a .csv"),
    (lambda p: p / "missing.zip", "File not found"),
    (lambda p: kvk_zip(p), "CSV has a header but no data rows"),
])
def test_kvk_unusable_files_fail(tmp_path, make, error):
    c = check_file(make(tmp_path), REQUIRED, OPTIONAL, 0.001)
    assert not c.passed and any(error in e for e in c.errors), c.errors


def test_kvk_already_ingested_file_fails_and_says_so(tmp_path):
    path = kvk_zip(tmp_path, '"20060721";"J";"BV";"92";"47741"')
    first = check_file(path, REQUIRED, OPTIONAL, 0.001)
    again = check_file(path, REQUIRED, OPTIONAL, 0.001, ingested={first.sha256: "kvk_batch_1"})
    assert again.errors == ["This exact file was already ingested as batch kvk_batch_1"]
    assert again.details["already_ingested"] == "kvk_batch_1"


def test_kvk_match_columns_ignores_case_and_punctuation():
    found, errors = match_columns(["Datum_Aanvang", "ACTIEF", "rechtsvorm", "Postcode-regio"],
                                  REQUIRED, OPTIONAL)
    assert found == {"start_date": 0, "active": 1, "legal_form": 2, "postcode_region": 3}
    assert errors == []
    _, errors = match_columns(["Actief"], REQUIRED, OPTIONAL)
    assert len(errors) == 3


def test_kvk_check_rows_counts():
    found = {"start_date": 0, "active": 1, "legal_form": 2, "postcode_region": 3}
    total, malformed, hits, newest = check_rows(
        [["20060721", "J", "BV", "92"], ["", "N", "NV", "01"], ["short"]], 4, found)
    assert (total, malformed) == (3, 1) and not any(hits.values())
    assert newest == "20060721"


# =========================================================================== CBS table lifecycle
NOW = datetime(2026, 10, 5, tzinfo=timezone.utc)


def test_regular_recent_table_has_no_warnings():
    assert all_passed(check_table_lifecycle(table_info(), NOW))


def test_stopped_table_is_a_warning_not_an_error():
    results = check_table_lifecycle(
        table_info("Stopgezet", "2024-04-17T02:00:00", "Gediscontinueerd"), NOW)
    assert [w.name for w in warnings(results)] == ["table_not_stopped"]
    assert failures(results) == []                 # never blocks ingestion
    assert "table_recently_modified" not in [r.name for r in results]  # no age check then


@pytest.mark.parametrize("frequency, modified, stale", [
    ("Perkwartaal", "2026-06-10T02:00:00", False),   # 116 days, limit 122
    ("Perkwartaal", "2026-05-01T02:00:00", True),    # 156 days
    ("Perjaar", "2025-08-01T02:00:00", False),
    ("Perjaar", "2025-06-01T02:00:00", True),
    ("Onregelmatig", "2010-01-01T00:00:00", False),  # unknown frequency: no age check
])
def test_stale_table_warning(frequency, modified, stale):
    results = check_table_lifecycle(table_info(frequency, modified), NOW)
    assert ("table_recently_modified" in [w.name for w in warnings(results)]) == stale
