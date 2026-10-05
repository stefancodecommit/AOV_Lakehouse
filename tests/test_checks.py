"""checks/: the CBS record checks and the quarantine folder, without the pipeline."""
import json

import pytest

from checks import all_passed, failures
from checks.cbs import check_metadata, check_typed_dataset, table_structure
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
