"""Reference data: the SBI 2008 -> SBI 2025 crosswalk, from xlsx to bronze to silver.
Small xlsx files are built in tmp_path with the real v2.0 header and sample rows."""
import pandas as pd
import pytest
from deltalake import DeltaTable

from pipeline.silver import cbs as s
from pipeline.bronze import reference as r

HEADER = ["SBI2008", "SBI2008_2digit", "SBI2008 Omschrijving", "SBI2025", "SBI2025_2digit",
          "SBI2025 titel dd 20260101", "Omschrijving schakeling"]
# Real v2.0 rows: 1:1, a split (01199 -> two codes), a merge (0111 and 01199 share
# nothing; 01131 and 01132 -> 01130 is invented to show a merge) and the CBS quirk
# where the SBI 2025 2-digit column was copied from SBI 2008 (71111 / 74).
ROWS = [
    ["0111", "01", "Teelt van granen", "01110", "01", "Teelt van granen", "Teelt van granen"],
    ["01131", "01", "Groenten volle grond", "01130", "01", "Groenten", "Groenten volle grond"],
    ["01132", "01", "Groenten onder glas", "01130", "01", "Groenten", "Groenten onder glas"],
    ["01199", "01", "Overige eenjarige gewassen", "01199", "01", "Overige", "Overige"],
    ["01199", "01", "Overige eenjarige gewassen", "01120", "01", "Teelt van rijst", "Rijst"],
    ["74103", "74", "Interieur- en ruimtelijk ontwerp", "71111", "74", "Architecten", "Ruimtelijk"],
]


def xlsx(path, rows=ROWS, header=HEADER, sheet="Schakeltabel v2.0", as_numbers=False):
    df = pd.DataFrame(rows, columns=header)
    if as_numbers:                                   # codes typed as numbers in Excel
        df["SBI2008"] = df["SBI2008"].astype(int)
    with pd.ExcelWriter(path) as w:
        df.to_excel(w, sheet_name=sheet, index=False)
        pd.DataFrame({"Versie": ["2.0"]}).to_excel(w, sheet_name="Versie historie", index=False)
    return path


def bronze():
    return DeltaTable(str(r.DATA_TABLE)).to_pandas()


def batch_log():
    return DeltaTable(str(r.LOG_TABLE)).to_pandas()


# =========================================================================== loading
def test_first_load_writes_every_row_as_text(tmp_path):
    report = r.ingest(xlsx(tmp_path / "v20.xlsx"), source_url="https://cbs/v20.xlsx")
    assert report["status"] == "INGESTED" and report["rows"] == 6 and report["version"] == "v2.0"
    df = bronze().sort_values("_row_number")
    assert df["sbi2008"].tolist()[0] == "0111"            # leading zero kept
    assert set(df["_batch_id"]) == {report["batch_id"]}
    assert df["_source_url"].iloc[0] == "https://cbs/v20.xlsx"
    assert report["warnings"] == ["1 SBI 2025 codes don't start with their 2-digit column, "
                                  "e.g. [['71111', '74']]"]


def test_loading_the_same_file_twice_adds_nothing(tmp_path):
    path = xlsx(tmp_path / "v20.xlsx")
    first = r.ingest(path)
    second = r.ingest(path)
    assert second["status"] == "SKIPPED" and second["batch_id"] == first["batch_id"]
    assert len(bronze()) == 6 and len(batch_log()) == 1


def test_same_content_under_another_name_is_skipped_too(tmp_path):
    a = xlsx(tmp_path / "v20.xlsx")
    b = tmp_path / "copy-of-crosswalk.xlsx"
    b.write_bytes(a.read_bytes())
    r.ingest(a)
    assert r.ingest(b)["status"] == "SKIPPED"
    assert len(bronze()) == 6


def test_rows_of_an_unlogged_batch_are_removed_before_the_next_load(tmp_path):
    """Crash after the data write but before the log: the retry must not double rows."""
    path = xlsx(tmp_path / "v20.xlsx")
    df, version, _, _ = r.read_crosswalk(path)
    r.write_batch(df, "crashed_batch", pd.Timestamp.now(tz="UTC"), path.name, None, "abc", version)
    report = r.ingest(path)
    assert report["status"] == "INGESTED"
    assert set(bronze()["_batch_id"]) == {report["batch_id"]} and len(bronze()) == 6


def test_a_new_version_becomes_a_second_batch(tmp_path):
    r.ingest(xlsx(tmp_path / "v20.xlsx"))
    new = r.ingest(xlsx(tmp_path / "v21.xlsx", rows=ROWS[:3], sheet="Schakeltabel v2.1"))
    assert new["status"] == "INGESTED" and new["version"] == "v2.1"
    assert len(bronze()) == 9 and len(batch_log()) == 2


# =========================================================================== rejected files
@pytest.mark.parametrize("kwargs, error", [
    ({"rows": ROWS + [ROWS[0]]}, "1 duplicate (SBI 2008, SBI 2025) pairs"),
    ({"header": HEADER[:-1], "rows": [row[:-1] for row in ROWS]}, "Column missing: link_description"),
    ({"sheet": "Blad1"}, "No sheet starting with 'Schakeltabel'"),
    ({"as_numbers": True}, "SBI 2008 codes seem to have lost a leading zero"),
    ({"rows": [["01x1", *ROWS[0][1:]]]}, "SBI 2008 codes are not 4-6 digits"),
])
def test_invalid_file_is_rejected_and_nothing_written(tmp_path, kwargs, error):
    report = r.ingest(xlsx(tmp_path / "bad.xlsx", **kwargs))
    assert report["status"] == "FAILED"
    assert any(error in e for e in report["errors"]), report["errors"]
    assert not (r.DATA_TABLE / "_delta_log").exists() and not (r.LOG_TABLE / "_delta_log").exists()
    folder = r.Path(report["quarantine"])
    assert folder.parent == r.QUARANTINE_ROOT / "reference" and (folder / "bad.xlsx").exists()


def test_title_header_with_another_date_is_accepted(tmp_path):
    header = HEADER[:5] + ["SBI2025 titel dd 20270101", HEADER[6]]
    assert r.ingest(xlsx(tmp_path / "v22.xlsx", header=header))["status"] == "INGESTED"


# =========================================================================== silver
def test_silver_map_link_types():
    ref = pd.DataFrame(ROWS, columns=list(r.COLUMNS))
    ref["_source_version"], ref["_batch_id"] = "v2.0", "b1"
    ref["_ingested_at"] = pd.Timestamp("2026-10-05", tz="UTC")
    m = s.build_map_sbi2008_sbi2025(ref).set_index(["sbi2008_code", "sbi2025_code"])
    assert m.loc[("0111", "01110"), "link_type"] == "1:1"
    assert m.loc[("01131", "01130"), "link_type"] == "merge"
    assert m.loc[("01199", "01120"), "link_type"] == "split"
    assert m.loc[("01199", "01120"), "sbi2025_targets"] == 2
    assert m.loc[("01131", "01130"), "sbi2008_sources"] == 2
    assert m.loc[("74103", "71111"), "sbi2025_division"] == "71"   # from the code itself


def test_silver_map_many_to_many():
    rows = [["01199", "01", "a", "01120", "01", "x", ""], ["01199", "01", "a", "01130", "01", "y", ""],
            ["01131", "01", "b", "01130", "01", "y", ""]]
    ref = pd.DataFrame(rows, columns=list(r.COLUMNS)).assign(
        _source_version="v", _batch_id="b", _ingested_at=pd.Timestamp("2026-10-05", tz="UTC"))
    m = s.build_map_sbi2008_sbi2025(ref).set_index(["sbi2008_code", "sbi2025_code"])
    assert m.loc[("01199", "01130"), "link_type"] == "many-to-many"


def test_silver_reads_only_the_latest_version(tmp_path):
    r.ingest(xlsx(tmp_path / "v20.xlsx"))
    r.ingest(xlsx(tmp_path / "v21.xlsx", rows=ROWS[:3], sheet="Schakeltabel v2.1"))
    m = s.build_map_sbi2008_sbi2025(s.read_reference("sbi2008_sbi2025"))
    assert len(m) == 3 and set(m["source_version"]) == {"v2.1"}


def test_silver_without_reference_data_is_empty():
    assert s.read_reference("sbi2008_sbi2025").empty
    assert s.build_map_sbi2008_sbi2025(pd.DataFrame()).empty
