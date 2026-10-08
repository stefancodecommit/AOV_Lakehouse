"""tools/lakehouse_viewer.py: finds every table per layer, filters the whole table, opens old
versions, computes the data quality statistics, and the page runs headless (AppTest)."""
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake
from streamlit.testing.v1 import AppTest

from pipeline.bronze import cbs as b
from pipeline.silver import cbs as s
from tools import lakehouse_viewer as v
from test_delta import ABSENCE, period_request, records

APP = str(Path(__file__).resolve().parents[1] / "tools/lakehouse_viewer.py")


def two_versions():
    b.write_bronze(period_request("2025KW04", "Definitief"), records("2025KW04", 5.6, 4.1))
    b.write_bronze(period_request("2026KW01", "Voorlopig"), records("2026KW01", 6.0))


def silver_table():
    """A small silver table with one of each problem: a repeated key, an identical row,
    padded and blank text, a missing number, a flag."""
    write_deltalake(str(s.SILVER_ROOT / "dim_sector"), pa.table({
        "sector_key": ["A", "B", "B", "C"],
        "sector_name": ["Landbouw", "Bouw ", "Bouw ", ""],
        "value": [1.5, None, None, 3.0],
        "is_total": [True, False, False, False],
        "run_id": ["r1", "r1", "r2", "r1"]}))


# =========================================================================== finding and reading
def test_finds_tables_per_layer():
    two_versions()
    silver_table()
    (b.BRONZE_ROOT / "kvk" / "basis_bedrijfsgegevens" / "_delta_log").mkdir(parents=True)
    (s.LAKEHOUSE / "_run_log" / "_delta_log").mkdir(parents=True)
    assert v.lakehouse_tables(s.LAKEHOUSE) == {
        "bronze": ["bronze/80072ned_typeddataset", "bronze/kvk/basis_bedrijfsgegevens"],
        "silver": ["silver/dim_sector"], "gold": [], "other": ["_run_log"]}


def test_filter_searches_the_whole_table_and_versions_travel_back():
    two_versions()
    path = b.BRONZE_ROOT / ABSENCE.name
    rows, matching = v.read(path, None, ["Perioden"], "_period", "kw01", limit=1)
    assert matching == 1 and rows.column("Perioden").to_pylist() == ["2026KW01"]
    rows, matching = v.read(path, None, None, None, "", limit=1)
    assert matching == 3 and rows.num_rows == 1                   # limit only limits the view
    _, matching = v.read(path, 0, None, None, "", limit=10)        # the first write
    assert matching == 2


# =========================================================================== data quality
def test_quality_statistics():
    silver_table()
    from deltalake import DeltaTable
    q = v.quality(DeltaTable(str(s.SILVER_ROOT / "dim_sector")).to_pyarrow_table(), "silver/dim_sector")
    prof = q["profile"].set_index("column")
    assert q["rows"] == 4
    assert q["key"] == ["sector_key"] and q["duplicate_keys"] == 1
    # run_id is bookkeeping: rows 2 and 3 are the same record
    assert "run_id" not in q["content_columns"] and q["duplicate_rows"] == 1
    name = prof.loc["sector_name"]
    assert (name.blank_text, name.padded_text, name.filled_pct) == (1, 2, 75.0)
    assert name.most_common == "Bouw   (2×)"
    value = prof.loc["value"]
    assert (value.missing, value["min"], value["max"], value.distinct) == (2, "1.5", "3.0", 2)
    assert prof.loc["is_total", "true"] == 1


def test_bronze_keys_and_content_columns():
    assert v.table_key("bronze/80072ned_typeddataset",
                       ["_request_key", "_record_index", "raw_json"]) == ["_request_key", "_record_index"]
    assert v.table_key("bronze/kvk/basis_bedrijfsgegevens", ["Actief", "_batch_id", "_row_number"]) \
        == ["_batch_id", "_row_number"]
    assert v.table_key("silver/kvk_company", []) == ["batch_id", "source_row_number"]
    assert v.table_key("silver/map_sbi2008_sbi2025", []) is None   # many-to-many: no key
    assert v.content_columns(["Actief", "_batch_id", "_row_number", "_ingested_at", "raw_json"]) \
        == ["Actief", "_batch_id"]


def test_empty_table_and_all_missing_column():
    q = v.quality(pa.table({"x": pa.array([None, None], pa.float64())}), "gold/t")
    assert q["profile"].iloc[0].missing == 2 and q["profile"].iloc[0].most_common is None
    assert v.quality(pa.table({"x": pa.array([], pa.string())}), "gold/t")["duplicate_rows"] == 0


# =========================================================================== page
def test_page_rows_quality_and_history():
    two_versions()
    silver_table()
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert not at.exception
    assert at.header[0].value == "bronze/80072ned_typeddataset"
    assert [m.value for m in at.metric] == ["3", "3"]
    at.selectbox(key="filter_column").set_value("Perioden").run()
    at.text_input(key="filter_text").set_value("2025").run()
    assert [m.value for m in at.metric] == ["2", "2"]

    at.radio(key="view").set_value("Data quality").run()
    assert not at.exception
    assert [m.value for m in at.metric][:1] == ["3"]

    at.radio(key="layer").set_value("silver").run()
    assert at.header[0].value == "silver/dim_sector"
    assert [m.value for m in at.metric] == ["4", "15.0%", "1", "1"]
    assert at.error and "repeat a key" in at.error[0].value
    assert at.warning and "leading or trailing spaces" in at.warning[0].value

    at.radio(key="view").set_value("History").run()
    assert not at.exception and at.dataframe
