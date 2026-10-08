"""lineage.py: the bronze -> silver column lineage is complete, points at fields that
exist, and docs/lineage.md matches it."""
import json
import sys
from pathlib import Path

import pytest

import cbs_bronze as b
import kvk_silver as k
import lineage as L
import reference_bronze as r
from conftest import sample_bronze
from test_kvk_silver import COLUMNS as KVK_HEADER, CROSSWALK, bronze as kvk_bronze

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def silver_columns(silver):
    """Columns of every silver table as built from the sample data (+ run_id, which
    publishing adds)."""
    out = {name: [*df.columns, "run_id"] for name, df in silver.items()}
    out["kvk_company"] = [*k.build_kvk_company(kvk_bronze(), CROSSWALK).columns, "run_id"]
    return out


def bronze_fields() -> dict[str, set[str]]:
    """Fields per bronze table: the JSON keys in the sample records, bronze's own
    columns, and the columns of the KVK and crosswalk tables."""
    lineage_cols = set(b.bronze_schema().names)
    out = {}
    for (table_id, entity), raw in sample_bronze().items():
        keys = set().union(*(json.loads(x) for x in raw["raw_json"]))
        out[f"{table_id}_{entity}".lower()] = {f"raw_json.{x}" for x in keys} | lineage_cols
    # CategoryGroups of the survey tables have the same fields as 80072NED's
    for t in ("86009ned_categorygroups", "83130ned_categorygroups"):
        out[t] = out["80072ned_categorygroups"]
    out[L.KVK] = set(KVK_HEADER) | {"_batch_id", "_ingested_at", "_source_file",
                                     "_source_sha256", "_row_number"}
    out[L.CROSSWALK] = set(r.COLUMNS) | {"_batch_id", "_ingested_at", "_source_file",
                                         "_source_url", "_source_sha256", "_source_version",
                                         "_row_number"}
    return out


# =========================================================================== completeness
def test_every_silver_table_has_lineage(silver_columns):
    assert set(L.SILVER_LINEAGE) == set(silver_columns)


@pytest.mark.parametrize("table", sorted(L.SILVER_LINEAGE))
def test_every_column_has_lineage_in_the_same_order(silver_columns, table):
    """A new silver column fails here until its origin is written down in lineage.py."""
    assert list(L.SILVER_LINEAGE[table]) == silver_columns[table]


# =========================================================================== correctness
def test_every_bronze_source_table_is_a_real_bronze_table():
    configured = {e.name for e in b.ENDPOINTS} | {L.KVK, L.CROSSWALK}
    for table, columns in L.SILVER_LINEAGE.items():
        for column, c in columns.items():
            for s in c.sources:
                if not s.is_silver:
                    assert s.table in configured, f"{table}.{column}: {s}"


def test_every_named_bronze_field_exists():
    fields = bronze_fields()
    for table, columns in L.SILVER_LINEAGE.items():
        for column, c in columns.items():
            for s in c.sources:
                if s.is_silver or s.field == "raw_json.<measure>":
                    continue
                assert s.field in fields[s.table], f"{table}.{column}: {s} not in bronze"


def test_silver_sources_point_at_documented_silver_columns():
    for table, columns in L.SILVER_LINEAGE.items():
        for column, c in columns.items():
            for s in c.sources:
                if s.is_silver:
                    other = s.table.removeprefix("silver:")
                    assert s.field in L.SILVER_LINEAGE[other], f"{table}.{column}: {s}"


def test_only_run_id_has_no_source():
    for table, columns in L.SILVER_LINEAGE.items():
        for column, c in columns.items():
            assert bool(c.sources) or column == "run_id", f"{table}.{column}"


def test_silver_to_silver_steps_are_followed_back_to_bronze():
    origin = L.bronze_sources("fct_absence_rate", "member_type")
    assert L.Source("80072ned_bedrijfskenmerkensbi2008", "raw_json.Title") in origin
    assert not any(s.is_silver for s in origin)


# =========================================================================== outputs
def test_docs_lineage_md_is_up_to_date():
    """Regenerate with: python lineage.py"""
    assert (ROOT / "docs" / "lineage.md").read_text(encoding="utf-8") == L.markdown()


def test_dagster_shows_the_lineage():
    sys.path.insert(0, str(ROOT / "orchestration"))
    import dagster as dg
    import definitions as d
    meta = d.silver_cbs.specs_by_key[dg.AssetKey("silver_cbs")].metadata
    deps = meta["dagster/column_lineage"].deps_by_column
    assert [(x.asset_key.to_user_string(), x.column_name)
            for x in deps["fct_absence_rate.absence_pct"]] == [
        ("cbs_bronze", "80072ned_typeddataset.raw_json.Ziekteverzuimpercentage_1")]
    # a silver-to-silver step inside the asset is followed back to bronze
    assert {x.asset_key.to_user_string() for x in deps["fct_absence_rate.member_type"]} == {"cbs_bronze"}
    kvk = d.silver_kvk.specs_by_key[dg.AssetKey("silver_kvk")].metadata["dagster/column_lineage"]
    assert {x.asset_key.to_user_string() for x in kvk.deps_by_column["kvk_company.sbi2008_code"]} \
        == {"kvk_bronze", "silver_cbs"}


def test_dagster_column_descriptions_start_with_their_origin():
    """The free Dagster UI shows the column list, not the lineage graph (Dagster+ only),
    so the origin has to be in the description."""
    cols = {c.name: c.description for c in
            L.dagster_lineage({"fct_absence_rate"})["dagster/column_schema"].columns}
    assert cols["fct_absence_rate.absence_pct"].startswith(
        "From 80072ned_typeddataset.raw_json.Ziekteverzuimpercentage_1 — as a number")
    assert cols["fct_absence_rate.run_id"].startswith("From the run — ")
