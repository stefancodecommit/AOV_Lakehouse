"""
Column lineage, bronze -> silver: for every silver column, the bronze table(s) and
field(s) it comes from, and what happens to it on the way.

This file is the single source of truth. From it:
- `python lineage.py` writes docs/lineage.md (a readable table per silver table)
- orchestration/definitions.py shows the same lineage in the Dagster UI (column lineage
  of the silver_cbs and silver_kvk assets)
- tests/test_lineage.py checks that it is complete and correct: every silver column has
  an entry, every named bronze field exists, and docs/lineage.md is up to date. So a new
  silver column can't be added without saying where it comes from.

Notation of a source field
--------------------------
    80072ned_typeddataset.raw_json.Perioden   a field inside the JSON record CBS sent
    80072ned_typeddataset._period_status      a lineage column bronze added
    86009ned_typeddataset.raw_json.<measure>  every measure field (one row per measure)
    kvk/basis_bedrijfsgegevens.Actief         a column of the KVK file (stored as text)
    silver:map_characteristic.member_type     a column of another silver table
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    table: str          # bronze table folder, e.g. "80072ned_typeddataset"; or "silver:<table>"
    field: str          # "raw_json.<key>", a bronze column, or a silver column

    @property
    def is_silver(self) -> bool:
        return self.table.startswith("silver:")

    def __str__(self) -> str:
        return f"{self.table}.{self.field}"


@dataclass(frozen=True)
class Column:
    sources: tuple[Source, ...]
    how: str            # the transformation, in plain words


def src(table: str, *fields: str) -> tuple[Source, ...]:
    return tuple(Source(table, f) for f in fields)


def each(tables, *fields: str) -> tuple[Source, ...]:
    return tuple(Source(t, f) for t in tables for f in fields)


# --------------------------------------------------------------------------- bronze tables
TYPED = {"80072NED": "80072ned_typeddataset", "86009NED": "86009ned_typeddataset",
         "83130NED": "83130ned_typeddataset", "81589NED": "81589ned_typeddataset",
         "81644NED": "81644ned_typeddataset"}
CODE_LISTS = ["80072ned_bedrijfskenmerkensbi2008", "86009ned_bedrijfskenmerken",
              "83130ned_bedrijfskenmerken", "81589ned_bedrijfstakkenbranchessbi2008",
              "81644ned_bedrijfstakkenbranchessbi2008"]
CATEGORY_GROUPS = ["80072ned_categorygroups", "86009ned_categorygroups",
                   "83130ned_categorygroups"]
SURVEY = ["86009ned_typeddataset", "83130ned_typeddataset"]
SURVEY_PROPS = ["86009ned_dataproperties", "83130ned_dataproperties"]
COUNT_PROPS = ["81589ned_dataproperties", "81644ned_dataproperties"]
KVK = "kvk/basis_bedrijfsgegevens"
CROSSWALK = "reference/sbi2008_sbi2025"
MAP = "silver:map_characteristic"

RUN_ID = Column((), "added when the run publishes the table: the silver run that made the "
                    "row; joins to lakehouse/_run_log (when, which code, which bronze versions)")
STATUS = "the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig)"
INGESTED = "when the bronze row was loaded (UTC)"


def characteristic() -> dict[str, Column]:
    """member_type, sector_key, size_key: looked up in map_characteristic."""
    return {
        "member_type": Column(src(MAP, "member_type"),
                              "looked up by characteristic_key: sector, size, total or unknown"),
        "sector_key": Column(src(MAP, "sector_key"), "looked up by characteristic_key; "
                             "filled for sectors and the total, joins to dim_sector"),
        "size_key": Column(src(MAP, "size_key"), "looked up by characteristic_key; filled "
                           "for size classes, joins to dim_company_size"),
    }


def counts(table_id: str, extra: dict[str, Column] | None = None) -> dict[str, Column]:
    """fct_business_count / fct_establishment_count (same steps, 81644 adds the region)."""
    typed, props = TYPED[table_id], f"{table_id.lower()}_dataproperties"
    return {
        "period_key": Column(src(typed, "raw_json.Perioden"), "as given (padding removed)"),
        "sector_key": Column(src(typed, "raw_json.BedrijfstakkenBranchesSBI2008"),
                             "as given (padding removed); joins to dim_sector"),
        "member_type": Column(src(MAP, "member_type"),
                              "looked up by sector_key: sector or total"),
        **(extra or {}),
        "breakdown": Column(src(props, "raw_json.ParentID", "raw_json.Title"),
                            "the measure's topic group in DataProperties: 'total' (none), "
                            "'size' (…grootte) or 'legal_form' (Rechtsvorm)"),
        "measure_key": Column(src(typed, "raw_json.<measure>"),
                              "the name of the measure field; one row per measure (wide -> long)"),
        "size_key": Column(src(props, "raw_json.Title"), "size class parsed from the measure's "
                           "title, e.g. '2 werkzame personen' -> WP_2_3 (size rows only)"),
        "size_band": Column(src(props, "raw_json.Title"),
                            "<10, 10-99 or 100+ from the parsed bounds (size rows only)"),
        "size_is_aggregate": Column(src(props, "raw_json.Title"), "true when the class contains "
                                    "other classes of the table (e.g. 0-50); leave out when summing"),
        "legal_form": Column(src(props, "raw_json.Title"),
                             "the measure's title, for legal-form rows"),
        "count": Column(src(typed, "raw_json.<measure>"),
                        "the measure's value, as a whole number (null when CBS left it empty)"),
        "period_status": Column(src(typed, "_period_status"), STATUS),
        "ingested_at": Column(src(typed, "_ingested_at"), INGESTED),
        "run_id": RUN_ID,
    }


# --------------------------------------------------------------------------- the lineage
SILVER_LINEAGE: dict[str, dict[str, Column]] = {
    "map_characteristic": {
        "source_table": Column(each(CODE_LISTS, "_table_id"), "the CBS table the key belongs to"),
        "dimension": Column(each(CODE_LISTS, "_entity"), "the code list's name, e.g. "
                            "BedrijfskenmerkenSBI2008"),
        "characteristic_key": Column(each(CODE_LISTS, "raw_json.Key"), "as given (padding removed)"),
        "title": Column(each(CODE_LISTS, "raw_json.Title"), "as given (spaces trimmed)"),
        "category_group": Column(each(CODE_LISTS, "raw_json.CategoryGroupID")
                                 + each(CATEGORY_GROUPS, "raw_json.ID", "raw_json.Title"),
                                 "the title of the key's category group (e.g. Bedrijfsgrootte)"),
        "member_type": Column(each(CODE_LISTS, "raw_json.Key", "raw_json.Title"),
                              "'size' when the title reads like a size class (whole words "
                              "'werknemers' / 'werkzame personen'), 'total' for T001081 or a "
                              "title starting with A-U, otherwise 'sector'"),
        "sector_key": Column(each(CODE_LISTS, "raw_json.Key"),
                             "the key itself, for sectors and the total"),
        "size_key": Column(each(CODE_LISTS, "raw_json.Title"), "size class parsed from the title, "
                           "e.g. '1 tot 10 werkzame personen' -> WP_1_10 (size rows only)"),
        "size_is_aggregate": Column(each(CODE_LISTS, "raw_json.Title"),
                                    "true when the class contains other classes of the same "
                                    "table, e.g. 1-9 contains 1-4 and 5-9 (size rows only)"),
        "run_id": RUN_ID,
    },
    "dim_sector": {
        "sector_key": Column(each(CODE_LISTS, "raw_json.Key") + src(MAP, "member_type"),
                             "the keys map_characteristic marks as sector or total; one row "
                             "per key (CBS uses the same key in every table)"),
        "sbi_code": Column(each(CODE_LISTS, "raw_json.Title"),
                           "the code at the start of the title: 'C Industrie' -> C"),
        "sector_name": Column(each(CODE_LISTS, "raw_json.Title"),
                              "the rest of the title: 'C Industrie' -> Industrie"),
        "sbi_level": Column(each(CODE_LISTS, "raw_json.Title"),
                            "from the code's form: A-U total, C section, B-F section_group, "
                            "47 division, 812 group, 10-12 division_group, ..."),
        "title": Column(each(CODE_LISTS, "raw_json.Title"),
                        "as given; when tables differ, the most common title"),
        "run_id": RUN_ID,
    },
    "dim_company_size": {
        "size_key": Column(each(CODE_LISTS, "raw_json.Title") + each(COUNT_PROPS, "raw_json.Title"),
                           "unit and bounds parsed from a size-class title: WN (employees) or "
                           "WP (persons employed), lower, upper -> e.g. WP_1_10"),
        "size_unit": Column(each(CODE_LISTS, "raw_json.Title") + each(COUNT_PROPS, "raw_json.Title"),
                            "'employees' (werknemers) or 'persons_employed' (werkzame personen)"),
        "lower_bound": Column(each(CODE_LISTS, "raw_json.Title") + each(COUNT_PROPS, "raw_json.Title"),
                              "the first number of the title ('1 000' -> 1000)"),
        "upper_bound_excl": Column(each(CODE_LISTS, "raw_json.Title") + each(COUNT_PROPS, "raw_json.Title"),
                                   "the upper bound, exclusive; empty for 'of meer' (no upper bound)"),
        "size_range": Column(each(CODE_LISTS, "raw_json.Title") + each(COUNT_PROPS, "raw_json.Title"),
                             "readable range from the bounds: 1-9, 100+"),
        "size_band": Column(each(CODE_LISTS, "raw_json.Title") + each(COUNT_PROPS, "raw_json.Title"),
                            "<10, 10-99 or 100+: the band that contains the class"),
        "title": Column(each(CODE_LISTS, "raw_json.Title") + each(COUNT_PROPS, "raw_json.Title"),
                        "as given (spaces normalised)"),
        "run_id": RUN_ID,
    },
    "dim_period": {
        "period_key": Column(each(TYPED.values(), "raw_json.Perioden"),
                             "every period key that occurs in a fact table"),
        "period_type": Column(each(TYPED.values(), "raw_json.Perioden"),
                              "from the key: KW quarter, JJ year, MM month, HJ half year"),
        "year": Column(each(TYPED.values(), "raw_json.Perioden"), "the first four digits"),
        "quarter": Column(each(TYPED.values(), "raw_json.Perioden"), "the quarter (KW, MM keys)"),
        "month": Column(each(TYPED.values(), "raw_json.Perioden"), "the month (MM keys)"),
        "start_date": Column(each(TYPED.values(), "raw_json.Perioden"), "first day of the period"),
        "end_date": Column(each(TYPED.values(), "raw_json.Perioden"), "last day of the period"),
        "period_label": Column(each(TYPED.values(), "raw_json.Perioden"),
                               "readable label: 2026 Q2, 2025"),
        "run_id": RUN_ID,
    },
    "dim_region": {
        "region_key": Column(src("81644ned_regios", "raw_json.Key"), "as given (padding removed)"),
        "region_name": Column(src("81644ned_regios", "raw_json.Title"),
                              "the title without its level suffix: 'Groningen (PV)' -> Groningen"),
        "region_level": Column(src("81644ned_regios", "raw_json.Key"), "from the key's prefix: NL "
                               "country, LD landsdeel, PV province, CR corop, GM municipality"),
        "description": Column(src("81644ned_regios", "raw_json.Description"), "as given"),
        "run_id": RUN_ID,
    },
    "dim_survey_measure": {
        "source_table": Column(each(SURVEY_PROPS, "_table_id"), "the survey table"),
        "measure_key": Column(each(SURVEY_PROPS, "raw_json.Key", "raw_json.Type"),
                              "the key of every Topic (measure) in DataProperties"),
        "measure_name": Column(each(SURVEY_PROPS, "raw_json.Key", "raw_json.ParentID",
                                    "raw_json.Title"),
                               "the key in snake_case ('AantalKeerVerzuimd_2' -> "
                               "aantal_keer_verzuimd); a name that occurs twice gets the era of "
                               "its topic group (_until_2022 / _from_2022)"),
        "title": Column(each(SURVEY_PROPS, "raw_json.Title"), "as given"),
        "topic_group": Column(each(SURVEY_PROPS, "raw_json.ParentID", "raw_json.Title"),
                              "the titles of the TopicGroups above the measure (via ParentID), "
                              "joined with ' / '"),
        "unit": Column(each(SURVEY_PROPS, "raw_json.Unit"), "as given: %, gemiddelde, ..."),
        "run_id": RUN_ID,
    },
    "map_sbi2008_sbi2025": {
        "sbi2008_code": Column(src(CROSSWALK, "sbi2008"), "as given, newest file version only"),
        "sbi2008_division": Column(src(CROSSWALK, "sbi2008"), "the code's first two digits"),
        "sbi2008_title": Column(src(CROSSWALK, "sbi2008_title"), "as given"),
        "sbi2025_code": Column(src(CROSSWALK, "sbi2025"), "as given"),
        "sbi2025_division": Column(src(CROSSWALK, "sbi2025"), "the code's first two digits "
                                   "(not the file's 2-digit column, which is wrong in 5 rows)"),
        "sbi2025_title": Column(src(CROSSWALK, "sbi2025_title"), "as given"),
        "link_description": Column(src(CROSSWALK, "link_description"), "as given"),
        "sbi2025_targets": Column(src(CROSSWALK, "sbi2008", "sbi2025"),
                                  "number of SBI 2025 codes this SBI 2008 code links to"),
        "sbi2008_sources": Column(src(CROSSWALK, "sbi2008", "sbi2025"),
                                  "number of SBI 2008 codes linking to this SBI 2025 code"),
        "link_type": Column(src(CROSSWALK, "sbi2008", "sbi2025"),
                            "1:1, split (one old code -> several new), merge (several old -> "
                            "one new) or many-to-many, from the two counts"),
        "source_version": Column(src(CROSSWALK, "_source_version"),
                                 "the file version, from its sheet name (v2.0)"),
        "batch_id": Column(src(CROSSWALK, "_batch_id"), "the loaded file version (batch)"),
        "ingested_at": Column(src(CROSSWALK, "_ingested_at"), "when the file was loaded (UTC)"),
        "run_id": RUN_ID,
    },
    "fct_absence_rate": {
        "period_key": Column(src(TYPED["80072NED"], "raw_json.Perioden"), "as given"),
        "characteristic_key": Column(src(TYPED["80072NED"], "raw_json.BedrijfskenmerkenSBI2008"),
                                     "as given (padding removed)"),
        **characteristic(),
        "absence_pct": Column(src(TYPED["80072NED"], "raw_json.Ziekteverzuimpercentage_1"),
                              "as a number: sick days as % of available working days"),
        "period_status": Column(src(TYPED["80072NED"], "_period_status"), STATUS),
        "ingested_at": Column(src(TYPED["80072NED"], "_ingested_at"), INGESTED),
        "run_id": RUN_ID,
    },
    "fct_absence_survey": {
        "source_table": Column(each(SURVEY, "_table_id"),
                               "86009NED or 83130NED: kept apart, never mixed"),
        "period_key": Column(each(SURVEY, "raw_json.Perioden"), "as given"),
        "characteristic_key": Column(each(SURVEY, "raw_json.Bedrijfskenmerken"),
                                     "as given (padding removed)"),
        **characteristic(),
        "size_is_aggregate": Column(src(MAP, "size_is_aggregate"),
                                    "looked up by characteristic_key (size classes only)"),
        "measure_key": Column(each(SURVEY, "raw_json.<measure>"),
                              "the name of the measure field; one row per measure (wide -> long)"),
        "measure_name": Column(src("silver:dim_survey_measure", "measure_name"),
                               "looked up by source_table and measure_key"),
        "value": Column(each(SURVEY, "raw_json.<measure>", "raw_json.Marges"),
                        "the measure's value in the record with Marges = MW00000 (the estimate)"),
        "lower_95": Column(each(SURVEY, "raw_json.<measure>", "raw_json.Marges"),
                           "the same measure in the record with Marges = MOG0095"),
        "upper_95": Column(each(SURVEY, "raw_json.<measure>", "raw_json.Marges"),
                           "the same measure in the record with Marges = MBG0095"),
        "period_status": Column(each(SURVEY, "_period_status"), STATUS),
        "ingested_at": Column(each(SURVEY, "_ingested_at"), INGESTED),
        "run_id": RUN_ID,
    },
    "fct_business_count": counts("81589NED"),
    "fct_establishment_count": counts("81644NED", extra={
        "region_key": Column(src(TYPED["81644NED"], "raw_json.RegioS"),
                             "as given (padding removed); joins to dim_region")}),
    "kvk_company": {
        "snapshot_date": Column(src(KVK, "_ingested_at"), "the day the upload was loaded"),
        "batch_id": Column(src(KVK, "_batch_id"), "the upload (one batch per file)"),
        "is_latest": Column(src(KVK, "_batch_id", "_ingested_at"),
                            "true for the newest successfully loaded upload"),
        "source_row_number": Column(src(KVK, "_row_number"), "the row's line in the CSV file"),
        "start_date": Column(src(KVK, "Datum_aanvang"), "as a date (YYYYMMDD); empty when it "
                             "doesn't parse"),
        "has_invalid_start_date": Column(src(KVK, "Datum_aanvang"),
                                         "true when a non-empty value doesn't parse as a date"),
        "is_active": Column(src(KVK, "Actief"), "J -> true, N -> false, anything else empty"),
        "legal_form": Column(src(KVK, "Rechtsvorm"), "as given (empty -> null)"),
        "insolvency_code": Column(src(KVK, "Insolventie"), "as given (empty -> null)"),
        "postcode_region": Column(src(KVK, "Postcode_regio"), "as given (empty -> null)"),
        "member_state": Column(src(KVK, "Lidstaat"), "as given (empty -> null)"),
        "main_activity_codes": Column(src(KVK, "Hoofdactiviteiten"),
                                      "as given: the original comma-separated list"),
        "main_activity_count": Column(src(KVK, "Hoofdactiviteiten"), "number of codes in the list"),
        "has_multiple_main_activities": Column(src(KVK, "Hoofdactiviteiten"), "more than one code"),
        "has_no_main_activity": Column(src(KVK, "Hoofdactiviteiten"), "the list is empty"),
        "is_kvk_subcode": Column(src(KVK, "Hoofdactiviteiten")
                                 + src("silver:map_sbi2008_sbi2025", "sbi2008_code", "sbi2025_code"),
                                 "a 6-digit code that is in neither SBI list"),
        "sbi_version": Column(src(KVK, "Hoofdactiviteiten")
                              + src("silver:map_sbi2008_sbi2025", "sbi2008_code", "sbi2025_code"),
                              "in which SBI list(s) the code(s) occur: 2025, 2008, both, "
                              "unknown, or mixed"),
        "sbi_mapping": Column(src(KVK, "Hoofdactiviteiten")
                              + src("silver:map_sbi2008_sbi2025", "sbi2008_code", "sbi2025_code"),
                              "how the code(s) reach SBI 2008: sbi2008, 1:1, merge, "
                              "ambiguous_version, unknown_code or none (the least certain one)"),
        "sbi2008_candidates": Column(src(KVK, "Hoofdactiviteiten")
                                     + src("silver:map_sbi2008_sbi2025", "sbi2008_code",
                                           "sbi2025_code"),
                                     "number of SBI 2008 codes the main code(s) could be"),
        "sbi2008_code": Column(src(KVK, "Hoofdactiviteiten")
                               + src("silver:map_sbi2008_sbi2025", "sbi2008_code", "sbi2025_code"),
                               "filled only when there is exactly one candidate"),
        "sbi2008_division": Column(src(KVK, "Hoofdactiviteiten")
                                   + src("silver:map_sbi2008_sbi2025", "sbi2008_code",
                                         "sbi2025_code"),
                                   "filled only when all candidates share their first two digits"),
        "sbi2008_section": Column(src(KVK, "Hoofdactiviteiten")
                                  + src("silver:map_sbi2008_sbi2025", "sbi2008_code",
                                        "sbi2025_code"),
                                  "the SBI 2008 section (letter) of that division"),
        "run_id": RUN_ID,
    },
}

SILVER_KVK_TABLES = {"kvk_company"}


# --------------------------------------------------------------------------- following back
def bronze_sources(table: str, column: str, _seen=None) -> set[Source]:
    """The bronze fields a silver column comes from, following silver-to-silver steps
    (e.g. fct_absence_rate.member_type -> map_characteristic -> the code lists)."""
    seen = _seen if _seen is not None else set()
    if (table, column) in seen:
        return set()
    seen.add((table, column))
    out = set()
    for s in SILVER_LINEAGE[table][column].sources:
        if s.is_silver:
            out |= bronze_sources(s.table.removeprefix("silver:"), s.field, seen)
        else:
            out.add(s)
    return out


# --------------------------------------------------------------------------- the document
def markdown() -> str:
    """docs/lineage.md: one table per silver table."""
    lines = [
        "# Column lineage: bronze → silver",
        "",
        "Where every silver column comes from in bronze, and what happens to it on the way.",
        "",
        "> Generated from `lineage.py` by `python lineage.py`; don't edit by hand. "
        "`tests/test_lineage.py` fails when a silver column is missing here or a named "
        "bronze field doesn't exist. The same lineage is shown in the Dagster UI "
        "(assets `silver_cbs` and `silver_kvk`, tab *Column lineage*). For the steps "
        "explained with real examples, see [bronze_to_silver.md](bronze_to_silver.md).",
        "",
        "How to read a source:",
        "",
        "| Source | Meaning |",
        "|---|---|",
        "| `80072ned_typeddataset.raw_json.Perioden` | a field in the JSON record CBS sent "
        "(bronze table `80072ned_typeddataset`) |",
        "| `80072ned_typeddataset._period_status` | a column bronze added when loading |",
        "| `86009ned_typeddataset.raw_json.<measure>` | every measure field of the record "
        "(silver has one row per measure) |",
        "| `kvk/basis_bedrijfsgegevens.Actief` | a column of the KVK file |",
        "| `silver:map_characteristic.member_type` | another silver table (its own lineage "
        "is listed below) |",
        "",
        "## Contents",
        "",
    ]
    for table in SILVER_LINEAGE:
        lines.append(f"- [`{table}`](#{table})")
    for table, columns in SILVER_LINEAGE.items():
        script = "kvk_silver.py" if table in SILVER_KVK_TABLES else "cbs_silver.py"
        lines += ["", f"## {table}", "", f"Built by `{script}`.", "",
                  "| Column | Comes from | How |", "|---|---|---|"]
        for column, c in columns.items():
            from_ = "<br>".join(f"`{s}`" for s in c.sources) or "*(the run)*"
            lines.append(f"| `{column}` | {from_} | {c.how} |")
    return "\n".join(lines) + "\n"


def dagster_lineage(asset_tables: set[str]) -> dict:
    """Column lineage and column descriptions for one Dagster asset (silver_cbs or
    silver_kvk), in Dagster's format. Columns are named '<table>.<column>'; bronze
    sources point at the bronze assets, silver sources outside the asset at silver_cbs."""
    import dagster as dg

    def asset_of(s: Source) -> str:
        if s.is_silver:
            return "silver_cbs"
        if s.table.startswith("kvk/"):
            return "kvk_bronze"
        if s.table.startswith("reference/"):
            return "sbi_crosswalk"
        return "cbs_bronze"

    deps, schema = {}, []
    for table in sorted(asset_tables):
        for column, c in SILVER_LINEAGE[table].items():
            name = f"{table}.{column}"
            schema.append(dg.TableColumn(name, description=c.how))
            sources = set()
            for s in c.sources:
                if s.is_silver and s.table.removeprefix("silver:") in asset_tables:
                    sources |= bronze_sources(s.table.removeprefix("silver:"), s.field)
                else:
                    sources.add(s)
            if sources:
                deps[name] = [dg.TableColumnDep(
                    asset_key=dg.AssetKey(asset_of(s)),
                    column_name=f"{s.table.removeprefix('silver:')}.{s.field}")
                    for s in sorted(sources, key=str)]
    return {"dagster/column_lineage": dg.TableColumnLineage(deps),
            "dagster/column_schema": dg.TableSchema(columns=schema)}


if __name__ == "__main__":
    from pathlib import Path
    path = Path(__file__).resolve().parent / "docs" / "lineage.md"
    path.write_text(markdown(), encoding="utf-8")
    print(f"Wrote {path} ({sum(len(c) for c in SILVER_LINEAGE.values())} columns, "
          f"{len(SILVER_LINEAGE)} tables)")
