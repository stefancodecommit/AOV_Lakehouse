"""
Column lineage, bronze -> silver -> gold: for every silver column, the bronze table(s)
and field(s) it comes from; for every gold column, the silver column(s) it is made from;
and what happens to it on the way.

This file is the single source of truth. From it:
- `python -m tools.lineage` writes docs/lineage.md (a diagram and a table per silver table),
  docs/lineage.html (all diagrams on one page, for a browser) and
  docs/lineage_explorer.html (interactive: pick a column or field, see its whole chain;
  built from lineage_explorer_template.html, works offline)
- orchestration/definitions.py shows the same lineage in the Dagster UI: in the free
  (open-source) Dagster, as the column list of the silver_cbs and silver_kvk assets,
  each description starting with "From <bronze field>"; the lineage is also stored in
  Dagster's own format, which only the paid Dagster+ draws as a graph
- tests/test_lineage.py checks that it is complete and correct: every silver column has
  an entry, every named bronze field exists, and docs/lineage.md is up to date. So a new
  silver column can't be added without saying where it comes from.

Notation of a source field
--------------------------
    80072ned_typeddataset.Perioden            a field of the record CBS sent (bronze
                                              keeps one column per field, and raw_json)
    80072ned_typeddataset._period_status      a lineage column bronze added
    86009ned_typeddataset.<measure>           every measure field (one row per measure)
    kvk/basis_bedrijfsgegevens.Actief         a column of the KVK file (stored as text)
    silver:map_characteristic.member_type     a column of another silver table
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    table: str          # bronze table folder, e.g. "80072ned_typeddataset"; or "silver:<table>"
    field: str          # a bronze column (a CBS field or a lineage column), or a silver column

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
KVK_LOG = "kvk/_batch_log"
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
        "period_key": Column(src(typed, "Perioden"), "as given (padding removed)"),
        "sector_key": Column(src(typed, "BedrijfstakkenBranchesSBI2008"),
                             "as given (padding removed); joins to dim_sector"),
        "member_type": Column(src(MAP, "member_type"),
                              "looked up by sector_key: sector or total"),
        **(extra or {}),
        "breakdown": Column(src(props, "ParentID", "Title"),
                            "the measure's topic group in DataProperties: 'total' (none), "
                            "'size' (…grootte) or 'legal_form' (Rechtsvorm)"),
        "measure_key": Column(src(typed, "<measure>"),
                              "the name of the measure field; one row per measure (wide -> long)"),
        "size_key": Column(src(props, "Title"), "size class parsed from the measure's "
                           "title, e.g. '2 werkzame personen' -> WP_2_3 (size rows only)"),
        "size_band": Column(src(props, "Title"),
                            "<10, 10-99 or 100+ from the parsed bounds (size rows only)"),
        "size_is_aggregate": Column(src(props, "Title"), "true when the class contains "
                                    "other classes of the table (e.g. 0-50); leave out when summing"),
        "legal_form": Column(src(props, "Title"),
                             "the measure's title, for legal-form rows"),
        "count": Column(src(typed, "<measure>"),
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
        "characteristic_key": Column(each(CODE_LISTS, "Key"), "as given (padding removed)"),
        "title": Column(each(CODE_LISTS, "Title"), "as given (spaces trimmed)"),
        "category_group": Column(each(CODE_LISTS, "CategoryGroupID")
                                 + each(CATEGORY_GROUPS, "ID", "Title"),
                                 "the title of the key's category group (e.g. Bedrijfsgrootte)"),
        "member_type": Column(each(CODE_LISTS, "Key", "Title"),
                              "'size' when the title reads like a size class (whole words "
                              "'werknemers' / 'werkzame personen'), 'total' for T001081 or a "
                              "title starting with A-U, otherwise 'sector'"),
        "sector_key": Column(each(CODE_LISTS, "Key"),
                             "the key itself, for sectors and the total"),
        "size_key": Column(each(CODE_LISTS, "Title"), "size class parsed from the title, "
                           "e.g. '1 tot 10 werkzame personen' -> WP_1_10 (size rows only)"),
        "size_is_aggregate": Column(each(CODE_LISTS, "Title"),
                                    "true when the class contains other classes of the same "
                                    "table, e.g. 1-9 contains 1-4 and 5-9 (size rows only)"),
        "run_id": RUN_ID,
    },
    "dim_sector": {
        "sector_key": Column(each(CODE_LISTS, "Key") + src(MAP, "member_type"),
                             "the keys map_characteristic marks as sector or total; one row "
                             "per key (CBS uses the same key in every table)"),
        "sbi_code": Column(each(CODE_LISTS, "Title"),
                           "the code at the start of the title: 'C Industrie' -> C"),
        "sector_name": Column(each(CODE_LISTS, "Title"),
                              "the rest of the title: 'C Industrie' -> Industrie"),
        "sbi_level": Column(each(CODE_LISTS, "Title"),
                            "from the code's form: A-U total, C section, B-F section_group, "
                            "47 division, 812 group, 10-12 division_group, ..."),
        "title": Column(each(CODE_LISTS, "Title"),
                        "as given; when tables differ, the most common title"),
        "run_id": RUN_ID,
    },
    "dim_company_size": {
        "size_key": Column(each(CODE_LISTS, "Title") + each(COUNT_PROPS, "Title"),
                           "unit and bounds parsed from a size-class title: WN (employees) or "
                           "WP (persons employed), lower, upper -> e.g. WP_1_10"),
        "size_unit": Column(each(CODE_LISTS, "Title") + each(COUNT_PROPS, "Title"),
                            "'employees' (werknemers) or 'persons_employed' (werkzame personen)"),
        "lower_bound": Column(each(CODE_LISTS, "Title") + each(COUNT_PROPS, "Title"),
                              "the first number of the title ('1 000' -> 1000)"),
        "upper_bound_excl": Column(each(CODE_LISTS, "Title") + each(COUNT_PROPS, "Title"),
                                   "the upper bound, exclusive; empty for 'of meer' (no upper bound)"),
        "size_range": Column(each(CODE_LISTS, "Title") + each(COUNT_PROPS, "Title"),
                             "readable range from the bounds: 1-9, 100+"),
        "size_band": Column(each(CODE_LISTS, "Title") + each(COUNT_PROPS, "Title"),
                            "<10, 10-99 or 100+: the band that contains the class"),
        "title": Column(each(CODE_LISTS, "Title") + each(COUNT_PROPS, "Title"),
                        "as given (spaces normalised)"),
        "run_id": RUN_ID,
    },
    "dim_period": {
        "period_key": Column(each(TYPED.values(), "Perioden"),
                             "every period key that occurs in a fact table"),
        "period_type": Column(each(TYPED.values(), "Perioden"),
                              "from the key: KW quarter, JJ year, MM month, HJ half year"),
        "year": Column(each(TYPED.values(), "Perioden"), "the first four digits"),
        "quarter": Column(each(TYPED.values(), "Perioden"), "the quarter (KW, MM keys)"),
        "month": Column(each(TYPED.values(), "Perioden"), "the month (MM keys)"),
        "start_date": Column(each(TYPED.values(), "Perioden"), "first day of the period"),
        "end_date": Column(each(TYPED.values(), "Perioden"), "last day of the period"),
        "period_label": Column(each(TYPED.values(), "Perioden"),
                               "readable label: 2026 Q2, 2025"),
        "run_id": RUN_ID,
    },
    "dim_region": {
        "region_key": Column(src("81644ned_regios", "Key"), "as given (padding removed)"),
        "region_name": Column(src("81644ned_regios", "Title"),
                              "the title without its level suffix: 'Groningen (PV)' -> Groningen"),
        "region_level": Column(src("81644ned_regios", "Key"), "from the key's prefix: NL "
                               "country, LD landsdeel, PV province, CR corop, GM municipality"),
        "description": Column(src("81644ned_regios", "Description"), "as given"),
        "run_id": RUN_ID,
    },
    "dim_survey_measure": {
        "source_table": Column(each(SURVEY_PROPS, "_table_id"), "the survey table"),
        "measure_key": Column(each(SURVEY_PROPS, "Key", "Type"),
                              "the key of every Topic (measure) in DataProperties"),
        "measure_name": Column(each(SURVEY_PROPS, "Key", "ParentID",
                                    "Title"),
                               "the key in snake_case ('AantalKeerVerzuimd_2' -> "
                               "aantal_keer_verzuimd); a name that occurs twice gets the era of "
                               "its topic group (_until_2022 / _from_2022)"),
        "title": Column(each(SURVEY_PROPS, "Title"), "as given"),
        "topic_group": Column(each(SURVEY_PROPS, "ParentID", "Title"),
                              "the titles of the TopicGroups above the measure (via ParentID), "
                              "joined with ' / '"),
        "unit": Column(each(SURVEY_PROPS, "Unit"), "as given: %, gemiddelde, ..."),
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
        "period_key": Column(src(TYPED["80072NED"], "Perioden"), "as given"),
        "characteristic_key": Column(src(TYPED["80072NED"], "BedrijfskenmerkenSBI2008"),
                                     "as given (padding removed)"),
        **characteristic(),
        "absence_pct": Column(src(TYPED["80072NED"], "Ziekteverzuimpercentage_1"),
                              "as a number: sick days as % of available working days"),
        "period_status": Column(src(TYPED["80072NED"], "_period_status"), STATUS),
        "ingested_at": Column(src(TYPED["80072NED"], "_ingested_at"), INGESTED),
        "run_id": RUN_ID,
    },
    "fct_absence_survey": {
        "source_table": Column(each(SURVEY, "_table_id"),
                               "86009NED or 83130NED: kept apart, never mixed"),
        "period_key": Column(each(SURVEY, "Perioden"), "as given"),
        "characteristic_key": Column(each(SURVEY, "Bedrijfskenmerken"),
                                     "as given (padding removed)"),
        **characteristic(),
        "size_is_aggregate": Column(src(MAP, "size_is_aggregate"),
                                    "looked up by characteristic_key (size classes only)"),
        "measure_key": Column(each(SURVEY, "<measure>"),
                              "the name of the measure field; one row per measure (wide -> long)"),
        "measure_name": Column(src("silver:dim_survey_measure", "measure_name"),
                               "looked up by source_table and measure_key"),
        "value": Column(each(SURVEY, "<measure>", "Marges"),
                        "the measure's value in the record with Marges = MW00000 (the estimate)"),
        "lower_95": Column(each(SURVEY, "<measure>", "Marges"),
                           "the same measure in the record with Marges = MOG0095"),
        "upper_95": Column(each(SURVEY, "<measure>", "Marges"),
                           "the same measure in the record with Marges = MBG0095"),
        "period_status": Column(each(SURVEY, "_period_status"), STATUS),
        "ingested_at": Column(each(SURVEY, "_ingested_at"), INGESTED),
        "run_id": RUN_ID,
    },
    "fct_business_count": counts("81589NED"),
    "fct_establishment_count": counts("81644NED", extra={
        "region_key": Column(src(TYPED["81644NED"], "RegioS"),
                             "as given (padding removed); joins to dim_region")}),
    "kvk_company": {
        "snapshot_date": Column(src(KVK_LOG, "file_date") + src(KVK, "_batch_id", "_ingested_at"),
                                "KVK's export date of the file (batch log, from the zip); "
                                "the day the upload was loaded when the file had no date"),
        "snapshot_date_source": Column(src(KVK_LOG, "file_date") + src(KVK, "_batch_id"),
                                       "kvk_file when the batch log has the file date, "
                                       "else upload"),
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


# --------------------------------------------------------------------------- silver -> gold
# Gold reads only silver, so every source is a silver column. A column computed from
# others (a difference, a rank, last year's value) lists the silver columns it is computed
# from; the "how" says what is done. A fixed value set by gold has no source.
def sv(table: str, *columns: str) -> tuple[Source, ...]:
    return src(f"silver:{table}", *columns)


GOLD_RUN_ID = Column((), "added when the run publishes the table: the gold run that made the "
                         "row; joins to lakehouse/_run_log (which silver run_ids were read)")
RATE, SURVEY_FCT, COUNT_FCT = "fct_absence_rate", "fct_absence_survey", "fct_business_count"


def gold_period(fact: str, quarterly: bool = True, label: bool = True) -> dict[str, Column]:
    """period_key, its label/year/quarter from dim_period, period_status and is_provisional."""
    out = {"period_key": Column(sv(fact, "period_key"), "as in silver")}
    if label:
        out["period_label"] = Column(sv("dim_period", "period_label"), "looked up in dim_period")
    out["year"] = Column(sv("dim_period", "year"), "looked up in dim_period")
    if quarterly:
        out["quarter"] = Column(sv("dim_period", "quarter"), "looked up in dim_period")
    out["period_status"] = Column(sv(fact, "period_status"), "as in silver")
    out["is_provisional"] = Column(sv(fact, "period_status"),
                                   "true unless the status is Definitief (final)")
    return out


def gold_sector(fact: str) -> dict[str, Column]:
    """sector_key from the fact, its code, level and name from dim_sector."""
    return {
        "sector_key": Column(sv(fact, "sector_key"), "as in silver"),
        "sbi_code": Column(sv("dim_sector", "sbi_code"), "looked up in dim_sector"),
        "sbi_level": Column(sv("dim_sector", "sbi_level"), "looked up in dim_sector"),
        "sector_name": Column(sv("dim_sector", "sector_name"), "looked up in dim_sector"),
    }


def comparisons(values: tuple[Source, ...], statuses: tuple[Source, ...], names: dict[str, str],
                rolling: bool = True) -> dict[str, Column]:
    """The comparison columns of add_comparisons (pipeline/gold/build.py); `names` maps its
    generic column names to the ones the table uses."""
    out = {
        "national": Column(values, "the value of the national total (member_type 'total') "
                                   "in the same period"),
        "diff": Column(values, "value minus the national value, in percentage points"),
        "rank": Column(values, "rank within the same period (1 = highest); totals and "
                               "aggregate classes are not ranked"),
        "rank_of": Column(values, "how many were ranked together"),
        "last_year": Column(values, "the value in the same period one year earlier"),
        "last_year_status": Column(statuses, "the status of that period one year earlier"),
        "change": Column(values, "value minus last year's value, in percentage points"),
        "yoy_mixes_status": Column(statuses, "true when the comparison with last year mixes "
                                             "a provisional and a final figure"),
    }
    if rolling:
        out["rolling"] = Column(values, "average of four consecutive quarters (empty "
                                        "when one is missing)")
        out["rolling_has_provisional"] = Column(statuses, "true when one of those four "
                                                          "quarters is provisional")
    return {names.get(k, k): v for k, v in out.items() if names.get(k, k)}


GOLD_LINEAGE: dict[str, dict[str, Column]] = {
    "gold_absence_by_sector": {
        **gold_period(RATE),
        **gold_sector(RATE),
        "absence_pct": Column(sv(RATE, "absence_pct"), "as in silver; rows of sectors and "
                              "the national total (member_type sector or total)"),
        **comparisons(sv(RATE, "absence_pct"), sv(RATE, "period_status"), {
            "national": "national_pct", "diff": "diff_from_national_pp", "rank": "rank_in_level",
            "rank_of": "sectors_in_level", "last_year": "last_year_pct",
            "change": "change_vs_last_year_pp", "rolling": "rolling_4q_pct",
            "rolling_has_provisional": "rolling_4q_has_provisional"}),
        "data_as_of": Column(sv(RATE, "ingested_at"), "when the bronze row behind it was loaded"),
        "run_id": GOLD_RUN_ID,
    },
    "gold_absence_by_size": {
        "source_table": Column(sv(SURVEY_FCT, "source_table"), "'80072NED' for the quarterly "
                               "rows from fct_absence_rate; 86009NED for the survey rows"),
        "measure": Column((), "fixed by gold: absence_pct (80072NED) or "
                              "share_employees_absent (86009NED)"),
        "period_type": Column(sv(SURVEY_FCT, "source_table"), "quarter for 80072NED, "
                              "year for 86009NED"),
        "period_key": Column(sv(RATE, "period_key") + sv(SURVEY_FCT, "period_key"), "as in silver"),
        "period_label": Column(sv("dim_period", "period_label"), "looked up in dim_period"),
        "year": Column(sv("dim_period", "year"), "looked up in dim_period"),
        "quarter": Column(sv("dim_period", "quarter"), "looked up in dim_period (empty for years)"),
        "period_status": Column(sv(RATE, "period_status") + sv(SURVEY_FCT, "period_status"),
                                "as in silver"),
        "is_provisional": Column(sv(RATE, "period_status") + sv(SURVEY_FCT, "period_status"),
                                 "true unless the status is Definitief (final)"),
        "member_type": Column(sv(RATE, "member_type") + sv(SURVEY_FCT, "member_type"),
                              "as in silver: size or total"),
        "size_key": Column(sv(RATE, "size_key") + sv(SURVEY_FCT, "size_key"), "as in silver"),
        "size_range": Column(sv("dim_company_size", "size_range"), "looked up in dim_company_size"),
        "size_unit": Column(sv("dim_company_size", "size_unit"), "looked up in dim_company_size"),
        "lower_bound": Column(sv("dim_company_size", "lower_bound"), "looked up in dim_company_size"),
        "size_band": Column(sv("dim_company_size", "size_band"), "looked up in dim_company_size"),
        "size_is_aggregate": Column(sv(SURVEY_FCT, "size_is_aggregate"),
                                    "as in silver for the survey; false for 80072NED"),
        "value": Column(sv(RATE, "absence_pct") + sv(SURVEY_FCT, "value", "measure_key")
                        + sv("dim_survey_measure", "measure_name"),
                        "absence % (80072NED); for 86009NED the share of employees absent, "
                        "the survey measure picked by its name in dim_survey_measure"),
        "lower_95": Column(sv(SURVEY_FCT, "lower_95"), "as in silver (survey only)"),
        "upper_95": Column(sv(SURVEY_FCT, "upper_95"), "as in silver (survey only)"),
        **comparisons(sv(RATE, "absence_pct") + sv(SURVEY_FCT, "value"),
                      sv(RATE, "period_status") + sv(SURVEY_FCT, "period_status"), {
            "national": "national_value", "diff": "diff_from_national_pp",
            "rank": "rank_in_source", "rank_of": "classes_ranked",
            "last_year": "last_year_value", "change": "change_vs_last_year_pp",
            "rolling": "rolling_4q_value", "rolling_has_provisional": "rolling_4q_has_provisional"}),
        "data_as_of": Column(sv(RATE, "ingested_at") + sv(SURVEY_FCT, "ingested_at"),
                             "when the bronze row behind it was loaded"),
        "run_id": GOLD_RUN_ID,
    },
    "gold_absence_profile": {
        **gold_period(SURVEY_FCT, quarterly=False, label=False),
        "member_type": Column(sv(SURVEY_FCT, "member_type"), "as in silver"),
        "characteristic_key": Column(sv(SURVEY_FCT, "characteristic_key"), "as in silver"),
        **gold_sector(SURVEY_FCT),
        "size_key": Column(sv(SURVEY_FCT, "size_key"), "as in silver"),
        "size_range": Column(sv("dim_company_size", "size_range"), "looked up in dim_company_size"),
        "size_unit": Column(sv("dim_company_size", "size_unit"), "looked up in dim_company_size"),
        "lower_bound": Column(sv("dim_company_size", "lower_bound"), "looked up in dim_company_size"),
        "size_is_aggregate": Column(sv(SURVEY_FCT, "size_is_aggregate"), "as in silver"),
        "measure_key": Column(sv(SURVEY_FCT, "measure_key"), "as in silver (86009NED only)"),
        "measure_name": Column(sv(SURVEY_FCT, "measure_name"), "as in silver"),
        "measure_title": Column(sv("dim_survey_measure", "title"), "looked up in dim_survey_measure"),
        "topic_group": Column(sv("dim_survey_measure", "topic_group"), "looked up in dim_survey_measure"),
        "unit": Column(sv("dim_survey_measure", "unit"), "looked up in dim_survey_measure"),
        "value": Column(sv(SURVEY_FCT, "value"), "as in silver"),
        "lower_95": Column(sv(SURVEY_FCT, "lower_95"), "as in silver"),
        "upper_95": Column(sv(SURVEY_FCT, "upper_95"), "as in silver"),
        "national_value": Column(sv(SURVEY_FCT, "value"), "the national total's value for the "
                                 "same measure and period"),
        "diff_from_national": Column(sv(SURVEY_FCT, "value"), "value minus the national value"),
        "differs_from_national": Column(sv(SURVEY_FCT, "lower_95", "upper_95", "value"),
                                        "true when the whole 95% interval lies above or "
                                        "below the national value"),
        "direction": Column(sv(SURVEY_FCT, "lower_95", "upper_95", "value"),
                            "above or below, when it differs from the national value"),
        "rank_in_topic": Column(sv(SURVEY_FCT, "value") + sv("dim_survey_measure", "topic_group"),
                                "rank of the value among the measures of the same topic "
                                "group (1 = highest)"),
        "data_as_of": Column(sv(SURVEY_FCT, "ingested_at"), "when the bronze row behind it was loaded"),
        "run_id": GOLD_RUN_ID,
    },
    "gold_business_context": {
        **gold_period(COUNT_FCT),
        **gold_sector(COUNT_FCT),
        "size_band": Column(sv(COUNT_FCT, "size_band", "breakdown"),
                            "the size band; 'all' for CBS's total (breakdown total)"),
        "businesses": Column(sv(COUNT_FCT, "count", "size_is_aggregate"),
                             "sum of the counts of the band's size classes (aggregate "
                             "classes left out); empty when one class is missing"),
        "data_as_of": Column(sv(COUNT_FCT, "ingested_at"), "the newest ingest time in the group"),
        "run_id": GOLD_RUN_ID,
    },
    "gold_kvk_context": {
        "snapshot_date": Column(sv("kvk_company", "snapshot_date"), "of the latest snapshot "
                                "(is_latest)"),
        "batch_id": Column(sv("kvk_company", "batch_id"), "of the latest snapshot"),
        "sbi2008_section": Column(sv("kvk_company", "sbi2008_section"),
                                  "as in silver; 'unknown' when not determined"),
        "sector_name": Column(sv("dim_sector", "sector_name"), "the section's name, looked "
                              "up in dim_sector"),
        "legal_form": Column(sv("kvk_company", "legal_form"), "as in silver; 'all' for "
                             "every legal form together"),
        "active_companies": Column(sv("kvk_company", "is_active", "is_latest"),
                                   "number of active companies in the latest snapshot"),
        "companies_with_age": Column(sv("kvk_company", "start_date"),
                                     "how many of them have a valid start date"),
        "median_age_years": Column(sv("kvk_company", "start_date", "snapshot_date"),
                                   "median of (snapshot date - start date), in years"),
        "is_small_group": Column(sv("kvk_company", "is_active", "is_latest"),
                                 "true under 10 companies (KVK re-identification rule)"),
        "run_id": GOLD_RUN_ID,
    },
    "gold_risk_index": {
        "basis": Column(sv(RATE, "period_status"), "'all': every quarter, provisional included; "
                        "'final': final quarters only (the dashboard's switch)"),
        "period_key": Column(sv(RATE, "period_key"), "the latest quarter of the basis"),
        "period_label": Column(sv("dim_period", "period_label"), "looked up in dim_period"),
        "group_type": Column(sv(RATE, "member_type"), "sector or size"),
        "group_key": Column(sv(RATE, "sector_key", "size_key"), "the sector or size class key"),
        "sbi_code": Column(sv("dim_sector", "sbi_code"), "looked up in dim_sector (sectors only)"),
        "sbi_level": Column(sv("dim_sector", "sbi_level"), "looked up in dim_sector (sectors only)"),
        "group_name": Column(sv("dim_sector", "sector_name") + sv("dim_company_size", "size_range"),
                             "the sector's name, or the size class"),
        "absence_4q_pct": Column(sv(RATE, "absence_pct"), "average of the last 4 quarters"),
        "national_4q_pct": Column(sv(RATE, "absence_pct", "member_type"),
                                  "the national total's average of the same 4 quarters"),
        "relative_risk": Column(sv(RATE, "absence_pct", "member_type"),
                                "absence_4q_pct ÷ national_4q_pct; 1.00 = national"),
        "band_low": Column(sv(RATE, "absence_pct", "member_type"),
                           "the lowest quarterly relative risk in the window (8 quarters)"),
        "band_high": Column(sv(RATE, "absence_pct", "member_type"),
                            "the highest quarterly relative risk in the window"),
        "quarters_in_band": Column(sv(RATE, "period_key"), "quarters with figures in the window"),
        "provisional_quarters": Column(sv(RATE, "period_status"),
                                       "how many of those quarters are provisional"),
        "assessment": Column(sv(RATE, "absence_pct", "member_type"),
                             "higher (band wholly above 1), lower (wholly below), around "
                             "national, or too few quarters"),
        "trend_per_year": Column(sv(RATE, "absence_pct", "member_type"),
                                 "slope of a straight line through the window's quarterly "
                                 "relative risks, per year"),
        "trend": Column(sv(RATE, "absence_pct", "member_type"),
                        "rising / falling beyond ±0.05 per year (config.yaml), else stable"),
        "businesses": Column(sv(COUNT_FCT, "count", "breakdown", "sector_key"),
                             "all businesses in the sector, CBS's latest quarter (sectors only; "
                             "joined on sector_key)"),
        "businesses_natural_persons": Column(sv(COUNT_FCT, "count", "breakdown", "legal_form",
                                                "sector_key"),
                                             "businesses run by natural persons (self-employed)"),
        "businesses_one_person": Column(sv(COUNT_FCT, "count", "breakdown", "size_key",
                                           "sector_key"),
                                        "businesses with one person employed"),
        "businesses_period": Column(sv(COUNT_FCT, "period_key"), "the quarter of the counts"),
        "explanation": Column(sv(RATE, "absence_pct") + sv("dim_sector", "sector_name")
                              + sv(COUNT_FCT, "count"),
                              "one sentence that puts the row's numbers in words"),
        "data_as_of": Column(sv(RATE, "ingested_at"), "the newest ingest time in the window"),
        "run_id": GOLD_RUN_ID,
    },
    "gold_risk_index_history": {
        "period_key": Column(sv(RATE, "period_key"), "as in silver"),
        "period_label": Column(sv("dim_period", "period_label"), "looked up in dim_period"),
        "year": Column(sv("dim_period", "year"), "looked up in dim_period"),
        "quarter": Column(sv("dim_period", "quarter"), "looked up in dim_period"),
        "period_status": Column(sv(RATE, "period_status"), "as in silver"),
        "is_provisional": Column(sv(RATE, "period_status"), "true unless the status is Definitief"),
        "group_type": Column(sv(RATE, "member_type"), "sector or size"),
        "group_key": Column(sv(RATE, "sector_key", "size_key"), "the sector or size class key"),
        "sbi_code": Column(sv("dim_sector", "sbi_code"), "looked up in dim_sector (sectors only)"),
        "sbi_level": Column(sv("dim_sector", "sbi_level"), "looked up in dim_sector (sectors only)"),
        "group_name": Column(sv("dim_sector", "sector_name") + sv("dim_company_size", "size_range"),
                             "the sector's name, or the size class"),
        "absence_pct": Column(sv(RATE, "absence_pct"), "as in silver"),
        "national_pct": Column(sv(RATE, "absence_pct", "member_type"),
                               "the national total's absence % in the same quarter"),
        "relative_risk": Column(sv(RATE, "absence_pct", "member_type"),
                                "absence_pct ÷ national_pct (the quarterly relative risk)"),
        "data_as_of": Column(sv(RATE, "ingested_at"), "when the bronze row behind it was loaded"),
        "run_id": GOLD_RUN_ID,
    },
}

# Every table, silver and gold, for following a column back to bronze
LINEAGE: dict[str, dict[str, Column]] = {**SILVER_LINEAGE, **GOLD_LINEAGE}


# --------------------------------------------------------------------------- following back
def bronze_sources(table: str, column: str, _seen=None) -> set[Source]:
    """The bronze fields a silver or gold column comes from, following every silver step
    (e.g. fct_absence_rate.member_type -> map_characteristic -> the code lists)."""
    seen = _seen if _seen is not None else set()
    if (table, column) in seen:
        return set()
    seen.add((table, column))
    out = set()
    for s in LINEAGE[table][column].sources:
        if s.is_silver:
            out |= bronze_sources(s.table.removeprefix("silver:"), s.field, seen)
        else:
            out.add(s)
    return out


# --------------------------------------------------------------------------- diagrams
# Mermaid flowcharts (text that GitHub and the HTML page draw as diagrams). Where several
# bronze tables feed a column through the same field (e.g. the five code lists' Title),
# they are drawn as one box, named after the group, so the diagrams stay readable.
GROUPS = {
    frozenset(CODE_LISTS): "5 code lists of sectors and size classes",
    frozenset(TYPED.values()): "the 5 data tables (TypedDataSet)",
    frozenset(SURVEY): "survey data: 86009NED + 83130NED",
    frozenset(SURVEY_PROPS): "survey DataProperties: 86009NED + 83130NED",
    frozenset(COUNT_PROPS): "DataProperties of 81589NED + 81644NED",
    frozenset(CATEGORY_GROUPS): "3 category-group lists",
}


def _source_label(table: str) -> str:
    """Which source a bronze table belongs to, for the overview's groups."""
    if table.startswith("kvk/"):
        return "KVK file"
    if table.startswith("reference/"):
        return "CBS SBI crosswalk (xlsx)"
    return f"CBS {table.split('_')[0].upper()}"


def _text(s: str) -> str:
    """Text that is safe inside a quoted Mermaid label."""
    return (s.replace('"', "'").replace("<measure>", "(each measure)")
            .replace("<", "‹").replace(">", "›"))


def _id(*parts: str) -> str:
    return "n_" + "".join(ch if ch.isalnum() else "_" for ch in "_".join(parts))


def mermaid_overview() -> str:
    """Sources -> silver tables -> gold tables: one box per source (CBS table, KVK file,
    crosswalk), listing the bronze tables of that source it uses. The per-table diagrams
    show the columns; here only which table feeds which."""
    edges, bronze = set(), {}
    for table, columns in LINEAGE.items():
        layer = "gold" if table in GOLD_LINEAGE else "silver"
        for c in columns.values():
            for s in c.sources:
                if s.is_silver:
                    edges.add((_id("silver", s.table.removeprefix("silver:")), _id(layer, table)))
                else:
                    label = _source_label(s.table)
                    bronze.setdefault(label, set()).add(s.table)
                    edges.add((_id("source", label), _id("silver", table)))
    lines = ["flowchart LR", '  subgraph bronze_layer["bronze"]']
    for label, tables in sorted(bronze.items()):
        listed = "<br/>".join(_text(t) for t in sorted(tables))
        lines.append(f'    {_id("source", label)}["<b>{_text(label)}</b><br/>{listed}"]:::bronze')
    lines.append("  end")
    lines.append('  subgraph silver_layer["silver"]')
    lines += [f'    {_id("silver", t)}["{t}"]:::silver' for t in SILVER_LINEAGE]
    lines.append("  end")
    lines.append('  subgraph gold_layer["gold"]')
    lines += [f'    {_id("gold", t)}["{t}"]:::gold' for t in GOLD_LINEAGE]
    lines.append("  end")
    lines += [f"  {a} --> {b}" for a, b in sorted(edges)]
    return "\n".join(lines + _CLASSES)


def mermaid_table(table: str) -> str:
    """One silver or gold table: the bronze fields and silver columns each column comes
    from. Sources that differ only in their bronze table are grouped into one box."""
    layer = "gold" if table in GOLD_LINEAGE else "silver"
    lines = ["flowchart LR"]
    nodes, edges = {}, []
    for column, c in LINEAGE[table].items():
        # bronze sources are grouped by field; each silver column is its own box
        by_field: dict[tuple, set[str]] = {}
        for s in c.sources:
            key = (True, s.table, s.field) if s.is_silver else (False, s.field)
            by_field.setdefault(key, set()).add(s.table)
        for (is_silver, *_, fld), tables in by_field.items():
            if is_silver:
                (other,) = tables
                label, cls = f"{_text(other.removeprefix('silver:'))}<br/>{_text(fld)}", "silverref"
            else:
                group = GROUPS.get(frozenset(tables))
                name = group if group and len(tables) > 1 else ", ".join(sorted(tables))
                label, cls = f"{_text(name)}<br/>{_text(fld)}", "bronze"
            node = _id("src", *sorted(tables), fld)
            nodes[node] = f'  {node}["{label}"]:::{cls}'
            edges.append(f"  {node} --> {_id('col', column)}")
        if not c.sources and column == "run_id":
            nodes["run"] = f'  run(["the {layer} run (pipeline/common/publish.py)"]):::run'
            edges.append(f"  run --> {_id('col', column)}")
        elif not c.sources:
            nodes["build"] = '  build(["a fixed value set by pipeline/gold/build.py"]):::run'
            edges.append(f"  build --> {_id('col', column)}")
    lines += sorted(nodes.values())
    lines.append(f'  subgraph {_id("tbl", table)}["{layer}: {table}"]')
    lines += [f'    {_id("col", col)}["{col}"]:::{layer}' for col in LINEAGE[table]]
    lines.append("  end")
    return "\n".join(lines + edges + _CLASSES)


_CLASSES = [
    "  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d",
    "  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d",
    "  classDef gold fill:#f6ecb8,stroke:#9c7f12,color:#1d1d1d",
    "  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d",
    "  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e",
]


def html() -> str:
    """docs/lineage.html: all diagrams on one page (opens in any browser; loads the
    Mermaid library from the internet to draw them)."""
    sections = [("Overview: bronze → silver → gold", mermaid_overview())]
    sections += [(f"silver: {t}", mermaid_table(t)) for t in SILVER_LINEAGE]
    sections += [(f"gold: {t}", mermaid_table(t)) for t in GOLD_LINEAGE]
    body = "\n".join(
        f'<section id="{_id("s", title)}"><h2>{title}</h2>'
        f'<pre class="mermaid">\n{diagram}\n</pre></section>' for title, diagram in sections)
    toc = "\n".join(f'<li><a href="#{_id("s", title)}">{title}</a></li>' for title, _ in sections)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Column lineage</title>
<style>
  :root {{ --bg: #fcfcfb; --ink: #0b0b0b; --muted: #52514e; --line: #e1e0d9; }}
  body {{ margin: 0; padding: 24px 16px 64px; background: var(--bg); color: var(--ink);
         font-family: system-ui, -apple-system, "Segoe UI", sans-serif; line-height: 1.5; }}
  main {{ max-width: 1200px; margin: 0 auto; }}
  h1 {{ font-size: 1.6rem; margin: 0 0 4px; }}
  h2 {{ font-size: 1.15rem; margin: 40px 0 8px; padding-top: 16px; border-top: 1px solid var(--line); }}
  p, li {{ color: var(--muted); }}
  .mermaid {{ background: #fff; border: 1px solid var(--line); border-radius: 8px; padding: 12px;
              overflow-x: auto; }}
  .legend span {{ display: inline-block; padding: 2px 8px; margin-right: 8px; border-radius: 4px;
                  border: 1px solid; font-size: .85rem; color: var(--ink); }}
</style>
</head>
<body>
<main>
<h1>Column lineage: bronze → silver → gold</h1>
<p>Where every silver column comes from in bronze, and every gold column in silver. Generated from <code>tools/lineage.py</code>
by <code>python -m tools.lineage</code>; the same lineage as a table:
<a href="lineage.md">lineage.md</a>; explained with examples:
<a href="bronze_to_silver.md">bronze_to_silver.md</a>.</p>
<p class="legend"><span style="background:#f3e3c9;border-color:#a8743a">bronze field</span>
<span style="background:#e4e7ec;border-color:#6b7480">silver column</span>
<span style="background:#f6ecb8;border-color:#9c7f12">gold column</span>
<span style="background:#f2f3f5;border-color:#9aa1ab;border-style:dashed">silver column it is made from</span></p>
<ul>
{toc}
</ul>
{body}
</main>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{ startOnLoad: true, theme: "neutral", flowchart: {{ useMaxWidth: false }} }});</script>
</body>
</html>
"""


# --------------------------------------------------------------------------- the document
def markdown() -> str:
    """docs/lineage.md: one table per silver table."""
    lines = [
        "# Column lineage: bronze → silver → gold",
        "",
        "Where every silver column comes from in bronze, where every gold column comes from "
        "in silver, and what happens to it on the way. To follow a gold column all the way "
        "back to the bronze fields, use the [lineage explorer](lineage_explorer.html).",
        "",
        "> Generated from `tools/lineage.py` by `python -m tools.lineage`; don't edit by hand. "
        "`tests/test_lineage.py` fails when a silver or gold column is missing here or a "
        "named bronze field or silver column doesn't exist. The same lineage is visible in the Dagster UI: "
        "assets `silver_cbs` and `silver_kvk`, the *Columns* list on their overview page "
        "(each description starts with *From …*). For the steps explained with real "
        "examples, see [bronze_to_silver.md](bronze_to_silver.md).",
        "",
        "How to read a source:",
        "",
        "| Source | Meaning |",
        "|---|---|",
        "| `80072ned_typeddataset.Perioden` | a field of the record CBS sent: a column of "
        "bronze table `80072ned_typeddataset` (the whole record is also kept in `raw_json`) |",
        "| `80072ned_typeddataset._period_status` | a column bronze added when loading |",
        "| `86009ned_typeddataset.<measure>` | every measure field of the record "
        "(silver has one row per measure) |",
        "| `kvk/basis_bedrijfsgegevens.Actief` | a column of the KVK file |",
        "| `silver:map_characteristic.member_type` | a silver column (its own lineage "
        "is listed below) |",
        "",
        "Diagrams of the same lineage: below in this file (drawn by GitHub), or all on one page "
        "in [lineage.html](lineage.html) (open it in a browser). Interactive, to follow one "
        "column or field through all steps: [lineage_explorer.html](lineage_explorer.html).",
        "",
        "## Overview: bronze → silver → gold",
        "",
        "```mermaid",
        mermaid_overview(),
        "```",
        "",
        "## Contents",
        "",
    ]
    lines.append("Silver: " + ", ".join(f"[`{t}`](#{t})" for t in SILVER_LINEAGE))
    lines += ["", "Gold: " + ", ".join(f"[`{t}`](#{t})" for t in GOLD_LINEAGE)]
    for table, columns in LINEAGE.items():
        if table in GOLD_LINEAGE:
            script = "pipeline/gold/build.py"
        else:
            script = "pipeline/silver/kvk.py" if table in SILVER_KVK_TABLES else "pipeline/silver/cbs.py"
        layer = "gold" if table in GOLD_LINEAGE else "silver"
        lines += ["", f"## {table}", "", f"A {layer} table, built by `{script}`.", "",
                  "```mermaid", mermaid_table(table), "```", "",
                  "| Column | Comes from | How |", "|---|---|---|"]
        for column, c in columns.items():
            from_ = "<br>".join(f"`{s}`" for s in c.sources) or (
                "*(the run)*" if column == "run_id" else "*(set by gold)*")
            lines.append(f"| `{column}` | {from_} | {c.how} |")
    return "\n".join(lines) + "\n"


def dagster_lineage(asset_tables: set[str]) -> dict:
    """Column lineage and column descriptions for one Dagster asset (silver_cbs,
    silver_kvk or gold), in Dagster's format. Columns are named '<table>.<column>'; bronze
    sources point at the bronze assets, silver sources outside the asset at their
    silver asset.

    The free Dagster UI shows the column list (dagster/column_schema) but not the column
    lineage graph (a Dagster+ feature), so each description starts with its sources."""
    import dagster as dg

    def asset_of(s: Source) -> str:
        if s.is_silver:
            return "silver_kvk" if s.table.removeprefix("silver:") in SILVER_KVK_TABLES else "silver_cbs"
        if s.table.startswith("kvk/"):
            return "kvk_bronze"
        if s.table.startswith("reference/"):
            return "sbi_crosswalk"
        return "cbs_bronze"

    deps, schema = {}, []
    for table in sorted(asset_tables):
        for column, c in LINEAGE[table].items():
            name = f"{table}.{column}"
            origin = ", ".join(str(s) for s in c.sources) or (
                "the run" if column == "run_id" else "gold (a fixed value)")
            schema.append(dg.TableColumn(name, description=f"From {origin} — {c.how}"))
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


# --------------------------------------------------------------------------- explorer
EXPLORER_TEMPLATE = "lineage_explorer_template.html"
EXPLORER_START = "gold:gold_absence_by_sector.absence_pct"   # bronze -> silver -> gold


def _bronze_how(s: Source) -> str:
    if s.table.startswith("kvk/"):
        return "a column of the KVK file, stored as text"
    if s.table.startswith("reference/"):
        return ("a column bronze added when loading the file" if s.field.startswith("_")
                else "a column of the CBS crosswalk file, stored as text")
    if s.field == "<measure>":
        return "every measure field of the records CBS sent (one silver row per measure)"
    if s.field.startswith("_"):
        return "a column bronze added when loading (lineage)"
    return "a field of the record CBS sent, as received (also kept in raw_json)"


def explorer_data() -> dict:
    """Nodes (gold and silver columns, bronze fields, the runs) and edges for the
    explorer page."""
    nodes, edges = {}, []
    for table, columns in LINEAGE.items():
        layer = "gold" if table in GOLD_LINEAGE else "silver"
        for column, c in columns.items():
            cid = f"{layer}:{table}.{column}"
            nodes[cid] = {"id": cid, "layer": layer, "table": table, "name": column,
                          "how": c.how}
            if not c.sources:
                if column == "run_id":
                    rid = f"run:{layer}"
                    nodes[rid] = {"id": rid, "layer": "run", "table": f"the {layer} run",
                                  "name": "pipeline/common/publish.py",
                                  "how": "adds run_id to every row it publishes"}
                else:
                    rid = "run:gold-build"
                    nodes[rid] = {"id": rid, "layer": "run", "table": "the gold build",
                                  "name": "pipeline/gold/build.py", "how": "sets a fixed value"}
                edges.append([rid, cid])
            for s in c.sources:
                sid = f"{s.table}.{s.field}" if s.is_silver else f"bronze:{s.table}.{s.field}"
                if not s.is_silver:
                    nodes[sid] = {"id": sid, "layer": "bronze", "table": s.table,
                                  "name": s.field, "how": _bronze_how(s)}
                edges.append([sid, cid])
    return {"nodes": sorted(nodes.values(), key=lambda n: n["id"]),
            "edges": sorted(edges), "start": EXPLORER_START}


def explorer_html() -> str:
    """docs/lineage_explorer.html: the template with the lineage filled in. Works offline
    (no libraries from the internet)."""
    import json
    from pathlib import Path
    template = (Path(__file__).resolve().parent / EXPLORER_TEMPLATE).read_text(encoding="utf-8")
    data = json.dumps(explorer_data(), ensure_ascii=False, separators=(",", ":"))
    return template.replace("__DATA__", data.replace("</", "<\\/"))


if __name__ == "__main__":
    from pathlib import Path
    docs = Path(__file__).resolve().parents[1] / "docs"
    (docs / "lineage.md").write_text(markdown(), encoding="utf-8")
    (docs / "lineage.html").write_text(html(), encoding="utf-8")
    (docs / "lineage_explorer.html").write_text(explorer_html(), encoding="utf-8")
    print(f"Wrote {docs / 'lineage.md'}, lineage.html and lineage_explorer.html "
          f"({sum(len(c) for c in LINEAGE.values())} columns, {len(SILVER_LINEAGE)} silver and "
          f"{len(GOLD_LINEAGE)} gold tables)")
