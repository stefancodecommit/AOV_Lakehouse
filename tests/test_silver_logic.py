"""Silver CBS: parsers, dimensions, facts and the model's rules. Offline, pandas only."""
from datetime import date

import pandas as pd
import pytest

import cbs_silver as s
from conftest import INGESTED_AT, raw


# =========================================================================== parsers
@pytest.mark.parametrize("title, expected", [
    ("1 tot 10 werkzame personen", ("WP", 1, 10)),
    ("10 tot 100  werknemers", ("WN", 10, 100)),           # CBS double space
    ("500 tot 1 000 werknemers", ("WN", 500, 1000)),       # thousands with a space
    ("1 000 of meer werknemers", ("WN", 1000, None)),
    ("100 werkzame personen of meer", ("WP", 100, None)),
    ("10 of meer werkzame personen", ("WP", 10, None)),
    ("0 werkzame personen", ("WP", 0, 1)),
    ("1 werkzaam persoon", ("WP", 1, 2)),
    ("1 werkzame persoon", ("WP", 1, 2)),
    ("2 werkzame personen", ("WP", 2, 3)),
    ("A Landbouw, bosbouw en visserij", None),
    ("10-12 Voedings-, genotmiddelenindustrie", None),
    ("1 tot 5 werkdagen", None),                           # duration, not a size class
    ("942 Werknemersorganisaties", None),                  # a sector (trade unions)
    ("9420 Werknemersorganisaties", None),
])
def test_parse_size(title, expected):
    assert s.parse_size(title) == expected


@pytest.mark.parametrize("lo, hi, band", [
    (0, 1, "<10"), (1, 10, "<10"), (2, 10, "<10"), (10, 20, "10-99"), (10, 100, "10-99"),
    (100, None, "100+"), (500, 1000, "100+"), (10, None, None), (0, 50, None), (0, 250, None),
])
def test_size_band_only_when_class_fits_in_one_band(lo, hi, band):
    assert s.size_band(lo, hi) == band


@pytest.mark.parametrize("lo, hi, label", [(0, 1, "0"), (3, 5, "3-4"), (100, None, "100+")])
def test_size_range(lo, hi, label):
    assert s.size_range(lo, hi) == label


def test_size_key():
    assert s.size_key("WN", 1000, None) == "WN_1000_plus"
    assert s.size_key("WP", 1, 10) == "WP_1_10"


def test_aggregate_flags_mark_classes_containing_others():
    classes = [(1, 10), (1, 5), (5, 10), (100, None), (100, 250), (250, None)]
    assert s.aggregate_flags(classes) == [True, False, False, True, False, False]


def test_aggregate_flags_disjoint_classes_are_not_aggregates():
    assert s.aggregate_flags([(1, 10), (10, 100), (100, None)]) == [False, False, False]


@pytest.mark.parametrize("title, code, level", [
    ("A-U Alle economische activiteiten", "A-U", "total"),
    ("C Industrie", "C", "section"),
    ("B-F Nijverheid en energie", "B-F", "section_group"),
    ("01 Landbouw", "01", "division"),
    ("812 Schoonmaakbedrijven", "812", "group"),
    ("0111 Teelt van granen en peulvruchten", "0111", "class"),
    ("01131 Teelt van groenten in volle grond", "01131", "subclass"),
    ("10-12 Voedings-, genotmiddelenindustrie", "10-12", "division_group"),
    ("24-30, 33 Metaal-elektro industrie", "24-30, 33", "division_group"),
    ("Business Economy, B-N, excl. K, incl. 95", None, "aggregate"),
    ("Totaal (ex. financieel of nutsbedrijven)", None, "aggregate"),
])
def test_parse_sector(title, code, level):
    got_code, name, got_level = s.parse_sector(title)
    assert (got_code, got_level) == (code, level)
    assert name and not name.startswith(" ")


@pytest.mark.parametrize("key, ptype, start, end, label", [
    ("2024KW01", "quarter", date(2024, 1, 1), date(2024, 3, 31), "2024 Q1"),
    ("2024KW04", "quarter", date(2024, 10, 1), date(2024, 12, 31), "2024 Q4"),
    ("2024JJ00", "year", date(2024, 1, 1), date(2024, 12, 31), "2024"),
    ("2024MM02", "month", date(2024, 2, 1), date(2024, 2, 29), "2024-02"),  # leap year
    ("2024HJ02", "half_year", date(2024, 7, 1), date(2024, 12, 31), "2024 H2"),
])
def test_parse_period(key, ptype, start, end, label):
    p = s.parse_period(key)
    assert (p["period_type"], p["start_date"], p["end_date"], p["period_label"]) == (ptype, start, end, label)


def test_parse_period_quarter_number_and_unknown_keys():
    assert s.parse_period("2025KW03")["quarter"] == 3
    assert s.parse_period("2025MM11")["quarter"] == 4
    assert s.parse_period("2025X000")["period_type"] == "other"


@pytest.mark.parametrize("key, name", [
    ("AandeelWerknemersDieHebbenVerzuimd_1", "aandeel_werknemers_die_hebben_verzuimd"),
    ("k_1Tot5Werkdagen_5", "k_1_tot_5_werkdagen"),
    ("k_210WerkdagenOfMeer_8", "k_210_werkdagen_of_meer"),
    ("NVSEnBVS_8", "nvs_en_bvs"),
    ("Ziekteverzuimpercentage_1", "ziekteverzuimpercentage"),
    ("LangAchterDeComputerWerken_58", "lang_achter_de_computer_werken"),
])
def test_snake(key, name):
    assert s.snake(key) == name


@pytest.mark.parametrize("key, level", [
    ("NL01", "country"), ("LD03", "landsdeel"), ("LDG4", "landsdeel"),
    ("PV20", "province"), ("GM0014", "municipality"), ("CR01", "corop"), ("XX99", "other")])
def test_region_level(key, level):
    assert s.region_level(key) == level


# =========================================================================== bronze parsing
def test_parse_raw_trims_padded_keys_and_drops_row_id():
    df = s.parse_raw(raw([{"ID": 7, "Bedrijfskenmerken": "301000 ", "Perioden": "2024KW01 ", "v": 1.5}]))
    assert df.loc[0, "Bedrijfskenmerken"] == "301000" and df.loc[0, "Perioden"] == "2024KW01"
    assert "ID" not in df and df.loc[0, "v"] == 1.5
    assert {"_period_status", "_ingested_at"} <= set(df.columns)


def test_parse_raw_keeps_id_for_metadata_tables():
    df = s.parse_raw(raw([{"ID": 3, "ParentID": None, "Title": "x"}]), drop_id=False)
    assert df.loc[0, "ID"] == 3


def test_parse_raw_empty():
    assert s.parse_raw(pd.DataFrame()).empty


def test_latest_keeps_most_recent_ingest():
    df = pd.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3],
                       "_ingested_at": [INGESTED_AT, INGESTED_AT + pd.Timedelta(days=1), INGESTED_AT]})
    out = s.latest(df, ["k"]).sort_values("k")
    assert out["v"].tolist() == [2, 3]


def test_topics_builds_group_path(fake_read):
    t = s.topics(fake_read("86009NED", "DataProperties")).set_index("measure_key")
    assert t.at["Rug_9", "topic_group"] == (
        "Werknemers met verzuim in afgelopen jaar / Klacht meest recente verzuim (tot 2022)")
    assert t.at["AandeelWerknemersDieHebbenVerzuimd_1", "topic_group"] == "Werknemers"
    assert "Bedrijfskenmerken" not in t.index  # dimensions are not measures


# =========================================================================== dimensions
def test_map_characteristic_member_types(silver):
    m = silver["map_characteristic"].set_index(["source_table", "characteristic_key"])
    assert m.at[("80072NED", "T001081"), "member_type"] == "total"
    assert m.at[("80072NED", "301000"), "member_type"] == "sector"
    assert m.at[("80072NED", "WP19078"), "member_type"] == "size"
    assert m.at[("80072NED", "WP19078"), "size_key"] == "WP_1_10"
    assert m.at[("80072NED", "WP19078"), "category_group"] == "Bedrijfsgrootte"
    assert m.at[("81589NED", "800011"), "member_type"] == "sector"  # aggregate without SBI code


def test_map_characteristic_aggregate_flags_are_per_source_table(silver):
    m = silver["map_characteristic"].set_index(["source_table", "characteristic_key"])
    assert m.at[("86009NED", "WN19044"), "size_is_aggregate"]       # 1-10 contains 1-5 and 5-10
    assert not m.at[("86009NED", "WN19041"), "size_is_aggregate"]
    assert not m.at[("83130NED", "WN19044"), "size_is_aggregate"]   # no sub-classes in 83130 sample


def test_dim_sector_is_conformed_on_cbs_key(silver):
    d = silver["dim_sector"]
    assert d["sector_key"].is_unique
    row = d.set_index("sector_key").loc["301000"]
    assert (row["sbi_code"], row["sector_name"], row["sbi_level"]) == (
        "A", "Landbouw, bosbouw en visserij", "section")
    assert not d["sector_key"].str.startswith(("WP", "WN")).any()   # size classes excluded


def test_dim_company_size(silver):
    d = silver["dim_company_size"].set_index("size_key")
    assert d["size_unit"].isin(["employees", "persons_employed"]).all()
    assert d.at["WN_500_1000", "size_band"] == "100+"
    assert pd.isna(d.at["WP_10_plus", "size_band"])                 # spans two bands
    assert {"WP_0_1", "WP_1_2", "WP_0_50", "WN_1_5"} <= set(d.index)


def test_dim_region(silver):
    d = silver["dim_region"].set_index("region_key")
    assert d.at["PV20", "region_name"] == "Groningen"               # '(PV)' suffix removed
    assert d.at["PV20", "region_level"] == "province"
    assert d.at["NL01", "region_level"] == "country"


def test_dim_period_covers_every_fact_period(silver):
    known = set(silver["dim_period"]["period_key"])
    for name in ("fct_absence_rate", "fct_absence_survey", "fct_business_count",
                 "fct_establishment_count"):
        assert set(silver[name]["period_key"]) <= known, name


def test_survey_measure_names_unique_and_era_suffixed(silver):
    d = silver["dim_survey_measure"]
    assert not d.duplicated(["source_table", "measure_name"]).any()
    names = set(d.loc[d["source_table"] == "86009NED", "measure_name"])
    assert {"rug_until_2022", "rug_from_2022"} <= names


def test_survey_sources_never_share_a_measure_name(silver):
    """83130NED and 86009NED averages use different populations; names must not collide."""
    d = silver["dim_survey_measure"]
    a = set(d.loc[d["source_table"] == "86009NED", "measure_name"])
    b = set(d.loc[d["source_table"] == "83130NED", "measure_name"])
    assert a and b and not (a & b)


# =========================================================================== facts
def test_fct_absence_rate(silver):
    f = silver["fct_absence_rate"]
    assert len(f) == 8
    row = f[(f.period_key == "2024KW04") & (f.characteristic_key == "T001081")].iloc[0]
    assert row["absence_pct"] == 4.5 and row["member_type"] == "total"
    assert row["period_status"] == "Voorlopig"
    size = f[f.characteristic_key == "WP19078"].iloc[0]
    assert size["size_key"] == "WP_1_10" and pd.isna(size["sector_key"])


def test_unknown_keys_are_kept_and_flagged(silver):
    f = silver["fct_absence_rate"]
    unknown = f[f.characteristic_key == "999999"]
    assert len(unknown) == 2 and (unknown["member_type"] == "unknown").all()


def test_fct_absence_survey_pivots_margins(silver):
    f = silver["fct_absence_survey"]
    row = f[(f.source_table == "86009NED") & (f.period_key == "2023JJ00")
            & (f.characteristic_key == "T001081")
            & (f.measure_key == "AandeelWerknemersDieHebbenVerzuimd_1")]
    assert len(row) == 1
    assert row[["value", "lower_95", "upper_95"]].iloc[0].tolist() == [40.0, 39.0, 41.0]
    assert "Marges" not in f.columns


def test_fct_absence_survey_grain_is_unique(silver):
    f = silver["fct_absence_survey"]
    assert not f.duplicated(["source_table", "period_key", "characteristic_key", "measure_key"]).any()
    # 86009: 3 characteristics x 2 years x 5 measures; 83130: 1 x 1 x 2
    assert len(f) == 3 * 2 * 5 + 2


def test_fct_absence_survey_keeps_cbs_nulls(silver):
    f = silver["fct_absence_survey"]
    rug = f[(f.characteristic_key == "301000") & f.measure_key.isin(["Rug_9", "Rug_28"])]
    assert len(rug) == 4 and rug[["value", "lower_95", "upper_95"]].isna().all().all()


def test_fct_business_count_breakdowns(silver):
    f = silver["fct_business_count"]
    t = f[(f.sector_key == "T001081")]
    assert set(t["breakdown"]) == {"total", "size", "legal_form"}
    assert t.loc[t.breakdown == "total", "count"].tolist() == [1000]
    assert str(f["count"].dtype) == "Int64"
    sizes = t[t.breakdown == "size"].set_index("size_key")
    assert sizes.at["WP_0_50", "size_is_aggregate"]                 # 0-50 contains 1, 2, 3-5, ...
    assert not sizes.at["WP_3_5", "size_is_aggregate"]
    assert set(t.loc[t.breakdown == "legal_form", "legal_form"]) == {"Natuurlijke personen", "Rechtspersonen"}


def test_fct_business_count_size_rows_sum_without_aggregates(silver):
    f = silver["fct_business_count"]
    base = f[(f.breakdown == "size") & (f.size_is_aggregate == False)]  # noqa: E712
    assert base.groupby("sector_key")["size_key"].nunique().eq(6).all()


def test_fct_establishment_count_has_regions(silver):
    f = silver["fct_establishment_count"]
    assert set(f["region_key"]) == {"NL01", "PV20"}
    total_nl = f[(f.region_key == "NL01") & (f.breakdown == "total")]["count"].tolist()
    assert total_nl == [10]
    assert not f.duplicated(["period_key", "sector_key", "region_key", "measure_key"]).any()


def test_fct_counts_require_dataproperties(bronze, fake_read):
    del bronze[("81589NED", "DataProperties")]
    with pytest.raises(RuntimeError, match="DataProperties"):
        s.build_all(read=fake_read)


# =========================================================================== whole build
EXPECTED_COLUMNS = {
    "dim_period": {"period_key", "period_type", "year", "quarter", "start_date", "end_date"},
    "dim_sector": {"sector_key", "sbi_code", "sector_name", "sbi_level"},
    "dim_company_size": {"size_key", "size_unit", "lower_bound", "upper_bound_excl", "size_band"},
    "dim_region": {"region_key", "region_name", "region_level"},
    "dim_survey_measure": {"source_table", "measure_key", "measure_name", "unit"},
    "map_characteristic": {"source_table", "characteristic_key", "member_type", "sector_key", "size_key"},
    "map_sbi2008_sbi2025": {"sbi2008_code", "sbi2025_code", "sbi2008_division", "sbi2025_division",
                            "link_type", "sbi2025_targets", "sbi2008_sources", "source_version"},
    "fct_absence_rate": {"period_key", "characteristic_key", "absence_pct", "period_status"},
    "fct_absence_survey": {"source_table", "measure_name", "value", "lower_95", "upper_95"},
    "fct_business_count": {"period_key", "sector_key", "breakdown", "count"},
    "fct_establishment_count": {"period_key", "sector_key", "region_key", "breakdown", "count"},
}


def test_build_all_produces_every_table_with_expected_columns(silver):
    assert set(silver) == set(EXPECTED_COLUMNS)
    for name, cols in EXPECTED_COLUMNS.items():
        assert not silver[name].empty, name
        assert cols <= set(silver[name].columns), (name, cols - set(silver[name].columns))


def test_fact_sector_keys_exist_in_dim_sector(silver):
    sectors = set(silver["dim_sector"]["sector_key"])
    for name in ("fct_absence_rate", "fct_absence_survey"):
        f = silver[name]
        keys = set(f.loc[f.member_type.isin(["sector", "total"]), "sector_key"])
        assert keys <= sectors, name
    for name in ("fct_business_count", "fct_establishment_count"):
        assert set(silver[name]["sector_key"]) <= sectors, name


def test_no_padded_keys_anywhere(silver):
    for name, df in silver.items():
        for col in df.columns:
            if col.endswith("_key"):
                vals = df[col].dropna().astype(str)
                assert (vals == vals.str.strip()).all(), (name, col)


def test_missing_bronze_tables_give_empty_tables_not_errors(bronze, fake_read):
    for key in [k for k in bronze if k[0] == "80072NED"]:
        del bronze[key]
    out = s.build_all(read=fake_read)
    assert out["fct_absence_rate"].empty
    assert not out["fct_absence_survey"].empty
