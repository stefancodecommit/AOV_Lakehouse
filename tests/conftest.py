"""Shared fixtures. All tests are offline (no CBS API calls) except those marked `live`.

The sample records below are copied from real CBS OData responses (Oct 2026),
including CBS's habit of padding dimension keys with spaces ('301000 ').
"""
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cbs_bronze  # noqa: E402
import cbs_silver  # noqa: E402
import kvk_bronze_agent  # noqa: E402
import gold  # noqa: E402
import kvk_silver  # noqa: E402
import maintenance  # noqa: E402
import runlog  # noqa: E402
import reference_bronze  # noqa: E402

INGESTED_AT = pd.Timestamp("2026-10-05 09:00", tz="UTC")
MARGES = ["MW00000", "MOG0095", "MBG0095"]


# --------------------------------------------------------------------------- safety
@pytest.fixture(autouse=True)
def isolated_lakehouse(tmp_path, monkeypatch):
    """Point every lakehouse path at a temp folder so tests never touch real data."""
    monkeypatch.setattr(cbs_bronze, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(cbs_bronze, "LOG_TABLE", tmp_path / "bronze" / "_ingestion_log")
    monkeypatch.setattr(cbs_bronze, "QUARANTINE_ROOT", tmp_path / "quarantine")
    monkeypatch.setattr(cbs_bronze, "PAUSE_SECONDS", 0)
    monkeypatch.setattr(cbs_silver, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(cbs_silver, "SILVER_ROOT", tmp_path / "silver")
    monkeypatch.setattr(cbs_silver, "LAKEHOUSE", tmp_path)
    monkeypatch.setattr(cbs_silver, "RUN_LOG", tmp_path / "_run_log")
    monkeypatch.setattr(runlog, "RUN_LOG", tmp_path / "_run_log")
    monkeypatch.setattr(kvk_silver, "LAKEHOUSE", tmp_path)
    monkeypatch.setattr(kvk_silver, "KVK_BRONZE", tmp_path / "bronze" / "kvk")
    monkeypatch.setattr(kvk_silver, "SILVER_ROOT", tmp_path / "silver")
    monkeypatch.setattr(kvk_silver, "RUN_LOG", tmp_path / "_run_log")
    monkeypatch.setattr(gold, "LAKEHOUSE", tmp_path)
    monkeypatch.setattr(gold, "SILVER_ROOT", tmp_path / "silver")
    monkeypatch.setattr(gold, "GOLD_ROOT", tmp_path / "gold")
    monkeypatch.setattr(gold, "RUN_LOG", tmp_path / "_run_log")
    ref = tmp_path / "bronze" / "reference"
    monkeypatch.setattr(reference_bronze, "REF_ROOT", ref)
    monkeypatch.setattr(reference_bronze, "DATA_TABLE", ref / "sbi2008_sbi2025")
    monkeypatch.setattr(reference_bronze, "LOG_TABLE", ref / "_batch_log")
    monkeypatch.setattr(reference_bronze, "DOWNLOAD_DIR", tmp_path / "inbox")
    monkeypatch.setattr(reference_bronze, "QUARANTINE_ROOT", tmp_path / "quarantine")
    monkeypatch.setattr(kvk_bronze_agent, "KVK_ROOT", tmp_path / "bronze" / "kvk")
    monkeypatch.setattr(kvk_bronze_agent, "DATA_TABLE", tmp_path / "bronze" / "kvk" / "basis_bedrijfsgegevens")
    monkeypatch.setattr(kvk_bronze_agent, "LOG_TABLE", tmp_path / "bronze" / "kvk" / "_batch_log")
    monkeypatch.setattr(kvk_bronze_agent, "QUARANTINE_ROOT", tmp_path / "quarantine")
    monkeypatch.setattr(kvk_bronze_agent, "INBOX", tmp_path / "inbox" / "kvk")
    monkeypatch.setattr(maintenance, "LAKEHOUSE", tmp_path)
    monkeypatch.setattr(maintenance, "QUARANTINE_ROOT", tmp_path / "quarantine")
    monkeypatch.setattr(maintenance, "BATCH_LOGS", {
        "cbs": tmp_path / "bronze" / "_ingestion_log",
        "kvk": tmp_path / "bronze" / "kvk" / "_batch_log",
        "reference": tmp_path / "bronze" / "reference" / "_batch_log"})

    # Safety net: no module-level path may still point into the real lakehouse or inbox
    import config
    real = [config.project_path(config.settings["paths"][k]) for k in ("lakehouse", "inbox")]
    for mod in (cbs_bronze, cbs_silver, reference_bronze, kvk_bronze_agent, maintenance, runlog,
                kvk_silver, gold):
        for name, value in vars(mod).items():
            values = value.values() if isinstance(value, dict) else [value]
            for v in values:
                if isinstance(v, Path) and any(v == r or r in v.parents for r in real):
                    pytest.fail(f"{mod.__name__}.{name} still points at real data: {v}")


# --------------------------------------------------------------------------- builders
def raw(rows, status=None):
    """Rows as they sit in a bronze table: raw_json plus lineage columns."""
    return pd.DataFrame({"_period_status": [status] * len(rows),
                         "_ingested_at": [INGESTED_AT] * len(rows),
                         "raw_json": [json.dumps(r) for r in rows]})


def code_list(pairs, groups=None):
    """CBS dimension members, keys padded to 7 characters like the real API."""
    return [{"Key": k.ljust(7), "Title": t, "Description": None,
             "CategoryGroupID": (groups or {}).get(k)} for k, t in pairs]


def dataprops(rows):
    """(ID, ParentID, Type, Key, Title, Unit) tuples -> DataProperties records."""
    return [dict(zip(["ID", "ParentID", "Type", "Key", "Title", "Unit"], r)) for r in rows]


SECTORS_80072 = [
    ("T001081", "A-U Alle economische activiteiten"), ("301000", "A Landbouw, bosbouw en visserij"),
    ("300003", "B-F Nijverheid en energie"), ("307610", "10-12 Voedings-, genotmiddelenindustrie"),
    ("328110", "24-30, 33 Metaal-elektro industrie"), ("415300", "812 Schoonmaakbedrijven"),
    ("WP19078", "1 tot 10 werkzame personen"), ("WP19091", "10 tot 100 werkzame personen"),
    ("WP19098", "100 of meer werkzame personen")]
SECTORS_86009 = [
    ("T001081", "A-U Alle economische activiteiten"), ("301000", "A Landbouw, bosbouw en visserij"),
    ("WN19044", "1 tot 10 werknemers"), ("WN19041", "1 tot 5 werknemers"),
    ("WN19049", "5 tot 10 werknemers"), ("WN19051", "10 tot 100  werknemers"),
    ("WN19060", "100 of meer werknemers"), ("WN19070", "500 tot 1 000 werknemers"),
    ("WN19071", "1 000 of meer werknemers")]
SECTORS_81589 = [
    ("T001081", "A-U Alle economische activiteiten"), ("800011", "Business Economy, B-N, excl. K, incl. 95"),
    ("301000", "A Landbouw, bosbouw en visserij"), ("301100", "01 Landbouw"),
    ("301200", "011 Teelt van eenjarige gewassen"), ("301300", "0111 Teelt van granen en peulvruchten"),
    ("301402", "01131 Teelt van groenten in volle grond")]

DP_86009 = dataprops([
    (0, None, "Dimension", "Bedrijfskenmerken", "Bedrijfskenmerken", None),
    (3, None, "TopicGroup", None, "Werknemers", None),
    (4, 3, "Topic", "AandeelWerknemersDieHebbenVerzuimd_1", "Aandeel werknemers die hebben verzuimd", "%"),
    (5, None, "TopicGroup", None, "Werknemers met verzuim in afgelopen jaar", None),
    (7, 5, "Topic", "AantalWerkdagenVerzuimd_3", "Aantal werkdagen verzuimd", "gemiddelde"),
    (9, 5, "TopicGroup", None, "Duur meest recente verzuimgeval", None),
    (10, 9, "Topic", "k_1Tot5Werkdagen_5", "1 tot 5 werkdagen", "%"),
    (14, 5, "TopicGroup", None, "Klacht meest recente verzuim (tot 2022)", None),
    (15, 14, "Topic", "Rug_9", "Rug", "%"),
    (30, 5, "TopicGroup", None, "Klacht meest recente verzuim (va. 2022)", None),
    (35, 30, "Topic", "Rug_28", "Rug", "%")])
DP_83130 = dataprops([
    (2, None, "Topic", "AandeelWerknemersDatHeeftVerzuimd_1", "Aandeel werknemers dat heeft verzuimd", "%"),
    (3, None, "Topic", "GemiddeldeVerzuimduur_3", "Gemiddelde verzuimduur", "aantal werkdagen")])
DP_81589 = dataprops(
    [(2, None, "Topic", "TotaalBedrijven_1", "Totaal bedrijven", "aantal"),
     (3, None, "TopicGroup", None, "Bedrijfsgrootte", None)]
    + [(i + 4, 3, "Topic", k, t, "aantal") for i, (k, t) in enumerate([
        ("k_1WerkzaamPersoon_2", "1 werkzaam persoon"), ("k_2WerkzamePersonen_3", "2 werkzame personen"),
        ("k_3Tot5WerkzamePersonen_4", "3 tot 5 werkzame personen"),
        ("k_5Tot10WerkzamePersonen_5", "5 tot 10 werkzame personen"),
        ("k_10Tot20WerkzamePersonen_6", "10 tot 20 werkzame personen"),
        ("k_100WerkzamePersonenOfMeer_9", "100 werkzame personen of meer"),
        ("k_0Tot50WerkzamePersonen_10", "0 tot 50 werkzame personen")])]
    + [(14, None, "TopicGroup", None, "Rechtsvorm", None),
       (15, 14, "Topic", "NatuurlijkePersonen_12", "Natuurlijke personen", "aantal"),
       (16, 14, "Topic", "Rechtspersonen_13", "Rechtspersonen", "aantal")])
DP_81644 = dataprops([
    (3, None, "Topic", "Vestigingen_1", "Vestigingen", "aantal"),
    (4, None, "TopicGroup", None, "Vestigingsgrootte", None),
    (5, 4, "Topic", "k_0WerkzamePersonen_2", "0 werkzame personen", "aantal"),
    (6, 4, "Topic", "k_1WerkzamePersoon_3", "1 werkzame persoon", "aantal"),
    (7, 4, "Topic", "k_2Tot10WerkzamePersonen_4", "2 tot 10 werkzame personen", "aantal"),
    (8, 4, "Topic", "k_10OfMeerWerkzamePersonen_5", "10 of meer werkzame personen", "aantal"),
    (9, None, "TopicGroup", None, "Rechtsvorm", None),
    (12, 9, "Topic", "NVSEnBVS_8", "NV's en BV's", "aantal")])


def topic_keys(dp):
    return [r["Key"] for r in dp if r["Type"] == "Topic"]


def survey_rows(keys, periods, measures, null_for=()):
    """Survey records: value 40+j, lower bound one less, upper bound one more."""
    rows, i = [], 0
    for k in keys:
        for p in periods:
            for m in MARGES:
                offset = {"MW00000": 0, "MOG0095": -1, "MBG0095": 1}[m]
                rows.append({"ID": i, "Bedrijfskenmerken": k.ljust(7), "Marges": m, "Perioden": p,
                             **{ms: None if (k, ms) in null_for else 40 + offset + j
                                for j, ms in enumerate(measures)}})
                i += 1
    return rows


def sample_bronze():
    absence = [{"ID": i, "BedrijfskenmerkenSBI2008": k.ljust(7), "Perioden": p,
                "Ziekteverzuimpercentage_1": round(4.5 + i / 10, 1)}
               for i, (k, p) in enumerate([(k, p) for k in ["T001081", "301000", "WP19078", "999999"]
                                           for p in ["2024KW04", "2025KW01"]])]
    business = [{"ID": i, "BedrijfstakkenBranchesSBI2008": k.ljust(7), "Perioden": "2025KW03",
                 **{key: 1000 * (i + 1) + j for j, key in enumerate(topic_keys(DP_81589))}}
                for i, k in enumerate(["T001081", "301000", "301402"])]
    establishments = [{"ID": i, "BedrijfstakkenBranchesSBI2008": "T001081", "RegioS": r,
                       "Perioden": "2025JJ00",
                       **{key: 10 * (i + 1) + j for j, key in enumerate(topic_keys(DP_81644))}}
                      for i, r in enumerate(["NL01  ", "PV20  "])]
    return {
        ("80072NED", "BedrijfskenmerkenSBI2008"): raw(code_list(SECTORS_80072, {"WP19078": 5})),
        ("80072NED", "CategoryGroups"): raw([
            {"ID": 1, "DimensionKey": "BedrijfskenmerkenSBI2008", "Title": "Totaal", "ParentID": 0},
            {"ID": 5, "DimensionKey": "BedrijfskenmerkenSBI2008", "Title": "Bedrijfsgrootte", "ParentID": 0}]),
        ("80072NED", "TypedDataSet"): raw(absence, "Voorlopig"),
        ("86009NED", "Bedrijfskenmerken"): raw(code_list(SECTORS_86009)),
        ("86009NED", "DataProperties"): raw(DP_86009),
        ("86009NED", "TypedDataSet"): raw(survey_rows(
            ["T001081", "301000", "WN19044"], ["2022JJ00", "2023JJ00"], topic_keys(DP_86009),
            null_for={("301000", "Rug_9"), ("301000", "Rug_28")}), "Definitief"),
        ("83130NED", "Bedrijfskenmerken"): raw(code_list(SECTORS_86009[:3])),
        ("83130NED", "DataProperties"): raw(DP_83130),
        ("83130NED", "TypedDataSet"): raw(survey_rows(["T001081"], ["2023JJ00"],
                                                      topic_keys(DP_83130)), "Definitief"),
        ("81589NED", "BedrijfstakkenBranchesSBI2008"): raw(code_list(SECTORS_81589)),
        ("81589NED", "DataProperties"): raw(DP_81589),
        ("81589NED", "TypedDataSet"): raw(business, "Voorlopig"),
        ("81644NED", "BedrijfstakkenBranchesSBI2008"): raw(code_list(SECTORS_81589[:3])),
        ("81644NED", "RegioS"): raw([
            {"Key": "NL01  ", "Title": "Nederland", "Description": None, "CategoryGroupID": None},
            {"Key": "PV20  ", "Title": "Groningen (PV)", "Description": "Provincie", "CategoryGroupID": None},
            {"Key": "GM0014", "Title": "Groningen", "Description": None, "CategoryGroupID": None}]),
        ("81644NED", "DataProperties"): raw(DP_81644),
        ("81644NED", "TypedDataSet"): raw(establishments, "Voorlopig"),
    }


# Real v2.0 crosswalk rows (reference_bronze.py), as they sit in bronze
CROSSWALK = [
    ["0111", "01", "Teelt van granen, peulvruchten en oliehoudende zaden", "01110", "01",
     "Teelt van andere granen dan rijst", "Teelt van granen (excl. rijst)"],
    ["01199", "01", "Teelt van overige eenjarige gewassen (rest)", "01199", "01",
     "Teelt van overige eenjarige gewassen (rest)", "Teelt van overige eenjarige gewassen"],
    ["01199", "01", "Teelt van overige eenjarige gewassen (rest)", "01120", "01",
     "Teelt van rijst", "Teelt van rijst (in principe leeg)"],
    ["74103", "74", "Interieur- en ruimtelijk ontwerp", "71111", "74",
     "Activiteiten van architecten", "Ruimtelijk ontwerp"]]
CROSSWALK_BATCH = "sbi2008_sbi2025_test"


def sample_crosswalk():
    df = pd.DataFrame(CROSSWALK, columns=list(reference_bronze.COLUMNS))
    return df.assign(_batch_id=CROSSWALK_BATCH, _ingested_at=INGESTED_AT, _source_version="v2.0",
                     _row_number=range(1, len(df) + 1))


def table_info(frequency="Perkwartaal", modified="2026-09-15T02:00:00",
               status="Regulier", metadata="2026-09-15T02:00:00"):
    """A TableInfos record with the fields the lifecycle check reads."""
    return {"ID": 0, "Identifier": "x", "Frequency": frequency, "Modified": modified,
            "MetaDataModified": metadata, "OutputStatus": status, "Period": "2020-2026"}


@pytest.fixture
def bronze():
    """Mutable copy of the sample bronze tables, keyed by (table_id, entity)."""
    return sample_bronze()


@pytest.fixture
def fake_read(bronze):
    """Drop-in for cbs_silver.read_bronze that reads the sample tables."""
    def read(table_id, entity):
        r = bronze.get((table_id, entity))
        if r is None:
            return pd.DataFrame()
        return cbs_silver.parse_raw(r, drop_id=entity == "TypedDataSet")
    return read


@pytest.fixture
def silver(fake_read):
    """All silver tables built from the sample bronze tables."""
    return cbs_silver.build_all(read=fake_read, read_ref=lambda dataset: sample_crosswalk())
