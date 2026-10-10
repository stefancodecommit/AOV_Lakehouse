# From bronze to silver

This document explains, table by table, how the raw data in **bronze** becomes the cleaned,
typed tables in **silver**. Every example below is real: a record taken from this
project's lakehouse, next to the silver rows it turns into. You don't need to have read
the code; the last section says where each step lives in it.

- Silver CBS is built by [`pipeline/silver/cbs.py`](../pipeline/silver/cbs.py)
- Silver KVK is built by [`pipeline/silver/kvk.py`](../pipeline/silver/kvk.py)
- For the bigger picture (sources, layers, how to run), see the [README](../README.md)
- For the origin of every single silver column, see the column lineage:
  [lineage.md](lineage.md)

## Contents

1. [What bronze holds, and why it looks "raw"](#1-what-bronze-holds-and-why-it-looks-raw)
2. [What silver adds](#2-what-silver-adds)
3. [Which bronze tables feed which silver tables](#3-which-bronze-tables-feed-which-silver-tables)
4. [Steps every silver CBS table goes through](#4-steps-every-silver-cbs-table-goes-through)
5. [Code lists → `map_characteristic`, `dim_sector`, `dim_company_size`](#5-code-lists--map_characteristic-dim_sector-dim_company_size)
6. [Periods → `dim_period`](#6-periods--dim_period)
7. [Absence percentage (80072NED) → `fct_absence_rate`](#7-absence-percentage-80072ned--fct_absence_rate)
8. [Absence survey (86009NED, 83130NED) → `dim_survey_measure`, `fct_absence_survey`](#8-absence-survey-86009ned-83130ned--dim_survey_measure-fct_absence_survey)
9. [Business counts (81589NED) → `fct_business_count`](#9-business-counts-81589ned--fct_business_count)
10. [Establishments (81644NED) → `fct_establishment_count`, `dim_region`](#10-establishments-81644ned--fct_establishment_count-dim_region)
11. [SBI crosswalk → `map_sbi2008_sbi2025`](#11-sbi-crosswalk--map_sbi2008_sbi2025)
12. [KVK companies → `kvk_company`](#12-kvk-companies--kvk_company)
13. [Tracing a silver row back to its source](#13-tracing-a-silver-row-back-to-its-source)
14. [Where each step lives in the code](#14-where-each-step-lives-in-the-code)

---

## 1. What bronze holds, and why it looks "raw"

Bronze is a faithful copy of what the source delivered, plus a note of where and when it
came from. That is deliberate: if a silver rule turns out to be wrong, silver can simply be
rebuilt from bronze, without asking CBS or KVK again.

**CBS** delivers data through an API (OData). Each answer is a list of records in JSON.
Bronze stores **one Delta table per API endpoint**, for example `80072ned_typeddataset`
(the figures of table 80072NED) or `80072ned_bedrijfskenmerkensbi2008` (its code list of
sectors and size classes). Every bronze CBS table has the same lineage columns, the
record as text, and then **one column per field CBS sent**:

| Column | What it is |
|---|---|
| `raw_json` | the record exactly as CBS sent it, as text |
| one column per CBS field | e.g. `Perioden`, `Ziekteverzuimpercentage_1`: the value *as received* (padding and all), typed from the table's `DataProperties` (codes as text, measures as numbers) |
| `_request_key` | which API request it came from (used to never ask the same thing twice) |
| `_table_id`, `_entity` | the CBS table and endpoint, e.g. `80072NED`, `TypedDataSet` |
| `_period`, `_period_status` | the period requested, and its CBS status then (final or provisional) |
| `_source_url` | the exact URL that was called |
| `_ingested_at` | when it was loaded (UTC) |
| `_record_index` | position of the record in the answer |

A real bronze row (table `80072ned_typeddataset`):

```
_request_key   80072NED/TypedDataSet?Perioden=2026KW02#status=Voorlopig
_period        2026KW02
_period_status Voorlopig                       (= provisional)
_source_url    https://opendata.cbs.nl/ODataApi/odata/80072NED/TypedDataSet?$filter=Perioden eq '2026KW02'
_ingested_at   2026-10-05 16:19 UTC
raw_json       {"ID": 151, "BedrijfskenmerkenSBI2008": "T001081", "Perioden": "2026KW02",
                "Ziekteverzuimpercentage_1": 5.4}
ID                          151
BedrijfskenmerkenSBI2008    T001081
Perioden                    2026KW02
Ziekteverzuimpercentage_1   5.4        (a number: DataProperties says Float)
```

The field columns hold the same values as `raw_json`, unchanged, so bronze can be queried
directly; `raw_json` stays as the proof of what was received, and `python -m pipeline.bronze.cbs
--rebuild-columns` rebuilds the columns from it. A field CBS adds later becomes a new column.

Read on its own, such a row says little: what is `T001081`? What does
`Ziekteverzuimpercentage_1` measure, in which unit? The answers sit in *other* bronze
tables (the code lists and the table's metadata, `DataProperties`). Combining them is
silver's job.

**KVK** delivers one CSV file of ~1.9 million companies. Bronze stores every row as text
(`kvk/basis_bedrijfsgegevens`), with the upload it came from (`_batch_id`) and its row
number in the file. **The SBI crosswalk** (an Excel file from CBS) is stored the same way
(`reference/sbi2008_sbi2025`).

## 2. What silver adds

| Bronze has | Silver makes it |
|---|---|
| JSON text | one column per field, with a proper type (number, date, true/false) |
| codes like `T001081`, `WP19078` | their meaning: *this key is the national total, that one the size class 1-9* |
| keys padded with spaces (`'307500 '`) | clean keys, so tables can be joined |
| one table per API endpoint | tables per *subject*: dimensions (what) and facts (how much) |
| a value and its margins as separate records | the value with its 95% interval on one row |
| 13 numbers side by side in one record | one row per number, labelled (total, size class, legal form) |
| KVK text (`J`/`N`, `19720516`, `64210`) | true/false, a date, and the company's sector in SBI 2008 |

Two terms used below:

- A **dimension** (`dim_…`) describes things: one row per sector, size class, period,
  region or survey measure.
- A **fact table** (`fct_…`) holds the numbers: one row per measured value, with keys that
  point to the dimensions. Facts are stored "long": one value per row, never many values
  side by side, so new measures don't need new columns.

Silver CBS rebuilds all its tables on every run (the data is small, and a full rebuild is
always consistent). It publishes them all at once through a staging folder, and every
silver row carries the `run_id` of the run that made it (see section 13).

## 3. Which bronze tables feed which silver tables

```
 BRONZE (one table per CBS endpoint)               SILVER

 80072ned_typeddataset ────────────────────────────► fct_absence_rate
 86009ned_typeddataset ─┐
 83130ned_typeddataset ─┴──────────────────────────► fct_absence_survey
 86009ned_dataproperties ─┐
 83130ned_dataproperties ─┴────────────────────────► dim_survey_measure
 81589ned_typeddataset ──────── + its dataproperties ► fct_business_count
 81644ned_typeddataset ──────── + its dataproperties ► fct_establishment_count
 81644ned_regios ──────────────────────────────────► dim_region

 code lists of sectors and size classes:
   80072ned_bedrijfskenmerkensbi2008               ┐
   86009ned_bedrijfskenmerken                      │  map_characteristic
   83130ned_bedrijfskenmerken                      ├─► dim_sector
   81589ned_bedrijfstakkenbranchessbi2008          │  dim_company_size
   81644ned_bedrijfstakkenbranchessbi2008          │
   *_categorygroups (80072NED, 86009NED, 83130NED) ┘

 the period keys found in all fact tables ─────────► dim_period

 reference/sbi2008_sbi2025 (newest file version) ──► map_sbi2008_sbi2025
 kvk/basis_bedrijfsgegevens + map_sbi2008_sbi2025 ─► kvk_company      (pipeline/silver/kvk.py)
```

Bronze tables that silver does **not** read, and why:

| Bronze table | Used by | Why silver doesn't need it |
|---|---|---|
| `*_perioden` | bronze | bronze uses it to plan which periods to request; silver reads the periods from the facts themselves |
| `*_marges` | nobody (kept for reference) | the three margin codes are recognised by their prefix (`MW`, `MOG`, `MBG`) |
| `*_tableinfos` | bronze, Dagster | lifecycle warnings: has CBS stopped the table? |
| `81589ned_root` | nobody (kept for reference) | the list of endpoints of that table |

## 4. Steps every silver CBS table goes through

1. **Read the bronze table's field columns.** Keep the period status and ingest time
   from bronze's lineage columns. (A table written before bronze had field columns would
   be read from `raw_json` instead; the result is the same.)
2. **Strip the padding.** CBS pads keys with spaces (`'307500 '`, `'NL01  '`). Stripping
   them once here means every later join just works.
3. **Drop the running number `ID`** in data records (it is only a position); in code lists
   and metadata the `ID` is a real key and is kept.
4. **Keep one version per key.** Bronze already replaces a revised period instead of adding
   it, so there should be one row per key; silver guarantees it anyway by keeping the most
   recently ingested row.
5. **Attach the meaning of each key** through `map_characteristic` (section 5): is this a
   sector, a size class, or the national total? A key that is missing from the CBS code
   list is kept, marked `member_type = 'unknown'`, and logged, never silently dropped.

## 5. Code lists → `map_characteristic`, `dim_sector`, `dim_company_size`

### The problem

In every CBS absence table, sectors **and** size classes **and** the national total are
mixed in **one** list of keys. Here are three real entries of the 80072NED code list:

```json
{"Key": "T001081", "Title": "A-U Alle economische activiteiten", "CategoryGroupID": 1}
{"Key": "307500 ", "Title": "C Industrie",                       "CategoryGroupID": 3}
{"Key": "WP19078", "Title": "1 tot 10 werkzame personen",        "CategoryGroupID": 5}
```

…and the category groups (`80072ned_categorygroups`) that the `CategoryGroupID` refers to:
1 = *Totaal*, 3 = *Bedrijfstak* (sector), 5 = *Bedrijfsgrootte* (size).

### `map_characteristic`: what each key is

One row per key per source table, saying what kind of key it is:

| source_table | characteristic_key | title | category_group | member_type | sector_key | size_key |
|---|---|---|---|---|---|---|
| 80072NED | T001081 | A-U Alle economische activiteiten | Totaal | `total` | T001081 | |
| 80072NED | 307500 | C Industrie | Bedrijfstak | `sector` | 307500 | |
| 80072NED | WP19078 | 1 tot 10 werkzame personen | Bedrijfsgrootte | `size` | | WP_1_10 |

How the kind is decided, in this order:

1. **Size class** if the title reads like one: `1 tot 10 werkzame personen`,
   `100 of meer werknemers`, `2 werkzame personen`. The words must be whole words: the
   sector *"942 Werknemersorganisaties"* (trade unions) contains "werknemer" but is not a
   size class (this was once a real bug, now covered by a test).
2. **Total** if the key is `T001081` or the title starts with `A-U`.
3. Otherwise **sector**.

### `dim_sector`: one row per sector

The sector's SBI code and level are read from the start of its title:

| sector_key | sbi_code | sector_name | sbi_level |
|---|---|---|---|
| 307500 | C | Industrie | `section` |
| T001081 | A-U | Alle economische activiteiten | `total` |

`sbi_level` matters because CBS publishes several levels side by side: sections (C),
divisions (47), groups (812), and combinations (B-F, 10-12). Comparing a section with one
of its own divisions would count the same companies twice, so later layers always compare
within one level. CBS uses the same key for the same sector in every table, so one
`dim_sector` serves all fact tables.

### `dim_company_size`: one row per size class

The bounds are parsed from the title:

| size_key | size_unit | lower_bound | upper_bound_excl | size_range | size_band | title |
|---|---|---|---|---|---|---|
| WP_1_10 | persons_employed | 1 | 10 | 1-9 | <10 | 1 tot 10 werkzame personen |

- `size_unit`: the absence table counts *persons employed* (WP, including owners); the
  survey counts *employees* (WN). The unit is part of the key, so the two are never mixed.
- `size_band` (`<10`, `10-99`, `100+`) is a coarse common level for comparing tables whose
  classes differ.
- Classes that contain other classes of the same table (1-9 contains 1-4 and 5-9) are
  flagged `size_is_aggregate` in `map_characteristic` and in the facts, so they are left
  out whenever classes are added up.

## 6. Periods → `dim_period`

CBS period keys are compact codes. Silver turns each one that occurs in any fact table into
dates and a readable label:

| period_key | period_type | year | quarter | start_date | end_date | period_label |
|---|---|---|---|---|---|---|
| 2026KW02 | quarter | 2026 | 2 | 2026-04-01 | 2026-06-30 | 2026 Q2 |
| 2025JJ00 | year | 2025 | | 2025-01-01 | 2025-12-31 | 2025 |

(`KW` = kwartaal/quarter, `JJ` = jaar/year, `MM` = month.)

## 7. Absence percentage (80072NED) → `fct_absence_rate`

The main figure of the project: sick days as a percentage of available working days, per
quarter, per sector or size class.

**Bronze** (two of the 39 records for 2026 Q2):

```json
{"ID": 759,  "BedrijfskenmerkenSBI2008": "307500 ", "Perioden": "2026KW02", "Ziekteverzuimpercentage_1": 6.2}
{"ID": 5623, "BedrijfskenmerkenSBI2008": "WP19078", "Perioden": "2026KW02", "Ziekteverzuimpercentage_1": 2.5}
```

**Steps:** read, strip, drop `ID` (section 4); look up each key in `map_characteristic`;
rename the CBS column names to plain ones; take the period status from bronze.

**Silver:**

| period_key | characteristic_key | member_type | sector_key | size_key | absence_pct | period_status |
|---|---|---|---|---|---|---|
| 2026KW02 | 307500 | sector | 307500 | | 6.2 | Voorlopig |
| 2026KW02 | WP19078 | size | | WP_1_10 | 2.5 | Voorlopig |

Read: *"in 2026 Q2 (provisional), Industrie (section C) had 6.2% absence, and companies
with 1-9 persons employed 2.5%."* `sector_key` joins to `dim_sector`, `size_key` to
`dim_company_size`, `period_key` to `dim_period`.

## 8. Absence survey (86009NED, 83130NED) → `dim_survey_measure`, `fct_absence_survey`

The yearly survey explains *what lies behind* absence: how often, how long, which
complaints, whether it was work-related. It has 65 measures and every estimate comes with
a 95% margin of error.

### The survey's quirk: one estimate, three records

CBS delivers the estimate and the two bounds of its interval as **three separate
records**, told apart by the dimension `Marges`. Real records for sector Q (health care),
2025, showing the first three of 65 measures:

```json
{"Bedrijfskenmerken": "422400 ", "Marges": "MW00000", "Perioden": "2025JJ00", "AandeelWerknemersDieHebbenVerzuimd_1": 54.3, "AantalKeerVerzuimd_2": 3.0, "AantalWerkdagenVerzuimd_3": 19.4, ...}
{"Bedrijfskenmerken": "422400 ", "Marges": "MOG0095", "Perioden": "2025JJ00", "AandeelWerknemersDieHebbenVerzuimd_1": 53.2, "AantalKeerVerzuimd_2": 2.9, "AantalWerkdagenVerzuimd_3": 18.3, ...}
{"Bedrijfskenmerken": "422400 ", "Marges": "MBG0095", "Perioden": "2025JJ00", "AandeelWerknemersDieHebbenVerzuimd_1": 55.3, "AantalKeerVerzuimd_2": 3.2, "AantalWerkdagenVerzuimd_3": 20.5, ...}
```

`MW00000` = *waarde* (the estimate), `MOG0095` = lower bound, `MBG0095` = upper bound.

### Steps

1. **Wide to long:** each record has 65 measures side by side; turn them into one row per
   measure (65 rows per record).
2. **Three margins to three columns:** for each sector × year × measure, the `MW`, `MOG`
   and `MBG` rows become the columns `value`, `lower_95`, `upper_95` of **one** row, so an
   estimate and its interval always stay together.
3. Attach the meaning of the key (section 5) and the measure's name (below).

### Silver `fct_absence_survey`

| source_table | period_key | characteristic_key | member_type | measure_name | value | lower_95 | upper_95 | period_status |
|---|---|---|---|---|---|---|---|---|
| 86009NED | 2025JJ00 | 422400 | sector | aandeel_werknemers_die_hebben_verzuimd | 54.3 | 53.2 | 55.3 | Definitief |
| 86009NED | 2025JJ00 | 422400 | sector | aantal_werkdagen_verzuimd | 19.4 | 18.3 | 20.5 | Definitief |

Read: *"in 2025, 54.3% of employees in health care were absent at least once (95%
interval 53.2-55.3%)."*

The two survey tables overlap (2014-2023) but average over different groups of people, so
they are never mixed: `source_table` keeps them apart.

### `dim_survey_measure`: what each measure is

The meaning of a measure is in the table's `DataProperties`, a small tree: measures
(`Topic`) hang under topic groups (`TopicGroup`) through `ParentID`. Real entries:

```
ID 5   TopicGroup  "Werknemers met verzuim in afgelopen jaar"
ID 9   TopicGroup  "Duur meest recente verzuimgeval"        ParentID 5
ID 10  Topic       k_1Tot5Werkdagen_5  "1 tot 5 werkdagen"  ParentID 9   Unit %
```

Silver walks up the tree to give every measure its full topic path, and makes a readable
name from its key:

| source_table | measure_key | measure_name | title | topic_group | unit |
|---|---|---|---|---|---|
| 86009NED | AandeelWerknemersDieHebbenVerzuimd_1 | aandeel_werknemers_die_hebben_verzuimd | Aandeel werknemers die hebben verzuimd | Werknemers | % |
| 86009NED | Rug_9 | rug_until_2022 | Rug | … / Klacht meest recente verzuim (tot 2022) | % |
| 86009NED | Rug_28 | rug_from_2022 | Rug | … / Klacht meest recente verzuim (va. 2022) | % |

The last two show why the topic path matters: CBS changed its list of complaints in 2022,
so "Rug" (back) exists twice. The era in the topic group (*tot 2022* / *va. 2022*) is added
to the name, so the two series are never confused.

## 9. Business counts (81589NED) → `fct_business_count`

How many businesses each sector has, by size and legal form, per quarter.

**Bronze**: one record per sector per quarter holds **13 numbers side by side**. Real
record for construction (sector F), 2026 Q3:

```json
{"BedrijfstakkenBranchesSBI2008": "350000 ", "Perioden": "2026KW03",
 "TotaalBedrijven_1": 269845,
 "k_1WerkzaamPersoon_2": 233700, "k_2WerkzamePersonen_3": 18595, "k_3Tot5WerkzamePersonen_4": 6990,
 "k_5Tot10WerkzamePersonen_5": 5045, "k_10Tot20WerkzamePersonen_6": 2870, "k_20Tot50WerkzamePersonen_7": 1695,
 "k_50Tot100WerkzamePersonen_8": 510, "k_100WerkzamePersonenOfMeer_9": 440,
 "k_0Tot50WerkzamePersonen_10": 268895, "k_0Tot250WerkzamePersonen_11": 269675,
 "NatuurlijkePersonen_12": 230705, "Rechtspersonen_13": 39135}
```

**Which number is what** comes from `DataProperties`: the size classes hang under the
topic group *Bedrijfsgrootte*, the legal forms under *Rechtsvorm*, and the total under
neither.

**Steps:** wide to long (13 rows); label each row with its `breakdown` (`total`, `size`
or `legal_form`); for size rows add the size key, band and aggregate flag (section 5);
counts become whole numbers.

**Silver** (some of the 13 rows):

| period_key | sector_key | breakdown | measure_key | size_key | size_band | size_is_aggregate | legal_form | count |
|---|---|---|---|---|---|---|---|---|
| 2026KW03 | 350000 | total | TotaalBedrijven_1 | | | | | 269845 |
| 2026KW03 | 350000 | size | k_1WerkzaamPersoon_2 | WP_1_2 | <10 | False | | 233700 |
| 2026KW03 | 350000 | size | k_100WerkzamePersonenOfMeer_9 | WP_100_plus | 100+ | False | | 440 |
| 2026KW03 | 350000 | size | k_0Tot50WerkzamePersonen_10 | WP_0_50 | | **True** | | 268895 |
| 2026KW03 | 350000 | legal_form | NatuurlijkePersonen_12 | | | | Natuurlijke personen | 230705 |

The row marked `size_is_aggregate = True` is "0 to 50 persons": it overlaps the detailed
classes, so it must be left out when adding them up. (Adding all the non-aggregate classes
gives 269,845, the total.) CBS rounds these counts to multiples of 5, so for some sectors
the classes add up to a few businesses more or less than the total.

## 10. Establishments (81644NED) → `fct_establishment_count`, `dim_region`

Exactly the same steps as section 9, with one extra dimension: the **region**. An
establishment (*vestiging*) is a location of a business, so one business can have several.
The result has 97,020 rows (year × sector × region × measure).

The regions come from the code list `81644ned_regios`. Its keys say the level, which silver
turns into a column:

| region_key | region_name | region_level |
|---|---|---|
| NL01 | Nederland | country |
| LD01 | Noord-Nederland | landsdeel |
| PV20 | Groningen | province |
| CR01 | Oost-Groningen | corop |

(`LD` = landsdeel, a group of provinces; `PV` = province; `CR` = COROP region, a group of
municipalities. CBS writes the province as "Groningen (PV)"; silver removes the suffix,
because the level now has its own column.) The table has 70 regions: 4 country-level
totals, 8 landsdelen, 15 provinces (incl. totals) and 43 COROP regions. It isn't used by
the main question yet; it is kept for regional analyses later.

## 11. SBI crosswalk → `map_sbi2008_sbi2025`

The CBS tables classify sectors with **SBI 2008**; the KVK register has largely moved to
the newer **SBI 2025**. The crosswalk links the two. It is many-to-many: one old code can be
split over several new codes, and several old codes can merge into one.

**Bronze** (one row of the Excel file, all text so leading zeros survive):

```
sbi2008 0111   "Teelt van granen, peulvruchten en oliehoudende zaden"
sbi2025 01110  "Teelt van andere granen dan rijst, en van peulgewassen en oliehoudende zaden"
_source_version v2.0
```

**Steps:** take the newest file version only (bronze keeps every version); count, for each
link, how many codes are on the other side; name the kind of link.

**Silver** (real rows):

| sbi2008_code | sbi2025_code | sbi2025_targets | sbi2008_sources | link_type |
|---|---|---|---|---|
| 0111 | 01110 | 1 | 1 | `1:1` |
| 01199 | 01120 | 5 | 1 | `split` (01199 goes to 5 new codes) |
| 01199 | 01134 | 5 | 2 | `many-to-many` (01134 also comes from another old code) |

## 12. KVK companies → `kvk_company`

Built by `pipeline/silver/kvk.py`, after silver CBS (it needs the crosswalk). One row per company
per upload; the dashboard uses the latest upload (`is_latest`). Unlike the CBS tables it is
not rebuilt every run: a run adds the uploads that are new and removes the retired ones
(see the docstring of `pipeline/silver/kvk.py`).

### Typing and cleaning

| Bronze (text) | Silver |
|---|---|
| `Datum aanvang` `"19720516"` | `start_date` 1972-05-16 (a date); `has_invalid_start_date` when a value doesn't parse |
| `Actief` `"J"` / `"N"` | `is_active` true / false |
| `Insolventie` `""` | `insolvency_code` empty (null), or e.g. `FAIL` |
| `Rechtsvorm`, `Postcode regio`, `Lidstaat` | `legal_form`, `postcode_region`, `member_state` (empty → null) |

### The sector: from the main activity, without guessing

A company's sector is its **main activity** (`Hoofdactiviteiten`), not the full list of
its activities. To compare with the CBS figures, it has to be translated to SBI 2008. That
is not always possible unambiguously, and the rule is: **never guess; mark it**. The
original codes are kept, a derived column is filled only when there is exactly one
answer, and flag columns say why it's empty otherwise.

Three real companies:

**1. Straightforward** (row 1 of the KVK file)

```
Bronze: Hoofdactiviteiten "64210", Actief "J", Datum aanvang "19720516"
```

`64210` exists only in SBI 2025; the crosswalk links it to exactly one SBI 2008 code.

| sbi_version | sbi_mapping | sbi2008_candidates | sbi2008_code | sbi2008_division | sbi2008_section |
|---|---|---|---|---|---|
| 2025 | `1:1` | 1 | 6420 | 64 | K |

**2. Ambiguous version, but the sector is still clear** (row 2)

```
Bronze: Hoofdactiviteiten "47741"
```

The number `47741` exists in **both** SBI 2008 and SBI 2025, so the number alone doesn't
say which one KVK meant. Its four possible SBI 2008 codes (47741 itself, or 47899, 47911,
47991 if it is the SBI 2025 code) all lie in division 47 (retail), so the division and
section can be filled; the exact code can't.

| sbi_version | sbi_mapping | sbi2008_candidates | sbi2008_code | sbi2008_division | sbi2008_section |
|---|---|---|---|---|---|
| both | `ambiguous_version` | 4 | *(empty)* | 47 | G |

**3. Several main activities** (row 7)

```
Bronze: Hoofdactiviteiten "6420,68204", Actief "N"
```

Two main activities in different divisions (64 and 68). Picking one would be a guess, so
the sector stays empty and the flag says why.

| has_multiple_main_activities | sbi2008_candidates | sbi2008_code | sbi2008_division | sbi2008_section |
|---|---|---|---|---|
| True | 2 | *(empty)* | *(empty)* | *(empty)* |

Result on the real file: 90% of the active companies get an SBI 2008 section and
division; the other 10% are marked, not guessed. The full list of flags is in the docstring
of `pipeline/silver/kvk.py`.

## 13. Tracing a silver row back to its source

Every silver row can be followed back to the API call or file it came from:

1. **`run_id`** (every silver table): the silver run that built the row. Look it up in
   `lakehouse/_run_log`, which records when it ran, the code version, and the **Delta
   version of every bronze table it read**.
2. **`period_status`** and **`ingested_at`** (CBS facts): the status the period had when it
   was loaded, and when that was.
3. In bronze, the row's **`_request_key`** and **`_source_url`** name the exact API call;
   for KVK, **`_batch_id`** and **`_row_number`** (copied to silver as `batch_id` and
   `source_row_number`) name the exact upload and line in the file.

Example: the silver row *2026 Q2, Industrie, 6.2%* came from bronze table
`80072ned_typeddataset`, request `80072NED/TypedDataSet?Perioden=2026KW02#status=Voorlopig`,
loaded on 2026-10-05.

## 14. Where each step lives in the code

| Silver table | Function in `pipeline/silver/cbs.py` |
|---|---|
| (every table) reading bronze, stripping padding, dropping `ID` | `read_bronze`, `clean_fields`, `latest` (`parse_raw` for bronze without field columns) |
| `map_characteristic` | `build_map_characteristic` (uses `parse_size`, `parse_sector`, `aggregate_flags`) |
| `dim_sector` | `build_dim_sector` |
| `dim_company_size` | `build_dim_company_size` |
| `dim_period` | `build_dim_period` (uses `parse_period`) |
| `dim_region` | `build_dim_region` |
| `dim_survey_measure` | `build_dim_survey_measure` (uses `topics`, `snake`) |
| `fct_absence_rate` | `build_fct_absence_rate` |
| `fct_absence_survey` | `build_fct_absence_survey` |
| `fct_business_count`, `fct_establishment_count` | `build_fct_counts` |
| `map_sbi2008_sbi2025` | `build_map_sbi2008_sbi2025` (reads with `read_reference`) |
| all of them, in order | `build_all`; publishing: `run` (via `pipeline/common/publish.py`) |

| Silver table | Function in `pipeline/silver/kvk.py` |
|---|---|
| reading bronze (only the needed columns) | `read_kvk_bronze` |
| typing and cleaning | `build_kvk_company` |
| the SBI columns and flags | `classify_main_activities` |

Each function has a docstring explaining what it does and why. The tests in
`tests/test_silver_logic.py` and `tests/test_kvk_silver.py` show the same steps on small
examples.
