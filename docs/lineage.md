# Column lineage: bronze → silver

Where every silver column comes from in bronze, and what happens to it on the way.

> Generated from `lineage.py` by `python lineage.py`; don't edit by hand. `tests/test_lineage.py` fails when a silver column is missing here or a named bronze field doesn't exist. The same lineage is shown in the Dagster UI (assets `silver_cbs` and `silver_kvk`, tab *Column lineage*). For the steps explained with real examples, see [bronze_to_silver.md](bronze_to_silver.md).

How to read a source:

| Source | Meaning |
|---|---|
| `80072ned_typeddataset.raw_json.Perioden` | a field in the JSON record CBS sent (bronze table `80072ned_typeddataset`) |
| `80072ned_typeddataset._period_status` | a column bronze added when loading |
| `86009ned_typeddataset.raw_json.<measure>` | every measure field of the record (silver has one row per measure) |
| `kvk/basis_bedrijfsgegevens.Actief` | a column of the KVK file |
| `silver:map_characteristic.member_type` | another silver table (its own lineage is listed below) |

## Contents

- [`map_characteristic`](#map_characteristic)
- [`dim_sector`](#dim_sector)
- [`dim_company_size`](#dim_company_size)
- [`dim_period`](#dim_period)
- [`dim_region`](#dim_region)
- [`dim_survey_measure`](#dim_survey_measure)
- [`map_sbi2008_sbi2025`](#map_sbi2008_sbi2025)
- [`fct_absence_rate`](#fct_absence_rate)
- [`fct_absence_survey`](#fct_absence_survey)
- [`fct_business_count`](#fct_business_count)
- [`fct_establishment_count`](#fct_establishment_count)
- [`kvk_company`](#kvk_company)

## map_characteristic

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `source_table` | `80072ned_bedrijfskenmerkensbi2008._table_id`<br>`86009ned_bedrijfskenmerken._table_id`<br>`83130ned_bedrijfskenmerken._table_id`<br>`81589ned_bedrijfstakkenbranchessbi2008._table_id`<br>`81644ned_bedrijfstakkenbranchessbi2008._table_id` | the CBS table the key belongs to |
| `dimension` | `80072ned_bedrijfskenmerkensbi2008._entity`<br>`86009ned_bedrijfskenmerken._entity`<br>`83130ned_bedrijfskenmerken._entity`<br>`81589ned_bedrijfstakkenbranchessbi2008._entity`<br>`81644ned_bedrijfstakkenbranchessbi2008._entity` | the code list's name, e.g. BedrijfskenmerkenSBI2008 |
| `characteristic_key` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Key`<br>`86009ned_bedrijfskenmerken.raw_json.Key`<br>`83130ned_bedrijfskenmerken.raw_json.Key`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Key`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Key` | as given (padding removed) |
| `title` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title` | as given (spaces trimmed) |
| `category_group` | `80072ned_bedrijfskenmerkensbi2008.raw_json.CategoryGroupID`<br>`86009ned_bedrijfskenmerken.raw_json.CategoryGroupID`<br>`83130ned_bedrijfskenmerken.raw_json.CategoryGroupID`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.CategoryGroupID`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.CategoryGroupID`<br>`80072ned_categorygroups.raw_json.ID`<br>`80072ned_categorygroups.raw_json.Title`<br>`86009ned_categorygroups.raw_json.ID`<br>`86009ned_categorygroups.raw_json.Title`<br>`83130ned_categorygroups.raw_json.ID`<br>`83130ned_categorygroups.raw_json.Title` | the title of the key's category group (e.g. Bedrijfsgrootte) |
| `member_type` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Key`<br>`80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Key`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Key`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Key`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Key`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title` | 'size' when the title reads like a size class (whole words 'werknemers' / 'werkzame personen'), 'total' for T001081 or a title starting with A-U, otherwise 'sector' |
| `sector_key` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Key`<br>`86009ned_bedrijfskenmerken.raw_json.Key`<br>`83130ned_bedrijfskenmerken.raw_json.Key`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Key`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Key` | the key itself, for sectors and the total |
| `size_key` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title` | size class parsed from the title, e.g. '1 tot 10 werkzame personen' -> WP_1_10 (size rows only) |
| `size_is_aggregate` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title` | true when the class contains other classes of the same table, e.g. 1-9 contains 1-4 and 5-9 (size rows only) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_sector

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `sector_key` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Key`<br>`86009ned_bedrijfskenmerken.raw_json.Key`<br>`83130ned_bedrijfskenmerken.raw_json.Key`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Key`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Key`<br>`silver:map_characteristic.member_type` | the keys map_characteristic marks as sector or total; one row per key (CBS uses the same key in every table) |
| `sbi_code` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title` | the code at the start of the title: 'C Industrie' -> C |
| `sector_name` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title` | the rest of the title: 'C Industrie' -> Industrie |
| `sbi_level` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title` | from the code's form: A-U total, C section, B-F section_group, 47 division, 812 group, 10-12 division_group, ... |
| `title` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title` | as given; when tables differ, the most common title |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_company_size

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `size_key` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81589ned_dataproperties.raw_json.Title`<br>`81644ned_dataproperties.raw_json.Title` | unit and bounds parsed from a size-class title: WN (employees) or WP (persons employed), lower, upper -> e.g. WP_1_10 |
| `size_unit` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81589ned_dataproperties.raw_json.Title`<br>`81644ned_dataproperties.raw_json.Title` | 'employees' (werknemers) or 'persons_employed' (werkzame personen) |
| `lower_bound` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81589ned_dataproperties.raw_json.Title`<br>`81644ned_dataproperties.raw_json.Title` | the first number of the title ('1 000' -> 1000) |
| `upper_bound_excl` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81589ned_dataproperties.raw_json.Title`<br>`81644ned_dataproperties.raw_json.Title` | the upper bound, exclusive; empty for 'of meer' (no upper bound) |
| `size_range` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81589ned_dataproperties.raw_json.Title`<br>`81644ned_dataproperties.raw_json.Title` | readable range from the bounds: 1-9, 100+ |
| `size_band` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81589ned_dataproperties.raw_json.Title`<br>`81644ned_dataproperties.raw_json.Title` | <10, 10-99 or 100+: the band that contains the class |
| `title` | `80072ned_bedrijfskenmerkensbi2008.raw_json.Title`<br>`86009ned_bedrijfskenmerken.raw_json.Title`<br>`83130ned_bedrijfskenmerken.raw_json.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.raw_json.Title`<br>`81589ned_dataproperties.raw_json.Title`<br>`81644ned_dataproperties.raw_json.Title` | as given (spaces normalised) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_period

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `period_key` | `80072ned_typeddataset.raw_json.Perioden`<br>`86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden`<br>`81589ned_typeddataset.raw_json.Perioden`<br>`81644ned_typeddataset.raw_json.Perioden` | every period key that occurs in a fact table |
| `period_type` | `80072ned_typeddataset.raw_json.Perioden`<br>`86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden`<br>`81589ned_typeddataset.raw_json.Perioden`<br>`81644ned_typeddataset.raw_json.Perioden` | from the key: KW quarter, JJ year, MM month, HJ half year |
| `year` | `80072ned_typeddataset.raw_json.Perioden`<br>`86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden`<br>`81589ned_typeddataset.raw_json.Perioden`<br>`81644ned_typeddataset.raw_json.Perioden` | the first four digits |
| `quarter` | `80072ned_typeddataset.raw_json.Perioden`<br>`86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden`<br>`81589ned_typeddataset.raw_json.Perioden`<br>`81644ned_typeddataset.raw_json.Perioden` | the quarter (KW, MM keys) |
| `month` | `80072ned_typeddataset.raw_json.Perioden`<br>`86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden`<br>`81589ned_typeddataset.raw_json.Perioden`<br>`81644ned_typeddataset.raw_json.Perioden` | the month (MM keys) |
| `start_date` | `80072ned_typeddataset.raw_json.Perioden`<br>`86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden`<br>`81589ned_typeddataset.raw_json.Perioden`<br>`81644ned_typeddataset.raw_json.Perioden` | first day of the period |
| `end_date` | `80072ned_typeddataset.raw_json.Perioden`<br>`86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden`<br>`81589ned_typeddataset.raw_json.Perioden`<br>`81644ned_typeddataset.raw_json.Perioden` | last day of the period |
| `period_label` | `80072ned_typeddataset.raw_json.Perioden`<br>`86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden`<br>`81589ned_typeddataset.raw_json.Perioden`<br>`81644ned_typeddataset.raw_json.Perioden` | readable label: 2026 Q2, 2025 |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_region

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `region_key` | `81644ned_regios.raw_json.Key` | as given (padding removed) |
| `region_name` | `81644ned_regios.raw_json.Title` | the title without its level suffix: 'Groningen (PV)' -> Groningen |
| `region_level` | `81644ned_regios.raw_json.Key` | from the key's prefix: NL country, LD landsdeel, PV province, CR corop, GM municipality |
| `description` | `81644ned_regios.raw_json.Description` | as given |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_survey_measure

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `source_table` | `86009ned_dataproperties._table_id`<br>`83130ned_dataproperties._table_id` | the survey table |
| `measure_key` | `86009ned_dataproperties.raw_json.Key`<br>`86009ned_dataproperties.raw_json.Type`<br>`83130ned_dataproperties.raw_json.Key`<br>`83130ned_dataproperties.raw_json.Type` | the key of every Topic (measure) in DataProperties |
| `measure_name` | `86009ned_dataproperties.raw_json.Key`<br>`86009ned_dataproperties.raw_json.ParentID`<br>`86009ned_dataproperties.raw_json.Title`<br>`83130ned_dataproperties.raw_json.Key`<br>`83130ned_dataproperties.raw_json.ParentID`<br>`83130ned_dataproperties.raw_json.Title` | the key in snake_case ('AantalKeerVerzuimd_2' -> aantal_keer_verzuimd); a name that occurs twice gets the era of its topic group (_until_2022 / _from_2022) |
| `title` | `86009ned_dataproperties.raw_json.Title`<br>`83130ned_dataproperties.raw_json.Title` | as given |
| `topic_group` | `86009ned_dataproperties.raw_json.ParentID`<br>`86009ned_dataproperties.raw_json.Title`<br>`83130ned_dataproperties.raw_json.ParentID`<br>`83130ned_dataproperties.raw_json.Title` | the titles of the TopicGroups above the measure (via ParentID), joined with ' / ' |
| `unit` | `86009ned_dataproperties.raw_json.Unit`<br>`83130ned_dataproperties.raw_json.Unit` | as given: %, gemiddelde, ... |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## map_sbi2008_sbi2025

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `sbi2008_code` | `reference/sbi2008_sbi2025.sbi2008` | as given, newest file version only |
| `sbi2008_division` | `reference/sbi2008_sbi2025.sbi2008` | the code's first two digits |
| `sbi2008_title` | `reference/sbi2008_sbi2025.sbi2008_title` | as given |
| `sbi2025_code` | `reference/sbi2008_sbi2025.sbi2025` | as given |
| `sbi2025_division` | `reference/sbi2008_sbi2025.sbi2025` | the code's first two digits (not the file's 2-digit column, which is wrong in 5 rows) |
| `sbi2025_title` | `reference/sbi2008_sbi2025.sbi2025_title` | as given |
| `link_description` | `reference/sbi2008_sbi2025.link_description` | as given |
| `sbi2025_targets` | `reference/sbi2008_sbi2025.sbi2008`<br>`reference/sbi2008_sbi2025.sbi2025` | number of SBI 2025 codes this SBI 2008 code links to |
| `sbi2008_sources` | `reference/sbi2008_sbi2025.sbi2008`<br>`reference/sbi2008_sbi2025.sbi2025` | number of SBI 2008 codes linking to this SBI 2025 code |
| `link_type` | `reference/sbi2008_sbi2025.sbi2008`<br>`reference/sbi2008_sbi2025.sbi2025` | 1:1, split (one old code -> several new), merge (several old -> one new) or many-to-many, from the two counts |
| `source_version` | `reference/sbi2008_sbi2025._source_version` | the file version, from its sheet name (v2.0) |
| `batch_id` | `reference/sbi2008_sbi2025._batch_id` | the loaded file version (batch) |
| `ingested_at` | `reference/sbi2008_sbi2025._ingested_at` | when the file was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## fct_absence_rate

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `period_key` | `80072ned_typeddataset.raw_json.Perioden` | as given |
| `characteristic_key` | `80072ned_typeddataset.raw_json.BedrijfskenmerkenSBI2008` | as given (padding removed) |
| `member_type` | `silver:map_characteristic.member_type` | looked up by characteristic_key: sector, size, total or unknown |
| `sector_key` | `silver:map_characteristic.sector_key` | looked up by characteristic_key; filled for sectors and the total, joins to dim_sector |
| `size_key` | `silver:map_characteristic.size_key` | looked up by characteristic_key; filled for size classes, joins to dim_company_size |
| `absence_pct` | `80072ned_typeddataset.raw_json.Ziekteverzuimpercentage_1` | as a number: sick days as % of available working days |
| `period_status` | `80072ned_typeddataset._period_status` | the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig) |
| `ingested_at` | `80072ned_typeddataset._ingested_at` | when the bronze row was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## fct_absence_survey

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `source_table` | `86009ned_typeddataset._table_id`<br>`83130ned_typeddataset._table_id` | 86009NED or 83130NED: kept apart, never mixed |
| `period_key` | `86009ned_typeddataset.raw_json.Perioden`<br>`83130ned_typeddataset.raw_json.Perioden` | as given |
| `characteristic_key` | `86009ned_typeddataset.raw_json.Bedrijfskenmerken`<br>`83130ned_typeddataset.raw_json.Bedrijfskenmerken` | as given (padding removed) |
| `member_type` | `silver:map_characteristic.member_type` | looked up by characteristic_key: sector, size, total or unknown |
| `sector_key` | `silver:map_characteristic.sector_key` | looked up by characteristic_key; filled for sectors and the total, joins to dim_sector |
| `size_key` | `silver:map_characteristic.size_key` | looked up by characteristic_key; filled for size classes, joins to dim_company_size |
| `size_is_aggregate` | `silver:map_characteristic.size_is_aggregate` | looked up by characteristic_key (size classes only) |
| `measure_key` | `86009ned_typeddataset.raw_json.<measure>`<br>`83130ned_typeddataset.raw_json.<measure>` | the name of the measure field; one row per measure (wide -> long) |
| `measure_name` | `silver:dim_survey_measure.measure_name` | looked up by source_table and measure_key |
| `value` | `86009ned_typeddataset.raw_json.<measure>`<br>`86009ned_typeddataset.raw_json.Marges`<br>`83130ned_typeddataset.raw_json.<measure>`<br>`83130ned_typeddataset.raw_json.Marges` | the measure's value in the record with Marges = MW00000 (the estimate) |
| `lower_95` | `86009ned_typeddataset.raw_json.<measure>`<br>`86009ned_typeddataset.raw_json.Marges`<br>`83130ned_typeddataset.raw_json.<measure>`<br>`83130ned_typeddataset.raw_json.Marges` | the same measure in the record with Marges = MOG0095 |
| `upper_95` | `86009ned_typeddataset.raw_json.<measure>`<br>`86009ned_typeddataset.raw_json.Marges`<br>`83130ned_typeddataset.raw_json.<measure>`<br>`83130ned_typeddataset.raw_json.Marges` | the same measure in the record with Marges = MBG0095 |
| `period_status` | `86009ned_typeddataset._period_status`<br>`83130ned_typeddataset._period_status` | the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig) |
| `ingested_at` | `86009ned_typeddataset._ingested_at`<br>`83130ned_typeddataset._ingested_at` | when the bronze row was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## fct_business_count

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `period_key` | `81589ned_typeddataset.raw_json.Perioden` | as given (padding removed) |
| `sector_key` | `81589ned_typeddataset.raw_json.BedrijfstakkenBranchesSBI2008` | as given (padding removed); joins to dim_sector |
| `member_type` | `silver:map_characteristic.member_type` | looked up by sector_key: sector or total |
| `breakdown` | `81589ned_dataproperties.raw_json.ParentID`<br>`81589ned_dataproperties.raw_json.Title` | the measure's topic group in DataProperties: 'total' (none), 'size' (…grootte) or 'legal_form' (Rechtsvorm) |
| `measure_key` | `81589ned_typeddataset.raw_json.<measure>` | the name of the measure field; one row per measure (wide -> long) |
| `size_key` | `81589ned_dataproperties.raw_json.Title` | size class parsed from the measure's title, e.g. '2 werkzame personen' -> WP_2_3 (size rows only) |
| `size_band` | `81589ned_dataproperties.raw_json.Title` | <10, 10-99 or 100+ from the parsed bounds (size rows only) |
| `size_is_aggregate` | `81589ned_dataproperties.raw_json.Title` | true when the class contains other classes of the table (e.g. 0-50); leave out when summing |
| `legal_form` | `81589ned_dataproperties.raw_json.Title` | the measure's title, for legal-form rows |
| `count` | `81589ned_typeddataset.raw_json.<measure>` | the measure's value, as a whole number (null when CBS left it empty) |
| `period_status` | `81589ned_typeddataset._period_status` | the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig) |
| `ingested_at` | `81589ned_typeddataset._ingested_at` | when the bronze row was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## fct_establishment_count

Built by `cbs_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `period_key` | `81644ned_typeddataset.raw_json.Perioden` | as given (padding removed) |
| `sector_key` | `81644ned_typeddataset.raw_json.BedrijfstakkenBranchesSBI2008` | as given (padding removed); joins to dim_sector |
| `member_type` | `silver:map_characteristic.member_type` | looked up by sector_key: sector or total |
| `region_key` | `81644ned_typeddataset.raw_json.RegioS` | as given (padding removed); joins to dim_region |
| `breakdown` | `81644ned_dataproperties.raw_json.ParentID`<br>`81644ned_dataproperties.raw_json.Title` | the measure's topic group in DataProperties: 'total' (none), 'size' (…grootte) or 'legal_form' (Rechtsvorm) |
| `measure_key` | `81644ned_typeddataset.raw_json.<measure>` | the name of the measure field; one row per measure (wide -> long) |
| `size_key` | `81644ned_dataproperties.raw_json.Title` | size class parsed from the measure's title, e.g. '2 werkzame personen' -> WP_2_3 (size rows only) |
| `size_band` | `81644ned_dataproperties.raw_json.Title` | <10, 10-99 or 100+ from the parsed bounds (size rows only) |
| `size_is_aggregate` | `81644ned_dataproperties.raw_json.Title` | true when the class contains other classes of the table (e.g. 0-50); leave out when summing |
| `legal_form` | `81644ned_dataproperties.raw_json.Title` | the measure's title, for legal-form rows |
| `count` | `81644ned_typeddataset.raw_json.<measure>` | the measure's value, as a whole number (null when CBS left it empty) |
| `period_status` | `81644ned_typeddataset._period_status` | the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig) |
| `ingested_at` | `81644ned_typeddataset._ingested_at` | when the bronze row was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## kvk_company

Built by `kvk_silver.py`.

| Column | Comes from | How |
|---|---|---|
| `snapshot_date` | `kvk/basis_bedrijfsgegevens._ingested_at` | the day the upload was loaded |
| `batch_id` | `kvk/basis_bedrijfsgegevens._batch_id` | the upload (one batch per file) |
| `is_latest` | `kvk/basis_bedrijfsgegevens._batch_id`<br>`kvk/basis_bedrijfsgegevens._ingested_at` | true for the newest successfully loaded upload |
| `source_row_number` | `kvk/basis_bedrijfsgegevens._row_number` | the row's line in the CSV file |
| `start_date` | `kvk/basis_bedrijfsgegevens.Datum_aanvang` | as a date (YYYYMMDD); empty when it doesn't parse |
| `has_invalid_start_date` | `kvk/basis_bedrijfsgegevens.Datum_aanvang` | true when a non-empty value doesn't parse as a date |
| `is_active` | `kvk/basis_bedrijfsgegevens.Actief` | J -> true, N -> false, anything else empty |
| `legal_form` | `kvk/basis_bedrijfsgegevens.Rechtsvorm` | as given (empty -> null) |
| `insolvency_code` | `kvk/basis_bedrijfsgegevens.Insolventie` | as given (empty -> null) |
| `postcode_region` | `kvk/basis_bedrijfsgegevens.Postcode_regio` | as given (empty -> null) |
| `member_state` | `kvk/basis_bedrijfsgegevens.Lidstaat` | as given (empty -> null) |
| `main_activity_codes` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten` | as given: the original comma-separated list |
| `main_activity_count` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten` | number of codes in the list |
| `has_multiple_main_activities` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten` | more than one code |
| `has_no_main_activity` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten` | the list is empty |
| `is_kvk_subcode` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten`<br>`silver:map_sbi2008_sbi2025.sbi2008_code`<br>`silver:map_sbi2008_sbi2025.sbi2025_code` | a 6-digit code that is in neither SBI list |
| `sbi_version` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten`<br>`silver:map_sbi2008_sbi2025.sbi2008_code`<br>`silver:map_sbi2008_sbi2025.sbi2025_code` | in which SBI list(s) the code(s) occur: 2025, 2008, both, unknown, or mixed |
| `sbi_mapping` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten`<br>`silver:map_sbi2008_sbi2025.sbi2008_code`<br>`silver:map_sbi2008_sbi2025.sbi2025_code` | how the code(s) reach SBI 2008: sbi2008, 1:1, merge, ambiguous_version, unknown_code or none (the least certain one) |
| `sbi2008_candidates` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten`<br>`silver:map_sbi2008_sbi2025.sbi2008_code`<br>`silver:map_sbi2008_sbi2025.sbi2025_code` | number of SBI 2008 codes the main code(s) could be |
| `sbi2008_code` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten`<br>`silver:map_sbi2008_sbi2025.sbi2008_code`<br>`silver:map_sbi2008_sbi2025.sbi2025_code` | filled only when there is exactly one candidate |
| `sbi2008_division` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten`<br>`silver:map_sbi2008_sbi2025.sbi2008_code`<br>`silver:map_sbi2008_sbi2025.sbi2025_code` | filled only when all candidates share their first two digits |
| `sbi2008_section` | `kvk/basis_bedrijfsgegevens.Hoofdactiviteiten`<br>`silver:map_sbi2008_sbi2025.sbi2008_code`<br>`silver:map_sbi2008_sbi2025.sbi2025_code` | the SBI 2008 section (letter) of that division |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |
