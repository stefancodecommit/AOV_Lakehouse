# Column lineage: bronze → silver

Where every silver column comes from in bronze, and what happens to it on the way.

> Generated from `lineage.py` by `python lineage.py`; don't edit by hand. `tests/test_lineage.py` fails when a silver column is missing here or a named bronze field doesn't exist. The same lineage is visible in the Dagster UI: assets `silver_cbs` and `silver_kvk`, the *Columns* list on their overview page (each description starts with *From …*). For the steps explained with real examples, see [bronze_to_silver.md](bronze_to_silver.md).

How to read a source:

| Source | Meaning |
|---|---|
| `80072ned_typeddataset.Perioden` | a field of the record CBS sent: a column of bronze table `80072ned_typeddataset` (the whole record is also kept in `raw_json`) |
| `80072ned_typeddataset._period_status` | a column bronze added when loading |
| `86009ned_typeddataset.<measure>` | every measure field of the record (silver has one row per measure) |
| `kvk/basis_bedrijfsgegevens.Actief` | a column of the KVK file |
| `silver:map_characteristic.member_type` | another silver table (its own lineage is listed below) |

Diagrams of the same lineage: below in this file (drawn by GitHub), or all on one page in [lineage.html](lineage.html) (open it in a browser). Interactive, to follow one column or field through all steps: [lineage_explorer.html](lineage_explorer.html).

## Overview: bronze tables → silver tables

```mermaid
flowchart LR
  subgraph bronze_layer["bronze"]
    n_source_CBS_80072NED["<b>CBS 80072NED</b><br/>80072ned_bedrijfskenmerkensbi2008<br/>80072ned_categorygroups<br/>80072ned_typeddataset"]:::bronze
    n_source_CBS_81589NED["<b>CBS 81589NED</b><br/>81589ned_bedrijfstakkenbranchessbi2008<br/>81589ned_dataproperties<br/>81589ned_typeddataset"]:::bronze
    n_source_CBS_81644NED["<b>CBS 81644NED</b><br/>81644ned_bedrijfstakkenbranchessbi2008<br/>81644ned_dataproperties<br/>81644ned_regios<br/>81644ned_typeddataset"]:::bronze
    n_source_CBS_83130NED["<b>CBS 83130NED</b><br/>83130ned_bedrijfskenmerken<br/>83130ned_categorygroups<br/>83130ned_dataproperties<br/>83130ned_typeddataset"]:::bronze
    n_source_CBS_86009NED["<b>CBS 86009NED</b><br/>86009ned_bedrijfskenmerken<br/>86009ned_categorygroups<br/>86009ned_dataproperties<br/>86009ned_typeddataset"]:::bronze
    n_source_CBS_SBI_crosswalk__xlsx_["<b>CBS SBI crosswalk (xlsx)</b><br/>reference/sbi2008_sbi2025"]:::bronze
    n_source_KVK_file["<b>KVK file</b><br/>kvk/basis_bedrijfsgegevens"]:::bronze
  end
  subgraph silver_layer["silver"]
    n_silver_map_characteristic["map_characteristic"]:::silver
    n_silver_dim_sector["dim_sector"]:::silver
    n_silver_dim_company_size["dim_company_size"]:::silver
    n_silver_dim_period["dim_period"]:::silver
    n_silver_dim_region["dim_region"]:::silver
    n_silver_dim_survey_measure["dim_survey_measure"]:::silver
    n_silver_map_sbi2008_sbi2025["map_sbi2008_sbi2025"]:::silver
    n_silver_fct_absence_rate["fct_absence_rate"]:::silver
    n_silver_fct_absence_survey["fct_absence_survey"]:::silver
    n_silver_fct_business_count["fct_business_count"]:::silver
    n_silver_fct_establishment_count["fct_establishment_count"]:::silver
    n_silver_kvk_company["kvk_company"]:::silver
  end
  n_silver_dim_survey_measure --> n_silver_fct_absence_survey
  n_silver_map_characteristic --> n_silver_dim_sector
  n_silver_map_characteristic --> n_silver_fct_absence_rate
  n_silver_map_characteristic --> n_silver_fct_absence_survey
  n_silver_map_characteristic --> n_silver_fct_business_count
  n_silver_map_characteristic --> n_silver_fct_establishment_count
  n_silver_map_sbi2008_sbi2025 --> n_silver_kvk_company
  n_source_CBS_80072NED --> n_silver_dim_company_size
  n_source_CBS_80072NED --> n_silver_dim_period
  n_source_CBS_80072NED --> n_silver_dim_sector
  n_source_CBS_80072NED --> n_silver_fct_absence_rate
  n_source_CBS_80072NED --> n_silver_map_characteristic
  n_source_CBS_81589NED --> n_silver_dim_company_size
  n_source_CBS_81589NED --> n_silver_dim_period
  n_source_CBS_81589NED --> n_silver_dim_sector
  n_source_CBS_81589NED --> n_silver_fct_business_count
  n_source_CBS_81589NED --> n_silver_map_characteristic
  n_source_CBS_81644NED --> n_silver_dim_company_size
  n_source_CBS_81644NED --> n_silver_dim_period
  n_source_CBS_81644NED --> n_silver_dim_region
  n_source_CBS_81644NED --> n_silver_dim_sector
  n_source_CBS_81644NED --> n_silver_fct_establishment_count
  n_source_CBS_81644NED --> n_silver_map_characteristic
  n_source_CBS_83130NED --> n_silver_dim_company_size
  n_source_CBS_83130NED --> n_silver_dim_period
  n_source_CBS_83130NED --> n_silver_dim_sector
  n_source_CBS_83130NED --> n_silver_dim_survey_measure
  n_source_CBS_83130NED --> n_silver_fct_absence_survey
  n_source_CBS_83130NED --> n_silver_map_characteristic
  n_source_CBS_86009NED --> n_silver_dim_company_size
  n_source_CBS_86009NED --> n_silver_dim_period
  n_source_CBS_86009NED --> n_silver_dim_sector
  n_source_CBS_86009NED --> n_silver_dim_survey_measure
  n_source_CBS_86009NED --> n_silver_fct_absence_survey
  n_source_CBS_86009NED --> n_silver_map_characteristic
  n_source_CBS_SBI_crosswalk__xlsx_ --> n_silver_map_sbi2008_sbi2025
  n_source_KVK_file --> n_silver_kvk_company
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

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

```mermaid
flowchart LR
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_CategoryGroupID["5 code lists of sectors and size classes<br/>CategoryGroupID"]:::bronze
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Key["5 code lists of sectors and size classes<br/>Key"]:::bronze
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title["5 code lists of sectors and size classes<br/>Title"]:::bronze
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken__entity["5 code lists of sectors and size classes<br/>_entity"]:::bronze
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken__table_id["5 code lists of sectors and size classes<br/>_table_id"]:::bronze
  n_src_80072ned_categorygroups_83130ned_categorygroups_86009ned_categorygroups_ID["3 category-group lists<br/>ID"]:::bronze
  n_src_80072ned_categorygroups_83130ned_categorygroups_86009ned_categorygroups_Title["3 category-group lists<br/>Title"]:::bronze
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_map_characteristic["silver: map_characteristic"]
    n_col_source_table["source_table"]:::silver
    n_col_dimension["dimension"]:::silver
    n_col_characteristic_key["characteristic_key"]:::silver
    n_col_title["title"]:::silver
    n_col_category_group["category_group"]:::silver
    n_col_member_type["member_type"]:::silver
    n_col_sector_key["sector_key"]:::silver
    n_col_size_key["size_key"]:::silver
    n_col_size_is_aggregate["size_is_aggregate"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken__table_id --> n_col_source_table
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken__entity --> n_col_dimension
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Key --> n_col_characteristic_key
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_title
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_CategoryGroupID --> n_col_category_group
  n_src_80072ned_categorygroups_83130ned_categorygroups_86009ned_categorygroups_ID --> n_col_category_group
  n_src_80072ned_categorygroups_83130ned_categorygroups_86009ned_categorygroups_Title --> n_col_category_group
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Key --> n_col_member_type
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_member_type
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Key --> n_col_sector_key
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_size_key
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_size_is_aggregate
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `source_table` | `80072ned_bedrijfskenmerkensbi2008._table_id`<br>`86009ned_bedrijfskenmerken._table_id`<br>`83130ned_bedrijfskenmerken._table_id`<br>`81589ned_bedrijfstakkenbranchessbi2008._table_id`<br>`81644ned_bedrijfstakkenbranchessbi2008._table_id` | the CBS table the key belongs to |
| `dimension` | `80072ned_bedrijfskenmerkensbi2008._entity`<br>`86009ned_bedrijfskenmerken._entity`<br>`83130ned_bedrijfskenmerken._entity`<br>`81589ned_bedrijfstakkenbranchessbi2008._entity`<br>`81644ned_bedrijfstakkenbranchessbi2008._entity` | the code list's name, e.g. BedrijfskenmerkenSBI2008 |
| `characteristic_key` | `80072ned_bedrijfskenmerkensbi2008.Key`<br>`86009ned_bedrijfskenmerken.Key`<br>`83130ned_bedrijfskenmerken.Key`<br>`81589ned_bedrijfstakkenbranchessbi2008.Key`<br>`81644ned_bedrijfstakkenbranchessbi2008.Key` | as given (padding removed) |
| `title` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title` | as given (spaces trimmed) |
| `category_group` | `80072ned_bedrijfskenmerkensbi2008.CategoryGroupID`<br>`86009ned_bedrijfskenmerken.CategoryGroupID`<br>`83130ned_bedrijfskenmerken.CategoryGroupID`<br>`81589ned_bedrijfstakkenbranchessbi2008.CategoryGroupID`<br>`81644ned_bedrijfstakkenbranchessbi2008.CategoryGroupID`<br>`80072ned_categorygroups.ID`<br>`80072ned_categorygroups.Title`<br>`86009ned_categorygroups.ID`<br>`86009ned_categorygroups.Title`<br>`83130ned_categorygroups.ID`<br>`83130ned_categorygroups.Title` | the title of the key's category group (e.g. Bedrijfsgrootte) |
| `member_type` | `80072ned_bedrijfskenmerkensbi2008.Key`<br>`80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Key`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Key`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Key`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Key`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title` | 'size' when the title reads like a size class (whole words 'werknemers' / 'werkzame personen'), 'total' for T001081 or a title starting with A-U, otherwise 'sector' |
| `sector_key` | `80072ned_bedrijfskenmerkensbi2008.Key`<br>`86009ned_bedrijfskenmerken.Key`<br>`83130ned_bedrijfskenmerken.Key`<br>`81589ned_bedrijfstakkenbranchessbi2008.Key`<br>`81644ned_bedrijfstakkenbranchessbi2008.Key` | the key itself, for sectors and the total |
| `size_key` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title` | size class parsed from the title, e.g. '1 tot 10 werkzame personen' -> WP_1_10 (size rows only) |
| `size_is_aggregate` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title` | true when the class contains other classes of the same table, e.g. 1-9 contains 1-4 and 5-9 (size rows only) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_sector

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Key["5 code lists of sectors and size classes<br/>Key"]:::bronze
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title["5 code lists of sectors and size classes<br/>Title"]:::bronze
  n_src_silver_map_characteristic_member_type["map_characteristic<br/>member_type"]:::silverref
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_dim_sector["silver: dim_sector"]
    n_col_sector_key["sector_key"]:::silver
    n_col_sbi_code["sbi_code"]:::silver
    n_col_sector_name["sector_name"]:::silver
    n_col_sbi_level["sbi_level"]:::silver
    n_col_title["title"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Key --> n_col_sector_key
  n_src_silver_map_characteristic_member_type --> n_col_sector_key
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_sbi_code
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_sector_name
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_sbi_level
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81644ned_bedrijfstakkenbranchessbi2008_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_title
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `sector_key` | `80072ned_bedrijfskenmerkensbi2008.Key`<br>`86009ned_bedrijfskenmerken.Key`<br>`83130ned_bedrijfskenmerken.Key`<br>`81589ned_bedrijfstakkenbranchessbi2008.Key`<br>`81644ned_bedrijfstakkenbranchessbi2008.Key`<br>`silver:map_characteristic.member_type` | the keys map_characteristic marks as sector or total; one row per key (CBS uses the same key in every table) |
| `sbi_code` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title` | the code at the start of the title: 'C Industrie' -> C |
| `sector_name` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title` | the rest of the title: 'C Industrie' -> Industrie |
| `sbi_level` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title` | from the code's form: A-U total, C section, B-F section_group, 47 division, 812 group, 10-12 division_group, ... |
| `title` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title` | as given; when tables differ, the most common title |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_company_size

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81589ned_dataproperties_81644ned_bedrijfstakkenbranchessbi2008_81644ned_dataproperties_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title["80072ned_bedrijfskenmerkensbi2008, 81589ned_bedrijfstakkenbranchessbi2008, 81589ned_dataproperties, 81644ned_bedrijfstakkenbranchessbi2008, 81644ned_dataproperties, 83130ned_bedrijfskenmerken, 86009ned_bedrijfskenmerken<br/>Title"]:::bronze
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_dim_company_size["silver: dim_company_size"]
    n_col_size_key["size_key"]:::silver
    n_col_size_unit["size_unit"]:::silver
    n_col_lower_bound["lower_bound"]:::silver
    n_col_upper_bound_excl["upper_bound_excl"]:::silver
    n_col_size_range["size_range"]:::silver
    n_col_size_band["size_band"]:::silver
    n_col_title["title"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81589ned_dataproperties_81644ned_bedrijfstakkenbranchessbi2008_81644ned_dataproperties_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_size_key
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81589ned_dataproperties_81644ned_bedrijfstakkenbranchessbi2008_81644ned_dataproperties_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_size_unit
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81589ned_dataproperties_81644ned_bedrijfstakkenbranchessbi2008_81644ned_dataproperties_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_lower_bound
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81589ned_dataproperties_81644ned_bedrijfstakkenbranchessbi2008_81644ned_dataproperties_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_upper_bound_excl
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81589ned_dataproperties_81644ned_bedrijfstakkenbranchessbi2008_81644ned_dataproperties_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_size_range
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81589ned_dataproperties_81644ned_bedrijfstakkenbranchessbi2008_81644ned_dataproperties_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_size_band
  n_src_80072ned_bedrijfskenmerkensbi2008_81589ned_bedrijfstakkenbranchessbi2008_81589ned_dataproperties_81644ned_bedrijfstakkenbranchessbi2008_81644ned_dataproperties_83130ned_bedrijfskenmerken_86009ned_bedrijfskenmerken_Title --> n_col_title
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `size_key` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title`<br>`81589ned_dataproperties.Title`<br>`81644ned_dataproperties.Title` | unit and bounds parsed from a size-class title: WN (employees) or WP (persons employed), lower, upper -> e.g. WP_1_10 |
| `size_unit` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title`<br>`81589ned_dataproperties.Title`<br>`81644ned_dataproperties.Title` | 'employees' (werknemers) or 'persons_employed' (werkzame personen) |
| `lower_bound` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title`<br>`81589ned_dataproperties.Title`<br>`81644ned_dataproperties.Title` | the first number of the title ('1 000' -> 1000) |
| `upper_bound_excl` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title`<br>`81589ned_dataproperties.Title`<br>`81644ned_dataproperties.Title` | the upper bound, exclusive; empty for 'of meer' (no upper bound) |
| `size_range` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title`<br>`81589ned_dataproperties.Title`<br>`81644ned_dataproperties.Title` | readable range from the bounds: 1-9, 100+ |
| `size_band` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title`<br>`81589ned_dataproperties.Title`<br>`81644ned_dataproperties.Title` | <10, 10-99 or 100+: the band that contains the class |
| `title` | `80072ned_bedrijfskenmerkensbi2008.Title`<br>`86009ned_bedrijfskenmerken.Title`<br>`83130ned_bedrijfskenmerken.Title`<br>`81589ned_bedrijfstakkenbranchessbi2008.Title`<br>`81644ned_bedrijfstakkenbranchessbi2008.Title`<br>`81589ned_dataproperties.Title`<br>`81644ned_dataproperties.Title` | as given (spaces normalised) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_period

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden["the 5 data tables (TypedDataSet)<br/>Perioden"]:::bronze
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_dim_period["silver: dim_period"]
    n_col_period_key["period_key"]:::silver
    n_col_period_type["period_type"]:::silver
    n_col_year["year"]:::silver
    n_col_quarter["quarter"]:::silver
    n_col_month["month"]:::silver
    n_col_start_date["start_date"]:::silver
    n_col_end_date["end_date"]:::silver
    n_col_period_label["period_label"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_period_key
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_period_type
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_year
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_quarter
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_month
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_start_date
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_end_date
  n_src_80072ned_typeddataset_81589ned_typeddataset_81644ned_typeddataset_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_period_label
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `period_key` | `80072ned_typeddataset.Perioden`<br>`86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden`<br>`81589ned_typeddataset.Perioden`<br>`81644ned_typeddataset.Perioden` | every period key that occurs in a fact table |
| `period_type` | `80072ned_typeddataset.Perioden`<br>`86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden`<br>`81589ned_typeddataset.Perioden`<br>`81644ned_typeddataset.Perioden` | from the key: KW quarter, JJ year, MM month, HJ half year |
| `year` | `80072ned_typeddataset.Perioden`<br>`86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden`<br>`81589ned_typeddataset.Perioden`<br>`81644ned_typeddataset.Perioden` | the first four digits |
| `quarter` | `80072ned_typeddataset.Perioden`<br>`86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden`<br>`81589ned_typeddataset.Perioden`<br>`81644ned_typeddataset.Perioden` | the quarter (KW, MM keys) |
| `month` | `80072ned_typeddataset.Perioden`<br>`86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden`<br>`81589ned_typeddataset.Perioden`<br>`81644ned_typeddataset.Perioden` | the month (MM keys) |
| `start_date` | `80072ned_typeddataset.Perioden`<br>`86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden`<br>`81589ned_typeddataset.Perioden`<br>`81644ned_typeddataset.Perioden` | first day of the period |
| `end_date` | `80072ned_typeddataset.Perioden`<br>`86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden`<br>`81589ned_typeddataset.Perioden`<br>`81644ned_typeddataset.Perioden` | last day of the period |
| `period_label` | `80072ned_typeddataset.Perioden`<br>`86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden`<br>`81589ned_typeddataset.Perioden`<br>`81644ned_typeddataset.Perioden` | readable label: 2026 Q2, 2025 |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_region

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_81644ned_regios_Description["81644ned_regios<br/>Description"]:::bronze
  n_src_81644ned_regios_Key["81644ned_regios<br/>Key"]:::bronze
  n_src_81644ned_regios_Title["81644ned_regios<br/>Title"]:::bronze
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_dim_region["silver: dim_region"]
    n_col_region_key["region_key"]:::silver
    n_col_region_name["region_name"]:::silver
    n_col_region_level["region_level"]:::silver
    n_col_description["description"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_81644ned_regios_Key --> n_col_region_key
  n_src_81644ned_regios_Title --> n_col_region_name
  n_src_81644ned_regios_Key --> n_col_region_level
  n_src_81644ned_regios_Description --> n_col_description
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `region_key` | `81644ned_regios.Key` | as given (padding removed) |
| `region_name` | `81644ned_regios.Title` | the title without its level suffix: 'Groningen (PV)' -> Groningen |
| `region_level` | `81644ned_regios.Key` | from the key's prefix: NL country, LD landsdeel, PV province, CR corop, GM municipality |
| `description` | `81644ned_regios.Description` | as given |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## dim_survey_measure

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_83130ned_dataproperties_86009ned_dataproperties_Key["survey DataProperties: 86009NED + 83130NED<br/>Key"]:::bronze
  n_src_83130ned_dataproperties_86009ned_dataproperties_ParentID["survey DataProperties: 86009NED + 83130NED<br/>ParentID"]:::bronze
  n_src_83130ned_dataproperties_86009ned_dataproperties_Title["survey DataProperties: 86009NED + 83130NED<br/>Title"]:::bronze
  n_src_83130ned_dataproperties_86009ned_dataproperties_Type["survey DataProperties: 86009NED + 83130NED<br/>Type"]:::bronze
  n_src_83130ned_dataproperties_86009ned_dataproperties_Unit["survey DataProperties: 86009NED + 83130NED<br/>Unit"]:::bronze
  n_src_83130ned_dataproperties_86009ned_dataproperties__table_id["survey DataProperties: 86009NED + 83130NED<br/>_table_id"]:::bronze
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_dim_survey_measure["silver: dim_survey_measure"]
    n_col_source_table["source_table"]:::silver
    n_col_measure_key["measure_key"]:::silver
    n_col_measure_name["measure_name"]:::silver
    n_col_title["title"]:::silver
    n_col_topic_group["topic_group"]:::silver
    n_col_unit["unit"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_83130ned_dataproperties_86009ned_dataproperties__table_id --> n_col_source_table
  n_src_83130ned_dataproperties_86009ned_dataproperties_Key --> n_col_measure_key
  n_src_83130ned_dataproperties_86009ned_dataproperties_Type --> n_col_measure_key
  n_src_83130ned_dataproperties_86009ned_dataproperties_Key --> n_col_measure_name
  n_src_83130ned_dataproperties_86009ned_dataproperties_ParentID --> n_col_measure_name
  n_src_83130ned_dataproperties_86009ned_dataproperties_Title --> n_col_measure_name
  n_src_83130ned_dataproperties_86009ned_dataproperties_Title --> n_col_title
  n_src_83130ned_dataproperties_86009ned_dataproperties_ParentID --> n_col_topic_group
  n_src_83130ned_dataproperties_86009ned_dataproperties_Title --> n_col_topic_group
  n_src_83130ned_dataproperties_86009ned_dataproperties_Unit --> n_col_unit
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `source_table` | `86009ned_dataproperties._table_id`<br>`83130ned_dataproperties._table_id` | the survey table |
| `measure_key` | `86009ned_dataproperties.Key`<br>`86009ned_dataproperties.Type`<br>`83130ned_dataproperties.Key`<br>`83130ned_dataproperties.Type` | the key of every Topic (measure) in DataProperties |
| `measure_name` | `86009ned_dataproperties.Key`<br>`86009ned_dataproperties.ParentID`<br>`86009ned_dataproperties.Title`<br>`83130ned_dataproperties.Key`<br>`83130ned_dataproperties.ParentID`<br>`83130ned_dataproperties.Title` | the key in snake_case ('AantalKeerVerzuimd_2' -> aantal_keer_verzuimd); a name that occurs twice gets the era of its topic group (_until_2022 / _from_2022) |
| `title` | `86009ned_dataproperties.Title`<br>`83130ned_dataproperties.Title` | as given |
| `topic_group` | `86009ned_dataproperties.ParentID`<br>`86009ned_dataproperties.Title`<br>`83130ned_dataproperties.ParentID`<br>`83130ned_dataproperties.Title` | the titles of the TopicGroups above the measure (via ParentID), joined with ' / ' |
| `unit` | `86009ned_dataproperties.Unit`<br>`83130ned_dataproperties.Unit` | as given: %, gemiddelde, ... |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## map_sbi2008_sbi2025

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_reference_sbi2008_sbi2025__batch_id["reference/sbi2008_sbi2025<br/>_batch_id"]:::bronze
  n_src_reference_sbi2008_sbi2025__ingested_at["reference/sbi2008_sbi2025<br/>_ingested_at"]:::bronze
  n_src_reference_sbi2008_sbi2025__source_version["reference/sbi2008_sbi2025<br/>_source_version"]:::bronze
  n_src_reference_sbi2008_sbi2025_link_description["reference/sbi2008_sbi2025<br/>link_description"]:::bronze
  n_src_reference_sbi2008_sbi2025_sbi2008["reference/sbi2008_sbi2025<br/>sbi2008"]:::bronze
  n_src_reference_sbi2008_sbi2025_sbi2008_title["reference/sbi2008_sbi2025<br/>sbi2008_title"]:::bronze
  n_src_reference_sbi2008_sbi2025_sbi2025["reference/sbi2008_sbi2025<br/>sbi2025"]:::bronze
  n_src_reference_sbi2008_sbi2025_sbi2025_title["reference/sbi2008_sbi2025<br/>sbi2025_title"]:::bronze
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_map_sbi2008_sbi2025["silver: map_sbi2008_sbi2025"]
    n_col_sbi2008_code["sbi2008_code"]:::silver
    n_col_sbi2008_division["sbi2008_division"]:::silver
    n_col_sbi2008_title["sbi2008_title"]:::silver
    n_col_sbi2025_code["sbi2025_code"]:::silver
    n_col_sbi2025_division["sbi2025_division"]:::silver
    n_col_sbi2025_title["sbi2025_title"]:::silver
    n_col_link_description["link_description"]:::silver
    n_col_sbi2025_targets["sbi2025_targets"]:::silver
    n_col_sbi2008_sources["sbi2008_sources"]:::silver
    n_col_link_type["link_type"]:::silver
    n_col_source_version["source_version"]:::silver
    n_col_batch_id["batch_id"]:::silver
    n_col_ingested_at["ingested_at"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_reference_sbi2008_sbi2025_sbi2008 --> n_col_sbi2008_code
  n_src_reference_sbi2008_sbi2025_sbi2008 --> n_col_sbi2008_division
  n_src_reference_sbi2008_sbi2025_sbi2008_title --> n_col_sbi2008_title
  n_src_reference_sbi2008_sbi2025_sbi2025 --> n_col_sbi2025_code
  n_src_reference_sbi2008_sbi2025_sbi2025 --> n_col_sbi2025_division
  n_src_reference_sbi2008_sbi2025_sbi2025_title --> n_col_sbi2025_title
  n_src_reference_sbi2008_sbi2025_link_description --> n_col_link_description
  n_src_reference_sbi2008_sbi2025_sbi2008 --> n_col_sbi2025_targets
  n_src_reference_sbi2008_sbi2025_sbi2025 --> n_col_sbi2025_targets
  n_src_reference_sbi2008_sbi2025_sbi2008 --> n_col_sbi2008_sources
  n_src_reference_sbi2008_sbi2025_sbi2025 --> n_col_sbi2008_sources
  n_src_reference_sbi2008_sbi2025_sbi2008 --> n_col_link_type
  n_src_reference_sbi2008_sbi2025_sbi2025 --> n_col_link_type
  n_src_reference_sbi2008_sbi2025__source_version --> n_col_source_version
  n_src_reference_sbi2008_sbi2025__batch_id --> n_col_batch_id
  n_src_reference_sbi2008_sbi2025__ingested_at --> n_col_ingested_at
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

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

```mermaid
flowchart LR
  n_src_80072ned_typeddataset_BedrijfskenmerkenSBI2008["80072ned_typeddataset<br/>BedrijfskenmerkenSBI2008"]:::bronze
  n_src_80072ned_typeddataset_Perioden["80072ned_typeddataset<br/>Perioden"]:::bronze
  n_src_80072ned_typeddataset_Ziekteverzuimpercentage_1["80072ned_typeddataset<br/>Ziekteverzuimpercentage_1"]:::bronze
  n_src_80072ned_typeddataset__ingested_at["80072ned_typeddataset<br/>_ingested_at"]:::bronze
  n_src_80072ned_typeddataset__period_status["80072ned_typeddataset<br/>_period_status"]:::bronze
  n_src_silver_map_characteristic_member_type["map_characteristic<br/>member_type"]:::silverref
  n_src_silver_map_characteristic_sector_key["map_characteristic<br/>sector_key"]:::silverref
  n_src_silver_map_characteristic_size_key["map_characteristic<br/>size_key"]:::silverref
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_fct_absence_rate["silver: fct_absence_rate"]
    n_col_period_key["period_key"]:::silver
    n_col_characteristic_key["characteristic_key"]:::silver
    n_col_member_type["member_type"]:::silver
    n_col_sector_key["sector_key"]:::silver
    n_col_size_key["size_key"]:::silver
    n_col_absence_pct["absence_pct"]:::silver
    n_col_period_status["period_status"]:::silver
    n_col_ingested_at["ingested_at"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_80072ned_typeddataset_Perioden --> n_col_period_key
  n_src_80072ned_typeddataset_BedrijfskenmerkenSBI2008 --> n_col_characteristic_key
  n_src_silver_map_characteristic_member_type --> n_col_member_type
  n_src_silver_map_characteristic_sector_key --> n_col_sector_key
  n_src_silver_map_characteristic_size_key --> n_col_size_key
  n_src_80072ned_typeddataset_Ziekteverzuimpercentage_1 --> n_col_absence_pct
  n_src_80072ned_typeddataset__period_status --> n_col_period_status
  n_src_80072ned_typeddataset__ingested_at --> n_col_ingested_at
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `period_key` | `80072ned_typeddataset.Perioden` | as given |
| `characteristic_key` | `80072ned_typeddataset.BedrijfskenmerkenSBI2008` | as given (padding removed) |
| `member_type` | `silver:map_characteristic.member_type` | looked up by characteristic_key: sector, size, total or unknown |
| `sector_key` | `silver:map_characteristic.sector_key` | looked up by characteristic_key; filled for sectors and the total, joins to dim_sector |
| `size_key` | `silver:map_characteristic.size_key` | looked up by characteristic_key; filled for size classes, joins to dim_company_size |
| `absence_pct` | `80072ned_typeddataset.Ziekteverzuimpercentage_1` | as a number: sick days as % of available working days |
| `period_status` | `80072ned_typeddataset._period_status` | the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig) |
| `ingested_at` | `80072ned_typeddataset._ingested_at` | when the bronze row was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## fct_absence_survey

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_83130ned_typeddataset_86009ned_typeddataset_Bedrijfskenmerken["survey data: 86009NED + 83130NED<br/>Bedrijfskenmerken"]:::bronze
  n_src_83130ned_typeddataset_86009ned_typeddataset_Marges["survey data: 86009NED + 83130NED<br/>Marges"]:::bronze
  n_src_83130ned_typeddataset_86009ned_typeddataset_Perioden["survey data: 86009NED + 83130NED<br/>Perioden"]:::bronze
  n_src_83130ned_typeddataset_86009ned_typeddataset__ingested_at["survey data: 86009NED + 83130NED<br/>_ingested_at"]:::bronze
  n_src_83130ned_typeddataset_86009ned_typeddataset__measure_["survey data: 86009NED + 83130NED<br/>(each measure)"]:::bronze
  n_src_83130ned_typeddataset_86009ned_typeddataset__period_status["survey data: 86009NED + 83130NED<br/>_period_status"]:::bronze
  n_src_83130ned_typeddataset_86009ned_typeddataset__table_id["survey data: 86009NED + 83130NED<br/>_table_id"]:::bronze
  n_src_silver_dim_survey_measure_measure_name["dim_survey_measure<br/>measure_name"]:::silverref
  n_src_silver_map_characteristic_member_type["map_characteristic<br/>member_type"]:::silverref
  n_src_silver_map_characteristic_sector_key["map_characteristic<br/>sector_key"]:::silverref
  n_src_silver_map_characteristic_size_is_aggregate["map_characteristic<br/>size_is_aggregate"]:::silverref
  n_src_silver_map_characteristic_size_key["map_characteristic<br/>size_key"]:::silverref
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_fct_absence_survey["silver: fct_absence_survey"]
    n_col_source_table["source_table"]:::silver
    n_col_period_key["period_key"]:::silver
    n_col_characteristic_key["characteristic_key"]:::silver
    n_col_member_type["member_type"]:::silver
    n_col_sector_key["sector_key"]:::silver
    n_col_size_key["size_key"]:::silver
    n_col_size_is_aggregate["size_is_aggregate"]:::silver
    n_col_measure_key["measure_key"]:::silver
    n_col_measure_name["measure_name"]:::silver
    n_col_value["value"]:::silver
    n_col_lower_95["lower_95"]:::silver
    n_col_upper_95["upper_95"]:::silver
    n_col_period_status["period_status"]:::silver
    n_col_ingested_at["ingested_at"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_83130ned_typeddataset_86009ned_typeddataset__table_id --> n_col_source_table
  n_src_83130ned_typeddataset_86009ned_typeddataset_Perioden --> n_col_period_key
  n_src_83130ned_typeddataset_86009ned_typeddataset_Bedrijfskenmerken --> n_col_characteristic_key
  n_src_silver_map_characteristic_member_type --> n_col_member_type
  n_src_silver_map_characteristic_sector_key --> n_col_sector_key
  n_src_silver_map_characteristic_size_key --> n_col_size_key
  n_src_silver_map_characteristic_size_is_aggregate --> n_col_size_is_aggregate
  n_src_83130ned_typeddataset_86009ned_typeddataset__measure_ --> n_col_measure_key
  n_src_silver_dim_survey_measure_measure_name --> n_col_measure_name
  n_src_83130ned_typeddataset_86009ned_typeddataset__measure_ --> n_col_value
  n_src_83130ned_typeddataset_86009ned_typeddataset_Marges --> n_col_value
  n_src_83130ned_typeddataset_86009ned_typeddataset__measure_ --> n_col_lower_95
  n_src_83130ned_typeddataset_86009ned_typeddataset_Marges --> n_col_lower_95
  n_src_83130ned_typeddataset_86009ned_typeddataset__measure_ --> n_col_upper_95
  n_src_83130ned_typeddataset_86009ned_typeddataset_Marges --> n_col_upper_95
  n_src_83130ned_typeddataset_86009ned_typeddataset__period_status --> n_col_period_status
  n_src_83130ned_typeddataset_86009ned_typeddataset__ingested_at --> n_col_ingested_at
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `source_table` | `86009ned_typeddataset._table_id`<br>`83130ned_typeddataset._table_id` | 86009NED or 83130NED: kept apart, never mixed |
| `period_key` | `86009ned_typeddataset.Perioden`<br>`83130ned_typeddataset.Perioden` | as given |
| `characteristic_key` | `86009ned_typeddataset.Bedrijfskenmerken`<br>`83130ned_typeddataset.Bedrijfskenmerken` | as given (padding removed) |
| `member_type` | `silver:map_characteristic.member_type` | looked up by characteristic_key: sector, size, total or unknown |
| `sector_key` | `silver:map_characteristic.sector_key` | looked up by characteristic_key; filled for sectors and the total, joins to dim_sector |
| `size_key` | `silver:map_characteristic.size_key` | looked up by characteristic_key; filled for size classes, joins to dim_company_size |
| `size_is_aggregate` | `silver:map_characteristic.size_is_aggregate` | looked up by characteristic_key (size classes only) |
| `measure_key` | `86009ned_typeddataset.<measure>`<br>`83130ned_typeddataset.<measure>` | the name of the measure field; one row per measure (wide -> long) |
| `measure_name` | `silver:dim_survey_measure.measure_name` | looked up by source_table and measure_key |
| `value` | `86009ned_typeddataset.<measure>`<br>`86009ned_typeddataset.Marges`<br>`83130ned_typeddataset.<measure>`<br>`83130ned_typeddataset.Marges` | the measure's value in the record with Marges = MW00000 (the estimate) |
| `lower_95` | `86009ned_typeddataset.<measure>`<br>`86009ned_typeddataset.Marges`<br>`83130ned_typeddataset.<measure>`<br>`83130ned_typeddataset.Marges` | the same measure in the record with Marges = MOG0095 |
| `upper_95` | `86009ned_typeddataset.<measure>`<br>`86009ned_typeddataset.Marges`<br>`83130ned_typeddataset.<measure>`<br>`83130ned_typeddataset.Marges` | the same measure in the record with Marges = MBG0095 |
| `period_status` | `86009ned_typeddataset._period_status`<br>`83130ned_typeddataset._period_status` | the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig) |
| `ingested_at` | `86009ned_typeddataset._ingested_at`<br>`83130ned_typeddataset._ingested_at` | when the bronze row was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## fct_business_count

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_81589ned_dataproperties_ParentID["81589ned_dataproperties<br/>ParentID"]:::bronze
  n_src_81589ned_dataproperties_Title["81589ned_dataproperties<br/>Title"]:::bronze
  n_src_81589ned_typeddataset_BedrijfstakkenBranchesSBI2008["81589ned_typeddataset<br/>BedrijfstakkenBranchesSBI2008"]:::bronze
  n_src_81589ned_typeddataset_Perioden["81589ned_typeddataset<br/>Perioden"]:::bronze
  n_src_81589ned_typeddataset__ingested_at["81589ned_typeddataset<br/>_ingested_at"]:::bronze
  n_src_81589ned_typeddataset__measure_["81589ned_typeddataset<br/>(each measure)"]:::bronze
  n_src_81589ned_typeddataset__period_status["81589ned_typeddataset<br/>_period_status"]:::bronze
  n_src_silver_map_characteristic_member_type["map_characteristic<br/>member_type"]:::silverref
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_fct_business_count["silver: fct_business_count"]
    n_col_period_key["period_key"]:::silver
    n_col_sector_key["sector_key"]:::silver
    n_col_member_type["member_type"]:::silver
    n_col_breakdown["breakdown"]:::silver
    n_col_measure_key["measure_key"]:::silver
    n_col_size_key["size_key"]:::silver
    n_col_size_band["size_band"]:::silver
    n_col_size_is_aggregate["size_is_aggregate"]:::silver
    n_col_legal_form["legal_form"]:::silver
    n_col_count["count"]:::silver
    n_col_period_status["period_status"]:::silver
    n_col_ingested_at["ingested_at"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_81589ned_typeddataset_Perioden --> n_col_period_key
  n_src_81589ned_typeddataset_BedrijfstakkenBranchesSBI2008 --> n_col_sector_key
  n_src_silver_map_characteristic_member_type --> n_col_member_type
  n_src_81589ned_dataproperties_ParentID --> n_col_breakdown
  n_src_81589ned_dataproperties_Title --> n_col_breakdown
  n_src_81589ned_typeddataset__measure_ --> n_col_measure_key
  n_src_81589ned_dataproperties_Title --> n_col_size_key
  n_src_81589ned_dataproperties_Title --> n_col_size_band
  n_src_81589ned_dataproperties_Title --> n_col_size_is_aggregate
  n_src_81589ned_dataproperties_Title --> n_col_legal_form
  n_src_81589ned_typeddataset__measure_ --> n_col_count
  n_src_81589ned_typeddataset__period_status --> n_col_period_status
  n_src_81589ned_typeddataset__ingested_at --> n_col_ingested_at
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `period_key` | `81589ned_typeddataset.Perioden` | as given (padding removed) |
| `sector_key` | `81589ned_typeddataset.BedrijfstakkenBranchesSBI2008` | as given (padding removed); joins to dim_sector |
| `member_type` | `silver:map_characteristic.member_type` | looked up by sector_key: sector or total |
| `breakdown` | `81589ned_dataproperties.ParentID`<br>`81589ned_dataproperties.Title` | the measure's topic group in DataProperties: 'total' (none), 'size' (…grootte) or 'legal_form' (Rechtsvorm) |
| `measure_key` | `81589ned_typeddataset.<measure>` | the name of the measure field; one row per measure (wide -> long) |
| `size_key` | `81589ned_dataproperties.Title` | size class parsed from the measure's title, e.g. '2 werkzame personen' -> WP_2_3 (size rows only) |
| `size_band` | `81589ned_dataproperties.Title` | <10, 10-99 or 100+ from the parsed bounds (size rows only) |
| `size_is_aggregate` | `81589ned_dataproperties.Title` | true when the class contains other classes of the table (e.g. 0-50); leave out when summing |
| `legal_form` | `81589ned_dataproperties.Title` | the measure's title, for legal-form rows |
| `count` | `81589ned_typeddataset.<measure>` | the measure's value, as a whole number (null when CBS left it empty) |
| `period_status` | `81589ned_typeddataset._period_status` | the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig) |
| `ingested_at` | `81589ned_typeddataset._ingested_at` | when the bronze row was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## fct_establishment_count

Built by `cbs_silver.py`.

```mermaid
flowchart LR
  n_src_81644ned_dataproperties_ParentID["81644ned_dataproperties<br/>ParentID"]:::bronze
  n_src_81644ned_dataproperties_Title["81644ned_dataproperties<br/>Title"]:::bronze
  n_src_81644ned_typeddataset_BedrijfstakkenBranchesSBI2008["81644ned_typeddataset<br/>BedrijfstakkenBranchesSBI2008"]:::bronze
  n_src_81644ned_typeddataset_Perioden["81644ned_typeddataset<br/>Perioden"]:::bronze
  n_src_81644ned_typeddataset_RegioS["81644ned_typeddataset<br/>RegioS"]:::bronze
  n_src_81644ned_typeddataset__ingested_at["81644ned_typeddataset<br/>_ingested_at"]:::bronze
  n_src_81644ned_typeddataset__measure_["81644ned_typeddataset<br/>(each measure)"]:::bronze
  n_src_81644ned_typeddataset__period_status["81644ned_typeddataset<br/>_period_status"]:::bronze
  n_src_silver_map_characteristic_member_type["map_characteristic<br/>member_type"]:::silverref
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_fct_establishment_count["silver: fct_establishment_count"]
    n_col_period_key["period_key"]:::silver
    n_col_sector_key["sector_key"]:::silver
    n_col_member_type["member_type"]:::silver
    n_col_region_key["region_key"]:::silver
    n_col_breakdown["breakdown"]:::silver
    n_col_measure_key["measure_key"]:::silver
    n_col_size_key["size_key"]:::silver
    n_col_size_band["size_band"]:::silver
    n_col_size_is_aggregate["size_is_aggregate"]:::silver
    n_col_legal_form["legal_form"]:::silver
    n_col_count["count"]:::silver
    n_col_period_status["period_status"]:::silver
    n_col_ingested_at["ingested_at"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_81644ned_typeddataset_Perioden --> n_col_period_key
  n_src_81644ned_typeddataset_BedrijfstakkenBranchesSBI2008 --> n_col_sector_key
  n_src_silver_map_characteristic_member_type --> n_col_member_type
  n_src_81644ned_typeddataset_RegioS --> n_col_region_key
  n_src_81644ned_dataproperties_ParentID --> n_col_breakdown
  n_src_81644ned_dataproperties_Title --> n_col_breakdown
  n_src_81644ned_typeddataset__measure_ --> n_col_measure_key
  n_src_81644ned_dataproperties_Title --> n_col_size_key
  n_src_81644ned_dataproperties_Title --> n_col_size_band
  n_src_81644ned_dataproperties_Title --> n_col_size_is_aggregate
  n_src_81644ned_dataproperties_Title --> n_col_legal_form
  n_src_81644ned_typeddataset__measure_ --> n_col_count
  n_src_81644ned_typeddataset__period_status --> n_col_period_status
  n_src_81644ned_typeddataset__ingested_at --> n_col_ingested_at
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

| Column | Comes from | How |
|---|---|---|
| `period_key` | `81644ned_typeddataset.Perioden` | as given (padding removed) |
| `sector_key` | `81644ned_typeddataset.BedrijfstakkenBranchesSBI2008` | as given (padding removed); joins to dim_sector |
| `member_type` | `silver:map_characteristic.member_type` | looked up by sector_key: sector or total |
| `region_key` | `81644ned_typeddataset.RegioS` | as given (padding removed); joins to dim_region |
| `breakdown` | `81644ned_dataproperties.ParentID`<br>`81644ned_dataproperties.Title` | the measure's topic group in DataProperties: 'total' (none), 'size' (…grootte) or 'legal_form' (Rechtsvorm) |
| `measure_key` | `81644ned_typeddataset.<measure>` | the name of the measure field; one row per measure (wide -> long) |
| `size_key` | `81644ned_dataproperties.Title` | size class parsed from the measure's title, e.g. '2 werkzame personen' -> WP_2_3 (size rows only) |
| `size_band` | `81644ned_dataproperties.Title` | <10, 10-99 or 100+ from the parsed bounds (size rows only) |
| `size_is_aggregate` | `81644ned_dataproperties.Title` | true when the class contains other classes of the table (e.g. 0-50); leave out when summing |
| `legal_form` | `81644ned_dataproperties.Title` | the measure's title, for legal-form rows |
| `count` | `81644ned_typeddataset.<measure>` | the measure's value, as a whole number (null when CBS left it empty) |
| `period_status` | `81644ned_typeddataset._period_status` | the period's CBS status when it was loaded (Definitief / Voorlopig / NaderVoorlopig) |
| `ingested_at` | `81644ned_typeddataset._ingested_at` | when the bronze row was loaded (UTC) |
| `run_id` | *(the run)* | added when the run publishes the table: the silver run that made the row; joins to lakehouse/_run_log (when, which code, which bronze versions) |

## kvk_company

Built by `kvk_silver.py`.

```mermaid
flowchart LR
  n_src_kvk_basis_bedrijfsgegevens_Actief["kvk/basis_bedrijfsgegevens<br/>Actief"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens_Datum_aanvang["kvk/basis_bedrijfsgegevens<br/>Datum_aanvang"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten["kvk/basis_bedrijfsgegevens<br/>Hoofdactiviteiten"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens_Insolventie["kvk/basis_bedrijfsgegevens<br/>Insolventie"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens_Lidstaat["kvk/basis_bedrijfsgegevens<br/>Lidstaat"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens_Postcode_regio["kvk/basis_bedrijfsgegevens<br/>Postcode_regio"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens_Rechtsvorm["kvk/basis_bedrijfsgegevens<br/>Rechtsvorm"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens__batch_id["kvk/basis_bedrijfsgegevens<br/>_batch_id"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens__ingested_at["kvk/basis_bedrijfsgegevens<br/>_ingested_at"]:::bronze
  n_src_kvk_basis_bedrijfsgegevens__row_number["kvk/basis_bedrijfsgegevens<br/>_row_number"]:::bronze
  n_src_silver_map_sbi2008_sbi2025_sbi2008_code["map_sbi2008_sbi2025<br/>sbi2008_code"]:::silverref
  n_src_silver_map_sbi2008_sbi2025_sbi2025_code["map_sbi2008_sbi2025<br/>sbi2025_code"]:::silverref
  run(["the silver run (publish.py)"]):::run
  subgraph n_tbl_kvk_company["silver: kvk_company"]
    n_col_snapshot_date["snapshot_date"]:::silver
    n_col_batch_id["batch_id"]:::silver
    n_col_is_latest["is_latest"]:::silver
    n_col_source_row_number["source_row_number"]:::silver
    n_col_start_date["start_date"]:::silver
    n_col_has_invalid_start_date["has_invalid_start_date"]:::silver
    n_col_is_active["is_active"]:::silver
    n_col_legal_form["legal_form"]:::silver
    n_col_insolvency_code["insolvency_code"]:::silver
    n_col_postcode_region["postcode_region"]:::silver
    n_col_member_state["member_state"]:::silver
    n_col_main_activity_codes["main_activity_codes"]:::silver
    n_col_main_activity_count["main_activity_count"]:::silver
    n_col_has_multiple_main_activities["has_multiple_main_activities"]:::silver
    n_col_has_no_main_activity["has_no_main_activity"]:::silver
    n_col_is_kvk_subcode["is_kvk_subcode"]:::silver
    n_col_sbi_version["sbi_version"]:::silver
    n_col_sbi_mapping["sbi_mapping"]:::silver
    n_col_sbi2008_candidates["sbi2008_candidates"]:::silver
    n_col_sbi2008_code["sbi2008_code"]:::silver
    n_col_sbi2008_division["sbi2008_division"]:::silver
    n_col_sbi2008_section["sbi2008_section"]:::silver
    n_col_run_id["run_id"]:::silver
  end
  n_src_kvk_basis_bedrijfsgegevens__ingested_at --> n_col_snapshot_date
  n_src_kvk_basis_bedrijfsgegevens__batch_id --> n_col_batch_id
  n_src_kvk_basis_bedrijfsgegevens__batch_id --> n_col_is_latest
  n_src_kvk_basis_bedrijfsgegevens__ingested_at --> n_col_is_latest
  n_src_kvk_basis_bedrijfsgegevens__row_number --> n_col_source_row_number
  n_src_kvk_basis_bedrijfsgegevens_Datum_aanvang --> n_col_start_date
  n_src_kvk_basis_bedrijfsgegevens_Datum_aanvang --> n_col_has_invalid_start_date
  n_src_kvk_basis_bedrijfsgegevens_Actief --> n_col_is_active
  n_src_kvk_basis_bedrijfsgegevens_Rechtsvorm --> n_col_legal_form
  n_src_kvk_basis_bedrijfsgegevens_Insolventie --> n_col_insolvency_code
  n_src_kvk_basis_bedrijfsgegevens_Postcode_regio --> n_col_postcode_region
  n_src_kvk_basis_bedrijfsgegevens_Lidstaat --> n_col_member_state
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_main_activity_codes
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_main_activity_count
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_has_multiple_main_activities
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_has_no_main_activity
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_is_kvk_subcode
  n_src_silver_map_sbi2008_sbi2025_sbi2008_code --> n_col_is_kvk_subcode
  n_src_silver_map_sbi2008_sbi2025_sbi2025_code --> n_col_is_kvk_subcode
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_sbi_version
  n_src_silver_map_sbi2008_sbi2025_sbi2008_code --> n_col_sbi_version
  n_src_silver_map_sbi2008_sbi2025_sbi2025_code --> n_col_sbi_version
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_sbi_mapping
  n_src_silver_map_sbi2008_sbi2025_sbi2008_code --> n_col_sbi_mapping
  n_src_silver_map_sbi2008_sbi2025_sbi2025_code --> n_col_sbi_mapping
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_sbi2008_candidates
  n_src_silver_map_sbi2008_sbi2025_sbi2008_code --> n_col_sbi2008_candidates
  n_src_silver_map_sbi2008_sbi2025_sbi2025_code --> n_col_sbi2008_candidates
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_sbi2008_code
  n_src_silver_map_sbi2008_sbi2025_sbi2008_code --> n_col_sbi2008_code
  n_src_silver_map_sbi2008_sbi2025_sbi2025_code --> n_col_sbi2008_code
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_sbi2008_division
  n_src_silver_map_sbi2008_sbi2025_sbi2008_code --> n_col_sbi2008_division
  n_src_silver_map_sbi2008_sbi2025_sbi2025_code --> n_col_sbi2008_division
  n_src_kvk_basis_bedrijfsgegevens_Hoofdactiviteiten --> n_col_sbi2008_section
  n_src_silver_map_sbi2008_sbi2025_sbi2008_code --> n_col_sbi2008_section
  n_src_silver_map_sbi2008_sbi2025_sbi2025_code --> n_col_sbi2008_section
  run --> n_col_run_id
  classDef bronze fill:#f3e3c9,stroke:#a8743a,color:#1d1d1d
  classDef silver fill:#e4e7ec,stroke:#6b7480,color:#1d1d1d
  classDef silverref fill:#f2f3f5,stroke:#9aa1ab,stroke-dasharray:4 3,color:#1d1d1d
  classDef run fill:#ffffff,stroke:#9aa1ab,color:#52514e
```

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
