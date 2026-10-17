# Requirements

What the CBS & KVK lakehouse must do (functional requirements), how well it must do it
(non-functional requirements), what the organisation must arrange around it
(organisational requirements), and under which conditions its results may be used. Written
for the insurer's stakeholders and for review; the organisation and its need are described
in [PROJECT.md](PROJECT.md#the-organisation-and-its-need).

**How to read the tables.** Every requirement has an ID, a priority and a status, and says
where it is implemented and how it is verified, so each claim can be checked.

- **Priority (MoSCoW):** *Must* (the product fails without it), *Should* (important, a
  workaround exists), *Could* (nice to have).
- **Status:** ✅ met · ⚠️ partly met (the gap is named) · ❌ not met yet.
- **Verified by:** a test in `tests/` (run with `pytest`, also on every push in GitHub
  Actions), a check that runs with the pipeline, or a measurement.

## Contents

1. [Stakeholders](#1-stakeholders)
2. [Use cases](#2-use-cases)
3. [Functional requirements](#3-functional-requirements)
4. [Non-functional requirements](#4-non-functional-requirements)
5. [Organisational requirements](#5-organisational-requirements)
6. [Conditions for use](#6-conditions-for-use)
7. [Summary](#7-summary)

---

## 1. Stakeholders

### Data suppliers

| Supplier | Supplies | How | Agreement |
|---|---|---|---|
| **CBS** (Statistics Netherlands), StatLine | Sickness absence: 80072NED (quarterly %), 86009NED and 83130NED (yearly survey, 95% intervals); businesses: 81589NED (quarterly, by size and legal form), 81644NED (establishments by region); code lists and metadata | OData API, pulled monthly | Open data, CC BY 4.0; no SLA. CBS revises provisional figures and sometimes stops a table (83130NED ended in 2023) |
| **CBS**, classifications | The SBI 2008 → SBI 2025 crosswalk (Excel) | File download, when CBS publishes a new version | Open data |
| **KVK** (Chamber of Commerce) | Open dataset "basis bedrijfsgegevens": active companies (BV, NV only; no sole traders) with legal form, start date, main activity | Zip file, downloaded by a person and placed in `inbox/kvk/` | Open data, CC BY 4.0; terms forbid re-identifying companies or people |

### Developers and maintainers

| Role | Responsibility |
|---|---|
| **Data engineer** (in this project: the author) | Builds and maintains the pipeline, the checks and the dashboard; loads KVK files; follows up failed runs and warnings |
| **In the insurer:** the data team | The same role in production: owns the platform, its schedule and its monitoring |

### Users

| User | Relation | Need |
|---|---|---|
| **Data analyst** | Direct user of the dashboard (and, for detail, the lakehouse tables) | Find the risks per SBI sector and size class; judge whether a difference is real; explain a finding |
| **Manager** of the analyst | Receives the findings | Decide what is worth acting on |
| **Risk management** | Receives findings via the manager | Early warning on sectors in the portfolio; input for premiums and acceptance |
| **Account managers** (employer clients) | Receive findings via the manager | Figures to advise an employer on prevention |
| **Privacy officer** (DPO) and **information security** | Approve use | Assurance that no personal data is processed; a say before internal data is linked |

## 2. Use cases

The main interactions with the system (a use-case view, as on the course slides):

| ID | Actor | Use case | Supported by |
|---|---|---|---|
| UC-1 | Data analyst | Find the sectors and size classes with a higher absence risk than national, and which are rising | Dashboard page *Risk index* (`gold_risk_index`) |
| UC-2 | Data analyst | Check whether a difference is real or within the margin of error | Risk index band; page *Behind the numbers* (95% intervals of the survey) |
| UC-3 | Data analyst | Explain one sector to the manager, in words and over time | Risk index explanation sentence and chart over time |
| UC-4 | Data analyst | Look up how many businesses (and self-employed) a risk concerns | Risk index business counts; page *Context* |
| UC-5 | Data engineer | Load a new KVK file | KVK intake agent, or Dagster asset `kvk_bronze` with a typed confirmation |
| UC-6 | System (schedule) | Fetch new CBS figures and rebuild what depends on them | Dagster schedule and automation |
| UC-7 | Data engineer | Find and investigate a data problem | `tools/inspect_lakehouse.py`, `tools/lakehouse_viewer.py`, the Dagster UI, the quarantine |

---

## 3. Functional requirements

What the system must do.

### Ingestion

| ID | Requirement | Prio | Status | Implemented in | Verified by |
|---|---|---|---|---|---|
| FR-1 | Fetch the configured CBS tables, including code lists and metadata, and only what is not yet loaded: a second run makes no data requests | Must | ✅ | `pipeline/bronze/cbs.py`, `config.yaml` | `test_second_run_only_makes_the_planning_call`, `test_run_twice_then_revision` |
| FR-2 | When CBS revises a provisional period, replace that period's rows, never duplicate them | Must | ✅ | request key includes the period status | `test_revised_period_replaces_its_rows_and_keeps_other_periods`, `test_writing_the_same_request_twice_gives_no_duplicates` |
| FR-3 | Check every CBS batch before writing (columns, period, numeric measures, unique keys, not empty); put a failing batch in quarantine with a report, and retry it on the next run | Must | ✅ | `checks/cbs.py`, `checks/quarantine.py` | `test_batch_failing_a_check_is_quarantined_not_written_and_retried`, `test_each_check_stops_a_batch` |
| FR-4 | Load a KVK file only after a person confirms it, verify it first, and never load the same file twice | Must | ✅ | `pipeline/bronze/kvk_agent.py`, `checks/kvk.py` | `test_dagster_ingest_needs_the_exact_file_name_as_confirmation`, `test_kvk_already_ingested_file_fails_and_says_so` |
| FR-5 | Load the SBI crosswalk; skip a file that is already loaded | Must | ✅ | `pipeline/bronze/reference.py` | `test_loading_the_same_file_twice_adds_nothing` |
| FR-6 | Keep every record as received (`raw_json`) next to one column per field, with where and when it came from | Should | ✅ | `pipeline/bronze/cbs.py` | `test_fields_are_columns_with_values_as_received_and_raw_json_kept`, `test_bronze_schema_and_lineage` |

### Transformation

| ID | Requirement | Prio | Status | Implemented in | Verified by |
|---|---|---|---|---|---|
| FR-7 | Build typed dimensions and facts from bronze: give every CBS key its meaning (sector, size class, total), strip CBS's padding, keep unknown keys and flag them | Must | ✅ | `pipeline/silver/cbs.py` | `test_unknown_keys_are_kept_and_flagged`, `test_no_padded_keys_anywhere` |
| FR-8 | Translate each KVK company's main activity to SBI 2008 via the crosswalk; mark ambiguous cases with flags instead of guessing | Must | ✅ | `pipeline/silver/kvk.py` | `test_kvk_company_types_and_flags`, `test_merge_into_one_2025_code` |
| FR-9 | Keep KVK snapshots up to date incrementally: add new uploads, remove retired ones | Should | ✅ | `pipeline/silver/kvk.py` | `test_a_new_upload_only_builds_the_new_snapshot`, `test_a_retired_batch_is_removed` |

### Serving

| ID | Requirement | Prio | Status | Implemented in | Verified by |
|---|---|---|---|---|---|
| FR-10 | Per sector and size class: absence compared with national, rank, change versus a year earlier, 4-quarter average | Must | ✅ | `pipeline/gold/build.py` | `test_absence_by_sector`, `test_year_on_year_and_rolling_average` |
| FR-11 | **Risk index** per SBI sector and size class: relative risk, band, assessment, trend, number of businesses (of which run by natural persons), one explanatory sentence | Must | ✅ | `pipeline/gold/build.py` (`gold_risk_index`) | `test_relative_risk_band_and_assessment`, `test_trend_removes_the_season_and_uses_the_threshold`, `test_business_counts_and_explanation` |
| FR-12 | Show whether a survey value differs from national beyond its 95% interval | Must | ✅ | `gold_absence_profile` | `test_absence_profile_flags_values_outside_the_national_interval` |
| FR-13 | Mark provisional figures everywhere, and offer "final figures only" on every page | Must | ✅ | gold `is_provisional`, `basis`; dashboard switch | `test_every_page_renders_without_errors` (both switch positions), `test_final_basis_uses_only_final_quarters` |
| FR-14 | A dashboard for the data analyst: risk index, sectors, size classes, what lies behind the numbers, context, data dictionary | Must | ✅ | `dashboard/` | `test_dashboard.py` (every page), `test_risk_page_shows_the_analyst_what_is_above_national_and_rising` |

### Workflow and monitoring

| ID | Requirement | Prio | Status | Implemented in | Verified by |
|---|---|---|---|---|---|
| FR-15 | Fetch CBS figures monthly; rebuild silver and gold only after new data, and only when the layer below passed its checks | Must | ✅ | `orchestration/definitions.py` | `test_new_data_triggers_the_next_layer_only_when_upstream_is_known`, `test_failing_blocking_check_stops_gold` |
| FR-16 | Warn when a source is stale or stopped (CBS quarterly > 4 months, yearly > 15 months, KVK > 45 days) | Should | ✅ | `checks/freshness.py`, `checks/cbs.py` | `test_freshness_checks_warn_without_failing`, `test_stale_table_warning` |
| FR-17 | Make every number traceable: the run that made it, the input versions it read, and the source column it came from | Must | ✅ | `lakehouse/_run_log`, `tools/lineage.py` | `test_successful_run_is_logged_and_joinable`, `test_every_column_has_lineage_in_the_same_order`, `test_every_gold_column_has_lineage_in_the_same_order` |
| FR-18 | Let the data engineer inspect any table, any version, with data-quality statistics | Should | ✅ | `tools/lakehouse_viewer.py`, `tools/inspect_lakehouse.py` | `test_quality_statistics`, `test_page_rows_quality_and_history` |
| FR-19 | Alert the data team by email or chat when a run or check fails | Could | ❌ | — (failures are shown in the Dagster UI only) | — |

---

## 4. Non-functional requirements

How well the system must work, per quality attribute.

### Reliability and integrity

| ID | Requirement | Prio | Status | How | Verified by |
|---|---|---|---|---|---|
| NFR-1 | Every step can be repeated safely: a rerun never duplicates data or requests | Must | ✅ | request keys, ingestion logs, file hashes | `test_writing_the_same_request_twice_gives_no_duplicates`, `test_loading_the_same_file_twice_adds_nothing` |
| NFR-2 | A failed silver CBS or gold run leaves the published tables untouched | Must | ✅ | staging, then publish (`pipeline/common/publish.py`) | `test_failed_build_leaves_live_tables_untouched`, `test_staging_failure_leaves_live_tables_untouched` |
| NFR-3 | A run that stops halfway is detected, and finished or reported by the next one | Must | ✅ | run log; silver KVK redoes snapshots of failed runs | `test_publish_stopping_halfway_is_logged_and_detected`, `test_a_crash_halfway_is_finished_by_the_next_run` |
| NFR-4 | Wrong data never reaches the dashboard: unique keys, keys present in their dimension, one run per layer, checked before the next layer is built | Must | ✅ | `checks/layers.py`, Dagster blocking checks | `test_silver_and_gold_wait_for_blocking_checks`, `test_failed_blocking_check_stops_automation` |
| NFR-5 | At most one run writes to the lakehouse at a time | Must | ✅ | Dagster tag concurrency limit | `test_dagster_yaml_limits_writing_runs_to_one` |

### Performance and scalability

| ID | Requirement | Prio | Status | How | Verified by |
|---|---|---|---|---|---|
| NFR-6 | The whole pipeline runs on one ordinary machine | Must | ✅ | Delta Lake with delta-rs, no Spark or database server | measured: silver CBS ~3 s, gold ~17 s, silver KVK ~9–12 s (2026-10) |
| NFR-7 | Silver KVK's memory does not grow with the number of KVK snapshots kept (latest + 1 per month) | Must | ✅ | incremental, one snapshot per write, partitioned by snapshot | measured on simulated uploads: full rebuild of 4 snapshots 1.65 GB, adding one 1.8 GB, nothing new 0.18 GB; `test_present_snapshots_are_read_from_the_delta_log_not_the_data` |
| NFR-8 | Docker needs at most ~3 GB of memory for the pipeline | Should | ✅ | one writing run at a time | measured (README, *Minimum machine*) |
| NFR-9 | Storage stays small: old file versions removed after 7 days, small files compacted monthly | Should | ✅ | `tools/maintenance.py` (optimize, vacuum) | `test_optimize_and_vacuum_removes_files_old_versions_no_longer_need` |

### Security and privacy

| ID | Requirement | Prio | Status | How | Verified by |
|---|---|---|---|---|---|
| NFR-10 | No personal data is stored or shown: only public, aggregated statistics and the KVK dataset of legal entities | Must | ✅ | source choice; KVK open data has no sole traders | source documentation; [PROJECT.md](PROJECT.md#scope-and-boundaries) |
| NFR-11 | No re-identification: KVK groups under 10 companies are hidden by default | Must | ✅ | `is_small_group` in `gold_kvk_context` | `test_kvk_context` |
| NFR-12 | No secrets in the code or the repository | Must | ✅ | open data needs no API keys; `.env` is not in git | git history checked before publishing (2026-10-15) |
| NFR-13 | Services are reachable from this machine only | Must | ⚠️ | Docker: ports on 127.0.0.1 only. **Gap:** the dashboard started locally (`streamlit run`) listens on all network interfaces | `docker-compose.yml`; open item in TODO |
| NFR-14 | Containers run without root rights | Should | ✅ | `user: HOST_UID:HOST_GID` | `docker-compose.yml` |
| NFR-15 | Default passwords are not used in production | Should | ⚠️ | Postgres (Dagster's run history only) has a default password `dagster` in `.env.example`; to be replaced by a secret in a real deployment | — |
| NFR-16 | Tests can never write to the real data | Must | ✅ | `tests/conftest.py`: isolated paths, checked before and after each test; every Delta write to the real lakehouse refused | proven with a probe test (2026-10-10) |

### Compliance and retention

| ID | Requirement | Prio | Status | How | Verified by |
|---|---|---|---|---|---|
| NFR-17 | Credit CBS and KVK (CC BY 4.0) wherever their data is shown | Must | ✅ | sidebar credits on every page | `test_every_page_renders_without_errors` |
| NFR-18 | Keep data no longer than needed: quarantine entries 90 days, KVK snapshots the latest plus one per month, old file versions 7 days | Should | ✅ | `config.yaml`, `tools/maintenance.py` | `test_cleanup_removes_resolved_and_old_keeps_recent`, `test_kvk_retention_keeps_newest_and_first_of_each_month` |

### Maintainability and portability

| ID | Requirement | Prio | Status | How | Verified by |
|---|---|---|---|---|---|
| NFR-19 | All settings in one file; a typo is an error, not a silent default | Must | ✅ | `config.yaml`, `pipeline/common/config.py` | `test_typos_are_errors` |
| NFR-20 | Automated tests on every change | Must | ✅ | 372 tests; GitHub Actions on every push (Python 3.12 and the Docker image) | GitHub Actions runs |
| NFR-21 | The same package versions everywhere (locally, CI, Docker) | Must | ✅ | exact versions in `requirements.txt` and `constraints.txt` | CI install; found and fixed a pandas 3 mismatch (2026-10-08) |
| NFR-22 | Runs the same on any machine with one command | Should | ✅ | Docker Compose | `docker compose config`; image built in CI |
| NFR-23 | Documentation a reviewer can follow: what, why and how to run, per phase | Must | ⚠️ | README, INSTRUCTIONS, PROJECT, bronze_to_silver, lineage. **Gap:** a README per phase (course slide 22) | open item in TODO |

### Usability and explainability

| ID | Requirement | Prio | Status | How | Verified by |
|---|---|---|---|---|---|
| NFR-24 | Every figure shows its period, whether it is provisional, its source and the date the data was loaded | Must | ✅ | freshness line and provisional markers on every page | `test_overview_shows_the_latest_quarter` |
| NFR-25 | The risk model is a white box: every number can be redone by hand and is explained in words | Must | ✅ | simple arithmetic, documented; explanation sentence per row | `test_business_counts_and_explanation`; [PROJECT.md](PROJECT.md#gold--pipelinegoldbuildpy-built) |
| NFR-26 | No value is readable only by hovering: every chart has its numbers as a table | Should | ✅ | table view on every page | dashboard code |
| NFR-27 | Timely: new CBS figures in the dashboard within about a month of publication | Should | ✅ | monthly schedule (1st of the month) | `test_graph_schedules_and_sensor` |

---

## 5. Organisational requirements

What the insurer must arrange for the system to deliver value.

| ID | Requirement | Status |
|---|---|---|
| OR-1 | **A data analyst** who knows the SBI classification and can read margins and trends interprets the figures; the dashboard supports, it doesn't decide | Assumed; the dashboard is designed for this role |
| OR-2 | **Human in command:** the manager decides what to act on; premiums, acceptance and advice stay human decisions | Stated in PROJECT.md; no automated decisions |
| OR-3 | **An owner of the platform** (the data team): keeps it running, follows up failed runs and warnings in the Dagster UI, and loads a KVK file each month | In this project: the author. Without email alerts (FR-19), someone must look at the Dagster UI regularly |
| OR-4 | **A data steward** decides on changes in scope (tables, start year) and on new crosswalk versions | Configuration in `config.yaml`; scope cleanup supported |
| OR-5 | **The privacy officer** is involved before any internal data (claims, policies) is linked to these figures: that would be health data (GDPR art. 9) and needs a legal basis and a DPIA | Out of scope for this project; a condition for any next step |
| OR-6 | **Communication rules:** findings passed on include the period, whether figures are provisional, and the band | Supported by the explanation sentence |

## 6. Conditions for use

Under which conditions the results may be used, and what they can't be used for.

1. **Employees, not the self-employed.** CBS's absence figures describe employees. For
   products for the self-employed they are a proxy, not a measurement of their absence.
2. **The band is stability, not statistical certainty.** CBS gives no margin of error for
   the quarterly absence percentage (80072NED); the risk index band shows how stable the
   relative risk was over 8 quarters. The survey (86009NED) does give 95% intervals.
3. **Correlation, not cause.** A sector with high absence is a higher-risk group; the data
   doesn't say why, and says nothing about an individual company or person.
4. **Groups, not individuals.** Use the figures for sectors and size classes; never to
   judge a single employer or insured person.
5. **Sector and size separately.** CBS publishes absence per sector *or* per size class,
   never combined (e.g. "small construction companies").
6. **Provisional figures can change.** The newest quarters are provisional; use "final
   figures only" for decisions that need final numbers.
7. **Public data only.** Combining the figures with the insurer's own data requires the
   privacy officer's approval first (OR-5).
8. **Source continuity.** CBS can stop or renumber a table; the lifecycle check warns
   (FR-16), but a replacement table must be configured by hand.
9. **Prototype infrastructure.** The system runs on one machine or Docker Compose. Running
   it in production needs the cloud design, real secrets (NFR-15) and alerting (FR-19).

## 7. Summary

| Group | Met | Partly | Not yet |
|---|---|---|---|
| Functional (FR-1 – FR-19) | 18 | 0 | 1 (FR-19 alerts, *Could*) |
| Non-functional (NFR-1 – NFR-27) | 24 | 3 (NFR-13 local dashboard network, NFR-15 default password, NFR-23 README per phase) | 0 |

All *Must* requirements are met except two partly met ones: NFR-13 (the locally started
dashboard listens on the network; Docker doesn't) and NFR-23 (a README per phase). Both
are open items in [TODO.md](../TODO.md).
