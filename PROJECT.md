# CBS & KVK Lakehouse

A local data platform that combines Dutch business statistics from CBS (Statistics
Netherlands) with the KVK business register, refines them through bronze, silver and gold
Delta Lake layers, and presents insights in a Streamlit dashboard. Dagster orchestrates the
pipeline and Docker Compose packages it so it runs the same on any machine.

## Purpose

### Main question

**Which company characteristics go together with higher sickness absence?**
The two characteristics in scope are:

- **Sector:** the company's economic activity (SBI 2008)
- **Size class (grootteklasse):** the number of employees or persons employed

### Sub-questions the dashboard must answer

1. Which sectors have the highest and lowest absence rate, and how far are they from the
   national average?
2. Does absence rise or fall with company size?
3. Are these differences stable over time, or are some sectors or size classes rising?
4. Where absence is high, what is behind it: more people absent, longer absences, or more
   work-related absence (e.g. workload, physical strain)?
5. Is a difference real or within the survey's margin of error?
6. How many businesses does each sector and size class represent, so a high rate can be
   weighed against how many companies it concerns?

### Which data answers what

| Question | Source | Measure |
|---|---|---|
| 1, 2, 3 | 80072NED (quarterly) | Absence percentage: sick days as % of available working days |
| 4 | 86009NED (yearly survey) | Share of employees absent, frequency, duration, duration bands, work-relatedness, causes |
| 5 | 86009NED | 95% confidence intervals per measure |
| 6 | 81589NED, KVK | Number of businesses per sector and size class; KVK BVs/NVs per sector |

### Known limitation: sector and size are reported separately

In every CBS absence table, company characteristics form one list: each figure is for
**either** a sector **or** a size class, never a combination. The dashboard can rank
sectors, and rank size classes, but cannot show e.g. "small construction companies".
Combining the two would need data that CBS doesn't publish in these tables.

Further points to keep in mind:

- **Seasonality.** Absence peaks in winter (flu season), so compare a quarter with the same
  quarter a year earlier, or use a rolling four-quarter average, not consecutive quarters.
- **Sector detail.** The common level across the absence tables is the SBI section
  (A, B, C, ...), plus a few section groups and divisions. The dashboard works at section
  level by default.
- **Size units differ.** The quarterly absence table counts persons employed
  (1–10, 10–100, 100+), the survey counts employees (finer classes). They're shown side by
  side, not merged.
- **KVK has no absence data.** It adds context per sector (number of BVs/NVs, company
  age), not an explanation of absence.

### Audience

To be decided (see `TODO.md`). This affects the level of explanation in the dashboard and
whether it must be shareable outside this machine.

## Goals

- Answer the questions above with data that is complete, current and correct.
- Never request or ingest the same data twice.
- Never let wrong data into the lakehouse: verify before ingestion and, where that isn't
  possible, check right after ingestion.
- Run everything on a schedule, only doing downstream work when new data arrived.
- Be portable: one configuration, one `docker compose up`.

## Architecture

```
 CBS OData API ──(quarterly)──► Bronze CBS ──► Silver CBS ──┐
                                                            ├──► Gold ──► Streamlit dashboard
 KVK zip ──(AI agent + user)──► Bronze KVK ──► Silver KVK ──┘

 Dagster orchestrates every arrow. Docker Compose runs Dagster, Streamlit and Ollama.
```

Storage: Delta Lake tables on disk under `lakehouse/` (delta-rs via the `deltalake`
package, no Spark).

```
lakehouse/
  bronze/   <table_id>_<entity>/       CBS raw records (one table per endpoint)
            _ingestion_log/            CBS: one row per API request already ingested
            kvk/basis_bedrijfsgegevens KVK raw CSV rows, one snapshot per batch
            kvk/_batch_log             KVK: one row per uploaded batch
            reference/sbi2008_sbi2025  CBS SBI 2008 -> SBI 2025 crosswalk, one batch per file version
            reference/_batch_log       reference files: one row per loaded file (by SHA-256)
  quarantine/                          batches that failed their checks, kept for inspection
  _run_log/                            one row per silver/gold run, joinable on run_id
  silver/   dim_*, fct_*, map_*        cleaned, typed, conformed tables, each with run_id
            _staging/<run_id>/         a run's tables before they replace the live ones
  gold/     ...                        tables shaped for the dashboard
```

## Data sources

### CBS (via the OData API)

| Table | Content | Grain | Period | Used for |
|---|---|---|---|---|
| 80072NED | Sickness absence percentage | Sector or size class | Quarter | Main metric |
| 86009NED | Absence survey: share absent, frequency, duration, complaints, work-relatedness, with 95% margins | Sector or size class | Year | Explaining differences |
| 83130NED | Earlier absence survey (2014–2023, averages over all employees) | Sector or size class | Year | Historical comparison |
| 81589NED | Number of businesses by size class and legal form | Sector | Quarter | Context |
| 81644NED | Number of establishments by size class and legal form | Sector × region | Year | Not used by the main question; kept for later regional analysis |

Plus each table's code lists and metadata (sector and size codes, regions, margins,
category groups, measure definitions, periods). Scope: every period from 2020 onward,
quarters where available, otherwise years.

### KVK Handelsregister Open Dataset Basis Bedrijfsgegevens

Anonymised data on all Dutch BVs and NVs: start date, active flag, insolvency, legal form,
postcode region (2 digits), SBI activities, member state. A zip with one semicolon-separated
CSV, refreshed every working day. No company names or KVK numbers. Its SBI codes mix
SBI 2008 and SBI 2025 (Oct 2026: mostly SBI 2025).

### CBS crosswalk SBI 2008 -> SBI 2025

An xlsx from the CBS classifications page ("schakelschema", v2.0) linking SBI 2008 codes
to SBI 2025 codes. Many-to-many: a code can be split over several new codes or merged with
others. Needed to join KVK codes (partly SBI 2025) with the CBS tables (SBI 2008).
Loaded by `reference_bronze.py`; the same file (by SHA-256) is never loaded twice, and
silver `map_sbi2008_sbi2025` uses the newest version only.

## Layers

### Bronze CBS — `cbs_bronze.py` (built)

- Calls the CBS API only for data that isn't in the lakehouse yet. Every request has a
  fixed key; keys already in `_ingestion_log` are skipped.
- TypedDataSets are requested one period at a time, so a run only fetches new periods.
- The period's CBS status (provisional/final) is part of the key, so a revised period is
  fetched again and replaces its old rows instead of duplicating them.
- Follows pagination to the last page; a request is logged only after its data is written,
  so failures are retried on the next run.

### Bronze KVK — `kvk_bronze_agent.py` (built)

- An AI agent (local Ollama model with tool calling) gives the user the download link and
  asks them to upload the zip.
- Verification runs in Python before anything is written: valid zip with a CSV, file not
  ingested before (SHA-256), expected columns, field counts per row, value checks.
- Only a file that passed verification can be ingested, and the user must type `YES`.
  The model can't bypass either gate.
- Each upload gets a `batch_id`, is written in one Delta commit, and is recorded in
  `kvk/_batch_log` with its timestamp, file hash, row count and verification report.

### Silver CBS — `cbs_silver.py` (built)

Rebuilt from bronze on every run.

- **Dimensions:** `dim_period`, `dim_sector` (conformed on the CBS key, with SBI code and
  level), `dim_company_size` (bounds and a coarse band: <10, 10–99, 100+), `dim_region`,
  `dim_survey_measure`, and `map_characteristic` (what each source key is: total, sector,
  size class).
- **Facts:** `fct_absence_rate`, `fct_absence_survey` (long format, value plus 95% bounds,
  the two survey tables kept apart), `fct_business_count`, `fct_establishment_count`.

### Silver KVK — `kvk_silver.py` (built)

One table, `kvk_company`: typed columns, snapshot date and latest-snapshot flag, and the
company's sector. The sector is the main activity ("Hoofdactiviteiten"), not the full list
of activities. Main codes are mapped to SBI 2008 with `map_sbi2008_sbi2025` and to section
and division, so KVK joins with `dim_sector`. Edge cases (several or no main activities,
KVK sub-codes, codes in both SBI lists, codes that came from several SBI 2008 codes) are
marked with flag columns; derived sector columns are filled only when unambiguous (90% of
active companies). Built by `kvk_silver.py`; see its docstring for every column.

### Gold (to do)

Built around the main question. Proposed tables:

- **`gold_absence_by_sector`** (quarterly, from 80072NED): absence % per SBI section and
  the national total; difference from the national total in percentage points; rank per
  quarter; change versus the same quarter a year earlier; rolling four-quarter average.
  Answers questions 1 and 3.
- **`gold_absence_by_size`** (quarterly from 80072NED, yearly from 86009NED): absence per
  size class, with the same comparisons as above. Answers questions 2 and 3.
- **`gold_absence_profile`** (yearly, from 86009NED): per sector and per size class: share
  of employees absent, average frequency and duration, duration bands, share
  work-related, and the most common causes. Each value has its 95% interval and a flag
  showing whether the national value falls outside it. Answers questions 4 and 5.
- **`gold_business_context`** (quarterly from 81589NED, plus KVK): number of businesses
  per sector and size band; active BVs/NVs and median company age per sector from KVK.
  Answers question 6.

Each gold row carries its period status (provisional/final) and the date of the newest
data behind it.

### Streamlit dashboard (to do)

Reads only gold tables. Caches data per gold table version, so it shows new data as soon
as gold is rebuilt without needing a restart. Proposed pages:

1. **Overview:** national absence trend, plus the sectors and size classes furthest above
   and below average in the latest quarter.
2. **Sectors:** ranking and trend per sector, compared with the national total.
3. **Size classes:** absence per size class over time.
4. **Behind the numbers:** survey profile (frequency, duration, work-relatedness, causes)
   for a selected sector or size class, with confidence intervals.
5. **Context:** number of businesses per sector and size band.

Every page shows the data's freshness and credits CBS and KVK as sources.

## Data quality

Two kinds of checks, for two different purposes:

1. **Data checks at runtime** decide whether a batch may be written. They run inside the
   pipeline on every ingestion, before writing. KVK has these already. CBS bronze still
   needs them (expected columns from DataProperties, the period in the data matches the
   period requested, numeric measures, known dimension keys).
2. **Checks after ingestion** cover anything that can only be judged once the data is in:
   row counts per period, no duplicate keys, keys present in their dimension, no sudden
   drops between snapshots. These run as Dagster asset checks after each layer; a failing
   check blocks the downstream layers.

**pytest** verifies the code itself (parsers, skip rules, transformations) on every
change, and in CI before deploying. It doesn't inspect each incoming batch; that is the
job of the runtime checks above, which share the same validation functions.

### When a check fails

- **Quarantine:** a batch that fails a runtime check is saved to `lakehouse/quarantine/`
  with the check report, instead of being dropped, so it can be inspected. It's not
  logged as ingested, so a corrected batch can still be loaded. The same content is
  saved once. A monthly cleanup removes entries that were resolved later or are older
  than the retention period (90 days); a problem that persists is quarantined again.
- **Alerting:** Dagster sends a notification (email or Slack) for failed runs and failed
  checks.
- **Freshness:** a check warns when the newest data is older than expected:
  CBS quarterly data more than 4 months behind the current quarter, yearly data more than
  15 months, the latest KVK snapshot older than the agreed upload interval. This catches a
  pipeline that is silently stuck.

## Consistent layers

Silver and gold are rebuilt table by table. If a run fails halfway, readers would see a
mix of new and old tables. To prevent that:

- Each silver and gold run writes into a staging folder and only replaces the live
  tables once every table in that run succeeded.
- Every table records the `run_id` it was built in; gold and the dashboard check that the
  tables they combine come from the same run.
- Every run, successful or failed, is a row in `lakehouse/_run_log` (`runlog.py`): run_id,
  pipeline, start and end time, status, error, rows per table, the Delta version of each
  input table it read, and the git commit of the code. Joining a table's `run_id` to it
  shows when and how any row was produced.
- Delta can't replace several tables in one commit, so a crash while publishing could
  still leave tables of two runs. `inspect_lakehouse.py` reports that (every silver table
  must carry the latest successful run_id); a rerun fixes it.

## Storage and maintenance

- **KVK retention:** each upload appends a full snapshot, potentially hundreds of MB.
  Keep the latest snapshot plus one per month; older snapshots are removed by a
  maintenance job (the batch log keeps their record).
- **Compaction:** CBS bronze writes one small file per period. A maintenance job runs
  Delta `optimize` (merge small files) and `vacuum` (remove files no longer referenced)
  on all tables.
- **Schedule:** maintenance runs monthly as its own Dagster job, never at the same time
  as an ingestion.

## Source lifecycle and scope changes

- **Discontinued CBS tables:** CBS regularly stops a table and continues the series under
  a new ID (83130NED ended in 2023 this way). Bronze checks each table's status and last
  modification date in CBS's `TableInfos` on every run and warns when a table is stopped
  or hasn't been updated for longer than its normal interval (quarterly 4 months, yearly
  15 months). Warnings don't block ingestion. A `TableInfos` snapshot is kept in bronze
  each time CBS changes it.
- **Scope changes:** when the start year or period preference changes or a table is
  removed from `config.yaml`, `python cbs_bronze.py --cleanup-scope` (try `--dry-run`
  first) removes bronze rows and log entries outside the new scope, so bronze matches
  the configuration. It's run by hand, never automatically.

## Configuration

All settings come from one file (`config.yaml`, read by `config.py`) instead of being
written into the scripts:

- lakehouse and inbox paths (relative to the project folder)
- CBS base URL, data tables, metadata endpoints, start year, period preference, pause
- KVK download URL, accepted column names and the malformed-row limit
- SBI crosswalk URL
- Ollama host and model
- still to add when built: KVK retention, schedules and alert targets

Overrides, later wins: `config.yaml` < `config.local.yaml` (per machine, not in git) <
environment variables `LAKEHOUSE__<SECTION>__<KEY>` (e.g. `LAKEHOUSE__CBS__START_YEAR=2022`,
read as YAML). `LAKEHOUSE_CONFIG` points at another file. An override of a key that
doesn't exist in `config.yaml` is an error, so typos don't go unnoticed. The same code
then runs locally, in Docker, or on another machine with only the configuration changed.

## Licence and privacy

- **Attribution:** CBS and KVK data are licensed CC BY 4.0. The dashboard credits both
  sources on every page.
- **No re-identification:** KVK's terms forbid using the data to trace individual
  companies or people. Postcode region, start date and a detailed SBI code together can
  narrow down to a single company, so gold only contains aggregates, and groups with fewer
  than 10 companies are hidden.
- **No secrets needed:** all sources are open data without API keys.

## Orchestration — Dagster (to do)

- Each layer is a Dagster asset: bronze CBS, bronze KVK, silver CBS, silver KVK, gold.
- **Bronze CBS runs every quarter.** It only produces a new materialisation when it
  ingested new rows, so an empty run stops the chain there.
- **Silver, gold and the dashboard refresh run only when new data arrived upstream**,
  using Dagster's automation conditions on the upstream materialisation.
- **Bronze KVK is triggered by a person**, because the agent needs a user to upload and
  confirm. A new KVK batch then triggers silver KVK and gold the same way.
- Asset checks (see Data quality) run after each asset and block downstream work when
  they fail.
- A separate monthly maintenance job (see Storage and maintenance).

Note on timing: CBS publishes quarterly figures some weeks after a quarter ends, and dates
vary. A single run per quarter can therefore miss a release by a few days. Because runs
without new data cost one small request per dataset, running more often (e.g. weekly) is a
safe alternative if that happens.

## Deployment — Docker Compose (to do)

Services:

- **dagster-webserver** and **dagster-daemon**: the Dagster UI and the scheduler.
- **postgres**: Dagster's run and event storage.
- **streamlit**: the dashboard.
- **ollama**: the model server for the KVK agent; pulls the configured model on first
  start.
- **kvk-agent**: the interactive agent, started on demand with
  `docker compose run --rm kvk-agent`. It reads the downloaded zip from a mounted
  `inbox/` folder, since a container can't see the host's Downloads folder.

Volumes: one shared volume for `lakehouse/` (the data moves with the project) and the
`inbox/` folder for KVK uploads.

Minimum machine: 8 GB RAM for the Ollama model (7B, CPU), plus about 4 GB for the other
services; disk space depends mostly on the KVK retention policy.

## Development

- **CI:** a GitHub Actions workflow runs pytest on every push and before building the
  Docker image.
- **Data dictionary:** a page in the dashboard (and a generated Markdown file) describing
  every gold table and column, built largely from CBS metadata (`dim_survey_measure`,
  measure titles and units).

## Repository layout (target)

```
config.yaml, config.py  all settings, with local and environment overrides
cbs_bronze.py           CBS API -> bronze
kvk_bronze_agent.py     KVK zip -> bronze, via the agent
reference_bronze.py     CBS SBI 2008 -> SBI 2025 crosswalk -> bronze
checks/                 data checks (cbs.py, kvk.py) shared by ingestion, Dagster and
                        pytest; quarantine.py saves failed batches and cleans them up
maintenance.py          monthly tasks: quarantine cleanup (KVK retention, optimize/vacuum to do)
cbs_silver.py           bronze CBS -> silver
kvk_silver.py           bronze KVK -> silver kvk_company
publish.py, runlog.py   staging, publishing and the run log, shared by the silver pipelines
gold/                   silver -> gold (to do)
dashboard/              Streamlit app (to do)
orchestration/          Dagster definitions: assets, schedules, checks, maintenance (to do)
tests/                  pytest
inspect_lakehouse.py    overview of all Delta tables and problems after a run
inbox/                  kvk/: KVK zip uploads for the agent; reference/: downloaded
                        reference files (not in git)
docker-compose.yml      (to do)
Dockerfile              (to do)
.github/workflows/      CI: tests.yml runs pytest on every push
requirements.txt
requirements-dev.txt
PROJECT.md              this file
TODO.md                 open work
```

## Status

| Part | Status |
|---|---|
| First real run (all built layers) | Done 2026-10-05; results in `TODO.md` |
| Bronze CBS | Built and run on real data (92 requests, 39 s); lifecycle warnings, scope cleanup |
| Bronze KVK + agent | Built and run on the real file (1.92M rows) with Ollama; reads `inbox/kvk/` |
| Reference: SBI 2008 -> 2025 crosswalk | Built; loaded v2.0 |
| Silver CBS | Built, incl. `map_sbi2008_sbi2025`; checked against CBS StatLine; staging, `run_id`, run log |
| Silver KVK | Built (`kvk_company`): sector = main activity, edge cases as flags; 90% of active companies get an SBI 2008 section |
| Data checks before ingestion (CBS) | Built (`checks/cbs.py`) |
| Quarantine | Built for CBS, KVK and reference files; monthly cleanup in `maintenance.py` |
| Alerting, freshness checks | To do |
| pytest | 225 tests incl. real Delta tables in tmp_path; one opt-in live CBS test |
| Configuration file | Built (`config.yaml` + `config.py`) |
| Gold | To do; tables proposed around the main question |
| Streamlit dashboard | To do; pages proposed |
| Dagster orchestration and maintenance | To do |
| Docker Compose | To do |
| CI | Stage 1 built: pytest on every push; Docker build step to do |
