# TODO

Open work, in the order of `PROJECT.md`. Each phase builds on the one before it.

Rule for every phase: run each new layer on real data and check it before building the
next layer on top of it.

## Next: first real run

Nothing has run against the real CBS API or real Delta tables yet; so far everything is
tested on sample records. Do this on a local machine, one layer at a time, and use
`python inspect_lakehouse.py` after each step for an overview (rows, periods, latest
ingest, problems).

### Bronze CBS (done 2026-10-05)
- [x] `python cbs_bronze.py --dry-run`: check the periods found per dataset (2020 onward)
      86009NED 2020–2025 (JJ), 80072NED 2020KW01–2026KW02, 83130NED 2020–2023 (JJ),
      81644NED 2020–2026 (JJ), 81589NED 2020KW01–2026KW03. 83130NED ends at 2023
      because CBS marks it as discontinued (`Frequency = Stopgezet`, last modified
      2024-04-17); 86009NED is its successor.
- [x] `python cbs_bronze.py`: full first load; note how long it takes and the disk used
      92 requests, 0 failures, 39 s, 6.6 MB on disk, ~240 MB peak memory.
- [x] `inspect_lakehouse.py --layer bronze`: every configured endpoint has a table, no
      failures in the log (28 tables incl. `_ingestion_log`, no problems)
- [x] Run `cbs_bronze.py` a second time: it should make no data requests
      fetched=0, skipped=92 (only the 5 small `Perioden` planning requests are sent)
- [x] Check 81644NED (sector × region): row count, and whether paging was needed
      1,540 rows per year (22 sectors × 70 regions), 10,780 in total; size classes and
      legal forms are measures, not dimensions. No request needed more than one page
      (largest was 1,540 rows), so the paging code is still untested on real data.
      Note: 81644NED counts establishments (vestigingen), not companies.
- [x] Check that every period in `Perioden` has a `Status` value
      All do. Not final yet: 80072NED 2026KW01–02 (Voorlopig); 81644NED 2025–2026
      (Voorlopig); 81589NED 2025KW01–04 (NaderVoorlopig), 2026KW01–03 (Voorlopig).

### Bronze KVK (done 2026-10-05)
- [x] Load one real file with the agent (`python kvk_bronze_agent.py`)
      Ollama 0.35.1 (CPU only) with `qwen2.5:7b`; verify + confirm + ingest took 3.8 min,
      ~470 MB peak memory. Batch `kvk_20261005T153745Z_52785a6f`.
- [x] Note the real CSV header; update `REQUIRED_COLUMNS` if needed
      `"Datum aanvang";"Actief";"Insolventie";"Rechtsvorm";"Postcode regio";`
      `"SBI activiteiten";"Hoofdactiviteiten";"Lidstaat"` (quoted, `;`, UTF-8, CRLF).
      Required columns matched as they were; the SBI columns were not recognised, so
      `OPTIONAL_COLUMNS` now has `sbi` = "SBI activiteiten" and `main_activity` =
      "Hoofdactiviteiten" (test: `tests/test_kvk_logic.py`).
- [x] Answer: one row per company, or one row per company per activity?
      One row per company. "SBI activiteiten" is a comma-separated list (1–6+ codes,
      73% have one); "Hoofdactiviteiten" is usually one code (99.4%) but can be several,
      and is always part of the SBI list. Codes are 4, 5 or 6 digits.
- [x] Note the file size and row count (input for the retention policy)
      Zip 12.9 MB, CSV 96.7 MB, 1,921,911 rows; 22 MB per snapshot in bronze Delta.
      File dated 2026-10-01 inside the zip.
- Data notes for silver KVK: only BV (1,916,604) and NV (5,307); Actief J 1,277,560 /
  N 644,351; Insolventie empty, FAIL (77,284) or SURS (14); Lidstaat always NL;
  174,286 rows (9.1%) have an empty postcode region (mostly inactive, nearly all with
  no SBI code) -> the verification warning is expected; 167,002 rows have no SBI code;
  5 start dates don't parse, range 1806–2026; 381,513 rows are exact duplicates of
  another row, which is expected without a company ID (don't deduplicate).

### Silver CBS (done 2026-10-05)
- [x] `python cbs_silver.py --preview`: row counts per table, and any warnings about
      keys missing from the code lists (no warnings)
- [x] `python cbs_silver.py`, then `inspect_lakehouse.py --layer silver`: all ten tables
      present, no unknown keys. 3.7 s, ~590 MB peak memory, 1.6 MB on disk; largest is
      `fct_business_count` (521,937 rows). No problems reported.
- [x] Spot-check against CBS StatLine: the national absence percentage for the latest
      quarter, and one sector's figure
      2026KW02 (provisional): national 5.4%, C Industrie 6.2%, both equal to the CBS
      table page for 80072NED; also 2025KW02 5.2%, 2025KW04 5.6%, 2024KW04 5.4% match
      CBS. All 39 rows of 2026KW02 and 2020KW01 equal a fresh API request.
      (The OData v4 API at datasets.cbs.nl returned 404, so it couldn't be used.)
- [x] Check the types after reading back: dates, whole-number counts, UTC timestamps
      Delta schema is right: `date32` dates, `int64` counts/bounds/year/quarter,
      `timestamp[us, UTC]`. Note: plain `DeltaTable.to_pandas()` turns int columns with
      nulls into float64 (`count` has 1,586 nulls, `quarter`/`month` are null for
      other period types). Gold and the dashboard should read with nullable types,
      e.g. `to_pandas(types_mapper=pd.ArrowDtype)`.

### Afterwards
- [ ] Fix what the run uncovered, and add a test for each fix
- [ ] Turn the checks above into the Delta tests (section 7) and, later, Dagster asset
      checks

## 0. Foundations
- [ ] Decide the dashboard's audience (personal use, team, or shared outside this machine)
- [ ] `config.yaml` with all settings (lakehouse path, CBS endpoints, start year, KVK URL
      and required columns, retention, Ollama host and model, schedules, alert targets),
      overridable with environment variables
- [ ] Replace the hard-coded settings in `cbs_bronze.py`, `cbs_silver.py` and
      `kvk_bronze_agent.py` with the configuration

## 1. Bronze

### CBS: data checks before ingestion
`cbs_bronze.py` already avoids repeat requests. It doesn't yet check the data itself
before writing. Add a validation step between `fetch_all` and `write_bronze`; a request
that fails is not written and not logged, so it's retried next run.
- [ ] Every record has the table's dimension columns and the measures from DataProperties
- [ ] The `Perioden` value in every record equals the period that was requested
- [ ] Measures are numeric or null
- [ ] At least one record for a period that CBS lists as published
- [ ] Put the checks in `checks/` so ingestion, pytest and Dagster asset checks reuse them

### CBS: source lifecycle and scope
- [ ] Ingest each table's `TableInfos`; warn when CBS marks a table as stopped or it
      hasn't been modified for longer than its normal interval
- [ ] Cleanup step for scope changes: remove bronze rows and log entries outside the
      configured start year or for tables no longer configured

### Quarantine
- [ ] Batches that fail a runtime check go to `lakehouse/quarantine/<source>/<timestamp>/`
      with the check report; not logged as ingested
- [ ] KVK: rejected files are recorded in quarantine with their verification report

### KVK
- [x] Load the first real file and confirm the CSV header (see "Bronze KVK" above)
- [x] One row per company, with SBI codes as comma-separated lists (see above)
- [ ] Move the verification checks into `checks/` so the agent and Dagster both use them
- [ ] Read uploads from the `inbox/` folder (needed for Docker)

## 2. Silver

### CBS
- [x] Dimensions, facts and mapping table (`cbs_silver.py`)
- [ ] Write into a staging folder and replace the live tables only when every table
      succeeded; record the `run_id` on every table

### KVK (postponed)
Build one thin table, `lakehouse/silver/kvk_company`, from
`lakehouse/bronze/kvk/basis_bedrijfsgegevens`.

Why it's needed:
- Every upload is a full snapshot appended as a new batch, so querying bronze directly
  counts each company once per upload. Silver should own the "which snapshot" rule.
- Bronze stores everything as text (dates, J/N flags, codes).
- KVK SBI codes (up to 6 digits) must be mapped to SBI section and division to join
  with the CBS `dim_sector`.

Planned columns:
- [ ] `snapshot_date`, `batch_id`, `is_latest` (from `_batch_id` / `_ingested_at`)
- [ ] `start_date` (date), `is_active` (bool from J/N)
- [ ] `legal_form`, `insolvency_code` (null when empty), `postcode_region`, `member_state`
- [ ] `sbi_code`, `sbi_section`, `sbi_division` (joinable with `dim_sector`)
- [ ] `is_main_activity`: needed, the CSV lists several activities per company. Explode
      "SBI activiteiten" to one row per company per code, or keep one row per company
      with the main activity only (decide per gold question; several main codes occur)

Notes:
- There is no KVK number or company ID in the open dataset, so individual companies
  can't be tracked across snapshots; compare snapshots only in aggregate.
- Until this exists, gold can read the latest batch from bronze directly
  (filter on the newest `_batch_id` in `_batch_log`).

## 3. Gold
Main question: which company characteristics (sector, size class) go together with
higher sickness absence. See `PROJECT.md` for the sub-questions.
- [ ] `gold_absence_by_sector`: absence % per SBI section and national total; difference
      from the total in percentage points; rank per quarter; change versus the same
      quarter a year earlier; rolling four-quarter average
- [ ] `gold_absence_by_size`: the same comparisons per size class (quarterly from
      80072NED, yearly from 86009NED, shown side by side because the size units differ)
- [ ] `gold_absence_profile`: per sector and size class: share absent, frequency,
      duration, duration bands, share work-related, top causes; 95% intervals and a flag
      when the national value falls outside the interval
- [ ] `gold_business_context`: businesses per sector and size band (81589NED); active
      BVs/NVs and median company age per sector (KVK, needs silver KVK)
- [ ] Every gold row carries its period status and the date of the newest data behind it
- [ ] Hide groups with fewer than 10 companies (KVK re-identification rule)
- [ ] Same staging-and-swap publishing and `run_id` as silver
- [ ] Decide whether provisional figures are shown by default

## 4. Streamlit dashboard
- [ ] App in `dashboard/` that reads gold tables only
- [ ] Cache per gold Delta table version, so new data shows without a restart
- [ ] Check that the gold tables shown come from the same `run_id`
- [ ] Pages: Overview, Sectors, Size classes, Behind the numbers, Context
- [ ] Filters for period, sector and size class; seasonally fair comparisons (same
      quarter last year, rolling four-quarter average)
- [ ] Show data freshness and provisional/final status on every page
- [ ] Credit CBS and KVK (CC BY 4.0) on every page
- [ ] Data dictionary page, generated from CBS metadata

## 5. Orchestration (Dagster)
- [ ] Assets: bronze CBS, bronze KVK, silver CBS, silver KVK, gold
- [ ] Quarterly schedule for bronze CBS
- [ ] Bronze CBS only materialises when it ingested new rows (so an empty run stops there)
- [ ] Silver, gold and dashboard refresh triggered only by new upstream data
- [ ] Bronze KVK started by a person (agent needs upload and confirmation); a new batch
      triggers silver KVK and gold
- [ ] Asset checks after each layer: row counts, no duplicate keys, keys present in
      dimensions; a failing check blocks downstream assets
- [ ] Freshness checks: CBS quarterly data at most 4 months behind, yearly data at most
      15 months, KVK snapshot within the agreed upload interval
- [ ] Alerts (email or Slack) for failed runs and failed checks
- [ ] Monthly maintenance job: KVK retention (latest + one snapshot per month), Delta
      `optimize` and `vacuum`; never at the same time as an ingestion
- [ ] Decide whether quarterly is often enough, given CBS release dates vary
      (a weekly run is cheap if nothing new is published)

## 6. Deployment (Docker Compose)
- [ ] `Dockerfile` for the Python code
- [ ] `docker-compose.yml` with dagster-webserver, dagster-daemon, postgres, streamlit,
      ollama and an on-demand kvk-agent service
- [ ] Volumes: shared `lakehouse/`, and `inbox/` for KVK uploads
- [ ] Pull the Ollama model on first start
- [ ] Document the minimum machine (about 12 GB RAM in total; disk depends on KVK
      retention)
- [ ] README: how to configure, start the stack and run the KVK agent

## 7. Tests and CI

The current tests (`tests/`) cover the pure logic and run without `deltalake`/`pyarrow`.

Still to add, using `tmp_path` and real Delta tables (on a local machine):
- [ ] Bronze: a revised period replaces its rows (no duplicates) after `write_bronze`
- [ ] Bronze: `done_keys()` reads back what `log_success()` wrote
- [ ] Silver: `cbs_silver.py` end to end from real bronze Delta tables, then read the
      silver tables back and check types (dates, Int64 counts, UTC timestamps)
- [ ] Optional live test, marked `@pytest.mark.live` and skipped by default: fetch one
      small CBS endpoint (e.g. `81589NED/Perioden`) to catch API or code-list changes

Later, as each part is built:
- [ ] Tests for the data checks, quarantine, scope cleanup, silver KVK, gold, the
      dashboard's data loading and the Dagster definitions
- [x] GitHub Actions stage 1: run pytest on every push (`.github/workflows/tests.yml`)
- [ ] GitHub Actions stage 3: build the Docker image after the tests pass (once the
      Dockerfile exists)
