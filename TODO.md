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
- [x] Fix what the run uncovered, and add a test for each fix (only fix: the KVK SBI
      column names, tested in `tests/test_kvk_logic.py`)
- [x] Turn the checks above into the Delta tests (section 7: `tests/test_delta.py`)
- [ ] Later: the same checks as Dagster asset checks (section 5)

## 0. Foundations
- [x] Decide the dashboard's audience: personal use, for an educational project
      (2026-10-05). Runs on this machine; no login or public hosting needed.
- [x] `config.yaml` + `config.py` (2026-10-05): paths, CBS base URL/tables/metadata/start
      year/period preference/pause, KVK URLs/columns/malformed limit, SBI crosswalk URL,
      Ollama host and model. Overrides: `config.local.yaml`, then environment variables
      `LAKEHOUSE__<SECTION>__<KEY>`; unknown keys are an error (`tests/test_config.py`)
- [x] Replace the hard-coded settings in `cbs_bronze.py`, `cbs_silver.py`,
      `kvk_bronze_agent.py`, `reference_bronze.py` and `inspect_lakehouse.py`; paths are
      now relative to the project folder, so the scripts work from any directory
- [ ] Add retention, Dagster schedules and alert targets to `config.yaml` when those
      parts are built

## 1. Bronze

### CBS: data checks before ingestion (done 2026-10-05)
A validation step between `fetch_all` and `write_bronze`; a request that fails is not
written and not logged, so it's retried next run. The expected structure comes from the
table's DataProperties, fetched live once per table per run, only when there is new data.
- [x] Every record has the table's dimension columns and the measures from DataProperties
- [x] The `Perioden` value in every record equals the period that was requested
- [x] Measures are numeric or null (for measures whose Datatype is numeric)
- [x] At least one record for a period that CBS lists as published (code lists too)
- [x] Extra: no two records with the same dimension keys
- [x] Put the checks in `checks/` (`checks/cbs.py`) so ingestion, pytest and Dagster
      asset checks reuse them
- Ran on all 92 batches already in bronze: all pass (no false alarms). A forced
  re-fetch of 80072NED with live checks: 26 of 26 passed.

### CBS: source lifecycle and scope (done 2026-10-05)
- [x] Ingest each table's `TableInfos`; warn when CBS marks a table as stopped or it
      hasn't been modified for longer than its normal interval. Every run fetches it (5
      small requests); a snapshot is stored in `bronze/<table>_tableinfos` only when its
      Modified/MetaDataModified dates change (part of the request key, so bronze keeps a
      history). Warnings never block ingestion (`check_table_lifecycle`, severity
      warning); `inspect_lakehouse.py` shows them separately from problems. Real run:
      83130NED warned as stopped (OutputStatus Gediscontinueerd); others regular
- [x] Cleanup step for scope changes: `python cbs_bronze.py --cleanup-scope [--dry-run]`
      removes tables no longer configured (with their log entries) and periods outside
      the start year / period preference. Periods are judged with the ingestion rule on
      CBS's current period list (the Perioden table in bronze is a one-time snapshot and
      would miss newer periods); periods CBS stopped listing are kept. KVK and reference
      tables are never touched. Tested on a copy of the real lakehouse with start year
      2025: 54 periods removed, the next run fetched nothing again

### Reference: SBI 2008 -> SBI 2025 crosswalk (done 2026-10-05)
The CBS tables used here classify sectors with SBI 2008 (CBS publishes SBI 2025 versions
as separate tables, e.g. 86280NED for the business counts). The KVK file mixes both:
of 2.55M SBI codes, 1.42M exist only in SBI 2025 and 0.50M only in SBI 2008; 224
distinct codes are in neither list (mostly 6-digit KVK sub-codes such as 011301).
Source: CBS "schakelschema SBI 2008 - SBI 2025" v2.0
(https://www.cbs.nl/-/media/cbs/onze-diensten/methoden/classificaties/documents/2026/schakelschema-sbi2008sbi2025-v20.xlsx).
- [x] `reference_bronze.py`: download (or `--file`), check, load into
      `lakehouse/bronze/reference/sbi2008_sbi2025` with a batch log
- [x] No double loads: the file's SHA-256 is logged and the same file is skipped
      (whatever its name); one Delta commit per file; rows of a batch that was written
      but never logged are removed before the next write; a new CBS version is a new batch
- [x] Checks: expected sheet and columns, codes 4-6 digits, no lost leading zeros, no
      duplicate (SBI 2008, SBI 2025) pairs. v2.0: 1,564 rows, 934 SBI 2008 and 907
      SBI 2025 codes, many-to-many (218 codes split, 277 merge). Warning (CBS quirk): in
      5 rows the SBI 2025 2-digit column was copied from the SBI 2008 side (71111 / 74)
- [x] Silver `map_sbi2008_sbi2025` from the latest batch only (see Silver CBS)
- [ ] Check periodically for a newer version of the file on the CBS classifications page
      and load it with `--url` (Dagster: a manual or monthly reference asset)
- [ ] Decide how silver KVK handles SBI 2025 main codes that came from several SBI 2008
      codes (see Silver KVK below)

### Quarantine (done 2026-10-05)
- [x] Batches that fail a runtime check go to `lakehouse/quarantine/<source>/<timestamp>_<name>/`
      with `report.json` and `records.json`; not logged as ingested (`checks/quarantine.py`)
- [x] The same content (SHA-256) is quarantined once per source and name, so a source that
      keeps failing doesn't add a copy on every run
- [x] KVK: rejected files are copied to quarantine with their verification report (not
      when the file was rejected only because it is already in bronze)
- [x] Reference files (SBI crosswalk) that fail their checks are quarantined the same way
- [x] `inspect_lakehouse.py` lists quarantined batches and counts them as problems
- [x] Monthly quarantine cleanup (decided 2026-10-05): `python maintenance.py quarantine
      [--dry-run]` removes entries that are resolved (same CBS table/entity/period, or a
      newer KVK/reference batch, ingested after the entry) and entries older than
      `quarantine.retention_days` (90). A problem that persists is quarantined again by
      the next run. Tested on real data: a corrupted 2026KW02 batch was kept while
      unresolved and removed after the correct data was ingested
- [ ] Schedule it monthly in Dagster (section 5, maintenance job)

### KVK
- [x] Load the first real file and confirm the CSV header (see "Bronze KVK" above)
- [x] One row per company, with SBI codes as comma-separated lists (see above)
- [x] Move the verification checks into `checks/kvk.py` so the agent and Dagster both use
      them (2026-10-05); the report on the real file is identical before and after
- [x] Read uploads from `inbox/kvk/` (2026-10-05): agent tool `list_inbox` (newest first,
      marks files already ingested, says what to do next), bare file names are looked up
      in the inbox, `python kvk_bronze_agent.py --inbox` loads the newest new file
      without the LLM. Checked with Ollama on the real inbox

## 2. Silver

### CBS
- [x] Dimensions, facts and mapping table (`cbs_silver.py`)
- [x] `map_sbi2008_sbi2025`: crosswalk with division per code (from the code itself),
      `sbi2025_targets`, `sbi2008_sources` and `link_type` (1:1, split, merge,
      many-to-many), `source_version`, `batch_id`
- [x] Write into a staging folder and replace the live tables only when every table
      succeeded; record the `run_id` on every table (2026-10-05). `silver/_staging/<run_id>/`
      is removed after each run; a required table that comes out empty fails the run,
      `map_sbi2008_sbi2025` is optional
- [x] Run log `lakehouse/_run_log` (`runlog.py`), one row per run: run_id, pipeline,
      start/end, status, error, rows per table, Delta version of every bronze input,
      git commit. Joinable with every silver table on `run_id`
- [x] `inspect_lakehouse.py` shows the latest runs and reports a failed latest run or
      silver tables whose run_id isn't the latest successful one

### KVK (postponed)
Build one thin table, `lakehouse/silver/kvk_company`, from
`lakehouse/bronze/kvk/basis_bedrijfsgegevens`. Same staging, `run_id` column and run log
as silver CBS (pipeline `silver_kvk`).

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
- [x] Decision (2026-10-05): a company's sector is its **main activity**
      ("Hoofdactiviteiten"). "SBI activiteiten" (all activities) is not used for the
      sector; keep it in bronze only.
- [x] Decision (2026-10-05): edge cases get a **marker column, not a rule**. Original
      codes stay unchanged; derived columns are filled only where the answer is
      unambiguous, and a flag says why one is missing or uncertain. Gold and the
      dashboard can then filter on the flags.
- [ ] Keep the original main code list as it is (`main_activity_codes`)
- [ ] Flags for the edge cases found in the 2026-10-01 file:
      - `has_multiple_main_activities`: 11,433 rows (0.6%) list several main codes
      - `has_no_main_activity`: 167,002 rows (nearly all inactive)
      - `is_kvk_subcode`: 6-digit KVK sub-codes such as 620102 (not in either SBI list)
      - `sbi_version`: `2025`, `2008`, `both` (code exists in both lists, so the number
        alone doesn't say which) or `unknown`. Active companies with one main code:
        942,484 only-2025, 307,732 both, 64 only-2008, 9 unknown; only-2008 codes occur
        almost only on inactive companies (371,900). So KVK has evidently recoded
        active companies to SBI 2025, but `both` stays `both` rather than being assumed
      - `sbi_mapping`: how the code maps to SBI 2008 for joining `dim_sector`
        (`1:1`, `merge` = the 2025 code came from several 2008 codes, `none`)
- [ ] Map SBI 2025 main codes to SBI 2008 with `map_sbi2008_sbi2025` (2025 -> 2008
      direction) for the join with the CBS `dim_sector`; fill the SBI 2008
      section/division only when all source codes share it, else leave it null and
      the flags explain why

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
- [ ] Mark groups with fewer than 10 companies (KVK re-identification rule) with a flag;
      the dashboard hides them by default (personal use, so marking is enough)
- [ ] Same staging-and-swap publishing, `run_id` and run log (`runlog.py`) as silver
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
- [ ] Monthly maintenance job: quarantine cleanup (built: `maintenance.py quarantine`),
      KVK retention (latest + one snapshot per month), Delta `optimize` and `vacuum`;
      never at the same time as an ingestion
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

## Known issues
- [ ] deltalake aborts at interpreter exit ("terminate called without an active
      exception", exit code 134) in some runs after reading a Delta table: ~50% with
      `to_pyarrow_table()`, ~5% with `to_pandas()`; seen with deltalake 1.4.2, 1.5.1 and
      1.6.6. The work is done before it happens, but the exit code is wrong (4 of 20
      `cbs_bronze.py` runs). Matters for Dagster and CI: find or report the upstream
      issue in delta-rs; meanwhile, judge a run by its log, not only its exit code.

## 7. Tests and CI

`tests/test_bronze_logic.py` and `tests/test_silver_logic.py` cover the pure logic;
`tests/test_kvk_logic.py` the KVK file verification; `tests/test_reference.py` the SBI
crosswalk; `tests/test_checks.py` the CBS and KVK checks and quarantine;
`tests/test_maintenance.py` the quarantine cleanup; `tests/test_config.py` the
configuration; `tests/test_delta.py` writes real Delta tables in `tmp_path` (209 tests,
~10 s). `conftest.py` fails a test when any module still points at the real
lakehouse or inbox. Checked by breaking the code on purpose:
removing the delete-before-write, an empty `done_keys()` and non-UTC silver timestamps
each make the right tests fail.

Delta tests (done 2026-10-05, `tests/test_delta.py`):
- [x] Bronze: a revised period replaces its rows (no duplicates) after `write_bronze`;
      also a repeated write (crash before the log) and a non-split request
- [x] Bronze: `done_keys()` reads back what `log_success()` wrote
- [x] Bronze: `run()` with fake HTTP but real Delta: second run makes no data
      requests; a status change refetches only that period and replaces its rows
- [x] Silver: `cbs_silver.py` end to end from real bronze Delta tables; values equal
      the in-memory build; types read back (date32, int64 counts, bool, UTC timestamps);
      a rerun replaces the tables
- [x] Optional live test, marked `@pytest.mark.live` and skipped by default (also in
      `pytest.ini`): fetches `81589NED/Perioden`. Run with `pytest -m live`

Later, as each part is built:
- [ ] Tests for the data checks, quarantine, scope cleanup, silver KVK, gold, the
      dashboard's data loading and the Dagster definitions
- [x] GitHub Actions stage 1: run pytest on every push (`.github/workflows/tests.yml`)
- [ ] GitHub Actions stage 3: build the Docker image after the tests pass (once the
      Dockerfile exists)
