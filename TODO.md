# TODO

Open work, in the order of `PROJECT.md`. Each phase builds on the one before it.

Rule for every phase: run each new layer on real data and check it before building the
next layer on top of it.

## Where to continue (status 2026-10-05)

Built and run on real data: bronze (CBS, KVK, SBI crosswalk) with checks and quarantine,
silver (CBS, KVK), gold (5 tables), the Streamlit dashboard, Dagster orchestration,
Docker Compose, config, run log, 286 tests.
All committed.

**Next step: 7. Tests and CI** (the remaining items). Sections 4-6 are built; see the
README for how to start everything (locally or with `docker compose up -d`). Open
question for the *local* dashboard: limit it to this machine (see section 4; in Docker
it already is).

Run order (from the project folder, with the virtualenv):

    .venv/bin/python cbs_bronze.py                 # CBS API -> bronze (only new data)
    .venv/bin/python reference_bronze.py           # SBI crosswalk (skips a known file)
    .venv/bin/python kvk_bronze_agent.py --inbox   # newest new KVK file in inbox/kvk/
    .venv/bin/python cbs_silver.py
    .venv/bin/python kvk_silver.py                 # needs silver CBS (crosswalk)
    .venv/bin/python gold.py
    .venv/bin/python inspect_lakehouse.py          # overview, warnings, problems
    .venv/bin/streamlit run dashboard/app.py       # the dashboard
    DAGSTER_HOME=$PWD/orchestration .venv/bin/dagster dev -f orchestration/definitions.py
                                                   # Dagster UI: http://localhost:3000
    .venv/bin/pytest                               # tests (live CBS test: -m live)

Local setup: `.venv/` holds the Python packages (`pip install -r requirements-dev.txt`).
Ollama is installed in `~/.local/ollama` (no sudo) with model `qwen2.5:7b`; start the
server with `~/.local/ollama/bin/ollama serve` before using the KVK agent in chat mode
(not needed for `--inbox` / `--file`).

## First real run (done 2026-10-05)

Done on this machine, one layer at a time, using
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
- [x] Fixed 2026-10-05: "942/9420 Werknemersorganisaties" (sectors: trade unions) were
      parsed as size classes because their name contains "werknemer"; size words now
      have to be whole words. Checked over all 1,678 titles: only these two changed
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
- [x] Decision (2026-10-05): no SCD type 2 in silver. Silver keeps only current rows (full
      rebuild, 1.6 MB live); SCD2 would make tables larger, need merge logic and force
      every gold query to filter on "current". Disk growth comes from Delta keeping old
      versions after each overwrite (~1.6 MB per run) and is handled by the monthly
      `vacuum`. CBS revision history (provisional -> final) is not tracked; revisit only
      if revisions become an analysis question. KVK can't use SCD2: no company key

### KVK (built 2026-10-05: `kvk_silver.py`)
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
- [x] `snapshot_date`, `batch_id`, `is_latest` (from `_batch_id` / `_ingested_at`;
      snapshot_date is the ingest day, the file date isn't in bronze), `source_row_number`
- [x] `start_date` (date) + `has_invalid_start_date`, `is_active` (bool from J/N, null
      otherwise)
- [x] `legal_form`, `insolvency_code`, `postcode_region`, `member_state` (empty -> null)
- [x] `sbi2008_code`, `sbi2008_section`, `sbi2008_division` (joinable with `dim_sector`
      on `sbi_code`; every section/division found in the real data exists there)
- [x] Decision (2026-10-05): a company's sector is its **main activity**
      ("Hoofdactiviteiten"). "SBI activiteiten" (all activities) is not used for the
      sector; keep it in bronze only.
- [x] Decision (2026-10-05): edge cases get a **marker column, not a rule**. Original
      codes stay unchanged; derived columns are filled only where the answer is
      unambiguous, and a flag says why one is missing or uncertain. Gold and the
      dashboard can then filter on the flags.
- [x] Keep the original main code list as it is (`main_activity_codes`), plus
      `main_activity_count`
- [x] Flags for the edge cases found in the 2026-10-01 file:
      - `has_multiple_main_activities`: 11,433 rows (0.6%) list several main codes
      - `has_no_main_activity`: 167,002 rows (nearly all inactive)
      - `is_kvk_subcode`: 6-digit KVK sub-codes such as 620102 (not in either SBI list)
      - `sbi_version`: `2025`, `2008`, `both` (code exists in both lists, so the number
        alone doesn't say which), `unknown`, or `mixed` (several codes of different
        versions). Active companies with one main code:
        942,484 only-2025, 307,732 both, 64 only-2008, 9 unknown; only-2008 codes occur
        almost only on inactive companies (371,900). So KVK has evidently recoded
        active companies to SBI 2025, but `both` stays `both` rather than being assumed
      - `sbi_mapping`: how the code maps to SBI 2008 for joining `dim_sector`
        (`sbi2008`, `1:1`, `merge` = the 2025 code came from several 2008 codes,
        `ambiguous_version`, `unknown_code`, `none`; for several codes the least
        certain one), and `sbi2008_candidates` (number of possible SBI 2008 codes)
- [x] Map SBI 2025 main codes to SBI 2008 with `map_sbi2008_sbi2025` (2025 -> 2008
      direction) for the join with the CBS `dim_sector`; fill the SBI 2008
      section/division only when all source codes share it, else leave it null and
      the flags explain why
- Result on the real file (2026-10-05): 1,277,560 active companies, 90.2% with an SBI
  2008 section and division, 72.1% with a single SBI 2008 code. Section filled per
  mapping: 1:1 100%, ambiguous_version 90%, merge 71%. 12 s, ~1.3 GB peak memory
  (2.3 GB before the memory fix of 2026-10-08)
  (matters for Docker sizing), 23 MB per snapshot in silver
- [x] Decision (2026-10-05): 4-digit KVK codes such as 7111 (SBI 2008 class codes without
      a subclass, not in the crosswalk) stay `unknown_code`; no extra mapping rule

Notes:
- There is no KVK number or company ID in the open dataset, so individual companies
  can't be tracked across snapshots; compare snapshots only in aggregate.
- Gold counts companies with `is_latest`; never sum across snapshots.

## 3. Gold (built 2026-10-05: `gold.py`)
Main question: which company characteristics (sector, size class) go together with
higher sickness absence. See `PROJECT.md` for the sub-questions.
- [x] `gold_absence_by_sector`: absence % per sector (every CBS level, `sbi_level` says
      which; the dashboard defaults to sections) and national total; difference from the
      total in percentage points; rank within the level per quarter; change versus the
      same quarter a year earlier; rolling four-quarter average (only with four
      consecutive quarters). Checked by hand: C Industrie 2026KW02 6.2%, +0.8 pp vs
      national, +0.2 pp vs 2025KW02, rolling 6.3%
- [x] `gold_absence_by_size`: the same comparisons per size class, quarterly absence % from
      80072NED (persons employed) and yearly share of employees absent from 86009NED
      (employees, with 95% bounds), side by side. Real data: absence rises with size
      (2026KW02: 1-9 2.5%, 10-99 4.7%, 100+ 6.2%)
- [x] `gold_absence_profile`: every 86009NED measure per sector and size class with 95%
      interval, national value, `differs_from_national` (interval excludes the national
      value) + `direction`, and `rank_in_topic` (e.g. top causes)
- [x] `gold_business_context`: businesses per sector and size band (81589NED). CBS rounds
      counts to fives, so bands can differ from the total by up to 15
- [x] `gold_kvk_context` (split off from business context: other grain and period):
      active companies and median company age per SBI 2008 section and legal form from
      the latest KVK snapshot; companies without a determined section as "unknown"
- [x] Every gold row carries its period status (`is_provisional`) and `data_as_of`;
      comparisons that mix provisional and final figures are flagged
      (`yoy_mixes_status`, `rolling_4q_has_provisional`); final rows never mix
- [x] Mark groups with fewer than 10 companies (KVK re-identification rule) with
      `is_small_group`; the dashboard hides them by default
- [x] Same staging-and-swap publishing, `run_id` and run log as silver (pipeline `gold`);
      the run log's inputs include the silver run_ids read. Gold refuses to build from
      silver CBS tables of different runs
- [x] Decision (2026-10-05): provisional figures are shown by default, clearly marked
      (`period_status`, `is_provisional`), with a dashboard switch "final figures only".
      Comparisons that mix provisional and final figures are flagged

## 4. Streamlit dashboard (built 2026-10-06: `dashboard/`)
Run: `.venv/bin/streamlit run dashboard/app.py`
- [x] App in `dashboard/` that reads gold tables only
- [x] Cache per gold Delta table version (and folder), so new data shows without a restart
- [x] Check that the gold tables shown come from the same `run_id` (warning per page)
- [x] Pages: Overview, Sectors, Size classes, Behind the numbers, Context
- [x] Filters for period, sector level, sector and size class; seasonally fair
      comparisons (same quarter last year, rolling four-quarter average)
- [x] Show data freshness and provisional/final status on every page; provisional
      points drawn hollow, with a footnote
- [x] Switch "final figures only" (default off, kept across pages), a note when a
      comparison mixes provisional and final figures, and an explanation when nothing
      final is left to show
- [x] Credit CBS and KVK (CC BY 4.0) on every page (sidebar)
- [x] Data dictionary page: columns and types read from the gold tables, survey measures
      from CBS metadata
- [x] Table view on every page; KVK groups under 10 hidden by default
- Checked on real data: every page renders in both switch states (AppTest); charts
  rendered to images and reviewed (diverging bars: colour marks the side only; single
  series without a legend; direct labels on the size lines)
- [ ] Optional: re-run the palette validator once Node.js is available (the values are
      the guidance's published, validated set)
- [ ] Open question (2026-10-06): limit the dashboard to this machine? Streamlit also
      listens on the network address by default (Dagster already listens on 127.0.0.1
      only). Fix: `.streamlit/config.toml` with `[server] address = "localhost"`; for
      Docker (section 6) the port mapping decides instead

## 5. Orchestration (Dagster) (built 2026-10-06: `orchestration/`)
Run: `DAGSTER_HOME=$PWD/orchestration .venv/bin/dagster dev -f orchestration/definitions.py`
- [x] Assets: cbs_bronze, sbi_crosswalk, kvk_bronze, silver_cbs, silver_kvk, gold
- [x] Decision (2026-10-06): CBS is checked **monthly** (1st, 06:00), not quarterly: CBS
      release dates vary, and a run without new data costs a few small requests
- [x] Bronze CBS (and the crosswalk) only materialise when something new was loaded
      (`skippable`); an empty run stops there
- [x] Silver and gold rebuild only after new upstream data (eager automation condition)
      and only when the upstream blocking checks passed; the dashboard needs no trigger
      (it reads gold by Delta version)
- [x] Bronze KVK started by a person in the UI, with the file name typed as confirmation
      (`ingest_confirmed_file`, deliberately not an agent tool); a new batch triggers
      silver KVK and gold
- [x] Asset checks after each layer (`checks/layers.py`): tables filled, one run_id,
      unique keys, keys present in dimensions; blocking. Checked on real data: all pass
- [x] Freshness checks (`checks/freshness.py`, warnings): CBS quarterly <= 4 months
      behind, yearly <= 15 months (stopped tables skipped), KVK snapshot <= 45 days
      (`kvk.max_snapshot_age_days`); own monthly job, because checks on a skipped asset
      don't run
- [x] Decision (2026-10-06): failures are shown in the Dagster UI; no email/Slack alerts
- [x] Monthly maintenance job (15th, 03:00): quarantine cleanup, KVK retention (latest +
      first snapshot of each month; removed batches marked "retired" in the batch log),
      Delta optimize and vacuum (`delta.vacuum_retention_hours`, 7 days); at most one
      writing run at a time (`orchestration/dagster.yaml`)
- [x] First-time setup job `register_existing_data`: records bronze loaded before Dagster
      as materialized; without it automatic rebuilds never start (found while testing)
- Real runs (2026-10-06): monthly CBS ingest skipped (nothing new), silver + gold built
  with all 12 checks passing, freshness and maintenance jobs succeeded
- Found while testing: Dagster runs steps in their own process and drops the project
  folder from the import path there, so modules the scripts import late failed
  ("No module named 'publish'"); definitions.py now imports them up front, and a test
  runs the real `dagster` command to catch this
- [ ] Optional later: email or Slack alerts (a run failure sensor)

## 6. Deployment (Docker Compose) (built 2026-10-08)
Run: `docker compose up -d` (Dagster http://localhost:3000, dashboard :8501)
- [x] `Dockerfile`: one image (python 3.12, 1.3 GB) for Dagster, the dashboard and the
      agent; `CODE_VERSION` build arg for the run log (no git in the image)
- [x] `docker-compose.yml`: dagster-webserver, dagster-daemon, postgres (Dagster's run
      history; `orchestration/docker/dagster.yaml`), dashboard; ollama, ollama-pull and
      kvk-agent behind the profile `agent` (the model is a 4.7 GB download)
- [x] Volumes: `lakehouse/` and `inbox/` bind-mounted from the project folder (Docker
      and local runs share the data); containers run as the host user, so files stay
      yours; ports on 127.0.0.1 only (answers the open question for Docker)
- [x] Pull the Ollama model on first start (`ollama-pull`), or reuse the host's models
      with `OLLAMA_DIR=~/.ollama`
- [x] Minimum machine documented (README), measured: Docker ~3 GB memory for the
      pipeline, ~8 GB with the chat agent; disk ~1.3 GB image + lakehouse
- [x] README: Docker section (start, stop, first time, agent, minimum machine)
- Tested in Docker (2026-10-08): jobs through the queue (register_existing_data,
  freshness), silver CBS, silver KVK and gold with all checks passing, the automatic
  rebuild of gold after silver KVK, every dashboard page inside its container
- Found while testing, fixed:
  - dagster-postgres needs a driver it doesn't install, and with SQLAlchemy 2.1 queued
    runs failed with an SQL syntax error: the image pins SQLAlchemy 2.0 + psycopg2
  - Docker Desktop here has 3.67 GB: silver KVK (2.3 GB) and gold (1.6 GB) were killed
    for lack of memory. Silver KVK now reads only the columns it needs and keeps
    repeated text as categories (1.3 GB); gold reads only the latest KVK snapshot's
    columns (1.1 GB). Output checked identical; silver KVK rows now in a fixed order
- [x] Docker Desktop's memory raised to 8 GB (2026-10-08; `MemoryMiB` in
      ~/.docker/desktop/settings-store.json, a backup of the old file next to it)
- [x] KVK chat agent tried in Docker (2026-10-08): Ollama in Docker with the model copied
      from ~/.ollama (no download), `docker compose --profile agent run --rm kvk-agent`;
      it read the inbox and lakehouse through the mounts and answered correctly
- Fixed while trying it: `up` also started the interactive kvk-agent in the background
  (now its own profile, started only with `run`); mounting ~/.ollama was dropped as an
  option (Ollama runs as root and could leave root-owned files in the home folder) in
  favour of copying the model into the Docker volume

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
configuration; `tests/test_kvk_silver.py` silver KVK; `tests/test_gold.py` gold;
`tests/test_dashboard.py` the dashboard (every page headless); `tests/test_orchestration.py`
Dagster; `tests/test_delta.py` writes real Delta tables in `tmp_path` (286 tests, ~55 s). `conftest.py` fails a test when any module still points at the real
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
- [x] GitHub Actions stage 3: build the Docker image after the tests pass, and check the
      code and the Dagster definitions load in it (2026-10-08)
