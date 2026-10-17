# TODO

Open work, in the order of `docs/PROJECT.md`. Each phase builds on the one before it.

Rule for every phase: run each new layer on real data and check it before building the
next layer on top of it.

## Where to continue (status 2026-10-17)

Built and run on real data: bronze (CBS, KVK, SBI crosswalk) with checks and quarantine
(CBS bronze with a column per field next to `raw_json`),
silver (CBS, KVK), gold (7 tables, with the risk index), the Streamlit dashboard, Dagster orchestration,
Docker Compose (including the KVK chat agent), column lineage, config, run log, the
lakehouse viewer with data quality statistics (`python -m tools.lakehouse_viewer`),
incremental silver KVK (2026-10-10), 373 tests. Code is organised in `pipeline/` (per
layer) and `tools/` (2026-10-08), with a README per phase. Requirements in
docs/requirements.md (every Must met; open: FR-19 alerts, NFR-15 default password, left
open on purpose), Azure design in docs/cloud_design.md (not built). All committed; no containers or servers left running. See the README for how to start
everything (locally, or with `docker compose up -d`).

**The assignment** (Erasmus, Data and AI Engineering; slides "Day 1 - assignment", pages
10–18). Organisation: an **income-protection insurer** (AOV, sick leave and incapacity
for work). Proposal approved. Code on GitHub:
<https://github.com/stefancodecommit/AOV_Lakehouse>. **Deadline report + code: 4 January
2027**; presentation with demo in January. Grading: technical 40%, business value 25%,
data & AI engineering 20%, documentation 15%. See section "Assignment" below.

**Next steps, in order** (details and slide references in section "Assignment"):
1. ~~Frame the project for the insurer~~ (done 2026-10-15)
2. ~~Gold for the insurer~~ (done 2026-10-15: risk index, dashboard page)
3. ~~Requirements~~ and ~~README per phase~~ (done 2026-10-17)
4. ~~Azure design~~, ~~GDPR and risks~~, ~~GenAI section~~ (done 2026-10-17; the
   author's own reflection in docs/genai_use.md is still to write)
5. Report (≤5000 words) + executive summary + canvas; presentation and demo

Technical items, lower priority for the assignment:
- **pandas 3** (section 7): make the code work with it, then raise the pin
- **When CBS publishes a new SBI crosswalk version:** update
  `reference.sbi_crosswalk_url` in config.yaml (section 1, Reference)
- **Open question** (Lakehouse viewer section): `CategoryGroupID` in `81644ned_regios`
  is typed as text because every value is empty; give all-empty fields Arrow's null type?
- **Open question** (Silver KVK): `snapshot_date` is the upload date, not KVK's own date
  of the file (matters for data provenance)
- Optional: Polars for silver KVK (Silver KVK); email/Slack alerts (section 5); re-run
  the palette validator (needs Node.js, section 4); report the deltalake exit race
  upstream (Known issues)

Run order (from the project folder, with the virtualenv):

    .venv/bin/python -m pipeline.bronze.cbs                # CBS API -> bronze (only new data)
    .venv/bin/python -m pipeline.bronze.reference          # SBI crosswalk (skips a known file)
    .venv/bin/python -m pipeline.bronze.kvk_agent --inbox  # newest new KVK file in inbox/kvk/
    .venv/bin/python -m pipeline.silver.cbs
    .venv/bin/python -m pipeline.silver.kvk  # needs silver CBS (crosswalk)
    .venv/bin/python -m pipeline.gold.build
    .venv/bin/python -m tools.inspect_lakehouse  # overview, warnings, problems
    .venv/bin/streamlit run dashboard/app.py     # the dashboard
    DAGSTER_HOME=$PWD/orchestration .venv/bin/dagster dev -f orchestration/definitions.py
                                                   # Dagster UI: http://localhost:3000
    .venv/bin/pytest                               # tests (live CBS test: -m live)

Local setup: `.venv/` holds the Python packages (`pip install -r requirements-dev.txt`).
Ollama is installed in `~/.local/ollama` (no sudo) with model `qwen2.5:7b`; start the
server with `~/.local/ollama/bin/ollama serve` before using the KVK agent in chat mode
(not needed for `--inbox` / `--file`).

## First real run (done 2026-10-05)

Done on this machine, one layer at a time, using
`python -m tools.inspect_lakehouse` after each step for an overview (rows, periods, latest
ingest, problems).

### Bronze CBS (done 2026-10-05)
- [x] `python -m pipeline.bronze.cbs --dry-run`: check the periods found per dataset (2020 onward)
      86009NED 2020–2025 (JJ), 80072NED 2020KW01–2026KW02, 83130NED 2020–2023 (JJ),
      81644NED 2020–2026 (JJ), 81589NED 2020KW01–2026KW03. 83130NED ends at 2023
      because CBS marks it as discontinued (`Frequency = Stopgezet`, last modified
      2024-04-17); 86009NED is its successor.
- [x] `python -m pipeline.bronze.cbs`: full first load; note how long it takes and the disk used
      92 requests, 0 failures, 39 s, 6.6 MB on disk, ~240 MB peak memory.
- [x] `python -m tools.inspect_lakehouse --layer bronze`: every configured endpoint has a table, no
      failures in the log (28 tables incl. `_ingestion_log`, no problems)
- [x] Run `pipeline/bronze/cbs.py` a second time: it should make no data requests
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
- [x] Load one real file with the agent (`python -m pipeline.bronze.kvk_agent`)
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
- [x] `python -m pipeline.silver.cbs --preview`: row counts per table, and any warnings about
      keys missing from the code lists (no warnings)
- [x] `python -m pipeline.silver.cbs`, then `python -m tools.inspect_lakehouse --layer silver`: all ten tables
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
- [x] Later: the same checks as Dagster asset checks (section 5, `checks/layers.py`)

## Assignment (Data and AI Engineering, deadline 2027-01-04)

Checked against the slides (pages 10–18) on 2026-10-15. Strong already: ETL pipeline and
automation, performance work, tests (≥1 required; 359), pipeline steps (≥3 required; all
six covered: ingestion, transformation, serving, model design/testing of the data models,
model integration CBS+KVK and the LLM intake agent, deployment & monitoring), technical
documentation, AI-driven automation (KVK agent).

- [x] Proposal submitted and approved by the teacher
- [x] Code on GitHub (2026-10-15): <https://github.com/stefancodecommit/AOV_Lakehouse>,
      public; the first CI run passed (section 7). Commits show the author's email
- [x] **Insurer framing** (2026-10-15; business value 25%; criteria 1, 3, 7): docs/PROJECT.md
      "The organisation and its need" (need: own benchmarks use privacy-sensitive data, so
      CBS as the privacy-free second-best; users: data analyst finds risks per SBI ->
      manager -> risk management and account managers; business goals,
      scope and boundaries: public aggregated data only, people decide, figures describe
      employees not the self-employed, correlation not cause); README intro. Users: underwriters/actuaries
      (premium differentiation per sector and size), risk management (portfolio early
      warning), account managers (prevention advice to employer clients), sales/product
      (market size)
- [x] **Gold for the insurer** (built 2026-10-15; technical 40%; criterion 5 "quantitative models"):
      self-employed per sector from CBS 81589NED `NatuurlijkePersonen_12` (KVK's open
      data has only BV/NV, no sole traders); an explainable risk index per sector and
      size class (relative to the national figure, with an uncertainty band and a
      trend), a white-box model as on slide 48; tests that validate it.
      Done: `gold_risk_index` + `gold_risk_index_history` (defaults: window 8 quarters,
      trend ±0.05 per year, both self-employed measures, sectors and size classes),
      dashboard page "Risk index", lineage, 7 tests (known answers; season removed;
      final basis; content of the page). Real data 2026 Q2: 5 of 19 sections above
      national (healthcare 1.40), size 1–9 at 0.48 and 100+ at 1.16; only section S
      (Overige dienstverlening) rising (+0.06 per year), still below national
- [x] **Requirements** (2026-10-17; criterion 3; report chapter "Eisenpakket"):
      [docs/requirements.md](docs/requirements.md), in English: stakeholders (suppliers,
      developers, users, DPO), 7 use cases, 19 functional and 27 non-functional
      requirements with MoSCoW priority, status and the test or measurement that verifies
      each (50 tests cited, all checked to exist), 6 organisational requirements, 9
      conditions for use. Open: FR-19 alerts (Could); partly met: NFR-15 default
      Postgres password (Should). NFR-13 met since 2026-10-17: every Must is met
- [x] **README per phase** (2026-10-17; slide 22): pipeline/bronze (1 ingestion),
      pipeline/silver (2 transformation, 5 model integration), pipeline/gold (3 serving,
      4 the risk model), dashboard (3), orchestration (6 deployment & monitoring), tests
      (testing); each with design decisions, how to run, and a proof of working (test
      command + expected result). Phase map in the README. Closes NFR-23
- [x] **Cloud design, Azure** (2026-10-17; learning objective 3):
      [docs/cloud_design.md](docs/cloud_design.md), design only, nothing built: three
      options weighed (chosen: the existing containers on Azure Container Apps with
      Azure Storage and PostgreSQL; rejected for now: Data Factory + Databricks/Fabric,
      and Functions), architecture diagram, component mapping, storage in two steps
      (Azure Files without code change, then ADLS Gen2), security per CIAA (Entra ID,
      managed identities, Key Vault, private endpoints), monitoring and alerts, CI/CD
      with OIDC and Bicep, indicative costs (~€50–200/month), migration path, risks.
      Would close FR-19 and NFR-15 when built
- [x] **GDPR and risks** (2026-10-17; learning objective 6, criterion 7):
      [docs/privacy_and_risks.md](docs/privacy_and_risks.md): data inventory checked on the
      real lakehouse (no personal data about insured persons; minimal personal data of
      employees: `confirmed_by` in the KVK upload log; 851,804 of 1.92M KVK companies
      unique on postcode region + SBI code + start date, so row-level KVK stays
      internal), GDPR principles, DPIA needed only before linking claims data, other
      rules (KVK terms, CC BY, EU AI Act, equal treatment in pricing), risk register
      R1–R18 with measures, actions. New open actions from it: backup of lakehouse/ and
      inbox/kvk/ (R14), vulnerability scanning in CI (R13), retention for the KVK upload
      log and Dagster history (R10)
- [x] **GenAI section** (2026-10-17; slide 15, required): [docs/genai_use.md](docs/genai_use.md),
      in English (the report summarises it in Dutch): tools, why, the way of working,
      the decisions and ideas that came from the author (from the session record), how
      results were checked, advantages, risks with 7 real incidents and how each was
      caught, general risks (understanding, confidentiality: no insurer data shared,
      reliability, over-reliance, licensing). 55 of 56 commits credit Claude
- [ ] **Author's own reflection** in docs/genai_use.md section 6 (only the author can
      write it)
- [ ] **Report** ≤5000 words in the structure of slide 17, with an **executive
      summary** (slide 16) and the canvas (slide 20). **In Dutch** (decided
      2026-10-15); everything else (code, comments, documentation, dashboard) stays in
      English
- [ ] **Presentation with demo** (January): dashboard, Dagster, lineage explorer,
      lakehouse viewer

## 0. Foundations
- [x] Decide the dashboard's audience: personal use, for an educational project
      (2026-10-05). Runs on this machine; no login or public hosting needed.
      Update 2026-10-15: framed for an income-protection insurer (the assignment's
      organisation); still runs locally as a prototype
- [x] `config.yaml` + `pipeline/common/config.py` (2026-10-05): paths, CBS base URL/tables/metadata/start
      year/period preference/pause, KVK URLs/columns/malformed limit, SBI crosswalk URL,
      Ollama host and model. Overrides: `config.local.yaml`, then environment variables
      `LAKEHOUSE__<SECTION>__<KEY>`; unknown keys are an error (`tests/test_config.py`)
- [x] Replace the hard-coded settings in `pipeline/bronze/cbs.py`, `pipeline/silver/cbs.py`,
      `pipeline/bronze/kvk_agent.py`, `pipeline/bronze/reference.py` and `tools/inspect_lakehouse.py`; paths are
      now relative to the project folder, so the scripts work from any directory
- [x] Add retention, Dagster schedules and alert targets to `config.yaml` when those
      parts are built: `quarantine.retention_days`, `delta.vacuum_retention_hours`,
      `kvk.max_snapshot_age_days`, `schedules` (2026-10-08); no alert targets (alerts are
      shown in the Dagster UI)

## 1. Bronze

### CBS: structured bronze, raw_json kept (done 2026-10-08)
Bronze held each CBS record only as JSON text, so it couldn't be queried without parsing.
Now every CBS bronze table also has one column per field, values as received (padding
kept; stripping and renaming stay in silver).
- [x] Field types from the table's DataProperties (dimensions text, measures by Datatype),
      otherwise inferred; nested values as JSON text; a field named like a lineage column
      becomes `field_<name>`
- [x] A later batch can't change a column's type; a field CBS adds becomes a new column
- [x] `python -m pipeline.bronze.cbs --rebuild-columns`: field columns from `raw_json`, no API calls; run
      on the real lakehouse (32 tables)
- [x] Silver reads the columns (falls back to `raw_json` for bronze without them); all 11
      silver tables verified identical to before the change on the real data
- [x] Lineage, docs/lineage.*, bronze_to_silver.md, README and PROJECT updated

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
      warning); `tools/inspect_lakehouse.py` shows them separately from problems. Real run:
      83130NED warned as stopped (OutputStatus Gediscontinueerd); others regular
- [x] Cleanup step for scope changes: `python -m pipeline.bronze.cbs --cleanup-scope [--dry-run]`
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
- [x] `pipeline/bronze/reference.py`: download (or `--file`), check, load into
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
      and load it with `--url`. Dagster's monthly `sbi_crosswalk` only re-downloads the
      configured URL; a new CBS version gets a new URL, so a person must update
      `reference.sbi_crosswalk_url` in config.yaml
- [x] Decide how silver KVK handles SBI 2025 main codes that came from several SBI 2008
      codes: flags, derived columns only when unambiguous (see Silver KVK below)

### Quarantine (done 2026-10-05)
- [x] Batches that fail a runtime check go to `lakehouse/quarantine/<source>/<timestamp>_<name>/`
      with `report.json` and `records.json`; not logged as ingested (`checks/quarantine.py`)
- [x] The same content (SHA-256) is quarantined once per source and name, so a source that
      keeps failing doesn't add a copy on every run
- [x] KVK: rejected files are copied to quarantine with their verification report (not
      when the file was rejected only because it is already in bronze)
- [x] Reference files (SBI crosswalk) that fail their checks are quarantined the same way
- [x] `tools/inspect_lakehouse.py` lists quarantined batches and counts them as problems
- [x] Monthly quarantine cleanup (decided 2026-10-05): `python -m tools.maintenance quarantine
      [--dry-run]` removes entries that are resolved (same CBS table/entity/period, or a
      newer KVK/reference batch, ingested after the entry) and entries older than
      `quarantine.retention_days` (90). A problem that persists is quarantined again by
      the next run. Tested on real data: a corrupted 2026KW02 batch was kept while
      unresolved and removed after the correct data was ingested
- [x] Schedule it monthly in Dagster (section 5, maintenance job)

### KVK
- [x] Load the first real file and confirm the CSV header (see "Bronze KVK" above)
- [x] One row per company, with SBI codes as comma-separated lists (see above)
- [x] Move the verification checks into `checks/kvk.py` so the agent and Dagster both use
      them (2026-10-05); the report on the real file is identical before and after
- [x] Read uploads from `inbox/kvk/` (2026-10-05): agent tool `list_inbox` (newest first,
      marks files already ingested, says what to do next), bare file names are looked up
      in the inbox, `python -m pipeline.bronze.kvk_agent --inbox` loads the newest new file
      without the LLM. Checked with Ollama on the real inbox

## 2. Silver

### CBS
- [x] Dimensions, facts and mapping table (`pipeline/silver/cbs.py`)
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
- [x] Run log `lakehouse/_run_log` (`pipeline/common/runlog.py`), one row per run: run_id, pipeline,
      start/end, status, error, rows per table, Delta version of every bronze input,
      git commit. Joinable with every silver table on `run_id`
- [x] `tools/inspect_lakehouse.py` shows the latest runs and reports a failed latest run or
      silver tables whose run_id isn't the latest successful one
- [x] Decision (2026-10-05): no SCD type 2 in silver. Silver keeps only current rows (full
      rebuild, 1.6 MB live); SCD2 would make tables larger, need merge logic and force
      every gold query to filter on "current". Disk growth comes from Delta keeping old
      versions after each overwrite (~1.6 MB per run) and is handled by the monthly
      `vacuum`. CBS revision history (provisional -> final) is not tracked; revisit only
      if revisions become an analysis question. KVK can't use SCD2: no company key

### KVK (built 2026-10-05: `pipeline/silver/kvk.py`)
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

### KVK: incremental updates (done 2026-10-10)
Why: KVK is the part that grows. With the latest snapshot plus one per month kept, a full
rebuild every run would need ~1.2 GB more memory per month kept (~23M rows after a year).
- [x] A run compares the successful batches in bronze's `_batch_log` with the batches in
      kvk_company: builds only new snapshots, deletes retired ones, one snapshot per
      commit; `is_latest` moves in one commit (never two latest, never none)
- [x] Full rebuild (newest snapshot first, then the older ones, one at a time) when the
      SBI crosswalk's content changed (fingerprint in the run log), when `LOGIC_VERSION`
      was raised, or with `--full`; `--preview` shows what the next run would do
- [x] A run that stops halfway is finished by the next one: a snapshot from a run that
      didn't succeed is rebuilt. `run_id` is now per snapshot; the layer check is "one
      run per snapshot" (Dagster too); `inspect_lakehouse` flags rows of failed runs
- [x] Partitioned by `batch_id`: the batches present are read from the Delta log (no
      data read), a retired snapshot is dropped as a folder
- [x] Memory, measured on simulated monthly uploads (4 snapshots, 7.7M rows): full
      rebuild 1.65 GB (3.2 GB before the fixes below), adding one 1.8 GB, nothing new
      0.18 GB. Real data: full rebuild 1.46 GB, nothing new 0.7 s. Fixes found by
      measuring: present batches from the Delta log instead of reading two columns of
      every row; one snapshot per write call (the libraries keep memory in proportion
      to everything passed through one call); staging before a partitioned write
      (+400 MB otherwise); Arrow's system allocator and at most 2 glibc arenas, so
      memory is handed back between snapshots (`lean_memory`)
- [x] Tests: the plan (full/incremental/redo), a second upload (only that one is read
      from bronze), nothing new (no new Delta version), a retired batch, a crash before
      the flag moves, a full rebuild stopped after the first snapshot, a crosswalk
      change (same content: nothing; other content: full); each time the table equals
      a full rebuild. Two deliberately broken versions (flag never moves; failed runs
      never redone) each fail tests. Real data: identical to the table of 2026-10-08
- [x] Safety net (found while testing): `monkeypatch.undo()` in a test also undid the
      isolated lakehouse; caught before anything was written, but conftest now checks
      the paths at the end of every test too, and refuses every Delta write to the real
      lakehouse or inbox for the whole test session (proven with a probe test)
- [ ] Optional, later: Polars for silver KVK and gold's KVK table (the one place with
      millions of rows); not needed for memory any more, a learning exercise
- [ ] Open question: `snapshot_date` is the upload date; KVK's own date may be in the file

Notes:
- There is no KVK number or company ID in the open dataset, so individual companies
  can't be tracked across snapshots; compare snapshots only in aggregate.
- Gold counts companies with `is_latest`; never sum across snapshots.

## 3. Gold (built 2026-10-05: `pipeline/gold/build.py`)
Main question: which company characteristics (sector, size class) go together with
higher sickness absence. See `docs/PROJECT.md` for the sub-questions.
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
- [x] Limit the dashboard to this machine (done 2026-10-17, requirement NFR-13):
      `.streamlit/config.toml` with `address = "127.0.0.1"`. Proven: before, Streamlit
      listened on `*:8598` and answered on the network address; after, it listens on
      127.0.0.1 and the network address is refused. Docker keeps working: its command
      line `--server.address 0.0.0.0` takes precedence (checked), and the port is
      published on 127.0.0.1 only. Test: `test_dashboard_is_reachable_from_this_machine_only`
      (fails when the address is changed, checked)

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

## Folder structure (reorganised 2026-10-08)
- [x] The root had 14 scripts side by side; now `pipeline/` (bronze/, silver/, gold/,
      common/) holds the data flow and `tools/` what is run by hand; PROJECT.md moved to
      `docs/`. Scripts run as modules: `python -m pipeline.bronze.cbs`. Checked: all tests,
      every entry point on the real lakehouse (read-only modes), the Docker image (imports,
      Dagster definitions, the KVK agent) and `docker compose config`

## Lakehouse viewer (built 2026-10-08: `tools/lakehouse_viewer.py`)
- [x] Browse every Delta table (bronze, silver, gold, run log), any version, read-only;
      text filter over the whole table
- [x] Data quality view: completeness, blank and padded text, distinct values, min/max,
      most common value per column; flag true counts; duplicate rows; duplicate keys
      (keys from `checks/layers.py`). Real data: 381,513 duplicate rows in bronze KVK (as
      known), 0 duplicate keys everywhere, no padded text in silver or gold
- [ ] Open question: `CategoryGroupID` in `81644ned_regios` is text (every value is
      empty, so bronze inferred text) while it is a number in other code lists

## Documentation
- [x] `docs/bronze_to_silver.md`: how each silver table is made, with real examples
      (2026-10-08)
- [x] Column lineage bronze -> silver (2026-10-08): `tools/lineage.py` is the source; it
      generates `docs/lineage.md` (`python -m tools.lineage`) and the column lineage of the
      Dagster assets silver_cbs and silver_kvk. Correction (2026-10-08): the free Dagster
      UI doesn't draw column lineage (a Dagster+ feature); it does show the column list,
      so each column's description there starts with "From <bronze field>".
- [x] Lineage diagrams (2026-10-08), generated by `python -m tools.lineage` as Mermaid: an
      overview (sources -> silver tables) and one diagram per silver table (bronze fields
      -> columns), in `docs/lineage.md` (GitHub draws them) and `docs/lineage.html` (one
      page for a browser; loads Mermaid from the internet). Checked by rendering the page
- [x] Interactive lineage explorer (2026-10-08): `docs/lineage_explorer.html`, generated
      by `python -m tools.lineage` from `tools/lineage_explorer_template.html`; pick a silver column
      (its whole chain back to bronze) or a bronze field (everything that depends on it),
      click boxes to follow the path; the address keeps the selection (#table.column);
      works offline. Checked by rendering both directions `tests/test_lineage.py` fails when a
      silver column has no entry, a named bronze field doesn't exist, or the document
      is out of date. 133 columns in 12 tables
- [x] Column lineage silver -> gold (2026-10-08): `GOLD_LINEAGE` in `tools/lineage.py`; every
      gold column names the silver column(s) it is made from (comparisons such as
      diff_from_national_pp name the value they compare). docs/lineage.* and the explorer
      show bronze -> silver -> gold (the explorer now opens on a gold column); the Dagster
      gold asset lists its columns with their origin; tests check that every gold column
      is documented, in order, and reads only silver

## Known issues
- [x] deltalake aborts at interpreter exit ("terminate called without an active
      exception", exit code 134) in some runs after reading a Delta table; seen with
      deltalake 1.4.2, 1.5.1 and 1.6.6. Cause found 2026-10-08: a timing race, a
      background thread of the library is still busy for a moment after a read; scripts
      that exit right after a read crash (9-14 of 20 runs), scripts that do more work
      afterwards rarely do. Mitigated: pipeline/common/config.py (imported by every script) pauses 0.1 s
      at exit; measured 0 of 30 with even 0.02 s. Test: tests/test_config.py
- [ ] Optional: report the race upstream (delta-rs) and remove the pause once fixed

## 7. Tests and CI

`tests/test_bronze_logic.py` and `tests/test_silver_logic.py` cover the pure logic;
`tests/test_kvk_logic.py` the KVK file verification; `tests/test_reference.py` the SBI
crosswalk; `tests/test_checks.py` the CBS and KVK checks and quarantine;
`tests/test_maintenance.py` the quarantine cleanup; `tests/test_config.py` the
configuration; `tests/test_kvk_silver.py` silver KVK; `tests/test_gold.py` gold;
`tests/test_dashboard.py` the dashboard (every page headless); `tests/test_orchestration.py`
Dagster; `tests/test_delta.py` writes real Delta tables in `tmp_path` (373 tests, ~75 s). `conftest.py` fails a test when any module still points at the real
lakehouse or inbox. Checked by breaking the code on purpose:
removing the delete-before-write, an empty `done_keys()` and non-UTC silver timestamps
each make the right tests fail.

Delta tests (done 2026-10-05, `tests/test_delta.py`):
- [x] Bronze: a revised period replaces its rows (no duplicates) after `write_bronze`;
      also a repeated write (crash before the log) and a non-split request
- [x] Bronze: `done_keys()` reads back what `log_success()` wrote
- [x] Bronze: `run()` with fake HTTP but real Delta: second run makes no data
      requests; a status change refetches only that period and replaces its rows
- [x] Silver: `pipeline/silver/cbs.py` end to end from real bronze Delta tables; values equal
      the in-memory build; types read back (date32, int64 counts, bool, UTC timestamps);
      a rerun replaces the tables
- [x] Optional live test, marked `@pytest.mark.live` and skipped by default (also in
      `pytest.ini`): fetches `81589NED/Perioden`. Run with `pytest -m live`

Later, as each part is built:
- [x] Tests for the data checks, quarantine, scope cleanup, silver KVK, gold, the
      dashboard's data loading and the Dagster definitions (all built with their parts)
- [x] GitHub Actions stage 1: run pytest on every push (`.github/workflows/tests.yml`).
      **Ran on GitHub 2026-10-15** (first push to stefancodecommit/AOV_Lakehouse, commit
      aa04b82): Python 3.12, pinned versions, all tests passed (job `pytest`, 1 min 38 s).
      Before that, run locally the way
      CI would (2026-10-08): a clean `python:3.12` container with only the committed
      files, `pip install -r requirements-dev.txt`, `pytest -m "not live"`. The first
      try failed (79 tests): a fresh install got **pandas 3.0.6** (`pandas>=2.0`), which
      the code doesn't support yet. Fixed by pinning exact versions (requirements.txt,
      constraints.txt); then all 348 tests pass on Python 3.12
- [x] GitHub Actions stage 3: build the Docker image after the tests pass, and check the
      code and the Dagster definitions load in it. **Ran on GitHub 2026-10-15**: image
      built, imports and `dagster definitions validate` passed (job `docker-build`, 47 s).
      Before that, run locally
      2026-10-08 with the pinned versions: image builds, imports and
      `dagster definitions validate` pass
- [x] Exact package versions (2026-10-08): requirements.txt pins the packages the project
      uses, constraints.txt every package they pull in; the Dockerfile installs the
      same. The Docker image had pandas 3.0.6 and still the code from before the
      reorganisation; rebuilt (`docker compose build`): new layout, pandas 2.3.3

### pandas 3 (to do)
- [ ] Make the code work with pandas 3 (text columns are Arrow strings by default), then
      raise the pin in requirements.txt and regenerate constraints.txt. Known so far, in
      the clean 3.12 run: `build_dim_survey_measure` (pipeline/silver/cbs.py) adds a text
      column to one with empty values when it suffixes measure names with their era
      (73 of the 79 failures); about 5 smaller differences (a timestamp unit, a
      missing value compared as 'odd', Dagster's register job). The real data doesn't
      hit the first one today (silver --preview runs on pandas 3), the tests do
