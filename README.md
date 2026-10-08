# CBS & KVK Lakehouse

An educational data-engineering project. It collects Dutch business statistics from
**CBS** (Statistics Netherlands) and the **KVK** business register, refines them in three
layers, and prepares tables for a dashboard that answers one question:

> **Which company characteristics (sector, size class) go together with higher sickness
> absence?**

Everything runs locally in Python, without a database server or Spark: the data is stored
as [Delta Lake](https://delta.io) tables in the folder `lakehouse/`.

| Document | What it's for |
|---|---|
| `README.md` (this file) | What the project does, how the scripts fit together, how to run it |
| [`PROJECT.md`](PROJECT.md) | The design: sub-questions, data sources, and *why* each choice was made |
| [`TODO.md`](TODO.md) | Open work, decisions taken, and results of each real run |
| [`docs/bronze_to_silver.md`](docs/bronze_to_silver.md) | How bronze becomes silver, table by table, with real examples |
| [`docs/lineage.md`](docs/lineage.md) | Column lineage: for every silver column, the bronze field(s) it comes from, as diagrams and tables (generated from `lineage.py`) |
| [`docs/lineage.html`](docs/lineage.html) | The same lineage diagrams on one page, to open in a browser |
| [`docs/lineage_explorer.html`](docs/lineage_explorer.html) | Interactive lineage: pick a silver column or bronze field and see its whole chain, in both directions (works offline) |

## Contents

1. [Concepts in two minutes](#concepts-in-two-minutes)
2. [How the scripts fit together](#how-the-scripts-fit-together)
3. [Setup](#setup)
4. [Running the pipeline](#running-the-pipeline)
5. [Running it automatically: Dagster](#running-it-automatically-dagster)
6. [Running everything in Docker](#running-everything-in-docker)
7. [The dashboard](#the-dashboard)
8. [The scripts one by one](#the-scripts-one-by-one)
9. [What ends up in `lakehouse/`](#what-ends-up-in-lakehouse)
10. [Configuration](#configuration)
11. [Data quality: checks, quarantine and the run log](#data-quality-checks-quarantine-and-the-run-log)
12. [Tests](#tests)
13. [Status and known limitations](#status-and-known-limitations)
14. [Sources and licence](#sources-and-licence)

## Concepts in two minutes

**Bronze, silver, gold (the "medallion" pattern).** Data moves through three layers, each
with one job:

- **Bronze** stores the data *exactly as received* (CBS records as JSON text and as one
  column per field, KVK rows as text), plus where and when it came from. Nothing is interpreted yet, so any later step
  can be redone from bronze without asking the source again.
- **Silver** cleans and types the data (dates, numbers, true/false), gives codes a
  meaning (which key is a sector, which a size class), and links the sources together.
- **Gold** shapes silver into the tables the dashboard needs, with the comparisons
  already calculated (difference from the national average, ranking, change versus last
  year).

**Delta Lake.** A table format on top of ordinary Parquet files. It adds a transaction log
(`_delta_log/`), so a write either fully succeeds or doesn't happen, and earlier versions
of a table stay readable ("time travel"). The `deltalake` package (delta-rs) reads and
writes it from plain Python.

**Idempotent ingestion.** Running a script twice must not create duplicates or request
the same data twice. Every bronze script keeps a log of what it already loaded and skips
it next time (details per script below).

## How the scripts fit together

```
 SOURCES                  BRONZE                      SILVER                   GOLD
 (outside)                raw, as received            cleaned, typed           dashboard-ready

 CBS OData API ──────► cbs_bronze.py ──────────┐
 (5 tables + code lists)  lakehouse/bronze/    ├──► cbs_silver.py ─────┐
                                               │    11 tables           │
 CBS crosswalk xlsx ──► reference_bronze.py ───┘    (dims, facts,      ├──► gold.py ──► dashboard/
 (SBI 2008 -> 2025)       bronze/reference/          SBI crosswalk) ───┤    5 tables     (Streamlit)
                                                          │             │
 KVK zip in inbox/kvk ► kvk_bronze_agent.py ──────► kvk_silver.py ──────┘
 (1.9M companies)         bronze/kvk/               kvk_company
                          (Ollama agent or --inbox)  (needs the crosswalk)

 Shared building blocks, used by the scripts above:
   config.py + config.yaml   all settings (paths, URLs, tables, model)
   checks/                   data checks before writing (cbs.py, kvk.py) and quarantine
   publish.py + runlog.py    silver/gold: staging, publishing, and the run log
   inspect_lakehouse.py      overview of every table, warnings and problems
   maintenance.py            monthly tasks (quarantine, KVK retention, Delta vacuum)
   orchestration/            Dagster: schedules, automatic rebuilds, layer checks
   dashboard/                the Streamlit app; reads the gold tables only
```

Reading the diagram:

- Each arrow is a script reading the layer on its left and writing the layer on its right.
  The scripts don't call each other; they only share data through `lakehouse/`. That is
  why they have to run in order (see [Running the pipeline](#running-the-pipeline)).
- `kvk_silver.py` needs the SBI crosswalk from silver CBS, because KVK codes companies in
  a newer classification (SBI 2025) than CBS (SBI 2008).
- They can be run by hand (below), or by Dagster, which runs them on a schedule and
  only when new data arrived (see [Dagster](#running-it-automatically-dagster)).

## Setup

Requirements: Linux or macOS, Python 3.10 or newer, about 2 GB free memory (silver KVK
peaks at ~1.3 GB) and ~100 MB disk for the data.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt     # runtime packages + pytest
```

Optional, only for the KVK *chat* agent: [Ollama](https://ollama.com) with a model that
supports tool calling. On the development machine it is installed without root in
`~/.local/ollama`:

```bash
~/.local/ollama/bin/ollama serve &                 # start the model server
~/.local/ollama/bin/ollama pull qwen2.5:7b         # once, ~4.7 GB
```

The KVK file can also be loaded without Ollama (`--inbox` or `--file`, see below).

## Running the pipeline

From the project folder, in this order. Every step can be repeated safely: it only does
work when something is new.

```bash
.venv/bin/python cbs_bronze.py                 # 1. CBS API -> bronze (~40 s first time)
.venv/bin/python reference_bronze.py           # 2. SBI crosswalk -> bronze
.venv/bin/python kvk_bronze_agent.py --inbox   # 3. newest new KVK zip in inbox/kvk/ -> bronze
.venv/bin/python cbs_silver.py                 # 4. bronze CBS + crosswalk -> silver
.venv/bin/python kvk_silver.py                 # 5. bronze KVK -> silver (needs step 4)
.venv/bin/python gold.py                       # 6. silver -> gold
.venv/bin/python inspect_lakehouse.py          # 7. check the result
.venv/bin/streamlit run dashboard/app.py       # 8. open the dashboard in the browser
```

For step 3, first download the zip from
<https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip> into
`inbox/kvk/`. The script verifies the file and asks you to type `YES` before writing.

Useful options:

| Command | What it does |
|---|---|
| `cbs_bronze.py --dry-run` | List what would be requested, fetch nothing |
| `cbs_bronze.py --only 81589` | Only endpoints whose name contains "81589" |
| `cbs_bronze.py --cleanup-scope --dry-run` | Show bronze data outside the configuration (e.g. after raising the start year); without `--dry-run` it removes it |
| `cbs_bronze.py --rebuild-columns` | Rebuild bronze's field columns from `raw_json`, no API calls (after a change in how fields are typed) |
| `kvk_bronze_agent.py` | Chat with the KVK intake agent (needs Ollama) |
| `kvk_bronze_agent.py --file <zip>` | Load one specific file without the agent |
| `cbs_silver.py --preview`, `kvk_silver.py --preview`, `gold.py --preview` | Build and summarise, write nothing |
| `inspect_lakehouse.py --layer silver` | Overview of one layer |
| `maintenance.py quarantine --dry-run` | Show which quarantine entries the monthly cleanup would remove |

## Running it automatically: Dagster

[Dagster](https://dagster.io) runs the same scripts on a schedule, rebuilds a layer only
when new data arrived upstream, and checks every layer before the next one is built on it.

```bash
export DAGSTER_HOME=$PWD/orchestration          # Dagster's settings and run history
.venv/bin/dagster dev -f orchestration/definitions.py
```

Then open <http://localhost:3000>. Dagster only acts while this command runs.

```
 schedule: 1st of the month 06:00          the person, in the Dagster UI
        │                                          │ (types the file name to confirm)
   cbs_bronze    sbi_crosswalk                kvk_bronze
        └──────┬──────┘                            │
           silver_cbs ──── blocking checks ────────┤
               │                              silver_kvk ── blocking checks
               └──────────────┬────────────────────┘
                            gold ── blocking checks ──► dashboard (reads gold)
```

- **cbs_bronze and sbi_crosswalk** run monthly (1st, 06:00). When CBS has nothing new
  they are *skipped*, and nothing downstream runs.
- **kvk_bronze** is never scheduled: select it in the UI, click *Materialize*, and type
  the file name in `confirm_file_name` (the Dagster version of typing YES).
- **silver_cbs, silver_kvk, gold** rebuild automatically after new upstream data, but
  only when the upstream *blocking checks* passed (tables filled, one run_id, unique
  keys, keys present in their dimension; `checks/layers.py`).
- **Freshness** warnings (1st, 09:00): CBS quarterly data more than 4 months old,
  yearly data more than 15 months, the KVK snapshot more than 45 days.
- **Maintenance** (15th, 03:00): quarantine cleanup, KVK retention (latest snapshot plus
  the first of each month), Delta optimize and vacuum.
- At most one run that writes to the lakehouse at a time (`orchestration/dagster.yaml`).
- Failed runs and failed checks are shown in the Dagster UI (no email or Slack).

**First time** on a lakehouse that was filled by hand: run the job
`register_existing_data` once (Jobs in the UI, or
`.venv/bin/dagster job execute -f orchestration/definitions.py -j register_existing_data`).
Dagster only rebuilds a layer automatically when every input was materialized in Dagster
at least once; this records the existing bronze data as such.

## Running everything in Docker

The same project in containers, so it runs the same on any machine with Docker. Only
Docker is needed (no Python setup).

```bash
docker compose up -d                       # build (first time ~3 min) and start
                                           #   Dagster:   http://localhost:3000
                                           #   dashboard: http://localhost:8501
docker compose down                        # stop (data and run history stay)
```

| Service | What it is |
|---|---|
| `dagster-webserver` | the Dagster UI |
| `dagster-daemon` | schedules, automatic rebuilds; the pipeline runs execute here |
| `postgres` | Dagster's run history (not the data) |
| `dashboard` | the Streamlit dashboard |
| `ollama`, `kvk-agent` | optional: the model server (profile `agent`) and the KVK chat agent (started on demand) |

- **The data stays in this project folder.** `lakehouse/` and `inbox/` are mounted into
  the containers, so Docker and local runs work on the same lakehouse. Containers run as
  your user (`HOST_UID`/`HOST_GID`, see `.env.example`), so the files they write are yours.
- **Only reachable from this machine:** the ports are published on 127.0.0.1.
- **First time** on an existing lakehouse: run the job `register_existing_data` once in
  the Dagster UI (Jobs), as described in the Dagster section.
- **Record the code version** in the run log: `docker compose build --build-arg
  CODE_VERSION=$(git rev-parse --short HEAD)` (the image has no git).
- **KVK chat agent** (optional; needs ~5 GB more memory for the model):

  ```bash
  docker compose --profile agent up -d              # Ollama; downloads the model once (~4.7 GB)
  docker compose --profile agent run --rm kvk-agent # the chat; type exit to stop
  ```

  To reuse a model already on this machine instead of downloading it, copy it into
  Docker's volume once (this only reads your `~/.ollama`):
  `docker compose --profile agent run --rm --no-deps --entrypoint sh -v ~/.ollama/models:/src:ro ollama -c "mkdir -p /root/.ollama/models && cp -a /src/. /root/.ollama/models/"`.
  The Ollama image is large (~9 GB, it includes GPU libraries). Without the agent, load
  KVK files from the Dagster UI (`kvk_bronze`).

**Minimum machine** (measured): Docker needs about **3 GB of memory** for the pipeline
(the services use ~0.7 GB idle; the largest steps, silver KVK and gold, peak at ~1.3 and
~1.1 GB, and only one runs at a time), or about **8 GB** with the KVK chat agent (its
7B model needs ~5 GB). On Docker Desktop, set this under Settings → Resources → Memory.
Disk: ~1.3 GB for the image, ~115 MB for the lakehouse so far plus ~22 MB per kept KVK
snapshot (about 300 MB a year with monthly uploads), and ~4.7 GB for the agent's model.

## The dashboard

```bash
.venv/bin/streamlit run dashboard/app.py         # opens http://localhost:8501
```

It reads only the gold tables. A new gold run shows up on the next page load (the cache is
keyed on each table's Delta version); no restart needed.

| Page | Answers |
|---|---|
| Overview | National absence trend; the highest and lowest sectors; small vs large companies |
| Sectors | Sectors ranked against the national figure (per SBI level), one sector over time; this quarter or the 4-quarter average |
| Size classes | Absence per size class over time; survey share absent per size class with 95% intervals |
| Behind the numbers | For one sector or size class: which survey measures (frequency, duration, work-relatedness, complaints) differ from national beyond the margin of error |
| Context | Businesses per sector and size band (CBS); active BVs/NVs and median company age per sector (KVK) |
| Data dictionary | Every gold table and column (read from the tables), and every survey measure |

On every page: the latest period and whether it is provisional, the data date and gold
run, a table view of the numbers, and the source credits (sidebar). The sidebar switch
**Final figures only** hides provisional CBS figures on all pages; provisional points are
drawn hollow, and comparisons that mix provisional and final figures are flagged. KVK
groups under 10 companies are hidden by default. A warning appears when the gold tables
come from different gold runs.

Chart design (`dashboard/charts.py`): colour only where it carries meaning - one blue
series with the national figure in grey, red/blue for above/below the national figure,
at most three categorical colours (the first three slots of a palette validated for
colour-blind readers, with its own dark-mode steps); thin marks, hairline grid, tooltips,
and direct labels on lines. The palette values were taken as published (a validated
set); the validator script itself was not re-run, because Node.js isn't installed here.

## The scripts one by one

Every script starts with a docstring that explains it in more detail; this is the summary.

### Bronze

**`cbs_bronze.py`** — reads the CBS OData API (v3) for the five tables in `config.yaml`:
the data itself (`TypedDataSet`) and the code lists that explain it.
- *Incremental:* data is requested one period at a time. A log table
  (`bronze/_ingestion_log`) records every request already loaded, so a second run only
  asks for new periods.
- *Revisions:* CBS marks recent periods as provisional and revises them later. The
  period's status is part of the request key, so a revised period is fetched again and
  replaces its old rows.
- *Checks before writing* (`checks/cbs.py`): columns, period, numbers, unique keys, not
  empty. A batch that fails goes to quarantine and is retried next run.
- *Lifecycle:* warns when CBS has stopped a table or hasn't updated it for too long.

**`reference_bronze.py`** — downloads the CBS crosswalk between the old (SBI 2008) and new
(SBI 2025) sector classification. A file is recognised by its SHA-256 fingerprint, so the
same file is never loaded twice; a new CBS version becomes a new batch.

**`kvk_bronze_agent.py`** — loads the KVK open dataset (one zip with a CSV of ~1.9 million
companies). A local language model (Ollama) guides the user through download and upload;
the actual checks run in Python (`checks/kvk.py`), and only a human typing `YES` can start
the write. Each upload is a full snapshot, stored as one batch.

### Silver

How each silver table is made from bronze, with real before/after examples:
[`docs/bronze_to_silver.md`](docs/bronze_to_silver.md). Where every single silver column
comes from: [`docs/lineage.md`](docs/lineage.md), generated from `lineage.py` (also visible
in the Dagster UI: the *Columns* list of the assets `silver_cbs` and `silver_kvk`, where
every description starts with *From …*; Dagster's column-lineage graph itself is only in the
paid Dagster+).

**`cbs_silver.py`** — turns the raw CBS records into dimension tables (`dim_period`,
`dim_sector`, `dim_company_size`, `dim_region`, `dim_survey_measure`), fact tables
(`fct_absence_rate`, `fct_absence_survey`, `fct_business_count`,
`fct_establishment_count`), and mapping tables (`map_characteristic`,
`map_sbi2008_sbi2025`). Rebuilt completely on every run (the data is small).

**`kvk_silver.py`** — builds `kvk_company`: one typed row per company per snapshot, with
the company's sector taken from its main activity and translated to SBI 2008 via the
crosswalk. Unclear cases (several main activities, unknown codes, codes that exist in
both classifications) are *marked with flag columns* instead of being guessed; the
sector is only filled in when there is exactly one answer (90% of active companies).

### Gold

**`gold.py`** — five tables, each answering part of the main question:

| Table | Answers |
|---|---|
| `gold_absence_by_sector` | Which sectors are above/below the national absence rate, their rank, change vs last year, rolling 4-quarter average |
| `gold_absence_by_size` | Does absence rise with company size? (quarterly rate and yearly survey side by side) |
| `gold_absence_profile` | What lies behind it: frequency, duration, work-related causes, with 95% intervals |
| `gold_business_context` | How many businesses each sector and size band represents (CBS) |
| `gold_kvk_context` | Active companies and median company age per sector (KVK) |

Provisional CBS figures are kept but marked (`is_provisional`), so the dashboard can show
"final figures only".

### Dashboard

**`dashboard/`** — `app.py` (entry point and navigation), `views/` (one file per page),
`common.py` (the frame every page shares), `charts.py` (chart styles), `data.py` (reading
gold, caching, the run check, the "final only" filter).

### Orchestration

**`orchestration/definitions.py`** — the Dagster assets, checks, jobs and schedules; it
calls the scripts above and contains no data logic of its own. `dagster.yaml` holds the
instance settings (one writing run at a time).

### Shared modules

| Module | Role |
|---|---|
| `config.py`, `config.yaml` | Every setting in one file, see [Configuration](#configuration) |
| `checks/cbs.py`, `checks/kvk.py` | Data checks before writing, used by the scripts, the tests and Dagster |
| `checks/layers.py`, `checks/freshness.py` | Checks on a published layer, and freshness; run by Dagster after each layer |
| `checks/quarantine.py` | Saves failed batches with a report; monthly cleanup |
| `publish.py` | Silver and gold: write all tables to a staging folder first, replace the live tables only when all succeeded |
| `runlog.py` | One row per silver/gold run in `lakehouse/_run_log`: when, what, status, which inputs |
| `lineage.py` | Column lineage bronze → silver: the source of `docs/lineage.md`, `docs/lineage.html` (diagrams), `docs/lineage_explorer.html` (interactive; layout in `lineage_explorer_template.html`) and the lineage in Dagster; a test keeps it complete |
| `inspect_lakehouse.py` | One-screen overview: rows, periods, freshness per table; warnings; problems (exit code 1) |
| `maintenance.py` | Monthly maintenance: quarantine cleanup, KVK retention, Delta optimize/vacuum |

## What ends up in `lakehouse/`

```
lakehouse/                              (not in git; rebuilt from the sources)
  bronze/<table>_<entity>/              CBS raw records, one Delta table per endpoint
  bronze/_ingestion_log/                which CBS requests are already loaded
  bronze/kvk/basis_bedrijfsgegevens/    KVK rows, one batch per uploaded file
  bronze/kvk/_batch_log/                which KVK files are loaded (by SHA-256)
  bronze/reference/...                  the SBI crosswalk and its batch log
  silver/                               11 CBS tables + kvk_company, each with run_id
  gold/                                 5 dashboard tables, each with run_id
  _run_log/                             one row per silver/gold run
  quarantine/<source>/<time>_<name>/    batches that failed a check: report.json + data
```

Sizes after the first full load: bronze CBS 6.6 MB, bronze KVK 22 MB per snapshot,
silver ~25 MB, gold < 1 MB.

## Configuration

All settings live in [`config.yaml`](config.yaml): paths, the CBS tables and start year,
the KVK download URL and accepted column names, the crosswalk URL, the Ollama model, and
the quarantine retention. Override them without editing the file:

- `config.local.yaml` next to it (not in git): only the keys you want to change.
- Environment variables `LAKEHOUSE__<SECTION>__<KEY>`, e.g.
  `LAKEHOUSE__CBS__START_YEAR=2022` (useful for Docker later).

A misspelled setting name stops the script with an error instead of being ignored.

## Data quality: checks, quarantine and the run log

- **Checks before writing** decide whether a batch may enter bronze (`checks/`). A failed
  batch is not written and not logged, so the next run tries again.
- **Quarantine** keeps every failed batch with a report of what failed, for inspection
  (`lakehouse/quarantine/`). The same content is saved once; `maintenance.py` removes
  entries that were resolved or are older than 90 days.
- **Safe publishing** (silver and gold): all tables of a run are first written to a
  staging folder; the live tables are only replaced when every table succeeded.
- **Run log:** every silver and gold table has a `run_id` column. Joining it with
  `lakehouse/_run_log` shows when the row was made, by which code version, and from
  which input versions.
- **`inspect_lakehouse.py`** reports empty or missing tables, quarantined batches, failed
  runs, and tables from different runs.

## Tests

```bash
.venv/bin/pytest              # 329 tests, ~65 s, no internet needed
.venv/bin/pytest -m live      # one extra test against the real CBS API
```

The tests use small samples copied from real CBS and KVK responses, and write real Delta
tables in a temporary folder; a safety check makes a test fail if any script still points
at the real `lakehouse/`. A GitHub Actions workflow (`.github/workflows/tests.yml`) runs
the tests and builds the Docker image on every push; it hasn't run yet, because the
repository isn't on GitHub yet.

| Test file | Covers |
|---|---|
| `test_bronze_logic.py` | Period selection, request keys, paging, skip rules, checks and quarantine in `cbs_bronze.run()` |
| `test_silver_logic.py` | Parsers and silver CBS tables |
| `test_delta.py` | Bronze and silver on real Delta tables: revisions, the log, publishing, the run log, scope cleanup |
| `test_checks.py` | `checks/`: CBS and KVK checks, quarantine |
| `test_kvk_logic.py`, `test_kvk_silver.py` | KVK file checks, inbox, silver KVK and its SBI mapping |
| `test_reference.py` | The SBI crosswalk: loading, rejecting, no duplicates |
| `test_gold.py` | Gold comparisons and tables, end to end from bronze to gold |
| `test_config.py`, `test_maintenance.py` | Configuration overrides, quarantine cleanup |
| `test_lineage.py` | The column lineage is complete (every silver column), its bronze fields exist, and `docs/lineage.md` is up to date |
| `test_dashboard.py` | Dashboard helpers, and every page run headless (both switch states) on gold built from sample data |
| `test_orchestration.py` | Dagster: the graph and schedules, skipping when nothing is new, the KVK confirmation, blocking checks stopping the next layer, automatic rebuilds, and one run of the real `dagster` command |

## Status and known limitations

Built and run on real data: bronze, silver and gold, with checks, quarantine and the run
log, the Streamlit dashboard, Dagster orchestration, and Docker Compose (including the
KVK chat agent). See [`TODO.md`](TODO.md) for what's open.

Limitations worth knowing when reading the results:

- **Sector and size are separate.** CBS publishes absence per sector *or* per size class,
  never per combination, so "small construction companies" can't be shown.
- **KVK has no company number** in the open dataset: companies can't be followed between
  snapshots, only counted.
- **Docker memory:** with Docker Desktop's default memory (3.67 GB on this machine) the
  pipeline fits, but not the KVK chat agent's model; this machine now gives Docker 8 GB.
- **KVK sector for 10% of active companies is not determined**, because their codes can't
  be translated to SBI 2008 unambiguously; they are marked, not guessed.
- **Provisional figures** (the latest CBS periods) can still change.
- **A known library issue, worked around:** `deltalake` could crash while Python shut
  down right after reading a table (exit code 134, after the work was done). `config.py`
  pauses 0.1 s at exit, which prevents it; details in `TODO.md`, "Known issues".

## Sources and licence

- CBS StatLine tables 80072NED, 86009NED, 83130NED, 81589NED, 81644NED and the SBI 2008 –
  SBI 2025 crosswalk: © CBS, [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- KVK Handelsregister Open Dataset Basis Bedrijfsgegevens: © KVK, CC BY 4.0.
