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

## Contents

1. [Concepts in two minutes](#concepts-in-two-minutes)
2. [How the scripts fit together](#how-the-scripts-fit-together)
3. [Setup](#setup)
4. [Running the pipeline](#running-the-pipeline)
5. [The dashboard](#the-dashboard)
6. [The scripts one by one](#the-scripts-one-by-one)
7. [What ends up in `lakehouse/`](#what-ends-up-in-lakehouse)
8. [Configuration](#configuration)
9. [Data quality: checks, quarantine and the run log](#data-quality-checks-quarantine-and-the-run-log)
10. [Tests](#tests)
11. [Status and known limitations](#status-and-known-limitations)
12. [Sources and licence](#sources-and-licence)

## Concepts in two minutes

**Bronze, silver, gold (the "medallion" pattern).** Data moves through three layers, each
with one job:

- **Bronze** stores the data *exactly as received* (CBS records as JSON text, KVK rows as
  text), plus where and when it came from. Nothing is interpreted yet, so any later step
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
   maintenance.py            monthly tasks (quarantine cleanup)
   dashboard/                the Streamlit app; reads the gold tables only
```

Reading the diagram:

- Each arrow is a script reading the layer on its left and writing the layer on its right.
  The scripts don't call each other; they only share data through `lakehouse/`. That is
  why they have to run in order (see [Running the pipeline](#running-the-pipeline)).
- `kvk_silver.py` needs the SBI crosswalk from silver CBS, because KVK codes companies in
  a newer classification (SBI 2025) than CBS (SBI 2008).
- Later, Dagster will run these steps on a schedule and only when new data arrived (see
  `TODO.md`, section 5). Until then they are run by hand.

## Setup

Requirements: Linux or macOS, Python 3.10 or newer, about 3 GB free memory (silver KVK
peaks at ~2.3 GB) and ~100 MB disk for the data.

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
| `kvk_bronze_agent.py` | Chat with the KVK intake agent (needs Ollama) |
| `kvk_bronze_agent.py --file <zip>` | Load one specific file without the agent |
| `cbs_silver.py --preview`, `kvk_silver.py --preview`, `gold.py --preview` | Build and summarise, write nothing |
| `inspect_lakehouse.py --layer silver` | Overview of one layer |
| `maintenance.py quarantine --dry-run` | Show which quarantine entries the monthly cleanup would remove |

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

### Shared modules

| Module | Role |
|---|---|
| `config.py`, `config.yaml` | Every setting in one file, see [Configuration](#configuration) |
| `checks/cbs.py`, `checks/kvk.py` | Data checks, written once and used by the scripts, the tests and (later) Dagster |
| `checks/quarantine.py` | Saves failed batches with a report; monthly cleanup |
| `publish.py` | Silver and gold: write all tables to a staging folder first, replace the live tables only when all succeeded |
| `runlog.py` | One row per silver/gold run in `lakehouse/_run_log`: when, what, status, which inputs |
| `inspect_lakehouse.py` | One-screen overview: rows, periods, freshness per table; warnings; problems (exit code 1) |
| `maintenance.py` | Monthly maintenance; for now the quarantine cleanup |

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
.venv/bin/pytest              # 261 tests, ~30 s, no internet needed
.venv/bin/pytest -m live      # one extra test against the real CBS API
```

The tests use small samples copied from real CBS and KVK responses, and write real Delta
tables in a temporary folder; a safety check makes a test fail if any script still points
at the real `lakehouse/`. GitHub Actions runs the tests on every push
(`.github/workflows/tests.yml`).

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
| `test_dashboard.py` | Dashboard helpers, and every page run headless (both switch states) on gold built from sample data |

## Status and known limitations

Built and run on real data: bronze, silver and gold, with checks, quarantine and the run
log, and the Streamlit dashboard. Still to do (see [`TODO.md`](TODO.md)): Dagster
orchestration, Docker Compose.

Limitations worth knowing when reading the results:

- **Sector and size are separate.** CBS publishes absence per sector *or* per size class,
  never per combination, so "small construction companies" can't be shown.
- **KVK has no company number** in the open dataset: companies can't be followed between
  snapshots, only counted.
- **KVK sector for 10% of active companies is not determined**, because their codes can't
  be translated to SBI 2008 unambiguously; they are marked, not guessed.
- **Provisional figures** (the latest CBS periods) can still change.
- **A known library issue:** `deltalake` sometimes crashes while Python shuts down, after
  the work is done (exit code 134). Details in `TODO.md`, "Known issues".

## Sources and licence

- CBS StatLine tables 80072NED, 86009NED, 83130NED, 81589NED, 81644NED and the SBI 2008 –
  SBI 2025 crosswalk: © CBS, [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- KVK Handelsregister Open Dataset Basis Bedrijfsgegevens: © KVK, CC BY 4.0.
