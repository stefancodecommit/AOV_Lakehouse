# CBS & KVK Lakehouse

A data platform for an **income-protection insurer**, built as the assignment for the
Erasmus course *Data and AI Engineering*. It collects Dutch business statistics from
**CBS** (Statistics Netherlands) and the **KVK** business register, refines them in three
layers, and prepares tables for a dashboard that answers one question:

> **Which company characteristics (sector, size class) go together with higher sickness
> absence?**

The insurer's own benchmarks are built on privacy-sensitive data and can't be used here,
so this project uses public CBS figures instead: free of privacy risk, covering the whole
market. A **data analyst** uses the dashboard to find the risks per SBI sector and size
class; through their manager, the findings reach **risk management** (portfolio, premiums)
and **account managers** (prevention advice to employer clients). The organisation, its
need, the goals and the boundaries (public, aggregated data only; people decide, the
dashboard informs) are in [docs/PROJECT.md](docs/PROJECT.md#the-organisation-and-its-need).

Everything runs locally in Python, without a database server or Spark: the data is stored
as [Delta Lake](https://delta.io) tables in the folder `lakehouse/`.

| Document | What it's for |
|---|---|
| `README.md` (this file) | What the project does, how the scripts fit together, how to run it |
| [`docs/PROJECT.md`](docs/PROJECT.md) | The design: sub-questions, data sources, and *why* each choice was made |
| [`INSTRUCTIONS.md`](INSTRUCTIONS.md) | Which commands to run: the whole project or one part, by hand, with Dagster or in Docker |
| [`TODO.md`](TODO.md) | Open work, decisions taken, and results of each real run |
| [`docs/requirements.md`](docs/requirements.md) | Requirements: stakeholders, use cases, functional and non-functional requirements (each with its status and the test that verifies it), organisational requirements, conditions for use |
| [`docs/cloud_design.md`](docs/cloud_design.md) | Design (not built) for running the project on Azure: options considered, architecture, security, monitoring, CI/CD, indicative costs, migration path |
| [`docs/privacy_and_risks.md`](docs/privacy_and_risks.md) | Privacy (GDPR) and risks: data inventory, GDPR principles, DPIA, other rules (KVK terms, EU AI Act, equal treatment), risk register with measures, actions |
| [`docs/bronze_to_silver.md`](docs/bronze_to_silver.md) | How bronze becomes silver, table by table, with real examples |
| [`docs/lineage.md`](docs/lineage.md) | Column lineage: for every silver column, the bronze field(s) it comes from; for every gold column, the silver column(s) it is made from; as diagrams and tables (generated from `tools/lineage.py`) |
| [`docs/lineage.html`](docs/lineage.html) | The same lineage diagrams on one page, to open in a browser |
| [`docs/lineage_explorer.html`](docs/lineage_explorer.html) | Interactive lineage: pick a gold or silver column or a bronze field and see its whole chain bronze → silver → gold, in both directions (works offline) |

## Contents

1. [Concepts in two minutes](#concepts-in-two-minutes)
2. [How the scripts fit together](#how-the-scripts-fit-together)
   - [The six phases, each with its README](#the-six-phases-each-with-its-readme)
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

The folders follow the layers. `pipeline/` holds the data flow, one subfolder per layer;
`tools/` holds what you run by hand to look at or maintain the lakehouse.

```
cbs-kvk-lakehouse/
├── pipeline/               the data flow, in layer order
│   ├── bronze/             sources -> bronze: cbs.py, kvk_agent.py, reference.py
│   ├── silver/             bronze -> silver: cbs.py, kvk.py
│   ├── gold/               silver -> gold: build.py
│   └── common/             shared by all layers: config.py, publish.py, runlog.py
├── checks/                 data checks before writing, layer checks, quarantine
├── tools/                  inspect_lakehouse.py, lakehouse_viewer.py, maintenance.py,
│                           lineage.py (+ the explorer's HTML template)
├── dashboard/              the Streamlit app; reads the gold tables only
├── orchestration/          Dagster: schedules, automatic rebuilds, layer checks
├── docs/                   PROJECT.md (the design) and the bronze -> silver documents
├── tests/                  pytest
├── config.yaml             all settings (paths, URLs, tables, model)
└── lakehouse/, inbox/      the data and the KVK uploads (not in git)
```

How data moves through the scripts:

```
 SOURCES                  BRONZE                   SILVER                    GOLD
 (outside)                as received              cleaned, typed            dashboard-ready

 CBS OData API ─────► bronze/cbs.py ───────┐
 (5 tables +              lakehouse/bronze/  ├──► silver/cbs.py ─────────┐
  code lists)                                │    11 tables (dims,       │
 CBS crosswalk xlsx ─► bronze/reference.py ─┘    facts, SBI crosswalk)  ├──► gold/build.py ──► dashboard/
 (SBI 2008 -> 2025)       bronze/reference/            │                 │    7 tables          (Streamlit)
                                                       │ crosswalk       │
 KVK zip in inbox/kvk ─► bronze/kvk_agent.py ──────► silver/kvk.py ─────┘
 (1.9M companies)         bronze/kvk/              kvk_company
                          (Ollama agent or --inbox)

 (script paths relative to pipeline/; shared: pipeline/common/ and checks/)
```

Reading the diagram:

- Each arrow is a script reading the layer on its left and writing the layer on its right.
  Each is run as a module from the project folder, e.g. `python -m pipeline.silver.cbs`.
  The scripts don't call each other; they only share data through `lakehouse/`. That is
  why they have to run in order (see [Running the pipeline](#running-the-pipeline)).
- `silver/kvk.py` needs the SBI crosswalk from silver CBS, because KVK codes companies in
  a newer classification (SBI 2025) than CBS (SBI 2008).
- They can be run by hand (below), or by Dagster, which runs them on a schedule and
  only when new data arrived (see [Dagster](#running-it-automatically-dagster)).

### The six phases, each with its README

The course describes data and AI engineering as six phases. Each has a README next to its
code: what the phase does, the design decisions, how to run it, and a **proof of
working** (the test command and what to expect).

| Phase | README | In this project |
|---|---|---|
| 1. Ingestion | [pipeline/bronze/](pipeline/bronze/README.md) | CBS API, the SBI crosswalk, the KVK file with an AI intake agent; checks and quarantine before writing |
| 2. Transformation | [pipeline/silver/](pipeline/silver/README.md) | typed dimensions and facts; KVK companies translated to SBI 2008; edge cases flagged |
| 3. Serving | [pipeline/gold/](pipeline/gold/README.md), [dashboard/](dashboard/README.md) | 7 gold tables; the Streamlit dashboard for the data analyst |
| 4. Model design and testing | [pipeline/gold/](pipeline/gold/README.md#phase-4--model-design-and-testing-the-risk-index), [tests/](tests/README.md) | the risk index (a white-box model) and how everything is tested |
| 5. Model integration | [pipeline/silver/](pipeline/silver/README.md#phase-5--model-integration) | CBS and KVK joined through the SBI crosswalk; absence with business counts; an LLM inside the KVK intake |
| 6. Deployment and monitoring | [orchestration/](orchestration/README.md) | Dagster schedules and checks, Docker Compose, GitHub Actions, the run log |

## Setup

Requirements: Linux or macOS, Python 3.10 or newer, about 2 GB free memory (silver KVK
peaks at ~1.5–1.8 GB) and ~100 MB disk for the data.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt     # runtime packages + pytest
```

The versions are exact (`requirements.txt`, and `constraints.txt` for everything those
packages pull in), so your install, CI and the Docker image are the same. pandas stays on
2.x for now: the code doesn't support pandas 3 yet (see TODO.md).

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
.venv/bin/python -m pipeline.bronze.cbs                # 1. CBS API -> bronze (~40 s first time)
.venv/bin/python -m pipeline.bronze.reference          # 2. SBI crosswalk -> bronze
.venv/bin/python -m pipeline.bronze.kvk_agent --inbox  # 3. newest new KVK zip in inbox/kvk/ -> bronze
.venv/bin/python -m pipeline.silver.cbs                # 4. bronze CBS + crosswalk -> silver
.venv/bin/python -m pipeline.silver.kvk                # 5. bronze KVK -> silver (needs step 4)
.venv/bin/python -m pipeline.gold.build                # 6. silver -> gold
.venv/bin/python -m tools.inspect_lakehouse            # 7. check the result
.venv/bin/streamlit run dashboard/app.py               # 8. open the dashboard in the browser
```

For step 3, first download the zip from
<https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip> into
`inbox/kvk/`. The script verifies the file and asks you to type `YES` before writing.

Useful options:

| Command | What it does |
|---|---|
| `python -m pipeline.bronze.cbs --dry-run` | List what would be requested, fetch nothing |
| `python -m pipeline.bronze.cbs --only 81589` | Only endpoints whose name contains "81589" |
| `python -m pipeline.bronze.cbs --cleanup-scope --dry-run` | Show bronze data outside the configuration (e.g. after raising the start year); without `--dry-run` it removes it |
| `python -m pipeline.bronze.cbs --rebuild-columns` | Rebuild bronze's field columns from `raw_json`, no API calls (after a change in how fields are typed) |
| `python -m pipeline.bronze.kvk_agent` | Chat with the KVK intake agent (needs Ollama) |
| `python -m pipeline.bronze.kvk_agent --file <zip>` | Load one specific file without the agent |
| `python -m pipeline.silver.cbs --preview`, `python -m pipeline.silver.kvk --preview`, `python -m pipeline.gold.build --preview` | Build and summarise, write nothing (silver KVK: what the next run would add or remove) |
| `python -m pipeline.silver.kvk --full` | Rebuild every KVK snapshot instead of only the new ones |
| `python -m tools.inspect_lakehouse --layer silver` | Overview of one layer |
| `python -m tools.lakehouse_viewer` | Browse every bronze, silver and gold table at http://localhost:8502: rows (with a text filter over the whole table), data quality statistics, history, older versions |
| `python -m tools.maintenance quarantine --dry-run` | Show which quarantine entries the monthly cleanup would remove |

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
- **Only reachable from this machine:** the ports are published on 127.0.0.1. Started
  locally, the dashboard listens on 127.0.0.1 too (`.streamlit/config.toml`).
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
(the services use ~0.7 GB idle; the largest steps, silver KVK and gold, peak at ~1.5–1.8
and ~1.1 GB, and only one runs at a time), or about **8 GB** with the KVK chat agent (its
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
| Risk index | For the data analyst: which sectors and size classes are above national, how certain (the band), which are rising; one sector or size class explained in a sentence, with its relative risk over time |
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

**`pipeline/bronze/cbs.py`** — reads the CBS OData API (v3) for the five tables in `config.yaml`:
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

**`pipeline/bronze/reference.py`** — downloads the CBS crosswalk between the old (SBI 2008) and new
(SBI 2025) sector classification. A file is recognised by its SHA-256 fingerprint, so the
same file is never loaded twice; a new CBS version becomes a new batch.

**`pipeline/bronze/kvk_agent.py`** — loads the KVK open dataset (one zip with a CSV of ~1.9 million
companies). A local language model (Ollama) guides the user through download and upload;
the actual checks run in Python (`checks/kvk.py`), and only a human typing `YES` can start
the write. Each upload is a full snapshot, stored as one batch.

### Silver

How each silver table is made from bronze, with real before/after examples:
[`docs/bronze_to_silver.md`](docs/bronze_to_silver.md). Where every single silver column
comes from: [`docs/lineage.md`](docs/lineage.md), generated from `tools/lineage.py` (also visible
in the Dagster UI: the *Columns* list of the assets `silver_cbs` and `silver_kvk`, where
every description starts with *From …*; Dagster's column-lineage graph itself is only in the
paid Dagster+).

**`pipeline/silver/cbs.py`** — turns the raw CBS records into dimension tables (`dim_period`,
`dim_sector`, `dim_company_size`, `dim_region`, `dim_survey_measure`), fact tables
(`fct_absence_rate`, `fct_absence_survey`, `fct_business_count`,
`fct_establishment_count`), and mapping tables (`map_characteristic`,
`map_sbi2008_sbi2025`). Rebuilt completely on every run (the data is small).

**`pipeline/silver/kvk.py`** — builds `kvk_company`: one typed row per company per snapshot, with
the company's sector taken from its main activity and translated to SBI 2008 via the
crosswalk. Unclear cases (several main activities, unknown codes, codes that exist in
both classifications) are *marked with flag columns* instead of being guessed; the
sector is only filled in when there is exactly one answer (90% of active companies).
It is updated **incrementally**: a run only builds the snapshots that are new in bronze
and deletes the ones maintenance retired, one snapshot at a time, so its memory stays at
about one snapshot however many months are kept. Everything is rebuilt (still one
snapshot at a time) when the SBI crosswalk's content changed, `LOGIC_VERSION` in the
script was raised, or with `--full`. The table is partitioned by `batch_id`.

### Gold

**`pipeline/gold/build.py`** — seven tables, each answering part of the main question:

| Table | Answers |
|---|---|
| `gold_absence_by_sector` | Which sectors are above/below the national absence rate, their rank, change vs last year, rolling 4-quarter average |
| `gold_absence_by_size` | Does absence rise with company size? (quarterly rate and yearly survey side by side) |
| `gold_absence_profile` | What lies behind it: frequency, duration, work-related causes, with 95% intervals |
| `gold_business_context` | How many businesses each sector and size band represents (CBS) |
| `gold_kvk_context` | Active companies and median company age per sector (KVK) |
| `gold_risk_index` | For the insurer's data analyst: per sector and size class the **relative risk** (4-quarter absence ÷ national; 1.00 = national), its band (lowest–highest of the last 8 quarters), an assessment (higher / around / lower), a trend (rising / stable / falling), the number of businesses and how many are run by natural persons (self-employed), and one sentence explaining it |
| `gold_risk_index_history` | The quarterly relative risk behind it |

The risk index is a deliberately simple, explainable ("white box") model: every number is
arithmetic the analyst can redo by hand, and the thresholds (window of 8 quarters, a trend
of ±0.05 per year) are in `config.yaml`.

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
| `pipeline/common/config.py`, `config.yaml` | Every setting in one file, see [Configuration](#configuration) |
| `checks/cbs.py`, `checks/kvk.py` | Data checks before writing, used by the scripts, the tests and Dagster |
| `checks/layers.py`, `checks/freshness.py` | Checks on a published layer, and freshness; run by Dagster after each layer |
| `checks/quarantine.py` | Saves failed batches with a report; monthly cleanup |
| `pipeline/common/publish.py` | Silver and gold: write all tables to a staging folder first, replace the live tables only when all succeeded |
| `pipeline/common/runlog.py` | One row per silver/gold run in `lakehouse/_run_log`: when, what, status, which inputs |
| `tools/lineage.py` | Column lineage bronze → silver → gold: the source of `docs/lineage.md`, `docs/lineage.html` (diagrams), `docs/lineage_explorer.html` (interactive; layout in `tools/lineage_explorer_template.html`) and the lineage in Dagster; a test keeps it complete |
| `tools/inspect_lakehouse.py` | One-screen overview: rows, periods, freshness per table; warnings; problems (exit code 1) |
| `tools/lakehouse_viewer.py` | Small Streamlit app to look at any Delta table, any version, read-only; its *Data quality* view shows per column how complete it is, padded text, distinct values, min/max and the most common value, the flags' true counts, duplicate rows and duplicate keys |
| `tools/maintenance.py` | Monthly maintenance: quarantine cleanup, KVK retention, Delta optimize/vacuum |

## What ends up in `lakehouse/`

```
lakehouse/                              (not in git; rebuilt from the sources)
  bronze/<table>_<entity>/              CBS raw records, one Delta table per endpoint
  bronze/_ingestion_log/                which CBS requests are already loaded
  bronze/kvk/basis_bedrijfsgegevens/    KVK rows, one batch per uploaded file
  bronze/kvk/_batch_log/                which KVK files are loaded (by SHA-256)
  bronze/reference/...                  the SBI crosswalk and its batch log
  silver/                               11 CBS tables + kvk_company, each with run_id
  gold/                                 7 dashboard tables, each with run_id
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
  (`lakehouse/quarantine/`). The same content is saved once; `tools/maintenance.py` removes
  entries that were resolved or are older than 90 days.
- **Safe publishing** (silver and gold): all tables of a run are first written to a
  staging folder; the live tables are only replaced when every table succeeded.
- **Run log:** every silver and gold table has a `run_id` column. Joining it with
  `lakehouse/_run_log` shows when the row was made, by which code version, and from
  which input versions.
- **`tools/inspect_lakehouse.py`** reports empty or missing tables, quarantined batches, failed
  runs, and tables from different runs.

## Tests

```bash
.venv/bin/pytest              # 373 tests, ~75 s, no internet needed
.venv/bin/pytest -m live      # one extra test against the real CBS API
```

The tests use small samples copied from real CBS and KVK responses, and write real Delta
tables in a temporary folder; a safety check makes a test fail if any script still points
at the real `lakehouse/`. A GitHub Actions workflow (`.github/workflows/tests.yml`) runs
the tests and builds the Docker image on every push; it hasn't run yet, because the
repository isn't on GitHub yet.

| Test file | Covers |
|---|---|
| `test_bronze_logic.py` | Period selection, request keys, paging, skip rules, checks and quarantine in `pipeline/bronze/cbs.py` (`run()`) |
| `test_silver_logic.py` | Parsers and silver CBS tables |
| `test_delta.py` | Bronze and silver on real Delta tables: revisions, the log, publishing, the run log, scope cleanup |
| `test_checks.py` | `checks/`: CBS and KVK checks, quarantine |
| `test_kvk_logic.py`, `test_kvk_silver.py` | KVK file checks, inbox, silver KVK and its SBI mapping |
| `test_reference.py` | The SBI crosswalk: loading, rejecting, no duplicates |
| `test_gold.py` | Gold comparisons and tables, end to end from bronze to gold |
| `test_config.py`, `test_maintenance.py` | Configuration overrides, quarantine cleanup |
| `test_lineage.py` | The column lineage is complete (every silver and gold column), its bronze fields and silver columns exist, and `docs/lineage.md` is up to date |
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
  down right after reading a table (exit code 134, after the work was done). `pipeline/common/config.py`
  pauses 0.1 s at exit, which prevents it; details in `TODO.md`, "Known issues".

## Sources and licence

- CBS StatLine tables 80072NED, 86009NED, 83130NED, 81589NED, 81644NED and the SBI 2008 –
  SBI 2025 crosswalk: © CBS, [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- KVK Handelsregister Open Dataset Basis Bedrijfsgegevens: © KVK, CC BY 4.0.
