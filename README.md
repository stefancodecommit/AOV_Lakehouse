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
need, the goals and the boundaries are in
[docs/PROJECT.md](docs/PROJECT.md#the-organisation-and-its-need).

Everything runs locally in Python, without a database server or Spark: the data is stored
as [Delta Lake](https://delta.io) tables in the folder `lakehouse/`.

### Start here (for the teacher)

Four things, about 20 minutes, in this order:

1. **The report**: [`docs/rapport.md`](docs/rapport.md) (Dutch; also as Word and PDF with
   `python -m tools.export_report`). Chapter 5 evaluates the result critically, including
   a backtest of the model.
2. **The model and its validation**: [phase 4 in gold's README](pipeline/gold/README.md#phase-4--model-design-and-testing-the-risk-index):
   the risk index, why no machine learning, the tests, and the backtest (a *higher* or
   *lower* verdict held a year later in all 440 cases; the trend describes, it doesn't
   predict).
3. **Proof that it works**: [the six phases, each with its README](#the-six-phases-each-with-its-readme),
   [`tests/README.md`](tests/README.md) (380 tests, run by GitHub Actions on every push,
   plus a weekly vulnerability scan) and [`docs/requirements.md`](docs/requirements.md)
   (every requirement with its status and the test that verifies it).
4. **How GenAI was used**: [`docs/genai_use.md`](docs/genai_use.md): which decisions were
   the author's, how the output was checked, and eight real mistakes that were caught.

To see it running: the dashboard (`streamlit run dashboard/app.py`), page *Risk index*.
To look inside: the **lakehouse viewer** (every table with its data quality,
`python -m tools.lakehouse_viewer`) and the **lineage explorer** (where every column comes
from, `docs/lineage_explorer.html` in a browser); both are explained in
[INSTRUCTIONS.md, section 4](INSTRUCTIONS.md#4-look-at-the-results).

| Document | What it's for |
|---|---|
| [`INSTRUCTIONS.md`](INSTRUCTIONS.md) | Every command: the whole pipeline or one part, by hand, with Dagster or in Docker; maintenance, backup, tests |
| [`docs/PROJECT.md`](docs/PROJECT.md) | The design: sub-questions, data sources, and *why* each choice was made |
| [`docs/rapport.md`](docs/rapport.md) | The assignment report, in Dutch (≤5000 words) |
| [`docs/requirements.md`](docs/requirements.md) | Stakeholders, use cases, requirements with status and the test that verifies each |
| [`docs/privacy_and_risks.md`](docs/privacy_and_risks.md) | GDPR, DPIA, other rules (KVK terms, EU AI Act), the risk register |
| [`docs/cloud_design.md`](docs/cloud_design.md) | A design (not built) for running the project on Azure |
| [`docs/genai_use.md`](docs/genai_use.md) | How generative AI (Claude) was used, checked, and where it went wrong |
| [`docs/bronze_to_silver.md`](docs/bronze_to_silver.md) | How bronze becomes silver, table by table, with real examples |
| [`docs/lineage.md`](docs/lineage.md), [`lineage.html`](docs/lineage.html), [`lineage_explorer.html`](docs/lineage_explorer.html) | Column lineage bronze → silver → gold, generated from `tools/lineage.py`: tables, diagrams, and an interactive explorer |
| [`docs/presentatie.pptx`](docs/presentatie.pptx), [`demo.md`](docs/demo.md), [`qa_practice.md`](docs/qa_practice.md) | The presentation (Dutch), the demo script, practice questions |
| [`TODO.md`](TODO.md) | Open work, and a log of every decision and real run, including the code review |

## Concepts in two minutes

**Bronze, silver, gold (the "medallion" pattern).** Data moves through three layers, each
with one job:

- **Bronze** stores the data *exactly as received*, plus where and when it came from.
  Nothing is interpreted yet, so any later step can be redone without asking the source
  again.
- **Silver** cleans and types the data, gives codes a meaning (which key is a sector,
  which a size class), and links the sources together.
- **Gold** shapes silver into the tables the dashboard needs, with the comparisons
  already calculated (difference from national, ranking, change versus last year).

**Delta Lake.** A table format on top of Parquet files. Its transaction log
(`_delta_log/`) makes a write all-or-nothing and keeps earlier versions readable ("time
travel"). The `deltalake` package reads and writes it from plain Python.

**Idempotent ingestion.** Running a script twice never creates duplicates or requests the
same data twice: every bronze script logs what it loaded and skips it next time.

## How it fits together

```
cbs-kvk-lakehouse/
├── pipeline/       the data flow, one subfolder per layer
│   ├── bronze/     sources -> bronze: cbs.py, reference.py (SBI crosswalk), kvk_agent.py
│   ├── silver/     bronze -> silver: cbs.py, kvk.py
│   ├── gold/       silver -> gold: build.py (incl. the risk index)
│   └── common/     config.py (settings), publish.py (all-or-nothing publishing), runlog.py
├── checks/         data checks before writing, layer checks, freshness, quarantine
├── dashboard/      the Streamlit app; reads the gold tables only
├── orchestration/  Dagster: schedules, automatic rebuilds, blocking checks
├── tools/          inspect, viewer, maintenance, backup, lineage, backtest, demo, report export
├── tests/          pytest
├── docs/           design, report, requirements, privacy, lineage
├── config.yaml     every setting in one file
└── lakehouse/, inbox/, backups/    the data, the KVK uploads, the backups (not in git)
```

```
 SOURCES                  BRONZE                   SILVER                    GOLD
 (outside)                as received              cleaned, typed            dashboard-ready

 CBS OData API ─────► bronze/cbs.py ───────┐
 (3 tables +              lakehouse/bronze/  ├──► silver/cbs.py ─────────┐
  code lists)                                │    9 tables (dims,        │
 CBS crosswalk xlsx ─► bronze/reference.py ─┘    facts, SBI crosswalk)  ├──► gold/build.py ──► dashboard/
 (SBI 2008 -> 2025)       bronze/reference/            │                 │    7 tables          (Streamlit)
                                                       │ crosswalk       │
 KVK zip in inbox/kvk ─► bronze/kvk_agent.py ──────► silver/kvk.py ─────┘
 (1.9M companies)         bronze/kvk/              kvk_company
                          (Ollama agent or --inbox)
```

- Each arrow is a script reading the layer on its left and writing the one on its right.
  The scripts don't call each other; they share data only through `lakehouse/`, so they
  run in order: by hand, or by Dagster on a schedule and only when new data arrived.
- `silver/kvk.py` needs the SBI crosswalk from silver CBS, because KVK codes companies in a
  newer classification (SBI 2025) than CBS (SBI 2008).
- Data quality is built in at every step: checks before writing (a failed batch goes to
  `lakehouse/quarantine/` with a report and is retried next run), silver and gold
  published all-or-nothing through a staging folder, a `run_id` on every row that joins
  to the run log (when, which code version, which inputs), and blocking checks in Dagster
  before the next layer is built.

### The six phases, each with its README

The course describes data and AI engineering as six phases. Each has a README next to its
code: what the phase does, the design decisions, how to run it, and a **proof of
working** (the test command and what to expect). Every script also starts with a
docstring that explains it in detail.

| Phase | README | In this project |
|---|---|---|
| 1. Ingestion | [pipeline/bronze/](pipeline/bronze/README.md) | CBS API, the SBI crosswalk, the KVK file with an AI intake agent; checks and quarantine before writing |
| 2. Transformation | [pipeline/silver/](pipeline/silver/README.md) | typed dimensions and facts; KVK companies translated to SBI 2008; edge cases flagged |
| 3. Serving | [pipeline/gold/](pipeline/gold/README.md), [dashboard/](dashboard/README.md) | 7 gold tables; the Streamlit dashboard for the data analyst |
| 4. Model design and testing | [pipeline/gold/](pipeline/gold/README.md#phase-4--model-design-and-testing-the-risk-index), [tests/](tests/README.md) | the risk index (a white-box model) and how everything is tested |
| 5. Model integration | [pipeline/silver/](pipeline/silver/README.md#phase-5--model-integration) | CBS and KVK joined through the SBI crosswalk; absence with business counts; an LLM inside the KVK intake |
| 6. Deployment and monitoring | [orchestration/](orchestration/README.md) | Dagster schedules and checks, Docker Compose, GitHub Actions, the run log |

## Quick start

Linux or macOS, Python 3.11 or newer, about 2 GB free memory. Every command, and what to
do on other setups, is in [INSTRUCTIONS.md](INSTRUCTIONS.md).

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt          # exact versions (requirements.txt, constraints.txt)

.venv/bin/python -m pipeline.bronze.cbs                # CBS API -> bronze
.venv/bin/python -m pipeline.bronze.reference          # SBI crosswalk -> bronze
.venv/bin/python -m pipeline.bronze.kvk_agent --inbox  # KVK zip in inbox/kvk/ -> bronze (asks YES)
.venv/bin/python -m pipeline.silver.cbs                # -> silver
.venv/bin/python -m pipeline.silver.kvk                # -> silver (needs silver CBS)
.venv/bin/python -m pipeline.gold.build                # -> gold
.venv/bin/python -m tools.inspect_lakehouse            # check the result
.venv/bin/streamlit run dashboard/app.py               # the dashboard: http://localhost:8501

.venv/bin/pytest                                       # the tests (no internet needed)
```

The KVK zip comes from
<https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip>. Every step can be
repeated safely: it only does work when something is new. To run it all automatically:
Dagster ([INSTRUCTIONS §5](INSTRUCTIONS.md#5-run-it-automatically-dagster)) or Docker
Compose ([§6](INSTRUCTIONS.md#6-run-everything-in-docker)); for the live demo,
`python -m tools.demo` starts everything at once.

## Configuration

All settings are in [`config.yaml`](config.yaml): paths, the CBS tables and start year, the
KVK and crosswalk URLs, the Ollama model, the risk-index thresholds, schedules and
retention. Override them without editing the file, in `config.local.yaml` (not in git) or
with environment variables such as `LAKEHOUSE__CBS__START_YEAR=2022`. A misspelled setting
name stops the script with an error instead of being ignored.

## Known limitations

- **Sector and size are separate.** CBS publishes absence per sector *or* per size class,
  never per combination, so "small construction companies" can't be shown.
- **KVK has no company number** in the open dataset: companies can't be followed between
  uploads, only counted. Silver therefore holds the newest upload only; bronze keeps the
  older ones.
- **The KVK sector of 10% of active companies is not determined**, because their codes
  can't be translated to SBI 2008 unambiguously; they are marked, not guessed.
- **Provisional figures** (the latest CBS periods) can still change; they are marked, and
  the dashboard can show final figures only.
- **A known library issue, worked around:** `deltalake` could crash while Python shut down
  right after reading a table (exit code 134, after the work was done).
  `pipeline/common/config.py` pauses 0.1 s at exit, which prevents it (TODO.md, "Known
  issues").

## Sources and licence

- CBS StatLine tables 80072NED, 86009NED, 81589NED and the SBI 2008 – SBI 2025 crosswalk:
  © CBS, [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- KVK Handelsregister Open Dataset Basis Bedrijfsgegevens: © KVK, CC BY 4.0.
