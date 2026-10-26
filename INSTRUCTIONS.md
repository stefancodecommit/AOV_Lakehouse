# Instructions: which commands to run

The commands to run the whole project or one part of it, ready to copy. Run them from the
project folder (`cbs-kvk-lakehouse/`). For *why* things work this way, see
[README.md](README.md); for the design, [docs/PROJECT.md](docs/PROJECT.md).

There are three ways to run the project. Pick one per session; all three work on the same
data in `lakehouse/`.

| Way | When | Section |
|---|---|---|
| By hand, step by step | to see each step, or to run one part | [2](#2-run-the-whole-pipeline-by-hand), [3](#3-run-one-part) |
| Dagster on this machine | automatic: schedules and rebuilds after new data | [5](#5-run-it-automatically-dagster) |
| Docker Compose | everything in containers, no Python setup | [6](#6-run-everything-in-docker) |

## Contents

1. [One-time setup](#1-one-time-setup)
2. [Run the whole pipeline by hand](#2-run-the-whole-pipeline-by-hand)
3. [Run one part](#3-run-one-part)
4. [Look at the results](#4-look-at-the-results)
5. [Run it automatically: Dagster](#5-run-it-automatically-dagster)
6. [Run everything in Docker](#6-run-everything-in-docker)
7. [Maintenance and repairs](#7-maintenance-and-repairs)
8. [Tests and documentation](#8-tests-and-documentation)
9. [Stop everything](#9-stop-everything)

---

## 1. One-time setup

Python 3.11 or newer (Linux or macOS); CI and the Docker image use 3.12. Ubuntu 22.04
comes with 3.10, which is too old since the security updates of 2026-10-26 (pillow 13,
filelock 4.2 need 3.11+); get 3.12 without sudo through [uv](https://docs.astral.sh/uv/):

```bash
uv python install 3.12
"$(uv python find 3.12)" -m venv .venv            # or: python3 -m venv .venv, if that is 3.11+
.venv/bin/pip install -r requirements-dev.txt
```

The KVK file is not downloaded automatically. Put it in `inbox/kvk/` (about 13 MB):

```bash
mkdir -p inbox/kvk
curl -L -o inbox/kvk/kvk-open-dataset-basis-bedrijfsgegevens.zip \
  https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip
```

Optional, only for the KVK *chat* agent: Ollama with a model (on this machine it lives
in `~/.local/ollama`):

```bash
~/.local/ollama/bin/ollama serve &               # start the model server
~/.local/ollama/bin/ollama pull qwen2.5:7b       # once, ~4.7 GB
```

## 2. Run the whole pipeline by hand

In this order; each step needs the ones before it. Every step can be repeated safely:
it skips what is already there.

```bash
.venv/bin/python -m pipeline.bronze.cbs                # 1. CBS API -> bronze (~40 s the first time)
.venv/bin/python -m pipeline.bronze.reference          # 2. SBI crosswalk (xlsx) -> bronze
.venv/bin/python -m pipeline.bronze.kvk_agent --inbox  # 3. newest new KVK zip in inbox/kvk/ -> bronze (type YES)
.venv/bin/python -m pipeline.silver.cbs                # 4. bronze CBS + crosswalk -> silver (11 tables)
.venv/bin/python -m pipeline.silver.kvk                # 5. bronze KVK -> silver kvk_company (needs 4)
.venv/bin/python -m pipeline.gold.build                # 6. silver -> gold (7 tables)
.venv/bin/python -m tools.inspect_lakehouse            # 7. check: should end with "problems: none"
.venv/bin/streamlit run dashboard/app.py               # 8. the dashboard: http://localhost:8501
```

Step 8 keeps running until you press Ctrl+C.

## 3. Run one part

### Which steps to rerun after a change

Each layer is built from the one before it, so after a change, rerun from that layer
down. Bronze steps only fetch what is new; silver and gold are rebuilt as a whole.

| What changed | Run |
|---|---|
| CBS published new figures (about monthly) | `pipeline.bronze.cbs`, `pipeline.silver.cbs`, `pipeline.gold.build` |
| A new KVK file in `inbox/kvk/` | `pipeline.bronze.kvk_agent --inbox`, `pipeline.silver.kvk`, `pipeline.gold.build` |
| A new SBI crosswalk version from CBS | `pipeline.bronze.reference --url <url>`, `pipeline.silver.cbs`, `pipeline.silver.kvk` (rebuilds every upload by itself), `pipeline.gold.build` |
| Code in `pipeline/silver/` | `pipeline.silver.cbs`, and/or `pipeline.silver.kvk --full` (or raise `LOGIC_VERSION` in it), then `pipeline.gold.build` |
| Code in `pipeline/gold/` | `pipeline.gold.build` |
| Code in `dashboard/` | nothing; reload the page (restart Streamlit for a new Python module) |
| `config.yaml`: a table, start year or period type removed | [section 7](#remove-data-outside-the-configuration), then silver and gold |

Each `pipeline.…` above is run as `.venv/bin/python -m pipeline.…`.

### Bronze: get the data

```bash
# CBS
.venv/bin/python -m pipeline.bronze.cbs --dry-run           # only list what would be requested
.venv/bin/python -m pipeline.bronze.cbs                     # fetch what is new
.venv/bin/python -m pipeline.bronze.cbs --only 81589        # only endpoints whose name contains 81589
.venv/bin/python -m pipeline.bronze.cbs --only 81589 --force   # fetch again even if already loaded (replaces, never duplicates)

# SBI 2008 -> 2025 crosswalk (a file already loaded is skipped)
.venv/bin/python -m pipeline.bronze.reference               # from the URL in config.yaml
.venv/bin/python -m pipeline.bronze.reference --file inbox/reference/schakelschema-sbi2008sbi2025-v20.xlsx
.venv/bin/python -m pipeline.bronze.reference --url <url of a newer version>

# KVK (asks you to type YES before writing)
.venv/bin/python -m pipeline.bronze.kvk_agent --inbox       # the newest file in inbox/kvk/ not loaded yet
.venv/bin/python -m pipeline.bronze.kvk_agent --file kvk-open-dataset-basis-bedrijfsgegevens.zip
.venv/bin/python -m pipeline.bronze.kvk_agent               # chat with the agent (Ollama must be running)
```

### Silver and gold: build the tables

```bash
.venv/bin/python -m pipeline.silver.cbs --preview           # build and summarise, write nothing
.venv/bin/python -m pipeline.silver.cbs
.venv/bin/python -m pipeline.silver.kvk --preview           # what the next run would add or remove
.venv/bin/python -m pipeline.silver.kvk                     # only new uploads (~1.5-1.8 GB when it adds one)
.venv/bin/python -m pipeline.silver.kvk --full              # rebuild every kept upload, one at a time
.venv/bin/python -m pipeline.gold.build --preview
.venv/bin/python -m pipeline.gold.build
```

A silver CBS or gold run writes all its tables, or none: if one table fails, the
published tables stay as they were. Gold refuses to build from silver CBS tables of
different runs. Silver KVK is updated per upload instead: a run that stops halfway is
finished by the next one.

## 4. Look at the results

```bash
.venv/bin/python -m tools.inspect_lakehouse                 # every table: rows, periods, freshness; warnings; problems
.venv/bin/python -m tools.inspect_lakehouse --layer silver  # one layer (bronze, silver or gold)
.venv/bin/python -m tools.lakehouse_viewer                  # browse any table + data quality: http://localhost:8502
.venv/bin/streamlit run dashboard/app.py                    # the dashboard: http://localhost:8501
```

`inspect_lakehouse` exits with code 1 when it finds problems. The viewer and the
dashboard keep running until Ctrl+C; both can run at the same time.

The column lineage (where every silver and gold column comes from) is in
[docs/lineage_explorer.html](docs/lineage_explorer.html) (open it in a browser, works
offline) and [docs/lineage.md](docs/lineage.md).

## 5. Run it automatically: Dagster

```bash
export DAGSTER_HOME=$PWD/orchestration
.venv/bin/dagster dev -f orchestration/definitions.py       # UI: http://localhost:3000
```

Dagster acts only while this command runs (Ctrl+C stops it). Then:

- **Once, on a lakehouse filled by hand:** run the job `register_existing_data` (UI:
  *Jobs*), or from a second terminal:

  ```bash
  DAGSTER_HOME=$PWD/orchestration .venv/bin/dagster job execute \
    -f orchestration/definitions.py -j register_existing_data
  ```

- **CBS and the crosswalk** are fetched on the 1st of each month at 06:00; silver and
  gold follow automatically when something new arrived and the checks passed.
- **A new KVK file:** put it in `inbox/kvk/`, select the asset `kvk_bronze` in the UI,
  click *Materialize*, and type the file name in `confirm_file_name`.
- **By hand in the UI:** select an asset (e.g. `silver_cbs`) and click *Materialize*.
- Freshness warnings: 1st of the month 09:00; maintenance: 15th 03:00. Results and
  failures are shown in the UI.

## 6. Run everything in Docker

Needs only Docker (about 3 GB memory for the pipeline, 8 GB with the chat agent).

```bash
docker compose build --build-arg CODE_VERSION=$(git rev-parse --short HEAD)   # after a code change
docker compose up -d                       # start: Dagster http://localhost:3000, dashboard http://localhost:8501
docker compose ps                          # what is running
docker compose logs -f dagster-daemon      # follow the pipeline runs (Ctrl+C stops following, not the run)
docker compose down                        # stop (data and run history stay)
```

- The containers use the same `lakehouse/` and `inbox/` folders as the local commands.
- Once, on a lakehouse filled by hand: run the job `register_existing_data` in the
  Dagster UI (*Jobs*).
- Loading a KVK file: in the Dagster UI, as in [section 5](#5-run-it-automatically-dagster).
- The KVK chat agent (optional, downloads a 4.7 GB model the first time):

  ```bash
  docker compose --profile agent up -d                 # start Ollama
  docker compose --profile agent run --rm kvk-agent    # the chat; type exit to stop
  docker compose --profile agent down                  # stop Ollama too
  ```

## 7. Maintenance and repairs

### Monthly maintenance

Dagster runs this on the 15th (and then also deletes Dagster runs older than 180 days and
makes a backup); by hand, try `--dry-run` first:

```bash
.venv/bin/python -m tools.maintenance all --dry-run         # what would be done
.venv/bin/python -m tools.maintenance all                   # quarantine cleanup, KVK retention, personal data in the upload log, Delta vacuum
.venv/bin/python -m tools.maintenance quarantine            # or one task: quarantine, kvk-retention, personal-data, delta
```

The retention periods are in `config.yaml`, section `retention`.

### Backup and restore

A backup is one archive of `lakehouse/` and `inbox/kvk/` in `backups/` (`backup.path` in
`config.yaml`; point it at another disk in `config.local.yaml` for a real backup). Each new
archive is checked right after writing; the newest three are kept.

```bash
.venv/bin/python -m tools.backup create      # make, verify and rotate (~15 s, ~160 MB)
.venv/bin/python -m tools.backup list        # the backups, newest first
.venv/bin/python -m tools.backup verify      # check the newest again (or give an archive)
```

To restore, unpack into a new folder (never over the live lakehouse), check it, and swap
the folders by hand while nothing runs:

```bash
.venv/bin/python -m tools.backup restore backups/backup_<time>.tar.gz --to ../restore
# stop Dagster and the dashboard (section 9), then:
mv lakehouse lakehouse.broken && mv ../restore/lakehouse lakehouse
cp -n ../restore/inbox/kvk/* inbox/kvk/       # KVK uploads that are missing
.venv/bin/python -m tools.inspect_lakehouse   # ends with "problems: none"
```

### Known vulnerabilities in the packages

GitHub checks the pinned packages weekly (`.github/workflows/audit.yml`). To check by hand,
in a separate environment so the project's own stays as pinned, on the same Python as the
project (pip-audit resolves the packages for the Python it runs on):

```bash
.venv/bin/python -m venv /tmp/audit && /tmp/audit/bin/pip install pip-audit==2.10.1
/tmp/audit/bin/pip-audit -r requirements-dev.txt -r requirements-docker.txt
```

If a package is reported: raise its version in `requirements.txt` (or `constraints.txt`
for an indirect one), run the tests, rebuild the Docker image. Done once so far
(2026-10-26): pillow 12.3 → 13.0, filelock 4.0 → 4.2.1, certifi; streamlit 1.65 → 1.66.1
because 1.65 doesn't allow pillow 13.

### Remove data outside the configuration

After raising the start year, removing a table or changing the period type in
`config.yaml`:

```bash
.venv/bin/python -m pipeline.bronze.cbs --cleanup-scope --dry-run   # list what is outside the configuration
.venv/bin/python -m pipeline.bronze.cbs --cleanup-scope             # remove it
.venv/bin/python -m pipeline.silver.cbs && .venv/bin/python -m pipeline.gold.build
```

### Other repairs

```bash
.venv/bin/python -m pipeline.bronze.cbs --rebuild-columns   # rebuild bronze's field columns from raw_json (no API calls)
ls lakehouse/quarantine/                                     # batches that failed their checks; report.json says why
```

A quarantined CBS batch is fetched and checked again on the next `pipeline.bronze.cbs`
run; a quarantined KVK file is checked again when you load it again.

## 8. Tests and documentation

```bash
.venv/bin/pytest                              # all tests (~70 s, no internet needed)
.venv/bin/pytest tests/test_gold.py           # one file
.venv/bin/pytest -m live                      # the one test that calls the real CBS API
.venv/bin/python -m tools.lineage             # regenerate docs/lineage.md, .html and the explorer
```

After changing a silver or gold column, regenerate the lineage: a test fails until
`tools/lineage.py` describes the new column.

The report (in Dutch) is written in `docs/rapport.md`. To make the Word and PDF files for
submission:

```bash
.venv/bin/python -m tools.export_report       # -> build/rapport.docx, build/rapport.pdf (~10 s)
```

It needs Firefox (to draw the diagrams), LibreOffice and an internet connection. Edit
`docs/rapport.md` and export again; changes made in the Word file are lost at the next
export.

## 9. Stop everything

```bash
# a command running in a terminal (dashboard, viewer, dagster dev): Ctrl+C in that terminal
docker compose --profile agent down           # all containers of this project
pkill -f "ollama serve"                       # the local Ollama server, if started
```
