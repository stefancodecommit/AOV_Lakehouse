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

### The lakehouse viewer: every table, with data quality

Needs data in the lakehouse (run the pipeline first, section 2). Start it, then open the
address it prints:

```bash
.venv/bin/python -m tools.lakehouse_viewer       # http://localhost:8502, stop with Ctrl+C
```

In the sidebar, choose a **layer** (bronze, silver, gold, other), a **table**, a
**version** (older versions too, Delta time travel) and a **view**:

- **Rows**: the rows, with a text filter on one column that searches the whole table.
- **Data quality**: per column how complete it is, text with padding spaces (normal in
  bronze, 0 in silver), distinct values, min/max, the most common value; the flag
  columns' counts; duplicate rows and duplicate keys. Computed on the whole table: the
  KVK tables (1.9 million rows) take about 13 seconds the first time, then it is cached.
- **History**: one line per write to the table.

The viewer only reads; it never changes a table. It is reachable from this machine only.

### The lineage explorer: where every column comes from

A single web page, [docs/lineage_explorer.html](docs/lineage_explorer.html), that works
offline and needs no data. Open it **in a browser**; clicking the link in VS Code or on
GitHub shows its HTML source instead of the page:

```bash
xdg-open docs/lineage_explorer.html              # Linux (macOS: open, Windows: start)
```

or double-click the file in the file manager. Then pick a gold or silver column, or a
bronze field, in the list on the left (or search for it). The diagram shows **left**
where it comes from, back to the bronze fields, and **right** every column that depends
on it; click a box to follow the path. The address bar keeps the chosen column, so a link
opens it directly, for example
`docs/lineage_explorer.html#gold:gold_risk_index.relative_risk` (add the part from `#` to
the address in the browser).

The same lineage as tables and static diagrams: [docs/lineage.md](docs/lineage.md) and
[docs/lineage.html](docs/lineage.html). All three are generated from `tools/lineage.py`:
after changing the lineage there, run `.venv/bin/python -m tools.lineage`.

### The demo, with one command

```bash
.venv/bin/python -m tools.demo                   # dashboard, viewer and Dagster; opens a start page
```

Starts the three, waits until they answer, and opens a start page with a button per step
of the live demo ([docs/demo.md](docs/demo.md)). Ctrl+C stops all three.

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

### When the computer was off: Dagster catches up

The schedules only run while Dagster runs (this command, `python -m tools.demo`, or
Docker, section 6). A moment that passed while it didn't run is not lost: when Dagster
starts again, it starts the run of the **most recent missed moment** of each schedule
right away (once, not once per missed month). Example from this project: Dagster was
started on 9 November at 19:43 and immediately ran `cbs_monthly` (missed on 1 November
06:00), `freshness_monthly` (1 November 09:00) and `maintenance_monthly` (15 October
03:00); silver and gold followed at 19:44 and 19:45 because CBS had new data.

So it's enough to start Dagster now and then, for example once a month or whenever you
work on the project. Then:

- **let it run a few minutes**, until the *Runs* page in the UI shows nothing running. A
  run stopped halfway does no harm (silver and gold publish all or nothing), but its
  work is redone the next time;
- **local and Docker each keep their own history** (local in `orchestration/`, Docker in
  Postgres), so switching between them can make each catch up once. That is harmless:
  both use the same lakehouse, and bronze only fetches what is new.

For runs that don't depend on a laptop being switched on, see the cloud design
([docs/cloud_design.md](docs/cloud_design.md)).

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
`config.yaml`). Each new archive is checked right after writing; the newest three are kept.
On the lakehouse's own disk (the default) every backup logs a warning. For a real backup,
point it at another disk in `config.local.yaml`, and refuse to back up when that disk is
missing (otherwise the folder of an unplugged USB drive is created on the internal disk):

```yaml
backup:
  path: /media/<you>/<drive>/lakehouse-backups
  require_other_disk: true
```

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
for an indirect one), run the tests, rebuild the Docker image.

The same workflow scans the built Docker image with [Trivy](https://trivy.dev), including
the operating-system packages of its Debian base. It fails only on a critical or high
finding that has a fix; findings without a fix are listed in the log. By hand, with
Docker running (Trivy as a fixed release with its checksum, see the workflow file):

```bash
docker compose build
trivy image --scanners vuln --ignore-unfixed --severity CRITICAL,HIGH cbs-kvk-lakehouse:latest
```

A fixable finding in a Debian package disappears by rebuilding the image: the Dockerfile
applies Debian's security updates at build time (`apt-get upgrade`), so a fix Debian has
released is in the next build even before the official `python:3.12-slim` has it
(`docker compose build --pull`; GitHub rebuilds on every run). A finding in a Python
package needs its pin raised, as above.

So far: 2026-10-26, pillow 12.3 → 13.0, filelock 4.0 → 4.2.1 and certifi (streamlit
1.65 → 1.66.1, because 1.65 doesn't allow pillow 13); 2026-11-09, zlib in the Debian base
(CVE-2026-85091, fixed by the build-time updates).

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
ls lakehouse/quarantine/      # batches that failed their checks; report.json says why
```

A quarantined CBS batch is fetched and checked again on the next `pipeline.bronze.cbs`
run; a quarantined KVK file is checked again when you load it again.

## 8. Tests and documentation

```bash
.venv/bin/pytest                              # all tests (~70 s, no internet needed)
.venv/bin/pytest tests/test_gold.py           # one file
.venv/bin/pytest -m live                      # the one test that calls the real CBS API
.venv/bin/python -m tools.lineage             # regenerate docs/lineage.md, .html and the explorer
.venv/bin/ruff check .                        # the linter: errors, bug patterns, import order
.venv/bin/ruff check --fix .                  # fix what it can fix safely
```

GitHub Actions runs `ruff check .` before the tests; its settings (which rules, and why
some are left out) are in `pyproject.toml`. The formatter (`ruff format`) is not used
yet: it would rewrite most files.

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
