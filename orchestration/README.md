# Phase 6 — Deployment and monitoring

Runs the pipeline automatically, checks every layer before the next one is built on it,
and makes failures and stale data visible. Packaged with Docker Compose so it runs the
same on any machine, and tested on every push by GitHub Actions.

## Workflow: Dagster

[`definitions.py`](definitions.py) defines the pipeline as Dagster assets:

```
 schedule: 1st of the month 06:00            the person, in the Dagster UI
        │                                            │ (types the file name to confirm)
   cbs_bronze    sbi_crosswalk                  kvk_bronze
        └──────┬──────┘                              │
           silver_cbs ──── blocking checks ──────────┤
               │                                silver_kvk ── blocking checks
               └──────────────┬──────────────────────┘
                            gold ── blocking checks ──► dashboard
```

- **Only when needed.** CBS and the crosswalk are fetched monthly; when nothing is new,
  nothing downstream runs. Silver and gold rebuild automatically after new upstream data,
  but **only when the upstream blocking checks passed** (tables filled, one run, unique
  keys, keys present in their dimension; [`checks/layers.py`](../checks/layers.py)).
- **A person loads KVK.** `kvk_bronze` never runs on a schedule; it needs the file name
  typed as confirmation.
- **One writer at a time** ([`dagster.yaml`](dagster.yaml)): runs that write to the
  lakehouse never overlap.
- **Maintenance** on the 15th, one step after the other (a step waits for the previous
  one, so a vacuum or the backup never overlaps a delete): quarantine cleanup, KVK
  retention (latest snapshot plus one per month), personal data in the KVK upload log,
  Delta optimize and vacuum ([`tools/maintenance.py`](../tools/maintenance.py)), Dagster
  runs older than 180 days, and a verified backup ([`tools/backup.py`](../tools/backup.py)).
  Old runs are deleted with their logs, except a run that holds an asset's latest
  materialization or a check's latest result: without those, the automatic rebuilds would
  stop. Schedule and automation ticks are purged after 90 days ([`dagster.yaml`](dagster.yaml)).

## Monitoring

| What | Where |
|---|---|
| Failed runs and failed checks | the Dagster UI (no email or chat alerts yet: requirement FR-19) |
| Stale or stopped sources | freshness checks on the 1st, 09:00 (CBS quarterly > 4 months, yearly > 15 months, KVK > 45 days) and CBS's table status on every run |
| Every silver and gold run: when, which code version, which input versions, success or failure | `lakehouse/_run_log`; each row of silver and gold carries its `run_id` |
| One-screen overview: rows, periods, freshness, problems (exit code 1 on problems) | `python -m tools.inspect_lakehouse` |
| Any table, any version, with data-quality statistics | `python -m tools.lakehouse_viewer` (http://localhost:8502) |
| Where every column comes from | [docs/lineage_explorer.html](../docs/lineage_explorer.html), and in the Dagster UI per asset |
| Batches that failed their checks, with a report | `lakehouse/quarantine/` |

## Deployment: Docker Compose

[`docker-compose.yml`](../docker-compose.yml) with one image ([`Dockerfile`](../Dockerfile))
for all Python services:

| Service | Role |
|---|---|
| `dagster-webserver` | the Dagster UI, http://localhost:3000 |
| `dagster-daemon` | schedules and automation; the pipeline runs here |
| `postgres` | Dagster's run history (not the data) |
| `dashboard` | the Streamlit dashboard, http://localhost:8501 |
| `ollama`, `kvk-agent` | optional (profile `agent`): the local model and the KVK chat agent |

- The data stays in the project folder: `lakehouse/` and `inbox/` are mounted, so Docker
  and local runs share one lakehouse.
- Ports are published on 127.0.0.1 only; containers run as your user, not root.
- Package versions are exact (`requirements.txt`, `constraints.txt`), the same as
  locally and in CI. The code version is recorded in the run log:
  `docker compose build --build-arg CODE_VERSION=$(git rev-parse --short HEAD)`.

## Continuous integration: GitHub Actions

[`.github/workflows/tests.yml`](../.github/workflows/tests.yml) runs on every push:
1. **pytest** on a clean Python 3.12 with the exact package versions;
2. if that passes, **builds the Docker image** and checks that the code and the Dagster
   definitions load inside it.

## Run it

```bash
export DAGSTER_HOME=$PWD/orchestration
.venv/bin/dagster dev -f orchestration/definitions.py   # http://localhost:3000
# or everything in containers:
docker compose up -d
```

First time on a lakehouse filled by hand: run the job `register_existing_data` once
([INSTRUCTIONS.md](../INSTRUCTIONS.md#5-run-it-automatically-dagster)).

## Proof of working

```bash
.venv/bin/pytest tests/test_orchestration.py tests/test_maintenance.py
docker compose config -q && echo "compose file valid"
```

Expected: all tests pass (25 at the time of writing), and `compose file valid`. The
tests check the asset graph and schedules, that silver and gold wait for blocking checks
and a failing check stops gold, that new data triggers only the next layer, the one-writer
limit, the KVK confirmation, the freshness warnings, and every maintenance task. The
latest GitHub Actions run (tests on Python 3.12 + Docker build) is on the repository's
*Actions* tab.
