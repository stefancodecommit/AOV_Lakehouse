# Phase 1 — Ingestion (bronze)

Gets the data from its sources into the lakehouse **exactly as received**, plus where and
when it came from. Nothing is interpreted here; that is the next phase. Because bronze
keeps the original, every later step can be rebuilt without asking a source again.

```
 CBS OData API ──► cbs.py ───────► lakehouse/bronze/<table>_<entity>/      (one Delta table per endpoint)
 CBS crosswalk ──► reference.py ─► lakehouse/bronze/reference/sbi2008_sbi2025/
 KVK zip file  ──► kvk_agent.py ─► lakehouse/bronze/kvk/basis_bedrijfsgegevens/
                                   + a log per source: what was loaded, when, from where
```

## The three sources

| Script | Source | How it arrives | Key behaviour |
|---|---|---|---|
| [`cbs.py`](cbs.py) | CBS StatLine: absence (80072NED, 86009NED, 83130NED), businesses (81589NED, 81644NED), their code lists and metadata | pulled from the OData API, monthly | one request per period; only periods not yet loaded; a revised provisional period replaces its old rows |
| [`reference.py`](reference.py) | the CBS SBI 2008 → SBI 2025 crosswalk (Excel) | downloaded when CBS publishes a new version | a file already loaded (same content) is skipped |
| [`kvk_agent.py`](kvk_agent.py) | the KVK open dataset (zip, ~13 MB, ~1.9M companies) | placed in `inbox/kvk/` by a person | verified, then loaded only after the person confirms; never the same file twice |

## Design decisions

- **Ask only once (idempotent).** Every CBS request has a key that includes the period
  and its CBS status. A key in the ingestion log (`bronze/_ingestion_log`) is never
  requested again; when CBS revises a period, the status and therefore the key change,
  so only that period is fetched again, and its rows are replaced, not added.
- **Check before writing.** Every CBS batch is checked against the table's own metadata
  (columns, the requested period, numeric measures, unique keys, not empty)
  ([`checks/cbs.py`](../../checks/cbs.py)). A batch that fails is not written; it goes to
  `lakehouse/quarantine/` with a report and is retried on the next run. KVK files are
  verified the same way ([`checks/kvk.py`](../../checks/kvk.py)).
- **Raw and structured.** A CBS record is kept as received in `raw_json` *and* as one
  column per field (typed from the table's metadata), so bronze can be queried directly
  and still proves what was received.
- **A human confirms the KVK load.** An AI agent (a local LLM via Ollama) helps the user
  find and verify the file, but it can't write: loading happens only after the user
  types `YES` (or, in Dagster, the file name). The LLM chooses *which* tool to call; the
  checks and the load are ordinary code. This is the AI part of the ingestion
  (*model integration*, see [silver's README](../silver/README.md#phase-5--model-integration)).
- **Source lifecycle.** CBS sometimes stops a table (83130NED ended in 2023). Every run
  reads each table's status and warns when it is stopped or not updated for too long.

More detail: the docstring at the top of each script, and
[README → The scripts one by one](../../README.md#the-scripts-one-by-one).

## Run it

From the project folder:

```bash
.venv/bin/python -m pipeline.bronze.cbs --dry-run           # what would be requested
.venv/bin/python -m pipeline.bronze.cbs                     # fetch what is new
.venv/bin/python -m pipeline.bronze.reference               # the crosswalk
.venv/bin/python -m pipeline.bronze.kvk_agent --inbox       # the newest new KVK file (type YES)
```

All options: [INSTRUCTIONS.md](../../INSTRUCTIONS.md#bronze-get-the-data).

## Proof of working

```bash
.venv/bin/pytest tests/test_bronze_logic.py tests/test_checks.py tests/test_reference.py tests/test_kvk_logic.py
```

Expected: all tests pass (88 at the time of writing), without internet. They cover period
selection, request keys and paging, "a second run makes no data requests", quarantine of
a failing batch, every CBS and KVK check, the crosswalk load (twice: nothing added), and
the KVK inbox and confirmation. The writes to real Delta tables (a revised period
replaces its rows; a repeated write adds nothing) are in `tests/test_delta.py`.

On the real data, a dry run shows that everything is already loaded:

```bash
.venv/bin/python -m pipeline.bronze.cbs --dry-run
# ... INFO Done. fetched=0 skipped(already in bronze)=92 failed=0 quarantined=0 ...
```
