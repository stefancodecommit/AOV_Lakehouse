# Testing

How the project is tested, and how to check it yourself. This is the *testing* half of
phase 4 (model design and testing) and the proof of working for every phase.

```bash
.venv/bin/pytest            # all tests, ~75 s, no internet needed
```

Expected: all pass (372 at the time of writing, 1 deselected: the live test below). The
same suite runs on every push in GitHub Actions, on a clean Python 3.12.

## What is tested, per phase

| Phase | Test files | Tests* | What they prove |
|---|---|---|---|
| 1. Ingestion | `test_bronze_logic.py`, `test_checks.py`, `test_reference.py`, `test_kvk_logic.py` | 88 | only new data is requested; a failing batch is quarantined and retried; every CBS and KVK check; a file is never loaded twice |
| 2. Transformation | `test_silver_logic.py`, `test_kvk_silver.py` | 116 | every silver table and its keys; no padded keys; unknown keys flagged; SBI translation per case; incremental KVK equals a full rebuild |
| 3. Serving | `test_gold.py`, `test_dashboard.py` | 45 | every gold table; every dashboard page, with and without *Final figures only* |
| 4. Model | `test_gold.py` (risk index) | (in 45) | the risk index on data with known answers (see [gold's README](../pipeline/gold/README.md#phase-4--model-design-and-testing-the-risk-index)) |
| 6. Deployment & monitoring | `test_orchestration.py`, `test_maintenance.py` | 25 | the Dagster graph, blocking checks, automation, the one-writer limit, maintenance |
| All layers on real Delta tables | `test_delta.py` | 29 | bronze writes (revisions replace, repeats add nothing), silver end to end with types read back, publishing all-or-nothing, the run log |
| Documentation and tools | `test_lineage.py`, `test_config.py`, `test_lakehouse_viewer.py` | 69 | every silver and gold column has a documented origin; the generated docs are up to date; config errors are caught; the viewer's statistics |

\* At the time of writing (2026-10-17).

## Kinds of tests

- **Unit tests** on pure logic: parsers, request keys, checks, the risk index arithmetic.
  Fast, on small hand-made data where the right answer is known in advance.
- **Integration tests on real Delta tables** in a temporary folder: write bronze, build
  silver and gold, read them back, publish, crash halfway, rerun.
- **Page tests:** every dashboard page runs headless with Streamlit's `AppTest`.
- **Orchestration tests:** Dagster materialises the assets in memory, with checks and
  automation.
- **One live test** against the real CBS API, off by default:
  `.venv/bin/pytest -m live` (needs internet).

## Safety nets

- **Tests never touch the real data.** [`conftest.py`](conftest.py) points every
  lakehouse and inbox path at a temporary folder, checks that before *and* after every
  test, and refuses every Delta write to the real `lakehouse/` or `inbox/` for the whole
  session.
- **The tests are tested.** Several were checked by breaking the code on purpose (e.g. the
  delete-before-write in bronze, the `is_latest` flag in silver KVK, redoing failed KVK
  runs): each time the right tests failed.
- **Documentation can't drift.** A new silver or gold column fails `test_lineage.py` until
  its origin is written down in `tools/lineage.py`, and the generated lineage documents
  must be up to date.

## Run part of it

```bash
.venv/bin/pytest tests/test_gold.py                         # one file
.venv/bin/pytest tests/test_gold.py -k risk                 # tests whose name contains "risk"
.venv/bin/pytest -x                                         # stop at the first failure
```
