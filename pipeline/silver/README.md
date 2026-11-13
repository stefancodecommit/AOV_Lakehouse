# Phase 2 — Transformation (silver)

Turns bronze into clean, typed tables that can be joined: every CBS code gets its
meaning, every figure its type, and the sources are linked. Silver is the single place
where data is interpreted; gold and the dashboard only combine what silver provides.

```
 bronze CBS tables + crosswalk ──► cbs.py ──► 9 tables: dimensions, facts, maps
 bronze KVK + silver crosswalk ──► kvk.py ──► kvk_company (one row per company per snapshot)
```

## What it builds

| Table | Kind | What |
|---|---|---|
| `dim_period`, `dim_sector`, `dim_company_size`, `dim_survey_measure` | dimensions | one row per period, SBI sector (every level), size class, survey measure |
| `map_characteristic` | map | what each CBS company-characteristic key means: a sector, a size class, the total |
| `map_sbi2008_sbi2025` | map | the crosswalk between the two SBI versions (many-to-many) |
| `fct_absence_rate` | fact | quarterly absence % per sector and size class (80072NED) |
| `fct_absence_survey` | fact | yearly survey measures with 95% bounds (86009NED) |
| `fct_business_count` | fact | businesses per sector, size and legal form (81589NED) |
| `kvk_company` | fact | KVK companies with their sector, translated to SBI 2008 |

Every column, where it comes from in bronze and what happens to it on the way:
[docs/bronze_to_silver.md](../../docs/bronze_to_silver.md) (explained with real examples)
and [docs/lineage.md](../../docs/lineage.md) (generated, complete).

## Design decisions

- **Flag, don't guess.** An unknown key, a KVK company with two main activities, a code
  that exists in both SBI versions: the original value is kept and a flag column says
  what is special, instead of picking one. Gold and the dashboard can filter on flags.
- **Strip CBS's padding once** (`'301000 '` → `'301000'`), so every join just works.
- **Publish all or nothing.** Silver CBS and KVK write their tables to a staging folder first
  and replaces the live tables only when all succeeded
  ([`pipeline/common/publish.py`](../common/publish.py)). Every row carries the `run_id`
  of its run, which joins to the run log.
- **KVK: the newest upload only.** Each KVK upload is a snapshot of ~1.9M rows without a
  company number, so snapshots can't be linked; gold uses the newest. Silver therefore
  holds only that one (memory ~1.7 GB, however many uploads bronze keeps); bronze keeps
  the older uploads, because KVK can't serve them again. (Until 2026-11 silver kept every
  snapshot and updated them incrementally; nothing used that history.)
- **A snapshot is dated by KVK, not by us.** `snapshot_date` is KVK's export date, read
  from the zip when the file is verified (bronze's batch log, `file_date`); the newest
  start date in the file must not be later. A file without a usable date (a loose CSV)
  gets the upload day, and `snapshot_date_source` says which one it is (`kvk_file` or
  `upload`).

## Phase 5 — Model integration

Model integration combines separate data models and an AI model into one system. In this
project:

1. **CBS and KVK through the crosswalk.** KVK registers companies in SBI 2025, CBS
   publishes in SBI 2008. `kvk.py` translates each company's main activity through
   `map_sbi2008_sbi2025`, so KVK companies join CBS's sectors; cases the crosswalk can't
   decide unambiguously are flagged (`sbi_mapping`), not guessed.
2. **Absence and business counts.** Gold's risk index joins CBS absence (80072NED) with
   CBS business counts (81589NED) on the sector key, which both CBS tables share (not on the
   SBI code: aggregates have none, and one code can have two keys), so a risk can be weighed by how
   many businesses, and how many self-employed, it concerns
   ([gold's README](../gold/README.md#phase-4--model-design-and-testing-the-risk-index)).
3. **An LLM in the ingestion.** The KVK intake agent uses a local language model to guide
   the user, with human confirmation and deterministic checks around it
   ([bronze's README](../bronze/README.md#design-decisions)).

## Run it

```bash
.venv/bin/python -m pipeline.silver.cbs --preview     # build and summarise, write nothing
.venv/bin/python -m pipeline.silver.cbs
.venv/bin/python -m pipeline.silver.kvk --preview     # what the next run would add or remove
.venv/bin/python -m pipeline.silver.kvk               # needs silver CBS (the crosswalk)
```

## Proof of working

```bash
.venv/bin/pytest tests/test_silver_logic.py tests/test_kvk_silver.py
```

Expected: all tests pass (101 at the time of writing). They cover the parsers (periods,
size classes, sectors), every silver table and its keys, "no padded keys anywhere",
unknown keys kept and flagged, the SBI translation per case, and silver KVK runs (only
the newest upload is read and published; a retired upload is skipped; a failed publish
leaves the live table untouched; an older partitioned table is replaced). Silver built from real Delta bronze tables and read
back with the right types: `tests/test_delta.py`.
