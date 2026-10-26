# Phase 2 — Transformation (silver)

Turns bronze into clean, typed tables that can be joined: every CBS code gets its
meaning, every figure its type, and the sources are linked. Silver is the single place
where data is interpreted; gold and the dashboard only combine what silver provides.

```
 bronze CBS tables + crosswalk ──► cbs.py ──► 11 tables: dimensions, facts, maps
 bronze KVK + silver crosswalk ──► kvk.py ──► kvk_company (one row per company per snapshot)
```

## What it builds

| Table | Kind | What |
|---|---|---|
| `dim_period`, `dim_sector`, `dim_company_size`, `dim_region`, `dim_survey_measure` | dimensions | one row per period, SBI sector (every level), size class, region, survey measure |
| `map_characteristic` | map | what each CBS company-characteristic key means: a sector, a size class, the total |
| `map_sbi2008_sbi2025` | map | the crosswalk between the two SBI versions (many-to-many) |
| `fct_absence_rate` | fact | quarterly absence % per sector and size class (80072NED) |
| `fct_absence_survey` | fact | yearly survey measures with 95% bounds (86009NED, 83130NED) |
| `fct_business_count`, `fct_establishment_count` | facts | businesses per sector, size and legal form; establishments per region |
| `kvk_company` | fact | KVK companies with their sector, translated to SBI 2008 |

Every column, where it comes from in bronze and what happens to it on the way:
[docs/bronze_to_silver.md](../../docs/bronze_to_silver.md) (explained with real examples)
and [docs/lineage.md](../../docs/lineage.md) (generated, complete).

## Design decisions

- **Flag, don't guess.** An unknown key, a KVK company with two main activities, a code
  that exists in both SBI versions: the original value is kept and a flag column says
  what is special, instead of picking one. Gold and the dashboard can filter on flags.
- **Strip CBS's padding once** (`'301000 '` → `'301000'`), so every join just works.
- **Publish all or nothing.** Silver CBS writes all its tables to a staging folder first
  and replaces the live tables only when all succeeded
  ([`pipeline/common/publish.py`](../common/publish.py)). Every row carries the `run_id`
  of its run, which joins to the run log.
- **KVK incrementally.** Each kept KVK upload is a snapshot of ~1.9M rows. A run adds only
  new snapshots and removes the ones maintenance retired, one at a time, so memory stays
  at about one snapshot however many months are kept (measured: 1.65 GB for a full
  rebuild of 4 snapshots, 0.18 GB when nothing is new).

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

Expected: all tests pass (116 at the time of writing). They cover the parsers (periods,
size classes, sectors), every silver table and its keys, "no padded keys anywhere",
unknown keys kept and flagged, the SBI translation per case, and the incremental KVK
runs (a new upload, nothing new, a retired upload, a crash halfway, a changed crosswalk;
each compared with a full rebuild). Silver built from real Delta bronze tables and read
back with the right types: `tests/test_delta.py`.
