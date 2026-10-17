# Phase 3 — Serving: the dashboard

A Streamlit dashboard for the insurer's **data analyst**. It reads only the gold tables
and turns them into pages that answer the main question: which company characteristics
(sector, size class) go together with higher sickness absence?

```
 lakehouse/gold/ ──► data.py (read + cache) ──► views/*.py (pages) ──► browser, http://localhost:8501
```

## Pages

| Page | File | For the analyst |
|---|---|---|
| Overview | [`views/overview.py`](views/overview.py) | National trend; the highest and lowest sectors; small vs large companies |
| Risk index | [`views/risk.py`](views/risk.py) | Which sectors and size classes are above national, how stable (the band), which are rising; one group explained in a sentence and over time |
| Sectors | [`views/sectors.py`](views/sectors.py) | Sectors ranked against national per SBI level; one sector over time |
| Size classes | [`views/sizes.py`](views/sizes.py) | Absence per size class; the survey's share absent with 95% intervals |
| Behind the numbers | [`views/profile.py`](views/profile.py) | Which survey measures (frequency, duration, work-related causes) differ from national beyond the margin of error |
| Context | [`views/context.py`](views/context.py) | Businesses per sector and size band (CBS); active BVs/NVs and company age (KVK) |
| Data dictionary | [`views/dictionary.py`](views/dictionary.py) | Every gold table and column, read from the tables |

## Design decisions

- **Gold only.** The dashboard never reads silver or bronze, so what it shows is always a
  published, checked gold run. A new gold run shows up on the next page load: the cache
  is keyed on each table's Delta version ([`data.py`](data.py)).
- **Provisional figures are visible but marked.** Hollow points and *(provisional)*
  labels; one sidebar switch, *Final figures only*, applies to every page
  ([`common.py`](common.py)).
- **Every figure has its context:** the latest period and whether it is provisional, the
  date the data was loaded, the gold run, and the source credits (CC BY 4.0).
- **Charts with a purpose** ([`charts.py`](charts.py)): red above national, blue below,
  grey for context; colours validated for colour-blind readers, with dark-mode variants;
  every chart also as a table, so no number is reachable only by hovering.
- **Warnings instead of wrong pictures:** a page says so when gold is missing or its
  tables come from different gold runs.

## Run it

```bash
.venv/bin/streamlit run dashboard/app.py               # http://localhost:8501
```

In Docker it runs as the `dashboard` service (`docker compose up -d`), reachable from
this machine only.

## Proof of working

```bash
.venv/bin/pytest tests/test_dashboard.py
```

Expected: all tests pass (25 at the time of writing). Every page is run headless
(Streamlit's AppTest) on gold tables built end to end from the sample data, with the
*Final figures only* switch off and on; plus the data helpers, the warnings, the
overview's latest quarter, and the risk page's content (counts, the rising groups, the
explanation sentence, the final-figures variant).
