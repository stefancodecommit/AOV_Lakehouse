# Phase 3 — Serving (gold) and Phase 4 — Model design and testing

Gold shapes silver into the tables the dashboard reads, with the comparisons already
calculated. It also holds the project's quantitative model, the **risk index** for the
insurer's data analyst.

```
 silver (dimensions, facts, kvk_company) ──► build.py ──► 7 gold tables ──► dashboard/
```

## Phase 3 — Serving: the gold tables

| Table | Answers |
|---|---|
| `gold_risk_index` | Per sector and size class: how much higher or lower is absence than national, how stable, is it rising, how many businesses does it concern? (see below) |
| `gold_risk_index_history` | The quarterly relative risk behind the index |
| `gold_absence_by_sector` | Absence per sector against national: difference, rank, change vs a year earlier, 4-quarter average |
| `gold_absence_by_size` | Absence per size class, quarterly (80072NED) and from the yearly survey (86009NED), side by side |
| `gold_absence_profile` | What lies behind absence: frequency, duration, work-related causes, with 95% intervals and whether a value differs from national |
| `gold_business_context` | Businesses per sector and size band |
| `gold_kvk_context` | Active companies and median company age per sector (latest KVK snapshot); groups under 10 hidden |

Every row says whether its period is provisional and when the data behind it was loaded
(`data_as_of`). Gold publishes like silver: all tables or none, each with its `run_id`,
and it refuses to build from silver CBS tables of different runs.

## Phase 4 — Model design and testing: the risk index

**Purpose.** Help the data analyst find the SBI sectors and size classes with a higher
absence risk than national, judge how certain that is, and see where it is rising. The
insurer's own benchmarks use privacy-sensitive data; this is the public, privacy-free
complement.

**The model** (`build_risk_index` in [`build.py`](build.py)):

| Output | Calculation |
|---|---|
| relative risk | the group's 4-quarter average absence ÷ the national 4-quarter average; 1.00 = national. Four quarters, so winter peaks don't distort it |
| band | the lowest and highest *quarterly* relative risk of the last 8 quarters |
| assessment | band wholly above 1 → *higher*; wholly below → *lower*; otherwise *around national*; fewer than 4 quarters → *too few quarters* |
| trend | slope of a straight line through the 8 quarterly relative risks, per year; beyond ±0.05 → *rising* / *falling*, else *stable*. Dividing by the national figure of the same quarter removes the season |
| size of the risk | businesses in the sector, of which run by natural persons (the self-employed) and with one person employed (CBS 81589NED) |
| explanation | one sentence that puts the row in words |

Settings (window, minimum quarters, trend threshold) are in [`config.yaml`](../../config.yaml)
under `gold.risk_index`. Two variants per group: all figures, and final figures only.

**Why this model, and not machine learning.** The question is descriptive ("where is the
risk higher, and is it rising?"), the data is small (26 quarters, ~38 groups), and the
users must be able to explain every number to a manager. A white-box model of simple
arithmetic fits that better than a trained model: every figure can be redone by hand,
and the analyst, not the model, draws the conclusion (human in command).

**Assumptions and limits.**
- CBS publishes no margin of error for 80072NED, so the band shows *stability* over 8
  quarters, not statistical certainty. The survey (86009NED) does have 95% intervals; the
  dashboard page *Behind the numbers* uses those.
- The figures describe employees; for self-employed products they are a proxy.
- Correlation, not cause. Sector and size are published separately, never combined.

**Validation.**
- Tests on made-up data where the right answer is known in advance: relative risk and
  band, the assessment rule, the window, a trend with and without a season, the final
  basis, the business counts and the explanation sentence.
- On the real data: healthcare's 4-quarter average (7.68%) equals the one in
  `gold_absence_by_sector`; the relative risk lies inside its band for every group; the
  ranking matches the published sector differences (healthcare 1.40 highest, hospitality
  0.61 lowest; size 1–9 at 0.48, 100+ at 1.16).

All 7 tables' columns and their silver origin: [docs/lineage.md](../../docs/lineage.md).

## Run it

```bash
.venv/bin/python -m pipeline.gold.build --preview     # build and summarise, write nothing
.venv/bin/python -m pipeline.gold.build               # needs silver CBS (and KVK, optional)
```

## Proof of working

```bash
.venv/bin/pytest tests/test_gold.py
```

Expected: all tests pass (20 at the time of writing): the comparisons (year on year, the
4-quarter average with gaps, provisional figures flagged), every gold table on the
sample data, the risk index tests above, and a full run from bronze to gold. The risk
page as the analyst sees it: `test_risk_page_shows_the_analyst_what_is_above_national_and_rising`
in `tests/test_dashboard.py`.
