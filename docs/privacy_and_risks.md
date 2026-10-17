# Privacy (GDPR) and risks

Which data the project processes, whether any of it is personal data, how the GDPR
principles are met, which other rules apply, and which risks remain, with the measures
taken and what is still open. Written for the insurer's privacy officer, compliance and
data team, and for review (course learning objective 6 and assessment criterion 7).

This is an analysis by the project, not legal advice. Where a decision belongs to the
privacy officer or compliance, it says so.

## Contents

1. [Summary](#1-summary)
2. [Data inventory](#2-data-inventory)
3. [GDPR assessment](#3-gdpr-assessment)
4. [Other rules and agreements](#4-other-rules-and-agreements)
5. [Risk register](#5-risk-register)
6. [Value against risk](#6-value-against-risk)
7. [Actions](#7-actions)

---

## 1. Summary

- **No personal data about insured persons or the public.** The lakehouse holds public
  statistics (CBS) and the KVK open dataset of legal entities (BV, NV). The insurer's
  own data, which does contain health data, is deliberately not used.
- **Two small, known exceptions,** both about the people who run the system: the KVK
  upload log records the operating-system user who confirmed a load, and the git history
  shows the author's name and email (by the author's choice).
- **The main privacy risk is re-identification of companies,** not people: 851,804 of the
  1.92 million KVK companies are unique on postcode region, SBI code and start date. The
  dashboard only shows aggregates with groups under 10 hidden; the row-level tables must
  stay restricted to the data team.
- **The main future risk is linking these figures to claims data.** That would be health
  data (GDPR art. 9) and requires a DPIA before anything is built.
- **The main business risk is misuse of the figures:** reading a group's risk as an
  individual employer's, or a correlation as a cause, or using sector in pricing in a way
  that indirectly disadvantages a protected group. Compliance should assess the latter
  before the figures feed into premiums.

## 2. Data inventory

What is stored, where, and whether it is personal data. Checked against the real
lakehouse on 2026-10-17.

| Data | Where | Contents | Personal data? | Classification |
|---|---|---|---|---|
| CBS absence and business statistics | bronze, silver, gold (`80072ned_*`, `86009ned_*`, `81589ned_*`, …) | aggregated figures per sector, size class, region and period | **No:** aggregates published by CBS, which applies its own disclosure control | public |
| SBI crosswalk | `bronze/reference/`, `silver/map_sbi2008_sbi2025` | code-to-code mapping | No | public |
| KVK companies | `bronze/kvk/`, `silver/kvk_company` | per company: start date, active, insolvency code, legal form (BV 1,916,604; NV 5,307), 2-digit postcode region, SBI codes. No names, addresses or KVK numbers | **Not in itself:** data about legal entities. **But** a one-person BV is closely tied to its director, and 851,804 companies are unique on (postcode region, SBI code, start date), so linking with other sources could identify a company and through it a person | public source, but treat the row-level tables as **internal** |
| KVK aggregates | `gold/gold_kvk_context` | counts and median age per section and legal form; groups under 10 flagged (10 of 64) and hidden by default | No | public |
| KVK upload log | `bronze/kvk/_batch_log` | per upload: file, hash, row count, verification report, **`confirmed_by`** (the operating-system user, e.g. `stefan`); the report contains local file paths including the home folder | **Yes, minimal:** identifies the employee who confirmed a load | internal |
| Run log | `lakehouse/_run_log` | per run: pipeline, times, status, input versions, code version | No | internal |
| Quarantine | `lakehouse/quarantine/` | failed batches with a report | No (same data as above) | internal |
| Dagster run history | `orchestration/` (local) or PostgreSQL (Docker) | runs, events, logs | **Minimal:** may contain usernames and paths | internal |
| Git history (GitHub, public) | the repository | code and documentation; author name and email in every commit | **Yes:** the author's, published by the author's own choice | public |

Not stored anywhere: the insurer's policy, claims or health data; dashboard visitors
(the dashboard has no login and keeps no access log; usage statistics to Streamlit are
switched off in `.streamlit/config.toml`).

## 3. GDPR assessment

**Does the GDPR apply?** For the statistics and the KVK aggregates: no, they aren't
personal data. It does apply, in a small way, to the KVK upload log (an employee's
username) and possibly to row-level KVK data where a company can be traced to a person.
If the insurer runs this in production, the insurer is the controller for those.

**The principles (art. 5), and how the project meets them:**

| Principle | How |
|---|---|
| Lawfulness, fairness, transparency | Public sources under CC BY 4.0, credited on every dashboard page. The `confirmed_by` field serves accountability for what was loaded (a legitimate interest of the insurer); it should be mentioned in the data team's internal privacy notice |
| Purpose limitation | The figures serve one purpose: benchmarking absence risk per sector and size class. Using row-level KVK data for anything aimed at individual companies is outside that purpose and against KVK's terms |
| Data minimisation | Silver KVK reads only the columns it uses (not the full list of activities, not the source file names); gold keeps aggregates only. Bronze keeps the file as received, to prove what was loaded |
| Accuracy | Checks before writing, quarantine, blocking layer checks, provisional figures marked, revised periods replacing old ones |
| Storage limitation | Configured retention: quarantine 90 days, KVK snapshots the latest plus one per month, old Delta file versions 7 days (`config.yaml`, `tools/maintenance.py`). **Gap:** no retention for the KVK upload log and Dagster's run history (see actions) |
| Integrity and confidentiality | Local only (127.0.0.1), no secrets, containers without root rights; in the cloud design, Entra ID login, roles, private endpoints, encryption ([cloud_design.md](cloud_design.md#6-security-and-privacy)) |
| Accountability | Run log, column lineage, requirements with verification, this document |

**Is a DPIA needed?** Not for the current scope: no special categories, no large-scale
processing of personal data, no monitoring of people. **A DPIA is required before** the
figures are combined with the insurer's own policy or claims data: that is health data
(art. 9), on a large scale, used to assess people, which are criteria for a mandatory
DPIA (art. 35). Such a link belongs in a separate, more strictly secured environment.

**Transfers outside the EU:** none. Locally everything stays on the machine; the cloud
design uses region West Europe (Netherlands). The external services called are CBS's
open-data API and the public download pages of CBS and KVK, and only data is fetched from
them, nothing is sent. The optional LLM (Ollama) runs on the machine itself.

## 4. Other rules and agreements

| Rule | Applies how | Status |
|---|---|---|
| **KVK open data terms** | Using the data to trace individual companies or people is not allowed | Gold aggregates only, small groups hidden; row-level tables internal |
| **CC BY 4.0** (CBS, KVK) | Credit the source | On every dashboard page (tested) |
| **EU AI Act** | The risk index is fixed arithmetic set by people, with no learning or inference, so it is unlikely to count as an *AI system*. The KVK intake agent does use an LLM, but only to guide a person, who confirms every load. Note: AI used for risk assessment and pricing of **natural persons** in life and health insurance is a high-risk use (Annex III). If the insurer ever adds machine learning that scores individuals, that assessment changes | To be confirmed by compliance; no high-risk use now |
| **Equal treatment in insurance** | Pricing may not use gender (EU case law, *Test-Achats*). Sector is allowed, but sectors differ strongly in their workforce (e.g. healthcare employs mostly women), so pricing by sector could indirectly disadvantage a protected group | **For compliance to assess** before the risk index feeds into premiums |
| **Insurance supervision** (governance of data and models) | Figures used in pricing need documented data lineage, validation and human oversight | Lineage, tests, white-box model, human in command are in place; formal model approval is the insurer's process |

## 5. Risk register

Likelihood and impact: low / medium / high. *Residual* is the risk after the measures
already in place.

### Data and model

| ID | Risk | Likelihood | Impact | Measures in place | Residual | Next step |
|---|---|---|---|---|---|---|
| R1 | A provisional CBS figure is revised and a conclusion turns out wrong | high (the newest quarters are always provisional) | medium | provisional figures marked everywhere; *Final figures only* switch; revisions replace old rows | low | — |
| R2 | CBS stops or renumbers a table, or publishes a new crosswalk | medium (happened: 83130NED in 2023) | medium | lifecycle and freshness warnings; checks and quarantine; configuration in one file | medium | a replacement table must still be configured by hand |
| R3 | A group's figure is used to judge an individual employer or person (ecological fallacy) | medium | high | conditions for use; explanation sentences speak of groups; human in command | medium | brief the analysts and account managers on the conditions for use |
| R4 | Correlation read as cause ("this sector *causes* absence") | medium | medium | stated on the dashboard and in the documentation | low | — |
| R5 | Employees' figures applied to self-employed products without care | medium | medium | stated on the risk page and in PROJECT.md | medium | for self-employed products, combine with the insurer's own experience |
| R6 | The risk index band is read as a statistical margin of error | medium | low | explained on the page and in the documentation; survey figures with real 95% intervals elsewhere | low | — |

### Privacy, legal and ethics

| ID | Risk | Likelihood | Impact | Measures in place | Residual | Next step |
|---|---|---|---|---|---|---|
| R7 | A company (and through it, its director) is re-identified from row-level KVK data | low | medium | dashboard shows aggregates only; groups under 10 hidden; only columns needed are kept in silver | low | keep `silver/kvk_company` and bronze KVK restricted to the data team (cloud design: roles) |
| R8 | The figures are linked to claims or health data without a legal basis | low now | high | out of scope; stated as a condition for use (OR-5) | low | DPIA and the privacy officer's approval before any such link |
| R9 | Pricing by sector indirectly disadvantages a protected group | medium (if used in pricing) | high | figures are a benchmark, decisions are human | medium | compliance review before use in premiums |
| R10 | Personal data in logs (username in the KVK upload log, paths in reports, Dagster history) kept indefinitely | high (it is there) | low | minimal data; internal only | low | add retention for these logs; mention in the internal privacy notice |

### Security and operations

| ID | Risk | Likelihood | Impact | Measures in place | Residual | Next step |
|---|---|---|---|---|---|---|
| R11 | Unauthorised access to the dashboard or the Dagster UI | low (local) | medium | everything listens on 127.0.0.1; containers without root; no secrets; cloud design with Entra ID | low | — |
| R12 | Default password of Dagster's run-history database (NFR-15) | low | low (no business data in it) | local only; open on purpose for the prototype | low | Key Vault when deployed (cloud design) |
| R13 | A vulnerable or compromised Python package | medium | medium | exact versions pinned; CI on every change | medium | add automatic vulnerability scanning (e.g. GitHub Dependabot or `pip-audit` in CI) |
| R14 | Data loss: the lakehouse and inbox are not in git and not backed up; old KVK snapshots can't be downloaded again (the KVK download link serves the current file only) | low | high (KVK history lost) | Delta keeps old versions for 7 days; CBS data can be fetched again | medium | back up `lakehouse/` and `inbox/kvk/`; in the cloud, zone-redundant storage with soft delete |
| R15 | Failures go unnoticed: no email or chat alerts (FR-19) | medium | medium | failures and freshness warnings in the Dagster UI; `inspect_lakehouse` exits with an error | medium | someone checks the Dagster UI regularly; alerts in the cloud design |
| R16 | Dependence on one maintainer | high (now one person) | medium | README, INSTRUCTIONS, a README per phase, requirements, tests and CI | medium | hand-over to the insurer's data team |

### AI

| ID | Risk | Likelihood | Impact | Measures in place | Residual | Next step |
|---|---|---|---|---|---|---|
| R17 | The LLM in the KVK intake misunderstands or proposes the wrong file | medium | low | the LLM can't load anything itself: files are verified by ordinary code and loaded only after a person types `YES`; the Dagster route doesn't use the LLM at all | low | — |
| R18 | Errors in code written with GenAI assistance | medium | medium | 373 tests, CI, mutation checks, real-data comparisons, review by the author | medium | described in the GenAI section of the report |

## 6. Value against risk

The project gives the insurer an independent, privacy-free benchmark of absence risk per
sector and size class, kept up to date automatically and traceable to its sources. The
risks that remain are mostly about **how the figures are used**, not about the data
itself: reading group figures as individual ones (R3), employees' figures for the
self-employed (R5), and indirect effects in pricing (R9). These are handled by people and
processes (briefing, compliance review), not by more code. The technical risks with a
real impact are a missing backup of the KVK history (R14) and the absence of vulnerability
scanning (R13), both straightforward to fix.

## 7. Actions

| Action | For risk | Who | Status |
|---|---|---|---|
| Brief analysts and account managers on the conditions for use | R3, R5 | manager of the analyst | open |
| Compliance review of sector-based pricing | R9 | compliance | open, before use in premiums |
| DPIA before any link with internal data | R8 | privacy officer | condition, not planned |
| Retention for the KVK upload log and Dagster's history; mention `confirmed_by` in the internal privacy notice | R10 | data team | open |
| Restrict row-level KVK tables to the data team | R7 | data team | in the cloud design (roles) |
| Vulnerability scanning of dependencies in CI | R13 | data team | open |
| Back up `lakehouse/` and `inbox/kvk/` | R14 | data team | open |
| Alerts by email or chat | R15 | data team | in the cloud design (FR-19) |
