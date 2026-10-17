# Use of generative AI in this project

How and why generative AI (GenAI) was used to build this project, who decided what, how
the results were checked, and what the advantages and risks were, with real examples.
The course asks for this explicitly (*"Altijd weergeven waarom en hoe je GenAI gebruikt
hebt; geef daarbij de voor- en nadelen (risico's) van GenAI weer"*).

Two different uses of AI appear in this project; this document is about the first:

1. **GenAI as a development assistant:** to design, write, test and document the project.
2. **AI inside the product:** the KVK intake agent uses a local language model (Ollama,
   `qwen2.5:7b`) to guide a person through loading a file. That is part of the design and
   is described in [pipeline/bronze/README.md](../pipeline/bronze/README.md) and in the risk
   register ([privacy_and_risks.md](privacy_and_risks.md), R17).

## 1. Which tools

| Tool | Used for |
|---|---|
| **Claude** (Anthropic), in the claude.ai chat | the starting point: a first project description (`PROJECT.md`) and a task list (`TODO.md`), imported into the repository on 2026-10-05 |
| **Claude Code** (Anthropic), in VS Code, model Claude Opus | the rest: code, tests, documentation, analyses, measurements, git commits. Sessions from 2026-10-05 to 2026-10-17 |

Transparency in the history: **every commit except one** carries the line
`Co-Authored-By: Claude Opus 5.5`; the one without is GitHub's own placeholder commit.

## 2. Why

- **Scope within the time available.** A complete pipeline (three sources, three layers,
  orchestration, deployment, a dashboard, 373 tests, CI) in about two weeks. Without an
  assistant the project would have stopped at a smaller scope.
- **Learning.** Many questions were "how does this work?" rather than "build this": what
  `python -m` does, how Delta Lake decides which Parquet files are current, what GitHub
  Actions actually ran. The assistant explains, and shows it on the real project.
- **Breadth of tools.** Dagster, Docker Compose, Delta Lake, Streamlit, Azure: the
  assistant knows their details, which lowered the cost of trying the right tool.
- **Discipline that is otherwise skipped:** tests for every part, documentation per
  phase, measurements before claims.

## 3. How: the way of working

The author set the direction and made the decisions; the assistant proposed options with
their trade-offs, built what was chosen, tested it and reported back. Concretely:

1. **The author asks or proposes** a next step, usually from the TODO list or from a
   question of their own.
2. **The assistant investigates first** (reads the code, measures, checks the data), then
   proposes, often as options with a recommendation.
3. **The author decides.** Larger choices were asked explicitly, e.g. through a choice
   menu.
4. **The assistant builds and verifies:** tests, real-data runs, checks of its own claims.
5. **The author reviews and commits** (the assistant commits only when asked) and **pushes
   to GitHub themselves** (a working agreement: the author decides what becomes public).

### Decisions and ideas that came from the author

From the session record, the following directions came from the author, not from the
assistant:

| Topic | The author's decision or idea |
|---|---|
| Data | Add the SBI 2008 → 2025 crosswalk (the author found the CBS file and asked for it to be loaded, without duplicates) |
| Data | Use KVK's *main* activity (`Hoofdactiviteiten`) as a company's sector |
| Data quality | **Flag edge cases instead of making assumptions or changing data** |
| Data | Keep provisional CBS figures, clearly marked, with a "final figures only" switch |
| Architecture | A run log with a `run_id` that silver tables can be joined to |
| Architecture | No SCD type 2 in silver (after weighing it) |
| Architecture | Bronze should hold structured columns instead of only JSON ("a Raw to Bronze layer"); chosen: structured bronze while keeping `raw_json` |
| Architecture | The folder reorganisation ("better now than later") |
| Scalability | The question whether pandas would scale, or PySpark/Polars would be better; then the question *how* the latest KVK snapshot is determined, which led to the incremental design |
| Transparency | Column lineage from bronze to silver, a visual version, then an interactive explorer, then extended to gold |
| Tools | A viewer for the Delta tables, then data-quality statistics in it |
| Documentation | Comment the code more; a README for the teacher; an instructions file with the commands to run |
| Operations | Monthly CBS schedule; alerts in the Dagster UI only; KVK retention (latest + one per month) |
| Organisation | The organisation is an income-protection insurer; its own benchmarks use privacy-sensitive data, so CBS is the second-best; the chain *data analyst → manager → risk management / account managers* |
| Assignment | The report and executive summary in Dutch, everything else in English; leave alerts (FR-19) and the default password (NFR-15) open for now |
| Review | **"How did you test that?"** — the question that exposed a wrongly ticked CI item (see section 5) |

### What the assistant did

Designed the code structure within those decisions, wrote all Python code, tests and
documentation, ran the pipeline on the real data, measured performance and memory,
checked claims against the data and the sources, and wrote the analyses
(requirements, cloud design, privacy and risks) for the author to review.

## 4. How the results were checked

GenAI output was not accepted on trust. The checks used:

| Check | Example |
|---|---|
| **Automated tests** (373), on every change and in GitHub Actions on a clean machine | the tests run before every commit; CI found that a fresh install got pandas 3, which the code didn't support (fixed by pinning versions) |
| **Tests on purpose-broken code** (mutation checks): does a test actually catch the error? | breaking the `is_latest` flag, the delete-before-write, the dashboard's network setting: each time the right test failed |
| **Comparison with the real data** before and after a change | silver identical after restructuring bronze (11 tables); KVK identical after the incremental redesign (1,921,911 rows) |
| **Measurements instead of assumptions** | memory of silver KVK measured for 1 to 4 snapshots; the first design turned out to grow with each snapshot, and was improved until it didn't |
| **Claims checked against sources** | that CBS's absence tables describe employees, checked in CBS's own table descriptions (and an overstated claim corrected) |
| **Mechanical checks of documentation** | every test cited in the requirements exists (50 of 50); every link and anchor works |
| **The author's review and questions** | see the example in section 5 |

## 5. Advantages and risks, with real examples

### Advantages

- **Speed and scope:** far more was built, tested and documented than would otherwise fit
  in the time.
- **Explanations on demand:** concepts explained on the project's own code and data.
- **Consistency:** naming, documentation and test style stay the same across the 68
  Python and Markdown files.
- **Thoroughness on request:** measuring, verifying and documenting is cheap with an
  assistant, so it gets done.

### Risks, and what happened

These happened during the project; each was caught and corrected:

| What happened | Risk it shows | Caught by |
|---|---|---|
| The assistant ticked the GitHub Actions items in the TODO as done, although the workflow had never run (there was no GitHub repository yet) | **Overclaiming:** GenAI states things with confidence that it hasn't verified | the author's question *"How did you test that?"*; the status was corrected, and CI was later verified on GitHub |
| The assistant described a "Column lineage tab" in the free Dagster UI that doesn't exist there (the lineage graph is a paid feature) | **Hallucination** of product features | corrected in the same session; the lineage was shown in the free UI's column descriptions instead |
| Early tests wrote into the real lakehouse | **Unsafe test design** | the test data was removed and a safety net added in `tests/conftest.py` |
| A later test undid that safety net (`monkeypatch.undo()`) and pointed at the real lakehouse; it failed before writing anything | **Subtle errors in generated code** | the assistant noticed; the safety net was then made impossible to undo and checked after every test |
| A claim that the Docker image "would crash" on pandas 3 turned out wrong when tested on the real data | **Plausible but untested conclusions** | testing it; corrected in the same session |
| The assistant cleaned up all unused Docker images on the machine, not just this project's, without asking | **Actions beyond the request** | reported by the assistant itself; scoped to the project afterwards |
| A command meant to stop a test server stopped the wrong process (twice) | **Errors in routine commands** | checking the result; stopped by process number |

General risks, beyond these incidents:

- **Understanding.** Code the author didn't write line by line must still be understood
  and explained (the presentation includes a demo). Mitigation: explanations asked during
  the work, the README per phase, and this project's documentation of *why*.
- **Confidentiality.** Prompts and code are sent to the AI provider. Mitigation: only
  public data and the project's own code were shared; **no data from the insurer was
  used or shared**. For work with the insurer's own data, the insurer's policy on GenAI
  tools would apply.
- **Reliability of facts.** GenAI can be confidently wrong (see above). Mitigation: tests,
  measurements, checks against sources, and asking "how do you know?".
- **Over-reliance and skills.** Letting the assistant decide would weaken the author's
  own judgement. Mitigation: the decisions in section 3 stayed with the author.
- **Licensing and ownership** of generated code: the code uses common open-source
  libraries under their licences; the generated code is part of the author's submission,
  with the assistant credited in every commit.

## 6. Reflection by the author

*To be written by the author: what GenAI meant for your learning and your result, what
you would do the same or differently next time, and where you think the line lies
between your work and the assistant's.*
