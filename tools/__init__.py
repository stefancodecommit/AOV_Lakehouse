"""Tools you run by hand (some are also scheduled by Dagster), not steps of the pipeline:
- inspect_lakehouse: health check of the whole lakehouse (terminal; exit code 1 on problems)
- lakehouse_viewer: look inside one table, with data-quality statistics (browser)
- maintenance: monthly clean-up (quarantine, KVK retention, logs, Delta vacuum)
- backup: verified backup of lakehouse/ and inbox/kvk/, and restore
- lineage: the column lineage, and the docs generated from it
- backtest_risk: backtest of the risk index
- demo: starts everything for the live demo, with a start page
- export_report: the report (docs/rapport.md) to Word and PDF

Run from the project folder, e.g. `python -m tools.inspect_lakehouse`.
"""
