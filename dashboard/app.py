"""
Streamlit dashboard: which company characteristics (sector, size class) go together
with higher sickness absence?

Run from the project folder:
    .venv/bin/streamlit run dashboard/app.py

It reads only the gold tables (lakehouse/gold/, built by gold.py). Pages live in
dashboard/views/; the shared frame (switch, freshness, credits) in common.py, chart
styles in charts.py, data access and caching in data.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))   # dashboard modules

import common  # noqa: E402

st.set_page_config(page_title="Sickness absence by company characteristics", page_icon="📊",
                   layout="wide")

VIEWS = Path(__file__).resolve().parent / "views"
pages = st.navigation([
    st.Page(VIEWS / "overview.py", title="Overview", default=True),
    st.Page(VIEWS / "sectors.py", title="Sectors"),
    st.Page(VIEWS / "sizes.py", title="Size classes"),
    st.Page(VIEWS / "profile.py", title="Behind the numbers"),
    st.Page(VIEWS / "context.py", title="Context"),
    st.Page(VIEWS / "dictionary.py", title="Data dictionary"),
])
common.branding()             # logo (when configured) and project note
common.final_switch()         # one switch in the sidebar, shared by every page
common.credits()              # source credits in the sidebar of every page
pages.run()
