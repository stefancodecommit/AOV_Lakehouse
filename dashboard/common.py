"""
The frame every page shares: the "final figures only" switch, the run check, the
freshness line, the provisional footnote, a table view, and the source credits.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

import data
from tools import lineage  # what every gold column means: the hover help in tables

PROVISIONAL_NOTE = ("Hollow points and periods marked *(provisional)* are provisional CBS "
                    "figures (voorlopig): they can still be revised. Switch on *Final figures "
                    "only* in the sidebar to hide them.")


def final_switch() -> bool:
    """The sidebar switch; its value is kept across pages (session state)."""
    return st.sidebar.toggle("Final figures only", key="final_only",
                             help="Hide provisional CBS figures. Off by default: the newest "
                                  "periods are provisional and shown with hollow markers.")


def load(names: list[str]) -> dict[str, pd.DataFrame]:
    """Load gold tables for a page and warn when they come from different gold runs."""
    tables = {n: data.load_table(n) for n in names}
    missing = [n for n, df in tables.items() if df.empty]
    if missing:
        st.warning(f"Gold table(s) not available yet: {', '.join(missing)}. "
                   "Run the pipeline up to `pipeline/gold/build.py` first (see README).")
    if problem := data.run_problem(tables):
        st.warning(problem)
    return tables


def freshness_line(df: pd.DataFrame, source: str) -> None:
    """One caption line: latest period (and whether it is provisional), data date, run."""
    f = data.freshness(df)
    if f["latest_period"] is None:
        return
    parts = [f"Latest period: **{f['latest_period']}**"
             + (" (provisional)" if f["latest_is_provisional"] else " (final)")]
    if f["data_as_of"] is not None:
        parts.append(f"data loaded {f['data_as_of']:%Y-%m-%d}")
    if "run_id" in df and not df.empty:
        parts.append(f"gold run `{df['run_id'].iloc[0]}`")
    st.caption(" · ".join(parts) + f" · Source: {source}")


def mix_note(df: pd.DataFrame, columns: list[str]) -> None:
    """Note when a shown comparison combines provisional and final figures."""
    flagged = [c for c in columns if c in df and df[c].fillna(False).astype(bool).any()]
    if flagged:
        st.caption("⚠ Some comparisons on this page combine provisional and final figures "
                   "(e.g. this year provisional vs last year final); read them with care.")


# Words in column names shown differently in a table header
_WORDS = {"pct": "%", "pp": "(pp)", "sbi": "SBI", "sbi2008": "SBI 2008", "4q": "4 quarters",
          "95": "95%", "kvk": "KVK", "id": "ID"}


def column_title(name: str) -> str:
    """A column name as a header: 'diff_from_national_pp' -> 'Diff from national (pp)'."""
    words = [_WORDS.get(w, w) for w in name.split("_")]
    title = " ".join(words)
    return title[0].upper() + title[1:]


def column_help(table: str, extra: dict[str, str] | None = None) -> dict[str, str]:
    """Column -> what it means, for a gold table: the descriptions of the column lineage
    (tools/lineage.py, kept complete by a test), plus `extra` for columns a page derives."""
    out = {c: col.how[0].upper() + col.how[1:]
           for c, col in lineage.GOLD_LINEAGE.get(table, {}).items() if col.how}
    return {**out, **(extra or {})}


def table_view(df: pd.DataFrame, table: str | None = None, label: str = "Show the numbers as a table",
               extra: dict[str, str] | None = None) -> None:
    """The same numbers as a table, so nothing is only readable by hovering. With the gold
    `table` it comes from, each column gets a readable title and, on hover, what it means."""
    helps = column_help(table, extra) if table else {}
    config = {c: st.column_config.Column(column_title(c), help=helps.get(c)) for c in df.columns}
    with st.expander(label):
        st.dataframe(df, hide_index=True, width="stretch", column_config=config)


def stop_if_empty(df: pd.DataFrame) -> None:
    """Explain an empty page instead of showing nothing, then stop the page."""
    if df.empty:
        if st.session_state.get("final_only"):
            st.info("No final figures to show here yet: switch off *Final figures only* in "
                    "the sidebar to see the provisional ones.")
        st.stop()


def branding() -> None:
    """The logo at the top of the sidebar, in its dark-mode variant when the viewer uses
    a dark theme. Without a logo file, the project note is shown as text instead."""
    import charts
    from pipeline.common.config import project_path, settings
    cfg = settings.get("dashboard", {})
    logo = cfg.get("logo_dark") if charts.theme_mode() == "dark" else None
    logo = logo if logo and project_path(logo).is_file() else cfg.get("logo")
    if logo and project_path(logo).is_file():
        st.logo(str(project_path(logo)), size="large")
    elif cfg.get("project_note"):
        st.sidebar.caption(cfg["project_note"])


def credits() -> None:
    """Source credits (CC BY 4.0 asks for attribution). Shown in the sidebar, which is
    drawn before the page runs, so they appear on every page, also one that stops early."""
    st.sidebar.divider()
    st.sidebar.caption("Sources: CBS StatLine (80072NED, 86009NED, 81589NED; SBI 2008 - SBI 2025 "
               "crosswalk) and KVK Handelsregister Open Dataset Basis Bedrijfsgegevens, "
               "both licensed [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). "
               "Processed in this project; figures may differ from StatLine by rounding.")
