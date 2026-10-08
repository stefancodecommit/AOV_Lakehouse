"""
Data access for the dashboard: it reads the gold tables only, never silver or bronze.

Caching: each table is cached together with its Delta version. When pipeline/gold/build.py publishes
a new version, the version number changes, the cache misses, and the new data shows on
the next page load, without restarting the dashboard.

Consistency: every gold table carries the run_id of the gold run that built it. If the
tables shown don't share one run_id (a gold publish that stopped halfway), the pages
show a warning instead of silently combining tables from different runs.

The functions without Streamlit in them (filters, freshness, run check) are plain
pandas, so they are tested in tests/test_dashboard.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # project root: pipeline/common/config.py
from pipeline.common.config import project_path, settings  # noqa: E402

GOLD_ROOT = project_path(settings["paths"]["lakehouse"]) / "gold"
GOLD_TABLES = ["gold_absence_by_sector", "gold_absence_by_size", "gold_absence_profile",
               "gold_business_context", "gold_kvk_context"]
FINAL = "Definitief"


# =========================================================================== reading
def table_version(name: str, root: Path | None = None) -> int | None:
    """Current Delta version of a gold table, or None when it doesn't exist (yet)."""
    from deltalake import DeltaTable
    path = (root or GOLD_ROOT) / name
    if not (path / "_delta_log").exists():
        return None
    return DeltaTable(str(path)).version()


def read_table(name: str, version: int | None, root: Path | None = None) -> pd.DataFrame:
    """A gold table as a DataFrame. `version` is part of the cache key (see load_table);
    whole-number columns with empty values stay integers (pandas' nullable types)."""
    from deltalake import DeltaTable
    if version is None:
        return pd.DataFrame()
    df = DeltaTable(str((root or GOLD_ROOT) / name)).to_pandas()
    return df.convert_dtypes(convert_string=False, convert_boolean=True,
                             convert_floating=False)


def load_table(name: str) -> pd.DataFrame:
    """Cached read: the cache key includes the table's Delta version, so a new gold run
    is picked up on the next page load."""
    import streamlit as st

    @st.cache_data(show_spinner=False)
    def _cached(table: str, version: int | None, root: str) -> pd.DataFrame:
        return read_table(table, version, Path(root))

    # the folder is part of the key too: another lakehouse (config, tests) starts at
    # version 0 again and must not get this one's cached data
    return _cached(name, table_version(name), str(GOLD_ROOT))


# =========================================================================== pure helpers
def final_only(df: pd.DataFrame, enabled: bool) -> pd.DataFrame:
    """Rows with final figures only, when the switch is on. Provisional periods are the
    most recent ones, so a final row's comparisons never use provisional figures."""
    if not enabled or df.empty or "is_provisional" not in df:
        return df
    return df[~df["is_provisional"].astype(bool)]


def run_ids(tables: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    """run_id(s) per non-empty table."""
    return {n: sorted(df["run_id"].dropna().unique())
            for n, df in tables.items() if not df.empty and "run_id" in df}


def run_problem(tables: dict[str, pd.DataFrame]) -> str | None:
    """A warning when the tables come from different gold runs, else None."""
    ids = run_ids(tables)
    distinct = {i for v in ids.values() for i in v}
    if len(distinct) <= 1:
        return None
    per_run = {i: sorted(n for n, v in ids.items() if i in v) for i in sorted(distinct)}
    return ("These tables come from different gold runs, so they may not fit together: "
            + "; ".join(f"{i}: {', '.join(n)}" for i, n in per_run.items())
            + ". Run pipeline/gold/build.py again to fix this.")


def freshness(df: pd.DataFrame) -> dict:
    """Facts for the freshness line: newest data date, latest period, and whether the
    latest period is provisional."""
    out = {"data_as_of": None, "latest_period": None, "latest_is_provisional": False}
    if df.empty:
        return out
    if "data_as_of" in df:
        out["data_as_of"] = pd.to_datetime(df["data_as_of"]).max()
    if "period_key" in df:
        latest = df.loc[df["period_key"] == df["period_key"].max()]
        out["latest_period"] = (latest["period_label"].iloc[0] if "period_label" in latest
                                else latest["period_key"].iloc[0])
        if "is_provisional" in latest:
            out["latest_is_provisional"] = bool(latest["is_provisional"].astype(bool).any())
    return out


def latest_period(df: pd.DataFrame) -> str | None:
    """The newest period_key in a table (after filtering), or None when empty."""
    return None if df.empty else df["period_key"].max()


def period_options(df: pd.DataFrame) -> list[tuple[str, str]]:
    """(period_key, label) pairs, newest first, with '(provisional)' on provisional ones."""
    cols = ["period_key", "period_label", "is_provisional"]
    p = df[[c for c in cols if c in df]].drop_duplicates("period_key")
    p = p.sort_values("period_key", ascending=False)
    label = p["period_label"] if "period_label" in p else p["period_key"]
    if "is_provisional" in p:
        label = label.where(~p["is_provisional"].astype(bool), label + " (provisional)")
    return list(zip(p["period_key"], label))


def with_interval_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Add differs_from_national: True when the whole 95% interval lies above or below
    the national value (gold_absence_profile has this column already; the size table
    has the bounds but not the flag)."""
    out = df.copy()
    out["differs_from_national"] = ((out["lower_95"] > out["national_value"])
                                    | (out["upper_95"] < out["national_value"])).fillna(False)
    return out


def rises_with_size(df: pd.DataFrame) -> tuple[int, int]:
    """In how many periods absence strictly rises from the smallest to the largest size
    class (non-aggregate classes, ordered by lower bound): (periods where it does, periods)."""
    rises = total = 0
    for _, g in df.dropna(subset=["value"]).groupby("period_key"):
        v = g.sort_values("lower_bound")["value"].tolist()
        if len(v) < 2:
            continue
        total += 1
        rises += all(a < b for a, b in zip(v, v[1:]))
    return rises, total
