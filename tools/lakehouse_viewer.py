"""
Lakehouse viewer: look at any Delta table (bronze, silver, gold, run log) in the browser,
with data quality statistics.

Pick a layer and a table in the sidebar, then a view:
- Rows: the rows, with a choice of columns and a text filter that searches one column
  (case-insensitive) in the whole table, not only in the rows shown.
- Data quality: per column how complete it is (missing values, blank text), text with
  leading/trailing spaces (normal in bronze, should be 0 in silver), distinct values,
  min/max and the most common value; true counts of the flag columns (is_*, has_*);
  duplicate rows; and duplicate keys for tables with a declared key (checks/layers.py).
  Computed on the whole table when the view is opened, then cached per table version.
- History: one line per write to the table.

Any earlier version of a table can be opened (Delta time travel), as long as maintenance
hasn't vacuumed its files yet. Tables are read through the Delta log, so you see exactly
the files of the chosen version. The viewer only reads, never writes.

Usage
-----
    .venv/bin/python -m tools.lakehouse_viewer  # opens http://localhost:8502
(the dashboard uses 8501, so both can run at once; only reachable from this machine)
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

# `streamlit run` puts only this file's folder on the path; the project root has the
# pipeline and checks packages
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from checks.layers import GOLD_KEYS, SILVER_CBS_KEYS  # noqa: E402
from pipeline.silver import cbs as cbs_silver  # noqa: E402

PORT = 8502
LAYERS = ["bronze", "silver", "gold", "other"]
# Columns that identify a row, per table (silver/gold from the layer checks). Bronze
# keys depend on the source and are recognised by their columns (bronze_key).
KEYS = {**SILVER_CBS_KEYS, **GOLD_KEYS, "kvk_company": ["batch_id", "source_row_number"]}
# Left out when looking for duplicate rows: lineage and load bookkeeping, which differ
# between two otherwise identical records. _batch_id stays: KVK duplicates count per upload.
NOT_CONTENT = {"raw_json", "run_id", "source_row_number", "data_as_of"}


# =========================================================================== finding tables
def lakehouse_tables(root: Path) -> dict[str, list[str]]:
    """Every Delta table under the lakehouse, per layer, as a path relative to the root
    (e.g. 'bronze/kvk/basis_bedrijfsgegevens'). Tables outside the three layers, like
    the run log, are under 'other'."""
    out: dict[str, list[str]] = {layer: [] for layer in LAYERS}
    for log in root.rglob("_delta_log"):
        rel = log.parent.relative_to(root)
        out[rel.parts[0] if rel.parts[0] in LAYERS[:3] else "other"].append(str(rel))
    return {layer: sorted(names) for layer, names in out.items()}


def read(table_path: Path, version: int | None, columns: list[str] | None,
         column: str | None, text: str, limit: int) -> tuple[pa.Table, int]:
    """The first `limit` rows (optionally only those whose `column` contains `text`), and
    how many rows match in total."""
    from deltalake import DeltaTable
    ds = DeltaTable(str(table_path), version=version).to_pyarrow_dataset()
    where = None
    if column and text:
        where = pc.match_substring(pc.field(column).cast(pa.string()), text, ignore_case=True)
    return ds.head(limit, columns=columns, filter=where), ds.count_rows(filter=where)


# =========================================================================== data quality
def _is_text(t: pa.DataType) -> bool:
    return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t)


def column_profile(col: pa.ChunkedArray) -> dict:
    """Quality statistics of one column."""
    n, t = len(col), col.type
    if _is_text(t):
        col = col.cast(pa.large_string())
    nulls = col.null_count
    out = {"type": str(t), "missing": nulls, "blank_text": None, "padded_text": None,
           "true": None, "min": None, "max": None}
    if _is_text(t):
        trimmed = pc.utf8_trim_whitespace(col)
        out["blank_text"] = pc.sum(pc.equal(trimmed, "")).as_py() or 0
        out["padded_text"] = pc.sum(pc.not_equal(trimmed, col)).as_py() or 0
    elif pa.types.is_boolean(t):
        out["true"] = pc.sum(col).as_py() or 0
    elif (pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_temporal(t)) \
            and nulls < n:
        mm = pc.min_max(col)
        out["min"], out["max"] = str(mm["min"].as_py()), str(mm["max"].as_py())
    out["filled_pct"] = 100 * (n - nulls - (out["blank_text"] or 0)) / n if n else 0.0
    out["distinct"] = pc.count_distinct(col).as_py()
    counts = pc.value_counts(col.drop_null()) if nulls < n else None
    if counts is not None and len(counts):
        top = counts[pc.index(counts.field("counts"), pc.max(counts.field("counts"))).as_py()]
        value = str(top["values"].as_py())
        out["most_common"] = (value[:40] + "…" if len(value) > 40 else value) \
            + f"  ({top['counts'].as_py():,}×)"
    else:
        out["most_common"] = None
    return out


def profile(table: pa.Table) -> pd.DataFrame:
    """One row of quality statistics per column."""
    rows = [{"column": name, **column_profile(table.column(name))} for name in table.column_names]
    cols = ["column", "type", "filled_pct", "missing", "blank_text", "padded_text", "distinct",
            "true", "min", "max", "most_common"]
    return pd.DataFrame(rows, columns=cols)


def duplicates(table: pa.Table, columns: list[str]) -> int:
    """Rows beyond the first with the same values in `columns`."""
    if not columns or table.num_rows == 0:
        return 0
    part = table.select(columns)
    part = part.cast(pa.schema([pa.field(f.name, pa.large_string() if _is_text(f.type) else f.type)
                                for f in part.schema]))
    return table.num_rows - part.group_by(columns).aggregate([]).num_rows


def content_columns(names: list[str]) -> list[str]:
    """The columns that describe the record itself (for duplicate rows)."""
    return [c for c in names if c not in NOT_CONTENT and (not c.startswith("_") or c == "_batch_id")]


def table_key(rel: str, names: list[str]) -> list[str] | None:
    """The columns that should identify a row of this table, if it has a declared key."""
    name = rel.split("/")[-1]
    if rel.startswith("bronze/"):
        for key in (["_request_key", "_record_index"], ["_batch_id", "_row_number"]):
            if set(key) <= set(names):
                return key
        return None
    return KEYS.get(name)


def quality(table: pa.Table, rel: str) -> dict:
    """Everything the Data quality view shows, for one table."""
    names = table.column_names
    content, key = content_columns(names), table_key(rel, names)
    return {"rows": table.num_rows, "profile": profile(table),
            "content_columns": content, "duplicate_rows": duplicates(table, content),
            "key": key, "duplicate_keys": duplicates(table, key) if key else None}


# =========================================================================== page
def main() -> None:
    import streamlit as st
    from deltalake import DeltaTable

    @st.cache_data(show_spinner="Computing statistics over the whole table…", max_entries=8)
    def cached_quality(path: str, version: int, rel: str) -> dict:
        return quality(DeltaTable(path, version=version).to_pyarrow_table(), rel)

    st.set_page_config(page_title="Lakehouse viewer", layout="wide")
    root = cbs_silver.LAKEHOUSE
    tables = lakehouse_tables(root)

    with st.sidebar:
        st.title("Lakehouse viewer")
        layer = st.radio("Layer", [lay for lay in LAYERS if tables[lay]], horizontal=True,
                         key="layer")
        if layer is None:
            st.warning(f"No Delta tables found under {root}.")
            return
        rel = st.selectbox("Table", tables[layer], key="table",
                           format_func=lambda r: r.split("/", 1)[-1] if layer != "other" else r)
        latest = DeltaTable(str(root / rel))
        history = latest.history()                   # newest first
        version = st.selectbox(
            "Version", [h["version"] for h in history], key="version",
            format_func=lambda v: f"{v}{'  (latest)' if v == latest.version() else ''}")
        view = st.radio("View", ["Rows", "Data quality", "History"], key="view")
    try:
        dt = DeltaTable(str(root / rel), version=version)
    except Exception as e:                           # e.g. files already vacuumed
        st.error(f"Version {version} can't be read: {e}")
        return
    schema = pa.schema(dt.schema().to_arrow())
    st.header(rel)
    st.caption(f"version {version} · {len(schema)} columns")

    if view == "Rows":
        with st.sidebar:
            columns = st.multiselect("Columns (empty = all)", schema.names)
            column = st.selectbox("Filter column", ["(none)", *schema.names], key="filter_column")
            text = st.text_input("contains", disabled=column == "(none)", key="filter_text")
            limit = st.number_input("Rows to show", 10, 100_000, 1000, step=500)
        rows, matching = read(root / rel, version, columns or None,
                              None if column == "(none)" else column, text.strip(), int(limit))
        a, b = st.columns(2)
        a.metric("Rows matching", f"{matching:,}")
        b.metric("Rows shown", f"{rows.num_rows:,}")
        st.dataframe(rows.to_pandas(), width="stretch", hide_index=True)
        with st.expander("Columns and types"):
            st.dataframe(pd.DataFrame({"column": schema.names,
                                       "type": [str(t) for t in schema.types]}), hide_index=True)

    elif view == "Data quality":
        q = cached_quality(str(root / rel), version, rel)
        prof = q["profile"]
        n = q["rows"]
        cells = n * len(prof)
        missing = int(prof["missing"].sum() + prof["blank_text"].fillna(0).sum())
        a, b, c, d = st.columns(4)
        a.metric("Rows", f"{n:,}")
        b.metric("Cells missing or blank", f"{100 * missing / cells:.1f}%" if cells else "–")
        c.metric("Duplicate rows", f"{q['duplicate_rows']:,}",
                 help="Rows identical to an earlier one in: " + ", ".join(q["content_columns"]))
        d.metric("Duplicate keys", "–" if q["key"] is None else f"{q['duplicate_keys']:,}",
                 help=("No declared key for this table" if q["key"] is None
                       else "Key: " + ", ".join(q["key"])))
        if q["key"] and q["duplicate_keys"]:
            st.error(f"{q['duplicate_keys']:,} rows repeat a key ({', '.join(q['key'])}).")
        if layer != "bronze" and (prof["padded_text"].fillna(0) > 0).any():
            st.warning("Text with leading or trailing spaces: silver and gold should have none.")

        st.subheader("Columns")
        st.caption("filled = not missing and not blank · missing = null · blank = empty or "
                   "only spaces · padded = leading/trailing spaces (CBS pads codes; bronze "
                   "keeps them, silver strips them) · distinct = different non-missing values")
        st.dataframe(prof, width="stretch", hide_index=True, column_config={
            "filled_pct": st.column_config.ProgressColumn("filled", format="%.1f%%",
                                                          min_value=0, max_value=100),
            "blank_text": "blank", "padded_text": "padded", "most_common": "most common"})

        flags = prof[prof["type"] == "bool"]
        if not flags.empty:
            st.subheader("Flags")
            st.caption("Edge cases are marked, not removed: the share of rows where each flag is true.")
            st.dataframe(pd.DataFrame({
                "flag": flags["column"],
                "true": flags["true"].astype(int),
                "share": 100 * flags["true"].astype(int) / n if n else 0.0,
            }), hide_index=True, column_config={"share": st.column_config.ProgressColumn(
                "share", format="%.2f%%", min_value=0, max_value=100)})

    else:
        st.dataframe(pd.DataFrame([{"version": h["version"],
                                    "time": pd.to_datetime(h.get("timestamp"), unit="ms"),
                                    "operation": h.get("operation"),
                                    "parameters": str(h.get("operationParameters", ""))}
                                   for h in history]), width="stretch", hide_index=True)


if __name__ == "__main__":
    from streamlit.runtime.scriptrunner import get_script_run_ctx
    if get_script_run_ctx(suppress_warning=True) is not None:   # running inside Streamlit
        main()
    else:  # started as `python -m tools.lakehouse_viewer`
        print(f"Lakehouse viewer: http://localhost:{PORT}  (Ctrl+C to stop)", flush=True)
        # headless: no first-run e-mail question, no browser opened automatically
        sys.exit(subprocess.call([sys.executable, "-m", "streamlit", "run", __file__,
                                  "--server.port", str(PORT), "--server.address", "127.0.0.1",
                                  "--server.headless", "true",
                                  "--browser.gatherUsageStats", "false"]))
