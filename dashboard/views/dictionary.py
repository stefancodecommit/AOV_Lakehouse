"""Data dictionary: every gold table and column, and every survey measure.

Generated, not hand-written where possible: columns and types come from the Delta
schemas of the gold tables, and the survey measures (title, topic, unit) from the CBS
metadata that silver carried into gold_absence_profile. Only the table descriptions are
written here.
"""
import pandas as pd
import streamlit as st

import common
import data

DESCRIPTIONS = {
    "gold_absence_by_sector": "Quarterly absence % per sector (every CBS level) and the "
                              "national total, with difference from national, rank, change "
                              "vs a year earlier and a rolling 4-quarter average. CBS 80072NED.",
    "gold_absence_by_size": "Absence per size class: quarterly absence % per class of persons "
                            "employed (80072NED) and the yearly share of employees absent per "
                            "class of employees with 95% bounds (86009NED).",
    "gold_absence_profile": "Every yearly survey measure (86009NED) per sector and size class, "
                            "with 95% interval, national value and whether they differ.",
    "gold_business_context": "Number of businesses per sector, quarter and size band "
                             "(CBS 81589NED).",
    "gold_kvk_context": "Active BVs and NVs per SBI 2008 section and legal form, with median "
                        "company age, from the latest KVK snapshot.",
}
COMMON = {
    "period_status": "CBS status: Definitief (final), Voorlopig or NaderVoorlopig (provisional)",
    "is_provisional": "True for provisional figures (hidden by 'Final figures only')",
    "data_as_of": "When the newest data behind the row was loaded",
    "run_id": "The gold run that built the row (see lakehouse/_run_log)",
    "yoy_mixes_status": "The year-on-year comparison combines provisional and final figures",
    "rolling_4q_has_provisional": "The 4-quarter average includes a provisional quarter",
    "is_small_group": "Fewer than 10 companies (KVK rule): hidden by default",
}

st.title("Data dictionary")
st.markdown("The dashboard reads only these gold tables (built by `gold.py`). Columns and "
            "types are read from the tables themselves.")

for name in data.GOLD_TABLES:
    df = data.load_table(name)
    st.subheader(f"`{name}`")
    st.markdown(DESCRIPTIONS.get(name, ""))
    if df.empty:
        st.caption("Not available yet.")
        continue
    cols = pd.DataFrame({"column": df.columns, "type": [str(t) for t in df.dtypes]})
    cols["description"] = cols["column"].map(COMMON).fillna("")
    st.caption(f"{len(df):,} rows · version {data.table_version(name)}")
    st.dataframe(cols, hide_index=True, width="stretch")

st.subheader("Survey measures (CBS metadata)")
profile = data.load_table("gold_absence_profile")
if not profile.empty:
    m = (profile[["measure_key", "measure_title", "topic_group", "unit"]]
         .drop_duplicates("measure_key").sort_values(["topic_group", "measure_key"]))
    st.dataframe(m, hide_index=True, width="stretch")
