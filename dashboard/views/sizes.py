"""Size classes: does absence rise or fall with company size? (sub-question 2)

Two sources, kept apart because they count differently:
- quarterly absence % per class of *persons employed* (80072NED)
- yearly share of *employees* absent at least once, with 95% intervals (86009NED survey)
"""
import streamlit as st

import charts
import common
import data

st.title("Size classes")
t = common.load(["gold_absence_by_size"])
df = data.final_only(t["gold_absence_by_size"], st.session_state.get("final_only", False))
common.stop_if_empty(df)

# --- quarterly absence % per size class (three classes + the national figure as context)
q = df[df["source_table"] == "80072NED"]
common.freshness_line(q, "CBS 80072NED")
st.subheader("Absence % by number of persons employed")
q = q.assign(series=q["size_range"].fillna("All companies")
             .where(q["member_type"] == "total", q["size_range"] + " persons employed"))
lines = q[q["member_type"] == "size"].sort_values("lower_bound")
st.altair_chart(charts.trend(lines, "value", "series", "Size class", end_label="size_range"),
                width="stretch", theme=None)
latest = q[q["period_key"] == data.latest_period(q)].sort_values("lower_bound")
nat = latest[latest["member_type"] == "total"]
if not nat.empty:
    sizes = latest[latest["member_type"] == "size"]
    st.caption(f"{latest['period_label'].iloc[0]}: "
               + ", ".join(f"{r.size_range}: {r.value:.1f}%" for r in sizes.itertuples())
               + f" (national {nat['value'].iloc[0]:.1f}%).")
    share, total = data.rises_with_size(lines)
    if total:
        st.caption(f"Absence rises with company size in {share} of the {total} quarters shown.")

# --- yearly survey: share of employees absent, with intervals
st.subheader("Share of employees absent at least once in the year (survey)")
s = df[(df["source_table"] == "86009NED")]
years = data.period_options(s)
year = st.selectbox("Year", [p for p, _ in years], format_func=dict(years).get)
sy = s[s["period_key"] == year]
show_all = st.toggle("Include combined classes (e.g. 1-9 = 1-4 + 5-9)", value=False)
classes = sy[(sy["member_type"] == "size") & (show_all | ~sy["size_is_aggregate"].astype(bool))]
classes = data.with_interval_flag(classes.sort_values("lower_bound"))
classes = classes.assign(label=classes["size_range"] + " employees")
st.altair_chart(charts.intervals(classes, "label", "Size class", "% of employees"),
                width="stretch", theme=None)
st.caption("Dot = survey estimate, line = 95% interval, dark tick = national value. Blue: "
           "the whole interval lies above or below the national value; grey: the difference "
           "could be sampling noise.")

if df["is_provisional"].astype(bool).any():
    st.caption(common.PROVISIONAL_NOTE)
common.mix_note(df, ["yoy_mixes_status", "rolling_4q_has_provisional"])
common.table_view(df[["source_table", "measure", "period_label", "size_range", "size_unit",
                      "value", "lower_95", "upper_95", "national_value",
                      "diff_from_national_pp", "change_vs_last_year_pp", "period_status"]])
