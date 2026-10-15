"""Risk index: for the insurer's data analyst. Which sectors and size classes carry a higher
absence risk than the national figure, how certain is that, and where is it rising?

Reads gold_risk_index (one row per sector and size class) and gold_risk_index_history
(the quarterly relative risk behind it). Every number is simple arithmetic, explained on
the page and in one sentence per row, so the analyst can pass it on with confidence.
"""
import pandas as pd
import streamlit as st

import charts
import common
import data

LEVELS = {"section": "Sections (A, B, C, ...)", "division": "Divisions (2 digits)",
          "section_group": "Section groups (B-F, G-N, ...)",
          "division_group": "Division groups (10-12, ...)", "group": "Groups (3 digits)"}

st.title("Risk index")
st.caption("**Relative risk** = the group's absence over the last 4 quarters ÷ the national "
           "absence over the same quarters; **1.00 = national**, 1.20 = 20% more absence. "
           "The **band** is the lowest and highest quarter of the last 8: red when the group "
           "was above national in every quarter, blue when below in every quarter, grey when "
           "it varies. CBS absence figures describe employees, not the self-employed.")

t = common.load(["gold_risk_index", "gold_risk_index_history"])
final = st.session_state.get("final_only", False)
index = t["gold_risk_index"]
index = index[index["basis"] == ("final" if final else "all")] if not index.empty else index
history = data.final_only(t["gold_risk_index_history"], final)
common.stop_if_empty(index)
common.freshness_line(history, "CBS 80072NED (absence), 81589NED (businesses)")

# --- one filter row above everything it scopes
f1, f2 = st.columns(2)
group = f1.segmented_control("Group", ["Sectors", "Size classes"], default="Sectors") or "Sectors"
if group == "Sectors":
    sectors = index[index["group_type"] == "sector"]
    levels = [lv for lv in LEVELS if lv in set(sectors["sbi_level"])]
    level = f2.selectbox("Sector level", levels, format_func=LEVELS.get,
                         help="Compare sectors of the same level only.")
    view = sectors[sectors["sbi_level"] == level]
    view = view.assign(label=view["sbi_code"] + " " + view["group_name"])
else:
    view = index[index["group_type"] == "size"]
    view = view.assign(label=view["group_name"])
common.stop_if_empty(view)
assessed = view[view["assessment"] != "too few quarters"]

period = view["period_label"].iloc[0]
m1, m2, m3 = st.columns(3)
m1.metric(f"Above national, {period}", f"{(assessed['assessment'] == 'higher').sum()} "
          f"of {len(assessed)}", help="Above national in every quarter of the band.")
m2.metric("Rising", int((assessed["trend"] == "rising").sum()),
          help="Relative risk rising by more than 0.05 per year over the last 8 quarters.")
m3.metric("Falling", int((assessed["trend"] == "falling").sum()))

st.subheader("Relative risk with its band")
if assessed.empty:
    st.info("Too few quarters with figures to assess this group.")
else:
    st.altair_chart(charts.risk_bands(assessed, "label", "Sector" if group == "Sectors" else "Size class"),
                    width="stretch", theme=None)
    rising = assessed[assessed["trend"] == "rising"].sort_values("trend_per_year", ascending=False)
    if not rising.empty:
        st.markdown("**Rising:** " + "; ".join(
            f"{r.label} ({r.trend_per_year:+.2f} per year)" for r in rising.itertuples()))
too_few = view[view["assessment"] == "too few quarters"]
if not too_few.empty:
    st.caption("Not assessed (too few quarters with figures): " + ", ".join(too_few["label"]))

# --- one group in detail
st.subheader("One group in detail")
choices = list(view.sort_values("relative_risk", ascending=False, na_position="last")["label"])
pick = st.selectbox("Sector" if group == "Sectors" else "Size class", choices)
row = view[view["label"] == pick].iloc[0]
st.markdown(row["explanation"])
series = history[history["group_key"] == row["group_key"]]
if not series.empty:
    series = series.assign(series=pick)
    national = series.assign(series="National (= 1.00)", relative_risk=1.0)
    st.altair_chart(charts.trend(pd.concat([national, series]), "relative_risk", "series",
                                 "Series", highlight=pick, value_title="Relative risk",
                                 fmt=".2f"),
                    width="stretch", theme=None)
    st.caption("Each point is one quarter's absence ÷ the national absence of that quarter; "
               "the season cancels out, so quarters can be compared directly.")

if not final and (view["provisional_quarters"] > 0).any():
    st.caption(common.PROVISIONAL_NOTE)
common.table_view(view[["label", "relative_risk", "band_low", "band_high", "assessment",
                        "trend_per_year", "trend", "absence_4q_pct", "national_4q_pct",
                        "quarters_in_band", "provisional_quarters", "businesses",
                        "businesses_natural_persons", "businesses_one_person",
                        "businesses_period", "period_label"]])
