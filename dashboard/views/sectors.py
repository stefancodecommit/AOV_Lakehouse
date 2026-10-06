"""Sectors: ranking of sectors against the national figure, and one sector's trend.

Answers sub-questions 1 (highest/lowest) and 3 (stable over time?).
"""
import streamlit as st

import charts
import common
import data

LEVELS = {"section": "Sections (A, B, C, ...)", "division": "Divisions (2 digits)",
          "section_group": "Section groups (B-F, G-N, ...)",
          "division_group": "Division groups (10-12, ...)", "group": "Groups (3 digits)"}

st.title("Sectors")
t = common.load(["gold_absence_by_sector"])
df = data.final_only(t["gold_absence_by_sector"], st.session_state.get("final_only", False))
common.stop_if_empty(df)
common.freshness_line(df, "CBS 80072NED")
df = df.assign(sector_label=df["sbi_code"] + " " + df["sector_name"])

# --- one filter row above everything it scopes
f1, f2, f3 = st.columns(3)
levels = [lv for lv in LEVELS if lv in set(df["sbi_level"])]
level = f1.selectbox("Sector level", levels, format_func=LEVELS.get,
                     help="CBS publishes several levels; compare sectors of the same level only.")
periods = data.period_options(df)
period = f2.selectbox("Quarter", [p for p, _ in periods], format_func=dict(periods).get)
measure = f3.segmented_control(
    "Compare", ["This quarter", "4-quarter average"], default="This quarter",
    help="The 4-quarter average smooths out the seasons (winter peaks).") or "This quarter"

now = df[(df["period_key"] == period) & (df["sbi_level"] == level)]
value, diff = "absence_pct", "diff_from_national_pp"
if measure == "4-quarter average":
    # the national 4-quarter average of the same period, to compare averages with averages
    nat_roll = df[(df["period_key"] == period) & (df["sbi_level"] == "total")]["rolling_4q_pct"]
    now = now.assign(rolling_diff=(now["rolling_4q_pct"] - nat_roll.iloc[0]).round(2)
                     if not nat_roll.empty else None)
    value, diff = "rolling_4q_pct", "rolling_diff"
now = now.dropna(subset=[diff])

st.subheader(f"{LEVELS[level]} vs national")
if now.empty:
    st.info("No figures for this choice (the 4-quarter average needs four consecutive quarters).")
else:
    st.altair_chart(charts.diverging_bars(now, "sector_label", diff, value, "Sector"),
                    width="stretch", theme=None)
    st.caption("Red: more absence than the national figure; blue: less. "
               + ("Averages over the four quarters up to and including the chosen one."
                  if measure == "4-quarter average" else ""))

# --- one sector over time, against the national figure
st.subheader("One sector over time")
choices = sorted(df[df["sbi_level"] == level]["sector_label"].unique())
default = now.sort_values(diff, ascending=False)["sector_label"].iloc[0] if not now.empty else choices[0]
pick = st.selectbox("Sector", choices, index=choices.index(default))
series = df[(df["sector_label"] == pick) | (df["sbi_level"] == "total")]
series = series.assign(series=series["sector_label"].where(series["sbi_level"] != "total",
                                                           "National (all sectors)"))
show = "rolling_4q_pct" if measure == "4-quarter average" else "absence_pct"
st.altair_chart(charts.trend(series.dropna(subset=[show]), show, "series", "Series",
                             highlight=pick,
                             value_title="Absence % (4-quarter average)"
                             if show == "rolling_4q_pct" else "Absence %"),
                width="stretch", theme=None)
row = df[(df["sector_label"] == pick) & (df["period_key"] == period)]
if not row.empty and row["change_vs_last_year_pp"].notna().iloc[0]:
    r = row.iloc[0]
    st.caption(f"{r['period_label']}: {r['absence_pct']:.1f}% vs {r['last_year_pct']:.1f}% "
               f"a year earlier ({r['change_vs_last_year_pp']:+.1f} pp); rank "
               f"{r['rank_in_level']} of {r['sectors_in_level']} at this level.")

common.mix_note(now, ["yoy_mixes_status", "rolling_4q_has_provisional"])
if df["is_provisional"].astype(bool).any():
    st.caption(common.PROVISIONAL_NOTE)
common.table_view(now[["sbi_code", "sector_name", "absence_pct", "national_pct",
                       "diff_from_national_pp", "rank_in_level", "last_year_pct",
                       "change_vs_last_year_pp", "rolling_4q_pct", "period_status"]])
