"""Overview: the national absence trend, and the sectors furthest above and below it.

Answers sub-questions 1-3 at a glance; the other pages go into detail.
"""
import streamlit as st

import charts
import common
import data

st.title("Sickness absence by company characteristics")
st.markdown("Which sectors and company sizes go together with **higher sickness absence**? "
            "Absence % = sick days as a share of available working days (CBS, quarterly).")

t = common.load(["gold_absence_by_sector", "gold_absence_by_size"])
final = st.session_state.get("final_only", False)
sector = data.final_only(t["gold_absence_by_sector"], final)
size = data.final_only(t["gold_absence_by_size"], final)
common.stop_if_empty(sector)

common.freshness_line(sector, "CBS 80072NED")
period = data.latest_period(sector)
now = sector[sector["period_key"] == period]
national = now[now["sbi_level"] == "total"].iloc[0]
sections = now[now["sbi_level"] == "section"].sort_values("absence_pct", ascending=False)
sections = sections.assign(sector_label=sections["sbi_code"] + " " + sections["sector_name"])
q_size = size[(size["source_table"] == "80072NED") & (size["period_key"] == period)
              & (size["member_type"] == "size")].sort_values("lower_bound")

# --- headline figures: national rate and the extremes, for the latest quarter
label = national["period_label"] + (" (provisional)" if national["is_provisional"] else "")
cols = st.columns(4)
cols[0].metric(f"National absence, {label}", f"{national['absence_pct']:.1f}%",
               delta=(None if national["change_vs_last_year_pp"] is None
                      else f"{national['change_vs_last_year_pp']:+.1f} pp vs a year earlier"),
               delta_color="inverse",   # more absence is worse, so up is red
               help="Compared with the same quarter one year earlier, because absence is "
                    "seasonal (higher in winter).")
top, low = sections.iloc[0], sections.iloc[-1]
cols[1].metric("Highest sector", f"{top['absence_pct']:.1f}%", help=top["sector_name"],
               delta=f"{top['diff_from_national_pp']:+.1f} pp vs national", delta_color="inverse")
cols[1].caption(f"{top['sbi_code']} {top['sector_name']}")
cols[2].metric("Lowest sector", f"{low['absence_pct']:.1f}%", help=low["sector_name"],
               delta=f"{low['diff_from_national_pp']:+.1f} pp vs national", delta_color="inverse")
cols[2].caption(f"{low['sbi_code']} {low['sector_name']}")
if not q_size.empty:
    big, small = q_size.iloc[-1], q_size.iloc[0]
    cols[3].metric("Largest vs smallest companies",
                   f"{big['value']:.1f}% vs {small['value']:.1f}%",
                   help="Absence % for companies with 100+ persons employed vs 1-9.")
    cols[3].caption(f"{big['size_range']} vs {small['size_range']} persons employed")

# --- national trend
left, right = st.columns([3, 2])
with left:
    st.subheader("National absence over time")
    nat = sector[sector["sbi_level"] == "total"]
    st.altair_chart(charts.trend(nat, "absence_pct", "sector_name", "Series",
                                 highlight=nat["sector_name"].iloc[0]),
                    width="stretch", theme=None)
    st.caption("Absence peaks in winter. Compare a quarter with the same quarter a year "
               "earlier (see *Sectors*), not with the quarter before.")

# --- sections vs national for the latest quarter
with right:
    st.subheader(f"Sectors vs national, {label}")
    st.altair_chart(charts.diverging_bars(sections, "sector_label", "diff_from_national_pp",
                                          "absence_pct", "Sector"),
                    width="stretch", theme=None)
    st.caption("Red: more absence than the national figure; blue: less (SBI sections).")

common.mix_note(now, ["yoy_mixes_status"])
if (sector["is_provisional"].astype(bool)).any():
    st.caption(common.PROVISIONAL_NOTE)
common.table_view(sections[["sbi_code", "sector_name", "absence_pct", "diff_from_national_pp",
                            "rank_in_level", "change_vs_last_year_pp", "period_status"]])
