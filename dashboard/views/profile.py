"""Behind the numbers: what lies behind high absence in a sector or size class?
(sub-questions 4 and 5)

Yearly survey (86009NED): how often and how long people are absent, whether it is
work-related, and the complaints behind it, each with a 95% interval, compared with the
national value. A difference only counts as real when the whole interval lies above or
below the national value.
"""
import streamlit as st

import charts
import common
import data

# Short English names for the survey's (Dutch) topic groups, in a reading order
TOPICS = {
    "Werknemers": "Share of employees absent",
    "Werknemers met verzuim in afgelopen jaar": "How often and how long",
    "Werknemers met verzuim in afgelopen jaar / Duur meest recente verzuimgeval":
        "Duration of the latest absence",
    "Werknemers met verzuim in afgelopen jaar / Recentste ziekteverzuim werkgerelateerd":
        "Was the latest absence work-related?",
    "Werknemers met werkgerelateerd verzuim / Reden werkgerelateerd verzuim (va. 2022)":
        "Reason for work-related absence (from 2022)",
    "Werknemers met werkgerelateerd verzuim / Reden werkgerelateerd verzuim (tot 2022)":
        "Reason for work-related absence (until 2022)",
    "Werknemers met verzuim in afgelopen jaar / Klacht meest recente verzuim (va. 2022)":
        "Complaint behind the latest absence (from 2022)",
    "Werknemers met verzuim in afgelopen jaar / Klacht meest recente verzuim (tot 2022)":
        "Complaint behind the latest absence (until 2022)",
    "Werknemers met verzuim in afgelopen jaar / Vrouwenklachten recentste verzuim":
        "Women's complaints (latest absence)",
}

st.title("Behind the numbers")
st.markdown("What lies behind absence in a sector or size class: more people absent, "
            "longer absences, or more work-related absence? (CBS survey, yearly)")
t = common.load(["gold_absence_profile"])
df = data.final_only(t["gold_absence_profile"], st.session_state.get("final_only", False))
common.stop_if_empty(df)
common.freshness_line(df.assign(period_label=df["year"].astype(str)), "CBS 86009NED")

f1, f2, f3 = st.columns([1, 2, 1])
kind = f1.segmented_control("Look at", ["Sector", "Size class"], default="Sector") or "Sector"
if kind == "Sector":
    groups = df[df["sbi_level"] == "section"]
    groups = groups.assign(group=groups["sbi_code"] + " " + groups["sector_name"])
else:
    groups = df[(df["member_type"] == "size") & ~df["size_is_aggregate"].fillna(False).astype(bool)]
    groups = groups.sort_values("lower_bound")
    groups = groups.assign(group=groups["size_range"] + " employees")
names = list(dict.fromkeys(groups["group"]))
default = names.index(next((n for n in names if n.startswith("Q ")), names[0]))
pick = f2.selectbox(kind, names, index=default)
years = sorted(df["year"].unique(), reverse=True)
year = f3.selectbox("Year", years)

rows = groups[(groups["group"] == pick) & (groups["year"] == year)].dropna(subset=["value"])
if rows.empty:
    st.info("No survey figures for this choice.")
    st.stop()

# --- what differs from the national value, in one list
differs = rows[rows["differs_from_national"].astype(bool)]
if differs.empty:
    st.success(f"For {pick} in {year}, no survey measure differs from the national value "
               "beyond the margin of error.")
else:
    above = differs[differs["direction"] == "above"]
    below = differs[differs["direction"] == "below"]
    st.markdown(f"**{pick}, {year}: {len(differs)} measures differ from the national value** "
                "beyond the margin of error.")
    c1, c2 = st.columns(2)
    c1.markdown("**Higher than national**\n" + "".join(
        f"\n- {r.measure_title} ({TOPICS.get(r.topic_group, r.topic_group)}): "
        f"{r.value:.1f} vs {r.national_value:.1f}" for r in above.itertuples()) if not above.empty
        else "**Higher than national**\n\n- none")
    c2.markdown("**Lower than national**\n" + "".join(
        f"\n- {r.measure_title} ({TOPICS.get(r.topic_group, r.topic_group)}): "
        f"{r.value:.1f} vs {r.national_value:.1f}" for r in below.itertuples()) if not below.empty
        else "**Lower than national**\n\n- none")

# --- one topic at a time, each measure with its interval
st.subheader("Per topic")
available = [g for g in TOPICS if g in set(rows["topic_group"])]
topic = st.selectbox("Topic", available, format_func=TOPICS.get)
tr = rows[rows["topic_group"] == topic].sort_values("value", ascending=False)
if set(tr["unit"]) != {"%"}:
    # different units (number of times, number of days): one figure each, not one axis
    cols = st.columns(len(tr))
    for col, r in zip(cols, tr.itertuples()):
        col.metric(r.measure_title, f"{r.value:.1f}",
                   delta=f"{r.diff_from_national:+.1f} vs national ({r.national_value:.1f})",
                   delta_color="inverse",
                   help=f"95% interval {r.lower_95:.1f} - {r.upper_95:.1f} ({r.unit}). "
                        + ("Differs from national." if r.differs_from_national
                           else "Within the margin of error."))
else:
    st.altair_chart(charts.intervals(tr, "measure_title", "Measure", "% of employees"),
                    width="stretch", theme=None)
    st.caption("Dot = estimate, line = 95% interval, dark tick = national value. Blue: the "
               "whole interval lies above or below the national value.")

common.table_view(rows[["topic_group", "measure_title", "unit", "value", "lower_95",
                        "upper_95", "national_value", "differs_from_national", "direction",
                        "rank_in_topic"]].sort_values(["topic_group", "rank_in_topic"]))
