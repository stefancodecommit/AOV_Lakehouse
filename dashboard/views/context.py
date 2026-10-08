"""Context: how many businesses does each sector and size band represent? (sub-question 6)

A high absence rate weighs differently for a sector of 10,000 businesses than for one of
500,000. CBS counts businesses per sector and size band; KVK adds the active BVs and NVs
per sector and their median age. KVK has no absence data: this is context, not cause.
"""
import streamlit as st

import charts
import common
import data

BANDS = ["<10", "10-99", "100+"]

st.title("Context")
t = common.load(["gold_business_context", "gold_kvk_context"])
final = st.session_state.get("final_only", False)
biz = data.final_only(t["gold_business_context"], final)

# --- CBS: businesses per section and size band
st.subheader("Businesses per sector and size band (CBS)")
if not biz.empty:
    common.freshness_line(biz, "CBS 81589NED")
    periods = data.period_options(biz)
    period = st.selectbox("Quarter", [p for p, _ in periods], format_func=dict(periods).get)
    b = biz[(biz["period_key"] == period) & (biz["sbi_level"] == "section")]
    b = b.assign(sector=b["sbi_code"] + " " + b["sector_name"])
    bands = b[b["size_band"].isin(BANDS)].assign(
        band_order=lambda x: x["size_band"].map({k: i for i, k in enumerate(BANDS)}))
    st.altair_chart(charts.stacked_bands(bands, "sector", "businesses", "size_band", BANDS,
                                         "Number of businesses"),
                    width="stretch", theme=None)
    st.caption("Size bands by persons employed. Most businesses have fewer than 10 persons "
               "(often one-person businesses). CBS rounds counts to multiples of 5, so the "
               "bands can differ from the total by a few.")
    common.table_view(b[b["size_band"] == "all"][["sbi_code", "sector_name", "businesses",
                                                    "period_status"]]
                      .sort_values("businesses", ascending=False))

# --- KVK: active BVs and NVs per section
kvk = t["gold_kvk_context"]
st.subheader("Active BVs and NVs per sector (KVK)")
if kvk.empty:
    st.info("No KVK data yet: load a KVK file and run pipeline/silver/kvk.py and pipeline/gold/build.py.")
else:
    st.caption(f"KVK snapshot of {kvk['snapshot_date'].iloc[0]} · Source: KVK Handelsregister "
               "Open Dataset. Sector from the company's main activity, translated to SBI 2008.")
    hide = st.toggle("Hide groups of fewer than 10 companies", value=True,
                     help="KVK's rule against recognising individual companies in small groups.")
    k = kvk[kvk["legal_form"] == "all"]
    if hide:
        k = k[~k["is_small_group"].astype(bool)]
    known = k[k["sbi2008_section"] != "unknown"]
    known = known.assign(sector=known["sbi2008_section"] + " " + known["sector_name"].fillna(""))
    unknown = k[k["sbi2008_section"] == "unknown"]["active_companies"].sum()
    total = kvk[kvk["legal_form"] == "all"]["active_companies"].sum()
    c1, c2 = st.columns(2)
    with c1:
        st.altair_chart(charts.bars(known, "sector", "active_companies", "Active companies"),
                        width="stretch", theme=None)
    with c2:
        st.altair_chart(charts.bars(known, "sector", "median_age_years",
                                    "Median company age (years)", fmt=".1f"),
                        width="stretch", theme=None)
    st.caption(f"{unknown:,} of {total:,} active companies ({unknown / total:.0%}) have no "
               "single SBI 2008 sector (several main activities, or codes that can't be "
               "translated unambiguously); they are not in the charts. Many companies in "
               "K (financial services) are holding companies.")
    common.table_view(kvk.sort_values(["legal_form", "active_companies"],
                                      ascending=[True, False]))
