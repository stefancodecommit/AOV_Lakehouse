"""
Backtest of the risk index (phase 4, evaluation): does a verdict still hold a year later?

For every quarter that has a full window behind it and a full year after it (the anchor),
the risk index is built with gold's own code (pipeline/gold/build.py, build_risk_index) on
the figures up to that quarter only, as an analyst would have seen it then. Each verdict
is then compared with what happened in the next four quarters:

  assessment higher   was the group's relative risk over the next year still above 1?
  assessment lower    ... still below 1?
  around national     ... above 1 (no right answer: shows which way it went)
  trend               was the next year's relative risk higher than the anchor's?

Only final CBS figures are used: provisional figures are revised later, and a backtest
has to judge a verdict on the figures it would really have been checked against.

Caveat: the anchors overlap (each window shares 7 of its 8 quarters with the next one),
so the pairs are not independent and the shares overstate the evidence somewhat.

Result on the real data (2026-10-26): docs and numbers in pipeline/gold/README.md
(section "Backtest").

Usage
-----
    python -m tools.backtest_risk                 # the summary table
    python -m tools.backtest_risk --rows out.csv  # also every anchor and group
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from pipeline.gold import build as gold

HORIZON = 4                     # quarters after the anchor: one year
TABLES = ["fct_absence_rate", "dim_period", "dim_sector", "dim_company_size"]


def backtest(t: dict, settings_: dict | None = None, horizon: int = HORIZON) -> pd.DataFrame:
    """One row per anchor quarter and group: the verdict at the anchor (assessment, trend,
    relative risk) and the relative risk over the `horizon` quarters after it."""
    s = settings_ or gold.RISK
    window = int(s["window_quarters"])
    rate = t["fct_absence_rate"]
    rate = rate[rate["period_status"] == gold.FINAL]
    t = {**t, "fct_absence_rate": rate,
         "fct_business_count": t.get("fct_business_count", pd.DataFrame())}
    history = gold._relative_risk(t)
    quarters = sorted(history["_q"].unique())
    out = []
    for anchor in quarters:
        if anchor - window + 1 < quarters[0] or anchor + horizon > quarters[-1]:
            continue
        seen = history.loc[history["_q"] <= anchor, "period_key"].unique()
        index = gold.build_risk_index({**t, "fct_absence_rate": rate[rate["period_key"].isin(seen)]}, s)
        index = index[(index["basis"] == "all") & (index["assessment"] != "too few quarters")]
        later = history[(history["_q"] > anchor) & (history["_q"] <= anchor + horizon)]
        nxt = (later.groupby(["group_type", "group_key"])
               .agg(quarters=("_q", "size"), absence=("absence_pct", "mean"),
                    national=("national_pct", "mean")))
        nxt = nxt[nxt["quarters"] == horizon]
        nxt["relative_risk_next"] = (nxt["absence"] / nxt["national"]).round(3)
        rows = index.merge(nxt["relative_risk_next"].reset_index(), on=["group_type", "group_key"])
        out.append(rows[["period_label", "group_type", "group_key", "group_name", "relative_risk",
                         "assessment", "trend", "trend_per_year", "relative_risk_next"]]
                   .rename(columns={"period_label": "anchor"}))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def summary(rows: pd.DataFrame) -> pd.DataFrame:
    """Per verdict: how many anchor-group pairs, and how often the next year agreed."""
    out = []
    for verdict, question, hit in [
            ("higher", "next year still above national", lambda r: r["relative_risk_next"] > 1),
            ("lower", "next year still below national", lambda r: r["relative_risk_next"] < 1),
            ("around national", "next year above national", lambda r: r["relative_risk_next"] > 1)]:
        part = rows[rows["assessment"] == verdict]
        out.append({"verdict": verdict, "pairs": len(part), "question": question,
                    "share": hit(part).mean() if len(part) else np.nan, "mean_change": np.nan})
    for trend in ("rising", "stable", "falling"):
        part = rows[rows["trend"] == trend]
        change = part["relative_risk_next"] - part["relative_risk"]
        out.append({"verdict": f"trend {trend}", "pairs": len(part),
                    "question": "next year higher than at the anchor",
                    "share": (change > 0).mean() if len(part) else np.nan,
                    "mean_change": change.mean() if len(part) else np.nan})
    return pd.DataFrame(out)


def trend_correlation(rows: pd.DataFrame) -> float:
    """Correlation between the trend at the anchor and the change over the next year:
    near 0 means the trend says nothing about the next year."""
    change = rows["relative_risk_next"] - rows["relative_risk"]
    return float(np.corrcoef(rows["trend_per_year"], change)[0, 1])


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backtest the risk index on final CBS figures.")
    ap.add_argument("--rows", help="also write every anchor and group to this CSV file")
    args = ap.parse_args()
    rows = backtest(gold.read_silver(TABLES))
    if rows.empty:
        raise SystemExit("not enough final quarters for a backtest")
    anchors = rows["anchor"].nunique()
    print(f"Backtest on final figures: {anchors} anchor quarters ({rows['anchor'].min()} to "
          f"{rows['anchor'].max()}), {len(rows)} anchor-group pairs, horizon {HORIZON} quarters\n")
    table = summary(rows)
    table["share"] = table["share"].map(lambda v: f"{v:.0%}" if pd.notna(v) else "")
    table["mean_change"] = table["mean_change"].map(lambda v: f"{v:+.3f}" if pd.notna(v) else "")
    print(table.to_string(index=False))
    print(f"\nCorrelation of the trend with the next year's change: {trend_correlation(rows):.2f}")
    print("The anchors overlap, so the pairs are not independent.")
    if args.rows:
        rows.to_csv(args.rows, index=False)
        print(f"wrote {args.rows}")
