"""
Chart builders (Altair) shared by every page, so all charts look and behave the same.

Design rules (from the data-visualisation guidance used for this project):
- Colour has a job. One series is blue; the national figure is grey context
  ("emphasis": the thing being looked at stands out, the rest recedes).
- Above/below the national average: red bars above (*more* absence than average),
  blue below, grey when equal. Colour marks only the side; the bar length already shows
  the size, so a colour gradient would say the same thing twice.
- At most three categorical colours on one chart (size bands); they are the first
  three slots of a palette validated for colour-blind readers, in a fixed order.
- Thin marks: 2px lines, bars at most 24px thick with 4px rounded ends, hairline grid;
  text stays in ink colours, never in a series colour.
- Provisional points are hollow; final points are filled. Every chart has tooltips, and
  every page offers the numbers as a table too, so no value is reachable only by hover.
- Light and dark mode each have their own validated colour steps (not an inverted copy).
"""
from __future__ import annotations

import altair as alt
import pandas as pd

# Reference palette (validated set; see README "Dashboard"). Light and dark steps.
PALETTE = {
    "light": {"surface": "#fcfcfb", "ink": "#0b0b0b", "ink2": "#52514e", "muted": "#898781",
              "grid": "#e1e0d9", "axis": "#c3c2b7", "series": ["#2a78d6", "#eb6834", "#1baf7a"],
              "context": "#898781", "below": "#2a78d6", "mid": "#f0efec", "above": "#e34948"},
    "dark": {"surface": "#1a1a19", "ink": "#ffffff", "ink2": "#c3c2b7", "muted": "#898781",
             "grid": "#2c2c2a", "axis": "#383835", "series": ["#3987e5", "#d95926", "#199e70"],
             "context": "#898781", "below": "#3987e5", "mid": "#383835", "above": "#e66767"},
}
FONT = "system-ui, -apple-system, 'Segoe UI', sans-serif"
BAR = 24            # maximum bar thickness in px


def theme_mode() -> str:
    """'dark' or 'light', following the viewer's Streamlit theme."""
    try:
        import streamlit as st
        return "dark" if getattr(st.context.theme, "type", None) == "dark" else "light"
    except Exception:          # outside Streamlit (tests) or an older Streamlit
        return "light"


def colors() -> dict:
    return PALETTE[theme_mode()]


def style(chart: alt.Chart | alt.LayerChart, height: int = 320) -> alt.Chart:
    """Common chrome: transparent background, recessive hairline grid and axes,
    ink-coloured text, the system sans."""
    c = colors()
    return (chart.properties(height=height, background="transparent")
            .configure_view(stroke=None)
            .configure_axis(gridColor=c["grid"], gridWidth=1, domainColor=c["axis"],
                            tickColor=c["axis"], labelColor=c["muted"], titleColor=c["ink2"],
                            labelFont=FONT, titleFont=FONT, labelFontSize=12,
                            titleFontSize=12, titleFontWeight="normal")
            .configure_legend(labelColor=c["ink2"], titleColor=c["ink2"], labelFont=FONT,
                              titleFont=FONT, orient="top", titleFontWeight="normal")
            .configure_text(font=FONT, color=c["ink2"]))


def add_period_date(df: pd.DataFrame) -> pd.DataFrame:
    """A real date per period (first day of the quarter or year), so time is a proper
    time axis with gaps where periods are missing."""
    out = df.copy()
    month = 1 if "quarter" not in out else (3 * out["quarter"].fillna(1).astype(int) - 2)
    out["period_date"] = pd.to_datetime(dict(year=out["year"].astype(int), month=month, day=1))
    return out


def _status(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["status"] = out["is_provisional"].astype(bool).map({True: "provisional", False: "final"})
    return out


def trend(df: pd.DataFrame, value: str, series: str, title: str, *, highlight: str | None = None,
          value_title: str = "Absence %", fmt: str = ".1f", height: int = 320,
          end_label: str | None = None) -> alt.Chart:
    """Lines over time, one per value of `series`. With `highlight`, that series is blue
    and the others grey context (emphasis); without it, up to three series get the
    categorical colours. Provisional points are hollow. `end_label`: column with a short
    name written at the end of each line (2-4 series without highlight)."""
    c = colors()
    d = _status(add_period_date(df))
    names = list(dict.fromkeys(d[series]))
    if highlight is not None:
        rng = [c["series"][0] if n == highlight else c["context"] for n in names]
    else:
        rng = (c["series"] * 3)[:len(names)]
    # one series: no legend box (the title names it); two or more: always a legend
    color = alt.Color(f"{series}:N", title=None, scale=alt.Scale(domain=names, range=rng),
                      legend=None if len(names) == 1 else alt.Legend())
    tooltip = [alt.Tooltip(f"{series}:N", title=title), alt.Tooltip("period_label:N", title="Period"),
               alt.Tooltip(f"{value}:Q", title=value_title, format=fmt),
               alt.Tooltip("status:N", title="Status")]
    x = alt.X("period_date:T", title=None, axis=alt.Axis(format="%Y", tickCount="year"))
    y = alt.Y(f"{value}:Q", title=value_title, scale=alt.Scale(zero=False))
    base = alt.Chart(d).encode(x=x, y=y, color=color, detail=f"{series}:N")
    line = base.mark_line(strokeWidth=2, strokeCap="round", strokeJoin="round")
    final = base.transform_filter(alt.datum.status == "final").mark_point(
        filled=True, size=40, opacity=1).encode(tooltip=tooltip)
    # hollow: outline in the series colour, filled with the surface so the line behind
    # it doesn't show through
    prov = base.transform_filter(alt.datum.status == "provisional").mark_point(
        filled=False, fill=c["surface"], size=70, opacity=1, strokeWidth=2).encode(
        tooltip=tooltip)
    layers = [line, final, prov]
    if end_label and highlight is None and 2 <= len(names) <= 4:
        # direct labels at the line ends (besides the legend), in ink, never series colour
        last = d.loc[d.groupby(series)["period_date"].idxmax()]
        layers.append(alt.Chart(last).mark_text(align="left", dx=8, fontSize=12,
                                                color=c["ink2"]).encode(
            x="period_date:T", y=f"{value}:Q", text=alt.Text(f"{end_label}:N")))
    return style(alt.layer(*layers), height)


def diverging_bars(df: pd.DataFrame, label: str, diff: str, value: str, title: str,
                   height: int | None = None) -> alt.Chart:
    """Horizontal bars of the difference from the national figure, sorted; blue below,
    red above, grey when equal (to one decimal). The tooltip also shows the value."""
    c = colors()
    d = _status(df)
    d["side"] = pd.cut(d[diff].round(1), [-float("inf"), -0.05, 0.05, float("inf")],
                       labels=["below national", "equal", "above national"]).astype(str)
    bar = alt.Chart(d).mark_bar(size=min(BAR, 18), cornerRadiusEnd=4).encode(
        y=alt.Y(f"{label}:N", sort=alt.EncodingSortField(field=diff, order="descending"),
                title=None, axis=alt.Axis(labelLimit=260)),
        x=alt.X(f"{diff}:Q", title="Difference from national (percentage points)",
                axis=alt.Axis(tickCount=8, format="+.1f")),
        color=alt.Color("side:N", legend=None, scale=alt.Scale(
            domain=["below national", "equal", "above national"],
            range=[c["below"], c["muted"], c["above"]])),
        tooltip=[alt.Tooltip(f"{label}:N", title=title),
                 alt.Tooltip(f"{value}:Q", title="Absence %", format=".1f"),
                 alt.Tooltip(f"{diff}:Q", title="vs national (pp)", format="+.1f"),
                 alt.Tooltip("status:N", title="Status")])
    zero = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color=c["axis"]).encode(x="x:Q")
    return style(alt.layer(bar, zero), height or max(160, 26 * d[label].nunique()))


def risk_bands(df: pd.DataFrame, label: str, title: str) -> alt.Chart:
    """Relative risk per row as a dot, its band (lowest–highest quarter) as a line, and a
    rule at 1.00 (national). Red: above national in every quarter; blue: below in every
    quarter; grey: around national. Sorted from highest to lowest risk."""
    c = colors()
    d = df.copy()
    order = list(d.sort_values("relative_risk", ascending=False)[label])
    color = alt.Color("assessment:N", title=None, scale=alt.Scale(
        domain=["higher", "around national", "lower"], range=[c["above"], c["muted"], c["below"]]))
    y = alt.Y(f"{label}:N", title=None, sort=order, axis=alt.Axis(labelLimit=320))
    tooltip = [alt.Tooltip(f"{label}:N", title=title),
               alt.Tooltip("relative_risk:Q", title="Relative risk", format=".2f"),
               alt.Tooltip("band_low:Q", title="Band low", format=".2f"),
               alt.Tooltip("band_high:Q", title="Band high", format=".2f"),
               alt.Tooltip("assessment:N", title="Assessment"),
               alt.Tooltip("trend:N", title="Trend"),
               alt.Tooltip("trend_per_year:Q", title="Trend per year", format="+.2f")]
    base = alt.Chart(d).encode(y=y)
    band = base.mark_rule(strokeWidth=2).encode(
        x=alt.X("band_low:Q", title="Relative risk (1.00 = national)", scale=alt.Scale(zero=False)),
        x2="band_high:Q", color=color)
    dot = base.mark_point(filled=True, size=70, opacity=1).encode(
        x="relative_risk:Q", color=color, tooltip=tooltip)
    one = alt.Chart(pd.DataFrame({"x": [1.0]})).mark_rule(color=c["ink2"], strokeDash=[4, 3]).encode(x="x:Q")
    return style(alt.layer(one, band, dot), max(140, 24 * d[label].nunique()))


def intervals(df: pd.DataFrame, label: str, title: str, unit: str) -> alt.Chart:
    """Estimate with its 95% interval per row, and the national value as a grey tick.
    Blue: the interval lies wholly above or below the national value (a real difference);
    grey: it includes the national value (could be sampling noise)."""
    c = colors()
    d = df.copy()
    d["difference"] = d["differs_from_national"].astype(bool).map(
        {True: "differs from national", False: "within margin"})
    color = alt.Color("difference:N", title=None, scale=alt.Scale(
        domain=["differs from national", "within margin"], range=[c["series"][0], c["context"]]))
    y = alt.Y(f"{label}:N", title=None, sort=None, axis=alt.Axis(labelLimit=320))
    tooltip = [alt.Tooltip(f"{label}:N", title=title), alt.Tooltip("value:Q", format=".1f"),
               alt.Tooltip("lower_95:Q", title="95% low", format=".1f"),
               alt.Tooltip("upper_95:Q", title="95% high", format=".1f"),
               alt.Tooltip("national_value:Q", title="National", format=".1f")]
    base = alt.Chart(d).encode(y=y)
    rule = base.mark_rule(strokeWidth=2).encode(x=alt.X("lower_95:Q", title=unit),
                                                x2="upper_95:Q", color=color)
    dot = base.mark_point(filled=True, size=70, opacity=1).encode(
        x="value:Q", color=color, tooltip=tooltip)
    nat = base.mark_tick(thickness=2, size=14, color=c["ink"]).encode(
        x="national_value:Q", tooltip=tooltip)
    return style(alt.layer(rule, nat, dot), max(140, 24 * d[label].nunique()))


def stacked_bands(df: pd.DataFrame, label: str, value: str, band: str, order: list[str],
                  title: str) -> alt.Chart:
    """Horizontal stacked bars (part-to-whole) with up to three bands in the
    categorical colours, in a fixed band order; a 2px surface gap between segments."""
    c = colors()
    return style(alt.Chart(df).mark_bar(size=min(BAR, 18), stroke=c["surface"],
                                        strokeWidth=2).encode(
        y=alt.Y(f"{label}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260)),
        x=alt.X(f"{value}:Q", title=title, stack="zero"),
        color=alt.Color(f"{band}:N", title="Size band", sort=order,
                        scale=alt.Scale(domain=order, range=c["series"][:len(order)])),
        order=alt.Order("band_order:Q"),
        tooltip=[alt.Tooltip(f"{label}:N", title="Sector"), alt.Tooltip(f"{band}:N", title="Band"),
                 alt.Tooltip(f"{value}:Q", title=title, format=",")]),
        max(160, 26 * df[label].nunique()))


def bars(df: pd.DataFrame, label: str, value: str, title: str, fmt: str = ",") -> alt.Chart:
    """Single-series horizontal bars, sorted, in one colour (no colour by rank)."""
    c = colors()
    return style(alt.Chart(df).mark_bar(size=min(BAR, 18), cornerRadiusEnd=4,
                                        color=c["series"][0]).encode(
        y=alt.Y(f"{label}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260)),
        x=alt.X(f"{value}:Q", title=title),
        tooltip=[alt.Tooltip(f"{label}:N", title="Sector"),
                 alt.Tooltip(f"{value}:Q", title=title, format=fmt)]),
        max(160, 26 * df[label].nunique()))
