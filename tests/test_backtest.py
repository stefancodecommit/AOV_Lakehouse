"""Backtest of the risk index (tools/backtest_risk.py) on series with a known answer."""
import pytest

from test_gold import F, SETTINGS, V, risk_inputs
from tools import backtest_risk as bt


def test_each_anchor_sees_only_its_past_and_is_judged_on_the_next_year():
    # 12 quarters: anchors need 8 behind and 4 after them, so exactly one anchor (Q8)
    t = risk_inputs({"total": [5.0] * 12, "A": [6.0] * 12, "B": [4.0] * 8 + [6.0] * 4})
    rows = bt.backtest(t, SETTINGS).set_index("group_key")
    assert rows["anchor"].nunique() == 1
    assert rows.loc["A", "assessment"] == "higher" and rows.loc["A", "relative_risk_next"] == 1.2
    # B was below national up to the anchor; the later rise is not seen by its verdict
    assert rows.loc["B", "assessment"] == "lower" and rows.loc["B", "relative_risk"] == 0.8
    assert rows.loc["B", "relative_risk_next"] == 1.2
    s = bt.summary(rows.reset_index()).set_index("verdict")
    assert s.loc["higher", "share"] == 1.0 and s.loc["lower", "share"] == 0.0


def test_a_continuing_rise_is_a_hit_for_the_trend():
    up = [5.0 * (1 + 0.03 * i) for i in range(14)]
    rows = bt.backtest(risk_inputs({"total": [5.0] * 14, "A": up}), SETTINGS)
    assert len(rows) == 3 and (rows["trend"] == "rising").all()
    s = bt.summary(rows).set_index("verdict")
    assert s.loc["trend rising", "share"] == 1.0 and s.loc["trend rising", "mean_change"] > 0


def test_provisional_quarters_are_left_out():
    # the last 4 quarters are provisional: no year after any anchor, so no rows
    t = risk_inputs({"total": [5.0] * 12, "A": [6.0] * 12}, statuses=[F] * 8 + [V] * 4)
    assert bt.backtest(t, SETTINGS).empty


def test_for_pure_noise_a_trend_is_followed_by_a_fall_back():
    # sectors that are national plus noise: a "rise" at the end of a window is chance, and
    # the next year falls back to the mean, so the correlation is negative, not 0. The
    # real data's +0.12 is better than noise, but a rising trend is still no forecast
    import numpy as np
    rng = np.random.default_rng(1)
    t = risk_inputs({"total": [5.0] * 40,
                     **{f"S{i}": list(5.0 + rng.normal(0, 0.3, 40)) for i in range(10)}})
    rows = bt.backtest(t, SETTINGS)
    assert bt.trend_correlation(rows) < -0.2
    assert pytest.approx(rows["relative_risk_next"].mean(), abs=0.05) == 1.0
