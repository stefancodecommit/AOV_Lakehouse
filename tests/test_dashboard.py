"""Dashboard: the data helpers, and every page run headless (Streamlit's AppTest) on gold
tables built end to end from the sample bronze data."""
import sys
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from pipeline.silver import cbs as s
from pipeline.gold import build as g
from pipeline.common import publish
from conftest import dashboard_data as data
from test_delta import bronze_delta  # noqa: F401  (fixture)

APP = str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")
PAGES = ["overview", "risk", "sectors", "sizes", "profile", "context", "dictionary"]


# =========================================================================== helpers
def rows(statuses, periods=None):
    periods = periods or [f"2025KW0{i + 1}" for i in range(len(statuses))]
    return pd.DataFrame({"period_key": periods, "period_label": periods,
                         "period_status": statuses,
                         "is_provisional": [st != "Definitief" for st in statuses],
                         "data_as_of": pd.Timestamp("2026-10-05", tz="UTC"), "run_id": "gold_1"})


def test_final_only_switch():
    df = rows(["Definitief", "NaderVoorlopig", "Voorlopig"])
    assert len(data.final_only(df, False)) == 3
    assert list(data.final_only(df, True)["period_status"]) == ["Definitief"]


def test_run_problem_only_for_different_runs():
    a, b = rows(["Definitief"]), rows(["Definitief"])
    assert data.run_problem({"t1": a, "t2": b}) is None
    msg = data.run_problem({"t1": a, "t2": b.assign(run_id="gold_2")})
    assert "different gold runs" in msg and "gold_1: t1" in msg and "gold_2: t2" in msg
    assert data.run_problem({"t1": a, "empty": pd.DataFrame()}) is None


def test_freshness_and_period_options():
    df = rows(["Definitief", "Voorlopig"])
    f = data.freshness(df)
    assert f["latest_period"] == "2025KW02" and f["latest_is_provisional"]
    assert data.period_options(df) == [("2025KW02", "2025KW02 (provisional)"),
                                       ("2025KW01", "2025KW01")]


def test_interval_flag_and_rises_with_size():
    df = pd.DataFrame({"lower_95": [52, 40, 45], "upper_95": [55, 44, 49],
                       "national_value": [50, 50, 47]})
    assert list(data.with_interval_flag(df)["differs_from_national"]) == [True, True, False]
    sizes = pd.DataFrame({"period_key": ["q1"] * 3 + ["q2"] * 3, "lower_bound": [1, 10, 100] * 2,
                          "value": [2.5, 4.7, 6.2, 3.0, 2.9, 6.0]})
    assert data.rises_with_size(sizes) == (1, 2)


# =========================================================================== pages
@pytest.fixture
def gold_lakehouse(bronze_delta, monkeypatch):   # noqa: F811
    """Sample bronze -> silver -> gold in tmp_path (the dashboard reads gold only)."""
    monkeypatch.setattr(sys, "argv", ["x"])
    s.run()
    g.run()


def run_page(page: str, final: bool = False) -> AppTest:
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()
    if final:
        at.toggle(key="final_only").set_value(True).run()
    if page != "overview":
        at.switch_page(f"views/{page}.py")
        at.run()
    return at


@pytest.mark.parametrize("final", [False, True])
@pytest.mark.parametrize("page", PAGES)
def test_every_page_renders_without_errors(gold_lakehouse, page, final):
    at = run_page(page, final)
    assert not at.exception, [e.value for e in at.exception]
    assert any("CC BY 4.0" in c.value for c in at.sidebar.caption)  # credits on every page


def test_overview_shows_the_latest_quarter(gold_lakehouse):
    at = run_page("overview")
    assert at.metric[0].label.startswith("National absence, 2025 Q1")
    assert any(c.value.startswith("Latest period: **2025 Q1** (provisional)") for c in at.caption)


def test_final_only_explains_an_empty_page(gold_lakehouse):
    """The sample absence data is all provisional, so 'final only' leaves nothing."""
    at = run_page("overview", final=True)
    assert any("No final figures to show here yet" in i.value for i in at.info)


def test_pages_warn_without_gold():
    at = run_page("overview")
    assert any("not available yet" in w.value for w in at.warning)
    assert not at.exception


def test_pages_warn_when_gold_tables_come_from_different_runs(gold_lakehouse):
    sector = data.read_table("gold_absence_by_sector", 0)
    publish.write_table(data.GOLD_ROOT / "gold_absence_by_sector", sector.assign(run_id="gold_old"))
    at = run_page("overview")
    assert any("different gold runs" in w.value for w in at.warning)


def test_logo_when_the_file_exists_otherwise_the_note(tmp_path, monkeypatch):
    """st.logo isn't part of the page tree AppTest inspects, so calls to it are recorded."""
    from pipeline.common import config
    import streamlit
    calls = []
    monkeypatch.setattr(streamlit, "logo", lambda image, **kw: calls.append(image))
    monkeypatch.setitem(config.settings, "dashboard", {
        "project_note": "Erasmus University Rotterdam", "logo": str(tmp_path / "logo.svg"),
        "logo_dark": str(tmp_path / "logo_dark.svg")})
    at = run_page("overview")
    assert calls == []                               # configured, but no file yet
    assert any(c.value == "Erasmus University Rotterdam" for c in at.sidebar.caption)

    (tmp_path / "logo.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    at = run_page("overview")
    assert calls == [str(tmp_path / "logo.svg")] and not at.exception
    assert not any(c.value == "Erasmus University Rotterdam" for c in at.sidebar.caption)


def test_dark_logo_in_dark_mode(tmp_path, monkeypatch):
    import charts
    from pipeline.common import config
    import streamlit
    calls = []
    monkeypatch.setattr(streamlit, "logo", lambda image, **kw: calls.append(image))
    monkeypatch.setattr(charts, "theme_mode", lambda: "dark")
    for name in ("logo.svg", "logo_dark.svg"):
        (tmp_path / name).write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    monkeypatch.setitem(config.settings, "dashboard", {
        "logo": str(tmp_path / "logo.svg"), "logo_dark": str(tmp_path / "logo_dark.svg")})
    run_page("overview")
    assert calls == [str(tmp_path / "logo_dark.svg")]


def test_risk_page_shows_the_analyst_what_is_above_national_and_rising():
    """A risk index over 8 quarters (the sample data has too few): the page counts the
    groups above national and rising, names the rising ones and explains the chosen one."""
    from test_gold import F, V, risk_inputs, SETTINGS
    t = risk_inputs({"total": [5.0] * 8, "Q": [7.0] * 8, "I": [3.0] * 8,
                     "S": [5.0 * (0.85 + 0.02 * i) for i in range(8)]}, [F] * 6 + [V, V])
    for name, df in {"gold_risk_index": g.build_risk_index(t, SETTINGS),
                     "gold_risk_index_history": g.build_risk_index_history(t)}.items():
        publish.write_table(g.GOLD_ROOT / name, df.assign(run_id="gold_test"))
    at = run_page("risk")
    assert not at.exception, [e.value for e in at.exception]
    metrics = {m.label: m.value for m in at.metric}
    # (the test periods are labelled with their code; real ones read "2025 Q4")
    assert metrics["Above national, 2025KW04"] == "1 of 3" and metrics["Rising"] == "1"
    assert any(m.value.startswith("**Rising:** S S") for m in at.markdown)
    assert any(m.value.startswith("Q Q: 40% more absence than nationally") for m in at.markdown)
    # final figures only: the index of the latest final quarter, without the provisional ones
    at.toggle(key="final_only").set_value(True).run()
    assert not at.exception
    assert "Above national, 2025KW02" in {m.label for m in at.metric}


def test_dashboard_is_reachable_from_this_machine_only():
    """Requirement NFR-13: started locally, Streamlit listens on 127.0.0.1 only (its default
    is every network interface); in Docker the port is published on 127.0.0.1 only."""
    try:
        import tomllib                       # Python 3.11+
    except ModuleNotFoundError:              # Python 3.10
        import tomli as tomllib
    import yaml
    root = Path(APP).parents[1]
    config = tomllib.loads((root / ".streamlit" / "config.toml").read_text())
    assert config["server"]["address"] == "127.0.0.1"
    compose = yaml.safe_load((root / "docker-compose.yml").read_text())
    ports = compose["services"]["dashboard"]["ports"]
    assert ports and all(str(p).startswith("127.0.0.1:") for p in ports)
