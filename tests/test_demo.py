"""The demo launcher (tools/demo.py): every button on its start page leads somewhere that
exists, and the dashboard and viewer open what a link asks for."""
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from streamlit.testing.v1 import AppTest

from pipeline.bronze import kvk_agent
from tools import demo, lineage

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "orchestration"))
import definitions as dagster_defs  # noqa: E402


def links():
    return [url for step in demo.steps() for _, url in step["links"]]


def test_the_start_page_has_a_button_per_link_and_a_dot_per_service(tmp_path):
    page = demo.start_page(tmp_path / "start.html").read_text()
    assert len(demo.steps()) == 5                       # the five steps of docs/demo.md
    for url in links():
        assert f'href="{url.replace("&", "&amp;")}"' in page
    for service in demo.SERVICES:
        assert f'data-url="{service.url}{service.health}"' in page


def test_every_link_points_at_something_that_exists():
    pages = {p.stem for p in (ROOT / "dashboard" / "views").glob("*.py")}
    assets = {k.to_user_string() for k in dagster_defs.defs.resolve_asset_graph().get_all_asset_keys()}
    explorer = lineage.explorer_data()
    explorer_ids = {n["id"] for n in (explorer["nodes"].values()
                                      if isinstance(explorer["nodes"], dict) else explorer["nodes"])}
    for url in links():
        u = urlparse(url)
        if u.scheme == "file":
            assert Path(u.path).exists() and u.fragment in explorer_ids, url
        elif u.port == 8501:
            assert u.path.strip("/") in pages, url
        elif u.port == 8502:
            q = parse_qs(u.query)
            assert q["layer"][0] in demo_layers() and q["view"][0] in ("Rows", "Data quality",
                                                                        "History"), url
        elif u.port == 3000 and u.path.startswith("/assets/"):
            assert u.path.split("/")[2] in assets, url
    # the KVK table the viewer opens is where bronze KVK writes it
    lakehouse = kvk_agent.DATA_TABLE.parents[2]           # .../bronze/kvk/<table>
    assert demo.KVK_TABLE == str(kvk_agent.DATA_TABLE.relative_to(lakehouse))


def demo_layers():
    from tools import lakehouse_viewer
    return lakehouse_viewer.LAYERS


def test_a_link_opens_one_sector_on_the_risk_page():
    from pipeline.common import publish
    from pipeline.gold import build as g
    from test_dashboard import APP
    from test_gold import SETTINGS, F, V, risk_inputs
    t = risk_inputs({"total": [5.0] * 8, "Q": [7.0] * 8, "I": [3.0] * 8}, [F] * 6 + [V, V])
    for name, df in {"gold_risk_index": g.build_risk_index(t, SETTINGS),
                     "gold_risk_index_history": g.build_risk_index_history(t)}.items():
        publish.write_table(g.GOLD_ROOT / name, df.assign(run_id="gold_test"))
    at = AppTest.from_file(APP, default_timeout=60)
    at.query_params["group"] = "I"          # not the first in the list (Q is highest)
    at.run()
    at.switch_page("views/risk.py").run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.selectbox[-1].value.startswith("I ")


def test_a_link_opens_a_table_and_view_in_the_viewer():
    from test_lakehouse_viewer import APP, silver_table, two_versions
    two_versions()
    silver_table()
    at = AppTest.from_file(APP, default_timeout=30)
    at.query_params.update(layer="silver", table="silver/dim_sector", view="Data quality")
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.header[0].value == "silver/dim_sector"
    assert at.radio(key="view").value == "Data quality"


def test_the_start_page_opens_in_the_default_browser_not_via_browser_setting(monkeypatch, tmp_path):
    """$BROWSER can point at an editor (VS Code's terminal), which shows HTML as source;
    on Linux the page goes to xdg-open, the desktop's default browser."""
    import shutil
    import subprocess
    calls = []
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kw: calls.append(cmd))
    monkeypatch.setenv("BROWSER", "/some/editor/helper")
    demo.open_in_browser(tmp_path / "start.html")
    assert calls == [["xdg-open", str(tmp_path / "start.html")]]
