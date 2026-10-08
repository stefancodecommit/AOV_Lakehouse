"""pipeline/common/config.py: config.yaml < config.local.yaml < LAKEHOUSE__* environment variables."""
import pytest

from pipeline.bronze import cbs as b
from pipeline.common import config as c

BASE = """
paths:
  lakehouse: lakehouse
cbs:
  start_year: 2020
  period_preference: [KW, JJ]
ollama:
  host: http://localhost:11434
"""


@pytest.fixture
def cfg_file(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(BASE)
    return path


def test_loads_the_file(cfg_file):
    s = c.load(cfg_file, environ={})
    assert s["cbs"]["start_year"] == 2020 and s["cbs"]["period_preference"] == ["KW", "JJ"]


def test_environment_overrides_with_yaml_types(cfg_file):
    s = c.load(cfg_file, environ={"LAKEHOUSE__CBS__START_YEAR": "2022",
                                  "LAKEHOUSE__CBS__PERIOD_PREFERENCE": "[JJ]",
                                  "LAKEHOUSE__OLLAMA__HOST": "http://ollama:11434",
                                  "OTHER_VARIABLE": "ignored"})
    assert s["cbs"]["start_year"] == 2022 and s["cbs"]["period_preference"] == ["JJ"]
    assert s["ollama"]["host"] == "http://ollama:11434"
    assert s["paths"]["lakehouse"] == "lakehouse"          # untouched keys stay


def test_local_file_overrides_and_environment_wins(cfg_file):
    (cfg_file.parent / "config.local.yaml").write_text("cbs:\n  start_year: 2021\n")
    assert c.load(cfg_file, environ={})["cbs"]["start_year"] == 2021
    s = c.load(cfg_file, environ={"LAKEHOUSE__CBS__START_YEAR": "2023"})
    assert s["cbs"]["start_year"] == 2023


def test_lakehouse_config_points_at_another_file(cfg_file, tmp_path):
    other = tmp_path / "other" / "config.yaml"
    other.parent.mkdir()
    other.write_text(BASE.replace("2020", "2019"))
    assert c.load(environ={"LAKEHOUSE_CONFIG": str(other)})["cbs"]["start_year"] == 2019


@pytest.mark.parametrize("source, error", [
    ("env", "unknown setting 'cbs.start_yaer'"),
    ("local", "unknown setting 'paths.lakehous'"),
    ("not_a_mapping", "'cbs' must be a mapping"),
])
def test_typos_are_errors(cfg_file, source, error):
    environ = {}
    if source == "env":
        environ = {"LAKEHOUSE__CBS__START_YAER": "2022"}
    elif source == "local":
        (cfg_file.parent / "config.local.yaml").write_text("paths:\n  lakehous: /data\n")
    else:
        environ = {"LAKEHOUSE__CBS": "2022"}
    with pytest.raises(c.ConfigError, match=error.replace(".", r"\.")):
        c.load(cfg_file, environ=environ)


def test_project_path():
    assert c.project_path("lakehouse") == c.PROJECT_DIR / "lakehouse"
    assert str(c.project_path("/data/lakehouse")) == "/data/lakehouse"


# =========================================================================== the real config.yaml
def test_real_config_loads_without_overrides():
    s = c.load(c.PROJECT_DIR / "config.yaml", environ={})
    assert {"paths", "cbs", "kvk", "reference", "ollama"} <= set(s)


def test_endpoints_from_config():
    eps = b.endpoints_from_config({
        "tables": ["80072NED"],
        "metadata": ["80072NED/Perioden", "81589NED",
                     {"table_id": "81644NED", "entity": "TypedDataSet", "period_op": "ge",
                      "period_value": "2025JJ00"}]})
    assert [(e.table_id, e.entity, e.split_by_period) for e in eps] == [
        ("80072NED", "TypedDataSet", True), ("80072NED", "Perioden", False),
        ("81589NED", "", False), ("81644NED", "TypedDataSet", False)]
    assert eps[3].odata_filter() == "Perioden ge '2025JJ00'"


def test_exit_pause_prevents_the_deltalake_crash_at_exit(tmp_path):
    """See config.EXIT_PAUSE_SECONDS: without the pause, a script that reads a Delta
    table and exits right away aborts in about half of all runs (exit code 134)."""
    import subprocess
    import sys
    import pyarrow as pa
    from deltalake import write_deltalake
    write_deltalake(str(tmp_path / "t"), pa.table({"v": list(range(1000))}))
    script = ("import sys; sys.path.insert(0, %r); import pipeline.common.config\n"
              "from deltalake import DeltaTable\n"
              "DeltaTable(%r).to_pyarrow_table()\n") % (str(c.PROJECT_DIR), str(tmp_path / "t"))
    codes = [subprocess.run([sys.executable, "-c", script], capture_output=True).returncode
             for _ in range(10)]
    assert codes == [0] * 10, codes
