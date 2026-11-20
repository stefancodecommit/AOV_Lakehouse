"""pipeline/common/config.py: config.yaml < config.local.yaml < LAKEHOUSE__* environment variables."""
from pathlib import Path

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
        "metadata": ["80072NED/Perioden", "81589NED"]})
    assert [(e.table_id, e.entity, e.split_by_period) for e in eps] == [
        ("80072NED", "TypedDataSet", True), ("80072NED", "Perioden", False),
        ("81589NED", "", False)]


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


def test_docker_images_are_pinned_and_local_config_stays_out_of_the_image():
    """Like the Python packages, every image is an exact version: a moving tag (latest,
    3.12-slim, 16-alpine) gives a different image under the same name later. And the
    machine-specific config.local.yaml is never copied into the image."""
    import re

    import yaml
    root = Path(__file__).resolve().parents[1]
    compose = yaml.safe_load((root / "docker-compose.yml").read_text())
    images = [s["image"] for s in compose["services"].values()
              if "image" in s and "build" not in s]           # our own image is built here
    images += re.findall(r"^FROM (\S+)", (root / "Dockerfile").read_text(), re.M)
    assert images
    for image in images:
        tag = image.rsplit(":", 1)[1] if ":" in image else "latest"
        # an exact version has every part: 3.12.15 (3.12 is a moving tag), but for
        # Postgres two parts are exact (16.15; 16 alone moves)
        parts = 2 if image.startswith("postgres:") else 3
        assert re.match(rf"\d+(\.\d+){{{parts - 1}}}(-|$)", tag), f"{image}: not an exact version"
    ignored = (root / ".dockerignore").read_text().splitlines()
    assert "config.local.yaml" in ignored
