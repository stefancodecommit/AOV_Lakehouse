"""Dagster definitions (orchestration/definitions.py): the graph, schedules, and asset
runs in an ephemeral Dagster instance on tmp_path data (HTTP to CBS faked)."""
import sys
from pathlib import Path

import dagster as dg
import pytest

from pipeline.bronze import cbs as b
from pipeline.common import publish
from conftest import table_info
from test_bronze_logic import DP_81589_FULL, business_record, periods
from test_delta import bronze_delta  # noqa: F401  (fixture)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "orchestration"))
import definitions as d  # noqa: E402

ALL = [d.cbs_bronze_asset, d.sbi_crosswalk_asset, d.kvk_bronze, d.silver_cbs, d.silver_kvk,
       d.gold, d.silver_cbs_checks, d.silver_kvk_checks, d.gold_checks, d.cbs_fresh,
       d.kvk_fresh]


def keys(result, kind="materialization"):
    events = (result.get_asset_materialization_events() if kind == "materialization"
              else result.get_asset_check_evaluations())
    if kind == "materialization":
        return sorted(e.asset_key.to_user_string() for e in events)
    return {(e.asset_key.to_user_string(), e.check_name): e.passed for e in events}


# =========================================================================== the definitions
def test_graph_schedules_and_sensor():
    graph = d.defs.resolve_asset_graph()
    deps = {k.to_user_string(): sorted(p.to_user_string() for p in graph.get(k).parent_keys)
            for k in graph.get_all_asset_keys()}
    assert deps == {"cbs_bronze": [], "sbi_crosswalk": [], "kvk_bronze": [],
                    "silver_cbs": ["cbs_bronze", "sbi_crosswalk"],
                    "silver_kvk": ["kvk_bronze", "silver_cbs"],
                    "gold": ["silver_cbs", "silver_kvk"]}
    crons = {s.name: s.cron_schedule for s in d.defs.schedules}
    assert crons == {"cbs_monthly": "0 6 1 * *", "freshness_monthly": "0 9 1 * *",
                     "maintenance_monthly": "0 3 15 * *"}
    assert d.defs.resolve_job_def("maintenance_job").tags["lakehouse"] == "write"
    assert d.defs.resolve_job_def("cbs_ingest").tags["lakehouse"] == "write"
    assert {s.name for s in d.defs.sensors} == {"rebuild_after_new_data"}


def test_silver_and_gold_wait_for_blocking_checks():
    for asset in (d.silver_cbs, d.silver_kvk, d.gold):
        cond = str(next(iter(asset.automation_conditions_by_key.values())))
        assert "blocking" in cond.lower(), cond


def test_dagster_yaml_limits_writing_runs_to_one():
    import yaml
    cfg = yaml.safe_load((Path(d.__file__).parent / "dagster.yaml").read_text())
    assert cfg["concurrency"]["runs"]["tag_concurrency_limits"] == [
        {"key": "lakehouse", "value": "write", "limit": 1}]


# =========================================================================== bronze assets
@pytest.fixture
def fake_cbs(monkeypatch):
    """One data endpoint (81589NED) with two periods, answered without the internet."""
    monkeypatch.setattr(b, "ENDPOINTS", [b.Endpoint("81589NED", "TypedDataSet", split_by_period=True)])
    monkeypatch.setattr(b, "make_session", lambda: None)

    def fetch_all(session, url, params):
        if url.endswith("/Perioden"):
            return periods("2026KW01", "2026KW02"), 1
        if url.endswith("/DataProperties"):
            return DP_81589_FULL, 1
        if url.endswith("/TableInfos"):
            return [table_info()], 1
        return [business_record(params["$filter"].split("'")[1])], 1

    monkeypatch.setattr(b, "fetch_all", fetch_all)


def test_cbs_bronze_materialises_only_when_something_is_new(fake_cbs):
    first = dg.materialize([d.cbs_bronze_asset])
    assert first.success and keys(first) == ["cbs_bronze"]
    second = dg.materialize([d.cbs_bronze_asset])          # nothing new at CBS
    assert second.success and keys(second) == []


def test_kvk_bronze_needs_the_file_name_typed_as_confirmation(tmp_path):
    from test_kvk_logic import inbox_zip
    inbox_zip("kvk.zip")
    run = lambda confirm: dg.materialize(
        [d.kvk_bronze], raise_on_error=False,
        run_config={"ops": {"kvk_bronze": {"config": {"confirm_file_name": confirm}}}})
    assert not run("YES").success
    ok = run("kvk.zip")
    assert ok.success and keys(ok) == ["kvk_bronze"]


# =========================================================================== silver, gold, checks
def test_silver_and_gold_with_their_checks(bronze_delta, monkeypatch):   # noqa: F811
    monkeypatch.setattr(sys, "argv", ["x"])
    result = dg.materialize([d.silver_cbs, d.gold, d.silver_cbs_checks, d.gold_checks,
                             d.silver_kvk, d.silver_kvk_checks],
                            selection=["silver_cbs", "gold"])
    assert result.success and keys(result) == ["gold", "silver_cbs"]
    checks = keys(result, "checks")
    assert all(passed for (asset, name), passed in checks.items()
               if name != "no_unknown_characteristics"), checks
    assert ("silver_cbs", "keys_in_dimensions") in checks and ("gold", "unique_keys") in checks


def test_failing_blocking_check_stops_gold(bronze_delta, monkeypatch):   # noqa: F811
    from checks import CheckResult
    import checks.layers as layers
    real = layers.check_silver_cbs
    monkeypatch.setattr(layers, "check_silver_cbs", lambda t: [
        r if r.name != "unique_keys" else CheckResult("unique_keys", False, "duplicates")
        for r in real(t)])
    result = dg.materialize([d.silver_cbs, d.gold, d.silver_cbs_checks, d.gold_checks,
                             d.silver_kvk], selection=["silver_cbs", "gold"],
                            raise_on_error=False)
    assert keys(result) == ["silver_cbs"]                   # gold never ran
    assert keys(result, "checks")[("silver_cbs", "unique_keys")] is False


def test_freshness_checks_warn_without_failing(bronze_delta):   # noqa: F811
    result = dg.materialize(ALL, selection=dg.AssetSelection.checks(d.cbs_fresh, d.kvk_fresh))
    checks = keys(result, "checks")
    assert result.success
    assert checks[("kvk_bronze", "kvk_fresh")] is False       # no KVK snapshot loaded here


# =========================================================================== maintenance job
def test_maintenance_job_runs(bronze_delta):   # noqa: F811
    publish.write_table(b.BRONZE_ROOT.parent / "silver" / "x", __import__("pandas").DataFrame({"v": [1]}))
    result = d.defs.resolve_job_def("maintenance_job").execute_in_process()
    assert result.success



# =========================================================================== automation
# Dagster requests nothing on the very first evaluation of a condition (it only reacts to
# changes it sees after that), so each test evaluates once, changes something, and
# evaluates again with the remembered state (the cursor) - as the sensor does every 30 s.
def evaluate(instance, cursor=None):
    return dg.evaluate_automation_conditions(d.defs, instance, cursor=cursor)


def test_new_data_triggers_the_next_layer_only_when_upstream_is_known(bronze_delta):   # noqa: F811
    instance = dg.DagsterInstance.ephemeral()
    d.register_existing_data.execute_in_process(instance=instance)   # first-time setup
    # (the sample lakehouse has a crosswalk log but no CBS ingestion log: only the
    # crosswalk is registered; CBS bronze is materialized once more, as a first load)
    instance.report_runless_asset_event(dg.AssetMaterialization("cbs_bronze"))
    first = evaluate(instance)
    instance.report_runless_asset_event(dg.AssetMaterialization("cbs_bronze"))  # new CBS data
    result = evaluate(instance, first.cursor)
    # both bronze inputs are known: silver_cbs is due; gold waits for its own upstream
    assert result.get_num_requested(dg.AssetKey("silver_cbs")) == 1
    assert result.get_num_requested(dg.AssetKey("gold")) == 0
    assert evaluate(instance, result.cursor).total_requested == 0   # requested once, not again


def test_without_registration_silver_never_starts(bronze_delta):   # noqa: F811
    """The gap register_existing_data fixes: new cbs_bronze data alone isn't enough
    while sbi_crosswalk was never materialized in Dagster."""
    instance = dg.DagsterInstance.ephemeral()
    first = evaluate(instance)
    instance.report_runless_asset_event(dg.AssetMaterialization("cbs_bronze"))
    assert evaluate(instance, first.cursor).get_num_requested(dg.AssetKey("silver_cbs")) == 0


def test_failed_blocking_check_stops_automation(bronze_delta, monkeypatch):   # noqa: F811
    """Gold needs new silver data AND passing blocking checks on all its inputs."""
    from checks import CheckResult
    import checks.layers as layers
    instance = dg.DagsterInstance.ephemeral()
    for key in ("cbs_bronze", "sbi_crosswalk", "kvk_bronze"):
        instance.report_runless_asset_event(dg.AssetMaterialization(key))
    # silver_kvk built and checked (the sample has no KVK data, so both are stand-ins);
    # without checks that passed, gold would wait for silver_kvk too
    monkeypatch.setattr(d.kvk_silver, "run", lambda: "silver_kvk_test")
    monkeypatch.setattr(layers, "check_silver_kvk",
                        lambda df: [CheckResult(n, True) for n in
                                    ("tables_not_empty", "one_run_per_snapshot",
                                     "latest_snapshot_present", "valid_sbi_sections")])
    dg.materialize([d.silver_kvk, d.silver_kvk_checks], instance=instance)
    first = evaluate(instance)
    real = layers.check_silver_cbs
    monkeypatch.setattr(layers, "check_silver_cbs", lambda t: [
        CheckResult(r.name, False, "broken") if r.name == "unique_keys" else r for r in real(t)])
    dg.materialize([d.silver_cbs, d.silver_cbs_checks], instance=instance, raise_on_error=False)
    # silver_cbs is new, but its blocking check failed: gold must not be requested
    assert evaluate(instance, first.cursor).get_num_requested(dg.AssetKey("gold")) == 0

    monkeypatch.setattr(layers, "check_silver_cbs", real)          # the check passes again
    first = evaluate(instance)
    dg.materialize([d.silver_cbs, d.silver_cbs_checks], instance=instance)
    assert evaluate(instance, first.cursor).get_num_requested(dg.AssetKey("gold")) == 1


# =========================================================================== real dagster command
def test_dagster_command_runs_a_step_in_its_own_process(bronze_delta, tmp_path):   # noqa: F811
    """Dagster runs each step in a separate process; modules the scripts import late
    (publish, runlog, checks.*) must still be found there. In-process tests can't show
    this, so this one runs the real `dagster` command against the temporary lakehouse."""
    import os
    import subprocess
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "DAGSTER_HOME": str(tmp_path / "dagster_home"),
           "LAKEHOUSE__PATHS__LAKEHOUSE": str(tmp_path)}
    (tmp_path / "dagster_home").mkdir()
    out = subprocess.run([sys.executable, "-m", "dagster", "asset", "materialize",
                          "-f", str(root / "orchestration/definitions.py"),
                          "--select", "silver_cbs"],
                         cwd=tmp_path, env=env, capture_output=True, text=True, timeout=300)
    assert out.returncode == 0, out.stderr[-3000:]
    assert "RUN_SUCCESS" in out.stderr + out.stdout
    assert (tmp_path / "silver" / "fct_absence_rate" / "_delta_log").exists()
