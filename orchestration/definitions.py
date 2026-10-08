"""
Dagster definitions: runs the pipeline on a schedule, rebuilds a layer only when new
data arrived upstream, checks every layer, and does the monthly maintenance.

Start the Dagster UI from the project folder:
    DAGSTER_HOME=$PWD/orchestration .venv/bin/dagster dev -f orchestration/definitions.py
and open http://localhost:3000. DAGSTER_HOME holds Dagster's own state (run history,
schedules) and dagster.yaml; its state folders are not in git.

The data logic lives in the existing scripts; this file only wires them together.

Assets (each one a dataset Dagster keeps up to date)
------
cbs_bronze      cbs_bronze.run(): monthly. Materialises only when CBS had new data
                (or a changed TableInfos); otherwise it is skipped and nothing
                downstream runs.
sbi_crosswalk   reference_bronze: the SBI 2008 -> 2025 file, monthly. Materialises
                only when a new version of the file was loaded.
kvk_bronze      kvk_bronze_agent: started by a person in the UI ("Materialize", with
                the file name typed as confirmation, like typing YES). Never scheduled.
silver_cbs      after new cbs_bronze or sbi_crosswalk data
silver_kvk      after new kvk_bronze data or a new silver_cbs (it uses the crosswalk)
gold            after new silver_cbs or silver_kvk
The dashboard needs no asset: it reads gold and notices a new version by itself.

"After new data" is Dagster's eager automation condition, combined with "all blocking
checks of the upstream assets passed": a layer with a failing check is never built on.

Checks
------
silver_cbs, silver_kvk, gold   blocking checks (checks/layers.py): tables filled, one
                               run_id, unique keys, keys present in their dimension
cbs_bronze, kvk_bronze         freshness (checks/freshness.py): warnings only. They run
                               in a separate monthly job, because a check attached to
                               an asset doesn't run when that asset is skipped, and a
                               stale source is exactly a skipped asset.

Schedules and jobs (the times are in config.yaml, section schedules)
------------------
cbs_monthly        1st of the month 06:00: cbs_bronze and sbi_crosswalk
freshness_monthly  1st of the month 09:00: the freshness checks
maintenance        15th of the month 03:00: quarantine cleanup, KVK retention, Delta
                   optimize/vacuum (maintenance.py)
Every run that writes to the lakehouse has the tag lakehouse=write; dagster.yaml allows
one such run at a time, so maintenance never overlaps an ingestion.

First time on an existing lakehouse: run the job register_existing_data once (see
register_existing_bronze), then materialize silver_cbs, silver_kvk and gold once.

Failures are shown in the Dagster UI (decided 2026-10-06: no email or Slack alerts).
"""
# (no "from __future__ import annotations": Dagster reads the type annotations at runtime)

import getpass
import json
import sys
from pathlib import Path

import dagster as dg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # the project's modules

import cbs_bronze  # noqa: E402
import cbs_silver  # noqa: E402
import gold as gold_layer  # noqa: E402
import kvk_bronze_agent  # noqa: E402
import kvk_silver  # noqa: E402
import lineage  # noqa: E402
import maintenance  # noqa: E402
import reference_bronze  # noqa: E402
from checks import CheckResult  # noqa: E402
from config import settings  # noqa: E402

# Dagster runs each step in its own process and restores Python's search path after
# loading this file, so the project folder added above is gone by the time a step
# runs. The scripts import some modules inside functions (late, when needed); importing
# them here, while the folder is still on the path, keeps them available (Python
# remembers imported modules).
import checks.cbs  # noqa: E402,F401
import checks.freshness  # noqa: E402,F401
import checks.kvk  # noqa: E402,F401
import checks.layers  # noqa: E402,F401
import checks.quarantine  # noqa: E402,F401
import publish  # noqa: E402,F401
import runlog  # noqa: E402,F401

WRITE = {"lakehouse": "write"}       # see dagster.yaml: one writing run at a time
# rebuild after new upstream data, but only when every upstream blocking check passed
AFTER_NEW_DATA = (dg.AutomationCondition.eager()
                  & dg.AutomationCondition.all_deps_blocking_checks_passed())


def check_result(r: CheckResult, asset: str) -> dg.AssetCheckResult:
    """One of our CheckResults as a Dagster check result (errors fail, warnings warn)."""
    return dg.AssetCheckResult(
        asset_key=asset, check_name=r.name, passed=r.passed,
        severity=dg.AssetCheckSeverity.ERROR if r.severity == "error"
        else dg.AssetCheckSeverity.WARN,
        metadata={"detail": r.detail, "examples": json.dumps(r.examples, default=str)})


# =========================================================================== bronze
@dg.multi_asset(specs=[dg.AssetSpec(
    "cbs_bronze", group_name="bronze", skippable=True,
    description="CBS OData -> bronze (cbs_bronze.py). Skipped when CBS had nothing new.")])
def cbs_bronze_asset(context: dg.AssetExecutionContext):
    summary = cbs_bronze.run(only=None, force=False, dry_run=False)
    if summary["failed"] and not summary["fetched"]:
        raise dg.Failure("Every CBS request that was needed failed (see the run log); "
                         "they are retried next run.", metadata=summary)
    if summary["fetched"] or summary["tableinfos_changed"]:
        yield dg.MaterializeResult(asset_key="cbs_bronze", metadata=summary)
    else:
        context.log.info("No new CBS data: nothing downstream needs to run. %s", summary)


@dg.multi_asset(specs=[dg.AssetSpec(
    "sbi_crosswalk", group_name="bronze", skippable=True,
    description="CBS SBI 2008 -> 2025 crosswalk -> bronze (reference_bronze.py). "
                "Skipped when the file is unchanged.")])
def sbi_crosswalk_asset(context: dg.AssetExecutionContext):
    url = reference_bronze.CROSSWALK_URL
    report = reference_bronze.ingest(reference_bronze.download(url), url)
    if report["status"] == "FAILED":
        raise dg.Failure("The crosswalk file failed its checks (it is in quarantine).",
                         metadata={"errors": "; ".join(report["errors"])})
    if report["status"] == "INGESTED":
        yield dg.MaterializeResult(asset_key="sbi_crosswalk",
                                   metadata={k: str(v) for k, v in report.items()})
    else:
        context.log.info("Crosswalk unchanged: %s", report.get("reason"))


class KvkUpload(dg.Config):
    """Launch settings for kvk_bronze (filled in the UI's launchpad)."""
    file_name: str = ""             # a file in inbox/kvk/; empty = the newest new file
    confirm_file_name: str = ""     # type the exact file name to confirm the write


@dg.asset(group_name="bronze", description="KVK zip in inbox/kvk/ -> bronze. Started by a "
          "person; type the file name in confirm_file_name to confirm the write.")
def kvk_bronze(context: dg.AssetExecutionContext, config: KvkUpload) -> dg.MaterializeResult:
    name = config.file_name or kvk_bronze_agent.newest_new_inbox_file()
    if not name:
        raise dg.Failure(f"No new KVK file in {kvk_bronze_agent.INBOX}")
    result = kvk_bronze_agent.ingest_confirmed_file(
        name, config.confirm_file_name, confirmed_by=f"dagster:{getpass.getuser()}")
    if result["status"] != "INGESTED":
        raise dg.Failure(f"KVK file {name} not loaded: {result['status']}",
                         metadata={"result": json.dumps(result, default=str)[:2000]})
    return dg.MaterializeResult(metadata={k: str(v) for k, v in result.items()})


# =========================================================================== silver & gold
def _published(pipeline_run) -> dg.MaterializeResult:
    """Run a silver/gold pipeline (publish.run_pipeline) and report its run_id."""
    run_id = pipeline_run()
    return dg.MaterializeResult(metadata={"run_id": run_id})


# Column lineage (lineage.py): in the free Dagster UI visible as the asset's column list
# (each description starts with "From <bronze field>"); the lineage graph is Dagster+ only
CBS_SILVER_TABLES = set(lineage.SILVER_LINEAGE) - lineage.SILVER_KVK_TABLES


@dg.asset(group_name="silver", deps=["cbs_bronze", "sbi_crosswalk"],
          automation_condition=AFTER_NEW_DATA, description="cbs_silver.py: 11 tables",
          metadata=lineage.dagster_lineage(CBS_SILVER_TABLES))
def silver_cbs() -> dg.MaterializeResult:
    return _published(cbs_silver.run)


@dg.asset(group_name="silver", deps=["kvk_bronze", "silver_cbs"],
          automation_condition=AFTER_NEW_DATA, description="kvk_silver.py: kvk_company",
          metadata=lineage.dagster_lineage(lineage.SILVER_KVK_TABLES))
def silver_kvk() -> dg.MaterializeResult:
    return _published(kvk_silver.run)


@dg.asset(group_name="gold", deps=["silver_cbs", "silver_kvk"],
          automation_condition=AFTER_NEW_DATA, description="gold.py: 5 dashboard tables")
def gold() -> dg.MaterializeResult:
    return _published(gold_layer.run)


# =========================================================================== checks
def _read(root: Path, names) -> dict:
    from deltalake import DeltaTable
    import pandas as pd
    return {n: (DeltaTable(str(root / n)).to_pandas() if (root / n / "_delta_log").exists()
                else pd.DataFrame()) for n in names}


@dg.multi_asset_check(specs=[dg.AssetCheckSpec(n, asset="silver_cbs", blocking=n != "no_unknown_characteristics")
                             for n in ["tables_not_empty", "one_run_id", "unique_keys",
                                       "keys_in_dimensions", "no_unknown_characteristics"]])
def silver_cbs_checks():
    from checks.layers import SILVER_CBS_KEYS, check_silver_cbs
    results = check_silver_cbs(_read(cbs_silver.SILVER_ROOT, SILVER_CBS_KEYS))
    keys = [r for r in results if r.name.endswith(("_in_dim_period", "_in_dim_sector",
                                                   "_in_dim_company_size"))]
    merged = CheckResult("keys_in_dimensions", all(r.passed for r in keys),
                         "; ".join(r.name for r in keys if not r.passed) or
                         f"{len(keys)} key comparisons passed",
                         [e for r in keys for e in r.examples][:5])
    for r in [r for r in results if r not in keys] + [merged]:
        yield check_result(r, "silver_cbs")


@dg.multi_asset_check(specs=[dg.AssetCheckSpec(n, asset="silver_kvk", blocking=True)
                             for n in ["tables_not_empty", "one_run_id",
                                       "latest_snapshot_present", "valid_sbi_sections"]])
def silver_kvk_checks():
    from checks.layers import check_silver_kvk
    for r in check_silver_kvk(_read(kvk_silver.SILVER_ROOT, ["kvk_company"])["kvk_company"]):
        yield check_result(r, "silver_kvk")


@dg.multi_asset_check(specs=[dg.AssetCheckSpec(n, asset="gold", blocking=True)
                             for n in ["tables_not_empty", "one_run_id", "unique_keys"]])
def gold_checks():
    from checks.layers import GOLD_KEYS, check_gold
    for r in check_gold(_read(gold_layer.GOLD_ROOT, GOLD_KEYS)):
        yield check_result(r, "gold")


@dg.asset_check(asset="cbs_bronze", description="Newest CBS period recent enough (warning)")
def cbs_fresh() -> dg.AssetCheckResult:
    from checks.freshness import check_cbs_freshness
    latest, stopped = bronze_cbs_state()
    results = check_cbs_freshness(latest, stopped)
    stale = [r for r in results if not r.passed]
    return dg.AssetCheckResult(
        passed=not stale, severity=dg.AssetCheckSeverity.WARN,
        metadata={r.name: r.detail for r in results} | {"stopped_tables": ", ".join(sorted(stopped))})


@dg.asset_check(asset="kvk_bronze", description="Newest KVK snapshot recent enough (warning)")
def kvk_fresh() -> dg.AssetCheckResult:
    from checks.freshness import check_kvk_freshness
    r = check_kvk_freshness(latest_kvk_snapshot(), int(settings["kvk"]["max_snapshot_age_days"]))
    return dg.AssetCheckResult(passed=r.passed, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"detail": r.detail})


def bronze_cbs_state() -> tuple[dict[str, str], set[str]]:
    """(table_id -> newest period in bronze, tables CBS has stopped) for the freshness check."""
    from deltalake import DeltaTable
    from checks import warnings
    from checks.cbs import check_table_lifecycle
    latest, stopped = {}, set()
    for ep in cbs_bronze.ENDPOINTS:
        path = cbs_bronze.BRONZE_ROOT / ep.name
        if not ep.split_by_period or not (path / "_delta_log").exists():
            continue
        periods = DeltaTable(str(path)).to_pandas(columns=["_period"])["_period"].dropna()
        if not periods.empty:
            latest[ep.table_id] = periods.max()
        infos = cbs_bronze.BRONZE_ROOT / cbs_bronze.Endpoint(ep.table_id, "TableInfos").name
        if (infos / "_delta_log").exists():
            df = DeltaTable(str(infos)).to_pandas().sort_values("_ingested_at")
            if any(w.name == "table_not_stopped" for w in
                   warnings(check_table_lifecycle(json.loads(df["raw_json"].iloc[-1])))):
                stopped.add(ep.table_id)
    return latest, stopped


def latest_kvk_snapshot():
    """Day the newest successful KVK batch was ingested, or None."""
    log = maintenance._read_log(kvk_bronze_agent.LOG_TABLE)
    return None if log.empty else log["ingested_at"].max().date()


# =========================================================================== maintenance
@dg.op
def clean_quarantine(context: dg.OpExecutionContext) -> None:
    for folder, reason in maintenance.clean_quarantine():
        context.log.info("Removed %s (%s)", folder, reason)


@dg.op
def kvk_retention(context: dg.OpExecutionContext) -> None:
    context.log.info("Retired KVK batches: %s", maintenance.kvk_retention())


@dg.op
def delta_optimize_vacuum(context: dg.OpExecutionContext) -> None:
    files = maintenance.optimize_and_vacuum()
    context.log.info("Vacuumed %d old files in %d tables", sum(files.values()), len(files))


@dg.job(tags=WRITE, description="Monthly: quarantine cleanup, KVK retention, Delta optimize/vacuum")
def maintenance_job():
    clean_quarantine()
    kvk_retention()
    delta_optimize_vacuum()


# =========================================================================== first-time setup
@dg.op(description="Record bronze data that already exists as materialized in Dagster")
def register_existing_bronze(context: dg.OpExecutionContext) -> None:
    """Dagster only rebuilds a layer automatically when every upstream asset was
    materialized (in Dagster) at least once. Bronze loaded before Dagster was set up
    counts as "missing", which would block silver forever; run this job once to record
    that data as materialized. Assets without data are left alone."""
    existing = {
        "cbs_bronze": cbs_bronze.LOG_TABLE,
        "sbi_crosswalk": reference_bronze.LOG_TABLE,
        "kvk_bronze": kvk_bronze_agent.LOG_TABLE,
    }
    for key, log_table in existing.items():
        if (log_table / "_delta_log").exists():
            context.log_event(dg.AssetMaterialization(
                asset_key=key, description="existing bronze data registered",
                metadata={"log_table": str(log_table)}))
        else:
            context.log.info("%s: no data yet, not registered", key)


@dg.job(description="Run once after setting up Dagster on an existing lakehouse")
def register_existing_data():
    register_existing_bronze()


# =========================================================================== jobs & schedules
cbs_ingest_job = dg.define_asset_job(
    "cbs_ingest", tags=WRITE,
    # without the freshness checks: they have their own job (see the module docstring)
    selection=dg.AssetSelection.assets("cbs_bronze", "sbi_crosswalk").without_checks(),
    description="Check CBS and the crosswalk for new data")
freshness_job = dg.define_asset_job(
    "freshness", selection=dg.AssetSelection.checks(cbs_fresh, kvk_fresh),
    description="Freshness warnings for CBS and KVK (reads only)")

defs = dg.Definitions(
    assets=[cbs_bronze_asset, sbi_crosswalk_asset, kvk_bronze, silver_cbs, silver_kvk, gold],
    asset_checks=[silver_cbs_checks, silver_kvk_checks, gold_checks, cbs_fresh, kvk_fresh],
    jobs=[cbs_ingest_job, freshness_job, maintenance_job, register_existing_data],
    schedules=[                      # times: config.yaml, section schedules
        dg.ScheduleDefinition(name=name, job=job,
                              cron_schedule=settings["schedules"][config_key],
                              execution_timezone=settings["schedules"]["timezone"],
                              default_status=dg.DefaultScheduleStatus.RUNNING)
        for name, config_key, job in [
            ("cbs_monthly", "cbs_ingest", cbs_ingest_job),
            ("freshness_monthly", "freshness", freshness_job),
            ("maintenance_monthly", "maintenance", maintenance_job)]
    ],
    sensors=[dg.AutomationConditionSensorDefinition(
        "rebuild_after_new_data", target=dg.AssetSelection.all(), run_tags=WRITE,
        default_status=dg.DefaultSensorStatus.RUNNING)],
)
