"""
Silver layer for the KVK open dataset: lakehouse/silver/kvk_company.

One row per company per snapshot (every ingested KVK batch), typed, with the company's
sector taken from its main activity ("Hoofdactiviteiten"). Edge cases are marked with
flag columns instead of being resolved by assumptions: original values stay as they
are, derived columns are filled only where the answer is unambiguous, and the flags say
why one is empty. Gold and the dashboard can filter on the flags.

Columns
-------
snapshot_date, batch_id, is_latest   which upload the row comes from (snapshot_date =
                                     the day the batch was ingested); count companies
                                     with is_latest, never across snapshots
source_row_number                    row in the uploaded CSV
start_date, has_invalid_start_date   date; the flag is set when a non-empty value
                                     doesn't parse (the original is in bronze)
is_active                            True/False from J/N, null otherwise
legal_form, insolvency_code, postcode_region, member_state   as given, empty -> null
main_activity_codes                  the original list, e.g. "47741" or "6420,6619"
main_activity_count                  number of codes in it
has_multiple_main_activities, has_no_main_activity, is_kvk_subcode (a 6-digit KVK
                                     sub-code that is in neither SBI list)
sbi_version                          2008 | 2025 | both (the code exists in both lists,
                                     so the number alone doesn't say which) | unknown |
                                     mixed (several main codes of different versions)
sbi_mapping                          how the main code(s) reach SBI 2008:
                                     sbi2008 (already SBI 2008) | 1:1 | merge (the 2025
                                     code came from several 2008 codes) |
                                     ambiguous_version (code in both lists) |
                                     unknown_code | none (no main activity)
sbi2008_candidates                   number of SBI 2008 codes the main code(s) can be
sbi2008_code                         filled only when there is exactly one candidate
sbi2008_division, sbi2008_section    filled only when all candidates share it; join with
                                     CBS dim_sector on sbi_code (level division/section)

Uses silver map_sbi2008_sbi2025 (run pipeline/silver/cbs.py first).

Incremental runs
----------------
KVK grows: every kept upload is a snapshot of ~1.9 million rows. So kvk_company is not
rebuilt every run (memory would grow with every month kept) but brought in line with
bronze: a run compares the successful batches in bronze/kvk/_batch_log with the batches
in kvk_company, builds only the new ones and deletes the ones maintenance retired, one
snapshot at a time (memory: about one snapshot). is_latest then moves in one commit.
Everything is rebuilt, still one snapshot at a time and newest first, when a rule that
applies to all snapshots changed: the SBI crosswalk's content (its fingerprint is kept
in the run log) or LOGIC_VERSION below, or with --full. A run that stops halfway is
finished by the next one: a snapshot written by a run that didn't succeed is rebuilt.

run_id: each snapshot carries the run_id of the run that built it, so the table holds
several run_ids (checked: one per snapshot). The run log records what each run did
(mode, reason, batches added and removed). The table is partitioned by batch_id.

Usage
-----
    python -m pipeline.silver.kvk            # add new uploads, remove retired ones
    python -m pipeline.silver.kvk --full     # rebuild every kept upload
    python -m pipeline.silver.kvk --preview  # what the next run would do, write nothing
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field

import pandas as pd

from checks.kvk import match_columns
from pipeline.common.config import project_path, settings

LAKEHOUSE = project_path(settings["paths"]["lakehouse"])
KVK_BRONZE = LAKEHOUSE / "bronze" / "kvk"
SILVER_ROOT = LAKEHOUSE / "silver"
RUN_LOG = LAKEHOUSE / "_run_log"
PIPELINE = "silver_kvk"
REQUIRED_COLUMNS = {k: set(v) for k, v in settings["kvk"]["required_columns"].items()}
OPTIONAL_COLUMNS = {k: set(v) for k, v in settings["kvk"]["optional_columns"].items()}

log = logging.getLogger("kvk_silver")

# SBI 2008 (= NACE Rev. 2) divisions per section
_SECTION_RANGES = [("A", 1, 3), ("B", 5, 9), ("C", 10, 33), ("D", 35, 35), ("E", 36, 39),
                   ("F", 41, 43), ("G", 45, 47), ("H", 49, 53), ("I", 55, 56), ("J", 58, 63),
                   ("K", 64, 66), ("L", 68, 68), ("M", 69, 75), ("N", 77, 82), ("O", 84, 84),
                   ("P", 85, 85), ("Q", 86, 88), ("R", 90, 93), ("S", 94, 96), ("T", 97, 98),
                   ("U", 99, 99)]
DIVISION_SECTION = {f"{d:02d}": s for s, lo, hi in _SECTION_RANGES for d in range(lo, hi + 1)}

# worst first: the mapping of several codes is the worst of theirs
MAPPING_ORDER = ["none", "unknown_code", "ambiguous_version", "merge", "1:1", "sbi2008"]


# =========================================================================== reading
def read_kvk_bronze(versions: dict | None = None, batches: list[str] | None = None) -> pd.DataFrame:
    """Rows of every successfully logged KVK batch, with batch_id and ingested_at.
    `batches`: only these batch_ids (the incremental run reads one snapshot at a time)."""
    from deltalake import DeltaTable
    data, batch_log = KVK_BRONZE / "basis_bedrijfsgegevens", KVK_BRONZE / "_batch_log"
    if not (data / "_delta_log").exists() or not (batch_log / "_delta_log").exists():
        log.warning("KVK bronze missing: %s (run pipeline/bronze/kvk_agent.py first)", data)
        return pd.DataFrame()
    log_dt, data_dt = DeltaTable(str(batch_log)), DeltaTable(str(data))
    if versions is not None:
        versions["bronze/kvk/_batch_log"] = log_dt.version()
        versions["bronze/kvk/basis_bedrijfsgegevens"] = data_dt.version()
    logged = log_dt.to_pandas(columns=["batch_id", "ingested_at", "status"])
    good = set(logged.loc[logged["status"] == "success", "batch_id"])

    # Memory: 1.9M rows per snapshot. Read only the columns kvk_company uses (not the
    # long lineage texts _source_file/_source_sha256 or SBI_activiteiten), and keep
    # repeated text as pandas categories (each distinct value stored once). This took
    # silver KVK's peak memory from ~2.3 GB to ~1.3 GB; Docker's memory is limited.
    names = data_dt.schema().to_arrow().names if hasattr(data_dt.schema(), "to_arrow") \
        else [f.name for f in data_dt.schema().fields]
    found, _ = match_columns(names, REQUIRED_COLUMNS, OPTIONAL_COLUMNS)
    keep = [names[i] for k, i in found.items() if k != "sbi"]
    keep += ["_batch_id", "_ingested_at", "_row_number"]
    filters = [("_batch_id", "in", list(batches))] if batches is not None else None
    table = data_dt.to_pyarrow_table(columns=keep, filters=filters)
    df = table.to_pandas(strings_to_categorical=True)
    df = df[df["_batch_id"].isin(good)]
    df["_batch_id"] = df["_batch_id"].cat.remove_unused_categories()
    # a fixed row order: reading the Delta files doesn't guarantee one
    return df.sort_values(["_batch_id", "_row_number"], ignore_index=True)


def read_crosswalk(versions: dict | None = None) -> pd.DataFrame:
    """The SBI 2008 <-> 2025 crosswalk from silver (map_sbi2008_sbi2025), only the two
    code columns. Without it every code becomes 'unknown', so run pipeline/silver/cbs.py first."""
    from deltalake import DeltaTable
    path = SILVER_ROOT / "map_sbi2008_sbi2025"
    if not (path / "_delta_log").exists():
        log.warning("Crosswalk missing: %s (run pipeline/bronze/reference.py and pipeline/silver/cbs.py first)",
                    path)
        return pd.DataFrame(columns=["sbi2008_code", "sbi2025_code"])
    dt = DeltaTable(str(path))
    if versions is not None:
        versions["silver/map_sbi2008_sbi2025"] = dt.version()
    return dt.to_pandas(columns=["sbi2008_code", "sbi2025_code"])


# =========================================================================== SBI logic
def classify_main_activities(values, crosswalk: pd.DataFrame) -> pd.DataFrame:
    """One row per distinct "Hoofdactiviteiten" value with its SBI columns and flags."""
    in2008, in2025 = set(crosswalk["sbi2008_code"]), set(crosswalk["sbi2025_code"])
    # SBI 2025 code -> the SBI 2008 codes it came from (several for a merge)
    sources = crosswalk.groupby("sbi2025_code")["sbi2008_code"].agg(set).to_dict()
    rows = []
    for value in values:
        codes = [c.strip() for c in str(value or "").split(",") if c.strip()]
        # per code: which classification it belongs to, how it maps, and which SBI 2008
        # codes it could stand for (the candidates of all main codes are combined)
        versions, mappings, candidates = [], [], set()
        for c in codes:
            if c in in2008 and c in in2025:
                # the same number exists in both lists: we can't tell which one KVK meant,
                # so both readings stay candidates
                versions.append("both")
                mappings.append("ambiguous_version")
                candidates |= {c} | sources.get(c, set())
            elif c in in2008:
                versions.append("2008")
                mappings.append("sbi2008")
                candidates.add(c)
            elif c in in2025:
                versions.append("2025")
                mappings.append("merge" if len(sources[c]) > 1 else "1:1")
                candidates |= sources[c]
            else:
                versions.append("unknown")
                mappings.append("unknown_code")
        # derived columns only when unambiguous: one candidate code, or (for division and
        # section) all candidates in one division; never when a code is unknown
        unknown = "unknown" in versions
        divisions = {c[:2] for c in candidates}
        division = divisions.pop() if len(divisions) == 1 and not unknown else None
        rows.append({
            "main_activity_codes": value,
            "main_activity_count": len(codes),
            "has_multiple_main_activities": len(codes) > 1,
            "has_no_main_activity": not codes,
            "is_kvk_subcode": any(len(c) == 6 and v == "unknown" for c, v in zip(codes, versions)),
            "sbi_version": (None if not codes else versions[0] if len(set(versions)) == 1
                            else "mixed"),
            "sbi_mapping": min(mappings, key=MAPPING_ORDER.index) if mappings else "none",
            "sbi2008_candidates": len(candidates),
            "sbi2008_code": next(iter(candidates)) if len(candidates) == 1 and not unknown else None,
            "sbi2008_division": division,
            "sbi2008_section": DIVISION_SECTION.get(division) if division else None,
        })
    return pd.DataFrame(rows)


# =========================================================================== build
def _blank_to_null(s: pd.Series) -> pd.Series:
    """Text column with surrounding spaces removed and empty strings as null."""
    s = s.astype("string").str.strip()
    return s.mask(s == "")


def build_kvk_company(bronze: pd.DataFrame, crosswalk: pd.DataFrame,
                      latest: str | None = None) -> pd.DataFrame:
    """kvk_company from the bronze rows: typed columns plus the SBI columns and flags.
    `latest`: the batch_id that gets is_latest (default: the newest batch in `bronze`).

    Columns are found by name through the same accepted spellings as the KVK file
    checks (config.yaml), so a renamed header in a future KVK file only needs a config
    change. The SBI logic runs once per distinct main-activity value (about 8,400 for
    1.9 million rows) and is joined back, which keeps the run at seconds."""
    if bronze.empty:
        return pd.DataFrame()
    found, missing = match_columns(list(bronze.columns), REQUIRED_COLUMNS, OPTIONAL_COLUMNS)
    if missing:
        raise ValueError(f"KVK bronze columns not recognised: {missing}")
    col = {k: bronze.columns[i] for k, i in found.items()}

    batches = (bronze.groupby("_batch_id", observed=True)["_ingested_at"].min().sort_values())
    raw_start = _blank_to_null(bronze[col["start_date"]])
    start = pd.to_datetime(raw_start, format="%Y%m%d", errors="coerce")
    active = bronze[col["active"]].str.strip().str.upper().map({"J": True, "N": False})

    out = pd.DataFrame({
        "snapshot_date": bronze["_ingested_at"].dt.date,
        "batch_id": bronze["_batch_id"],
        "is_latest": bronze["_batch_id"] == (latest if latest is not None else batches.index[-1]),
        "source_row_number": bronze["_row_number"],
        "start_date": start.dt.date.astype(object).where(start.notna(), None),
        "has_invalid_start_date": raw_start.notna() & start.isna(),
        "is_active": active.astype("boolean"),
        "legal_form": _blank_to_null(bronze[col["legal_form"]]),
        "insolvency_code": (_blank_to_null(bronze[col["insolvency"]])
                            if "insolvency" in col else pd.NA),
        "postcode_region": _blank_to_null(bronze[col["postcode_region"]]),
        "member_state": (_blank_to_null(bronze[col["member_state"]])
                         if "member_state" in col else pd.NA),
        "main_activity_codes": (_blank_to_null(bronze[col["main_activity"]])
                                if "main_activity" in col else pd.NA),
    })
    # SBI logic once per distinct value, then looked up per row. A lookup (map) per
    # column instead of a merge: a merge would copy the whole 1.9M-row table at once.
    codes = out["main_activity_codes"]
    sbi = classify_main_activities(codes.dropna().unique(), crosswalk).set_index(
        "main_activity_codes")
    # companies without any main activity aren't in `sbi` (nothing to classify)
    no_codes = {"main_activity_count": 0, "has_multiple_main_activities": False,
                "has_no_main_activity": True, "is_kvk_subcode": False,
                "sbi_mapping": "none", "sbi2008_candidates": 0}
    for c in sbi.columns:
        values = codes.map(sbi[c])
        if c in no_codes:
            values = values.where(codes.notna(), no_codes[c])
        out[c] = values
    for c in ("main_activity_count", "sbi2008_candidates"):
        out[c] = out[c].astype("int64")
    for c in ("has_multiple_main_activities", "has_no_main_activity", "is_kvk_subcode"):
        out[c] = out[c].astype(bool)
    return out


# =========================================================================== incremental run
# kvk_company keeps one snapshot per kept KVK upload. Instead of rebuilding all of them
# every run (memory grows with every month kept), a run compares two lists:
#   wanted  = the successful batches in bronze/kvk/_batch_log (the register of uploads)
#   present = the batches already in silver kvk_company
# and only builds what is new and deletes what was retired, one snapshot at a time.
# Everything is rebuilt (still one snapshot at a time) when a rule that applies to all
# snapshots changed: the SBI crosswalk's content or this module's LOGIC_VERSION.
TABLE = "kvk_company"
# Raise this when build_kvk_company changes what it produces (a column, a rule) or how
# the table is stored: the next run then rebuilds every snapshot instead of only adding
# new ones. 2: partitioned by batch_id (one folder per snapshot).
LOGIC_VERSION = 2
PARTITION = ["batch_id"]       # one folder per snapshot: removing one drops its folder


@dataclass
class Plan:
    full: bool                  # rebuild every snapshot
    reason: str                 # why (shown in the log and the run log)
    add: list[str] = field(default_factory=list)      # batches to build, oldest first
    remove: list[str] = field(default_factory=list)   # batches to delete from silver
    latest: str | None = None   # the batch that gets is_latest


def plan_sync(wanted: list[str], present: dict[str, set[str]], ok_runs: set[str],
              fingerprint: str, last_inputs: dict | None, full: bool = False) -> Plan:
    """What a run has to do. `wanted`: successful bronze batches, oldest first;
    `present`: batch -> run_ids of its rows in silver; `ok_runs`: run_ids of successful
    silver KVK runs; `last_inputs`: the inputs of the last successful run (None: none).

    A snapshot whose rows come from a run that didn't succeed (it crashed after
    writing them) or from several runs counts as not done: it is deleted and rebuilt."""
    latest = wanted[-1] if wanted else None
    reasons = [(full, "requested (--full)"),
               (not present, "no kvk_company table yet"),
               (last_inputs is None, "no successful run to continue from"),
               (last_inputs is not None and "crosswalk_fingerprint" not in last_inputs,
                "the last run is from before incremental updates"),
               (last_inputs is not None and last_inputs.get("crosswalk_fingerprint") != fingerprint,
                "the SBI crosswalk changed"),
               (last_inputs is not None and last_inputs.get("logic_version") != LOGIC_VERSION,
                f"LOGIC_VERSION changed to {LOGIC_VERSION}")]
    for needed, reason in reasons:
        if needed:
            return Plan(True, reason, add=list(wanted), latest=latest)
    redo = {b for b, runs in present.items() if len(runs) != 1 or not runs <= ok_runs}
    add = [b for b in wanted if b not in present or b in redo]
    remove = sorted((set(present) - set(wanted)) | (redo & set(present)))
    if add or remove:
        return Plan(False, "incremental", add, remove, latest)
    return Plan(False, "nothing new", latest=latest)


def read_batches(versions: dict | None = None) -> list[str]:
    """The successful KVK batches in bronze, oldest first (the register of uploads)."""
    from deltalake import DeltaTable
    batch_log = KVK_BRONZE / "_batch_log"
    if not (batch_log / "_delta_log").exists():
        return []
    dt = DeltaTable(str(batch_log))
    if versions is not None:
        versions["bronze/kvk/_batch_log"] = dt.version()
    data = KVK_BRONZE / "basis_bedrijfsgegevens"
    if versions is not None and (data / "_delta_log").exists():
        versions["bronze/kvk/basis_bedrijfsgegevens"] = DeltaTable(str(data)).version()
    log_df = dt.to_pandas(columns=["batch_id", "ingested_at", "status"])
    ok = log_df[log_df["status"] == "success"].sort_values("ingested_at")
    return list(ok["batch_id"])


def crosswalk_fingerprint(crosswalk: pd.DataFrame) -> str:
    """A hash of the crosswalk's content: the same code pairs give the same fingerprint,
    however often silver CBS rewrites the table."""
    import hashlib
    pairs = sorted(map(tuple, crosswalk[["sbi2008_code", "sbi2025_code"]].astype(str).to_numpy()))
    return hashlib.sha256(repr(pairs).encode()).hexdigest()[:16]


def table_batches(path) -> dict[str, set[str]]:
    """batch_id -> the run_ids of its rows in silver kvk_company (empty: no table).

    Read from the Delta log, not from the data: kvk_company is partitioned by batch_id
    (one folder per snapshot), and the log keeps each file's min and max run_id. Only a
    file with several run_ids (or without statistics) is read, two columns of it.
    Reading both columns of every row instead would cost ~300 MB per kept snapshot."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from deltalake import DeltaTable
    if not (path / "_delta_log").exists():
        return {}
    a = pa.table(DeltaTable(str(path)).get_add_actions(flatten=True)).to_pydict()

    def single(column: str, i: int):
        low, high = a.get(f"min.{column}"), a.get(f"max.{column}")
        return low[i] if low and high and low[i] is not None and low[i] == high[i] else None

    out: dict[str, set[str]] = {}
    for i, file in enumerate(a["path"]):
        batch = a["partition.batch_id"][i] if "partition.batch_id" in a else single("batch_id", i)
        run = single("run_id", i)
        if batch is not None and run is not None:
            out.setdefault(batch, set()).add(run)
            continue
        unknown = [c for c, v in (("batch_id", batch), ("run_id", run)) if v is None]
        pairs = pq.read_table(str(path / file), columns=unknown).group_by(unknown).aggregate([])
        for p in pairs.to_pylist():
            out.setdefault(p.get("batch_id", batch), set()).add(p.get("run_id", run))
    return out


def _snapshot(batch_id: str, latest: str, crosswalk: pd.DataFrame, run_id: str, schema=None):
    """One snapshot of kvk_company as an Arrow table with run_id, read from bronze on its
    own (memory: one snapshot). `schema`: cast to the table's existing column types."""
    from pipeline.common.publish import to_arrow
    df = build_kvk_company(read_kvk_bronze(batches=[batch_id]), crosswalk, latest=latest)
    df["run_id"] = run_id            # in place: assign() would copy the whole snapshot first
    table = to_arrow(df)
    del df
    return table.cast(schema) if schema is not None else table


def lean_memory() -> None:
    """Make the process hand memory back between snapshots. Without this, each snapshot
    read and written leaves ~100-300 MB behind (memory the libraries keep for reuse), so
    a run's peak still grew with the number of snapshots; with it, it stays flat. Arrow
    uses the system allocator, and glibc keeps at most 2 memory arenas (no-op elsewhere)."""
    import ctypes
    import pyarrow as pa
    pa.set_memory_pool(pa.system_memory_pool())
    try:
        ctypes.CDLL("libc.so.6").mallopt(-8, 2)       # -8 = M_ARENA_MAX
    except (OSError, AttributeError):                  # not Linux/glibc (e.g. macOS)
        pass


def _write_snapshot(path, batch_id: str, latest: str, crosswalk: pd.DataFrame, run_id: str,
                    schema=None, replace: bool = False):
    """Build one snapshot and write it to the live table (append, or replace the table),
    in its own commit. Returns its schema, for the next snapshot.

    Memory: one snapshot. It goes through a small staging table first: writing a whole
    snapshot at once into a partitioned table costs ~400 MB more than streaming it in
    from files, and one write call per snapshot keeps the libraries from holding on to
    memory in proportion to everything written."""
    import shutil
    from deltalake import DeltaTable, write_deltalake
    staging = SILVER_ROOT / "_staging" / run_id
    snap = _snapshot(batch_id, latest, crosswalk, run_id, schema)
    schema, rows = snap.schema, snap.num_rows
    write_deltalake(str(staging), snap, mode="overwrite")
    del snap
    try:
        reader = (DeltaTable(str(staging)).to_pyarrow_dataset()
                  .scanner(batch_size=65_536, batch_readahead=1, fragment_readahead=1).to_reader())
        write_deltalake(str(path), reader, mode="overwrite" if replace else "append",
                        schema_mode="overwrite" if replace else None, partition_by=PARTITION)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        if (SILVER_ROOT / "_staging").exists() and not any((SILVER_ROOT / "_staging").iterdir()):
            (SILVER_ROOT / "_staging").rmdir()
    log.info("kvk_company: wrote snapshot %s (%d rows)", batch_id, rows)
    return schema


def _write_full(path, plan: Plan, crosswalk: pd.DataFrame, run_id: str) -> None:
    """Rebuild every snapshot, one per commit: the newest first, replacing the table, so
    the right latest snapshot is there from the first commit on; then the older ones.
    Until the run ends the table holds only part of the history; if it stops halfway,
    the next run sees the missing (or failed-run) snapshots and builds them."""
    newest_first = [plan.latest, *[b for b in plan.add if b != plan.latest]]
    schema = None
    for i, b in enumerate(newest_first):
        schema = _write_snapshot(path, b, plan.latest, crosswalk, run_id, schema, replace=i == 0)


def _write_incremental(path, plan: Plan, crosswalk: pd.DataFrame, run_id: str) -> None:
    """Delete removed batches, append new ones (one commit each), then move is_latest in
    one commit. After a crash, the next run's plan finishes the work: a snapshot is
    added in one commit, so never half."""
    import pyarrow as pa
    from deltalake import DeltaTable
    dt = DeltaTable(str(path))
    if plan.remove:
        dt.delete("batch_id IN (" + ", ".join(f"'{b}'" for b in plan.remove) + ")")
        log.info("kvk_company: removed %s", ", ".join(plan.remove))
    schema = pa.schema(dt.schema().to_arrow())
    for b in plan.add:
        # added with is_latest false; the flag moves below, so readers never see two
        schema = _write_snapshot(path, b, "", crosswalk, run_id, schema)
    # one commit: the old latest false, the new latest true. Rewrites the files of those
    # two snapshots only (~650 MB), however many snapshots are kept.
    latest = plan.latest.replace("'", "''")
    DeltaTable(str(path)).update(
        updates={"is_latest": f"batch_id = '{latest}'"},
        predicate=f"(is_latest AND batch_id <> '{latest}') OR (NOT is_latest AND batch_id = '{latest}')")


def _last_success_inputs() -> tuple[dict | None, set[str]]:
    """Inputs of the last successful silver KVK run, and the run_ids of all successful ones."""
    import json
    from pipeline.common import runlog
    runs = runlog.read_runs(RUN_LOG)
    ok = runs[(runs["pipeline"] == PIPELINE) & (runs["status"] == "success")]
    if ok.empty:
        return None, set()
    return json.loads(ok.iloc[-1]["inputs"]), set(ok["run_id"])


def run(full: bool = False) -> str:
    """Bring silver kvk_company in line with bronze KVK and log the run (lakehouse/_run_log).
    Incremental unless a full rebuild is needed or asked for (`full`). Returns the run_id."""
    from datetime import datetime, timezone
    from deltalake import DeltaTable
    from pipeline.common import runlog
    from pipeline.common.publish import RunFailed
    lean_memory()
    started = datetime.now(timezone.utc)
    run_id = runlog.new_run_id(PIPELINE, started)
    inputs: dict = {}
    tables: dict = {}
    path = SILVER_ROOT / TABLE
    try:
        wanted = read_batches(inputs)
        if not wanted:
            raise RunFailed("no KVK batches in bronze (load a KVK file first)")
        crosswalk = read_crosswalk(inputs)
        fingerprint = crosswalk_fingerprint(crosswalk)
        inputs.update(crosswalk_fingerprint=fingerprint, logic_version=LOGIC_VERSION)
        last_inputs, ok_runs = _last_success_inputs()
        plan = plan_sync(wanted, table_batches(path), ok_runs, fingerprint, last_inputs, full)
        log.info("kvk_company: %s (%s); add %s, remove %s", "full rebuild" if plan.full
                 else "incremental", plan.reason, plan.add or "nothing", plan.remove or "nothing")
        tables = {TABLE: {"mode": "full" if plan.full else "incremental", "reason": plan.reason,
                          "added": plan.add, "removed": plan.remove}}
        if plan.full:
            _write_full(path, plan, crosswalk, run_id)
        elif plan.add or plan.remove:
            _write_incremental(path, plan, crosswalk, run_id)
        rows = DeltaTable(str(path)).to_pyarrow_dataset().count_rows()
        tables[TABLE].update(rows=rows, status="published" if plan.full
                             else ("updated" if plan.add or plan.remove else "unchanged"))
    except Exception as e:
        runlog.write_run(run_id, PIPELINE, started, "failed", tables, inputs,
                         error=f"{type(e).__name__}: {e}", path=RUN_LOG)
        log.error("Run %s failed: %s", run_id, e)
        raise
    runlog.write_run(run_id, PIPELINE, started, "success", tables, inputs, path=RUN_LOG)
    return run_id


def preview() -> None:
    """What the next run would do, and a summary of the newest snapshot (writes nothing)."""
    lean_memory()
    wanted = read_batches()
    if not wanted:
        log.info("No KVK batches in bronze")
        return
    crosswalk = read_crosswalk()
    last_inputs, ok_runs = _last_success_inputs()
    plan = plan_sync(wanted, table_batches(SILVER_ROOT / TABLE), ok_runs,
                     crosswalk_fingerprint(crosswalk), last_inputs)
    log.info("Next run: %s (%s); add %s, remove %s", "full rebuild" if plan.full else "incremental",
             plan.reason, plan.add or "nothing", plan.remove or "nothing")
    df = build_kvk_company(read_kvk_bronze(batches=[plan.latest]), crosswalk, latest=plan.latest)
    log.info("Newest snapshot %s: %d rows; columns: %s", plan.latest, len(df), ", ".join(df.columns))
    for c in ("sbi_version", "sbi_mapping"):
        log.info("%s: %s", c, df[c].value_counts(dropna=False).to_dict())


def main() -> None:
    """Command line: update kvk_company (incremental), --full to rebuild it, --preview."""
    ap = argparse.ArgumentParser(description="Build silver kvk_company from bronze KVK.")
    ap.add_argument("--preview", action="store_true",
                    help="show what the next run would do, write nothing")
    ap.add_argument("--full", action="store_true",
                    help="rebuild every snapshot instead of only adding new ones")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.preview:
        preview()
        return
    try:
        run(full=args.full)
    except Exception:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
