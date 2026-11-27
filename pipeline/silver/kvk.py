"""
Silver layer for the KVK open dataset: lakehouse/silver/kvk_company.

One row per company in the newest KVK upload, typed, with the company's
sector taken from its main activity ("Hoofdactiviteiten"). Edge cases are marked with
flag columns instead of being resolved by assumptions: original values stay as they
are, derived columns are filled only where the answer is unambiguous, and the flags say
why one is empty. Gold and the dashboard can filter on the flags.

Columns
-------
snapshot_date, batch_id              which upload the rows come from (one per table)
snapshot_date_source                 where snapshot_date comes from: kvk_file (KVK's
                                     export date, read from the zip; the normal case)
                                     or upload (the day the batch was loaded, when the
                                     file had no usable date)
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

Only the newest upload
----------------------
Every KVK upload is a full snapshot of ~1.9 million companies. Bronze keeps the uploads
(the newest plus the first of each month, tools/maintenance.py), because KVK serves only
its current file: an old one can't be downloaded again. Silver holds only the newest:
the open dataset has no company number, so snapshots can't be linked company by
company, and gold uses the newest one only. (Until 2026-11 silver kept every bronze
snapshot and updated them one by one; nothing used that history, so it was simplified.)

Each run rebuilds kvk_company from the successful batch in bronze/kvk/_batch_log with
the newest snapshot date (KVK's file date; not the last one loaded) and publishes it like silver CBS (pipeline/common/publish.py): staged first, then
replaced in one commit, with a run_id and a row in lakehouse/_run_log. Memory: one
snapshot, however many uploads bronze keeps.

A run is skipped when nothing it reads has changed since the last successful run: the
same newest upload, the same crosswalk batch and the same committed code
(unchanged_since). A new CBS quarter rebuilds silver CBS, which used to rebuild all
1.9M KVK rows as well.

Usage
-----
    python -m pipeline.silver.kvk            # rebuild from the newest upload (if changed)
    python -m pipeline.silver.kvk --force    # rebuild even when nothing changed
    python -m pipeline.silver.kvk --preview  # build and summarise, write nothing
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import pandas as pd

from checks.kvk import match_columns
from pipeline.common.config import project_path, settings

LAKEHOUSE = project_path(settings["paths"]["lakehouse"])
KVK_BRONZE = LAKEHOUSE / "bronze" / "kvk"
SILVER_ROOT = LAKEHOUSE / "silver"
RUN_LOG = LAKEHOUSE / "_run_log"
PIPELINE = "silver_kvk"
# run-log input keys that identify what a run was built from (see unchanged_since)
NEWEST_BATCH = "bronze/kvk/_batch_log#batch_id"
CROSSWALK_BATCH = "silver/map_sbi2008_sbi2025#batch_id"
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
# what classify_main_activities gives per distinct main-activity value
SBI_COLUMNS = ["main_activity_codes", "main_activity_count", "has_multiple_main_activities",
               "has_no_main_activity", "is_kvk_subcode", "sbi_version", "sbi_mapping",
               "sbi2008_candidates", "sbi2008_code", "sbi2008_division", "sbi2008_section"]


# =========================================================================== reading
def _folders(lakehouse: Path | None) -> tuple[Path, Path, Path]:
    """(KVK bronze, silver, run log) of `lakehouse`, or the configured ones for None."""
    if lakehouse is None:
        return KVK_BRONZE, SILVER_ROOT, RUN_LOG
    return lakehouse / "bronze" / "kvk", lakehouse / "silver", lakehouse / "_run_log"


def read_kvk_bronze(versions: dict | None = None, batches: list[str] | None = None,
                    root: Path | None = None) -> pd.DataFrame:
    """Rows of every successfully logged KVK batch, with batch_id and ingested_at.
    `batches`: only these batch_ids (a run reads only the newest)."""
    from deltalake import DeltaTable
    root = KVK_BRONZE if root is None else root           # the KVK bronze folder
    data, batch_log = root / "basis_bedrijfsgegevens", root / "_batch_log"
    if not (data / "_delta_log").exists() or not (batch_log / "_delta_log").exists():
        log.warning("KVK bronze missing: %s (load a KVK file first: python -m pipeline.bronze.kvk --inbox)", data)
        return pd.DataFrame()
    log_dt, data_dt = DeltaTable(str(batch_log)), DeltaTable(str(data))
    if versions is not None:
        versions["bronze/kvk/_batch_log"] = log_dt.version()
        versions["bronze/kvk/basis_bedrijfsgegevens"] = data_dt.version()
    logged = log_dt.to_pandas(columns=["batch_id", "ingested_at", "status", "file_date"])
    good = set(logged.loc[logged["status"] == "success", "batch_id"])
    file_dates = logged.dropna(subset=["file_date"]).set_index("batch_id")["file_date"].to_dict()

    # Memory: 1.9M rows per snapshot. Read only the columns kvk_company uses (not the
    # long lineage texts _source_file/_source_sha256 or SBI_activiteiten), and keep
    # repeated text as pandas categories (each distinct value stored once). This took
    # silver KVK's peak memory from ~2.3 GB to ~1.3 GB; Docker's memory is limited.
    # build_kvk_company keeps its text columns as categories too (2026-11-26).
    names = data_dt.schema().to_arrow().names
    found, _ = match_columns(names, REQUIRED_COLUMNS, OPTIONAL_COLUMNS)
    keep = [names[i] for k, i in found.items() if k != "sbi"]
    keep += ["_batch_id", "_ingested_at", "_row_number"]
    filters = [("_batch_id", "in", list(batches))] if batches is not None else None
    table = data_dt.to_pyarrow_table(columns=keep, filters=filters)
    df = table.to_pandas(strings_to_categorical=True)
    df = df[df["_batch_id"].isin(good)]
    df["_batch_id"] = df["_batch_id"].cat.remove_unused_categories()
    # KVK's own export date per batch (from the batch log), one value per row
    df["_file_date"] = df["_batch_id"].map(file_dates).astype(object)
    # a fixed row order: reading the Delta files doesn't guarantee one
    return df.sort_values(["_batch_id", "_row_number"], ignore_index=True)


def read_crosswalk(versions: dict | None = None, root: Path | None = None) -> pd.DataFrame:
    """The SBI 2008 <-> 2025 crosswalk from silver (map_sbi2008_sbi2025), only the two
    code columns. Without it every code becomes 'unknown', so run pipeline/silver/cbs.py first."""
    from deltalake import DeltaTable
    path = (SILVER_ROOT if root is None else root) / "map_sbi2008_sbi2025"   # root: silver
    if not (path / "_delta_log").exists():
        log.warning("Crosswalk missing: %s (run pipeline/bronze/reference.py and pipeline/silver/cbs.py first)",
                    path)
        if versions is not None:
            versions[CROSSWALK_BATCH] = ""
        return pd.DataFrame(columns=["sbi2008_code", "sbi2025_code"])
    dt = DeltaTable(str(path))
    df = dt.to_pandas(columns=["sbi2008_code", "sbi2025_code", "batch_id"])
    if versions is not None:
        versions["silver/map_sbi2008_sbi2025"] = dt.version()
        # its content: silver CBS rewrites the table (a new Delta version) on every run,
        # but the crosswalk only changes with a new bronze batch (see unchanged_since)
        versions[CROSSWALK_BATCH] = _batch_ids(df["batch_id"])
    return df[["sbi2008_code", "sbi2025_code"]]


def _batch_ids(values) -> str:
    """The distinct batch_ids of a column as one text ('' when there are none)."""
    return ",".join(sorted(set(values.dropna())))


def crosswalk_batch(root: Path | None = None) -> str | None:
    """The batch_id(s) of the crosswalk in silver (`root`, default: the configured
    folder), as read_crosswalk records them; '' when there is no crosswalk table."""
    from deltalake import DeltaTable
    path = (SILVER_ROOT if root is None else root) / "map_sbi2008_sbi2025"
    if not (path / "_delta_log").exists():
        return ""
    return _batch_ids(DeltaTable(str(path)).to_pandas(columns=["batch_id"])["batch_id"])


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
    # the columns also without values (no company has a main activity)
    return pd.DataFrame(rows, columns=SBI_COLUMNS)


# =========================================================================== build
def _blank_to_null(s: pd.Series) -> pd.Series:
    """Text column with surrounding spaces removed and empty strings as null, as a
    category column (stored as plain text: publish.to_arrow).

    Memory: the cleaning runs once per distinct value (legal_form has 2, postcode_region
    91), not once per row; until 2026-11-26 the column became 1.9M separate strings."""
    s = s.astype("category")                 # no copy when bronze was read as categories
    clean = s.cat.categories.astype("string").str.strip()
    clean = clean.where(clean != "", None).astype(object)
    # one entry per distinct value; each row takes its value's entry (missing: -1 -> null)
    return pd.Series(pd.Categorical(clean).take(s.cat.codes.to_numpy(), allow_fill=True),
                     index=s.index)


def build_kvk_company(bronze: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    """kvk_company from the bronze rows of one upload: typed columns plus the SBI columns
    and flags.

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

    raw_start = _blank_to_null(bronze[col["start_date"]])
    start = pd.to_datetime(raw_start, format="%Y%m%d", errors="coerce")
    active = bronze[col["active"]].str.strip().str.upper().map({"J": True, "N": False})

    # KVK's export date when the batch log has it; otherwise the upload day, and the
    # source column says which (flag, don't assume)
    upload_day = bronze["_ingested_at"].dt.date
    file_day = (bronze["_file_date"] if "_file_date" in bronze
                else pd.Series(None, index=bronze.index, dtype=object))
    has_file_day = file_day.notna()
    out = pd.DataFrame({
        "snapshot_date": file_day.where(has_file_day, upload_day),
        "snapshot_date_source": pd.Series("upload", index=bronze.index).where(~has_file_day,
                                                                              "kvk_file"),
        "batch_id": bronze["_batch_id"],
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
    # SBI logic once per distinct value, then looked up per row through the category
    # codes (a merge would copy the whole 1.9M-row table at once). The text columns stay
    # categories: one value per distinct value, not per row.
    codes = out["main_activity_codes"].astype("category")
    sbi = classify_main_activities(list(codes.cat.categories), crosswalk)
    # companies without any main activity (code -1) aren't classified: they get one
    # extra row after the classified values
    no_codes = {"main_activity_count": 0, "has_multiple_main_activities": False,
                "has_no_main_activity": True, "is_kvk_subcode": False,
                "sbi_mapping": "none", "sbi2008_candidates": 0}
    row = codes.cat.codes.to_numpy().copy()
    row[row < 0] = len(sbi)
    for c in sbi.columns.drop("main_activity_codes"):
        per_value = pd.Series([*sbi[c], no_codes.get(c)], dtype=object)
        out[c] = pd.Series(pd.Categorical(per_value).take(row), index=out.index)
    for c in ("main_activity_count", "sbi2008_candidates"):
        out[c] = out[c].astype("int64")
    for c in ("has_multiple_main_activities", "has_no_main_activity", "is_kvk_subcode"):
        out[c] = out[c].astype(bool)
    return out


# =========================================================================== run
TABLE = "kvk_company"


def newest_batch(versions: dict | None = None, root: Path | None = None) -> str | None:
    """The successful KVK batch with the newest snapshot (KVK's file date, else the upload
    day; pipeline/bronze/kvk.by_snapshot_date), or None. Not the last one loaded:
    an older file loaded later must not replace a newer snapshot."""
    from deltalake import DeltaTable

    from pipeline.bronze.kvk import by_snapshot_date
    batch_log = (KVK_BRONZE if root is None else root) / "_batch_log"   # root: KVK bronze
    if not (batch_log / "_delta_log").exists():
        return None
    log_df = DeltaTable(str(batch_log)).to_pandas()
    ok = by_snapshot_date(log_df[log_df["status"] == "success"])
    newest = ok["batch_id"].iloc[-1] if not ok.empty else None
    if versions is not None and newest is not None:
        versions[NEWEST_BATCH] = newest
    return newest


def build_all(inputs: dict | None = None, lakehouse: Path | None = None) -> dict[str, pd.DataFrame]:
    """{kvk_company: DataFrame} from the newest upload; `inputs` collects what was read
    (Delta versions and the batch_id) for the run log; `lakehouse`: read from another
    lakehouse folder than the configured one."""
    from pipeline.common.publish import RunFailed
    kvk_bronze, silver, _ = _folders(lakehouse)
    batch = newest_batch(inputs, root=kvk_bronze)
    if batch is None:
        raise RunFailed("no KVK batches in bronze (load a KVK file first)")
    crosswalk = read_crosswalk(inputs, root=silver)
    return {TABLE: build_kvk_company(read_kvk_bronze(inputs, batches=[batch], root=kvk_bronze),
                                     crosswalk)}


def unchanged_since(lakehouse: Path | None = None) -> str | None:
    """The run_id of the last successful run when a new run would build the same table,
    else None. Same means: the same newest KVK batch, the same crosswalk batch, and the
    same code, and kvk_company still holds that run's rows.

    Why: silver_kvk depends on silver_cbs (for the crosswalk), so every new CBS quarter
    asked for a rebuild of all 1.9M rows, although nothing silver KVK reads had changed.
    The code version only counts when it is known and committed: with uncommitted
    changes ('-dirty'), no git and no CODE_VERSION, or Docker's default 'unknown', the
    code may differ, so the table is rebuilt. A changed setting in config.yaml (the
    accepted KVK column names) is not compared: rebuild with --force after one."""
    from deltalake import DeltaTable

    from pipeline.common import runlog
    kvk_bronze, silver, run_log = _folders(lakehouse)
    runs = runlog.read_runs(run_log)
    runs = runs[runs["pipeline"] == PIPELINE]
    if runs.empty or runs.iloc[-1]["status"] != "success":
        return None                       # nothing built yet, or the last run failed
    last = runs.iloc[-1]
    code = runlog.code_version()
    if not code or code == "unknown" or code.endswith("-dirty") or last["code_version"] != code:
        return None
    built_from = json.loads(last["inputs"] or "{}")
    if (built_from.get(NEWEST_BATCH) != newest_batch(root=kvk_bronze)
            or built_from.get(CROSSWALK_BATCH) != crosswalk_batch(silver)):
        return None                       # a run logged before 2026-11-26 has no crosswalk key
    path = silver / TABLE
    if not (path / "_delta_log").exists():
        return None
    ids = DeltaTable(str(path)).to_pyarrow_dataset().to_table(columns=["run_id"]).column("run_id")
    if set(ids.unique().to_pylist()) != {last["run_id"]}:
        return None                       # the table was replaced or changed since
    return last["run_id"]


def run(force: bool = False, lakehouse: Path | None = None) -> str:
    """Build, stage, publish and log one silver KVK run (pipeline/common/publish.py).
    Returns the run_id; raises (after logging the failed run) when something went wrong.

    When nothing silver KVK reads has changed since the last successful run
    (unchanged_since), nothing is rebuilt or logged and that run's run_id is returned;
    `force` rebuilds anyway. Dagster still reports silver_kvk as materialized then: a
    skipped asset would also skip gold in the same run (see orchestration/definitions.py).

    `lakehouse`: build in another lakehouse folder than the configured one (e.g. a copy
    to try a change on): read from, and write silver and the run log to, that folder."""
    from pipeline.common.publish import run_pipeline
    _, silver, run_log = _folders(lakehouse)
    if not force and (previous := unchanged_since(lakehouse)):
        log.info("Nothing changed since run %s (same KVK upload, crosswalk and code): "
                 "kvk_company not rebuilt (--force rebuilds it)", previous)
        return previous
    return run_pipeline(PIPELINE, lambda inputs: build_all(inputs, lakehouse), silver, run_log)


def preview() -> None:
    """Build kvk_company from the newest upload and summarise it (writes nothing)."""
    df = build_all()[TABLE]
    log.info("Newest upload %s: %d rows; columns: %s", df["batch_id"].iloc[0], len(df),
             ", ".join(df.columns))
    for c in ("sbi_version", "sbi_mapping"):
        log.info("%s: %s", c, df[c].value_counts(dropna=False).to_dict())


def main() -> None:
    """Command line: rebuild kvk_company from the newest upload, or only summarise it."""
    ap = argparse.ArgumentParser(description="Build silver kvk_company from bronze KVK.")
    ap.add_argument("--preview", action="store_true", help="build and summarise, write nothing")
    ap.add_argument("--force", action="store_true",
                    help="rebuild even when the KVK upload, crosswalk and code are unchanged")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.preview:
        preview()
        return
    try:
        run(force=args.force)
    except Exception:
        raise SystemExit(1) from None      # already logged by the run; exit code 1 only


if __name__ == "__main__":
    main()
