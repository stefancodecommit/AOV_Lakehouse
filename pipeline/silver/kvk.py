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

Uses silver map_sbi2008_sbi2025 (run pipeline/silver/cbs.py first). Publishing, staging,
run_id and the run log work as in silver CBS (pipeline/common/publish.py, pipeline silver_kvk).

Usage
-----
    python -m pipeline.silver.kvk            # rebuild kvk_company
    python -m pipeline.silver.kvk --preview  # build and print a summary, write nothing
"""
from __future__ import annotations

import argparse
import logging

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
def read_kvk_bronze(versions: dict | None = None) -> pd.DataFrame:
    """Rows of every successfully logged KVK batch, with batch_id and ingested_at."""
    from deltalake import DeltaTable
    data, batch_log = KVK_BRONZE / "basis_bedrijfsgegevens", KVK_BRONZE / "_batch_log"
    if not (data / "_delta_log").exists() or not (batch_log / "_delta_log").exists():
        log.warning("KVK bronze missing: %s (run pipeline/bronze/kvk_agent.py first)", data)
        return pd.DataFrame()
    log_dt, data_dt = DeltaTable(str(batch_log)), DeltaTable(str(data))
    if versions is not None:
        versions["bronze/kvk/_batch_log"] = log_dt.version()
        versions["bronze/kvk/basis_bedrijfsgegevens"] = data_dt.version()
    batches = log_dt.to_pandas(columns=["batch_id", "ingested_at", "status"])
    good = set(batches.loc[batches["status"] == "success", "batch_id"])

    # Memory: 1.9M rows per snapshot. Read only the columns kvk_company uses (not the
    # long lineage texts _source_file/_source_sha256 or SBI_activiteiten), and keep
    # repeated text as pandas categories (each distinct value stored once). This took
    # silver KVK's peak memory from ~2.3 GB to ~1.3 GB; Docker's memory is limited.
    names = data_dt.schema().to_arrow().names if hasattr(data_dt.schema(), "to_arrow") \
        else [f.name for f in data_dt.schema().fields]
    found, _ = match_columns(names, REQUIRED_COLUMNS, OPTIONAL_COLUMNS)
    keep = [names[i] for k, i in found.items() if k != "sbi"]
    keep += ["_batch_id", "_ingested_at", "_row_number"]
    table = data_dt.to_pyarrow_table(columns=keep)
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


def build_kvk_company(bronze: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    """kvk_company from the bronze rows: typed columns plus the SBI columns and flags.

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
        "is_latest": bronze["_batch_id"] == batches.index[-1],
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


def build_all(read_bronze=read_kvk_bronze, read_map=read_crosswalk, inputs=None) -> dict:
    """Build kvk_company; `inputs` (optional) collects the Delta versions read, for the run log."""
    kw = {} if inputs is None else {"versions": inputs}
    return {"kvk_company": build_kvk_company(read_bronze(**kw), read_map(**kw))}


def run() -> str:
    """Build, stage, publish and log one silver KVK run (pipeline/common/publish.py). Returns the run_id."""
    from pipeline.common.publish import run_pipeline
    return run_pipeline(PIPELINE, lambda inputs: build_all(inputs=inputs), SILVER_ROOT, RUN_LOG)


def main() -> None:
    """Command line: build and publish kvk_company, or only summarise with --preview."""
    ap = argparse.ArgumentParser(description="Build silver kvk_company from bronze KVK.")
    ap.add_argument("--preview", action="store_true", help="build and summarise, write nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.preview:
        df = build_all()["kvk_company"]
        log.info("kvk_company %d rows; columns: %s", len(df), ", ".join(df.columns))
        latest = df[df["is_latest"]] if not df.empty else df
        for c in ("sbi_version", "sbi_mapping"):
            log.info("%s (latest snapshot): %s", c, latest[c].value_counts(dropna=False).to_dict())
        return
    try:
        run()
    except Exception:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
