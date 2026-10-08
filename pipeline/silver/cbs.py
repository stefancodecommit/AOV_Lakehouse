"""
Silver layer for the CBS bronze tables written by pipeline/bronze/cbs.py.

Reads bronze Delta tables (one column per CBS field), cleans and types them, and rebuilds
the silver tables below on every run (the data is small, so a full rebuild is
simpler and always consistent).

Shared dimensions
-----------------
dim_period          one row per period key: type, year, quarter, start/end date, label
dim_sector          SBI 2008 sectors; CBS uses the same keys in every table (e.g. 301000 = A)
dim_company_size    size classes parsed from titles, with lower/upper bound and a coarse
                    size_band (<10, 10-99, 100+) for comparing across tables
dim_region          regions of 81644NED with their level (country, landsdeel, province, ...)
dim_survey_measure  measures of the absence survey: name, CBS title, topic group, unit
map_characteristic  every company-characteristic key per source table and what it is:
                    total, sector or size class
map_sbi2008_sbi2025 CBS crosswalk SBI 2008 <-> SBI 2025 (from pipeline/bronze/reference.py), latest
                    file version only. Many-to-many: link_type says whether a link is
                    1:1, a split (one 2008 code -> several 2025 codes), a merge, or both

Facts
-----
fct_absence_rate          80072NED  period x sector/size class -> absence_pct (quarterly)
fct_absence_survey        86009NED + 83130NED, long format:
                          period x characteristic x measure -> value, lower_95, upper_95.
                          The two tables overlap (2014-2023) and their averages use
                          different populations, so they are kept side by side
                          (source_table) and never mixed under one measure name.
fct_business_count        81589NED  period x sector x breakdown -> count
fct_establishment_count   81644NED  period x sector x region x breakdown -> count
                          breakdown = total | size | legal_form. Size rows carry
                          size_key, size_band and size_is_aggregate (True when the class
                          contains other classes of the same table: exclude those when
                          summing to avoid double counting).

Runs, staging and the run log (pipeline/common/publish.py)
------------------------------------------
Every run gets a run_id (e.g. silver_cbs_20261005T163000123456Z). All tables are first
built and written to silver/_staging/<run_id>/; only when every table succeeded are the
live tables replaced, each with a `run_id` column. A run that fails leaves the live
tables untouched. Each run, successful or not, is a row in lakehouse/_run_log
(pipeline/common/runlog.py): when it ran, status, error, rows per table, and the Delta version of every
bronze table it read, so silver rows can be joined to the run that made them. A
required table that comes out empty fails the run; map_sbi2008_sbi2025 is optional
(skipped until the crosswalk is loaded).

Usage
-----
    python -m pipeline.silver.cbs            # rebuild all silver tables
    python -m pipeline.silver.cbs --preview  # build and print a summary, write nothing
"""
from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline.common.config import project_path, settings

LAKEHOUSE = project_path(settings["paths"]["lakehouse"])
BRONZE_ROOT = LAKEHOUSE / "bronze"
SILVER_ROOT = LAKEHOUSE / "silver"
RUN_LOG = LAKEHOUSE / "_run_log"
PIPELINE = "silver_cbs"
OPTIONAL_TABLES = {"map_sbi2008_sbi2025"}   # empty until pipeline/bronze/reference.py has run

log = logging.getLogger("cbs_silver")

# source table -> name of its company-characteristic dimension
CHARACTERISTIC_DIMS = {
    "80072NED": "BedrijfskenmerkenSBI2008",
    "86009NED": "Bedrijfskenmerken",
    "83130NED": "Bedrijfskenmerken",
    "81589NED": "BedrijfstakkenBranchesSBI2008",
    "81644NED": "BedrijfstakkenBranchesSBI2008",
}
SURVEY_TABLES = ("86009NED", "83130NED")
TOTAL_KEY = "T001081"  # "A-U Alle economische activiteiten"

# Coarse bands for comparing size classes across tables: (name, lower, upper-exclusive)
SIZE_BANDS = [("<10", 0, 10), ("10-99", 10, 100), ("100+", 100, None)]

META_COLS = ["_period_status", "_ingested_at"]
# Columns every CBS bronze table has besides one column per field (cbs_bronze.bronze_schema)
BRONZE_FIXED = {"_request_key", "_table_id", "_entity", "_period", "_period_status",
                "_source_url", "_ingested_at", "_record_index", "raw_json"}


# =========================================================================== bronze io
def read_bronze(table_id: str, entity: str, versions: dict | None = None) -> pd.DataFrame:
    """Bronze table as a DataFrame: one column per CBS field, plus the period status and
    ingest time. Reads bronze's field columns; a table written before bronze had them is
    read from raw_json instead. `versions` (optional) collects the Delta version read,
    for the run log."""
    path = BRONZE_ROOT / f"{table_id}_{entity}".lower()
    if not (path / "_delta_log").exists():
        log.warning("Bronze table missing: %s (run pipeline/bronze/cbs.py first)", path)
        return pd.DataFrame()
    from deltalake import DeltaTable
    dt = DeltaTable(str(path))
    if versions is not None:
        versions[f"bronze/{path.name}"] = dt.version()
    names = [f.name for f in dt.schema().fields]
    fields = [n for n in names if n not in BRONZE_FIXED]
    # ID is only a row number in data rows; in CategoryGroups/DataProperties it's a real key
    drop_id = entity == "TypedDataSet"
    if not fields:     # a table written before bronze had a column per field
        raw = dt.to_pandas(columns=["_period_status", "_ingested_at", "raw_json"])
        return parse_raw(raw, drop_id=drop_id)
    df = dt.to_pandas(columns=[*fields, *META_COLS])
    df["_ingested_at"] = df["_ingested_at"].astype("datetime64[ns, UTC]")   # as parse_raw gives
    # a field that was renamed in bronze because it clashed with a lineage column
    df = df.rename(columns={n: n[len("field_"):] for n in fields
                            if n.startswith("field_") and n[len("field_"):] in BRONZE_FIXED})
    return clean_fields(df, drop_id)


def read_reference(dataset: str, versions: dict | None = None) -> pd.DataFrame:
    """Rows of the latest successful batch of a reference table (pipeline/bronze/reference.py).
    Bronze keeps every file version; silver uses only the newest one."""
    root = BRONZE_ROOT / "reference"
    if not (root / dataset / "_delta_log").exists() or not (root / "_batch_log" / "_delta_log").exists():
        log.warning("Reference table missing: %s (run pipeline/bronze/reference.py first)", root / dataset)
        return pd.DataFrame()
    from deltalake import DeltaTable
    batches = DeltaTable(str(root / "_batch_log")).to_pandas()
    batches = batches[(batches["dataset"] == dataset) & (batches["status"] == "success")]
    if batches.empty:
        return pd.DataFrame()
    newest = batches.sort_values("ingested_at")["batch_id"].iloc[-1]
    dt = DeltaTable(str(root / dataset))
    if versions is not None:
        versions[f"bronze/reference/{dataset}"] = dt.version()
        versions[f"bronze/reference/{dataset}#batch_id"] = newest
    df = dt.to_pandas()
    return df[df["_batch_id"] == newest].sort_values("_row_number").reset_index(drop=True)


def parse_raw(raw: pd.DataFrame, drop_id: bool = True) -> pd.DataFrame:
    """Turn bronze rows' raw_json into a DataFrame with one column per CBS field, plus the
    period status and ingest time from bronze's lineage columns. Used for bronze written
    before it had field columns, and for the tests' sample data.

    CBS pads keys with spaces ('301000 ', 'NL01  '); they are stripped here once, so
    every later join can compare keys directly. `drop_id`: in data rows the ID column is
    only a running number and is dropped; in code lists it is a real key and is kept."""
    if raw.empty:
        return pd.DataFrame()
    df = pd.DataFrame([json.loads(s) for s in raw["raw_json"]])
    df["_period_status"] = raw["_period_status"].to_numpy()
    df["_ingested_at"] = raw["_ingested_at"].to_numpy()
    return clean_fields(df, drop_id)


def clean_fields(df: pd.DataFrame, drop_id: bool) -> pd.DataFrame:
    """The first silver step for every CBS table, whichever way bronze was read: strip
    CBS's padding from text values ('301000 ' -> '301000') and drop the running number."""
    for col in df.columns:  # CBS pads keys with spaces: '301000 '
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(lambda v: v.strip() if isinstance(v, str) else v)
    return df.drop(columns=["ID"], errors="ignore") if drop_id else df


def latest(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Keep the most recently ingested row per key (safety net against duplicates).

    Bronze already replaces a revised period instead of appending it, so normally there
    is one row per key; this guarantees it even if bronze ever held two versions."""
    if df.empty or "_ingested_at" not in df:
        return df
    return df.sort_values("_ingested_at").drop_duplicates(keys, keep="last")


# =========================================================================== parsers
# A number as CBS writes it in titles: '20', or with a space as thousands separator '1 000'
_NUM = r"(\d{1,3}(?: \d{3})+|\d+)"


def _int(s: str) -> int:
    """'1 000' -> 1000: CBS writes thousands with a space in titles."""
    return int(s.replace(" ", ""))


def parse_size(title: str) -> tuple[str, int, int | None] | None:
    """'10 tot 20 werknemers' -> ('WN', 10, 20); '100 of meer ...' -> (.., 100, None);
    '2 werkzame personen' -> ('WP', 2, 3). Upper bound is exclusive."""
    t = " ".join(str(title).lower().split())
    # whole words only: "942 Werknemersorganisaties" is a sector (trade unions), not a size
    if re.search(r"\bwerknemers?\b", t):
        unit = "WN"   # employees
    elif re.search(r"\bwerkza(?:am|me) perso(?:on|nen)\b", t):
        unit = "WP"   # persons employed (incl. owners)
    else:
        return None
    if m := re.match(rf"^{_NUM} tot {_NUM}\b", t):              # '10 tot 20 ...'
        return unit, _int(m[1]), _int(m[2])
    if m := re.match(rf"^{_NUM}\b.*\bof meer\b", t):              # '100 ... of meer'
        return unit, _int(m[1]), None
    # a single number: '2 werkzame personen' is the class [2, 3)
    if m := re.match(rf"^{_NUM} (?:werkza(?:am|me) perso(?:on|nen)|werknemers?)\b", t):
        return unit, _int(m[1]), _int(m[1]) + 1
    return None


def size_key(unit: str, lo: int, hi: int | None) -> str:
    """Stable key for a size class: ('WN', 10, 20) -> 'WN_10_20', open-ended -> 'WN_100_plus'.

    WN = employees (werknemers), WP = persons employed (werkzame personen); the two
    units are never mixed, so the unit is part of the key."""
    return f"{unit}_{lo}_{'plus' if hi is None else hi}"


def size_range(lo: int, hi: int | None) -> str:
    """Readable range from an exclusive upper bound:
    (10, 20) -> '10-19', (2, 3) -> '2', (100, None) -> '100+'."""
    if hi is None:
        return f"{lo}+"
    return str(lo) if hi == lo + 1 else f"{lo}-{hi - 1}"


def size_band(lo: int, hi: int | None) -> str | None:
    """Band that fully contains the class, or None if the class spans several bands."""
    for name, blo, bhi in SIZE_BANDS:
        if lo >= blo and (bhi is None or (hi is not None and hi <= bhi)):
            return name
    return None


def aggregate_flags(classes: list[tuple[int, int | None]]) -> list[bool]:
    """True for a class that strictly contains another class of the same set.

    CBS lists both detailed classes (1-4, 5-9) and their sum (1-9). Summing all
    classes would count those companies twice, so aggregates are flagged and excluded
    whenever classes are added up."""
    def contains(a, b):
        (alo, ahi), (blo, bhi) = a, b
        a_hi = float("inf") if ahi is None else ahi
        b_hi = float("inf") if bhi is None else bhi
        return a != b and alo <= blo and b_hi <= a_hi
    return [any(contains(a, b) for b in classes) for a in classes]


# A sector title starts with its SBI code, then the name: 'C Industrie', '10-12 Voedings-,
# genotmiddelenindustrie', '24-30, 33 Metaal-elektro industrie', '4711 Supermarkten'
_SECTOR_RE = re.compile(
    r"^([A-U](?:-[A-U])?|\d{2,5}(?:-\d{2,5})?(?:,\s*\d{2,5}(?:-\d{2,5})?)*)\s+(.+)$")


def parse_sector(title: str) -> tuple[str | None, str, str]:
    """'C Industrie' -> ('C', 'Industrie', 'section'). Returns (sbi_code, name, level)."""
    title = str(title).strip()
    m = _SECTOR_RE.match(title)
    if not m:
        return None, title, "aggregate"   # e.g. 'Business Economy, B-N, excl. K'
    code, name = m[1], m[2]
    if code == "A-U":
        level = "total"
    elif re.fullmatch(r"[A-U]", code):
        level = "section"
    elif re.fullmatch(r"[A-U]-[A-U]", code):
        level = "section_group"
    elif code.isdigit():
        level = {2: "division", 3: "group", 4: "class", 5: "subclass"}[len(code)]
    else:
        level = "division_group"          # e.g. '10-12', '24-30, 33'
    return code, name, level


# CBS period keys: 4-digit year, kind (JJ year, KW quarter, MM month, HJ half year), number
_PERIOD_RE = re.compile(r"^(\d{4})(JJ|KW|MM|HJ)(\d{2})$")


def parse_period(key: str) -> dict:
    """CBS period key -> type, year, quarter/month, first and last day, and a label.

    '2025KW03' -> quarter 3 of 2025, 2025-07-01 .. 2025-09-30, '2025 Q3'. JJ = year,
    KW = quarter, HJ = half year, MM = month. Unknown formats get period_type 'other'."""
    m = _PERIOD_RE.match(key)
    if not m:
        return {"period_type": "other"}
    y, kind, n = int(m[1]), m[2], int(m[3])
    if kind == "JJ":
        start, months, ptype, label = date(y, 1, 1), 12, "year", f"{y}"
    elif kind == "KW":
        start, months, ptype, label = date(y, 3 * n - 2, 1), 3, "quarter", f"{y} Q{n}"
    elif kind == "HJ":
        start, months, ptype, label = date(y, 6 * n - 5, 1), 6, "half_year", f"{y} H{n}"
    else:
        start, months, ptype, label = date(y, n, 1), 1, "month", f"{y}-{n:02d}"
    end = (pd.Timestamp(start) + pd.DateOffset(months=months) - pd.Timedelta(days=1)).date()
    return {"period_type": ptype, "year": y,
            "quarter": n if kind == "KW" else (start.month - 1) // 3 + 1 if kind == "MM" else None,
            "month": n if kind == "MM" else None,
            "start_date": start, "end_date": end, "period_label": label}


def snake(key: str) -> str:
    """CBS measure key -> snake_case: 'AantalKeerVerzuimd_2' -> 'aantal_keer_verzuimd',
    'k_1Tot5Werkdagen_5' -> 'k_1_tot_5_werkdagen'."""
    base = re.sub(r"_\d+$", "", key)
    base = re.sub(r"^k_", "", base)
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", base)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    s = re.sub(r"(?<=[a-z])(?=\d)", "_", s).lower()
    return f"k_{s}" if s[:1].isdigit() else s


def region_level(key: str) -> str:
    """Level of a CBS region key, from its prefix: NL01 = country, PV20 = province,
    GM0014 = municipality, ..."""
    return {"NL": "country", "LD": "landsdeel", "PV": "province", "CR": "corop",
            "GM": "municipality"}.get(key[:2], "other")


# =========================================================================== metadata
def topics(dataprops: pd.DataFrame) -> pd.DataFrame:
    """Topic (measure) rows of DataProperties with their topic-group path.

    DataProperties is a tree: a Topic (measure) hangs under TopicGroups via ParentID.
    The path of group titles ('Werknemers met verzuim / Duur meest recente verzuimgeval')
    says what a measure belongs to; it is used to sort measures into breakdowns."""
    if dataprops.empty:
        return pd.DataFrame(columns=["measure_key", "title", "unit", "topic_group"])
    by_id = dataprops.set_index("ID") if "ID" in dataprops else None
    rows = []
    for _, r in dataprops[dataprops["Type"] == "Topic"].iterrows():
        path, parent = [], r.get("ParentID")
        while by_id is not None and pd.notna(parent) and parent in by_id.index:
            path.insert(0, by_id.at[parent, "Title"])
            parent = by_id.at[parent, "ParentID"]
        rows.append({"measure_key": r["Key"], "title": r["Title"], "unit": r.get("Unit"),
                     "topic_group": " / ".join(path) or None})
    return pd.DataFrame(rows)


# =========================================================================== dimensions
def build_map_characteristic(code_lists: dict[str, pd.DataFrame],
                             category_groups: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per (source_table, key) of every company-characteristic dimension.

    CBS puts sectors, size classes and the total in one list per table. This table
    says what each key is (member_type sector / size / total), so facts can be split
    into sector rows and size rows. Size classes are recognised from their title."""
    rows = []
    for table_id, members in code_lists.items():
        if members.empty:
            continue
        groups = category_groups.get(table_id, pd.DataFrame())
        group_title = dict(zip(groups.get("ID", []), groups.get("Title", [])))
        sizes = []
        for _, m in members.iterrows():
            key, title = m["Key"], str(m["Title"]).strip()
            row = {"source_table": table_id, "dimension": CHARACTERISTIC_DIMS[table_id],
                   "characteristic_key": key, "title": title,
                   "category_group": group_title.get(m.get("CategoryGroupID")),
                   "member_type": "sector", "sector_key": None, "size_key": None}
            if parsed := parse_size(title):          # a title like '10 tot 100 werknemers'
                row.update(member_type="size", size_key=size_key(*parsed))
                sizes.append((len(rows), parsed[1], parsed[2]))
            elif key == TOTAL_KEY or parse_sector(title)[2] == "total":
                row.update(member_type="total", sector_key=key)
            else:
                row["sector_key"] = key
            row["size_is_aggregate"] = None
            rows.append(row)
        for (i, *_), flag in zip(sizes, aggregate_flags([(lo, hi) for _, lo, hi in sizes])):
            rows[i]["size_is_aggregate"] = flag
    return pd.DataFrame(rows)


def build_dim_sector(code_lists: dict[str, pd.DataFrame], mapping: pd.DataFrame) -> pd.DataFrame:
    """One row per sector key (and the national total), with its SBI code, name and level.

    CBS uses the same keys for the same sector in every table (301000 = A Landbouw), so
    the dimension is shared ('conformed') across all fact tables. `sbi_level` (section,
    division, ...) lets gold and the dashboard pick one level and avoid double counting."""
    sector_keys = set(mapping.loc[mapping["member_type"].isin(["sector", "total"]),
                                  "characteristic_key"])
    titles = pd.concat([m[["Key", "Title"]] for m in code_lists.values() if not m.empty])
    titles = titles[titles["Key"].isin(sector_keys)]
    # same key in several tables: keep the most common title
    titles = (titles.groupby(["Key", "Title"]).size().reset_index(name="n")
              .sort_values(["Key", "n"], ascending=[True, False]).drop_duplicates("Key"))
    rows = []
    for _, r in titles.iterrows():
        code, name, level = parse_sector(r["Title"])
        rows.append({"sector_key": r["Key"], "sbi_code": code, "sector_name": name,
                     "sbi_level": level, "title": r["Title"]})
    return pd.DataFrame(rows).sort_values("sector_key").reset_index(drop=True)


def build_dim_company_size(titles: list[str]) -> pd.DataFrame:
    """One row per size class found in any table, with numeric bounds and a coarse band.

    The tables use different classes and units (employees in the survey, persons
    employed elsewhere); the band (<10, 10-99, 100+) is the common level for comparing them."""
    rows = {}
    for title in titles:
        if parsed := parse_size(title):
            unit, lo, hi = parsed
            rows.setdefault(size_key(unit, lo, hi), {
                "size_key": size_key(unit, lo, hi),
                "size_unit": "employees" if unit == "WN" else "persons_employed",
                "lower_bound": lo, "upper_bound_excl": hi,
                "size_range": size_range(lo, hi), "size_band": size_band(lo, hi),
                "title": " ".join(title.split())})
    df = pd.DataFrame(rows.values())
    if df.empty:
        return df
    df["lower_bound"] = df["lower_bound"].astype("Int64")
    df["upper_bound_excl"] = df["upper_bound_excl"].astype("Int64")
    return df.sort_values(["size_unit", "lower_bound", "upper_bound_excl"]).reset_index(drop=True)


def build_dim_region(regions: pd.DataFrame) -> pd.DataFrame:
    """Regions of 81644NED with their level (country, province, ...); the suffix
    '(PV)', '(GM)' etc. is removed from the name."""
    if regions.empty:
        return pd.DataFrame(columns=["region_key", "region_name", "region_level", "description"])
    return pd.DataFrame({
        "region_key": regions["Key"],
        "region_name": regions["Title"].str.replace(r"\s*\((LD|PV|CR|GM)\)$", "", regex=True),
        "region_level": regions["Key"].map(region_level),
        "description": regions.get("Description"),
    })


def build_dim_period(period_keys: set[str]) -> pd.DataFrame:
    """One row per period key that occurs in any fact table, with its dates and label."""
    rows = [{"period_key": k, **parse_period(k)} for k in sorted(period_keys)]
    df = pd.DataFrame(rows)
    for col in ("year", "quarter", "month"):
        if col in df:
            df[col] = df[col].astype("Int64")
    return df


# =========================================================================== facts
def attach_characteristic(df: pd.DataFrame, table_id: str, mapping: pd.DataFrame,
                          cols=("member_type", "sector_key", "size_key", "size_is_aggregate")
                          ) -> pd.DataFrame:
    """Rename a table's company-characteristic column to `characteristic_key` and add
    what each key is (sector, size class or total) from `map_characteristic`.

    Keys missing from the code list are kept with member_type 'unknown' and logged, so a
    new CBS key is visible instead of silently dropped."""
    dim = CHARACTERISTIC_DIMS[table_id]
    m = mapping[mapping["source_table"] == table_id][["characteristic_key", *cols]]
    out = df.rename(columns={dim: "characteristic_key"}).merge(m, on="characteristic_key", how="left")
    unknown = out["member_type"].isna()
    if unknown.any():
        log.warning("%s: %d rows with keys missing from the code list: %s", table_id,
                    unknown.sum(), sorted(out.loc[unknown, "characteristic_key"].unique())[:10])
        out.loc[unknown, "member_type"] = "unknown"
    return out


def build_fct_absence_rate(data: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """80072NED: one row per quarter x sector or size class with the absence percentage
    (sick days as % of available working days)."""
    if data.empty:
        return pd.DataFrame()
    dim = CHARACTERISTIC_DIMS["80072NED"]
    data = latest(data, [dim, "Perioden"])
    # the measure's key ends in a position number ('_1'); find it by its stable prefix
    measure = next(c for c in data.columns if c.startswith("Ziekteverzuimpercentage"))
    df = attach_characteristic(data, "80072NED", mapping)
    return pd.DataFrame({
        "period_key": df["Perioden"], "characteristic_key": df["characteristic_key"],
        "member_type": df["member_type"], "sector_key": df["sector_key"],
        "size_key": df["size_key"],
        "absence_pct": pd.to_numeric(df[measure], errors="coerce"),
        "period_status": df["_period_status"], "ingested_at": df["_ingested_at"],
    }).sort_values(["period_key", "characteristic_key"]).reset_index(drop=True)


_MARGIN_COLS = {"MW": "value", "MOG": "lower_95", "MBG": "upper_95"}


def _margin_col(key: str) -> str:
    """CBS margin key -> column: MW00000 -> value, MOG0095 -> lower_95,
    MBG0095 -> upper_95."""
    return next((v for p, v in _MARGIN_COLS.items() if key.startswith(p)), key.lower())


def build_dim_survey_measure(dataprops: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per survey measure (86009NED, 83130NED): key, snake_case name, CBS title,
    topic group and unit, taken from DataProperties."""
    frames = []
    for table_id in SURVEY_TABLES:
        t = topics(dataprops.get(table_id, pd.DataFrame()))
        if t.empty:
            continue
        t["source_table"] = table_id
        t["measure_name"] = t["measure_key"].map(snake)
        # same name twice (e.g. 'Rug' until 2022 and from 2022): add the group's era
        dup = t["measure_name"].duplicated(keep=False)
        era = t["topic_group"].fillna("").str.extract(r"\((tot|va\.?)\s*(\d{4})\)")
        suffix = era[0].map({"tot": "_until_", "va.": "_from_", "va": "_from_"}) + era[1]
        t.loc[dup & suffix.notna(), "measure_name"] += suffix[dup & suffix.notna()]
        still = t["measure_name"].duplicated(keep=False)
        t.loc[still, "measure_name"] += "_" + t.loc[still, "measure_key"].str.extract(r"_(\d+)$")[0]
        frames.append(t)
    if not frames:
        return pd.DataFrame()
    cols = ["source_table", "measure_key", "measure_name", "title", "topic_group", "unit"]
    return pd.concat(frames)[cols].reset_index(drop=True)


def build_fct_absence_survey(data: dict[str, pd.DataFrame], dim_measure: pd.DataFrame,
                             mapping: pd.DataFrame) -> pd.DataFrame:
    """86009NED and 83130NED in long format: one row per period x characteristic x
    measure, with the estimate and its 95% bounds side by side.

    In the source the bounds are separate rows (dimension 'Marges'); they are pivoted
    into columns so a value and its interval stay together. The two tables measure
    similar things over different populations, so `source_table` keeps them apart."""
    frames = []
    for table_id in SURVEY_TABLES:
        df = data.get(table_id, pd.DataFrame())
        if df.empty:
            continue
        dim = CHARACTERISTIC_DIMS[table_id]
        df = latest(df, [dim, "Marges", "Perioden"])
        measures = dim_measure.loc[dim_measure["source_table"] == table_id, "measure_key"]
        measures = [m for m in measures if m in df.columns] or [
            c for c in df.columns if c not in {dim, "Marges", "Perioden", *META_COLS}]
        # wide (one column per measure) -> long (one row per measure), then the three
        # margin rows per measure (estimate, lower, upper) -> three columns
        long = df.melt(id_vars=[dim, "Marges", "Perioden", *META_COLS], value_vars=measures,
                       var_name="measure_key", value_name="v")
        long["margin"] = long["Marges"].map(_margin_col)
        long["v"] = pd.to_numeric(long["v"], errors="coerce")
        idx = [dim, "Perioden", "measure_key"]
        wide = long.pivot_table(index=idx, columns="margin", values="v", aggfunc="first",
                                dropna=False).reset_index()
        meta = long.groupby(idx, as_index=False)[META_COLS].last()
        wide = wide.merge(meta, on=idx, how="left")
        for col in _MARGIN_COLS.values():
            if col not in wide:
                wide[col] = float("nan")
        wide["source_table"] = table_id
        wide = attach_characteristic(wide, table_id, mapping)
        frames.append(wide)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True).merge(
        dim_measure[["source_table", "measure_key", "measure_name"]],
        on=["source_table", "measure_key"], how="left")
    df["measure_name"] = df["measure_name"].fillna(df["measure_key"].map(snake))
    out = pd.DataFrame({
        "source_table": df["source_table"], "period_key": df["Perioden"],
        "characteristic_key": df["characteristic_key"], "member_type": df["member_type"],
        "sector_key": df["sector_key"], "size_key": df["size_key"],
        "size_is_aggregate": df["size_is_aggregate"],
        "measure_key": df["measure_key"], "measure_name": df["measure_name"],
        "value": df["value"], "lower_95": df["lower_95"], "upper_95": df["upper_95"],
        "period_status": df["_period_status"], "ingested_at": df["_ingested_at"],
    })
    return out.sort_values(["source_table", "period_key", "characteristic_key",
                            "measure_key"]).reset_index(drop=True)


def build_fct_counts(table_id: str, data: pd.DataFrame, dataprops: pd.DataFrame,
                     mapping: pd.DataFrame, extra_dims: dict[str, str] | None = None
                     ) -> pd.DataFrame:
    """Long count table: one row per period x sector (x region) x measure.

    Used for 81589NED (businesses) and 81644NED (establishments). Their measures are
    a total, counts per size class and counts per legal form; DataProperties' topic
    groups say which is which ('breakdown'). Size measures get a size key, band and
    aggregate flag, so gold can add up classes without double counting."""
    if data.empty:
        return pd.DataFrame()
    extra_dims = extra_dims or {}
    dim = CHARACTERISTIC_DIMS[table_id]
    t = topics(dataprops)
    if t.empty:
        raise RuntimeError(f"{table_id}: DataProperties missing in bronze; run pipeline/bronze/cbs.py")
    t = t[t["measure_key"].isin(data.columns)].copy()
    grp = t["topic_group"].fillna("").str.lower()
    # which breakdown a measure belongs to, from its topic group's Dutch title
    t["breakdown"] = "total"
    t.loc[grp.str.contains("grootte"), "breakdown"] = "size"
    t.loc[grp.str.contains("rechtsvorm"), "breakdown"] = "legal_form"

    parsed = t["title"].map(parse_size)
    t["size_key"] = [size_key(*p) if p and b == "size" else None
                     for p, b in zip(parsed, t["breakdown"])]
    t["size_band"] = [size_band(p[1], p[2]) if p and b == "size" else None
                      for p, b in zip(parsed, t["breakdown"])]
    size_rows = t["breakdown"] == "size"
    flags = aggregate_flags([(p[1], p[2]) for p in parsed[size_rows]])
    t["size_is_aggregate"] = None
    t.loc[size_rows, "size_is_aggregate"] = flags
    t["legal_form"] = t["title"].where(t["breakdown"] == "legal_form")

    data = latest(data, [dim, "Perioden", *extra_dims])
    id_vars = [dim, "Perioden", *extra_dims, *META_COLS]
    long = data.melt(id_vars=id_vars, value_vars=list(t["measure_key"]),
                     var_name="measure_key", value_name="count")
    long = long.merge(t[["measure_key", "breakdown", "size_key", "size_band",
                         "size_is_aggregate", "legal_form"]], on="measure_key", how="left")
    long = attach_characteristic(long, table_id, mapping, cols=("member_type",))
    out = pd.DataFrame({
        "period_key": long["Perioden"], "sector_key": long["characteristic_key"],
        "member_type": long["member_type"],
        **{name: long[src] for src, name in extra_dims.items()},
        "breakdown": long["breakdown"], "measure_key": long["measure_key"],
        "size_key": long["size_key"], "size_band": long["size_band"],
        "size_is_aggregate": long["size_is_aggregate"], "legal_form": long["legal_form"],
        # Int64 (nullable): a count is a whole number, but CBS leaves some cells empty
        "count": pd.to_numeric(long["count"], errors="coerce").round().astype("Int64"),
        "period_status": long["_period_status"], "ingested_at": long["_ingested_at"],
    })
    keys = ["period_key", "sector_key", *extra_dims.values(), "measure_key"]
    return out.sort_values(keys).reset_index(drop=True)


# =========================================================================== orchestration
def build_map_sbi2008_sbi2025(ref: pd.DataFrame) -> pd.DataFrame:
    """The SBI 2008 -> SBI 2025 crosswalk (latest file version) with, per link, how many
    codes each side has: `link_type` is 1:1, split (one 2008 code -> several 2025 codes),
    merge (several 2008 codes -> one 2025 code) or many-to-many. pipeline/silver/kvk.py uses it to
    translate KVK's SBI 2025 codes back to the SBI 2008 sectors of the CBS tables."""
    if ref.empty:
        return pd.DataFrame()
    df = pd.DataFrame({
        "sbi2008_code": ref["sbi2008"], "sbi2008_division": ref["sbi2008"].str[:2],
        "sbi2008_title": ref["sbi2008_title"],
        "sbi2025_code": ref["sbi2025"], "sbi2025_division": ref["sbi2025"].str[:2],
        "sbi2025_title": ref["sbi2025_title"], "link_description": ref["link_description"]})
    df = df.drop_duplicates(["sbi2008_code", "sbi2025_code"])     # bronze checks this too
    df["sbi2025_targets"] = df.groupby("sbi2008_code")["sbi2025_code"].transform("size")
    df["sbi2008_sources"] = df.groupby("sbi2025_code")["sbi2008_code"].transform("size")
    split, merge = df["sbi2025_targets"] > 1, df["sbi2008_sources"] > 1
    df["link_type"] = "1:1"
    df.loc[split & ~merge, "link_type"] = "split"
    df.loc[merge & ~split, "link_type"] = "merge"
    df.loc[split & merge, "link_type"] = "many-to-many"
    df["source_version"] = ref["_source_version"].to_numpy()
    df["batch_id"] = ref["_batch_id"].to_numpy()
    df["ingested_at"] = ref["_ingested_at"].to_numpy()
    return df.sort_values(["sbi2008_code", "sbi2025_code"]).reset_index(drop=True)


def build_all(read=read_bronze, read_ref=read_reference) -> dict[str, pd.DataFrame]:
    """Build every silver CBS table from bronze; returns {table name: DataFrame}.

    `read` and `read_ref` are passed in (instead of reading Delta directly) so the tests
    can feed sample data, and run() can record which bronze versions were read."""
    code_lists = {t: read(t, dim) for t, dim in CHARACTERISTIC_DIMS.items()}
    category_groups = {t: read(t, "CategoryGroups") for t in ("80072NED", *SURVEY_TABLES)}
    dataprops = {t: read(t, "DataProperties") for t in CHARACTERISTIC_DIMS}

    mapping = build_map_characteristic(code_lists, category_groups)
    dim_measure = build_dim_survey_measure(dataprops)

    fct_rate = build_fct_absence_rate(read("80072NED", "TypedDataSet"), mapping)
    fct_survey = build_fct_absence_survey(
        {t: read(t, "TypedDataSet") for t in SURVEY_TABLES}, dim_measure, mapping)
    fct_business = build_fct_counts("81589NED", read("81589NED", "TypedDataSet"),
                                    dataprops["81589NED"], mapping)
    fct_estab = build_fct_counts("81644NED", read("81644NED", "TypedDataSet"),
                                 dataprops["81644NED"], mapping, extra_dims={"RegioS": "region_key"})

    facts = [fct_rate, fct_survey, fct_business, fct_estab]
    period_keys = set().union(*[set(f["period_key"]) for f in facts if not f.empty])
    # size classes come from two places: members of the characteristic list (absence
    # tables) and measure titles in the count tables ('2 werkzame personen')
    size_titles = list(mapping.loc[mapping["member_type"] == "size", "title"])
    for t in ("81589NED", "81644NED"):
        tp = topics(dataprops[t])
        size_titles += list(tp.loc[tp["topic_group"].fillna("").str.lower()
                                   .str.contains("grootte"), "title"])

    return {
        "dim_period": build_dim_period(period_keys),
        "dim_sector": build_dim_sector(code_lists, mapping),
        "dim_company_size": build_dim_company_size(size_titles),
        "dim_region": build_dim_region(read("81644NED", "RegioS")),
        "dim_survey_measure": dim_measure,
        "map_characteristic": mapping,
        "map_sbi2008_sbi2025": build_map_sbi2008_sbi2025(read_ref("sbi2008_sbi2025")),
        "fct_absence_rate": fct_rate,
        "fct_absence_survey": fct_survey,
        "fct_business_count": fct_business,
        "fct_establishment_count": fct_estab,
    }


def run() -> str:
    """Build, stage, publish and log one silver CBS run (pipeline/common/publish.py). Returns the run_id;
    raises (after logging the failed run) when something went wrong."""
    from functools import partial
    from pipeline.common.publish import run_pipeline

    def build(inputs: dict) -> dict:
        return build_all(read=partial(read_bronze, versions=inputs),
                         read_ref=partial(read_reference, versions=inputs))

    return run_pipeline(PIPELINE, build, SILVER_ROOT, RUN_LOG, OPTIONAL_TABLES)


def main() -> None:
    """Command line: build and publish silver CBS, or only summarise with --preview."""
    ap = argparse.ArgumentParser(description="Build the silver layer from bronze CBS tables.")
    ap.add_argument("--preview", action="store_true", help="build and summarise, write nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.preview:
        for name, df in build_all().items():
            if df.empty:
                log.warning("%-26s empty", name)
            else:
                log.info("%-26s %8d rows  columns: %s", name, len(df), ", ".join(df.columns))
        return
    try:
        run()
    except Exception:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
