"""
Bronze ingestion for CBS OData (v3) endpoints into Delta Lake (delta-rs, no Spark needed).

How "ask only once" works
-------------------------
* Every API request gets a deterministic *request key*.
* After its data has landed in bronze, the key is written to the Delta table
  `bronze/_ingestion_log`. On the next run, any key already in the log is skipped,
  so the same API request is never sent twice.
* TypedDataSets can be split into one request per period (`split_by_period=True`).
  The script first reads the small `Perioden` list, then only requests the periods
  that are not in the log yet. New quarters/years are picked up automatically.
* The period's CBS status (Voorlopig / Nader voorlopig / Definitief) is part of the
  request key. When CBS revises a provisional period, the key changes, that one
  period is fetched again, and its old rows are replaced (not duplicated).
* Pagination inside a request is followed until the last page (odata.nextLink,
  or $skip when a page comes back full without a nextLink).
* Before writing, the records are checked (checks/cbs.py): data requests against the
  table's DataProperties (columns, period, numeric measures, unique keys, not empty),
  code lists on not being empty. A batch that fails is not written and not logged,
  so it's retried next run, and it's saved to lakehouse/quarantine/cbs/ with a report.
  DataProperties is fetched live, once per table per run, only when there is new data.
* Source lifecycle: every run fetches each data table's TableInfos (one small request
  per table) and warns when CBS has stopped the table or hasn't modified it for longer
  than its frequency allows (checks/cbs.py). The TableInfos record is stored in
  bronze/<table>_tableinfos only when it changed: its Modified and MetaDataModified
  dates are part of the request key, so bronze keeps one row per version.

What a bronze table holds
-------------------------
Lineage columns (request key, table, period and its status, source URL, ingest time,
record index), the record exactly as received in `raw_json`, and one column per CBS
field with the value as received: no stripping, renaming or recoding, that is silver's
job. Field types come from the table's DataProperties (dimensions as text, measures by
their Datatype); fields not described there are typed from their values, and nested
values are kept as JSON text. A field CBS adds later becomes a new column; a later batch
can't change an existing column's type. A field named like a lineage column is stored
as `field_<name>`. `--rebuild-columns` rebuilds the field columns from raw_json.

Scope cleanup
-------------
When the configuration shrinks (a later start year, another period preference, a table
or endpoint removed from config.yaml), `--cleanup-scope` removes the bronze rows and
ingestion-log entries that fall outside it, so bronze matches the configuration again.
Periods are judged with the same rule as ingestion (select_periods on CBS's current
period list): only periods before the start year or of a type that is no longer chosen
are removed. Run it with --dry-run first.

Usage
-----
    pip install -r requirements.txt
    python -m pipeline.bronze.cbs                            # ingest everything that's new
    python -m pipeline.bronze.cbs --dry-run                  # show what would be requested
    python -m pipeline.bronze.cbs --only 81589               # limit to endpoints whose name contains "81589"
    python -m pipeline.bronze.cbs --force                    # ignore the log and re-request (replaces rows)
    python -m pipeline.bronze.cbs --cleanup-scope --dry-run  # list what is outside the config
    python -m pipeline.bronze.cbs --cleanup-scope            # remove it
    python -m pipeline.bronze.cbs --rebuild-columns          # field columns again from raw_json
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.common.config import project_path, settings

_CFG = settings["cbs"]
BASE_URL = _CFG["base_url"]
BRONZE_ROOT = project_path(settings["paths"]["lakehouse"]) / "bronze"
LOG_TABLE = BRONZE_ROOT / "_ingestion_log"
QUARANTINE_ROOT = project_path(settings["paths"]["lakehouse"]) / "quarantine"
PAGE_SIZE = 10_000          # CBS ODataApi returns at most 10,000 records per response
# (when a page comes back full without a nextLink, fetch_all continues with $skip)
PAUSE_SECONDS = float(_CFG["pause_seconds"])

log = logging.getLogger("cbs_bronze")


# --------------------------------------------------------------------------- config
START_YEAR = int(_CFG["start_year"])                      # first year for TypedDataSets
PERIOD_PREFERENCE = tuple(_CFG["period_preference"])      # e.g. quarters, else years


@dataclass(frozen=True)
class Endpoint:
    table_id: str                    # e.g. "86009NED"
    entity: str = ""                 # "" = service root (list of entity sets)
    period_op: str | None = None     # eq | ge | gt | le | lt (only for non-split requests)
    period_value: str | None = None  # e.g. "2015KW01"
    top: int | None = None           # optional $top, sent exactly as given
    split_by_period: bool = False    # one request per period -> incremental
    start_year: int = START_YEAR     # split requests: first year to fetch
    period_types: tuple[str, ...] = PERIOD_PREFERENCE  # split requests: first type present wins

    @property
    def name(self) -> str:
        """Bronze folder name, e.g. '86009ned_typeddataset' ('_root' for the service root)."""
        return f"{self.table_id}_{self.entity or 'root'}".lower()

    @property
    def url(self) -> str:
        """OData URL of the endpoint, without query parameters."""
        return f"{BASE_URL}/{self.table_id}" + (f"/{self.entity}" if self.entity else "")

    def odata_filter(self) -> str | None:
        """OData $filter for a fixed period selection, e.g. "Perioden ge '2025JJ00'"."""
        if self.period_op:
            return f"Perioden {self.period_op} '{self.period_value}'"
        return None


def endpoints_from_config(cfg: dict) -> list[Endpoint]:
    """`tables` -> their TypedDataSet, one request per period from START_YEAR up to the
    latest CBS publishes; `metadata` -> "TABLE/Entity" (or "TABLE" for the service root),
    one request each. A metadata entry may also be a mapping of Endpoint fields."""
    eps = [Endpoint(t, "TypedDataSet", split_by_period=True) for t in cfg["tables"]]
    for item in cfg["metadata"]:
        if isinstance(item, dict):
            eps.append(Endpoint(**item))
        else:
            table_id, _, entity = str(item).partition("/")
            eps.append(Endpoint(table_id, entity))
    return eps


ENDPOINTS: list[Endpoint] = endpoints_from_config(_CFG)


# --------------------------------------------------------------------------- schemas
# pyarrow/deltalake are imported where Delta is read or written, so the planning and
# HTTP logic (and its tests, and --dry-run) work without them.
def bronze_schema():
    """The fixed columns of every CBS bronze table: lineage columns (_...) plus the raw
    record as JSON text. After these, every bronze table has one column per field of the
    records (see structured_columns), so bronze is readable and still a faithful copy."""
    import pyarrow as pa
    return pa.schema([
        ("_request_key", pa.string()),
        ("_table_id", pa.string()),
        ("_entity", pa.string()),
        ("_period", pa.string()),            # null when not split by period
        ("_period_status", pa.string()),
        ("_source_url", pa.string()),
        ("_ingested_at", pa.timestamp("us", tz="UTC")),
        ("_record_index", pa.int64()),
        ("raw_json", pa.string()),
    ])


def log_schema():
    """Columns of bronze/_ingestion_log: one row per request whose data is in bronze."""
    import pyarrow as pa
    return pa.schema([
        ("request_key", pa.string()),
        ("table_id", pa.string()),
        ("entity", pa.string()),
        ("period", pa.string()),
        ("source_url", pa.string()),
        ("row_count", pa.int64()),
        ("pages", pa.int64()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ])


# --------------------------------------------------------------------------- planning
@dataclass(frozen=True)
class Request:
    endpoint: Endpoint
    key: str
    params: dict
    period: str | None = None
    period_status: str | None = None

    @property
    def display_url(self) -> str:
        """Full URL including the query, as stored in bronze for lineage."""
        return f"{self.endpoint.url}?{urlencode(self.params)}" if self.params else self.endpoint.url


def select_periods(periods: list[dict], start_year: int,
                   preference: tuple[str, ...] = PERIOD_PREFERENCE) -> list[dict]:
    """Keep periods of the preferred type (KW = quarter, JJ = year, MM = month)
    from start_year on. CBS keys look like '2020KW01', '2020JJ00', '2020MM01'.
    The first type in `preference` that the dataset actually has is used."""
    def year(p): return int(p["Key"].strip()[:4])
    def ptype(p): return p["Key"].strip()[4:6]

    available = {ptype(p) for p in periods}
    chosen = next((t for t in preference if t in available), None)
    if chosen is None:
        log.warning("No period of type %s found; available: %s", preference, sorted(available))
        return []
    return [p for p in periods if ptype(p) == chosen and year(p) >= start_year]


def plan_requests(session: requests.Session, ep: Endpoint) -> list[Request]:
    """The requests needed for one endpoint, each with its request key.

    For a data table (split_by_period) this first asks CBS which periods exist, keeps
    the configured ones, and makes one request per period. The period's status is in
    the key: when CBS revises 'Voorlopig' to 'Definitief' the key changes, so run()
    fetches that period again. Other endpoints are one request with a fixed key."""
    if ep.split_by_period:
        periods, _ = fetch_all(session, f"{BASE_URL}/{ep.table_id}/Perioden", {})
        reqs = []
        selected = select_periods(periods, ep.start_year, ep.period_types)
        if selected:
            log.info("%-40s %d periods: %s .. %s", ep.name, len(selected),
                     selected[0]["Key"].strip(), selected[-1]["Key"].strip())
        for p in selected:
            # CBS pads period keys with spaces ('2024KW03 '); the status is part of the key
            key_val, status = p["Key"].strip(), (p.get("Status") or "").strip()
            reqs.append(Request(
                endpoint=ep,
                key=f"{ep.table_id}/{ep.entity}?Perioden={key_val}#status={status}",
                params={"$filter": f"Perioden eq '{key_val}'"},
                period=key_val,
                period_status=status,
            ))
        return reqs

    params = {}
    if ep.odata_filter():
        params["$filter"] = ep.odata_filter()
    if ep.top:
        params["$top"] = str(ep.top)
    key = f"{ep.table_id}/{ep.entity}" + (f"?{urlencode(sorted(params.items()))}" if params else "")
    return [Request(endpoint=ep, key=key, params=params)]


# --------------------------------------------------------------------------- http
def make_session() -> requests.Session:
    """HTTP session that retries up to 5 times, with increasing waits, on rate limits
    (429) and server errors (5xx), so a short CBS hiccup doesn't fail a run."""
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["Accept"] = "application/json"
    return s


def fetch_all(session: requests.Session, url: str, params: dict) -> tuple[list[dict], int]:
    """Fetch every page of one logical request. Returns (records, pages)."""
    records: list[dict] = []
    pages = 0
    user_top = "$top" in params
    base_params = {**params, "$format": "json"}
    next_url, next_params, skip = url, base_params, 0

    while next_url:
        resp = session.get(next_url, params=next_params, timeout=180)
        resp.raise_for_status()
        payload = resp.json()
        page = payload.get("value", [])
        records.extend(page)
        pages += 1
        time.sleep(PAUSE_SECONDS)

        if payload.get("odata.nextLink"):            # server-driven paging
            next_url, next_params = payload["odata.nextLink"], None
        elif len(page) >= PAGE_SIZE and not user_top:  # full page, no nextLink -> $skip
            skip += len(page)
            next_url, next_params = url, {**base_params, "$skip": str(skip)}
        else:
            next_url = None
    return records, pages


# --------------------------------------------------------------------------- delta io
def done_keys() -> set[str]:
    """Request keys already in bronze (from bronze/_ingestion_log); run() skips these."""
    if not (LOG_TABLE / "_delta_log").exists():
        return set()
    from deltalake import DeltaTable
    return set(DeltaTable(str(LOG_TABLE)).to_pyarrow_table(columns=["request_key"])
               .column("request_key").to_pylist())


# --------------------------------------------------------------------------- structured columns
# Bronze stores each record twice: as the exact JSON text (raw_json, the faithful copy)
# and as one column per field, with the field's own name and its value as received. No
# renaming, no stripping of CBS's padding spaces, no interpretation of codes: that is
# silver's job. A field's type comes from CBS's description of the table (DataProperties)
# when there is one; otherwise from the values. (Inferring from values alone isn't safe:
# 80072NED's absence % arrives as 5 in one record and 5.4 in another.)
_NUMERIC = {"Double": "float64", "Float": "float64", "Decimal": "float64",
            "Long": "int64", "Integer": "int64", "Int16": "int64", "Int32": "int64",
            "Int64": "int64", "Short": "int64", "Byte": "int64"}
_DIMENSION_TYPES = {"Dimension", "TimeDimension", "GeoDimension", "GeoDetail"}


def field_types(dataprops: list[dict] | None) -> dict[str, str]:
    """Field -> Arrow type name from DataProperties: measures by their Datatype,
    dimensions as text. Fields not described there are inferred from their values."""
    out = {}
    for d in dataprops or []:
        if d.get("Type") in _DIMENSION_TYPES:
            out[d["Key"]] = "string"
        elif d.get("Type") == "Topic":
            out[d["Key"]] = _NUMERIC.get(d.get("Datatype"), "string")
    return out


def _infer_type(values: list) -> str:
    kinds = {type(v) for v in values if v is not None}
    if not kinds or kinds == {str}:
        return "string"
    if kinds == {bool}:
        return "bool"
    if kinds == {int}:
        return "int64"
    if kinds <= {int, float}:
        return "float64"
    return "json"                     # nested values or a mix of kinds: kept as JSON text


def structured_columns(records: list[dict], dataprops: list[dict] | None = None):
    """One Arrow column per field (in order of first appearance), values as received."""
    import pyarrow as pa
    names = list(dict.fromkeys(k for r in records for k in r))
    types = field_types(dataprops)
    cols = {}
    for name in names:
        values = [r.get(name) for r in records]
        kind = types.get(name) or _infer_type(values)
        if kind == "json":
            cols[name] = pa.array([None if v is None else json.dumps(v, ensure_ascii=False)
                                   for v in values], pa.string())
        elif kind == "float64":
            cols[name] = pa.array([None if v is None else float(v) for v in values], pa.float64())
        else:
            cols[name] = pa.array(values, {"string": pa.string(), "bool": pa.bool_(),
                                           "int64": pa.int64()}[kind])
    return cols


def bronze_table(req: "Request", records: list[dict], now: datetime,
                 dataprops: list[dict] | None = None):
    """The rows to write for one request: lineage columns, raw_json, then the fields."""
    import pyarrow as pa
    ep, n = req.endpoint, len(records)
    fixed = pa.table({
        "_request_key": [req.key] * n,
        "_table_id": [ep.table_id] * n,
        "_entity": [ep.entity or "root"] * n,
        "_period": [req.period] * n,
        "_period_status": [req.period_status] * n,
        "_source_url": [req.display_url] * n,
        "_ingested_at": [now] * n,
        "_record_index": list(range(n)),
        "raw_json": [json.dumps(r, ensure_ascii=False) for r in records],
    }, schema=bronze_schema())
    for name, column in structured_columns(records, dataprops).items():
        if name in fixed.column_names:       # a field named like a lineage column
            name = f"field_{name}"
        fixed = fixed.append_column(name, column)
    return fixed


def _fit_to_table(path: Path, table):
    """Cast new rows to the types an existing table already has for the same columns,
    so a batch can't change a column's type; new fields become new columns (merge)."""
    from deltalake import DeltaTable
    import pyarrow as pa
    if not (path / "_delta_log").exists():
        return table
    existing = pa.schema(DeltaTable(str(path)).schema().to_arrow())
    fields = [existing.field(f.name) if f.name in existing.names else f for f in table.schema]
    return table.cast(pa.schema(fields))


def write_bronze(req: Request, records: list[dict], dataprops: list[dict] | None = None) -> None:
    """Write the records of one request to its bronze table.

    First deletes earlier rows of the same period (or the same request key), then
    appends. That makes the write repeatable: a revised period replaces its old rows,
    and a retry after a crash can't create duplicates. `dataprops` (the table's
    DataProperties) gives the fields their types; without it they are inferred."""
    from deltalake import DeltaTable, write_deltalake
    ep = req.endpoint
    path = BRONZE_ROOT / ep.name
    now = datetime.now(timezone.utc)

    # Replace earlier rows for the same period/request (revisions or --force), so a
    # crash between "write data" and "write log" can never create duplicates.
    if (path / "_delta_log").exists():
        pred = (f"_period = '{req.period}'" if req.period
                else f"_request_key = '{req.key.replace(chr(39), chr(39) * 2)}'")
        DeltaTable(str(path)).delete(pred)

    if not records:
        return
    table = _fit_to_table(path, bronze_table(req, records, now, dataprops))
    # merge: a field CBS adds later becomes a new column (older rows: empty)
    write_deltalake(str(path), table, mode="append", schema_mode="merge")


def log_success(req: Request, row_count: int, pages: int) -> None:
    """Record a request in bronze/_ingestion_log, after its data was written.

    The order matters: data first, log second. If the run stops in between, the request
    isn't logged, so the next run fetches and writes it again (replacing, see
    write_bronze)."""
    import pyarrow as pa
    from deltalake import write_deltalake
    ep = req.endpoint
    row = pa.table({
        "request_key": [req.key], "table_id": [ep.table_id], "entity": [ep.entity or "root"],
        "period": [req.period], "source_url": [req.display_url],
        "row_count": [row_count], "pages": [pages],
        "ingested_at": [datetime.now(timezone.utc)],
    }, schema=log_schema())
    write_deltalake(str(LOG_TABLE), row, mode="append")


# --------------------------------------------------------------------------- checks
def check_records(session, req: Request, records: list[dict], dataprops_cache: dict) -> list:
    """Run the checks for one request. DataProperties are fetched once per table."""
    from checks.cbs import check_metadata, check_typed_dataset
    ep = req.endpoint
    if ep.entity != "TypedDataSet":
        return check_metadata(records)
    if ep.table_id not in dataprops_cache:
        dataprops_cache[ep.table_id], _ = fetch_all(
            session, f"{BASE_URL}/{ep.table_id}/DataProperties", {})
    return check_typed_dataset(records, dataprops_cache[ep.table_id], req.period)


def table_infos_request(table_id: str, info: dict) -> Request:
    """Request for a table's TableInfos whose key contains CBS's modification dates, so a
    new snapshot is only stored when CBS changed the table description."""
    ep = Endpoint(table_id, "TableInfos")
    key = (f"{table_id}/TableInfos#modified={info.get('Modified')}"
           f"#metadata={info.get('MetaDataModified')}")
    return Request(endpoint=ep, key=key, params={})


def ingest_table_infos(session, table_ids: list[str], already: set[str],
                       dry_run: bool) -> tuple[int, int, int]:
    """Lifecycle warnings per table, and a bronze snapshot when TableInfos changed.
    Returns (snapshots written, warnings, failed requests)."""
    from checks import warnings
    from checks.cbs import check_table_lifecycle
    written = warned = failed = 0
    for table_id in table_ids:
        try:
            infos, pages = fetch_all(session, f"{BASE_URL}/{table_id}/TableInfos", {})
        except requests.RequestException as e:
            log.error("Could not fetch TableInfos for %s: %s", table_id, e)
            failed += 1
            continue
        if not infos:
            log.error("TableInfos for %s is empty", table_id)
            failed += 1
            continue
        for w in warnings(check_table_lifecycle(infos[0])):
            log.warning("%s: %s", table_id, w.detail)
            warned += 1
        req = table_infos_request(table_id, infos[0])
        if req.key in already or dry_run:
            continue
        write_bronze(req, infos)
        log_success(req, len(infos), pages)
        already.add(req.key)
        written += 1
        log.info("%-40s TableInfos changed (modified %s), snapshot stored",
                 req.endpoint.name, infos[0].get("Modified"))
    return written, warned, failed


def quarantine_batch(req: Request, records: list[dict], results: list, pages: int):
    """Save a batch that failed its checks to lakehouse/quarantine/cbs/ with a report
    (request, source URL, every check result) and the records as received."""
    from checks.quarantine import quarantine
    report = {"request_key": req.key, "table_id": req.endpoint.table_id,
              "entity": req.endpoint.entity or "root", "period": req.period,
              "period_status": req.period_status, "source_url": req.display_url,
              "row_count": len(records), "pages": pages,
              "checks": [r.as_dict() for r in results]}
    return quarantine(QUARANTINE_ROOT, "cbs", req.endpoint.name + (f"_{req.period}" if req.period else ""),
                      report, records=records)


# --------------------------------------------------------------------------- scope cleanup
def configured_table_names() -> set[str]:
    """Bronze folder names that the configuration produces, TableInfos snapshots included."""
    names = {e.name for e in ENDPOINTS}
    names |= {Endpoint(e.table_id, "TableInfos").name for e in ENDPOINTS if e.split_by_period}
    return names


def out_of_scope_periods(ep: Endpoint, present: set[str], cbs_periods: list[dict]) -> set[str]:
    """Periods in bronze that ingestion wouldn't select now: before the start year, or
    of a period type that is no longer the chosen one. Periods CBS no longer lists are
    kept (that is CBS's change, not a configuration change)."""
    selected = {p["Key"].strip() for p in select_periods(cbs_periods, ep.start_year, ep.period_types)}
    listed = {p["Key"].strip() for p in cbs_periods}
    return {p for p in present if p in listed and p not in selected}


def _sql_list(values) -> str:
    """Values as a quoted SQL list for a Delta delete predicate: 'a', 'b'."""
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in sorted(values))


def cleanup_scope(session=None, dry_run: bool = True) -> list[str]:
    """Remove bronze tables, rows and log entries outside the configuration.
    Returns a description of every action (done, or with dry_run only planned)."""
    import shutil
    from deltalake import DeltaTable
    actions: list[str] = []
    log_dt = DeltaTable(str(LOG_TABLE)) if (LOG_TABLE / "_delta_log").exists() else None
    log_df = (log_dt.to_pandas(columns=["table_id", "entity"]).drop_duplicates()
              if log_dt else None)
    configured = configured_table_names()

    # 1. whole tables that are no longer configured (KVK and reference live in subfolders)
    for path in sorted(BRONZE_ROOT.glob("*/_delta_log")):
        table = path.parent
        if table.name.startswith("_") or table.name in configured:
            continue
        actions.append(f"remove table bronze/{table.name} (not in config.yaml)")
        pairs = []
        if log_df is not None:
            pairs = [(r.table_id, r.entity) for r in log_df.itertuples()
                     if f"{r.table_id}_{r.entity}".lower() == table.name]
        if not dry_run:
            shutil.rmtree(table)
            for table_id, entity in pairs:
                log_dt.delete(f"table_id = '{table_id}' AND entity = '{entity}'")

    # 2. periods outside the start year / period preference
    session = session or make_session()
    for ep in ENDPOINTS:
        path = BRONZE_ROOT / ep.name
        if not ep.split_by_period or not (path / "_delta_log").exists():
            continue
        dt = DeltaTable(str(path))
        present = set(dt.to_pandas(columns=["_period"])["_period"].dropna())
        cbs_periods, _ = fetch_all(session, f"{BASE_URL}/{ep.table_id}/Perioden", {})
        drop = out_of_scope_periods(ep, present, cbs_periods)
        if not drop:
            continue
        actions.append(f"remove {len(drop)} periods from bronze/{ep.name}: "
                       f"{', '.join(sorted(drop))}")
        if not dry_run:
            dt.delete(f"_period IN ({_sql_list(drop)})")
            if log_dt:
                log_dt.delete(f"table_id = '{ep.table_id}' AND entity = '{ep.entity}' "
                              f"AND period IN ({_sql_list(drop)})")
    return actions


def _bronze_dataprops(table_id: str) -> list[dict] | None:
    """A table's DataProperties as stored in bronze (None when not ingested)."""
    from deltalake import DeltaTable
    path = BRONZE_ROOT / Endpoint(table_id, "DataProperties").name
    if not (path / "_delta_log").exists():
        return None
    return [json.loads(x) for x in DeltaTable(str(path)).to_pandas(columns=["raw_json"])["raw_json"]]


def rebuild_columns(dry_run: bool = False) -> list[str]:
    """Rebuild the field columns of every CBS bronze table from its raw_json, without
    calling the API: for tables written before bronze had a column per field, or after a
    change in how fields are typed. Each table is overwritten as a whole (a new Delta
    version; the old one stays readable through time travel). The lineage columns,
    raw_json and the ingestion log are left as they are. Returns the tables done."""
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    fixed_names = bronze_schema().names
    done = []
    for log_dir in sorted(BRONZE_ROOT.glob("*/_delta_log")):   # kvk/ and reference/ are deeper
        path = log_dir.parent
        if path.name.startswith("_"):
            continue
        dt = DeltaTable(str(path))
        fixed = dt.to_pyarrow_table(columns=fixed_names).cast(bronze_schema())
        if dry_run:
            done.append(f"bronze/{path.name}: {fixed.num_rows} rows")
            continue
        records = [json.loads(x) for x in fixed.column("raw_json").to_pylist()]
        table_id = fixed.column("_table_id")[0].as_py() if fixed.num_rows else None
        table = fixed
        for name, column in structured_columns(records, _bronze_dataprops(table_id)).items():
            table = table.append_column(f"field_{name}" if name in fixed_names else name, column)
        write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")
        done.append(f"bronze/{path.name}: {table.num_rows} rows, "
                    f"{table.num_columns - len(fixed_names)} field columns")
    return done


# --------------------------------------------------------------------------- main
def run(only: str | None, force: bool, dry_run: bool) -> dict:
    """Ingest everything new for the configured endpoints.

    Per request: skip it when its key is already logged; otherwise fetch every page,
    check the records, and either quarantine them or write them to bronze and log the
    key. A failing request doesn't stop the run; it is retried next time. `only`
    limits the run to endpoints whose name contains that text; `force` ignores the log
    (rows are replaced, not duplicated); `dry_run` only lists the requests. Returns the
    counts (fetched, skipped, failed, ...), which Dagster uses to decide whether
    anything new arrived."""
    from checks import failures
    session = make_session()
    already = set() if force else done_keys()
    fetched = skipped = failed = quarantined = 0
    dataprops_cache: dict[str, list[dict]] = {}

    # 1. lifecycle: warn about stopped or stale tables, store changed TableInfos
    data_tables = list(dict.fromkeys(e.table_id for e in ENDPOINTS if e.split_by_period))
    data_tables = [t for t in data_tables
                   if not only or only.lower() in f"{t}_tableinfos".lower()]
    infos_written, warned, infos_failed = ingest_table_infos(session, data_tables, already,
                                                             dry_run)
    failed += infos_failed

    # 2. data and code lists, one request at a time
    for ep in ENDPOINTS:
        if only and only.lower() not in ep.name:
            continue
        try:
            requests_ = plan_requests(session, ep)
        except requests.RequestException as e:
            log.error("Could not plan %s: %s", ep.name, e)
            failed += 1
            continue

        for req in requests_:
            if req.key in already:          # "ask only once": already in bronze
                skipped += 1
                continue
            if dry_run:
                log.info("[dry-run] would fetch %s -> bronze/%s", req.display_url, ep.name)
                continue
            try:
                records, pages = fetch_all(session, ep.url, req.params)
                results = check_records(session, req, records, dataprops_cache)
                if failures(results):
                    folder = quarantine_batch(req, records, results, pages)
                    quarantined += 1
                    log.error("Checks failed for %s: %s -> %s", req.display_url,
                              "; ".join(f"{r.name}: {r.detail}" for r in failures(results)),
                              folder or "already in quarantine")
                    continue
                write_bronze(req, records, dataprops_cache.get(ep.table_id))
                log_success(req, len(records), pages)
                already.add(req.key)
                fetched += 1
                log.info("%-40s %6d rows  %d page(s)  %s", ep.name, len(records), pages,
                         req.period or "")
            except Exception as e:  # keep going; failed keys are retried next run
                failed += 1
                log.error("Failed %s: %s", req.display_url, e)

    summary = {"fetched": fetched, "skipped": skipped, "failed": failed,
               "quarantined": quarantined, "tableinfos_changed": infos_written,
               "lifecycle_warnings": warned}
    log.info("Done. fetched=%d skipped(already in bronze)=%d failed=%d quarantined=%d "
             "tableinfos_changed=%d lifecycle_warnings=%d", *summary.values())
    return summary          # for Dagster: did this run bring new data?


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest CBS OData endpoints into a bronze Delta Lake.")
    ap.add_argument("--only", help="only endpoints whose name contains this text, e.g. 81589")
    ap.add_argument("--force", action="store_true", help="ignore the ingestion log and re-request")
    ap.add_argument("--dry-run", action="store_true", help="list requests without fetching data")
    ap.add_argument("--cleanup-scope", action="store_true",
                    help="remove bronze data outside config.yaml instead of ingesting "
                         "(combine with --dry-run to only list it)")
    ap.add_argument("--rebuild-columns", action="store_true",
                    help="rebuild bronze's field columns from raw_json, without API calls")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.rebuild_columns:
        for line in rebuild_columns(dry_run=args.dry_run):
            log.info("%s%s", "[dry-run] " if args.dry_run else "", line)
    elif args.cleanup_scope:
        actions = cleanup_scope(dry_run=args.dry_run)
        for a in actions:
            log.info("%s%s", "[dry-run] " if args.dry_run else "", a)
        log.info("Scope cleanup: %d action(s)%s", len(actions), " planned" if args.dry_run else "")
    else:
        run(args.only, args.force, args.dry_run)
