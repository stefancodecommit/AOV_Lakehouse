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

Bronze tables keep the raw record as JSON plus lineage columns; parsing and typing
happen in silver.

Usage
-----
    pip install -r requirements.txt
    python cbs_bronze.py                 # ingest everything that's new
    python cbs_bronze.py --dry-run       # show what would be requested
    python cbs_bronze.py --only 81589    # limit to endpoints whose name contains "81589"
    python cbs_bronze.py --force         # ignore the log and re-request (replaces rows)
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

from config import project_path, settings

_CFG = settings["cbs"]
BASE_URL = _CFG["base_url"]
BRONZE_ROOT = project_path(settings["paths"]["lakehouse"]) / "bronze"
LOG_TABLE = BRONZE_ROOT / "_ingestion_log"
QUARANTINE_ROOT = project_path(settings["paths"]["lakehouse"]) / "quarantine"
PAGE_SIZE = 10_000          # CBS ODataApi returns at most 10,000 records per response
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
        return f"{self.table_id}_{self.entity or 'root'}".lower()

    @property
    def url(self) -> str:
        return f"{BASE_URL}/{self.table_id}" + (f"/{self.entity}" if self.entity else "")

    def odata_filter(self) -> str | None:
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
    if ep.split_by_period:
        periods, _ = fetch_all(session, f"{BASE_URL}/{ep.table_id}/Perioden", {})
        reqs = []
        selected = select_periods(periods, ep.start_year, ep.period_types)
        if selected:
            log.info("%-40s %d periods: %s .. %s", ep.name, len(selected),
                     selected[0]["Key"].strip(), selected[-1]["Key"].strip())
        for p in selected:
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
    if not (LOG_TABLE / "_delta_log").exists():
        return set()
    from deltalake import DeltaTable
    return set(DeltaTable(str(LOG_TABLE)).to_pyarrow_table(columns=["request_key"])
               .column("request_key").to_pylist())


def write_bronze(req: Request, records: list[dict]) -> None:
    import pyarrow as pa
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
    n = len(records)
    table = pa.table({
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
    write_deltalake(str(path), table, mode="append")


def log_success(req: Request, row_count: int, pages: int) -> None:
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


def quarantine_batch(req: Request, records: list[dict], results: list, pages: int):
    from checks.quarantine import quarantine
    report = {"request_key": req.key, "table_id": req.endpoint.table_id,
              "entity": req.endpoint.entity or "root", "period": req.period,
              "period_status": req.period_status, "source_url": req.display_url,
              "row_count": len(records), "pages": pages,
              "checks": [r.as_dict() for r in results]}
    return quarantine(QUARANTINE_ROOT, "cbs", req.endpoint.name + (f"_{req.period}" if req.period else ""),
                      report, records=records)


# --------------------------------------------------------------------------- main
def run(only: str | None, force: bool, dry_run: bool) -> None:
    from checks import failures
    session = make_session()
    already = set() if force else done_keys()
    fetched = skipped = failed = quarantined = 0
    dataprops_cache: dict[str, list[dict]] = {}

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
            if req.key in already:
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
                write_bronze(req, records)
                log_success(req, len(records), pages)
                already.add(req.key)
                fetched += 1
                log.info("%-40s %6d rows  %d page(s)  %s", ep.name, len(records), pages,
                         req.period or "")
            except Exception as e:  # keep going; failed keys are retried next run
                failed += 1
                log.error("Failed %s: %s", req.display_url, e)

    log.info("Done. fetched=%d skipped(already in bronze)=%d failed=%d quarantined=%d",
             fetched, skipped, failed, quarantined)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest CBS OData endpoints into a bronze Delta Lake.")
    ap.add_argument("--only", help="only endpoints whose name contains this text, e.g. 81589")
    ap.add_argument("--force", action="store_true", help="ignore the ingestion log and re-request")
    ap.add_argument("--dry-run", action="store_true", help="list requests without fetching data")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(args.only, args.force, args.dry_run)
