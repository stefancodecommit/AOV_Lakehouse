"""
HTTP for the CBS OData (v3) API: a session that retries, and fetching every page of one
request. Used by pipeline/bronze/cbs.py, which decides *what* to request (endpoints,
periods, request keys) and where it goes; this module only knows *how* to get it.

Paging: CBS returns at most PAGE_SIZE records per response. fetch_all follows
odata.nextLink when CBS gives one, and continues with $skip when a page comes back full
without one. Between pages it waits PAUSE_SECONDS (config.yaml, cbs.pause_seconds), to
be polite to the API.
"""
from __future__ import annotations

import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.common.config import settings

PAGE_SIZE = 10_000          # CBS ODataApi returns at most 10,000 records per response
PAUSE_SECONDS = float(settings["cbs"]["pause_seconds"])


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
        elif len(page) >= PAGE_SIZE:                   # full page, no nextLink -> $skip
            skip += len(page)
            next_url, next_params = url, {**base_params, "$skip": str(skip)}
        else:
            next_url = None
    return records, pages
