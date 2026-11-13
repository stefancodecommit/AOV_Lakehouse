"""Freshness: is the newest data as recent as it should be? (warnings, never blocking)

A pipeline can fail silently: every run succeeds, but nothing new arrives (a changed
URL, a stopped table, nobody uploading KVK files). These checks compare the newest data
with today:
- CBS: the newest period ended at most `cbs.max_data_age_months` ago (config.yaml; per
  period type: the period's length + CBS's publication delay + slack)
- tables CBS has stopped are skipped (the lifecycle check already warns about them)
- KVK: the newest snapshot is at most kvk.max_snapshot_age_days old

This is not the same rule as the lifecycle check in checks/cbs.py, which measures how
long ago CBS last *updated* a table; that limit is shorter, because it has no
publication delay in it.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from checks import CheckResult


def period_end(key: str) -> date | None:
    """Last day of a CBS period: '2026KW02' -> 2026-06-30, '2025JJ00' -> 2025-12-31."""
    year, kind, n = int(key[:4]), key[4:6], int(key[6:8])
    if kind == "JJ":
        return date(year, 12, 31)
    if kind in ("KW", "MM"):
        month = 3 * n if kind == "KW" else n
        return (pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)).date()
    return None


def months_between(a: date, b: date) -> float:
    return (b - a).days / 30.44


def check_cbs_freshness(latest: dict[str, str], stopped: set[str], max_months: dict[str, float],
                        today: date | None = None) -> list[CheckResult]:
    """`latest`: table_id -> newest period key in bronze; `stopped`: tables CBS stopped;
    `max_months`: period type (KW, JJ) -> limit (`cbs.max_data_age_months`). A period type
    without a limit isn't checked."""
    today = today or date.today()
    results = []
    for table_id, key in sorted(latest.items()):
        if table_id in stopped:
            continue
        end, limit = period_end(key), max_months.get(key[4:6])
        if end is None or limit is None:
            continue
        behind = months_between(end, today)
        when = (f"ended {behind:.1f} months ago" if behind >= 0
                else "hasn't ended yet")     # e.g. a count on 1 January of next year
        results.append(CheckResult(
            f"{table_id}_fresh", behind <= limit,
            f"newest period {key} {when} (limit {limit} months)", severity="warning"))
    return results


def check_kvk_freshness(latest_snapshot: date | None, max_days: int,
                        today: date | None = None) -> CheckResult:
    today = today or date.today()
    if latest_snapshot is None:
        return CheckResult("kvk_fresh", False, "no KVK snapshot loaded yet", severity="warning")
    age = (today - latest_snapshot).days
    detail = f"newest snapshot from {latest_snapshot} is {age} days old (limit {max_days})"
    if age > max_days:
        detail += "; upload a new file to inbox/kvk/"
    return CheckResult("kvk_fresh", age <= max_days, detail, severity="warning")
