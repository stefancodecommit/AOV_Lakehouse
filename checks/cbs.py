"""Checks on CBS OData records before they are written to bronze.

The expected structure of a table comes from its DataProperties: the dimension columns
(Type Dimension/TimeDimension/GeoDimension/GeoDetail) and the measures (Type Topic, with
a Datatype such as Double, Float or Long).
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone

from checks import CheckResult

# How far behind a table may be before it counts as stale: the time since CBS last
# modified it (TableInfos.Modified), per publication frequency. Matches the freshness
# rule in docs/PROJECT.md (quarterly 4 months, yearly 15 months).
MAX_AGE = {"Perkwartaal": timedelta(days=122), "Perjaar": timedelta(days=456),
           "Permaand": timedelta(days=62), "Perweek": timedelta(days=14)}
STOPPED_FREQUENCY = "Stopgezet"
REGULAR_STATUS = "Regulier"

DIMENSION_TYPES = {"Dimension", "TimeDimension", "GeoDimension", "GeoDetail"}
NUMERIC_DATATYPES = {"Double", "Float", "Decimal", "Long", "Integer", "Int16", "Int32",
                     "Int64", "Short", "Byte"}
MAX_EXAMPLES = 5


def table_structure(dataprops: list[dict]) -> tuple[list[str], dict[str, str | None]]:
    """(dimension columns, {measure column: datatype}) from DataProperties records."""
    dims = [d["Key"] for d in dataprops if d.get("Type") in DIMENSION_TYPES]
    measures = {d["Key"]: d.get("Datatype") for d in dataprops if d.get("Type") == "Topic"}
    return dims, measures


def _strip(v):
    """Remove CBS's padding spaces from text values; other values unchanged."""
    return v.strip() if isinstance(v, str) else v


def check_typed_dataset(records: list[dict], dataprops: list[dict],
                        period: str | None = None) -> list[CheckResult]:
    """All checks for one TypedDataSet request (one period when `period` is given)."""
    dims, measures = table_structure(dataprops)
    results = [check_not_empty(records)]
    if not dims and not measures:
        results.append(CheckResult("structure_known", False,
                                   "DataProperties has no dimensions or measures"))
        return results
    if not records:
        return results

    # 1. every record has every dimension and measure column
    expected = dims + list(measures)
    missing = Counter(col for r in records for col in expected if col not in r)
    results.append(CheckResult(
        "has_all_columns", not missing,
        f"{sum(missing.values())} missing values in columns {sorted(missing)}" if missing
        else f"{len(dims)} dimensions and {len(measures)} measures in every record",
        [{"column": c, "records_missing": n} for c, n in missing.most_common(MAX_EXAMPLES)]))

    # 2. the period in every record is the one that was requested
    if period is not None:
        wrong = Counter(_strip(r.get("Perioden")) for r in records
                        if _strip(r.get("Perioden")) != period)
        results.append(CheckResult(
            "period_matches_request", not wrong,
            f"{sum(wrong.values())} records for other periods than {period}" if wrong
            else f"all records are for {period}",
            [{"period": p, "records": n} for p, n in wrong.most_common(MAX_EXAMPLES)]))

    # 3. numeric measures are numbers or null (bool is not a number here)
    numeric = [m for m, t in measures.items() if t in NUMERIC_DATATYPES]
    bad = [(m, r[m]) for r in records for m in numeric
           if m in r and r[m] is not None
           and (isinstance(r[m], bool) or not isinstance(r[m], (int, float)))]
    results.append(CheckResult(
        "measures_numeric_or_null", not bad,
        f"{len(bad)} non-numeric values in numeric measures" if bad
        else f"{len(numeric)} numeric measures are numbers or null",
        [{"column": m, "value": repr(v)} for m, v in bad[:MAX_EXAMPLES]]))

    # 4. no two records for the same combination of dimension values
    keys = Counter(tuple(_strip(r.get(d)) for d in dims) for r in records)
    dups = [(k, n) for k, n in keys.items() if n > 1]
    results.append(CheckResult(
        "unique_dimension_keys", not dups,
        f"{len(dups)} dimension key combinations occur more than once" if dups
        else f"{len(keys)} unique dimension key combinations",
        [{"key": dict(zip(dims, k)), "records": n} for k, n in dups[:MAX_EXAMPLES]]))
    return results


def check_not_empty(records: list[dict]) -> CheckResult:
    """A period that CBS lists as published (or a code list) must have records."""
    return CheckResult("not_empty", bool(records), f"{len(records)} records")


def check_metadata(records: list[dict]) -> list[CheckResult]:
    """Code lists, DataProperties, Perioden and the service root."""
    return [check_not_empty(records)]


def check_table_lifecycle(info: dict, now: datetime | None = None) -> list[CheckResult]:
    """Warnings (never errors) from a table's TableInfos record: stopped by CBS, or not
    modified for longer than its publication frequency allows."""
    now = now or datetime.now(timezone.utc)
    freq, status = info.get("Frequency"), info.get("OutputStatus")
    stopped = freq == STOPPED_FREQUENCY or (status not in (None, REGULAR_STATUS))
    results = [CheckResult(
        "table_not_stopped", not stopped,
        f"CBS has stopped this table (Frequency={freq}, OutputStatus={status}, period "
        f"{info.get('Period')}); no new data expected" if stopped
        else f"Frequency={freq}, OutputStatus={status}", severity="warning")]

    modified = info.get("Modified")
    if not stopped and freq in MAX_AGE and modified:
        # CBS gives Dutch local time without a zone; a few hours don't matter here
        age = now - datetime.fromisoformat(modified).replace(tzinfo=timezone.utc)
        stale = age > MAX_AGE[freq]
        results.append(CheckResult(
            "table_recently_modified", not stale,
            f"last modified {modified[:10]}, {age.days} days ago; expected within "
            f"{MAX_AGE[freq].days} days for {freq}" if stale
            else f"last modified {modified[:10]} ({age.days} days ago)", severity="warning"))
    return results
