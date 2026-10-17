"""Checks on a KVK open-dataset file (zip with one CSV, or the CSV itself) before it is
written to bronze. Used by pipeline/bronze/kvk_agent.py and, later, Dagster.

Errors (the file is rejected):
  - not a readable zip with a CSV inside, or a .csv
  - this exact file (SHA-256) was already ingested
  - header not ';'-separated, or a required column missing
  - no data rows, or more than `max_malformed_share` rows with a wrong field count
Warnings (counted per value check, the file is still accepted):
  - actief not J/N, rechtsvorm not BV/NV, postcode region not 2 digits, start date not
    in a known date format
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

DELIMITER = ";"
DATE_RE = re.compile(r"\d{8}|\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}")


@dataclass
class FileCheck:
    path: Path
    sha256: str
    csv_member: str | None = None
    encoding: str | None = None
    header: list[str] = field(default_factory=list)
    row_count: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)       # extra report fields

    @property
    def passed(self) -> bool:
        """True when there are no errors (warnings don't block a file)."""
        return not self.errors

    def report(self) -> dict:
        """The check result as a dict: the JSON report shown to the agent and saved in
        quarantine."""
        # the file name only: the full path holds the home folder, so a username (R10)
        return {"file": Path(self.path).name, **self.details, "sha256": self.sha256,
                "status": "PASSED" if self.passed else "FAILED",
                "errors": self.errors, "warnings": self.warnings}


# --------------------------------------------------------------------------- file helpers
def norm(name: str) -> str:
    """'Datum aanvang' -> 'datumaanvang': compare headers without case or punctuation."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def sha256(path: Path) -> str:
    """SHA-256 of a file: its fingerprint, used to recognise a file that was already loaded."""
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def open_csv_bytes(path: Path, member: str | None):
    """Binary stream of the CSV, from inside the zip or the file itself."""
    if member:
        return zipfile.ZipFile(path).open(member)
    return Path(path).open("rb")


def detect_encoding(path: Path, member: str | None) -> str:
    """The CSV's text encoding, guessed from its first megabyte: UTF-8 if it decodes,
    else Windows-1252 (common for Dutch exports), else Latin-1 (never fails)."""
    with open_csv_bytes(path, member) as f:
        sample = f.read(1 << 20)
    for enc in ("utf-8-sig", "cp1252"):
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def iter_rows(path: Path, member: str | None, encoding: str):
    """Yield the CSV rows (header first) as lists of strings, reading the file as a stream."""
    raw = open_csv_bytes(path, member)
    text = io.TextIOWrapper(raw, encoding=encoding, newline="")
    try:
        yield from csv.reader(text, delimiter=DELIMITER)
    finally:
        text.close()


# --------------------------------------------------------------------------- checks
def find_csv(path: Path, check: FileCheck) -> None:
    """Sets check.csv_member for a zip; adds an error when there is no usable CSV."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            bad = z.testzip()
            csvs = [i for i in z.infolist() if i.filename.lower().endswith(".csv")]
            if bad:
                check.errors.append(f"Zip is corrupt at member {bad}")
            elif not csvs:
                check.errors.append(f"No CSV in zip; contents: {z.namelist()[:10]}")
            else:
                largest = max(csvs, key=lambda i: i.file_size)
                check.csv_member = largest.filename
                if len(csvs) > 1:
                    check.warnings.append(f"{len(csvs)} CSVs in zip, using the largest: "
                                          f"{largest.filename}")
                check.details["csv_member"] = largest.filename
                check.details["csv_size_mb"] = round(largest.file_size / 1e6, 1)
    elif Path(path).suffix.lower() != ".csv":
        check.errors.append("File is neither a zip nor a .csv")


def match_columns(header: list[str], required: dict[str, set[str]],
                  optional: dict[str, set[str]]) -> tuple[dict[str, int], list[str]]:
    """Logical column -> index in the header, and an error per missing required column."""
    normed = {norm(h): i for i, h in enumerate(header)}
    found, errors = {}, []
    for logical, aliases in {**required, **optional}.items():
        idx = next((normed[a] for a in aliases if a in normed), None)
        if idx is not None:
            found[logical] = idx
        elif logical in required:
            errors.append(f"Required column missing: {logical} (accepted: {sorted(aliases)})")
    return found, errors


def check_rows(rows, n_fields: int, found: dict[str, int]) -> tuple[int, int, dict[str, int]]:
    """Count rows, malformed rows and value-check hits (data rows only, no header)."""
    total = malformed = 0
    hits = {"active_not_J_N": 0, "legal_form_not_BV_NV": 0,
            "postcode_region_not_2_digits": 0, "start_date_unparsed": 0}
    for row in rows:
        total += 1
        if len(row) != n_fields:
            malformed += 1
            continue
        if row[found["active"]].strip().upper() not in {"J", "N"}:
            hits["active_not_J_N"] += 1
        if row[found["legal_form"]].strip().upper() not in {"BV", "NV"}:
            hits["legal_form_not_BV_NV"] += 1
        if not re.fullmatch(r"\d{2}", row[found["postcode_region"]].strip()):
            hits["postcode_region_not_2_digits"] += 1
        d = row[found["start_date"]].strip()
        if d and not DATE_RE.fullmatch(d):
            hits["start_date_unparsed"] += 1
    return total, malformed, hits


def check_file(path: Path, required: dict[str, set[str]], optional: dict[str, set[str]],
               max_malformed_share: float, ingested: dict[str, str] | None = None) -> FileCheck:
    """All checks for one file. `ingested` maps the SHA-256 of files already in bronze
    to their batch_id."""
    path = Path(path)
    if not path.is_file():
        return FileCheck(path, "", errors=[f"File not found: {path}"])

    check = FileCheck(path, sha256(path))
    find_csv(path, check)
    if (batch := (ingested or {}).get(check.sha256)):
        check.errors.append(f"This exact file was already ingested as batch {batch}")
        check.details["already_ingested"] = batch
    if check.errors:
        return check

    check.encoding = detect_encoding(path, check.csv_member)
    check.details["encoding"] = check.encoding
    reader = iter_rows(path, check.csv_member, check.encoding)
    check.header = [h.strip() for h in next(reader, [])]
    check.details["columns"] = check.header
    if len(check.header) < 2:
        check.errors.append(f"Header doesn't look ';'-separated: {check.header[:3]}")
    found, missing = match_columns(check.header, required, optional)
    check.errors += missing
    check.details["recognised_columns"] = {k: check.header[v] for k, v in found.items()}
    if check.errors:
        return check

    rows, malformed, hits = check_rows(reader, len(check.header), found)
    check.row_count = rows
    check.details["row_count"] = rows
    check.details["malformed_rows"] = malformed
    if rows == 0:
        check.errors.append("CSV has a header but no data rows")
    elif malformed / rows > max_malformed_share:
        check.errors.append(f"{malformed} of {rows} rows have the wrong number of fields")
    for name, n in hits.items():
        if n:
            check.warnings.append(f"{name}: {n} rows ({n / max(rows, 1):.2%})")
    return check
