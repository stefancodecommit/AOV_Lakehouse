"""
KVK intake agent: a local Ollama model guides you through getting the
"KVK Handelsregister Open Dataset Basis Bedrijfsgegevens" into the KVK bronze Delta Lake.

Flow
----
1. The agent tells you where to download the zip and asks you to upload it
   (type or drag the file path into the terminal).
2. verify_dataset() checks the file. The checks run in Python, not in the model:
     - a readable zip with a CSV inside (or a CSV directly)
     - SHA-256 not ingested before (the same file is never loaded twice)
     - semicolon-separated header with the expected columns
     - every row has the right number of fields (max 0.1% malformed)
     - value checks on actief, rechtsvorm, postcode region and start date (warnings)
3. Only if verification PASSED for that exact file (same hash) can
   ingest_verified_dataset() run, and it still asks you to type YES at the keyboard.
   The model cannot type that for you.
4. Ingest writes the rows to bronze in ONE Delta commit, tagged with a new batch_id,
   then records the batch in the batch log.

Bronze tables
-------------
lakehouse/bronze/kvk/basis_bedrijfsgegevens   raw CSV rows (all text) +
                                              _batch_id, _ingested_at, _source_file,
                                              _source_sha256, _row_number
lakehouse/bronze/kvk/_batch_log               batch_id, ingested_at, source_file,
                                              file_sha256, csv_member, row_count,
                                              column_count, verified_at,
                                              verification_report, confirmed_by, status

Usage
-----
    ollama pull qwen2.5:7b                     # any model that supports tool calling
    python kvk_bronze_agent.py                 # chat with the agent
    python kvk_bronze_agent.py --model llama3.1
    python kvk_bronze_agent.py --file ~/Downloads/kvk-open-dataset-basis-bedrijfsgegevens.zip
                                               # no LLM: verify, confirm, ingest directly
"""
from __future__ import annotations

import argparse
import csv
import getpass
import hashlib
import io
import json
import re
import sys
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------- config
KVK_ROOT = Path("lakehouse/bronze/kvk")
DATA_TABLE = KVK_ROOT / "basis_bedrijfsgegevens"
LOG_TABLE = KVK_ROOT / "_batch_log"

DATASET_PAGE = "https://www.kvk.nl/producten-bestellen/kvk-handelsregister-open-data-set/"
DOWNLOAD_URL = "https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip"
LICENSE = "CC BY 4.0 (source: KVK)"

DELIMITER = ";"
MAX_MALFORMED_SHARE = 0.001        # fail if more than 0.1% of rows have a wrong field count
BATCH_ROWS = 200_000               # rows per Arrow batch while writing (memory, not commits)

# Logical column -> accepted header spellings, compared after lower-casing and removing
# everything that isn't a letter or digit ("datum_aanvang" == "datumAanvang").
REQUIRED_COLUMNS = {
    "start_date": {"datumaanvang", "startdatum"},
    "active": {"actief", "indicatieactief"},
    "legal_form": {"rechtsvormcode", "rechtsvorm"},
    "postcode_region": {"postcoderegio", "postcode"},
}
OPTIONAL_COLUMNS = {
    "insolvency": {"insolventiecode", "insolventie"},
    # Real file (2026-10): "SBI activiteiten" = all codes, "Hoofdactiviteiten" = main
    # code(s); both comma-separated lists within one row per company.
    "sbi": {"sbiactiviteiten", "sbicode", "sbi", "activiteiten"},
    "main_activity": {"hoofdactiviteiten", "hoofdactiviteit"},
    "member_state": {"lidstaat"},
}


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


# --------------------------------------------------------------------------- state
@dataclass
class Verification:
    path: str
    sha256: str
    passed: bool
    csv_member: str | None
    encoding: str | None
    header: list[str]
    row_count: int
    report: dict
    verified_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


VERIFIED: dict[str, Verification] = {}   # resolved file path -> latest verification


# --------------------------------------------------------------------------- file helpers
def _resolve(file_path: str) -> Path:
    # dragging a file into a terminal often adds quotes or escaped spaces
    cleaned = file_path.strip().strip("'\"").replace("\\ ", " ")
    return Path(cleaned).expanduser().resolve()


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _open_csv_bytes(path: Path, member: str | None):
    """Binary stream of the CSV, from inside the zip or the file itself."""
    if member:
        return zipfile.ZipFile(path).open(member)
    return path.open("rb")


def _detect_encoding(path: Path, member: str | None) -> str:
    with _open_csv_bytes(path, member) as f:
        sample = f.read(1 << 20)
    for enc in ("utf-8-sig", "cp1252"):
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _iter_rows(path: Path, member: str | None, encoding: str):
    raw = _open_csv_bytes(path, member)
    text = io.TextIOWrapper(raw, encoding=encoding, newline="")
    try:
        yield from csv.reader(text, delimiter=DELIMITER)
    finally:
        text.close()


def ingested_hashes() -> dict[str, str]:
    """file_sha256 -> batch_id of every successful batch."""
    if not (LOG_TABLE / "_delta_log").exists():
        return {}
    from deltalake import DeltaTable
    log = DeltaTable(str(LOG_TABLE)).to_pyarrow_table(
        columns=["file_sha256", "batch_id", "status"]).to_pylist()
    return {r["file_sha256"]: r["batch_id"] for r in log if r["status"] == "success"}


# --------------------------------------------------------------------------- tools
def get_dataset_info() -> str:
    """
    Get where and how to download the KVK Handelsregister Open Dataset Basis Bedrijfsgegevens,
    and what the user must do next.

    Returns:
        str: JSON with the dataset page, download link, licence and instructions.
    """
    return json.dumps({
        "dataset": "KVK Handelsregister Open Dataset Basis Bedrijfsgegevens",
        "page": DATASET_PAGE,
        "download_url": DOWNLOAD_URL,
        "format": "zip containing one semicolon-separated CSV (too large for Excel)",
        "refreshed": "every working day",
        "license": LICENSE,
        "next_step": "Ask the user to download the zip and give its file path "
                     "(they can drag the file into the terminal).",
    })


def verify_dataset(file_path: str) -> str:
    """
    Verify an uploaded KVK dataset file (zip or csv) before it may be ingested.
    Must be called before ingest_verified_dataset.

    Args:
        file_path (str): Path to the downloaded zip or csv file on this computer.

    Returns:
        str: JSON verification report with status PASSED or FAILED and the reasons.
    """
    errors, warnings = [], []
    path = _resolve(file_path)
    result = {"file": str(path)}

    if not path.is_file():
        return json.dumps({**result, "status": "FAILED", "errors": [f"File not found: {path}"]})

    member = None
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            bad = z.testzip()
            csvs = [i for i in z.infolist() if i.filename.lower().endswith(".csv")]
            if bad:
                errors.append(f"Zip is corrupt at member {bad}")
            elif not csvs:
                errors.append(f"No CSV in zip; contents: {z.namelist()[:10]}")
            else:
                member = max(csvs, key=lambda i: i.file_size).filename
                if len(csvs) > 1:
                    warnings.append(f"{len(csvs)} CSVs in zip, using the largest: {member}")
                result["csv_member"] = member
                result["csv_size_mb"] = round(max(c.file_size for c in csvs) / 1e6, 1)
    elif path.suffix.lower() != ".csv":
        errors.append("File is neither a zip nor a .csv")

    sha = _sha256(path)
    result["sha256"] = sha
    if (batch := ingested_hashes().get(sha)):
        errors.append(f"This exact file was already ingested as batch {batch}")

    header, rows, encoding = [], 0, None
    if not errors:
        encoding = _detect_encoding(path, member)
        result["encoding"] = encoding
        reader = _iter_rows(path, member, encoding)
        header = [h.strip() for h in next(reader, [])]
        result["columns"] = header
        if len(header) < 2:
            errors.append(f"Header doesn't look ';'-separated: {header[:3]}")
        normed = {_norm(h): i for i, h in enumerate(header)}

        found = {}
        for logical, aliases in {**REQUIRED_COLUMNS, **OPTIONAL_COLUMNS}.items():
            idx = next((normed[a] for a in aliases if a in normed), None)
            if idx is not None:
                found[logical] = idx
            elif logical in REQUIRED_COLUMNS:
                errors.append(f"Required column missing: {logical} (accepted: {sorted(aliases)})")
        result["recognised_columns"] = {k: header[v] for k, v in found.items()}

        if not errors:
            malformed, checks = 0, {"active_not_J_N": 0, "legal_form_not_BV_NV": 0,
                                    "postcode_region_not_2_digits": 0, "start_date_unparsed": 0}
            for row in reader:
                rows += 1
                if len(row) != len(header):
                    malformed += 1
                    continue
                if row[found["active"]].strip().upper() not in {"J", "N"}:
                    checks["active_not_J_N"] += 1
                if row[found["legal_form"]].strip().upper() not in {"BV", "NV"}:
                    checks["legal_form_not_BV_NV"] += 1
                if not re.fullmatch(r"\d{2}", row[found["postcode_region"]].strip()):
                    checks["postcode_region_not_2_digits"] += 1
                d = row[found["start_date"]].strip()
                if d and not re.fullmatch(r"\d{8}|\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}", d):
                    checks["start_date_unparsed"] += 1
            result["row_count"] = rows
            result["malformed_rows"] = malformed
            if rows == 0:
                errors.append("CSV has a header but no data rows")
            elif malformed / rows > MAX_MALFORMED_SHARE:
                errors.append(f"{malformed} of {rows} rows have the wrong number of fields")
            for name, n in checks.items():
                if n:
                    warnings.append(f"{name}: {n} rows ({n / max(rows, 1):.2%})")

    passed = not errors
    report = {**result, "status": "PASSED" if passed else "FAILED",
              "errors": errors, "warnings": warnings}
    VERIFIED[str(path)] = Verification(str(path), sha, passed, member, encoding,
                                       header, rows, report)
    return json.dumps(report, ensure_ascii=False)


def _confirm(v: Verification, batch_id: str) -> bool:
    """Human gate: reads from the real keyboard, never from the model."""
    print("\n" + "=" * 70)
    print(f"  Write {v.row_count:,} rows from {Path(v.path).name}")
    print(f"  to {DATA_TABLE} as batch {batch_id}?")
    print("=" * 70)
    try:
        return input("  Type YES to confirm: ").strip() == "YES"
    except EOFError:
        return False


def ingest_verified_dataset(file_path: str) -> str:
    """
    Ingest a verified KVK dataset file into the KVK bronze Delta Lake.
    Only works after verify_dataset PASSED for this exact file; the user must then
    confirm at the keyboard.

    Args:
        file_path (str): The same file path that was verified.

    Returns:
        str: JSON with the result: batch_id and row count, or why nothing was written.
    """
    path = _resolve(file_path)
    v = VERIFIED.get(str(path))
    if v is None:
        return json.dumps({"status": "REFUSED", "reason": "File was not verified. Run verify_dataset first."})
    if not v.passed:
        return json.dumps({"status": "REFUSED", "reason": "Verification FAILED for this file.",
                           "errors": v.report["errors"]})
    if _sha256(path) != v.sha256:
        VERIFIED.pop(str(path))
        return json.dumps({"status": "REFUSED", "reason": "File changed since verification. Verify again."})
    if (batch := ingested_hashes().get(v.sha256)):
        return json.dumps({"status": "REFUSED", "reason": f"Already ingested as batch {batch}"})

    now = datetime.now(timezone.utc)
    batch_id = f"kvk_{now:%Y%m%dT%H%M%SZ}_{v.sha256[:8]}"
    if not _confirm(v, batch_id):
        return json.dumps({"status": "CANCELLED", "reason": "User did not type YES. Nothing written."})

    rows = write_batch(v, batch_id, now)
    write_log(v, batch_id, now, rows)
    VERIFIED.pop(str(path))
    return json.dumps({"status": "INGESTED", "batch_id": batch_id, "rows": rows,
                       "ingested_at": now.isoformat(), "table": str(DATA_TABLE)})


def list_batches() -> str:
    """
    List the batches already ingested into the KVK bronze layer.

    Returns:
        str: JSON list of batches with batch_id, ingested_at, file and row count.
    """
    if not (LOG_TABLE / "_delta_log").exists():
        return json.dumps({"batches": []})
    from deltalake import DeltaTable
    rows = DeltaTable(str(LOG_TABLE)).to_pyarrow_table(
        columns=["batch_id", "ingested_at", "source_file", "row_count", "status"]).to_pylist()
    for r in rows:
        r["ingested_at"] = r["ingested_at"].isoformat()
    return json.dumps({"batches": sorted(rows, key=lambda r: r["ingested_at"])}, default=str)


# --------------------------------------------------------------------------- delta writes
def _clean_name(name: str, seen: set[str]) -> str:
    base = re.sub(r"[^0-9A-Za-z_]", "_", name.strip()) or "column"
    out, i = base, 2
    while out in seen:
        out, i = f"{base}_{i}", i + 1
    seen.add(out)
    return out


def remove_orphan_rows() -> None:
    """Delete rows of a batch that was written but never logged (crash in between)."""
    if not (DATA_TABLE / "_delta_log").exists():
        return
    from deltalake import DeltaTable
    good = set(ingested_hashes().values())
    dt = DeltaTable(str(DATA_TABLE))
    present = set(dt.to_pyarrow_table(columns=["_batch_id"]).column("_batch_id").unique().to_pylist())
    for orphan in present - good:
        dt.delete(f"_batch_id = '{orphan}'")
        print(f"Removed rows of unlogged batch {orphan}")


def write_batch(v: Verification, batch_id: str, now: datetime) -> int:
    """Stream the CSV into the bronze table as one Delta commit. Returns rows written."""
    import pyarrow as pa
    from deltalake import write_deltalake

    remove_orphan_rows()
    seen: set[str] = set()
    cols = [_clean_name(h, seen) for h in v.header]
    meta = ["_batch_id", "_ingested_at", "_source_file", "_source_sha256", "_row_number"]
    schema = pa.schema([pa.field(c, pa.string()) for c in cols] + [
        pa.field("_batch_id", pa.string()), pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("_source_file", pa.string()), pa.field("_source_sha256", pa.string()),
        pa.field("_row_number", pa.int64())])
    written = 0

    def batches():
        nonlocal written
        reader = _iter_rows(Path(v.path), v.csv_member, v.encoding)
        next(reader)  # header
        buf: list[list[str]] = []
        for row in reader:
            if len(row) != len(cols):
                continue  # malformed rows were counted (<= 0.1%) during verification
            buf.append(row)
            if len(buf) >= BATCH_ROWS:
                yield _to_batch(buf)
                buf = []
        if buf:
            yield _to_batch(buf)

    def _to_batch(buf):
        nonlocal written
        n = len(buf)
        arrays = [pa.array([r[i] for r in buf], pa.string()) for i in range(len(cols))]
        arrays += [pa.array([batch_id] * n, pa.string()),
                   pa.array([now] * n, pa.timestamp("us", tz="UTC")),
                   pa.array([Path(v.path).name] * n, pa.string()),
                   pa.array([v.sha256] * n, pa.string()),
                   pa.array(range(written + 1, written + n + 1), pa.int64())]
        written += n
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    import deltalake
    reader = pa.RecordBatchReader.from_batches(schema, batches())
    # One call = one Delta commit: a batch is either fully in bronze or not at all.
    # schema_mode="merge" lets KVK add columns later without breaking older batches
    # (deltalake < 1.0 only supports that with the rust engine).
    kwargs = {"schema_mode": "merge"}
    if int(deltalake.__version__.split(".")[0]) < 1:
        kwargs["engine"] = "rust"
    write_deltalake(str(DATA_TABLE), reader, mode="append", **kwargs)
    return written


def write_log(v: Verification, batch_id: str, now: datetime, rows: int) -> None:
    import pyarrow as pa
    from deltalake import write_deltalake

    table = pa.table({
        "batch_id": [batch_id], "ingested_at": [now], "source_file": [Path(v.path).name],
        "file_sha256": [v.sha256], "csv_member": [v.csv_member], "row_count": [rows],
        "column_count": [len(v.header)], "verified_at": [v.verified_at],
        "verification_report": [json.dumps(v.report, ensure_ascii=False)],
        "confirmed_by": [getpass.getuser()], "status": ["success"],
    }, schema=pa.schema([
        ("batch_id", pa.string()), ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("source_file", pa.string()), ("file_sha256", pa.string()), ("csv_member", pa.string()),
        ("row_count", pa.int64()), ("column_count", pa.int64()),
        ("verified_at", pa.timestamp("us", tz="UTC")), ("verification_report", pa.string()),
        ("confirmed_by", pa.string()), ("status", pa.string())]))
    write_deltalake(str(LOG_TABLE), table, mode="append")


# --------------------------------------------------------------------------- agent
TOOLS = {f.__name__: f for f in (get_dataset_info, verify_dataset,
                                 ingest_verified_dataset, list_batches)}

SYSTEM_PROMPT = f"""You are the KVK intake agent. Your job is to get the KVK Handelsregister
Open Dataset Basis Bedrijfsgegevens into the KVK bronze Delta Lake.

Steps:
1. Call get_dataset_info and tell the user, briefly, to download the zip from
   {DOWNLOAD_URL} and then give you the file path (they can drag the file into the terminal).
2. When the user gives a path, call verify_dataset with it. Summarise the report:
   status, row count, recognised columns, errors and warnings.
3. If the status is PASSED, ask the user whether to ingest it. Only if they agree,
   call ingest_verified_dataset with the same path. The tool asks the user to type YES itself.
4. If the status is FAILED, explain the errors and ask for a different file. Never ingest it.
5. Report the batch_id and row count from the tool result.

Rules: never claim data was verified or ingested unless a tool result says so. Never
invent file paths or batch ids. Keep replies short."""


def run_agent(model: str) -> None:
    try:
        from ollama import chat
    except ImportError:
        sys.exit("The 'ollama' package is missing: pip install ollama")

    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": "Hi, I want to load the KVK open dataset."}]
    print(f"KVK intake agent ({model}). Type 'exit' to stop.\n")
    while True:
        try:
            response = chat(model=model, messages=messages, tools=list(TOOLS.values()),
                            options={"temperature": 0})
        except Exception as e:
            sys.exit(f"Could not reach Ollama ({e}). Is it running, and is '{model}' pulled?")
        msg = response.message
        messages.append(msg)

        if msg.tool_calls:
            for call in msg.tool_calls:
                fn = TOOLS.get(call.function.name)
                try:
                    output = fn(**(call.function.arguments or {})) if fn else \
                        json.dumps({"error": f"Unknown tool {call.function.name}"})
                except Exception as e:  # bad arguments from the model, IO errors, ...
                    output = json.dumps({"error": f"{type(e).__name__}: {e}"})
                parsed = json.loads(output)
                status = parsed.get("status") or ("ERROR" if "error" in parsed else "ok")
                print(f"  [tool] {call.function.name} -> {status}")
                messages.append({"role": "tool", "content": output, "tool_name": call.function.name})
            continue  # let the model react to the tool results

        if msg.content:
            print(f"\nAgent: {msg.content}\n")
        try:
            user = input("You: ").strip()
        except EOFError:
            break
        if user.lower() in {"exit", "quit"}:
            break
        messages.append({"role": "user", "content": user})


def run_direct(file_path: str) -> None:
    """Same verification and gate, without an LLM."""
    report = json.loads(verify_dataset(file_path))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if report["status"] != "PASSED":
        sys.exit(1)
    print(json.dumps(json.loads(ingest_verified_dataset(file_path)), indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Get the KVK open dataset into the KVK bronze layer.")
    ap.add_argument("--model", default="qwen2.5:7b", help="Ollama model with tool support")
    ap.add_argument("--file", help="skip the agent: verify and ingest this file directly")
    args = ap.parse_args()
    run_direct(args.file) if args.file else run_agent(args.model)
