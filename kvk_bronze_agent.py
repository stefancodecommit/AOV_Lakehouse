"""
KVK intake agent: a local Ollama model guides you through getting the
"KVK Handelsregister Open Dataset Basis Bedrijfsgegevens" into the KVK bronze Delta Lake.

Flow
----
1. The agent looks in the inbox (inbox/kvk/, the folder Docker shares with your
   computer) for a file that isn't ingested yet. If there is none, it tells you where to
   download the zip: save it in the inbox, or type or drag its path into the terminal.
2. verify_dataset() checks the file with checks/kvk.py. The checks run in Python,
   not in the model:
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
    python kvk_bronze_agent.py --inbox         # no LLM: the newest new file in inbox/kvk/
"""
from __future__ import annotations

import argparse
import getpass
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from checks.kvk import check_file, iter_rows as _iter_rows, sha256 as _sha256
from config import project_path, settings

# --------------------------------------------------------------------------- config
_CFG = settings["kvk"]
KVK_ROOT = project_path(settings["paths"]["lakehouse"]) / "bronze" / "kvk"
DATA_TABLE = KVK_ROOT / "basis_bedrijfsgegevens"
LOG_TABLE = KVK_ROOT / "_batch_log"
QUARANTINE_ROOT = project_path(settings["paths"]["lakehouse"]) / "quarantine"
INBOX = project_path(settings["paths"]["inbox"]) / "kvk"     # uploads land here (Docker volume)

DATASET_PAGE = _CFG["dataset_page"]
DOWNLOAD_URL = _CFG["download_url"]
LICENSE = "CC BY 4.0 (source: KVK)"

MAX_MALFORMED_SHARE = float(_CFG["max_malformed_share"])  # reject above this share
BATCH_ROWS = 200_000               # rows per Arrow batch while writing (memory, not commits)

# Logical column -> accepted header spellings (see config.yaml), compared after
# lower-casing and removing everything that isn't a letter or digit.
REQUIRED_COLUMNS = {k: set(v) for k, v in _CFG["required_columns"].items()}
OPTIONAL_COLUMNS = {k: set(v) for k, v in _CFG["optional_columns"].items()}
OLLAMA_HOST = settings["ollama"]["host"]
OLLAMA_MODEL = settings["ollama"]["model"]


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
    """A file path as typed or dragged into the terminal -> an absolute Path.
    A bare file name that doesn't exist here is looked up in the inbox."""
    cleaned = file_path.strip().strip("'\"").replace("\\ ", " ")
    path = Path(cleaned).expanduser()
    if not path.exists() and path.name == cleaned and (INBOX / cleaned).exists():
        path = INBOX / cleaned                       # a bare file name: look in the inbox
    return path.resolve()


def inbox_files() -> list[dict]:
    """Zip and CSV files in the inbox, newest first, marked when already ingested."""
    if not INBOX.is_dir():
        return []
    done = ingested_hashes()
    files = sorted((f for f in INBOX.iterdir()
                    if f.is_file() and f.suffix.lower() in {".zip", ".csv"}),
                   key=lambda f: f.stat().st_mtime, reverse=True)
    out = []
    for f in files:
        batch = done.get(_sha256(f))
        out.append({"file": f.name, "size_mb": round(f.stat().st_size / 1e6, 1),
                    "modified": datetime.fromtimestamp(f.stat().st_mtime, timezone.utc).isoformat(),
                    "already_ingested_as": batch})
    return out


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
        "inbox": str(INBOX),
        "next_step": "Ask the user to download the zip and save it in the inbox folder, "
                     "or give its file path (they can drag the file into the terminal).",
    })


def list_inbox() -> str:
    """
    List the KVK files (zip or csv) waiting in the inbox folder, newest first.

    Returns:
        str: JSON with the inbox folder and its files: name, size, modification time and,
             when this exact file is already in bronze, the batch_id it was ingested as.
    """
    files = inbox_files()
    new = [f["file"] for f in files if not f["already_ingested_as"]]
    if new:
        next_step = (f"Tell the user that {new[0]} is new and offer to verify it "
                     f"(call verify_dataset with '{new[0]}').")
    else:
        next_step = ("There is no new file (every file here is already ingested). Call "
                     "get_dataset_info and ask the user to download a new zip into the inbox.")
    return json.dumps({"inbox": str(INBOX), "files": files, "new_files": new,
                       "next_step": next_step})


def verify_dataset(file_path: str) -> str:
    """
    Verify an uploaded KVK dataset file (zip or csv) before it may be ingested.
    Must be called before ingest_verified_dataset.

    Args:
        file_path (str): Path to the downloaded zip or csv file on this computer.

    Returns:
        str: JSON verification report with status PASSED or FAILED and the reasons.
    """
    path = _resolve(file_path)
    check = check_file(path, REQUIRED_COLUMNS, OPTIONAL_COLUMNS, MAX_MALFORMED_SHARE,
                       ingested=ingested_hashes())
    report = check.report()
    if not check.passed and path.is_file() and "already_ingested" not in check.details:
        # a rejected file is kept with its report; a file that is already in bronze isn't
        from checks.quarantine import quarantine
        folder = quarantine(QUARANTINE_ROOT, "kvk", path.name, report, file=path)
        report["quarantine"] = str(folder) if folder else "already in quarantine"
    VERIFIED[str(path)] = Verification(str(path), check.sha256, check.passed, check.csv_member,
                                       check.encoding, check.header, check.row_count, report)
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
    """CSV header -> a valid, unique Delta column name: 'Datum aanvang' -> 'Datum_aanvang'."""
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
        """Yield the CSV rows as Arrow batches of BATCH_ROWS rows (keeps memory flat)."""
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
        """Rows of text -> one Arrow record batch with the lineage columns added."""
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
    """Record a written batch in bronze/kvk/_batch_log, with its hash and verification
    report. Written after the data, so an unlogged batch is recognisable as unfinished
    (remove_orphan_rows deletes it before the next write)."""
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
TOOLS = {f.__name__: f for f in (get_dataset_info, list_inbox, verify_dataset,
                                 ingest_verified_dataset, list_batches)}

SYSTEM_PROMPT = f"""You are the KVK intake agent. Your job is to get the KVK Handelsregister
Open Dataset Basis Bedrijfsgegevens into the KVK bronze Delta Lake.

Steps:
1. Call list_inbox. If it has a file that is not already ingested, tell the user and
   offer to verify the newest one. Otherwise call get_dataset_info and tell the user,
   briefly, to download the zip from {DOWNLOAD_URL} and save it in the inbox folder
   ({INBOX}), or give you the file path (they can drag the file into the terminal).
2. When the user gives a path or file name, or agrees to an inbox file, call
   verify_dataset with it (an inbox file name alone is enough). Summarise the report:
   status, row count, recognised columns, errors and warnings.
3. If the status is PASSED, ask the user whether to ingest it. Only if they agree,
   call ingest_verified_dataset with the same path. The tool asks the user to type YES itself.
4. If the status is FAILED, explain the errors and ask for a different file. Never ingest it.
5. Report the batch_id and row count from the tool result.

Rules: never claim data was verified or ingested unless a tool result says so. Never
invent file paths or batch ids. Keep replies short."""


def run_agent(model: str) -> None:
    """Chat loop: the model talks to the user and calls the tools (TOOLS).

    The model only decides *which* tool to call; checking, confirming and writing all
    happen in the Python tools. The final YES is read from the keyboard by _confirm(),
    so the model can never approve a write on its own."""
    try:
        from ollama import Client
    except ImportError:
        sys.exit("The 'ollama' package is missing: pip install ollama")
    client = Client(host=OLLAMA_HOST)

    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": "Hi, I want to load the KVK open dataset."}]
    print(f"KVK intake agent ({model}). Type 'exit' to stop.\n")
    while True:
        try:
            response = client.chat(model=model, messages=messages, tools=list(TOOLS.values()),
                                   options={"temperature": 0})
        except Exception as e:
            sys.exit(f"Could not reach Ollama at {OLLAMA_HOST} ({e}). "
                     f"Is it running, and is '{model}' pulled?")
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


def newest_new_inbox_file() -> str | None:
    """Name of the newest file in the inbox that isn't ingested yet, or None."""
    return next((f["file"] for f in inbox_files() if not f["already_ingested_as"]), None)


def run_direct(file_path: str) -> None:
    """Same verification and gate, without an LLM."""
    report = json.loads(verify_dataset(file_path))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if report["status"] != "PASSED":
        sys.exit(1)
    print(json.dumps(json.loads(ingest_verified_dataset(file_path)), indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Get the KVK open dataset into the KVK bronze layer.")
    ap.add_argument("--model", default=OLLAMA_MODEL,
                    help="Ollama model with tool support (default: ollama.model in config.yaml)")
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--file", help="skip the agent: verify and ingest this file directly "
                                    "(a bare file name is looked up in the inbox)")
    src.add_argument("--inbox", action="store_true",
                     help="skip the agent: verify and ingest the newest new file in the inbox")
    args = ap.parse_args()
    if args.inbox:
        newest = newest_new_inbox_file()
        if newest is None:
            sys.exit(f"No new KVK file in {INBOX}")
        run_direct(newest)
    elif args.file:
        run_direct(args.file)
    else:
        run_agent(args.model)
