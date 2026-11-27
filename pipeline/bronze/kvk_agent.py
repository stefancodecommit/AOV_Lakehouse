"""
KVK intake agent: a local Ollama model guides you through getting the
"KVK Handelsregister Open Dataset Basis Bedrijfsgegevens" into the KVK bronze Delta Lake.

The agent only talks and chooses which tool to call. Every tool is a thin wrapper around
pipeline/bronze/kvk.py, which does the checking, asks for the confirmation and writes;
it returns JSON for the model. This module holds no Delta code and no checks.

Flow
----
1. The agent looks in the inbox (inbox/kvk/, the folder Docker shares with your
   computer) for a file that isn't ingested yet. If there is none, it tells you where to
   download the zip: save it in the inbox, or type or drag its path into the terminal.
2. verify_dataset() checks the file (kvk.verify, with checks/kvk.py). The checks run in
   Python, not in the model.
3. Only if verification PASSED for that exact file (same hash) can
   ingest_verified_dataset() run, and it still asks you to type YES at the keyboard
   (kvk.confirm_at_keyboard). The model cannot type that for you.
4. The rows go to bronze in ONE Delta commit with a new batch_id (see kvk.py).

The model gets only the functions in TOOLS. Dagster's entry point
(kvk.ingest_confirmed_file, which skips the keyboard) is not one of them.

Usage
-----
    ollama pull qwen2.5:7b               # any model that supports tool calling
    python -m pipeline.bronze.kvk_agent  # chat with the agent
    python -m pipeline.bronze.kvk_agent --model llama3.1
Without an LLM: python -m pipeline.bronze.kvk --inbox (or --file <path>).
"""
from __future__ import annotations

import argparse
import getpass
import json
import sys

from pipeline.bronze import kvk
from pipeline.common.config import settings

OLLAMA_HOST = settings["ollama"]["host"]
OLLAMA_MODEL = settings["ollama"]["model"]


# --------------------------------------------------------------------------- tools
# The docstrings below are what the model reads (Ollama turns them into tool
# descriptions), so they are written for the model.
def get_dataset_info() -> str:
    """
    Get where and how to download the KVK Handelsregister Open Dataset Basis Bedrijfsgegevens,
    and what the user must do next.

    Returns:
        str: JSON with the dataset page, download link, licence and instructions.
    """
    return json.dumps({
        "dataset": kvk.DATASET,
        "page": kvk.DATASET_PAGE,
        "download_url": kvk.DOWNLOAD_URL,
        "format": "zip containing one semicolon-separated CSV (too large for Excel)",
        "refreshed": "every working day",
        "license": kvk.LICENSE,
        "inbox": str(kvk.inbox_dir()),
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
    files = kvk.inbox_files()
    new = [f["file"] for f in files if not f["already_ingested_as"]]
    if new:
        next_step = (f"Tell the user that {new[0]} is new and offer to verify it "
                     f"(call verify_dataset with '{new[0]}').")
    else:
        next_step = ("There is no new file (every file here is already ingested). Call "
                     "get_dataset_info and ask the user to download a new zip into the inbox.")
    return json.dumps({"inbox": str(kvk.inbox_dir()), "files": files, "new_files": new,
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
    return json.dumps(kvk.verify(file_path), ensure_ascii=False)


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
    return json.dumps(kvk.ingest(kvk.resolve_path(file_path), kvk.confirm_at_keyboard,
                                 getpass.getuser()))


def list_batches() -> str:
    """
    List the batches already ingested into the KVK bronze layer.

    Returns:
        str: JSON list of batches with batch_id, ingested_at, file and row count.
    """
    return json.dumps({"batches": kvk.list_batches()}, default=str)


# --------------------------------------------------------------------------- agent
TOOLS = {f.__name__: f for f in (get_dataset_info, list_inbox, verify_dataset,
                                 ingest_verified_dataset, list_batches)}

SYSTEM_PROMPT = f"""You are the KVK intake agent. Your job is to get the KVK Handelsregister
Open Dataset Basis Bedrijfsgegevens into the KVK bronze Delta Lake.

Steps:
1. Call list_inbox. If it has a file that is not already ingested, tell the user and
   offer to verify the newest one. Otherwise call get_dataset_info and tell the user,
   briefly, to download the zip from {kvk.DOWNLOAD_URL} and save it in the inbox folder
   ({kvk.inbox_dir()}), or give you the file path (they can drag the file into the terminal).
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
    happen in pipeline/bronze/kvk.py. The final YES is read from the keyboard by
    kvk.confirm_at_keyboard, so the model can never approve a write on its own."""
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


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Chat with the KVK intake agent (without an LLM: python -m pipeline.bronze.kvk).")
    ap.add_argument("--model", default=OLLAMA_MODEL,
                    help="Ollama model with tool support (default: ollama.model in config.yaml)")
    run_agent(ap.parse_args().model)
