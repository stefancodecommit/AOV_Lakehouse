"""
Demo: start everything the live demonstration needs with one command, and open a start
page with a button per demo step (docs/demo.md describes the steps and what to say).

Starts, each in the background with its output in build/demo_logs/:
  the dashboard          http://localhost:8501
  the lakehouse viewer   http://localhost:8502
  Dagster (dagster dev)  http://localhost:3000
waits until all three answer, then opens the start page (build/demo_start.html, made
fresh each time) in the default browser. Open that file in a browser, not in an editor:
VS Code shows it as HTML source. Ctrl+C stops all three. A service that is already
running on its port is used as it is and left running afterwards.

The start page opens each step in a new tab: the risk index, sector Q explained (the
dashboard reads ?group=Q), the lineage of gold_risk_index.relative_risk, Dagster's asset
graph and checks, and the KVK table's data quality in the viewer (it reads ?layer=,
?table= and ?view=). A dot per service shows whether it answers. Everything runs on this
machine; no internet needed.

Usage
-----
    .venv/bin/python -m tools.demo                # start, open the page, Ctrl+C to stop
    .venv/bin/python -m tools.demo --no-browser   # start only; open build/demo_start.html yourself
"""
from __future__ import annotations

import argparse
import html
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import webbrowser
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "build"
LOGS = OUT / "demo_logs"
START_PAGE = OUT / "demo_start.html"
LINEAGE = ROOT / "docs" / "lineage_explorer.html"
STARTUP_SECONDS = 120          # Dagster needs the longest: it loads the definitions


@dataclass(frozen=True)
class Service:
    name: str
    port: int
    health: str                # path that answers 200 once the service is ready
    command: list[str]
    env: dict[str, str] | None = None

    @property
    def url(self) -> str:
        return f"http://localhost:{self.port}"


SERVICES = [
    Service("dashboard", 8501, "/_stcore/health",
            [sys.executable, "-m", "streamlit", "run", "dashboard/app.py",
             "--server.headless", "true"]),
    Service("lakehouse viewer", 8502, "/_stcore/health",
            [sys.executable, "-m", "tools.lakehouse_viewer"]),
    Service("Dagster", 3000, "/server_info",
            [sys.executable, "-m", "dagster", "dev", "-f", "orchestration/definitions.py",
             "-h", "127.0.0.1", "-p", "3000"],
            env={"DAGSTER_HOME": str(ROOT / "orchestration")}),
]
DASHBOARD, VIEWER, DAGSTER = (s.url for s in SERVICES)
KVK_TABLE = "bronze/kvk/basis_bedrijfsgegevens"


def steps() -> list[dict]:
    """The five demo steps of docs/demo.md: a title, what it shows, and its buttons."""
    lineage = LINEAGE.as_uri() + "#gold:gold_risk_index.relative_risk"
    quality = f"{VIEWER}/?layer=bronze&table={KVK_TABLE}&view=Data%20quality"
    return [
        {"title": "De risico-index", "what": "per sector het relatieve risico, de band en de "
         "trend; zet daarna “Final figures only” aan",
         "links": [("Risk index", f"{DASHBOARD}/risk")]},
        {"title": "Eén sector uitgelegd", "what": "de zorg (sectie Q): de uitleg in één zin "
         "en het verloop per kwartaal",
         "links": [("Sector Q", f"{DASHBOARD}/risk?group=Q"),
                   ("Behind the numbers", f"{DASHBOARD}/profile")]},
        {"title": "Waar komt een getal vandaan", "what": "de herkomst van het relatieve "
         "risico, terug tot het CBS-veld in bronze",
         "links": [("Lineage explorer", lineage)]},
        {"title": "De workflow", "what": "de stappen van bronze tot gold, de controles "
         "ertussen en de maandelijkse planning",
         "links": [("Asset graph", f"{DAGSTER}/asset-groups"),
                   ("Checks silver_cbs", f"{DAGSTER}/assets/silver_cbs?view=checks"),
                   ("Schedules", f"{DAGSTER}/automation")]},
        {"title": "Datakwaliteit", "what": "het KVK-bestand in bronze: 381.513 identieke "
         "rijen, opvulspaties; daarna silver",
         "links": [("KVK in bronze", quality),
                   ("Silver kvk_company",
                    f"{VIEWER}/?layer=silver&table=silver/kvk_company&view=Data%20quality")]},
    ]


def start_page(path: Path = START_PAGE) -> Path:
    """Write the start page: a numbered card per step with its buttons, and a dot per
    service that turns green when it answers. Returns its path."""
    cards = []
    for i, step in enumerate(steps(), 1):
        buttons = "".join(f'<a class="btn" href="{html.escape(url)}" target="_blank">'
                          f"{html.escape(label)}</a>" for label, url in step["links"])
        cards.append(f'<section><div class="n">{i}</div><div><h2>{html.escape(step["title"])}'
                     f'</h2><p>{html.escape(step["what"])}</p><div>{buttons}</div></div></section>')
    dots = "".join(f'<span class="svc" data-url="{s.url}{s.health}"><i></i>{s.name}</span>'
                   for s in SERVICES)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"""<!doctype html>
<html lang="nl"><head><meta charset="utf-8"><title>Demo</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root {{ --dark:#0B3B30; --green:#0C8066; --tint:#E8F2EE; --ink:#1A1A1A; --grey:#9AA5A0; }}
body {{ margin:0; font-family:Calibri,'Segoe UI',Arial,sans-serif; color:var(--ink);
       background:#fff; }}
header {{ background:var(--dark); color:#fff; padding:24px 40px; }}
header h1 {{ margin:0; font-size:30px; }}
header p {{ margin:6px 0 0; color:#cfe3dc; }}
.svcs {{ margin-top:12px; }}
.svc {{ margin-right:18px; font-size:15px; }}
.svc i {{ display:inline-block; width:10px; height:10px; border-radius:50%;
         background:var(--grey); margin-right:6px; }}
.svc.up i {{ background:#3ddc97; }} .svc.down i {{ background:#e05d4f; }}
main {{ padding:24px 40px; max-width:1000px; }}
section {{ display:flex; gap:20px; align-items:flex-start; background:var(--tint);
          border-radius:12px; padding:16px 20px; margin-bottom:14px; }}
.n {{ flex:none; width:46px; height:46px; border-radius:50%; background:var(--dark);
      color:#fff; font-size:24px; font-weight:bold; display:flex; align-items:center;
      justify-content:center; }}
h2 {{ margin:2px 0 4px; font-size:22px; color:var(--dark); }}
p {{ margin:0 0 10px; }}
.btn {{ display:inline-block; background:var(--green); color:#fff; text-decoration:none;
        padding:8px 16px; border-radius:8px; margin:0 8px 4px 0; font-size:16px; }}
.btn:hover {{ background:var(--dark); }}
</style></head>
<body><header><h1>Demo: verzuimrisico per sector en bedrijfsgrootte</h1>
<p>Elke knop opent een nieuw tabblad; deze pagina blijft staan.</p>
<div class="svcs">{dots}</div></header>
<main>{"".join(cards)}</main>
<script>
// green when the service answers (a no-cors request succeeds), red when it doesn't
function check() {{
  document.querySelectorAll(".svc").forEach(el => {{
    fetch(el.dataset.url, {{mode: "no-cors", cache: "no-store"}})
      .then(() => {{ el.className = "svc up"; }})
      .catch(() => {{ el.className = "svc down"; }});
  }});
}}
check(); setInterval(check, 5000);
</script></body></html>
""", encoding="utf-8")
    return path


def port_in_use(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def answers(service: Service) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{service.port}{service.health}",
                                    timeout=2) as r:
            return r.status == 200
    except OSError:
        return False


def start(service: Service) -> subprocess.Popen:
    """Start one service in its own process group, its output in build/demo_logs/."""
    LOGS.mkdir(parents=True, exist_ok=True)
    log = open(LOGS / f"{service.name.replace(' ', '_')}.log", "w")
    return subprocess.Popen(service.command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                            env={**os.environ, **(service.env or {})}, start_new_session=True)


def stop(processes: dict[str, subprocess.Popen]) -> None:
    """Stop the services this script started (the whole process group: Dagster has child
    processes), first politely, then firmly."""
    for proc in processes.values():
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
    deadline = time.time() + 15
    for proc in processes.values():
        try:
            proc.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)


def open_in_browser(path: Path) -> None:
    """Open a local page in the default browser. Not through $BROWSER first (Python's
    webbrowser does that): in an editor's terminal it can point at the editor, which shows
    an HTML file as source code."""
    opener = {"linux": "xdg-open", "darwin": "open"}.get(sys.platform)
    if opener and shutil.which(opener):
        subprocess.Popen([opener, str(path)], stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True)
    else:
        webbrowser.open(path.as_uri())


def _interrupt(*_) -> None:
    raise KeyboardInterrupt


def main() -> int:
    ap = argparse.ArgumentParser(description="Start the demo and open its start page.")
    ap.add_argument("--no-browser", action="store_true", help="don't open the start page")
    args = ap.parse_args()
    sys.stdout.reconfigure(line_buffering=True)      # progress shows at once, also in a log
    # stopped by a signal instead of Ctrl+C (kill, a closed terminal): clean up the same way
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, _interrupt)
    if not (ROOT / "lakehouse" / "gold").exists():
        print("No gold tables yet: run the pipeline first (INSTRUCTIONS.md, section 2).")
        return 1

    processes: dict[str, subprocess.Popen] = {}
    try:
        for s in SERVICES:
            if port_in_use(s.port):
                print(f"  {s.name:17s} already running on {s.url}: used as it is")
            else:
                processes[s.name] = start(s)
        deadline = time.time() + STARTUP_SECONDS
        waiting = list(SERVICES)
        while waiting and time.time() < deadline:
            for s in list(waiting):
                if answers(s):
                    print(f"  {s.name:17s} ready on {s.url}")
                    waiting.remove(s)
                elif s.name in processes and processes[s.name].poll() is not None:
                    print(f"  {s.name:17s} stopped while starting; see {LOGS}")
                    waiting.remove(s)
            time.sleep(1)
        for s in waiting:
            print(f"  {s.name:17s} not ready after {STARTUP_SECONDS} s; see {LOGS}")

        page = start_page()
        if not args.no_browser:
            open_in_browser(page)
        # a printed path is a link in VS Code's terminal that opens the file in the editor
        # (HTML source); this says how to open it in a browser instead
        print(f"\nStart page, to open in a browser: {page.as_uri()}\n"
              f"  (if it didn't open: xdg-open {page.relative_to(ROOT)}; macOS: open ...)")
        print("Ctrl+C stops everything this command started.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping…")
    finally:
        stop(processes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
