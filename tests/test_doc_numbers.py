"""Numbers that the documents state match the project: the number of tests, and the
number of columns with a documented lineage.

The report, the Q&A, the GenAI section, the requirements and tests/README.md say how
many tests there are; they are read on their own (the report is submitted as a PDF), so
they keep the number instead of linking to it. This test makes sure no copy goes stale:
when the count changes, it fails and names every file to update.

Not checked here: TODO.md (a dated log, it keeps the counts of earlier days on purpose),
and the slides (docs/presentatie.pptx), which are on the "Before submitting" list in
TODO.md.
"""
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# How the documents write the total: "380 tests", "**380 geautomatiseerde tests**",
# "**Automated tests** (380)", and tests/README.md's "all pass (380 at the time ...".
# (Counts of a part of the tests, such as silver's README, are written differently.)
PATTERNS = [re.compile(r"\b(\d{3}) (?:geautomatiseerde )?tests\b"),
            re.compile(r"tests\*\* \((\d{3})\)"),
            re.compile(r"all pass \((\d{3}) at the time")]


def documents() -> list[Path]:
    """The project's own Markdown files, except TODO.md and the generated lineage."""
    files = [*ROOT.glob("*.md"), *(ROOT / "docs").glob("*.md"), *ROOT.glob("*/README.md"),
             *ROOT.glob("*/*/README.md")]
    return sorted(f for f in files if f.name != "TODO.md" and f.name != "lineage.md"
                  and ".venv" not in f.parts)


def real_test_count() -> int:
    """The number of tests pytest runs by default (the live CBS test deselected)."""
    out = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-q",
                          "-p", "no:cacheprovider", "-o", "addopts=", "-m", "not live"],
                         cwd=ROOT, capture_output=True, text=True, timeout=120).stdout
    return int(re.search(r"(\d+)(?:/\d+)? tests? collected", out)[1])


def stated_counts() -> dict[str, list[int]]:
    """Every total test count written in the documents, per file."""
    found = {}
    for f in documents():
        text = f.read_text(encoding="utf-8")
        numbers = [int(m) for p in PATTERNS for m in p.findall(text)]
        if numbers:
            found[str(f.relative_to(ROOT))] = numbers
    return found


def test_the_documents_state_the_real_number_of_tests():
    real = real_test_count()
    stated = stated_counts()
    assert stated, "no test count found in the documents: did the wording change?"
    wrong = {f: n for f, n in stated.items() if any(x != real for x in n)}
    assert not wrong, (f"there are {real} tests, but these documents say otherwise: {wrong}"
                       f" (and check the slides, docs/presentatie.pptx)")


def test_the_report_states_the_real_number_of_lineage_columns():
    """docs/rapport.md: "Voor elk van de N kolommen in silver en gold" (tools/lineage.py)."""
    from tools import lineage
    real = sum(len(columns) for columns in lineage.LINEAGE.values())
    text = (ROOT / "docs" / "rapport.md").read_text(encoding="utf-8")
    stated = [int(n) for n in re.findall(r"(\d+) kolommen in silver en gold", text)]
    assert stated == [real], f"the report says {stated} columns, the lineage has {real}"
