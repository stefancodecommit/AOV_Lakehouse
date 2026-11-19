"""The data model diagrams (ERD) in README.md match the project.

The diagrams are written by hand, so this test keeps them honest: every silver and gold
table is in them and no other, every column they name exists (tools/lineage.py lists
them all), the keys marked PK are the keys the layer checks enforce (checks/layers.py),
and every relationship joins on a column both tables have.

Not checked here: whether the real data has orphans; that was checked on the lakehouse
when the diagram was drawn (README.md says when).
"""
import re
from pathlib import Path

from checks.layers import GOLD_KEYS, SILVER_CBS_KEYS
from tools.lineage import LINEAGE

ROOT = Path(__file__).resolve().parents[1]
# The two silver tables without an entry in checks/layers.py, and their key.
OTHER_KEYS = {"kvk_company": ["source_row_number"],
              "map_sbi2008_sbi2025": ["sbi2008_code", "sbi2025_code"]}
KEYS = {**SILVER_CBS_KEYS, **GOLD_KEYS, **OTHER_KEYS}

ENTITY = re.compile(r"^\s*(\w+) \{\n(.*?)^\s*\}", re.M | re.S)
ATTRIBUTE = re.compile(r"^\s*(\w+) (\w+)(?: (PK|FK|PK, FK))?\s*$")
RELATION = re.compile(r"^\s*(\w+) [|}][|o](--|\.\.)[|o][|{] (\w+) : (.+)$", re.M)


def diagrams() -> list[str]:
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    return re.findall(r"```mermaid\nerDiagram\n(.*?)```", text, re.S)


def entities(diagram: str) -> dict[str, list[tuple[str, str]]]:
    """table -> [(column, marker)] as the diagram declares them."""
    found = {}
    for name, body in ENTITY.findall(diagram):
        rows = [ATTRIBUTE.match(line) for line in body.splitlines() if line.strip()]
        assert all(rows), f"{name}: a line is not 'type column [PK|FK]'"
        found[name] = [(m[2], m[3] or "") for m in rows]
    return found


def test_the_readme_has_a_silver_and_a_gold_diagram():
    silver, gold = diagrams()
    assert set(entities(silver)) == {t for t in LINEAGE if not t.startswith("gold_")}
    assert set(entities(gold)) == set(GOLD_KEYS)


def test_every_column_in_the_diagrams_exists():
    for diagram in diagrams():
        for table, columns in entities(diagram).items():
            missing = [c for c, _ in columns if c not in LINEAGE[table]]
            assert not missing, f"{table} has no column {missing}"


def test_the_primary_keys_are_the_checked_keys():
    for diagram in diagrams():
        for table, columns in entities(diagram).items():
            pk = [c for c, marker in columns if "PK" in marker]
            assert pk == KEYS[table], f"{table}: diagram says {pk}, checks say {KEYS[table]}"


def test_every_relationship_joins_on_a_column_both_tables_have():
    silver = diagrams()[0]
    tables = entities(silver)
    relations = RELATION.findall(silver)
    assert relations, "no relationships found: did the notation change?"
    for parent, line, child, label in relations:
        assert parent in tables and child in tables, f"{parent} -> {child}"
        if line == "--":   # a key; dashed lines are lookups, described in words
            assert label in LINEAGE[parent] and label in LINEAGE[child], label
            assert (label, "FK") in tables[child] or (label, "PK, FK") in tables[child], \
                f"{child}.{label} is not marked FK"
