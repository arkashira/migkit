"""A module that declares `UNPROVED_ON` (what it was built for and could
not be run on) also declares `UNPROVED_ITEMS` (which depth items that
covers), so the state's "built, unproved" block names the cells it holds
(backlog AUD-Z, CRIT17 finding)."""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent / "migkit"

#: modules that declare the reason alone, each owned by a row that writes
#: the items (the file is the row's: an entry is removed when it does)
TODO = {}


def _names(path):
    got = set()
    for node in ast.parse(path.read_text(), str(path)).body:
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        got.update(t.id for t in targets if isinstance(t, ast.Name))
    return got


def test_a_module_that_says_unproved_on_lists_unproved_items():
    lacking = []
    for path in sorted(ROOT.rglob("*.py")):
        names = _names(path)
        if "UNPROVED_ON" in names and "UNPROVED_ITEMS" not in names:
            lacking.append(str(path.relative_to(ROOT.parent)))
    assert sorted(set(lacking) - set(TODO)) == [], \
        f"UNPROVED_ON without UNPROVED_ITEMS: {sorted(set(lacking) - set(TODO))}"


def test_nothing_waits_in_todo_that_has_its_items():
    stale = [m for m in TODO if "UNPROVED_ITEMS" in _names(ROOT.parent / m)]
    assert stale == [], f"remove from TODO: {stale}"
