"""Every import in migkit/ goes to its own layer or one below it.

`docs/agents/LAYERS.md` is written by `tools/gen_codemap.py` from one
ordered list (`LAYERS`) and a table that gives every file a responsibility
package. A lower package that imports a higher one - an engine calling the
workflow that drives it, a conversion asking the decision engine which way
to go - is the dependency that keeps the source from being read layer by
layer (backlog P1-SOURCE-HIERARCHY.layout). The ones that are there today
are listed in `KNOWN_UPWARD`, each with its reason; this fails on an
import added (with its `file:line`) and on a listed one that is gone, so
the list only shrinks.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import gen_codemap  # noqa: E402


def _listed():
    return {tuple(p.split(" ")): reason
            for reason, pairs in gen_codemap.KNOWN_UPWARD.items()
            for p in pairs}


def test_no_import_goes_up_that_is_not_listed():
    listed = _listed()
    new = [f"{f}:{line} imports {m} ({a} -> {b})"
           for f, line, m, a, b in gen_codemap.upward_imports()
           if (f, m) not in listed]
    assert not new, ("a package imports one above it (see docs/agents/"
                     "LAYERS.md): move the shared piece down or invert the"
                     " call:\n  " + "\n  ".join(new))


def test_no_listed_import_is_gone():
    found = {(f, m) for f, _l, m, _a, _b in gen_codemap.upward_imports()}
    gone = sorted(set(_listed()) - found)
    assert not gone, ("listed in KNOWN_UPWARD and no longer there - take"
                      " them off the list: " + ", ".join(
                          f"{f} {m}" for f, m in gone))


def test_every_listed_import_says_why_in_one_line():
    for reason, pairs in gen_codemap.KNOWN_UPWARD.items():
        assert reason.strip() and "\n" not in reason
        assert pairs and len(set(pairs)) == len(pairs), reason
        assert all(len(p.split(" ")) == 2 for p in pairs), reason


def test_the_listed_count_is_what_the_state_says():
    state = (ROOT / "docs" / "agents" / "STATE.md").read_text()
    n = len(gen_codemap.upward_imports())
    assert f"{n} upward imports still listed" in state


def _tree(root, modules):
    (root / "migkit").mkdir(exist_ok=True)
    (root / "migkit" / "__init__.py").write_text("")
    for name, text in modules.items():
        (root / "migkit" / f"{name}.py").write_text(text)


def test_a_planted_upward_import_is_found_with_its_line(tmp_path):
    """`hopkeys` is the configuration layer, `cli` the command line above
    everything: a top-level import and one inside a function both fail."""
    _tree(tmp_path, {
        "hopkeys": "X = 1\n\n\ndef late():\n    from . import cli\n",
        "envkeys": "from migkit import cli\n",
        "cli": "from . import hopkeys\nfrom migkit.envkeys import Y\n",
    })
    got = gen_codemap.upward_imports(tmp_path)
    assert [(f, line, m) for f, line, m, _a, _b in got] == [
        ("migkit/envkeys.py", 1, "migkit.cli"),
        ("migkit/hopkeys.py", 5, "migkit.cli")]
    assert {(a, b) for *_x, a, b in got} == {("configuration", "cli")}


def test_a_module_named_by_a_string_counts_as_an_import(tmp_path):
    _tree(tmp_path, {
        "hopkeys": "import importlib\n\n\ndef late():\n"
                   "    return importlib.import_module('migkit.cli')\n",
        "cli": ""})
    got = gen_codemap.upward_imports(tmp_path)
    assert [(f, m) for f, _l, m, *_x in got] == [
        ("migkit/hopkeys.py", "migkit.cli")]


def test_imports_down_and_across_are_not_flagged(tmp_path):
    _tree(tmp_path, {
        "cli": "from . import hopkeys, decide\n",
        "decide": "from . import hopkeys\n",
        "hopkeys": "from . import envkeys\n",
        "envkeys": "",
        # beside each other: the rank of two
        "tools": "from . import cloudapi\n",
        "cloudapi": "from . import tools\n"})
    assert gen_codemap.upward_imports(tmp_path) == []


def test_a_package_may_import_inside_its_own_rank(tmp_path):
    (tmp_path / "migkit" / "engines").mkdir(parents=True)
    _tree(tmp_path, {"hopkeys": ""})
    (tmp_path / "migkit" / "engines" / "__init__.py").write_text("")
    (tmp_path / "migkit" / "engines" / "a.py").write_text(
        "from . import b\nfrom .. import hopkeys\n")
    (tmp_path / "migkit" / "engines" / "b.py").write_text("")
    assert gen_codemap.upward_imports(tmp_path) == []
