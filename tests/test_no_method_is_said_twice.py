"""No class or module says the same function twice.

The second of two `def`s of one name in a class silently replaces the
first: measured on 2026-10-03, a pre-flight added under a name a class
already had took the place of three checks - the tables in scope, names
differing only by case, another writer on the target - and nothing said
so. Two parts written apart and merged are where it happens; this reads
every module's tree and names any such pair.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "migkit"


def _twice():
    out = []
    for p in sorted(ROOT.rglob("*.py")):
        tree = ast.parse(p.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, (ast.ClassDef, ast.Module)):
                continue
            seen = {}
            for b in node.body:
                if not isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                said = [ast.unparse(d) for d in b.decorator_list]
                if b.name in seen and not any(
                        w in d for d in said
                        for w in (".setter", ".deleter", "overload",
                                  ".register")):
                    out.append(f"{p.relative_to(ROOT.parent)}:"
                               f" {getattr(node, 'name', '<module>')}."
                               f"{b.name} at {seen[b.name]} and {b.lineno}")
                seen[b.name] = b.lineno
    return out


def test_no_function_replaces_another_of_its_name():
    assert _twice() == []


#: the places that still reshape a rung outside the decision engine, each
#: in a file another piece of work owns: the list only shrinks - a rung
#: made anew is made in `decide`, or declared whole where it is defined
RESHAPED_ELSEWHERE = {
    ("migkit/bulkways.py", "ladder"),
    ("migkit/freeze.py", "keep_rungs"),
    ("migkit/movers.py", "fitted"),
    ("migkit/tableway.py", "_given"),
    ("migkit/tableway.py", "batch_or_copier"),
}


def _rung_fields():
    import dataclasses

    from migkit import decide
    # its name and words are every dataclass's; the rest only a rung has
    return {f.name for f in dataclasses.fields(decide.Rung)} - {"name",
                                                                "said"}


def _reshaped(source, fields):
    """(function, line) of every `dataclasses.replace(...)` in `source`
    that sets what only a rung has."""
    tree = ast.parse(source)
    names = {"dataclasses.replace"} | {
        a.asname or a.name for n in ast.walk(tree)
        if isinstance(n, ast.ImportFrom) and n.module == "dataclasses"
        for a in n.names if a.name == "replace"}
    out = []

    def walk(node, where):
        for child in ast.iter_child_nodes(node):
            inner = (child.name if isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef)) else where)
            if (isinstance(child, ast.Call)
                    and ast.unparse(child.func) in names
                    and {k.arg for k in child.keywords} & fields):
                out.append((where, child.lineno))
            walk(child, inner)
    walk(tree, "<module>")
    return out


def test_a_rung_is_reshaped_only_by_the_decision_engine():
    """A rung's needs, gives or cost changed by `dataclasses.replace`
    outside `decide.py` is a second reading of what the ladder says: the
    one cli made of the change stream's ladder moved into
    `movers.stream_ways`, and none comes back."""
    fields = _rung_fields()
    found = {}
    for p in sorted(ROOT.rglob("*.py")):
        rel = str(p.relative_to(ROOT.parent))
        if rel == "migkit/decide.py":
            continue
        for where, line in _reshaped(p.read_text(), fields):
            found.setdefault((rel, where), []).append(line)
    new = {k: v for k, v in found.items() if k not in RESHAPED_ELSEWHERE}
    assert new == {}, new
    gone = set(RESHAPED_ELSEWHERE) - set(found)
    assert not gone, f"no longer reshaped there - take them off: {gone}"


def test_the_rung_lint_sees_a_replace_and_passes_others_by():
    fields = _rung_fields()
    assert _reshaped("import dataclasses\ndef f(r):\n"
                     "    return dataclasses.replace(r, needs=())\n",
                     fields) == [("f", 3)]
    assert _reshaped("from dataclasses import replace as re_\n"
                     "def g(r):\n    return re_(r, gives=frozenset())\n",
                     fields) == [("g", 3)]
    # a hop or a choice reshaped is not a rung
    assert _reshaped("import dataclasses\ndef h(c):\n"
                     "    return dataclasses.replace(c, reason='x')\n",
                     fields) == []
