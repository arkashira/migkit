"""Every fact a choice reads is in the one catalogue (backlog P0-SMART, the
owner's note B: "every factor").

`migkit.factbook` is what a decision is said to have stood on - known,
measured, predicted, unknown - so a fact a rung reads and the catalogue
lacks would be a fact no decision record could name. This reads the code
the way a climb reads the facts: every `f["x"]` and `f.get("x")` inside a
`Need`'s predicate or reason and a rung's `prove` - a lambda, or a
function of the same module by name - and fails on a fact the catalogue
does not declare. Reading by a key held in a variable is not followable
and is listed here with the facts it stands for, as the hop keys' test
does.
"""
import ast
import pathlib

from migkit import factbook

ROOT = pathlib.Path(__file__).resolve().parents[1] / "migkit"

#: the constructors whose callables read facts, and which of their
#: arguments do: positions, keyword names
READERS = {"Need": ((1, 2), ("holds", "why")),
           "_N": ((1, 2), ("holds", "why")),
           "_need": ((1, 2), ("holds", "why"))}
RUNG_READERS = ("prove",)

#: reads by a key held in a variable: (file, the line's function) -> the
#: facts it stands for
DYNAMIC = {
    "mysql.py": ("src", "dst"),
    # a hold's needs made by `_is` and `_at_least` (`freeze.Hold.rungs`)
    "freeze.py": ("alter system", "read only switch", "version", "acl",
                  "alter", "alone in its group", "control", "kill",
                  "authorizer", "authorize"),
}


def _keys(fn, where, found, dynamic):
    args = [a.arg for a in fn.args.args]
    if not args:
        return
    f = args[0]
    for n in ast.walk(fn):
        if isinstance(n, ast.Subscript) and isinstance(n.value, ast.Name) \
                and n.value.id == f:
            if isinstance(n.slice, ast.Constant) \
                    and isinstance(n.slice.value, str):
                found.setdefault(n.slice.value, where)
            else:
                dynamic.add(where)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) \
                and isinstance(n.func.value, ast.Name) \
                and n.func.value.id == f and n.func.attr == "get":
            if n.args and isinstance(n.args[0], ast.Constant):
                found.setdefault(n.args[0].value, where)
            else:
                dynamic.add(where)


def reads(src, name="x.py"):
    """({fact: where}, {where of a read by a variable}) of one module."""
    tree = ast.parse(src)
    funcs = {n.name: n for n in ast.walk(tree)
             if isinstance(n, (ast.FunctionDef, ast.Lambda))
             and hasattr(n, "name")}
    found, dynamic = {}, set()
    for n in ast.walk(tree):
        if not isinstance(n, ast.Call):
            continue
        fn = (n.func.id if isinstance(n.func, ast.Name) else
              n.func.attr if isinstance(n.func, ast.Attribute) else "")
        vals = []
        if fn in READERS:
            pos, kws = READERS[fn]
            vals = [n.args[i] for i in pos if i < len(n.args)]
            vals += [k.value for k in n.keywords if k.arg in kws]
        elif fn.endswith("Rung") or fn == "_R":
            vals = [k.value for k in n.keywords if k.arg in RUNG_READERS]
        for v in vals:
            where = f"{name}:{n.lineno}"
            if isinstance(v, ast.Lambda):
                _keys(v, where, found, dynamic)
            elif isinstance(v, ast.Name) and v.id in funcs:
                _keys(funcs[v.id], where, found, dynamic)
    return found, dynamic


def every_read():
    found, dynamic = {}, set()
    for p in sorted(ROOT.rglob("*.py")):
        got, dyn = reads(p.read_text(), p.name)
        for k, where in got.items():
            found.setdefault(k, where)
        dynamic |= dyn
    return found, dynamic


def test_every_fact_a_need_or_proof_reads_is_declared():
    found, _ = every_read()
    missing = {k: w for k, w in found.items() if not factbook.declared(k)}
    assert not missing, (
        "facts read by a rung and not in factbook.FACTS - declare each"
        f" with what it is and how it is known: {missing}")


def test_a_read_by_a_variable_is_one_the_test_knows():
    _, dynamic = every_read()
    unknown = [w for w in dynamic if w.split(":")[0] not in DYNAMIC]
    assert not unknown, (
        "a need reads a fact by a key held in a variable - add the file to"
        f" DYNAMIC with the facts it stands for: {sorted(unknown)}")
    for facts in DYNAMIC.values():
        assert not factbook.undeclared(facts), facts


def test_the_scan_sees_a_need_reading_an_undeclared_fact():
    """The scan itself can say "undeclared": a need reading a fact nobody
    declared is found, one reading a declared fact is not."""
    src = ('from migkit.decide import Need, Rung\n'
           'N = Need("x", lambda f: f["no such fact here"] and'
           ' f.get("version"))\n'
           'R = Rung("r", "r", prove=lambda f: f.get("another unknown"))\n')
    found, _ = reads(src)
    assert set(found) == {"no such fact here", "version",
                          "another unknown"}
    assert factbook.undeclared(found) == ["another unknown",
                                          "no such fact here"]


def test_a_family_is_declared_by_its_prefix():
    assert factbook.declared("change clone_plugin")
    assert factbook.declared("answer clone_plugin")
    assert factbook.declared("rtt:src")
    assert factbook.declared("rtt")
    assert not factbook.declared("chang clone_plugin")
    assert factbook.fact("rtt:dst").group == "link"


def test_every_fact_says_its_group_and_how_it_is_known():
    groups = {"side", "link", "machine", "table", "change", "limits",
              "history", "context"}
    for name, f in factbook.FACTS.items():
        assert f.group in groups, name
        assert f.how in (factbook.GIVEN, factbook.READ, factbook.MEASURED,
                         factbook.PREDICTED), name
        assert len(f.what.split()) >= 2, name
    # the plan's factors are all declared facts
    assert not factbook.undeclared(factbook.FACTORS)


def test_an_account_names_what_was_not_known():
    got = factbook.account(known=["bytes", "rows"], measured=["rtt"],
                           predicted=["costs", "rtt"])
    assert got["known"] == ["bytes", "rows"]
    assert got["measured"] == ["rtt"]
    # measured wins over predicted for the same fact
    assert got["predicted"] == ["costs"]
    assert "deadline" in got["unknown"] and "bytes" not in got["unknown"]
    line = factbook.said(got)
    assert line.startswith("known 2") and "unknown" in line
