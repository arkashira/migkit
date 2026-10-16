"""No fixed port and no fixed worker ceiling outside the decision engine
(owner rules, backlog AUD-Z): a numeric constant passed as a concurrency
bound is a ceiling nothing measured; a port written into a call is one
that can collide. A port belongs to `engines.PORTS` (the engine's default)
or to a hop; a width belongs to a rung in `decide.py`.

What stands is named with its reason; an entry that no longer matches the
code fails, so the list only shrinks."""
import ast
import pathlib

from migkit import engines

ROOT = pathlib.Path(__file__).resolve().parent.parent / "migkit"
WIDTH = {"max_workers", "workers", "n_workers", "pool_size",
         "max_connections", "concurrency", "parallel", "threads",
         "max_parallel", "jobs", "n_threads", "num_workers"}
POOLS = {"ThreadPoolExecutor", "ProcessPoolExecutor"}

#: a pair of sides read at once: the count is the number of sides, not a
#: ceiling on work
TWO_SIDES = {
    ("migkit/cli.py", "max_workers", 2),
    ("migkit/engines/hetero.py", "ThreadPoolExecutor", 2),
    ("migkit/interlock.py", "ThreadPoolExecutor", 2),
}
#: ceilings no rung measured yet: none since AUD-Z.workers (the driver's
#: in-flight requests are `decide.request_width`, the installer's fetches
#: `decide.fetch_width`); a new one is named here with its backlog row
OWED = set()


def _int(node):
    return (isinstance(node, ast.Constant) and isinstance(node.value, int)
            and not isinstance(node.value, bool))


def _widths():
    """{(file, how, number)} of every literal width above one outside
    decide.py."""
    got = set()
    for path in sorted(ROOT.rglob("*.py")):
        if path.name == "decide.py":
            continue
        rel = path.relative_to(ROOT.parent).as_posix()
        for node in ast.walk(ast.parse(path.read_text(), str(path))):
            if not isinstance(node, ast.Call):
                continue
            for kw in node.keywords:
                if kw.arg in WIDTH and _int(kw.value) and kw.value.value > 1:
                    got.add((rel, kw.arg, kw.value.value))
            name = getattr(node.func, "attr", getattr(node.func, "id", ""))
            if name in POOLS and node.args and _int(node.args[0]) \
                    and node.args[0].value > 1:
                got.add((rel, name, node.args[0].value))
    return got


def test_no_width_is_a_literal_outside_decide():
    unnamed = _widths() - TWO_SIDES - OWED
    assert unnamed == set(), f"a fixed worker ceiling: {sorted(unnamed)}"


def test_nothing_named_stands_that_the_code_no_longer_has():
    stale = (TWO_SIDES | OWED) - _widths()
    assert stale == set(), f"remove from the list: {sorted(stale)}"


def test_no_call_is_handed_a_literal_port():
    found = []
    for path in sorted(ROOT.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(), str(path))):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "port" and _int(kw.value) \
                            and kw.value.value > 0:
                        found.append(f"{path.name}:{node.lineno}")
    assert found == [], f"a port written into a call: {found}"


def test_an_engines_own_default_port_is_the_one_in_the_table():
    """The default an engine module repeats is the table's, never a second
    reading of it."""
    differ = []
    for path in sorted((ROOT / "engines").glob("*.py")):
        stem = path.stem
        for node in ast.parse(path.read_text()).body:
            if isinstance(node, ast.Assign) and _int(node.value) \
                    and any(getattr(t, "id", "") == "DEFAULT_PORT"
                            for t in node.targets):
                named = {n: p for n, p in engines.PORTS.items()
                         if p == node.value.value
                         and (n == stem or n.startswith(stem))}
                if not named and stem != "pgwire":
                    differ.append((stem, node.value.value))
    assert differ == [], f"a default port the table does not hold: {differ}"
