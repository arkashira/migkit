"""Every capability a command asks of the table it refuses by
(`capabilities.unavailable` / `require` / `implemented`, and the tables
`CAPABILITIES` / `PROBES` / `INSTEAD` read by key) is a key the table
names: read from the source, so a key renamed or misspelt fails here and
not on the operator's first move (W15J: the move unit asked "resume", a
capability with no name, and every `migrate` move unit stopped on it)."""

import ast
from pathlib import Path

from migkit import capabilities

ROOT = Path(__file__).resolve().parent.parent / "migkit"
#: the calls whose second argument is a capability key
CALLS = ("unavailable", "require", "implemented")
#: the tables whose subscript is a capability key
TABLES = ("CAPABILITIES", "PROBES", "INSTEAD")
#: the modules the command asks from first (the brief's), and every other
#: module of the package, read the same way
FIRST = ("cli.py", "autonomous.py", "ready.py", "guide.py", "verdict.py")


def _is_caps(node):
    return isinstance(node, ast.Name) and node.id in ("capabilities",)


def asked(source, where="<source>"):
    """(keys, unread): every literal capability key the source asks, as
    (key, where:line), and every key argument that is neither a literal
    nor a name bound by iterating one of the tables (where:line, said)."""
    tree = ast.parse(source)
    over = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.For, ast.comprehension)):
            it = node.iter
            if isinstance(it, ast.Attribute) and _is_caps(it.value) \
                    and it.attr in TABLES \
                    and isinstance(node.target, ast.Name):
                over.add(node.target.id)
    # a call imported by name (`from .capabilities import require`)
    bare = {a.asname or a.name for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and (node.module or "").split(".")[-1] == "capabilities"
            for a in node.names if a.name in CALLS}
    keys, unread = [], []

    def read(arg, line):
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            keys.append((arg.value, f"{where}:{line}"))
        elif not (isinstance(arg, ast.Name) and arg.id in over):
            unread.append((f"{where}:{line}", ast.unparse(arg)))

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and (
                isinstance(node.func, ast.Attribute)
                and _is_caps(node.func.value) and node.func.attr in CALLS
                or isinstance(node.func, ast.Name) and node.func.id in bare):
            arg = node.args[1] if len(node.args) > 1 else next(
                (k.value for k in node.keywords if k.arg == "capability"),
                None)
            if arg is not None:
                read(arg, node.lineno)
        elif isinstance(node, ast.Subscript) \
                and isinstance(node.value, ast.Attribute) \
                and _is_caps(node.value.value) \
                and node.value.attr in TABLES:
            read(node.slice, node.lineno)
    return keys, unread


def missing(keys):
    """The asked keys a table does not name, each with where it is asked."""
    return [(k, at) for k, at in keys
            if k not in capabilities.CAPABILITIES
            or k not in capabilities.PROBES or k not in capabilities.INSTEAD]


def _modules():
    first = [ROOT / n for n in FIRST]
    rest = sorted(p for p in ROOT.rglob("*.py") if p not in first
                  and p.name != "capabilities.py")
    return first + rest


def test_the_three_tables_name_the_same_capabilities():
    assert set(capabilities.PROBES) == set(capabilities.CAPABILITIES)
    assert set(capabilities.INSTEAD) >= set(capabilities.CAPABILITIES)


def test_every_key_the_command_asks_is_in_the_tables():
    keys, unread = [], []
    for path in _modules():
        got, odd = asked(path.read_text(), str(path.relative_to(ROOT)))
        keys += got
        unread += odd
    # the move unit's own ask (cli `_Steps.facts`) is among them
    assert ("table-copy" in {k for k, at in keys if at.startswith("cli.py")})
    assert {at.split(":")[0] for _, at in keys} >= {"cli.py"}
    assert not missing(keys), missing(keys)
    assert not unread, unread


def test_a_planted_key_is_caught():
    """The reader is not hollow: a key no table names, asked any of the
    ways a command asks one, is found and said with its line."""
    planted = (
        "from . import capabilities\n"
        "def f(hop):\n"
        "    capabilities.unavailable(hop.engine, 'resume')\n"
        "    capabilities.require(hop.engine, capability='tabel-copy')\n"
        "    from .capabilities import require as need\n"
        "    need(hop.engine, 'snapshots')\n"
        "    return capabilities.CAPABILITIES['fencing']\n")
    keys, unread = asked(planted, "planted.py")
    assert missing(keys) == [("resume", "planted.py:3"),
                             ("tabel-copy", "planted.py:4"),
                             ("snapshots", "planted.py:6"),
                             ("fencing", "planted.py:7")]
    assert not unread


def test_a_key_read_from_a_name_the_test_cannot_follow_is_said():
    """A key passed through a variable the reader cannot follow is said,
    never taken as known; one bound by iterating a table is followed."""
    src = ("from . import capabilities\n"
           "def f(hop, c):\n"
           "    ok = [x for x in capabilities.CAPABILITIES\n"
           "          if capabilities.unavailable(hop.engine, x)]\n"
           "    return capabilities.unavailable(hop.engine, c)\n")
    keys, unread = asked(src, "odd.py")
    assert keys == [] and unread == [("odd.py:5", "c")]
