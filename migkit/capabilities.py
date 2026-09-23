"""What each engine can do, read off the code, with every gap declared.

Owner's rule (2026-09-24): the same hop and the same command give the same
result on every engine. Until every engine can, a gap is written down here
with its reason, not discovered by an operator.

What an engine *can* do is never declared: it is read off the code by the
probes below, so it cannot drift. Only the gaps are declared, each as not
applicable (with why) or not yet (with the backlog item). The test holds
the two against each other: a gap the probes see and nobody declared fails,
and so does a declared gap the code has since closed.
"""
import ast
import inspect

from .engines import NAMES, _class_for, engines_with
from .engines.base import Engine

#: every capability, in the words an operator reads
CAPABILITIES = {
    "schema": "comparing the schema",
    "counts": "comparing row counts",
    "data": "comparing the data itself",
    "deep": "the deep checks",
    "sequences": "carrying sequences and auto-increment values",
    "params": "comparing server settings",
    "bulk-move": "moving a whole database in bulk",
    "table-copy": "copying table by table, resumably",
    "stream": "keeping the target following the source",
    "fence": "proving the target has caught up before cutover",
    "delta": "verifying only what changed",
    "users": "carrying users and their grants",
    "guard": "noticing a move that moved nothing",
    "statistics": "refreshing the target's statistics after a load",
    "snapshot": "snapshotting the target so a cutover can be rolled back",
}

NOT_APPLICABLE = "not applicable"
NOT_YET = "not yet"

_FILE_DB = ("a SQLite database is a file with no change log that can be"
            " read without writing to it, so it moves offline")
_NO_PLANNER = "the engine keeps no optimiser statistics to refresh"

#: every gap, by engine: (NOT_APPLICABLE, why) or (NOT_YET, backlog item)
GAPS = {
    "postgres": {},
    "mysql": {
        "fence": (NOT_YET, "0e"),
    },
    "mongodb": {
        "sequences": (NOT_APPLICABLE,
                      "collections have no sequences or auto-increment"),
        "table-copy": (NOT_YET, "0e"),
        "fence": (NOT_YET, "0e"),
        "guard": (NOT_YET, "0e"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "mssql": {
        "bulk-move": (NOT_YET, "0e"),
        "table-copy": (NOT_YET, "0e"),
        "stream": (NOT_YET, "0e"),
        "fence": (NOT_YET, "0e"),
        "users": (NOT_YET, "0e"),
        "guard": (NOT_YET, "0e"),
        "statistics": (NOT_YET, "0e"),
        "snapshot": (NOT_YET, "0e"),
    },
    "redis": {
        "schema": (NOT_YET, "0e"),
        "sequences": (NOT_APPLICABLE, "keys have no sequences"),
        "params": (NOT_YET, "0e"),
        "bulk-move": (NOT_YET, "0e"),
        "table-copy": (NOT_YET, "0e"),
        "stream": (NOT_YET, "0e"),
        "fence": (NOT_YET, "0e"),
        "users": (NOT_YET, "0e"),
        "guard": (NOT_YET, "0e"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
        "snapshot": (NOT_YET, "0e"),
    },
    "kafka": {
        "sequences": (NOT_APPLICABLE,
                      "offsets are assigned by the broker, not carried"),
        "params": (NOT_YET, "0e"),
        "bulk-move": (NOT_YET, "0e"),
        "table-copy": (NOT_YET, "0e"),
        "stream": (NOT_YET, "0e"),
        "fence": (NOT_YET, "0e"),
        "users": (NOT_YET, "0e"),
        "guard": (NOT_YET, "0e"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
        "snapshot": (NOT_YET, "0e"),
    },
    "sqlite": {
        "params": (NOT_YET, "0e"),
        "bulk-move": (NOT_YET, "0e"),
        "table-copy": (NOT_YET, "0e"),
        "stream": (NOT_APPLICABLE, _FILE_DB),
        "fence": (NOT_APPLICABLE, _FILE_DB),
        "delta": (NOT_APPLICABLE, _FILE_DB),
        "users": (NOT_APPLICABLE, "a SQLite database has no users"),
        "guard": (NOT_YET, "0e"),
        "statistics": (NOT_YET, "0e"),
        "snapshot": (NOT_YET, "0e"),
    },
    "hetero": {
        "deep": (NOT_YET, "0e"),
        "sequences": (NOT_YET, "0e"),
        "params": (NOT_YET, "0e"),
        "fence": (NOT_YET, "0e"),
        "delta": (NOT_YET, "0e"),
        "users": (NOT_YET, "0e"),
        "guard": (NOT_YET, "0e"),
        "statistics": (NOT_YET, "0e"),
        "snapshot": (NOT_YET, "0e"),
    },
    "generic": {
        "sequences": (NOT_YET, "0e"),
        "params": (NOT_YET, "0e"),
        "bulk-move": (NOT_YET, "33"),
        "table-copy": (NOT_YET, "33"),
        "stream": (NOT_YET, "33"),
        "fence": (NOT_YET, "33"),
        "delta": (NOT_YET, "33"),
        "users": (NOT_YET, "0e"),
        "guard": (NOT_YET, "0e"),
        "statistics": (NOT_YET, "0e"),
        "snapshot": (NOT_YET, "0e"),
    },
}


def _own(cls, method):
    """The engine does it itself, not the base class's placeholder."""
    found = getattr(cls, method, None)
    return found is not None and found is not getattr(Engine, method, None)


def _users_engines():
    """The engines `users` dispatches on, read from its source: the
    comparison branches on the engine name rather than on a method."""
    from . import users
    tree = ast.parse(inspect.getsource(users.compare))
    names = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Compare)
                and isinstance(node.left, ast.Name)
                and node.left.id == "eng"):
            continue
        for right in node.comparators:
            for c in ast.walk(right):
                if isinstance(c, ast.Constant) and isinstance(c.value, str):
                    names.add(c.value)
    return names


def _bulk(name):
    from . import movers
    return any(movers.supported(name, via) for via in movers.VIAS
               if via not in ("auto", "builtin"))


def _has(method):
    return lambda n, c: n in engines_with(method)


def _stream(name):
    from . import movers
    return movers.stream_supported(name) or name in engines_with("tail_apply")


PROBES = {
    "schema": lambda n, c: _own(c, "check_schema"),
    "counts": lambda n, c: _own(c, "check_counts"),
    "data": lambda n, c: _own(c, "check_data"),
    "deep": lambda n, c: _own(c, "check_deep"),
    "sequences": lambda n, c: _own(c, "check_autoinc"),
    "params": lambda n, c: _own(c, "check_params"),
    "bulk-move": lambda n, c: _bulk(n),
    "table-copy": _has("move_table"),
    "stream": lambda n, c: _stream(n),
    "fence": _has("fence_wait"),
    "delta": _has("delta_verify"),
    "users": lambda n, c: n in _users_engines(),
    "guard": lambda n, c: _own(c, "moved_nothing"),
    "statistics": lambda n, c: _own(c, "settle_target"),
    "snapshot": _has("snapshot_state"),
}


def implemented(name, capability):
    """Whether the code does it for this engine, measured, not declared."""
    return bool(PROBES[capability](name, _class_for(name)))


def matrix():
    """{engine: {capability: "yes" | NOT_APPLICABLE | NOT_YET}}."""
    out = {}
    for name in NAMES:
        row = {}
        for cap in CAPABILITIES:
            if implemented(name, cap):
                row[cap] = "yes"
            else:
                row[cap] = GAPS.get(name, {}).get(cap, ("undeclared",))[0]
        out[name] = row
    return out


def undeclared():
    """(engine, capability) pairs the code lacks and nobody declared."""
    return [(n, cap) for n, row in matrix().items()
            for cap, state in row.items() if state == "undeclared"]


def stale():
    """Declared gaps the code has since closed."""
    return [(n, cap) for n, gaps in GAPS.items() for cap in gaps
            if implemented(n, cap)]
