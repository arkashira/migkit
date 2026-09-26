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
    "confirm": ("telling a difference still arriving from one that is"
                " wrong"),
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
_STREAM_ONLY = ("{} is where a pair delivers its changes (engine:"
                " hetero, target_engine: {}), not a hop's engine: it"
                " holds a stream of changes, not a copy to compare")
_FILES = ("Parquet files are data at rest, with no server, settings or"
          " change log of their own")

#: every gap, by engine: (NOT_APPLICABLE, why) or (NOT_YET, backlog item)
GAPS = {
    "postgres": {},
    "mysql": {},
    "mongodb": {
        "sequences": (NOT_APPLICABLE,
                      "collections have no sequences or auto-increment"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "mssql": {
        "confirm": (NOT_YET, "1"),
        "bulk-move": (NOT_YET, "0e"),
        "users": (NOT_YET, "0e"),
    },
    "redis": {
        "sequences": (NOT_APPLICABLE, "keys have no sequences"),
        "bulk-move": (NOT_YET, "0e"),
        "delta": (NOT_YET, "0e"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "kafka": {
        "confirm": (NOT_YET, "1"),
        "sequences": (NOT_APPLICABLE,
                      "offsets are assigned by the broker, not carried"),
        "bulk-move": (NOT_YET, "0e"),
        "users": (NOT_YET, "0e"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
        "snapshot": (NOT_YET, "0e"),
    },
    "sqlite": {
        "confirm": (NOT_APPLICABLE, _FILE_DB),
        "bulk-move": (NOT_YET, "0e"),
        "stream": (NOT_APPLICABLE, _FILE_DB),
        "fence": (NOT_APPLICABLE, _FILE_DB),
        "delta": (NOT_APPLICABLE, _FILE_DB),
        "users": (NOT_APPLICABLE, "a SQLite database has no users"),
    },
    "parquet": {
        "sequences": (NOT_APPLICABLE, _FILES),
        "params": (NOT_APPLICABLE, _FILES),
        "bulk-move": (NOT_YET, "34"),
        "stream": (NOT_YET, "34"),
        "fence": (NOT_APPLICABLE, _FILES),
        "confirm": (NOT_APPLICABLE, _FILES),
        "delta": (NOT_APPLICABLE, _FILES),
        "users": (NOT_APPLICABLE, "who may read the files is the storage's"
                                  " own access control, not the table's"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
        "snapshot": (NOT_YET, "34"),
    },
    "clickhouse": {
        "sequences": (NOT_APPLICABLE, "ClickHouse has no sequences or"
                                      " auto-increment to carry"),
        "params": (NOT_YET, "33"),
        "bulk-move": (NOT_YET, "33"),
        "stream": (NOT_YET, "33"),
        "fence": (NOT_YET, "33"),
        "confirm": (NOT_YET, "33"),
        "delta": (NOT_YET, "33"),
        "users": (NOT_YET, "33"),
        "statistics": (NOT_APPLICABLE, "a MergeTree reads by its sorting"
                                       " key and keeps no optimiser"
                                       " statistics to refresh"),
        "snapshot": (NOT_YET, "33"),
    },
    "dynamodb": {
        "sequences": (NOT_APPLICABLE, "DynamoDB has no sequences or"
                                      " auto-increment to carry"),
        "params": (NOT_YET, "34"),
        "bulk-move": (NOT_YET, "34"),
        "stream": (NOT_YET, "34"),
        "fence": (NOT_YET, "34"),
        "confirm": (NOT_YET, "34"),
        "delta": (NOT_YET, "34"),
        "users": (NOT_APPLICABLE, "who may read a table is IAM's, not the"
                                  " table's"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
        "snapshot": (NOT_YET, "34"),
    },
    "oracle": {
        "sequences": (NOT_YET, "11"),
        "params": (NOT_YET, "11"),
        "bulk-move": (NOT_YET, "11"),
        "stream": (NOT_YET, "11"),
        "fence": (NOT_YET, "11"),
        "confirm": (NOT_YET, "11"),
        "delta": (NOT_YET, "11"),
        "users": (NOT_YET, "11"),
        "snapshot": (NOT_YET, "11"),
    },
    "db2": {
        "sequences": (NOT_YET, "34"),
        "params": (NOT_YET, "34"),
        "bulk-move": (NOT_YET, "34"),
        "stream": (NOT_YET, "34"),
        "fence": (NOT_YET, "34"),
        "confirm": (NOT_YET, "34"),
        "delta": (NOT_YET, "34"),
        "users": (NOT_YET, "34"),
        "statistics": (NOT_YET, "34"),
        "snapshot": (NOT_YET, "34"),
    },
    "ase": {
        "deep": (NOT_YET, "34"),
        "sequences": (NOT_YET, "34"),
        "params": (NOT_YET, "34"),
        "bulk-move": (NOT_YET, "34"),
        "stream": (NOT_YET, "34"),
        "fence": (NOT_YET, "34"),
        "confirm": (NOT_YET, "34"),
        "delta": (NOT_YET, "34"),
        "users": (NOT_YET, "34"),
        "statistics": (NOT_YET, "34"),
        "snapshot": (NOT_YET, "34"),
    },
    "opensearch": {
        "sequences": (NOT_APPLICABLE, "an index has no sequences or"
                                      " auto-increment to carry"),
        "params": (NOT_YET, "34"),
        "bulk-move": (NOT_YET, "34"),
        "stream": (NOT_YET, "34"),
        "fence": (NOT_YET, "34"),
        "confirm": (NOT_YET, "34"),
        "delta": (NOT_YET, "34"),
        "users": (NOT_YET, "34"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
        "snapshot": (NOT_YET, "34"),
    },
    "cassandra": {
        "sequences": (NOT_APPLICABLE, "Cassandra has no sequences or"
                                      " auto-increment to carry"),
        "params": (NOT_YET, "34"),
        "bulk-move": (NOT_YET, "34"),
        "stream": (NOT_YET, "34"),
        "fence": (NOT_YET, "34"),
        "confirm": (NOT_YET, "34"),
        "delta": (NOT_YET, "34"),
        "users": (NOT_YET, "34"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
        "snapshot": (NOT_YET, "34"),
    },
    **{name: {
        "deep": (NOT_YET, "33"),
        "sequences": (NOT_YET, "33"),
        "params": (NOT_YET, "33"),
        "bulk-move": (NOT_YET, "33"),
        "stream": (NOT_YET, "33"),
        "fence": (NOT_YET, "33"),
        "confirm": (NOT_YET, "33"),
        "delta": (NOT_YET, "33"),
        "users": (NOT_YET, "33"),
        "statistics": (NOT_YET, "33"),
        "snapshot": (NOT_YET, "33"),
    } for name in ("redshift", "snowflake", "bigquery")},
    "kinesis": {cap: (NOT_APPLICABLE, _STREAM_ONLY.format("Kinesis",
                                                          "kinesis"))
                for cap in CAPABILITIES},
    "pubsub": {cap: (NOT_APPLICABLE, _STREAM_ONLY.format("Pub/Sub",
                                                         "pubsub"))
               for cap in CAPABILITIES},
    "hetero": {
        "users": (NOT_YET, "0e"),
    },
    "generic": {
        "confirm": (NOT_YET, "33"),
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


def _refuses_only(method):
    """A method whose whole body is a return of an error or skip result.

    Redis had a `delta_verify` that did nothing but say it could not, so
    the matrix counted Redis as verifying deltas. A method that only
    refuses is a gap wearing the method's name.
    """
    import textwrap
    try:
        fn = ast.parse(textwrap.dedent(inspect.getsource(method))).body[0]
    except (OSError, TypeError, SyntaxError):
        return False
    body = fn.body
    if body and isinstance(body[0], ast.Expr) \
            and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    if len(body) != 1 or not isinstance(body[0], ast.Return):
        return False
    return any(isinstance(n, ast.Constant) and n.value in ("error", "skip")
               for n in ast.walk(body[0]))


def _own(cls, method):
    """The engine does it itself, not the base class's placeholder."""
    found = getattr(cls, method, None)
    return (found is not None and found is not getattr(Engine, method, None)
            and not _refuses_only(found))


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
    return lambda n, c: (n in engines_with(method)
                         and not _refuses_only(getattr(c, method)))


def _stream(name):
    from . import movers
    return (movers.stream_supported(name) or name in engines_with("tail_apply")
            or name in engines_with("replicate_sql"))


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
    # the confirm pass is the base's; an engine runs it when it can fence
    # and can compare rows by key
    "confirm": lambda n, c: (n in engines_with("fence_wait")
                             and _own(c, "_compare_pks")),
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


#: what the operator can do meanwhile, for each capability
INSTEAD = {
    "schema": "compare the schema by hand before cutover",
    "counts": "compare the row counts by hand before cutover",
    "data": "compare the data by hand before cutover",
    "deep": "run migkit check, whose other checks still apply",
    "sequences": ("set each sequence on the target above the source's"
                  " highest value before cutover"),
    "params": "compare the server settings by hand",
    "bulk-move": "move it table by table, or restore a backup of the source",
    "table-copy": ("move it with the engine's own backup and restore, and"
                   " let migkit check prove the result"),
    "stream": ("stop writes on the source, move it once, and prove it with"
               " migkit check before cutover"),
    "fence": ("stop writes on the source and let migkit check prove the"
              " target before cutover"),
    "confirm": ("stop writes on the source before the final check, so"
                " nothing is still arriving when it looks"),
    "delta": ("run migkit check, which compares everything rather than only"
              " what changed"),
    "users": "create the users and their grants on the target by hand",
    "guard": "run migkit check after the move",
    "statistics": "refresh the target's statistics by hand after the load",
    "snapshot": "take a backup of the target yourself before cutover",
}


def unavailable(engine, capability, instead=None):
    """Why this engine cannot do it, in migkit's words, or None if it can.

    One sentence for every command and every engine, instead of each
    command wording its own refusal: the refusals this replaces said
    `--go not available`, `cdc not available`, and listed the engines that
    could, which tells an operator nothing about what to do. `instead`
    replaces the general advice where the command has a better one.
    """
    from .engines import ALIASES
    name = ALIASES.get(engine, engine)
    if name not in GAPS:
        return f"migkit does not know the engine {engine!r}"
    if implemented(name, capability):
        return None
    words = CAPABILITIES[capability]
    words = words[0].upper() + words[1:]
    state, why = GAPS[name].get(capability, (NOT_YET, "0e"))
    if state == NOT_APPLICABLE:
        return f"{words} does not apply to {engine} hops: {why}."
    return (f"{words} is not available for {engine} hops yet (backlog item"
            f" {why}). Meanwhile, {instead or INSTEAD[capability]}.")


def require(engine, capability, instead=None):
    """Stop with `unavailable`'s sentence when this engine cannot do it."""
    why = unavailable(engine, capability, instead)
    if why:
        raise SystemExit(why)
