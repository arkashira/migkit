"""What each engine can do, read off the code, with every gap declared.

Owner's rule (2026-09-24): the same hop and the same command give the same
result on every engine. Until every engine can, a gap is written down here
with its reason, not discovered by an operator.

What an engine *can* do is never declared: it is read off the code by the
probes below, so it cannot drift. Only the gaps are declared, each as not
applicable (with why) or not yet (with the backlog item). The test holds
the two against each other: a gap the probes see and nobody declared fails,
and so does a declared gap the code has since closed.

A probe reads the named engine's class only, and a method's source once.
Measured (CPython 3.12, Apple arm64, cached bytecode, three runs): asking
whether PostgreSQL can do six things imported all 34 engine modules and
read their methods' source, 360-420 ms a command; now 2 engine modules
and 138-161 ms, 82 of them the movers module the bulk and stream probes
read. The gap words `decide` declares its shapes with are these
(`NOT_YET`, `NOT_APPLICABLE`, `STREAM_ONLY`), not a second set.

How deep each engine goes is the depth matrix below (`depth`, backlog
P0-PARITY): every item of every area read off the code the same way, a
way counted only once a test proves it on that engine (the tests'
`PROVES`), every gap declared (`DEPTH_GAPS`, `UNPROVED`). It is read for
the state of migkit (`tools/gen_codemap.py`) and by its test, never by a
command.
"""
import ast
import functools
import inspect

from .engines import NAMES, _class_for
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
#: a stream of changes as an engine: `decide`'s shapes say it too
STREAM_ONLY = ("{} is where a pair delivers its changes (engine:"
               " hetero, target_engine: {}), not a hop's engine: it"
               " holds a stream of changes, not a copy to compare")
_FILES = ("Parquet files are data at rest, with no server, settings or"
          " change log of their own")
_LAKE = ("an Iceberg or Delta table is files and a log of commits, with no"
         " server, settings or sequences of its own")
_ZOS_LOG = ("Db2 for z/OS hands its log out only to licensed code - a load"
            " module on the host reading it, or the change tables IBM's"
            " replication product fills - and no open reader of either"
            " exists, so a z/OS source moves by copies, each proved by"
            " check")
_EXTRACT = ("a file described by a copybook is an extract at rest, with no"
            " server, settings or change log; what changed since the last"
            " extract is found by comparing the two, which check does")
_TD_LOG = ("Teradata keeps no change log a client can read, so a Teradata"
           " source moves by copies, each proved by check")
_READ_ONLY = ("{} is read, never written: it is a pair's source (engine:"
              " hetero, source_engine: {}), and a hop between two of them"
              " compares them")
_READ_FOLLOW = ("{} is followed as a pair's source, its changes carried"
                " into another engine (engine: hetero, source_engine: {});"
                " two of them are compared, never one written")

#: every gap, by engine: (NOT_APPLICABLE, why) or (NOT_YET, backlog item)
GAPS = {
    "postgres": {},
    "mysql": {},
    "mongodb": {
        "sequences": (NOT_APPLICABLE,
                      "collections have no sequences or auto-increment"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "mssql": {},
    # bulk-move is read off the code: the Redis products' own copy
    # (`movers.riotx_move`), into a target its terms allow; any other
    # target is copied key by key (`table-copy`)
    "redis": {
        "sequences": (NOT_APPLICABLE, "keys have no sequences"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "kafka": {
        "sequences": (NOT_APPLICABLE,
                      "offsets are assigned by the broker, not carried"),
        # bulk-move is read off the code: the cluster mirror
        # (`movers.supported("kafka", "mirror")`) copies a topic whole
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "sqlite": {
        "confirm": (NOT_APPLICABLE, _FILE_DB),
        "stream": (NOT_APPLICABLE, _FILE_DB),
        "fence": (NOT_APPLICABLE, _FILE_DB),
        "delta": (NOT_APPLICABLE, _FILE_DB),
        "users": (NOT_APPLICABLE, "a SQLite database has no users"),
    },
    "duckdb": {
        "confirm": (NOT_APPLICABLE, _FILE_DB),
        "stream": (NOT_APPLICABLE, _FILE_DB),
        "fence": (NOT_APPLICABLE, _FILE_DB),
        "delta": (NOT_APPLICABLE, _FILE_DB),
        "users": (NOT_APPLICABLE, "a DuckDB database has no users"),
        "params": (NOT_APPLICABLE, "a DuckDB file keeps no settings: they"
                                   " are the process's that opens it"),
        "statistics": (NOT_APPLICABLE, "DuckDB keeps its statistics as it"
                                       " writes"),
    },
    "parquet": {
        "sequences": (NOT_APPLICABLE, _FILES),
        "params": (NOT_APPLICABLE, _FILES),
        "stream": (NOT_APPLICABLE, _FILES),
        "fence": (NOT_APPLICABLE, _FILES),
        "confirm": (NOT_APPLICABLE, _FILES),
        "delta": (NOT_APPLICABLE, _FILES),
        "users": (NOT_APPLICABLE, "who may read the files is the storage's"
                                  " own access control, not the table's"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "copybook": {
        "sequences": (NOT_APPLICABLE, _EXTRACT),
        "params": (NOT_APPLICABLE, _EXTRACT),
        "stream": (NOT_APPLICABLE, _EXTRACT),
        "fence": (NOT_APPLICABLE, _EXTRACT),
        "confirm": (NOT_APPLICABLE, _EXTRACT),
        "delta": (NOT_APPLICABLE, _EXTRACT),
        "users": (NOT_APPLICABLE, "who may read a dataset is the security"
                                  " manager's or the file system's, not the"
                                  " file's"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "clickhouse": {
        "sequences": (NOT_APPLICABLE, "ClickHouse has no sequences or"
                                      " auto-increment to carry"),
        "statistics": (NOT_APPLICABLE, "a MergeTree reads by its sorting"
                                       " key and keeps no optimiser"
                                       " statistics to refresh"),
    },
    "dynamodb": {
        "sequences": (NOT_APPLICABLE, "DynamoDB has no sequences or"
                                      " auto-increment to carry"),
        "users": (NOT_APPLICABLE, "who may read a table is IAM's, not the"
                                  " table's"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    # every cell closed (F5): sequences and identity columns, settings,
    # Data Pump over a link or direct path loads, the redo read by
    # LogMiner or by the redo-file reader, fence and confirm on the SCN
    # every commit has been read through, delta from the redo, users and
    # roles with their password hashes, the target's SCN and sequences
    "oracle": {},
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
    "db2i": {
        "sequences": (NOT_YET, "34"),
        "params": (NOT_YET, "34"),
        "bulk-move": (NOT_YET, "34"),
        "statistics": (NOT_APPLICABLE, "IBM i's query engine gathers and"
                                       " keeps its own statistics as the"
                                       " tables change"),
        "snapshot": (NOT_YET, "34"),
    },
    "db2zos": {
        "sequences": (NOT_YET, "34"),
        "params": (NOT_YET, "34"),
        "bulk-move": (NOT_YET, "34"),
        "stream": (NOT_APPLICABLE, _ZOS_LOG),
        "fence": (NOT_APPLICABLE, _ZOS_LOG),
        "confirm": (NOT_APPLICABLE, _ZOS_LOG),
        "delta": (NOT_APPLICABLE, _ZOS_LOG),
        "users": (NOT_YET, "34"),
        "statistics": (NOT_YET, "34"),
        "snapshot": (NOT_YET, "34"),
    },
    "teradata": {
        "sequences": (NOT_APPLICABLE, "Teradata has no sequences, and an"
                                      " identity column's next number is"
                                      " the system's own, set on no table"
                                      " that exists"),
        "params": (NOT_APPLICABLE, "Teradata's system settings are read by"
                                   " its own utility on a node, not through"
                                   " SQL"),
        "bulk-move": (NOT_APPLICABLE, "Teradata's whole-system copy is its"
                                      " backup and restore, licensed apart"
                                      " from the database; tables are"
                                      " copied in its parallel sessions"
                                      " (table-copy)"),
        "stream": (NOT_APPLICABLE, _TD_LOG),
        "fence": (NOT_APPLICABLE, _TD_LOG),
        "confirm": (NOT_APPLICABLE, _TD_LOG),
        "delta": (NOT_APPLICABLE, _TD_LOG),
        "snapshot": (NOT_APPLICABLE, "Teradata keeps no snapshot a client"
                                     " can take; a copy of the target is its"
                                     " backup tool's"),
    },
    **{name: {
        "deep": (NOT_APPLICABLE, f"{said} holds no state of a load to look"
                                 " into: it is read through its own"
                                 " interface"),
        "sequences": (NOT_APPLICABLE, _READ_ONLY.format(said, name)),
        "params": (NOT_APPLICABLE, _READ_ONLY.format(said, name)),
        "bulk-move": (NOT_APPLICABLE, _READ_ONLY.format(said, name)),
        "table-copy": (NOT_APPLICABLE, _READ_ONLY.format(said, name)),
        "guard": (NOT_APPLICABLE, _READ_ONLY.format(said, name)),
        "statistics": (NOT_APPLICABLE, _READ_ONLY.format(said, name)),
        "snapshot": (NOT_APPLICABLE, _READ_ONLY.format(said, name)),
        "stream": (NOT_APPLICABLE, _READ_FOLLOW.format(said, name)),
        "fence": (NOT_APPLICABLE, _READ_FOLLOW.format(said, name)),
        "confirm": (NOT_APPLICABLE, _READ_FOLLOW.format(said, name)),
        "delta": (NOT_APPLICABLE, _READ_FOLLOW.format(said, name)),
        "users": (NOT_APPLICABLE, users),
    } for name, said, users in (
        ("sapodp", "an SAP system's application data",
         "SAP authorisations are the system's roles (PFCG), not carried"),
        ("saas", "a SaaS application",
         "who may use an application is the application's own, not"
         " carried"))},
    "ase": {
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
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "cassandra": {
        "sequences": (NOT_APPLICABLE, "Cassandra has no sequences or"
                                      " auto-increment to carry"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
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
    **{name: {
        "sequences": (NOT_APPLICABLE, _LAKE),
        "params": (NOT_APPLICABLE, _LAKE),
        "users": (NOT_APPLICABLE, "who may read the table is the catalog's"
                                  " and the storage's own access control,"
                                  " not the table's"),
        "statistics": (NOT_APPLICABLE, "the table's statistics are its own"
                                       " files' footers and manifests,"
                                       " written with each commit"),
        "deep": (NOT_YET, "33"),
        "bulk-move": (NOT_YET, "33"),
        "stream": (NOT_YET, "33"),
        "fence": (NOT_YET, "33"),
        "confirm": (NOT_YET, "33"),
        "delta": (NOT_YET, "33"),
        "snapshot": (NOT_YET, "33"),
    } for name in ("iceberg", "delta")},
    # the engines on another's wire and the document stores followed
    # through their change feeds (ER4, ER5): their bulk paths, users and
    # snapshots are their own tools', not yet wrapped
    "tidb": {
        "bulk-move": (NOT_YET, "ER4"),
        "users": (NOT_YET, "ER4"),
        "snapshot": (NOT_YET, "ER4"),
    },
    "cockroachdb": {
        "sequences": (NOT_YET, "ER4"),
        "params": (NOT_YET, "ER4"),
        "bulk-move": (NOT_YET, "ER4"),
        "users": (NOT_YET, "ER4"),
        "statistics": (NOT_APPLICABLE, "CockroachDB collects its tables'"
                                       " statistics itself as they change"),
        "snapshot": (NOT_YET, "ER4"),
    },
    "dsql": {
        "sequences": (NOT_YET, "ER4"),
        "params": (NOT_APPLICABLE, "a DSQL cluster keeps no settings of"
                                   " its operator's to compare"),
        "bulk-move": (NOT_APPLICABLE, "DSQL takes 3,000 rows a"
                                      " transaction whatever writes them:"
                                      " the table copier writes within it"),
        "users": (NOT_YET, "ER4"),
        "statistics": (NOT_YET, "ER4"),
        "snapshot": (NOT_YET, "ER4"),
    },
    "couchbase": {
        "sequences": (NOT_APPLICABLE, "a bucket has no sequences or"
                                      " auto-increment to carry"),
        "params": (NOT_YET, "ER4"),
        "bulk-move": (NOT_YET, "ER4"),
        "users": (NOT_YET, "ER4"),
        "statistics": (NOT_YET, "ER4"),
        "snapshot": (NOT_YET, "ER4"),
    },
    "cosmosdb": {
        "sequences": (NOT_APPLICABLE, "a container has no sequences or"
                                      " auto-increment to carry"),
        "bulk-move": (NOT_YET, "ER5"),
        "users": (NOT_APPLICABLE, "who may read a container is the"
                                  " account's role assignments, not the"
                                  " container's"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
        "snapshot": (NOT_YET, "ER5"),
    },
    "kinesis": {cap: (NOT_APPLICABLE, STREAM_ONLY.format("Kinesis",
                                                         "kinesis"))
                for cap in CAPABILITIES},
    "pubsub": {cap: (NOT_APPLICABLE, STREAM_ONLY.format("Pub/Sub",
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


@functools.lru_cache(maxsize=None)
def _refuses_only(method):
    """A method whose whole body is a return of an error or skip result,
    or of nothing at all - None, `(None, None)`, an empty list.

    Redis had a `delta_verify` that did nothing but say it could not, so
    the matrix counted Redis as verifying deltas; Kafka's `_server_versions`
    says it cannot tell, and was read as reading both versions. A method
    that only refuses is a gap wearing the method's name.
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
    value = body[0].value
    if value is None or _nothing(value):
        return True
    return any(isinstance(n, ast.Constant) and n.value in ("error", "skip")
               for n in ast.walk(body[0]))


def _nothing(node):
    """None, or an empty or all-None list, tuple, set or dict."""
    if isinstance(node, ast.Constant):
        return node.value is None
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return all(_nothing(e) for e in node.elts)
    if isinstance(node, ast.Dict):
        return not node.keys
    return False


def _own(cls, method):
    """The engine does it itself, not the base class's placeholder."""
    found = getattr(cls, method, None)
    return (found is not None and found is not getattr(Engine, method, None)
            and not _refuses_only(found))


def _users_engines():
    """The engines `users` dispatches on, read from its source: the
    comparison branches on the engine name rather than on a method."""
    from . import users
    return _compared(users.compare, "eng")


@functools.lru_cache(maxsize=None)
def _compared(fn, var):
    """The names a function's dispatch compares `var` with (`if eng ==
    "mysql"`, `elif kind in ("postgres", ...)`), read from its source:
    a module that branches on the engine's name rather than on a method
    says which engines it serves only so."""
    tree = ast.parse(inspect.getsource(fn))
    names = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Compare)
                and isinstance(node.left, ast.Name)
                and node.left.id == var):
            continue
        for right in node.comparators:
            for c in ast.walk(right):
                if isinstance(c, ast.Constant) and isinstance(c.value, str):
                    names.add(c.value)
    return frozenset(names)


def _bulk(name):
    from . import movers
    return any(movers.supported(name, via) for via in movers.VIAS
               if via not in ("auto", "builtin"))


def _there(cls, method):
    """The class has the method, its own or inherited - what the engine
    list (`engines_with`) counts - read off that one class."""
    return cls is not None and getattr(cls, method, None) is not None


def _has(method):
    return lambda n, c: (_there(c, method)
                         and not _refuses_only(getattr(c, method)))


def _stream(name, cls):
    from . import movers
    return (movers.stream_supported(name) or _there(cls, "tail_apply")
            or _there(cls, "replicate_sql"))


PROBES = {
    "schema": lambda n, c: _own(c, "check_schema"),
    "counts": lambda n, c: _own(c, "check_counts"),
    "data": lambda n, c: _own(c, "check_data"),
    "deep": lambda n, c: _own(c, "check_deep"),
    "sequences": lambda n, c: _own(c, "check_autoinc"),
    "params": lambda n, c: _own(c, "check_params"),
    "bulk-move": lambda n, c: _bulk(n),
    "table-copy": _has("move_table"),
    "stream": _stream,
    "fence": _has("fence_wait"),
    # the confirm pass is the base's; an engine runs it when it can fence
    # and can compare rows by key
    "confirm": lambda n, c: (_there(c, "fence_wait")
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


# ---- the depth matrix (backlog P0-PARITY) ----------------------------------
#
# The owner (2026-10-04): every engine as deep as PostgreSQL and MySQL in
# everything - "complete, all of it deep, all of it good" - and an engine
# left thinner is a red test, never a silent gap. The table above says what
# an engine can do at all; this one says how deep it goes, item by item, in
# every area. As above, a way is never declared: it is read off the code the
# engine runs - its own methods and those it inherits from a mixin or from
# another engine, the base class's helpers they call, the pair's machinery
# where it hands the work to it (`Engine._as_pair`), never a base
# placeholder or a method that only refuses. And a way is "yes" only where a
# test proves it on that engine too: a test module says which items it
# proves on which engines in a module-level `PROVES` (read here without
# importing the tests). A gap is declared in `DEPTH_GAPS` - not applicable
# (with why) or not yet (with the backlog row it waits on) - and a way whose
# test is not written yet in `UNPROVED`, which leaves it "not yet". The
# guard, `tests/test_every_engine_is_as_deep_as_postgres_and_mysql.py`,
# fails on a cell nobody declared, a way nobody tested ("untested"), a
# declaration the code or a test has closed, a proof of a way the code
# does not show, and a "not yet" whose row is not in
# `docs/backlog-index.md`.

#: the areas of the depth matrix: what each holds, in the operator's words,
#: and the backlog row a gap in it waits on (`DEPTH_GAPS` may name another)
AREAS = {
    "scope": ("what the hop takes, held in every path", "P0-PARITY.scope"),
    "verify": ("the checks, what they find and what repairs it",
               "P0-PARITY.verify"),
    "tail": ("following changes table by table", "P0-PARITY.tail"),
    "assess": ("the assessment and the pre-flight", "P0-PARITY.all"),
    "schema": ("the schema made on the target, and its objects",
               "P0-PARITY.all"),
    "move": ("the move itself", "P0-PARITY.all"),
    "cutover": ("the cutover and the way back", "P0-PARITY.all"),
    "security": ("the connection's security", "P0-PARITY.all"),
    "managed": ("managed services", "P0-PARITY.all"),
    "operations": ("high availability and fencing", "P0-PARITY.all"),
}

#: the name of the list a test module proves items with:
#: `PROVES = [("scope.deep", "postgres"), ("verify.delta", ("mysql",
#: "tidb")), ...]`, literal, at the module's top level
REGISTRY = "PROVES"

_FIELDS = ("rows_apart", "tables", "table")


@functools.lru_cache(maxsize=None)
def _defs(path):
    """{first line: def node} of a module, parsed once."""
    try:
        with open(path) as f:
            tree = ast.parse(f.read())
    except (OSError, SyntaxError, ValueError):
        return {}
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            first = min([node.lineno] + [d.lineno for d in
                                         node.decorator_list])
            out[first] = node
            out.setdefault(node.lineno, node)
    return out


def _last_word(node):
    """The last word of a finding's scope where the code spells it: a
    constant, or an f-string ending in one."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        text = node.value
    elif isinstance(node, ast.JoinedStr) and node.values \
            and isinstance(node.values[-1], ast.Constant):
        text = str(node.values[-1].value)
    else:
        return None
    return text.split()[-1] if text.split() else None


def _nodes(node):
    """Every node under `node`, as `ast.walk` gives them, without the
    load and store markers every name carries - a quarter of a tree, and
    nothing a probe reads."""
    stack = [node]
    while stack:
        n = stack.pop()
        yield n
        for name in n._fields:
            if name == "ctx":
                continue
            v = getattr(n, name, None)
            if type(v) is list:
                stack.extend(x for x in v if isinstance(x, ast.AST))
            elif isinstance(v, ast.AST):
                stack.append(v)


class _Body:
    """What one function's body does, read once from its module's tree:
    the names it says, what it calls on itself (`self.x`), on the pair it
    hands work to (`self._as_pair().x`), on the pair's two engines
    (`self.src_engine.x`) and by bare name; the findings it makes (the
    last word of each scope, the fields it fills, a category it sets) and
    whether a repair action it makes carries its undo."""

    def __init__(self, fn):
        self.words, self.selfs, self.pairs, self.sides = set(), set(), \
            set(), set()
        self.bare, self.fields, self.results = set(), set(), []
        self.calls, self.imports, self.modcalls = {}, {}, set()
        self.undo = False
        code = getattr(fn, "__code__", None)
        node = code and _defs(code.co_filename).get(code.co_firstlineno)
        if node is None:
            return
        pairs, attributes = set(), []
        for n in _nodes(node):
            kind = type(n)
            if kind is ast.Name:
                self.words.add(n.id)
            elif kind is ast.Attribute:
                attributes.append(n)
            elif kind is ast.Call:
                self._call(n)
            elif kind is ast.Assign and type(n.value) is ast.Call \
                    and type(n.value.func) is ast.Attribute \
                    and n.value.func.attr in ("_as_pair", "columns_pair"):
                pairs.update(t.id for t in n.targets
                             if type(t) is ast.Name)
            elif kind is ast.ImportFrom:
                for a in n.names:
                    self.imports[a.asname or a.name] = (n.level, n.module,
                                                        a.name)
        for n in attributes:
            self._attribute(n, pairs)

    def _attribute(self, n, pairs):
        self.words.add(n.attr)
        v = n.value
        if isinstance(v, ast.Name) and v.id in ("self", "cls"):
            self.selfs.add(n.attr)
        elif isinstance(v, ast.Name) and v.id in pairs or (
                isinstance(v, ast.Call) and isinstance(v.func, ast.Attribute)
                and v.func.attr in ("_as_pair", "columns_pair")):
            self.pairs.add(n.attr)
        elif isinstance(v, ast.Attribute) \
                and v.attr in ("src_engine", "dst_engine") \
                and isinstance(v.value, ast.Name) and v.value.id == "self":
            self.sides.add(n.attr)
        if isinstance(n.ctx, ast.Store) and n.attr in _FIELDS \
                and not (isinstance(v, ast.Name) and v.id == "self"):
            self.fields.add(n.attr)

    def _call(self, n):
        f = n.func
        name = f.id if isinstance(f, ast.Name) else \
            f.attr if isinstance(f, ast.Attribute) else None
        if isinstance(f, ast.Name):
            self.bare.add(f.id)
        elif isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
            self.modcalls.add((f.value.id, f.attr))
        if name:
            self.calls.setdefault(name, set()).update(
                k.arg for k in n.keywords if k.arg)
        kw = {k.arg: k.value for k in n.keywords if k.arg}
        if name == "Result":
            scope = n.args[1] if len(n.args) > 1 else kw.get("scope")
            status = n.args[2] if len(n.args) > 2 else kw.get("status")
            cat = kw.get("category")
            # a finding that can only skip or fail says the check is not
            # there (`_refuses_only`)
            if not (isinstance(status, ast.Constant)
                    and status.value in ("skip", "error")):
                self.results.append((
                    _last_word(scope), set(kw),
                    cat.value if isinstance(cat, ast.Constant) else None))
        elif name == "RepairAction":
            undo = n.args[3] if len(n.args) > 3 else kw.get("undo")
            if undo is not None and not (
                    isinstance(undo, (ast.List, ast.Tuple)) and not undo.elts):
                self.undo = True


@functools.lru_cache(maxsize=None)
def _body(fn):
    return _Body(fn)


def _resolve(cls, name):
    """The function `cls` runs for `name`, or None."""
    for k in getattr(cls, "__mro__", ()):
        if name in k.__dict__:
            v = k.__dict__[name]
            if isinstance(v, (staticmethod, classmethod)):
                v = v.__func__
            elif isinstance(v, property):
                v = v.fget
            elif isinstance(v, functools.cached_property):
                v = v.func
            v = inspect.unwrap(v) if callable(v) else v
            return v if inspect.isfunction(v) else None
    return None


@functools.lru_cache(maxsize=None)
def _reach(cls, entries):
    """Every function an engine runs from `entries`, those of its own
    (`_own`: never the base's placeholder, never one that only refuses):
    what each calls on itself, the base's helpers included; on the pair it
    hands the work to, and from there on the engine again; and the
    functions of the engines' modules it calls by name."""
    from .engines.hetero import HeteroEngine
    todo = [(_resolve(cls, e), cls) for e in entries if _own(cls, e)]
    seen = set()
    while todo:
        fn, home = todo.pop()
        if fn is None or (fn, home) in seen:
            continue
        seen.add((fn, home))
        body = _body(fn)
        todo += [(_resolve(home, a), home) for a in body.selfs]
        todo += [(_resolve(HeteroEngine, a), HeteroEngine)
                 for a in body.pairs]
        if home is HeteroEngine and cls is not HeteroEngine:
            todo += [(_resolve(cls, a), cls) for a in body.sides]
        names = getattr(fn, "__globals__", {})
        todo += [(g, home) for g in (names.get(b) for b in body.bare)
                 if inspect.isfunction(g)
                 and g.__module__.startswith("migkit.engines")]
    return frozenset(fn for fn, _ in seen)


@functools.lru_cache(maxsize=None)
def _reached(cls, entries):
    """(every name called, every name said) in what `entries` reach."""
    calls, words = set(), set()
    for fn in _reach(cls, entries):
        calls |= set(_body(fn).calls)
        words |= _body(fn).words
    return frozenset(calls), frozenset(words)


def _asks(cls, entries, ask):
    """Whether what `entries` reach calls one of `ask`'s functions or
    names one of its modules: `ask` is (call names, module names) - a
    name merely said, a local of the same spelling, is not a call."""
    calls, words = _reached(cls, tuple(entries))
    return bool(calls & ask[0] or words & ask[1])


#: the one scope answer a listing asks (backlog P0-SCOPE.6) - never a name
#: split by hand and handed to `Hop.excluded`
_SCOPE = (frozenset({"in_scope", "_scoped_rows", "takes", "takes_table",
                     "_drills_in_scope"}), frozenset())
_OBJECTS = (frozenset({"takes", "takes_object"}), frozenset())
_ROW_FILTER = (frozenset({"row_filter"}), frozenset({"rowfilter"}))
_TRANSFORMS = (frozenset({"masked_on_the_way"}), frozenset({"transforms"}))
#: a table the hop renames or maps handed to the pair, which reads the
#: name rules (`through_pair`), or the rules read here
_NAME_RULES = (frozenset({"target_table_in", "target_table",
                          "through_pair"}), frozenset())
#: `movers.stream_cannot_carry` holds a table to the changes it carries
#: (`Hop.carries`) along with its filter and its names
_CARRIES = (frozenset({"carries", "stream_cannot_carry"}), frozenset())
_UNDO = (frozenset({"repair_io"}), frozenset({"repairlog"}))
_VERSIONS = (frozenset({"_version_row"}), frozenset())
_HANDWORK = (frozenset({"_handwork"}), frozenset({"handwork"}))
#: what an engine's checks are run from
_CHECKS = ("check_schema", "check_counts", "check_data", "check_deep",
           "check_autoinc", "check_params")


def _every(entries, ask):
    """Each of `entries` the engine has of its own reaches `ask` - and it
    has one."""
    def probe(n, c):
        have = [e for e in entries if _own(c, e)]
        return bool(have) and all(_asks(c, (e,), ask) for e in have)
    return probe


#: where a table's copy is listed and made
_COPY = ("list_move_tables", "move_table")


def _both(check, ask):
    """The copy reads it - where its tables are listed or where each is
    copied - and so does each of `check` the engine has."""
    return lambda n, c: any(_own(c, e) for e in _COPY) \
        and _asks(c, _COPY, ask) and _every(check, ask)(n, c)


def _any(entries, ask):
    return lambda n, c: _asks(c, entries, ask)


def _module(fn, alias):
    """The migkit module `alias` names in `fn`: imported where the module
    is, or within the function (`from .. import verdict`)."""
    import importlib
    got = getattr(fn, "__globals__", {}).get(alias)
    if inspect.ismodule(got):
        return got if got.__name__.startswith("migkit") else None
    imp = _body(fn).imports.get(alias)
    if imp is None:
        return None
    level, module, name = imp
    base = fn.__module__.rsplit(".", level)[0] if level else ""
    path = ".".join(p for p in (base, module, name) if p)
    if not path.startswith("migkit"):
        return None
    try:
        return importlib.import_module(path)
    except ImportError:
        return None


def _builders(fn):
    """The functions of other migkit modules `fn` calls by their module's
    name that make a finding themselves (`verdict.timeshift_result`)."""
    out = []
    for alias, attr in _body(fn).modcalls:
        mod = _module(fn, alias)
        got = getattr(mod, attr, None) if mod is not None else None
        if inspect.isfunction(got) and _finds(got.__code__.co_filename) \
                and _body(got).results:
            out.append(got)
    return out


@functools.lru_cache(maxsize=None)
def _finds(path):
    """Whether a module makes a finding anywhere - read as text, so a
    module that makes none is never parsed."""
    try:
        with open(path) as f:
            return "Result(" in f.read()
    except OSError:
        return False


@functools.lru_cache(maxsize=None)
def _said(cls):
    """({category}, {field}) the engine's checks can put in a finding -
    built where they run, or by a function of another module they call
    to build it."""
    from .engines.base import CATEGORIES
    entries = _CHECKS + tuple(getattr(cls, "PREFLIGHT", ()) or ())
    cats, fields = set(), set()
    reached = _reach(cls, entries)
    for fn in list(reached) + [b for f in reached for b in _builders(f)]:
        body = _body(fn)
        for word, kws, cat in body.results:
            if cat:
                cats.add(cat)
            elif word in CATEGORIES:
                cats.add(CATEGORIES[word])
            fields |= kws & set(_FIELDS)
        fields |= body.fields
    return frozenset(cats), frozenset(fields)


def _says(category):
    return lambda n, c: category in _said(c)[0]


def _fills(field_):
    return lambda n, c: field_ in _said(c)[1]


def _undone(n, c):
    """A repair's writes undone exactly: an action carrying its undo, or
    rows kept with their undo by the repair's record (`repairlog`)."""
    if not (_own(c, "repair_plan") and _own(c, "apply")):
        return False
    entries = ("repair_plan", "apply")
    return any(_body(f).undo for f in _reach(c, entries)) \
        or _asks(c, entries, _UNDO)


def _own_policy(c):
    """A Kafka tail's topics (`topicstates.TopicSet`) ask the hop's
    per-table DDL policy (`tailtables.policy`) themselves: their changes
    are not a change log's DDL."""
    if not (_own(c, "tail_apply")
            and "TopicSet" in _reached(c, ("tail_apply",))[0]):
        return False
    from . import topicstates
    return "policy" in _body(topicstates.TopicSet.word).calls


def _ddl(*keywords):
    """The engine's change reader hands a DDL to the tail's policy
    (`canon.ddl`), saying `keywords` with it."""
    def probe(n, c):
        return any("ddl" in _body(fn).calls
                   and set(keywords) <= _body(fn).calls["ddl"]
                   for fn in _reach(c, ("neutral_changes",)))
    return probe


@functools.lru_cache(maxsize=None)
def _verifies(fn):
    """Whether a connection's posture (`tls._POSTURE`) can come out
    verified: its source, or a function of `tls` it calls, says so. One
    that is always plain, never checks the certificate or can only say
    "unknown" is not a way."""
    import textwrap

    from . import tls
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    except (OSError, TypeError, SyntaxError):
        return False
    said = [fn] + [getattr(tls, n.func.id) for n in ast.walk(tree)
                   if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                   and inspect.isfunction(getattr(tls, n.func.id, None))]
    return any('"verified"' in inspect.getsource(f) for f in said)


def _tls(n, c):
    """Its connections verified: a posture `tls` reads for the engine's
    family that can come out verified, or local."""
    from . import tls
    family = getattr(c, "ENGINE_FAMILY", "") or \
        getattr(c, "CANON_ENGINE", "") or n
    fn = tls._POSTURE.get(family)
    return fn is not None and _verifies(fn)


def _catalogued(n, c):
    """A managed service of this engine's own in the catalogue - not one
    of the engine whose wire it speaks."""
    from . import managed
    return any(s.managed and s.family == n
               for s in managed.services().values())


def _fenced(n, c):
    from . import fencing
    return getattr(c, "OWN_DIALECT", None) in fencing.SQL \
        and _own(c, "own_table")


def _drained(n, c):
    from . import drainmark
    return n in drainmark.MARKED or (_own(c, "log_position")
                                     and _own(c, "position_reached"))


def _jobs(n, c):
    from . import jobs
    return n in _compared(jobs._read, "kind")


def _states(n, c):
    """The tail the engine runs keeps each table in a state of its own
    (`tailtables.TableSet`; a Kafka tail's topics, `topicstates.TopicSet`):
    its own tail, where it hands its changes to the pair's, or - with no
    tail of its own - the pair's tail reading its change log
    (`cli._tail_pair`)."""
    if _own(c, "tail_apply"):
        return bool({"TableSet", "TopicSet"}
                    & _reached(c, ("tail_apply",))[0])
    return _own(c, "neutral_changes")


def _ops(n, c):
    """Its own tail holds `mapping.ops`; or its changes go through the
    pair's tail, which holds it, and the servers' own replication, which
    a hop carrying fewer changes is refused (`cli._own_refused`)."""
    if _own(c, "tail_apply"):
        return _every(("tail_apply",), _CARRIES)(n, c)
    return any(_own(c, m) for m in ("neutral_changes", "neutral_apply",
                                    "replicate_sql"))


#: items another table already reads - the capability table above, or the
#: decision engine's shapes (`decide.coverage`) - by that table's name for
#: them: one probe and one declaration each, never a second
LINKED = {
    "verify.delta": ("capability", "delta"),
    "verify.fence": ("capability", "fence"),
    "verify.confirm": ("capability", "confirm"),
    "verify.users": ("capability", "users"),
    "tail.follows": ("capability", "stream"),
    "move.table-copy": ("capability", "table-copy"),
    "move.bulk": ("capability", "bulk-move"),
    "move.large-values": ("shape", "large values"),
    "move.partitioned": ("shape", "partitioned"),
    "cutover.freeze": ("shape", "writes held"),
    "cutover.read-only-target": ("shape", "read-only target"),
    "cutover.counters": ("capability", "sequences"),
    "cutover.rollback": ("capability", "snapshot"),
}


def _linked(item):
    """(words, probe) of an item another table reads. A shape's words are
    the decision engine's (`depth_words`), which imports this module."""
    kind, name = LINKED[item]
    if kind == "capability":
        return CAPABILITIES[name], lambda n, c: implemented(n, name)

    def probe(n, c):
        from . import decide
        return decide.cell(n, name)[0] == "yes"
    return None, probe


def depth_words(item):
    """What an item is, in the operator's words."""
    words = DEPTH[item][0]
    if words is None:
        from . import decide
        words = "a way for " + decide.SHAPES[LINKED[item][1]]
    return words


def _items():
    from .engines.base import CATEGORIES
    own = lambda *ms: lambda n, c: all(_own(c, m) for m in ms)  # noqa: E731
    return {
        "scope.listings": (
            "the tables its counts, data and schema checks read are the"
            " hop's, asked of the one scope answer",
            _every(("check_counts", "check_data", "check_schema"), _SCOPE)),
        "scope.deep": ("its deep checks read only the tables the hop takes",
                       _every(("check_deep",), _SCOPE)),
        "scope.copy": ("the tables its copy lists are the hop's",
                       _every(("list_move_tables",), _SCOPE)),
        "scope.repair": ("its repairs act only on tables the hop takes",
                         _every(("repair_plan",), _SCOPE)),
        "scope.counters": ("its sequences and counters are of the tables"
                           " the hop takes",
                           _every(("check_autoinc",), _SCOPE)),
        "scope.objects": ("the objects beside its tables held to the hop's"
                          " `objects`",
                          _any(_CHECKS + ("repair_plan",), _OBJECTS)),
        "scope.ops": ("the changes it applies are only those the hop's"
                      " `mapping.ops` carries", _ops),
        "scope.row-filter": ("the hop's row filter read by its copy and its"
                             " check alike", _both(("check_counts",
                                                    "check_data"),
                                                   _ROW_FILTER)),
        "scope.transforms": ("a column written transformed is copied and"
                             " checked as written",
                             _both(("check_data",), _TRANSFORMS)),
        "scope.names": ("the hop's name rules read by its copy and its"
                        " check alike", lambda n, c: _both(
                            ("check_data",), _NAME_RULES)(n, c) or (
                            # the move hands a renamed table to the pair
                            # (`cli._copy_table_by`, `through_pair`)
                            not getattr(c, "OWN_PATHS_READ_COLUMN_MAPPING",
                                        True)
                            and _every(("check_data",), _NAME_RULES)(n, c))),
        **{f"verify.{cat}": (f"its checks find {cat}", _says(cat))
           for cat in sorted(set(CATEGORIES.values()))},
        **{item: _linked(item) for item in LINKED},
        "verify.repair": ("what its checks find is repaired",
                          own("repair_plan", "apply")),
        "verify.repair-undo": ("every repair undone exactly", _undone),
        "verify.rows-apart": ("a finding counts the rows apart, table by"
                              " table (`Result.rows_apart`)",
                              _fills("rows_apart")),
        "verify.tables": ("a finding says how each table it compared came"
                          " out (`Result.tables`)", _fills("tables")),
        "verify.table": ("a finding names its table as the source names it"
                         " (`Result.table`)", _fills("table")),
        "tail.table-states": ("its changes followed table by table, each in"
                              " a state of its own", _states),
        "tail.added": ("a table added to a running tail joins it at an"
                       " exact seam", lambda n, c: _states(n, c) and own(
                           "snapshot_mark", "mark_covers")(n, c)),
        "tail.readded": ("a table that left a running tail comes back with"
                         " only its changes since", lambda n, c:
                         _states(n, c) and bool(getattr(
                             c, "REREADS_FROM_A_POSITION", False))
                         and own("position_reached")(n, c)),
        "tail.ddl": ("a schema change met in its changes goes by the hop's"
                     " per-table DDL policy", lambda n, c: _states(n, c)
                     and (_ddl()(n, c) or _own_policy(c))),
        "tail.renames": ("a table renamed or swapped while followed is told"
                         " apart", lambda n, c: _states(n, c) and (
                             _own(c, "table_oids") or _ddl("to")(n, c))),
        "assess.versions": ("both servers' versions read and held to each"
                            " other", lambda n, c:
                            _own(c, "_server_versions") or (
                                _own(c, "assess") and _asks(
                                    c, ("assess",), _VERSIONS))),
        "assess.brands": ("which product answers on each side, and what it"
                          " cannot do", own("_brand_probes")),
        "assess.engine-checks": ("readiness checks of its own",
                                 lambda n, c: _own(c, "_assess_extra")
                                 or _own(c, "assess")),
        "assess.preflight": ("the deep checks that predict trouble run"
                             " before the move", lambda n, c: bool(
                                 getattr(c, "PREFLIGHT", ()))),
        "assess.capacity": ("the target's room measured before the move",
                            lambda n, c: _own(c, "capacity")
                            or _own(c, "free_bytes")),
        "assess.writers": ("every other writer on the source named",
                           own("stream_writers")),
        "assess.handwork": ("what the move will not do, counted and named",
                            _any(("assess", "_assess_extra"), _HANDWORK)),
        "schema.create": ("its tables made on the target from the source's",
                          own("neutral_create_sql")),
        "schema.indexes": ("its indexes read and made on the target",
                           own("neutral_indexes", "neutral_create_index_sql")),
        "schema.foreign-keys": ("its foreign keys read for the copy's order"
                                " and the target", own("foreign_keys")),
        "schema.defaults": ("its columns' defaults, nullability and identity"
                            " carried", own("neutral_column_rules")),
        "schema.views": ("its views read and carried", own("neutral_views")),
        "schema.routines": ("its stored code read, converted and proved",
                            own("code_routines")),
        "move.sized": ("its rows sized where they lie, the copy's memory set"
                       " from the widest", own("row_sizes")),
        "move.load-window": ("its indexes and triggers held off while a load"
                             " writes", own("load_window")),
        "move.create-missing": ("a table the target lacks made as the move"
                                " goes", own("create_missing")),
        "cutover.drain": ("the drain proved: everything written before the"
                          " freeze is on the target", _drained),
        "cutover.pause-replication": ("the servers' own replication into"
                                      " the target paused for a repair and"
                                      " let go", own("pause_writer")),
        "cutover.two-way": ("what migkit writes marked, so a way back does"
                            " not carry it twice", own("origin_mark")),
        "cutover.jobs": ("scheduled jobs in the database run on one side at"
                         " a time", _jobs),
        "security.tls": ("its connections verified: the server's"
                         " certificate checked, or nothing crossing a"
                         " network", _tls),
        "managed.catalogued": ("its managed services catalogued: their"
                               " limits, admin rights and changes",
                               _catalogued),
        "operations.fencing": ("a writer that lost its hold refused by the"
                               " target itself", _fenced),
    }


#: every item of the depth matrix: "area.item" -> (what it is, in the
#: operator's words, the probe that reads it off the code)
DEPTH = dict(sorted(_items().items(),
                    key=lambda kv: list(AREAS).index(kv[0].split(".")[0])))


def _each(state, why, engines):
    """{engine: (state, why)} for the engines a space-separated string
    names."""
    return {n: (state, why) for n in engines.split()}


def _as(capability, engines):
    """The engines' declarations of a capability this item stands or falls
    with: the one reason, said where it was said first (`GAPS`)."""
    return {n: GAPS[n][capability] for n in engines.split()}


# why an item does not apply, where it is the same for several engines
_PAIR = ("the pair joins two engines of their own, and each side's engine"
         " says this in its own row")
_NO_OWNER = ("its objects belong to no account and run with no account's"
             " privileges")
_COUNTER_GRANT = ("a counter is its column's - in MariaDB a sequence is a"
                  " table - granted with the table it is")
_NO_SEQUENCES = "it has no sequences to grant: a counter is its column's"
_NO_FK = ("it declares no foreign keys: nothing in one table refers to"
          " another")
_AT_ONCE = ("every constraint it has is checked as each row is written:"
            " none is deferred or left unvalidated")
_INFORMATIONAL = ("its constraints are informational and never enforced:"
                  " none is deferred or left unvalidated")
_NO_CONSTRAINT = "it declares no constraints to defer or validate"
_NO_EXTENSION = ("it installs no extensions into a database: what a server"
                 " offers is its version and settings, compared as such")
_EXTENSION_LOADED = ("its extensions are loaded by the process that opens"
                     " the file, not kept in it")
_NO_INDEX = "it keeps no secondary indexes"
_ATOMIC_INDEX = ("an index it fails to build leaves nothing behind (atomic"
                 " DDL), and one it marks corrupt has no catalogue column to"
                 " read: no index state is there to find")
_KEYED = "every record is kept by its key, so none lacks one"
_ADDRESSED = ("a message is addressed by its partition and offset, never by"
              " a key")
_NO_KEY = "a file declares no key"
_NO_MATVIEW = "it has no materialized views"
_NOTHING_BESIDE = ("it keeps nothing beside its data: no view, routine,"
                   " trigger or sequence")
_NO_PARTITIONS = ("it declares no partitions of a table: how its data is"
                  " spread is the store's own layout")
_INDEXED_VIEW = ("its only stored view result is an indexed view, which the"
                 " server keeps current inside every write that touches the"
                 " tables under it: there is no refresh to miss and none to"
                 " go stale, and a view without its index stores nothing")
_NO_RLS = "it keeps no row-level security policy in the database"
_SCHEMALESS = "it declares no schema of columns to compare structurally"
_ONE_DATABASE = ("a table is not sharded inside one database: a sharded"
                 " estate is read as one through its router")
_SPREAD_BY_STORE = ("how its data is spread is the store's own, with no"
                    " shard key of the table's")
_SPREAD_BY_PARTITIONS = (
    "a topic is spread over its partitions, which are compared as"
    " partitions: it has no shard key or router beyond them")
_COPIED_AS_BYTES = (
    "a message is bytes, copied as they are, and the text migkit writes"
    " itself is UTF-8: no character set is declared or converted that could"
    " change on the way")
_PLACED_BY_KEY = ("where a record lives is its key's hash, fixed by the"
                  " server")
_NO_TRIGGER = "it runs no triggers inside the database"
_HASHED_HERE = ("it hashes rows here, from the values its driver decodes,"
                " with one rendering for both sides: no server writes a"
                " value as text that could differ between builds")
_NO_DEFAULTS = ("it has no column default, NOT NULL or identity: every column"
                " outside the key may be null, and a key column is always"
                " set")
_NO_COLUMNS = "it declares no columns: a record's fields are its own"
_BYTES = "it compares text by its bytes, with no collation"
_NO_GENERATED = "it has no generated columns"
_UNTYPED = ("a value is bytes, with no declared type to narrow, round,"
            " render or shift in time")
_NO_NULL = ("a key holds a value or does not exist: there is no NULL to tell"
            " from empty")
_TYPED = ("every value of a column has the column's declared type, so no"
          " field's type drifts")
_NO_COLLECTION = "it has no collection whose options could differ"
_NOT_RENAMED = ("what holds its data cannot be renamed: a new name is a new"
                " one")
_NO_JOBS = "it runs no scheduled jobs inside the database"
_MADE_BY_WRITE = "a write makes what it writes into: nothing is made first"
_NOTHING_SLOWS = ("nothing on a table slows a load: no index or trigger to"
                  " hold off")
_NO_VIEW = "it has no views"
_NO_CODE = "it keeps no stored code"
_BYTE_STRINGS = ("a key and a value are bytes the server never decodes or"
                 " converts, so no character set can change on the way")
_KIND_COMPARED = ("a value is bytes under a key of one kind (string, hash,"
                  " list, set, sorted set, stream), and a key whose kind"
                  " changed is a changed key the data check finds: there are"
                  " no fields with types of their own to drift")
_JSON_IS_UTF8 = ("a document is JSON text, always UTF-8: no index or field"
                 " has a character set that could differ")
_AS_SENT = ("a document's `_source` comes back exactly as it was sent: the"
            " server renders none of a value, so none can come back"
            " rendered another way")
_ONE_PAUSE = ("migkit sets up no replication of the server's own into its"
              " target: the only writer beside a repair is migkit's own tail,"
              " paused one way for every engine")


#: every depth gap, by item: {engine: (NOT_APPLICABLE, why) or (NOT_YET,
#: backlog row)} - where the code has no way. An item another table reads
#: (`LINKED`) is declared there, never here. Not applicable only where the
#: thing the item is about does not exist on the engine; anything less sure
#: is not yet. The generic engine's gaps wait on P0-PARITY.small: deepened,
#: or declared a thin fallback
DEPTH_GAPS = {
    "scope.listings": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "sqlite parquet dynamodb db2 "
                "db2i db2zos teradata redshift "
                "snowflake bigquery duckdb copybook iceberg delta couchbase "
                "cosmosdb sapodp saas hetero"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "scope.deep": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "sqlite parquet dynamodb "
                "redshift snowflake bigquery duckdb copybook iceberg delta "
                "dsql couchbase cosmosdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "scope.copy": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "sqlite parquet dynamodb db2 db2i "
                "db2zos teradata redshift snowflake "
                "bigquery duckdb copybook iceberg delta couchbase cosmosdb "
                "sapodp saas hetero"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "scope.repair": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "parquet dynamodb db2 db2i db2zos ase "
                "teradata redshift snowflake bigquery "
                "duckdb copybook iceberg delta"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
    },
    "scope.counters": {
        **_as("sequences",
              "mongodb redis kafka parquet clickhouse dynamodb teradata "
              "opensearch cassandra copybook iceberg delta couchbase "
              "cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.scope",
                "sqlite db2 db2i db2zos ase redshift snowflake bigquery "
                "duckdb cockroachdb dsql hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "scope.objects": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "sqlite db2 db2i db2zos ase "
                "teradata redshift snowflake bigquery "
                "duckdb iceberg delta tidb cockroachdb dsql couchbase "
                "cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _NOTHING_BESIDE,
                "redis parquet dynamodb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "scope.ops": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "kafka db2 ase"),
        **_as("stream", "sqlite parquet db2zos teradata duckdb copybook"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "scope.row-filter": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "redis kafka sapodp saas"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "scope.transforms": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "postgres mysql mongodb mssql redis kafka sqlite parquet "
                "clickhouse dynamodb oracle db2 db2i db2zos ase teradata "
                "opensearch cassandra redshift snowflake bigquery duckdb "
                "copybook iceberg delta tidb cockroachdb dsql couchbase "
                "cosmosdb sapodp saas hetero"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "scope.names": {
        **_each(NOT_YET, "P0-PARITY.scope",
                "redis kafka sapodp saas"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.access.object-ownership": {
        **_each(NOT_APPLICABLE, _NO_OWNER,
                "mongodb redis kafka opensearch cassandra bigquery "
                "couchbase"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "db2 db2i db2zos ase teradata "
                "redshift snowflake tidb cockroachdb dsql hetero"),
        **_as("users",
              "sqlite parquet dynamodb duckdb copybook iceberg delta "
              "cosmosdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.access.sequence-grants": {
        **_each(NOT_APPLICABLE, _COUNTER_GRANT, "mysql tidb"),
        **_as("sequences",
              "mongodb redis kafka parquet clickhouse dynamodb teradata "
              "opensearch cassandra copybook iceberg delta couchbase "
              "cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "db2 db2i db2zos snowflake cockroachdb dsql "
                "hetero"),
        **_as("users", "sqlite duckdb"),
        **_each(NOT_APPLICABLE, _NO_SEQUENCES, "ase redshift bigquery"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.access.table-grants": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "db2 db2i "
                "db2zos ase teradata redshift "
                "snowflake bigquery tidb cockroachdb dsql couchbase hetero"),
        **_as("users",
              "sqlite parquet dynamodb duckdb copybook iceberg delta "
              "cosmosdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.identity.sequence-collision": {
        **_as("sequences",
              "mongodb redis kafka parquet clickhouse dynamodb teradata "
              "opensearch cassandra copybook iceberg delta couchbase "
              "cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase redshift snowflake bigquery "
                "duckdb tidb cockroachdb dsql hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.identity.sequence-parity": {
        **_as("sequences",
              "mongodb redis kafka parquet clickhouse dynamodb teradata "
              "opensearch cassandra copybook iceberg delta couchbase "
              "cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase redshift snowflake bigquery "
                "duckdb tidb cockroachdb dsql hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.movement.target-ahead": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet dynamodb db2 "
                "db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb copybook iceberg delta tidb "
                "cockroachdb dsql couchbase cosmosdb hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.collection-options": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "mssql sqlite dynamodb "
                "db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery duckdb iceberg delta tidb "
                "cockroachdb dsql couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _NO_COLLECTION, "redis parquet copybook"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.columns": {
        **_each(NOT_APPLICABLE, _NO_COLUMNS,
                "mongodb redis kafka dynamodb couchbase cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet db2 db2i db2zos ase "
                "teradata redshift snowflake bigquery "
                "duckdb copybook iceberg delta tidb cockroachdb dsql hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.deferrable": {
        **_each(NOT_APPLICABLE, _AT_ONCE,
                "mysql mssql clickhouse db2 db2i db2zos ase teradata duckdb "
                "delta tidb cockroachdb dsql"),
        **_each(NOT_APPLICABLE, _NO_CONSTRAINT,
                "mongodb redis kafka parquet dynamodb opensearch cassandra "
                "copybook iceberg couchbase cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify", "sqlite hetero"),
        **_each(NOT_APPLICABLE, _INFORMATIONAL, "redshift snowflake bigquery"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.extensions": {
        **_each(NOT_APPLICABLE, _NO_EXTENSION,
                "mysql mongodb mssql redis kafka parquet clickhouse "
                "dynamodb oracle db2 db2i db2zos ase teradata opensearch "
                "cassandra redshift snowflake bigquery copybook iceberg "
                "delta tidb cockroachdb dsql couchbase cosmosdb"),
        **_each(NOT_APPLICABLE, _EXTENSION_LOADED, "sqlite duckdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.verify", "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.foreign-keys": {
        **_each(NOT_APPLICABLE, _NO_FK,
                "mongodb redis kafka parquet clickhouse dynamodb opensearch "
                "cassandra copybook iceberg delta dsql couchbase cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb tidb cockroachdb hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.indexes": {
        **_each(NOT_APPLICABLE, _ATOMIC_INDEX, "mysql"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite dynamodb db2 db2i db2zos "
                "ase teradata snowflake bigquery duckdb "
                "cockroachdb dsql couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _NO_INDEX,
                "redis kafka parquet opensearch redshift copybook iceberg "
                "delta"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.keys": {
        **_each(NOT_APPLICABLE, _KEYED,
                "mongodb dynamodb opensearch cassandra couchbase cosmosdb"),
        **_each(NOT_APPLICABLE, _ADDRESSED, "kafka"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery duckdb copybook iceberg tidb "
                "cockroachdb dsql hetero"),
        **_each(NOT_APPLICABLE, _NO_KEY, "parquet delta"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.materialized-views": {
        **_each(NOT_APPLICABLE, _NO_MATVIEW,
                "mysql mongodb redis kafka sqlite parquet dynamodb "
                "opensearch duckdb copybook delta tidb dsql couchbase"),
        **_each(NOT_APPLICABLE, _INDEXED_VIEW, "mssql"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery iceberg cockroachdb "
                "cosmosdb hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.objects": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery iceberg "
                "delta tidb dsql couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _NOTHING_BESIDE,
                "redis parquet dynamodb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.partitions": {
        **_each(NOT_APPLICABLE, _NO_PARTITIONS,
                "mongodb redis sqlite dynamodb opensearch cassandra duckdb "
                "copybook couchbase cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "parquet db2 db2i db2zos ase "
                "teradata redshift snowflake bigquery iceberg delta "
                "cockroachdb dsql hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.row-security": {
        **_each(NOT_APPLICABLE, _NO_RLS,
                "mysql mongodb redis kafka sqlite parquet dynamodb "
                "cassandra duckdb copybook tidb dsql couchbase cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery iceberg delta "
                "cockroachdb hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.schema-diff": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "mssql sqlite parquet clickhouse oracle db2 db2i "
                "db2zos ase teradata opensearch redshift "
                "snowflake bigquery duckdb copybook iceberg delta tidb "
                "cockroachdb dsql hetero"),
        **_each(NOT_APPLICABLE, _SCHEMALESS,
                "mongodb redis kafka dynamodb couchbase cosmosdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.sharding": {
        **_each(NOT_APPLICABLE, _ONE_DATABASE,
                "postgres mysql mssql sqlite oracle db2 db2i db2zos ase "
                "duckdb"),
        **_each(NOT_APPLICABLE, _PLACED_BY_KEY,
                "redis dynamodb cassandra couchbase"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "teradata redshift cosmosdb "
                "hetero"),
        **_each(NOT_APPLICABLE, _SPREAD_BY_STORE,
                "parquet snowflake bigquery copybook iceberg delta tidb "
                "cockroachdb dsql"),
        **_each(NOT_APPLICABLE, _SPREAD_BY_PARTITIONS, "kafka"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.triggers": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase teradata "
                "cockroachdb couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _NO_TRIGGER,
                "mongodb redis kafka parquet clickhouse dynamodb opensearch "
                "redshift snowflake bigquery duckdb copybook iceberg delta "
                "tidb dsql"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.structure.unvalidated-constraints": {
        **_each(NOT_APPLICABLE, _NO_CONSTRAINT,
                "mongodb redis kafka parquet dynamodb opensearch cassandra "
                "copybook iceberg couchbase cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase teradata tidb "
                "cockroachdb hetero"),
        **_each(NOT_APPLICABLE, _AT_ONCE, "clickhouse duckdb delta dsql"),
        **_each(NOT_APPLICABLE, _INFORMATIONAL, "redshift snowflake bigquery"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.charset": {
        **_each(NOT_APPLICABLE, _COPIED_AS_BYTES, "kafka"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet "
                "dynamodb db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery duckdb copybook "
                "iceberg delta tidb cockroachdb dsql couchbase cosmosdb "
                "hetero"),
        **_each(NOT_APPLICABLE, _JSON_IS_UTF8, "opensearch"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_APPLICABLE, _BYTE_STRINGS, "redis"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.collation": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase "
                "teradata redshift snowflake bigquery duckdb "
                "delta tidb cockroachdb dsql hetero"),
        **_each(NOT_APPLICABLE, _BYTES,
                "clickhouse "
                "redis kafka parquet dynamodb cassandra copybook iceberg "
                "couchbase cosmosdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.generated": {
        **_each(NOT_APPLICABLE, _NO_GENERATED,
                "mongodb redis kafka parquet dynamodb cassandra copybook "
                "iceberg couchbase"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite db2 db2i db2zos ase "
                "teradata redshift snowflake bigquery duckdb "
                "delta tidb cockroachdb dsql cosmosdb hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.narrowing": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet dynamodb "
                "db2 db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb copybook iceberg delta tidb "
                "cockroachdb dsql couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _UNTYPED, "redis kafka"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.null-empty": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet dynamodb db2 "
                "db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb copybook iceberg delta tidb "
                "cockroachdb dsql couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _NO_NULL, "redis"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.precision": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet dynamodb "
                "db2 db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb copybook iceberg delta tidb "
                "cockroachdb dsql couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _UNTYPED, "redis kafka"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.rendering": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet dynamodb "
                "db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery duckdb copybook iceberg delta "
                "tidb cockroachdb dsql couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _UNTYPED, "redis kafka"),
        **_each(NOT_APPLICABLE, _AS_SENT, "opensearch"),
        **_each(NOT_APPLICABLE, _HASHED_HERE, "cassandra"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.timezone": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet dynamodb "
                "db2 db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb copybook iceberg delta tidb "
                "cockroachdb dsql couchbase cosmosdb hetero"),
        **_each(NOT_APPLICABLE, _UNTYPED, "redis kafka"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.value.type-drift": {
        **_each(NOT_APPLICABLE, _TYPED,
                "postgres mysql mssql parquet clickhouse oracle db2 db2i "
                "db2zos ase teradata cassandra redshift snowflake bigquery "
                "duckdb copybook iceberg delta tidb cockroachdb dsql"),
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite dynamodb couchbase cosmosdb "
                "hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_APPLICABLE, _KIND_COMPARED, "redis"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.repair": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "parquet dynamodb db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery duckdb "
                "copybook iceberg delta"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
    },
    "verify.repair-undo": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "parquet dynamodb db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery duckdb "
                "copybook iceberg delta"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
    },
    "verify.rows-apart": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "parquet dynamodb db2 "
                "db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb copybook iceberg delta tidb "
                "cockroachdb dsql couchbase cosmosdb sapodp saas hetero"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.tables": {
        **_each(NOT_YET, "P0-PARITY.verify",
                "sqlite parquet "
                "dynamodb db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery duckdb copybook "
                "iceberg delta tidb cockroachdb dsql couchbase cosmosdb "
                "sapodp saas hetero"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "verify.table": {
        **_each(NOT_YET, "P0-PARITY.verify", ""),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "tail.table-states": {
        **_each(NOT_YET, "P0-PARITY.tail",
                "mongodb redis clickhouse db2 ase opensearch "
                "cassandra redshift snowflake bigquery iceberg delta"),
        **_as("stream", "sqlite parquet db2zos teradata duckdb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "tail.added": {
        **_each(NOT_YET, "P0-PARITY.tail",
                "mongodb redis clickhouse dynamodb db2 db2i "
                "ase opensearch cassandra redshift snowflake bigquery "
                "iceberg delta dsql couchbase cosmosdb sapodp saas"),
        **_as("stream", "sqlite parquet db2zos teradata duckdb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "tail.readded": {
        **_each(NOT_YET, "P0-PARITY.tail",
                "postgres mongodb redis clickhouse dynamodb "
                "db2 db2i ase opensearch cassandra redshift snowflake "
                "bigquery iceberg delta cockroachdb dsql couchbase cosmosdb "
                "sapodp saas"),
        **_as("stream", "sqlite parquet db2zos teradata duckdb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "tail.ddl": {
        **_each(NOT_YET, "P0-PARITY.tail",
                "mongodb mssql redis clickhouse dynamodb db2 "
                "db2i ase opensearch cassandra redshift snowflake bigquery "
                "iceberg delta tidb cockroachdb dsql couchbase cosmosdb "
                "sapodp saas"),
        **_as("stream", "sqlite parquet db2zos teradata duckdb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "tail.renames": {
        **_each(NOT_YET, "P0-PARITY.tail",
                "mongodb redis clickhouse db2 db2i ase "
                "opensearch redshift snowflake bigquery iceberg delta tidb "
                "cockroachdb dsql sapodp saas"),
        **_each(NOT_APPLICABLE, _NOT_RENAMED,
                "kafka dynamodb cassandra couchbase cosmosdb"),
        **_as("stream", "sqlite parquet db2zos teradata duckdb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "assess.versions": {
        **_each(NOT_YET, "P0-PARITY.all",
                "dynamodb db2 db2i db2zos ase "
                "teradata redshift snowflake bigquery duckdb tidb "
                "cockroachdb dsql couchbase cosmosdb sapodp saas"),
        **_as("params", "parquet copybook iceberg delta"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "assess.brands": {
        **_each(NOT_YET, "P0-PARITY.all",
                "sqlite dynamodb db2 db2i db2zos "
                "ase teradata redshift snowflake bigquery duckdb "
                "cockroachdb dsql couchbase cosmosdb sapodp saas"),
        **_as("params", "parquet copybook iceberg delta"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "assess.engine-checks": {
        **_each(NOT_YET, "P0-PARITY.all",
                "parquet dynamodb db2 ase "
                "kinesis pubsub duckdb copybook iceberg delta"),
    },
    "assess.preflight": {
        **_each(NOT_YET, "P0-PARITY.all",
                "mongodb sqlite parquet "
                "dynamodb db2 db2i db2zos ase teradata "
                "redshift snowflake bigquery duckdb copybook "
                "iceberg delta cockroachdb dsql couchbase cosmosdb "
                "hetero"),
        **_as("schema", "kinesis pubsub"),
        **_as("deep", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "assess.capacity": {
        **_each(NOT_YET, "P0-PARITY.all",
                "sqlite parquet dynamodb db2 db2i "
                "db2zos ase teradata redshift "
                "snowflake bigquery kinesis pubsub duckdb copybook iceberg "
                "delta cockroachdb dsql couchbase cosmosdb"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "assess.writers": {
        **_each(NOT_YET, "P0-PARITY.all",
                "mongodb sqlite parquet dynamodb "
                "db2 db2i db2zos ase teradata cassandra "
                "redshift snowflake bigquery duckdb copybook iceberg delta "
                "tidb cockroachdb dsql couchbase cosmosdb sapodp saas "
                "hetero"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "assess.handwork": {
        **_each(NOT_YET, "P0-PARITY.all",
                "sqlite dynamodb db2 "
                "db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb iceberg delta tidb cockroachdb "
                "dsql couchbase cosmosdb sapodp saas hetero"),
        **_each(NOT_APPLICABLE, _NOTHING_BESIDE, "parquet copybook"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "schema.create": {
        **_each(NOT_APPLICABLE, _MADE_BY_WRITE, "mongodb redis"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "schema.indexes": {
        **_each(NOT_YET, "P0-PARITY.all",
                "mongodb dynamodb db2 db2i db2zos "
                "ase snowflake bigquery duckdb couchbase cosmosdb"),
        **_each(NOT_APPLICABLE, _NO_INDEX,
                "redis kafka parquet opensearch redshift copybook iceberg "
                "delta"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "schema.foreign-keys": {
        **_each(NOT_APPLICABLE, _NO_FK,
                "mongodb redis kafka parquet clickhouse dynamodb opensearch "
                "cassandra copybook iceberg delta dsql couchbase cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.all",
                "sqlite db2 db2i db2zos ase teradata redshift snowflake "
                "bigquery duckdb cockroachdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "schema.defaults": {
        **_each(NOT_APPLICABLE, _NO_COLUMNS,
                "mongodb redis kafka dynamodb couchbase cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.all",
                "parquet db2 db2i db2zos ase "
                "redshift snowflake bigquery duckdb "
                "copybook iceberg delta"),
        **_each(NOT_APPLICABLE, _NO_DEFAULTS, "cassandra"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "schema.views": {
        **_each(NOT_YET, "P0-PARITY.all",
                "mongodb sqlite db2 db2i db2zos ase teradata "
                "opensearch redshift snowflake bigquery duckdb iceberg "
                "cockroachdb dsql couchbase"),
        **_each(NOT_APPLICABLE, _NO_VIEW,
                "redis kafka parquet dynamodb cassandra copybook delta "
                "cosmosdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "schema.routines": {
        **_each(NOT_YET, "P0-PARITY.all",
                "mongodb redis clickhouse db2 db2i db2zos ase teradata "
                "opensearch cassandra redshift snowflake bigquery duckdb "
                "cockroachdb dsql couchbase cosmosdb"),
        **_each(NOT_APPLICABLE, _NO_CODE,
                "kafka sqlite parquet dynamodb copybook iceberg delta"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "move.sized": {
        **_each(NOT_YET, "P0-PARITY.all",
                "parquet dynamodb copybook iceberg "
                "delta couchbase cosmosdb sapodp saas"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "move.load-window": {
        **_each(NOT_YET, "P0-PARITY.all",
                "mongodb sqlite dynamodb db2 db2i db2zos "
                "ase teradata duckdb cockroachdb dsql "
                "couchbase cosmosdb"),
        **_each(NOT_APPLICABLE, _NOTHING_SLOWS,
                "redis kafka parquet redshift snowflake bigquery copybook "
                "iceberg delta"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "move.create-missing": {
        **_each(NOT_APPLICABLE, _MADE_BY_WRITE, "mongodb redis"),
        **_each(NOT_YET, "P0-PARITY.all",
                "sqlite parquet dynamodb db2 "
                "db2i db2zos ase teradata redshift "
                "snowflake bigquery duckdb copybook iceberg delta "
                "cockroachdb dsql couchbase cosmosdb"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "cutover.drain": {
        **_each(NOT_YET, "P0-PARITY.all",
                "db2 ase redshift snowflake bigquery iceberg "
                "delta hetero"),
        **_as("stream", "sqlite parquet db2zos teradata duckdb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "cutover.pause-replication": {
        **_each(NOT_APPLICABLE, _ONE_PAUSE,
                "mongodb mssql kafka clickhouse dynamodb oracle db2 db2i "
                "ase opensearch cassandra redshift snowflake bigquery "
                "iceberg delta tidb cockroachdb dsql couchbase cosmosdb "
                "sapodp saas hetero"),
        **_each(NOT_YET, "P0-PARITY.all", "redis"),
        **_as("stream", "sqlite parquet db2zos teradata duckdb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "cutover.two-way": {
        **_each(NOT_YET, "P0-PARITY.all",
                "mongodb mssql redis kafka clickhouse dynamodb oracle db2 "
                "db2i ase opensearch cassandra redshift snowflake bigquery "
                "iceberg delta tidb cockroachdb dsql couchbase cosmosdb"),
        **_as("stream", "sqlite parquet db2zos duckdb copybook"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "cutover.jobs": {
        **_each(NOT_APPLICABLE, _NO_JOBS,
                "mongodb redis kafka sqlite parquet dynamodb cassandra "
                "duckdb copybook iceberg delta tidb dsql cosmosdb"),
        **_each(NOT_YET, "P0-PARITY.all",
                "db2 db2i db2zos ase teradata "
                "opensearch redshift snowflake bigquery cockroachdb "
                "couchbase"),
        **_as("schema", "kinesis pubsub"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "security.tls": {
        **_each(NOT_YET, "P0-PARITY.all", "mssql db2 db2zos ase saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "managed.catalogued": {
        **_each(NOT_YET, "P0-PARITY.all",
                "sqlite parquet db2i db2zos ase teradata snowflake duckdb "
                "iceberg delta tidb couchbase sapodp saas"),
        **_as("params", "copybook"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
    "operations.fencing": {
        **_each(NOT_YET, "P0-PARITY.all",
                "mongodb mssql redis kafka sqlite parquet clickhouse "
                "dynamodb oracle db2 db2i db2zos ase teradata opensearch "
                "cassandra redshift snowflake bigquery kinesis pubsub "
                "duckdb copybook iceberg delta cockroachdb dsql couchbase "
                "cosmosdb"),
        **_as("table-copy", "sapodp saas"),
        **_each(NOT_APPLICABLE, _PAIR, "hetero"),
        **_each(NOT_YET, "P0-PARITY.small", "generic"),
    },
}

#: the ways the code has whose test on that engine is not written yet, by
#: item: the engines, each waiting on its area's backlog row (`AREAS`).
#: Such a cell is "not yet": a way is one only once a test proves it
UNPROVED = {
    "scope.listings": "ase",
    "scope.deep": "db2i db2zos ase hetero",
    "scope.copy": "ase",
    "scope.repair": "tidb cockroachdb dsql couchbase cosmosdb",
    "scope.ops": "dynamodb db2i redshift snowflake bigquery kinesis pubsub iceberg delta tidb cockroachdb dsql couchbase cosmosdb sapodp saas",
    "scope.row-filter": "parquet dynamodb db2 db2i db2zos ase teradata cassandra snowflake copybook iceberg delta tidb couchbase cosmosdb mssql",
    "scope.names": "sqlite parquet dynamodb db2 db2i db2zos ase teradata redshift snowflake bigquery duckdb copybook iceberg delta tidb cockroachdb dsql couchbase cosmosdb mssql",
    "verify.value.type-drift": "kafka",
    # `rolesInfo` (the privileges of the roles made in a database) and a
    # `$regex` count of U+FFFD have not been run against a server
    "verify.access.table-grants": "mongodb clickhouse mssql oracle",
    "verify.value.charset": "mongodb clickhouse mssql",
    "verify.delta": "db2i tidb cockroachdb dsql couchbase cosmosdb",
    "verify.fence": "db2i",
    "verify.confirm": "clickhouse dynamodb db2i tidb cockroachdb dsql couchbase cosmosdb",
    # the catalogue reads of these are written and have not run against a
    # server (`_deep_triggers`, `_deep_render`, `_deep_table_options`)
    "verify.structure.triggers": "mysql oracle cassandra",
    "verify.structure.collection-options": "mysql clickhouse oracle cassandra",
    "verify.value.rendering": "mysql clickhouse mssql oracle",
    "verify.repair": "tidb cockroachdb dsql couchbase cosmosdb clickhouse opensearch cassandra",
    "verify.repair-undo": "tidb cockroachdb dsql couchbase cosmosdb clickhouse opensearch cassandra",
    "verify.table": "parquet dynamodb db2 db2i db2zos ase teradata redshift snowflake bigquery copybook iceberg delta tidb cockroachdb dsql couchbase cosmosdb sapodp saas hetero",
    "tail.follows": "db2i",
    "tail.table-states": ("mssql dynamodb oracle db2i tidb dsql couchbase "
                          "cosmosdb sapodp saas hetero"),
    "assess.versions": "mongodb clickhouse mssql oracle cassandra",
    "verify.structure.partitions": "mssql oracle tidb clickhouse",
    "move.partitioned": "mssql oracle tidb",
    "assess.engine-checks": "db2i snowflake sapodp clickhouse opensearch mssql cassandra",
    "schema.create": "db2i db2zos kafka",
    "assess.capacity": "kafka opensearch mssql oracle cassandra",
    "assess.writers": "kafka clickhouse opensearch mssql oracle",
    "move.create-missing": "kafka clickhouse mssql cassandra",
    "schema.indexes": "sqlite teradata clickhouse mssql oracle cassandra",
    "schema.defaults": "teradata sapodp clickhouse mssql oracle",
    # TiDB runs MySQL's `assess`, which now asks the pre-flight
    "assess.preflight": "tidb clickhouse opensearch mssql oracle cassandra",
    "schema.routines": "tidb",
    "move.table-copy": "db2i db2zos snowflake bigquery dsql",
    "move.bulk": "hetero",
    "move.large-values": "db2i db2zos snowflake bigquery",
    "move.sized": "oracle db2i db2zos teradata redshift snowflake tidb cockroachdb dsql cassandra",
    "move.load-window": "tidb clickhouse mssql cassandra",
    "cutover.rollback": "mongodb copybook",
    "cutover.freeze": ("oracle db2 ase teradata redshift snowflake couchbase"
                       " hetero"),
    "cutover.read-only-target": "db2i db2zos dsql hetero",
    "cutover.drain": "db2i saas",
    "operations.fencing": "tidb",
    "scope.objects": "clickhouse opensearch cassandra",
    "verify.access.object-ownership": "clickhouse mssql oracle",
    "verify.structure.columns": "clickhouse oracle",
    "verify.structure.keys": "clickhouse oracle",
    "verify.structure.materialized-views": "clickhouse oracle cassandra",
    "verify.structure.objects": "clickhouse opensearch cassandra",
    "verify.structure.row-security": "clickhouse mssql oracle",
    "verify.structure.sharding": "clickhouse opensearch",
    "verify.value.generated": "clickhouse mssql oracle",
    "verify.value.narrowing": "clickhouse mssql oracle",
    "verify.value.null-empty": "clickhouse mssql oracle",
    "verify.value.precision": "clickhouse mssql oracle",
    "verify.value.timezone": "clickhouse opensearch mssql oracle",
    "assess.handwork": "clickhouse opensearch mssql oracle cassandra",
    "cutover.jobs": "clickhouse mssql oracle",
    "schema.views": "clickhouse",
    "verify.movement.target-ahead": "clickhouse opensearch oracle",
    "verify.structure.indexes": "clickhouse mssql oracle cassandra",
    "assess.brands": "clickhouse mssql oracle cassandra",
    "tail.renames": "mssql oracle",
    "verify.value.collation": "mssql oracle",
    "verify.structure.unvalidated-constraints": "mssql",
    "verify.access.sequence-grants": "mssql oracle",
    "security.tls": "oracle",
    "verify.structure.foreign-keys": "oracle",
    "verify.structure.deferrable": "oracle",
    "tail.ddl": "oracle",
    "verify.structure.schema-diff": "cassandra",
}


@functools.lru_cache(maxsize=None)
def proofs(tests=None):
    """{(item, engine): [test module]} from every test module's `PROVES`
    (under `tests`, the repo's own by default), read from its source,
    never imported."""
    from pathlib import Path
    out = {}
    tests = Path(tests) if tests else \
        Path(__file__).resolve().parent.parent / "tests"
    for path in sorted(tests.glob("test_*.py")) if tests.is_dir() else ():
        text = path.read_text()
        if REGISTRY not in text:
            continue
        for node in ast.parse(text).body:
            if not (isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == REGISTRY
                    for t in node.targets)):
                continue
            for item, engines in ast.literal_eval(node.value):
                for eng in [engines] if isinstance(engines, str) \
                        else engines:
                    out.setdefault((item, eng), []).append(path.name)
    return out


def depth_has(engine, item):
    """Whether the code has the way, read off it."""
    return bool(DEPTH[item][1](engine, _class_for(engine)))


def _gap(engine, item):
    """The gap declared where the code has no way, or None."""
    if item not in LINKED:
        return DEPTH_GAPS.get(item, {}).get(engine)
    kind, name = LINKED[item]
    if kind == "capability":
        return GAPS.get(engine, {}).get(name)
    from . import decide
    got = decide.cell(engine, name)
    return got if got[0] in (NOT_YET, NOT_APPLICABLE) else None


def _unproved(engine, item):
    """The backlog row a way's missing test waits on, or None."""
    if engine in UNPROVED.get(item, "").split():
        return AREAS[item.split(".", 1)[0]][1]
    return None


def depth_cell(engine, item):
    """(state, why): "yes" where the code has the way and a test proves it
    on this engine; the gap declared - a way whose test is not written yet
    (`UNPROVED`) is not yet one; or what the test fails on: "untested" (a
    way no test proves and nobody declared), "undeclared" (no way, no
    declaration), "stale" (a declaration the code or a test has since
    closed), "claimed" (a proof of a way the code does not show)."""
    has = depth_has(engine, item)
    tested = (item, engine) in proofs()
    gap, waits = _gap(engine, item), _unproved(engine, item)
    if has:
        if gap is not None:
            return ("stale", f"declared {gap[0]}, and the code has it")
        if tested:
            return ("stale", "declared unproved, and a test proves it") \
                if waits else ("yes", "")
        return (NOT_YET, waits) if waits else ("untested", "")
    if waits:
        return ("stale", "declared unproved, and the code has no way")
    if tested:
        return ("claimed", ", ".join(proofs()[(item, engine)]))
    return gap if gap is not None else ("undeclared", "")


def depth():
    """{engine: {item: state}}: the depth matrix."""
    return {n: {i: depth_cell(n, i)[0] for i in DEPTH} for n in NAMES}


def backlog_rows():
    """The ids of `docs/backlog-index.md`'s rows."""
    from pathlib import Path
    index = Path(__file__).resolve().parent.parent / "docs" / \
        "backlog-index.md"
    rows = set()
    for line in index.read_text().splitlines() if index.exists() else ():
        cells = [c.strip() for c in line.split("|")]
        if len(cells) > 3 and cells[1].isdigit():
            rows.add(cells[2])
    return rows


def depth_failing():
    """{what: [(engine, item, detail)]} the depth matrix's test fails on:
    the four states above, a "not yet" whose backlog row is not in the
    index, and a declaration or a proof of an item or engine nobody
    knows."""
    out = {"undeclared": [], "untested": [], "stale": [], "claimed": [],
           "unrowed": [], "unknown": []}
    rows = backlog_rows()
    for n in NAMES:
        for i in DEPTH:
            state, why = depth_cell(n, i)
            if state in out:
                out[state].append((n, i, why))
            elif state == NOT_YET and why not in rows:
                out["unrowed"].append((n, i, why))
    for item, gaps in DEPTH_GAPS.items():
        out["unknown"] += [(n, item, "declared") for n in gaps
                           if item not in DEPTH or item in LINKED
                           or n not in NAMES]
    for item, engines in UNPROVED.items():
        out["unknown"] += [(n, item, "unproved") for n in engines.split()
                           if item not in DEPTH or n not in NAMES]
    out["unknown"] += [(eng, item, "proved") for item, eng in proofs()
                       if item not in DEPTH or eng not in NAMES]
    return out
