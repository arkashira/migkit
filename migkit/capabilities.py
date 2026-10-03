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

_GIVEN_KEYS = ("a {} is kept by the key the application gives it, not one"
               " a sequence hands out: there is no sequence to carry")
_IAM = ("who may read {} is the cloud's access policies', outside the"
        " database")

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
    # the cloud-native services (P0-CLOUDS part 2): their own bulk paths
    # (CN1), accounts (CN2) and the follows not built (CN4)
    "spanner": {
        "bulk-move": (NOT_YET, "CN1"),
        "users": (NOT_YET, "CN2"),
    },
    "bigtable": {
        "sequences": (NOT_APPLICABLE, _GIVEN_KEYS.format("row")),
        "bulk-move": (NOT_YET, "CN1"),
        "users": (NOT_APPLICABLE, _IAM.format("a table")),
        "statistics": (NOT_APPLICABLE, "a Bigtable table is read by its row"
                                       " keys and keeps no optimiser"
                                       " statistics"),
    },
    "firestore": {
        "sequences": (NOT_APPLICABLE, _GIVEN_KEYS.format("document")),
        "bulk-move": (NOT_YET, "CN1"),
        "users": (NOT_APPLICABLE, "who may read a database is IAM's and its"
                                  " security rules', not the collection's"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "datastore": {
        "sequences": (NOT_APPLICABLE, "an entity's numeric id is allocated by"
                                      " the service and carried as it is:"
                                      " there is no sequence to carry"),
        "bulk-move": (NOT_YET, "CN1"),
        **{cap: (NOT_APPLICABLE, "Datastore mode has no change feed and no"
                                 " listener: nothing a client can follow")
           for cap in ("stream", "fence", "confirm", "delta")},
        "users": (NOT_APPLICABLE, _IAM.format("a kind")),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "gaussdb": {
        "bulk-move": (NOT_YET, "CN1"),
        "users": (NOT_YET, "CN2"),
    },
    **{name: {
        "bulk-move": (NOT_YET, "CN1"),
        "users": (NOT_YET, "CN2"),
    } for name in ("oceanbase", "polardbx", "dcdb")},
    "oceanbase-oracle": {
        "bulk-move": (NOT_YET, "CN1"),
        **{cap: (NOT_YET, "CN4")
           for cap in ("stream", "fence", "confirm", "delta")},
        "users": (NOT_YET, "CN2"),
    },
    "keyspaces": {
        "sequences": (NOT_APPLICABLE, "Cassandra has no sequences or"
                                      " auto-increment to carry"),
        "users": (NOT_APPLICABLE, _IAM.format("a table")),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "neptune": {
        "sequences": (NOT_APPLICABLE, _GIVEN_KEYS.format("vertex or edge")),
        "bulk-move": (NOT_YET, "CN1"),
        "users": (NOT_APPLICABLE, _IAM.format("the graph")),
    },
    "timestream": {
        "deep": (NOT_APPLICABLE, "a record is its dimensions, time and"
                                 " measures, which the data check compares"
                                 " whole; there are no indexes or"
                                 " constraints to hold"),
        "sequences": (NOT_APPLICABLE, "a record is kept by its dimensions"
                                      " and its time: there is no sequence"
                                      " to carry"),
        "bulk-move": (NOT_YET, "CN1"),
        **{cap: (NOT_APPLICABLE, "Timestream keeps no change log, and a"
                                 " record written late or at a higher"
                                 " version lands in the past, where no"
                                 " watermark finds it")
           for cap in ("stream", "fence", "confirm", "delta")},
        "users": (NOT_APPLICABLE, _IAM.format("a table")),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "lindorm": {
        "sequences": (NOT_APPLICABLE, _GIVEN_KEYS.format("row")),
        "bulk-move": (NOT_YET, "CN1"),
        **{cap: (NOT_YET, "CN4")
           for cap in ("stream", "fence", "confirm", "delta")},
        "users": (NOT_YET, "CN2"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "geminidb-cassandra": {
        "sequences": (NOT_APPLICABLE, "Cassandra has no sequences or"
                                      " auto-increment to carry"),
        **{cap: (NOT_APPLICABLE, "GeminiDB's Cassandra API keeps no change"
                                 " log a client can read")
           for cap in ("stream", "fence", "confirm", "delta")},
        "users": (NOT_YET, "CN2"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "geminidb-redis": {
        "sequences": (NOT_APPLICABLE, "keys have no sequences"),
        "bulk-move": (NOT_YET, "CN1"),
        **{cap: (NOT_APPLICABLE, "a GeminiDB instance neither is nor serves"
                                 " a replica, and keeps no change stream a"
                                 " client can read")
           for cap in ("stream", "fence", "confirm")},
        "users": (NOT_YET, "CN2"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
    },
    "geminidb-mongo": {
        "sequences": (NOT_APPLICABLE,
                      "collections have no sequences or auto-increment"),
        "bulk-move": (NOT_YET, "CN1"),
        "users": (NOT_YET, "CN2"),
        "statistics": (NOT_APPLICABLE, _NO_PLANNER),
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
