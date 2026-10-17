"""How deeply the changes of a table can be read from its source: the
scale `assess` prints per table, with the statement that would raise it
(backlog LT.R1).

D4 whole before and after images, D3 the key and the changed columns,
D2 which rows changed (values read again), D1 which tables changed (a
ranged digest diff of those), D0 nothing (a digest diff of everything,
exact only with writes stopped). Said from the settings the source
reports and each table's key; what a deeper depth would cost is the
statement's own words. Reading a setting is not choosing a method: the
method is `decide`'s, and so is the level a depth earns: from the goal
(`decide.change_depth_level`), given here by `goal` or by the readiness
check around the engine's own assessment (`for_goal`).
"""

import contextlib
import contextvars

#: (goal, follows) of the readiness check asking: what `lines` reads where
#: its caller - an engine's own assessment - does not say the goal
_GOAL = contextvars.ContextVar("changedepth_goal", default=(None, None))

SCALE = {
    "D4": "every change with its before and after image",
    "D3": "every change with its key and the columns it changed",
    "D2": "which rows changed, their values read again",
    "D1": "which tables changed, then a digest diff of those",
    "D0": "no change reading; a digest diff of everything",
}


def mysql(log_bin, binlog_format, row_image, row_metadata, has_key):
    """(depth, the statement that raises it or "") of one MySQL table."""
    if str(log_bin).upper() not in ("ON", "1"):
        return "D0", "start the server with log_bin on (a restart)"
    if str(binlog_format).upper() != "ROW":
        return "D1", "set global binlog_format = 'ROW'"
    if str(row_metadata).upper() != "FULL":
        return "D0", "set global binlog_row_metadata = 'FULL'"
    if not has_key:
        return "D1", "give the table a primary key"
    if str(row_image).upper() in ("MINIMAL", "NOBLOB"):
        return "D3", "set global binlog_row_image = 'FULL'"
    return "D4", ""


_LOGICAL = "alter system set wal_level = 'logical' (a restart)"


def postgres(wal_level, replica_identity, has_key, way=None):
    """The same for one PostgreSQL table; `replica_identity` is its
    `relreplident` letter (d default, n nothing, f full, i an index), and
    `way` how its changes are followed where the WAL carries no rows
    (`PostgresEngine.FOLLOW_WAYS`): its physical records name the blocks
    a change touched, read again by place and told apart by key - which
    rows changed, their values read again (D2), and a table with no key
    only as one that changed (D1)."""
    level = str(wal_level).lower()
    if level == "minimal":
        return "D0", "alter system set wal_level = 'replica' (a restart)"
    if level != "logical":
        if way == "trigger capture":
            return ("D3", _LOGICAL) if has_key else (
                "D1", "give the table a primary key")
        if way in ("wal inspect", "wal dump"):
            return ("D2", _LOGICAL) if has_key else (
                "D1", "give the table a primary key")
        return "D0", _LOGICAL
    ident = str(replica_identity).lower()
    if ident == "f":
        return "D4", ""
    if ident == "n" or (ident == "d" and not has_key):
        return "D1", ("give the table a primary key, or alter table ..."
                      " replica identity full")
    return "D3", "alter table ... replica identity full"


_PRE_IMAGES = ("db.runCommand({collMod: ..., changeStreamPreAndPostImages:"
               " {enabled: true}})")


def mongodb(replica_set, pre_images, way=None, why=""):
    """The same for one MongoDB collection: `replica_set` whether the
    server keeps an oplog (a replica set, or a sharded cluster's router),
    `pre_images` whether the collection keeps its documents' images
    before a change, `way` how its changes are followed
    (`MongoEngine.FOLLOW_WAYS`, the change stream where not said) and
    `why` the way above it was passed over. A change stream gives every
    change with its images where they are kept (D4), else its key and
    fields (D3); the oplog an entry's key and the fields it set and
    unset, never the image before (D3); a sweep, comparing the
    collections again and again, which of them changed (D1)."""
    if not replica_set:
        return "D1", "convert to a single-node replica set"
    if way == "bucket sweep":
        return "D1", "let the source's oplog be read" + (
            f" ({why})" if why else "")
    if way == "oplog":
        return "D3", "let the source open a change stream" + (
            f" ({why})" if why else "") + (
            "" if pre_images else f", then {_PRE_IMAGES}")
    if pre_images:
        return "D4", ""
    return "D3", _PRE_IMAGES


@contextlib.contextmanager
def for_goal(goal, follows):
    """The goal (`goal` its word, `follows` whether the step follows the
    source's changes) every `lines` below reads where its caller does not
    say one."""
    token = _GOAL.set((goal, follows))
    try:
        yield
    finally:
        _GOAL.reset(token)


def lines(table_depths, shown=5, goal=None, follows=None):
    """Assess rows, one per depth reached, from {table: (depth, raise)}:
    (level, item, detail) - the tables named up to `shown`, and the one
    statement that raises each depth. The level is `decide`'s, from the
    depth and the goal (`goal`, `follows`; the readiness check's where not
    given - `for_goal`): a depth the goal does not need is a note, its
    statement kept for a later goal that follows changes."""
    from . import decide
    if goal is None and follows is None:
        goal, follows = _GOAL.get()
    by = {}
    for table, (depth, fix) in sorted(table_depths.items()):
        by.setdefault((depth, fix), []).append(table)
    out = []
    for (depth, fix), names in sorted(by.items(), reverse=True):
        more = f" and {len(names) - shown} more" if len(names) > shown else ""
        got = decide.change_depth_level(
            {"depth": depth, "goal": goal, "follows": follows})
        unread = got.path == "unread"
        out.append((
            decide.CHANGE_DEPTH_LEVELS[got.path],
            f"change reading {depth} ({SCALE[depth]}):"
            f" {len(names)} table{'s' if len(names) != 1 else ''}",
            ", ".join(names[:shown]) + more
            + (f" - a note: goal {goal} reads no changes, so its check"
               " compares every row once" if unread else "")
            + (f" - to follow changes later: {fix}" if fix and unread
               else f" - to raise it: {fix}" if fix else "")))
    return out


_REPLICATE = ("ACL SETUSER <account> +psync +replconf +ping (let the"
              " account replicate)")


def redis(way, why=""):
    """(depth, the statement that raises it or "") of one Redis database,
    from the live way `decide.redis_live` chose (`way`, its rung's name or
    None) and why the replica stream was passed over (`why`). The replica
    stream carries every write in the source's order with its key and
    what it wrote, never the value it replaced (D3); the idle-time sweep
    finds the keys touched since its last round and reads their values
    again (D2); with neither, only a digest diff of everything (D0)."""
    if way == "replica stream":
        return "D3", ""
    if way == "idle-time sweep":
        return "D2", _REPLICATE if "replicate" in str(why) else ""
    return "D0", _REPLICATE if "replicate" in str(why) else ""


def kafka(topic, cleanup_policy):
    """The same for one topic, from its `cleanup.policy` (None where the
    broker would not say: no depth is guessed). A topic's records are its
    changes, each whole, read in its partitions' order and nothing read
    again (D4) - while the topic deletes by time only; one compacted by
    key keeps the latest record of each key, the records between it
    removed (D3)."""
    if cleanup_policy is None:
        return None
    if "compact" in str(cleanup_policy).lower():
        return "D3", (f"set cleanup.policy=delete on topic {topic} (records"
                      " kept by time instead of the latest of each key)")
    return "D4", ""


def cassandra(scylla, cdc, counters, table=""):
    """The same for one table: `scylla` whether the server keeps the
    change log migkit follows (ScyllaDB's per table), `cdc` the table's
    log options (`system_schema.scylla_tables.cdc`), `counters` whether it
    holds counters (only ever added to, which no log carries once). The
    delta log holds the key and the changed columns (D3); with the
    pre- and post-images on, every change with both (D4)."""
    on = {str(k): str(v).lower() for k, v in dict(cdc or {}).items()}
    if not scylla or counters:
        return "D0", ""
    if on.get("enabled") != "true":
        return "D0", f"ALTER TABLE {table} WITH cdc = {{'enabled': true}}"
    if on.get("preimage") not in ("true", "full") \
            or on.get("postimage") != "true":
        return "D3", (f"ALTER TABLE {table} WITH cdc = {{'enabled': true,"
                      " 'preimage': true, 'postimage': true}")
    return "D4", ""
