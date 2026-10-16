"""The ways an engine reads, writes, digests and moves a table by, each
reached on every branch it has on fakes (backlog
AUD-Z.rungs-outside-owed): a way is taken from the ladder its engine
keeps (a class's own ladder, or the function building it), held on facts
its needs read as true, and refused - with that need's own reason - on
facts one of them does not, so a move never takes a way its server,
program or table cannot carry and says why it fell to the next."""
import pytest

from migkit import decide
from migkit.engines import (mssql, mysql, neo4j, neptune, opensearch, oracle,
                            oracle_logminer as lm, oracle_pump, postgres,
                            provedsql, qdrant, redis, spanner, teradata,
                            timestream, warehouse, warehouse_sql)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why


def _kept(cls):
    """The rungs a class keeps as ladders of its own."""
    got = []
    for value in vars(cls).values():
        for r in value if isinstance(value, (tuple, list)) else ():
            if isinstance(r, decide.Rung):
                got.append(r)
    return got


def _built(cls, ladder):
    return lambda: getattr(cls, ladder)(object.__new__(cls))


#: (module, ladder) as the guard names it -> the rungs it builds
LADDERS = {
    ("engines.mssql", "MSSQLEngine"): lambda: _kept(mssql.MSSQLEngine),
    ("engines.mysql", "MySQLEngine"): lambda: _kept(mysql.MySQLEngine),
    ("engines.mysql", "_both_ways"): lambda: [mysql._both_ways()],
    ("engines.neo4j", "Neo4jEngine"): _built(neo4j.Neo4jEngine,
                                             "_move_ways"),
    ("engines.neptune", "NeptuneEngine"): _built(neptune.NeptuneEngine,
                                                 "_table_ways"),
    ("engines.opensearch", "OpenSearchEngine"): _built(
        opensearch.OpenSearchEngine, "_table_ways"),
    ("engines.oracle", "WRITE_WAYS"): lambda: oracle.WRITE_WAYS,
    ("engines.oracle_logminer", "dictionary_ways"): lm.dictionary_ways,
    ("engines.oracle_logminer", "places"): lm.places,
    ("engines.oracle_pump", "ways"): oracle_pump.ways,
    ("engines.postgres", "PostgresEngine"): lambda: _kept(
        postgres.PostgresEngine),
    ("engines.provedsql", "ProvedDigest"): lambda: _kept(
        provedsql.ProvedDigest),
    ("engines.qdrant", "QdrantEngine"): _built(qdrant.QdrantEngine,
                                               "_move_ways"),
    ("engines.redis", "RedisEngine"): lambda: _kept(redis.RedisEngine),
    ("engines.spanner", "SpannerEngine"): lambda: (
        _built(spanner.SpannerEngine, "_partition_ways")()
        + _built(spanner.SpannerEngine, "_table_ways")()),
    ("engines.teradata", "TeradataEngine"): lambda: (
        _built(teradata.TeradataEngine, "_read_ways")()
        + _built(teradata.TeradataEngine, "_write_ways")()),
    ("engines.timestream", "TimestreamEngine"): _built(
        timestream.TimestreamEngine, "_table_ways"),
    ("engines.warehouse", "BigQueryEngine"): lambda: _kept(
        warehouse.BigQueryEngine),
    ("engines.warehouse", "RedshiftEngine"): lambda: _kept(
        warehouse.RedshiftEngine),
    ("engines.warehouse_sql", "DigestsInSQL"): lambda: _kept(
        warehouse_sql.DigestsInSQL),
}

#: a rung named by an expression -> the name it is built with
COMPUTED = {"<KEEP>": lm.KEEP, "<REBUILD>": lm.REBUILD, "<CARRY>": lm.CARRY,
            "<STANDBY>": lm.STANDBY, "<SOURCE>": lm.SOURCE,
            "<INSTANCE>": lm.INSTANCE}

_REPLICA = {"id": 1, "same": "0", "gtid": "ON", "passes": "1", "step": "2",
            "offset": "1"}
_WAL = {"no key": False, "wal_level": "replica", "version": 160000,
        "wal functions": "installed", "wal read granted": True,
        "wal kept": True, "wal program": "/usr/bin/true",
        "wal dirs": ["/var/wal"]}

#: (module, ladder, rung, facts it holds on, facts it is refused on or
#:  None where it has no need, the reason said)
CASES = [
    ("engines.mssql", "MSSQLEngine", "bulk load",
     {"past a piece": False, "driver": None, "refused": None,
      "unproved": None}, {"past a piece": True},
     "writes no value in pieces"),
    ("engines.mssql", "MSSQLEngine", "arrow",
     {"driver": None, "there": True, "unproved": None},
     {"driver": None, "there": False}, "the copier makes it"),
    ("engines.mssql", "MSSQLEngine", "statements", {}, None, None),
    ("engines.mssql", "MSSQLEngine", "copier", {}, None, None),
    ("engines.mysql", "MySQLEngine", "dump-load",
     {"dump": True, "via": "auto", "batch": True}, {"dump": False},
     "doctor --install puts it in place"),
    ("engines.mysql", "MySQLEngine", "stream-copy",
     {"shell": True, "local_infile": True}, {"shell": False},
     "the straight copy is not installed here"),
    ("engines.mysql", "MySQLEngine", "pass-through",
     {"straight_only": False, "client": True, "loads": True,
      "unfit": None}, {"straight_only": True},
     "the move is asked to copy straight across"),
    ("engines.mysql", "MySQLEngine", "rows", {"straight_only": False},
     {"straight_only": True}, "the move is asked to copy straight across"),
    ("engines.mysql", "MySQLEngine", "staged", {}, None, None),
    ("engines.mysql", "MySQLEngine", "statements", {}, None, None),
    ("engines.mysql", "MySQLEngine", "load", {}, None, None),
    ("engines.mysql", "_both_ways", "replicas both ways",
     {"src": _REPLICA, "dst": dict(_REPLICA, id=2, offset="2"),
      "keyed": True},
     {"src": _REPLICA, "dst": _REPLICA, "keyed": True},
     "both sides have server_id 1"),
    ("engines.neo4j", "Neo4jEngine", "dump",
     {"changed": False, "stoppable": True, "admin": True, "empty": True,
      "sizes": {"ranges_s": 100.0, "bytes": 10}, "disk": 1},
     {"changed": True}, "a row filter, mask, label or type scope"),
    ("engines.neo4j", "Neo4jEngine", "ranges", {}, None, None),
    ("engines.neptune", "NeptuneEngine", "loader", {"unfit": None},
     {"unfit": "the cluster has no role to read files by"},
     "the cluster has no role to read files by"),
    ("engines.neptune", "NeptuneEngine", "copier", {}, None, None),
    ("engines.opensearch", "OpenSearchEngine", "snapshot",
     {"reader": True, "unfit": None}, {"reader": False},
     "the snapshot reader is not built here"),
    ("engines.opensearch", "OpenSearchEngine", "scroll", {}, None, None),
    ("engines.oracle", "WRITE_WAYS", "direct",
     {"rows": 5000, "from": 1000, "big": False, "stageable": True},
     {"rows": 10, "from": 1000}, "costs a direct path load"),
    ("engines.oracle", "WRITE_WAYS", "array", {}, None, None),
    ("engines.oracle_logminer", "places", "<STANDBY>", {"standby": True},
     {"standby": False}, "names no mine_standby"),
    ("engines.oracle_logminer", "places", "<SOURCE>",
     {"off source": False, "short": []},
     {"off source": False, "short": ["LOGMINING"]},
     "needs LOGMINING, which its account lacks"),
    ("engines.oracle_logminer", "places", "<INSTANCE>",
     {"instance": True, "archive": True, "dictionary": "in the redo"},
     {"instance": True, "archive": False}, "(archive_from)"),
    ("engines.oracle_logminer", "dictionary_ways", "<KEEP>",
     {"fits": True, "pending": False},
     {"fits": False, "pending": False, "size": 2 ** 30, "bound": 2 ** 29,
      "limit": "the reader's memory"},
     "1024 MB, over the 512 MB the reader holds"),
    ("engines.oracle_logminer", "dictionary_ways", "<REBUILD>",
     {"pending": False}, {"pending": True},
     "a newer dictionary is written already"),
    ("engines.oracle_logminer", "dictionary_ways", "<CARRY>", {}, None,
     None),
    ("engines.oracle_pump", "ways", "pump",
     {"triggers": True, "why not": None}, {"triggers": False},
     "brings each table's triggers with it"),
    ("engines.oracle_pump", "ways", "copier", {}, None, None),
    ("engines.postgres", "PostgresEngine", "as a replica", {"quiet": "flag"},
     {"quiet": "none"}, "may not write as a replica"),
    ("engines.postgres", "PostgresEngine", "as they are", {"quiet": "none"},
     {"quiet": "flag", "write refused": None}, "triggers or foreign keys"),
    ("engines.postgres", "PostgresEngine", "logical slot",
     {"wal_level": "logical"}, {"wal_level": "replica"},
     "wal_level is replica on the source"),
    ("engines.postgres", "PostgresEngine", "wal inspect", _WAL,
     dict(_WAL, **{"no key": True}), "the table has no primary key"),
    ("engines.postgres", "PostgresEngine", "wal dump", _WAL,
     dict(_WAL, **{"no key": True}), "the table has no primary key"),
    ("engines.postgres", "PostgresEngine", "digest diff",
     {"no key": True, "digest way": True}, {"no key": False},
     "the table has a key"),
    ("engines.postgres", "PostgresEngine", "lanes",
     {"lanes": 4, "tables": 3}, {"lanes": 1, "tables": 3},
     "nothing to share"),
    ("engines.postgres", "PostgresEngine", "pass-through", {"binary": None},
     {"binary": "a column of a type with no binary encoding"},
     "binary encoding"),
    ("engines.postgres", "PostgresEngine", "pass-through unawaited",
     {"binary": None, "dst_version": 160000},
     {"binary": None, "dst_version": 90600}, "commit"),
    ("engines.postgres", "PostgresEngine", "streaming",
     {"streaming": True, "large": True, "truncatable": True,
      "binary": None}, {"streaming": False}, "the streaming copy"),
] + [("engines.postgres", "PostgresEngine", name, {}, None, None)
     for name in ("binary", "digest", "hashes", "names", "one", "own text",
                  "own typed", "pipe", "public text", "public typed",
                  "ranges", "read back", "staged", "statements",
                  "temp text", "temp typed", "text")] + [
    ("engines.provedsql", "ProvedDigest", "server", {"rendered": True},
     {"rendered": False}, "a column has no rendering in the server's SQL"),
    ("engines.provedsql", "ProvedDigest", "here", {}, None, None),
    ("engines.qdrant", "QdrantEngine", "snapshot",
     {"changed": False, "fresh": True, "done": False,
      "transfer": {"snapshot_s": 1.0, "scroll_s": 5.0}},
     {"changed": True}, "a mask or row filter changes them"),
    ("engines.qdrant", "QdrantEngine", "pages", {}, None, None),
    ("engines.redis", "RedisEngine", "moment", {"abs": True},
     {"abs": False}, "older than 5.0"),
    ("engines.redis", "RedisEngine", "left", {}, None, None),
    ("engines.spanner", "SpannerEngine", "partitions", {}, None, None),
    ("engines.spanner", "SpannerEngine", "stream", {}, None, None),
    ("engines.spanner", "SpannerEngine", "copier", {}, None, None),
    ("engines.teradata", "TeradataEngine", "fastexport",
     {"rows": 10 ** 6, "lobs": False, "sessions": True}, {"rows": 10},
     "only from 100,000 rows"),
    ("engines.teradata", "TeradataEngine", "select", {}, None, None),
    ("engines.teradata", "TeradataEngine", "fastload",
     {"rows": 10 ** 6, "odd": [], "sessions": True}, {"rows": 10},
     "only from 100,000 rows"),
    ("engines.teradata", "TeradataEngine", "statements", {}, None, None),
    ("engines.timestream", "TimestreamEngine", "batch", {"unfit": None},
     {"unfit": "a measure of a type the batch load takes not"},
     "a measure of a type the batch load takes not"),
    ("engines.timestream", "TimestreamEngine", "copier", {}, None, None),
    ("engines.warehouse", "BigQueryEngine", "load job",
     {"loads left": None}, {"loads left": 0}, "are used up"),
    ("engines.warehouse", "BigQueryEngine", "write stream", {}, None, None),
    ("engines.warehouse", "RedshiftEngine", "copy", {}, None, None),
    ("engines.warehouse", "RedshiftEngine", "insert", {}, None, None),
    ("engines.warehouse_sql", "DigestsInSQL", "server", {"pick": "md5"},
     {"pick": None}, "a column has no rendering in the server's SQL"),
    ("engines.warehouse_sql", "DigestsInSQL", "here", {}, None, None),
]

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('engines.mssql', 'MSSQLEngine', 'bulk load', 'holds'),
    ('engines.mssql', 'MSSQLEngine', 'bulk load', 'refuses'),
    ('engines.mssql', 'MSSQLEngine', 'arrow', 'holds'),
    ('engines.mssql', 'MSSQLEngine', 'arrow', 'refuses'),
    ('engines.mssql', 'MSSQLEngine', 'statements', 'holds'),
    ('engines.mssql', 'MSSQLEngine', 'copier', 'holds'),
    ('engines.mysql', 'MySQLEngine', 'dump-load', 'holds'),
    ('engines.mysql', 'MySQLEngine', 'dump-load', 'refuses'),
    ('engines.mysql', 'MySQLEngine', 'stream-copy', 'holds'),
    ('engines.mysql', 'MySQLEngine', 'stream-copy', 'refuses'),
    ('engines.mysql', 'MySQLEngine', 'pass-through', 'holds'),
    ('engines.mysql', 'MySQLEngine', 'pass-through', 'refuses'),
    ('engines.mysql', 'MySQLEngine', 'rows', 'holds'),
    ('engines.mysql', 'MySQLEngine', 'rows', 'refuses'),
    ('engines.mysql', 'MySQLEngine', 'staged', 'holds'),
    ('engines.mysql', 'MySQLEngine', 'statements', 'holds'),
    ('engines.mysql', 'MySQLEngine', 'load', 'holds'),
    ('engines.mysql', '_both_ways', 'replicas both ways', 'holds'),
    ('engines.mysql', '_both_ways', 'replicas both ways', 'refuses'),
    ('engines.neo4j', 'Neo4jEngine', 'dump', 'holds'),
    ('engines.neo4j', 'Neo4jEngine', 'dump', 'refuses'),
    ('engines.neo4j', 'Neo4jEngine', 'ranges', 'holds'),
    ('engines.neptune', 'NeptuneEngine', 'loader', 'holds'),
    ('engines.neptune', 'NeptuneEngine', 'loader', 'refuses'),
    ('engines.neptune', 'NeptuneEngine', 'copier', 'holds'),
    ('engines.opensearch', 'OpenSearchEngine', 'snapshot', 'holds'),
    ('engines.opensearch', 'OpenSearchEngine', 'snapshot', 'refuses'),
    ('engines.opensearch', 'OpenSearchEngine', 'scroll', 'holds'),
    ('engines.oracle', 'WRITE_WAYS', 'direct', 'holds'),
    ('engines.oracle', 'WRITE_WAYS', 'direct', 'refuses'),
    ('engines.oracle', 'WRITE_WAYS', 'array', 'holds'),
    ('engines.oracle_logminer', 'places', '<STANDBY>', 'holds'),
    ('engines.oracle_logminer', 'places', '<STANDBY>', 'refuses'),
    ('engines.oracle_logminer', 'places', '<SOURCE>', 'holds'),
    ('engines.oracle_logminer', 'places', '<SOURCE>', 'refuses'),
    ('engines.oracle_logminer', 'places', '<INSTANCE>', 'holds'),
    ('engines.oracle_logminer', 'places', '<INSTANCE>', 'refuses'),
    ('engines.oracle_logminer', 'dictionary_ways', '<KEEP>', 'holds'),
    ('engines.oracle_logminer', 'dictionary_ways', '<KEEP>', 'refuses'),
    ('engines.oracle_logminer', 'dictionary_ways', '<REBUILD>', 'holds'),
    ('engines.oracle_logminer', 'dictionary_ways', '<REBUILD>', 'refuses'),
    ('engines.oracle_logminer', 'dictionary_ways', '<CARRY>', 'holds'),
    ('engines.oracle_pump', 'ways', 'pump', 'holds'),
    ('engines.oracle_pump', 'ways', 'pump', 'refuses'),
    ('engines.oracle_pump', 'ways', 'copier', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'as a replica', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'as a replica', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'as they are', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'as they are', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'logical slot', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'logical slot', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'wal inspect', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'wal inspect', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'wal dump', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'wal dump', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'digest diff', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'digest diff', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'lanes', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'lanes', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'pass-through', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'pass-through', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'pass-through unawaited', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'pass-through unawaited', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'streaming', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'streaming', 'refuses'),
    ('engines.postgres', 'PostgresEngine', 'binary', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'digest', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'hashes', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'names', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'one', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'own text', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'own typed', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'pipe', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'public text', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'public typed', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'ranges', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'read back', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'staged', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'statements', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'temp text', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'temp typed', 'holds'),
    ('engines.postgres', 'PostgresEngine', 'text', 'holds'),
    ('engines.provedsql', 'ProvedDigest', 'server', 'holds'),
    ('engines.provedsql', 'ProvedDigest', 'server', 'refuses'),
    ('engines.provedsql', 'ProvedDigest', 'here', 'holds'),
    ('engines.qdrant', 'QdrantEngine', 'snapshot', 'holds'),
    ('engines.qdrant', 'QdrantEngine', 'snapshot', 'refuses'),
    ('engines.qdrant', 'QdrantEngine', 'pages', 'holds'),
    ('engines.redis', 'RedisEngine', 'moment', 'holds'),
    ('engines.redis', 'RedisEngine', 'moment', 'refuses'),
    ('engines.redis', 'RedisEngine', 'left', 'holds'),
    ('engines.spanner', 'SpannerEngine', 'partitions', 'holds'),
    ('engines.spanner', 'SpannerEngine', 'stream', 'holds'),
    ('engines.spanner', 'SpannerEngine', 'copier', 'holds'),
    ('engines.teradata', 'TeradataEngine', 'fastexport', 'holds'),
    ('engines.teradata', 'TeradataEngine', 'fastexport', 'refuses'),
    ('engines.teradata', 'TeradataEngine', 'select', 'holds'),
    ('engines.teradata', 'TeradataEngine', 'fastload', 'holds'),
    ('engines.teradata', 'TeradataEngine', 'fastload', 'refuses'),
    ('engines.teradata', 'TeradataEngine', 'statements', 'holds'),
    ('engines.timestream', 'TimestreamEngine', 'batch', 'holds'),
    ('engines.timestream', 'TimestreamEngine', 'batch', 'refuses'),
    ('engines.timestream', 'TimestreamEngine', 'copier', 'holds'),
    ('engines.warehouse', 'BigQueryEngine', 'load job', 'holds'),
    ('engines.warehouse', 'BigQueryEngine', 'load job', 'refuses'),
    ('engines.warehouse', 'BigQueryEngine', 'write stream', 'holds'),
    ('engines.warehouse', 'RedshiftEngine', 'copy', 'holds'),
    ('engines.warehouse', 'RedshiftEngine', 'insert', 'holds'),
    ('engines.warehouse_sql', 'DigestsInSQL', 'server', 'holds'),
    ('engines.warehouse_sql', 'DigestsInSQL', 'server', 'refuses'),
    ('engines.warehouse_sql', 'DigestsInSQL', 'here', 'holds'),
]


def _rungs(module, ladder, name):
    name = COMPUTED.get(name, name)
    got = [r for r in LADDERS[(module, ladder)]() if r.name == name]
    assert got, (module, ladder, name)
    return got


def _holding(rung, facts):
    """`facts`, with every change the rung needs on the source there."""
    facts = dict(facts)
    for c in rung.changes:
        facts["change " + c.name] = decide.Stand(decide.PRESENT, "there")
    return facts


def test_the_declaration_is_what_the_cases_reach():
    said = {c[:3] + (HOLDS,) for c in CASES} | {
        c[:3] + (REFUSES,) for c in CASES if c[4] is not None}
    assert said == set(PROVES_RUNGS)


@pytest.mark.parametrize("module,ladder,name,ok,bad,said", CASES,
                         ids=[f"{c[0]}.{c[2]}" for c in CASES])
def test_a_way_is_held_where_its_needs_hold_and_refused_where_one_does_not(
        module, ladder, name, ok, bad, said):
    for r in _rungs(module, ladder, name):
        with watched() as seen:
            kept, out = decide.prune([r], _holding(r, ok))
            assert kept == [r], out
            if bad is not None:
                kept, out = decide.prune([r], dict(bad))
        if bad is None:
            assert not r.needs and not r.changes, r
            assert branch(seen, r.name) == [HOLDS]
            continue
        assert not kept and out[0][0] is r
        assert branch(seen, r.name) == [HOLDS, REFUSES], seen
        assert said in out[0][1] and why(seen, r.name) == out[0][1], out


def test_a_change_the_source_has_not_answered_refuses_the_rebuild():
    """The newer dictionary is written into the source's redo only once
    the operator answers yes: unanswered, the rebuild is refused by its
    change and the window keeps what the reader holds."""
    rebuild, = _rungs("engines.oracle_logminer", "dictionary_ways",
                      "<REBUILD>")
    c, = rebuild.changes
    facts = {"pending": False,
             "change " + c.name: decide.Stand(decide.POSSIBLE, "")}
    with watched() as seen:
        kept, out = decide.prune([rebuild], facts)
    assert not kept and "offered, never assumed" in out[0][1]
    assert branch(seen, rebuild.name) == [REFUSES]
