"""The ways a side's writes are held by, each reached on every branch it
has on fakes (backlog AUD-Z.rungs-outside-owed): a way is taken from the
ladder its own hold builds (`rungs` freezing a source, `keep_rungs`
keeping a target), held on facts its needs read as true, and refused -
with that need's own reason - on facts one of them does not: so a cutover
never freezes a source by a way the server cannot keep, nor reads a hold
that could not be made as made."""
import pytest

from migkit import decide, freeze
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

_UNHELD = {"server": [], "database": [], "kept": []}


def _unheld(scope, why_):
    return {"sides unheld": dict(_UNHELD, **{scope: [why_]})}


#: (hold class, its ladder, the class the guard names it by, the rung,
#:  facts it holds on, facts it is refused on or None, the reason said)
CASES = [
    (freeze.GroupHold, "rungs", "GroupHold", "group_server",
     {"sides unheld": _UNHELD}, _unheld("server", "a: not alone"),
     "a: not alone"),
    (freeze.GroupHold, "rungs", "GroupHold", "group_database",
     {"sides unheld": _UNHELD}, _unheld("database", "b: no grant option"),
     "b: no grant option"),
    (freeze.GroupHold, "keep_rungs", "GroupHold", "group_kept",
     {"sides unheld": _UNHELD}, _unheld("kept", "c: no authorizer"),
     "c: no authorizer"),
    (freeze.PostgresHold, "keep_rungs", "PostgresHold", "pg_database",
     {}, None, None),
    (freeze.PostgresHold, "rungs", "PostgresHold", "pg_database",
     {}, {"owner roles": True, "connect revoke": False},
     "not migkit's account's to take the right to connect from"),
    (freeze.PostgresHold, "rungs", "PostgresHold", "pg_server",
     {"others": [], "alter system": True},
     {"others": ["billing"], "alter system": True},
     "the server holds databases the hop does not move (billing)"),
    (freeze.PostgresHold, "rungs", "PostgresHold", "pg_role_limit",
     {"alter roles": True, "roles elsewhere": []},
     {"alter roles": True, "roles elsewhere": ["reports"]},
     "roles are in reports as well"),
    (freeze.MySQLHold, "rungs", "MySQLHold", "my_server",
     {"others": [], "read only switch": True},
     {"others": [], "read only switch": False},
     "may not change the server's read_only"),
    (freeze.MySQLHold, "rungs", "MySQLHold", "my_param_read_only",
     {"others": [], "change param_read_only":
      decide.Stand(decide.PRESENT, "on already")},
     {"others": [], "change param_read_only":
      decide.Stand(decide.IMPOSSIBLE, "its group is shared")},
     "which this source does not allow: its group is shared"),
    (freeze.MySQLHold, "rungs", "MySQLHold", "my_database",
     {}, {"grant option": False}, "holds no GRANT OPTION"),
    (freeze.TiDBHold, "rungs", "TiDBHold", "tidb_server",
     {"others": [], "version": (7, 5), "read only switch": True},
     {"others": [], "version": (6, 1), "read only switch": True},
     "needs 6.2, and the server runs 6.1"),
    (freeze.MongoHold, "rungs", "MongoHold", "mongo_block",
     {"others": [], "version": (7, 0),
      "actions": ["setUserWriteBlockMode"]},
     {"others": [], "version": (5, 0)}, "needs 6.0"),
    (freeze.MongoHold, "rungs", "MongoHold", "mongo_fsync",
     {"others": [], "actions": ["fsync", "unlock"]},
     {"others": [], "actions": ["fsync", "unlock"], "locked": True},
     "the server is locked already"),
    (freeze.MongoHold, "rungs", "MongoHold", "mongo_database",
     {"auth": True}, {"auth": False}, "authorization off"),
    (freeze.RedisHold, "rungs", "RedisHold", "redis_pause",
     {"others": [], "one server": True, "version": (7, 2),
      "stream back": False},
     {"others": [], "one server": True, "version": (7, 2),
      "stream back": True}, "the stream back applies on the old source"),
    (freeze.RedisHold, "rungs", "RedisHold", "redis_accounts",
     {"one server": True, "version": (7, 2), "acl": True},
     {"one server": False}, "node by node"),
    (freeze.MSSQLHold, "rungs", "MSSQLHold", "mssql_read_only",
     {"alter": True, "alone in its group": True},
     {"alter": True, "alone in its group": False}, "availability group"),
    (freeze.MSSQLHold, "rungs", "MSSQLHold", "mssql_deny",
     {"control": True, "kill": True},
     {"control": True, "kill": True, "sees": False}, "VIEW SERVER STATE"),
    (freeze.CassandraHold, "rungs", "CassandraHold", "cql_revoke",
     {"authorizer": True, "authorize": True, "cache": 2.0, "budget": 30.0},
     {"authorizer": True, "authorize": True, "cache": 60.0,
      "budget": 30.0}, "60s, past the 30s budget"),
    (freeze.KafkaHold, "rungs", "KafkaHold", "kafka_cluster",
     {"others": [], "authorizer": True},
     {"others": [], "authorizer": True, "alters": False},
     "may not change the cluster's access lists"),
    (freeze.KafkaHold, "rungs", "KafkaHold", "kafka_topics",
     {"authorizer": True}, {"authorizer": False}, "no authorizer"),
    (freeze.KafkaHold, "keep_rungs", "KafkaHold", "kafka_super",
     {"authorizer": True, "super": True},
     {"authorizer": True, "super": False}, "alone pass a deny"),
    (freeze.KafkaHold, "keep_rungs", "KafkaHold", "kafka_principals",
     {"authorizer": True, "principal": True, "closed": True,
      "named allows": True},
     {"authorizer": True, "principal": True, "closed": True,
      "named allows": False}, "(User:*)"),
    (freeze.OpenSearchHold, "keep_rungs", "OpenSearchHold", "os_named",
     {"numbered": True}, {"numbered": False}, "keeps no numbers"),
    (freeze.OpenSearchHold, "rungs", "OpenSearchHold", "os_cluster",
     {"others": []}, {"others": None}, "indices could not be listed"),
    (freeze.OpenSearchHold, "rungs", "OpenSearchHold", "os_indices",
     {}, None, None),
    (freeze.ClickHouseHold, "rungs", "_Grants", "<self.WAY>",
     {"grants": True, "ends": True, "sees": True},
     {"grants": True, "ends": True, "sees": False},
     "an open write would read as none"),
    (freeze.ClickHouseHold, "keep_rungs", "_Grants", "<self.WAY>",
     {"grants": True}, {"grants": False},
     "the privilege to manage access is not granted"),
    (freeze.CouchbaseHold, "rungs", "CouchbaseHold", "<self.WAY>",
     {"sees": True}, {"sees": False}, "(system:transactions)"),
    (freeze.CouchbaseHold, "keep_rungs", "CouchbaseHold", "<self.WAY>",
     {}, None, None),
    (freeze.DynamoDBHold, "keep_rungs", "DynamoDBHold", "<self.WAY>",
     {"answer": "yes", "me": True, "policies": True},
     {"answer": "no", "me": True, "policies": True},
     "the answer to the change of the target's access is no"),
    (freeze.BigQueryHold, "keep_rungs", "BigQueryHold", "<self.WAY>",
     {"answer": "yes", "me": True}, {"answer": "yes", "me": False},
     "(a service account)"),
]

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("freeze", "GroupHold", "group_server", "holds"),
    ("freeze", "GroupHold", "group_server", "refuses"),
    ("freeze", "GroupHold", "group_database", "holds"),
    ("freeze", "GroupHold", "group_database", "refuses"),
    ("freeze", "GroupHold", "group_kept", "holds"),
    ("freeze", "GroupHold", "group_kept", "refuses"),
    ("freeze", "PostgresHold", "pg_database", "holds"),
    ("freeze", "PostgresHold", "pg_database", "refuses"),
    ("freeze", "PostgresHold", "pg_server", "holds"),
    ("freeze", "PostgresHold", "pg_server", "refuses"),
    ("freeze", "PostgresHold", "pg_role_limit", "holds"),
    ("freeze", "PostgresHold", "pg_role_limit", "refuses"),
    ("freeze", "MySQLHold", "my_server", "holds"),
    ("freeze", "MySQLHold", "my_server", "refuses"),
    ("freeze", "MySQLHold", "my_param_read_only", "holds"),
    ("freeze", "MySQLHold", "my_param_read_only", "refuses"),
    ("freeze", "MySQLHold", "my_database", "holds"),
    ("freeze", "MySQLHold", "my_database", "refuses"),
    ("freeze", "TiDBHold", "tidb_server", "holds"),
    ("freeze", "TiDBHold", "tidb_server", "refuses"),
    ("freeze", "MongoHold", "mongo_block", "holds"),
    ("freeze", "MongoHold", "mongo_block", "refuses"),
    ("freeze", "MongoHold", "mongo_fsync", "holds"),
    ("freeze", "MongoHold", "mongo_fsync", "refuses"),
    ("freeze", "MongoHold", "mongo_database", "holds"),
    ("freeze", "MongoHold", "mongo_database", "refuses"),
    ("freeze", "RedisHold", "redis_pause", "holds"),
    ("freeze", "RedisHold", "redis_pause", "refuses"),
    ("freeze", "RedisHold", "redis_accounts", "holds"),
    ("freeze", "RedisHold", "redis_accounts", "refuses"),
    ("freeze", "MSSQLHold", "mssql_read_only", "holds"),
    ("freeze", "MSSQLHold", "mssql_read_only", "refuses"),
    ("freeze", "MSSQLHold", "mssql_deny", "holds"),
    ("freeze", "MSSQLHold", "mssql_deny", "refuses"),
    ("freeze", "CassandraHold", "cql_revoke", "holds"),
    ("freeze", "CassandraHold", "cql_revoke", "refuses"),
    ("freeze", "KafkaHold", "kafka_cluster", "holds"),
    ("freeze", "KafkaHold", "kafka_cluster", "refuses"),
    ("freeze", "KafkaHold", "kafka_topics", "holds"),
    ("freeze", "KafkaHold", "kafka_topics", "refuses"),
    ("freeze", "KafkaHold", "kafka_super", "holds"),
    ("freeze", "KafkaHold", "kafka_super", "refuses"),
    ("freeze", "KafkaHold", "kafka_principals", "holds"),
    ("freeze", "KafkaHold", "kafka_principals", "refuses"),
    ("freeze", "OpenSearchHold", "os_named", "holds"),
    ("freeze", "OpenSearchHold", "os_named", "refuses"),
    ("freeze", "OpenSearchHold", "os_cluster", "holds"),
    ("freeze", "OpenSearchHold", "os_cluster", "refuses"),
    ("freeze", "OpenSearchHold", "os_indices", "holds"),
    ("freeze", "_Grants", "<self.WAY>", "holds"),
    ("freeze", "_Grants", "<self.WAY>", "refuses"),
    ("freeze", "CouchbaseHold", "<self.WAY>", "holds"),
    ("freeze", "CouchbaseHold", "<self.WAY>", "refuses"),
    ("freeze", "DynamoDBHold", "<self.WAY>", "holds"),
    ("freeze", "DynamoDBHold", "<self.WAY>", "refuses"),
    ("freeze", "BigQueryHold", "<self.WAY>", "holds"),
    ("freeze", "BigQueryHold", "<self.WAY>", "refuses"),
]


def _hold(cls):
    """The hold as its ladder sees it: a source's, nothing read yet."""
    h = object.__new__(cls)
    h.hop, h.eng, h.side, h.kind = None, None, "src", cls.__name__
    return h


def _rung(cls, ladder, name):
    name = cls.WAY if name == "<self.WAY>" else name
    got = [r for r in getattr(_hold(cls), ladder)() if r.name == name]
    assert len(got) == 1, (cls, ladder, name)
    return got[0]


def test_the_declaration_is_what_the_cases_reach():
    said = {("freeze", c[2], c[3], HOLDS) for c in CASES} | {
        ("freeze", c[2], c[3], REFUSES) for c in CASES if c[5] is not None}
    assert said == set(PROVES_RUNGS)


@pytest.mark.parametrize("cls,ladder,_named,name,ok,bad,said", CASES,
                         ids=[f"{c[0].__name__}.{c[1]}.{c[3]}"
                              for c in CASES])
def test_a_way_is_held_where_its_needs_hold_and_refused_where_one_does_not(
        cls, ladder, _named, name, ok, bad, said):
    r = _rung(cls, ladder, name)
    assert isinstance(r, decide.Rung)
    with watched() as seen:
        kept, out = decide.prune([r], dict(ok))
        assert kept == [r], out
        if bad is not None:
            kept, out = decide.prune([r], dict(bad))
    if bad is None:
        assert not r.needs and not r.changes
        assert branch(seen, r.name) == [HOLDS]
        return
    assert not kept and out[0][0] is r
    assert branch(seen, r.name) == [HOLDS, REFUSES], seen
    assert said in out[0][1] and why(seen, r.name) == out[0][1]


def test_a_freeze_by_a_way_the_server_cannot_keep_falls_to_the_next():
    """Over the source's whole ladder, the server-wide way refused by a
    database the hop does not move leaves the per-database way chosen."""
    h = _hold(freeze.MySQLHold)
    facts = {"others": ["billing"], "read only switch": True}
    with watched() as seen:
        got = decide.climb("holding the source's writes", h.rungs(), facts)
    assert got.rung is not None and got.rung.name == "my_database"
    assert REFUSES in branch(seen, "my_server")
    assert "(billing)" in why(seen, "my_server")
