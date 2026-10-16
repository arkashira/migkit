"""A quiet tail says why on every source with a log it can read, not only
MySQL (FIELD1 #3): SQL Server, Oracle and MongoDB answer `tail_where` in
the shape `decide.tail_state` reads - where the tail is, the source's now,
how far the source's log moved since that position, and whether the
position is still kept - each through the engine's own connection, with
fake connections here. Every other following engine answers through
`tailctl.where_from`: its position now and a write counter of its own
where it keeps one, attached said unread, never assumed. PostgreSQL and
MySQL are run on the sandbox (lanes 6 and 7: a quiet source is still, a
write elsewhere moves its log, a change carried on the hop's tables
makes it still again, a dropped slot is detached); the rest are built
and declared unproved."""
import pytest
from bson.timestamp import Timestamp

from migkit import decide, tailctl
from tests import servers
from tests.conftest import needs_docker
from tests.test_every_ad_hoc_switch_goes_through_the_decision import _hop

PROVES = [("tail.quiet", "mysql"), ("tail.quiet", "postgres")]


def test_the_logs_counter_moves_from_the_first_ask_of_a_position():
    memo = {}
    assert tailctl.log_moved(memo, {"cdc": "0x1"}, 100) == 0
    assert tailctl.log_moved(memo, {"cdc": "0x1"}, 160) == 60
    # a restarted server's counter went back: not known, never 0
    assert tailctl.log_moved(memo, {"cdc": "0x1"}, 10) is None
    # a new position starts the count again
    assert tailctl.log_moved(memo, {"cdc": "0x2"}, 500) == 0
    assert tailctl.log_moved({}, {"cdc": "0x2"}, None) is None


def _states(eng, db, token, rounds):
    got = []
    for _ in range(rounds):
        facts = eng.tail_where(db, token)
        facts.update(quiet_for=99.0, interval=1.0)
        got.append(decide.tail_state(facts).path)
    return got, facts


def test_sql_server_says_its_log_moved_elsewhere(monkeypatch):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql"))
    wrote = iter([1000, 1000, 5096])
    monkeypatch.setattr(MSSQLEngine, "log_position",
                        lambda self, side, db: {"cdc": "0x00000025"})
    monkeypatch.setattr(MSSQLEngine, "_rows",
                        lambda self, side, db, sql: [(next(wrote),)])
    states, facts = _states(eng, "shop", {"cdc": "0x00000020"}, 3)
    assert states == ["idle", "idle", "elsewhere"]
    assert facts["moved"] == 4096 and facts["server"] == "0x00000025"
    assert "4,096 bytes of its log elsewhere" in tailctl.quiet_line(
        "shop", "elsewhere", facts, 0, 0)


def test_sql_server_that_cannot_be_read_is_detached_in_words(monkeypatch):
    import pymssql
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql"))

    def refuse(self, side, db):
        raise pymssql.OperationalError("login failed for user 'migkit'")
    monkeypatch.setattr(MSSQLEngine, "log_position", refuse)
    facts = eng.tail_where("shop", {"cdc": "0x1"})
    assert facts["attached"] is False and "login failed" in \
        facts["why_detached"]


def test_oracle_says_its_redo_moved_and_a_position_no_longer_kept(
        monkeypatch):
    from migkit.engines.oracle import OracleEngine
    eng = OracleEngine(_hop("oracle"))
    kept = [100]
    redo = iter([5000, 5600])

    def rows(self, side, db, sql):
        return [(next(redo),)] if "v$sysstat" in sql else [(kept[0],)]
    monkeypatch.setattr(OracleEngine, "log_position",
                        lambda self, side, db: 900)
    monkeypatch.setattr(OracleEngine, "_rows", rows)
    states, facts = _states(eng, "ORCL", 500, 2)
    assert states == ["idle", "elsewhere"] and facts["moved"] == 600
    assert facts["at"] == "SCN 500" and facts["server"] == "SCN 900"
    kept[0], redo = 700, iter([5600])
    facts = eng.tail_where("ORCL", 500)
    assert facts["attached"] is False
    assert "redo from SCN 700 only" in facts["why_detached"]


class _Cursor(list):
    def sort(self, key, direction):
        return _Cursor(sorted(self, key=lambda e: e["ts"],
                              reverse=direction < 0))

    def limit(self, n):
        return _Cursor(self[:n])


class _Mongo:
    def __init__(self, oplog, ops):
        self.oplog, self.ops = oplog, ops
        self.local = {"oplog.rs": self}
        self.admin = self

    def find(self, flt, proj):
        return _Cursor({"ts": t} for t in self.oplog)

    def command(self, name):
        assert name == "serverStatus"
        return {"opcounters": next(self.ops)}


def test_mongodb_says_its_writes_and_an_oplog_past_the_tail(monkeypatch):
    from migkit.engines.mongodb import MongoEngine
    eng = MongoEngine(_hop("mongodb"))
    ops = iter([{"insert": 5, "update": 1, "delete": 0},
                {"insert": 9, "update": 1, "delete": 2}])
    client = _Mongo([Timestamp(100, 1), Timestamp(200, 3)], ops)
    monkeypatch.setattr(MongoEngine, "_client", lambda self, side: client)
    token = {"ts": [150, 1]}
    states, facts = _states(eng, "shop", token, 2)
    assert states == ["idle", "elsewhere"] and facts["moved"] == 6
    assert facts["server"] == "200:3" and "attached" not in facts
    assert "6 writes elsewhere" in tailctl.quiet_line(
        "shop", "elsewhere", facts, 0, 0)
    client.oplog, client.ops = [Timestamp(300, 1)], iter([{}])
    facts = eng.tail_where("shop", token)
    assert facts["attached"] is False and "past the tail's" in \
        facts["why_detached"]


@pytest.mark.parametrize("engine", ["mssql", "oracle", "mongodb",
                                    "postgres"])
def test_every_source_named_here_has_a_where(engine):
    from migkit.engines import mongodb, mssql, oracle, postgres
    cls = {"mssql": mssql.MSSQLEngine, "oracle": oracle.OracleEngine,
           "mongodb": mongodb.MongoEngine,
           "postgres": postgres.PostgresEngine}[engine]
    assert tailctl._where_now(cls(_hop(engine)), "db") is not None


def test_a_refused_egress_stops_the_sign_in_not_its_groups(monkeypatch):
    from migkit.web import identity

    def gate(url):
        raise identity.Refused("the view may not reach idp.example.com")
    monkeypatch.setattr(identity, "_gate", gate)
    with pytest.raises(identity.Refused, match="may not reach"):
        identity.Oidc._userinfo(object.__new__(identity.Oidc),
                                "https://idp.example.com/userinfo", "t", "s")


def _pg(monkeypatch, row, way=None):
    from types import SimpleNamespace
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres"))
    asked = []

    def rows(self, side, db, sql, args=None):
        asked.append(args)
        return [row]
    monkeypatch.setattr(PostgresEngine, "_slot_rows", rows)
    if way:
        eng.__dict__["_follow_ways"] = {
            ("src", "shop"): SimpleNamespace(path=way)}
    return eng, asked


def test_postgres_slot_held_by_the_tail_is_attached_and_still(monkeypatch):
    eng, asked = _pg(monkeypatch, ("0/3000", 0, True, True, 42, "reserved",
                                   42))
    states, facts = _states(eng, "shop", "0/3000", 1)
    assert states == ["idle"] and facts["attached"] is True
    assert facts["moved"] == 0 and facts["server"] == "0/3000"
    assert asked[0][2] == eng.slot_name()


def test_postgres_measures_the_wal_from_the_last_carried_change(
        monkeypatch):
    eng, asked = _pg(monkeypatch, ("0/5000", 8192, True, False, None,
                                   "reserved", 42))
    eng._tail_marked("src", "shop", ([{"op": "i"}], "0/3000"))
    # a quiet read moved the tail to the end: the mark stays where the
    # hop's last change was, so the writes read past are still said
    eng._tail_marked("src", "shop", ([], "0/5000"))
    states, facts = _states(eng, "shop", "0/5000", 1)
    assert asked[0][:2] == ("0/3000", "0/3000")
    assert states == ["elsewhere"] and facts["moved"] == 8192
    assert "8,192 bytes of its log elsewhere" in tailctl.quiet_line(
        "shop", "elsewhere", facts, 0, 0)


def test_postgres_slot_held_by_another_session_is_detached(monkeypatch):
    eng, _ = _pg(monkeypatch, ("0/3000", 0, True, True, 77, "reserved", 42))
    facts = eng.tail_where("shop", "0/3000")
    assert facts["attached"] is False and "pid 77" in facts["why_detached"]
    assert decide.tail_state({**facts, "quiet_for": 9}).path == "detached"


@pytest.mark.parametrize("kept,status,said", [
    (False, None, "has no slot"), (True, "lost", "lost its WAL")])
def test_postgres_slot_gone_or_lost_is_detached(monkeypatch, kept, status,
                                                said):
    eng, _ = _pg(monkeypatch, ("0/3000", 0, kept, False, None, status, 42))
    facts = eng.tail_where("shop", "0/3000")
    assert facts["attached"] is False and said in facts["why_detached"]


def test_postgres_followed_by_its_wal_leaves_attached_unread(monkeypatch):
    eng, _ = _pg(monkeypatch, ("0/3000", 0, False, False, None, None, 42),
                 way="wal inspect")
    facts = eng.tail_where("shop", "0/3000")
    assert "attached" not in facts and "wal inspect" in \
        facts["attached_unread"]
    line = tailctl.quiet_line("shop", "idle", facts, 0, 0)
    assert line.startswith("shop: at 0/3000 (followed by its WAL")
    assert "attached at" not in line


def test_postgres_that_cannot_be_read_is_detached_in_words(monkeypatch):
    import psycopg2
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop("postgres"))

    def refuse(self, side, db, sql, args=None):
        raise psycopg2.OperationalError("connection refused\nmore")
    monkeypatch.setattr(PostgresEngine, "_slot_rows", refuse)
    facts = eng.tail_where("shop", "0/3000")
    assert facts == {"at": "0/3000", "attached": False,
                     "why_detached": "connection refused"}


class _Counted:
    """A following engine with a position of its own and a write counter,
    its answers set by the test."""
    log_position = staticmethod(lambda side, db: "pos-now")

    def __init__(self, counts):
        self.counts = iter(counts)

    def tail_where(self, db, token):
        return tailctl.where_from(self, db, token, lambda: next(self.counts),
                                  "rows")


def test_a_following_engines_counter_is_counted_from_the_last_change():
    eng = _Counted([10, 10, 25, 30, 30])
    where = tailctl._where_now(eng, "shop")
    got = [where("t1") for _ in range(3)]
    assert [g["moved"] for g in got] == [0, 0, 15]
    assert got[2]["server"] == "pos-now"
    assert got[2]["attached_unread"] == tailctl.NO_READER
    assert "attached" not in got[2]
    assert "15 rows elsewhere" in tailctl.quiet_line(
        "shop", "elsewhere", got[2], 0, 0)
    # a batch carried a change on the hop's tables: the count starts
    # again there, though the position (a clock) moved on while quiet
    where.carried()
    assert [where("t9")["moved"] for _ in range(2)] == [0, 0]


def test_a_counter_refused_or_kept_by_none_is_unread_never_still():
    class Refused(_Counted):
        def tail_where(self, db, token):
            def wrote():
                raise PermissionError("not granted")
            return tailctl.where_from(self, db, token, wrote, "rows")
    facts = Refused([]).tail_where("shop", "t1")
    assert facts["moved"] is None and decide.tail_state(
        {**facts, "quiet_for": 99}).path == "following"
    from migkit.engines.dbapi import VerticaEngine
    facts = VerticaEngine(_hop("vertica")).tail_where("shop", {"at": 4})
    assert facts["moved"] is None and "server" not in facts
    assert facts["attached_unread"] == tailctl.NO_READER


def test_a_position_the_server_does_not_answer_is_detached_in_words():
    class Gone(_Counted):
        @staticmethod
        def log_position(side, db):
            raise ConnectionError("connection reset by peer\ntrace")
    facts = Gone([1]).tail_where("shop", "t1")
    assert facts == {"at": "t1", "attached": False,
                     "why_detached": "connection reset by peer"}


@pytest.mark.parametrize("engine", [
    "redis", "kafka", "clickhouse", "dynamodb", "db2", "db2i",
    "opensearch", "snowflake", "bigquery", "cockroachdb", "dsql",
    "couchbase", "cosmosdb", "spanner", "bigtable", "firestore", "gaussdb",
    "neptune", "geminidb-redis", "neo4j", "analyticdb-postgres",
    "yugabytedb", "netezza", "saphana", "informix", "progress", "vertica",
    "exasol", "databricks"])
def test_every_following_engine_has_a_where(engine):
    from migkit.engines import _class_for
    from migkit.engines.base import Engine
    cls = _class_for(engine)
    assert cls.tail_where is not getattr(Engine, "tail_where", None)
    assert callable(cls.tail_where)


def test_a_source_that_follows_nothing_says_no_quiet_line():
    from migkit.engines.db2zos import Db2ZosEngine
    assert tailctl._where_now(Db2ZosEngine(_hop("db2zos")), "db") is None


pg_live = servers.fixture("pg")
my_live = servers.fixture("mysql")


def _live(cls, name, got, where, db):
    from migkit.config import Hop
    hop = Hop(name=f"quiet-{name}", engine=name,
              source=got.src.endpoint(), target=got.dst.endpoint(),
              databases=[db])
    hop.report_dir = lambda db=None: where
    return cls(hop)


def _state(facts):
    return decide.tail_state({**facts, "quiet_for": 99.0,
                              "interval": 1.0}).path


@needs_docker
def test_a_quiet_postgresql_source_is_still_until_it_writes(pg_live,
                                                           tmp_path):
    from migkit.engines.postgres import PostgresEngine
    db, src = "postgres", pg_live.src
    eng = _live(PostgresEngine, "postgres", pg_live, tmp_path, db)
    slot = eng.slot_name()
    src.sql("create table if not exists quiet_elsewhere (id int)", db)
    src.sql(f"select pg_create_logical_replication_slot('{slot}',"
            " 'test_decoding')", db)
    try:
        token = src.sql("select pg_current_wal_flush_lsn()::text", db)[0][0]
        facts = eng.tail_where(db, token)
        assert facts["attached"] is True and facts["moved"] == 0, facts
        assert facts["server"] == token and _state(facts) == "idle"
        src.sql("insert into quiet_elsewhere values (1)", db)
        facts = eng.tail_where(db, token)
        assert facts["moved"] > 0 and _state(facts) == "elsewhere", facts
        # the batch carried a change on the hop's tables at the end now
        eng._tail_marked("src", db, ([{"op": "i"}], facts["server"]))
        facts = eng.tail_where(db, facts["server"])
        assert facts["moved"] == 0 and _state(facts) == "idle", facts
    finally:
        src.sql(f"select pg_drop_replication_slot('{slot}')", db)
    facts = eng.tail_where(db, token)
    assert facts["attached"] is False and "has no slot" in \
        facts["why_detached"]
    assert _state(facts) == "detached"


@needs_docker
def test_a_quiet_mysql_source_is_still_until_it_writes(my_live, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    db, src = "quiet_where", my_live.src
    src.sql(f"create database if not exists {db}")
    src.sql(f"create table if not exists {db}.elsewhere (id int)")
    eng = _live(MySQLEngine, "mysql", my_live, tmp_path, db)
    log, pos = eng._binlog_position("src")
    token = {"log_file": log, "log_pos": pos}
    facts = eng.tail_where(db, token)
    assert facts["moved"] == 0 and facts["server"] == f"{log}:{pos}", facts
    # a reader that reads the log to its end each batch holds nothing
    # the server could list between batches: never said detached
    assert facts.get("attached") is not False and _state(facts) == "idle"
    src.sql(f"insert into {db}.elsewhere values (1)")
    facts = eng.tail_where(db, token)
    assert facts["moved"] > 0 and _state(facts) == "elsewhere", facts
    log, pos = eng._binlog_position("src")
    now = {"log_file": log, "log_pos": pos}
    eng._tail_saw("src", db, token, now, True)
    facts = eng.tail_where(db, now)
    assert facts["moved"] == 0 and _state(facts) == "idle", facts
