"""A quiet tail says why on every source with a log it can read, not only
MySQL (FIELD1 #3): SQL Server, Oracle and MongoDB answer `tail_where` in
the shape `decide.tail_state` reads - where the tail is, the source's now,
how far the source's log moved since that position, and whether the
position is still kept - each through the engine's own connection, with
fake connections here. Not run against a real server or the sandbox:
unproved there."""
import pytest
from bson.timestamp import Timestamp

from migkit import decide, tailctl
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
