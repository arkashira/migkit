"""The MySQL engine opens every connection one way, keeps one a side and
thread for its questions, and pins every session to UTC (audit
2026-10-01, DU12, SP2, LG4, W11).

Connections were opened six ways - some with keepalive, one with TLS from
the options alone - each `_q` a connection of its own signed in again,
`run_rule` read in each server's own zone, and the deep check's zone pin
was a `set time_zone` whose failure was passed over. A TIMESTAMP is
handed out and taken in the session's zone, so a source and a target in
two zones read one instant as two values.
"""
import pymysql
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import mysql as my
from migkit.engines.mysql import MySQLEngine
from tests import servers
from tests.conftest import needs_docker

pair = servers.fixture("mysql")


class _Cur:
    def __init__(self, conn):
        self.conn = conn
        self.description = (("x",),)

    def execute(self, sql, args=None):
        self.conn.ran.append(sql)
        if self.conn.fail:
            err = self.conn.fail.pop(0)
            if err is not None:
                self.conn.open = False
                raise err
        return 1

    def fetchall(self):
        return ((1,),)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def __init__(self, fail=()):
        self.ran, self.fail, self.open, self.closed = [], list(fail), True, 0

    def cursor(self, *a):
        return _Cur(self)

    def ping(self, reconnect=False):
        if not self.open:
            raise pymysql.err.OperationalError(2006, "gone away")

    def close(self):
        self.closed += 1
        self.open = False

    def commit(self):
        pass

    def rollback(self):
        pass


def _engine(**opts):
    return MySQLEngine(Hop(
        name="h", engine="mysql",
        source=Endpoint(host="127.0.0.1", port=3306, user="u",
                        password="p", options=opts),
        target=Endpoint(host="127.0.0.1", port=3307, user="u",
                        password="p", options=opts)))


def test_every_connection_is_opened_one_way(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("not read: the driver is not called")
    monkeypatch.setenv("MIGKIT_READ_TIMEOUT", "77")
    monkeypatch.setenv("MIGKIT_KEEPALIVE_IDLE", "33")
    seen, idle = [], []

    def connect(**kw):
        seen.append(kw)
        c = _Conn()
        c._sock = object()
        return c
    monkeypatch.setattr(pymysql, "connect", connect)
    monkeypatch.setattr(my, "_loading", lambda: None)
    monkeypatch.setattr(my, "_keepalive",
                        lambda sock, idle=60: idle_seen(idle))

    def idle_seen(n):
        idle.append(n)
    eng = _engine(ssl_ca=str(ca))
    read = eng._conn("src")
    eng._conn("dst")
    eng._connect("src", "zone")
    eng._connect("dst", "kept")
    utc = "time_zone = '+00:00'"
    assert seen[0]["init_command"] == f"set session {utc}"
    assert utc in seen[1]["init_command"] and "sql_mode" in \
        seen[1]["init_command"]
    # what asks the server for its own clock keeps the server's zone
    assert seen[2]["init_command"] is None
    assert seen[3]["autocommit"] is True and seen[0]["autocommit"] is False
    # the hop's TLS on every one, from the one place that renders it
    assert all(k["ssl_ca"] == str(ca) and k["ssl_verify_cert"] is True
               for k in seen), seen
    assert read._read_timeout == 77 and idle == [33, 33, 33, 33]
    assert all(k["connect_timeout"] == 15 for k in seen)


def _stubbed(monkeypatch, conns):
    """An engine whose connections are `conns`, handed out in order."""
    from migkit import util
    monkeypatch.setattr(util.time, "sleep", lambda s: None)
    eng = _engine()
    given = []

    def connect(side, purpose="read", retry=True):
        given.append(purpose)
        return conns.pop(0)
    eng._connect = connect
    return eng, given


def test_a_read_rides_the_kept_connection(monkeypatch):
    kept = _Conn()
    eng, given = _stubbed(monkeypatch, [kept])
    for _ in range(5):
        eng._q("src", "select 1")
        eng._q_named("src", "show replica status")
    assert given == ["kept"] and len(kept.ran) == 10


def test_each_thread_keeps_its_own(monkeypatch):
    import threading
    a, b = _Conn(), _Conn()
    eng, given = _stubbed(monkeypatch, [a, b])
    eng._q("src", "select 1")
    t = threading.Thread(target=lambda: eng._q("src", "select 2"))
    t.start()
    t.join()
    assert a.ran == ["select 1"] and b.ran == ["select 2"]
    assert given == ["kept", "kept"]


def test_a_lost_kept_connection_is_opened_again_and_the_read_asked_again(
        monkeypatch):
    lost = _Conn(fail=[pymysql.err.OperationalError(
        2013, "Lost connection to MySQL server during query")])
    fresh = _Conn()
    eng, given = _stubbed(monkeypatch, [lost, fresh])
    assert eng._q("src", "select 1") == ((1,),)
    assert given == ["kept", "kept"] and fresh.ran == ["select 1"]


def test_a_statement_that_writes_is_never_sent_twice(monkeypatch):
    """A DDL resent after `Lost connection` may have been applied: only a
    failure to connect is tried again (W11)."""
    lost = _Conn(fail=[pymysql.err.OperationalError(
        2013, "Lost connection to MySQL server during query")])
    eng, given = _stubbed(monkeypatch, [lost, _Conn()])
    with pytest.raises(pymysql.err.OperationalError):
        eng._q("dst", "alter table t add column c int")
    # on a connection of its own, and once
    assert given == ["read"] and lost.ran == [
        "alter table t add column c int"]


def test_a_script_through_the_client_is_never_sent_twice(monkeypatch):
    got = {}

    def run(argv, **kw):
        got.update(kw, argv=argv)
    monkeypatch.setattr(my, "run", run)
    _engine()._target_client("app", "alter table t add c int;")
    assert got["writes"] is True and got["env"] == {"MYSQL_PWD": "p"}
    assert "p" not in got["argv"]


@needs_docker
def test_a_timestamp_crosses_two_zones_as_one_instant(pair, tmp_path):
    """The source in +07:00, the target in -05:00: a TIMESTAMP read off
    one and written into the other is the same instant, and a rule and
    the deep check read both in UTC; what asks the server for its clock
    still reads the server's own zone."""
    for side, zone in (("src", "+07:00"), ("dst", "-05:00")):
        getattr(pair, side).sql(f"set global time_zone = '{zone}'")
        getattr(pair, side).sql(
            "create table z (id int primary key, ts timestamp(6) null,"
            " dt datetime(6) null)", pair.name)
    pair.src.sql("set time_zone = '+00:00'; insert into z values"
                 " (1, '2026-01-01 00:00:00', '2026-01-01 00:00:00')",
                 pair.name)
    hop = Hop(name="z", engine="mysql", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[pair.name])
    hop.report_dir = lambda d=None: tmp_path
    eng = MySQLEngine(hop)
    cols = [("id", "integer"), ("ts", "timestamp"), ("dt", "timestamp")]
    rows, _ = eng.neutral_read("src", pair.name, "z", cols)
    eng.neutral_write("dst", pair.name, "z", cols, rows)

    def instant(side):
        return getattr(pair, side).sql(
            "set time_zone = '+00:00'; select unix_timestamp(ts),"
            " cast(dt as char) from z", pair.name)[0]
    assert instant("dst") == instant("src"), (instant("src"),
                                              instant("dst"))
    for side in ("src", "dst"):
        assert eng.run_rule(side, pair.name, "select @@session.time_zone"
                            ) == (("+00:00",),)
        assert eng._q(side, "select @@session.time_zone") == (("+00:00",),)
    assert eng.zone_evidence("src", pair.name)[0] == 420
    assert eng.zone_evidence("dst", pair.name)[0] == -300
    assert eng._q_server_zone("src", "select @@session.time_zone") == (
        ("+07:00",),)
    eng.release_kept()
