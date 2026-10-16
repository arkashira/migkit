"""A wait the server stopped answering is told from a slow answer, and only
the attempt's own idle session is closed (backlog P0-CONNECTION-PROGRESS).
On fakes of the server's session view: no database."""
import json
import socket
from types import SimpleNamespace

import pytest

from migkit import attempts, decide, util
from migkit.attempts import Ledger, Monitor, View


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakeEngine:
    """Sessions as the server lists them, and what was closed."""

    def __init__(self, views, user="miguser"):
        self.views, self.user, self.closed = views, user, []

    def session_view(self, side, session):
        got = self.views[session]
        if isinstance(got, Exception):
            raise got
        return got

    def owns_session(self, side, view):
        return view.owner == self.user

    def close_session(self, side, session, view):
        self.closed.append((side, session))
        self.views[session] = View(session, present=False)
        return True


def _monitor(engine, ledger=None):
    clock, said = Clock(), []
    mon = Monitor(engine, ledger or Ledger(), clock=clock, say=said.append,
                  thread=False)
    for d in (8.0, 10.0, 30.0):
        mon.ledger.measured("a verify read", d)
    return mon, clock, said


def test_patience_is_measured_never_written():
    secs, got = decide.session_patience([10, 12, 30])
    assert got.path == "measured" and secs == 30 + (30 - 12)
    secs, got = decide.session_patience([10], deadline=3600)
    assert got.path == "deadline" and secs == 3600
    assert "fewer than two" in got.reason
    secs, got = decide.session_patience([], predicted=40.0, deadline=3600)
    assert got.path == "predicted" and secs == 40.0
    secs, got = decide.session_patience([])
    assert secs is None and got.rung is None


def test_each_wait_is_named_from_the_server_and_the_client():
    a = attempts.Attempt("src", "op")
    assert attempts.classify(View(1, state="idle"), a) == attempts.IDLE
    assert attempts.classify(View(1, state="running"), a) == attempts.COMPUTING
    assert attempts.classify(View(1, state="sending"), a) == attempts.TRANSFER
    assert attempts.classify(View(1, state="locked"), a) == attempts.LOCKED
    assert attempts.classify(View(1, present=False), a) == attempts.GONE
    assert attempts.classify(View(1, error="no"), a) == attempts.UNAVAILABLE
    assert attempts.classify(None, a) == attempts.UNAVAILABLE
    a.stage = "reading"
    assert attempts.classify(View(1, state="sending"), a) == attempts.CLIENT_STALL
    assert attempts.classify(View(1, state="idle"), a) == attempts.BETWEEN


def test_an_idle_session_is_closed_and_only_that_one():
    eng = FakeEngine({42: View(42, "miguser", "idle", 60.0),
                      43: View(43, "miguser", "running", 60.0)})
    mon, clock, said = _monitor(eng)
    idle = mon.begin("src", "a verify read", "shop.orders", "p1", session=42)
    busy = mon.begin("src", "a verify read", "shop.items", "p2", session=43)
    assert idle.patience == 30 + (30 - 10)
    clock.now += idle.patience - 1
    mon.check()
    assert eng.closed == [] and idle.looked == 0
    clock.now += 2
    mon.check()
    assert eng.closed == [] and idle.idle_reads == 1
    eng.views[42] = View(42, "miguser", "idle", 75.0)
    clock.now += idle.patience / 4
    mon.check()
    assert eng.closed == [("src", 42)]
    assert busy.stage == "waiting" and busy.closes == 0
    assert idle.stage == "closed" and idle.closes == 1
    assert len(said) == 1
    line = said[0]
    assert "session 42 sat idle 75s" in line
    assert "Closed that session only" in line
    assert "read again from its last recorded position" in line
    mon.done(idle, False, "OperationalError 2013")
    rec = mon.ledger.earlier(idle.key)
    assert rec["reason"].startswith("the server's session sat idle")
    assert rec["closes"] == 1 and rec["unprogressed"] == 1


def test_an_idle_server_within_the_wait_measured_is_left_alone():
    eng = FakeEngine({7: View(7, "miguser", "idle", 5.0)})
    mon, clock, said = _monitor(eng)
    a = mon.begin("src", "a verify read", "t", "p", session=7)
    for _ in range(4):
        clock.now += a.patience
        mon.check()
    assert eng.closed == []
    assert "within the 50s measured" in said[0]


def test_a_slow_legitimate_read_is_never_closed_nor_said():
    eng = FakeEngine({9: View(9, "miguser", "running", 900.0)})
    mon, clock, said = _monitor(eng)
    a = mon.begin("src", "a verify read", "t", "p", session=9)
    for _ in range(6):
        clock.now += a.patience
        mon.check()
    assert eng.closed == [] and said == [] and a.kind == attempts.COMPUTING


def test_a_write_left_idle_is_said_never_closed():
    eng = FakeEngine({5: View(5, "miguser", "idle", 500.0)})
    mon, clock, said = _monitor(eng)
    a = mon.begin("dst", "a verify read", "t", "p", session=5, reads=False)
    clock.now += a.patience
    mon.check()
    clock.now += a.patience
    mon.check()
    assert eng.closed == []
    assert "a write closed under it is left uncertain" in said[0]
    assert "on the target" in said[0]


def test_another_accounts_session_is_never_closed():
    eng = FakeEngine({5: View(5, "someone", "idle", 500.0)})
    mon, clock, said = _monitor(eng)
    a = mon.begin("src", "a verify read", "t", "p", session=5)
    for _ in range(3):
        clock.now += a.patience
        mon.check()
    assert eng.closed == []
    assert "not exactly this attempt's" in said[0]


def test_an_unreadable_view_says_the_grant_and_closes_nothing():
    eng = FakeEngine({5: PermissionError("access denied")})
    mon, clock, said = _monitor(eng)
    a = mon.begin("src", "a verify read", "t", "p", session=5)
    clock.now += a.patience
    mon.check()
    assert eng.closed == [] and a.kind == attempts.UNAVAILABLE
    assert "cannot be read" in said[0] and "grant" in said[0]


def test_a_unit_closed_without_progress_is_not_closed_again():
    eng = FakeEngine({1: View(1, "miguser", "idle", 500.0)})
    mon, clock, said = _monitor(eng)

    def stall(session):
        eng.views[session] = View(session, "miguser", "idle", 500.0)
        a = mon.begin("src", "a verify read", "t", "p", session=session)
        clock.now += a.patience
        mon.check()
        clock.now += a.patience
        mon.check()
        mon.done(a, False, "OperationalError 2013")
        return a
    first = stall(1)
    second = stall(2)
    assert eng.closed == [("src", 1)]
    assert first.closes == 1 and second.retries == 1
    assert "closed once already and the retry made no progress" in said[-1]
    # progress clears the bound: the unit may be closed again later
    third = mon.begin("src", "a verify read", "t", "p", session=3)
    mon.progressed(third, "p")
    mon.done(third, True)
    stall(4)
    assert eng.closed == [("src", 1), ("src", 4)]


def test_a_session_the_server_dropped_ends_migkits_own_end():
    ended = []
    eng = FakeEngine({8: View(8, present=False)})
    mon, clock, said = _monitor(eng)
    a = mon.begin("src", "a verify read", "t", "p", session=8,
                  abandon=lambda: ended.append(8))
    clock.now += a.patience
    mon.check()
    assert ended == [8] and eng.closed == [] and a.stage == "closed"
    assert "no longer has session 8" in said[0]


def test_the_ledger_keeps_attempts_without_statements(tmp_path):
    path = tmp_path / "attempts.json"
    eng = FakeEngine({42: View(42, "miguser", "idle", 500.0)})
    mon, clock, said = _monitor(eng, Ledger(str(path)))
    a = mon.begin("src", "a verify read", "shop.orders", "digest1",
                  session=42, transport={"read_deadline": 3600,
                                         "keepalive": True})
    clock.now += a.patience
    mon.check()
    clock.now += a.patience
    mon.check()
    mon.done(a, False, "OperationalError 2013")
    text = path.read_text()
    assert "select" not in text.lower() and "password" not in text.lower()
    rec = json.loads(text)["attempts"][a.key]
    for k in ("started", "session", "operation", "position", "sent",
              "answered", "progress", "reason", "transport"):
        assert k in rec
    assert rec["session"] == "42" and rec["position"] == "digest1"
    again = Ledger(str(path))
    assert again.waits("a verify read") == [8.0, 10.0, 30.0]
    assert again.earlier(a.key)["closes"] == 1


def test_keepalive_is_read_back_off_the_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        asked = util.keepalive(s, idle=61, interval=11, count=4)
        got = attempts.transport(s, 3600, asked)
    finally:
        s.close()
    assert asked == {} and got["keepalive"] is True
    assert got["idle"] == 61 and got["read_deadline"] == 3600
    assert util.keepalive(None) == {"error": "no socket to set"}

    class Refusing:
        def setsockopt(self, *a):
            raise OSError(22, "Invalid argument")
    assert "keepalive not set" in util.keepalive(Refusing())["error"]


# --- MySQL: the process list, KILL CONNECTION, and the retry -------------

def _mysql(views, user="miguser"):
    from migkit.engines.mysql import MySQLEngine
    eng = object.__new__(MySQLEngine)
    eng.hop = SimpleNamespace(source=SimpleNamespace(user=user),
                              target=SimpleNamespace(user=user),
                              report_dir=lambda db="": None)
    killed = []

    class Cur:
        def __init__(self):
            self.row = None

        def execute(self, sql, args=None):
            if sql.startswith("KILL"):
                killed.append(sql)
                views.pop(int(sql.split()[-1]), None)
            elif "USER, COMMAND, TIME, STATE" in sql:
                self.row = views.get(args[0])
            else:
                self.row = (args[0],) if args and args[0] in views else None

        def fetchone(self):
            return self.row

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    class Control:
        _read_timeout = 15

        def cursor(self):
            return Cur()

        def close(self):
            pass
    eng._conn = lambda side, retry=True: Control()
    return eng, killed


def test_mysql_reads_the_process_list_without_the_statement():
    eng, killed = _mysql({42: ("miguser", "Sleep", 61, ""),
                          43: ("miguser", "Query", 5, "Sending to client"),
                          44: ("miguser", "Query", 5,
                               "Waiting for table metadata lock"),
                          45: ("miguser", "Query", 5, "executing")})
    assert eng.session_view("src", 42) == View(42, "miguser", "idle", 61.0)
    assert eng.session_view("src", 43).state == "sending"
    assert eng.session_view("src", 44).state == "locked"
    assert eng.session_view("src", 45).state == "running"
    assert eng.session_view("src", 46).present is False
    assert eng.SESSION_VIEW != "not yet"


def test_mysql_closes_only_an_owned_session_still_idle():
    views = {42: ("miguser", "Sleep", 61, ""),
             43: ("someone", "Sleep", 61, ""),
             44: ("miguser", "Query", 1, "executing")}
    eng, killed = _mysql(views)
    seen = View(42, "miguser", "idle", 60.0)
    assert eng.close_session("src", 43, View(43, "someone", "idle", 60.0)) \
        is False
    assert eng.close_session("src", 44, View(44, "miguser", "idle", 60.0)) \
        is False
    assert eng.close_session("src", 42, seen) is True
    assert killed == ["KILL CONNECTION 42"]


def test_mysql_read_resumes_after_its_idle_session_is_closed(monkeypatch):
    import pymysql.err
    views = {101: ("miguser", "Sleep", 500, "")}
    eng, killed = _mysql(views)
    clock, said = Clock(), []
    mon = Monitor(eng, Ledger(), clock=clock, say=said.append, thread=False)
    for d in (8.0, 10.0, 30.0):
        mon.ledger.measured("a verify read", d)
    eng._attempts = mon
    monkeypatch.setattr(util, "_backoff", lambda attempt, base=0.8: 0.0)
    control = eng._conn

    class Data:
        def __init__(self, tid):
            self.tid = tid

        def thread_id(self):
            return self.tid

        def cursor(self):
            tid = self.tid

            class Cur:
                def mogrify(self, sql, args=None):
                    return sql

                def execute(self, sql, args=None):
                    if tid == 101:
                        # the wait: the watcher reads the server twice
                        clock.now += 50
                        mon.check()
                        clock.now += 50
                        mon.check()
                        raise pymysql.err.OperationalError(
                            2013, "Lost connection to MySQL server")

                def fetchall(self):
                    return [(3, 99)]

                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    pass
            return Cur()

        def close(self):
            pass
    data = [Data(101), Data(102)]
    eng._conn = lambda side, retry=True: data.pop(0) if retry else control()
    with eng.attempt_unit("a verify read", "shop.orders", "d1"):
        assert eng._q("src", "select count(*) from t") == [(3, 99)]
    assert killed == ["KILL CONNECTION 101"]
    rec = mon.ledger.earlier("src|a verify read|shop.orders|d1")
    assert rec["retries"] == 1 and rec["unprogressed"] == 0
    assert rec["stage"] == "done" and rec["session"] == "102"
    assert "Closed that session only" in said[0]


@pytest.mark.parametrize("sql,reads", [
    ("select 1", True), ("  SHOW tables", True),
    ("select * from t for update", False), ("delete from t", False),
    ("select a into outfile '/x' from t", False)])
def test_mysql_closes_only_under_reads(sql, reads):
    from migkit.engines.mysql import MySQLEngine
    assert MySQLEngine._reads(sql) is reads


# --- PostgreSQL: pg_stat_activity and pg_terminate_backend ---------------

def test_postgres_reads_pg_stat_activity_and_terminates_only_owned():
    from migkit.engines.postgres import PostgresEngine
    eng = object.__new__(PostgresEngine)
    eng.hop = SimpleNamespace(source=SimpleNamespace(user="miguser"),
                              target=SimpleNamespace(user="miguser"))
    rows = {7: "miguser|idle|Client|ClientRead|125.5",
            8: "miguser|active|Client|ClientWrite|3",
            9: "miguser|active|Lock|relation|3",
            10: "other||||0"}
    sent = []

    def psql(side, db, sql, *a, **k):
        sent.append(sql)
        if "pg_terminate_backend" in sql:
            rows.pop(int(sql.split("(")[1].split(")")[0]))
            return "t"
        pid = int(sql.rsplit("=", 1)[1])
        return rows.get(pid, "")
    eng._psql = psql
    assert eng.session_view("src", 7) == View(7, "miguser", "idle", 125.5)
    assert eng.session_view("src", 8).state == "sending"
    assert eng.session_view("src", 9).state == "locked"
    assert "pg_read_all_stats" in eng.session_view("src", 10).error
    assert eng.session_view("src", 11).present is False
    assert eng.close_session("src", 9, View(9, "miguser", "idle", 1)) is False
    assert eng.close_session("src", 7, View(7, "miguser", "idle", 100)) is True
    assert sum("pg_terminate_backend(7)" in s for s in sent) == 1
    assert not any("query" in s for s in sent)


def test_an_engine_without_a_session_view_declares_it():
    from migkit.engines.base import Engine
    eng = Engine(SimpleNamespace(source=None, target=None))
    v = eng.session_view("src", 1)
    assert v.error.startswith("not yet")
    assert attempts.classify(v, attempts.Attempt("src", "op")) \
        == attempts.UNAVAILABLE
    assert eng.close_session("src", 1, v) is False


# --- range units: one statement each, and move health from the server ------

def _range_unit(ledger, name="public.t r:10000", parts=3):
    ledger.unit_opened(name, "move", "range", "public.t", parts)
    return name


def test_a_unit_spanning_two_statements_is_refused():
    led = Ledger()
    name = _range_unit(led)
    led.unit_statement(name, "load a1", "dst", 101)
    # its retry is the same statement on a new session: kept, not refused
    led.unit_statement(name, "load a1", "dst", 102)
    assert led.unit_record(name)["sessions"] == [["dst", "101"],
                                                 ["dst", "102"]]
    with pytest.raises(attempts.Spanning):
        led.unit_statement(name, "load b2", "dst", 103)
    assert led.unit_record(name)["statement"] == "load a1"


def test_a_range_whose_session_is_gone_is_replayed_and_nothing_closed():
    eng = FakeEngine({"101": View("101", present=False)})
    mon, _, said = _monitor(eng)
    name = _range_unit(mon.ledger)
    mon.ledger.unit_statement(name, "load a1", "dst", 101)
    got = mon.unit_health(name, keepalive=60.0)
    assert got.path == "replay"
    assert "no longer lists" in got.reason and "idle 0.0s" in got.reason
    assert eng.closed == [] and mon.closed == []
    rec = mon.ledger.unit_record(name)
    assert rec["stalls"] == 1 and "no longer lists" in rec["stalled"]
    assert any("stalled" in s and "replayed" in s for s in said)


def test_only_the_range_idle_past_the_observed_keepalive_is_closed():
    eng = FakeEngine({
        "101": View("101", "miguser", "idle", 75.0),
        "202": View("202", "miguser", "idle", 75.0)})
    mon, _, _ = _monitor(eng)
    a, b = _range_unit(mon.ledger), _range_unit(mon.ledger, "public.t r:20000")
    mon.ledger.unit_statement(a, "load a1", "dst", 101)
    mon.ledger.unit_statement(b, "load b2", "dst", 202)
    keep = attempts.keepalive_seconds(
        {"keepalive": True, "idle": 30, "interval": 10, "count": 3})
    assert keep == 60.0
    got = mon.unit_health(a, keepalive=keep)
    assert got.path == "close"
    assert "idle 75.0s" in got.reason and "lists the session" in got.reason
    assert "60.0s keepalive" in got.reason
    # exactly that session: the other range's stays
    assert eng.closed == [("dst", "101")] and "202" in eng.views
    assert mon.ledger.unit_record(b).get("stalls") is None


def test_a_range_idle_within_the_keepalive_or_unobserved_waits():
    eng = FakeEngine({"101": View("101", "miguser", "idle", 20.0)})
    mon, _, said = _monitor(eng)
    name = _range_unit(mon.ledger)
    mon.ledger.unit_statement(name, "load a1", "dst", 101)
    got = mon.unit_health(name, keepalive=60.0)
    assert got.path == "wait" and "idle 20.0s" in got.reason
    assert "lists the session" in got.reason
    got = mon.unit_health(name, keepalive=None)
    assert got.path == "wait" and "no keepalive observed" in got.reason
    # a session the hop's account does not own is never closed
    eng.views["101"] = View("101", "someone", "idle", 500.0)
    got = mon.unit_health(name, keepalive=60.0)
    assert got.path == "wait" and eng.closed == [] and said == []
    assert decide.range_stall({"listed": True, "state": "idle",
                               "idle_seconds": 500.0, "keepalive": 60.0,
                               "owned": True, "open": False}).path == "wait"
