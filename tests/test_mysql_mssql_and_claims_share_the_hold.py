"""MySQL's range readers start at one moment, SQL Server reads its write
states back after the hold, the machines sharing a table hold it under one
record, the cross-engine copy writes a large value's way a row a statement
and names the grant statement on the side that refused. Fakes only."""
import json
import threading

import pytest

from migkit import claims, tuning
from migkit.decide import Blocked
from migkit.engines import base, enterprise_connectors as ec, mysql
from migkit.engines.hetero import HeteroEngine
from migkit.engines.mssql import MSSQLEngine


class _Hop:
    engine = "mssql"


def _mssql(states):
    eng = object.__new__(MSSQLEngine)
    eng.hop = _Hop()
    asked = []

    def run_rule(side, db, sql):
        asked.append(sql)
        return list(states.pop(0).items())
    eng.run_rule = run_rule
    eng.load_window = lambda db, log, tables: _Window()
    eng.session_hold = lambda db: {}
    return eng, asked


class _Window:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_sql_server_reads_its_triggers_and_keys_back_after_the_hold():
    eng, asked = _mssql([{"trigger dbo.audit": 1, "key fk_a": 1},
                         {"trigger dbo.audit": 0, "key fk_a": 1}])
    with pytest.raises(Blocked, match="trigger dbo.audit"):
        with eng.table_hold("app", "dbo.orders"):
            pass
    assert "s.name = 'dbo'" in asked[0] and "t.name = 'orders'" in asked[0]
    assert "sys.foreign_keys" in asked[0] and "is_not_trusted" in asked[0]


def test_a_hold_as_it_was_passes_and_one_not_the_last_does_not_check():
    eng, _ = _mssql([{"trigger dbo.audit": 1}, {"trigger dbo.audit": 1}])
    with eng.table_hold("app", "dbo.orders") as h:
        assert h == {}
    eng, asked = _mssql([{"trigger dbo.audit": 0}])
    hold = eng.table_hold("app", "dbo.orders", before={"trigger dbo.audit":
                                                       True})
    with hold:
        hold.check = False
    # the shared states stood in for a read before; the one after is read
    assert len(asked) == 1


def test_the_machines_holding_a_table_share_one_reading_and_the_last_checks():
    rec, before = claims.held_in({"state": {}}, "a", {"trigger t": True})
    rec, later = claims.held_in(rec, "b", {"trigger t": False})
    assert before == later == {"trigger t": True}
    rec, last = claims.held_out(rec, "a")
    assert last is False and rec["holding"] == ["b"]
    rec, last = claims.held_out(rec, "b")
    assert last is True


class _Cur:
    def __init__(self, answers, said):
        self.answers, self.said = answers, said

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, args=None):
        self.said.append((sql, args))

    def fetchone(self):
        return self.answers.pop(0)


class _Conn:
    def __init__(self, answers):
        self.said, self.answers, self.closed = [], answers, False

    def cursor(self, *a):
        return _Cur(self.answers, self.said)

    def close(self):
        self.closed = True


def _reader(conn):
    eng = object.__new__(mysql.MySQLEngine)
    eng._conn = lambda side, retry=True: conn
    return eng


def test_a_mysql_reader_adopts_the_moment_only_while_its_lock_is_held():
    token = json.dumps({"lock": "m", "holder": 7, "mark": "u:1-5",
                        "readers": 2})
    conn = _Conn([(7,), (1,)])
    eng = _reader(conn)
    eng.adopt_moment("app", token)
    assert eng._moment_source("app") is conn
    assert conn.said[0][0] == "start transaction with consistent snapshot"
    assert conn.said[2] == ("select get_lock(%s, 0)", ("m-0",))
    # kept for the thread's next range
    assert eng._moment_source("app") is conn
    assert mysql.MySQLEngine.moment_mark(token) == "u:1-5"
    mysql._moment_done(token)
    assert conn.closed

    late = _Conn([(None,)])
    eng = _reader(late)
    eng.adopt_moment("app", token)
    got = []
    t = threading.Thread(target=lambda: got.append(
        eng._moment_source("app")))
    t.start()
    t.join()
    assert got == [None] and late.closed
    assert not any("get_lock" in s for s, _ in late.said)
    mysql._moment_done(token)


def test_the_moment_is_asked_for_the_tables_and_readers_where_taken():
    asked = {}

    class Src(base.Engine):
        def __init__(self):
            pass

        def export_moment(self, db, tables=(), readers=1):
            asked.update(tables=tables, readers=readers)
            return "tok", lambda: asked.update(closed=True)
    with Src().read_moment("app", tables=["t"], readers=4) as tok:
        assert tok == "tok"
    assert asked == {"tables": ["t"], "readers": 4, "closed": True}


def _pair():
    eng = object.__new__(HeteroEngine)

    class Side:
        CANON_ENGINE = "mysql"
        hop = _Hop()

        def account_grant(self, side, privilege, db, table):
            return f"{privilege} ON {db}.{table}"

        def grant_statement(self, side, privilege, on):
            return f"GRANT {privilege} ON {on} TO app"
    eng.src_engine, eng.dst_engine = Side(), Side()
    return eng


def test_a_refused_write_names_the_target_and_the_statement_to_run_there():
    err = Exception(1142, "INSERT command denied to user 'app'@'%'")
    with pytest.raises(ec.MissingGrant) as got:
        _pair()._refused("dst", "copying rows in", "app", "orders",
                         "INSERT, SELECT", err)
    text = str(got.value)
    assert "the target" in text and "GRANT INSERT, SELECT ON app.orders" \
        in text
    # anything else is the caller's to raise
    assert _pair()._refused("src", "reading rows", "app", "t", "SELECT",
                            ValueError("bad value")) is None


def test_a_large_value_way_writes_a_row_a_statement(monkeypatch):
    eng = object.__new__(HeteroEngine)
    parts = []
    eng._write_checked = lambda *a: parts.append(len(a[6]))
    monkeypatch.setattr(tuning, "sized", lambda r, u, w, *a, **k: w())

    class Retune:
        def __init__(self, way):
            self.way = way

        def settings(self):
            return {"rows": 100, "parts": 1, "bytes": 1 << 30,
                    "way": self.way}
    rows = [[i, "x"] for i in range(5)]
    eng._write_sized("app", "k", "t", [], None, ["id"], rows,
                     Retune("pieces"), "u")
    assert parts == [1] * 5
    parts.clear()
    eng._write_sized("app", "k", "t", [], None, ["id"], rows,
                     Retune("whole"), "u")
    assert parts == [5]
