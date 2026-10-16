"""A SQL Server source is followed through Change Data Capture where the
database captures and its Agent runs, through Change Tracking otherwise,
and through its own log where neither is on (backlog MSSQL-x86, LT.R2,
LT.R3): the way is a rung (`decide.mssql_follow_way`) on what the server
says, never a default, and the token a way mints is the way it is read
on after a stop.

The capture's reads - `sys.fn_cdc_get_max_lsn`, `fn_cdc_get_min_lsn`, the
`cdc.fn_cdc_get_all_changes_<instance>` rows - are replayed from the
answers the server's documentation shows: the sandbox is SQL Server's
edge edition, which has no Agent and refuses to switch capture on (its
refusal is asserted on the `sqlserver` lane, recorded below). There the
ladder is run against the edition itself: with tracking on it follows
through tracking, and with neither it follows the live log through the
one hook (`MSSQLEngine.log_follower`). Built, unproved on a full SQL
Server (`mssql.UNPROVED_ON`).
"""
import subprocess
import time

import pytest

from migkit import decide
from migkit.config import Endpoint, Hop
from migkit.engines.mssql import MSSQLEngine
from tests import servers
from tests.conftest import mssql_run, needs_docker

#: what the edge edition answered to `exec sys.sp_cdc_enable_db`
#: (2026-10-16, the `sqlserver` lane): capture is not in this edition
EDGE_CDC_REFUSAL = ("sp_cdc_enable_db", "22988", "not available in every"
                    " edition", "Change data capture is only available")


def _lsn(n):
    return int(n).to_bytes(10, "big")


def _hex(n):
    return "0x" + _lsn(n).hex().upper()


class _Fake(MSSQLEngine):
    def __init__(self, cdc=True, agent="Running", captured=True,
                 tracking=False, tracked=True, edition=3, live=False,
                 changes=(), top=0, low=1):
        ep = Endpoint(host="10.0.0.5", port=1433, user="u",
                      password="CHANGE_ME")
        super().__init__(Hop(name="ms", engine="mssql", source=ep,
                             target=ep, databases=["app"]))
        self.cdc, self.agent, self.captured = cdc, agent, captured
        self.tracking, self.tracked, self.edition = tracking, tracked, edition
        self.live, self.changes, self.top, self.low = live, list(changes), \
            top, low
        self.asked = []

    def _rows(self, side, db, sql, args=()):
        if "is_cdc_enabled" in sql:
            return [(int(self.cdc), int(self.tracking), self.edition)]
        if "dm_server_services" in sql:
            if self.agent is None:
                raise RuntimeError("VIEW SERVER STATE permission was denied")
            return [(self.agent,)]
        if "cdc.change_tables" in sql:
            return [("dbo.orders", "dbo_orders")] if self.captured else []
        if "sys.change_tracking_tables" in sql:
            return [("dbo.orders",)] if self.tracked else []
        if "top 1 [Current LSN]" in sql:
            if not self.live:
                raise RuntimeError("The user does not have permission")
            return [("00000025:00000010:0001",)]
        if "recovery_model_desc" in sql:
            return [("SIMPLE",)]
        if "change_tracking_current_version" in sql:
            return [(7 if self.tracking else None,)]
        if "fn_cdc_get_max_lsn" in sql:
            return [(_lsn(self.top) if self.top else None,)]
        if "fn_cdc_get_min_lsn" in sql:
            return [(_lsn(self.low),)]
        if "fn_cdc_get_all_changes_dbo_orders" in sql:
            self.asked.append(tuple(args))
            to = int(args[1], 16)
            # as a server replaying a range wider than asked would: every
            # row up to the top, the ones already handed on among them
            return [r for r in self.changes if int.from_bytes(r[0], "big")
                    <= to]
        raise AssertionError(sql)

    def neutral_tables(self, side, db):
        return ["dbo.orders"]

    def in_scope(self, db, t):
        return True

    def neutral_key(self, side, db, t):
        return ["id"]

    def neutral_columns(self, side, db, t):
        return [("id", "int"), ("amount", "int")]


def _row(at, seq, op, i, amount):
    return (_lsn(at), _lsn(seq), op, 3, i, amount)


def test_the_follow_rung_turns_on_whether_the_capture_and_its_agent_run():
    assert _Fake().follow_way("src", "app").path == "capture"
    # the Agent stopped: nothing fills the change tables
    got = _Fake(agent="Stopped", tracking=True).follow_way("src", "app")
    assert got.path == "tracking" and "Agent is not running" in got.reason
    # the Agent not visible to the login is not taken as running
    assert _Fake(agent=None, tracking=True).follow_way(
        "src", "app").path == "tracking"
    # Azure SQL Database runs the capture itself, no Agent asked
    assert _Fake(agent="Stopped", edition=5).follow_way(
        "src", "app").path == "capture"
    # capture on, a table in scope with no capture instance
    got = _Fake(captured=False, tracking=True).follow_way("src", "app")
    assert got.path == "tracking" and "only 0 of the 1" in got.reason
    # neither on: the source's own log, reached through the one hook
    assert _Fake(cdc=False, live=True).follow_way("src", "app").path == "log"
    got = _Fake(cdc=False).follow_way("src", "app")
    assert got.rung is None and "Change Tracking is off" in got.reason
    with pytest.raises(SystemExit, match="no way to follow it holds"):
        _Fake(cdc=False).change_point("src", "app")


def test_a_capture_stopped_and_resumed_goes_on_from_its_saved_lsn():
    rows = [_row(100, 1, 2, 1, 10), _row(100, 2, 2, 2, 20),
            _row(110, 1, 3, 1, 10), _row(110, 2, 4, 1, 11),
            _row(120, 1, 1, 2, 20), _row(130, 1, 2, 3, 30)]
    eng = _Fake(changes=rows, top=130)
    start = eng.change_point("src", "app")
    assert start == {"cdc": _hex(130)}
    # a tail that started at 99 reads whole transactions, cut by the limit
    # at a commit's end, never inside one
    first, at = eng.neutral_changes("src", "app", {"cdc": _hex(99)}, limit=3)
    assert [(c["op"], c["key"]["id"]) for c in first] == [
        ("insert", 1), ("insert", 2), ("update", 1)]
    assert at == {"cdc": _hex(110)}
    assert eng.asked[-1] == (_hex(99), _hex(130))
    # stopped here; started again on a fresh engine from the saved token:
    # the server hands 100 and 110 again, and they are left out
    again = _Fake(changes=rows, top=130)
    rest, end = again.neutral_changes("src", "app", at)
    assert [(c["op"], c["key"]["id"]) for c in rest] == [
        ("delete", 2), ("insert", 3)]
    assert rest[0]["values"] is None or not rest[0]["values"]
    assert end == {"cdc": _hex(130)}
    # and once more: nothing new, nothing twice
    assert again.neutral_changes("src", "app", end) == ([], end)
    assert again.position_reached(end, start) is True
    assert again.position_reached(at, start) is False
    assert again.position_reached(end, 5) is None  # another feed's


def test_a_capture_cleaned_up_past_the_position_stops_named():
    eng = _Fake(changes=[_row(500, 1, 2, 9, 1)], top=500, low=300)
    with pytest.raises(SystemExit, match="no longer keeps the changes of"
                       " dbo.orders"):
        eng.neutral_changes("src", "app", {"cdc": _hex(120)})
    assert eng.position_lost("src", "app", {"cdc": _hex(120)})
    assert eng.position_lost("src", "app", {"cdc": _hex(299)}) is None


@needs_docker
def test_the_edge_edition_refuses_capture_and_is_followed_by_what_it_has():
    import pymssql
    name, port = servers.name("migkit-test-mssql-follow"), \
        servers.private_port(8)
    pw = servers.STRONG
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(mssql_run(name, port, pw), check=True,
                   capture_output=True)
    try:
        def sql(db, *qs):
            conn = pymssql.connect(server="127.0.0.1", port=str(port),
                                   user="sa", password=pw, database=db,
                                   autocommit=True, tds_version="7.4",
                                   login_timeout=10)
            try:
                cur, out = conn.cursor(), None
                for q in qs:
                    cur.execute(q[0], q[1]) if isinstance(q, tuple) else \
                        cur.execute(q)
                    out = cur.fetchall() if cur.description else None
                return out
            finally:
                conn.close()
        for _ in range(150):
            try:
                sql("master", "select 1")
                break
            except Exception:  # noqa: BLE001 - starting
                time.sleep(2)
        sql("master", "create database f", "create database g")
        for d in ("f", "g"):
            sql(d, "create table dbo.orders (id int primary key, amount"
                " int null)")
        with pytest.raises(Exception) as refused:
            sql("f", "exec sys.sp_cdc_enable_db")
        said = str(refused.value)
        assert any(w in said for w in EDGE_CDC_REFUSAL), said

        class Lane(MSSQLEngine):
            def _rows(self, side, db, q, args=()):
                from migkit.engines.dbapi import MARK
                return sql(db, (q.replace(MARK, "%s"), tuple(args))
                           if args else q.replace(MARK, "%s")) or []
        ep = Endpoint(host="127.0.0.1", port=port, user="sa", password=pw)
        eng = Lane(Hop(name="lane", engine="mssql", source=ep, target=ep,
                       databases=["f", "g"]))
        facts = eng.follow_facts("src", "f")
        assert facts.get("capture") is False and facts.get("capturing") \
            is False
        # tracking on: followed through tracking, the version its token
        sql("master", "alter database f set change_tracking = on")
        sql("f", "alter table dbo.orders enable change_tracking")
        assert eng.follow_way("src", "f").path == "tracking"
        assert isinstance(eng.change_point("src", "f"), int)
        # neither on: the live log, through the one hook
        way = eng.follow_way("src", "g")
        assert way.path == "log", way.reason
        token = eng.change_point("src", "g")
        assert isinstance(token, dict) and "lsn" in token
        sql("g", "insert into dbo.orders values (1, 5)",
            "update dbo.orders set amount = 6 where id = 1")
        got, after = eng.neutral_changes("src", "g", token)
        assert [(c["op"], c["key"]["id"]) for c in got] == [
            ("insert", 1), ("update", 1)], got
        assert eng.neutral_changes("src", "g", after)[0] == []
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
