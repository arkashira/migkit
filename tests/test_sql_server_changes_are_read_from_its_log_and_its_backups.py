"""A SQL Server source followed through its own log where neither Change
Tracking nor Change Data Capture is on (backlog LT.R2, LT.R3).

The live log's rows are replayed from a recorded transcript of the log
function's answers - the data rows' images laid out as the server's page
format documents them - and, on the `sqlserver` docker lane, read off the
sandbox's own build: a table changed there is read back from its log by
the same reader. The log backups' way is replayed: restored in order onto
a server migkit owns, a backup already restored read again and not
restored twice, and a chain with a gap refused. The way is the decision's
(`decide.mssql_log_way`) from what the server says. Built, unproved on a
real SQL Server (`mssql_log.UNPROVED_ON`).
"""
import struct
import subprocess
import time
from decimal import Decimal

import pytest

from migkit import decide
from migkit.engines import mssql_log as log
from tests import servers
from tests.conftest import mssql_run, needs_docker

COLS = [("id", "int", 4, 10, 0), ("amount", "decimal", 5, 9, 2),
        ("name", "nvarchar", 40, 0, 0)]
#: what the sandbox's build said when asked to restore a log backup in
#: standby (`test_the_sandbox_reads_its_live_log_back_as_changes`): it took
#: it, 2026-10-16, so the refusal replayed below is a server's that would not
EDGE_RESTORE = None


def _image(i, amount, name):
    fixed = struct.pack("<i", i) + bytes([1 if amount >= 0 else 0]) + \
        abs(int(amount * 100)).to_bytes(4, "little")
    end = 4 + len(fixed)
    nulls = 0b100 if name is None else 0
    data = b"" if name is None else name.encode("utf-16-le")
    return bytes([0x30, 0]) + struct.pack("<H", end) + fixed + \
        struct.pack("<HB", 3, nulls) + struct.pack("<HH", 1, end + 7 +
                                                   len(data)) + data


def _l(n):
    return log.lsn_text(n)


def _tx(tid, at, rows, end="LOP_COMMIT_XACT"):
    out = [(_l(at), "LOP_BEGIN_XACT", "LCX_NULL", tid, None, None, None,
            None)]
    for k, (op, unit, page, slot, image) in enumerate(rows, 1):
        out.append((_l(at + k), op, "LCX_CLUSTERED", tid, unit, page, slot,
                    image))
    out.append((_l(at + len(rows) + 1), end, "LCX_NULL", tid, None, None,
                None, None))
    return out


class _Eng:
    def __init__(self, records, recovery="SIMPLE", live=True, now=None,
                 standby=None, backups=()):
        self.records, self.recovery, self.live = records, recovery, live
        self.now, self.standby, self.backups = now or {}, standby, backups
        self.asked = []

    def _endpoint(self, side):
        from migkit.config import Endpoint
        return Endpoint(host="10.0.0.5", port=1433, user="u",
                        password="CHANGE_ME", options={
                            "log_standby": self.standby} if self.standby
                        else {})

    def _rows(self, side, db, sql, args=()):
        if "top 1 [Current LSN]" in sql:
            if not self.live:
                raise RuntimeError("The user does not have permission")
            return [(_l(1),)]
        if "fn_dblog(%s" in sql:
            self.asked.append(args[0])
            return [r for r in self.records if log.lsn(r[0]) >=
                    log.lsn(args[0])]
        if "recovery_model_desc" in sql:
            return [(self.recovery,)]
        if "sys.allocation_units" in sql:
            return [(72057594043236352, "dbo", "orders"),
                    (72057594043301888, "dbo", "audit")]
        if "sys.columns" in sql:
            return COLS
        if "msdb.dbo.backupset" in sql:
            return [b for b in self.backups if b[1] > args[0]]
        if "%%physloc%%" in sql:
            self.asked.append(args[0])
            got = self.now.get(args[0])
            return [got] if got else []
        raise AssertionError(sql)

    def _said(self, n):
        return n

    def in_scope(self, db, t):
        return t != "audit"

    def neutral_key(self, side, db, t):
        return ["id"]

    def _q(self, n):
        return f"[{n}]"

    def _qualified(self, side, db, t):
        return f"[dbo].[{t}]"


U = 72057594043236352


def test_the_live_log_is_read_back_whole_transactions_and_aborts_left_out():
    loc = log.physloc("0001:00000110", 2)
    recs = (_tx("0000:00000401", 100, [
        ("LOP_INSERT_ROWS", U, "0001:00000110", 2,
         _image(1, Decimal("12.50"), "naïve")),
        ("LOP_INSERT_ROWS", 72057594043301888, "0001:00000200", 0,
         _image(9, Decimal("1"), "audit"))])
        + _tx("0000:00000402", 110, [
            ("LOP_MODIFY_ROW", U, "0001:00000110", 2, b"\x01")])
        + _tx("0000:00000403", 120, [
            ("LOP_INSERT_ROWS", U, "0001:00000111", 0,
             _image(7, Decimal("0"), "x"))], end="LOP_ABORT_XACT")
        + _tx("0000:00000404", 130, [
            ("LOP_DELETE_ROWS", U, "0001:00000110", 2,
             _image(1, Decimal("-3.25"), None))]))
    eng = _Eng(recs, now={loc: (1, Decimal("-3.25"), None)})
    got, token = log.neutral_changes(eng, "src", "app",
                                     {"lsn": _l(99), "from": _l(99)})
    assert [(c["op"], c["key"]) for c in got] == [
        ("insert", {"id": 1}), ("update", {"id": 1}), ("delete", {"id": 1})]
    assert got[0]["values"] == {"id": 1, "amount": Decimal("12.50"),
                                "name": "naïve"}
    assert got[1]["values"]["amount"] == Decimal("-3.25")
    assert got[1]["values"]["name"] is None and loc in eng.asked
    assert token == {"lsn": _l(132), "from": _l(132)}


def test_a_reader_stopped_mid_batch_starts_again_handing_nothing_twice():
    one = _tx("0000:00000501", 200, [("LOP_INSERT_ROWS", U, "0001:00000300",
                                      0, _image(20, Decimal("1"), "a"))])
    # transaction 502 has begun, and not committed, when the read is made
    two = _tx("0000:00000502", 203, [("LOP_INSERT_ROWS", U, "0001:00000300",
                                      1, _image(21, Decimal("2"), "b"))])
    eng = _Eng(one + two[:-1])
    got, token = log.neutral_changes(eng, "src", "app",
                                     {"lsn": _l(199), "from": _l(199)})
    assert [c["key"] for c in got] == [{"id": 20}]
    assert token == {"lsn": _l(202), "from": _l(202)}
    # stopped here; started again on the same saved token, the log on
    eng = _Eng(one + two)
    again, token2 = log.neutral_changes(eng, "src", "app", dict(token))
    assert [c["key"] for c in again] == [{"id": 21}] and \
        eng.asked == [_l(202)]
    eng = _Eng(one + two)
    assert log.neutral_changes(eng, "src", "app", dict(token))[0] == again
    assert token2 == {"lsn": _l(205), "from": _l(205)}


def test_the_way_is_what_the_server_says():
    f = {"live": True, "recovery": "FULL", "standby": True, "chain": True}
    assert decide.mssql_log_way(f).path == "live log"
    assert decide.mssql_log_way(dict(f, live=False)).path == "log backups"
    # the lag each way was measured at here ranks them
    assert decide.mssql_log_way(f, costs={"live log": 9.0,
                                          "log backups": 2.0}).path == \
        "log backups"
    got = decide.mssql_log_way(dict(f, live=False, recovery="SIMPLE"))
    assert got.rung is None and "takes no log backups" in got.reason
    got = decide.mssql_log_way(dict(f, live=False, chain=False))
    assert got.rung is None and "chain is broken" in got.reason
    with pytest.raises(SystemExit, match="followed no way"):
        log.follow_way(_Eng([], live=False), "src", "app")


class _Standby:
    def __init__(self, records, restored=None, refuse=None):
        self.records, self.restored, self.refuse = records, restored, refuse
        self.done, self.read = [], []

    def _rows(self, sql, args=()):
        if sql.startswith("restore log"):
            if self.refuse:
                raise RuntimeError(self.refuse)
            self.done.append(args[0])
            return []
        if "redo_start_lsn" in sql:
            return [(self.restored,)]
        if "fn_dump_dblog" in sql:
            path = sql.split("N'DISK', 1, N'")[1].split("'")[0]
            self.read.append(path)
            return self.records[path]
        raise AssertionError(sql)


def test_the_log_backups_are_restored_in_order_and_once():
    a = _tx("0000:00000601", 300, [("LOP_INSERT_ROWS", U, "0001:00000400",
                                    0, _image(30, Decimal("3"), "c"))])
    b = _tx("0000:00000602", 310, [("LOP_DELETE_ROWS", U, "0001:00000400",
                                    0, _image(30, Decimal("3"), "c"))])
    backups = [(log.lsn_decimal(250), log.lsn_decimal(305), "/b/1.trn"),
               (log.lsn_decimal(305), log.lsn_decimal(320), "/b/2.trn")]
    # the first was restored by a run that stopped after it
    standby = _Standby({"/b/1.trn": a, "/b/2.trn": b},
                       restored=log.lsn_decimal(305))
    eng = _Eng([], recovery="FULL", live=False, standby="10.0.0.9:1433",
               backups=backups)
    got, token = log.neutral_changes(eng, "src", "app",
                                     {"lsn": _l(299), "from": _l(299)},
                                     standby=standby)
    assert [(c["op"], c["key"]) for c in got] == [("insert", {"id": 30}),
                                                  ("delete", {"id": 30})]
    assert standby.done == ["/b/2.trn"]
    assert standby.read == ["/b/1.trn", "/b/2.trn"]
    assert token["lsn"] == _l(312)
    # a gap in the chain: the changes in it cannot be read, so no way holds
    gap = [backups[0], (log.lsn_decimal(306), log.lsn_decimal(320), "/x")]
    eng = _Eng([], recovery="FULL", live=False, standby="10.0.0.9:1433",
               backups=gap)
    with pytest.raises(SystemExit, match="chain is broken"):
        log.neutral_changes(eng, "src", "app", {"lsn": _l(299),
                                                "from": _l(299)},
                            standby=standby)
    # a server that refuses the restore in standby says so
    eng = _Eng([], recovery="FULL", live=False, standby="10.0.0.9:1433",
               backups=backups)
    no = _Standby({}, refuse=EDGE_RESTORE or "RESTORE LOG is not supported")
    with pytest.raises(SystemExit, match="would not restore"):
        log.neutral_changes(eng, "src", "app", {"lsn": _l(299),
                                                "from": _l(299)}, standby=no)


def test_the_lsn_notations_read_alike():
    n = log.lsn("00000021:00000110:0002")
    assert log.lsn(log.lsn_text(n)) == n
    assert log.lsn_decimal(n) == 33000000027200002
    assert log.from_decimal(log.lsn_decimal(n)) == n


# ---- the sandbox's own build ------------------------------------------

@needs_docker
def test_the_sandbox_reads_its_live_log_back_as_changes():
    import pymssql
    name, port = servers.name("migkit-test-mssql-log"), servers.private_port(7)
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
        for _ in range(90):
            try:
                sql("master", "select 1")
                break
            except Exception:  # noqa: BLE001 - starting
                time.sleep(2)
        sql("master", "create database l", "alter database l set recovery"
            " full", "backup database l to disk = '/var/opt/mssql/data/l.bak'")
        sql("l", "create table dbo.orders (id int primary key, amount"
            " decimal(9,2) null, name nvarchar(20) null)")

        class Eng(_Eng):
            def _rows(self, side, db, q, args=()):
                return sql("l", (q, tuple(args)) if args else q) or []
        eng = Eng([])
        token = log.change_point(eng, "src", "l")
        sql("l", "insert into dbo.orders values (1, 12.50, N'naïve'),"
            " (2, -3.25, null)", "update dbo.orders set amount = 7 where"
            " id = 1", "delete from dbo.orders where id = 2")
        got, after = log.neutral_changes(eng, "src", "l", token)
        assert [(c["op"], c["key"]["id"]) for c in got] == [
            ("insert", 1), ("insert", 2), ("update", 1), ("delete", 2)], got
        assert got[0]["values"] == {"id": 1, "amount": Decimal("12.50"),
                                    "name": "naïve"}
        assert got[1]["values"]["name"] is None
        assert got[2]["values"]["amount"] == Decimal("7.00")
        assert log.neutral_changes(eng, "src", "l", after)[0] == []
        # the log backups' way: restored in standby where the build lets it
        sql("master", "backup log l to disk = '/var/opt/mssql/data/l.trn'")
        try:
            sql("master", "restore database l2 from disk ="
                " '/var/opt/mssql/data/l.bak' with move 'l' to"
                " '/var/opt/mssql/data/l2.mdf', move 'l_log' to"
                " '/var/opt/mssql/data/l2.ldf', standby ="
                " '/var/opt/mssql/data/l2.undo'",
                "restore log l2 from disk = '/var/opt/mssql/data/l.trn'"
                " with standby = '/var/opt/mssql/data/l2.undo'")
            said = None
        except Exception as e:  # noqa: BLE001 - the build's refusal
            said = str(e)
        print("restore in standby:", said or "taken")
        if said is None:
            assert sql("l2", "select count(*) from dbo.orders") == [(1,)]
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
