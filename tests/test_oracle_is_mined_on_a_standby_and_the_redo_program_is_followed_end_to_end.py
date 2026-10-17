"""Oracle's redo followed end to end through the redo-file program's own
stream, and the reader chosen from what was measured (backlog LT.C5, LT.*
Oracle redo).

The program is replayed from a recorded transcript of its protocol: INFO
answered, START from an SCN, its stream of begins, row changes, commits
and checkpoints, CONFIRM after a batch is applied - and a stop: the
connection dropped mid-transaction, the program holding everything not
confirmed, a new reader continuing from the saved position and handed the
unconfirmed messages again, of which it hands on only what it had not.
The mining places rank by the lag measured at each, and LogMiner is
refused where it was measured mining slower than the source writes redo.
Built, unproved on a real Oracle with the program and a standby.
"""
import json
import socket
import struct
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import oracle_logminer as miner
from migkit.engines import oracle_redo as redo


def _msg(scn, xid, c_scn, c_idx, op, **row):
    p = {"op": op}
    if row:
        p.update(schema={"owner": "APP", "table": "T"}, **row)
    return {"scn": scn, "xid": xid, "c_scn": c_scn, "c_idx": c_idx,
            "payload": [p]}


#: the transcript: two transactions, a checkpoint, a third begun
STREAM = [
    _msg(700, "1.1.1", 690, 1, "begin"),
    _msg(700, "1.1.1", 690, 2, "c", after={"ID": 1}),
    _msg(700, "1.1.1", 690, 3, "commit"),
    _msg(710, "1.1.2", 700, 1, "begin"),
    _msg(710, "1.1.2", 700, 2, "d", before={"ID": 2}),
    _msg(710, "1.1.2", 700, 3, "commit"),
    {"scn": 720, "c_scn": 720, "c_idx": 0,
     "payload": [{"op": "chkpt", "seq": 4, "offset": 512}]},
    _msg(730, "1.1.3", 720, 1, "begin"),
    _msg(730, "1.1.3", 720, 2, "c", after={"ID": 3}),
    # the stop falls here: 1.1.3's commit is sent after the reconnect
    _msg(730, "1.1.3", 720, 3, "commit")]
CUT = 9


class _Program:
    """The program's network writer: it holds every message not confirmed
    and sends them again to a reader that continues."""

    def __init__(self):
        self.requests, self.confirmed, self.conns = [], (0, 0), 0
        self.cut = CUT
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(2)
        self.uri = f"127.0.0.1:{self.sock.getsockname()[1]}"
        threading.Thread(target=self.serve, daemon=True).start()

    def _read(self, conn):
        head = conn.recv(4)
        if not head:
            return None
        n, body = struct.unpack("<I", head)[0], b""
        while len(body) < n:
            body += conn.recv(n - len(body))
        return redo.response(body)

    def serve(self):
        while True:
            conn, _ = self.sock.accept()
            self.conns += 1
            with conn:
                while True:
                    try:
                        got = self._read(conn)
                    except OSError:
                        break
                    if got is None:
                        break
                    self.requests.append(got)
                    code = got[1]
                    if code == redo.INFO:
                        conn.sendall(redo.frame(bytes([8, redo.READY if
                                                       self.conns == 1 else
                                                       redo.ALREADY_STARTED])))
                    elif code in (redo.START, redo.CONTINUE):
                        conn.sendall(redo.frame(bytes([8, redo.REPLICATE])))
                        upto = self.cut if self.conns == 1 else len(STREAM)
                        for m in STREAM[:upto]:
                            if (m["c_scn"], m["c_idx"]) > self.confirmed:
                                conn.sendall(redo.frame(json.dumps(m)
                                                        .encode()))
                    elif code == redo.CONFIRM:
                        self.confirmed = (got.get(8, 0), got.get(9, 0))


class _Shapes:
    def _shape(self, table):
        return {"ID": "NUMBER"}, ["ID"]

    def _as_of(self, *a):
        pass

    def close(self):
        pass


def _reader(program):
    hop = Hop(name="o", engine="oracle",
              source=Endpoint(host="10.0.0.5", port=1521, user="u",
                              password="CHANGE_ME",
                              options={"redo_reader": program.uri}),
              target=Endpoint(host="10.0.0.6", port=1521, user="u",
                              password="CHANGE_ME"), databases=["app"])

    class Eng:
        def __init__(self):
            self.hop = hop

        def _endpoint(self, side):
            return hop.source

        def _rows(self, side, db, sql, args=None):
            return [["FREE"]]

        @staticmethod
        def _said(n):
            return n
    r = redo.RedoFileReader.__new__(redo.RedoFileReader)
    r.eng, r.side, r.db = Eng(), "src", "app"
    r.stream, r.pending, r.txn, r.shapes = None, [], [], _Shapes()
    return r


def test_the_stream_is_followed_through_a_stop_and_its_redelivery(
        monkeypatch):
    monkeypatch.setattr(redo.RedoFileReader, "IDLE", 0.4)
    program = _Program()
    first = _reader(program)
    token = {"scn": 650, "commit": 0, "xids": [], "through": 0}
    got, token = first.read(token, limit=100)
    assert [(c["op"], c["key"]) for c in got] == [("insert", {"ID": 1}),
                                                  ("delete", {"ID": 2})]
    # 1.1.3 is open: it is not handed on half, and the saved position is
    # the checkpoint before it
    assert token["redo"] == {"c_scn": 720, "c_idx": 0}
    assert (token["commit"], token["xids"]) == (710, ["1.1.2"])
    # nothing is confirmed until the batch is applied and saved
    assert not [r for r in program.requests if r[1] == redo.CONFIRM]
    # the reader stops before its next read; the program confirmed nothing
    # of it, so it sends the whole stream again to the next
    first.close()
    second = _reader(program)
    again, token = second.read(dict(token), limit=100)
    assert [(c["op"], c["key"]) for c in again] == [("insert", {"ID": 3})]
    kinds = [r[1] for r in program.requests]
    assert kinds[:2] == [redo.INFO, redo.START]
    assert redo.CONTINUE in kinds[2:] and redo.START not in kinds[2:]
    cont = [r for r in program.requests if r[1] == redo.CONTINUE][0]
    assert (cont.get(8), cont.get(9, 0)) == (720, 0)
    # the next read confirms what was applied: the position after 1.1.3
    assert second.read(dict(token), limit=100)[0] == []
    for _ in range(20):
        if program.confirmed != (0, 0):
            break
        time.sleep(0.05)
    assert program.confirmed == (720, 3), program.confirmed
    second.close()


def _eng(options=None):
    hop = Hop(name="o", engine="oracle",
              source=Endpoint(host="10.0.0.5", port=1521, user="u",
                              password="CHANGE_ME", options=options or {}),
              target=Endpoint(host="10.0.0.6", port=1521, user="u",
                              password="CHANGE_ME"), databases=["app"])

    class Eng:
        def __init__(self):
            self.hop = hop

        def _endpoint(self, side):
            return hop.source
    return Eng()


def test_logminer_is_refused_where_it_was_measured_falling_behind(
        monkeypatch):
    monkeypatch.delenv("MIGKIT_ORACLE_READER", raising=False)
    monkeypatch.setattr(redo, "platform_runs_it", lambda: False)
    eng = _eng()
    assert redo.chosen(eng, "src", "app") == redo.LOGMINER
    miner._RATES[(id(eng.hop), "src", "app")] = (8 * 2 ** 20, 3 * 2 ** 20)
    try:
        with pytest.raises(SystemExit, match="would fall behind"):
            redo.chosen(eng, "src", "app")
        # the same facts, and the program listening beside the redo
        eng2 = _eng({"redo_reader": "10.0.0.7:9000"})
        miner._RATES[(id(eng2.hop), "src", "app")] = (8 * 2 ** 20,
                                                      3 * 2 ** 20)
        assert redo.chosen(eng2, "src", "app") == redo.RUNG
        # mined faster than written: LogMiner holds again
        miner._RATES[(id(eng.hop), "src", "app")] = (2 ** 20, 3 * 2 ** 20)
        assert redo.chosen(eng, "src", "app") == redo.LOGMINER
    finally:
        miner._RATES.clear()


def test_the_mining_places_rank_by_the_lag_measured_at_each(monkeypatch):
    eng = _eng({"mine_standby": "10.0.0.8:1521", "mine_instance":
                "10.0.0.9:1521", "archive_from": "/arch",
                "archive_to": "/ship"})
    monkeypatch.setattr(miner, "granted", lambda *a: miner.MINING_GRANTS)
    monkeypatch.setattr(miner, "dictionary_scn", lambda *a, **k: 500)
    assert miner.place(eng, "src", "app").path == miner.STANDBY
    try:
        for _ in range(3):
            miner.lagged(eng, "src", "app", miner.STANDBY, 90.0)
            miner.lagged(eng, "src", "app", miner.INSTANCE, 12.0)
            miner.lagged(eng, "src", "app", miner.SOURCE, 30.0)
        assert miner.place(eng, "src", "app").path == miner.INSTANCE
    finally:
        miner._LAGS.clear()


def test_standby_gate_checks_role_mode_and_dbid():
    class DummyCursor:
        def __init__(self, row):
            self.row = row

        def execute(self, sql):
            return self

        def fetchone(self):
            return self.row

    class DummyConn:
        def __init__(self, row):
            self.cur = DummyCursor(row)
            self.closed = False

        def cursor(self):
            return self.cur

        def close(self):
            self.closed = True

    class DummyEng:
        def _rows(self, side, db, sql):
            return [[123456]]

    eng = DummyEng()
    # not a physical standby
    conn = DummyConn(["PRIMARY", "READ WRITE", 123456, 1000])
    with pytest.raises(SystemExit, match="not a physical standby"):
        miner.standby_gate(eng, "src", "app", conn)
    # not open read only
    conn = DummyConn(["PHYSICAL STANDBY", "MOUNTED", 123456, 1000])
    with pytest.raises(SystemExit, match="not open read-only"):
        miner.standby_gate(eng, "src", "app", conn)
    # different DBID
    conn = DummyConn(["PHYSICAL STANDBY", "READ ONLY WITH APPLY", 999999, 1000])
    with pytest.raises(SystemExit, match="standby of another database"):
        miner.standby_gate(eng, "src", "app", conn)
    # valid active data guard
    conn = DummyConn(["PHYSICAL STANDBY", "READ ONLY WITH APPLY", 123456, 1050])
    assert miner.standby_gate(eng, "src", "app", conn) == 1050


def test_miner_open_cleans_up_connection_when_standby_gate_fails(monkeypatch):
    class DummyCursor:
        def execute(self, sql):
            return self

        def fetchone(self):
            return ["PRIMARY", "READ WRITE", 123, 100]

    class DummyConn:
        def __init__(self):
            self.closed = False

        def cursor(self):
            return DummyCursor()

        def close(self):
            self.closed = True

    eng = _eng({"mine_standby": "10.0.0.8:1521"})
    conn = DummyConn()
    eng._dedicated = lambda *a, **k: conn
    eng._dsn = lambda *a, **k: "10.0.0.8:1521"
    eng._rows = lambda *a, **k: [[123]]
    m = miner.Miner(eng, "src", "app")
    with pytest.raises(SystemExit):
        m._open()
    assert conn.closed is True
    assert m.conn is None

