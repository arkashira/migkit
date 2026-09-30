"""Oracle's redo read back as changes, held to everything that does not
need a server (backlog F5).

* LogMiner's own statements (`sql_redo`) are read back to the values they
  write and the row they name, quoting and NULLs included, and a literal
  the reader does not know is never guessed at - the column is read from
  the table instead.
* The redo-file reader is spoken to in its own protocol - requests
  written as its protobuf, every message framed by its length - and its
  JSON read back as the same changes: text as the hex of its UTF-8, so a
  value it could not decode (`?`) is never taken for one.
* Which reader follows a source is decided by migkit, and a standby is
  mined only when it is one of this very source, open and applying: this
  machine's sandbox has no Data Guard, so the gate is what is held here.
"""
import datetime
import json
import socket
import struct
import threading
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import oracle_logminer as miner
from migkit.engines import oracle_redo as redo


# ---- LogMiner's statements ----------------------------------------------

def test_an_insert_is_read_back_to_its_values():
    op, owner, table, after, before = miner.parse_redo(
        'insert into "APP"."T"("ID","S","N","D","R","TS","NV","E")'
        " values ('10','it''s \"quoted\"','-0.01',"
        " TO_DATE('2024-02-29 13:14:15', 'YYYY-MM-DD HH24:MI:SS'),"
        " HEXTORAW('00ff'),"
        " TO_TIMESTAMP('2024-05-01 01:02:03.987654321'),"
        " UNISTR('na\\00EFve \\D834\\DD1E'), NULL);")
    assert (op, owner, table, before) == ("insert", "APP", "T", {})
    kinds = {"ID": "NUMBER", "S": "VARCHAR2", "N": "NUMBER", "D": "DATE",
             "R": "RAW", "TS": "TIMESTAMP", "NV": "NVARCHAR2",
             "E": "VARCHAR2"}
    got = {n: miner.value_of(kinds[n], v) for n, v in after.items()}
    assert got == {
        "ID": Decimal(10), "S": 'it\'s "quoted"', "N": Decimal("-0.01"),
        "D": datetime.datetime(2024, 2, 29, 13, 14, 15),
        "R": b"\x00\xff",
        # every digit after the second kept, as a read keeps them
        "TS": datetime.datetime(2024, 5, 1, 1, 2, 3, 987654),
        "NV": "naïve \U0001D11E", "E": None}, got
    assert got["TS"].nanosecond == 321, got["TS"]


def test_an_update_names_its_row_by_what_its_where_held():
    op, _, _, after, before = miner.parse_redo(
        'update "APP"."T" set "S" = \'b\', "N" = NULL where "ID" = \'10\''
        ' and "S" = \'a\' and "N" IS NULL and ROWID = \'AAAR3sAAMAAAACGAAA\';')
    assert op == "update"
    assert after == {"S": ("str", "b"), "N": ("null",)}, after
    assert before == {"ID": ("str", "10"), "S": ("str", "a"),
                      "N": ("null",)}, before


def test_a_delete_carries_the_row_it_removed():
    op, _, _, after, before = miner.parse_redo(
        'delete from "APP"."T" where "ID" = \'5\' and "S" IS NULL and'
        ' ROWID = \'AAAR3sAAMAAAACGAAB\';')
    assert (op, after) == ("delete", {})
    assert before == {"ID": ("str", "5"), "S": ("null",)}, before


def test_a_literal_the_reader_does_not_know_is_not_guessed():
    """A LOB written empty, an interval, a time with its zone: read from
    the table as of the commit instead - never turned into a value that
    merely looks right."""
    for kind, v in (("CLOB", ("fn", "EMPTY_CLOB", [])),
                    ("INTERVAL DAY TO SECOND",
                     ("fn", "TO_DSINTERVAL", [("str", "+01 02:03:04")])),
                    ("TIMESTAMP WITH TIME ZONE",
                     ("fn", "TO_TIMESTAMP_TZ",
                      [("str", "2024-01-01 10:00:00 ASIA/BANGKOK")])),
                    ("DATE", ("str", "29-FEB-24"))):
        with pytest.raises(miner.NotRead):
            miner.value_of(kind, v)
    # and one it does know still reads
    assert miner.value_of("CHAR", ("str", "ab   ")) == "ab"


def test_a_statement_it_cannot_read_is_said_not_skipped():
    with pytest.raises(ValueError):
        miner.parse_redo("Unsupported")
    with pytest.raises(ValueError):
        miner.parse_redo('insert into "APP"."T"("ID") values (?);')


def test_a_row_whose_key_moved_arrives_whole():
    """The statement of an update that moved the key names only what it
    changed; the row at its new address is made from all of the row as
    it was where the redo holds it, and from the table otherwise."""
    kinds = {"ID": "NUMBER", "S": "VARCHAR2", "N": "NUMBER"}
    whole = {"ID": Decimal(2), "S": "two", "N": Decimal(5)}
    change, missing = miner.row_change("app", "T", "update", kinds, ["ID"],
                                       {"ID": Decimal(20)}, [], whole, [],
                                       900)
    assert change["key"] == {"ID": Decimal(2)}
    assert change["values"] == {"ID": Decimal(20), "S": "two",
                                "N": Decimal(5)}, change
    assert missing == [] and change["before"] == whole
    # the key alone logged: the rest read from the table as of the commit
    change, missing = miner.row_change("app", "T", "update", kinds, ["ID"],
                                       {"ID": Decimal(20)}, [],
                                       {"ID": Decimal(2)}, [], 900)
    assert change["values"] == {"ID": Decimal(20)} and \
        sorted(missing) == ["N", "S"], (change, missing)
    assert "before" not in change
    # a key that did not move: only what changed, nothing asked
    change, missing = miner.row_change("app", "T", "update", kinds, ["ID"],
                                       {"S": "b"}, [], {"ID": Decimal(2)},
                                       [], 900)
    assert change["values"] == {"S": "b"} and missing == []
    with pytest.raises(SystemExit) as e:
        miner.row_change("app", "T", "update", kinds, ["ID"], {"S": "b"},
                         [], {"S": "a"}, [], 900)
    assert "supplemental log data (primary key)" in str(e.value)


# ---- the position ----------------------------------------------------------

def test_the_fence_waits_on_every_commit_not_on_the_last():
    """`through` is the SCN every commit up to has been handed back; the
    last commit handed back says nothing about one not read yet."""
    token = {"scn": 100, "commit": 500, "xids": ["1.2.3"], "through": 480}
    assert miner.reached(token, 480) is True
    assert miner.reached(token, 481) is False
    assert miner.reached(token, None) is None
    assert miner.reached({"scn": 1}, 5) is None


def test_the_window_is_only_trusted_after_the_redo_is_written(monkeypatch):
    """A commit that did not wait for its redo can have an SCN below the
    window's end and not be in the files yet: `through` and the next
    window's start move only to what was true seconds before."""
    m = miner.Miner.__new__(miner.Miner)
    m.samples = [(0.0, 1000, 900), (5.0, 1500, 1400), (9.0, 1800, 1700),
                 (10.0, 2000, 1950)]
    through, restart = m._settled(10.0, {"scn": 800, "through": 950})
    # 4 s before: the sample at 5 s is the newest that old
    assert through == 1500, through
    # 10 s before: the sample at 0 s
    assert restart == 900, restart
    # never backwards
    through, restart = m._settled(10.0, {"scn": 950, "through": 1600})
    assert (through, restart) == (1600, 950)


# ---- a standby, gated ------------------------------------------------------

class _Conn:
    def __init__(self, row):
        self.row = row

    def cursor(self):
        conn = self

        class Cur:
            def execute(self, sql, args=None):
                return self

            def fetchone(self):
                return conn.row
        return Cur()


class _Eng:
    def __init__(self, dbid):
        self.dbid = dbid

    def _rows(self, side, db, sql, args=None):
        return [[self.dbid]]


@pytest.mark.parametrize("row,said", [
    (("PRIMARY", "READ WRITE", 42, 900), "not a physical standby"),
    (("PHYSICAL STANDBY", "MOUNTED", 42, 900), "not open read-only"),
    (("PHYSICAL STANDBY", "READ ONLY WITH APPLY", 7, 900),
     "standby of another database")])
def test_a_standby_is_mined_only_when_it_is_this_ones(row, said):
    with pytest.raises(SystemExit) as e:
        miner.standby_gate(_Eng(42), "src", "app", _Conn(row))
    assert said in str(e.value), e.value


def test_a_standby_that_is_this_ones_bounds_the_window():
    got = miner.standby_gate(_Eng(42), "src", "app", _Conn(
        ("PHYSICAL STANDBY", "READ ONLY WITH APPLY", 42, 900)))
    assert got == 900


# ---- the redo-file reader's protocol ----------------------------------------

def _fields(data):
    return redo.response(data)


def test_a_request_is_written_as_its_protobuf():
    body = redo.request(redo.START, "FREE", scn=2146047)
    got = _fields(body)
    assert got[1] == redo.START and got[2] == b"FREE" and \
        got[3] == 2146047, got
    body = redo.request(redo.CONFIRM, "FREE", c_scn=300, c_idx=4)
    got = _fields(body)
    assert (got[1], got[8], got[9]) == (redo.CONFIRM, 300, 4), got
    assert 3 not in got


def test_every_message_is_framed_by_its_length():
    assert redo.frame(b"abc") == b"\x03\x00\x00\x00abc"
    big = type("Big", (bytes,), {"__len__": lambda self: redo.LONG + 1})
    framed = redo.frame(big(b""))
    assert framed[:4] == b"\xff\xff\xff\xff"
    assert struct.unpack("<Q", framed[4:12])[0] == redo.LONG + 1


class _Writer:
    """The redo-file reader's network writer, as far as migkit speaks to
    it: answers INFO and START, streams `messages`, records CONFIRMs."""

    def __init__(self, messages):
        self.messages = messages
        self.requests = []
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.uri = f"127.0.0.1:{self.sock.getsockname()[1]}"
        threading.Thread(target=self.serve, daemon=True).start()

    def _read(self, conn):
        head = conn.recv(4)
        if not head:
            return None
        n = struct.unpack("<I", head)[0]
        body = b""
        while len(body) < n:
            body += conn.recv(n - len(body))
        return redo.response(body)

    def _send(self, conn, body):
        conn.sendall(redo.frame(body))

    def serve(self):
        conn, _ = self.sock.accept()
        with conn:
            while True:
                got = self._read(conn)
                if got is None:
                    return
                self.requests.append(got)
                if got[1] == redo.INFO:
                    self._send(conn, bytes([8, redo.READY]))
                elif got[1] == redo.START:
                    self._send(conn, bytes([8, redo.REPLICATE]))
                    for m in self.messages:
                        self._send(conn, json.dumps(m).encode())


class _Shapes:
    def __init__(self):
        self.asked = []

    def _shape(self, table):
        return ({"ID": "NUMBER", "S": "VARCHAR2", "CL": "CLOB",
                 "TZ": "TIMESTAMP WITH TIME ZONE", "N": "NUMBER"}, ["ID"])

    def _as_of(self, table, commit, wanted):
        self.asked.append((table, commit, [c for _, c in wanted]))

    def close(self):
        pass


def _hex(text):
    return text.encode().hex().upper()


def test_its_stream_is_read_back_as_changes_and_confirmed(monkeypatch):
    msgs = [
        {"scn": 700, "xid": "1.2.3", "c_scn": 690, "c_idx": 1,
         "payload": [{"op": "begin"}]},
        {"scn": 700, "xid": "1.2.3", "c_scn": 690, "c_idx": 2,
         "payload": [{"op": "c", "schema": {"owner": "APP", "table": "T"},
                      "after": {"ID": 1, "S": _hex("naïve"), "N": 12.50,
                                "CL": "?",
                                "TZ": "1704078000250000000,Asia/Bangkok"}}]},
        {"scn": 700, "xid": "1.2.3", "c_scn": 690, "c_idx": 3,
         "payload": [{"op": "u", "schema": {"owner": "APP", "table": "T"},
                      "before": {"ID": 1}, "after": {"S": _hex("?")}}]},
        {"scn": 700, "xid": "1.2.3", "c_scn": 690, "c_idx": 4,
         "payload": [{"op": "commit"}]},
        {"scn": 701, "xid": "1.2.4", "c_scn": 700, "c_idx": 1,
         "payload": [{"op": "d", "schema": {"owner": "APP", "table": "T"},
                      "before": {"ID": 2}}]},
        {"scn": 701, "xid": "1.2.4", "c_scn": 700, "c_idx": 2,
         "payload": [{"op": "commit"}]},
        {"scn": 750, "c_scn": 750, "c_idx": 0,
         "payload": [{"op": "chkpt", "seq": 13, "offset": 1024}]}]
    writer = _Writer(msgs)
    hop = Hop(name="o", engine="oracle",
              source=Endpoint(host="10.0.0.5", port=1521, user="u",
                              password="CHANGE_ME",
                              options={"redo_reader": writer.uri}),
              target=Endpoint(host="10.0.0.6", port=1521, user="u",
                              password="CHANGE_ME"), databases=["app"])

    class Eng:
        def __init__(self):
            self.hop = hop

        def _endpoint(self, side):
            return hop.source

        def _rows(self, side, db, sql, args=None):
            assert "v$database" in sql
            return [["FREE"]]

        @staticmethod
        def _said(n):
            return n

    shapes = _Shapes()
    reader = redo.RedoFileReader.__new__(redo.RedoFileReader)
    reader.eng, reader.side, reader.db = Eng(), "src", "app"
    reader.stream, reader.pending, reader.shapes = None, [], shapes
    monkeypatch.setattr(redo.RedoFileReader, "IDLE", 0.5)
    token = {"scn": 650, "commit": 0, "xids": [], "through": 640}
    got, token = reader.read(token, limit=100)
    assert [c["op"] for c in got] == ["insert", "update", "delete"], got
    ins = got[0]
    assert ins["values"]["S"] == "naïve"
    assert ins["values"]["N"] == Decimal("12.50")
    tz = ins["values"]["TZ"]
    assert tz == datetime.datetime(2024, 1, 1, 3, 0, 0, 250000,
                                   tzinfo=datetime.timezone.utc), tz
    # a value the reader could not decode is asked of the table; a text
    # that holds a question mark is a text
    assert "CL" not in ins["values"] and got[1]["values"]["S"] == "?"
    assert ("T", 700, ["CL"]) in [(t, c, cols[0]) for t, c, cols in
                                  shapes.asked], shapes.asked
    assert got[1]["key"] == {"ID": 1} and got[2]["key"] == {"ID": 2}
    assert (token["commit"], token["xids"]) == (701, ["1.2.4"]), token
    assert token["through"] == 749, token
    assert token["redo"] == {"c_scn": 750, "c_idx": 0}, token
    # the next read confirms what the last one handed back
    again, token2 = reader.read(token, limit=100)
    assert again == []
    import time
    for _ in range(20):
        if any(r[1] == redo.CONFIRM for r in writer.requests):
            break
        time.sleep(0.1)
    confirm = [r for r in writer.requests if r[1] == redo.CONFIRM]
    assert confirm and (confirm[0][8], confirm[0].get(9, 0)) == (750, 0), \
        writer.requests
    start = [r for r in writer.requests if r[1] == redo.START][0]
    assert start[3] == 650, start


def test_a_transaction_already_handed_back_is_not_handed_back_again():
    msgs = [
        {"scn": 700, "xid": "1.2.3", "c_scn": 690, "c_idx": 1,
         "payload": [{"op": "begin"}]},
        {"scn": 700, "xid": "1.2.3", "c_scn": 690, "c_idx": 2,
         "payload": [{"op": "d", "schema": {"owner": "APP", "table": "T"},
                      "before": {"ID": 9}}]},
        {"scn": 700, "xid": "1.2.3", "c_scn": 690, "c_idx": 3,
         "payload": [{"op": "commit"}]}]
    writer = _Writer(msgs)
    hop = Hop(name="o", engine="oracle",
              source=Endpoint(host="10.0.0.5", port=1521, user="u",
                              password="CHANGE_ME",
                              options={"redo_reader": writer.uri}),
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
    reader = redo.RedoFileReader.__new__(redo.RedoFileReader)
    reader.eng, reader.side, reader.db = Eng(), "src", "app"
    reader.stream, reader.pending, reader.shapes = None, [], _Shapes()
    reader.IDLE = 0.5
    got, _ = reader.read({"scn": 650, "commit": 700, "xids": ["1.2.3"]},
                         limit=100)
    assert got == [], got


# ---- which reader --------------------------------------------------------------

def _eng(options=None):
    hop = Hop(name="o", engine="oracle",
              source=Endpoint(host="10.0.0.5", port=1521, user="u",
                              password="CHANGE_ME", options=options or {}),
              target=Endpoint(host="10.0.0.6", port=1521, user="u",
                              password="CHANGE_ME"), databases=["app"])
    from migkit.engines.oracle import OracleEngine
    return OracleEngine(hop)


def test_logminer_follows_unless_a_redo_file_reader_can(monkeypatch):
    monkeypatch.delenv("MIGKIT_ORACLE_READER", raising=False)
    monkeypatch.setattr(redo, "platform_runs_it", lambda: True)
    monkeypatch.setattr(redo, "program", lambda: "/opt/reader")
    monkeypatch.setattr(redo, "redo_here", lambda *a: False)
    assert redo.chosen(_eng(), "src", "app") == redo.LOGMINER
    # the redo readable here, the program here, on its platform
    monkeypatch.setattr(redo, "redo_here", lambda *a: True)
    assert redo.chosen(_eng(), "src", "app") == redo.RUNG
    monkeypatch.setattr(redo, "platform_runs_it", lambda: False)
    assert redo.chosen(_eng(), "src", "app") == redo.LOGMINER
    # the operator runs one next to the redo, and says where
    assert redo.chosen(_eng({"redo_reader": "10.0.0.5:9000"}), "src",
                       "app") == redo.RUNG
    monkeypatch.setenv("MIGKIT_ORACLE_READER", "logminer")
    assert redo.chosen(_eng({"redo_reader": "10.0.0.5:9000"}), "src",
                       "app") == redo.LOGMINER
    monkeypatch.setenv("MIGKIT_ORACLE_READER", "tea-leaves")
    with pytest.raises(SystemExit):
        redo.chosen(_eng(), "src", "app")


def test_its_configuration_reads_back_exactly(monkeypatch):
    eng = _eng({"service": "FREEPDB1"})
    monkeypatch.setattr(type(eng), "_rows",
                        lambda self, side, db, sql, args=None: [["FREE"]])
    got = redo.config(eng, "src", "app", "127.0.0.1:9000", "/tmp/cp")
    src = got["source"][0]
    assert got["version"] == redo.VERSION
    assert src["name"] == "FREE"
    assert src["reader"]["server"] == "//10.0.0.5:1521/FREEPDB1"
    assert src["filter"]["table"] == [{"owner": "APP", "table": ".*"}]
    # text as hex, so `?` is never a value; commit SCN on every message;
    # an instant as nanoseconds with its zone
    assert (src["format"]["char"], src["format"]["scn-type"],
            src["format"]["timestamp-tz"]) == (2, 5, 0)
    # checkpoint records asked for: without them `through` never moves and
    # a fence never passes
    assert src["flags"] & redo.CHECKPOINTS
    assert got["target"][0]["writer"] == {"type": "network",
                                          "uri": "127.0.0.1:9000"}


def test_the_redo_file_readers_values():
    assert redo.value_of("VARCHAR2", _hex("Céline")) == "Céline"
    assert redo.value_of("CHAR", _hex("ab  ")) == "ab"
    assert redo.value_of("RAW", "00FF") == b"\x00\xff"
    assert redo.value_of("NUMBER", Decimal("1.50")) == Decimal("1.50")
    at = redo.value_of("TIMESTAMP", "2024-05-01 01:02:03.987654321")
    assert at == datetime.datetime(2024, 5, 1, 1, 2, 3, 987654)
    assert at.nanosecond == 321
    zoned = redo.value_of("TIMESTAMP WITH TIME ZONE",
                          "1704078000250000123,Asia/Bangkok")
    assert zoned == datetime.datetime(2024, 1, 1, 3, 0, 0, 250000,
                                      tzinfo=datetime.timezone.utc)
    assert (zoned.zone, zoned.nanosecond) == ("ASIA/BANGKOK", 123)
    with pytest.raises(miner.NotRead):
        redo.value_of("VARCHAR2", "?")
    with pytest.raises(miner.NotRead):
        redo.value_of("INTERVAL DAY TO SECOND", "01 02:03:04")


# ---- the ways, as the decision engine climbs them ------------------------------

def test_a_batch_climbs_its_ways_by_what_each_costs(monkeypatch):
    """Each way tried once on a table's large batches, then the cheaper by
    the row; a small batch, a value to write in pieces or a type the stage
    has no column for goes by array DML, and says why."""
    from migkit.engines.oracle import Bind
    monkeypatch.delenv("MIGKIT_ORACLE_WRITE", raising=False)
    eng = _eng()
    binds = [Bind("id", "NUMBER"), Bind("s", "VARCHAR2")]
    # the load names columns unquoted: one whose name keeps its small
    # letters has no column in a stage
    assert not Bind("MixedCase", "NUMBER").stageable
    assert eng._write_choice("app", "m", 5000, [Bind("MixedCase",
                                                     "NUMBER")],
                             False).path == "array"
    first = eng._write_choice("app", "t", 5000, binds, False)
    assert first.path == "direct", first
    key = eng._write_key("app", "t", 5000)
    eng._took(key, "direct", 1.0, 5000)
    assert eng._write_way("app", "t", 5000, binds) == "array"
    eng._took(key, "array", 0.5, 5000)
    got = eng._write_choice("app", "t", 5000, binds, False)
    assert got.path == "array" and "measured the cheapest" in got.reason, \
        got
    eng._took(key, "array", 3.0, 5000)
    assert eng._write_way("app", "t", 5000, binds) == "direct"
    # what the decision layer reads back: both ways timed, one chosen
    seen = [w for w in eng.measured_ways() if w["work"] == "write"]
    assert seen and seen[0]["chosen"] == "direct", seen
    # a batch of another size is its own measurement
    assert eng._write_choice("app", "t", 40000, binds, False).path == \
        "direct"
    small = eng._write_choice("app", "t", 10, binds, False)
    assert small.path == "array" and "this small" in small.reason, small
    big = eng._write_choice("app", "u", 5000, binds, True)
    assert big.path == "array" and "in pieces" in big.reason, big
    odd = eng._write_choice("app", "v", 5000,
                            binds + [Bind("x", "XMLTYPE")], False)
    assert odd.path == "array" and "no stage" in odd.reason, odd
    monkeypatch.setenv("MIGKIT_ORACLE_WRITE", "array")
    assert eng._write_way("app", "w", 5000, binds) == "array"


def test_the_reader_is_the_first_way_that_holds(monkeypatch):
    monkeypatch.delenv("MIGKIT_ORACLE_READER", raising=False)
    asked = []
    monkeypatch.setattr(redo, "platform_runs_it",
                        lambda: asked.append("platform") or False)
    monkeypatch.setattr(redo, "redo_here",
                        lambda *a: asked.append("redo") or True)
    got = redo.choice(_eng(), "src", "app")
    assert got.path == redo.LOGMINER, got
    assert "is not run beside this source's redo" in got.reason, got
    # the server is not asked about its redo files where the platform
    # already rules the program out
    assert asked == ["platform"], asked
    got = redo.choice(_eng({"redo_reader": "10.0.0.5:9000"}), "src", "app")
    assert got.path == redo.RUNG and "database does no mining" in \
        got.reason, got


def test_oracle_to_oracle_climbs_to_the_copier_where_data_pump_cannot(
        monkeypatch):
    from migkit.engines import oracle_pump
    monkeypatch.setattr(oracle_pump, "why_not",
                        lambda eng, db: "the target has no Data Pump")
    steps = oracle_pump.bulk(_eng(), "app", ["t"], False, None)
    assert steps == ["t: copied table by table (the target has no Data"
                     " Pump)"], steps
    monkeypatch.setattr(oracle_pump, "why_not", lambda eng, db: None)
    steps = oracle_pump.bulk(_eng(), "app", ["t"], False, None)
    assert steps == ["t: pulled by the target's Data Pump over a database"
                     " link, as of one SCN"], steps


def test_a_time_is_bound_as_the_time_it_is():
    """A time is bound as its text with every digit after the second (the
    driver sends six); a change feed's text of one is read as the time it
    is; an instant goes into a column with no zone as its UTC wall clock -
    as the copier writes the driver's value - and into one with a zone in
    the zone it came in, which the statement names (`at time zone` takes
    no bind)."""
    from migkit import nanotime
    from migkit.engines.dbapi import MARK
    from migkit.engines.oracle import Bind, Zoned, _by_zone, _marks
    utc = datetime.timezone.utc
    ts = Bind("AT", "TIMESTAMP")
    assert ts.values("2024-01-01 07:01:00+07") == \
        ["2024-01-01 00:01:00.000000000"]
    assert ts.values(datetime.datetime(2024, 1, 1, 7, 1, tzinfo=utc)) == \
        ["2024-01-01 07:01:00.000000000"]
    assert ts.values(nanotime.of(datetime.datetime(2024, 1, 1, 7, 1, 0, 5),
                                 7)) == ["2024-01-01 07:01:00.000005007"]
    assert ts.values("infinity") == ["infinity"]
    assert ts.mark() == f"to_timestamp({MARK}, 'YYYY-MM-DD HH24:MI:SS.FF9')"
    zoned = Bind("AT", "TIMESTAMP WITH TIME ZONE")
    paris = Zoned.at(datetime.datetime(2024, 5, 1, 8, 0), "EUROPE/PARIS")
    assert zoned.values(paris) == ["2024-05-01 08:00:00.000000000"]
    assert zoned.values(paris, staged=True) == \
        ["2024-05-01 08:00:00.000000000", "EUROPE/PARIS"]
    assert zoned.values("2024-05-01 10:00:00+02", staged=True) == \
        ["2024-05-01 08:00:00.000000000", "UTC"]
    assert zoned.mark("EUROPE/PARIS").count(MARK) == 1
    assert "at time zone 'EUROPE/PARIS')" in zoned.mark("EUROPE/PARIS")
    assert "at time zone 'x''y')" in zoned.mark("x'y")
    # a batch of two zones is two statements, each naming its own
    binds = [Bind("ID", "NUMBER"), zoned]
    rows = [[1, paris], [2, Zoned.at(datetime.datetime(2024, 1, 1),
                                     "ASIA/BANGKOK")], [3, paris],
            [4, None]]
    groups = _by_zone(binds, rows)
    assert {z: [r[0] for r in g] for z, g in groups.items()} == \
        {("EUROPE/PARIS",): [1, 3], ("ASIA/BANGKOK",): [2], ("UTC",): [4]}
    assert "'ASIA/BANGKOK'" in _marks(binds, ("ASIA/BANGKOK",))[1]
    # the stage holds each value's zone, and the promotion names each
    promoted = zoned.promote(lambda n: f'"{n}"', {"EUROPE/PARIS", "UTC"})
    assert promoted.count("when ") == 2 and promoted.endswith(" end")
    assert Bind("D", "DATE").values("2024-01-01 07:01:00+07") == \
        [datetime.datetime(2024, 1, 1, 0, 1)]
    assert Bind("N", "NUMBER").values(Decimal("1.50")) == [Decimal("1.50")]


def test_an_interval_is_bound_as_the_length_it_is():
    """Another engine's interval (months, days and nanoseconds apart) is
    bound as the driver takes each of Oracle's two kinds: a year-to-month
    as years and months, a negative one negative in both; a day-to-second
    as a `timedelta`. What the column's kind cannot hold is refused, not
    written as another length of time."""
    import oracledb
    from migkit.engines.oracle import Bind
    from migkit.intervals import Interval
    ym = Bind("I", "INTERVAL YEAR(2) TO MONTH")
    assert ym.values(Interval(14, 0, 0)) == [oracledb.IntervalYM(1, 2)]
    assert ym.values(Interval(-14, 0, 0)) == [oracledb.IntervalYM(-1, -2)]
    assert ym.values("P14M0DT0S") == [oracledb.IntervalYM(1, 2)]
    assert ym.values(oracledb.IntervalYM(3, 0)) == \
        [oracledb.IntervalYM(3, 0)]
    assert ym.values(None) == [None]
    assert ym.sizes() == [oracledb.DB_TYPE_INTERVAL_YM]
    with pytest.raises(ValueError, match="year-to-month"):
        ym.values(Interval(1, 2, 0))
    ds = Bind("I", "INTERVAL DAY(2) TO SECOND(6)")
    with pytest.raises(ValueError, match="microsecond"):
        ds.values(Interval(0, 3, 4 * 3600 * 10 ** 9 + 500))
    assert ds.values(Interval(0, 3, 4 * 3600 * 10 ** 9 + 5000)) == \
        [datetime.timedelta(days=3, hours=4, microseconds=5)]
    assert ds.values(Interval(0, -1, -2 * 10 ** 9)) == \
        [-datetime.timedelta(days=1, seconds=2)]
    assert ds.sizes() == [oracledb.DB_TYPE_INTERVAL_DS]
    with pytest.raises(ValueError, match="day-to-second"):
        ds.values(Interval(1, 0, 0))
