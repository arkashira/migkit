"""A Db2 source's changes read from its own log through the log-read API,
replayed from a recorded transcript of the API's answers (backlog LT.R6).

No Db2 server runs here (x86 only): the buffers below are laid out as the
client's documentation gives a log record - its LSN, length, type and
transaction, and for a row change the data manager's function, table
space, table, record id and image - and the API's calls are answered from
them in the order a real read makes them. What is proved is migkit's side
of it: the rows handed on, the choice of how by each table's own logging
setting, the refusal where the client is not installed, and a reader
stopped mid-batch that starts again from its saved position handing
nothing twice. Built, unproved on a real server (`db2_log.UNPROVED_ON`).
"""
import struct
from decimal import Decimal

import pytest

from migkit import decide
from migkit.engines import db2_log as log

COLS = [("ID", "INTEGER", 4, 0, "N"), ("AMOUNT", "DECIMAL", 7, 2, "Y"),
        ("NAME", "VARCHAR", 20, 0, "Y")]


def _image(i, amount, name):
    digits = f"{abs(int(amount * 100)):07d}" + ("c" if amount >= 0 else "d")
    fixed = struct.pack("<i", i) + b"\x00" + bytes.fromhex(digits)
    data = name.encode()
    return fixed + b"\x00" + struct.pack("<HH", 0, len(data)) + data


def _rec(lsn, rtype, tid, body=b""):
    head = struct.pack("<IH", 12 + len(body), rtype) + tid.to_bytes(6, "big")
    return struct.pack("<Q", lsn) + head + body


def _row(lsn, tid, fn, table, rid, image, whole=True, old=b""):
    body = struct.pack("<BHH", fn, 2, table) + rid.to_bytes(6, "big") + \
        struct.pack("<BH", log.WHOLE if whole else log.PARTIAL, len(image)) \
        + image + old
    return _rec(lsn, log.NORMAL, tid, body)


class _Transcript:
    """The API answered from recorded buffers: each read hands back the
    records from its start LSN on, `per` records a call."""

    def __init__(self, records, per=2):
        self.records, self.per, self.calls = list(records), per, []

    def query(self):
        return {"first": 1, "next": max(struct.unpack_from("<Q", r)[0]
                                        for r in self.records) + 1}

    def read(self, start, end, size):
        self.calls.append(start)
        got = [r for r in self.records
               if start <= struct.unpack_from("<Q", r)[0] < end][:self.per]
        if not got:
            return b"", end
        last = struct.unpack_from("<Q", got[-1])[0]
        return b"".join(got), last + 1


class _Eng:
    def __init__(self, capture="Y", now=None):
        self.capture, self.now, self.asked = capture, now or {}, []

    def _rows(self, side, db, sql, args=()):
        if "syscat.tables" in sql:
            return [("ORDERS", 2, 7, self.capture), ("AUDIT", 2, 8, "Y")]
        if "dbcfg" in sql:
            return [("DISK:/db2/archive/",)]
        if "syscat.columns" in sql:
            return [(n, t, ln, sc, nul) for n, t, ln, sc, nul in COLS]
        if "rid(t)" in sql:
            self.asked.append(args[0])
            got = self.now.get(args[0])
            return [got] if got else []
        raise AssertionError(sql)

    def _owner(self, side, db):
        return "APP"

    def _said(self, n):
        return n

    def in_scope(self, db, t):
        return t != "AUDIT"

    def neutral_key(self, side, db, t):
        return ["ID"]

    def _q(self, n):
        return f'"{n}"'

    def _qualified(self, side, db, t):
        return f'"APP"."{t}"'


@pytest.fixture(autouse=True)
def _client(monkeypatch):
    monkeypatch.setattr(log, "library", lambda: object())


def test_whole_rows_are_handed_on_from_the_log_and_aborts_left_out():
    api = _Transcript([
        _row(10, 1, log.INSERT, 7, 0x10, _image(1, Decimal("12.50"), "naïve")),
        _row(11, 1, log.INSERT, 8, 0x11, _image(9, Decimal("1"), "audit")),
        _rec(12, log.COMMIT, 1),
        _row(13, 2, log.UPDATE, 7, 0x10, _image(1, Decimal("-3.25"), "b"),
             old=_image(1, Decimal("12.50"), "naïve")),
        _row(14, 3, log.INSERT, 7, 0x12, _image(5, Decimal("0"), "x")),
        _rec(15, log.ABORT, 3),
        _rec(16, log.COMMIT, 2),
        _row(17, 4, log.DELETE, 7, 0x10, _image(1, Decimal("-3.25"), "b")),
        _rec(18, log.COMMIT, 4)])
    got, token = log.neutral_changes(_Eng(), "src", "app",
                                     {"lsn": 9, "from": 9}, reader=api)
    assert [(c["op"], c["key"]) for c in got] == [
        ("insert", {"ID": 1}), ("update", {"ID": 1}), ("delete", {"ID": 1})]
    assert got[0]["values"] == {"ID": 1, "AMOUNT": Decimal("12.50"),
                                "NAME": "naïve"}
    assert got[1]["values"]["AMOUNT"] == Decimal("-3.25")
    assert [c.get("txn_end") for c in got] == [True, True, True]
    assert token == {"lsn": 18, "from": 19}


def test_a_row_logged_in_part_is_read_by_its_record_id_as_it_is_now():
    eng = _Eng(capture="N", now={0x10: (1, Decimal("7.00"), "now")})
    api = _Transcript([
        _row(20, 5, log.UPDATE, 7, 0x10, b"\x01\x02", whole=False),
        _row(21, 5, log.UPDATE, 7, 0x13, b"\x01", whole=False),
        _row(22, 5, log.DELETE, 7, 0x13, _image(3, Decimal("1"), "gone")),
        _rec(23, log.COMMIT, 5)])
    got, _ = log.neutral_changes(eng, "src", "app", {"lsn": 19, "from": 19},
                                 reader=api)
    assert [(c["op"], c["key"]) for c in got] == [
        ("update", {"ID": 1}), ("delete", {"ID": 3})]
    assert got[0]["values"]["NAME"] == "now" and eng.asked == [0x10, 0x13]
    # gone, and no delete of it follows: not guessed at
    api = _Transcript([_row(30, 6, log.UPDATE, 7, 0x14, b"\x01",
                            whole=False), _rec(31, log.COMMIT, 6)])
    with pytest.raises(SystemExit, match="no longer where the log put it"):
        log.neutral_changes(eng, "src", "app", {"lsn": 29, "from": 29},
                            reader=api)


def test_a_reader_stopped_mid_batch_starts_again_handing_nothing_twice():
    first = [
        _row(40, 7, log.INSERT, 7, 0x20, _image(20, Decimal("1"), "a")),
        _row(41, 8, log.INSERT, 7, 0x21, _image(21, Decimal("2"), "b")),
        _rec(42, log.COMMIT, 7)]
    api = _Transcript(first, per=1)
    got, token = log.neutral_changes(_Eng(), "src", "app",
                                     {"lsn": 39, "from": 39}, limit=1,
                                     reader=api)
    assert [c["key"] for c in got] == [{"ID": 20}]
    # transaction 8 was open when the read stopped: the next starts at it
    assert token == {"lsn": 42, "from": 41}
    # the process stops here; the log goes on, and a new reader resumes
    api = _Transcript(first + [_rec(43, log.COMMIT, 8)], per=1)
    again, token2 = log.neutral_changes(_Eng(), "src", "app", dict(token),
                                        reader=api)
    assert [c["key"] for c in again] == [{"ID": 21}], again
    assert api.calls[0] == 41 and token2 == {"lsn": 43, "from": 44}
    # and once more from the same saved token: the same, nothing more
    api = _Transcript(first + [_rec(43, log.COMMIT, 8)], per=1)
    assert log.neutral_changes(_Eng(), "src", "app", dict(token),
                               reader=api)[0] == again


def test_the_way_is_the_tables_own_logging_and_a_missing_client_a_refusal(
        monkeypatch):
    f = {"library": True, "archived": True, "captured": 2, "tables": 2}
    assert decide.db2_log_way(f).path == "images"
    assert decide.db2_log_way(dict(f, captured=1)).path == "by record"
    got = decide.db2_log_way(dict(f, archived=False))
    assert got.rung is None and "circular" in got.reason
    got = decide.db2_log_way(dict(f, library=False))
    assert got.rung is None and "install" in got.reason
    monkeypatch.setattr(log, "library", lambda: None)
    with pytest.raises(log.Refused) as e:
        log.api()
    said = str(e.value)
    assert "install" in said and "data server client" in said
    for name in ("db2ReadLog", "ctypes", "ibm", "libdb2"):
        assert name.lower() not in said.lower()
    with pytest.raises(log.Refused, match="install"):
        log.way(_Eng(), "src", "app")


def test_the_engine_reaches_the_reader_by_its_one_hook():
    from migkit.engines.db2 import Db2Engine
    reader = Db2Engine.change_log(object.__new__(Db2Engine))
    assert reader is log and callable(reader.neutral_changes)
    assert reader.CHANGE_POINT_READS_ONLY and reader.copy_point


# ---- the tail reaches the reader through the engine (LT.R6) ----------------

def _db2(capture="Y", archive="DISK:/db2/archive/"):
    """A Db2 engine whose catalogue is answered from the recorded rows."""
    from migkit.engines.db2 import Db2Engine

    class _Real(_Eng, Db2Engine):
        def _rows(self, side, db, sql, args=()):
            if "dbcfg" in sql:
                return [(archive,)]
            return _Eng._rows(self, side, db, sql, args)

    eng = _Real.__new__(_Real)
    _Eng.__init__(eng, capture=capture)
    return eng


def _tail_log(*extra):
    return [
        _row(50, 1, log.INSERT, 7, 0x30, _image(30, Decimal("1.50"), "a")),
        _rec(51, log.COMMIT, 1),
        _row(52, 2, log.INSERT, 7, 0x31, _image(31, Decimal("2.50"), "b")),
        _row(53, 3, log.DELETE, 7, 0x30, _image(30, Decimal("1.50"), "a")),
        _rec(54, log.COMMIT, 3), *extra]


def test_the_tail_reads_the_log_through_the_engine_and_hands_on_once(
        monkeypatch):
    eng = _db2()
    assert eng.CHANGE_POINT_READS_ONLY and eng._log_readable()
    monkeypatch.setattr(log, "api", lambda: _Transcript([
        _rec(48, log.COMMIT, 9)]))
    # the position is taken first (before a copy), and is only a read
    point = eng.change_point("src", "app")
    assert point == {"lsn": 49, "from": 49}
    assert eng.neutral_changes("src", "app") == ([], point)
    assert eng.copy_point("src", "app") == point
    # the log goes on: the first read hands the transactions committed
    api = _Transcript(_tail_log(_rec(55, log.COMMIT, 2)), per=2)
    monkeypatch.setattr(log, "api", lambda: api)
    got, token = eng.neutral_changes("src", "app", dict(point))
    assert [(c["op"], c["key"]) for c in got] == [
        ("insert", {"ID": 30}), ("delete", {"ID": 30}),
        ("insert", {"ID": 31})]
    assert token == {"lsn": 55, "from": 56}
    # from the saved position: nothing again
    assert eng.neutral_changes("src", "app", dict(token))[0] == []


def test_a_tail_stopped_in_the_engine_resumes_twice_to_the_same_result(
        monkeypatch):
    eng = _db2()
    full = _tail_log(_rec(55, log.COMMIT, 2))
    monkeypatch.setattr(log, "api", lambda: _Transcript(full[:4], per=1))
    got, saved = eng.neutral_changes("src", "app",
                                     {"lsn": 49, "from": 49}, limit=1)
    assert [c["key"] for c in got] == [{"ID": 30}]
    # the process dies holding `saved`; two fresh ones resume from it
    monkeypatch.setattr(log, "api", lambda: _Transcript(full, per=1))
    first = eng.neutral_changes("src", "app", dict(saved))
    second = eng.neutral_changes("src", "app", dict(saved))
    assert first == second
    assert [(c["op"], c["key"]) for c in first[0]] == [
        ("delete", {"ID": 30}), ("insert", {"ID": 31})]
    # what the first read handed on is not handed on again
    handed = [c["key"] for c in got] + [c["key"] for c in first[0]]
    assert handed == [{"ID": 30}, {"ID": 30}, {"ID": 31}]
    assert first[1] == {"lsn": 55, "from": 56}


def test_a_source_that_cannot_be_read_is_said_never_stepped_over(
        monkeypatch):
    from migkit.engines.db2 import Db2Engine
    eng = _db2()
    monkeypatch.setattr(log, "library", lambda: None)
    assert not eng._log_readable()
    for call in (lambda: eng.change_point("src", "app"),
                 lambda: eng.neutral_changes("src", "app"),
                 lambda: eng.neutral_changes("src", "app",
                                             {"lsn": 1, "from": 1})):
        with pytest.raises(log.Refused) as e:
            call()
        assert "install" in str(e.value)
        assert "db2readlog" not in str(e.value).lower()
    # no client: the delta check takes the next rung, not the log
    assert decide.db2_delta_way({"log": eng._log_readable(), "stamped": 1,
                                 "tables": 1, "same": True}).path == \
        "stamped"
    # a circular log is refused by name, the tail not started on it
    monkeypatch.setattr(log, "library", lambda: object())
    with pytest.raises(log.Refused, match="circular"):
        _db2(archive="OFF").change_point("src", "app")
    assert isinstance(eng, Db2Engine)


def test_only_the_platform_that_has_the_reader_follows_through_it():
    from migkit import capabilities
    from migkit.engines.base import Engine
    from migkit.engines.db2 import Db2Engine
    from migkit.engines.db2i import Db2iEngine
    from migkit.engines.db2zos import Db2ZosEngine
    assert Db2Engine.neutral_changes is not Engine.neutral_changes
    assert Db2Engine.change_point is not Engine.change_point
    # IBM i follows by its journal, z/OS by nothing it may read
    assert Db2iEngine.neutral_changes is not Db2Engine.neutral_changes
    assert Db2iEngine.change_point is not Db2Engine.change_point
    assert Db2ZosEngine.neutral_changes is Engine.neutral_changes
    assert Db2ZosEngine.change_point is Engine.change_point
    assert not Db2ZosEngine._log_readable(object.__new__(Db2ZosEngine))
    assert not Db2ZosEngine.CHANGE_POINT_READS_ONLY
    assert "stream" not in capabilities.GAPS["db2"]
    from migkit.engines import db2
    assert "tail.follows" in db2.UNPROVED_ITEMS.split()
    assert not capabilities.depth_failing().get("untested")
