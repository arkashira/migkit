"""MariaDB's compressed events (`log_bin_compress`) are read, or they stop
the change tail with a reason - they are never passed over.

No server is reached. The events are built here, byte by byte, as the
plain events a server writes (a table map, a write, an update and a
delete, a statement), and the compressed ones are made from the plain ones
with `zlib`: the header byte, the length, the stream, in the layout
`binlog_payload` documents. What this proves is what migkit does with
bytes laid out that way: a compressed event decodes to the rows the plain
one does, through the replication library's own parser; one that is not
what a compressed event should be (corrupt stream, a different algorithm,
a wrong length, bytes after the stream) is refused, and the tail stops on
it with the position it was asked to read from.

What it does NOT prove: that a real MariaDB writes exactly this layout.
Which part of an event is compressed (the rows after the column bitmaps,
the statement after the database name), the byte order of the length and
that MariaDB writes version-1 rows only are from the server's code as
remembered, never read from a server in this work. The test that would
settle it, against a MariaDB with `log_bin_compress = ON`, is
`test_a_compressed_binlog_is_read_or_stops_the_tail.py`
(`test_mariadb_compressed_rows_are_read_by_the_tail`), and has not been
run. Until it is, a layout that differs stops the tail with the reason
(proved here), and is not misread.
"""
import struct
import zlib

import pytest

pytest.importorskip("pymysqlreplication")

from pymysql.protocol import MysqlPacket  # noqa: E402
from pymysqlreplication.event import QueryEvent, XidEvent  # noqa: E402
from pymysqlreplication.packet import BinLogPacketWrapper  # noqa: E402
from pymysqlreplication.row_event import (DeleteRowsEvent,  # noqa: E402
                                          TableMapEvent, UpdateRowsEvent,
                                          WriteRowsEvent)

from migkit import binlog_payload as bp  # noqa: E402
from migkit.engines.mysql import MySQLEngine  # noqa: E402

PLAIN = {"query": 2, "write": 23, "update": 24, "delete": 25}
PACKED = {"query": 165, "write": 166, "update": 167, "delete": 168}
LONG, VARCHAR = 3, 15


class Ctl:
    """What the parser asks of its control connection when the log carries
    no column names: nothing."""
    charset = "utf8"

    def _get_dbms(self):
        return "mariadb"


def _frame(kind, body, pos=1000, checksum=False):
    body = body + (b"\0\0\0\0" if checksum else b"")
    return struct.pack("<IBIIIH", 1700000000, kind, 7,
                       19 + len(body), pos, 0) + body


def _row(i, v):
    # the null bitmap, the int, the length-prefixed string
    return b"\x00" + struct.pack("<i", i) + bytes([len(v)]) + v


def _plain_bodies():
    """The plain events of one transaction, by name."""
    table_map = (struct.pack("<Q", 91)[:6] + b"\0\0"
                 + b"\x05appdb\0" + b"\x01t\0" + b"\x02"
                 + bytes([LONG, VARCHAR]) + b"\x02" + struct.pack("<H", 100)
                 + b"\x02")
    head = struct.pack("<Q", 91)[:6] + b"\0\0" + b"\x02" + b"\x03"
    return {
        "map": table_map,
        # width, one bitmap, then the rows
        "write": head + _row(1, b"alpha") + _row(2, b"b" * 90),
        # width, two bitmaps, then (before, after) pairs
        "update": head[:-1] + b"\x03\x03" + _row(1, b"alpha")
        + _row(1, b"alpha moved"),
        "delete": head + _row(2, b"b" * 90),
        "query": (struct.pack("<IIBHH", 5, 0, 5, 0, 0) + b"appdb\0"
                  + b"alter table t add column w int, add column x int"),
    }


def _pack(raw, lenlen=2, order="big", algorithm=0):
    """A compressed block: the header byte, the length, the zlib stream."""
    head = 0x80 | (algorithm << 4) | lenlen
    return bytes([head]) + len(raw).to_bytes(lenlen, order) \
        + zlib.compress(raw)


def _split(kind, body):
    """(the part kept plain, the part compressed) of a plain body."""
    if kind == "query":
        at = 13 + 0 + 5 + 1       # post-header, no status variables, name
    else:
        at = 8 + 1 + (2 if kind == "update" else 1)   # + width, bitmaps
    return body[:at], body[at:]


def _compressed(kind, body, **how):
    keep, rest = _split(kind, body)
    return keep + _pack(rest, **how)


@pytest.fixture(scope="module", autouse=True)
def registered():
    return bp.register()


def _wrap(raw, table_map, checksum=False):
    packet = MysqlPacket(b"\x00" + raw, "utf8")
    allowed = {TableMapEvent, WriteRowsEvent, UpdateRowsEvent,
               DeleteRowsEvent, QueryEvent, XidEvent,
               bp.MariadbCompressedEvent}
    return BinLogPacketWrapper(
        packet, table_map, Ctl(), (0, 0, 0), checksum, allowed,
        None, None, None, None, False, False, False, False, False, False,
        None).event


def _mapped():
    table_map = {}
    ev = _wrap(_frame(19, _plain_bodies()["map"]), table_map)
    table_map[ev.table_id] = ev.get_table()
    return table_map


def _rows(ev):
    return [dict(r) for r in ev.rows]


@pytest.mark.parametrize("kind", ["write", "update", "delete"])
@pytest.mark.parametrize("checksum", [False, True])
def test_a_compressed_rows_event_decodes_to_the_rows_of_the_plain_one(
        kind, checksum):
    body = _plain_bodies()[kind]
    plain = _wrap(_frame(PLAIN[kind], body, checksum=checksum), _mapped(),
                  checksum)
    packed = _wrap(_frame(PACKED[kind], _compressed(kind, body),
                          checksum=checksum), _mapped(), checksum)
    assert isinstance(packed, bp.MariadbCompressedEvent)
    assert packed.error is None, packed.error
    assert len(packed.events) == 1
    got = packed.events[0]
    assert type(got) is type(plain)
    assert _rows(got) == _rows(plain) != []
    assert (got.schema, got.table) == ("appdb", "t") == (plain.schema,
                                                         plain.table)


def test_the_values_are_the_ones_written():
    body = _plain_bodies()["update"]
    got = _wrap(_frame(167, _compressed("update", body)), _mapped())
    [row] = _rows(got.events[0])
    assert list(row["before_values"].values())[0] == 1
    assert "alpha moved" in str(row["after_values"].values())
    assert "alpha moved" not in str(row["before_values"].values())


@pytest.mark.parametrize("lenlen,order", [(1, "big"), (2, "big"),
                                          (2, "little"), (4, "big")])
def test_the_length_may_be_of_either_byte_order_and_any_size(lenlen, order):
    # the rows are 100 bytes or so: one byte can hold it
    body = _plain_bodies()["write"]
    plain = _wrap(_frame(23, body), _mapped())
    packed = _wrap(_frame(166, _compressed("write", body, lenlen=lenlen,
                                           order=order)), _mapped())
    assert packed.error is None, packed.error
    assert _rows(packed.events[0]) == _rows(plain)


def test_a_compressed_statement_is_read_as_the_plain_one():
    body = _plain_bodies()["query"]
    plain = _wrap(_frame(2, body), {})
    packed = _wrap(_frame(165, _compressed("query", body)), {})
    assert packed.error is None, packed.error
    [got] = packed.events
    assert isinstance(got, QueryEvent)
    assert got.query == plain.query == \
        "alter table t add column w int, add column x int"
    assert got.schema == plain.schema == b"appdb"


def test_reading_it_again_gives_the_same():
    """Idempotent: the same bytes, opened twice, read the same, and the
    reader's table map is not changed by it."""
    body = _plain_bodies()["write"]
    raw = _frame(166, _compressed("write", body))
    table_map = _mapped()
    before = dict(table_map)
    first = _rows(_wrap(raw, table_map).events[0])
    second = _rows(_wrap(raw, table_map).events[0])
    assert first == second != []
    assert table_map == before


@pytest.mark.parametrize("make,why", [
    (lambda rest: bytes([0x82]) + len(rest).to_bytes(2, "big")
     + zlib.compress(rest)[:-6], "zlib"),
    (lambda rest: bytes([0x82]) + len(rest).to_bytes(2, "big")
     + b"\x78\x9c" + b"\xff" * 20, "corrupt"),
    (lambda rest: _pack(rest, algorithm=1), "algorithm 1"),
    (lambda rest: bytes([0x82]) + (len(rest) + 5).to_bytes(2, "big")
     + zlib.compress(rest), "header says"),
    (lambda rest: _pack(rest) + b"tail", "follow"),
    (lambda rest: bytes([0x85]) + b"\0" * 8, "length of 5 bytes"),
    (lambda rest: b"", "empty"),
])
def test_what_is_not_a_compressed_event_is_refused_with_a_reason(make, why):
    body = _plain_bodies()["write"]
    keep, rest = _split("write", body)
    ev = _wrap(_frame(166, keep + make(rest)), _mapped())
    assert isinstance(ev, bp.MariadbCompressedEvent)
    assert ev.events == []
    assert ev.error and why in ev.error, ev.error


def test_a_corrupted_byte_in_the_stream_is_refused():
    body = _plain_bodies()["write"]
    keep, rest = _split("write", body)
    block = bytearray(_pack(rest))
    block[len(block) // 2] ^= 0xFF
    ev = _wrap(_frame(166, keep + bytes(block)), _mapped())
    assert ev.events == [] and ev.error, ev.error


def test_a_layout_that_is_not_the_one_read_is_refused_not_misread():
    """If a server compressed the whole body, rather than the rows after
    their bitmaps, the part taken as plain is not, and what follows it is
    not a zlib block."""
    body = _plain_bodies()["write"]
    ev = _wrap(_frame(166, _pack(body)), _mapped())
    assert ev.events == [] and ev.error, ev.error


def test_a_declared_post_header_that_differs_is_refused():
    declared = [0] * 171
    declared[165] = 9
    with pytest.raises(bp.Unreadable, match="declares a post-header"):
        bp.opened(166, b"\0" * 20, declared)


def test_the_version_two_rows_are_not_read():
    with pytest.raises(bp.Unreadable, match="not a compressed event"):
        bp.opened(169, b"\0" * 20)
    assert 169 in MySQLEngine.COMPRESSED_ROWS


def test_a_bomb_is_not_opened_beyond_what_the_block_says():
    big = zlib.compress(b"\0" * 10_000_000)
    with pytest.raises(bp.Unreadable, match="more than"):
        bp.unpack(bytes([0x82]) + (100).to_bytes(2, "big") + big)


def _stream(*raws, checksum=False):
    table_map = {}
    out = []
    for raw in raws:
        ev = _wrap(raw, table_map, checksum)
        if isinstance(ev, TableMapEvent):
            table_map[ev.table_id] = ev.get_table()
        out.append(ev)
    return out


def test_the_tail_reads_the_rows_of_a_compressed_event_in_order():
    """Through the engine's own flattening of the stream: plain and
    compressed events in one transaction come out as plain ones, with the
    commit after them."""
    b = _plain_bodies()
    xid = struct.pack("<Q", 5)
    raws = [_frame(19, b["map"]),
            _frame(166, _compressed("write", b["write"])),
            _frame(23, b["write"]),
            _frame(167, _compressed("update", b["update"])),
            _frame(168, _compressed("delete", b["delete"])),
            _frame(16, xid)]
    table_map = {}
    stream = []
    for raw in raws:
        ev = _wrap(raw, table_map)
        if isinstance(ev, TableMapEvent):
            table_map[ev.table_id] = ev.get_table()
        stream.append(ev)
    from migkit.engines.mysql import COMMITTED
    got = list(MySQLEngine._row_events(stream))
    assert [type(e).__name__ for e in got] == [
        "TableMapEvent", "WriteRowsEvent", "WriteRowsEvent",
        "UpdateRowsEvent", "DeleteRowsEvent"] + [
        type(COMMITTED).__name__]
    assert got[-1] is COMMITTED
    assert _rows(got[1]) == _rows(got[2])


def test_a_compressed_statement_reaches_the_tail_as_a_statement():
    b = _plain_bodies()
    stream = _stream(_frame(165, _compressed("query", b["query"])))
    from migkit.engines.mysql import COMMITTED
    got = list(MySQLEngine._row_events(stream, ddl=True))
    assert isinstance(got[0], QueryEvent) and got[1] is COMMITTED
    assert "add column w int" in got[0].query


def test_the_tail_stops_on_one_it_cannot_open_and_says_why():
    b = _plain_bodies()
    keep, rest = _split("write", b["write"])
    stream = _stream(_frame(19, b["map"]),
                     _frame(166, keep + _pack(rest, algorithm=1)))
    [_, bad] = list(MySQLEngine._row_events(stream))
    assert isinstance(bad, bp.MariadbCompressedEvent) and bad.error
    token = {"log_file": "bin.000007", "log_pos": 4242}
    with pytest.raises(SystemExit) as e:
        MySQLEngine._compressed_stop(token, bad.event_type, bad.error)
    said = str(e.value)
    assert "log_bin_compress" in said and "algorithm 1" in said, said
    assert "bin.000007" in said and "4242" in said, said
    assert "was applied" in said or "applied" in said, said


def test_the_delta_says_the_same_and_does_not_pass_the_rows():
    said = MySQLEngine._compressed_said(
        {"log_file": "bin.000001", "log_pos": 4}, 167, "verified",
        "its zlib stream is corrupt")
    assert "verified" in said and "corrupt" in said, said


def test_the_old_message_stays_for_a_reader_without_the_events():
    said = MySQLEngine._compressed_said(
        {"log_file": "bin.000001", "log_pos": 4}, 169, "applied")
    assert "set global log_bin_compress = OFF" in said, said


def test_the_events_are_registered_for_the_reader_by_type():
    table = BinLogPacketWrapper._BinLogPacketWrapper__event_map
    assert all(table[t] is bp.MariadbCompressedEvent
               for t in (165, 166, 167, 168))
    assert not any(t in table for t in (169, 170, 171))
    assert set(bp.register()) == {bp.TransactionPayloadEvent,
                                  bp.MariadbCompressedEvent}
