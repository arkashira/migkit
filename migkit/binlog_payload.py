"""MySQL's compressed transactions, read rather than stopped on (backlog 6).

With `binlog_transaction_compression = ON` (MySQL 8.0.20 and later), a
transaction's events are written as one TRANSACTION_PAYLOAD event: a few
header fields, then the events themselves, compressed. The binlog reader
migkit uses does not know the event, and skipped what it held without a
word, which is why the tail used to stop on it.

This reads it. The header's fields are each a type, a length and a value,
packed as MySQL packs integers, up to an end mark. What follows is the
payload, compressed with zstd (or not at all, where the header says so).
Decompressed, it is the transaction's events one after another, in the
form the log writes them. Each one is handed to the reader's own event
parser as if it had arrived on its own, and table maps update the reader's
table map as they would there. So a row event read out of a compressed
transaction is the same object as one read from an uncompressed one.

MariaDB's compressed events (`log_bin_compress`) are another format, read
by `MariadbCompressedEvent` below: each is one event whose rows (or whose
statement text) are compressed with zlib behind a one-byte header. The part
in front of them is written as in the plain event, so it is read as there,
the compressed part is opened, and the plain event rebuilt from the two is
handed to the reader's own parser. What is not known about the format is
said where it is used; where what arrives is not what is expected, the
event is not read and the tail stops (`error`) - it is never passed over.
"""
import struct
import zlib

from pymysqlreplication.event import BinLogEvent

TRANSACTION_PAYLOAD = 0x28

#: header field types
END, SIZE, COMPRESSION, UNCOMPRESSED_SIZE = 0, 1, 2, 3
ZSTD, NONE = 0, 255


def _packed(data, at):
    """(value, next offset) of an integer packed as MySQL packs them: one
    byte below 251, else a marker byte and 2, 3 or 8 bytes."""
    first = data[at]
    if first < 251:
        return first, at + 1
    size = {252: 2, 253: 3, 254: 8}.get(first)
    if size is None:
        raise ValueError(f"no packed integer starts with {first}")
    return int.from_bytes(data[at + 1:at + 1 + size], "little"), \
        at + 1 + size


def header(body):
    """({field: value}, offset of the payload)."""
    fields, at = {}, 0
    while True:
        kind, at = _packed(body, at)
        if kind == END:
            return fields, at
        length, at = _packed(body, at)
        value, _ = _packed(body, at)
        fields[kind] = value
        at += length


def inner_events(raw):
    """Each event in a decompressed payload, as its bytes."""
    at = 0
    while at + 19 <= len(raw):
        size = int.from_bytes(raw[at + 9:at + 13], "little")
        if size < 19:
            raise ValueError(f"an event of {size} bytes inside a payload")
        yield raw[at:at + size]
        at += size


class TransactionPayloadEvent(BinLogEvent):
    """The transaction's events, parsed, in `events`: row events and the
    rest, in the order written."""

    def __init__(self, from_packet, event_size, table_map, ctl_connection,
                 **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection,
                         **kwargs)
        body = self.packet.read(event_size)
        fields, at = header(body)
        payload = body[at:]
        if fields.get(COMPRESSION, ZSTD) == ZSTD:
            import zstandard
            payload = zstandard.ZstdDecompressor().decompress(
                payload, max_output_size=fields.get(UNCOMPRESSED_SIZE, 0)
                or 64 * 2 ** 20)
        self.events = list(self._parse(payload, table_map, ctl_connection,
                                       kwargs))

    #: what the stream passes the parser, by the name the parser takes
    PASSED = ("only_tables", "ignored_tables", "only_schemas",
              "ignored_schemas", "freeze_schema", "ignore_decode_errors",
              "optional_meta_data", "enable_logging",
              "use_column_name_cache", "post_header_lengths")

    @classmethod
    def _parse(cls, payload, table_map, ctl_connection, kwargs):
        from pymysql.protocol import MysqlPacket
        from pymysqlreplication.event import QueryEvent, XidEvent
        from pymysqlreplication.packet import BinLogPacketWrapper
        from pymysqlreplication.row_event import (DeleteRowsEvent,
                                                  TableMapEvent,
                                                  UpdateRowsEvent,
                                                  WriteRowsEvent)
        from .binlog_marks import registered
        allowed = {TableMapEvent, WriteRowsEvent, UpdateRowsEvent,
                   DeleteRowsEvent, QueryEvent, XidEvent, *registered()}
        for raw in inner_events(payload):
            # the ok byte a packet from the server starts with; the events
            # in a payload carry no checksum of their own
            packet = MysqlPacket(b"\x00" + raw, "utf8")
            wrapped = BinLogPacketWrapper(
                packet, table_map, ctl_connection,
                kwargs.get("mysql_version", (0, 0, 0)), False, allowed,
                *(kwargs.get(k) for k in cls.PASSED[:6]),
                False, *(kwargs.get(k) for k in cls.PASSED[6:]))
            ev = wrapped.event
            if ev is None:
                continue
            if isinstance(ev, TableMapEvent):
                table_map[ev.table_id] = ev.get_table()
            yield ev


#: MariaDB's compressed events, by type (log_event.h; the reader's own
#: constants stop at 164): the statement, and the rows of version 1, each
#: with the plain event it is written as
QUERY_COMPRESSED = 165
ROWS_COMPRESSED = {166: 23, 167: 24, 168: 25}   # write, update, delete
#: the version-2 rows (169-171) are not read: nothing has shown a MariaDB
#: that writes them, and their post-header is not known to be MySQL's
ROWS_COMPRESSED_V2 = (169, 170, 171)
UPDATE_ROWS = 24
#: bytes in front of a rows event's column count: table id and flags
ROWS_POST_HEADER = 8
#: bytes in front of a statement's status variables
QUERY_POST_HEADER = 13


class Unreadable(ValueError):
    """A compressed event that is not what is expected of one."""


def unpack(block, limit=None):
    """The bytes a compressed block holds. The block is one header byte -
    bits 0-2 the number of bytes that give the uncompressed length, bits
    4-6 the algorithm, 0 being zlib - then that length, then the zlib
    stream. Nothing here is taken on trust: a stream that is not complete,
    not valid, with bytes after it, or not the length the block says is
    refused. The byte order of the length is not known (big-endian is what
    is remembered of the server's code, never read from a server), so the
    length is a check that either order can pass, while the stream's own
    checksum is what vouches for the bytes."""
    if not block:
        raise Unreadable("the compressed part is empty")
    head = block[0]
    size, algorithm = head & 7, (head >> 4) & 7
    if algorithm != 0:
        raise Unreadable(f"it names compression algorithm {algorithm}, and"
                         " only zlib (0) is known")
    if not 1 <= size <= 4 or len(block) < 1 + size:
        raise Unreadable(f"its header byte {head:#04x} gives a length of"
                         f" {size} bytes, which is not one to 4 and"
                         " present")
    said = {int.from_bytes(block[1:1 + size], "big"),
            int.from_bytes(block[1:1 + size], "little")}
    cap = limit if limit is not None else max(said)
    stream = zlib.decompressobj()
    try:
        out = stream.decompress(block[1 + size:], cap + 1)
    except zlib.error as e:
        raise Unreadable(f"its zlib stream is corrupt ({e})") from None
    if len(out) > cap:
        raise Unreadable(f"it holds more than the {cap} bytes it allows")
    if not stream.eof:
        raise Unreadable("its zlib stream ends before it is complete")
    if stream.unused_data:
        raise Unreadable(f"{len(stream.unused_data)} bytes follow its zlib"
                         " stream")
    if len(out) not in said:
        raise Unreadable(f"it holds {len(out)} bytes, and its header says"
                         f" {sorted(said)}")
    return out


def opened(event_type, body, declared=None):
    """(the plain event's type, the plain event's body) of a MariaDB
    compressed event's body. `declared`: the post-header lengths the log
    began with, checked against what is read here."""
    if event_type == QUERY_COMPRESSED:
        plain, known = 2, QUERY_POST_HEADER
    elif event_type in ROWS_COMPRESSED:
        plain, known = ROWS_COMPRESSED[event_type], ROWS_POST_HEADER
    else:
        raise Unreadable(f"event type {event_type} is not a compressed"
                         " event of a layout known here")
    if declared and event_type - 1 < len(declared) \
            and declared[event_type - 1] != known:
        raise Unreadable(f"the log declares a post-header of"
                         f" {declared[event_type - 1]} bytes for it, and"
                         f" {known} is what is read")
    try:
        if plain == 2:
            # thread id, time, database name length, error code, length of
            # the status variables, those, the name and its end byte
            _, _, dblen, _, status = struct.unpack("<IIBHH", body[:13])
            at = known + status + dblen + 1
        else:
            width, at = _packed(body, known)
            at += (width + 7) // 8 * (2 if plain == UPDATE_ROWS else 1)
    except (struct.error, ValueError, IndexError) as e:
        raise Unreadable(f"the part in front of the compressed data cannot"
                         f" be read ({e})") from None
    if at > len(body):
        raise Unreadable("the part in front of the compressed data is"
                         " longer than the event")
    return plain, body[:at] + unpack(body[at:])


class MariadbCompressedEvent(BinLogEvent):
    """A MariaDB compressed statement or rows event, opened. `events`: the
    plain event it holds, parsed by the reader's own parser (empty where
    the reader's filters left it out). `error`: why it could not be read,
    else None - the caller stops on that."""
    error = None

    def __init__(self, from_packet, event_size, table_map, ctl_connection,
                 **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection,
                         **kwargs)
        self.events = []
        body = self.packet.read(event_size)
        try:
            plain, body = opened(self.event_type, body,
                                 kwargs.get("post_header_lengths"))
            head = struct.pack(
                "<IBIIIH", self.packet.timestamp, plain,
                self.packet.server_id, 19 + len(body), self.packet.log_pos,
                self.packet.flags)
            self.events = list(TransactionPayloadEvent._parse(
                head + body, table_map, ctl_connection, kwargs))
        except Unreadable as e:
            self.error = str(e)
        except Exception as e:  # the parser's own, on bytes it cannot read
            self.error = f"it could not be parsed ({type(e).__name__}: {e})"


def register():
    """Teach the reader the events, once: its parser looks events up in a
    table of its own, by type. The classes to name in `only_events`."""
    from pymysqlreplication.packet import BinLogPacketWrapper
    table = BinLogPacketWrapper._BinLogPacketWrapper__event_map
    table[TRANSACTION_PAYLOAD] = TransactionPayloadEvent
    for kind in (QUERY_COMPRESSED, *ROWS_COMPRESSED):
        table[kind] = MariadbCompressedEvent
    return TransactionPayloadEvent, MariadbCompressedEvent
