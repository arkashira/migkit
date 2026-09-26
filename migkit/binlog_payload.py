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

MariaDB's compressed row events (`log_bin_compress`) are another format,
and still stop the tail.
"""
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
        allowed = {TableMapEvent, WriteRowsEvent, UpdateRowsEvent,
                   DeleteRowsEvent, QueryEvent, XidEvent}
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


def register():
    """Teach the reader the event, once: its parser looks events up in a
    table of its own, by type."""
    from pymysqlreplication.packet import BinLogPacketWrapper
    table = BinLogPacketWrapper._BinLogPacketWrapper__event_map
    table[TRANSACTION_PAYLOAD] = TransactionPayloadEvent
    return TransactionPayloadEvent
