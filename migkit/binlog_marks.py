"""The binlog events a two-way tail's marks travel in, as migkit reads them
(`marks`): MySQL's tagged GTID, which the binlog reader does not know,
and each statement's text logged with its rows.

From 8.3 a transaction's GTID may carry a tag (`UUID:tag:n`); such a GTID
is written as an event of a type of its own (42) in MySQL's field
serialization, not in the fixed layout of the untagged one, and the
reader hands it back as an event it cannot open. Opened here: a message
of numbered fields, each number and each integer a variable-length
integer whose first byte's trailing one bits say how many bytes follow;
signed integers with the sign moved to the lowest bit.
"""

#: the event type of a tagged GTID
GTID_TAGGED = 42
#: the event type of a statement's text logged with its rows
ROWS_QUERY = 0x1D
#: the tag of the GTIDs a two-way tail applies under
TAG = "migkit"
#: what every statement a two-way tail applies starts with, on the rung
#: that marks them so (`marks`, `comment`)
MARK_COMMENT = "/* migkit two-way */ "


def varlen(buf, at):
    """(value, next offset) of the unsigned variable-length integer at
    `at`: one byte and as many more as its trailing one bits count, the
    value in the bits above them, least significant byte first."""
    first = buf[at]
    extra = 0
    while extra < 8 and first >> extra & 1:
        extra += 1
    if extra == 8:
        return int.from_bytes(buf[at + 1:at + 9], "little"), at + 9
    raw = int.from_bytes(buf[at:at + 1 + extra], "little")
    return raw >> (extra + 1), at + 1 + extra


def signed(u):
    return (u >> 1) ^ -(u & 1)


def tagged_gtid(body):
    """{"flags", "uuid", "gno", "tag"} of a tagged GTID event's body.

    Read off MySQL 8.4 (`bench/marks_probe_my.py`), four events whose GTIDs
    were set by hand: the format's version (1), the message's size - the
    whole body, which is checked - and the last field a reader cannot do
    without (0); then numbered fields: 0 the flags, 1 the UUID as sixteen
    integers of a byte each, 2 the number (signed), 3 the tag, its length
    first. A body that does not add up raises: which transaction is
    migkit's is not guessed."""
    import uuid as _uuid
    _, at = varlen(body, 0)
    size, at = varlen(body, at)
    if size != len(body):
        raise ValueError(f"a tagged GTID event of {len(body)} bytes says it"
                         f" is {size}: not the layout migkit reads")
    _, at = varlen(body, at)
    out = {}
    while at < len(body) and len(out) < 4:
        field, at = varlen(body, at)
        if field == 0:
            out["flags"], at = varlen(body, at)
        elif field == 1:
            raw = bytearray()
            for _ in range(16):
                b, at = varlen(body, at)
                raw.append(b)
            out["uuid"] = str(_uuid.UUID(bytes=bytes(raw)))
        elif field == 2:
            u, at = varlen(body, at)
            out["gno"] = signed(u)
        elif field == 3:
            n, at = varlen(body, at)
            out["tag"] = body[at:at + n].decode("ascii", "replace")
            at += n
        else:
            break
    return out


def gtid_top(executed, uuid, tag):
    """The highest number `executed` (a GTID set as the server prints it)
    holds for `uuid:tag`, 0 where it holds none."""
    top = 0
    for one in str(executed or "").replace("\n", "").split(","):
        parts = one.strip().split(":")
        if not parts or parts[0].lower() != uuid.lower():
            continue
        now = ""
        for p in parts[1:]:
            if p and not p[0].isdigit():
                now = p
            elif now == tag and p:
                top = max(top, int(p.rpartition("-")[2]))
    return top


def _classes():
    from pymysqlreplication.event import BinLogEvent

    class TaggedGtidEvent(BinLogEvent):
        """A transaction's tagged GTID (`tagged_gtid`)."""

        def __init__(self, from_packet, event_size, table_map, ctl_connection,
                     **kwargs):
            super().__init__(from_packet, event_size, table_map,
                             ctl_connection, **kwargs)
            got = tagged_gtid(self.packet.read(event_size))
            self.uuid, self.tag = got.get("uuid"), got.get("tag", "")
            self.gno = got.get("gno")

        @property
        def gtid(self):
            return f"{self.uuid}:{self.tag}:{self.gno}"

    class RowsQueryEvent(BinLogEvent):
        """A statement's text, logged before the rows it wrote: a length
        byte, then the text, to the end of the event."""

        def __init__(self, from_packet, event_size, table_map, ctl_connection,
                     **kwargs):
            super().__init__(from_packet, event_size, table_map,
                             ctl_connection, **kwargs)
            self.query = self.packet.read(event_size)[1:].decode(
                "utf-8", "replace")

    return TaggedGtidEvent, RowsQueryEvent


_REGISTERED = []


def registered():
    """The two classes where `register` has taught the reader them, else
    an empty tuple (nothing is an instance of it)."""
    return tuple(_REGISTERED)


def register():
    """Teach the reader both events, once: its parser looks events up in a
    table of its own, by type. Returns (tagged GTID, rows-query)."""
    if not _REGISTERED:
        from pymysqlreplication.packet import BinLogPacketWrapper
        tagged, rows_query = _classes()
        table = BinLogPacketWrapper._BinLogPacketWrapper__event_map
        table[GTID_TAGGED] = tagged
        table[ROWS_QUERY] = rows_query
        _REGISTERED.extend((tagged, rows_query))
    return tuple(_REGISTERED)
