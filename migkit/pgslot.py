"""Reading what a PostgreSQL logical slot says, when it says it in text.

PostgreSQL ships two output plugins and neither is comfortable. `pgoutput` is
the one replication actually uses and it speaks a binary protocol that needs
its own decoder. `test_decoding` is readable over an ordinary connection
through `pg_logical_slot_get_changes`, and is named after what its authors
intended it for. `wal2json`, which emits JSON and needs no parsing at all, is
not in a stock server - measured, a `postgres:16` image carries exactly
`pgoutput.so` and `test_decoding.so`.

So this file parses `test_decoding`, and does it properly rather than with a
split on spaces, because the format has three traps that a quick parse walks
straight into. All three were read off a running server:

    table public.t: INSERT: id[bigint]:9
      name[character varying]:'has '' quote and : colon and [bracket]'
      amount[numeric]:1.0000 b[bytea]:null flag[boolean]:false

1. A quoted value can contain the separator, the colon, and the brackets that
   delimit the type. Splitting on any of them tears the value apart.
2. `''` inside quotes is one apostrophe, and the value ends at the quote that
   is *not* doubled.
3. `null` unquoted is NULL; `'null'` quoted is the four-letter word. A parse
   that compares the text would confuse a column holding "null" with one
   holding nothing.

And two shapes that are not traps but are easy to miss:

    UPDATE: id[bigint]:3 name[...]:'plain' ...
    UPDATE: old-key: id[bigint]:1 new-tuple: id[bigint]:3 name[...]:'moved'

The first is an UPDATE that did not move the key: there is one tuple and it is
the new one. The second appears when the key changed, or when the table is set
to REPLICA IDENTITY FULL - and then `old-key` is the whole previous row rather
than just its key.

One more, measured and worth knowing: with REPLICA IDENTITY FULL a DELETE's
old tuple **omits the columns that were NULL**. A parser that treated a
missing column as "this column does not exist" would build a key out of
whatever happened to be non-null in that row.
"""
import json


# What the plugin prints for a value that is not there. Unquoted, which is
# what tells it apart from the string.
NULL_TOKEN = "null"

# What the plugin prints, unquoted, for a large value stored out of line
# that an UPDATE did not touch: the log holds a pointer to it, not the
# value. Read as text, it was written over the target's value
UNCHANGED_TOAST = "unchanged-toast-datum"

# The markers that split an UPDATE into its two tuples.
OLD_KEY = "old-key:"
NEW_TUPLE = "new-tuple:"


def _unquote(text):
    """A quoted value with its doubled apostrophes collapsed."""
    return text[1:-1].replace("''", "'")


def _scan_fields(body):
    """[(name, declared type, raw value, quoted)] from one tuple's text.

    Hand-written rather than a regular expression: the value is quoted with
    an escape that is itself the quote character, and expressing "ends at the
    first apostrophe that is not doubled" as a regex is where a parser like
    this usually goes wrong quietly.
    """
    out, i, n = [], 0, len(body)
    while i < n:
        while i < n and body[i] == " ":
            i += 1
        if i >= n:
            break
        open_bracket = body.find("[", i)
        if open_bracket < 0:
            raise ValueError(f"no column type in {body[i:i + 60]!r}")
        name = body[i:open_bracket]
        # the bracket followed by the colon: an array's type has brackets
        # of its own (`integer[]`), and the first `]` ended it too soon
        close_bracket = body.find("]:", open_bracket)
        if close_bracket < 0:
            raise ValueError(f"unclosed type in {body[i:i + 60]!r}")
        declared = body[open_bracket + 1:close_bracket]
        if close_bracket + 1 >= n or body[close_bracket + 1] != ":":
            raise ValueError(f"no value after {name}[{declared}]")
        i = close_bracket + 2
        if i < n and body[i] == "'":
            j = i + 1
            while j < n:
                if body[j] == "'":
                    if j + 1 < n and body[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            if j >= n:
                raise ValueError(f"unterminated value for {name}")
            out.append((name, declared, _unquote(body[i:j + 1]), True))
            i = j + 1
        else:
            j = body.find(" ", i)
            if j < 0:
                j = n
            out.append((name, declared, body[i:j], False))
            i = j
    return out


def parse_line(line):
    """One decoded line as a dict, or None for a line that is not a change.

    `BEGIN`, `COMMIT` and the plugin's other bookkeeping come back as None:
    they are real output, not errors, and a transaction boundary carries
    nothing migkit applies.

    The dict is deliberately not a change record yet - it still holds the
    PostgreSQL type names, which is what turns the text back into values.
    """
    line = (line or "").strip()
    if not line or not line.startswith("table "):
        return None
    head, sep, rest = line[len("table "):].partition(": ")
    if not sep:
        raise ValueError(f"no table name in {line[:80]!r}")
    op, sep, body = rest.partition(": ")
    if not sep and rest.strip().upper().startswith("TRUNCATE"):
        op, sep, body = rest.strip(), ":", ""
    if not sep:
        raise ValueError(f"no operation in {line[:80]!r}")
    op = op.strip().lower().rstrip(":")
    if op == "truncate":
        # `table public.a, public.b: TRUNCATE: (no-flags)` - one line for
        # every table the statement emptied, read off 14 and 16
        return {"table": head.split(",")[0].strip(), "op": "truncate",
                "tables": [t.strip() for t in head.split(",") if t.strip()],
                "old": [], "new": []}
    if op not in ("insert", "update", "delete"):
        raise ValueError(f"unknown operation {op!r} in {line[:80]!r}")

    old, new = [], []
    if body.startswith(OLD_KEY):
        body = body[len(OLD_KEY):]
        marker = body.find(" " + NEW_TUPLE + " ")
        if marker < 0:
            raise ValueError(f"{OLD_KEY} with no {NEW_TUPLE} in"
                             f" {line[:80]!r}")
        old = _scan_fields(body[:marker])
        new = _scan_fields(body[marker + len(NEW_TUPLE) + 2:])
    elif op == "delete":
        old = _scan_fields(body)
    else:
        new = _scan_fields(body)
    return {"table": head.strip(), "op": op, "old": old, "new": new}


def commit_time(line):
    """The time a `COMMIT` line says the source committed, in seconds since
    the epoch, or None where it says none: the plugin prints it with
    `include-timestamp`, in the session's zone, which the tail's
    connection holds at UTC -

        COMMIT 745 (at 2026-09-30 10:00:00.123456+00)
    """
    import datetime
    import re
    got = re.search(r"\(at ([^)]+)\)\s*$", line or "")
    if not got:
        return None
    text = got.group(1).strip()
    if re.search(r"[+-]\d\d$", text):
        text += ":00"
    try:
        return datetime.datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


#: the prefix of the logical message a two-way tail writes first in each
#: transaction it applies (`marks`)
MARK_PREFIX = "migkit"
_MESSAGE = f"message: transactional: 1 prefix: {MARK_PREFIX}, sz: "


def mark_message(line):
    """The content of migkit's own transactional message, or None for any
    other line. Read off PostgreSQL 14 and 16:

        message: transactional: 1 prefix: migkit, sz: 5 content:hello

    printed after the transaction's BEGIN and before its rows. A message
    written outside a transaction prints `transactional: 0` and belongs
    to none, and one under another prefix is not migkit's."""
    line = (line or "").strip()
    if not line.startswith(_MESSAGE):
        return None
    _, sep, content = line[len(_MESSAGE):].partition(" content:")
    return content if sep else None


def _cstring(buf, at):
    end = buf.index(b"\x00", at)
    return buf[at:end].decode("utf-8", "replace"), end + 1


def pgoutput_origin(buf):
    """(origin name, its commit position) of the binary protocol's Origin
    message: 'O', the commit's position on the origin as an Int64, the
    name null-terminated."""
    buf = bytes(buf)
    if buf[:1] != b"O":
        return None
    lsn = int.from_bytes(buf[1:9], "big")
    name, _ = _cstring(buf, 9)
    return name, f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def pgoutput_message(buf, streamed=False):
    """(transactional, prefix, content) of the binary protocol's Message:
    'M', the transaction's id where it is being streamed, a flag byte (1
    for a transactional message), the message's position as an Int64,
    the prefix null-terminated, an Int32 length and the content."""
    buf = bytes(buf)
    if buf[:1] != b"M":
        return None
    at = 5 if streamed else 1
    transactional = bool(buf[at])
    at += 1 + 8
    prefix, at = _cstring(buf, at)
    size = int.from_bytes(buf[at:at + 4], "big")
    return transactional, prefix, buf[at + 4:at + 4 + size]


def unchanged(raw, quoted):
    """Whether a field is the plugin's word for a large value the change
    did not touch (`UNCHANGED_TOAST`) - never a value of that text, which
    is quoted."""
    return not quoted and raw == UNCHANGED_TOAST


def value(declared, raw, quoted):
    """One field turned back into something a driver can be handed.

    `quoted` is not decoration: it is the only thing separating a NULL from
    the string "null", and the only thing separating the boolean `true` from
    a text column holding the word.
    """
    from . import canon
    if not quoted and raw == NULL_TOKEN:
        return None
    element = canon.array_element("postgres", declared)
    if element is not None:
        return array(raw, canon.type_class("postgres", element))
    cls = canon.type_class("postgres", declared)
    return canon.from_text(cls, raw) if cls else raw


def array(raw, cls):
    """PostgreSQL's text of an array - `{1,NULL,"a b"}`, `{{1,2},{3,4}}`,
    `[0:1]={5,6}` - as the nested list it is, each element turned back
    into its value by its class."""
    from . import canon
    text = str(raw)
    if text.startswith("["):
        text = text[text.index("=") + 1:]
    at = 0

    def one():
        nonlocal at
        if text[at] == "{":
            at += 1
            out = []
            if text[at] == "}":
                at += 1
                return out
            while True:
                out.append(one())
                if text[at] == ",":
                    at += 1
                    continue
                if text[at] == "}":
                    at += 1
                    return out
                raise ValueError(f"no , or }} at {at} in {text!r}")
        if text[at] == '"':
            at += 1
            chars = []
            while text[at] != '"':
                if text[at] == "\\":
                    at += 1
                chars.append(text[at])
                at += 1
            at += 1
            return canon.from_text(cls, "".join(chars))
        end = at
        while text[end] not in ",}":
            end += 1
        word, at = text[at:end], end
        return None if word.upper() == "NULL" else canon.from_text(cls, word)
    return one()


def change(parsed, keys, txn=None):
    """A parsed line as a `canon.change` record, given the table's key -
    and the transaction it committed in, where the caller read it.

    `keys` comes from the catalogue rather than from the line, because the
    line does not always carry one: an UPDATE that did not move the key has
    no `old-key` section at all, and a DELETE under REPLICA IDENTITY FULL
    drops whatever columns were NULL.

    A large value stored out of line that an UPDATE did not touch comes
    as the plugin's placeholder (`unchanged`), not the value: taken from
    the whole previous row where the table keeps one, and otherwise left
    out of the change, so the target keeps the value it holds - the
    change names those columns (`canon.UNCHANGED`) for the tail's
    accounts.
    """
    from . import canon
    table = parsed["table"]
    old = {n: value(t, v, q) for n, t, v, q in parsed["old"]
           if not unchanged(v, q)}
    new, kept = {}, []
    for n, t, v, q in parsed["new"]:
        if not unchanged(v, q):
            new[n] = value(t, v, q)
        elif n in old:
            new[n] = old[n]
        else:
            kept.append(n)
    if parsed["op"] == "delete":
        source = old or new
    else:
        source = old or new
    missing = [k for k in keys if k not in source]
    if missing:
        raise ValueError(
            f"{table}: the change log did not carry {', '.join(missing)},"
            " so migkit cannot say which row it meant. Set REPLICA IDENTITY"
            f" to a unique index on {table}, or exclude it")
    key = {k: source[k] for k in keys}
    # the whole previous row, where the table keeps one (REPLICA IDENTITY
    # FULL); only a key changing is not a row
    before = old if old and set(old) - set(keys) else None
    if parsed["op"] == "delete":
        return canon.change("delete", table, key, before=before, txn=txn)
    out = canon.change(parsed["op"], table, key, new, before=before,
                       txn=txn)
    if kept:
        out[canon.UNCHANGED] = kept
    return out


def lsn_int(text):
    """'16/B374D848' as the 64-bit number the replication protocol sends."""
    hi, _, lo = str(text).partition("/")
    return (int(hi, 16) << 32) | int(lo or "0", 16)


def lsn_text(n):
    return f"{int(n) >> 32:X}/{int(n) & 0xFFFFFFFF:X}"


class Stream:
    """A logical slot read as the server streams it (START_REPLICATION on
    a replication connection) instead of peeked again batch by batch: a
    peek decodes the slot from its confirmed position on every call, so a
    tail behind by N changes decodes N each batch; the stream decodes each
    change once and pushes it as it commits. Lines come as the rows a peek
    gives - (lsn, xid, line) - so the same reader turns them into changes.

    The slot moves only by `confirm`: the standby status sent with the
    flushed LSN is the position a caller handed back as applied, never
    what was read. A stream lost and opened again starts from the slot's
    confirmed position, which re-sends what was not confirmed - the
    appliers converge on it by key."""

    def __init__(self, connect, slot, start=None, options=None, pace=None):
        self.pace, self.said = pace, None
        self.conn = connect()
        self.cur = self.conn.cursor()
        self.cur.start_replication(
            slot_name=slot, decode=True,
            start_lsn=lsn_int(start) if start else 0,
            options=dict(options or {}))
        self.xid, self.end, self.confirmed = None, None, None

    def confirm(self, lsn):
        if lsn and lsn != self.confirmed:
            self.cur.send_feedback(flush_lsn=lsn_int(lsn), reply=True)
            self.confirmed = lsn

    def beat(self, now):
        """The standby status sent again - the confirmed position, nothing
        read past it - once the pace's heartbeat (`tailctl.Pace`) has gone
        by since the last: a busy source hears it as often as a batch
        lands, a quiet one once in the tail's longest wait."""
        if self.pace is None:
            return
        if self.said is not None and now - self.said < self.pace.heartbeat():
            return
        self.said = now
        self.cur.send_feedback(flush_lsn=lsn_int(self.confirmed)
                               if self.confirmed else 0, reply=False)

    def rows(self, limit, wait, wait_for=None):
        """Up to `limit` lines, a batch cut only where a transaction ends;
        waits `wait` seconds for the first and none once one transaction
        is whole. `end` is the log's end the server's keepalive said."""
        import select
        import time
        out, until = [], time.monotonic() + max(wait, 0.0)
        wait_for = wait_for or (lambda s: select.select([self.cur], [], [],
                                                       s))
        while True:
            msg = self.cur.read_message()
            if msg is None:
                self.beat(time.monotonic())
                said = getattr(self.cur, "wal_end", 0) or 0
                if said:
                    self.end = lsn_text(said)
                left = until - time.monotonic()
                if self.xid is None and (out or left <= 0):
                    return out
                wait_for(max(left, 0.05))
                continue
            line = msg.payload
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            if line.startswith("BEGIN"):
                self.xid = line.split()[1] if len(line.split()) > 1 \
                    else self.xid
            out.append((lsn_text(msg.data_start), self.xid, line))
            if line.startswith("COMMIT"):
                self.xid = None
                if len(out) >= limit:
                    return out

    def close(self):
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001 - it is being let go
            pass


def _values(c):
    return c.get("values") if "values" in c else c.get("row")


def columnar(changes, compiled=None, ordered=False):
    """A batch of `canon.change` records as Arrow record batches, one per
    (table, op, columns): the key and row values as typed columns, the
    types taken from the values canon gave them. The compiled builder
    (Arrow's own, from Python objects in one call) where the library has
    it and `compiled` is not False; the pure one - column lists built
    here, each turned into an array - otherwise. The two are equal batch
    for batch (`test_the_tail_streams_and_decodes_in_batches`).

    `ordered` is the batch as an applier takes it: runs of changes next to
    each other, a run ended by another table, op or set of columns, or by
    a key seen again in it - so the batches applied one after another make
    the changes' own order, a row's insert before its delete."""
    import pyarrow as pa
    groups, run, keys = {}, None, set()
    for c in changes:
        row = {**{f"key.{k}": v for k, v in (c.get("key") or {}).items()},
               **{f"row.{k}": v for k, v in (_values(c) or {}).items()}}
        shape = (c["table"], c["op"], tuple(row))
        if ordered:
            ident = json.dumps(sorted((c.get("key") or {}).items()),
                               default=repr)
            if run is None or run[0] != shape or ident in keys:
                run, keys = (shape, len(groups)), set()
            keys.add(ident)
            shape = run
        groups.setdefault(shape, []).append(row)
    fast = compiled is not False and hasattr(pa.RecordBatch, "from_pylist")
    out = []
    for shape, rows in groups.items():
        (table, op, names) = shape[0] if ordered else shape
        if fast:
            batch = pa.RecordBatch.from_pylist(rows)
        else:
            batch = pa.RecordBatch.from_arrays(
                [pa.array([r[n] for r in rows]) for n in names],
                names=list(names))
        out.append((table, op, batch))
    return out


def _same(a, b):
    if type(a) is not type(b):
        return False
    if isinstance(a, float) and a != a:
        return b != b
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(_same(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(map(_same, a, b))
    return a == b


def exact(changes, batches):
    """None where the ordered batches (`columnar`) give back every change
    of `changes` - in order, each value of the same type and equal, the
    key never moved - else why not: a value Arrow types as another (an
    integer past 64 bits, text and numbers in one column, a value canon
    marks as untouched) goes as the change records it came as."""
    at = iter(changes)
    for table, op, batch in batches:
        cols = {n: batch.column(i).to_pylist()
                for i, n in enumerate(batch.schema.names)}
        for i in range(batch.num_rows):
            c = next(at, None)
            if c is None or (c["table"], c["op"]) != (table, op):
                return "the columns do not follow the changes' order"
            if c.get("before") is not None and any(
                    k in c["before"] and not _same(c["before"][k], v)
                    for k, v in (c.get("key") or {}).items()):
                return f"a change to {table} moved a row's key"
            for prefix, part in (("key.", c.get("key")),
                                 ("row.", _values(c))):
                for k, v in (part or {}).items():
                    if not _same(v, cols[prefix + k][i]):
                        return (f"a value of {table}.{k} is not typed"
                                " exactly as columns")
    if next(at, None) is not None:
        return "the columns hold fewer changes than the batch"
    return None
