"""A Db2 (LUW) source's changes read from its own log, through the log-read
API the data server client documents (backlog LT.R6) - the reader
`Db2Engine.change_log` reaches.

The API is a C function of the client's library, loaded here by `ctypes`
only when a read asks for it: a machine without the client is told what
to install, never handed an import error. Each call reads the log records
between two log sequence numbers into a buffer; the records of a table's
row changes carry the table space and table it is in, the row's record id
and its image (`parse`).

What decides how a row is handed on is the table's own setting, read from
`SYSCAT.TABLES.DATACAPTURE` (`decide.db2_log_way`): a table that logs its
whole row hands the row over from the log; one that logs only the bytes
an update changed names the row by its record id, and the row is read
from the source as it is now (`RID()`), which the later changes of the
same row - also in the log - converge on. A row named that way and no
longer found, with no delete of it later in the batch, is not guessed at:
the read stops on the table, named.

The position (`token`) is {"lsn": the commit last handed on, "from": where
the next read starts - the begin of the oldest transaction still open}:
a read started again from "from" sees the transactions it handed back
already, and leaves every one committed at or before "lsn" out.

Written 2026-10-16 without a Db2 server: the record layouts are the ones
the client's documentation gives, replayed from recorded buffers in
`tests/test_db2_changes_are_read_from_its_log_on_a_transcript.py`.
"""
import ctypes
import ctypes.util
import os
import struct
from decimal import Decimal

#: the position is read off the log, never written to it
CHANGE_POINT_READS_ONLY = True

# the API's caller actions and filter, as its header gives them
QUERY, READ = 1, 2
FILTER_ON = 1
#: the log manager's record types the reader acts on
NORMAL, COMMIT, ABORT = 0x004E, 0x0084, 0x0041
#: the data manager's functions of a row change
INSERT, DELETE, UPDATE = 118, 112, 120
#: a header's record kinds: the whole row, or only the changed bytes
WHOLE, PARTIAL = 0, 1


class Refused(SystemExit):
    """The log cannot be read here: what to do is the message."""


def library():
    """The client library's handle, or None where it is not installed:
    `MIGKIT_DB2_CLIENT_LIB` names the file where it is not on the path."""
    path = os.environ.get("MIGKIT_DB2_CLIENT_LIB") or \
        ctypes.util.find_library("db2")
    if not path:
        return None
    try:
        return ctypes.CDLL(path)
    except OSError:
        return None


class _ReadLogInfo(ctypes.Structure):
    _fields_ = [("initialLSN", ctypes.c_uint64),
                ("firstReadLSN", ctypes.c_uint64),
                ("nextStartLSN", ctypes.c_uint64),
                ("firstReuseLSN", ctypes.c_uint64),
                ("logRecsWritten", ctypes.c_uint32),
                ("logBytesWritten", ctypes.c_uint32)]


class _ReadLog(ctypes.Structure):
    _fields_ = [("iCallerAction", ctypes.c_uint32),
                ("piStartLSN", ctypes.POINTER(ctypes.c_uint64)),
                ("piEndLSN", ctypes.POINTER(ctypes.c_uint64)),
                ("poLogBuffer", ctypes.c_char_p),
                ("iLogBufferSize", ctypes.c_uint32),
                ("iFilterOption", ctypes.c_uint32),
                ("poReadLogInfo", ctypes.POINTER(_ReadLogInfo))]


class Api:
    """The log-read API of a loaded client library: `query` (the first and
    the next LSN the log has) and `read` (the records from an LSN, and
    where the next read starts). A recorded transcript stands in for it
    in the tests, call for call."""

    VERSION = 0x0B010000

    def __init__(self, lib):
        self.fn = getattr(lib, "db2ReadLog")

    def _call(self, action, start=0, end=0, size=0):
        info = _ReadLogInfo()
        buf = ctypes.create_string_buffer(size) if size else None
        lo, hi = ctypes.c_uint64(start), ctypes.c_uint64(end)
        parm = _ReadLog(action, ctypes.pointer(lo), ctypes.pointer(hi),
                        ctypes.cast(buf, ctypes.c_char_p) if buf else None,
                        size, FILTER_ON, ctypes.pointer(info))
        sqlca = ctypes.create_string_buffer(136)
        self.fn(ctypes.c_uint32(self.VERSION), ctypes.byref(parm), sqlca)
        code = struct.unpack_from("<i", sqlca.raw, 12)[0]
        if code < 0:
            raise SystemExit(f"the source's log could not be read (SQL code"
                             f" {code})")
        return info, (buf.raw[:info.logBytesWritten] if buf else b"")

    def query(self):
        info, _ = self._call(QUERY)
        return {"first": info.firstReuseLSN or info.initialLSN,
                "next": info.nextStartLSN}

    def read(self, start, end, size):
        info, data = self._call(READ, start, end, size)
        return data, info.nextStartLSN


def api():
    """The API, or the refusal that says what to install."""
    lib = library()
    if lib is None or not hasattr(lib, "db2ReadLog"):
        raise Refused(
            "the Db2 source's log cannot be read from this machine: install"
            " the database's data server client runtime, which carries its"
            " log-read API, or follow the source another way")
    return Api(lib)


# ---- the records ---------------------------------------------------------

def parse(data):
    """[(lsn, kind, fields)] of a read's buffer: each record its LSN (8
    bytes), its length, type and transaction (4 + 2 + 6), and for a row
    change the data manager's function, table space, table, record id,
    image kind and the image (1 + 2 + 2 + 6 + 1 + 2 + bytes; an update
    has the old image after the new). Little-endian, as the client hands
    it back on the platforms it runs on."""
    out, at = [], 0
    while at + 20 <= len(data):
        lsn, length, rtype = struct.unpack_from("<QIH", data, at)
        tid = data[at + 14:at + 20].hex()
        body = data[at + 20:at + 8 + length]
        at += 8 + length
        if rtype == COMMIT:
            out.append((lsn, "commit", {"tid": tid}))
        elif rtype == ABORT:
            out.append((lsn, "abort", {"tid": tid}))
        elif rtype == NORMAL and body:
            fn, space, table = struct.unpack_from("<BHH", body, 0)
            rid = body[5:11].hex()
            kind, n = struct.unpack_from("<BH", body, 11)
            image = body[14:14 + n]
            old = body[14 + n:] if fn == UPDATE else b""
            op = {INSERT: "insert", DELETE: "delete",
                  UPDATE: "update"}.get(fn)
            if op:
                out.append((lsn, op, {"tid": tid, "table": (space, table),
                                      "rid": rid, "whole": kind == WHOLE,
                                      "image": image, "old": old}))
    return out


class NotRead(Exception):
    """A column the image decoder does not know: read from the table."""


def _packed(raw, scale):
    digits = raw.hex()
    sign = -1 if digits[-1] in "bd" else 1
    return sign * Decimal(int(digits[:-1] or "0")).scaleb(-int(scale))


def decode(image, columns):
    """{column: value} of a row image in the table's column order:
    `columns` [(name, type, length, scale, nullable)]. A nullable column
    is a byte first (0xFF null); SMALLINT, INTEGER and BIGINT their
    little-endian bytes, DECIMAL packed, CHAR its length in UTF-8 and
    VARCHAR an offset and a length into the part after the fixed one. A
    type it does not know raises `NotRead` with the columns after it."""
    fixed = {"SMALLINT": 2, "INTEGER": 4, "BIGINT": 8}
    out, at, missing = {}, 0, []
    width = 0
    for _, t, ln, sc, nul in columns:
        width += (1 if nul else 0) + (fixed.get(t) or (
            int(ln) // 2 + 1 if t == "DECIMAL" else int(ln)
            if t == "CHARACTER" else 4 if t == "VARCHAR" else 0))
    for name, t, ln, sc, nul in columns:
        if nul:
            if image[at] == 0xFF:
                out[name] = None
                at += 1 + (fixed.get(t) or (int(ln) // 2 + 1 if t ==
                                            "DECIMAL" else int(ln) if t ==
                                            "CHARACTER" else 4))
                continue
            at += 1
        if t in fixed:
            n = fixed[t]
            out[name] = int.from_bytes(image[at:at + n], "little",
                                       signed=True)
        elif t == "DECIMAL":
            n = int(ln) // 2 + 1
            out[name] = _packed(image[at:at + n], sc)
        elif t == "CHARACTER":
            n = int(ln)
            out[name] = image[at:at + n].decode()
        elif t == "VARCHAR":
            n = 4
            off, size = struct.unpack_from("<HH", image, at)
            out[name] = image[width + off:width + off + size].decode()
        else:
            missing.append(name)
            continue
        at += n
    if missing:
        raise NotRead(missing)
    return out


# ---- the reader ------------------------------------------------------------

def tables(eng, side, db):
    """{(table space id, table id): (table, capture)} of the tables in
    scope, and their columns in order: what an image is read by."""
    rows = eng._rows(side, db, "select tabname, tbspaceid, tableid,"
                     " datacapture from syscat.tables where tabschema = ?"
                     " and type = 'T'", (eng._owner(side, db),))
    out = {}
    for name, space, tid, capture in rows:
        t = eng._said(str(name).strip())
        if eng.in_scope(db, t):
            out[(int(space), int(tid))] = (t, str(capture).strip())
    return out


def facts(eng, side, db):
    """What `decide.db2_log_way` reads, each from the server or this
    machine."""
    from ..decide import Facts
    held = tables(eng, side, db)

    def archived():
        got = eng._rows(side, db, "select value from sysibmadm.dbcfg where"
                        " name = 'logarchmeth1'")
        return bool(got) and str(got[0][0]).strip().upper() != "OFF"
    return Facts({"tables": len(held),
                  "captured": sum(1 for _, c in held.values() if c != "N")},
                 library=lambda: library() is not None, archived=archived)


def way(eng, side, db, f=None):
    """The `Choice` of how this source's changes are read; none holding
    is the refusal its reason says."""
    from .. import decide
    got = decide.db2_log_way(f if f is not None else facts(eng, side, db))
    if got.rung is None:
        raise Refused(f"{db}: the source's changes cannot be read from its"
                      f" log - {got.reason}")
    return got


def change_point(eng, side, db, reader=None):
    """Where the log is now: a read started here sees every change from
    this point on."""
    at = int((reader or api()).query()["next"])
    return {"lsn": at, "from": at}


def copy_point(eng, side, db):
    return change_point(eng, side, db)


def _columns(eng, side, db, table):
    return [(eng._said(str(n).strip()), str(t).strip().upper(), ln, sc,
             str(nul).strip() == "Y") for n, t, ln, sc, nul in eng._rows(
        side, db, "select colname, typename, length, scale, nulls from"
        " syscat.columns where tabschema = ? and tabname = ? order by colno",
        (eng._owner(side, db), table))]


def _by_rid(eng, side, db, table, cols, rid):
    """The row whose record id is `rid`, as it is now, or None."""
    names = ", ".join(eng._q(c[0]) for c in cols)
    got = eng._rows(side, db, f"select {names} from"
                    f" {eng._qualified(side, db, table)} t where rid(t) = ?",
                    (int(rid, 16),))
    return dict(zip([c[0] for c in cols], got[0])) if got else None


def neutral_changes(eng, side, db, token=None, limit=1000, reader=None,
                    size=1 << 20):
    """(changes, token): the row changes of whole transactions committed
    after `token`, read from the log until about `limit` changes or the
    end of the log; each transaction's last change marked as its end
    (`canon.ended`) at the position a read goes on from."""
    from .. import canon
    reader = reader or api()
    way(eng, side, db)
    if token is None:
        return [], change_point(eng, side, db, reader)
    held = tables(eng, side, db)
    done, start = int(token.get("lsn") or 0), int(token.get("from") or 0)
    shapes, open_, out, at = {}, {}, [], start
    end = int(reader.query()["next"])
    while at < end and len(out) < limit:
        data, nxt = reader.read(at, end, size)
        if not data and nxt <= at:
            break
        for lsn, kind, f in parse(data):
            if kind in ("insert", "delete", "update"):
                if f["table"] in held:
                    open_.setdefault(f["tid"], [lsn, []])[1].append((lsn,
                                                                     kind, f))
                else:
                    open_.setdefault(f["tid"], [lsn, []])
                continue
            begun, recs = open_.pop(f["tid"], [lsn, []])
            if kind == "abort" or lsn <= done:
                continue
            n = len(out)
            out += _changes(eng, side, db, held, shapes, recs, canon)
            canon.ended(out, n, at=lsn)
            for c in out[n:]:
                c.setdefault("txn", lsn)
            done = lsn
        at = max(nxt, at + 1)
    # the next read starts at the oldest transaction still open, so none
    # of it is lost; what it hands back again is left out by `lsn`
    restart = min([b for b, _ in open_.values()] + [at])
    return out, {"lsn": done, "from": restart}


def _changes(eng, side, db, held, shapes, recs, canon):
    out, gone = [], set()
    for _, kind, f in recs:
        if kind == "delete":
            gone.add(f["rid"])
    for _, kind, f in recs:
        table = held[f["table"]][0]
        if table not in shapes:
            shapes[table] = (_columns(eng, side, db, table),
                             eng.neutral_key(side, db, table))
        cols, key = shapes[table]
        row = None
        if f["whole"]:
            try:
                row = decode(f["image"], cols)
            except NotRead:
                row = None
        if row is None and kind == "delete":
            try:
                row = decode(f["image"] or f["old"], cols)
            except (NotRead, IndexError):
                raise SystemExit(
                    f"{db}.{table}: a delete in the source's log names a"
                    " row whose key cannot be read from it: run check on"
                    " the table") from None
        if row is None:
            row = _by_rid(eng, side, db, table, cols, f["rid"])
            if row is None:
                if f["rid"] in gone:
                    continue
                raise SystemExit(
                    f"{db}.{table}: a row the source's log changed is no"
                    " longer where the log put it, and no delete of it"
                    " follows: run check on the table, then start the tail"
                    " again")
        ident = {k: row[k] for k in key}
        if kind == "delete":
            out.append(canon.change("delete", table, ident))
        else:
            out.append(canon.change(kind, table, ident, row))
    return out
