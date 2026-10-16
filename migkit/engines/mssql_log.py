"""A SQL Server source followed through its own log, where neither Change
Tracking nor Change Data Capture is on (backlog LT.R2, LT.R3).

Two ways, ranked by `decide.mssql_log_way` on what the server says:
* the live log: the server's own log function read from the position on,
  where the account holds the administrator's role;
* the log backups: each log backup since the position restored, in
  order and the chain checked unbroken, onto a server migkit owns, in
  standby, and its records read there by the same function over the
  backup file - the source does nothing it does not do already.

Either way the records are the same rows: a transaction's begin and its
commit, and between them the inserts, changes and deletes of the rows of
the tables in scope, each named by its allocation unit (read off
`sys.allocation_units`, the heap or clustered index of a table only). An
insert or a delete carries the whole row, read from its image (`decode`,
the row format the server documents for its data pages); a change carries
only the bytes it changed, so the row is read by its place on its page as
it is now - on the restored standby, as it was at that backup's end. A row
so named and not found, with no delete of it after, is not guessed at:
the read stops, named. A value whose type the image reader does not know
is read by the row's key.

The position is {"lsn": the commit last handed on, "from": the begin of
the oldest transaction still open}, in the log's own notation: a read
starts at "from" and leaves out every transaction committed at or before
"lsn", so a tail stopped mid-batch and started again hands nothing twice
and loses nothing.

The engine reaches it through one hook in `mssql.py`, made where that
module's owner wires its follow ladder; until then `follow_way` and
`neutral_changes` here are the whole of it.

Written 2026-10-16: on the sandbox's SQL Server build (the arm64 one) a
table's inserts, change and delete were read back from its live log by
this reader, and a log backup was restored in standby; reading a restored
backup's records and the chain on a server migkit owns were replayed from
recorded answers, `tests/test_sql_server_changes_are_read_
from_its_log_and_its_backups.py`.
"""
import struct
from decimal import Decimal

CHANGE_POINT_READS_ONLY = True

ROWS = ("LOP_INSERT_ROWS", "LOP_DELETE_ROWS", "LOP_MODIFY_ROW",
        "LOP_MODIFY_COLUMNS")
ENDS = ("LOP_BEGIN_XACT", "LOP_COMMIT_XACT", "LOP_ABORT_XACT")
#: the rows of a table's data, not of its other indexes: a delete from a
#: clustered index marks its row a ghost, as the sandbox's build logged it
CONTEXTS = ("LCX_HEAP", "LCX_CLUSTERED", "LCX_MARK_AS_GHOST")
FIELDS = ("[Current LSN], Operation, Context, [Transaction ID], AllocUnitId,"
          " [Page ID], [Slot ID], [RowLog Contents 0]")
LIVE = (f"select {FIELDS} from fn_dblog(%s, NULL) where Operation in"
        f" ({', '.join(repr(o) for o in ROWS + ENDS)})")
UNITS = ("select au.allocation_unit_id, s.name, o.name from"
         " sys.allocation_units au join sys.partitions p on"
         " au.container_id = p.partition_id join sys.objects o on"
         " o.object_id = p.object_id join sys.schemas s on s.schema_id ="
         " o.schema_id where au.type in (1, 3) and p.index_id in (0, 1) and"
         " o.type = 'U'")
COLUMNS = ("select c.name, t.name, c.max_length, c.precision, c.scale from"
           " sys.columns c join sys.types t on t.user_type_id ="
           " c.user_type_id where c.object_id = object_id(%s) order by"
           " c.column_id")


# ---- the log's own notation ----------------------------------------------

def lsn(text):
    """An LSN as an integer that orders as the log does: the virtual log
    file, the block and the slot of 'vvvvvvvv:bbbbbbbb:ssss'."""
    v, b, s = (int(p, 16) for p in str(text).replace("0x", "").split(":"))
    return (v << 48) | (b << 16) | s


def lsn_text(n):
    return f"0x{n >> 48:08X}:{(n >> 16) & 0xFFFFFFFF:08X}:{n & 0xFFFF:04X}"


def lsn_decimal(n):
    """The backup history's decimal notation of the same LSN."""
    return (n >> 48) * 10 ** 15 + ((n >> 16) & 0xFFFFFFFF) * 10 ** 5 + (
        n & 0xFFFF)


def from_decimal(d):
    d = int(d)
    return ((d // 10 ** 15) << 48) | (((d // 10 ** 5) % 10 ** 10) << 16) | (
        d % 10 ** 5)


def physloc(page, slot):
    """The row's place, as `%%physloc%%` holds it: page, file, slot."""
    f, p = (int(x, 16) for x in str(page).split(":"))
    return struct.pack("<IHH", p, f, int(slot))


# ---- the row image ---------------------------------------------------------

class NotRead(Exception):
    """Columns the image reader does not know: read by the row's key."""


FIXED = {"tinyint": 1, "smallint": 2, "int": 4, "bigint": 8, "real": 4,
         "float": 8, "money": 8, "datetime": 8, "date": 3,
         "uniqueidentifier": 16}
VAR = ("varchar", "nvarchar", "varbinary")


def _decimal_width(precision):
    return 5 if precision <= 9 else 9 if precision <= 19 else \
        13 if precision <= 28 else 17


def decode(rec, columns):
    """{column: value} of a data row's image: its status byte, the end of
    its fixed part (2 bytes at 2), the fixed columns in order, the column
    count and null bitmap, then the variable columns' end offsets and
    their bytes. `columns` [(name, type, max length, precision, scale)]."""
    rec = bytes(rec)
    has_var = rec[0] & 0x20
    end = struct.unpack_from("<H", rec, 2)[0]
    count = struct.unpack_from("<H", rec, end)[0]
    nulls = rec[end + 2:end + 2 + (count + 7) // 8]
    out, missing, at = {}, [], 4
    for i, (name, t, size, prec, scale) in enumerate(columns):
        if t in VAR:
            continue
        width = FIXED.get(t) or (int(size) if t in ("char", "nchar",
                                                    "binary") else
                                 _decimal_width(int(prec)) if t in (
                                     "decimal", "numeric") else None)
        if width is None:
            missing += [c[0] for c in columns[i:] if c[1] not in VAR]
            break
        raw = rec[at:at + width]
        at += width
        if nulls[i // 8] & (1 << (i % 8)):
            out[name] = None
        elif t in ("tinyint", "smallint", "int", "bigint"):
            out[name] = int.from_bytes(raw, "little", signed=t != "tinyint")
        elif t in ("decimal", "numeric"):
            sign = 1 if raw[0] else -1
            out[name] = sign * Decimal(int.from_bytes(raw[1:], "little")
                                       ).scaleb(-int(scale))
        elif t == "char":
            out[name] = raw.decode("cp1252")
        elif t == "nchar":
            out[name] = raw.decode("utf-16-le")
        elif t == "binary":
            out[name] = raw
        else:
            missing.append(name)
    var = [c for c in columns if c[1] in VAR]
    if var:
        k = end + 2 + (count + 7) // 8
        n = struct.unpack_from("<H", rec, k)[0] if has_var else 0
        ends = [struct.unpack_from("<H", rec, k + 2 + 2 * j)[0]
                for j in range(n)]
        start = k + 2 + 2 * n
        for j, (name, t, *_r) in enumerate(var):
            i = columns.index(var[j])
            if nulls[i // 8] & (1 << (i % 8)):
                out[name] = None
                if j < n:
                    start = ends[j] & 0x7FFF
                continue
            if j >= n:
                # the trailing variable columns a row leaves out are empty
                out[name] = b"" if t == "varbinary" else ""
                continue
            if ends[j] & 0x8000:
                missing.append(name)
                start = ends[j] & 0x7FFF
                continue
            raw = rec[start:ends[j]]
            start = ends[j]
            out[name] = raw if t == "varbinary" else raw.decode(
                "utf-16-le" if t == "nvarchar" else "cp1252")
    if missing:
        raise NotRead(missing)
    return out


# ---- the facts the way is chosen on ---------------------------------------

def _standby_named(eng, side):
    return eng._endpoint(side).options.get("log_standby")


def facts(eng, side, db, since=None):
    from ..decide import Facts

    def live():
        try:
            eng._rows(side, db, "select top 1 [Current LSN] from"
                      " fn_dblog(NULL, NULL)")
            return True
        except Exception:  # noqa: BLE001 - not readable: refused
            return False

    def recovery():
        got = eng._rows(side, db, "select recovery_model_desc from"
                        " sys.databases where name = db_name()")
        return str(got[0][0]) if got else None
    return Facts({"standby": bool(_standby_named(eng, side))},
                 live=live, recovery=recovery,
                 chain=lambda: since is None or bool(
                     contiguous(chain(eng, side, db, since), since)))


def follow_way(eng, side, db, since=None, lags=None):
    """The `Choice` of how the log is followed; none holding is a stop
    whose reason says what each way lacked."""
    from .. import decide
    got = decide.mssql_log_way(facts(eng, side, db, since), costs=lags)
    if got.rung is None:
        raise SystemExit(f"{db}: the source's log can be followed no way"
                         f" here - {got.reason}")
    return got


# ---- the log backups -------------------------------------------------------

def chain(eng, side, db, since):
    """[(first, last, file)] of the log backups holding anything past
    `since`, oldest first, from the server's backup history."""
    rows = eng._rows(side, db, "select b.first_lsn, b.last_lsn,"
                     " m.physical_device_name from msdb.dbo.backupset b join"
                     " msdb.dbo.backupmediafamily m on m.media_set_id ="
                     " b.media_set_id where b.database_name = db_name() and"
                     " b.type = 'L' and b.last_lsn > %s order by b.first_lsn",
                     (lsn_decimal(since),))
    return [(from_decimal(a), from_decimal(b), str(f)) for a, b, f in rows]


def contiguous(backups, since):
    """The backups, where each begins where the one before ended and the
    first holds `since`; [] where one is missing."""
    if not backups or backups[0][0] > since:
        return []
    for a, b in zip(backups, backups[1:]):
        if b[0] != a[1]:
            return []
    return backups


def dump(path):
    """The log function over a backup file: every argument after the
    file is its default, as the function asks."""
    return (f"select {FIELDS} from fn_dump_dblog(NULL, NULL, N'DISK', 1,"
            f" N'{path.replace(chr(39), chr(39) * 2)}'"
            + ", DEFAULT" * 63 + f") where Operation in"
            f" ({', '.join(repr(o) for o in ROWS + ENDS)})")


def restore(standby, name, path, undo):
    """The backup restored onto the standby, left readable: a server
    that refuses a restore in standby says so, and the way stops."""
    q = name.replace("]", "]]")
    try:
        standby._rows(f"restore log [{q}] from disk = %s with standby = %s",
                      (path, undo))
    except Exception as e:  # noqa: BLE001 - said
        raise SystemExit(f"the server migkit owns would not restore the log"
                         f" backup in standby: {str(e).splitlines()[0][:160]}"
                         ) from None


def restored_to(standby, name):
    """The LSN the standby's database is restored through (the next one a
    log backup must hold), or None where it holds none yet."""
    got = standby._rows("select redo_start_lsn from sys.master_files where"
                        " database_id = db_id(%s) and file_id = 1", (name,))
    return from_decimal(got[0][0]) if got and got[0][0] is not None else None


# ---- the reader ------------------------------------------------------------

def _units(eng, side, db):
    """{allocation unit: table} of the heaps and clustered indexes."""
    return {int(u): f"{s}.{t}" for u, s, t in
            eng._rows(side, db, UNITS)}


def change_point(eng, side, db):
    got = eng._rows(side, db, "select max([Current LSN]) from"
                    " fn_dblog(NULL, NULL)")
    at = lsn(got[0][0])
    return {"lsn": lsn_text(at), "from": lsn_text(at)}


def copy_point(eng, side, db):
    return change_point(eng, side, db)


class _Live:
    """Rows of the source's live log, and rows read off the source."""

    def __init__(self, eng, side, db):
        self.eng, self.side, self.db = eng, side, db

    def records(self, start):
        return self.eng._rows(self.side, self.db, LIVE, (lsn_text(start),))

    def rows(self, sql, args):
        return self.eng._rows(self.side, self.db, sql, args)


def neutral_changes(eng, side, db, token=None, limit=1000, standby=None):
    """(changes, token): the row changes of whole transactions committed
    after `token`, from the live log or, where the way is the log
    backups, from each backup restored onto `standby` (an object whose
    `_rows(sql, args)` asks the server migkit owns, its database in
    standby), in order."""
    from .. import canon
    if token is None:
        return [], change_point(eng, side, db)
    done, start = lsn(token["lsn"]), lsn(token["from"])
    path = follow_way(eng, side, db, since=start).path
    units = {u: t for u, t in _units(eng, side, db).items()
             if eng.in_scope(db, t)}
    if path == "live log":
        src = _Live(eng, side, db)
        batches = [(src.records(start), src.rows)]
    else:
        if standby is None:
            raise SystemExit(f"{db}: the log backups are restored onto the"
                             " server migkit owns, which is not reachable")
        batches, have = [], restored_to(standby, db)
        for _, last, f in contiguous(chain(eng, side, db, start), start):
            # a backup the standby holds already (a run stopped after its
            # restore) is read again, not restored again
            if have is None or last > have:
                restore(standby, db, f, f + ".undo")
            batches.append((standby._rows(dump(f), ()),
                            lambda sql, args: standby._rows(sql, args)))
    out, open_, shapes = [], {}, {}
    for records, read in batches:
        for at_text, op, ctx, tid, unit, page, slot, image in records:
            at = lsn(at_text)
            if at < start:
                continue
            if op == "LOP_BEGIN_XACT":
                open_.setdefault(tid, [at, []])
                continue
            if op in ROWS:
                if ctx in CONTEXTS and int(unit or 0) in units:
                    open_.setdefault(tid, [at, []])[1].append(
                        (op, units[int(unit)], page, slot, image))
                continue
            begun, recs = open_.pop(tid, [at, []])
            if op == "LOP_ABORT_XACT" or at <= done:
                continue
            n = len(out)
            out += _changes(eng, side, db, recs, shapes, read, canon)
            canon.ended(out, n, at=lsn_text(at))
            done = at
            if len(out) >= limit and not open_:
                break
    # the commit last handed on is a record the log function starts at;
    # it is itself left out by `lsn`
    restart = min([b for b, _ in open_.values()] + [max(done, start)])
    return out, {"lsn": lsn_text(done), "from": lsn_text(restart)}


def _changes(eng, side, db, recs, shapes, read, canon):
    out = []
    gone = {(p, s) for op, _, p, s, _ in recs if op == "LOP_DELETE_ROWS"}
    for op, table, page, slot, image in recs:
        if table not in shapes:
            cols = [(str(n), str(t).lower(), sz, pr, sc) for n, t, sz, pr, sc
                    in eng._rows(side, db, COLUMNS, (table,))]
            shapes[table] = (cols, eng.neutral_key(side, db, table))
        cols, key = shapes[table]
        names = ", ".join(eng._q(c[0]) for c in cols)
        quoted = eng._qualified(side, db, table)
        row = None
        if op in ("LOP_INSERT_ROWS", "LOP_DELETE_ROWS"):
            try:
                row = decode(image, cols)
            except NotRead as e:
                row = {"_missing": e.args[0]}
        if op == "LOP_DELETE_ROWS":
            if any(k not in row for k in key):
                raise SystemExit(f"{db}.{table}: a delete in the source's"
                                 " log names a row whose key cannot be read"
                                 " from it: run check on the table")
            out.append(canon.change("delete", table,
                                    {k: row[k] for k in key}))
            continue
        if row is None or "_missing" in row:
            got = read(f"select {names} from {quoted} where %%physloc%% ="
                       " %s", (physloc(page, slot),)) if row is None else \
                read(f"select {names} from {quoted} where "
                     + " and ".join(f"{eng._q(k)} = %s" for k in key),
                     tuple(row[k] for k in key))
            if not got:
                if (page, slot) in gone or op == "LOP_INSERT_ROWS":
                    # deleted later in the same transaction: its delete
                    # follows
                    continue
                raise SystemExit(f"{db}.{table}: a row the source's log"
                                 " changed is no longer where the log put"
                                 " it, and no delete of it follows: run"
                                 " check on the table, then start the tail"
                                 " again")
            row = dict(zip([c[0] for c in cols], got[0]))
        out.append(canon.change("insert" if op == "LOP_INSERT_ROWS"
                                else "update", table,
                                {k: row[k] for k in key}, row))
    return out
