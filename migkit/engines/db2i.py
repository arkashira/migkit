"""Db2 for i (IBM i) as one side of a pair, and a source whose changes are
followed through its journal (backlog 34, W5).

Reached without a Db2 Connect licence. In the order they are taken:
* IBM's own ODBC driver for IBM i (native on macOS arm64 and on Linux x86;
  `migkit doctor --install` puts it in place once IBM's terms are
  accepted), through `pyodbc`;
* DRDA through `ibm_db`, where the operator's Db2 Connect licence is in
  place beside the IBM data server driver and the endpoint names the
  system's relational database (`database`);
* Mapepire, IBM's open-source database server for IBM i clients, where
  the operator runs it on the system (its port, 8076 by default, as the
  endpoint's `port`), through `mapepire-python` (Apache-2.0): its values
  cross as JSON, so a decimal, a date, a time and bytes are read as their
  text and turned back here, exactly.

A hop's databases are its schemas (libraries). Tables, columns and keys
come from `QSYS2`'s catalogue; a table with no key is read by relative
record number (`RRN`), a range at a time. Rows are compared through the
renderer every engine without a canonical SQL rendering shares, and two
IBM i systems compare first by `HASH_ROW` - a SHA-512 of each row, summed
on each server - reading row by row only a table whose sums differ.

Changes are followed through the table's journal with
`QSYS2.DISPLAY_JOURNAL`, which reads without consuming anything: row
entries (PT and PX put, UB and UP the before and after images, DL delete)
and the commitment-control entries (SC, CM, RB) of the tables' journals,
by sequence number - the position a tail keeps and a cutover is fenced by.
A transaction is carried whole once it commits, in commit order; one
rolled back carries nothing; one rolled back to a savepoint is carried as
its rows now are. Each entry's record image is cut by the columns' storage
in the record format (`QSYS2.SYSCOLUMNS.STORAGE`, in their order): packed
and zoned decimals through `migkit.copybook`, text by each column's CCSID.
A table whose image holds what cannot be cut there (a LOB, XML, DECFLOAT,
BOOLEAN, a ROWID) is carried by its key, the row read again as it is now.
A table not journaled, journaled with after images only, or journaled
with minimized entries, and a table with no primary key, is refused by
name with the command that fixes it. The starting receiver must still be
on the system: a position whose receivers are gone is said, never
stepped over.

Written 2026-09-30 without an IBM i to run it against - no IBM i runs in a
container - so what is held here is every statement's shape and what is
made of the rows each returns (`tests/test_db2_for_i_as_a_side_of_a_pair
.py`). PUB400.com, a free public IBM i with journals and
`DISPLAY_JOURNAL`, is where the operator can prove it on a real system.
"""
import codecs
import datetime
import struct
from decimal import Decimal

from .base import Result
from .db2 import Db2Engine, _twelve, db2_connect_licence, large_values
from .dbapi import MARK

#: the name IBM's ODBC driver for IBM i registers itself by
ODBC_DRIVER = "IBM i Access ODBC Driver"
#: where Mapepire listens unless its operator moved it
MAPEPIRE_PORT = 8076

#: the declared type from `QSYS2.SYSCOLUMNS`, with the numbers a
#: counterpart needs; a character column of CCSID 65535 holds bytes
DECLARED = ("case when data_type in ('DECIMAL', 'NUMERIC') then data_type"
            " || '(' || rtrim(char(length)) || ',' ||"
            " rtrim(char(coalesce(numeric_scale, 0))) || ')'"
            " when data_type in ('CHAR', 'VARCHAR') and ccsid = 65535 then"
            " 'VARBINARY(' || rtrim(char(length)) || ')'"
            " when data_type in ('CHAR', 'VARCHAR', 'GRAPHIC', 'VARG',"
            " 'BINARY', 'VARBIN') then data_type || '(' ||"
            " rtrim(char(length)) || ')'"
            " when data_type = 'TIMESTMP' then 'TIMESTAMP(' ||"
            " rtrim(char(coalesce(datetime_precision, 6))) || ')'"
            " when data_type = 'FLOAT' and length = 4 then 'REAL'"
            " when data_type = 'FLOAT' then 'DOUBLE'"
            " else data_type end")

#: the journal entries a follow reads: a row put (PT; PX at a given
#: relative record number), its before and after images (UB, UP), a
#: delete (DL), their undoing inside a rolled-back transaction (BR, UR,
#: DR), and a transaction's start, commit and rollback (SC, CM, RB)
ROW_TYPES = ("PT", "PX", "UB", "UP", "DL", "BR", "UR", "DR")
COMMIT_TYPES = ("SC", "CM", "RB")
#: receivers whose entries can still be read
READABLE = ("ATTACHED", "ONLINE", "SAVED")

JOURNAL_SQL = (
    "select sequence_number, journal_code, journal_entry_type,"
    " count_or_rrn, commit_cycle, {data}, null_value_indicators, object,"
    " minimized_entry_data from table(qsys2.display_journal("
    "cast(? as varchar(10)), cast(? as varchar(10)),"
    " starting_receiver_name => '*CURCHAIN',"
    " starting_sequence => cast(? as decimal(21, 0)),"
    " ending_sequence => cast(? as decimal(21, 0)),"
    " journal_codes => 'R,C', journal_entry_types => '"
    + ",".join(ROW_TYPES + COMMIT_TYPES) + "')) as j"
    " order by sequence_number fetch first {rows} rows only")


def odbc_driver():
    """The IBM i ODBC driver's name as the ODBC manager knows it, or None
    where it is not installed - or where no ODBC manager is, which the
    driver module needs to load at all."""
    try:
        import pyodbc
        names = pyodbc.drivers()
    except Exception:  # noqa: BLE001 - no manager: no driver
        return None
    return next((n for n in names if n.startswith(ODBC_DRIVER)), None)


def _odbc_value(v):
    v = str(v)
    return "{" + v.replace("}", "}}") + "}" if any(c in v for c in ";{}") \
        else v


def ccsid_codec(ccsid):
    """The Python codec for a column's CCSID, or None where there is none
    here (bytes, CCSID 65535, are None too)."""
    from .. import copybook
    copybook._codepages()
    c = int(ccsid or 0)
    name = {1208: "utf-8", 1200: "utf-16-be", 13488: "utf-16-be",
            367: "ascii", 819: "latin-1"}.get(c, f"cp{c:03d}")
    if c in (0, 65535):
        return None
    try:
        codecs.lookup(name)
    except LookupError:
        return None
    return name


class Reread(Exception):
    """A record image that cannot be cut here: the row is read again."""


class Image:
    """How a table's rows lie in its journal entries: each column's kind,
    offset and storage, in the order of the record format."""

    def __init__(self, columns):
        from .. import copybook
        self.cols, at, self.whole = [], 0, True
        for name, typ, length, scale, ccsid, storage, precision in columns:
            typ = str(typ).strip().upper()
            size = int(storage or 0)
            kind, extra = "reread", None
            if typ in ("SMALLINT", "INTEGER", "BIGINT"):
                kind, extra = "int", int(scale or 0)
            elif typ in ("DECIMAL", "NUMERIC"):
                kind = "packed" if typ == "DECIMAL" else "zoned"
                extra = copybook.field_codec(kind, int(length),
                                             int(scale or 0), name=name)[0]
            elif typ == "FLOAT":
                kind = "float"
            elif typ in ("CHAR", "VARCHAR", "GRAPHIC", "VARG"):
                codec = ccsid_codec(ccsid)
                if int(ccsid or 0) == 65535:
                    kind = "bytes" if typ == "CHAR" else "varbytes"
                elif codec:
                    kind = {"CHAR": "char", "VARCHAR": "varchar",
                            "GRAPHIC": "char", "VARG": "vargraphic"}[typ]
                    extra = codec
            elif typ == "BINARY":
                kind = "bytes"
            elif typ == "VARBIN":
                kind = "varbytes"
            elif typ in ("DATE", "TIME", "TIMESTMP"):
                kind, extra = typ.lower(), int(precision or 6)
            if kind == "reread":
                self.whole = False
            self.cols.append((name, kind, None if at is None else at, size,
                              extra))
            # a LOB's place in the image is a locator whose size the
            # catalogue does not give: nothing after one can be placed
            at = None if at is None or typ in ("CLOB", "BLOB", "DBCLOB",
                                               "XML") else at + size

    def cut(self, data, nulls, only=None):
        """{column: value} of one record image, `only` the columns named
        there; `Reread` where a column asked for cannot be cut."""
        out = {}
        if isinstance(nulls, (bytes, bytearray)):
            # x'F0'/x'F1' as the journal writes them, where the driver
            # hands the column over untranslated
            nulls = bytes(nulls).decode("cp037")
        nulls = str(nulls or "")
        for i, (name, kind, at, size, extra) in enumerate(self.cols):
            if only is not None and name not in only:
                continue
            if i < len(nulls) and nulls[i] == "1":
                out[name] = None
                continue
            if kind == "reread" or at is None:
                raise Reread(name)
            raw = bytes(data[at:at + size])
            if len(raw) < size:
                raise Reread(name)
            out[name] = _value(kind, raw, extra, name)
        return out


def _value(kind, raw, extra, name):
    from .. import copybook
    if kind == "int":
        n = int.from_bytes(raw, "big", signed=True)
        return Decimal(n).scaleb(-extra) if extra else n
    if kind in ("packed", "zoned"):
        try:
            v = extra(raw)
        except copybook.Unreadable as e:
            raise SystemExit(f"a journal entry's {name} is not a number:"
                             f" {e.why}")
        return v
    if kind == "float":
        return struct.unpack(">f" if len(raw) == 4 else ">d", raw)[0]
    if kind == "bytes":
        return raw
    if kind == "varbytes":
        n = int.from_bytes(raw[:2], "big")
        return raw[2:2 + n]
    if kind == "char":
        return raw.decode(extra).rstrip(" ")
    if kind == "varchar":
        n = int.from_bytes(raw[:2], "big")
        return raw[2:2 + n].decode(extra)
    if kind == "vargraphic":
        n = int.from_bytes(raw[:2], "big")
        return raw[2:2 + 2 * n].decode(extra)
    text = raw.decode("cp037").strip()
    if kind == "date":
        # *ISO and *JIS are YYYY-MM-DD, *EUR DD.MM.YYYY, *USA MM/DD/YYYY
        if "." in text:
            d, m, y = text.split(".")
            return datetime.date(int(y), int(m), int(d))
        if "/" in text:
            m, d, y = text.split("/")
            return datetime.date(int(y), int(m), int(d))
        return datetime.date.fromisoformat(text)
    if kind == "time":
        return datetime.time.fromisoformat(text.replace(".", ":"))
    # YYYY-MM-DD-HH.MM.SS[.fraction]
    head, frac = text[:19], text[20:]
    stamp = head[:10] + " " + head[11:].replace(".", ":")
    return _twelve(f"{stamp}.{frac}" if frac else stamp)


class Db2iEngine(Db2Engine):
    CANON_ENGINE = "db2i"
    PLATFORM = "i"
    #: DRDA on IBM i
    DRDA_PORT = 446
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " one of IBM i's")
    PADDED = ("CHAR", "GRAPHIC")
    SCALED = ("DECIMAL", "NUMERIC")
    #: large values as Db2's, a large table sized over every row: a
    #: sample of its pages is asked of Linux, UNIX and Windows only
    LARGE_VALUES = large_values()
    #: the journal is read by sequence, never consumed
    CHANGE_POINT_READS_ONLY = True

    # ---- reaching it ------------------------------------------------------

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def rung(self, side):
        """How this side is reached: ("odbc", driver) where IBM's driver is
        installed, ("drda", None) where a Db2 Connect licence is in place
        and the endpoint names the relational database, else ("mapepire",
        None)."""
        known = self.__dict__.setdefault("_rungs", {})
        if side not in known:
            driver = odbc_driver()
            if driver:
                known[side] = ("odbc", driver)
            elif self._ep(side).options.get("database") and \
                    db2_connect_licence():
                known[side] = ("drda", None)
            else:
                known[side] = ("mapepire", None)
        return known[side]

    def _connect(self, side, db):
        how, driver = self.rung(side)
        ep = self._ep(side)
        if how == "odbc":
            import pyodbc
            parts = {"SYSTEM": ep.host, "UID": ep.user, "PWD": ep.password,
                     # SQL naming, schema.table; read committed; text as
                     # UTF-8; CCSID 65535 kept as the bytes it is
                     "NAM": "0", "CMT": "1", "CCSID": "1208",
                     "TRANSLATE": "0"}
            if ep.options.get("tls"):
                parts["SSL"] = "1"
            conn = pyodbc.connect(
                f"DRIVER={{{driver}}};" + ";".join(
                    f"{k}={_odbc_value(v)}" for k, v in parts.items()),
                autocommit=False, timeout=15)
            return self._confirmed(conn, side,
                                   conn.getinfo(pyodbc.SQL_DBMS_NAME))
        if how == "drda":
            return super()._connect(side, db)
        import mapepire_python
        ca = None
        if ep.options.get("tls_ca_file"):
            with open(ep.options["tls_ca_file"]) as f:
                ca = f.read()
        try:
            return mapepire_python.connect({
                "host": ep.host, "user": ep.user, "password": ep.password,
                "port": int(ep.port or MAPEPIRE_PORT),
                "ignoreUnauthorized": bool(ep.options.get("tls_insecure")),
                "ca": ca})
        except Exception as e:  # noqa: BLE001 - said, with what to do
            raise SystemExit(
                f"the {'source' if side == 'src' else 'target'} IBM i is"
                " reached through IBM's ODBC driver, which is not installed"
                " on this machine (migkit doctor --install puts it in place"
                " once IBM's terms are accepted), or through the Mapepire"
                " server on the system, which did not answer at"
                f" {ep.host}:{ep.port or MAPEPIRE_PORT}"
                f" ({str(e).splitlines()[0][:120] if str(e) else type(e).__name__})")

    def _wide_times(self, side, db, table):
        # which side a column's reading is for: over Mapepire a value's
        # reading depends on it (`_read_as`)
        self.__dict__["_reading"] = side
        return super()._wide_times(side, db, table)

    def _read_as(self, quoted, declared):
        """Over Mapepire, whose rows cross as JSON: a decimal as its text (a
        JSON number is a double, and 17 digits of a DECIMAL(31) would be
        lost), a date, a time and a timestamp as their ISO text, bytes as
        hexadecimal."""
        got = super()._read_as(quoted, declared)
        side = self.__dict__.get("_reading")
        if got or side is None or self.rung(side)[0] != "mapepire":
            return got
        from .. import canon, nanotime
        name, nums = canon._split_declared(declared)
        base = str(declared).split("(")[0].strip().upper()
        if base in ("DECIMAL", "NUMERIC", "DECFLOAT"):
            return (f"varchar({quoted})", lambda t: Decimal(str(t).strip()))
        if base == "DATE":
            return (f"char({quoted}, iso)",
                    lambda t: datetime.date.fromisoformat(str(t).strip()))
        if base == "TIME":
            return (f"char({quoted}, jis)",
                    lambda t: datetime.time.fromisoformat(str(t).strip()))
        if base == "TIMESTAMP":
            digits = nums[0] if nums else 6
            return (f"varchar_format({quoted}, 'YYYY-MM-DD HH24:MI:SS.FF"
                    f"{max(digits, 1)}')", nanotime.parse)
        if base in ("BINARY", "VARBIN", "VARBINARY", "BLOB"):
            return (f"hex({quoted})", lambda t: bytes.fromhex(str(t)))
        return None

    # ---- the catalogue -------------------------------------------------------

    def neutral_tables(self, side, db):
        return [t for t in (self._said(n.strip()) for (n,) in self._rows(
            side, db, "select table_name from qsys2.systables where"
                      f" table_schema = {MARK} and table_type in ('T', 'P')"
                      " and file_type = 'D' order by 1",
            (self._owner(side, db),)))
            if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        return [(self._said(n.strip()), t.strip()) for n, t in self._rows(
            side, db, f"select column_name, {DECLARED} from"
                      f" qsys2.syscolumns where table_schema = {MARK} and"
                      f" table_name = {MARK} order by ordinal_position",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def neutral_key(self, side, db, table):
        return [self._said(n.strip()) for (n,) in self._rows(
            side, db, "select k.column_name from qsys2.syscst c join"
                      " qsys2.syskeycst k on k.constraint_schema ="
                      " c.constraint_schema and k.constraint_name ="
                      f" c.constraint_name where c.table_schema = {MARK}"
                      f" and c.table_name = {MARK} and c.constraint_type ="
                      " 'PRIMARY KEY' order by k.ordinal_position",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def table_facts(self, side, db):
        out = {}
        for name, rows, size in self._rows(
                side, db, "select table_name, number_rows, data_size from"
                          f" qsys2.systablestat where table_schema = {MARK}",
                (self._owner(side, db),)):
            out[self._said(str(name).strip())] = {
                "rows": None if rows is None else int(rows),
                "bytes": None if size is None else int(size), "key": None}
        return out

    def position_spans(self, side, db, table, rows_per):
        """Ranges of relative record numbers, about `rows_per` each: what a
        table with no key is read and resumed by. The last is open-ended,
        so rows added past the plan are read too."""
        quoted = self._qualified(side, db, table)
        got = self._rows(side, db, f"select min(rrn(t)), max(rrn(t)) from"
                                   f" {quoted} t")
        lo, hi = (got[0] if got else (None, None))
        if lo is None:
            return [[0, None]]
        step = max(1, int(rows_per))
        out, start = [], int(lo)
        while start <= int(hi):
            end = start + step
            out.append([start, f"rrn({quoted}) >= {start} and rrn({quoted})"
                               f" < {end}" if end <= int(hi) else
                        f"rrn({quoted}) >= {start}"])
            start = end
        return out

    def server_identity(self, side, db):
        """The system by its relational database name and host, the
        operating system's release, and the bytes the schema's tables
        hold."""
        rdb, host, version = self._rows(
            side, db, "select current server, host_name, os_version ||"
                      " '.' || os_release from sysibmadm.env_sys_info")[0]
        out = {"id": str(rdb).strip(), "name": str(host or "").strip(),
               "version": str(version or "").strip()}
        try:
            got = self._rows(side, db, "select sum(data_size) from"
                                       " qsys2.systablestat where"
                                       f" table_schema = {MARK}",
                             (self._owner(side, db),))
            if got and got[0][0] is not None:
                out["bytes"] = int(got[0][0])
        except Exception:  # noqa: BLE001 - the size is not needed
            pass
        return out

    # ---- comparing two IBM i systems on the servers ----------------------

    def _hash_rows(self, side, db, table):
        """(rows, the sum of each row's `HASH_ROW` - SHA-512 of the row -
        taken to its first 7 bytes): one number from the server."""
        n, total = self._rows(
            side, db, "select count(*), coalesce(sum(cast(interpret("
                      "bx'00' concat substr(hash_row(t), 1, 7) as bigint)"
                      " as decimal(31, 0))), 0) from"
                      f" {self._qualified(side, db, table)} t")[0]
        return int(n), int(total)

    def check_data(self, db, table=None, stream=None):
        """Each table's rows hashed on both systems (`HASH_ROW`, IBM i 7.4
        TR2 and later); only a table whose sums differ - or whose rows
        the hop filters or maps, or that a system cannot hash - is
        compared row by row through the pair. The hash is of the row as
        IBM i stores it, CCSIDs and all, so two tables holding the same
        values in different CCSIDs are read row by row, never passed."""
        tables = [table] if table else self.neutral_tables("src", db)
        pair = self._as_pair()
        out, rest = [], []
        for t in tables:
            where = pair._row_scope(db, t)
            if where[0] or where[1] or self.hop.column_rules(db, t) or \
                    self.hop.masks(db, t) or \
                    self.hop.target_table(db, t) != f"{db}.{t}":
                rest.append(t)
                continue
            try:
                s = self._hash_rows("src", db, t)
                d = self._hash_rows("dst", db, t)
            except Exception:  # noqa: BLE001 - the rows will say
                rest.append(t)
                continue
            if s != d:
                rest.append(t)
                continue
            out.append(Result("data", f"{db}.{t}", "ok",
                              f"{s[0]:,} rows, every row's hash equal on both"
                              " systems"))
            if stream:
                stream(f"{t}: ok")
        for t in rest:
            out += pair.check_data(db, t, stream)
        return out

    def _pending(self, db):
        """Constraints on the target left in check pending by a load: IBM i
        does not enforce them until checked."""
        try:
            held = self._rows("dst", db, "select table_name,"
                                         " constraint_name from qsys2.syscst"
                                         f" where table_schema = {MARK} and"
                                         " check_pending = 'YES'",
                              (self._owner("dst", db),))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target tables", "error",
                           "could not read the target's constraints:"
                           f" {str(e).splitlines()[0][:90]}")]
        held = self._held_in_scope(db, held)
        if held:
            return [Result("deep", f"{db} target tables", "diff",
                           f"{len(held)} constraints in check pending: "
                           + ", ".join(f"{r[0].strip()}.{r[1].strip()}"
                                       for r in held[:6]), "",
                           "check them with CHGPFCST ... STATE(*ENABLED)"
                           " before cutover")]
        return [Result("deep", f"{db} target tables", "ok",
                       "no constraint in check pending")]

    # ---- the journal ---------------------------------------------------------

    def _journaled(self, side, db):
        """{table: {"system": (file, library) as the journal names them,
        "journal": "LIB/JRN" or None, "images": *BOTH or *AFTER}}."""
        out = {}
        for name, sysname, syslib, jlib, jname, images in self._rows(
                side, db, "select t.table_name, t.system_table_name,"
                          " t.system_table_schema, j.journal_library,"
                          " j.journal_name, j.journal_images from"
                          " qsys2.systables t left join"
                          " qsys2.journaled_objects j on j.object_library ="
                          " t.system_table_schema and j.object_name ="
                          " t.system_table_name and j.object_type = '*FILE'"
                          f" where t.table_schema = {MARK} and t.table_type"
                          " in ('T', 'P') and t.file_type = 'D'",
                (self._owner(side, db),)):
            t = self._said(str(name).strip())
            if self.hop.excluded(db, t):
                continue
            out[t] = {"system": (str(sysname).strip(), str(syslib).strip()),
                      "journal": (f"{str(jlib).strip()}/{str(jname).strip()}"
                                  if jname else None),
                      "images": str(images or "").strip()}
        return out

    def _followable(self, side, db):
        """The hop's tables with their journals, once each can be followed:
        journaled, with before images, and keyed - refused by name, with
        the command that fixes it, otherwise."""
        tables = self._journaled(side, db)
        loose = sorted(t for t, i in tables.items() if not i["journal"])
        after = sorted(t for t, i in tables.items()
                       if i["journal"] and i["images"] != "*BOTH")
        keyless = sorted(t for t in tables if not self._key(side, db, t))
        if loose:
            lib = tables[loose[0]]["system"][1]
            raise SystemExit(
                f"{', '.join(loose[:6])} are not journaled, so their changes"
                " are recorded nowhere migkit can read: STRJRNPF"
                f" FILE({lib}/{tables[loose[0]]['system'][0]} ...)"
                " JRN(<library>/<journal>) IMAGES(*BOTH) for each")
        if after:
            i = tables[after[0]]
            raise SystemExit(
                f"{', '.join(after[:6])} are journaled with after images"
                " only, so an update's old key is not recorded: CHGJRNOBJ"
                f" OBJ(({i['system'][1]}/{i['system'][0]} *FILE))"
                " ATR(*IMAGES) IMAGES(*BOTH) for each")
        if keyless:
            raise SystemExit(
                f"{', '.join(keyless[:6])} have no primary key, so a change"
                " to one cannot be applied to the row it changed: give each"
                " a primary key, or leave them out of the hop")
        return tables

    def _key(self, side, db, table):
        """`neutral_key`, asked once a table for as long as this engine
        follows it, not once a journal entry."""
        known = self.__dict__.setdefault("_keys", {})
        at = (side, db, str(table))
        if at not in known:
            known[at] = self.neutral_key(side, db, table)
        return known[at]

    def _receivers(self, side, db, journal):
        lib, name = journal.split("/")
        return self._rows(
            side, db, "select journal_receiver_library,"
                      " journal_receiver_name, first_sequence_number,"
                      " last_sequence_number, status,"
                      " minimize_esd_for_files, pending_transactions from"
                      " qsys2.journal_receiver_info where journal_library ="
                      f" {MARK} and journal_name = {MARK}", (lib, name))

    def _ends(self, side, db, journals):
        """{journal: the last sequence number it holds now}, refusing a
        journal that keeps minimized entries."""
        out = {}
        for journal in journals:
            got = self._receivers(side, db, journal)
            attached = [r for r in got if str(r[4]).strip() == "ATTACHED"]
            if attached and str(attached[0][5] or "NO").strip() != "NO":
                raise SystemExit(
                    f"the journal {journal} keeps minimized entries, which"
                    " hold only the changed bytes of a row: CHGJRN"
                    f" JRN({journal}) MINENTDTA(*NONE)")
            last = [int(r[3]) for r in got if r[3] is not None]
            first = [int(r[2]) for r in got if r[2] is not None]
            out[journal] = max(last) if last else (min(first) - 1
                                                   if first else 0)
        return out

    def _open_since(self, side, db, journal, end):
        """The first sequence number of every transaction still open at
        `end`: a copy made now cannot see them, so a tail from `end`
        would carry only their later part."""
        lib, name = journal.split("/")
        seen = {}
        for seq, etype, cycle in self._rows(
                side, db, "select sequence_number, journal_entry_type,"
                          " commit_cycle from table(qsys2.display_journal("
                          "cast(? as varchar(10)), cast(? as varchar(10)),"
                          " starting_receiver_name => '*CURCHAIN',"
                          " ending_sequence => cast(? as decimal(21, 0)),"
                          " journal_codes => 'C', journal_entry_types =>"
                          " 'SC,CM,RB')) as j order by sequence_number",
                (lib, name, end)):
            if str(etype).strip() == "SC":
                seen[int(cycle)] = int(seq)
            else:
                seen.pop(int(cycle), None)
        return sorted(seen.values())

    def change_point(self, side, db):
        """{journal: [the sequence a tail starts after, []]} for each journal
        the hop's tables are in: the last entry now, or just before the
        oldest transaction still open, which a copy made now cannot see
        and a tail must carry whole."""
        tables = self._followable(side, db)
        journals = sorted({i["journal"] for i in tables.values()})
        out = {}
        for journal, end in self._ends(side, db, journals).items():
            pending = any(str(r[6] or "").strip() == "YES"
                          for r in self._receivers(side, db, journal))
            starts = self._open_since(side, db, journal, end) if pending \
                else []
            out[journal] = [(starts[0] - 1) if starts else end, []]
        return out

    def log_position(self, side, db):
        tables = self._journaled(side, db)
        journals = sorted({i["journal"] for i in tables.values()
                           if i["journal"]})
        return {j: [end, []] for j, end in
                self._ends(side, db, journals).items()}

    @staticmethod
    def position_reached(have, want):
        if not have or not want:
            return None
        return all(j in have and int(have[j][0]) >= int(at[0])
                   for j, at in want.items())

    def stream_identity(self, side, db):
        """The system and the journals: a sequence number of another
        system's journal means nothing here."""
        got = self._rows(side, db, "select current server from"
                                   " sysibm.sysdummy1")
        tables = self._journaled(side, db)
        return {"system": str(got[0][0]).strip() if got else "",
                "journals": sorted({i["journal"] for i in tables.values()
                                    if i["journal"]})}

    def position_lost(self, side, db, token):
        """Why a journal no longer holds every entry after `token`: the
        receivers holding them deleted or freed by a save, a gap in the
        chain, or sequence numbers reset since."""
        if not token:
            return None
        for journal, (at, _) in token.items():
            got = self._receivers(side, db, journal)
            live = sorted((int(r[2]), int(r[3]) if r[3] is not None
                           else int(r[2]) - 1, f"{str(r[0]).strip()}/"
                           f"{str(r[1]).strip()}")
                          for r in got if str(r[4]).strip() in READABLE
                          and r[2] is not None)
            want = int(at) + 1
            if not live:
                return (f"the journal {journal} has no receiver left whose"
                        " entries can be read")
            top = max((int(r[3]) for r in got if r[3] is not None),
                      default=live[-1][1])
            if int(at) > top:
                return (f"the journal {journal}'s sequence numbers were"
                        f" reset: its last entry is {top}, before the"
                        f" tail's {at}")
            if want < live[0][0]:
                return (f"the journal {journal} no longer holds entry {want}:"
                        f" its oldest receiver still readable, {live[0][2]},"
                        f" starts at {live[0][0]} - the ones before it were"
                        " deleted or saved with their storage freed")
            for (a, b, name), (c, _, nxt) in zip(live, live[1:]):
                if b >= want - 1 and c != b + 1:
                    return (f"the journal {journal}'s receivers are not a"
                            f" chain from {want}: {name} ends at {b} and"
                            f" {nxt} starts at {c}")
        return None

    def _image(self, side, db, table):
        known = self.__dict__.setdefault("_images", {})
        at = (side, db, str(table))
        if at not in known:
            known[at] = Image(self._rows(
                side, db, "select column_name, data_type, length,"
                          " numeric_scale, ccsid, storage,"
                          " datetime_precision from qsys2.syscolumns where"
                          f" table_schema = {MARK} and table_name = {MARK}"
                          " order by ordinal_position",
                (self._owner(side, db), self._q(table)[1:-1])))
            known[at].cols = [(self._said(str(n).strip()), *rest)
                              for n, *rest in known[at].cols]
        return known[at]

    def neutral_changes(self, side, db, token=None, limit=1000):
        """The committed changes after `token`, transaction by transaction
        in commit order, and the position to ask from next: before the
        oldest transaction still open, with the ones committed after it
        named as done, so none is carried twice."""
        if token is None:
            return [], self.change_point(side, db)
        lost = self.position_lost(side, db, token)
        if lost:
            raise SystemExit(lost)
        tables = self._followable(side, db)
        journals = {i["journal"] for i in tables.values()}
        late = sorted(journals - set(token))
        if late:
            raise SystemExit(
                f"the hop's tables are now journaled to {', '.join(late)},"
                " which this tail was not following when it began: start"
                " the tail again from a new copy")
        objects = {f"{i['system'][0]:<10}{i['system'][1]:<10}": t
                   for t, i in tables.items()}
        ends = self._ends(side, db, sorted(token))
        out, after = [], {}
        for journal in sorted(token):
            at, done = token[journal]
            if ends.get(journal, at) <= int(at):
                after[journal] = [int(at), list(done)]
                continue
            got, after[journal] = self._read_journal(
                side, db, journal, int(at), done, ends[journal],
                objects, max(1, limit - len(out)))
            out += got
        return out, after

    def _read_journal(self, side, db, journal, at, done, end, objects,
                      room):
        """(changes, position) of one journal after `at` up to `end`.

        The position is [the sequence to read after next time, [[what is
        done after it, the sequence it was done at]]]: a transaction by
        `c<commit cycle>`, an entry outside one by `s<sequence>`."""
        lib, name = journal.split("/")
        data = ("hex(entry_data)" if self.rung(side)[0] == "mapepire"
                else "entry_data")
        cycles, befores, emitted = {}, {}, []
        done = {str(d[0]): int(d[1]) for d in done}
        read = at
        page = max(1000, 2 * room)
        while read < end and len(emitted) < room:
            rows = self._rows(side, db, JOURNAL_SQL.format(
                data=data, rows=int(page)), (lib, name, read + 1, end))
            if not rows:
                break
            for (seq, code, etype, rrn, cycle, image, nulls, obj,
                 minimized) in rows:
                seq, cycle = int(seq), int(cycle or 0)
                code, etype = str(code).strip(), str(etype).strip()
                read = seq
                if isinstance(image, str):
                    image = bytes.fromhex(image)
                if code == "C":
                    if etype == "SC":
                        cycles.setdefault(cycle, {"start": seq, "rows": [],
                                                  "rolled": False})
                    elif etype == "CM":
                        c = cycles.pop(cycle, None)
                        if c is not None and f"c{cycle}" not in done:
                            emitted += self._settled(side, db, c)
                            done[f"c{cycle}"] = seq
                    else:
                        # rolled back: nothing of it happened
                        cycles.pop(cycle, None)
                    continue
                table = objects.get(str(obj or "")[:20])
                if table is None:
                    continue
                if str(minimized or "0").strip() not in ("0", "N", ""):
                    raise SystemExit(
                        f"the journal {journal} holds a minimized entry"
                        f" ({seq}) for {table}: CHGJRN JRN({journal})"
                        " MINENTDTA(*NONE), then start the tail again from"
                        " a new copy")
                entry = (seq, etype, table, int(rrn or 0), image, nulls)
                if cycle:
                    c = cycles.setdefault(cycle, {"start": seq, "rows": [],
                                                  "rolled": False})
                    c["rows"].append(entry)
                    if etype in ("BR", "UR", "DR"):
                        c["rolled"] = True
                elif f"s{seq}" not in done:
                    was = befores.get((table, entry[3]))
                    emitted += self._row_changes(side, db, [entry], befores)
                    if etype != "UB":
                        done[f"s{seq}"] = seq
                        if was is not None:
                            # its before-image is done with it, or it
                            # would wait again for an after-image already
                            # carried
                            done[f"s{was[0]}"] = seq
            if len(rows) < page:
                break
        # the position: before the oldest transaction still open and any
        # before-image still waiting for its after-image; what is done
        # after it is named, so it is not carried again
        held = [c["start"] for c in cycles.values()]
        held += [e[0] for e in befores.values()]
        mark = min(held) - 1 if held else read
        return emitted, [mark, sorted([k, v] for k, v in done.items()
                                      if v > mark)]

    def _settled(self, side, db, cycle):
        """A committed transaction's changes. One rolled back to a savepoint
        inside it is carried as its rows now are, read again by key: the
        undoing entries say what was put back, not what it amounts to."""
        if not cycle["rolled"]:
            return self._row_changes(side, db, cycle["rows"], {})
        touched = {}
        for seq, etype, table, rrn, image, nulls in cycle["rows"]:
            img = self._image(side, db, table)
            key = self._key(side, db, table)
            try:
                k = img.cut(image, nulls, set(key))
            except Reread:
                raise SystemExit(f"{table}: a key column cannot be read from"
                                 " its journal entries")
            touched.setdefault(table, {})[tuple(k[c] for c in key)] = k
        out = []
        for table, keys in touched.items():
            out += self._as_now(side, db, table, list(keys.values()))
        return out

    def _row_changes(self, side, db, entries, befores):
        """The changes a run of row entries make, a before-image held in
        `befores` until the after-image it belongs to."""
        from .. import canon
        out = []
        for seq, etype, table, rrn, image, nulls in entries:
            img = self._image(side, db, table)
            key = self._key(side, db, table)
            if etype == "UB":
                befores[(table, rrn)] = (seq, image, nulls)
                continue
            try:
                row = img.cut(image, nulls)
            except Reread:
                try:
                    k = img.cut(image, nulls, set(key))
                    was = befores.pop((table, rrn), None)
                    old = img.cut(was[1], was[2], set(key)) if was else k
                except Reread:
                    raise SystemExit(f"{table}: a key column cannot be read"
                                     " from its journal entries")
                if etype in ("DL", "DR"):
                    out.append(canon.change("delete", table, k))
                    continue
                if old != k:
                    out.append(canon.change("delete", table, old))
                out += self._as_now(side, db, table, [k])
                continue
            k = {c: row[c] for c in key}
            if etype in ("DL", "DR"):
                out.append(canon.change("delete", table, k))
            elif etype in ("PT", "PX"):
                out.append(canon.change("insert", table, k, row))
            elif etype in ("UP", "UR"):
                was = befores.pop((table, rrn), None)
                before = img.cut(was[1], was[2]) if was else None
                old = {c: before[c] for c in key} if before else k
                if old != k:
                    # a key that changed: the old row goes and the new one
                    # comes, since an update by key cannot move a key
                    out.append(canon.change("delete", table, old))
                    out.append(canon.change("insert", table, k, row))
                else:
                    out.append(canon.change("update", table, k, row,
                                            before))
        return out

    def _as_now(self, side, db, table, keys):
        """Each key's row as it is now: an update carrying it, or a delete
        where it is gone."""
        from .. import canon
        cols = [(n, canon.comparable(self.CANON_ENGINE, t)[0])
                for n, t in self.neutral_columns(side, db, table)]
        cols = [(n, c) for n, c in cols if c]
        key = self._key(side, db, table)
        found = self.neutral_rows_by_key(
            side, db, table, cols, key, [tuple(k[c] for c in key)
                                         for k in keys])
        names = [n for n, _ in cols]
        out = []
        for k in keys:
            ident = self._key_of(cols, key, [k.get(n) for n in names])
            row = found.get(ident)
            if row is None:
                out.append(canon.change("delete", table, k))
            else:
                out.append(canon.change("update", table, k,
                                        dict(zip(names, row))))
        return out

    # the same-engine hop keeps its target following through the pair's
    # tail, which reads the journal above and applies by key

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_seed(self, db, token_path, point):
        return self._as_pair().tail_seed(db, token_path, point)

    #: its change tail is the pair's (`Engine.tail_masks`)
    TAILS_THROUGH_PAIR = True

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def delta_verify(self, db, limit=20000, log=None):
        """Only the rows the journal names since the last clean pass, asked
        of both systems (the pair's own, which the journal can serve: it is
        read without being consumed)."""
        return self._as_pair().delta_verify(db, limit, log)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    # ---- what assess says of it -------------------------------------------

    def reach_items(self, side):
        """How this side is reached, and whether its tables' changes can be
        followed, for `assess`."""
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "reach", "item": item,
                          "detail": str(detail)})
        how = self.rung(side)[0]
        add("pass" if how != "mapepire" else "warn", "reached through",
            {"odbc": "IBM's ODBC driver for IBM i",
             "drda": "DRDA, under the operator's Db2 Connect licence",
             "mapepire": "the Mapepire server on the system: values cross"
                         " as JSON and are read as their text"}[how])
        for db in (self.hop.databases or []) if side == "src" else []:
            try:
                tables = self._journaled(side, db)
            except Exception as e:  # noqa: BLE001 - said, not guessed
                add("warn", f"{db}: journals not read",
                    str(e).splitlines()[0][:120] if str(e) else
                    type(e).__name__)
                continue
            loose = sorted(t for t, i in tables.items() if not i["journal"])
            after = sorted(t for t, i in tables.items()
                           if i["journal"] and i["images"] != "*BOTH")
            if loose or after:
                add("fail", f"{db}: changes cannot be followed",
                    (f"not journaled: {', '.join(loose[:6])}; " if loose
                     else "") + (f"after images only: {', '.join(after[:6])}"
                                 if after else ""))
            else:
                add("pass", f"{db}: changes can be followed",
                    f"{len(tables)} tables journaled with before and after"
                    " images")
        return items

    def _assess_extra(self):
        return self.reach_items("src")


# ---- what migkit asks of IBM's driver and of Mapepire (backlog 0f) --------

def _driver_account():
    used, unused = "used", "unused"
    as_left = ("left at the driver's own: no IBM i here to measure a better"
               " one against")
    return {
        ("", "DRIVER"): (used, "the driver as the ODBC manager registered"
                               " it (`odbc_driver`)"),
        ("", "SYSTEM"): (used, "the endpoint's host, the system reached"),
        ("", "UID"): (used, "the endpoint's user, the profile signed in"),
        ("", "PWD"): (used, "the endpoint's password, read from its"
                            " reference"),
        ("", "NAM"): (used, "0, SQL naming: every statement names a table"
                            " by its schema"),
        ("", "CMT"): (used, "1, read committed: a copy reads committed rows,"
                            " and a written batch commits whole"),
        ("", "CCSID"): (used, "1208: text crosses as UTF-8 whatever each"
                              " column's CCSID, and is compared as text"),
        ("", "TRANSLATE"): (used, "0: a CCSID 65535 column crosses as the"
                                  " bytes it holds, compared as bytes"),
        ("", "SSL"): (used, "1 where the endpoint says `tls`, the"
                            " connection encrypted"),
        ("", "BLOCKFETCH"): (unused, as_left),
        ("", "BLOCKSIZE"): (unused, as_left),
        ("", "COMPRESSION"): (unused, as_left),
        ("", "DBQ"): (unused, "every name migkit writes is qualified by its"
                              " schema, so no library list is needed"),
        ("", "DFT"): (unused, "dates cross as ODBC date values, which no"
                              " format changes"),
        ("", "TFT"): (unused, "times cross as ODBC time values, which no"
                              " format changes"),
    }


def _mapepire_account():
    used, unused = "used", "unused"
    return {
        ("connect", "host"): (used, "the endpoint's host, the system"
                                    " reached"),
        ("connect", "port"): (used, "the endpoint's port, 8076 where it"
                                    " names none"),
        ("connect", "user"): (used, "the endpoint's user, the profile"
                                    " signed in"),
        ("connect", "password"): (used, "the endpoint's password, read from"
                                        " its reference"),
        ("connect", "ca"): (used, "the endpoint's `tls_ca_file`: the"
                                  " system's certificate checked against"
                                  " it"),
        ("connect", "ignoreUnauthorized"): (used, "only where the endpoint"
                                                  " says `tls_insecure`"),
        ("query", "isTerseResults"): (unused, "rows are read through the"
                                              " DB-API cursor, as it hands"
                                              " them over"),
        ("query", "isClCommand"): (unused, "a CL command runs as the SQL"
                                           " procedure QSYS2.QCMDEXC, the"
                                           " same on every way in"),
        ("query", "autoClose"): (unused, "a cursor is closed with the"
                                         " connection it was opened on"),
    }


def _register():
    from .. import tooluse
    tooluse.register("ibmi-odbc", options=_driver_account())
    tooluse.register("mapepire", licence="Apache-2.0",
                     options=_mapepire_account())


_register()
