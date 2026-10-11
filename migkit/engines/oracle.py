"""Oracle as a side of a pair and a hop's engine (backlog 11, F5), through
python-oracledb in thin mode: no Instant Client on the machine.

A database is a schema - its owner. Oracle folds an unquoted name to
capitals, so a name in capitals is said in small letters and a name in
small letters is written in capitals, which is how `orders` on one side
meets `ORDERS` on the other. `CHAR` values come back padded, and are
compared without the padding, as every other engine's are. A `NUMBER`
keeps no trailing zeros - `12.50` is `12.5` - so a number is read back at
the scale its column declares. An empty string is a null in Oracle: a
text column that held `''` on the other side is a difference, because it
is one. A time with a zone is read as the instant it names, in UTC, as
every other engine's is (`sys_extract_utc`), with its zone beside it: the
driver cannot hand back one whose zone is a region at all (`Zoned`).

What is Oracle's own here, each chosen from what the server says:
* the rows summed in the server, over the rendering every engine shares,
  where every column of the table has an exact rendering in Oracle's SQL
  (`_digest_sql`), and folded here otherwise;
* large values read and written in pieces through the value's locator;
* a batch written through a direct path load into a table private to the
  session and promoted in migkit's own transaction, or by array DML,
  whichever this table measured cheaper;
* the change feed read by migkit's own LogMiner reader
  (`oracle_logminer`), or by the redo-file reader where the operator runs
  one next to the redo (`oracle_redo`);
* sequences and identity columns, settings, a restore point of the
  target, and Oracle to Oracle by Data Pump over a database link where
  the target has it.
"""
import datetime
import hashlib
import os
import re
import threading
import time

from ..decide import Need, Rung
from .base import Engine, RepairAction, Result
from .dbapi import MARK, DbapiRows, FoldsToCapitals
from .oracle_catalog import OracleCatalog
from .oracle_deep import OracleDeep

#: the declared type, with the numbers a counterpart needs
DECLARED = ("case when data_type like '%WITH LOCAL TIME ZONE' then"
            " 'timestamp with local time zone'"
            " when data_type like '%WITH TIME ZONE' then"
            " 'timestamp with time zone'"
            " when data_type = 'NUMBER' and data_precision is not null then"
            " 'NUMBER(' || data_precision || ',' || nvl(data_scale, 0) || ')'"
            " when data_type = 'NUMBER' and data_scale = 0 then"
            " 'NUMBER(38,0)'"
            " when data_type in ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR')"
            " then data_type || '(' || char_length || ')'"
            " when data_type = 'RAW' then 'RAW(' || data_length || ')'"
            " else data_type end")

#: what migkit makes on a target for its own use, and leaves out of every
#: listing: the session-private tables a direct path load is staged in
OWN_PREFIX = "MIGKIT$"

#: a large value's size from which it is read and written in pieces,
#: through its locator, rather than in one fetch or one bind
PIECE = 8 * 2 ** 20

#: kinds of column, by the catalogue's data type
LOBS = ("CLOB", "NCLOB", "BLOB")
ZONED = ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE")

_POOLS = {}
_POOLS_LOCK = threading.Lock()

#: an Oracle-to-Oracle digest: SHA-256 of the run's salt and each row's
#: text, its first 64 bits summed - never Oracle's CHECKSUM, which folds by
#: exclusive or, so two copies of a row cancel
SHA_HEX = 16


def _kind(data_type):
    """The catalogue's data type without its precision: `TIMESTAMP(6)
    WITH TIME ZONE` is `TIMESTAMP WITH TIME ZONE`."""
    return " ".join(re.sub(r"\(\d+\)", "", str(data_type or "")).split()
                    ).upper()


def _session(conn, requested_tag=None):
    """Every session migkit opens reads and writes instants in UTC and
    dates in one spelling, whatever the server's defaults: a time with a
    local zone comes back as UTC, and what the change reader parses out of
    the redo is always the same text."""
    conn.cursor().execute(
        "alter session set time_zone = 'UTC'"
        " nls_date_format = 'YYYY-MM-DD HH24:MI:SS'"
        " nls_timestamp_format = 'YYYY-MM-DD HH24:MI:SS.FF9'"
        " nls_timestamp_tz_format = 'YYYY-MM-DD HH24:MI:SS.FF9 TZR'"
        " nls_numeric_characters = '.,'")


class Zoned(datetime.datetime):
    """A value of a TIMESTAMP WITH TIME ZONE column: the instant the server
    computed, in UTC - so it renders, compares and moves as every other
    engine's instant does - carrying the zone the column held it in, which
    an Oracle target is written in again, and the digits past its
    microsecond (`nanotime`). The driver cannot hand the value back itself
    where the zone is a region (measured: `DPY-3022: named time zones are
    not supported in thin mode`)."""

    zone = None
    nanosecond = 0

    @classmethod
    def at(cls, utc, zone):
        got = cls(utc.year, utc.month, utc.day, utc.hour, utc.minute,
                  utc.second, utc.microsecond, tzinfo=datetime.timezone.utc)
        got.zone = zone
        got.nanosecond = getattr(utc, "nanosecond", 0) or 0
        return got

    def __reduce_ex__(self, protocol):
        # carried to the processes a copy runs its ranges in, zone and all
        return (_zoned, (datetime.datetime(
            self.year, self.month, self.day, self.hour, self.minute,
            self.second, self.microsecond, datetime.timezone.utc),
            self.zone, self.nanosecond))


def _zoned(utc, zone, ns):
    from .. import nanotime
    return Zoned.at(nanotime.of(utc, ns), zone)


def _units(piece):
    """How far a piece moves a LOB's offset: bytes, or a text's UTF-16
    units."""
    if isinstance(piece, (bytes, bytearray)):
        return len(piece)
    return len(piece.encode("utf-16-le")) // 2


def _as_time(value):
    """A time a change feed handed on as text - PostgreSQL's logical
    decoding prints `2024-01-01 00:01:00+00` - as the time it is: bound as
    text, Oracle reads it by the session's format and refuses it. Text that
    is no time (`infinity`) is left as it is, and refused by the server
    loudly."""
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value.strip())
        except ValueError:
            return value
    return value


def _utc_wall(value):
    """A time's UTC wall clock: an instant converted, a time with no zone
    taken as UTC already."""
    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return value


#: the kinds a session's stage can hold, by the type it holds them in
STAGED = {"NUMBER": "NUMBER", "FLOAT": "NUMBER", "DATE": "DATE",
          "BINARY_FLOAT": "BINARY_FLOAT", "BINARY_DOUBLE": "BINARY_DOUBLE",
          "CLOB": "CLOB", "NCLOB": "NCLOB", "BLOB": "BLOB",
          "TIMESTAMP": "VARCHAR2(40)",
          "TIMESTAMP WITH TIME ZONE": "VARCHAR2(40)",
          "TIMESTAMP WITH LOCAL TIME ZONE": "VARCHAR2(40)"}


#: how a time is bound: as its text with every digit after the second -
#: the driver sends a datetime at six, where a TIMESTAMP(9) holds nine
#: (`nanotime`) - read by the server in this spelling
TIME_TEXT = "YYYY-MM-DD HH24:MI:SS.FF9"


def _time_text(value):
    """A time as the text bound for it: an instant's UTC wall clock, and
    every one of its digits."""
    value = _as_time(value)
    if value is None or isinstance(value, str):
        return value
    ns = getattr(value, "nanosecond", 0) or 0
    wall = _utc_wall(value)
    return f"{wall:%Y-%m-%d %H:%M:%S}.{wall.microsecond:06d}{ns:03d}"


def _zone_literal(zone):
    """A zone as a literal: `at time zone` takes no bind and no column -
    measured on 23.26, each was refused (ORA-00917, then ORA-00907 inside
    brackets) - so the zones a batch holds are written into its
    statements."""
    return "'" + str(zone or "UTC").replace("'", "''") + "'"


def _interval(kind, value):
    """An interval as the driver binds it into a column of `kind`: a
    year-to-month as its years and months, a day-to-second as a
    `timedelta`. A month has no day-to-second value, nor a digit past the
    microsecond a `timedelta` holds: refused rather than written as some
    other length of time."""
    import datetime
    import oracledb
    from .. import intervals
    if value is None or isinstance(value, (datetime.timedelta,
                                           oracledb.IntervalYM)):
        return value
    months, days, nanos = intervals.of(value)
    if kind.startswith("INTERVAL YEAR"):
        if days or nanos:
            raise ValueError(f"{intervals.text(value)} has days or a time,"
                             f" which a year-to-month interval cannot hold")
        sign = -1 if months < 0 else 1
        years, left = divmod(abs(months), 12)
        return oracledb.IntervalYM(sign * years, sign * left)
    got = intervals.timedelta_of(value)
    if got is None:
        raise ValueError(f"{intervals.text(value)} has months or a digit"
                         f" past the microsecond, which a day-to-second"
                         f" interval bound from here cannot hold")
    return got


class Bind:
    """How one column is written: its place in a statement (`mark`), the
    values bound there (`values`), the driver's type for them (`sizes`),
    and how the session's stage holds it (`staged`, `promote`). A time
    with a zone is written as its instant in the zone it came in, which a
    statement names (`zone`); a batch of several zones is written a
    statement a zone."""

    def __init__(self, name, kind):
        self.name, self.kind = name, kind
        self.lob = kind in LOBS
        self.timed = kind.startswith("TIMESTAMP")
        self.zoned = kind == "TIMESTAMP WITH TIME ZONE"
        # the load names its columns without quotes (measured: a column
        # named in small letters was read as capitals, ORA-00904), so a
        # name of mixed case has no column in a stage
        self.stageable = name == name.lower() and (kind in STAGED or kind in (
            "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"))

    def zone(self, value):
        """The zone a value of this column is written in: its own where it
        came with one (`Zoned`), UTC otherwise; None for any other
        column."""
        if not self.zoned:
            return None
        return getattr(_as_time(value), "zone", None) or "UTC"

    def mark(self, zone=None):
        if not self.timed:
            return MARK
        at = f"to_timestamp({MARK}, '{TIME_TEXT}')"
        if self.zoned:
            return (f"(from_tz({at}, 'UTC') at time zone"
                    f" {_zone_literal(zone)})")
        if self.kind == "TIMESTAMP WITH LOCAL TIME ZONE":
            return f"from_tz({at}, 'UTC')"
        return at

    def values(self, value, staged=False):
        from .. import canon
        if self.timed:
            return [_time_text(value)] + ([self.zone(value)]
                                          if staged and self.zoned else [])
        if self.kind == "DATE":
            return [_utc_wall(_as_time(value))]
        if self.kind.startswith("INTERVAL"):
            return [_interval(self.kind, value)]
        return [canon.sql_value(value)]

    def sizes(self):
        import oracledb
        if self.kind.startswith("INTERVAL"):
            return [oracledb.DB_TYPE_INTERVAL_YM
                    if self.kind.startswith("INTERVAL YEAR")
                    else oracledb.DB_TYPE_INTERVAL_DS]
        return [{"BLOB": oracledb.DB_TYPE_BLOB,
                 "CLOB": oracledb.DB_TYPE_CLOB,
                 "NCLOB": oracledb.DB_TYPE_NCLOB}.get(self.kind)]

    def staged(self, fact):
        """[(column, type)] in the stage: a time as its text, and a zone
        beside it."""
        kind = self.kind
        if self.timed:
            return [(self.name, "VARCHAR2(40)")] + (
                [(f"{self.name}$z", "VARCHAR2(64)")] if self.zoned else [])
        if kind in STAGED:
            return [(self.name, STAGED[kind])]
        chars = int(fact.get("chars") or fact.get("length") or 1)
        if kind in ("VARCHAR2", "CHAR"):
            return [(self.name, f"{kind}({chars} CHAR)")]
        if kind in ("NVARCHAR2", "NCHAR"):
            return [(self.name, f"{kind}({chars})")]
        if kind == "RAW":
            return [(self.name, f"RAW({int(fact.get('length') or 1)})")]
        return [(self.name, kind)]

    def promote(self, q, zones=()):
        """The column read out of the stage `s`, as the table takes it;
        `zones` the ones the batch holds."""
        col = f"s.{q(self.name)}"
        if not self.timed:
            return col
        at = f"to_timestamp({col}, '{TIME_TEXT}')"
        if self.zoned:
            return ("case s." + q(self.name + "$z") + " " + " ".join(
                f"when {_zone_literal(z)} then (from_tz({at}, 'UTC') at"
                f" time zone {_zone_literal(z)})"
                for z in (sorted(zones) or ["UTC"])) + " end")
        if self.kind == "TIMESTAMP WITH LOCAL TIME ZONE":
            return f"from_tz({at}, 'UTC')"
        return at


def _by_zone(binds, rows):
    """{the zones of a row's zoned columns: [rows]}, in the rows' order -
    one statement for each."""
    zoned = [i for i, b in enumerate(binds) if b.zoned]
    out = {}
    for r in rows:
        out.setdefault(tuple(binds[i].zone(r[i]) for i in zoned),
                       []).append(r)
    return out


def _marks(binds, zones):
    """Each column's place in a statement written for these zones."""
    it = iter(zones)
    return [b.mark(next(it)) if b.zoned else b.mark() for b in binds]


#: how a batch is written (`OracleEngine._write_way`): each tried once on
#: a table's large batches, then the cheaper by the row
WRITE_WAYS = (
    Rung("direct", "a direct path load into the session's stage, merged in"
                   " migkit's transaction",
         needs=(Need("a batch large enough to pay for the load",
                     lambda f: f["rows"] >= f["from"],
                     "a batch this small costs {rung} more than it saves"),
                Need("no value to write in pieces", lambda f: not f["big"],
                     "{rung} writes no value in pieces"),
                Need("every column of a type the stage holds",
                     lambda f: f["stageable"],
                     "{rung} has no stage for one of the table's types")),
         because="the load bypasses the buffer cache, and one MERGE puts"
                 " the batch in",
         footprint=1),
    Rung("array", "array DML, a statement for the batch",
         because="deleted by key and inserted, one round trip each"))


class OracleEngine(OracleCatalog, OracleDeep, FoldsToCapitals, DbapiRows,
                   Engine):
    checks = ("schema", "counts", "autoinc", "data")
    CANON_ENGINE = "oracle"
    OWN_PREFIX = OWN_PREFIX
    BRAND_FAMILY = "oracle"
    #: what `assess` asks before anything moves: the tables no key names a
    #: row of, and text stored twice as UTF-8 on the source
    PREFLIGHT = ("_deep_keys", "_mojibake")
    CHARSETS = True
    SQL_DIALECT = "oracle"
    FOLDED_BECAUSE = ("holds a column this table has that Oracle's SQL"
                      " cannot render exactly, or a row too wide to hash"
                      " in the server")
    OVER_NETWORK = True
    PARAMSTYLE = "numeric"
    LIMIT = "fetch"
    PADDED = ("CHAR", "NCHAR")
    SCALED = ("NUMBER",)
    #: LogMiner and the redo-file reader only read: a plain full copy can
    #: note where the redo was before it for a later `--mode cdc`
    CHANGE_POINT_READS_ONLY = True
    @property
    def READS_AHEAD(self):
        """LogMiner's position says where to read from next and nothing
        more, so the tail reads the next batch while it applies this one.
        The redo-file reader is told at each read that the batch before it
        was applied, which a read made ahead would say too soon."""
        from . import oracle_redo
        try:
            return oracle_redo.chosen(self, "src", (self.hop.databases
                                                    or [""])[0]) \
                != oracle_redo.RUNG
        except Exception:  # noqa: BLE001 - not known: one at a time
            return False

    # ---- sessions -------------------------------------------------------

    def _endpoint(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _dsn(self, ep, service=None):
        service = (service or ep.options.get("service")
                   or ep.options.get("service_name") or "FREEPDB1")
        return f"{ep.host}:{ep.port or 1521}/{service}"

    def _tls(self, ep):
        """What the endpoint's TLS asks of a connection (`tls`,
        `tls_ca_file`, `tls_insecure`): the listener's TCPS port, the
        server's certificate checked against the system's authorities or
        the file's, and the name on it against the host's. Nothing is
        checked only where `tls_insecure` says so (which `assess` fails
        on); {} where none is asked, the connection plain as before."""
        o = ep.options or {}
        if not (o.get("tls") or o.get("tls_ca_file")
                or o.get("tls_insecure")):
            return {}
        import ssl
        ctx = ssl.create_default_context(
            cafile=str(o["tls_ca_file"]) if o.get("tls_ca_file") else None)
        insecure = bool(o.get("tls_insecure"))
        if insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return {"protocol": "tcps", "ssl_context": ctx,
                "ssl_server_dn_match": not insecure}

    def _pool(self, side):
        """One pool per endpoint and process. A connection each statement
        made cost a round of logon and session setup per catalogue question
        - and the table copier asks one per batch."""
        import oracledb
        ep = self._endpoint(side)
        name = (os.getpid(), ep.host, ep.port, ep.user,
                self._dsn(ep), bool(self._tls(ep)))
        with _POOLS_LOCK:
            pool = _POOLS.get(name)
            if pool is None:
                pool = _POOLS[name] = oracledb.create_pool(
                    user=ep.user, password=ep.password, dsn=self._dsn(ep),
                    min=0, max=16, increment=1, session_callback=_session,
                    getmode=oracledb.POOL_GETMODE_WAIT,
                    tcp_connect_timeout=15, **self._tls(ep))
        return pool

    def _connect(self, side, db):
        import oracledb
        # a LOB as its value, not as a handle to read it through; a number
        # as a decimal, not as a float that has already rounded it
        oracledb.defaults.fetch_lobs = False
        oracledb.defaults.fetch_decimals = True
        return self._pool(side).acquire()

    def _dedicated(self, side, dsn=None, user=None, password=None):
        """A connection of its own, outside the pool: a LogMiner session,
        or a load whose session-private rows must not meet another
        caller's."""
        import oracledb
        oracledb.defaults.fetch_lobs = False
        oracledb.defaults.fetch_decimals = True
        ep = self._endpoint(side)
        conn = oracledb.connect(user=user or ep.user,
                                password=password or ep.password,
                                dsn=dsn or self._dsn(ep),
                                tcp_connect_timeout=15, **self._tls(ep))
        _session(conn)
        return conn

    def databases(self):
        return list(self.hop.databases)

    def _nanos_text(self, quoted, declared):
        """A `TIMESTAMP` of seven to nine digits as the server writes it:
        the driver hands back six."""
        from .. import canon
        name, nums = canon._split_declared(declared)
        if name == "timestamp" and nums and nums[0] > 6:
            return f"to_char({quoted}, 'YYYY-MM-DD HH24:MI:SS.FF9')"
        return None

    # ---- what a table is -------------------------------------------------

    def neutral_tables(self, side, db):
        return [t for t in (self._said(n) for (n,) in self._rows(
            side, db, "select table_name from all_tables where owner ="
                      f" {MARK} and nested = 'NO' and secondary = 'N'"
                      " and temporary = 'N' and table_name not like"
                      f" '{OWN_PREFIX}%' escape '\\' order by 1",
            (self._owner(side, db),)))
            if self.in_scope(db, t)]

    def neutral_columns(self, side, db, table):
        return [(self._said(n), t) for n, t in self._rows(
            side, db, f"select column_name, {DECLARED} from all_tab_columns"
                      f" where owner = {MARK} and table_name = {MARK}"
                      " order by column_id",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def _catalog_reads(self, side, db):
        """(statement, binds) of the schema's columns `column_catalog`
        reads and `shape_mark` digests: one row a column - its table, its
        name, its declared type and its place."""
        return (f"select table_name ct, column_name cn, {DECLARED} cd,"
                " column_id cp from all_tab_columns where owner ="
                f" {MARK} and table_name not like '{OWN_PREFIX}%' escape"
                " '\\' and table_name in (select table_name from"
                f" all_tables where owner = {MARK} and nested ="
                " 'NO' and secondary = 'N' and temporary = 'N')",
                (self._owner(side, db), self._owner(side, db)))

    def column_catalog(self, side, db):
        """{table: [(column, type)]} for the whole schema in one query:
        what the change tail's shape gate compares before each batch, so a
        DDL made under it is seen rather than mined."""
        sql, binds = self._catalog_reads(side, db)
        out = {}
        for t, n, typ, _place in self._rows(
                side, db, sql + " order by 1, column_id", binds):
            out.setdefault(self._said(t), []).append((self._said(n),
                                                      str(typ)))
        return {t: sorted(c) for t, c in out.items()}

    def shape_mark(self, side, db):
        """One digest of what `column_catalog` reads, computed by the
        server (`_catalog_reads`): the columns counted, and each column's
        row - its values as their lengths and texts, a missing one apart -
        hashed twice with two seeds (`ora_hash`, 32 bits each) and each
        hash summed, so the digest holds however many columns there are
        and whatever order they are read in; a column's place is among
        its values. What a tail asks every batch before it reads the
        whole shape again (`hetero._shape_now`, audit SP8)."""
        sql, binds = self._catalog_reads(side, db)
        row = " || ".join(
            f"nvl2({v}, length({v}) || ':' || {v}, '-')"
            for v in ("q.ct", "q.cn", "q.cd", "to_char(q.cp)"))
        got = self._rows(side, db, (
            f"select count(*), sum(ora_hash({row}, 4294967295, 0)),"
            f" sum(ora_hash({row}, 4294967295, 1)) from ({sql}) q"), binds)
        return ":".join(str(v if v is not None else 0) for v in got[0]) \
            if got else None

    def _facts(self, side, db, table):
        """[{name, kind, length, chars, precision, scale}] in column order:
        what the reads, the writes and the server's digest are built from."""
        return [{"name": self._said(n), "kind": _kind(t), "length": ln,
                 "chars": cl, "precision": p, "scale": s,
                 "virtual": v == "YES"}
                for n, t, ln, cl, p, s, v in self._rows(
                    side, db, "select column_name, data_type, data_length,"
                              " char_length, data_precision, data_scale,"
                              " virtual_column from all_tab_cols where"
                              f" owner = {MARK} and table_name = {MARK} and"
                              " hidden_column = 'NO' order by column_id",
                    (self._owner(side, db), self._q(table)[1:-1]))]

    def _unwritable_columns(self, side, db, table):
        """Virtual columns: the server computes them and refuses to be
        told (ORA-54013)."""
        return {f["name"] for f in self._facts(side, db, table)
                if f["virtual"]}

    def neutral_key(self, side, db, table):
        return [self._said(n) for (n,) in self._rows(
            side, db, "select cc.column_name from all_constraints c join"
                      " all_cons_columns cc on cc.owner = c.owner and"
                      " cc.constraint_name = c.constraint_name where"
                      f" c.owner = {MARK} and c.table_name = {MARK} and"
                      " c.constraint_type = 'P' order by cc.position",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def table_facts(self, side, db):
        """Rows and bytes by the optimiser's statistics - the catalogue
        every account can read; the segments are the owner's own view."""
        out = {}
        keyed = {t for (t,) in self._rows(
            side, db, "select table_name from all_constraints where owner ="
                      f" {MARK} and constraint_type = 'P'",
            (self._owner(side, db),))}
        for name, rows, width in self._rows(
                side, db, "select table_name, num_rows, avg_row_len from"
                          f" all_tables where owner = {MARK} and"
                          f" table_name not like '{OWN_PREFIX}%' escape"
                          " '\\' and temporary = 'N'",
                (self._owner(side, db),)):
            out[self._said(name)] = {
                "rows": rows,
                "bytes": (int(rows) * int(width)) if rows and width
                else None,
                "key": name in keyed}
        return out

    def text_encodings(self, side, db):
        """{character set: what is stored in it}: the database's for
        character columns, the national one where a column uses it."""
        sets = dict(self._rows(side, db, "select parameter, value from"
                                         " nls_database_parameters where"
                                         " parameter in"
                                         " ('NLS_CHARACTERSET',"
                                         " 'NLS_NCHAR_CHARACTERSET')"))
        out = {sets.get("NLS_CHARACTERSET", "?"): ["text columns"]}
        national = self._rows(side, db, "select count(*) from all_tab_columns"
                                        f" where owner = {MARK} and"
                                        " data_type in ('NCHAR', 'NVARCHAR2',"
                                        " 'NCLOB')", (self._owner(side, db),))
        if national and national[0][0]:
            out.setdefault(sets.get("NLS_NCHAR_CHARACTERSET", "?"),
                           []).append("national text columns")
        return out

    def settle_target(self, db, from_source=True):
        """Statistics for the schema the load wrote."""
        self._target_only("dst", "refresh statistics")
        conn = self._connect("dst", db)
        try:
            self._run(conn.cursor(), "begin dbms_stats.gather_schema_stats("
                                     f"{MARK}); end;",
                      (self._owner("dst", db),))
        finally:
            conn.close()
        return "refreshed the target's statistics"

    # ---- reading ----------------------------------------------------------
    #
    # A column is read by an expression of its own where the driver would
    # hand back less than the column holds: a time with a zone as the UTC
    # instant, and a large value by its length and, below `PIECE`, inline -
    # above it, by the row's address, in pieces through its locator.

    def _read_plan(self, side, db, table, columns):
        """(select list, finish): `finish(rows)` gives the rows back in
        `columns`' order - a time of more than six digits with all of them
        (`_nanos_text`, as every engine reading through this base does), a
        time with a zone as the instant it names carrying its zone
        (`Zoned`), a large value read in pieces."""
        from .. import nanotime
        facts = {f["name"]: f for f in self._facts(side, db, table)}
        exprs, parts, lobs = [], [], False
        for n, cls in columns:
            f = facts.get(n, {})
            kind = f.get("kind")
            q = self._q(n)
            # a timestamp's digits after the second are its data_scale
            digits = f"timestamp({int(f.get('scale') or 6)})"
            if kind in ZONED:
                at = f"sys_extract_utc({q})"
                wide = self._nanos_text(at, digits)
                exprs.append(wide or at)
                if kind == "TIMESTAMP WITH TIME ZONE":
                    exprs.append(f"to_char({q}, 'TZR')")
                parts.append(("zoned" if kind == "TIMESTAMP WITH TIME ZONE"
                              else "utc", bool(wide), n))
            elif kind in LOBS:
                exprs += [f"case when dbms_lob.getlength({q}) <= {PIECE}"
                          f" then {q} end", f"dbms_lob.getlength({q})"]
                parts.append(("lob", False, n))
                lobs = True
            elif kind == "TIMESTAMP" and self._nanos_text(q, digits):
                exprs.append(self._nanos_text(q, digits))
                parts.append(("wide", True, n))
            else:
                exprs.append(q)
                parts.append(("plain", False, n))
        if lobs:
            exprs.append("rowidtochar(rowid)")
        if all(p[0] == "plain" for p in parts):
            return ", ".join(exprs), lambda raw: [list(r) for r in raw]

        def time_of(v, text):
            return nanotime.parse(v) if text and v is not None else v

        def finish(raw):
            out, big = [], []
            for r in raw:
                row, j = [], 0
                for i, (how, text, n) in enumerate(parts):
                    if how == "zoned":
                        at, zone = time_of(r[j], text), r[j + 1]
                        j += 2
                        row.append(None if at is None
                                   else Zoned.at(at, zone))
                    elif how == "utc":
                        at = time_of(r[j], text)
                        j += 1
                        row.append(None if at is None else nanotime.of(
                            at.replace(tzinfo=datetime.timezone.utc),
                            getattr(at, "nanosecond", 0)))
                    elif how == "lob":
                        value, size = r[j], r[j + 1]
                        j += 2
                        if value is None and size:
                            big.append((len(out), i, n, r[-1]))
                        row.append(value)
                    else:
                        row.append(time_of(r[j], text))
                        j += 1
                out.append(row)
            for at, i, n, rid in big:
                out[at][i] = self._read_in_pieces(side, db, table, n, rid)
            return out
        return ", ".join(exprs), finish

    def _read_in_pieces(self, side, db, table, column, rowid):
        """One large value, read through its locator a multiple of its
        chunk at a time rather than in one fetch."""
        import oracledb
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()

            def locator(cursor, metadata):
                if metadata.type_code in (oracledb.DB_TYPE_CLOB,
                                          oracledb.DB_TYPE_NCLOB,
                                          oracledb.DB_TYPE_BLOB):
                    return cursor.var(metadata.type_code,
                                      arraysize=cursor.arraysize)
            cur.outputtypehandler = locator
            cur.execute(f"select {self._q(column)} from"
                        f" {self._qualified(side, db, table)} where rowid ="
                        " chartorowid(:1)", [rowid])
            got = cur.fetchone()
            if got is None or got[0] is None:
                return None
            return self._pieces(got[0])
        finally:
            conn.close()

    @staticmethod
    def _pieces(lob):
        """A locator's whole value, read `PIECE` rounded to its chunk at a
        time. A text LOB counts in UTF-16 units, where a character past
        the first plane is two: an offset moves by those, and a piece that
        would end between the two halves of one is read a unit shorter
        (measured: split there, the driver refuses the piece,
        `UnicodeDecodeError`)."""
        chunk = lob.getchunksize() or 8192
        step = max(chunk, (PIECE // chunk) * chunk)
        size = lob.size()
        parts, at = [], 1
        while at <= size:
            try:
                part = lob.read(at, step)
            except UnicodeDecodeError:
                part = lob.read(at, step - 1)
            if not part:
                break
            parts.append(part)
            at += _units(part)
        if not parts:
            return b"" if isinstance(lob.read(1, 1) or b"", bytes) else ""
        return (b"" if isinstance(parts[0], bytes) else "").join(parts)

    def _read_query(self, side, db, table, columns, after, limit, where):
        names = [n for n, _ in columns]
        key = self.neutral_key(side, db, table)
        resume, args = ("", [])
        if key and after is not None:
            resume, args = self._after(key, after)
        parts = [p for p in (resume, f"({where})" if where else "") if p]
        cond = (" where " + " and ".join(parts)) if parts else ""
        order = (" order by " + ", ".join(self._q(k) for k in key)
                 if key else "")
        select, finish = self._read_plan(side, db, table, columns)
        return (self._select(select, self._qualified(side, db, table), cond,
                             order, limit if key else None),
                args, key, names, finish)

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        sql, args, key, names, finish = self._read_query(
            side, db, table, columns, after, limit, where)
        rows = self._unpad(side, db, table, columns,
                           finish(self._rows(side, db, sql, args)))
        if not rows or not key or not all(k in names for k in key):
            return (rows, None)
        return (rows, tuple(rows[-1][names.index(k)] for k in key))

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        sql, args, _, _, finish = self._read_query(side, db, table, columns,
                                                   None, None, where)
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.arraysize = max(int(size), 100)
            cur.prefetchrows = cur.arraysize + 1
            self._run(cur, sql, args)
            while True:
                rows = cur.fetchmany(size)
                if not rows:
                    break
                yield self._unpad(side, db, table, columns, finish(rows))
        finally:
            conn.close()

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        """The rows at these keys, read as the copier reads them - its
        padding taken off and its numbers at their scale, which the
        confirm pass compares against another engine's rows."""
        from .. import canon
        if not key or not keys:
            return {}
        keys = list(keys)
        select, finish = self._read_plan(side, db, table, columns)
        one = "(" + " and ".join(f"{self._q(k)} = {MARK}" for k in key) + ")"
        found = []
        for i in range(0, len(keys), self.DELETE_BATCH):
            part = keys[i:i + self.DELETE_BATCH]
            cond = " or ".join([one] * len(part))
            if where:
                cond = f"({cond}) and ({where})"
            found += finish(self._rows(
                side, db, f"select {select} from"
                          f" {self._qualified(side, db, table)} where {cond}",
                [canon.sql_value(v) for k in part for v in k]))
        return self._by_key_map(columns, key,
                                self._unpad(side, db, table, columns, found))

    # ---- the digest --------------------------------------------------------
    #
    # Each row rendered to the text every engine renders it to, hashed and
    # summed in the server: the same number `canon.fold_rows` makes here,
    # so a table can be compared with PostgreSQL's or MySQL's own sum, or
    # with a sum folded here, without a row crossing the network. A sum,
    # never Oracle's CHECKSUM: that folds by exclusive or, so two copies of
    # a row cancel and three read as one.

    #: widest row text a `standard_hash` input may be, by `max_string_size`
    ROW_TEXT_LIMIT = {"STANDARD": 4000, "EXTENDED": 32767}

    def _server_facts(self, side, db):
        """(character set, row text limit), read once per side."""
        cache = self.__dict__.setdefault("_server_facts_cache", {})
        if side not in cache:
            charset = dict(self._rows(
                side, db, "select parameter, value from"
                          " nls_database_parameters where parameter ="
                          " 'NLS_CHARACTERSET'")).get("NLS_CHARACTERSET")
            try:
                size = self._rows(side, db, "select value from v$parameter"
                                            " where name ="
                                            " 'max_string_size'")
                size = str(size[0][0]).upper() if size else "STANDARD"
            except Exception:  # noqa: BLE001 - not readable: the narrower
                size = "STANDARD"
            cache[side] = (charset, self.ROW_TEXT_LIMIT.get(size, 4000))
        return cache[side]

    #: the whole digits `to_char` writes out in full, and the fraction
    #: digits the format below does; a value outside is counted and sends
    #: the table to be folded here (measured: `to_char(1e61)` is
    #: `1.0000000000000000000000000000000000E+61`)
    WHOLE_DIGITS, FRACTION_DIGITS = 38, 40

    def _render_sql(self, fact, cls):
        """(expression yielding the canonical text, expression that is 1
        where a value falls outside what the expression renders exactly),
        or None where the column has no exact rendering in Oracle's SQL.

        Measured against the renderer every engine shares: `to_char` of a
        number writes `.5` for 0.5, and of a BINARY_DOUBLE 0.1
        `1.0000000000000001E-001` - so a number is written by a format of
        its declared scale, one with none whole part and fraction apart
        (one format of both widths is refused, ORA-01481), and a binary
        float is folded here. A time is written at six digits after the
        second and the three after them where it has any, as the renderer
        writes a value read with all nine (`nanotime`)."""
        from .. import canon
        q, kind = self._q(fact["name"]), fact["kind"]
        if cls in ("integer", "decimal", canon.NUMBER) and kind == "NUMBER":
            scale = fact.get("scale")
            # a decimal written with its declared scale's digits; one
            # compared by value (`canon.NUMBER`) - or of no declared scale
            # - with the digits it has, which is what Oracle keeps
            if cls != canon.NUMBER and scale is not None and scale > 0 \
                    and fact.get("precision") is not None:
                whole = max(int(fact["precision"]) - int(scale), 1)
                fmt = "FM" + "9" * (whole - 1) + "0." + "0" * int(scale)
                return f"to_char({q}, '{fmt}')", "0"
            if cls != canon.NUMBER and scale is not None and scale <= 0:
                return f"to_char({q})", "0"
            frac = f"abs({q}) - trunc(abs({q}))"
            return (f"case when {q} < 0 then '-' end ||"
                    f" to_char(trunc(abs({q}))) || case when {frac} <> 0"
                    f" then to_char({frac}, 'FM."
                    f"{'9' * self.FRACTION_DIGITS}') end",
                    f"case when {q} <> 0 and (abs({q}) >="
                    f" 1e{self.WHOLE_DIGITS} or abs({q}) <"
                    f" 1e-{self.FRACTION_DIGITS} or {q} <> round({q},"
                    f" {self.FRACTION_DIGITS})) then 1 else 0 end")
        if cls == "text" and kind in ("VARCHAR2", "CHAR", "NVARCHAR2",
                                      "NCHAR"):
            # a national value in the database's own character set, so the
            # hash is of its UTF-8 (measured: hashed as it is stored, the
            # UTF-16 of `naïve`, it was another value)
            e = q if kind in ("VARCHAR2", "CHAR") else f"to_char({q})"
            if kind in ("CHAR", "NCHAR"):
                e = f"rtrim({e}, ' ')"
            return e, "0"
        if cls == "bytes" and kind == "RAW":
            return f"rawtohex({q})", "0"
        # a year before the first is no year the renderer can write, and a
        # format of four places would write it as one after
        if cls == "timestamp" and kind == "DATE":
            # a NULL stays one: joined to a constant, Oracle's `||` would
            # make it the constant (measured, `.000000`)
            return (f"nvl2({q}, to_char({q}, 'YYYY-MM-DD HH24:MI:SS') ||"
                    " '.000000', null)",
                    f"case when {q} < date '0001-01-01' then 1 else 0 end")
        if cls == "timestamp" and kind.startswith("TIMESTAMP"):
            at = f"sys_extract_utc({q})" if kind in ZONED else q
            # six digits, and the three after them where a value has any
            # (`canon.FRACTION`): a value cut to six on the way across is
            # not the value
            return (f"to_char({at}, 'YYYY-MM-DD HH24:MI:SS.FF6') ||"
                    f" nullif(substr(to_char({at}, 'FF9'), 7, 3), '000')",
                    f"case when {at} < timestamp '0001-01-01 00:00:00'"
                    " then 1 else 0 end")
        return None

    @staticmethod
    def _rendered_width(fact):
        """The most bytes a column's rendering takes in the row text, its
        length and separator with it."""
        kind, length = fact["kind"], int(fact.get("length") or 0)
        if kind == "NUMBER":
            return 120
        if kind == "RAW":
            return 2 * length + 12
        if kind in ("NVARCHAR2", "NCHAR"):
            # two bytes a character held, at most four written
            return 2 * length + 12
        if kind == "DATE" or kind.startswith("TIMESTAMP"):
            return 40
        return length + 12

    @staticmethod
    def _sha_fold(classes, rows, salt, total=0):
        """(rows, total): each row's text as every engine renders it, its
        salted SHA-256's first 64 bits summed - what `_digest_sql` sums in
        the server for Oracle to Oracle, folded here."""
        from .. import canon, rowtext
        n = 0
        for row in rows:
            text = rowtext.encode([canon.render_value(c, v)
                                   for c, v in zip(classes, row)])
            h = hashlib.sha256((salt + text).encode(),
                               usedforsecurity=False).hexdigest()
            total += int(h[:SHA_HEX], 16)
            n += 1
        return n, total

    def _digest_sql(self, side, db, table, columns, where=None, salt=None):
        """The server's digest of the table, as (sql, args), or None where
        some column of it has no exact rendering here, or its rows may be
        wider than a hash's input can be (4,000 bytes, or 32,767 where
        `max_string_size` is EXTENDED: past it the concatenation is
        refused, ORA-01489). With `salt`, the Oracle-to-Oracle digest."""
        from .. import canon, rowtext
        charset, limit = self._server_facts(side, db)
        if str(charset).upper() != "AL32UTF8":
            # the hash is of the bytes the server holds, which are the
            # UTF-8 the other engines hash only in this character set
            return None
        facts = {f["name"]: f for f in self._facts(side, db, table)}
        parts, outside, width = [], [], 0
        for n, cls in columns:
            f = facts.get(n)
            got = self._render_sql(f, cls) if f else None
            if got is None:
                return None
            parts.append(got[0])
            if got[1] != "0":
                outside.append(got[1])
            width += self._rendered_width(f)
        if width + len(salt or "") > limit:
            return None
        # `lengthc`: `length` counts a character outside the first plane
        # as two (measured, U+1D11E is 2, `lengthc` 1), and the length in
        # the row text is of characters
        row = f" || '{rowtext.SEP}' || ".join(
            f"nvl(to_char(lengthc({e})), '{rowtext.NULL_LEN}') || ':' ||"
            f" {e}" for e in parts)
        if salt:
            # Oracle to Oracle: salted SHA-256, 64 bits of it
            row, how, digits = f"'{salt}' || {row}", "SHA256", SHA_HEX
        else:
            # to any other engine: the digest every engine sums
            how, digits = "MD5", canon.DIGEST_HEX
        hexes = "X" * digits
        cond = f" where ({where})" if where else ""
        return (f"select count(*), nvl(sum(to_number(substr(rawtohex("
                f"standard_hash({row}, '{how}')), 1, {digits}),"
                f" '{hexes}')), 0), "
                + (" + ".join(f"nvl(sum({o}), 0)" for o in outside)
                   if outside else "0")
                + f" from {self._qualified(side, db, table)}{cond}", [])

    def neutral_digest(self, side, db, table, columns, where=None):
        """The digest every engine sums, over the rendering they share:
        summed in the server where every column renders exactly in
        Oracle's SQL; folded here, large values read in pieces, where one
        does not."""
        return self._digest(side, db, table, columns, where, None)

    def same_engine_digest(self, side, db, table, columns, where=None,
                           salt=None):
        """Oracle against Oracle: a SHA-256 of the run's salt and each
        row's text, its first 64 bits summed exactly - never Oracle's
        CHECKSUM, which folds by exclusive or, so two copies of a row
        cancel. The salt draws again each run the chance of two tables
        summing alike (`checkpoint.new_salt`). Asked of both sides by the
        pair's check (`HeteroEngine._digests`); the digest every other
        path compares with a fold of its own stays `neutral_digest`."""
        return self._digest(side, db, table, columns, where, salt)

    def _digest(self, side, db, table, columns, where, salt):
        got = None
        try:
            got = self._digest_sql(side, db, table, columns, where, salt)
        except Exception:  # noqa: BLE001 - folded here instead
            got = None
        if got is not None:
            rows = self._rows(side, db, got[0], got[1])
            n, total, outside = rows[0]
            if not outside:
                self.__dict__["digested_in_server"] = \
                    self.__dict__.get("digested_in_server", 0) + 1
                return (int(n), str(int(total)))
        from .. import canon
        self.__dict__["digested_here"] = \
            self.__dict__.get("digested_here", 0) + 1
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns,
                                          size=5000, where=where):
            k, total = (self._sha_fold(classes, batch, salt, total) if salt
                        else canon.fold_rows(classes, batch, total))
            n += k
        return (n, str(total))

    # ---- writing -----------------------------------------------------------
    #
    # Each column is bound as the driver can take it (`Bind`): a time as
    # its text with all nine digits, one with a zone as its UTC wall clock
    # made the instant again by the server in the zone it came in; a large
    # object as one, so a bind past the 32 KB a VARCHAR2 takes is not
    # refused.

    def _binds(self, side, db, table, names, facts=None):
        """[Bind] for writing `names` to `table`, in order."""
        facts = {f["name"]: f for f in (facts if facts is not None
                                        else self._facts(side, db, table))}
        return [Bind(n, (facts.get(n) or {}).get("kind", ""))
                for n in names]

    @staticmethod
    def _row(binds, row, staged=False):
        out = []
        for b, v in zip(binds, row):
            out += b.values(v, staged)
        return out

    #: rows a batch holds before a direct path load is weighed against
    #: array DML for it; below this the load's own round trips dominate
    DIRECT_FROM = 2000

    def _write_way(self, db, table, n, binds=(), big=False):
        """"direct" or "array" for a batch of `n` rows of this table: the
        first two large batches are written one each way, timed, and the
        cheaper by the row is kept for the rest of the table.

        Measured on Oracle Free 23.26 (2 CPUs), 100,000 rows of four
        columns into an empty table with a primary key: array DML 0.38 s;
        a direct path load into the session's stage 0.16 s and the MERGE
        from it 0.20 s. Neither wins everywhere - a wide row, a table with
        many indexes, a busy server each move it - so each table is
        measured rather than one way chosen for all."""
        return self._write_choice(db, table, n, binds, big).path

    def _write_choice(self, db, table, n, binds, big):
        """The climb of `WRITE_WAYS` for one batch: the ways this batch
        can go, then the cheaper of them on this table's own costs for
        batches of its size (the engine's `_way_costs`, which the decision
        layer reads back through `measured_ways`).
        `MIGKIT_ORACLE_WRITE` puts one way first, to diagnose."""
        from ..decide import climb, prune
        facts = {"rows": n, "from": self.DIRECT_FROM, "big": big,
                 "stageable": all(b.stageable for b in binds)}
        forced = os.environ.get("MIGKIT_ORACLE_WRITE", "")
        if forced in {r.name for r in WRITE_WAYS}:
            return climb(str(table), sorted(WRITE_WAYS, key=lambda r:
                                            r.name != forced), facts)
        held, _ = prune(WRITE_WAYS, facts)
        if len(held) < 2:
            return climb(str(table), WRITE_WAYS, facts)
        key = self._write_key(db, table, n)
        return climb(str(table), WRITE_WAYS, facts,
                     costs=self._way_costs(key, held, facts=facts))

    def _write_key(self, db, table, n):
        return self._work_key("write", f"{db}.{table}", n)

    def _timed(self, db, table, way, n, write):
        began = time.perf_counter()
        write()
        self._took(self._write_key(db, table, n), way,
                   time.perf_counter() - began, n)

    def neutral_write(self, side, db, table, columns, rows):
        """Rows with the same key replaced in one transaction, which
        converges on a restart as an upsert does. A large batch goes into a
        table private to this session by a direct path load, which commits
        by itself, and from there into the table in migkit's transaction -
        so a batch is still in whole or not at all; or by array DML. Which,
        this table's first large batches measure (`_write_way`).

        Never a direct path load straight into the table: measured, one
        that met a key already there left 1,001 rows of a batch of 1,001 in
        the table and its primary key's index UNUSABLE."""
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        facts = self._facts(side, db, table)
        computed = {f["name"] for f in facts if f["virtual"]}
        if computed & set(names):
            keep = [i for i, n in enumerate(names) if n not in computed]
            names = [names[i] for i in keep]
            rows = [[r[i] for i in keep] for r in rows]
        key = [k for k in self.neutral_key(side, db, table) if k in names]
        binds = self._binds(side, db, table, names, facts)
        big = any(b.lob and v is not None and len(v) > PIECE
                  for r in rows for b, v in zip(binds, r))
        way = self._write_way(db, table, len(rows), binds, big)
        write = (self._staged if way == "direct" else self._arrayed)
        self._timed(db, table, way, len(rows), lambda: write(
            side, db, table, names, key, binds, rows))
        return len(rows)

    def _arrayed(self, side, db, table, names, key, binds, rows):
        """Deleted by key and inserted, each one statement for the batch;
        a value past `PIECE` inserted empty and written in pieces through
        its locator, in the same transaction."""
        quoted = self._qualified(side, db, table)
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            if key:
                at = [names.index(k) for k in key]
                cur.executemany(self._sql(
                    f"delete from {quoted} where "
                    + " and ".join(f"{self._q(k)} = {MARK}" for k in key),
                    True), [[binds[j].values(r[j])[0] for j in at]
                            for r in rows])
            small, large = [], []
            for r in rows:
                (large if any(b.lob and v is not None and len(v) > PIECE
                              for b, v in zip(binds, r)) else small).append(r)
            sizes = [s for b in binds for s in b.sizes()]
            for zones, group in _by_zone(binds, small).items():
                cur = conn.cursor()
                if any(sizes):
                    cur.setinputsizes(*sizes)
                cur.executemany(self._sql(
                    f"insert into {quoted}"
                    f" ({', '.join(self._q(n) for n in names)}) values"
                    f" ({', '.join(_marks(binds, zones))})", True),
                    [self._row(binds, r) for r in group])
            for r in large:
                self._insert_in_pieces(conn, quoted, names, binds, r)
            conn.commit()
        finally:
            conn.close()

    def _insert_in_pieces(self, conn, quoted, names, binds, row):
        """One row whose large values are written a piece at a time: in
        empty, and filled through the locators the insert returns."""
        import oracledb
        cur = conn.cursor()
        places, vals, outs = [], [], []
        for b, v in zip(binds, row):
            if b.lob and v is not None and len(v) > PIECE:
                places.append("empty_blob()" if b.kind == "BLOB"
                              else "empty_clob()")
                outs.append((b, v, cur.var(oracledb.DB_TYPE_BLOB
                                           if b.kind == "BLOB"
                                           else oracledb.DB_TYPE_CLOB)))
            else:
                places.append(b.mark(b.zone(v)))
                vals += b.values(v)
        returning = (" returning " + ", ".join(self._q(b.name)
                                               for b, _, _ in outs)
                     + " into " + ", ".join(MARK for _ in outs))
        cur.execute(self._sql(f"insert into {quoted}"
                              f" ({', '.join(self._q(n) for n in names)})"
                              f" values ({', '.join(places)}){returning}",
                              True), vals + [var for _, _, var in outs])
        for _, data, var in outs:
            lob = var.getvalue()
            lob = lob[0] if isinstance(lob, list) else lob
            chunk = lob.getchunksize() or 8192
            step = max(chunk, (PIECE // chunk) * chunk)
            # a text LOB's offset is in UTF-16 units, not characters
            offset = 1
            for at in range(0, len(data), step):
                piece = data[at:at + step]
                lob.write(piece, offset)
                offset += _units(piece)

    def _stage_name(self, table):
        digest = hashlib.sha256(str(table).encode(), usedforsecurity=False)
        return f"{OWN_PREFIX}S_{digest.hexdigest()[:20].upper()}"

    def _stage(self, conn, side, db, table, binds):
        """The session-private table a load is staged in: a global
        temporary table, made once, whose rows each session sees only its
        own of - ranges copied side by side stage into it at once without
        meeting. A zoned time is held there as its UTC wall clock beside
        its zone."""
        name = self._stage_name(table)
        owner = self._owner(side, db)
        cur = conn.cursor()
        cur.execute("select count(*) from all_tables where owner = :1 and"
                    " table_name = :2", [owner, name])
        if cur.fetchone()[0]:
            return name
        facts = {f["name"]: f for f in self._facts(side, db, table)}
        defs = []
        for b in binds:
            for col, typ in b.staged(facts.get(b.name) or {}):
                defs.append(f"{self._q(col)} {typ}")
        try:
            cur.execute(f"create global temporary table"
                        f" {self._q(owner)}.{self._q(name)}"
                        f" ({', '.join(defs)}) on commit preserve rows")
        except Exception as e:  # noqa: BLE001 - another range made it
            if "ORA-00955" not in str(e):
                raise
        return name

    def _staged(self, side, db, table, names, key, binds, rows):
        """Loaded by direct path into the session's stage, then merged
        into the table - or inserted, where it has no key - and committed:
        the load commits by itself, the table's transaction is migkit's.
        The stage is emptied first as well as after, in case a run before
        this one stopped between the two."""
        conn = self._dedicated(side)
        try:
            stage = self._stage(conn, side, db, table, binds)
            owner = self._owner(side, db)
            staged = f"{self._q(owner)}.{self._q(stage)}"
            cur = conn.cursor()
            cur.execute(f"truncate table {staged}")
            conn.direct_path_load(
                schema_name=owner, table_name=stage,
                column_names=[self._q(c)[1:-1] for b in binds
                              for c, _ in b.staged({})],
                data=[self._row(binds, r, staged=True) for r in rows],
                batch_size=len(rows))
            zones = {i: {b.zone(r[i]) for r in rows}
                     for i, b in enumerate(binds) if b.zoned}
            src = ", ".join(f"{b.promote(self._q, zones.get(i, ()))}"
                            f" {self._q(b.name)}"
                            for i, b in enumerate(binds))
            quoted = self._qualified(side, db, table)
            cols = ", ".join(self._q(n) for n in names)
            if key:
                sets = [n for n in names if n not in key]
                cur.execute(
                    f"merge into {quoted} t using (select {src} from"
                    f" {staged} s) s on ("
                    + " and ".join(f"t.{self._q(k)} = s.{self._q(k)}"
                                   for k in key) + ")"
                    + (" when matched then update set "
                       + ", ".join(f"t.{self._q(n)} = s.{self._q(n)}"
                                   for n in sets) if sets else "")
                    + f" when not matched then insert ({cols}) values ("
                    + ", ".join(f"s.{self._q(n)}" for n in names) + ")")
            else:
                cur.execute(f"insert into {quoted} ({cols}) select {src}"
                            f" from {staged} s")
            conn.commit()
            cur.execute(f"truncate table {staged}")
        finally:
            conn.close()

    def load_window(self, db, log=None, tables=None):
        """The stages a load made are dropped when it ends: they hold no
        rows between batches, but a table on the target the source does
        not have is one a schema check would name."""
        import contextlib

        @contextlib.contextmanager
        def window():
            try:
                yield
            finally:
                self._drop_stages(db)
        return window()

    def _drop_stages(self, db):
        try:
            owner = self._owner("dst", db)
            names = [n for (n,) in self._rows(
                "dst", db, "select table_name from all_tables where owner"
                           f" = {MARK} and table_name like"
                           f" '{OWN_PREFIX}S\\_%' escape '\\'", (owner,))]
        except Exception:  # noqa: BLE001 - nothing to drop, or no target
            return
        for n in names:
            try:
                conn = self._connect("dst", db)
                try:
                    conn.cursor().execute(f"drop table {self._q(owner)}."
                                          f"{self._q(n)} purge")
                finally:
                    conn.close()
            except Exception:  # noqa: BLE001 - a session still holds it
                pass

    # ---- applying a change stream ------------------------------------------
    #
    # A run of rows of one table and one set of columns (`_apply_net`) is
    # one MERGE bound to every row of it, and a run of deletes one DELETE:
    # array DML, a round trip for the run rather than two for each row.

    def _apply_upserts(self, side, db, table, rows):
        if not rows:
            return
        key = sorted(rows[0][0])
        facts = self._facts(side, db, table)
        computed = {f["name"] for f in facts if f["virtual"]}
        names = key + sorted(n for n in {**rows[0][0], **rows[0][1]}
                             if n not in key and n not in computed)
        binds = self._binds(side, db, table, names, facts)
        quoted = self._qualified(side, db, table)
        sets = [n for n in names if n not in key]

        def merge(marks):
            return (f"merge into {quoted} t using (select "
                    + ", ".join(f"{m} {self._q(b.name)}"
                                for m, b in zip(marks, binds))
                    + " from dual) s on ("
                    + " and ".join(f"t.{self._q(k)} = s.{self._q(k)}"
                                   for k in key) + ")"
                    + (" when matched then update set "
                       + ", ".join(f"t.{self._q(n)} = s.{self._q(n)}"
                                   for n in sets) if sets else "")
                    + " when not matched then insert"
                    f" ({', '.join(self._q(n) for n in names)}) values ("
                    + ", ".join(f"s.{self._q(n)}" for n in names) + ")")
        full = [[({**k, **v}).get(n) for n in names] for k, v in rows]
        sizes = [s for b in binds for s in b.sizes()]
        with self._writer(side, db) as conn:
            for zones, group in _by_zone(binds, full).items():
                cur = conn.cursor()
                if any(sizes):
                    cur.setinputsizes(*sizes)
                cur.executemany(self._sql(merge(_marks(binds, zones)), True),
                                [self._row(binds, r) for r in group])

    def _apply_deletes(self, side, db, table, keys):
        from .. import canon
        if not keys:
            return
        names = sorted(keys[0])
        with self._writer(side, db) as conn:
            conn.cursor().executemany(self._sql(
                f"delete from {self._qualified(side, db, table)} where "
                + " and ".join(f"{self._q(k)} = {MARK}" for k in names),
                True), [[canon.sql_value(k[n]) for n in names]
                        for k in keys])

    # ---- code -----------------------------------------------------------

    def neutral_views(self, side, db):
        return [(self._said(n), text) for n, text in self._rows(
            side, db, f"select view_name, text from all_views where owner ="
                      f" {MARK} order by 1", (self._owner(side, db),))
            if not self.hop.excluded(db, self._said(n))]

    def neutral_functions(self, side, db):
        """Functions whose body is `BEGIN RETURN <expression>; END`; every
        other function and procedure is listed with no expression, to be
        named."""
        import sqlglot
        owner = self._owner(side, db)
        src = {}
        for name, kind, text in self._rows(
                side, db, "select name, type, text from all_source where"
                          f" owner = {MARK} and type in ('FUNCTION',"
                          " 'PROCEDURE') order by name, line", (owner,)):
            src.setdefault((name, kind), []).append(text or "")
        out = []
        for (name, kind), lines in sorted(src.items()):
            params, returns = [], None
            for arg, declared, pos in self._rows(
                    side, db, "select argument_name, data_type, position"
                              " from all_arguments where owner ="
                              f" {MARK} and object_name = {MARK} and"
                              " package_name is null order by position",
                    (owner, name)):
                if pos == 0:
                    returns = declared
                else:
                    params.append((self._said(arg or ""), declared))
            body = " ".join("".join(lines).split())
            m = re.search(r"\bbegin\s+return\s+(.*?);\s*end\b[^;]*;?\s*$",
                          body, re.I | re.S)
            one = None
            if kind == "FUNCTION" and m and ";" not in m.group(1):
                try:
                    one = sqlglot.parse_one(m.group(1), read="oracle").sql(
                        "oracle")
                except Exception:  # noqa: BLE001 - named, not carried
                    one = None
            out.append((self._said(name), params, returns, one))
        return out

    def neutral_create_sql(self, side, db, table, columns, key=()):
        """As every engine makes a table, except that a length of text the
        source declared as Oracle is made in characters: the catalogue
        gives the length in characters, and made in bytes it would refuse
        a value of that many characters outside ASCII."""
        from .. import canon
        made = []
        for col in columns:
            numbers = col[2] if len(col) > 2 else ()
            if col[1] == canon.OWN and numbers:
                m = re.fullmatch(r"(VARCHAR2|CHAR)\((\d+)\)",
                                 str(numbers[0]).strip(), re.I)
                if m:
                    numbers = (f"{m.group(1).upper()}({m.group(2)} CHAR)",)
            made.append((col[0], col[1], numbers) + tuple(col[3:]))
        return super().neutral_create_sql(side, db, table, made, key)
    def code_routines(self, side, db):
        """Every standalone function, procedure and trigger in the schema,
        with the statement that makes each (`CREATE OR REPLACE` and the
        source the server keeps). A package's members are not standalone
        and are not listed here."""
        from ..storedcode.routines import Param, Routine
        owner = self._owner(side, db)
        src = {}
        for name, kind, text in self._rows(
                side, db, "select name, type, text from all_source where"
                          f" owner = {MARK} and type in ('FUNCTION',"
                          " 'PROCEDURE', 'TRIGGER') order by name, type,"
                          " line", (owner,)):
            src.setdefault((name, kind), []).append(text or "")
        args = {}
        for name, arg, declared, pos, mode in self._rows(
                side, db, f"select object_name, argument_name, {DECLARED},"
                          " position, in_out from all_arguments where owner"
                          f" = {MARK} and package_name is null and"
                          " data_level = 0 order by object_name, position",
                (owner,)):
            args.setdefault(name, []).append((arg, declared, int(pos),
                                              str(mode)))
        trig = {}
        for name, table, kind, event in self._rows(
                side, db, "select trigger_name, table_name, trigger_type,"
                          " triggering_event from all_triggers where owner ="
                          f" {MARK} and base_object_type = 'TABLE'",
                (owner,)):
            trig[name] = (self._said(table), str(kind), str(event))
        out = []
        for (name, kind), lines in sorted(src.items()):
            text = "".join(lines)
            k = kind.lower()
            r = Routine(kind=k, name=self._said(name), language="plsql",
                        definition="create or replace " + text.strip(),
                        body=text, settings={"owner": owner})
            mine = args.get(name, [])
            r.returns = next((d for a, d, p, _ in mine if p == 0), None) \
                if k == "function" else None
            r.params = [Param(self._said(a or ""), d, {"IN": "in",
                                                        "OUT": "out"}.get(
                                  m, "inout"))
                        for a, d, p, m in mine if p > 0]
            if k == "trigger" and name in trig:
                table, how, event = trig[name]
                r.table = table
                r.timing = ("BEFORE" if how.startswith("BEFORE") else
                            "INSTEAD OF" if how.startswith("INSTEAD")
                            else "AFTER")
                r.level = "row" if "EACH ROW" in how else "statement"
                r.events = tuple(e.strip() for e in event.split(" OR "))
                if self.hop.excluded(db, table):
                    continue
            out.append(r)
        return out

    def table_ddl(self, side, db, table):
        """The table as the server itself writes it (`DBMS_METADATA`),
        without storage, tablespace and the owner's name, for a copy in
        another schema on another Oracle (the proof's sandbox); its
        foreign keys are `foreign_keys`'."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute("begin dbms_metadata.set_transform_param("
                        "dbms_metadata.session_transform, 'STORAGE', false);"
                        " dbms_metadata.set_transform_param("
                        "dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES',"
                        " false); dbms_metadata.set_transform_param("
                        "dbms_metadata.session_transform,"
                        " 'REF_CONSTRAINTS', false); end;")
            cur.execute("select dbms_metadata.get_ddl('TABLE', :1, :2) from"
                        " dual", (self._q(table)[1:-1],
                                  self._owner(side, db)))
            text = str(cur.fetchone()[0])
        finally:
            conn.close()
        owner = self._owner(side, db)
        return text.replace(f'"{owner}".', "").strip()

    def foreign_keys(self, side, db):
        got = {}
        for name, child, col, parent, pcol in self._rows(
                side, db, "select c.constraint_name, c.table_name,"
                          " cc.column_name, p.table_name, pc.column_name"
                          " from all_constraints c join all_cons_columns cc"
                          " on cc.owner = c.owner and cc.constraint_name ="
                          " c.constraint_name join all_constraints p on"
                          " p.owner = c.r_owner and p.constraint_name ="
                          " c.r_constraint_name join all_cons_columns pc on"
                          " pc.owner = p.owner and pc.constraint_name ="
                          " p.constraint_name and pc.position = cc.position"
                          f" where c.owner = {MARK} and c.constraint_type ="
                          " 'R' order by c.constraint_name, cc.position",
                (self._owner(side, db),)):
            one = got.setdefault((name, child, parent), ([], []))
            one[0].append(self._said(col))
            one[1].append(self._said(pcol))
        return [(self._said(c), a, self._said(p), b)
                for (_, c, p), (a, b) in got.items()]

    def neutral_function_sql(self, name, params, returns, body):
        # quoted as the body quotes them; Oracle takes no length or scale
        # on an argument or a result
        args = ", ".join(f'"{p}" {t.split("(")[0]}' for p, t in params)
        return (f"create function {self._q(name)}({args}) return"
                f" {returns.split('(')[0]} is begin return {body}; end;")

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def delta_verify(self, db, limit=20000, log=None):
        """Only the rows the redo names as changed since the last clean
        pass, compared on both sides: LogMiner reads without moving
        anything, so the pair's delta pass can read it beside the tail."""
        return self._as_pair().delta_verify(db, limit, log)

    def check_deep(self, db):
        """Objects a load left invalid, and constraints it left disabled or
        not validated, on the target: Oracle keeps using a table under
        them without a word."""
        out = []
        owner = self._owner("dst", db)
        try:
            # each with the name that decides whether the hop takes it: a
            # trigger's and an index's table, any other object its own
            invalid = self._rows(
                "dst", db, "select o.object_type || ' ' || o.object_name,"
                           " coalesce((select t.table_name from all_triggers"
                           " t where o.object_type = 'TRIGGER' and t.owner ="
                           " o.owner and t.trigger_name = o.object_name),"
                           " (select i.table_name from all_indexes i where"
                           " o.object_type = 'INDEX' and i.owner = o.owner"
                           " and i.index_name = o.object_name),"
                           " o.object_name) from all_objects o where"
                           f" o.owner = {MARK} and o.status = 'INVALID'"
                           " order by 1", (owner,))
            loose = self._rows("dst", db, "select constraint_name || ' on '"
                                          " || table_name || ' (' || status"
                                          " || ', ' || validated || ')',"
                                          " table_name from"
                                          " all_constraints where owner ="
                                          f" {MARK} and (status ="
                                          " 'DISABLED' or validated ="
                                          " 'NOT VALIDATED') order by 1",
                               (owner,))
            invalid = [r for r in invalid
                       if self.in_scope(db, self._said(str(r[1])))]
            loose = [r for r in loose
                     if self.in_scope(db, self._said(str(r[1])))]
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target objects", "error",
                           "could not read the target's objects:"
                           f" {str(e).splitlines()[0][:90]}")]
        out.append(Result("deep", f"{db} target objects",
                          "diff" if invalid else "ok",
                          f"{len(invalid)} invalid:"
                          f" {', '.join(r[0] for r in invalid[:6])}"
                          if invalid else "no invalid object on the target",
                          "", "compile them again, and check what each"
                              " depends on" if invalid else ""))
        out.append(Result("deep", f"{db} target constraints",
                          "diff" if loose else "ok",
                          f"{len(loose)} not enforced:"
                          f" {', '.join(r[0] for r in loose[:6])}"
                          if loose else "every constraint enabled and"
                                        " validated", "",
                          "alter table ... enable validate constraint ..."
                          if loose else "",
                          category="structure.unvalidated-constraints"))
        out.append(self._deep_partitions(db))
        out.append(self._mojibake(db))
        out += self._deep_more(db)
        return out

    # ---- partitions ------------------------------------------------------

    def partition_scheme(self, side, db):
        """{table: {"m": method, "e": the key's columns, "p": {partition:
        its high value}}} of every partitioned table in scope, the
        partitions in their order. A composite table's subpartitions are
        its partitions' rows and read as them. A list's `DEFAULT` and a
        range's `MAXVALUE` are the partitions every other row lands in."""
        owner = self._owner(side, db)
        keys = {}
        for tbl, col in self._rows(
                side, db, "select name, column_name from all_part_key_columns"
                          f" where owner = {MARK} and object_type = 'TABLE'"
                          " order by name, column_position", (owner,)):
            keys.setdefault(str(tbl), []).append(str(col))
        out = {}
        for tbl, how in self._rows(
                side, db, "select table_name, partitioning_type from"
                          f" all_part_tables where owner = {MARK}"
                          " order by 1", (owner,)):
            if self.in_scope(db, self._said(str(tbl))):
                out[str(tbl)] = {"m": str(how),
                                 "e": ", ".join(keys.get(str(tbl), [])),
                                 "p": {}}
        for tbl, name, high in self._rows(
                side, db, "select table_name, partition_name, high_value"
                          f" from all_tab_partitions where table_owner ="
                          f" {MARK} order by table_name, partition_position",
                (owner,)):
            if str(tbl) in out:
                out[str(tbl)]["p"][str(name)] = " ".join(
                    str(high or "").split())
        return out

    #: partitions counted in one statement at most
    PARTITIONS_PER_READ = 100

    def _partition_rows(self, side, db, tbl, parts):
        """{partition: {"rows", "digest", "bound", "catchall"}} of one
        table on one side: each partition's rows counted with `PARTITION
        (p)`. Counted and not digested - the digest is left None, and the
        check says which partitions were compared by count only."""
        names = list(parts)
        out = {}
        for at in range(0, len(names), self.PARTITIONS_PER_READ):
            reads = []
            for i, name in enumerate(names[at:at + self.PARTITIONS_PER_READ],
                                     start=at):
                bound = parts[name] or ""
                out[name] = {"rows": 0, "digest": None, "bound": bound,
                             "catchall": bound.upper() in (
                                 "MAXVALUE", "DEFAULT")}
                reads.append(
                    f"select {i}, count(*) from "
                    f"{self._qualified(side, db, tbl)} partition "
                    f"({self._q(name)})")
            for i, rows in self._rows(side, db, " union all ".join(reads)):
                out[names[int(i)]]["rows"] = int(rows)
        return out

    def _deep_partitions(self, db):
        """Every partitioned table's method and key on both sides, and every
        partition held to its own count (`Engine._partitions_verdict`). A
        partition that arrived empty leaves the table's total plausible."""
        try:
            sp = self.partition_scheme("src", db)
            dp = self.partition_scheme("dst", db)
            plain = {side: {self._said(str(t[0])) for t in self._rows(
                side, db, "select table_name from all_tables where owner ="
                          f" {MARK}", (self._owner(side, db),))}
                     for side in ("src", "dst")}
            scheme, tables = [], {}
            for t, si in sorted(sp.items()):
                di = dp.get(t)
                if not di:
                    scheme.append(
                        f"{t}: partitioned on source, not on target"
                        if self._said(t) in plain["dst"] else
                        f"{t}: missing on target (partitioned by"
                        f" {si['m']} on the source)")
                    continue
                if (si["m"], si["e"]) != (di["m"], di["e"]):
                    scheme.append(f"{t}: partition scheme differs"
                                  f" src({si['m']} {si['e']})"
                                  f" dst({di['m']} {di['e']})")
                tables[t] = (self._partition_rows("src", db, t, si["p"]),
                             self._partition_rows("dst", db, t, di["p"]))
            for t, di in sorted(dp.items()):
                if t not in sp and self._said(t) in plain["src"]:
                    scheme.append(f"{t}: partitioned on target ({di['m']}"
                                  f" {di['e']}), not on source")
            return self._partitions_verdict(db, tables, scheme, "MAXVALUE")
        except Exception as e:  # noqa: BLE001 - said, and the rest runs
            return Result("deep", f"{db} partitions", "error",
                          "could not read the partitions:"
                          f" {str(e).splitlines()[-1][:120]}")

    # ---- settings --------------------------------------------------------

    #: settings that change what a query returns or a value becomes
    CRITICAL = ("nls_characterset", "nls_nchar_characterset",
                "nls_length_semantics", "nls_comp", "nls_sort",
                "nls_date_format", "nls_timestamp_format",
                "nls_timestamp_tz_format", "nls_numeric_characters",
                "nls_territory", "nls_language", "max_string_size",
                "compatible", "optimizer_features_enable",
                "cursor_sharing", "db_block_size", "dbtimezone")

    def _settings(self, side, db):
        """{name: value}: the settings the server was given, and the
        database's own language and character set. An account that cannot
        read the settings view gets the language ones alone, and says so."""
        out = {}
        for n, v in self._rows(side, db, "select lower(parameter), value"
                                         " from nls_database_parameters"):
            out[n] = v
        out["dbtimezone"] = self._rows(side, db,
                                       "select dbtimezone from dual")[0][0]
        try:
            for n, v in self._rows(side, db, "select name, value from"
                                             " v$parameter where isdefault ="
                                             " 'FALSE' or name in"
                                             " ('max_string_size',"
                                             " 'compatible',"
                                             " 'db_block_size',"
                                             " 'cursor_sharing',"
                                             " 'optimizer_features_enable')"):
                out[n] = v
        except Exception as e:  # noqa: BLE001 - half a reading, said
            out[self.UNREADABLE] = ("the account cannot read the server's"
                                    " settings (select on v$parameter):"
                                    f" {str(e).splitlines()[0][:60]}")
        return out

    def check_params(self, db):
        return self._param_result(
            db, self._settings("src", db), self._settings("dst", db),
            self.CRITICAL, "alter system set ... on the target (character"
                           " set and length semantics need the database"
                           " made again)")

    # ---- sequences and identity columns ----------------------------------
    #
    # `last_number` is where a sequence starts its next cache, not the last
    # value it handed out: a cache of 20 reads 20 ahead. What is carried is
    # that next value - the target handing out nothing the source did - and
    # for an identity column, past the largest value in it as well.

    def _sequences(self, side, db):
        """{name: next value} for the schema's sequences the hop takes,
        identity ones by their table and column: an identity column's goes
        with its table, a sequence a column's default draws from with the
        tables drawing from it (left out only where every one of them is),
        any other by its own name - and only where the hop carries
        sequences (`Engine.takes`). Every sequence was read before: the
        parity check and its repair set the counters of tables the hop
        left out, which the target writes itself."""
        owner = self._owner(side, db)
        idents = {s: (self._said(t), self._said(c))
                  for t, c, s in self._rows(
                      side, db, "select table_name, column_name,"
                                " sequence_name from all_tab_identity_cols"
                                f" where owner = {MARK}", (owner,))}
        drawn = self._sequence_users(side, db, owner)
        out = {}
        for name, last in self._rows(
                side, db, "select sequence_name, last_number from"
                          f" all_sequences where sequence_owner = {MARK}",
                (owner,)):
            said = self._said(name)
            if name in idents:
                t, c = idents[name]
                if self.takes(db, "sequence", said, t):
                    out[f"{t}.{c}"] = int(last)
                continue
            users = drawn.get(said.upper()) or []
            taken = (any(self.takes(db, "sequence", said, t) for t in users)
                     if users else self.takes(db, "sequence", said))
            if taken:
                out[said] = int(last)
        return out

    #: `SEQ.NEXTVAL`, `"OWNER"."SEQ".NEXTVAL` in a column's default: the
    #: sequence's own name
    _NEXTVAL = re.compile(r'"?([A-Za-z0-9_$#]+)"?\s*\.\s*"?nextval', re.I)

    def _sequence_users(self, side, db, owner):
        """{SEQUENCE: [table]} of the tables whose column defaults draw
        from each sequence; {} where the defaults cannot be read."""
        out = {}
        try:
            rows = self._rows(side, db, "select table_name, data_default"
                                        " from all_tab_columns where owner ="
                                        f" {MARK} and data_default is not"
                                        " null", (owner,))
        except Exception:  # noqa: BLE001 - each sequence by its own name
            return out
        for t, default in rows:
            for m in self._NEXTVAL.finditer(str(default or "")):
                out.setdefault(m.group(1).upper(), []).append(self._said(t))
        return out

    def _identity_next(self, db):
        """[(table, column, sequence, the next value it hands out)] of every
        identity column on the target."""
        return self._rows(
            "dst", db, "select i.table_name, i.column_name,"
                       " i.sequence_name, q.last_number from"
                       " all_tab_identity_cols i join all_sequences q"
                       " on q.sequence_owner = i.owner and"
                       " q.sequence_name = i.sequence_name where"
                       f" i.owner = {MARK}", (self._owner("dst", db),))

    def sequences_behind(self, db):
        """[(sequence, table, column, the column's largest value)] for each
        identity column on the target whose sequence would hand out a value
        the column already holds: the change tail writes each row with the
        source's key, and a sequence does not move for a key it did not
        hand out. Asked of the target alone."""
        owner = self._owner("dst", db)
        out = []
        for t, c, s, last in self._identity_next(db):
            if not self.in_scope(db, self._said(t)):
                continue
            top = self._rows("dst", db, f"select max({self._q(c)}) from"
                                        f" {self._q(owner)}.{self._q(t)}")
            top = top[0][0] if top else None
            if top is not None and int(top) >= int(last):
                out.append((s, self._said(t), self._said(c), int(top)))
        return out

    def _raise_past(self, behind):
        """The statements that move each identity column's sequence past
        its column's largest value."""
        return [f'alter table "{t.upper() if t == t.lower() else t}" modify'
                f' "{c.upper() if c == c.lower() else c}" generated by'
                " default as identity (start with limit value)"
                for _, t, c, _ in behind]

    def check_autoinc(self, db):
        """usable = would an identity column hand out a value a row holds;
        parity = every sequence is at least where the source's is."""
        try:
            src = self._sequences("src", db)
            dst = self._sequences("dst", db)
            behind = self.sequences_behind(db)
        except Exception as e:  # noqa: BLE001 - said
            return [Result("autoinc", db, "error", "could not read the"
                           f" sequences: {str(e).splitlines()[0][:90]}")]
        res = []
        if behind:
            res.append(Result(
                "autoinc", f"{db} usable", "diff",
                "an identity column would hand out a value a row holds: "
                + "; ".join(f"{t}.{c} largest {top}"
                            for _, t, c, top in behind[:6]), "",
                f"migkit sync {self.hop.name} --db {db} --kind sequences"))
        else:
            res.append(Result("autoinc", f"{db} usable", "ok",
                              "every identity column is past its rows"))
        short = [f"{n} source next {v} target"
                 f" {dst.get(n, 'missing')}"
                 for n, v in sorted(src.items())
                 if dst.get(n) is None or dst[n] < v]
        if short:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(short[:8]), "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind sequences"))
        else:
            res.append(Result("autoinc", f"{db} parity", "ok",
                              f"{len(src)} sequences at or past the"
                              " source's"))
        return res

    def _restart_sql(self, db, name, value):
        """A sequence, or an identity column's, set to hand out `value`
        next."""
        owner = self._owner("dst", db)
        if "." in name:
            t, c = name.split(".", 1)
            return (f"alter table {self._q(owner)}.{self._q(t)} modify"
                    f" {self._q(c)} generated by default as identity"
                    f" (start with {int(value)})")
        return (f"alter sequence {self._q(owner)}.{self._q(name)} restart"
                f" start with {int(value)}")

    def repair_plan(self, db, kind):
        actions = []
        if kind in ("sequences", "all"):
            src = self._sequences("src", db)
            dst = self._sequences("dst", db)
            stmts = [self._restart_sql(db, n, v) for n, v in sorted(
                src.items()) if n in dst and dst[n] < v]
            undo = [self._restart_sql(db, n, dst[n]) for n, v in sorted(
                src.items()) if n in dst and dst[n] < v]
            behind = self.sequences_behind(db)
            stmts += self._raise_past(behind)
            # each column raised past its rows put back to the next value
            # it handed out before, as each sequence moved is
            was = {(self._said(t), self._said(c)): int(last)
                   for t, c, _, last in self._identity_next(db)}
            undo += [self._restart_sql(db, f"{t}.{c}", was[(t, c)])
                     for _, t, c, _ in behind]
            if stmts:
                actions.append(RepairAction(
                    db, "sequences", stmts, undo,
                    f"{len(stmts)} sequences moved to where the source's"
                    " next value is, or past their column's rows"))
        if kind in ("rows", "all"):
            actions += self._as_pair().repair_plan(db, "rows")
            # after the rows: a row copied again takes the source's text
            text = self._mojibake_repair(db)
            if text:
                actions.append(text)
        return actions

    def apply(self, db, action):
        if action.kind == "text":
            return self._apply_text(db, action)
        if action.kind != "sequences":
            return self._as_pair().apply(db, action)
        self._target_only("dst", "set sequences")
        owner = self._owner("dst", db)
        conn = self._connect("dst", db)
        try:
            cur = conn.cursor()
            cur.execute(f"alter session set current_schema ="
                        f" {self._q(owner)}")
            try:
                for s in action.statements:
                    cur.execute(s.rstrip(";"))
            finally:
                # the session goes back to the pool as it came
                cur.execute("alter session set current_schema ="
                            f" {self._q(self._endpoint('dst').user)}")
        finally:
            conn.close()

    # ---- the target's restore point --------------------------------------

    def snapshot_state(self, db, state_dir, kind="all"):
        """What the target held before a repair: the SCN it was at - every
        table can be put back as of it while the undo reaches, by the
        statements left in `dst-flashback.sql` - each sequence's next
        value, which a rollback puts back, and unless only those are
        repaired, the definitions of its views, code and triggers as the
        server keeps them. A guaranteed restore point would outlast the
        undo, and is refused to every account but the container's own
        (measured, ORA-01031 in a pluggable database)."""
        owner = self._owner("dst", db)
        # a table is put back as of an SCN only where it could move its
        # rows before that SCN (measured: enabled after, the flashback
        # is refused, ORA-01466) - so first, and said in the state
        fixed = [t for (t,) in self._rows(
            "dst", db, "select table_name from all_tables where owner ="
                       f" {MARK} and row_movement = 'DISABLED' and"
                       f" temporary = 'N' and table_name not like"
                       f" '{OWN_PREFIX}%' escape '\\'", (owner,))]
        conn = self._connect("dst", db)
        try:
            for t in fixed:
                conn.cursor().execute(f"alter table {self._q(owner)}."
                                      f"{self._q(t)} enable row movement")
        finally:
            conn.close()
        (state_dir / "dst-row-movement.txt").write_text(
            "".join(f"{t}\n" for t in fixed))
        scn = [[self.scn_after_ddl("dst", db)]]
        (state_dir / "dst-scn.txt").write_text(f"{scn[0][0]}\n")
        (state_dir / "dst-oracle-sequences.txt").write_text("".join(
            f"{n}|{v}\n" for n, v in sorted(self._sequences("dst",
                                                            db).items())))
        undo = [f"flashback table {self._q(owner)}.{self._q(t)} to scn"
                f" {scn[0][0]};" for t in self.neutral_tables("dst", db)]
        (state_dir / "dst-flashback.sql").write_text(
            f"-- each table as it was at SCN {scn[0][0]}, while the undo"
            " reaches it\n" + "\n".join(undo) + "\n")
        if kind == "sequences":
            return
        defs = []
        for name, what in self._rows(
                "dst", db, "select object_name, object_type from"
                           f" all_objects where owner = {MARK} and"
                           " object_type in ('VIEW', 'FUNCTION',"
                           " 'PROCEDURE', 'PACKAGE', 'PACKAGE BODY',"
                           " 'TRIGGER', 'TYPE') order by 2, 1", (owner,)):
            try:
                got = self._rows("dst", db, "select dbms_metadata.get_ddl("
                                            f"{MARK}, {MARK}, {MARK}) from"
                                            " dual",
                                 (what.replace(" ", "_"), name, owner))
                text = got[0][0] if got else None
                defs.append(f"-- {name} ({what.lower()})\n{text}\n/\n")
            except Exception:  # noqa: BLE001 - one the account cannot see
                defs.append(f"-- {name} ({what.lower()}): not readable\n")
        (state_dir / "dst-schema.sql").write_text("".join(defs))

    #: seconds a table's last DDL must be behind an SCN for the table to
    #: be read, or put back, as of it
    DDL_SETTLES = 6

    def scn_after_ddl(self, side, db):
        """The SCN now, taken once every table of the schema has had
        `DDL_SETTLES` seconds since its last DDL. The server holds a DDL's
        time only to its clock's granularity, so an SCN taken in the
        seconds after one is refused - measured on 23.26: `flashback table`
        to an SCN read a second after `enable row movement`, and a count
        as of an SCN read a second after CREATE TABLE, both ORA-01466."""
        got = self._rows(side, db, "select nvl(max((cast(systimestamp as"
                                   " date) - last_ddl_time) * -86400), -1e9)"
                                   " from all_objects where owner ="
                                   f" {MARK} and object_type = 'TABLE'",
                         (self._owner(side, db),))
        wait = float(got[0][0]) + self.DDL_SETTLES if got else 0
        if wait > 0:
            time.sleep(min(wait, self.DDL_SETTLES))
        return int(self._rows(side, db, "select dbms_flashback."
                                        "get_system_change_number from"
                                        " dual")[0][0])

    def restore_sequences(self, db, state_dir):
        """The statements that put each sequence back where the snapshot
        found it."""
        path = state_dir / "dst-oracle-sequences.txt"
        if not path.exists():
            return []
        out = []
        for line in path.read_text().splitlines():
            if line:
                name, value = line.rsplit("|", 1)
                out.append(self._restart_sql(db, name, int(value)) + ";")
        return out

    # ---- the change feed ------------------------------------------------
    #
    # The redo, read by migkit's own LogMiner reader - or, where the
    # operator runs the redo-file reader next to the redo, by that - as
    # `canon.change` records in commit order, a transaction whole. The
    # token is SCN-positioned: where to mine from, the last commit handed
    # back, and how far every commit has been. See `oracle_logminer`.

    def _reader(self, side, db):
        from . import oracle_logminer, oracle_redo
        held = self.__dict__.setdefault("_readers", {})
        want = oracle_redo.chosen(self, side, db)
        got = held.get((side, db))
        if got is None or got.RUNG != want:
            if got is not None:
                got.close()
            got = held[(side, db)] = (
                oracle_redo.RedoFileReader(self, side, db)
                if want == oracle_redo.RUNG
                else oracle_logminer.Miner(self, side, db))
        return got

    def change_point(self, side, db):
        from . import oracle_logminer
        return oracle_logminer.change_point(self, side, db)

    def neutral_changes(self, side, db, token=None, limit=1000):
        if token is None:
            return [], self.change_point(side, db)
        return self._reader(side, db).read(token, limit, self._ddl_of)

    def release_changes(self):
        """The readers' sessions let go, once the tail is done with them
        (a table read again from where it left, beside the stream)."""
        for reader in list(self.__dict__.get("_readers", {}).values()):
            reader.close()
        self.__dict__["_readers"] = {}

    #: the redo is read again from any position it still holds, by a
    #: reader of its own that moves nothing of the tail's: a table that
    #: left the tail and comes back gets its changes since from there
    REREADS_FROM_A_POSITION = True

    def _ddl_of(self, db, sql, commit):
        """[canon.ddl] for a statement the redo carries as text, one for
        each table of the tail's schema it touches and the hop does not
        leave alone, at the SCN it committed at. A statement naming
        another schema's table is not this tail's."""
        from .. import canon, logddl
        owner = self._owner("src", db)
        out = []
        for d, t, kind, to in logddl.oracle(sql, owner):
            if d != owner:
                continue
            if to is not None and to[0] != owner:
                # renamed into another schema: gone from this one
                kind, to = "drop", None
            name = self._said(t)
            went = self._said(to[1]) if to is not None else None
            if self.hop.excluded(db, name) and not (
                    went is not None and not self.hop.excluded(db, went)):
                continue
            out.append(canon.ddl(name, kind, sql=str(sql)[:4000], to=went,
                                 txn=int(commit)))
        return out

    def _marked_on(self, side, db):
        """The database a mark belongs to - its id, its incarnation and
        its container - asked once: an SCN means something in one
        database only."""
        from . import oracle_logminer
        known = self.__dict__.setdefault("_marks_on", {})
        if (side, db) not in known:
            try:
                got = oracle_logminer.identity(self, side, db)
                known[(side, db)] = (
                    f"{got['database']}/{got['incarnation']}/"
                    f"{got['container']}" if got else None)
            except Exception:  # noqa: BLE001 - no mark, then
                known[(side, db)] = None
        return known[(side, db)]

    def snapshot_mark(self, side, db):
        """The SCN now, after the database it was read in: every
        transaction that committed at or below it is visible to a read
        begun after (a read sees the database as of the SCN it begins at).
        None where the database cannot be told from another."""
        from . import oracle_logminer
        on = self._marked_on(side, db)
        if on is None:
            return None
        try:
            return f"{on}|{oracle_logminer.current_scn(self, side, db)}"
        except Exception:  # noqa: BLE001 - no mark, then
            return None

    def mark_covers(self, side, db, mark, change):
        """Whether the SCN the change committed at (`txn`) is at or below
        the mark's, in the same database."""
        version = change.get("txn")
        if not isinstance(mark, str) or not isinstance(version, int) \
                or isinstance(version, bool):
            return None
        on, _, at = mark.rpartition("|")
        if not on or on != self._marked_on(side, db):
            return None
        try:
            return version <= int(at)
        except ValueError:
            return None

    def log_position(self, side, db):
        from . import oracle_logminer
        return oracle_logminer.current_scn(self, side, db)

    @staticmethod
    def position_reached(have, want):
        """Whether a tail at `have` has handed back every commit up to the
        SCN `want`."""
        from . import oracle_logminer
        return oracle_logminer.reached(have, want)

    def stream_identity(self, side, db):
        from . import oracle_logminer
        return oracle_logminer.identity(self, side, db)

    def side_database(self, side, db):
        """The database a side's connections go to for `db`, for the
        interlock: the service - a pluggable database of its own - and
        the schema in it. Two pluggable databases of one container are
        reached at one address, where the schema's name alone would take
        the target for the source."""
        service = self._dsn(self._endpoint(side)).rsplit("/", 1)[1]
        return f"{service}/{self._owner(side, db)}"

    def server_identity(self, side, db):
        """For the interlock on the target: the database, its incarnation
        and container (`stream_identity` - a standby or a clone shares the
        database's id, never its instance's start), when the instance
        answering started, its version, the host it runs on, and the bytes
        the schema holds - or that the schema is not there yet. What the
        account may not read is left out."""
        out = {}
        for key, sql in (
                ("running", "select to_char(startup_time, 'YYYY-MM-DD"
                            " HH24:MI:SS') from v$instance"),
                ("version", "select version_full from"
                            " product_component_version where rownum = 1"),
                ("name", "select sys_context('userenv', 'server_host')"
                         " from dual")):
            try:
                got = self._rows(side, db, sql)
            except Exception:  # noqa: BLE001 - not every account may ask
                continue
            if got and got[0][0] is not None:
                out[key] = str(got[0][0])
        if db:
            owner = self._owner(side, db)
            there = self._rows(side, db, "select count(*) from all_users"
                                         " where username = :1", [owner])
            if not there[0][0]:
                out["absent"] = True
            else:
                me = self._rows(side, db, "select user from dual")[0][0]
                sql = ("select nvl(sum(bytes), 0) from user_segments"
                       if me == owner else
                       "select nvl(sum(bytes), 0) from dba_segments where"
                       " owner = :1")
                try:
                    size = self._rows(side, db, sql,
                                      None if me == owner else [owner])
                    out["bytes"] = int(size[0][0])
                except Exception:  # noqa: BLE001 - the size is shown only
                    pass
        try:
            got = self.stream_identity(side, db)
        except Exception:  # noqa: BLE001 - v$database may not be granted
            got = None
        if got:
            out["id"] = got
        return out

    def position_lost(self, side, db, token):
        from . import oracle_logminer
        return oracle_logminer.lost(self, side, db, token)

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_seed(self, db, token_path, point):
        return self._as_pair().tail_seed(db, token_path, point)

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def change_reader_check(self, db):
        """(level, detail, measured) of the reader that would follow this
        source against the redo it writes, or None where the redo-file
        reader, run beside the redo, is the one chosen (it does not mine
        through the database, so the LogMiner measure is not its). The
        gate of a move or a tail reads this; the measure is
        `oracle_logminer.reader_check`."""
        from . import oracle_logminer, oracle_redo
        if oracle_redo.chosen(self, "src", db) == oracle_redo.RUNG:
            return None
        return oracle_logminer.reader_check(self, "src", db)

    def _assess_extra(self):
        from . import oracle_logminer
        items = []
        for db in self.databases():
            items += oracle_logminer.assess(self, "src", db)
        # what the move will not do, counted and named
        inv = self._handwork()
        return items + inv.rows() + inv.summary()

    # ---- Oracle to Oracle, server to server ------------------------------

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table copied by the target itself: Data Pump pulls it over a
        database link to the source, as of one SCN for every table, where
        the target has Data Pump and may make the link; otherwise the
        table copier carries it, writing by direct path loads."""
        from . import oracle_pump
        return oracle_pump.bulk(self, db, tables, go, log, shape_only)
