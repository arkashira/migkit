"""Teradata as one side of a pair (backlog ER3), through Teradata's own
driver for Python. Its licence is Teradata's (free to use, not open
source), so it is put in place by `migkit doctor --install` once its terms
are accepted (`tools.TERMS`), and a Teradata side stops with that command
where it is not.

A hop's database is a Teradata database (or user). Tables, columns, keys
and indexes come from the DBC views (`DBC.TablesV`, `DBC.ColumnsV`,
`DBC.IndicesV`); a table's key is its primary key, or its unique primary
index where every column of it is NOT NULL. Every session runs in ANSI
mode: a value too long for its column is refused there, where TERA mode
cuts it without a word, and a table migkit makes is MULTISET with
CASESPECIFIC text.

**Read.** Rows are spread over the AMPs by the hash of the primary index,
so a read in key order sorts the whole of the rest at every page. A table
is read in one pass, or in the buckets of its rows' hash
(`row_bucket_sql`, `HASHBUCKET(HASHROW(...))`), each resumable - and a
read of many rows goes through FastExport, the driver's parallel sessions
(`{fn teradata_try_fastexport}`), where the table holds enough rows for
them to pay (the driver's own guidance: 100,000) and the user may read
`DBC.SessionInfoV`, which it needs. A FastExport that cannot start is
switched from, and said (`decide.attempt`).

**Write.** A batch of the copy lands whole: where it holds enough rows,
by FastLoad into an empty table of its own - its errors and warnings read
after each batch and after the commit, and its rows counted, since
FastLoad drops a duplicate row without failing - then merged into the
target by key in one transaction; otherwise deleted by key and inserted in
one transaction. The way that did a row in less time is kept, each tried
once (`decide`). A change is applied by its key; a tail's batch is one
transaction with its line in `migkit_origin`, so a stopped tail goes on
after the last batch committed (`EXACT_BATCHES`).

**Types.** PERIOD is carried as the text `[begin, end)`, Teradata's own
closed-open meaning, and written back through a CAST; INTERVAL as its
months, days and time kept apart (`intervals`); TIME and TIMESTAMP WITH
TIME ZONE as the instant; NUMBER without a precision by its value; JSON as
JSON; BYTE and VARBYTE as bytes, a NULL of them bound as VARBYTE so the
database takes it.

**Compared.** Two Teradata systems compare on the servers: each row's
`HASHROW` of the table's salted values - its text as its bytes
(`CHAR2HEXINT`, so case and trailing blanks count, which the hash of text
would pass over) and a mark of which values are NULL (`HASHROW` takes
NULL, 0 and '' alike) - taken into `HASHBUCKET` under four salts and
summed on each system, with the count (`same_engine_digest`). A table
whose sums differ is read leaf by leaf and row by row by the pair; one
that holds what `HASHROW` cannot take (a LOB, JSON, XML) is folded in
this process. Across engines every table is read and folded by migkit's
own renderer; Teradata has no SHA-256 of its own.

**Deep checks.** The two systems' tables are held to each other in DBC's
views and in the tables (`check_deep`): keys, columns, secondary indexes,
character sets, case rules, identity columns, narrower columns, NULL against
empty, FLOAT sums, timestamps shifted as a whole, the objects beside the
tables the hop takes, triggers, foreign keys, partitions, the primary index
that spreads a table over the AMPs, rights and owners. A load disables the
target's triggers on the tables it fills and enables them again, its way
back saved to disk first (`load_window`); a repair is the pair's, each row it
overwrites kept to be put back (`repair_plan`).

Teradata keeps no change log a client can read, so a Teradata source moves
by copies, each proved by check. Written 2026-10-01 without a Teradata
system to run it against - no image of one runs in a container, and
ClearScape is an account migkit may not use here - so what is held is each
statement's shape and what is made of the rows it returns
(`tests/test_teradata_as_a_side_of_a_pair.py`); a Vantage Express VM on
x86 (`MIGKIT_TERADATA_HOST`) is where it is proved on a real system
(`tests/test_teradata_against_a_server.py`).
"""
import calendar
import datetime
import re
import time
from decimal import Decimal

from .. import pieces
from .base import Engine, RepairAction, Result, clauses
from .dbapi import MARK, DbapiRows

#: the port a Teradata system listens on
PORT = 1025
#: rows a batch or a table holds before FastLoad's and FastExport's own
#: sessions pay for themselves: the driver's documented guidance
BULK_ROWS = 100_000
#: the tables migkit keeps in a target database, left out of every listing
#: of the application's tables
ORIGIN = "migkit_origin"
STAGE = "migkit_stage_"

#: the driver's request-scope escapes migkit prepends
TRY_FASTEXPORT = "{fn teradata_try_fastexport}"
REQUIRE_FASTLOAD = "{fn teradata_require_fastload}"
NATIVE = "{fn teradata_nativesql}"
GET_ERRORS = "{fn teradata_get_errors}"
GET_WARNINGS = "{fn teradata_get_warnings}"
DB_VERSION = "{fn teradata_database_version}"
REMOTE = "{fn teradata_provide(remote_address)}"

#: `DBC.ColumnsV.ColumnType`, as each type is declared
CODES = {
    "I1": "BYTEINT", "I2": "SMALLINT", "I": "INTEGER", "I8": "BIGINT",
    "D": "DECIMAL", "N": "NUMBER", "F": "FLOAT",
    "CF": "CHAR", "CV": "VARCHAR", "CO": "CLOB",
    "BF": "BYTE", "BV": "VARBYTE", "BO": "BLOB",
    "DA": "DATE", "AT": "TIME", "TS": "TIMESTAMP",
    "TZ": "TIME WITH TIME ZONE", "SZ": "TIMESTAMP WITH TIME ZONE",
    "YR": "INTERVAL YEAR", "YM": "INTERVAL YEAR TO MONTH",
    "MO": "INTERVAL MONTH", "DY": "INTERVAL DAY",
    "DH": "INTERVAL DAY TO HOUR", "DM": "INTERVAL DAY TO MINUTE",
    "DS": "INTERVAL DAY TO SECOND", "HR": "INTERVAL HOUR",
    "HM": "INTERVAL HOUR TO MINUTE", "HS": "INTERVAL HOUR TO SECOND",
    "MI": "INTERVAL MINUTE", "MS": "INTERVAL MINUTE TO SECOND",
    "SC": "INTERVAL SECOND",
    "PD": "PERIOD(DATE)", "PT": "PERIOD(TIME)",
    "PZ": "PERIOD(TIME WITH TIME ZONE)", "PS": "PERIOD(TIMESTAMP)",
    "PM": "PERIOD(TIMESTAMP WITH TIME ZONE)",
    "JN": "JSON", "XM": "XML",
}
#: `DBC.ColumnsV.CharType`
CHARSETS = {1: "LATIN", 2: "UNICODE", 3: "KANJISJIS", 4: "GRAPHIC",
            5: "KANJI1"}
#: types neither FastLoad nor FastExport carries, nor `HASHROW` takes
LOBS = ("clob", "blob", "json", "xml")

COLUMNS_SQL = ("select ColumnName, ColumnType, ColumnLength,"
               " DecimalTotalDigits, DecimalFractionalDigits, CharType,"
               " UpperCaseFlag, Nullable, DefaultValue, IdColType"
               " from DBC.ColumnsV where DatabaseName = ? and TableName = ?"
               " order by ColumnId")


def declared_of(code, length, digits, frac, chartype, upper):
    """A column's declared type from its `DBC.ColumnsV` row, with the
    numbers a counterpart needs; None for a type migkit has no name for
    (a UDT, an ARRAY, a VECTOR)."""
    code = str(code or "").strip()
    name = CODES.get(code)
    if name is None:
        return None
    length = int(length or 0)
    digits = None if digits is None else int(digits)
    frac = None if frac is None else int(frac)
    if code == "D":
        return f"DECIMAL({digits},{frac or 0})"
    if code == "N":
        # a NUMBER of no precision is a floating decimal of up to 38
        # digits: DBC says -128 for the precision it does not have
        if digits is None or digits < 0:
            return "NUMBER"
        return f"NUMBER({digits},{max(frac or 0, 0)})"
    if code in ("CF", "CV"):
        cs = CHARSETS.get(int(chartype or 1), "LATIN")
        width = length // 2 if cs in ("UNICODE", "GRAPHIC") else length
        case = {"N": " NOT CASESPECIFIC", "U": " UPPERCASE"}.get(
            str(upper or "").strip(), "")
        return f"{name}({width}) CHARACTER SET {cs}{case}"
    if code in ("BF", "BV"):
        return f"{name}({length})"
    if code in ("AT", "TS", "TZ", "SZ"):
        head, _, tail = name.partition(" ")
        return f"{head}({frac if frac is not None else 6})" + (
            f" {tail}" if tail else "")
    if code in ("PT", "PZ", "PS", "PM"):
        inner = name[len("PERIOD("):-1]
        head, _, tail = inner.partition(" ")
        return f"PERIOD({head}({frac if frac is not None else 6})" + (
            f" {tail})" if tail else ")")
    if name.startswith("INTERVAL"):
        words = name.split()[1:]
        lead = digits if digits and digits > 0 else 2
        if words == ["SECOND"]:
            return f"INTERVAL SECOND({lead},{frac if frac is not None else 6})"
        out = f"INTERVAL {words[0]}({lead})"
        if len(words) == 3:
            out += f" TO {words[2]}"
            if words[2] == "SECOND":
                out += f"({frac if frac is not None else 6})"
        return out
    if code == "JN":
        return f"JSON({length})" if length else "JSON"
    return name


def _base(declared):
    return str(declared or "").split("(")[0].strip().lower()


def _kind(declared):
    """The declared type's first word: `varchar`, `interval`, `udt`."""
    return (_base(declared).split() or [""])[0]


def _squeeze(text):
    """A declared type or a default as one comparison reads it: upper
    case, one space between words."""
    return " ".join(str(text or "").upper().split())


def plain_declared(declared):
    """A declared type without its character set and case rule
    (`VARCHAR(100) CHARACTER SET UNICODE` as `VARCHAR(100)`): what the
    capacity tables (`canon.capacity`) read a length or a precision off."""
    return re.sub(r"\s+(?:CHARACTER SET \w+|NOT CASESPECIFIC|CASESPECIFIC"
                  r"|UPPERCASE)", "", str(declared or ""),
                  flags=re.I).strip()


def _charset_of(declared):
    got = re.search(r"CHARACTER SET (\w+)", str(declared).upper())
    return got.group(1) if got else None


def _case_rule(declared):
    """How a text column compares: `CASESPECIFIC`, `NOT CASESPECIFIC` or
    `UPPERCASE` (`declared_of` writes the two it knows; none is the
    first)."""
    low = str(declared).upper()
    if "NOT CASESPECIFIC" in low:
        return "NOT CASESPECIFIC"
    return "UPPERCASE" if "UPPERCASE" in low else "CASESPECIFIC"


def _epoch(value):
    """A timestamp as seconds since the epoch: an instant as it is, a wall
    clock as the same digits read in UTC (both sides read alike, so a
    faithful copy differs by none)."""
    if value.tzinfo is not None:
        return value.timestamp()
    return calendar.timegm(value.timetuple()) + value.microsecond / 1e6


#: `DBC.TablesV.TableKind` of the objects beside a table, with the kind of
#: the hop's `objects` that takes each and what it is called
OBJECT_KINDS = {
    "V": ("view", "view"), "M": ("procedure", "macro"),
    "P": ("procedure", "stored procedure"),
    "E": ("procedure", "external stored procedure"),
    "F": ("function", "function"), "A": ("function", "aggregate function"),
    "R": ("function", "table function"),
    "B": ("function", "aggregate and ordered analytic function"),
    "S": ("function", "ordered analytic function"),
    "G": ("trigger", "trigger"),
    "I": ("materialized view", "join index"),
    "N": ("materialized view", "hash index")}


def interval_kind(declared):
    """("DAY", "SECOND") for `INTERVAL DAY(4) TO SECOND(6)`; the second
    is the first where the interval has one field."""
    words = re.sub(r"\([^)]*\)", " ", str(declared)).upper().split()[1:]
    return words[0], words[-1]


_FIELDS = ("YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND")


def parse_interval(declared, text):
    """An `intervals.Interval` from the text Teradata gives an INTERVAL
    of this declared type (`  12-03`, `-5 04:05:06.500000`): one sign for
    the whole value; months apart from days and time."""
    from .. import intervals
    if text is None:
        return None
    if isinstance(text, intervals.Interval):
        return text
    first, last = interval_kind(declared)
    s = str(text).strip()
    sign = -1 if s.startswith("-") else 1
    s = s.lstrip("+-").strip()
    names = list(_FIELDS[_FIELDS.index(first):_FIELDS.index(last) + 1])
    parts = re.split(r"[ :-]", s) if first in ("YEAR", "MONTH") else \
        re.split(r"[ :]", s)
    if len(parts) != len(names):
        raise ValueError(f"not an INTERVAL {first} TO {last}: {text!r}")
    got = dict(zip(names, parts))
    months = int(got.get("YEAR", 0)) * 12 + int(got.get("MONTH", 0))
    days = int(got.get("DAY", 0))
    nanos = (int(got.get("HOUR", 0)) * 3600 + int(got.get("MINUTE", 0))
             * 60) * intervals.SECOND
    if "SECOND" in got:
        nanos += intervals._whole(got["SECOND"], intervals.SECOND)
    return intervals.Interval(sign * months, sign * days, sign * nanos)


def interval_text(declared, value):
    """An interval as Teradata reads one of this declared type, or a
    SystemExit where it holds a part the type has no room for - a month
    in a day-to-second interval, a day in a year-to-month one."""
    from .. import intervals
    got = intervals.of(value)
    first, last = interval_kind(declared)
    names = _FIELDS[_FIELDS.index(first):_FIELDS.index(last) + 1]
    sign = "-" if (got.months < 0 or got.days < 0 or got.nanos < 0) else ""
    months, days, nanos = abs(got.months), abs(got.days), abs(got.nanos)
    if first in ("YEAR", "MONTH"):
        if days or nanos:
            raise SystemExit(f"{intervals.text(got)} holds days or time,"
                             f" which a Teradata {declared} has no room for")
        if "YEAR" in names and "MONTH" in names:
            return f"{sign}{months // 12}-{months % 12:02d}"
        if names == ("YEAR",):
            if months % 12:
                raise SystemExit(f"{intervals.text(got)} is not whole years,"
                                 f" which a Teradata {declared} holds")
            return f"{sign}{months // 12}"
        return f"{sign}{months}"
    if months:
        raise SystemExit(f"{intervals.text(got)} holds months, which a"
                         f" Teradata {declared} has no room for")
    if "SECOND" not in names and nanos % intervals.SECOND:
        raise SystemExit(f"{intervals.text(got)} holds a fraction of a"
                         f" second, which a Teradata {declared} has no room"
                         " for")
    total = days * 86400 * intervals.SECOND + nanos
    secs, frac = divmod(total, intervals.SECOND)
    units = {"DAY": secs // 86400, "HOUR": secs // 3600,
             "MINUTE": secs // 60, "SECOND": secs}
    # each field below the first is what is left of the one above it
    out, rest = [], None
    for i, name in enumerate(names):
        if i == 0:
            val = units[name]
            rest = secs - val * {"DAY": 86400, "HOUR": 3600, "MINUTE": 60,
                                 "SECOND": 1}[name]
        else:
            size = {"HOUR": 3600, "MINUTE": 60, "SECOND": 1}[name]
            val, rest = divmod(rest, size)
        out.append(val)
    if rest:
        raise SystemExit(f"{intervals.text(got)} is finer than a Teradata"
                         f" {declared} holds")
    text = ""
    for i, (name, val) in enumerate(zip(names, out)):
        sep = "" if i == 0 else (" " if names[i - 1] == "DAY" else ":")
        text += f"{sep}{val if i == 0 else f'{val:02d}'}"
    if "SECOND" in names and frac:
        text += "." + f"{frac:09d}".rstrip("0")
    return sign + text


_PERIOD = re.compile(r"^\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)$")


def period_text(value):
    """A PERIOD as the text migkit carries it: `[begin, end)`, the begin
    in and the end out, as Teradata means it. Text in another form is
    kept as it is: both sides of a pair read it through this."""
    if value is None:
        return None
    m = _PERIOD.match(str(value).strip())
    return f"[{m.group(1)}, {m.group(2)})" if m else str(value)


def period_literal(text):
    """`[begin, end)` back into the character form Teradata's CAST to a
    PERIOD reads: `('begin', 'end')`."""
    m = re.match(r"^\[\s*([^,]+?)\s*,\s*([^)]+?)\s*\)$", str(text).strip())
    if not m:
        return str(text)
    return f"('{m.group(1)}', '{m.group(2)}')"


_VIEW_HEAD = re.compile(r"^\s*(?:create|replace)\s+(?:recursive\s+)?view\s+"
                        r".*?\bas\b\s*", re.I | re.S)


def _view_select(text):
    """The select a view is defined as: the text after the `AS` of its
    `CREATE VIEW` (or `REPLACE VIEW`), as the catalogue keeps it; the whole
    text where it does not begin that way."""
    got = _VIEW_HEAD.match(str(text))
    return str(text)[got.end():].rstrip(" \r\n\t;") if got else str(text)


#: the file stem of the record of triggers a load disabled (`setaside`)
TRIGGERS_ASIDE = "disabled-triggers"


class _TriggerWindow:
    """A load's window on a Teradata target: the enabled triggers on the
    tables it fills disabled, and enabled again on the way out.

    The statements that enable them are saved to disk before any is
    disabled (`setaside`), so a process killed in between leaves a record
    the next load enables from, and `check` names until then
    (`TeradataEngine.set_aside`). Where the target's triggers cannot be
    read, or the record cannot be written, nothing is disabled and the load
    does not begin: a trigger that fires for every row written could
    rewrite them, and it is not for a load to find that out afterwards."""

    def __init__(self, eng, db, log, tables):
        from ..setaside import SetAside
        self.eng, self.db, self.log = eng, db, log
        self.tables = (None if tables is None else
                       {str(t).rpartition(".")[2].lower() for t in tables})
        self.record = SetAside(eng.hop, db, TRIGGERS_ASIDE)
        self.off = {}

    def _say(self, text):
        if self.log:
            self.log(text)

    def __enter__(self):
        eng, db = self.eng, self.db
        try:
            rows = eng._rows(
                "dst", db, "select DatabaseName, TriggerName, TableName,"
                           " EnabledFlag from DBC.TriggersV where"
                           f" SubjectTableDataBaseName = {MARK}",
                (eng._d("dst", db),))
        except Exception as e:  # noqa: BLE001 - nothing may be loaded blind
            raise SystemExit(
                f"{db}: the target's triggers could not be read ("
                f"{(str(e).strip().splitlines() or [type(e).__name__])[0][:80]}"
                "), and a trigger that fires for every row the load writes"
                " could rewrite them. Nothing has been loaded.") from e
        there = {f"{str(d).strip()}.{str(t).strip()}"
                 for d, t, _, _ in rows}
        mine = {f"{str(d).strip()}.{str(t).strip()}": (str(d).strip(),
                                                         str(t).strip())
                for d, t, on, flag in rows
                if str(flag).strip().upper() == "Y"
                and eng.in_scope(db, str(on).strip())
                and (self.tables is None
                     or str(on).strip().lower() in self.tables)}
        left, stale = self.record.left_behind()
        adopted = {n: s for n, s in left.items() if n in there}
        if not mine and not adopted:
            for path in stale:
                path.unlink(missing_ok=True)
            return self
        defs = {n: eng._enable_trigger(d, t) for n, (d, t) in mine.items()}
        if not self.record.save({**adopted, **defs}, stale):
            raise SystemExit(
                f"{db}: the statements that enable the target's triggers"
                " again could not be saved, so none was disabled - and"
                f" {len(mine)} would fire for every row the load writes."
                " Nothing has been loaded.")
        self.off = dict(adopted)
        try:
            for n, (d, t) in mine.items():
                eng.neutral_create_code(
                    "dst", db, f"alter trigger {eng._q(d)}.{eng._q(t)}"
                               " disabled")
                self.off[n] = defs[n]
        except BaseException:
            self._put_back()
            raise
        self._say(f"{len(self.off)} triggers disabled on the target for the"
                  f" load; the statements that enable them are in"
                  f" {self.record.path}")
        return self

    def _put_back(self):
        failed = []
        for n, statement in sorted(self.off.items()):
            try:
                self.eng.neutral_create_code("dst", self.db, statement)
            except Exception as e:  # noqa: BLE001 - kept in the record
                failed.append(n)
                self._say(f"could not enable trigger {n}:"
                          f" {(str(e).strip().splitlines() or [''])[0][:80]}")
        self.off = {n: self.off[n] for n in failed}
        if not failed:
            self.record.done()

    def __exit__(self, *exc):
        self._put_back()
        return False


def _utc_time(value):
    """A TIME WITH TIME ZONE as the time it is in UTC: two values of one
    clock in two zones are two instants, which a rendering of the clock
    alone would take for one."""
    if value is None or getattr(value, "tzinfo", None) is None:
        return value
    at = datetime.datetime.combine(datetime.date(2000, 1, 1), value)
    return at.astimezone(datetime.timezone.utc).time().replace(tzinfo=None)


def _is_driver_error(e):
    return (type(e).__module__ or "").split(".")[0] == "teradatasql"


class TeradataEngine(DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "teradata"
    ENGINE_FAMILY = "teradata"
    CHARSETS = True
    #: what `assess` asks before anything moves (`Engine._preflight_items`):
    #: rows the target's columns have no room for, a time that means an
    #: instant on one side and a wall clock on the other, and text stored
    #: twice as UTF-8
    PREFLIGHT = ("_capacity_gaps", "_temporal_meaning", "_mojibake")
    #: large values (`pieces`), sized where they lie: a byte value's length
    #: as the server keeps it (`BYTES`), a character value's by
    #: `OCTET_LENGTH`, a JSON or XML document's as the character large
    #: object it casts to. They are read and written whole by the driver
    #: (`lob_support`) in batches cut by bytes, never in pieces and never
    #: hashed where they
    #: lie: whether a LOB may sit under a CASE expression, and whether a
    #: piece of one may be appended to a column, is not known without a
    #: system, so neither is attempted
    LARGE_VALUES = pieces.Sql(
        "sized by the server, read and written whole in batches cut by bytes",
        length={"blob": "bytes({c})", "byte": "bytes({c})",
                "varbyte": "bytes({c})", "clob": "octet_length({c})",
                "char": "octet_length({c})", "varchar": "octet_length({c})",
                "json": "octet_length(cast({c} as clob))",
                "xml": "octet_length(cast({c} as clob))"},
        types=("blob", "clob", "byte", "varbyte", "char", "varchar", "json",
               "xml"))
    #: the SQL a hop's row filter is written and read in
    #: (`HeteroEngine._row_scope_of`); without it a filter stops the hop
    SQL_DIALECT = "teradata"
    FOLDED_BECAUSE = ("has no SHA-256 of its own to render a row with, so"
                      " its rows are read - in its own parallel sessions"
                      " where a table is large - and folded by migkit's"
                      " own renderer")
    OVER_NETWORK = True
    PARAMSTYLE = "qmark"
    LIMIT = "top"
    PADDED = ("CHAR",)
    SCALED = ("DECIMAL", "NUMBER")
    #: rows lie by the hash of the primary index across the AMPs, not in
    #: key order: a read in key order sorts the rest at every page, so a
    #: table is read in one pass or by its rows' hash buckets
    RESUMES_BY_KEY = False
    #: a tail's batch is one transaction with its line in `migkit_origin`
    EXACT_BATCHES = True

    # ---- reaching it -----------------------------------------------------

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _params(self, side):
        """The driver's connection parameters: ANSI mode, so a value too
        long for its column is refused rather than cut; the data encrypted
        whatever TLS is taken; TLS as the endpoint says (`sslmode`, its
        authority in `sslca`); the database the hop names first; and the
        other side's session given its own time to answer."""
        ep = self._ep(side)
        o = ep.options or {}
        params = {"host": str(ep.host), "user": str(ep.user or ""),
                  "password": str(ep.password or ""),
                  "dbs_port": str(ep.port or PORT), "tmode": "ANSI",
                  "encryptdata": "true", "connect_timeout": "15000",
                  "teradata_values": "true", "lob_support": "true"}
        if o.get("sslmode"):
            params["sslmode"] = str(o["sslmode"]).upper()
        if o.get("sslrootcert"):
            params["sslca"] = str(o["sslrootcert"])
        if o.get("logmech"):
            params["logmech"] = str(o["logmech"]).upper()
        if self.hop.databases:
            params["database"] = str(self._d(side, self.hop.databases[0]))
        return params

    def _connect(self, side, db):
        try:
            import teradatasql
        except ImportError:
            raise SystemExit(
                f"the {'source' if side == 'src' else 'target'} is Teradata,"
                " which is reached through Teradata's own driver, and it is"
                " not installed on this machine: migkit doctor --install"
                " puts it in place once its terms are accepted") from None
        return teradatasql.connect(**self._params(side))

    def _writer_conn(self, side, db):
        """A connection whose transaction the caller commits: ANSI mode
        with auto-commit on commits each request by itself."""
        conn = self._connect(side, db)
        conn.autocommit = False
        return conn

    def _qualified(self, side, db, table):
        return f"{self._q(self._d(side, db))}.{self._q(table)}"

    def _create_name(self, side, db, table):
        return self._qualified(side, db, str(table).split(".")[-1])

    def _select(self, cols, source, cond, order, limit):
        """TOP n without parentheses, as Teradata writes it."""
        if limit:
            return f"select top {int(limit)} {cols} from {source}{cond}{order}"
        return f"select {cols} from {source}{cond}{order}"

    def databases(self):
        return list(self.hop.databases)

    # ---- the catalogue -----------------------------------------------------

    @staticmethod
    def _own(name):
        low = str(name).strip().lower()
        return low == ORIGIN or low.startswith(STAGE)

    def neutral_tables(self, side, db):
        return [t for t in (str(n).strip() for (n,) in self._rows(
            side, db, "select TableName from DBC.TablesV where DatabaseName"
                      f" = {MARK} and TableKind in ('T', 'O') order by 1",
            (self._d(side, db),)))
            if not self._own(t) and self.in_scope(db, t)]

    def _catalog(self, side, db, table):
        """The table's `DBC.ColumnsV` rows, asked once a table."""
        known = self.__dict__.setdefault("_catalogs", {})
        at = (side, db, str(table))
        if at not in known:
            got = self._rows(side, db, COLUMNS_SQL.replace("?", MARK),
                             (self._d(side, db), str(table)))
            if not got:
                return got
            known[at] = got
        return known[at]

    def neutral_columns(self, side, db, table):
        out = []
        for (name, code, length, digits, frac, chartype, upper, _, _,
             _) in self._catalog(side, db, table):
            declared = declared_of(code, length, digits, frac, chartype,
                                   upper)
            out.append((str(name).strip(), declared or
                        f"UDT {str(code).strip()}"))
        return out

    def neutral_column_rules(self, side, db, table):
        out = {}
        for (name, _, _, _, _, _, _, nullable, default,
             idcol) in self._catalog(side, db, table):
            out[str(name).strip()] = {
                "null": str(nullable or "Y").strip() != "N",
                "default": (str(default).strip() or None)
                if default is not None else None,
                "identity": str(idcol or "").strip() in ("GA", "GD")}
        return out

    def neutral_key(self, side, db, table):
        """The primary key; where there is none, the unique primary index
        where every column of it is NOT NULL - a legacy table's key in all
        but name. A key column that takes NULL would lose that row from
        every read by key, so such an index is not taken for one."""
        got = self._rows(
            side, db, "select IndexType, UniqueFlag, IndexNumber,"
                      " ColumnName, ColumnPosition from DBC.IndicesV where"
                      f" DatabaseName = {MARK} and TableName = {MARK} and"
                      " IndexType in ('K', 'P', 'Q') order by IndexNumber,"
                      " ColumnPosition", (self._d(side, db), str(table)))
        pk = [str(r[3]).strip() for r in got if str(r[0]).strip() == "K"]
        if pk:
            return pk
        upi = [str(r[3]).strip() for r in got
               if str(r[0]).strip() in ("P", "Q")
               and str(r[1]).strip() == "Y"]
        rules = self.neutral_column_rules(side, db, table)
        if upi and all(rules.get(c, {}).get("null") is False for c in upi):
            return upi
        return []

    def neutral_indexes(self, side, db, table):
        """Secondary indexes, unique or not; a join or hash index is its
        own object, and not carried."""
        out = {}
        for idx, name, unique, col in self._rows(
                side, db, "select IndexNumber, IndexName, UniqueFlag,"
                          " ColumnName from DBC.IndicesV where DatabaseName"
                          f" = {MARK} and TableName = {MARK} and IndexType"
                          " in ('S', 'U', 'V') order by IndexNumber,"
                          " ColumnPosition", (self._d(side, db),
                                              str(table))):
            got = out.setdefault(int(idx), [
                str(name).strip() if name else f"{table}_ix{int(idx)}",
                str(unique).strip() == "Y", []])
            got[2].append(str(col).strip())
        return [(n, u, c, True) for n, u, c in out.values()]

    def neutral_create_index_sql(self, side, db, table, name, unique,
                                 columns):
        return (f"create {'unique ' if unique else ''}index {self._q(name)}"
                f" ({', '.join(self._q(c) for c in columns)}) on"
                f" {self._qualified(side, db, table)}")

    def execute_ddl(self, side, db, sql):
        self._target_only(side, "change a schema")
        self.neutral_create_code(side, db, sql)

    def table_facts(self, side, db):
        """Bytes from the space each table takes on every AMP; rows from
        its statistics where they were collected - None where they were
        not, never a guess."""
        out = {}
        name = self._d(side, db)
        for t, size in self._rows(
                side, db, "select TableName, sum(CurrentPerm) from"
                          f" DBC.TableSizeV where DataBaseName = {MARK}"
                          " group by 1", (name,)):
            out[str(t).strip()] = {"rows": None, "bytes": int(size or 0),
                                   "key": None}
        try:
            for t, rows in self._rows(
                    side, db, "select TableName, max(RowCount) from"
                              f" DBC.TableStatsV where DatabaseName = {MARK}"
                              " group by 1", (name,)):
                if str(t).strip() in out and rows is not None:
                    out[str(t).strip()]["rows"] = int(rows)
        except Exception:  # noqa: BLE001 - no statistics: rows unknown
            pass
        return {t: v for t, v in out.items() if not self._own(t)}

    def _declared(self, side, db, table):
        return dict(self.neutral_columns(side, db, table))

    def _scalar(self, side, db, sql):
        """The first row of a statement, or None where it answers none -
        what the base's count of rows a target has no room for asks."""
        got = self._rows(side, db, sql)
        return got[0] if got else None

    def _capacity_probe(self, col, cap):
        """(the rows that do not fit, the largest value) for a capacity, in
        Teradata's SQL: a text length in characters, a byte value's in
        bytes, an integer's range, a decimal's digits left of the point."""
        kind = cap[0]
        if kind == "chars":
            return (f"character_length({col}) > {int(cap[1])}",
                    f"max(character_length({col}))")
        if kind == "bytes":
            return f"bytes({col}) > {int(cap[1])}", f"max(bytes({col}))"
        if kind == "int":
            return (f"({col} < {int(cap[1])} or {col} > {int(cap[2])})",
                    f"max(abs({col}))")
        if kind == "numeric":
            room = 10 ** max(int(cap[1]) - int(cap[2]), 0)
            return f"abs({col}) >= {room}", f"max(abs({col}))"
        raise ValueError(f"no capacity probe for kind {kind!r}")

    # ---- reading ------------------------------------------------------------

    def _read_as(self, quoted, declared):
        """A PERIOD as `[begin, end)`, an INTERVAL as its three parts, a
        TIME WITH TIME ZONE as the time in UTC: the driver hands the first
        two over as Teradata's text and the third with its zone."""
        base = _base(declared)
        if base == "period":
            return quoted, period_text
        if base.startswith("interval"):
            return quoted, (lambda t, d=declared: parse_interval(d, t))
        if base == "time" and "zone" in str(declared).lower():
            return quoted, _utc_time
        return None

    def _kref(self, side, db, table, name):
        """A key column as it is ordered and compared in a read: text as
        CASESPECIFIC on both, so a page ordered one way is not resumed the
        other (a NOT CASESPECIFIC column orders without case, and ANSI
        mode's parameters compare with it)."""
        declared = self._declared(side, db, table).get(name, "")
        if _base(declared) in ("char", "varchar"):
            return f"{self._q(name)} (CASESPECIFIC)"
        return self._q(name)

    def _read_query(self, side, db, table, columns, after, limit, where):
        from .. import canon
        names = [n for n, _ in columns]
        key = self.neutral_key(side, db, table)
        refs = [self._kref(side, db, table, k) for k in key]
        resume, args = "", []
        if key and after is not None:
            ors = []
            for i in range(len(key)):
                ands = [f"{refs[j]} = {MARK}" for j in range(i)]
                ands.append(f"{refs[i]} > {MARK}")
                ors.append("(" + " and ".join(ands) + ")")
                args += [canon.sql_value(v) for v in after[:i + 1]]
            resume = "(" + " or ".join(ors) + ")"
        parts = [p for p in (resume, f"({where})" if where else "") if p]
        cond = (" where " + " and ".join(parts)) if parts else ""
        order = (" order by " + ", ".join(refs)) if key and limit else ""
        wide = self._wide_times(side, db, table)
        return (self._select(", ".join(wide.get(n, self._q(n))
                                       for n in names),
                             self._qualified(side, db, table), cond, order,
                             limit if key else None),
                args, key, names)

    def _read_ways(self):
        """How a table's rows are read in one pass: in the driver's
        parallel sessions, or by one statement."""
        from .. import decide
        return (
            decide.Rung(
                "fastexport", "a read in the driver's parallel sessions",
                needs=(decide.Need(
                    "enough rows", lambda f: (f["rows"] or 0) >= BULK_ROWS,
                    "{rung} opens sessions of its own, which pay for"
                    " themselves only from 100,000 rows"),
                    decide.Need("no large objects",
                                lambda f: not f["lobs"],
                                "{rung} carries no LOB, JSON or XML"),
                    decide.Need("the session list readable",
                                lambda f: f["sessions"],
                                "{rung} reads DBC.SessionInfoV for its job,"
                                " which this user may not")),
                footprint=1),
            decide.Rung("select", "one statement's rows"))

    def _sessions_readable(self, side, db):
        """Whether this user may read `DBC.SessionInfoV`, which FastLoad
        and FastExport need; asked once a side."""
        known = self.__dict__.setdefault("_session_list", {})
        if side not in known:
            try:
                self._rows(side, db, "select top 1 SessionNo from"
                                     " DBC.SessionInfoV")
                known[side] = True
            except Exception:  # noqa: BLE001 - not readable: no bulk ways
                known[side] = False
        return known[side]

    def _rows_about(self, side, db, table):
        facts = self.__dict__.setdefault("_facts_of", {})
        if (side, db) not in facts:
            facts[(side, db)] = self.table_facts(side, db)
        got = facts[(side, db)].get(str(table)) or {}
        rows = got.get("rows")
        if rows is None and got.get("bytes"):
            # no statistics: the space it takes, at a generous 1 KB a row
            rows = int(got["bytes"]) // 1024
        return rows

    def _lobs(self, side, db, table, names=None):
        return [n for n, t in self.neutral_columns(side, db, table)
                if (names is None or n in names) and _kind(t) in LOBS]

    def _export(self, side, db, table, sql, args, names):
        """(connection, cursor) with `sql` executed: in FastExport's
        sessions where they pay, else as one statement - a FastExport that
        cannot start is switched from, and said (`decide.attempt`). Only
        the start switches: rows already handed on cannot be read twice."""
        from .. import decide
        facts = decide.Facts(
            {"rows": self._rows_about(side, db, table),
             "lobs": bool(self._lobs(side, db, table, set(names)))},
            sessions=lambda: self._sessions_readable(side, db))

        def run(choice):
            conn = self._connect(side, db)
            cur = conn.cursor()
            try:
                self._run(cur, self._fastexport_sql(sql)
                          if choice.rung.name == "fastexport" else sql, args)
            except Exception as e:
                conn.close()
                if choice.rung.name == "fastexport" and _is_driver_error(e):
                    raise decide.Blocked(str(e).strip().splitlines()[0][:160]
                                         if str(e).strip()
                                         else type(e).__name__) from e
                raise
            return conn, cur
        return self._attempt(f"reading {table}", self._read_ways(), run,
                             facts)[1]

    @staticmethod
    def _fastexport_sql(sql):
        return TRY_FASTEXPORT + sql

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        sql, args, key, names = self._read_query(side, db, table, columns,
                                                 None, None, where)
        conn, cur = self._export(side, db, table, sql, args, names)
        plan = self._read_plan(side, db, table, columns, key)
        try:
            # in batches of `size` rows or `pieces.BATCH_BYTES`, whichever
            # a batch reaches first: a table of wide rows never has many of
            # them held at once
            for rows in pieces.batches(cur, size, plan.size):
                yield self._whole_times(side, db, table, names,
                                        plan.finish([list(r) for r in rows]))
        finally:
            conn.close()

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded here, as every engine with no rendering of its own is,
        over the rows read in no order - a sum needs none, and FastExport
        is quickest without an ORDER BY."""
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns,
                                          size=5000, where=where):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        return (n, str(total))

    # ---- writing ---------------------------------------------------------------

    def _slot(self, i, declared):
        """(the placeholder for a value of this declared type, the escape
        its parameter needs): a PERIOD or an INTERVAL through a CAST from
        its text, a BYTE or VARBYTE bound as VARBYTE - a NULL is otherwise
        sent as VARCHAR, which the database will not turn into bytes."""
        base = _base(declared)
        if base == "period" or base.startswith("interval"):
            return f"cast({MARK} as {declared})", ""
        if base in ("byte", "varbyte", "blob"):
            n = (re.findall(r"\d+", str(declared)) or ["64000"])[0]
            typ = "BLOB" if base == "blob" else f"VARBYTE({n})"
            return MARK, f"{{fn teradata_parameter({i},{typ})}}"
        return MARK, ""

    def _bind(self, declared, cls, value):
        from .. import canon
        base = _base(declared)
        if value is None:
            return None
        if base == "period":
            return period_literal(value)
        if base.startswith("interval"):
            return interval_text(declared, value)
        return canon.write_value(self.CANON_ENGINE, cls, value)

    def _insert_text(self, side, db, table, names, into=None):
        """The insert of `names`, its parameters as MARK, each through its
        slot (`_slot`)."""
        declared = self._declared(side, db, table)
        slots, escapes = [], ""
        for i, n in enumerate(names, 1):
            slot, esc = self._slot(i, declared.get(n, ""))
            slots.append(slot)
            escapes += esc
        return (f"{escapes}insert into"
                f" {into or self._qualified(side, db, table)}"
                f" ({', '.join(self._q(n) for n in names)}) values"
                f" ({', '.join(slots)})")

    def _insert_sql(self, side, db, table, names, into=None):
        return self._sql(self._insert_text(side, db, table, names, into),
                         True)

    def _values(self, side, db, table, columns, rows):
        declared = self._declared(side, db, table)
        return [tuple(self._bind(declared.get(n, ""), c, v)
                      for (n, c), v in zip(columns, r)) for r in rows]

    def _write_ways(self):
        from .. import decide
        return (
            decide.Rung(
                "fastload", "a bulk load into an empty table of its own,"
                            " merged in by key in one transaction",
                needs=(decide.Need(
                    "enough rows", lambda f: f["rows"] >= BULK_ROWS,
                    "{rung} opens sessions of its own, which pay for"
                    " themselves only from 100,000 rows"),
                    decide.Need("types it carries",
                                lambda f: not f["odd"],
                                lambda f: f"{', '.join(f['odd'][:3])} hold"
                                          " types a bulk load does not"
                                          " carry"),
                    decide.Need("the session list readable",
                                lambda f: f["sessions"],
                                "{rung} reads DBC.SessionInfoV for its job,"
                                " which this user may not")),
                footprint=1),
            decide.Rung("statements", "deletes by key and inserts in one"
                                      " transaction"))

    def neutral_write(self, side, db, table, columns, rows):
        """The batch lands whole: bulk-loaded into a table of its own and
        merged in, or deleted by key and inserted - each in one
        transaction, so a batch written again after a restart replaces
        itself. The way that did a row in less time is kept for batches of
        this size (`_cheaper`); a bulk load that cannot run is switched
        from, and said."""
        from .. import decide
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        declared = self._declared(side, db, table)
        odd = [n for n in names if _kind(declared.get(n, ""))
               in LOBS + ("period",) or _kind(declared.get(n, ""))
               .startswith("interval")]
        key = self._work_key("write", str(table), len(rows))
        facts = decide.Facts({"rows": len(rows), "odd": odd},
                             sessions=lambda: self._sessions_readable(side,
                                                                      db))
        costs = self.__dict__.setdefault("_way_cost", {}).setdefault(
            key, decide.Costs())
        self.__dict__.setdefault("_way_rungs", {})[key] = self._write_ways()

        def run(choice):
            began = time.monotonic()
            if choice.rung.name == "fastload":
                self._fastload(side, db, table, columns, rows)
            else:
                self._statements(side, db, table, columns, rows)
            self._took(key, choice.rung.name, time.monotonic() - began,
                       len(rows))
        self._attempt(f"writing a batch into {table}", self._write_ways(),
                      run, facts, costs=costs)
        return len(rows)

    def _statements(self, side, db, table, columns, rows):
        from .. import canon
        names = [n for n, _ in columns]
        quoted = self._qualified(side, db, table)
        key = self.neutral_key(side, db, table)
        conn = self._writer_conn(side, db)
        try:
            cur = conn.cursor()
            if key and all(k in names for k in key):
                at = [names.index(k) for k in key]
                one = "(" + " and ".join(f"{self._q(k)} = {MARK}"
                                         for k in key) + ")"
                for i in range(0, len(rows), self.DELETE_BATCH):
                    part = rows[i:i + self.DELETE_BATCH]
                    self._run(cur, f"delete from {quoted} where "
                                   + " or ".join([one] * len(part)),
                              [canon.write_value(self.CANON_ENGINE,
                                                 columns[j][1], r[j])
                               for r in part for j in at])
            cur.executemany(self._insert_sql(side, db, table, names),
                            self._values(side, db, table, columns, rows))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _stage_name(self, table, rows):
        import hashlib
        h = hashlib.sha256(repr((str(table), len(rows), rows[:1],
                                 rows[-1:])).encode(),
                           usedforsecurity=False).hexdigest()[:12]
        leaf = re.sub(r"[^A-Za-z0-9_]", "_", str(table).split(".")[-1])
        return f"{STAGE}{leaf[:80]}_{h}"

    def _said_back(self, cur, what, insert):
        """What the driver says of the bulk load `insert` ran - its errors
        or its warnings - or '' where it says nothing."""
        cur.execute(NATIVE + what + insert)
        got = cur.fetchone()
        return str(got[0] or "").strip() if got else ""

    def _fastload(self, side, db, table, columns, rows):
        """The batch bulk-loaded into an empty table of its own, its errors,
        warnings and rows held to what was sent - a duplicate row it drops
        without failing - then merged into the target by key and dropped.
        Anything short of every row landing blocks this way (`Blocked`),
        its table dropped, and the batch goes by statements instead."""
        from .. import decide
        names = [n for n, _ in columns]
        target = self._qualified(side, db, table)
        stage_name = self._stage_name(table, rows)
        stage = self._qualified(side, db, stage_name)
        key = self.neutral_key(side, db, table)
        conn = self._connect(side, db)
        made, merging = False, False
        try:
            cur = conn.cursor()
            cur.execute(f"create multiset table {stage} as (select * from"
                        f" {target}) with no data no primary index")
            made = True
            conn.autocommit = False
            insert = REQUIRE_FASTLOAD + self._insert_sql(
                side, db, table, names, into=stage)
            cur.executemany(insert, self._values(side, db, table, columns,
                                                 rows))
            bad = self._said_back(cur, GET_ERRORS, insert) or \
                self._said_back(cur, GET_WARNINGS, insert)
            if bad:
                conn.rollback()
                raise decide.Blocked(f"the bulk load refused rows: {bad[:160]}")
            conn.commit()
            bad = self._said_back(cur, GET_ERRORS, insert) or \
                self._said_back(cur, GET_WARNINGS, insert)
            cur.execute(f"select count(*) from {stage}")
            landed = int(cur.fetchone()[0])
            if bad or landed != len(rows):
                raise decide.Blocked(
                    f"the bulk load kept {landed:,} of {len(rows):,} rows"
                    + (f": {bad[:160]}" if bad else ""))
            cols = ", ".join(self._q(n) for n in names)
            keyed = bool(key) and all(k in names for k in key)
            merging = True
            if keyed:
                on = " and ".join(f"{target}.{self._q(k)} = s.{self._q(k)}"
                                  for k in key)
                cur.execute(f"delete from {target} where exists (select 1"
                            f" from {stage} s where {on})")
            cur.execute(f"insert into {target} ({cols}) select {cols} from"
                        f" {stage}")
            conn.commit()
        except decide.Blocked:
            raise
        except Exception as e:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 - the connection may be gone
                pass
            # a table with no key whose merge may have committed is not
            # written again another way: its rows would land twice
            if _is_driver_error(e) and not (merging and not keyed):
                raise decide.Blocked(str(e).strip().splitlines()[0][:160]
                                     if str(e).strip()
                                     else type(e).__name__) from e
            raise
        finally:
            if made:
                try:
                    conn.autocommit = True
                    conn.cursor().execute(f"drop table {stage}")
                except Exception:  # noqa: BLE001 - check_deep names it
                    pass
            conn.close()

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._run(cur, f"delete from {self._qualified(side, db, table)}"
                           + (f" where ({where})" if where else ""))
            gone = cur.rowcount
        finally:
            conn.close()
        return int(gone) if gone is not None and gone >= 0 else 0

    def neutral_create_code(self, side, db, statement):
        """DDL with auto-commit on: in ANSI mode it must end a transaction,
        and here it is one of its own."""
        self._target_only(side, "create objects")
        conn = self._connect(side, db)
        try:
            self._run(conn.cursor(), statement)
        finally:
            conn.close()

    def neutral_create_sql(self, side, db, table, columns, key=()):
        """MULTISET, as ANSI mode makes a table; text CASESPECIFIC, so two
        keys that differ in case are two rows; the key as the primary key
        (a unique primary index, NOT NULL), and a table with none spread by
        no index at all."""
        from .. import canon
        defs = []
        for col in columns:
            typ = canon.ddl_type(self.CANON_ENGINE, col[1], col[2],
                                 col[0] in key)
            tail = self._column_tail(col[3] if len(col) > 3 else None,
                                     self.CANON_ENGINE)
            if col[0] in key and "not null" not in tail:
                tail += " not null"
            defs.append(f"{self._q(col[0])} {typ}{tail}")
        if key:
            defs.append("primary key (" + ", ".join(self._q(k) for k in key)
                        + ")")
        return (f"create multiset table {self._create_name(side, db, table)}"
                f" ({', '.join(defs)})"
                + ("" if key else " no primary index"))

    def run_rule(self, side, db, sql):
        """The operator's SQL, in a transaction that is always rolled
        back."""
        conn = self._writer_conn(side, db)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return [list(r) for r in cur.fetchall()]
        finally:
            conn.rollback()
            conn.close()

    # ---- applying a change stream ---------------------------------------------

    def _open_writer(self, side, db):
        self._target_only(side, "apply changes")
        return self._writer_conn(side, db)

    def _upsert_one(self, cur, side, db, table, key, values):
        """Updated by its key, inserted where no row was: PERIOD, INTERVAL
        and bytes through their slots (`_slot`)."""
        from .. import canon
        declared = self._declared(side, db, table)
        quoted = self._qualified(side, db, table)
        where, kargs = self._where_key(key)
        sets = [n for n in values if n not in key]

        def bound(n, v):
            return self._bind(declared.get(n, ""), None, v) \
                if _base(declared.get(n, "")) == "period" or _base(
                    declared.get(n, "")).startswith("interval") \
                else canon.sql_value(v)
        if sets:
            slots, esc = [], ""
            for i, n in enumerate(sets, 1):
                slot, e = self._slot(i, declared.get(n, ""))
                slots.append(f"{self._q(n)} = {slot}")
                esc += e
            self._run(cur, f"{esc}update {quoted} set " + ", ".join(slots)
                           + f" where {where}",
                      [bound(n, values[n]) for n in sets] + kargs)
            if cur.rowcount is not None and cur.rowcount > 0:
                return
        self._run(cur, f"select count(*) from {quoted} where {where}", kargs)
        if cur.fetchone()[0]:
            return
        row = {**key, **values}
        names = list(row)
        self._run(cur, self._insert_text(side, db, table, names),
                  [bound(n, row[n]) for n in names])

    def _origin_table(self, side, db):
        """`migkit_origin` in the target database, made once, with
        auto-commit on - DDL ends a transaction in ANSI mode."""
        import threading
        lock = self.__dict__.setdefault("_origin_lock", threading.Lock())
        with lock:
            if self.__dict__.get("_origin_made"):
                return
            if ORIGIN not in {str(t).strip().lower() for (t,) in self._rows(
                    side, db, "select TableName from DBC.TablesV where"
                              f" DatabaseName = {MARK}",
                    (self._d(side, db),))}:
                self.neutral_create_code(
                    side, db, "create multiset table"
                              f" {self._qualified(side, db, ORIGIN)} (origin"
                              " varchar(512) character set unicode"
                              " casespecific not null, n bigint not null"
                              " default 0, seen varchar(32000) character set"
                              " unicode) unique primary index (origin)")
            self.__dict__["_origin_made"] = True

    def origin_mark(self, side, db):
        """This hop's line in `migkit_origin`, written in the batch's own
        transaction: the batch and the line saying it was applied are
        there together or not at all."""
        from .. import twoway
        self._origin_table(side, db)
        ledger = self._qualified(side, db, ORIGIN)
        with self._writer(side, db) as conn:
            cur = conn.cursor()
            self._run(cur, f"update {ledger} set n = n + 1, seen = {MARK}"
                           f" where origin = {MARK}",
                      (twoway.batch_seen(self), self._origin_row()))
            if not cur.rowcount:
                self._run(cur, f"insert into {ledger} (origin, n, seen)"
                               f" values ({MARK}, 1, {MARK})",
                          (self._origin_row(), twoway.batch_seen(self)))

    def origin_seen(self, side, db):
        """What the last batch this hop committed here said it was, None
        where there is none."""
        from .. import twoway
        try:
            got = self._rows(side, db, "select seen from"
                                       f" {self._qualified(side, db, ORIGIN)}"
                                       f" where origin = {MARK}",
                             (self._origin_row(),))
        except Exception:  # noqa: BLE001 - no ledger yet: nothing committed
            return None
        return twoway.seen_of(got[0][0] if got else None)

    # ---- comparing two Teradata systems on the servers ----------------------

    #: salts a same-engine comparison's sums are drawn under: four
    #: `HASHBUCKET`s of 16 to 20 bits each
    SALTS = 4

    def _hashable(self, name, declared):
        """[(value expression)] a column is hashed as, or None where
        `HASHROW` cannot take it: text as its bytes in hexadecimal, so case
        and trailing blanks count; a PERIOD as its two ends."""
        base = _kind(declared)
        q = self._q(name)
        if not declared or base in LOBS or base == "udt":
            return None
        if base in ("char", "varchar"):
            return [f"char2hexint({q})"]
        if base == "period":
            return [f"begin({q})", f"end({q})"]
        return [q]

    @staticmethod
    def _hashrow(exprs):
        """`HASHROW` of any number of expressions: groups of 32, hashed
        again together."""
        if len(exprs) <= 32:
            return f"hashrow({', '.join(exprs)})"
        groups = [f"hashrow({', '.join(exprs[i:i + 32])})"
                  for i in range(0, len(exprs), 32)]
        return TeradataEngine._hashrow(groups)

    def hash_sums_sql(self, side, db, table, columns, salts, where=None):
        """The statement answering the count of `columns`' rows (where
        `where` holds) and one sum of `HASHBUCKET` a salt, or None where a
        column cannot be hashed."""
        declared = self._declared(side, db, table)
        exprs, nulls = [], []
        for name in columns:
            got = self._hashable(name, declared.get(name, ""))
            if got is None:
                return None
            exprs += got
            nulls.append(f"case when {self._q(name)} is null then '1' else"
                         " '0' end")
        if not exprs:
            return None
        marks = " || ".join(nulls)
        sums = [f"sum(cast(hashbucket({self._hashrow([repr(s), marks] + exprs)})"
                f" as decimal(38, 0)))" for s in salts]
        return (f"select count(*), {', '.join(sums)} from"
                f" {self._qualified(side, db, table)}"
                + (f" where ({where})" if where else ""))

    def same_engine_digest(self, side, db, table, columns, where=None,
                           salt=None):
        """Teradata against Teradata, on each system (`hash_sums_sql`): the
        count, and each row's salted `HASHROW` taken into `HASHBUCKET`
        under four salts and summed - asked of both sides by the pair's
        check (`HeteroEngine._digests`), which reads a table whose sums
        differ leaf by leaf and row by row. A table holding a column
        `HASHROW` cannot take is folded in this process, as every other
        path folds it; one system keeping 20-bit hash buckets beside one
        keeping 16 sums differently for the same rows, and is read row by
        row, never passed."""
        salt = salt or self.digest_salt()
        sql = self.hash_sums_sql(side, db, table, [n for n, _ in columns],
                                 [f"{salt}{i}" for i in range(self.SALTS)],
                                 where)
        if sql is not None:
            try:
                got = self._rows(side, db, sql)[0]
            except Exception as e:  # noqa: BLE001 - folded here instead
                if not _is_driver_error(e):
                    raise
            else:
                return (int(got[0]), "hashbucket " + " ".join(
                    str(int(x or 0)) for x in got[1:]))
        return self.neutral_digest(side, db, table, columns, where)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def row_bucket_sql(self, side, db, table, columns, n):
        """The server's own hash of the row, in [0, n): a table is read by
        these buckets, each a statement of its own, resumable
        (`ranges.keyless`). Columns `HASHROW` cannot take are left out: two
        rows alike in all the others share a bucket, which is all a bucket
        needs."""
        declared = self._declared(side, db, table)
        exprs = []
        for name, _ in columns:
            got = self._hashable(name, declared.get(name, ""))
            if got:
                exprs += [self._q(name)] if _base(declared.get(name, "")) \
                    in ("char", "varchar") else got
        if not exprs:
            return None
        return f"mod(hashbucket({self._hashrow(exprs)}), {int(n)})"

    def check_deep(self, db):
        """What a load left on the target (`_left_by_loads`), the triggers
        a load took off it and has not put back (`set_aside`), text stored
        twice as UTF-8 on the source, and the two systems' tables held to
        each other: keys, columns, indexes, character sets, case rules,
        identity columns, narrower columns, NULL against empty, floating
        point sums, timestamps shifted as a whole, objects beside the
        tables, triggers, foreign keys, partitions, the primary index that
        spreads a table over the AMPs, rights and owners. A check that
        cannot run says so as an error, never as a pass."""
        out = self._left_by_loads(db) + [self._mojibake(db)]
        aside = self.set_aside(db)
        if aside is not None:
            out.append(aside)
        for check in (self._deep_keys, self._deep_columns,
                      self._deep_indexes, self._deep_charset,
                      self._deep_collation, self._deep_generated,
                      self._deep_narrowing, self._deep_nullempty,
                      self._deep_float, self._deep_timeshift,
                      self._deep_boundary,
                      self._deep_objects, self._deep_triggers,
                      self._deep_foreign_keys, self._deep_partitions,
                      self._deep_sharding, self._deep_grants,
                      self._deep_ownership):
            try:
                got = check(db)
            except SystemExit:
                raise
            except Exception as e:  # noqa: BLE001 - said, as the error
                out.append(Result(
                    "deep", f"{db} {check.__name__.replace('_deep_', '')}",
                    "error", "could not run: "
                    + (str(e).strip().splitlines() or [type(e).__name__])[0]
                    [:120]))
                continue
            out += got if isinstance(got, list) else [got]
        return out

    def _left_by_loads(self, db):
        """On the target: a bulk load's error tables or migkit's own stage
        tables left by a load that died, and a table left mid-load, which
        refuses every read until the load is ended or the table dropped."""
        try:
            names = [str(t).strip() for (t,) in self._rows(
                "dst", db, "select TableName from DBC.TablesV where"
                           f" DatabaseName = {MARK} and TableKind in ('T',"
                           " 'O')", (self._d("dst", db),))]
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target tables", "error",
                           "could not read the target's tables:"
                           f" {str(e).splitlines()[0][:90]}")]
        left = [t for t in names if t.lower().startswith(STAGE)
                or re.search(r"_ERR_[12]$", t, re.I)]
        stuck = []
        for t in names:
            # a table the hop leaves out is the target's own
            if self._own(t) or t in left or not self.in_scope(db, t):
                continue
            try:
                self._rows("dst", db, "select top 1 1 from"
                                      f" {self._qualified('dst', db, t)}")
            except Exception as e:  # noqa: BLE001 - which is the finding
                if "2652" in str(e) or "being loaded" in str(e).lower():
                    stuck.append(t)
        out = []
        if stuck:
            out.append(Result("deep", f"{db} target tables", "diff",
                              f"{len(stuck)} tables left mid-load, which"
                              f" refuse every read: {', '.join(stuck[:6])}",
                              "", "copy those tables again: the load that"
                                  " left them is gone"))
        if left:
            out.append(Result("deep", f"{db} load leftovers", "diff",
                              f"{len(left)} tables a load that died left"
                              f" behind: {', '.join(left[:6])}", "",
                              "drop them once no copy runs"))
        return out or [Result("deep", f"{db} target tables", "ok",
                              "no table left mid-load, and nothing a load"
                              " left behind")]

    # ---- the two systems' tables held to each other ------------------------
    #
    # Every statement below reads DBC's views or the tables themselves; none
    # writes. Where a check reads only what `neutral_columns`, `neutral_key`
    # and `neutral_indexes` already read, it adds no statement of its own.

    def _shared(self, db):
        """[(source name, target name)] of the tables in scope that both
        systems hold, matched without case: Teradata compares names
        without it."""
        there = {t.lower(): t for t in self.neutral_tables("dst", db)}
        return [(t, there[t.lower()]) for t in self.neutral_tables("src", db)
                if t.lower() in there]

    def _columns_of(self, side, db, table):
        """{lower-cased name: (name, declared type)} of a table's
        columns."""
        return {n.lower(): (n, t)
                for n, t in self.neutral_columns(side, db, table)}

    def _deep_keys(self, db):
        """Tables on the source that have no key to read, compare and
        repair them by: a primary key, or a unique primary index over
        columns that refuse NULL (`neutral_key`)."""
        tables = self.neutral_tables("src", db)
        keyless = [t for t in tables if not self.neutral_key("src", db, t)]
        if keyless:
            return Result(
                "deep", f"{db} keys", "warn",
                f"{len(keyless)} of {len(tables)} tables have no primary key"
                " and no unique primary index of NOT NULL columns: "
                + clauses(keyless, 5) + " - their rows are copied by hash"
                " bucket and compared whole, and a row that differs cannot"
                " be named by its key or repaired row by row", "",
                "give each a primary key, or a unique primary index over"
                " NOT NULL columns, where its rows are to be repaired one by"
                " one")
        return Result("deep", f"{db} keys", "ok",
                      f"{len(tables)} tables, each with a key to read,"
                      " compare and repair it by" if tables
                      else "no tables in scope")

    def _deep_columns(self, db):
        """Each column of a table both systems hold, held to the source's:
        its declared type, whether it takes NULL, its default and whether
        it is an identity column."""
        drift, compared = [], 0
        for s, d in self._shared(db):
            there = self._columns_of("dst", db, d)
            mine = self.neutral_column_rules("src", db, s)
            theirs = self.neutral_column_rules("dst", db, d)
            for name, decl in self.neutral_columns("src", db, s):
                compared += 1
                got = there.get(name.lower())
                if got is None:
                    drift.append(f"{s}.{name}: not on the target")
                    continue
                other = got[1]
                if _squeeze(decl) != _squeeze(other):
                    drift.append(f"{s}.{name}: type {decl} -> {other}")
                a, b = mine.get(name, {}), theirs.get(got[0], {})
                for rule, word in (("null", "takes NULL"),
                                   ("default", "default"),
                                   ("identity", "identity")):
                    x, y = a.get(rule), b.get(rule)
                    if rule == "default":
                        x, y = _squeeze(x or ""), _squeeze(y or "")
                    if x != y:
                        drift.append(f"{s}.{name}: {word} {x!r} -> {y!r}")
        if drift:
            out = self.hop.report_dir(db) / "deep-columns.diff"
            out.write_text("\n".join(drift) + "\n")
            return Result("deep", f"{db} columns", "diff",
                          f"{len(drift)} differences in {compared} columns: "
                          + clauses(drift, 4), str(out),
                          "align the target's definition (type, NOT NULL,"
                          " default, identity) with the source's")
        return Result("deep", f"{db} columns", "ok",
                      f"{compared} columns compared, identical")

    def _deep_indexes(self, db):
        """The source's secondary indexes the target lacks."""
        missing, compared = [], 0
        for s, d in self._shared(db):
            have = {(tuple(c.lower() for c in cols), bool(unique))
                    for _, unique, cols, _ in
                    self.neutral_indexes("dst", db, d)}
            for name, unique, cols, _ in self.neutral_indexes("src", db, s):
                compared += 1
                if (tuple(c.lower() for c in cols), bool(unique)) \
                        not in have:
                    missing.append(f"{s}.{name} ({', '.join(cols)}"
                                   + (", unique)" if unique else ")"))
        if missing:
            return Result("deep", f"{db} indexes", "diff",
                          f"{len(missing)} of {compared} secondary indexes"
                          " are not on the target: " + clauses(missing, 4),
                          "", "create them on the target once the rows are"
                              " in (`migkit schema`)")
        return Result("deep", f"{db} indexes", "ok",
                      f"{compared} secondary indexes of the source, each on"
                      " the target")

    def _deep_boundary(self, db):
        """A table whose key is one whole number: the target's largest
        must not be past the source's. A target ahead of its source has been
        written by someone else, or a change applied twice."""
        ahead, behind, checked = [], [], 0
        for s, d in self._shared(db):
            key = self.neutral_key("src", db, s)
            decl = dict(self.neutral_columns("src", db, s)).get(
                key[0]) if len(key) == 1 else None
            if decl is None or _kind(decl) not in (
                    "byteint", "smallint", "integer", "bigint"):
                continue
            checked += 1
            q = self._q(key[0])
            a, b = (int(self._rows(side, db, f"select coalesce(max({q}), 0)"
                                             f" from {self._qualified(side, db, t)}"
                                   )[0][0] or 0)
                    for side, t in (("src", s), ("dst", d)))
            if b > a:
                ahead.append(f"{s} source max={a} target max={b}")
            elif b < a:
                behind.append(s)
        if ahead:
            return Result("deep", f"{db} boundary", "diff",
                          f"the target's largest key is AHEAD of the"
                          f" source's on {len(ahead)} tables: "
                          + clauses(ahead, 4), "",
                          "rows were written on the target, or a change"
                          " applied twice: find the writer before cutover")
        return Result("deep", f"{db} boundary", "ok",
                      f"largest key checked on {checked} tables, none ahead"
                      " of the source"
                      + (f"; {len(behind)} behind" if behind else ""))

    def _text_pairs(self, db):
        """[(table, column, source's type, target's type)] of the text
        columns both systems hold."""
        out = []
        for s, d in self._shared(db):
            there = self._columns_of("dst", db, d)
            for name, decl in self.neutral_columns("src", db, s):
                got = there.get(name.lower())
                if got is not None and _kind(decl) in ("char", "varchar",
                                                       "clob"):
                    out.append((s, name, decl, got[1]))
        return out

    def _deep_charset(self, db):
        """A text column whose character set the target holds narrower:
        LATIN keeps the characters of the Latin alphabets and refuses or
        replaces the rest, UNICODE keeps them all."""
        lossy, compared = [], 0
        for t, name, a, b in self._text_pairs(db):
            x, y = _charset_of(a), _charset_of(b)
            if not x or not y:
                continue
            compared += 1
            if x != y and y != "UNICODE":
                lossy.append(f"{t}.{name}: {x} -> {y}")
        if lossy:
            return Result("deep", f"{db} charset", "diff",
                          f"{len(lossy)} text columns hold a narrower"
                          " character set on the target: "
                          + clauses(lossy, 4) + " - a character it has no"
                          " code for is refused or replaced", "",
                          "make the target column UNICODE, or the source's"
                          " characters will not all arrive")
        return Result("deep", f"{db} charset", "ok",
                      f"{compared} text columns, none narrower on the"
                      " target")

    def _deep_collation(self, db):
        """Teradata's case rule is a column's: CASESPECIFIC, NOT
        CASESPECIFIC or UPPERCASE. Two keys that differ only in case are
        two rows under one and a single one under the others."""
        drift, compared = [], 0
        for t, name, a, b in self._text_pairs(db):
            compared += 1
            x, y = _case_rule(a), _case_rule(b)
            if x != y:
                drift.append(f"{t}.{name}: {x} -> {y}")
        if drift:
            return Result("deep", f"{db} collation", "diff",
                          f"{len(drift)} text columns compare by another"
                          " case rule on the target: " + clauses(drift, 4),
                          "", "declare the target column with the source's"
                              " case rule, or rows that differ only in case"
                              " merge in a key and sort and compare"
                              " differently")
        return Result("deep", f"{db} collation", "ok",
                      f"{compared} text columns, each compared by the same"
                      " case rule on both sides")

    def _identity_kinds(self, side, db, table):
        """{lower-cased column: "GA" | "GD"} of a table's identity columns:
        GENERATED ALWAYS, which refuses a value written to it, and BY
        DEFAULT, which takes one."""
        return {str(r[0]).strip().lower(): str(r[9]).strip()
                for r in self._catalog(side, db, table)
                if str(r[9] or "").strip() in ("GA", "GD")}

    def _deep_generated(self, db):
        """Identity columns: one the target generates always refuses the
        value the source holds; one on one side only is a counter the
        other lacks."""
        refused, lacks, compared = [], [], 0
        for s, d in self._shared(db):
            mine = self._identity_kinds("src", db, s)
            theirs = self._identity_kinds("dst", db, d)
            compared += len(mine) + len(set(theirs) - set(mine))
            for col, kind in sorted(theirs.items()):
                if kind == "GA":
                    refused.append(f"{d}.{col}")
            for col in sorted(set(mine) ^ set(theirs)):
                lacks.append(f"{s}.{col} is an identity column on the "
                             + ("source only" if col in mine
                                else "target only"))
        if refused:
            return Result("deep", f"{db} generated", "diff",
                          f"{len(refused)} identity columns are GENERATED"
                          " ALWAYS on the target: " + clauses(refused, 4)
                          + " - the target refuses the value the source"
                            " holds, and makes its own", "",
                          "declare them GENERATED BY DEFAULT AS IDENTITY on"
                          " the target before the rows are written")
        if lacks:
            return Result("deep", f"{db} generated", "warn",
                          clauses(lacks, 4) + " - a row written later gets"
                          " no number where the target has none, and one"
                          " the source does not make where it has", "",
                          "give both sides the same identity columns")
        return Result("deep", f"{db} generated", "ok",
                      f"{compared} identity columns, the same on both sides"
                      if compared else "no identity columns")

    def _deep_narrowing(self, db):
        """A target column narrower than the source's: a character length,
        a number of digits or a range the source's values may not fit."""
        from .. import verdict
        narrow = []
        for s, d in self._shared(db):
            there = self._columns_of("dst", db, d)
            for name, decl in self.neutral_columns("src", db, s):
                got = there.get(name.lower())
                if got is None:
                    continue
                why = verdict.narrowing(self.CANON_ENGINE, plain_declared(decl),
                                        plain_declared(got[1]))
                if why:
                    narrow.append(f"{s}.{name}: {why}")
        return verdict.narrowing_result(db, narrow)

    def _text_columns(self, db, limit=30):
        """[(table, source's name of the column, target's table and
        column)] of the first text columns of the tables both hold."""
        out = []
        for s, d in self._shared(db):
            there = self._columns_of("dst", db, d)
            for name, decl in self.neutral_columns("src", db, s):
                got = there.get(name.lower())
                if got is not None and _kind(decl) in ("char", "varchar"):
                    out.append((s, name, d, got[0]))
        return out[:limit]

    def _deep_nullempty(self, db):
        """NULL and '' are two values in ANSI mode, and a mover swaps them
        without a word: the count of each, per text column, held to the
        source's."""
        bad = []
        cols = self._text_columns(db)
        for s, name, d, other in cols:
            got = []
            for side, table, col in (("src", s, name), ("dst", d, other)):
                q = self._q(col)
                row = self._rows(
                    side, db, "select sum(cast(case when " + q + " is null"
                              " then 1 else 0 end as bigint)),"
                              " sum(cast(case when " + q + " = '' then 1"
                              " else 0 end as bigint)) from "
                              + self._qualified(side, db, table))[0]
                got.append((int(row[0] or 0), int(row[1] or 0)))
            if got[0] != got[1]:
                bad.append(f"{s}.{name}: source NULL|empty={got[0][0]}|"
                           f"{got[0][1]}, target {got[1][0]}|{got[1][1]}")
        if bad:
            return Result("deep", f"{db} nullempty", "diff",
                          f"{len(bad)} text columns split NULL and empty"
                          " otherwise on the target: " + clauses(bad, 4),
                          "", "decide which side is right, and load the"
                              " other again (NULLIF(col, '') turns an empty"
                              " value to NULL)")
        return Result("deep", f"{db} nullempty", "ok",
                      f"{len(cols)} text columns, NULL and empty counted"
                      " alike on both sides" if cols
                      else "no text columns on both sides")

    def _deep_float(self, db):
        """FLOAT is an approximation: its sum is held to the source's with
        a tolerance, and its non-NULL count exactly."""
        bad, cols = [], []
        for s, d in self._shared(db):
            there = self._columns_of("dst", db, d)
            for name, decl in self.neutral_columns("src", db, s):
                got = there.get(name.lower())
                if got is not None and _kind(decl) == "float":
                    cols.append((s, name, d, got[0]))
        for s, name, d, other in cols[:30]:
            seen = []
            for side, table, col in (("src", s, name), ("dst", d, other)):
                q = self._q(col)
                seen.append(self._rows(
                    side, db, f"select count({q}), coalesce(sum(cast({q} as"
                              f" float)), 0), coalesce(max(abs({q})), 0) from"
                              f" {self._qualified(side, db, table)}")[0])
            (sn, ss, sm), (dn, ds, dm) = seen
            if int(sn or 0) != int(dn or 0):
                bad.append(f"{s}.{name}: non-NULL count source={sn}"
                           f" target={dn}")
                continue
            ss, ds = float(ss or 0), float(ds or 0)
            scale = max(abs(ss), abs(ds), float(sm or 0), float(dm or 0), 1.0)
            if abs(ss - ds) > scale * 1e-9:
                bad.append(f"{s}.{name}: sum source={ss!r} target={ds!r}")
        if bad:
            return Result("deep", f"{db} float", "diff",
                          f"{len(bad)} FLOAT columns drift beyond"
                          " tolerance: " + clauses(bad, 4), "",
                          "compare the columns' definitions first: a change"
                          " of precision on the way does this")
        return Result("deep", f"{db} float", "ok",
                      f"{min(len(cols), 30)} FLOAT columns within tolerance"
                      if cols else "no FLOAT columns on both sides")

    #: timestamp columns sampled for a shift, and rows read of each
    SHIFT_COLUMNS, SHIFT_ROWS = 10, 200

    def _deep_timeshift(self, db):
        """A timestamp column every row of which the target holds shifted
        by one offset: a mover's session in another zone, not rows
        corrupted one by one (`verdict.uniform_shift`). Read by key, the
        first rows of each column, as instants."""
        from .. import canon, verdict
        shifts, looked = [], 0
        for s, d in self._shared(db):
            key = self.neutral_key("src", db, s)
            if len(key) != 1 or looked >= self.SHIFT_COLUMNS:
                continue
            there = self._columns_of("dst", db, d)
            kclass = canon.type_class(self.CANON_ENGINE,
                                      dict(self.neutral_columns(
                                          "src", db, s))[key[0]])
            for name, decl in self.neutral_columns("src", db, s):
                got = there.get(name.lower())
                if got is None or _kind(decl) != "timestamp" \
                        or looked >= self.SHIFT_COLUMNS:
                    continue
                looked += 1
                cols = [(key[0], kclass), (name, "timestamp")]
                rows, _ = self.neutral_read("src", db, s, cols, None,
                                            self.SHIFT_ROWS)
                # a row by its key as text, the way the reads by key name it
                a = {self._key_of(cols, [key[0]], r)[0]: _epoch(r[1])
                     for r in rows if r[1] is not None}
                found = self.neutral_rows_by_key(
                    "dst", db, d, [(key[0], kclass), (got[0], "timestamp")],
                    [key[0]], [(r[0],) for r in rows if r[1] is not None])
                b = {k[0]: _epoch(r[1]) for k, r in found.items()
                     if r[1] is not None}
                line = verdict.uniform_shift(f"{s}.{name}", a, b)
                if line:
                    shifts.append(line)
        return verdict.timeshift_result(db, shifts)

    def _objects(self, side, db):
        """{lower-cased name: (name, TableKind, the table a trigger is
        on)} of the objects beside the tables that the hop's `objects`
        takes (`takes`): views, macros, procedures, functions, triggers,
        join and hash indexes."""
        name = self._d(side, db)
        kinds = ", ".join(f"'{k}'" for k in OBJECT_KINDS)
        subject = {}
        try:
            subject = {str(t).strip(): str(on).strip() for t, on in
                       self._rows(side, db,
                                  "select TriggerName, TableName from"
                                  f" DBC.TriggersV where"
                                  f" SubjectTableDataBaseName = {MARK}",
                                  (name,))}
        except Exception:  # noqa: BLE001 - a trigger is then taken by name
            pass
        out = {}
        for n, kind in self._rows(
                side, db, "select TableName, TableKind from DBC.TablesV"
                          f" where DatabaseName = {MARK} and TableKind in"
                          f" ({kinds})", (name,)):
            n, kind = str(n).strip(), str(kind).strip()
            if self._own(n) or not self.takes(
                    db, OBJECT_KINDS[kind][0], n, by=subject.get(n)):
                continue
            out[n.lower()] = (n, kind, subject.get(n))
        return out

    def _deep_objects(self, db):
        """The objects beside the tables that the hop takes and the source
        has: each on the target?"""
        mine, there = self._objects("src", db), self._objects("dst", db)
        missing = [f"{OBJECT_KINDS[kind][1]} {n}"
                   for key, (n, kind, _) in sorted(mine.items())
                   if key not in there]
        if missing:
            return Result("deep", f"{db} objects", "diff",
                          f"{len(missing)} of {len(mine)} objects beside the"
                          " tables are not on the target: "
                          + clauses(missing, 5), "",
                          "create them on the target (`migkit schema`), or"
                          " leave their kind out of the hop's `objects`")
        return Result("deep", f"{db} objects", "ok",
                      f"{len(mine)} views, macros, procedures, functions,"
                      " triggers and join indexes, each on the target"
                      if mine else "no objects beside the tables")

    def _deep_triggers(self, db):
        """Triggers the target holds disabled: a trigger a load switched
        off and nobody switched on again does nothing after the cutover.
        (A load disables the target's triggers while it writes, and puts
        them back: `load_window`.)"""
        rows = self._rows(
            "dst", db, "select TriggerName, TableName, EnabledFlag from"
                       f" DBC.TriggersV where SubjectTableDataBaseName ="
                       f" {MARK}", (self._d("dst", db),))
        rows = [r for r in rows if self.takes(db, "trigger",
                                              str(r[0]).strip(),
                                              by=str(r[1]).strip())]
        off = [f"{str(r[0]).strip()} on {str(r[1]).strip()}" for r in rows
               if str(r[2]).strip().upper() == "N"]
        if off:
            return Result("deep", f"{db} triggers", "diff",
                          f"{len(off)} of {len(rows)} triggers are disabled"
                          " on the target: " + clauses(off, 5), "",
                          'alter trigger "db"."name" enabled')
        return Result("deep", f"{db} triggers", "ok",
                      f"{len(rows)} triggers on the target, every one"
                      " enabled" if rows else "no triggers on the target")

    def _references(self, side, db):
        """{(child, parent, name): (columns pairs, inconsistent)} of the
        foreign keys among the database's tables (`foreign_keys`)."""
        out = {}
        for idx, name, child, ccol, pdb, parent, pcol, bad in self._rows(
                side, db, "select IndexId, IndexName, ChildTable,"
                          " ChildKeyColumn, ParentDB, ParentTable,"
                          " ParentKeyColumn, InconsistencyFlag from"
                          " DBC.All_RI_ChildrenV where ChildDB ="
                          f" {MARK} order by IndexId",
                (self._d(side, db),)):
            child, parent = str(child).strip(), str(parent).strip()
            if str(pdb).strip().lower() != self._d(side, db).lower() \
                    or not self.in_scope(db, child):
                continue
            one = out.setdefault((child.lower(), parent.lower(), int(idx)),
                                 {"pairs": [], "bad": False,
                                  "name": str(name or "").strip() or None,
                                  "child": child, "parent": parent})
            one["pairs"].append((str(ccol).strip(), str(pcol).strip()))
            one["bad"] = one["bad"] or str(bad).strip().upper() == "Y"
        return out

    def _deep_foreign_keys(self, db):
        """The source's foreign keys the target lacks, and the target's
        that Teradata flags inconsistent (a reference it could not
        validate)."""
        mine = self._references("src", db)
        there = self._references("dst", db)
        have = {(c, p, tuple(sorted((a.lower(), b.lower())
                                    for a, b in v["pairs"])))
                for (c, p, _), v in there.items()}
        missing = [f"{v['child']} -> {v['parent']}"
                   for (c, p, _), v in sorted(mine.items())
                   if (c, p, tuple(sorted((a.lower(), b.lower())
                                          for a, b in v["pairs"])))
                   not in have]
        bad = [f"{v['child']} -> {v['parent']}" for v in there.values()
               if v["bad"]]
        if missing or bad:
            said = []
            if missing:
                said.append(f"{len(missing)} not on the target: "
                            + clauses(missing, 4))
            if bad:
                said.append(f"{len(bad)} flagged inconsistent on the"
                            " target: " + clauses(bad, 4))
            return Result("deep", f"{db} fk", "diff", "; ".join(said), "",
                          "create them on the target once the rows are in,"
                          " and validate an inconsistent one")
        return Result("deep", f"{db} fk", "ok",
                      f"{len(mine)} foreign keys of the source, each on the"
                      " target and consistent")

    def _partitioned(self, side, db):
        """{lower-cased table: (table, the partitioning expression)} of the
        tables partitioned by their primary index (`IndexType` Q)."""
        name = self._d(side, db)
        tables = {str(t).strip() for (t,) in self._rows(
            side, db, "select distinct TableName from DBC.IndicesV where"
                      f" DatabaseName = {MARK} and IndexType = 'Q'",
            (name,))}
        text = {}
        if tables:
            text = {str(t).strip(): " ".join(str(x or "").split())
                    for t, x in self._rows(
                        side, db, "select TableName, ConstraintText from"
                                  " DBC.PartitioningConstraintsV where"
                                  f" DatabaseName = {MARK}", (name,))}
        return {t.lower(): (t, text.get(t, "")) for t in tables
                if self.in_scope(db, t)}

    def _partition_rows(self, side, db, table):
        """{partition number: {"rows", "bound"}}: the rows each partition
        of a table holds, counted by the server."""
        return {f"p{int(n)}": {"rows": int(c), "digest": None,
                               "bound": None}
                for n, c in self._rows(
                    side, db, "select PARTITION, count(*) from "
                              + self._qualified(side, db, table)
                              + " group by 1")}

    def _deep_partitions(self, db):
        """Every table partitioned by its primary index, on either side:
        its partitioning expression held to the source's, and each
        partition's rows (`Engine._partitions_verdict`). A table migkit
        makes is not partitioned (`neutral_create_sql`): its rows land in
        one."""
        src, dst = self._partitioned("src", db), self._partitioned("dst", db)
        plain = {side: {t.lower() for t in self.neutral_tables(side, db)}
                 for side in ("src", "dst")}
        scheme, tables = [], {}
        for key, (t, expr) in sorted(src.items()):
            if key not in dst:
                scheme.append(f"{t}: partitioned on the source ({expr}), "
                              + ("not on the target" if key in plain["dst"]
                                 else "missing on the target"))
                continue
            other = dst[key]
            if " ".join(expr.lower().split()) != " ".join(
                    other[1].lower().split()):
                scheme.append(f"{t}: partitioning differs src({expr})"
                              f" dst({other[1]})")
            tables[t] = (self._partition_rows("src", db, t),
                         self._partition_rows("dst", db, other[0]))
        for key, (t, expr) in sorted(dst.items()):
            if key not in src and key in plain["src"]:
                scheme.append(f"{t}: partitioned on the target ({expr}),"
                              " not on the source")
        return self._partitions_verdict(db, tables, scheme, "NO RANGE")

    def _primary_index(self, side, db, table):
        """The columns a table is spread over the AMPs by - its primary
        index, lower-cased and in order - or () for a table with no
        primary index, whose rows are spread without one."""
        return tuple(str(c).strip().lower() for (c,) in self._rows(
            side, db, "select ColumnName from DBC.IndicesV where"
                      f" DatabaseName = {MARK} and TableName = {MARK} and"
                      " IndexType in ('P', 'Q') order by ColumnPosition",
            (self._d(side, db), str(table))))

    def _deep_sharding(self, db):
        """Rows lie on the AMPs by the hash of the primary index: a target
        spread by other columns - a table migkit makes is spread by its
        key, or not at all - changes which AMP holds a row, how evenly,
        and which joins stay on one AMP."""
        moved, compared = [], 0
        for s, d in self._shared(db):
            compared += 1
            a, b = (self._primary_index("src", db, s),
                    self._primary_index("dst", db, d))
            if a != b:
                moved.append(f"{s}: ({', '.join(a) or 'no primary index'})"
                             f" -> ({', '.join(b) or 'no primary index'})")
        if moved:
            return Result("deep", f"{db} sharding", "warn",
                          f"{len(moved)} of {compared} tables are spread"
                          " over the AMPs by other columns on the target: "
                          + clauses(moved, 4), "",
                          "create them with the source's primary index"
                          " where joins and skew are to stay as they were")
        return Result("deep", f"{db} sharding", "ok",
                      f"{compared} tables, each spread by the same primary"
                      " index on both sides")

    def _rights(self, side, db):
        """{(user, table, right)} the database's tables are granted, less
        the system's and migkit's own."""
        from .. import freeze
        skip = set(freeze.TeradataHold.SYSTEM) | {
            str(self._ep(side).user or "").strip().upper()}
        out = set()
        for user, table, right in self._rows(
                side, db, "select UserName, TableName, AccessRight from"
                          f" DBC.AllRightsV where DatabaseName = {MARK}",
                (self._d(side, db),)):
            user, table = str(user).strip(), str(table).strip()
            if user.upper() in skip or self._own(table):
                continue
            if table.lower() != "all" and not self.in_scope(db, table):
                continue
            out.add((user.lower(), table.lower(), str(right).strip()))
        return out

    def _deep_grants(self, db):
        """The rights on the database and its tables that the source
        grants and the target does not: an application that could read
        them there cannot here."""
        mine, there = self._rights("src", db), self._rights("dst", db)
        missing = sorted(mine - there)
        if missing:
            return Result("deep", f"{db} grants", "diff",
                          f"{len(missing)} of {len(mine)} rights granted on"
                          " the source are not on the target: "
                          + clauses([f"{u} {r} on {t}"
                                     for u, t, r in missing], 4), "",
                          "`migkit users` compares and grants them")
        return Result("deep", f"{db} grants", "ok",
                      f"{len(mine)} rights on the source, each granted on"
                      " the target" if mine else "no rights granted")

    def _deep_ownership(self, db):
        """The owner of the database, which writes everything in it by
        implicit right, and who made each view, macro, procedure and
        function: they run with the rights of the database that holds
        them."""
        owners = []
        for side in ("src", "dst"):
            got = self._rows(side, db, "select OwnerName from DBC.DatabasesV"
                                       f" where DatabaseName = {MARK}",
                             (self._d(side, db),))
            owners.append(str(got[0][0]).strip() if got else None)
        made, kinds = [], {}
        for side in ("src", "dst"):
            rows = self._rows(
                side, db, "select TableName, CreatorName, TableKind from"
                          f" DBC.TablesV where DatabaseName = {MARK} and"
                          " TableKind in ('V', 'M', 'P', 'E', 'F', 'A', 'R',"
                          " 'G')", (self._d(side, db),))
            made.append({str(n).strip().lower(): str(c).strip()
                         for n, c, _ in rows})
            kinds.update({str(n).strip().lower(): str(k).strip()
                          for n, _, k in rows})
        changed = [f"{n}: made by {a} -> {made[1][n]}"
                   for n, a in sorted(made[0].items())
                   if n in made[1] and made[1][n].lower() != a.lower()
                   and self.takes(db, OBJECT_KINDS[kinds[n]][0], n)]
        said = []
        if owners[0] and owners[1] and owners[0].lower() != owners[1].lower():
            said.append(f"the database is owned by {owners[0]} on the source"
                        f" and {owners[1]} on the target")
        if changed:
            said.append(f"{len(changed)} objects made by another account: "
                        + clauses(changed, 3))
        if said:
            return Result("deep", f"{db} ownership", "diff", "; ".join(said),
                          "", "an owner writes everything in its database,"
                              " and a view or macro runs with the rights of"
                              " the database that holds it: recreate them"
                              " under the account the application expects")
        return Result("deep", f"{db} ownership", "ok",
                      f"the database is owned by {owners[0]} on both sides,"
                      f" and {len(made[0])} views, macros, procedures and"
                      " functions keep their creator")

    def settle_target(self, db, from_source=True):
        """The optimiser's statistics for every table the load wrote: a
        summary (its rows) for each, and its key's columns where it has
        one - Teradata plans on collected statistics, and a table with
        none is planned from a random AMP sample."""
        self._target_only("dst", "refresh statistics")
        done = 0
        for t in self.neutral_tables("dst", db):
            quoted = self._qualified("dst", db, t)
            key = self.neutral_key("dst", db, t)
            self.neutral_create_code("dst", db,
                                     f"collect summary statistics on {quoted}")
            if key:
                self.neutral_create_code(
                    "dst", db, "collect statistics column ("
                               + ", ".join(self._q(k) for k in key)
                               + f") on {quoted}")
            done += 1
        return f"collected the target's statistics on {done} tables"

    # ---- what a move reads of the schema ---------------------------------------

    def foreign_keys(self, side, db):
        """[(child, [columns], parent, [parent's columns])] of the foreign
        keys among the database's tables (`DBC.All_RI_ChildrenV`, a row a
        column pair): the order the copy writes tables in, and what the
        target is held to. A reference to a table of another database is
        not among them."""
        return [(v["child"], [a for a, _ in sorted(v["pairs"])], v["parent"],
                 [b for _, b in sorted(v["pairs"])])
                for _, v in sorted(self._references(side, db).items())]

    #: `DBC.TablesV.RequestText` is a VARCHAR(12500): a view whose text is
    #: that long may be cut, and is not handed on
    VIEW_TEXT = 12500

    def _view_rows(self, side, db):
        """([(view, its text)], [views whose text may be cut]) of the views
        the hop takes (`takes`)."""
        got, cut = [], []
        for n, text in self._rows(
                side, db, "select TableName, RequestText from DBC.TablesV"
                          f" where DatabaseName = {MARK} and TableKind ="
                          " 'V' order by 1", (self._d(side, db),)):
            n, text = str(n).strip(), str(text or "")
            if self._own(n) or not self.takes(db, "view", n):
                continue
            if len(text) >= self.VIEW_TEXT:
                cut.append(n)
            else:
                got.append((n, text))
        return got, cut

    def neutral_views(self, side, db):
        """[(name, the select it is defined as)]: the text after the `AS`
        of its `CREATE VIEW`. A view whose text the catalogue may have cut
        is not among them (`_view_rows`; `_handwork` names it)."""
        return [(n, _view_select(text))
                for n, text in self._view_rows(side, db)[0]]

    # ---- the repair --------------------------------------------------------

    def repair_plan(self, db, kind):
        """The rows the last check found different, carried across by the
        pair's one repair (each action keeps what it replaced, to be put
        back: `repairlog`), and - off unless asked - text stored twice as
        UTF-8, repaired row by row."""
        out = []
        if kind in ("rows", "all"):
            out += self._as_pair().repair_plan(db, "rows")
            text = self._mojibake_repair(db)
            if text:
                out.append(text)
        return out

    def apply(self, db, action):
        if action.kind == "text":
            return self._apply_text(db, action)
        return self._as_pair().apply(db, action)

    def create_missing(self, db, log=None):
        """The tables the target lacks, made as the pair makes them, before
        any is filled."""
        return self._as_pair().create_missing(db, log)

    def finish_created(self, db, log=None):
        """The secondary indexes of the tables that were made, once their
        rows are in."""
        return self._as_pair().finish_created(db, log)

    def load_window(self, db, log=None, tables=None):
        """While a load writes: the target's enabled triggers on the tables
        it fills are disabled, and enabled again on the way out whether the
        load worked or not. A trigger fires for every row written - Teradata
        has no way to keep one from firing for one session - and one that
        sets a timestamp or writes an audit row rewrote what the source
        held. What a load that died left disabled is enabled at the end of
        the next (`set_aside`). Secondary indexes stay in place: whether
        setting them aside pays has not been measured on Teradata."""
        return _TriggerWindow(self, db, log, tables)

    def set_aside(self, db):
        """The triggers a load disabled on the target and has not enabled
        again. Held by a load still running they are off on purpose; held
        by one whose process is gone they are simply off, and the
        application's cutover onto this target would run without them."""
        from ..setaside import SetAside
        held, lost, unread = [], [], []
        for path, alive, defs in SetAside(self.hop, db,
                                          TRIGGERS_ASIDE).records():
            if defs is None:
                unread.append(str(path))
            elif alive:
                held += sorted(defs)
            else:
                lost += [(n, str(path)) for n in sorted(defs)]
        if lost:
            off = {f"{d}.{t}".lower() for d, t, flag in self._triggers(
                "dst", db) if flag == "N"}
            lost = [(n, p) for n, p in lost if n.lower() in off]
        scope = f"{db} triggers set aside"
        if lost or unread:
            said = []
            if lost:
                said.append(f"{len(lost)} triggers a load disabled on the"
                            " target and never enabled again: "
                            + ", ".join(n for n, _ in lost[:5])
                            + " - the statements that enable them are in "
                            + ", ".join(sorted({p for _, p in lost})))
            if unread:
                said.append("a record of triggers disabled cannot be read: "
                            + ", ".join(unread))
            return Result("deep", scope, "diff", "; ".join(said), "",
                          "the next `migkit move` into this database enables"
                          " them again; or run the statements in that file")
        if held:
            return Result("deep", scope, "ok",
                          f"{len(held)} triggers are disabled on the target"
                          " while a migkit load runs into it ("
                          + ", ".join(held[:5]) + "); they are enabled"
                          " again when it stops")
        return None

    def _enable_trigger(self, database, trigger):
        return f"alter trigger {self._q(database)}.{self._q(trigger)} enabled"

    def _triggers(self, side, db):
        """[(the database a trigger is in, its name, EnabledFlag)] of the
        triggers on this database's tables that the hop takes."""
        return [(str(d).strip(), str(t).strip(), str(f).strip().upper())
                for d, t, on, f in self._rows(
                    side, db, "select DatabaseName, TriggerName, TableName,"
                              " EnabledFlag from DBC.TriggersV where"
                              f" SubjectTableDataBaseName = {MARK}",
                    (self._d(side, db),))
                if self.takes(db, "trigger", str(t).strip(),
                              by=str(on).strip())]

    # ---- the assessment ------------------------------------------------------

    def _version_of(self, side, db):
        """The system's version, as its driver says it."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(NATIVE + DB_VERSION)
            said = str((cur.fetchone() or [""])[0] or "").split(" ", 1)[0]
        finally:
            conn.close()
        return said or None

    def _server_versions(self):
        """(the source's version, the target's), None for a side that would
        not say: unknown, never clean."""
        db = (self.hop.databases or [""])[0]
        out = []
        for side in ("src", "dst"):
            try:
                out.append(self._version_of(side, db))
            except (Exception, SystemExit):  # noqa: BLE001 - not said
                out.append(None)
        return tuple(out)

    def _brand_probes(self):
        """What each system says of itself in `DBC.DBCInfoV`, which only
        Teradata has: its release and its version."""
        db = (self.hop.databases or [""])[0]
        out = []
        for side in ("src", "dst"):
            fields = {}
            try:
                for key, data in self._rows(
                        side, db, "select InfoKey, InfoData from"
                                  " DBC.DBCInfoV"):
                    key = str(key).strip().lower()
                    if key in ("release", "version"):
                        fields[f"dbc_{key}"] = str(data).strip()
            except (Exception, SystemExit):  # noqa: BLE001 - not said
                fields = {}
            out.append(fields)
        return tuple(out)

    def free_bytes(self, side, db):
        """The room the database has to grow into, from the space it may
        take on each AMP (`DBC.DiskSpaceV`). Rows lie by the hash of the
        primary index, so the AMP with the least room is the one that
        refuses first: its headroom, for every AMP. None where the
        database has no space of its own (it draws on its owner's)."""
        got = self._rows(side, db,
                         "select MaxPerm, CurrentPerm from DBC.DiskSpaceV"
                         f" where DatabaseName = {MARK}",
                         (self._d(side, db),))
        if not got or any(int(m or 0) <= 0 for m, _ in got):
            return None
        return max(0, min(int(m) - int(c or 0) for m, c in got)) * len(got)

    def stream_writers(self, db):
        """Besides migkit's own tail, the accounts that may write this
        database - a right to insert, update or delete on it, held directly
        or by a role - and how many sessions each has open now (readable
        only by an account that may read `DBC.SessionInfoV`; otherwise the
        account is named without a count). Teradata keeps no change log a
        client can read and shows no replication; a writer outside these
        accounts is not seen."""
        from .. import freeze
        out = super().stream_writers(db)
        name = self._d("dst", db)
        me = str(self._ep("dst").user or "").strip().upper()
        skip = set(freeze.TeradataHold.SYSTEM) | {me}
        rights = {"I", "U", "D"}
        who = {str(u).strip() for u, r in self._rows(
            "dst", db, "select UserName, AccessRight from DBC.AllRightsV"
                       f" where DatabaseName = {MARK}", (name,))
            if str(r).strip() in rights}
        roles = {str(r).strip() for r, a in self._rows(
            "dst", db, "select RoleName, AccessRight from DBC.AllRoleRightsV"
                       f" where DatabaseName = {MARK}", (name,))
            if str(a).strip() in rights}
        if roles:
            who |= {str(g).strip() for r, g in self._rows(
                "dst", db, "select RoleName, Grantee from DBC.RoleMembersV")
                if str(r).strip() in roles}
        who = {u for u in who if u.upper() not in skip}
        open_ = {}
        if who and self._sessions_readable("dst", db):
            open_ = {str(u).strip().upper(): int(n) for u, n in self._rows(
                "dst", db, "select UserName, count(*) from DBC.SessionInfoV"
                           " group by 1")}
        for u in sorted(who):
            n = open_.get(u.upper())
            out.append((f"{u} may write {name}"
                        + (f" ({n} sessions open now)" if n is not None
                           else ""), False))
        return out

    def _assess_extra(self):
        inv = self._handwork()
        return self.reach_items("src") + self.reach_items("dst") \
            + inv.rows() + inv.summary()

    def _handwork(self):
        """The work the move will not do, counted and named
        (`migkit.handwork`): tables with no key to repair them by, the
        views, macros, procedures, functions and triggers that are not
        carried, join and hash indexes, queue tables - which are not
        copied - and the partitioning of a table, which a table migkit
        makes does not have."""
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                inv.add("no-row-key", db, "tables",
                        [t for t in self.neutral_tables("src", db)
                         if not self.neutral_key("src", db, t)])
            except Exception as e:  # noqa: BLE001 - unknown, not zero
                inv.unknown("no-row-key", db, f"catalogue query failed:"
                                              f" {str(e)[:80]}")
            try:
                kinds = ", ".join(f"'{k}'" for k in OBJECT_KINDS)
                found = {}
                for n, kind in self._rows(
                        "src", db, "select TableName, TableKind from"
                                   f" DBC.TablesV where DatabaseName ="
                                   f" {MARK} and TableKind in ({kinds}, 'Q')",
                        (self._d("src", db),)):
                    n, kind = str(n).strip(), str(kind).strip()
                    if self._own(n):
                        continue
                    if kind == "Q":
                        found.setdefault("not-carried", {}).setdefault(
                            "queue tables (not copied)", []).append(n)
                    elif self.takes(db, OBJECT_KINDS[kind][0], n):
                        what = "join and hash indexes" \
                            if kind in ("I", "N") else \
                            f"{OBJECT_KINDS[kind][1]}s"
                        found.setdefault(
                            "not-carried" if kind in ("I", "N")
                            else "server-side-code", {}).setdefault(
                                what, []).append(n)
                for kind, buckets in found.items():
                    for what, names in buckets.items():
                        inv.add(kind, db, what, names)
                for n in self._view_rows("src", db)[1]:
                    inv.unknown("server-side-code", db,
                                f"the text of view {n} may be cut in the"
                                " catalogue")
                inv.add("decide-then-apply", db,
                        "partitioned tables (a table migkit makes is not"
                        " partitioned)",
                        [t for t, _ in self._partitioned("src", db).values()])
            except Exception as e:  # noqa: BLE001 - unknown, not zero
                inv.unknown("server-side-code", db,
                            f"catalogue query failed: {str(e)[:80]}")
        return inv

    # ---- who it is -----------------------------------------------------------

    def server_identity(self, side, db):
        """The system by its version and the node answering, the database
        by its own id and when it was made - two databases of one name on
        two systems differ there - and the space its tables take."""
        name = self._d(side, db)
        out = {}
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(NATIVE + DB_VERSION + " " + REMOTE)
            said = str((cur.fetchone() or [""])[0] or "").split(" ", 1)
            out.update(version=said[0], name=said[1] if len(said) > 1
                       else str(self._ep(side).host))
            cur.execute("select DatabaseId, CreateTimeStamp from"
                        " DBC.DatabasesV where DatabaseName = ?", (name,))
            got = cur.fetchone()
            if got is None:
                out["absent"] = True
            else:
                ident = got[0]
                ident = bytes(ident).hex() if isinstance(
                    ident, (bytes, bytearray, memoryview)) else str(ident)
                out["id"] = f"{ident}@{got[1]}"
                cur.execute("select coalesce(sum(CurrentPerm), 0) from"
                            " DBC.TableSizeV where DataBaseName = ?",
                            (name,))
                out["bytes"] = int(cur.fetchone()[0] or 0)
        finally:
            conn.close()
        return out

    def reach_items(self, side):
        """What `assess` says of a Teradata side: whether its bulk sessions
        can run, and on a source that its changes are not followed."""
        items = []

        def add(level, item, detail=""):
            items.append({"level": level, "scope": "reach", "item": item,
                          "detail": str(detail)})
        db = (self.hop.databases or [""])[0]
        try:
            readable = self._sessions_readable(side, db)
        except SystemExit as e:
            add("fail", "reached", str(e))
            return items
        add("pass" if readable else "warn",
            "parallel sessions for large reads and loads",
            "available" if readable else
            "not available: the user may not read DBC.SessionInfoV, so"
            " large tables are read and written by single statements")
        if side == "src":
            add("info", "changes", "Teradata keeps no change log a client"
                                   " can read: the target is brought level"
                                   " by copies, each proved by check")
        return items


# ---- what migkit asks of Teradata's driver (backlog 0f) ---------------------

#: every connection parameter of the driver's release 20.0, as its
#: documentation lists them
PARAMETERS = frozenset((
    "account", "browser", "browser_tab_timeout", "browser_timeout",
    "code_append_file", "column_name", "column_type", "concurrent_interval",
    "concurrent_limit", "connect_failure_ttl", "connect_function",
    "connect_timeout", "cop", "coplast", "database", "dbs_port",
    "encryptdata", "error_query_count", "error_query_interval",
    "error_table_1_suffix", "error_table_2_suffix", "error_table_database",
    "fake_result_sets", "field_quote", "field_sep", "gateway_deadline",
    "govern", "host", "http_proxy", "http_proxy_password", "http_proxy_user",
    "https_port", "https_proxy", "https_proxy_password", "https_proxy_user",
    "https_retry", "jws_algorithm", "jws_audience", "jws_cert",
    "jws_private_key", "lob_support", "local_catalog", "log", "logdata",
    "logmech", "logon_sequence_number", "logon_timeout",
    "manage_error_tables", "max_message_body", "oauth_level", "oauth_scopes",
    "oidc_cache_size", "oidc_claim", "oidc_clientid", "oidc_metadata",
    "oidc_metadata_cache", "oidc_prompt", "oidc_redirect_port",
    "oidc_refresh", "oidc_refresh_percent", "oidc_scope", "oidc_sslmode",
    "oidc_token", "partition", "password", "proxy_bypass_hosts",
    "reconnect_count", "reconnect_interval", "redrive", "request_timeout",
    "runstartup", "sessions", "sip_support", "sp_spl", "sslbase64", "sslca",
    "sslcapath", "sslcipher", "sslcrc", "sslcrl", "sslmode",
    "sslnamedgroups", "sslocsp", "sslprotocol", "teradata_values", "tmode",
    "user"))

#: every escape function of that release, by its name
ESCAPES = frozenset((
    "teradata_amp_count", "teradata_connected", "teradata_database_version",
    "teradata_driver_version", "teradata_get_errors",
    "teradata_get_warnings", "teradata_getloglevel",
    "teradata_go_distribution", "teradata_go_runtime",
    "teradata_go_runtime_stats", "teradata_logon_sequence_number",
    "teradata_program_name", "teradata_provide", "teradata_session_number",
    "teradata_socket_info", "teradata_nativesql", "teradata_agkr",
    "teradata_array_transform_off", "teradata_array_transform_on",
    "teradata_clobtranslate", "teradata_create_procedure",
    "teradata_error_query_count", "teradata_error_query_interval",
    "teradata_error_table_1_suffix", "teradata_error_table_2_suffix",
    "teradata_error_table_database", "teradata_failfast",
    "teradata_fake_result_sets", "teradata_fake_result_sets_off",
    "teradata_field_quote", "teradata_field_sep", "teradata_govern_off",
    "teradata_govern_on", "teradata_lobselect",
    "teradata_manage_error_tables_off", "teradata_manage_error_tables_on",
    "teradata_parameter", "teradata_read_csv", "teradata_read_json",
    "teradata_read_jsonl", "teradata_read_parquet",
    "teradata_request_timeout", "teradata_require_fastexport",
    "teradata_require_fastload", "teradata_rpo", "teradata_sessions",
    "teradata_try_fastexport", "teradata_try_fastload",
    "teradata_udt_transforms_off", "teradata_udt_transforms_on",
    "teradata_untrusted", "teradata_values_off", "teradata_values_on",
    "teradata_write_csv", "teradata_write_json", "teradata_write_jsonl",
    "teradata_write_parquet"))


#: the escapes migkit prepends, by the name each is held in here: what the
#: account reads a use off
ESCAPE_NAMES = {"teradata_try_fastexport": "TRY_FASTEXPORT",
                "teradata_require_fastload": "REQUIRE_FASTLOAD",
                "teradata_nativesql": "NATIVE",
                "teradata_get_errors": "GET_ERRORS",
                "teradata_get_warnings": "GET_WARNINGS",
                "teradata_database_version": "DB_VERSION",
                "teradata_provide": "REMOTE"}


def _account():
    unused = "unused"
    left = "left at the driver's own, which no measurement here improves on"
    sign_in = ("a sign-in by a browser, a token or a key pair is not how a"
               " migration's service user signs in; TD2 or LDAP is")
    proxy = "the system is reached directly, as every other side is"
    tls_more = ("the endpoint's sslmode and authority file are what TLS is"
                " checked by, as every other side's are")
    files = ("rows cross as values migkit renders itself, never through a"
             " file the driver writes or reads")
    out = {("connect", p): (unused, left) for p in (
        "column_name", "column_type", "concurrent_interval",
        "concurrent_limit", "connect_failure_ttl", "connect_function", "cop",
        "coplast", "error_query_count", "error_query_interval", "log",
        "logon_timeout", "max_message_body", "partition",
        "reconnect_count", "reconnect_interval", "redrive",
        "request_timeout", "sip_support", "sp_spl", "local_catalog",
        "https_retry", "https_port", "fake_result_sets")}
    out.update({("connect", p): (unused, sign_in) for p in (
        "browser", "browser_tab_timeout", "browser_timeout",
        "code_append_file", "gateway_deadline", "jws_algorithm",
        "jws_audience", "jws_cert", "jws_private_key", "oauth_level",
        "oauth_scopes", "oidc_cache_size", "oidc_claim", "oidc_clientid",
        "oidc_metadata", "oidc_metadata_cache", "oidc_prompt",
        "oidc_redirect_port", "oidc_refresh", "oidc_refresh_percent",
        "oidc_scope", "oidc_sslmode", "oidc_token", "logdata")})
    out.update({("connect", p): (unused, proxy) for p in (
        "http_proxy", "http_proxy_password", "http_proxy_user",
        "https_proxy", "https_proxy_password", "https_proxy_user",
        "proxy_bypass_hosts")})
    out.update({("connect", p): (unused, tls_more) for p in (
        "sslbase64", "sslcapath", "sslcipher", "sslcrc", "sslcrl",
        "sslnamedgroups", "sslocsp", "sslprotocol")})
    out.update({
        ("connect", "account"): (unused, "the system's accounting string is"
                                         " the user's own default"),
        ("connect", "logon_sequence_number"): (unused, "each session of"
                                                       " migkit's is its own"
                                                       " job"),
        ("connect", "runstartup"): (unused, "a user's startup SQL is the"
                                            " application's, and could"
                                            " change what a read sees"),
        ("connect", "sessions"): (unused, "the driver asks the system how"
                                          " many sessions a bulk load or"
                                          " read takes, as its own guidance"
                                          " says to let it"),
        ("connect", "govern"): (unused, "left on: workload rules may delay"
                                        " a bulk load, never refuse it"),
        ("connect", "manage_error_tables"): (unused, "left on: the driver"
                                                     " drops the error"
                                                     " tables it made"),
        ("connect", "error_table_1_suffix"): (unused, "see"
                                                      " manage_error_tables"),
        ("connect", "error_table_2_suffix"): (unused, "see"
                                                      " manage_error_tables"),
        ("connect", "error_table_database"): (unused, "see"
                                                      " manage_error_tables"),
        ("connect", "field_quote"): (unused, files),
        ("connect", "field_sep"): (unused, files),
    })
    out.update({("escape", e): (unused, left) for e in (
        "teradata_connected", "teradata_driver_version",
        "teradata_getloglevel", "teradata_go_distribution",
        "teradata_go_runtime", "teradata_go_runtime_stats",
        "teradata_program_name", "teradata_session_number",
        "teradata_socket_info", "teradata_array_transform_off",
        "teradata_array_transform_on", "teradata_clobtranslate",
        "teradata_error_query_count", "teradata_error_query_interval",
        "teradata_fake_result_sets", "teradata_fake_result_sets_off",
        "teradata_lobselect", "teradata_request_timeout", "teradata_rpo",
        "teradata_udt_transforms_off", "teradata_udt_transforms_on",
        "teradata_values_off", "teradata_values_on",
        "teradata_manage_error_tables_off",
        "teradata_manage_error_tables_on", "teradata_error_table_1_suffix",
        "teradata_error_table_2_suffix", "teradata_error_table_database",
        "teradata_govern_on", "teradata_sessions")})
    out.update({("escape", e): (unused, files) for e in (
        "teradata_read_csv", "teradata_read_json", "teradata_read_jsonl",
        "teradata_read_parquet", "teradata_write_csv", "teradata_write_json",
        "teradata_write_jsonl", "teradata_write_parquet",
        "teradata_field_quote", "teradata_field_sep")})
    out.update({
        ("escape", "teradata_agkr"): (unused, "an identity column's value"
                                              " is carried, never made"),
        ("escape", "teradata_create_procedure"): (unused, "stored code is"
                                                          " not carried to"
                                                          " Teradata"),
        ("escape", "teradata_failfast"): (unused, "a read that waits for"
                                                  " the workload's turn"
                                                  " still ends"),
        ("escape", "teradata_govern_off"): (unused, "see teradata_failfast"),
        ("escape", "teradata_try_fastload"): (unused, "a bulk load that"
                                                      " falls back by itself"
                                                      " would hide which way"
                                                      " ran; migkit chooses"
                                                      " and requires it"),
        ("escape", "teradata_require_fastexport"): (unused, "a read that"
                                                            " cannot run in"
                                                            " parallel runs"
                                                            " as one"
                                                            " statement"),
        ("escape", "teradata_logon_sequence_number"): (unused, "a load's"
                                                               " job number"
                                                               " says nothing"
                                                               " its rows do"
                                                               " not"),
        ("escape", "teradata_untrusted"): (unused, "not implemented by the"
                                                   " driver"),
        ("escape", "teradata_amp_count"): (unused, "the driver sizes its"
                                                   " own sessions to the"
                                                   " AMPs"),
    })
    return out


def _register():
    from .. import tooluse
    tooluse.register(
        "teradatasql", release="20.0", options=_account(),
        declared={"connect": PARAMETERS, "escape": ESCAPES},
        reads=(tuple(f"engines/teradata.py::{fn}" for fn in (
            "_params", "_fastexport_sql", "_fastload", "_said_back",
            "_slot", "server_identity")),
               {**{("connect", p): p for p in PARAMETERS},
                **{("escape", e): name for e, name in ESCAPE_NAMES.items()},
                ("escape", "teradata_parameter"): "{fn teradata_parameter("}))


_register()
