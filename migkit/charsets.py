"""A change of character set or collation, planned and proved (backlog
P0-SCENARIOS): latin1, cp1252 and the other Windows code pages into
utf8mb4, EUC and TIS-620 into UTF-8, a collation or an ICU release
changed under the same text.

A move between two character sets was carried as whatever the path did:
PostgreSQL's copy converted through the client encoding, MySQL's bulk
load wrote the source's bytes, and the check hashed each side's own bytes
- so a correct conversion read as different, and a value the target
could not hold arrived as `?` without a word. Here the conversion is a
plan read from both servers, and held to:

* **What changes** (`plan`): each text column's character set and
  collation on both sides - the column's own where the target has the
  table, what the target makes a table in where it has not, and what the
  hop asks for (`charset: {to, collation, repair}`) - read from each
  server's catalogue, never from a name.
* **Every value converted exactly** (`convert`): text is carried as text,
  so a value keeps its characters; a value the target's character set has
  no code for is refused by name before anything is written (`scan`), and
  a value that was UTF-8 stored twice (`Engine._double_encoded`) is
  repaired on the way where the plan repairs, and said where it does not.
* **Keys re-checked under the new collation**: every unique key over text
  is put to the target's own comparison (`key_merges`) with the source's
  values, and two values it takes for one are refused before the move;
  MySQL's index length limits are counted in the new character set's
  bytes.
* **Proved in the new character set** (`prove`): both sides read by key
  and each row's canonical text - the source's as the plan converts it -
  hashed, the two sets compared by key, never by range: two collations
  order text keys otherwise, and a range read on each side by its own
  order takes different rows (`check` puts this proof in place of the
  data check for the tables it covers whole).
* **Done on the target after the rows are in** (`after_copy`): MySQL's
  own conversion (`CONVERT TO CHARACTER SET`) where the hop asks for a
  character set migkit's schema step did not make - every mover's rows
  land first, in the source's character set, so no loader is trusted to
  convert - then the double-encoded values repaired row by row, each
  matched on the value read, the old value kept for the undo.
"""
import hashlib
import json
import re
from dataclasses import dataclass, field

OPTION = "charset"
SCOPE = "character set"

#: the most values of a key column read into the target's comparison at
#: once
BATCH = 1000
#: examples shown per finding
EXAMPLES = 3
#: rows repaired in one pass; the next pass takes the rest (a repaired
#: value is no longer double-encoded, so passes converge)
REPAIR_CAP = 20000


@dataclass(frozen=True)
class Charset:
    """One side's character set of a column: its name as the server says
    it, the codec that holds exactly what it holds (None: unknown), the
    highest code point it holds, and the column's collation."""
    name: str
    codec: str = None
    top: int = 0x10FFFF
    collation: str = None
    #: False for a collation under which two different strings can be
    #: one key; None where the server does not say
    exact: bool = None

    @property
    def known(self):
        return self.codec is not None

    @property
    def unicode(self):
        return self.codec == "utf-8"


def _u(name, top=0x10FFFF):
    return ("utf-8", top)


#: MySQL's character sets by the codec that holds exactly what each does
#: (its `latin1` is Windows-1252; `utf8mb3` and `ucs2` stop at U+FFFF)
MYSQL = {"utf8mb4": _u(""), "utf16": _u(""), "utf16le": _u(""),
         "utf32": _u(""), "utf8mb3": _u("", 0xFFFF), "utf8": _u("", 0xFFFF),
         "ucs2": _u("", 0xFFFF), "sjis": ("shift_jis", 0xFFFF),
         "cp932": ("cp932", 0xFFFF), "ujis": ("euc_jp", 0xFFFF),
         "eucjpms": ("euc_jp", 0xFFFF), "euckr": ("euc_kr", 0xFFFF),
         "gb2312": ("gb2312", 0xFFFF), "gbk": ("gbk", 0xFFFF),
         "gb18030": ("gb18030", 0x10FFFF), "big5": ("big5", 0xFFFF),
         "cp850": ("cp850", 0xFF), "cp852": ("cp852", 0xFFFF),
         "cp866": ("cp866", 0xFFFF), "macroman": ("mac_roman", 0xFFFF),
         "macce": ("mac_latin2", 0xFFFF)}
#: PostgreSQL's server encodings
POSTGRES = {"utf8": _u(""), "latin1": ("latin_1", 0xFF),
            "latin2": ("iso8859_2", 0xFFFF), "latin3": ("iso8859_3", 0xFFFF),
            "latin4": ("iso8859_4", 0xFFFF), "latin5": ("iso8859_9", 0xFFFF),
            "latin6": ("iso8859_10", 0xFFFF),
            "latin7": ("iso8859_13", 0xFFFF),
            "latin8": ("iso8859_14", 0xFFFF),
            "latin9": ("iso8859_15", 0xFFFF),
            "latin10": ("iso8859_16", 0xFFFF),
            "win866": ("cp866", 0xFFFF), "win874": ("cp874", 0xFFFF),
            "koi8r": ("koi8_r", 0xFFFF), "koi8u": ("koi8_u", 0xFFFF),
            "iso_8859_5": ("iso8859_5", 0xFFFF),
            "iso_8859_6": ("iso8859_6", 0xFFFF),
            "iso_8859_7": ("iso8859_7", 0xFFFF),
            "iso_8859_8": ("iso8859_8", 0xFFFF),
            "euc_jp": ("euc_jp", 0xFFFF),
            "euc_jis_2004": ("euc_jis_2004", 0x10FFFF),
            "euc_kr": ("euc_kr", 0xFFFF), "euc_cn": ("gb2312", 0xFFFF),
            **{f"win{n}": (f"cp{n}", 0xFFFF) for n in range(1250, 1259)}}
#: SQL Server's code pages, by number (65001: a `_UTF8` collation)
MSSQL = {65001: _u(""), 932: ("cp932", 0xFFFF), 936: ("gbk", 0xFFFF),
         949: ("cp949", 0xFFFF), 950: ("cp950", 0xFFFF),
         874: ("cp874", 0xFFFF), 437: ("cp437", 0xFFFF),
         850: ("cp850", 0xFFFF),
         **{n: (f"cp{n}", 0xFFFF) for n in range(1250, 1259)}}
#: Oracle's database character sets
ORACLE = {"al32utf8": _u(""), "utf8": _u(""), "al16utf16": _u(""),
          "we8mswin1252": ("cp1252", 0xFFFF),
          "we8iso8859p1": ("latin_1", 0xFF),
          "we8iso8859p15": ("iso8859_15", 0xFFFF),
          "us7ascii": ("ascii", 0x7F), "th8tisascii": ("tis_620", 0xFFFF),
          "ja16sjis": ("shift_jis", 0xFFFF),
          "ja16sjistilde": ("shift_jis", 0xFFFF),
          "ja16euc": ("euc_jp", 0xFFFF), "ko16mswin949": ("cp949", 0xFFFF),
          "ko16ksc5601": ("euc_kr", 0xFFFF), "zhs16gbk": ("gbk", 0xFFFF),
          "zhs32gb18030": ("gb18030", 0x10FFFF),
          "zht16mswin950": ("cp950", 0xFFFF), "zht16big5": ("big5", 0xFFFF),
          "ee8mswin1250": ("cp1250", 0xFFFF),
          "cl8mswin1251": ("cp1251", 0xFFFF),
          "el8mswin1253": ("cp1253", 0xFFFF),
          "tr8mswin1254": ("cp1254", 0xFFFF),
          "iw8mswin1255": ("cp1255", 0xFFFF),
          "ar8mswin1256": ("cp1256", 0xFFFF),
          "blt8mswin1257": ("cp1257", 0xFFFF),
          "vn8mswin1258": ("cp1258", 0xFFFF),
          "ee8iso8859p2": ("iso8859_2", 0xFFFF),
          "cl8iso8859p5": ("iso8859_5", 0xFFFF)}
#: Db2's coded character set identifiers
DB2 = {1208: _u(""), 1200: _u(""), 13488: _u("", 0xFFFF),
       819: ("latin_1", 0xFF), 874: ("cp874", 0xFFFF),
       943: ("cp932", 0xFFFF), 954: ("euc_jp", 0xFFFF),
       970: ("euc_kr", 0xFFFF), 1386: ("gbk", 0xFFFF),
       1383: ("gb2312", 0xFFFF), 1392: ("gb18030", 0x10FFFF),
       950: ("cp950", 0xFFFF), 37: ("cp037", 0xFF), 500: ("cp500", 0xFF),
       **{n: (f"cp{n}", 0xFFFF) for n in range(1250, 1259)}}
#: the engines whose text is Unicode and nothing else
UNICODE_ONLY = {"sqlite", "duckdb", "mongodb", "cassandra", "redshift",
                "snowflake", "bigquery", "dynamodb", "opensearch",
                "clickhouse", "couchbase", "cosmosdb", "iceberg", "delta",
                "parquet", "kafka", "kinesis", "pubsub", "cockroachdb",
                "dsql", "generic", "saas", "sapodp"}
#: the engines whose charsets `_reader` reads from the server
READ = ("mysql", "postgres", "mssql", "oracle", "db2")
#: the engines whose text comparison is exact unless a collation says
#: otherwise, which `key_merges` answering None means there
EXACT_BY_DEFAULT = {"postgres", "sqlite", "duckdb", "clickhouse",
                    "cassandra", "oracle", "db2", "mongodb", "redshift",
                    "snowflake", "bigquery", "dynamodb", "parquet"}


def charset(engine, name, collation=None, exact=None):
    """The `Charset` an engine's name for a character set is."""
    engine = _family(engine)
    text = str(name or "").strip()
    low = text.lower()
    got = None
    if engine == "mysql":
        from .canon import CODECS
        got = MYSQL.get(low) or ((CODECS[low], 0xFFFF) if low in CODECS
                                 else None)
    elif engine == "postgres":
        got = POSTGRES.get(low.replace("-", "_"))
    elif engine == "mssql":
        if low in ("utf-16", "utf16"):
            got = _u("")
        else:
            try:
                got = MSSQL.get(int(low.replace("cp", "")))
            except ValueError:
                got = None
    elif engine == "oracle":
        got = ORACLE.get(low)
    elif engine == "db2":
        try:
            got = DB2.get(int(low))
        except ValueError:
            got = None
    elif engine in UNICODE_ONLY or low in ("utf-8", "utf8"):
        got = _u("")
    if got is None:
        return Charset(text or "unknown", None, 0, collation, exact)
    return Charset(text, got[0], got[1], collation, exact)


def _family(engine):
    """The engine whose catalogue a side's charsets are read from: a fork
    by its family's, TiDB by MySQL's (it answers the same schema)."""
    from .engines import ALIASES
    e = ALIASES.get(engine, engine)
    return "mysql" if e == "tidb" else e


def fits(value, cs):
    """Whether `cs` holds every character of `value`: True, False, or
    None where its character set is not one migkit knows."""
    if not isinstance(value, str):
        return True
    if not cs.known:
        return None
    if cs.unicode:
        return all(ord(ch) <= cs.top and not 0xD800 <= ord(ch) <= 0xDFFF
                   for ch in value)
    try:
        value.encode(cs.codec)
    except UnicodeEncodeError:
        return False
    return all(ord(ch) <= cs.top for ch in value)


def double_encoded(value):
    """What a value really says, where it is UTF-8 stored twice; None
    where it is not (`Engine._double_encoded`, the one test)."""
    if not isinstance(value, str) or value.isascii():
        return None
    from .engines.base import Engine
    got = Engine._double_encoded(value)
    return got[1] if got else None


class Unfit(ValueError):
    """A value the target's character set has no code for."""


def convert(value, src, dst, repair):
    """A value as the plan carries it into `dst`: the same characters, or
    what a double-encoded value really says where the plan repairs.
    Unfit where `dst` has no code for one of its characters."""
    if not isinstance(value, str):
        return value
    if repair:
        value = double_encoded(value) or value
    if fits(value, dst) is False:
        raise Unfit(value)
    return value


# --- the hop's word --------------------------------------------------------------

def settings(hop):
    """{"to", "collation", "repair"} of the hop's `charset:`, or None where
    it says none. `charset: utf8mb4` is the character set alone; repair is
    on unless the hop says `repair: none` - text that was UTF-8 stored
    twice is broken on the source, and a conversion that carries it keeps
    it broken for good."""
    raw = (hop.options or {}).get("charset")
    if raw in (None, False, ""):
        return None
    if isinstance(raw, str):
        raw = {"to": raw}
    if not isinstance(raw, dict):
        raise SystemExit(f"charset: {raw!r} - a character set, or {{to,"
                         " collation, repair}}")
    repair = str(raw.get("repair", "double-encoded")).lower()
    if repair not in ("double-encoded", "none"):
        raise SystemExit(f"charset: repair: {repair} - double-encoded or"
                         " none")
    to = str(raw.get("to") or "").strip()
    coll = str(raw.get("collation") or "").strip()
    for word in (to, coll):
        if word and not all(c.isalnum() or c in "_-" for c in word):
            raise SystemExit(f"charset: {word!r} is not a character set or"
                             " collation name")
    if not to and not coll:
        raise SystemExit("charset: says neither a character set (to) nor a"
                         " collation")
    return {"to": to, "collation": coll, "repair": repair != "none"}


def _pair(eng):
    from .engines.hetero import HeteroEngine
    return eng if isinstance(eng, HeteroEngine) else eng._as_pair()


# --- each server's word -----------------------------------------------------------

def _cached(e, key, read):
    got = e.__dict__.setdefault("_charsets_cache", {})
    if key not in got:
        got[key] = read()
    return got[key]


def _text_columns(e, side, db, table):
    """[(column, declared type)] of the text columns of one table."""
    from . import canon
    return [(n, t) for n, t in e.neutral_columns(side, db, table)
            if canon.type_class(e.CANON_ENGINE, t) == "text"]


def _pg_db(e, side, db):
    def read():
        rows = e.run_rule(side, db, "select pg_encoding_to_char(encoding),"
                                    " datcollate from pg_database where"
                                    " datname = current_database()")
        return (str(rows[0][0]), str(rows[0][1])) if rows else (None, None)
    return _cached(e, ("pgdb", side, db), read)


def _ora_db(e, side, db):
    def read():
        rows = dict(e.run_rule(side, db, "select parameter, value from"
                                         " nls_database_parameters where"
                                         " parameter in"
                                         " ('NLS_CHARACTERSET',"
                                         " 'NLS_NCHAR_CHARACTERSET')"))
        return (rows.get("NLS_CHARACTERSET"),
                rows.get("NLS_NCHAR_CHARACTERSET"))
    return _cached(e, ("oradb", side, db), read)


def _ms_page(e, side, db, collation):
    def read():
        rows = e.run_rule(side, db, "select collationproperty('"
                          + collation.replace("'", "") + "', 'CodePage')")
        return int(rows[0][0]) if rows and rows[0][0] is not None else None
    return _cached(e, ("mspage", side, collation), read)


def _pg_columns(e, side, db, table):
    sch, _, tbl = str(table).rpartition(".")
    rel = f'"{(sch or "public")}"."{tbl}"'.replace("'", "''")
    rows = e.run_rule(side, db,
                      "select a.attname, coalesce(c.collname, ''),"
                      " coalesce(c.collisdeterministic, true) from"
                      " pg_attribute a left join pg_collation c on c.oid ="
                      f" a.attcollation where a.attrelid = '{rel}'::regclass"
                      " and a.attnum > 0 and not a.attisdropped")
    return {str(n): (str(c), bool(d)) for n, c, d in rows}


def _reader(e, side, db, table, columns):
    """{column: Charset} of `columns` (text columns of `table`, a table
    this side has) as its server says them."""
    engine = _family(e.CANON_ENGINE)
    out = {}
    if engine == "mysql":
        facts = e.column_facts(side, db, table)
        for n, _ in columns:
            f = facts.get(n) or {}
            coll = f.get("collation")
            out[n] = charset("mysql", f.get("charset"), coll,
                             _mysql_exact(coll))
    elif engine == "postgres":
        enc, dbcoll = _pg_db(e, side, db)
        colls = _pg_columns(e, side, db, table)
        for n, _ in columns:
            name, det = colls.get(n, ("default", True))
            out[n] = charset("postgres", enc, dbcoll if name in (
                "default", "") else name, det)
    elif engine == "mssql":
        facts = e.column_facts(side, db, table)
        for n, typ in columns:
            coll = (facts.get(n) or {}).get("collation")
            if str(typ).lower().startswith(("nchar", "nvarchar", "ntext")):
                out[n] = charset("mssql", "utf-16", coll, _ms_exact(coll))
                continue
            page = _ms_page(e, side, db, coll) if coll else None
            out[n] = charset("mssql", f"cp{page}" if page else "", coll,
                             _ms_exact(coll))
    elif engine == "oracle":
        main, national = _ora_db(e, side, db)
        for n, typ in columns:
            nat = str(typ).lower().startswith(("nchar", "nvarchar", "nclob"))
            out[n] = charset("oracle", national if nat else main, None, True)
    elif engine == "db2":
        rows = dict(e._rows(side, db, "select colname, codepage from"
                                      " syscat.columns where tabschema ="
                                      " \x00?\x00 and tabname = \x00?\x00",
                            (e._owner(side, db), e._q(table)[1:-1])))
        for n, _ in columns:
            page = rows.get(e._q(n)[1:-1]) or rows.get(n)
            out[n] = charset("db2", str(page or ""), None, True)
    else:
        for n, _ in columns:
            out[n] = charset(engine, "utf-8" if engine in UNICODE_ONLY
                             else "", None, None)
    return out


#: each engine's text columns of a whole database, their declared types,
#: character sets and collations, in one statement - the catalogue read
#: once a database, not once a table
CATALOG_SQL = {
    "mysql": ("select table_name, column_name, column_type,"
              " character_set_name, collation_name from"
              " information_schema.columns where table_schema = database()"
              " and character_set_name is not null"),
    "postgres": ("select n.nspname || '.' || c.relname, a.attname,"
                 " format_type(a.atttypid, a.atttypmod),"
                 " pg_encoding_to_char((select encoding from pg_database"
                 " where datname = current_database())),"
                 " coalesce(nullif(co.collname, 'default'), (select"
                 " datcollate from pg_database where datname ="
                 " current_database())), coalesce(co.collisdeterministic,"
                 " true) from pg_attribute a join pg_class c on c.oid ="
                 " a.attrelid join pg_namespace n on n.oid = c.relnamespace"
                 " left join pg_collation co on co.oid = a.attcollation"
                 " where c.relkind in ('r', 'p') and a.attnum > 0 and not"
                 " a.attisdropped and a.attcollation <> 0 and n.nspname"
                 " not in ('pg_catalog', 'information_schema')"),
    "mssql": ("select s.name + '.' + t.name, c.name, ty.name,"
              " collationproperty(c.collation_name, 'CodePage'),"
              " c.collation_name from sys.columns c join sys.tables t on"
              " t.object_id = c.object_id join sys.schemas s on"
              " s.schema_id = t.schema_id join sys.types ty on"
              " ty.user_type_id = c.user_type_id where c.collation_name is"
              " not null"),
}


def _catalog(e, side, db):
    """{table: {column: (declared type, Charset)}} of one side's text
    columns, read in one statement where the engine has one
    (`CATALOG_SQL`); None elsewhere."""
    from . import canon
    engine = _family(e.CANON_ENGINE)
    if engine not in CATALOG_SQL:
        return None

    def read():
        out = {}
        for row in e.run_rule(side, db, CATALOG_SQL[engine]):
            t, n, typ = str(row[0]), str(row[1]), str(row[2])
            if canon.type_class(e.CANON_ENGINE, typ) != "text":
                continue
            if engine == "mysql":
                cs = charset("mysql", row[3], row[4], _mysql_exact(row[4]))
            elif engine == "postgres":
                cs = charset("postgres", row[3], row[4], bool(row[5]))
            elif typ.lower().startswith(("nchar", "nvarchar", "ntext")):
                cs = charset("mssql", "utf-16", row[4], _ms_exact(row[4]))
            else:
                cs = charset("mssql", f"cp{row[3]}" if row[3] else "",
                             row[4], _ms_exact(row[4]))
            out.setdefault(t, {})[n] = (typ, cs)
        return out
    return _cached(e, ("catalog", side, db), read)


def _columns(e, side, db, table):
    """{column: (declared type, Charset)} of one table's text columns."""
    whole = _catalog(e, side, db)
    if whole is not None:
        if table in whole:
            return whole[table]
        leaf = str(table).split(".")[-1]
        return next((v for t, v in whole.items()
                     if t.split(".")[-1] == leaf and "." not in str(table)),
                    {})
    cols = _text_columns(e, side, db, table)
    got = _reader(e, side, db, table, cols)
    return {n: (typ, got[n]) for n, typ in cols if n in got}


def _mysql_exact(coll):
    """Whether a MySQL collation keeps every two different strings apart:
    a binary one of NO PAD (`utf8mb4_0900_bin`); every PAD SPACE one takes
    `a` and `a ` for one key."""
    c = str(coll or "").lower()
    return c in ("utf8mb4_0900_bin", "binary") if c else None


def _ms_exact(coll):
    """No SQL Server collation keeps `a` and `a ` apart."""
    return False if coll else None


def _mysql_declared(how):
    """The `Charset` a MySQL target's columns are converted to after the
    copy (`after_copy`), or None where the hop asks for none: a collation
    alone names its character set by its first word."""
    if not how:
        return None
    coll = how["collation"] or None
    cs = how["to"] or (coll.split("_")[0] if coll else None)
    return charset("mysql", cs, coll, _mysql_exact(coll))


def _built_default(pair, db, src_cs, how):
    """The `Charset` a column gets on a target that has no table of it
    yet: the source's own where the same engine builds it from the
    source's definition (MySQL, SQL Server), and where the target's
    database decides it, the database's. (A MySQL target the hop asks a
    character set of is converted after the copy: `plan` reads that.)"""
    dst = pair.dst_engine
    engine = _family(dst.CANON_ENGINE)
    same = _family(pair.src_engine.CANON_ENGINE) == engine
    if same and engine in ("mysql", "mssql"):
        return src_cs
    if engine == "mysql":
        f = (dst.column_facts("dst", db, None) or {}).get(None) or {}
        return charset("mysql", f.get("charset"), f.get("collation"),
                       _mysql_exact(f.get("collation")))
    if engine == "postgres":
        enc, coll = _pg_db(dst, "dst", db)
        return charset("postgres", enc, coll, True)
    if engine == "oracle":
        from . import canon
        main, national = _ora_db(dst, "dst", db)
        typ = str(canon.ddl_type("oracle", "text", ())).lower()
        return charset("oracle", national if typ.startswith("n") else main,
                       None, True)
    if engine == "mssql":
        from . import canon
        typ = str(canon.ddl_type("mssql", "text", ())).lower()
        if typ.startswith("n"):
            return charset("mssql", "utf-16", None, False)
        rows = dst.run_rule("dst", db, "select collation_name from"
                                       " sys.databases where name ="
                                       " db_name()")
        coll = str(rows[0][0]) if rows else None
        page = _ms_page(dst, "dst", db, coll) if coll else None
        return charset("mssql", f"cp{page}" if page else "", coll, False)
    return charset(engine, "utf-8" if engine in UNICODE_ONLY else "",
                   None, None)


# --- the plan ----------------------------------------------------------------------

@dataclass
class Column:
    """One text column's two character sets, source and target."""
    table: str
    target: str
    column: str
    src: Charset
    dst: Charset

    @property
    def recoded(self):
        """Whether the characters the column holds change: by what each
        side's character set holds, not by its name - MySQL's `utf8` and
        `utf8mb3` are one, UTF-16 and UTF-8 hold the same."""
        return (self.src.codec, self.src.top) != (self.dst.codec,
                                                  self.dst.top)

    @property
    def recollated(self):
        return bool(self.src.collation and self.dst.collation
                     and self.src.collation.lower()
                     != self.dst.collation.lower())

    def said(self):
        parts = []
        if self.recoded:
            parts.append(f"{self.src.name} to {self.dst.name}")
        if self.recollated:
            parts.append(f"collation {self.src.collation} to"
                         f" {self.dst.collation}")
        return ", ".join(parts) or f"{self.dst.name} already"


@dataclass
class Plan:
    db: str
    columns: list = field(default_factory=list)
    repair: bool = False
    declared: dict = None
    unknown: list = field(default_factory=list)

    def tables(self):
        return sorted({c.table for c in self.columns})

    def of(self, table):
        return {c.column: c for c in self.columns if c.table == table}

    def words(self):
        """The change in a line, or ""."""
        if not self.columns:
            return ""
        pairs = {}
        for c in self.columns:
            pairs.setdefault(c.said(), set()).add(c.table)
        return "; ".join(f"{said} in {len(ts)} table"
                         f"{'s' if len(ts) != 1 else ''}"
                         for said, ts in sorted(pairs.items()))


def applies(hop, eng):
    """Whether the charsets of this hop's two sides are read at all: a
    hop that writes, between engines whose text has a character set of
    its own on either side, or that asks for one."""
    from .hopkeys import GOALS
    goal = getattr(hop, "goal", "") or "copy"
    if goal in GOALS and not GOALS[goal]["writes"]:
        return False
    if settings(hop) is not None:
        return True
    try:
        pair = _pair(eng)
    except Exception:  # noqa: BLE001 - a pair that cannot be made has no plan
        return False
    return any(_family(e.CANON_ENGINE) in READ
               for e in (pair.src_engine, pair.dst_engine))


def plan(hop, eng, db):
    """The `Plan` of `db`: every text column whose character set or
    collation differs between the two sides - or all of them, where the
    hop asks for a character set - read once per run."""
    known = eng.__dict__.setdefault("_charset_plans", {})
    if db in known:
        return known[db]
    how = settings(hop)
    pair = _pair(eng)
    src, dst = pair.src_engine, pair.dst_engine
    tables = [t for t in src.neutral_tables("src", db)
              if not hop.excluded(db, *str(t).split("."))]
    try:
        there = [] if dst.target_missing(db) else dst.neutral_tables("dst",
                                                                      db)
    except Exception:  # noqa: BLE001 - a database not made yet has none
        there = []
    pairs, _, _, _ = pair.match_tables(tables, there, pair._renamer(db))
    have = dict(pairs)
    p = Plan(db, repair=bool(how and how["repair"]), declared=how)
    mysql_to = _mysql_declared(how) if _family(
        dst.CANON_ENGINE) == "mysql" else None
    for t in tables:
        try:
            got_src = _columns(src, "src", db, t)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            p.unknown.append(f"{t}: {_last(e)}")
            continue
        cols = [(n, typ) for n, (typ, _) in got_src.items()]
        if not cols:
            continue
        scs = {n: cs for n, (_, cs) in got_src.items()}
        target = have.get(t)
        if mysql_to is not None:
            # what the conversion after the copy makes every column, the
            # target's table there already or not
            target = target or t
            got = {n: mysql_to for n, _ in cols}
        elif target is not None:
            _, back = pair._mapped_types(db, t, dict(cols))
            forward = {v: k for k, v in back.items()}
            try:
                dcs = {n: cs for n, (_, cs) in _columns(
                    dst, "dst", db, target).items()}
            except Exception as e:  # noqa: BLE001 - unknown, said so
                p.unknown.append(f"{target} on the target: {_last(e)}")
                continue
            got = {n: dcs.get(forward.get(n, n)) for n, _ in cols
                   if forward.get(n, n) in dcs}
        else:
            target = t
            got = {n: _built_default(pair, db, scs[n], how) for n, _ in cols}
        for n, _ in cols:
            d = got.get(n)
            s = scs.get(n)
            if d is None or s is None:
                continue
            c = Column(t, target, n, s, d)
            if c.recoded or c.recollated or how is not None:
                p.columns.append(c)
    known[db] = p
    return p


# --- what the values hold -------------------------------------------------------

@dataclass
class Found:
    """What the values of a plan's columns hold, read on the source."""
    scanned: int = 0
    unfit: dict = field(default_factory=dict)       # (t, c): [count, [keys]]
    doubled: dict = field(default_factory=dict)     # (t, c): [count, ex]
    merges: list = field(default_factory=list)      # (t, cols, n, ex, coll)
    unknown: list = field(default_factory=list)


def needs_scan(p):
    """Whether a value can decide anything here: a target that holds fewer
    characters than the source, a repair, or a key under a collation that
    may take two values for one."""
    return p.repair or any(
        not c.dst.known or not c.dst.unicode or c.dst.top < c.src.top
        or not c.src.known or c.recollated or c.dst.exact is False
        for c in p.columns)


def scan(hop, eng, db, p=None):
    """`Found` for `db`'s plan: every value of its columns read once on
    the source, through the hop's row filter."""
    from . import canon
    p = p or plan(hop, eng, db)
    got = Found()
    if not p.columns:
        return got
    pair = _pair(eng)
    src = pair.src_engine
    for t in p.tables():
        cols = p.of(t)
        key = list(src.neutral_key("src", db, t) or [])
        classes = {n: canon.type_class(src.CANON_ENGINE, typ)
                   for n, typ in src.neutral_columns("src", db, t)}
        read = [(k, classes.get(k)) for k in key] + [
            (n, "text") for n in sorted(cols) if n not in key]
        where = hop.row_filter(db, *str(t).split("."))
        for rows in src._every_row("src", db, t, read, where):
            for r in rows:
                keytext = tuple(canon.render_value(classes.get(k), v)
                                for k, v in zip(key, r[:len(key)]))
                for (n, _), v in zip(read, r):
                    if n not in cols or not isinstance(v, str):
                        continue
                    got.scanned += 1
                    c = cols[n]
                    if double_encoded(v) is not None:
                        d = got.doubled.setdefault((t, n), [0, []])
                        d[0] += 1
                        if len(d[1]) < EXAMPLES:
                            d[1].append((v, double_encoded(v)))
                    try:
                        convert(v, c.src, c.dst, p.repair)
                    except Unfit:
                        u = got.unfit.setdefault((t, n), [0, []])
                        u[0] += 1
                        if len(u[1]) < EXAMPLES:
                            u[1].append(keytext or (v[:40],))
        got.merges += _merges(hop, pair, db, t, cols, where, p.repair)
    return got


def _unique_keys(src, db, t):
    keys = [list(src.neutral_key("src", db, t) or [])]
    try:
        keys += [list(c) for _, unique, c, plain in
                 src.neutral_indexes("src", db, t) if unique and plain and c]
    except Exception:  # noqa: BLE001 - the primary key alone
        pass
    out = []
    for k in keys:
        if k and k not in out:
            out.append(k)
    return out


def _merges(hop, pair, db, t, cols, where, repair):
    """[(table, key columns, groups, examples, collation)] of the unique
    keys over the plan's text columns whose values the target's own
    comparison takes for one - asked of the target (`key_merges`), never
    modelled. A target that cannot be asked, under a collation that may
    fold, is said as unknown."""
    from . import canon
    src, dst = pair.src_engine, pair.dst_engine
    classes = {n: canon.type_class(src.CANON_ENGINE, typ)
               for n, typ in src.neutral_columns("src", db, t)}
    out = []
    for key in _unique_keys(src, db, t):
        texts = [k for k in key if k in cols]
        if not texts or not any(cols[k].recollated or cols[k].recoded
                                or cols[k].dst.exact is False
                                for k in texts):
            continue
        coll = [cols[k].dst.collation if k in cols else None for k in key]
        read = [(k, classes.get(k)) for k in key]

        def batches(read=read, key=key):
            for rows in src._every_row("src", db, t, read, where):
                yield [tuple(convert(v, cols[k].src, cols[k].dst, repair)
                             if k in cols else canon.render_value(
                                 classes.get(k), v)
                             for k, v in zip(key, r))
                       for r in rows if None not in r]
        try:
            got = dst.key_merges("dst", db, list(zip(key, coll)), batches())
        except Unfit:
            continue
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append((t, key, None, _last(e), "/".join(
                c for c in coll if c)))
            continue
        folds = [c for k, c in zip(key, coll) if k in cols
                 and cols[k].dst.exact is False]
        if got is None and folds and _family(dst.CANON_ENGINE) \
                not in EXACT_BY_DEFAULT:
            out.append((t, key, None, "the target cannot be asked how it"
                        " compares these values", "/".join(folds)))
        elif got and got[0]:
            out.append((t, key, got[0], got[1], "/".join(
                c for c in coll if c)))
    return out


def _mysql_key_lengths(hop, eng, db, p):
    """[(table, index, bytes, limit)] of the indexes over the plan's
    columns that the new character set makes longer than InnoDB takes
    (3072 bytes, 767 in a COMPACT or REDUNDANT row): MySQL's conversion
    would stop on them half way. A text part counts its characters at the
    most bytes its character set takes for one (`maxlen`, asked of the
    target), any other part eight bytes, the most a key part of a number
    or a time takes."""
    pair = _pair(eng)
    if _family(pair.dst_engine.CANON_ENGINE) != "mysql" \
            or _family(pair.src_engine.CANON_ENGINE) != "mysql":
        return []
    want = {c.table for c in p.columns if c.recoded}
    if not want:
        return []
    src, dst = pair.src_engine, pair.dst_engine
    sizes = {str(n).lower(): int(m) for n, m in dst._q(
        "dst", "select character_set_name, maxlen from"
               " information_schema.character_sets")}
    out = []
    for t in sorted(want):
        cols = p.of(t)
        rows = src._q("src", "select s.index_name, s.column_name,"
                             " s.sub_part, c.character_maximum_length,"
                             " c.character_set_name, t.row_format from"
                             " information_schema.statistics s join"
                             " information_schema.columns c on"
                             " c.table_schema = s.table_schema and"
                             " c.table_name = s.table_name and"
                             " c.column_name = s.column_name join"
                             " information_schema.tables t on"
                             " t.table_schema = s.table_schema and"
                             " t.table_name = s.table_name where"
                             " s.table_schema = %s and s.table_name = %s"
                             " order by s.index_name, s.seq_in_index",
                      (db, t))
        by = {}
        for index, col, part, length, cs, fmt in rows:
            c = cols.get(str(col))
            if c is not None:
                n = int(part or length or 0) * sizes.get(c.dst.name.lower(),
                                                         4)
            elif cs:
                n = int(part or length or 0) * sizes.get(str(cs).lower(), 4)
            else:
                n = 8
            got = by.setdefault(str(index), [0, str(fmt or "").upper()])
            got[0] += n
        for index, (n, fmt) in sorted(by.items()):
            limit = 767 if fmt in ("COMPACT", "REDUNDANT") else 3072
            if n > limit:
                out.append((t, index, n, limit))
    return out


#: bytes a MySQL row takes for a column that is neither text nor a large
#: value, by type, at the most (the manual's storage requirements)
MY_FIXED = {"tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4,
            "integer": 4, "bigint": 8, "float": 4, "double": 8, "real": 8,
            "date": 3, "time": 6, "datetime": 8, "timestamp": 7, "year": 1,
            "bit": 8, "enum": 2, "set": 8, "decimal": 32, "numeric": 32,
            "binary": 255, "varbinary": 65535}
#: MySQL's row limit, large values apart
MY_ROW = 65535


def _mysql_row_sizes(hop, eng, db, p):
    """[(table, bytes)] of the tables whose columns, in the new character
    set, pass the 65,535 bytes MySQL holds a row to (a large value counts
    the 12 bytes of its pointer): the conversion stops on them with `Row
    size too large`. A `varchar` counts its characters at the most bytes
    its character set takes for one, and its length's one or two."""
    pair = _pair(eng)
    if _family(pair.dst_engine.CANON_ENGINE) != "mysql" \
            or _family(pair.src_engine.CANON_ENGINE) != "mysql":
        return []
    want = {c.table for c in p.columns if c.recoded}
    if not want:
        return []
    src, dst = pair.src_engine, pair.dst_engine
    sizes = {str(n).lower(): int(m) for n, m in dst._q(
        "dst", "select character_set_name, maxlen from"
               " information_schema.character_sets")}
    out = []
    for t in sorted(want):
        cols = p.of(t)
        total = 0
        for name, typ, length, cs in src._q(
                "src", "select column_name, data_type,"
                       " character_maximum_length, character_set_name from"
                       " information_schema.columns where table_schema = %s"
                       " and table_name = %s", (db, t)):
            typ = str(typ).lower()
            if typ.endswith(("text", "blob")) or typ in ("json", "geometry"):
                total += 12
                continue
            if typ in ("char", "varchar"):
                c = cols.get(str(name))
                per = sizes.get((c.dst.name if c else str(cs or "")).lower(),
                                4)
                n = int(length or 0) * per
                total += n + ((2 if n > 255 else 1) if typ == "varchar"
                              else 0)
                continue
            if typ in ("binary", "varbinary"):
                total += int(length or 0) + (2 if typ == "varbinary" else 0)
                continue
            total += MY_FIXED.get(typ, 8)
        if total > MY_ROW:
            out.append((t, total))
    return out


# --- what assess and the gate hold to ----------------------------------------------

def _row(level, item, detail, fix=""):
    return {"level": level, "scope": SCOPE, "item": item,
            "detail": detail + (f"  [fix] {fix}" if fix and level != "pass"
                                else "")}


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def _keys(ex):
    return ", ".join("(" + ", ".join(str(v) for v in k) + ")"
                     if isinstance(k, tuple) and len(k) != 1 else
                     repr(k[0] if isinstance(k, tuple) else k) for k in ex)


def checks(hop, eng, dbs, quick=False):
    """assess rows of the character set change: what changes, and each
    value that would not arrive as it is - refused by name - each with
    its fix. `quick`, the gate's: the values are read only where one can
    decide something (`needs_scan`)."""
    if not applies(hop, eng):
        return []
    out = []
    for db in dbs:
        try:
            p = plan(hop, eng, db)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", f"{db}: the character sets",
                            f"could not be read: {_last(e)} - unknown, not"
                            " clean"))
            continue
        for u in p.unknown[:3]:
            out.append(_row("warn", f"{db}: the character sets",
                            f"could not be read for {u} - unknown, not"
                            " clean"))
        if not quick:
            out += order_rows(hop, eng, db)
        if not p.columns:
            continue
        out.append(_row("pass", f"{db}: a character set change",
                        p.words() + (" - double-encoded text repaired on"
                                     " the way" if p.repair else "")))
        unknown = sorted({f"{c.table}.{c.column} ({c.dst.name})"
                          for c in p.columns if not c.dst.known})
        if unknown:
            out.append(_row("fail", f"{db}: a character set migkit does"
                                    " not know",
                            f"{len(unknown)} columns go into one: "
                            + ", ".join(unknown[:4]) + " - which characters"
                            " it holds cannot be said, so neither can what"
                            " arrives", "make the target's character set"
                            " one migkit names (UTF-8 holds every"
                            " character), or exclude these tables"))
        out += _declared(hop, eng, db, p)
        try:
            long_ = _mysql_key_lengths(hop, eng, db, p)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            long_ = []
            out.append(_row("warn", f"{db}: index lengths",
                            f"could not be read: {_last(e)}"))
        for t, index, n, limit in long_:
            out.append(_row("fail", f"{db}: index too long",
                            f"{t}.{index} takes {n:,} bytes in the new"
                            f" character set, and the target takes"
                            f" {limit:,}: the conversion stops on it",
                            "an index on a prefix of the column, or a"
                            " shorter column, on the source or in the"
                            " target's schema first"))
        try:
            wide = _mysql_row_sizes(hop, eng, db, p)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            wide = []
            out.append(_row("warn", f"{db}: row sizes",
                            f"could not be read: {_last(e)}"))
        for t, n in wide:
            out.append(_row("fail", f"{db}: row too wide",
                            f"{t}'s columns take {n:,} bytes a row in the"
                            f" new character set, and MySQL holds a row to"
                            f" {MY_ROW:,}: the conversion stops on it",
                            "the widest varchar columns made text (or"
                            " shorter) on the source or in the target's"
                            " schema first"))
        if not needs_scan(p):
            if not quick:
                out.append(_row("pass", f"{db}: the values",
                                "every character each source column's"
                                " character set holds has a code in its"
                                " target's, and no key is compared"
                                " otherwise there: no value can decide"
                                " anything, so none is read"))
            continue
        try:
            f = scan(hop, eng, db, p)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", f"{db}: the values",
                            f"could not be read: {_last(e)} - unknown,"
                            " not clean"))
            continue
        out += _found_rows(hop, db, p, f)
    return out


def _declared(hop, eng, db, p):
    """The character set the hop asks for, against what the target is:
    only MySQL's is converted by migkit on the target; anywhere else the
    target's database decides it, and one that differs is refused."""
    how = p.declared
    if not how or not how["to"]:
        return []
    pair = _pair(eng)
    if _family(pair.dst_engine.CANON_ENGINE) == "mysql":
        return []
    wrong = sorted({f"{c.target}.{c.column} ({c.dst.name})"
                    for c in p.columns
                    if c.dst.name.lower().replace("-", "")
                    != how["to"].lower().replace("-", "")})
    if not wrong:
        return []
    return [_row("fail", f"{db}: the character set asked for",
                 f"the hop asks for {how['to']}, and the target keeps"
                 f" {len(wrong)} columns otherwise: " + ", ".join(wrong[:4]),
                 f"make the target's database in {how['to']} (its"
                 " character set is the database's there), then move")]


#: the collations PostgreSQL's indexes over text sort by, with the
#: version of the library each is read from on this server; the
#: database's own default where the server can say its version (15+)
PG_ORDER_SQL = (
    "select c.collname, coalesce(pg_collation_actual_version(c.oid), '')"
    " from pg_collation c where c.oid in (select distinct a.attcollation"
    " from pg_attribute a join pg_index i on a.attrelid = i.indexrelid"
    " where a.attcollation not in (0, 100))")
PG_DEFAULT_ORDER_SQL = (
    "select 'the database default', coalesce("
    "pg_database_collation_actual_version(oid), '') from pg_database"
    " where datname = current_database()")


def _orders(e, side, db):
    got = dict(e.run_rule(side, db, PG_ORDER_SQL))
    try:
        got.update(dict(e.run_rule(side, db, PG_DEFAULT_ORDER_SQL)))
    except Exception:  # noqa: BLE001 - a server before 15 cannot say
        pass
    return {str(k): str(v) for k, v in got.items() if v}


def order_rows(hop, eng, db):
    """PostgreSQL to PostgreSQL: the collation library each side sorts its
    text indexes by (glibc, ICU), compared by version. A logical copy
    builds every index on the target by the target's own library, so a
    new release there is only said; a copy of the pages carries indexes
    sorted by the source's, which the target's reads in another order -
    wrong answers, not an error - and each is rebuilt after it."""
    from . import physical
    pair = _pair(eng)
    if _family(pair.src_engine.CANON_ENGINE) != "postgres" \
            or _family(pair.dst_engine.CANON_ENGINE) != "postgres":
        return []
    try:
        a = _orders(pair.src_engine, "src", db)
        b = _orders(pair.dst_engine, "dst", db)
    except Exception as e:  # noqa: BLE001 - unknown, said so
        return [_row("warn", f"{db}: the sort order libraries",
                     f"could not be read: {_last(e)} - unknown, not clean")]
    moved = sorted(f"{n} {a[n]} to {b[n]}" for n in a if n in b
                   and a[n] != b[n])
    if not moved:
        return []
    if physical.settings(hop):
        return [_row("warn", f"{db}: text indexes after a copy of the"
                             " pages",
                     "the collations the source's text indexes sort by"
                     f" are another release on the target ({'; '.join(moved[:4])}):"
                     " a copy of the pages carries indexes the target's"
                     " library orders otherwise, and they answer wrongly"
                     " without an error",
                     "after the copy, before the application reaches it:"
                     f" reindex database {db}; then migkit check")]
    return [_row("pass", f"{db}: the sort order libraries",
                 f"another release on the target ({'; '.join(moved[:4])}):"
                 " the logical copy builds every index there by the"
                 " target's own library, and the proof reads rows by key,"
                 " never by a range either side orders")]


def _found_rows(hop, db, p, f):
    from . import masking
    out = []
    for (t, c), (n, keys) in sorted(f.unfit.items()):
        dst = p.of(t)[c].dst
        out.append(_row("fail", f"{db}: {t}.{c}",
                        f"{n:,} values hold a character {dst.name} has no"
                        f" code for - by key: {_keys(keys)}; they would"
                        " arrive as ? or not at all",
                        "a target character set that holds them (UTF-8"
                        " holds every character), or those values changed"
                        " on the source first"))
    for (t, c), (n, ex) in sorted(f.doubled.items()):
        shown = ("(the hop masks this column's values)"
                 if masking.column(hop, t, c) else
                 "(e.g. " + ", ".join(f"{a!r} is {b!r}" for a, b in ex[:2])
                 + ")")
        if p.repair:
            out.append(_row("pass", f"{db}: {t}.{c}",
                            f"{n:,} values are UTF-8 stored twice {shown}:"
                            " each is repaired on the target after the"
                            " copy and proved as repaired; the genuine"
                            " accented text beside them is left alone"))
        else:
            out.append(_row("warn", f"{db}: {t}.{c}",
                            f"{n:,} values are UTF-8 stored twice {shown},"
                            " and the hop carries them as they are"
                            " (charset: {repair: none})",
                            "charset: {repair: double-encoded} repairs them"
                            " on the target and proves them repaired"))
    for t, key, n, ex, coll in f.merges:
        if n is None:
            out.append(_row("warn", f"{db}: {t} ({', '.join(key)})",
                            f"whether the target's collation {coll} takes"
                            f" two of its values for one is unknown ({ex})"
                            " - unknown, not clean"))
            continue
        shown = "; ".join(" and ".join(repr(k[0]) if len(k) == 1 else
                                       repr(tuple(k)) for k in group)
                          for group in (ex or [])[:EXAMPLES])
        out.append(_row("fail", f"{db}: {t} ({', '.join(key)})",
                        f"{n:,} groups of its unique values are one under"
                        f" the target's collation {coll}: {shown} - the"
                        " second of each would be refused, or overwrite"
                        " the first",
                        "a collation that keeps them apart for this key"
                        " (a binary one, or case- and accent-sensitive),"
                        " or the values made distinct on the source first"))
    if f.scanned and not f.unfit:
        out.append(_row("pass", f"{db}: the values",
                        f"{f.scanned:,} text values read: each has a code"
                        " in the target's character set"))
    return out


def plan_lines(hop, eng, db):
    """The plan's line of the character set change, or []."""
    if not db or not applies(hop, eng):
        return []
    try:
        p = plan(hop, eng, db)
    except Exception as e:  # noqa: BLE001 - said, the plan goes on
        return [f"  a character set change could not be read: {_last(e)}"]
    if not p.columns:
        return []
    how = []
    pair = _pair(eng)
    if p.declared and p.declared["to"] and _family(
            pair.dst_engine.CANON_ENGINE) == "mysql":
        how.append("converted on the target once its rows are in, by the"
                   " server's own conversion")
    if p.repair:
        how.append("double-encoded text repaired row by row")
    how.append("every row proved by key in the new character set")
    return [f"  a character set change: {p.words()} - " + "; ".join(how)]


# --- the conversion on the target --------------------------------------------------

def after_copy(hop, eng, db, log):
    """Once the rows are in: MySQL's own conversion of each table the hop
    asks a character set or collation of, the database's defaults after
    it, then the double-encoded values repaired. [lines said]."""
    if settings(hop) is None:
        # a hop that asks for no conversion repairs and converts nothing
        return []
    forget(eng, db)
    p = plan(hop, eng, db)
    if not p.columns:
        return []
    out = []
    dst = _pair(eng).dst_engine
    if p.declared and _family(dst.CANON_ENGINE) == "mysql":
        out += _mysql_convert(hop, dst, db, p, log)
        forget(eng, db)
        p = plan(hop, eng, db)
    if p.repair:
        out += repair(hop, eng, db, p, log)
    return out


def forget(eng, db):
    """The plan read again the next time it is asked: the target
    changed under it."""
    (eng.__dict__.get("_charset_plans") or {}).pop(db, None)
    pair = _pair(eng)
    for e in (eng, pair.src_engine, pair.dst_engine):
        e.__dict__.pop("_charsets_cache", None)


#: a text column's line in `show create table`: its name, its type, and
#: the character set and collation it names where they are not the
#: table's own
_MY_COLUMN = re.compile(
    r"^(\s*`(?:[^`]|``)+`\s+(?:char|varchar|tinytext|text|mediumtext|"
    r"longtext|enum|set)\b(?:\((?:[^()']|'(?:[^']|'')*')*\))?)"
    r"(\s+CHARACTER SET \w+)?(\s+COLLATE \w+)?(.*?),?$", re.I)


def _mysql_modify(ddl, cs, coll):
    """The clauses of one `ALTER TABLE` that put every text column of
    `ddl` (a `show create table`) in `cs` and `coll`, each keeping its own
    type, its NULL, its default and its comment - and the table's own
    default after them."""
    said = f" CHARACTER SET {cs}" + (f" COLLATE {coll}" if coll else "")
    out = []
    for line in ddl.splitlines()[1:]:
        m = _MY_COLUMN.match(line)
        if m:
            out.append("modify " + m.group(1).strip() + said + m.group(4))
    return out + [f"default{said}"]


def _convert_ways():
    """The ways a MySQL table's text is converted: column by column, each
    keeping its type - a value that no longer fits its type stops it, and
    nothing changes - or the server's conversion of the whole table,
    which widens a `text` to a `mediumtext` where the new character set
    needs the room."""
    from .decide import Rung
    return (Rung("columns", "each text column converted, its type kept",
                 frozenset({"converted"}),
                 because="the schema stays the source's but for the"
                         " character set"),
            Rung("table", "the table converted whole by the server",
                 frozenset({"converted"}), footprint=1,
                 because="where a value does not fit its type in the new"
                         " character set"))


def _mysql_convert(hop, dst, db, p, log):
    """Each table whose text is not yet in the character set and collation
    the hop asks for, converted by the target itself (`_convert_ways`),
    and the database's defaults after it."""
    from . import decide
    q = dst._quote_ident
    ddb = dst._d("dst", db)
    want = _mysql_declared(p.declared)
    cs, coll = want.name, want.collation
    said = f"character set {cs}" + (f" collate {coll}" if coll else "")
    done = []
    for t in p.tables():
        cols = p.of(t)
        target = cols[next(iter(cols))].target
        now = _reader(dst, "dst", db, target,
                      _text_columns(dst, "dst", db, target))
        if not any(c.name.lower() != cs.lower() or (
                coll and (c.collation or "").lower() != coll.lower())
                for c in now.values()):
            continue
        name = f"{q(ddb)}.{q(target)}"

        def run(choice, name=name, target=target):
            if choice.rung.name == "columns":
                ddl = dst.table_ddl("dst", db, target)
                dst.execute_ddl("dst", db, f"alter table {name} "
                                + ", ".join(_mysql_modify(ddl, cs, coll)))
            else:
                dst.execute_ddl("dst", db, f"alter table {name} convert to"
                                           f" {said}")
            return choice.rung.said
        choice, how = decide.attempt(f"{target}: converted to {cs}",
                                     _convert_ways(), run, log=log)
        decide.record(hop, choice)
        done.append(target)
        log(f"{target}: converted to {cs}" + (f" ({coll})" if coll else "")
            + f" by the target itself - {how}")
    dst.execute_ddl("dst", db, f"alter database {q(ddb)} {said}")
    return [f"{len(done)} tables converted to {cs}; the database's default"
            " for what is made later too"]


def text_plan(eng, db):
    """A `Plan` of every text column of the target's tables in the hop's
    scope, for the repair `sync` makes (`Engine._mojibake_repair`) where
    no character set changes."""
    pair = _pair(eng)
    dst = pair.dst_engine
    p = Plan(db, repair=True)
    none = Charset("")
    for t in dst.neutral_tables("dst", db):
        if not dst.in_scope(db, t):
            continue
        for n, _ in _text_columns(dst, "dst", db, t):
            p.columns.append(Column(t, t, n, none, none))
    return p


def to_repair(hop, eng, db, p):
    """{table.column: double-encoded values} on the target, read only."""
    from . import canon
    dst = _pair(eng).dst_engine
    out = {}
    for t in p.tables():
        cols = p.of(t)
        target = cols[next(iter(cols))].target
        declared = dict(dst.neutral_columns("dst", db, target))
        read = [(n, canon.type_class(dst.CANON_ENGINE, declared[n]))
                for n in sorted(cols) if n in declared]
        for rows in dst._every_row("dst", db, target, read):
            for r in rows:
                for (n, _), v in zip(read, r):
                    if double_encoded(v) is not None:
                        k = f"{target}.{n}"
                        out[k] = out.get(k, 0) + 1
    return out


def _undo_path(hop, db):
    return hop.report_dir(db) / "charset-repair.jsonl"


def repair(hop, eng, db, p=None, log=None):
    """Each double-encoded value on the target rewritten as what it really
    says, a pass at a time until none is left: read by key, the repaired
    rows read again just before they are written and written only where
    nothing changed them since (a row edited in between is left and said),
    the old values kept for the undo (`charset-repair.jsonl`). Through the
    neutral contract, so every target engine is repaired the same way."""
    from . import canon
    say = log or (lambda m: None)
    p = p or plan(hop, eng, db)
    pair = _pair(eng)
    dst = pair.dst_engine
    total, moved, lines = 0, 0, []
    for t in p.tables():
        cols = p.of(t)
        target = cols[next(iter(cols))].target
        key = list(dst.neutral_key("dst", db, target) or [])
        declared = dict(dst.neutral_columns("dst", db, target))
        texts = [c.column for c in cols.values() if c.column in declared
                 and c.column not in key]
        if not key or not texts:
            if texts:
                lines.append(f"{target}: no key on the target, so no value"
                             " can be repaired row by row")
            continue
        classes = {n: canon.type_class(dst.CANON_ENGINE, typ)
                   for n, typ in declared.items()}
        columns = [(n, classes.get(n)) for n in key + texts]
        for _ in range(100):
            found = []
            for rows in dst._every_row("dst", db, target, columns):
                for r in rows:
                    new = list(r)
                    hit = False
                    for i in range(len(key), len(r)):
                        v = double_encoded(r[i])
                        if v is not None:
                            new[i], hit = v, True
                    if hit:
                        found.append((tuple(r), new))
                    if len(found) >= REPAIR_CAP:
                        break
                if len(found) >= REPAIR_CAP:
                    break
            if not found:
                break
            done, left = _rewrite(hop, dst, db, target, key, columns, found)
            total += done
            moved += left
            if not done:
                break
        if total:
            say(f"{target}: double-encoded values repaired on the target")
    if total or moved:
        lines.append(f"{total:,} double-encoded rows repaired on the target"
                     + (f"; {moved:,} changed while they were read were left"
                        " as they are - the check names them" if moved
                        else ""))
    return lines


def _rewrite(hop, dst, db, target, key, columns, found):
    """(written, left): each found row read again by key and written
    repaired only where it is still what was read."""
    from . import canon
    classes = dict(columns)
    names = [n for n, _ in columns]
    keys = [r[:len(key)] for r, _ in found]
    now = {}
    for i in range(0, len(keys), BATCH):
        now.update(dst.neutral_rows_by_key("dst", db, target, columns, key,
                                           keys[i:i + BATCH]))
    write, left = [], 0
    path = _undo_path(hop, db)
    with path.open("a") as undo:
        for old, new in found:
            k = tuple(canon.render_value(classes[n], v)
                      for n, v in zip(key, old))
            there = now.get(k)
            if there is None or [canon.render_value(classes[n], v)
                                 for n, v in zip(names, there)] != \
                    [canon.render_value(classes[n], v)
                     for n, v in zip(names, old)]:
                left += 1
                continue
            undo.write(json.dumps({"table": target, "key": list(k),
                                   "before": dict(zip(names, (
                                       canon.render_value(classes[n], v)
                                       for n, v in zip(names, old)))),
                                   "after": dict(zip(names, new))},
                                  default=str) + "\n")
            write.append(new)
    from . import consolidate, decide
    for i in range(0, len(write), BATCH):
        part = write[i:i + BATCH]

        def run(choice, part=part):
            put = (dst.neutral_write if choice.rung.name == "bulk"
                   else dst.neutral_rewrite)
            put("dst", db, target, columns, part)
            return len(part)
        decide.attempt(f"{target}: repaired text", consolidate.write_ways(),
                       run)
    return len(write), left


# --- the proof ------------------------------------------------------------------------

def _hash(texts):
    h = hashlib.sha256()
    for t in texts:
        h.update(b"\x00" if t is None else b"\x01" + str(t).encode(
            "utf-8", "surrogatepass"))
        h.update(b"\x1f")
    return h.digest()[:16]


def _digest(hashes):
    """An order-free digest of a set of row hashes: their sum, so two
    sides read in different orders give one number where they hold the
    same rows."""
    return f"{sum(int.from_bytes(h, 'big') for h in hashes) % (1 << 128):032x}"


def _proofs(hop, eng, db):
    """[(Result, whole)] of the plan's tables: each row's canonical text
    on the target held to the source's as the plan converts it, both read
    by key and compared by key - held in this process as a hash a row,
    since the two sides order text keys by their own collations; a table
    without a key, as a multiset of rows. `whole`: equal, with every
    column of the table compared."""
    from collections import Counter

    from .engines.base import Result
    p = plan(hop, eng, db)
    pair = _pair(eng)
    src, dst = pair.src_engine, pair.dst_engine
    out = []
    for t in p.tables():
        cols = p.of(t)
        target = cols[next(iter(cols))].target
        try:
            sc, dc, notes = pair._comparable_columns(db, src, t, dst, target)
        except Exception as e:  # noqa: BLE001 - said as the table's error
            out.append((Result("charset", f"{db}.{t}", "error",
                               f"could not be read: {_last(e)}"), False))
            continue
        names = [n for n, _ in sc]
        key = [k for k in (src.neutral_key("src", db, t) or [])]
        keyed = bool(key) and set(key) <= set(names)
        at = [names.index(k) for k in key] if keyed else []
        where = hop.row_filter(db, *str(t).split("."))
        a, b, unfit = ({} if keyed else Counter()), \
            ({} if keyed else Counter()), 0
        for rows in src._every_row("src", db, t, sc, where):
            for r in rows:
                row = list(r)
                for i, (n, _) in enumerate(sc):
                    c = cols.get(n)
                    if c is not None:
                        try:
                            row[i] = convert(row[i], c.src, c.dst, p.repair)
                        except Unfit:
                            unfit += 1
                texts = pair._row_text(sc, row)
                if keyed:
                    a[tuple(texts[i] for i in at)] = _hash(texts)
                else:
                    a[_hash(texts)] += 1
        for rows in dst._every_row("dst", db, target, dc):
            for r in rows:
                texts = pair._row_text(dc, list(r))
                if keyed:
                    b[tuple(texts[i] for i in at)] = _hash(texts)
                else:
                    b[_hash(texts)] += 1
        if keyed:
            missing = [k for k in a if k not in b]
            extra = [k for k in b if k not in a]
            changed = [k for k in a if k in b and a[k] != b[k]]
            da, db_ = _digest(a.values()), _digest(b.values())
            na = len(a)
        else:
            missing = list((a - b).elements())
            extra = list((b - a).elements())
            changed = []
            da = _digest([h for h, n in a.items() for _ in range(n)])
            db_ = _digest([h for h, n in b.items() for _ in range(n)])
            na = sum(a.values())
        whole = not notes
        said = []
        if unfit:
            said.append(f"{unfit:,} values the target's character set has"
                        " no code for")
        if missing:
            said.append(f"{len(missing):,} rows missing on the target"
                        + (f" ({_keys(missing[:EXAMPLES])})" if keyed
                           else ""))
        if extra:
            said.append(f"{len(extra):,} rows the source does not have"
                        + (f" ({_keys(extra[:EXAMPLES])})" if keyed
                           else ""))
        if changed:
            said.append(f"{len(changed):,} rows differ from the source's as"
                        f" converted ({_keys(changed[:EXAMPLES])})")
        cover = "every column" if whole else \
            f"{len(names)} columns ({'; '.join(notes)[:120]})"
        if said:
            out.append((Result("charset", f"{db}.{t}", "diff",
                               "; ".join(said) + f" - {cover} compared, the"
                               " source's text as the plan converts it"
                               f" (digests {da} and {db_})", "",
                               f"migkit move {hop.name} --mode full --go"),
                        False))
        else:
            out.append((Result("charset", f"{db}.{t}", "ok",
                               f"{na:,} rows, {cover}, the same in"
                               f" {cols[next(iter(cols))].dst.name}: the"
                               f" converted text's digest {da} on both"
                               " sides"
                               + (" (double-encoded values as repaired)"
                                  if p.repair else "")), whole))
    return out


def prove(hop, eng, db):
    """[Result] of the plan's tables (`_proofs`)."""
    return [r for r, _ in _proofs(hop, eng, db)]


def check(hop, eng, db, results):
    """`check`'s results of `db` with the conversion's proof in place of
    the data check of each table the proof covers whole and finds equal -
    a data check of each side's own bytes, or of ranges each side orders
    by its own collation, reads a correct conversion as different - and
    beside it everywhere else: a table the proof finds different, or
    covers only in part, keeps the data check's word too."""
    if not applies(hop, eng):
        return results
    try:
        p = plan(hop, eng, db)
    except Exception:  # noqa: BLE001 - the data check stands as it is
        return results
    if not p.columns:
        return results
    proved = _proofs(hop, eng, db)
    whole = {r.scope.split(".", 1)[1] for r, w in proved if w}
    leaves = {t.split(".")[-1]: t for t in whole}
    declared = _declared_columns(hop, eng, db, p)
    kept = []
    for r in results:
        if _dumps_differ(r, db) and declared is not None \
                and declared.status == "ok" \
                and _schema_as_planned(hop, db):
            kept.append(dict(r, status="ok", detail=(
                "differs from the source's only by the character set and"
                " collation the hop converts to, every text column of"
                " them held to it below")))
            continue
        if not isinstance(r, dict) or r.get("check") != "data":
            kept.append(r)
            continue
        scope = str(r.get("scope", ""))
        table = scope.split(".", 1)[1] if scope.startswith(f"{db}.") else ""
        if table and (table in whole or table in leaves):
            # a pair names a table by its leaf
            continue
        detail = str(r.get("detail", ""))
        if scope == db and r.get("status") == "diff" \
                and detail.startswith("tables differ: "):
            # a database's one result naming the tables that differ: the
            # ones the proof covers whole and finds equal are its to say
            named = detail[len("tables differ: "):].split(" (", 1)[0]
            left = [n.strip() for n in named.split(",") if n.strip()
                    and n.strip() not in whole
                    and n.strip().split(".")[-1] not in leaves]
            if not left:
                continue
            r = dict(r, detail=f"tables differ: {', '.join(left)}"
                     + (" (" + detail.split(" (", 1)[1]
                        if " (" in detail else ""))
        kept.append(r)
    return kept + [r.__dict__ for r, _ in proved] + (
        [declared.__dict__] if declared is not None else [])


#: what names a character set or a collation in a MySQL schema dump
_MY_WORDS = re.compile(r"\s+(?:DEFAULT\s+)?(?:CHARACTER\s+SET|CHARSET)"
                       r"\s*=?\s*\w+|\s+COLLATE\s*=?\s*\w+", re.I)


def _dumps_differ(r, db):
    """Whether `r` is the schema check's comparison of the two schema
    dumps, saying they differ - that one only: the objects each side has
    and a second schema reading are held to by their own, whatever the
    dumps say."""
    return (isinstance(r, dict) and r.get("check") == "schema"
            and r.get("scope") == db and r.get("status") == "diff"
            and str(r.get("report") or "").endswith("schema.diff"))


def _schema_as_planned(hop, db):
    """Whether the two schema dumps the check wrote differ by nothing but
    the words that name a character set or a collation."""
    import difflib
    d = hop.report_dir(db)
    try:
        a = (d / "schema-src.sql").read_text().splitlines()
        b = (d / "schema-dst.sql").read_text().splitlines()
    except OSError:
        return False
    a = [_MY_WORDS.sub("", line) for line in a]
    b = [_MY_WORDS.sub("", line) for line in b]
    return not any(line[:1] in "+-" and not line.startswith(("+++", "---"))
                   for line in difflib.unified_diff(a, b, lineterm=""))


def _declared_columns(hop, eng, db, p):
    """A Result: every text column of the plan on a MySQL target in the
    character set and collation the hop asks for - what a schema check
    that sets those words aside leaves to this - or None where the hop
    asks for none there."""
    from .engines.base import Result
    want = _mysql_declared(p.declared)
    if want is None:
        return None
    dst = _pair(eng).dst_engine
    if _family(dst.CANON_ENGINE) != "mysql":
        return None
    wrong = []
    for t in p.tables():
        cols = p.of(t)
        target = cols[next(iter(cols))].target
        now = _reader(dst, "dst", db, target,
                      _text_columns(dst, "dst", db, target))
        for n, c in sorted(now.items()):
            if c.name.lower() != want.name.lower() or (
                    want.collation and (c.collation or "").lower()
                    != want.collation.lower()):
                wrong.append(f"{target}.{n} ({c.name}"
                             + (f", {c.collation}" if c.collation else "")
                             + ")")
    if wrong:
        return Result("charset", f"{db} columns", "diff",
                      f"{len(wrong)} text columns are not in"
                      f" {want.name}" + (f" ({want.collation})"
                                         if want.collation else "")
                      + ": " + ", ".join(wrong[:6]), "",
                      f"migkit move {hop.name} --mode full --go")
    return Result("charset", f"{db} columns", "ok",
                  f"every text column in {want.name}"
                  + (f" ({want.collation})" if want.collation else ""))
