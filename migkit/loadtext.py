"""The source renders the target's load format in SQL (backlog R20.14).

MySQL into PostgreSQL: one `SELECT` on MySQL gives each row already as a
line of PostgreSQL's COPY text - fields apart by a tab, `\\N` for NULL, a
backslash, tab, newline and carriage return in a value written as `\\\\`,
`\\t`, `\\n` and `\\r`, bytes as `\\\\x` and their hex digits, times and
decimals in the form PostgreSQL reads back to the same value - and the
lines go as they come into `COPY ... FROM STDIN` on PostgreSQL. Both
servers do the work; between them the bytes are only passed on. The other
way, PostgreSQL renders MySQL's load format (`copy_out`, below).

Only a column whose class and declared types on both sides are listed in
`SOURCE_TYPES` and `TARGET_TYPES` (`PG_SOURCE_TYPES` and
`MYSQL_TARGET_TYPES` the other way) goes this way, and each is there because
`tests/test_the_source_renders_the_targets_load_format.py` held its
rendering to what PostgreSQL stores over every byte and every value of its
class it could generate: every one of the 256 byte values in a `bytea`,
every code point of Unicode in a text (the four escaped ones among them,
next to each other and at either end), the ends of every integer and
decimal range, every double's shortest text, times at the edges of the
day and of the calendar, JSON with escapes and four-byte characters. A
table with any other column is refused here, with the reason, and goes
another way.

A float is rendered as the double it is (`canon.read_expr`): MySQL's own
text of a FLOAT has six significant digits, and a value of 16777216 would
land as 16777200. As a double, its shortest text reads back as the same
double into `double precision` and as the same float into `real`.

What the class list leaves out, and why:
* a double into PostgreSQL's `real`: rounded on the way;
* a text into a PostgreSQL type that parses it (`uuid`, `inet`, `xml`, an
  enum): that type's input rules, not this escaping, decide what lands;
* a time with a zone and anything spatial: no class carries them.

A value or a line longer than the server's `max_allowed_packet` comes
back from MySQL's string functions as NULL, with a warning and nothing
else. So a field is `\\N` only where the column is NULL, a rendering that
answered NULL makes the whole line NULL (`concat`), and such a line is
written as the byte `0xFF`, which is not UTF-8: PostgreSQL stops the COPY
on it and the range goes another way, instead of a value landing as NULL.
"""
import re

#: MySQL's declared types, by the class they belong to, whose rendering
#: here is proved
SOURCE_TYPES = {
    "integer": {"tinyint", "smallint", "mediumint", "int", "integer",
                "bigint", "bit", "year"},
    "decimal": {"decimal", "numeric"},
    "float": {"double", "double precision", "real", "float"},
    "text": {"char", "varchar", "tinytext", "text", "mediumtext",
             "longtext", "enum", "set"},
    "bytes": {"binary", "varbinary", "tinyblob", "blob", "mediumblob",
              "longblob"},
    "date": {"date"},
    "timestamp": {"datetime", "timestamp"},
    "time": {"time"},
    "json": {"json"},
}

#: PostgreSQL's declared types a class lands in by this way
TARGET_TYPES = {
    "integer": {"smallint", "integer", "bigint", "numeric"},
    "decimal": {"numeric"},
    "float": {"double precision", "real"},
    "text": {"text", "character varying", "character", "bpchar"},
    "bytes": {"bytea"},
    "date": {"date"},
    "timestamp": {"timestamp without time zone", "timestamp with time zone"},
    "time": {"time without time zone"},
    "json": {"json", "jsonb"},
}

#: the four characters COPY's text format reads specially, each with what
#: stands for it, as MySQL literals that mean the same whatever the
#: session's `sql_mode` says of backslashes; the backslash first, so the
#: ones written for the others are not escaped again
_ESCAPES = (("5C", "5C5C"), ("09", "5C74"), ("0A", "5C6E"), ("0D", "5C72"))
_TAB = "_utf8mb4 X'09'"
_NULL = "_utf8mb4 X'5C4E'"
#: not UTF-8: a line MySQL could not build stops the COPY
_BROKEN = "X'FF'"


def base(declared):
    """A declared type without its numbers or `unsigned`, lower-cased:
    `int(10) unsigned` -> `int`, `timestamp(6) with time zone` ->
    `timestamp with time zone`."""
    text = re.sub(r"\([^)]*\)", "", str(declared or "").lower())
    words = [w for w in text.split() if w not in ("unsigned", "zerofill")]
    return " ".join(words)


def refused(columns, pair=("mysql", "postgres")):
    """Why this way cannot carry `columns` - [(source name, source type,
    target name, target type, class)] - from one engine of `pair` into
    the other, or None where it can."""
    if tuple(pair) == ("postgres", "mysql"):
        return _refused_into_mysql(columns)
    for name, stype, _, dtype, cls in columns:
        s, d = base(stype), base(dtype)
        if s not in SOURCE_TYPES.get(cls, ()):
            return (f"{name} is {stype}, whose rendering as PostgreSQL's load"
                    " format is not proved")
        if d not in TARGET_TYPES.get(cls, ()):
            return (f"{name} lands in {dtype}, whose own input rules and not"
                    " the load format's decide what it stores")
        if cls == "float" and s != "float" and d == "real":
            return (f"{name} is a double, which a {dtype} holds only"
                    " rounded")
    return None


def _escaped(sql):
    for was, now in _ESCAPES:
        sql = f"replace({sql}, _utf8mb4 X'{was}', _utf8mb4 X'{now}')"
    return sql


def cell(column, declared, cls):
    """The MySQL expression giving one field of the line: the value in
    PostgreSQL's COPY text, or `\\N`."""
    from . import canon
    c = "`" + str(column).replace("`", "``") + "`"
    s = base(declared)
    if cls == "integer":
        # as a number: a BIT's own text is its bytes, a YEAR of 0 `0000`
        got = f"cast(({c} + 0) as char)"
    elif cls in ("decimal", "float"):
        # the digits the server holds, and a double's shortest text that
        # reads back to the same double - a FLOAT read as the double it is
        got = f"cast({canon.read_expr('mysql', c, cls)} as char)"
    elif cls == "bytes":
        # `\\x` in the line is `\x` once COPY has read it: bytea's hex form
        got = f"concat(_utf8mb4 X'5C5C78', hex({c}))"
    elif cls == "date":
        got = f"date_format({c}, '%Y-%m-%d')"
    elif cls == "timestamp":
        got = f"date_format({c}, '%Y-%m-%d %H:%i:%s.%f')"
        if s == "timestamp":
            # an instant, read at +00:00 (`SESSION`), said to be one; a
            # column without a zone reads the zone and leaves it
            got = f"concat({got}, '+00')"
    elif cls == "time":
        got = f"time_format({c}, '%H:%i:%s.%f')"
    elif cls == "json":
        got = _escaped(f"cast({c} as char)")
    else:
        got = _escaped(f"convert({c} using utf8mb4) collate utf8mb4_bin")
    # `\\N` only where the value is NULL: a rendering that answers NULL for
    # a value that is there - a result past `max_allowed_packet` - makes
    # the whole line NULL (`select`) and stops the COPY
    return f"if({c} is null, {_NULL}, {got})"


#: what the reading session is set to: instants read as UTC shows them,
#: and every text in the one encoding the line is in
SESSION = ("set session time_zone = '+00:00',"
           " character_set_results = utf8mb4")


def select(table, columns, where=None):
    """The statement answering one line of COPY text a row: `columns` are
    [(source name, source type, class)] in the target's column order,
    `table` the source's qualified name, `where` in the source's SQL."""
    parts = f", {_TAB}, ".join(cell(n, t, c) for n, t, c in columns)
    line = f"coalesce(concat({parts}), {_BROKEN})"
    return (f"select {line} from {table}"
            + (f" where {where}" if where else ""))


# --- PostgreSQL renders MySQL's load ----------------------------------------
#
# PostgreSQL into MySQL: `COPY (SELECT ...) TO STDOUT` on PostgreSQL, each
# column rendered in SQL as MySQL's pinned `load data local` reads it
# (`engines.mysql._load_feed`), and the bytes fed to the load as they come.
# COPY's own text format is the load's but for two escapes: it writes a
# form feed and a vertical tab as `\f` and `\v`, which the load reads as
# the letters. So a text goes as the hex of its UTF-8 (`unhex` on the way
# in), which no escape touches; a JSON document as its text, which holds
# no raw control character JSON does not escape itself; the rest as digits
# and the times' own forms. NULL is COPY's `\N` and the load's alike.

#: PostgreSQL's declared types, by class, rendered here for MySQL's load
PG_SOURCE_TYPES = {
    "integer": {"smallint", "integer", "bigint"},
    "decimal": {"numeric"},
    "float": {"double precision", "real"},
    "boolean": {"boolean"},
    "text": {"text", "character varying", "character", "bpchar"},
    "uuid": {"uuid"},
    "bytes": {"bytea"},
    "date": {"date"},
    "timestamp": {"timestamp without time zone", "timestamp with time zone"},
    "time": {"time without time zone"},
    "json": {"json", "jsonb"},
}

#: MySQL's declared types each class lands in by this way: not a CHAR or a
#: BINARY, which pad what they hold and give it back cut
MYSQL_TARGET_TYPES = {
    "integer": {"tinyint", "smallint", "mediumint", "int", "integer",
                "bigint"},
    "boolean": {"tinyint", "smallint", "mediumint", "int", "integer",
                "bigint"},
    "decimal": {"decimal", "numeric"},
    "float": {"double", "float"},
    "text": {"varchar", "tinytext", "text", "mediumtext", "longtext"},
    # a UUID's text has no trailing spaces for a CHAR to cut
    "uuid": {"char", "varchar", "tinytext", "text", "mediumtext",
             "longtext"},
    "bytes": {"varbinary", "tinyblob", "blob", "mediumblob", "longblob"},
    "date": {"date"},
    "timestamp": {"datetime", "timestamp"},
    "time": {"time"},
    "json": {"json"},
}


def _refused_into_mysql(columns):
    for name, stype, _, dtype, cls in columns:
        s, d = base(stype), base(dtype)
        if s not in PG_SOURCE_TYPES.get(cls, ()):
            return (f"{name} is {stype}, whose rendering as MySQL's load"
                    " format is not proved")
        if d not in MYSQL_TARGET_TYPES.get(cls, ()):
            return (f"{name} lands in {dtype}, whose own input rules and not"
                    " the load format's decide what it stores")
        if cls == "float" and s != "real" and d == "float":
            return (f"{name} is a double, which a {dtype} holds only"
                    " rounded")
    return None


def mysql_field(column, declared, cls):
    """(the PostgreSQL expression giving one field of MySQL's load, how
    the load takes it back: `hex` or `text`)."""
    from . import canon
    c = '"' + str(column).replace('"', '""') + '"'
    s = base(declared)
    if cls in ("integer", "decimal"):
        return f"({canon.read_expr('postgres', c, cls)})::text", "text"
    if cls == "float":
        # the double's shortest text that reads back as the same double
        # (`extra_float_digits`, `PG_SESSION`), a `real` as the double it is
        return f"({canon.read_expr('postgres', c, cls)})::text", "text"
    if cls == "boolean":
        return f"{c}::int::text", "text"
    if cls == "uuid":
        return f"{c}::text", "text"
    if cls == "bytes":
        return f"encode({c}, 'hex')", "hex"
    if cls == "date":
        return f"to_char({c}, 'YYYY-MM-DD')", "text"
    if cls == "timestamp":
        at = f"({c} at time zone 'UTC')" if s == "timestamp with time zone" \
            else c
        return f"to_char({at}, 'YYYY-MM-DD HH24:MI:SS.US')", "text"
    if cls == "time":
        return f"to_char({c}, 'HH24:MI:SS.US')", "text"
    if cls == "json":
        return f"{c}::text", "text"
    return f"encode(convert_to({c}::text, 'UTF8'), 'hex')", "hex"


#: what the PostgreSQL session reading for MySQL's load is set to: times
#: as UTC, a double as its shortest exact text, one encoding
PG_SESSION = ("-c TimeZone=UTC -c DateStyle=ISO -c extra_float_digits=3"
              " -c client_encoding=UTF8")


def copy_out(table, columns, where=None):
    """(the `COPY ... TO STDOUT` giving MySQL's load a line a row, [how
    the load takes each field back]): `columns` are [(source name, source
    type, class)] in the target's column order, `table` the source's
    qualified name, `where` in the source's SQL."""
    fields = [mysql_field(n, t, c) for n, t, c in columns]
    pick = ", ".join(e for e, _ in fields)
    return (f"copy (select {pick} from {table}"
            + (f" where {where}" if where else "") + ") to stdout",
            [k for _, k in fields])
