"""One text rendering of a value that two different engines both produce.

migkit already compares rows inside the database: each side folds its rows
into a checksum and only the checksum crosses the network. That works because
both sides are the same software and render a value the same way. Across
engines they do not, and the differences are small enough to look like data
loss:

    value                    MySQL 8                PostgreSQL 16
    -----------------------  ---------------------  --------------------
    boolean true             1                      true
    1e20 (double)            1e20                   1e+20
    0.000001 (double)        0.000001               1e-06
    2026-01-01 00:00:00.0    ...00:00:00.000000     ...00:00:00
    00:00:00.000000 (time)   00:00:00.000000        00:00:00
    json {"b":1,"a":2}       {"a": 2, "b": 1}       {"a": 2, "b": 1}

Every line above was read off a running container. Four of the six disagree,
and a cross-engine comparison built on the engines' own text would report a
table of correct data as entirely different - which is worse than useless,
because after the first false alarm nobody reads the next one.

So this file is a written-down rendering for each class of value, and a pair
of SQL expressions per engine that produce it. The point is not that the
rendering is pretty. It is that it is **the same on both sides**, and that
where it cannot be, the value is called uncomparable instead of being made to
look equal.

Binary floats are where that last sentence earns its place
--------------------------------------------------------

The obvious fix for `1e+20` vs `1e20` is to cast both sides to a fixed-scale
decimal. Measured, that does work across the middle of the range, and it fixes
the `1e-06` band that plain text gets wrong. It also does this, on MySQL 8:

    double            cast(d as decimal(65,20))
    ----------------  --------------------------------------------------
    1e45              999999999999999999999999999999999999999999999.99...
    1e46              999999999999999999999999999999999999999999999.99...
    1e300             999999999999999999999999999999999999999999999.99...
    1.797...e308      999999999999999999999999999999999999999999999.99...

Four different numbers, one string, no warning. PostgreSQL renders each of
them in full. Adopting the decimal cast everywhere would have made every
double above 1e45 compare equal to every other one - a false negative
manufactured by the comparison itself, in the direction this codebase treats
as the worst kind of bug.

The same happens underneath: both engines round anything below 1e-20 to
`0.00000000000000000000`, so 5e-324 and 1e-30 would compare equal.

What the engines' own shortest text *does* get right is exactly those two
outer bands - measured identical on both sides for 1e45, 1e46, 1e300, -1e300,
1.7976931348623157e308, 1e-30 and 5e-324. So the rule is banded: fixed decimal
where it is exact, shortest text where it is not, and the seam sits where each
form stops being trustworthy rather than where it is convenient.

Anything a rendering cannot represent at all - Infinity and NaN, which
PostgreSQL stores and MySQL refuses - is reported as uncomparable and counted.
A row migkit did not verify is a number in the report, never a silence.
"""

# Neutral classes. An engine's declared type is mapped onto one of these and
# the rendering is chosen from the class, so adding an engine is a mapping
# rather than a new set of pairwise rules.
CLASSES = ("integer", "decimal", "float", "boolean", "text", "bytes",
           "date", "timestamp", "time", "json")

# Where the fixed-decimal rendering of a binary float is exact on both sides.
#
# The high end is MySQL's: `decimal(65,20)` leaves 45 integer digits, and a
# double past that saturates to the maximum instead of erroring. The low end
# is the scale itself: below 1e-20 both engines round to zero, so two
# different tiny values would render the same.
FLOAT_SCALE = 20
FLOAT_MIN = "1e-20"
FLOAT_MAX = "1e45"

# What a value that cannot be rendered at all is replaced with. It is not a
# value: rows carrying it are counted and excluded from the digest, because
# two uncomparable values rendering to the same marker would compare equal.
#
# Not a NUL byte, which would be the obvious choice for something no real
# value contains - PostgreSQL rejects NUL in text outright, so an expression
# carrying one fails the whole query rather than marking one row. 0x1F is a
# control character no rendering here produces and both engines accept.
UNCOMPARABLE = "\x1funcomparable"


class _Absent:
    """A field that is not there, as distinct from one holding NULL.

    MongoDB keeps those apart and every SQL engine here collapses them, so a
    value moving out of MongoDB carries which one it was and the mover
    decides - and counts - what the target can express. Reading absence as
    NULL at the point it is read would throw the distinction away before
    anyone could be told it was thrown away.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "ABSENT"

    def __bool__(self):
        return False


ABSENT = _Absent()

# declared type (lower-cased, without length/precision) -> neutral class
TYPES = {
    "mysql": {
        "tinyint": "integer", "smallint": "integer", "mediumint": "integer",
        "int": "integer", "integer": "integer", "bigint": "integer",
        "bit": "integer", "year": "integer",
        "decimal": "decimal", "numeric": "decimal",
        "float": "float", "double": "float", "real": "float",
        "char": "text", "varchar": "text", "tinytext": "text",
        "text": "text", "mediumtext": "text", "longtext": "text",
        "enum": "text", "set": "text",
        "binary": "bytes", "varbinary": "bytes", "tinyblob": "bytes",
        "blob": "bytes", "mediumblob": "bytes", "longblob": "bytes",
        "date": "date",
        "datetime": "timestamp", "timestamp": "timestamp",
        "time": "time",
        "json": "json",
    },
    "postgres": {
        "smallint": "integer", "integer": "integer", "bigint": "integer",
        "int2": "integer", "int4": "integer", "int8": "integer",
        "smallserial": "integer", "serial": "integer", "bigserial": "integer",
        "numeric": "decimal", "decimal": "decimal", "money": "decimal",
        "real": "float", "double precision": "float", "float4": "float",
        "float8": "float",
        "boolean": "boolean", "bool": "boolean",
        "character": "text", "character varying": "text", "varchar": "text",
        "bpchar": "text", "char": "text", "text": "text", "name": "text",
        "uuid": "text", "inet": "text", "cidr": "text", "macaddr": "text",
        "xml": "text",
        "bytea": "bytes",
        "date": "date",
        "timestamp without time zone": "timestamp", "timestamp": "timestamp",
        "timestamp with time zone": "timestamp", "timestamptz": "timestamp",
        "time without time zone": "time", "time": "time",
        "json": "json", "jsonb": "json",
    },
    # SQLite's declared type is a hint rather than a guarantee - a column
    # declared INTEGER will hold text if something writes text to it. Only
    # the classes whose rendering does not depend on the declaration are
    # mapped; `decimal`, the date types and `json` are deliberately absent
    # until migkit measures what they actually hold, and an unmapped type is
    # reported rather than guessed at.
    "sqlite": {
        "int": "integer", "integer": "integer", "tinyint": "integer",
        "smallint": "integer", "mediumint": "integer", "bigint": "integer",
        "int2": "integer", "int8": "integer", "boolean": "integer",
        "real": "float", "double": "float", "double precision": "float",
        "float": "float",
        "character": "text", "varchar": "text", "varying character": "text",
        "nchar": "text", "native character": "text", "nvarchar": "text",
        "text": "text", "clob": "text",
        "blob": "bytes",
        # SQLite has no date or time type. A column declared `datetime` holds
        # whatever was written into it - text, a number of seconds, a Julian
        # day - and the declared name is a hint to the reader rather than a
        # promise from the engine. So these are `text`: what is stored is
        # compared as what it is, which for a table migkit created is the
        # same canonical text the other engines print. The alternative was
        # leaving the column out of the comparison entirely.
        #
        # Measured, the affinity rules keep that honest. These names carry
        # NUMERIC affinity, and `2024-01-02 03:04:05.000006` cannot be turned
        # into a number, so it stays text and renders as itself. A column
        # holding `1704164645` comes back as the integer it was converted to
        # and reads as a difference against a PostgreSQL timestamp - which is
        # what it is. `numeric` itself stays unmapped below for the other
        # half of the same measurement: `1.50` written into one is stored as
        # the real 1.5, and rendering that beside a PostgreSQL `numeric(10,2)`
        # would invent a difference migkit cannot resolve.
        "date": "text", "datetime": "text", "timestamp": "text",
        "time": "text",
    },
    # MongoDB reports the BSON type of what is actually stored rather than a
    # declared one, and a field can hold more than one across a collection.
    # `type_class` takes the set with `null` and `missing` removed; a field
    # left holding two real types is genuinely ambiguous and comes back
    # unmapped rather than resolved to whichever is more common.
    #
    # `object` and `array` are absent on purpose: migkit has not measured a
    # rendering for them that another engine reproduces, and claiming one
    # would be claiming the comparison works.
    #
    # `decimal` is here because it has now been measured. A Decimal128 keeps
    # the scale it was given - `1.50` comes back `1.50` - so once it is taken
    # through `to_decimal()` the same fixed-point text comes out as
    # PostgreSQL's `::text` and MySQL's `cast(... as char)` produce.
    "mongodb": {
        "int": "integer", "long": "integer",
        "double": "float",
        "decimal": "decimal",
        "bool": "boolean",
        "string": "text", "objectid": "text", "symbol": "text",
        "bindata": "bytes",
        "date": "timestamp",
    },
}

# MySQL has no boolean: `tinyint(1)` is the convention and holds -128..127.
# It is mapped to `integer` above rather than `boolean` on purpose - a MySQL
# column holding 2 is not a boolean, and rendering it as one would hide the
# difference from a PostgreSQL side that cannot hold 2 at all. PostgreSQL's
# real boolean renders to 0/1 to meet it.


def type_class(engine, declared):
    """Neutral class for an engine's declared type, or None when unmapped.

    None rather than a guess: an unmapped type is reported as uncomparable by
    the caller, which is a line in the report. Guessing `text` would compare
    two renderings nobody checked agree.
    """
    if not declared:
        return None
    base = str(declared).strip().lower()
    for cut in ("(", "[", " ("):
        if cut in base:
            base = base.split(cut)[0].strip()
    if base.endswith(" unsigned"):
        base = base[:-9].strip()
    if "|" in base:
        # a set of types rather than one, which is how a schemaless engine
        # answers. `null` and `missing` say nothing about what the field
        # holds when it holds something, so they are dropped; anything left
        # over one real type is ambiguous and stays unmapped.
        seen = {t.strip() for t in base.split("|")} - {"null", "missing", ""}
        if len(seen) != 1:
            return None
        base = seen.pop()
    return TYPES.get(engine, {}).get(base)


def _mysql(col, cls):
    c = f"`{col}`"
    if cls == "float":
        # banded: exact decimal in the middle, the engine's own shortest text
        # outside it, because the decimal cast saturates above 1e45 and
        # underflows below 1e-20 - both silently, both to a shared string
        return (f"case when {c} is null then null"
                f" when {c} <> 0 and (abs({c}) >= {FLOAT_MAX}"
                f" or abs({c}) < {FLOAT_MIN}) then cast({c} as char)"
                f" else cast(cast({c} as decimal(65,{FLOAT_SCALE})) as char)"
                f" end")
    if cls == "bytes":
        return f"hex({c})"
    if cls == "timestamp":
        return f"date_format({c}, '%Y-%m-%d %H:%i:%s.%f')"
    if cls == "time":
        return f"time_format({c}, '%H:%i:%s.%f')"
    return f"cast({c} as char)"


def _postgres(col, cls):
    c = f'"{col}"'
    if cls == "float":
        # `e+20` -> `e20` is what makes the outer band agree with MySQL, and
        # Infinity/NaN have no MySQL counterpart at all, so they are marked
        # rather than rendered
        outer = (f"replace({c}::text, 'e+', 'e')")
        # `x <> x` is the portable NaN test everywhere except here:
        # PostgreSQL defines NaN as equal to itself so it can index and sort
        # it, so that test is always false and a NaN would fall through to
        # the renderer and come out as the literal text `NaN`. Measured.
        return (f"case when {c} is null then null"
                f" when {c} = 'Infinity'::float8 or {c} = '-Infinity'::float8"
                f" or {c} = 'NaN'::float8 then '{UNCOMPARABLE}'"
                f" when {c} <> 0 and (abs({c}) >= {FLOAT_MAX}"
                f" or abs({c}) < {FLOAT_MIN}) then {outer}"
                # through the text form, not straight to numeric. PostgreSQL's
                # `float8::numeric` cast is lossier than its own `::text`:
                # measured, 1.0/7 renders as 0.14285714285714285 as text and
                # as 0.142857142857143 through numeric - fifteen significant
                # digits against seventeen. MySQL's `cast(d as decimal)` goes
                # via the shortest round-trip decimal, so the two only meet
                # when this one does too, and the values that exposed it were
                # the first test data with a long decimal expansion.
                f" else round(({c}::text)::numeric, {FLOAT_SCALE})::text"
                f" end")
    if cls == "boolean":
        return f"{c}::int::text"
    if cls == "bytes":
        return f"upper(encode({c}, 'hex'))"
    if cls == "timestamp":
        return f"to_char({c}, 'YYYY-MM-DD HH24:MI:SS.US')"
    if cls == "time":
        return f"to_char({c}, 'HH24:MI:SS.US')"
    if cls == "json":
        # a `json` column keeps the text it was handed, spacing and duplicate
        # keys and all; `jsonb` is the normalised form MySQL's JSON matches
        return f"{c}::jsonb::text"
    return f"{c}::text"


def _sqlite(col, cls):
    """SQLite renders through a function migkit registers on the connection.

    There is no server to push the rendering into: SQLite runs inside the
    process that opened the file, so "computed in the database" and "computed
    here" are the same sentence. What still has to hold is that the text it
    produces is identical to what the other engines produce, which is what
    `render_value` below is for and what the cross-engine test measures.
    """
    return f'migkit_canon("{col}", \'{cls}\')'


BUILDERS = {"mysql": _mysql, "postgres": _postgres, "sqlite": _sqlite}


def _float_text(d):
    """The banded float rendering, in Python.

    Same rule as the SQL: a fixed twenty-place decimal where that is exact on
    every engine, the shortest round-trip text outside it. The decimal is
    taken from `repr`, not from formatting the binary value directly - both
    engines convert through the shortest decimal representation first, and
    `'%.20f' % 0.05` would print the binary error they do not.
    """
    import math
    from decimal import Decimal, localcontext
    if math.isinf(d) or math.isnan(d):
        return UNCOMPARABLE
    if d == 0:
        # -0.0 formats with a leading minus that neither engine produces
        d = 0.0
    if abs(d) >= float(FLOAT_MAX) or (d != 0 and abs(d) < float(FLOAT_MIN)):
        return repr(d).replace("e+", "e")
    with localcontext() as ctx:
        ctx.prec = 80
        return format(Decimal(repr(d)).quantize(Decimal(1).scaleb(-FLOAT_SCALE)),
                      "f")


def render_value(cls, value):
    """One value as the canonical text, or None when it is NULL.

    The third implementation of the rendering, after the two SQL ones. They
    are held together by the cross-engine test rather than by sharing code,
    which is the same arrangement MySQL and PostgreSQL were already in.
    """
    if value is None:
        return None
    if cls == "integer":
        return str(int(value))
    if cls == "float":
        return _float_text(float(value))
    if cls == "decimal":
        # what `cast(col as char)` and `col::text` produce: the digits the
        # server sent, trailing zeros and all, and never an exponent.
        # `str(Decimal)` is not that - measured against PostgreSQL, a
        # numeric(30,10) holding 0.0000000001 comes back from the driver as
        # `Decimal('1E-10')`, whose `str` is `1E-10` while the server's own
        # text is `0.0000000001`. Formatting with `f` is the same number
        # written the way both servers write it.
        #
        # BSON's Decimal128 is asked for its Decimal rather than its `str`
        # for exactly the same reason, measured the same way: a Decimal128
        # holding 0.0000000001 prints as `1E-10` too. Duck-typed so this
        # module does not import bson to know that.
        from decimal import Decimal
        if hasattr(value, "to_decimal"):
            value = value.to_decimal()
        return format(value if isinstance(value, Decimal) else Decimal(value),
                      "f")
    if cls == "text":
        return value if isinstance(value, str) else str(value)
    if cls == "bytes":
        return bytes(value).hex().upper()
    if cls == "boolean":
        # 0/1, which is what PostgreSQL's boolean renders to and what a
        # MySQL tinyint(1) already is
        return "1" if value else "0"
    if cls == "timestamp":
        # the same six-place form `to_char(..., 'US')` and
        # `date_format(..., '%f')` produce. BSON dates carry milliseconds, so
        # the last three digits are zeros - which is the truth about what the
        # field can hold, not a rounding migkit chose.
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if cls == "date":
        # what both SQL renderings fall through to: PostgreSQL's `::text` and
        # MySQL's `cast(d as char)` both print `2024-01-02`
        return value.strftime("%Y-%m-%d")
    if cls == "time":
        # `to_char(t, 'HH24:MI:SS.US')` and `time_format(t, '%H:%i:%s.%f')`
        return value.strftime("%H:%M:%S.%f")
    raise ValueError(f"no in-process rendering for class {cls!r}")


# What a class becomes when a table has to be created to receive it.
#
# `{0}` and `{1}` are the numbers the source's own declared type carried -
# a length for text, a precision and scale for decimal. They are kept because
# the class alone does not: `varchar(50)` and `text` are both `text`, and a
# target built from the class alone would quietly drop a limit the
# application may be relying on.
#
# Where the class carries no numbers, or the source did not supply them, the
# widest form is used. Creating a column wider than the source cannot lose a
# value; creating one narrower can, and this file will not do that.
DDL = {
    "postgres": {
        "integer": ("bigint", "bigint"),
        "decimal": ("numeric", "numeric({0},{1})"),
        "float": ("double precision", "double precision"),
        "boolean": ("boolean", "boolean"),
        "text": ("text", "varchar({0})"),
        "bytes": ("bytea", "bytea"),
        "date": ("date", "date"),
        "timestamp": ("timestamp(6)", "timestamp({0})"),
        "time": ("time(6)", "time({0})"),
        "json": ("jsonb", "jsonb"),
    },
    "mysql": {
        "integer": ("bigint", "bigint"),
        "decimal": ("decimal(65,10)", "decimal({0},{1})"),
        "float": ("double", "double"),
        # MySQL has no boolean; `tinyint(1)` is the convention every driver
        # and ORM reads back as one
        "boolean": ("tinyint(1)", "tinyint(1)"),
        # not `text`: MySQL cannot index or key a TEXT column without a
        # prefix length, and a key column is exactly what a mover needs
        "text": ("varchar(1024)", "varchar({0})"),
        "bytes": ("longblob", "varbinary({0})"),
        "date": ("date", "date"),
        "timestamp": ("datetime(6)", "datetime({0})"),
        "time": ("time(6)", "time({0})"),
        "json": ("json", "json"),
    },
    "sqlite": {
        "integer": ("integer", "integer"),
        "decimal": ("numeric", "numeric({0},{1})"),
        "float": ("real", "real"),
        "boolean": ("integer", "integer"),
        "text": ("text", "text"),
        "bytes": ("blob", "blob"),
        "date": ("text", "text"),
        "timestamp": ("text", "text"),
        "time": ("text", "text"),
        "json": ("text", "text"),
    },
}


def params(declared):
    """The numbers inside a declared type, as ints. `varchar(50)` -> (50,).

    Anything that is not a plain number is dropped rather than guessed at -
    `enum('a','b')` carries values, not a width, and passing them into a
    length would produce DDL that does not parse.
    """
    import re
    m = re.search(r"\(([^)]*)\)", str(declared or ""))
    if not m:
        return ()
    out = []
    for part in m.group(1).split(","):
        part = part.strip()
        if not part.isdigit():
            return ()
        out.append(int(part))
    return tuple(out)


def ddl_type(engine, cls, numbers=()):
    """The column type to create for a class on this engine.

    Raises for an engine or class with no mapping rather than falling back to
    something plausible: a table created with the wrong column type is harder
    to notice than one that was never created.
    """
    table = DDL.get(engine)
    if table is None:
        raise ValueError(f"no DDL types for engine {engine!r}")
    pair = table.get(cls)
    if pair is None:
        raise ValueError(f"no DDL type for class {cls!r} on {engine}")
    wide, parametrised = pair
    if not numbers or "{0}" not in parametrised:
        return wide
    try:
        return parametrised.format(*numbers)
    except IndexError:
        return wide


# What one change looks like once it has left the engine that produced it.
#
# The three logs this is read from say the same three things in three shapes:
# a MySQL binlog row event carries before and after images, a PostgreSQL
# logical slot emits a text or protocol message, a MongoDB change stream
# hands back a document. What survives translation is the operation, which
# table it was on, which row it was, and - for anything that is not a delete -
# what the row now holds.
#
# `key` is separate from `values` on purpose. Applying a change needs to
# address the row, and the address is not always inside the payload: a MySQL
# UPDATE that moved the primary key has one key in the before image and
# another in the after, and an applier that took the key from `values` would
# write a second row rather than move the first.
CHANGE_OPS = ("insert", "update", "delete")


def change(op, table, key, values=None):
    """One change record, checked at the point it is made.

    Not a bare dict: an op this file does not know is a log format that
    changed under migkit, and finding that out where the record is built is
    cheaper than finding it out as a row that never arrived.
    """
    if op not in CHANGE_OPS:
        raise ValueError(f"unknown change op {op!r}, expected one of"
                         f" {', '.join(CHANGE_OPS)}")
    if not key:
        raise ValueError(f"a {op} on {table} with no key cannot be applied -"
                         " migkit will not guess which row it meant")
    return {"op": op, "table": str(table), "key": dict(key),
            "values": dict(values or {})}


def sql_value(value):
    """One value on its way into a SQL driver.

    PostgreSQL hands back a `jsonb` column as a Python dict, and no SQL
    driver here will send one - `dict can not be used as parameter` is where
    a JSON column first shows up in a cross-engine move. Serialising it is
    the only step; MySQL and PostgreSQL both normalise the text on storage,
    so the key order chosen here is not what either one stores.

    Everything else is passed through untouched. A converter that guessed at
    types it was not written for would be a second, quieter rendering.
    """
    if isinstance(value, (dict, list)):
        import json
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, (memoryview, bytearray)):
        # psycopg2 hands a `bytea` column back as a memoryview, and pymysql
        # has no escape rule for one - measured, it stored the *text* of the
        # object: `<memory at 0x10ad03dc0>`, 23 bytes where the source held
        # 3. No error, the right number of rows, and the bytes replaced by
        # an address. The digest is what caught it.
        return bytes(value)
    return value


def from_text(cls, text):
    """A value back out of the text an engine printed for it.

    The inverse of the rendering, for the one place that needs it: a logical
    decoding plugin hands its output as text, and passing that text on to the
    target driver is how `\\x00ff41` lands in a binary column as nine
    characters instead of three bytes - the same failure as the memoryview,
    arriving from the other direction.

    A class this does not convert comes back as the text it was given. That
    is not a silent fallback: `text` is the class where that is correct, and
    for anything else the value is one a driver takes as a string anyway.
    """
    if text is None:
        return None
    if cls == "integer":
        return int(text)
    if cls == "float":
        return float(text)
    if cls == "decimal":
        import decimal
        return decimal.Decimal(text)
    if cls == "boolean":
        # PostgreSQL prints `t`/`f` in some contexts and `true`/`false` in
        # others; both appear depending on the caller, so both are read
        return str(text).lower() in ("t", "true", "1")
    if cls == "bytes":
        raw = str(text)
        if raw.startswith("\\x"):
            return bytes.fromhex(raw[2:])
        return raw.encode()
    return text


def digest_step(total, text):
    """Fold one row's text into a running digest, the same way the SQL does.

    Separate from the aggregate that calls it so the arithmetic can be tested
    without a database, and so an engine that has to accumulate in Python
    cannot drift from the one that accumulates in SQL.
    """
    import hashlib
    h = hashlib.md5(("" if text is None else str(text)).encode()).hexdigest()
    return total + int(h[:DIGEST_HEX], 16)


def expr(engine, col, cls):
    """SQL yielding the canonical text of one column, or NULL when it is NULL.

    Raises for an engine with no rendering rather than falling back to the
    engine's own text: a comparison that quietly used two different renderings
    would report every row as different, and the first thing anyone would
    doubt is the data.
    """
    if cls not in CLASSES:
        raise ValueError(f"no canonical rendering for class {cls!r}")
    build = BUILDERS.get(engine)
    if build is None:
        raise ValueError(f"no canonical rendering for engine {engine!r}")
    return build(col, cls)


def comparable(engine, declared):
    """(class, why-not). Exactly one of the two is set."""
    cls = type_class(engine, declared)
    if cls is None:
        return (None, f"{engine} type {declared!r} has no canonical rendering"
                      " in migkit, so a comparison across engines would be"
                      " comparing two renderings nobody checked agree")
    if not renders(engine):
        return (None, f"no canonical rendering for engine {engine!r}")
    return (cls, "")


# Engines that produce the canonical text in this process rather than in a
# SQL expression. Not a lesser arrangement - SQLite is in both lists, because
# it runs here anyway - but for MongoDB it means the documents cross the
# network to be folded, which `check` reports rather than leaves implied.
IN_PROCESS = {"mongodb", "sqlite"}


def renders(engine):
    """Whether migkit can produce canonical text for this engine at all."""
    return engine in BUILDERS or engine in IN_PROCESS


# The digest two different engines can both compute, over the canonical row
# text above.
#
# migkit's own per-engine checksums cannot be used for this: PostgreSQL folds
# rows with `sum(...::bit(64)::bigint)` and MySQL with `bit_xor(conv(...))`,
# which are different functions of the same data and never meet. What both can
# express is a sum over a fixed-width prefix of the row's MD5.
#
# 60 bits rather than 64, measured: MySQL's `conv()` returns a string, and
# summing it coerces to DOUBLE - the same three rows came back as
# `7.50945936868949e17` on MySQL against `750945936868948924` on PostgreSQL,
# a difference produced entirely by the aggregate. Casting to `decimal(65,0)`
# fixes that; 60 bits keeps every intermediate inside a signed 64-bit integer
# so neither side has to be trusted with an overflow rule.
#
# `sum` rather than `bit_xor` because it is in every engine at every version -
# PostgreSQL only grew a native `bit_xor` in 14, which is why AWS tells its
# own customers to hand-create the aggregate on 12 and 13.
DIGEST_HEX = 15
DIGEST_BITS = DIGEST_HEX * 4


def digest_expr(engine, row_expr):
    """Aggregate yielding one number for a whole table, comparable across
    engines. Order-independent, so neither side has to sort."""
    if engine == "mysql":
        return (f"coalesce(sum(cast(conv(substr(md5({row_expr}),1,"
                f"{DIGEST_HEX}),16,10) as decimal(65,0))), 0)")
    if engine == "postgres":
        return (f"coalesce(sum(('x'||substr(md5({row_expr}),1,{DIGEST_HEX}))"
                f"::bit({DIGEST_BITS})::bigint::numeric), 0)")
    if engine == "sqlite":
        # SQLite has neither md5 nor a wide enough number. Measured: `sum()`
        # over 60-bit values raises `integer overflow` once the total passes
        # 2**63, and `total()`, the obvious way around that, returns a real -
        # 20 rows of (2**60 - 1) came back as 2.305843009213694e+19 against
        # an exact 23058430092136939500. One refuses to answer and the other
        # answers approximately; neither can produce the digest.
        #
        # The aggregate migkit registers accumulates in Python integers,
        # which have no width, and returns the total as text - which is what
        # the other two engines produce as well.
        return f"migkit_digest({row_expr})"
    raise ValueError(f"no cross-engine digest for engine {engine!r}")


def row_expr(engine, columns):
    """The canonical injective row text for `[(name, class)]`."""
    from . import rowtext
    parts = [expr(engine, name, cls) for name, cls in columns]
    if engine == "mysql":
        return rowtext.mysql_row_from(parts)
    if engine == "postgres":
        return rowtext.postgres_row_from(parts)
    if engine == "sqlite":
        return rowtext.sqlite_row_from(parts)
    raise ValueError(f"no canonical row text for engine {engine!r}")
