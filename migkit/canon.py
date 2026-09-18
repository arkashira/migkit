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
    },
    # MongoDB reports the BSON type of what is actually stored rather than a
    # declared one, and a field can hold more than one across a collection.
    # `type_class` takes the set with `null` and `missing` removed; a field
    # left holding two real types is genuinely ambiguous and comes back
    # unmapped rather than resolved to whichever is more common.
    #
    # `decimal` (Decimal128), `object` and `array` are absent on purpose:
    # migkit has not measured a rendering for them that another engine
    # reproduces, and claiming one would be claiming the comparison works.
    "mongodb": {
        "int": "integer", "long": "integer",
        "double": "float",
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
    raise ValueError(f"no in-process rendering for class {cls!r}")


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
