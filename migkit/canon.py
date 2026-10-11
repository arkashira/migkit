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
PostgreSQL stores and MySQL refuses, a timestamp of `infinity`, a year before
Christ - is marked as uncomparable with its own name (`uncomparable`): two
sides holding the same such value still meet, and nothing else meets it. And
a value the target would not keep as it is - rounded, cut, turned into a
double or a default - is counted on the source and refused before the move
(`unfit`), rather than found by the digest after it.
"""
import hashlib as _hashlib
import re as _re
import uuid as _uuid
from typing import NamedTuple

from .pieces import Hashed as _Hashed

# Neutral classes. An engine's declared type is mapped onto one of these and
# the rendering is chosen from the class, so adding an engine is a mapping
# rather than a new set of pairwise rules.
CLASSES = ("integer", "decimal", "float", "boolean", "text", "bytes",
           "date", "timestamp", "time", "json", "own text", "number",
           "xml", "uuid", "inet", "json on one engine", "interval",
           "vector", "geometry", "bson")

#: A duration as months, days and nanoseconds kept apart (`intervals`):
#: PostgreSQL's and DuckDB's `interval`, Cassandra's `duration`, Oracle's
#: two. MySQL, SQL Server and MongoDB have no such type.
INTERVAL = "interval"
#: An embedding: its elements as the floats they are (`vectors`).
VECTOR = "vector"
#: A vector on its way into pgvector's `sparsevec`, which takes only its
#: own text (`write_value`); rendered as any vector is
SPARSE = "vector into sparsevec"
#: A geometry or a geography: its SRID and its ISO well-known binary,
#: longitude first (`geo`).
GEOMETRY = "geometry"
#: A MongoDB value of a type no other engine has (an ObjectId, a
#: `Timestamp`, a regular expression, MinKey, a document), or a field
#: holding values of several types: kept elsewhere as its canonical
#: Extended JSON in a JSON column, and compared with its type
#: (`bsontype`). Beside another engine's column a MongoDB field is
#: `bson/<tag>` and the column `bson:<tag>` (`bson_pair`).
BSON = "bson"

#: JSON compared between two columns of one engine and one type: that
#: engine's own normal form of it, rendered on the server as it always was.
#: Across two engines the two forms differ (`FOLDED_HERE`); within one they
#: are one function of the value, and folding a table in this process only
#: for that would have cost every same-engine range check its speed.
JSON_ONE = "json on one engine"

#: A decimal compared by its value rather than by the digits it was
#: written with: `1.5`, `1.50` and `1.5000000000` are one number. Given to
#: a pair of decimal columns that do not write a value with the same
#: number of digits after the point (`decimal_class`) - PostgreSQL's plain
#: `numeric`, which keeps the scale each value came with, against the
#: `decimal(65,10)` built for it on MySQL printed every row different.
NUMBER = "number"

#: A type with no rendering shared across engines - an interval, a range, a
#: text search vector - compared between two servers of the same engine by
#: that engine's own text of it. Only ever given to a column when both sides
#: are the same engine and declare the same type: across engines the two
#: texts were never checked to agree, and that is what `None` says.
OWN = "own text"

#: Digits after the second in a rendering of a timestamp or a time: six,
#: and nine - the most any engine here holds (SQL Server keeps seven,
#: ClickHouse, Oracle, DuckDB and Snowflake nine) - where the value carries
#: a digit past the sixth. Six was all Python's own types hold, and a value
#: read at six on both sides of a move that cut the seventh to ninth
#: compared equal. Nine only where there is something in them, so a value
#: every engine holds renders as it always has - a SQLite or DynamoDB
#: column holding the text of a time migkit wrote reads as that text still -
#: and one a six-digit engine could not hold cannot meet one it can.
FRACTION = 9


def _past_micro(ns):
    """The seventh to ninth digit of a second, where there are any."""
    return f"{ns:03d}" if ns else ""

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


def uncomparable(what):
    """The marker for one value no shared rendering can write, followed by
    the engine's own word for it: `NaN`, `-Infinity`, `infinity`, a year
    before Christ.

    One marker for all of them made a NaN on one side and an Infinity on
    the other the same text, and a PostgreSQL timestamp of `infinity`
    rendered through `to_char` came back NULL - measured, a target holding
    NULL where the source held `infinity` compared equal. With the word
    after it, two sides holding the same such value still meet (a NaN in a
    PostgreSQL double and in a MongoDB double are one NaN), and nothing
    else meets them: no rendering of a real value starts with 0x1F."""
    return f"{UNCOMPARABLE}:{what}"


#: A text or byte value longer than this many bytes is carried into a
#: digest's row text as its length and the md5 of its bytes, not as its
#: text or hex: the server hashes the value where it lies instead of
#: writing it out twice over. Measured by the move's model runs (backlog
#: R22): a PostgreSQL source digesting rows of 160 MB byte values was
#: killed by the kernel on a 4 GB machine - the hex of each value, the
#: row's text around it and its UTF-8 copy are held at once, a set a
#: parallel worker. Only digests take this form (`digest_value`, the SQL
#: renderings below); a key, a record of a repair and a column-by-column
#: comparison keep the value whole.
WIDE = 1 << 20
#: what a wide value's digest text begins with: 0x1F, which no rendering of
#: a real value starts with (`uncomparable`)
WIDE_MARK = "\x1fwide:"


def wide_bytes(data):
    """The digest text of a wide byte value: `WIDE_MARK`, its length, its
    md5 - what PostgreSQL's `md5(bytea)` and MySQL's `md5(blob)` write."""
    import hashlib
    data = bytes(data)
    return (f"{WIDE_MARK}{len(data)}:"
            f"{hashlib.md5(data, usedforsecurity=False).hexdigest()}")


def wide_text(value):
    """The digest text of a text value over `WIDE` bytes as UTF-8, or None
    where it is not that wide. A string of n characters is n to 4n bytes,
    so only one between WIDE/4 and WIDE characters is encoded to tell."""
    if len(value) * 4 <= WIDE:
        return None
    data = value.encode("utf-8", "surrogatepass")
    return wide_bytes(data) if len(data) > WIDE else None


def digest_value(cls, value):
    """A value's text in a digest's row: `render_value`'s, except a text or
    byte value wider than `WIDE`, which is its length and md5 - and one a
    server hashed where it lies (`pieces.Hashed`), which is what it wrote."""
    if type(value) is _Hashed:
        return value.text()
    if cls == "bytes" and isinstance(value, (bytes, bytearray, memoryview)) \
            and len(value) > WIDE:
        return wide_bytes(value)
    if cls == "text" and isinstance(value, str):
        got = wide_text(value)
        if got is not None:
            return got
    return render_value(cls, value)


class Added:
    """A counter's change as what it adds, not what it ends as (a two-way
    hop's `delta` columns): applied where the target's row stands, so what
    each side added is kept. `to` is what the source's row ended with,
    written where the target has no row to add to."""

    __slots__ = ("by", "to")

    def __init__(self, by, to):
        self.by, self.to = by, to

    def __repr__(self):
        return f"Added({self.by!r}, to={self.to!r})"

    def __eq__(self, other):
        return isinstance(other, Added) and (self.by, self.to) == \
            (other.by, other.to)

    __hash__ = None


class Kept(Added):
    """A value the target keeps as its row holds it (a two-way row's
    column the target changed and the source did not, or whose conflict
    the target's side won): left as it stands where the row is there, and
    written as `to` - the target's own value, read when the conflict was
    settled - only where the row has to be made. Not left out of the
    change instead: an upsert inserts first, and a column it leaves out is
    NULL there, which a NOT NULL column refuses before the key's conflict
    is found. Applied by the same statement as a counter (`Added`, `by`
    0), so what the target's side writes between the read and the batch
    is not written over either."""

    __slots__ = ()

    def __init__(self, to):
        super().__init__(0, to)

    def __repr__(self):
        return f"Kept({self.to!r})"

    def __eq__(self, other):
        return isinstance(other, Kept) and self.to == other.to

    __hash__ = None


def merged_values(earlier, later):
    """A row's values after a later change over an earlier one, in place:
    later over earlier, except that two additions to one counter are one
    addition of both, an addition to a value the batch itself wrote is
    that value moved, and a value kept where it stands (`Kept`) keeps one
    the batch wrote before it."""
    for name, value in later.items():
        was = earlier.get(name)
        if isinstance(value, Kept):
            if name in earlier and not isinstance(was, (Added, _Absent)):
                continue
        elif isinstance(value, Added):
            if isinstance(was, Kept):
                pass
            elif isinstance(was, Added):
                value = Added(was.by + value.by, value.to)
            elif was is not None and name in earlier \
                    and not isinstance(was, _Absent):
                value = was + value.by
        earlier[name] = value
    return earlier


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


def dialect(engine):
    """The engine whose values, declared types, DDL and SQL renderings
    `engine` shares (`engines.WIRE`: TiDB MySQL's, CockroachDB and Aurora
    DSQL PostgreSQL's) - itself for every other. Each table below keyed by
    engine holds the dialect's entry under the engine's name too
    (`_speak_as`), and what branches on an engine's name asks this."""
    from .engines import WIRE
    return WIRE.get(engine, engine)


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
        # MySQL 9's embedding, 32-bit floats read as their bytes
        "vector": "vector",
        "geometry": "geometry", "point": "geometry",
        "linestring": "geometry", "polygon": "geometry",
        "multipoint": "geometry", "multilinestring": "geometry",
        "multipolygon": "geometry", "geometrycollection": "geometry",
        "geomcollection": "geometry",
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
        # the extension's text compared without case: its values are text,
        # and `check` read them as text while a build found no class for
        # them (`sizing.UNBOUNDED` counted it a text already)
        "citext": "text",
        "uuid": "uuid", "inet": "inet", "cidr": "inet", "macaddr": "text",
        "xml": "xml",
        # a text search vector prints its lexemes sorted and once each, and
        # a query its normalised form, so their text is the value itself
        "tsvector": "text", "tsquery": "text",
        # pgvector's types, read as the 32-bit floats they hold (a
        # `halfvec` and a `sparsevec` through `::vector`). Unmapped, a
        # vector column was left out of every comparison: a wrong vector
        # passed `check` (measured)
        "vector": "vector", "halfvec": "vector", "sparsevec": "vector",
        "interval": "interval",
        # PostGIS: a geography is a geometry on the sphere, the same
        # binary with SRID 4326 unless it says otherwise
        "geometry": "geometry", "geography": "geometry",
        # a key/value set is a JSON object of strings: the extension casts
        # it to jsonb, which the `json` rendering then normalises the way
        # the other side's JSON is
        "hstore": "json",
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
    # SQL Server, read through its driver and rendered in this process.
    # Its `timestamp` is a row version, not a time, and `datetimeoffset`
    # carries an offset the renderer would drop: both stay unmapped, as
    # do `sql_variant`, `hierarchyid` and the spatial types.
    "mssql": {
        "tinyint": "integer", "smallint": "integer", "int": "integer",
        "bigint": "integer",
        "decimal": "decimal", "numeric": "decimal", "money": "decimal",
        "smallmoney": "decimal",
        "float": "float", "real": "float",
        "bit": "boolean",
        "char": "text", "varchar": "text", "nchar": "text",
        "nvarchar": "text", "text": "text", "ntext": "text",
        "uniqueidentifier": "uuid", "xml": "xml", "sysname": "text",
        "binary": "bytes", "varbinary": "bytes", "image": "bytes",
        "date": "date",
        "datetime": "timestamp", "datetime2": "timestamp",
        "smalldatetime": "timestamp",
        "time": "time",
        # read as their SRID and binary (`read_as`); a shape written here
        # goes in through its own constructor, which the row copy does
        # not call, so into SQL Server the move refuses them
        "geography": "geometry", "geometry": "geometry",
    },
    # Parquet, by the Arrow type names a table's `_table.json` gives:
    # `decimal_text` is a decimal kept as its text, where the source
    # declared no precision for it to be kept in
    "parquet": {
        "int64": "integer", "double": "float", "bool": "boolean",
        "string": "text", "binary": "bytes", "date32": "date",
        "timestamp": "timestamp", "time64": "time",
        "decimal128": "decimal", "decimal256": "decimal",
        "decimal_text": "decimal",
    },
    # ClickHouse, by the type a value is once `Nullable(...)` and
    # `LowCardinality(...)` are taken off it. It has no time-of-day type,
    # and a `String` is bytes: a text column read from it is decoded, a
    # column declared bytes on the other side is compared as bytes.
    "clickhouse": {
        "int8": "integer", "int16": "integer", "int32": "integer",
        "int64": "integer", "int128": "integer", "int256": "integer",
        "uint8": "integer", "uint16": "integer", "uint32": "integer",
        "uint64": "integer", "uint128": "integer", "uint256": "integer",
        "float32": "float", "float64": "float",
        "decimal": "decimal", "decimal32": "decimal", "decimal64": "decimal",
        "decimal128": "decimal", "decimal256": "decimal",
        "bool": "boolean",
        "string": "text", "fixedstring": "text", "uuid": "uuid",
        "enum8": "text", "enum16": "text", "ipv4": "inet", "ipv6": "inet",
        "date": "date", "date32": "date",
        "datetime": "timestamp", "datetime64": "timestamp",
    },
    # DynamoDB: a table migkit made carries each column's class in its
    # tags, by these names; one it did not make is described by the kinds
    # of attribute its items hold. A number there is a decimal, without
    # the scale it was written with.
    "dynamodb": {
        "integer": "integer", "decimal": "decimal", "float": "float",
        "boolean": "boolean", "text": "text", "bytes": "bytes",
        "date": "date", "timestamp": "timestamp", "time": "time",
        "s": "text", "n": "decimal", "b": "bytes", "bool": "boolean",
        "uuid": "uuid",
    },
    # Oracle: a DATE holds a time of day too. A time with a zone is the
    # instant it names, read in UTC by the server (`sys_extract_utc`), as
    # PostgreSQL's is read in a session pinned to UTC. `NUMBER` with no
    # scale holds integers and decimals alike, and renders the same text
    # either way.
    "oracle": {
        "number": "decimal", "integer": "decimal", "float": "float",
        "binary_float": "float", "binary_double": "float",
        "varchar2": "text", "nvarchar2": "text", "char": "text",
        "nchar": "text", "clob": "text", "nclob": "text", "long": "text",
        "raw": "bytes", "blob": "bytes", "long raw": "bytes",
        "date": "timestamp", "timestamp": "timestamp",
        "timestamp with time zone": "timestamp",
        "timestamp with local time zone": "timestamp",
        # the mappings only (the Oracle engine reads and writes them as
        # its driver hands them over): year-to-month is months alone,
        # day-to-second days and a time of nine digits
        "interval year to month": "interval",
        "interval day to second": "interval",
        "vector": "vector",
    },
    # Db2 (LUW), by `syscat.columns` type names. A character column with
    # no code page holds bytes (`FOR BIT DATA`), and is declared VARBINARY
    # by the reading of the catalogue.
    "db2": {
        "smallint": "integer", "integer": "integer", "bigint": "integer",
        "decimal": "decimal", "numeric": "decimal", "decfloat": "decimal",
        "real": "float", "double": "float", "float": "float",
        "boolean": "boolean",
        "character": "text", "char": "text", "varchar": "text",
        "graphic": "text", "vargraphic": "text", "clob": "text",
        "dbclob": "text",
        "blob": "bytes", "binary": "bytes", "varbinary": "bytes",
        "date": "date", "timestamp": "timestamp", "time": "time",
    },
    # Db2 for i, by `QSYS2.SYSCOLUMNS` type names as `engines.db2i` reads
    # them: NUMERIC is zoned, DECIMAL packed, both exact; VARG is
    # VARGRAPHIC and VARBIN VARBINARY; a CHAR of CCSID 65535 is read as
    # VARBINARY. ROWID, XML and DATALINK stay unmapped
    "db2i": {
        "smallint": "integer", "integer": "integer", "bigint": "integer",
        "decimal": "decimal", "numeric": "decimal", "decfloat": "decimal",
        "real": "float", "double": "float", "float": "float",
        "boolean": "boolean",
        "char": "text", "varchar": "text", "graphic": "text",
        "varg": "text", "clob": "text", "dbclob": "text",
        "binary": "bytes", "varbin": "bytes", "varbinary": "bytes",
        "blob": "bytes",
        "date": "date", "timestamp": "timestamp", "time": "time",
    },
    # Db2 for z/OS, by `SYSIBM.SYSCOLUMNS` COLTYPE as `engines.db2zos`
    # reads them; a column FOR BIT DATA is read as VARBINARY
    "db2zos": {
        "smallint": "integer", "integer": "integer", "bigint": "integer",
        "decimal": "decimal", "decfloat": "decimal",
        "real": "float", "double": "float",
        "char": "text", "varchar": "text", "longvar": "text",
        "graphic": "text", "varg": "text", "clob": "text", "dbclob": "text",
        "binary": "bytes", "varbin": "bytes", "varbinary": "bytes",
        "blob": "bytes",
        "date": "date", "timestamp": "timestamp", "time": "time",
    },
    # A file described by a COBOL copybook, by the types `migkit.copybook`
    # gives its fields: how a number is stored, then what it is. A number
    # that fits 64 bits at scale 0 is an integer; `COMP-2` is a decimal,
    # the exact value of its 56-bit fraction; an 88 level is a condition
    "copybook": {
        "text": "text", "national": "text", "bytes": "bytes",
        "zoned integer": "integer", "packed integer": "integer",
        "binary integer": "integer", "native integer": "integer",
        "zoned decimal": "decimal", "packed decimal": "decimal",
        "binary decimal": "decimal", "native decimal": "decimal",
        "hex float": "float", "long hex float": "decimal",
        "condition": "boolean",
    },
    # Teradata, by the declared types `engines.teradata` writes from
    # `DBC.ColumnsV`. NUMBER with no precision is a floating decimal of up
    # to 38 digits, compared by its value; a PERIOD is carried as the text
    # `[begin, end)`; an INTERVAL keeps its months apart from its days and
    # time. A UDT, an ARRAY and a VECTOR stay unmapped
    "teradata": {
        "byteint": "integer", "smallint": "integer", "integer": "integer",
        "bigint": "integer",
        "decimal": "decimal", "number": "decimal", "float": "float",
        "char": "text", "varchar": "text", "clob": "text",
        "byte": "bytes", "varbyte": "bytes", "blob": "bytes",
        "date": "date", "time": "time", "timestamp": "timestamp",
        "interval year": "interval", "interval month": "interval",
        "interval day": "interval", "interval hour": "interval",
        "interval minute": "interval", "interval second": "interval",
        "period": "text", "json": "json", "xml": "xml",
    },
    # An SAP system's extractors and CDS views, by the OData types their
    # services give each property (`engines.sapodp`); an ABAP date is an
    # `Edm.DateTime` its service marks as a date, declared `Edm.Date` here
    "sapodp": {
        "edm.string": "text", "edm.guid": "uuid",
        "edm.byte": "integer", "edm.sbyte": "integer",
        "edm.int16": "integer", "edm.int32": "integer",
        "edm.int64": "integer", "edm.decimal": "decimal",
        "edm.double": "float", "edm.single": "float", "edm.float": "float",
        "edm.boolean": "boolean", "edm.binary": "bytes",
        "edm.date": "date", "edm.datetime": "timestamp",
        "edm.datetimeoffset": "timestamp", "edm.time": "time",
    },
    # A SaaS application read by a program (`engines.saas`): each kind's
    # field types are said in these words by the engine, from a JSON
    # schema, a resource's hints or an object's description
    "saas": {
        "text": "text", "integer": "integer", "decimal": "decimal",
        "number": "decimal", "float": "float", "boolean": "boolean",
        "date": "date", "timestamp": "timestamp", "timestamptz": "timestamp",
        "time": "time", "json": "json", "bytes": "bytes", "uuid": "uuid",
    },
    # DuckDB, by `information_schema.columns` type names
    "duckdb": {
        "tinyint": "integer", "smallint": "integer", "integer": "integer",
        "bigint": "integer", "hugeint": "integer", "utinyint": "integer",
        "usmallint": "integer", "uinteger": "integer", "ubigint": "integer",
        "decimal": "decimal", "numeric": "decimal",
        "float": "float", "real": "float", "double": "float",
        "boolean": "boolean",
        "varchar": "text", "text": "text", "uuid": "uuid",
        "blob": "bytes",
        "date": "date", "timestamp": "timestamp",
        "timestamp with time zone": "timestamp", "time": "time",
        # read with all nine of their digits (`nanotime`)
        "timestamp_ns": "timestamp", "timestamp_ms": "timestamp",
        "timestamp_s": "timestamp",
        "json": "json",
        "interval": "interval",
    },
    # SAP ASE, by `systypes` names
    "ase": {
        "tinyint": "integer", "smallint": "integer", "int": "integer",
        "integer": "integer", "bigint": "integer",
        "unsigned smallint": "integer", "unsigned int": "integer",
        "unsigned bigint": "integer",
        "numeric": "decimal", "decimal": "decimal", "money": "decimal",
        "smallmoney": "decimal",
        "float": "float", "real": "float", "double precision": "float",
        "bit": "boolean",
        "char": "text", "varchar": "text", "nchar": "text",
        "nvarchar": "text", "unichar": "text", "univarchar": "text",
        "text": "text", "unitext": "text", "sysname": "text",
        "longsysname": "text",
        "binary": "bytes", "varbinary": "bytes", "image": "bytes",
        "date": "date", "time": "time", "bigtime": "time",
        "datetime": "timestamp", "smalldatetime": "timestamp",
        "bigdatetime": "timestamp",
    },
    # Amazon Redshift, by `information_schema.columns` type names
    "redshift": {
        "smallint": "integer", "integer": "integer", "bigint": "integer",
        "numeric": "decimal", "decimal": "decimal",
        "real": "float", "double precision": "float",
        "boolean": "boolean",
        "character": "text", "character varying": "text", "char": "text",
        "varchar": "text", "bpchar": "text", "text": "text",
        "binary varying": "bytes", "varbyte": "bytes",
        "date": "date",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamp",
        "time without time zone": "time", "time with time zone": "time",
        "super": "json",
    },
    # Snowflake. `NUMBER` at scale 0 is read as INTEGER; `VARIANT` holds
    # JSON's values
    "snowflake": {
        "integer": "integer", "number": "decimal", "decimal": "decimal",
        "numeric": "decimal", "float": "float", "double": "float",
        "real": "float", "boolean": "boolean",
        "text": "text", "varchar": "text", "char": "text", "string": "text",
        "binary": "bytes", "varbinary": "bytes",
        "date": "date", "timestamp_ntz": "timestamp",
        "timestamp_ltz": "timestamp", "timestamp_tz": "timestamp",
        "time": "time", "variant": "json", "object": "json",
    },
    # Google BigQuery. DATETIME has no zone and TIMESTAMP is an instant,
    # as PostgreSQL's two timestamps are
    "bigquery": {
        "int64": "integer", "numeric": "decimal", "bignumeric": "decimal",
        "float64": "float", "bool": "boolean", "string": "text",
        "bytes": "bytes", "date": "date", "datetime": "timestamp",
        "timestamp": "timestamp", "time": "time", "json": "json",
    },
    # Apache Iceberg, by the names its schema gives its types
    "iceberg": {
        "long": "integer", "int": "integer", "decimal": "decimal",
        "double": "float", "float": "float", "boolean": "boolean",
        "string": "text", "binary": "bytes", "fixed": "bytes",
        "date": "date", "timestamp": "timestamp", "timestamptz": "timestamp",
        "timestamp_ns": "timestamp", "timestamptz_ns": "timestamp",
        "time": "time",
    },
    # Delta Lake. `timestamp` is an instant, `timestamp_ntz` a wall clock
    "delta": {
        "long": "integer", "integer": "integer", "short": "integer",
        "byte": "integer", "decimal": "decimal", "double": "float",
        "float": "float", "boolean": "boolean", "string": "text",
        "binary": "bytes", "date": "date", "timestamp": "timestamp",
        "timestamp_ntz": "timestamp",
    },
    # OpenSearch: an index migkit made keeps each column's class in its
    # mapping's `_meta`; one it did not make is read by the class each kind
    # of field holds. Either way the names are the classes themselves.
    "opensearch": {
        "integer": "integer", "decimal": "decimal", "float": "float",
        "boolean": "boolean", "text": "text", "bytes": "bytes",
        "date": "date", "timestamp": "timestamp", "time": "time",
        "uuid": "uuid",
    },
    # Cassandra and ScyllaDB. A list or a set of plain values is a JSON
    # array, a map of them an object (`collection`); user types stay
    # unmapped. A `timestamp` holds milliseconds, a `duration` months,
    # days and nanoseconds, as PostgreSQL's interval does.
    "cassandra": {
        "tinyint": "integer", "smallint": "integer", "int": "integer",
        "bigint": "integer", "varint": "integer", "counter": "integer",
        "decimal": "decimal", "float": "float", "double": "float",
        "boolean": "boolean", "text": "text", "varchar": "text",
        "ascii": "text", "uuid": "uuid", "timeuuid": "uuid", "inet": "inet",
        "blob": "bytes", "date": "date", "timestamp": "timestamp",
        "time": "time", "duration": "interval",
    },
    # By the BSON type each field holds (`$type`), a binary by its subtype
    # (`uuid` for 4, `binData` for 0, `binData3` and on for the rest). A
    # type no other engine has, a document, an array, or a field holding
    # values of more than one type, is `bson`: carried elsewhere as its
    # canonical Extended JSON and compared with its type.
    "mongodb": {
        "int": "integer", "long": "integer",
        "double": "float",
        "decimal": "decimal",
        "bool": "boolean",
        "string": "text",
        "bindata": "bytes", "uuid": "uuid",
        "date": "timestamp",
        "objectid": "bson", "symbol": "bson", "timestamp": "bson",
        "regex": "bson", "minkey": "bson", "maxkey": "bson",
        "javascript": "bson", "javascriptwithscope": "bson",
        "dbpointer": "bson", "undefined": "bson", "object": "bson",
        "array": "bson",
    },
    # Couchbase and Cosmos DB hold JSON documents. A collection or
    # container migkit did not make is described by the JSON kinds its
    # documents hold (a number that is always whole an `integer`); one it
    # made keeps each field's class, and is read by the class's own name
    # (a decimal as its text, a time as ISO text, bytes as base64)
    **{name: {
        "string": "text", "integer": "integer", "number": "float",
        "boolean": "boolean", "object": "json", "array": "json",
        "json": "json", "text": "text", "float": "float",
        "decimal": "decimal", "bytes": "bytes", "date": "date",
        "timestamp": "timestamp", "time": "time", "uuid": "uuid",
    } for name in ("couchbase", "cosmosdb")},
}

# MySQL has no boolean: `tinyint(1)` is the convention and holds -128..127.
# It is mapped to `integer` above rather than `boolean` on purpose - a MySQL
# column holding 2 is not a boolean, and rendering it as one would hide the
# difference from a PostgreSQL side that cannot hold 2 at all. PostgreSQL's
# real boolean renders to 0/1 to meet it.


#: classes an array's elements can be of and still be a JSON array, whose
#: values JSON writes the same on every engine
ARRAY_ELEMENTS = ("integer", "decimal", "float", "boolean", "text")


def _generic(text, name):
    """The parameters of `name<...>` (Cassandra's `list<int>`, `map<text,
    int>`, `frozen<set<text>>`, `vector<float, 3>`) split at the top
    level, or None where `text` is not one."""
    low = text.strip()
    if not (low.lower().startswith(name + "<") and low.endswith(">")):
        return None
    inner, parts, depth, cur = low[len(name) + 1:-1], [], 0, []
    for ch in inner:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur).strip())
    return parts


def array_element(engine, declared):
    """The element type of an array type (`integer[]`, `INTEGER[3]`,
    ClickHouse's `Array(Nullable(Int64))`, Cassandra's `list<int>` and
    `set<int>`), or None for any other type."""
    text = str(declared or "").strip()
    low = text.lower()
    if engine == "clickhouse" and low.startswith("array(") \
            and low.endswith(")"):
        inner = text[len("array("):-1].strip()
        for wrapper in ("Nullable(", "LowCardinality("):
            while inner.startswith(wrapper) and inner.endswith(")"):
                inner = inner[len(wrapper):-1].strip()
        return inner
    if engine == "cassandra":
        got = _generic(text, "frozen")
        if got and len(got) == 1:
            text = got[0]
        for kind in ("list", "set"):
            got = _generic(text, kind)
            if got and len(got) == 1:
                return got[0]
        return None
    if "[" in low and engine != "clickhouse":
        return text[:low.index("[")].strip()
    return None


def collection_kind(engine, declared):
    """`list`, `set` or `map` for a Cassandra collection, else None - the
    types that hold an empty collection as NULL (`unfit`'s
    `empty-collection`)."""
    if engine != "cassandra":
        return None
    text = str(declared or "").strip()
    got = _generic(text, "frozen")
    if got and len(got) == 1:
        text = got[0]
    for kind in ("list", "set", "map"):
        if _generic(text, kind) is not None:
            return kind
    return None


def vector_dims(engine, declared):
    """How many elements a vector column holds, or None where it does not
    say (pgvector's plain `vector`, ClickHouse's `Array(Float32)`)."""
    text = str(declared or "").strip()
    got = _generic(text, "vector")
    if got and len(got) == 2 and got[1].strip().isdigit():
        return int(got[1])
    if engine == "duckdb":
        import re
        m = re.fullmatch(r"(?i)\s*(float|real|float4|double|float8)\s*"
                         r"\[\s*(\d+)\s*\]\s*", text)
        if m:
            return int(m.group(2))
    nums = params(text)
    return nums[0] if nums and str(text).lower().split("(")[0].strip() in (
        "vector", "halfvec", "sparsevec") else None


def _is_vector(engine, declared):
    """A type that is an embedding rather than an array of numbers:
    ClickHouse's `Array(Float32)` (elements that cannot be NULL), DuckDB's
    fixed-size `FLOAT[n]`, Cassandra's `vector<float, n>`."""
    text = str(declared or "").strip()
    low = text.lower().replace(" ", "")
    if engine == "clickhouse":
        return low == "array(float32)"
    if engine == "duckdb":
        import re
        return bool(re.fullmatch(r"(float|real|float4)\[\d+\]", low))
    if engine == "cassandra":
        got = _generic(text, "vector")
        return bool(got) and got[0].strip().lower() == "float"
    return False


def type_class(engine, declared):
    """Neutral class for an engine's declared type, or None when unmapped.

    None rather than a guess: an unmapped type is reported as uncomparable by
    the caller, which is a line in the report. Guessing `text` would compare
    two renderings nobody checked agree.
    """
    if not declared:
        return None
    base = str(declared).strip().lower()
    if _is_vector(engine, declared):
        return VECTOR
    element = array_element(engine, declared)
    if element is not None:
        # an array of the type, not the type: `integer[]` was classed
        # `integer`, so it was rendered through `int()` of a list and built
        # as a bigint on a target. An array of numbers, text or booleans is
        # the JSON array of them - what a JSON column on another engine
        # holds it as, and compared as JSON is; any other is named
        return ("json" if type_class(engine, element) in ARRAY_ELEMENTS
                else None)
    if collection_kind(engine, declared) == "map":
        # a map of plain values is a JSON object; its keys are text there
        got = _generic(str(declared).strip(), "frozen")
        inner = got[0] if got and len(got) == 1 else str(declared).strip()
        k, v = _generic(inner, "map")
        return ("json" if type_class(engine, k) in ("text", "uuid", "inet")
                and type_class(engine, v) in ARRAY_ELEMENTS else None)
    if engine == "oracle" and base.startswith("interval"):
        import re
        # `INTERVAL DAY(2) TO SECOND(6)`: the numbers inside, not the name
        base = " ".join(re.sub(r"\([^)]*\)", " ", base).split())
    for cut in ("(", " ("):
        if cut in base:
            base = base.split(cut)[0].strip()
    if base.endswith(" unsigned"):
        base = base[:-9].strip()
    if "|" in base:
        # a set of types rather than one, which is how a schemaless engine
        # answers. `null` and `missing` say nothing about what the field
        # holds when it holds something, so they are dropped
        seen = {t.strip() for t in base.split("|")} - {"null", "missing", ""}
        if len(seen) > 1 and {TYPES.get(engine, {}).get(t)
                              for t in seen} == {"integer"}:
            # a driver writes a whole number as a 32-bit one when it fits
            # and a 64-bit one when it does not, so one field holds both;
            # left unmapped, it was never compared
            return "integer"
        if len(seen) > 1 and engine == "mongodb":
            # several types in one field: each value kept with its own
            # (`bson`), where one column type would have changed some
            return BSON
        if len(seen) != 1:
            return None
        base = seen.pop()
    if engine == "mongodb" and base.startswith("bindata") \
            and base != "bindata":
        # a binary of a subtype other than 0 (bytes) and 4 (a UUID): a
        # legacy UUID of unknown byte order, a hash, encrypted - kept as it
        # is, subtype and all
        return BSON
    return TYPES.get(engine, {}).get(base)


def _mysql(col, cls):
    c = f"`{col}`"
    if cls == "float":
        # banded: exact decimal in the middle, the engine's own shortest text
        # outside it, because the decimal cast saturates above 1e45 and
        # underflows below 1e-20 - both silently, both to a shared string.
        #
        # Through a double first (`* 1e0`, which is exact and keeps the
        # sign): MySQL writes a FLOAT's own text at six significant digits.
        # Measured on 8.4, the float 3.4028234e38 printed `3.40282e38`
        # and 1e-40 `9.99995e-41`, where its double is
        # `3.4028234663852886e38` and `9.99994610111476e-41` - what
        # PostgreSQL's `real::float8` prints for the same float
        f = f"({c} * 1e0)"
        return (f"case when {c} is null then null"
                f" when {f} <> 0 and (abs({f}) >= {FLOAT_MAX}"
                f" or abs({f}) < {FLOAT_MIN}) then cast({f} as char)"
                f" else cast(cast({f} as decimal(65,{FLOAT_SCALE})) as char)"
                f" end")
    if cls == "bytes":
        # a wide value as its length and md5, hashed where it lies (`WIDE`)
        return (f"case when length({c}) > {WIDE} then concat('{WIDE_MARK}',"
                f" length({c}), ':', md5({c})) else hex({c}) end")
    if cls == "text":
        return (f"case when length({c}) > {WIDE} then concat('{WIDE_MARK}',"
                f" length(convert({c} using utf8mb4)), ':',"
                f" md5(convert({c} using utf8mb4))) else cast({c} as char)"
                " end")
    if cls == "timestamp":
        return f"date_format({c}, '%Y-%m-%d %H:%i:%s.%f')"
    if cls == "time":
        return f"time_format({c}, '%H:%i:%s.%f')"
    if cls == "integer":
        # as a number: `cast(... as char)` of a YEAR of 0 is `0000` and of
        # a BIT is its bytes, where every other engine writes the number -
        # measured, a PostgreSQL int of 0 moved into a YEAR compared equal
        # row by row and different by digest. `+ 0` keeps an unsigned
        # bigint an integer
        return f"cast(({c} + 0) as char)"
    if cls == NUMBER:
        return _value_text(c, f"cast({c} as char)", "locate('.', {t}) > 0",
                           "trim(trailing '.' from trim(trailing '0' from"
                           " {t}))")
    if cls == "uuid":
        # a text column holding a UUID beside a column that is one: in
        # small letters where it is written the one way a UUID is
        return (f"case when {c} regexp '{UUID_FORM}' then lower({c})"
                f" else cast({c} as char) end")
    return f"cast({c} as char)"


def _value_text(c, t, has_point, trimmed):
    """A decimal's text with the trailing zeros of its fraction off, and
    the point with them, and a zero without a sign: `NUMBER` in SQL, one
    shape for both engines (`trim_scale` is PostgreSQL 13 and later)."""
    return (f"case when {c} is null then null when {c} = 0 then '0'"
            f" when {has_point.format(t=t)} then {trimmed.format(t=t)}"
            f" else {t} end")


def _postgres(col, cls):
    c = f'"{col}"'
    if cls == "float":
        # a `real` as the double it is: its own text is the shortest that
        # reads back as the *float*, `0.1`, where MySQL's cast and every
        # driver that hands the float over as a double see
        # 0.10000000149011612 (measured on 16: `0.1::real::float8::text`)
        c = f"{c}::float8"
        # `e+20` -> `e20` is what makes the outer band agree with MySQL, and
        # Infinity/NaN have no MySQL counterpart at all, so they are marked
        # rather than rendered - each by its own name, so a NaN and an
        # Infinity are not the same text
        outer = (f"replace({c}::text, 'e+', 'e')")
        # `x <> x` is the portable NaN test everywhere except here:
        # PostgreSQL defines NaN as equal to itself so it can index and sort
        # it, so that test is always false and a NaN would fall through to
        # the renderer and come out as the literal text `NaN`. Measured.
        return (f"case when {c} is null then null"
                f" when {c} = 'Infinity'::float8 or {c} = '-Infinity'::float8"
                f" or {c} = 'NaN'::float8"
                f" then '{uncomparable('')}' || {c}::text"
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
        # a wide value as its length and md5 - `md5(bytea)` reads the bytes
        # where they lie, the hex of them twice their size is never written
        # (`WIDE`); `octet_length` reads the stored length, not the value
        return (f"case when octet_length({c}) > {WIDE} then '{WIDE_MARK}'"
                f" || octet_length({c}) || ':' || md5({c})"
                f" else upper(encode({c}, 'hex')) end")
    if cls == "text":
        # the UTF-8 bytes, as MySQL and this process hash them; in a UTF-8
        # database `convert_to` hands the value back as it is
        u = f"convert_to({c}::text, 'UTF8')"
        return (f"case when octet_length({c}::text) > {WIDE} then"
                f" '{WIDE_MARK}' || octet_length({u}) || ':' || md5({u})"
                f" else {c}::text end")
    if cls == "timestamp":
        # `to_char` of `infinity` is NULL, and of a year before Christ the
        # same digits as the year after: measured on 16, `infinity` and a
        # NULL rendered alike, and `0044-03-15 BC` as `0044-03-15`. No
        # other engine holds either, so each is marked with its own text
        return (f"case when {c} is null then null"
                f" when not isfinite({c}) or {c} < '0001-01-01'"
                f" or {c} >= '10000-01-01'"
                f" then '{uncomparable('')}' || {c}::text"
                f" else to_char({c}, 'YYYY-MM-DD HH24:MI:SS.US') end")
    if cls == "time":
        return f"to_char({c}, 'HH24:MI:SS.US')"
    if cls in ("json", JSON_ONE):
        # a `json` column keeps the text it was handed, spacing and duplicate
        # keys and all; `jsonb` is the normalised form. `to_jsonb` rather
        # than `::jsonb`, which an array does not take
        return f"to_jsonb({c})::text"
    if cls == "decimal":
        # through `numeric`, which a numeric already is: a `money` prints
        # its locale's text - `$1,234.56` - where its number is 1234.56
        return f"{c}::numeric::text"
    if cls == NUMBER:
        return _value_text(f"{c}::numeric", f"{c}::numeric::text",
                           "position('.' in {t}) > 0",
                           "rtrim(rtrim({t}, '0'), '.')")
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


#: TiDB renders MySQL's SQL as MySQL does (every function above is one
#: TiDB has). CockroachDB and DSQL are not given PostgreSQL's: CockroachDB
#: has no `isfinite` and its `to_char` is not PostgreSQL's, and a
#: rendering no server of theirs has run is folded in this process instead
#: (`IN_PROCESS`)
BUILDERS = {"mysql": _mysql, "postgres": _postgres, "sqlite": _sqlite,
            "tidb": _mysql}


#: engines whose decimal keeps the scale each value was written with, where
#: the column declares none: `1.50` reads back `1.50`
KEEPS_SCALE = {"postgres", "mongodb", "cassandra"}


def decimal_class(src, src_declared, dst, dst_declared):
    """`decimal` for a pair of decimal columns that write every value with
    the same digits after the point - the same fixed scale on both, or
    both keeping each value's own - and `number` for any other pair, which
    is then compared by value (`NUMBER`)."""
    a, b = _fixed_decimal(src, src_declared), _fixed_decimal(dst,
                                                             dst_declared)
    if a and b and a[1] == b[1]:
        return "decimal"
    if not a and not b and src in KEEPS_SCALE and dst in KEEPS_SCALE:
        return "decimal"
    return NUMBER


def bson_pair(src, src_declared, dst, dst_declared):
    """(source class, target class) for a column with MongoDB on exactly
    one side, or None for any other pair: MongoDB's field `bson/<tag>`
    and the other side's column `bson:<tag>`, where `<tag>` is the BSON
    type MongoDB holds that column's values as (`bsontype.column_tag`) -
    both rendered with the type in front, so a type that changes on the
    way is a difference (type-fidelity G5). A vector, a shape or an
    interval is compared as that on both sides. (None, None) where the
    other side's column has no BSON type at all (a time of day)."""
    if (src == "mongodb") == (dst == "mongodb"):
        return None
    from . import bsontype
    first = src == "mongodb"
    other, declared = (dst, dst_declared) if first else (src, src_declared)
    cls = type_class(other, declared)
    if cls in (VECTOR, GEOMETRY, INTERVAL):
        return (cls, cls)
    t = bsontype.column_tag(other, declared, cls) if cls else None
    if t is None:
        return (None, None)
    if t == "decimal" and decimal_class("mongodb", "decimal", other,
                                        declared) == NUMBER:
        t = "number"
    mine, theirs = bsontype.MONGO + t, bsontype.OTHER + t
    return (mine, theirs) if first else (theirs, mine)


def bson_alike(scls, dcls, src_declared, dst_declared):
    """Whether a MongoDB field and another engine's column (`bson_pair`)
    hold one kind of value: every type the field holds is the one the
    column is held as in MongoDB - a field of `long`s beside a `bigint`,
    anything beside a JSON column. A field of `int`s beside a `bigint`
    is not: it comes back a `long`."""
    from . import bsontype
    pair = (str(scls), str(dcls))
    if pair[0].startswith(bsontype.MONGO) and \
            pair[1].startswith(bsontype.OTHER):
        mongo, t = src_declared, pair[1][len(bsontype.OTHER):]
    elif pair[1].startswith(bsontype.MONGO) and \
            pair[0].startswith(bsontype.OTHER):
        mongo, t = dst_declared, pair[0][len(bsontype.OTHER):]
    else:
        return False
    if t == "json":
        return True
    seen = {x.strip() for x in str(mongo or "").split("|")} \
        - {"null", "missing", ""}
    want = {"number": "decimal"}.get(t, t)
    return bool(seen) and seen == {want}


#: what a hop that lets a MongoDB field's type change (`accept_changes`)
#: compares it as: its value alone, in the class of the column it went to
BY_VALUE = {"int": "integer", "long": "integer", "double": "float",
            "decimal": "decimal", "number": NUMBER, "bool": "boolean",
            "string": "text", "date": "timestamp", "binData": "bytes",
            "uuid": "uuid", "json": "json"}


def mongo_value_class(engine, declared, cls):
    """The class a MongoDB field's values alone go in, for a field whose
    type the hop lets change: an ObjectId or a symbol as text, a binary
    of any subtype as bytes; any other field its own class."""
    if engine != "mongodb" or cls != BSON:
        return cls
    seen = {t.strip().lower() for t in str(declared or "").split("|")} \
        - {"null", "missing", ""}
    if seen and seen <= {"objectid", "symbol", "string"}:
        return "text"
    if seen and all(t.startswith("bindata") or t == "uuid" for t in seen):
        return "bytes"
    return cls


def by_value(cls):
    """A `bson/...` or `bson:...` class as the plain class of its value,
    for a column whose type change the hop accepts; any other as it is."""
    if isinstance(cls, str) and cls.startswith(bsontype_prefixes()):
        return BY_VALUE.get(cls[5:], cls)
    return cls


def read_expr(engine, quoted, cls):
    """What to select for a column read to be *moved* - the value as the
    driver should get it - where the column's own text would lose part of
    it on the way.

    A single-precision float is the case: MySQL sends a FLOAT as text at
    six significant digits, and PostgreSQL a `real` as the shortest text
    that reads back as the float rather than as a double. Measured on
    MySQL 8.4 through the driver, a FLOAT holding 16777216 arrived as
    16777200.0 and one holding 3.4028234e38 as 3.40282e38: a move changed
    the value by 16, and nothing stopped it. Read as a double - exact for
    every float, and a no-op for a double - the value arrives whole, and
    is the same number every other reader and both renderings see."""
    engine = dialect(engine)
    if cls == "float":
        if engine == "mysql":
            return f"({quoted} * 1e0)"
        if engine == "postgres":
            return f"{quoted}::float8"
    if cls == "decimal" and engine == "postgres":
        # a `money` column arrives as its locale's text otherwise
        return f"{quoted}::numeric"
    if cls == INTERVAL and engine == "postgres":
        # its three parts, as ISO 8601 writes them: psycopg2 reads an
        # interval as a `timedelta`, which folds a month into 30 days
        # (`intervals`). An hour of an interval is not held to 24, and
        # `microseconds` is the seconds with their fraction
        c = quoted
        return (f"case when {c} is null then null else 'P' ||"
                f" (extract(year from {c}) * 12 + extract(month from {c}))"
                f"::bigint || 'M' || extract(day from {c})::bigint || 'DT'"
                f" || (round((extract(hour from {c}) * 3600"
                f" + extract(minute from {c}) * 60) * 1000000"
                f" + extract(microseconds from {c}))::numeric / 1000000)"
                f"::text || 'S' end")
    if cls == "bson:json" and engine == "postgres":
        # as the text it holds, as MySQL's JSON comes: decoded, a JSON
        # string read back as the Python string it spells, and `"5"` and
        # `5` were then one to whatever read it after
        return f"{quoted}::text"
    if cls == GEOMETRY and engine == "mysql":
        # MySQL's own storage, marked as its: four bytes of SRID and the
        # binary, latitude first for a geographic SRID (`geo.from_mysql`)
        return (f"case when {quoted} is null then null else"
                f" concat('mysql:', hex({quoted})) end")
    if cls == VECTOR and engine == "postgres":
        # as 32-bit floats: a `halfvec`'s text is the shortest decimal of
        # a 16-bit float, which read as a 32-bit one is another number,
        # and a `sparsevec` is written by index; `::vector` writes both
        # as the floats they hold
        return f"{quoted}::vector::text"
    return quoted


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
    if math.isnan(d):
        return uncomparable("NaN")
    if math.isinf(d):
        return uncomparable("Infinity" if d > 0 else "-Infinity")
    if d == 0:
        # -0.0 formats with a leading minus that neither engine produces
        d = 0.0
    if abs(d) >= float(FLOAT_MAX) or (d != 0 and abs(d) < float(FLOAT_MIN)):
        return repr(d).replace("e+", "e")
    with localcontext() as ctx:
        ctx.prec = 80
        return format(Decimal(repr(d)).quantize(Decimal(1).scaleb(-FLOAT_SCALE)),
                      "f")


#: Classes whose SQL renderings do not write what the in-process one does,
#: so a table holding one is folded in this process on every engine. An
#: address: MySQL keeps one as text, which SQL there does not parse. XML:
#: no engine here writes its canonical form (`xml_text`). JSON:
#: MySQL writes `1.10` back as `1.1` and `100000000000000000000` as
#: `1e20` (measured on 8.4), `jsonb` keeps both as written, and a `json`
#: value holding `\u0000` stopped the whole digest at `::jsonb`.
FOLDED_HERE = frozenset({"json", "xml", "inet", INTERVAL, VECTOR, GEOMETRY,
                         BSON})


def folded_here(cls):
    """Whether a column of this class is rendered in this process on every
    engine (`FOLDED_HERE`, and a MongoDB-typed class `bson/...` or
    `bson:...`, which no SQL writes)."""
    return cls in FOLDED_HERE or str(cls).startswith(bsontype_prefixes())


def fold_batches(columns, batches):
    """(rows, digest) over batches of rows - `fold_rows` across them, as a
    digest `neutral_digest` gives."""
    classes = [c for _, c in columns]
    total, n = 0, 0
    for batch in batches:
        k, total = fold_rows(classes, batch, total)
        n += k
    return n, str(total)


def json_text(value):
    """A JSON value as PostgreSQL's `jsonb` writes it: an object's keys by
    their length and then their bytes, the last of a key given twice,
    `", "` and `": "` between, and text escaped as JSON escapes it - with
    every number written as its value (`NUMBER`): `1.10` and `1.1`, and
    `1e20` and `100000000000000000000`, are one number to every reader of
    JSON and are printed differently by the two engines' own text of it."""
    import json
    from decimal import Decimal
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value, parse_float=Decimal, parse_int=int)

    def one(x):
        if x is None:
            return "null"
        if x is True:
            return "true"
        if x is False:
            return "false"
        if isinstance(x, int):
            return str(x)
        if isinstance(x, float):
            return one(Decimal(repr(x)))
        if isinstance(x, Decimal):
            return render_value(NUMBER, x)
        if isinstance(x, str):
            return json.dumps(x, ensure_ascii=False)
        if isinstance(x, dict):
            keys = sorted(x, key=lambda k: (len(str(k).encode()),
                                            str(k).encode()))
            return ("{" + ", ".join(f"{json.dumps(str(k), ensure_ascii=False)}"
                                     f": {one(x[k])}" for k in keys) + "}")
        if isinstance(x, (list, tuple)):
            return "[" + ", ".join(one(e) for e in x) + "]"
        return json.dumps(str(x), ensure_ascii=False)
    return one(value)


#: A UUID written the one way every engine with a UUID type prints it,
#: in either case: 8-4-4-4-12 hexadecimal digits
UUID_FORM = ("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
             "-[0-9a-fA-F]{12}$")
_UUID_RE = _re.compile(UUID_FORM)


def uuid_text(value):
    """A UUID as PostgreSQL, SQL Server's driver, ClickHouse, DuckDB and
    Cassandra all print one - 8-4-4-4-12 in small letters - and a text
    that holds one in capitals the same way; any other text as it is. A
    UUID kept as text in capitals beside a `uuid` read as a difference in
    every row."""
    if isinstance(value, _uuid.UUID):
        return str(value)
    text = value if isinstance(value, str) else str(value)
    return text.lower() if len(text) == 36 and _UUID_RE.fullmatch(text) \
        else text


def inet_text(value):
    """A network address as `address/prefix`, the prefix always written
    and the address as RFC 5952 writes it - lowercase, zeros compressed,
    an IPv4-mapped IPv6 address with its IPv4 part dotted, as PostgreSQL
    and ClickHouse print one. PostgreSQL writes `10.0.0.1` for a host and
    `10.0.0.1/24` for one with a mask, ClickHouse's `IPv4` has no mask,
    and a text column holds whatever case the application wrote: without
    the prefix and the form made one, the same address read as different
    and a masked one could not be told from its host. Text that is no
    address is compared as it is."""
    import ipaddress
    text = str(value).strip()
    try:
        iface = ipaddress.ip_interface(text)
    except ValueError:
        return text
    ip = iface.ip
    mapped = getattr(ip, "ipv4_mapped", None)
    addr = f"::ffff:{mapped}" if mapped else ip.compressed
    return f"{addr}/{iface.network.prefixlen}"


def xml_text(value):
    """An XML value as its Canonical XML 2.0 (W3C), comments kept, with the
    text that is nothing but whitespace between elements left out.

    SQL Server stores an `xml` value parsed and writes it back its own
    way - without the declaration and without the whitespace between
    elements, by its own documentation - where PostgreSQL keeps the text
    it was given. The canonical form is the one both come to, and is what
    the standard says two equal documents write, so a value is compared
    as the document it is. Content that is not one document (PostgreSQL
    keeps a fragment of several) is canonicalised inside a root of its
    own and written without it; text that parses as neither is compared
    as it is."""
    import xml.etree.ElementTree as ET
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    text = value if isinstance(value, str) else str(value)

    def one(root):
        for el in root.iter():
            if el.text is not None and not el.text.strip():
                el.text = None
            if el.tail is not None and not el.tail.strip():
                el.tail = None
        return ET.canonicalize(ET.tostring(root, encoding="unicode"),
                               with_comments=True)
    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
    try:
        return one(ET.fromstring(text, parser=parser))
    except ET.ParseError:
        pass
    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
    try:
        whole = one(ET.fromstring(f"<migkit-fragment>{text}"
                                  "</migkit-fragment>", parser=parser))
    except ET.ParseError:
        return text
    return whole[len("<migkit-fragment>"):-len("</migkit-fragment>")]


def render_value(cls, value):
    """One value as the canonical text, or None when it is NULL.

    The third implementation of the rendering, after the two SQL ones. They
    are held together by the cross-engine test rather than by sharing code,
    which is the same arrangement MySQL and PostgreSQL were already in.
    """
    if value is None:
        return None
    if cls == "integer":
        if isinstance(value, (bytes, bytearray, memoryview)):
            # a MySQL BIT, as the driver hands it back
            return str(int.from_bytes(bytes(value), "big"))
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
    if cls == NUMBER:
        from decimal import Decimal
        if hasattr(value, "to_decimal"):
            value = value.to_decimal()
        value = value if isinstance(value, Decimal) else Decimal(value)
        if not value.is_finite():
            return format(value, "f")
        # `normalize` writes 100 as 1E+2, which `f` turns back into 100;
        # a zero has no sign on any engine
        return "0" if value == 0 else format(value.normalize(), "f")
    if cls == "text":
        return value if isinstance(value, str) else str(value)
    if cls in ("json", JSON_ONE):
        return json_text(value)
    if cls == "xml":
        return xml_text(value)
    if cls == "uuid":
        return uuid_text(value)
    if cls == "inet":
        return inet_text(value)
    if cls == "bytes":
        return bytes(value).hex().upper()
    if cls == "boolean":
        # 0/1, which is what PostgreSQL's boolean renders to and what a
        # MySQL tinyint(1) already is
        return "1" if value else "0"
    if cls == "timestamp":
        # the six-place form `to_char(..., 'US')` and `date_format(...,
        # '%f')` produce, and the three digits after them where a value
        # carries any (`FRACTION`). BSON dates carry milliseconds, so the
        # last three digits are zeros - which is the truth about what the
        # field can hold, not a rounding migkit chose.
        #
        # An instant with its zone is written as UTC shows it, as the SQL
        # renderings write one: every PostgreSQL session migkit opens is
        # pinned to UTC, and MySQL's reads to +00:00. Written at its own
        # zone, the same instant read from DuckDB at the machine's +07 and
        # from PostgreSQL at UTC were seven hours apart (measured)
        ns = getattr(value, "nanosecond", 0) or 0
        if isinstance(value, str) and value.startswith(UNCOMPARABLE):
            # a value its reader marked: Db2's digits past the ninth
            return value
        if isinstance(value, str):
            # a value the driver could not make a datetime of - MySQL's
            # zero date - as `date_format` writes it
            head, _, frac = value.partition(".")
            return f"{head}.{(frac + '0' * 6)[:6]}"
        if getattr(value, "tzinfo", None) is not None:
            import datetime as _dt
            value = value.astimezone(_dt.timezone.utc)
        return value.strftime("%Y-%m-%d %H:%M:%S.%f") + _past_micro(ns)
    if cls == "date":
        # what both SQL renderings fall through to: PostgreSQL's `::text` and
        # MySQL's `cast(d as char)` both print `2024-01-02`; a zero date
        # comes from the driver as that text already
        return value if isinstance(value, str) else value.strftime(
            "%Y-%m-%d")
    if cls == "time":
        # `to_char(t, 'HH24:MI:SS.US')` and `time_format(t, '%H:%i:%s.%f')`
        import datetime as _dt
        if isinstance(value, _dt.timedelta):
            # a MySQL TIME, which the driver hands over as a duration
            # because it is one: -838:59:59 to 838:59:59. Measured on 8.4,
            # `time_format` writes `-01:00:00.000000` and
            # `100:00:00.000000`; `.strftime` did not exist on it, so every
            # in-process reading of a MySQL TIME raised
            us = (value.days * 86400 + value.seconds) * 10 ** 6 \
                + value.microseconds
            h, rest = divmod(abs(us), 3600 * 10 ** 6)
            m, rest = divmod(rest, 60 * 10 ** 6)
            s_, f = divmod(rest, 10 ** 6)
            return f"{'-' if us < 0 else ''}{h:02d}:{m:02d}:{s_:02d}.{f:06d}"
        return value.strftime("%H:%M:%S.%f") \
            + _past_micro(getattr(value, "nanosecond", 0))
    if cls == OWN:
        # both sides came through the same driver, so the same value is the
        # same Python object and the same text
        return value if isinstance(value, str) else str(value)
    if cls == INTERVAL:
        from . import intervals
        return _or_uncomparable(intervals.text, value)
    if cls == VECTOR:
        from . import vectors
        return _or_uncomparable(vectors.text, value)
    if cls == GEOMETRY:
        from . import geo
        return _or_uncomparable(geo.text, value)
    if cls == BSON or cls.startswith(bsontype_prefixes()):
        from . import bsontype
        if cls.startswith(bsontype.OTHER):
            t = cls[len(bsontype.OTHER):]
            return bsontype.render_other(t, value, t == "number")
        return bsontype.render(value, cls == bsontype.MONGO + "number")
    raise ValueError(f"no in-process rendering for class {cls!r}")


def bsontype_prefixes():
    """The class prefixes of a MongoDB field and another engine's column
    compared by BSON type (`bsontype`)."""
    return ("bson/", "bson:")


def _or_uncomparable(render, value):
    """`render(value)`, or the value marked uncomparable where it is not
    one of the class at all - text in a vector column that is no vector:
    a difference, never a match and never a stop of the whole table."""
    import struct
    try:
        return render(value)
    except (ValueError, TypeError, IndexError, KeyError,
            struct.error) as e:
        return uncomparable(f"{type(value).__name__}: {str(e)[:60]}")


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
        OWN: ("text", "{0}"),
        "integer": ("bigint", "bigint"),
        "decimal": ("numeric", "numeric({0},{1})"),
        "float": ("double precision", "double precision"),
        "boolean": ("boolean", "boolean"),
        "text": ("text", "varchar({0})"),
        "uuid": ("uuid", "uuid"),
        "inet": ("inet", "inet"),
        "bytes": ("bytea", "bytea"),
        "date": ("date", "date"),
        "timestamp": ("timestamp(6)", "timestamp({0})"),
        "time": ("time(6)", "time({0})"),
        "json": ("jsonb", "jsonb"),
        "xml": ("xml", "xml"),
        "interval": ("interval", "interval"),
        "vector": ("vector", "vector({0})"),
        "geometry": ("geometry", "geometry"),
        # a document keeps its fields in their order in `json`, where
        # `jsonb` sorts them; a key (and a value of one type no other
        # engine has) goes in `jsonb`, which can be one
        "bson": ("jsonb", "json"),
    },
    "mysql": {
        OWN: ("text", "{0}"),
        "integer": ("bigint", "bigint"),
        "decimal": ("decimal(65,10)", "decimal({0},{1})"),
        "float": ("double", "double"),
        # MySQL has no boolean; `tinyint(1)` is the convention every driver
        # and ORM reads back as one
        "boolean": ("tinyint(1)", "tinyint(1)"),
        # not `text`: MySQL cannot index or key a TEXT column without a
        # prefix length, and a key column is exactly what a mover needs
        "text": ("varchar(1024)", "varchar({0})"),
        "uuid": ("char(36)", "char(36)"),
        "inet": ("varchar(49)", "varchar(49)"),
        "bytes": ("longblob", "varbinary({0})"),
        "date": ("date", "date"),
        "timestamp": ("datetime(6)", "datetime({0})"),
        "time": ("time(6)", "time({0})"),
        "json": ("json", "json"),
        "xml": ("longtext", "longtext"),
        "vector": ("vector", "vector({0})"),
        "geometry": ("geometry", "geometry"),
        "bson": ("json", "json"),
    },
    "mssql": {
        OWN: ("nvarchar(max)", "{0}"),
        "integer": ("bigint", "bigint"),
        "decimal": ("decimal(38,10)", "decimal({0},{1})"),
        "float": ("float", "float"),
        "boolean": ("bit", "bit"),
        # a key column cannot be `max`; a length the source gave is kept
        "text": ("nvarchar(max)", "nvarchar({0})"),
        "uuid": ("uniqueidentifier", "uniqueidentifier"),
        "inet": ("varchar(49)", "varchar(49)"),
        "bytes": ("varbinary(max)", "varbinary({0})"),
        "date": ("date", "date"),
        "timestamp": ("datetime2(6)", "datetime2({0})"),
        "time": ("time(6)", "time({0})"),
        "json": ("nvarchar(max)", "nvarchar(max)"),
        "xml": ("xml", "xml"),
        "bson": ("nvarchar(max)", "nvarchar(max)"),
    },
    "clickhouse": {
        # the source's own type, as `neutral_columns` reads it without its
        # Nullable: ClickHouse to ClickHouse built every column String and
        # stopped on the first decimal written into one
        OWN: ("String", "{0}"),
        "integer": ("Int64", "Int64"),
        "decimal": ("Decimal(38, 10)", "Decimal({0}, {1})"),
        "float": ("Float64", "Float64"),
        "boolean": ("Bool", "Bool"),
        "text": ("String", "String"),
        "uuid": ("UUID", "UUID"),
        "inet": ("String", "String"),
        "bytes": ("String", "String"),
        "date": ("Date32", "Date32"),
        "timestamp": ("DateTime64(6)", "DateTime64({0})"),
        "vector": ("Array(Float32)", "Array(Float32)"),
        "bson": ("String", "String"),
    },
    "oracle": {
        # Oracle to Oracle makes a column as the source declares it, as
        # DuckDB to DuckDB does: through CLOB every column was text
        OWN: ("CLOB", "{0}"),
        "integer": ("NUMBER(19)", "NUMBER(19)"),
        "decimal": ("NUMBER", "NUMBER({0},{1})"),
        "float": ("BINARY_DOUBLE", "BINARY_DOUBLE"),
        "boolean": ("NUMBER(1)", "NUMBER(1)"),
        "text": ("VARCHAR2(4000 CHAR)", "VARCHAR2({0} CHAR)"),
        "uuid": ("VARCHAR2(36 CHAR)", "VARCHAR2(36 CHAR)"),
        "inet": ("VARCHAR2(49 CHAR)", "VARCHAR2(49 CHAR)"),
        "bytes": ("BLOB", "BLOB"),
        "date": ("DATE", "DATE"),
        "timestamp": ("TIMESTAMP(6)", "TIMESTAMP({0})"),
        "vector": ("VECTOR", "VECTOR({0}, FLOAT32)"),
        "bson": ("CLOB", "CLOB"),
    },
    "db2": {
        OWN: ("CLOB", "CLOB"),
        "integer": ("BIGINT", "BIGINT"),
        "decimal": ("DECIMAL(31,10)", "DECIMAL({0},{1})"),
        "float": ("DOUBLE", "DOUBLE"),
        "boolean": ("BOOLEAN", "BOOLEAN"),
        "text": ("VARCHAR(4000)", "VARCHAR({0})"),
        "uuid": ("CHAR(36)", "CHAR(36)"),
        "inet": ("VARCHAR(49)", "VARCHAR(49)"),
        # a byte value of no declared size as a LOB of the most Db2 logs:
        # a BLOB with no size is 1 MB, and a larger value was refused
        # halfway through the load
        "bytes": ("BLOB(1G)", "VARBINARY({0})"),
        "date": ("DATE", "DATE"),
        "timestamp": ("TIMESTAMP(6)", "TIMESTAMP({0})"),
        "time": ("TIME", "TIME"),
        "bson": ("CLOB", "CLOB"),
    },
    # Db2 for i and for z/OS: DB2's own types, a boolean as the SMALLINT
    # both hold on every release (BOOLEAN came to IBM i with 7.5), which
    # renders the same 0 or 1
    **{name: {
        OWN: ("CLOB", "CLOB"),
        "integer": ("BIGINT", "BIGINT"),
        "decimal": ("DECIMAL(31,10)", "DECIMAL({0},{1})"),
        "float": ("DOUBLE", "DOUBLE"),
        "boolean": ("SMALLINT", "SMALLINT"),
        "text": ("VARCHAR(4000)", "VARCHAR({0})"),
        "uuid": ("CHAR(36)", "CHAR(36)"),
        "inet": ("VARCHAR(49)", "VARCHAR(49)"),
        "bytes": ("BLOB(1G)", "VARBINARY({0})"),
        "date": ("DATE", "DATE"),
        "timestamp": ("TIMESTAMP(6)", "TIMESTAMP({0})"),
        "time": ("TIME", "TIME"),
        "bson": ("CLOB", "CLOB"),
    } for name in ("db2i", "db2zos")},
    # Teradata: no BOOLEAN, so the BYTEINT whose 0 and 1 render the same;
    # text CASESPECIFIC, as ANSI mode makes it, so two values that differ
    # in case are two values; a key's text at most 4,000 characters
    # (`KEY_TEXT`)
    "teradata": {
        OWN: ("CLOB CHARACTER SET UNICODE", "CLOB CHARACTER SET UNICODE"),
        "integer": ("BIGINT", "BIGINT"),
        "decimal": ("DECIMAL(38,10)", "DECIMAL({0},{1})"),
        "float": ("FLOAT", "FLOAT"),
        "boolean": ("BYTEINT", "BYTEINT"),
        "text": ("VARCHAR(32000) CHARACTER SET UNICODE CASESPECIFIC",
                 "VARCHAR({0}) CHARACTER SET UNICODE CASESPECIFIC"),
        "uuid": ("CHAR(36) CHARACTER SET LATIN CASESPECIFIC",
                 "CHAR(36) CHARACTER SET LATIN CASESPECIFIC"),
        "inet": ("VARCHAR(49) CHARACTER SET LATIN CASESPECIFIC",
                 "VARCHAR(49) CHARACTER SET LATIN CASESPECIFIC"),
        "bytes": ("BLOB", "VARBYTE({0})"),
        "date": ("DATE", "DATE"),
        "timestamp": ("TIMESTAMP(6)", "TIMESTAMP({0})"),
        "time": ("TIME(6)", "TIME({0})"),
        "json": ("JSON CHARACTER SET UNICODE", "JSON CHARACTER SET UNICODE"),
        "xml": ("XML", "XML"),
    },
    "duckdb": {
        # a type of the same engine is made as the source declares it:
        # measured, DuckDB to DuckDB built every column VARCHAR, and the
        # read back stopped the copy on the first double and timestamp
        OWN: ("VARCHAR", "{0}"),
        "integer": ("BIGINT", "BIGINT"),
        "decimal": ("DECIMAL(38,10)", "DECIMAL({0},{1})"),
        "float": ("DOUBLE", "DOUBLE"),
        "boolean": ("BOOLEAN", "BOOLEAN"),
        "text": ("VARCHAR", "VARCHAR"),
        "uuid": ("UUID", "UUID"),
        "inet": ("VARCHAR", "VARCHAR"),
        "bytes": ("BLOB", "BLOB"),
        "date": ("DATE", "DATE"),
        "timestamp": ("TIMESTAMP", "TIMESTAMP"),
        "time": ("TIME", "TIME"),
        "json": ("JSON", "JSON"),
        "interval": ("INTERVAL", "INTERVAL"),
        "vector": ("FLOAT[]", "FLOAT[{0}]"),
        "bson": ("JSON", "JSON"),
    },
    "ase": {
        OWN: ("text", "text"),
        "integer": ("bigint", "bigint"),
        "decimal": ("numeric(38,10)", "numeric({0},{1})"),
        "float": ("double precision", "double precision"),
        "boolean": ("bit", "bit"),
        "text": ("text", "varchar({0})"),
        "uuid": ("char(36)", "char(36)"),
        "inet": ("varchar(49)", "varchar(49)"),
        "bytes": ("image", "varbinary({0})"),
        "date": ("date", "date"),
        "timestamp": ("bigdatetime", "bigdatetime"),
        "time": ("bigtime", "bigtime"),
    },
    "redshift": {
        OWN: ("varchar(65535)", "{0}"),
        "integer": ("bigint", "bigint"),
        "decimal": ("numeric(38,10)", "numeric({0},{1})"),
        "float": ("double precision", "double precision"),
        "boolean": ("boolean", "boolean"),
        "text": ("varchar(65535)", "varchar({0})"),
        "uuid": ("char(36)", "char(36)"),
        "inet": ("varchar(49)", "varchar(49)"),
        "bytes": ("varbyte(1024000)", "varbyte({0})"),
        "date": ("date", "date"),
        "timestamp": ("timestamp", "timestamp"),
        "time": ("time", "time"),
        "json": ("super", "super"),
        "bson": ("super", "super"),
    },
    "snowflake": {
        OWN: ("VARCHAR", "{0}"),
        "integer": ("NUMBER(38,0)", "NUMBER(38,0)"),
        "decimal": ("NUMBER(38,10)", "NUMBER({0},{1})"),
        "float": ("FLOAT", "FLOAT"),
        "boolean": ("BOOLEAN", "BOOLEAN"),
        "text": ("VARCHAR", "VARCHAR({0})"),
        "uuid": ("VARCHAR(36)", "VARCHAR(36)"),
        "inet": ("VARCHAR(49)", "VARCHAR(49)"),
        "bytes": ("BINARY", "BINARY({0})"),
        "date": ("DATE", "DATE"),
        "timestamp": ("TIMESTAMP_NTZ(9)", "TIMESTAMP_NTZ({0})"),
        "time": ("TIME(9)", "TIME({0})"),
        "json": ("VARIANT", "VARIANT"),
        "bson": ("VARIANT", "VARIANT"),
    },
    "bigquery": {
        OWN: ("STRING", "{0}"),
        "integer": ("INT64", "INT64"),
        "decimal": ("BIGNUMERIC", "BIGNUMERIC({0},{1})"),
        "float": ("FLOAT64", "FLOAT64"),
        "boolean": ("BOOL", "BOOL"),
        "text": ("STRING", "STRING({0})"),
        "uuid": ("STRING", "STRING"),
        "inet": ("STRING", "STRING"),
        "bytes": ("BYTES", "BYTES({0})"),
        "date": ("DATE", "DATE"),
        "timestamp": ("DATETIME", "DATETIME"),
        "time": ("TIME", "TIME"),
        "json": ("JSON", "JSON"),
        "bson": ("JSON", "JSON"),
    },
    # the lakes take the source's own type between two tables of one
    # format. Iceberg's `timestamp` is a wall clock, as the other
    # warehouses make one; Delta's is an instant, since its wall clock
    # (`timestamp_ntz`) is a table feature not every reader has - either
    # way an instant is carried as its UTC clock (`TIME_MEANING`)
    "iceberg": {
        OWN: ("string", "{0}"),
        "integer": ("long", "long"),
        "decimal": ("decimal(38,10)", "decimal({0},{1})"),
        "float": ("double", "double"),
        "boolean": ("boolean", "boolean"),
        "text": ("string", "string"),
        "bytes": ("binary", "binary"),
        "date": ("date", "date"),
        "timestamp": ("timestamp", "timestamp"),
        "time": ("time", "time"),
    },
    "delta": {
        OWN: ("string", "{0}"),
        "integer": ("long", "long"),
        "decimal": ("decimal(38,10)", "decimal({0},{1})"),
        "float": ("double", "double"),
        "boolean": ("boolean", "boolean"),
        "text": ("string", "string"),
        "bytes": ("binary", "binary"),
        "date": ("date", "date"),
        "timestamp": ("timestamp", "timestamp"),
    },
    "cassandra": {
        # the source's own type between two Cassandra clusters: as text,
        # a table copied row by row was built with every column text and
        # stopped on its first number (`'int' object has no attribute
        # 'encode'`, measured on 4.1)
        OWN: ("text", "{0}"),
        "integer": ("bigint", "bigint"),
        "decimal": ("decimal", "decimal"),
        "float": ("double", "double"),
        "boolean": ("boolean", "boolean"),
        "text": ("text", "text"),
        "uuid": ("uuid", "uuid"),
        "inet": ("text", "text"),
        "bytes": ("blob", "blob"),
        "date": ("date", "date"),
        "timestamp": ("timestamp", "timestamp"),
        "time": ("time", "time"),
        "interval": ("duration", "duration"),
        "vector": ("vector<float>", "vector<float, {0}>"),
        "bson": ("text", "text"),
    },
    # the mapping type of each class, as `OpenSearchEngine.MAPPING` writes
    # it: a class with none is refused by name, as on every other engine
    "opensearch": {
        OWN: ("text", "{0}"),
        "integer": ("long", "long"),
        "decimal": ("scaled_float", "scaled_float"),
        "float": ("double", "double"),
        "boolean": ("boolean", "boolean"),
        "text": ("text", "text"),
        "bytes": ("binary", "binary"),
        "date": ("date", "date"),
        "timestamp": ("date_nanos", "date_nanos"),
        "time": ("keyword", "keyword"),
    },
    "sqlite": {
        OWN: ("text", "{0}"),
        "integer": ("integer", "integer"),
        "decimal": ("numeric", "numeric({0},{1})"),
        "float": ("real", "real"),
        "boolean": ("integer", "integer"),
        "text": ("text", "text"),
        "uuid": ("text", "text"),
        "inet": ("text", "text"),
        "bytes": ("blob", "blob"),
        "date": ("text", "text"),
        "timestamp": ("text", "text"),
        "time": ("text", "text"),
        "json": ("text", "text"),
        "bson": ("text", "text"),
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


#: Single-precision float types, by the name each engine declares them
#: under. A float of the source is built as one on the target: widened into
#: a double it keeps its value, and shows the application
#: 0.10000000149011612 where it had read 0.1.
FLOAT4 = {
    "mysql": ("float",), "postgres": ("real", "float4"),
    "mssql": ("real",), "clickhouse": ("float32",),
    "oracle": ("binary_float",), "db2": ("real",),
    "duckdb": ("float", "real", "float4"), "ase": ("real",),
    "redshift": ("real", "float4"), "cassandra": ("float",),
}

#: The single-precision type to build, where an engine has one
FLOAT4_DDL = {
    "postgres": "real", "mysql": "float", "mssql": "real",
    "clickhouse": "Float32", "oracle": "BINARY_FLOAT", "db2": "REAL",
    "duckdb": "FLOAT", "ase": "real", "redshift": "real",
    "cassandra": "float",
}


#: The 32-bit integer to build, where a MongoDB field holds 32-bit ones
#: only: in a `bigint` it came back a `long` (`bsontype`)
INT4_DDL = {
    "postgres": "integer", "mysql": "int", "mssql": "int",
    "clickhouse": "Int32", "oracle": "NUMBER(10)", "db2": "INTEGER",
    "duckdb": "INTEGER", "ase": "int", "redshift": "integer",
    "snowflake": "NUMBER(10,0)", "bigquery": "INT64", "cassandra": "int",
    "sqlite": "integer",
}

#: An array of plain values built as the engine's own array, of the
#: element's type, where it has one; a JSON array elsewhere. PostgreSQL
#: keeps building `jsonb` for another engine's array, as it did.
ARRAY_DDL = {"clickhouse": "Array(Nullable({0}))", "cassandra": "list<{0}>",
             "duckdb": "{0}[]"}

#: element classes each engine's array holds exactly: a decimal element
#: would be held at the built column's scale, and is refused instead
ARRAY_HOLDS = {"clickhouse": ("integer", "float", "boolean", "text"),
               "cassandra": ("integer", "float", "boolean", "text",
                             "decimal"),
               "duckdb": ("integer", "float", "boolean", "text")}

#: engines whose JSON column cannot be a key
JSON_UNKEYED = {"mysql": "a JSON column", "mssql": "an nvarchar(max)",
                "oracle": "a CLOB", "db2": "a CLOB", "redshift": "a SUPER",
                "snowflake": "a VARIANT", "bigquery": "a JSON column"}


def ddl_numbers(engine, declared, cls):
    """The numbers a target column is built from: the source's own
    (`params`), and for a float its width - `(4,)` for a single-precision
    one - which its declared name carries rather than its parentheses. An
    array says `("array", element class)`, a vector its dimensions, a
    MongoDB field of 32-bit integers `(32,)` and one holding documents
    or values of several types `("document",)`."""
    if cls == "float":
        base = str(declared or "").strip().lower().split("(")[0].strip()
        return (4,) if base in FLOAT4.get(engine, ()) else ()
    if engine == "mongodb":
        seen = {t.strip().lower() for t in str(declared or "").split("|")} \
            - {"null", "missing", ""}
        if cls == "integer":
            return (32,) if seen == {"int"} else ()
        if cls == BSON:
            return ("document",) if len(seen) > 1 or seen & {
                "object", "array", "javascriptwithscope"} else ()
        return ()
    if cls == "json":
        element = array_element(engine, declared)
        if element is not None:
            inner = type_class(engine, element)
            # the element's own numbers after its class: a `real[]` is
            # built as an array of 32-bit floats
            return ("array", inner) + tuple(
                ddl_numbers(engine, element, inner) if inner else ())
        return ()
    if cls == VECTOR:
        dims = vector_dims(engine, declared)
        return (dims,) if dims else ()
    if cls == GEOMETRY:
        base = str(declared or "").strip().lower().split("(")[0].strip()
        return ("geography",) if base == "geography" else ()
    return params(declared)


#: The widest text a key column can be built as, where the widest text a
#: column can be is not: InnoDB keys at most 3,072 bytes, 768 characters of
#: utf8mb4 (measured on 8.4, a `varchar(1024)` key was `Specified key was
#: too long; max key length is 3072 bytes`, so a PostgreSQL `text` key could
#: not be moved at all), and SQL Server keys no `max` and 900 bytes.
KEY_TEXT = {"mysql": "varchar(768)", "mssql": "nvarchar(450)",
            "teradata": "VARCHAR(4000) CHARACTER SET UNICODE"
                        " CASESPECIFIC"}


class Limits(NamedTuple):
    """What one server lets a built object be, where it differs by server
    rather than by engine: the longest name, the widest key in bytes, the
    bytes a character of its default character set takes, the widest
    bounded string, the collation that compares strings byte by byte, the
    authentication plugins it signs in with, its reserved words. None
    where not known - `LIMITS` holds each engine's documented default, and
    an engine that asks its server (`limits`) replaces what it read."""
    identifier: int = None
    key_bytes: int = None
    char_bytes: int = None
    max_string: int = None
    binary_collation: str = None
    auth_plugins: frozenset = None
    keywords: frozenset = None


#: each engine's defaults, from its documentation: InnoDB's DYNAMIC rows
#: key 3,072 bytes (COMPACT and 5.6's 767), SQL Server a clustered key
#: 900 bytes, Oracle's MAX_STRING_SIZE=STANDARD 4,000 bytes (EXTENDED
#: 32,767), PostgreSQL's NAMEDATALEN-1 63, MySQL 64, SQL Server and Oracle
#: 12.2+ 128
LIMITS = {
    "postgres": Limits(identifier=63),
    "mysql": Limits(identifier=64, key_bytes=3072, char_bytes=4,
                    binary_collation="utf8mb4_0900_bin"),
    "mssql": Limits(identifier=128, key_bytes=900, char_bytes=2),
    "oracle": Limits(identifier=128, max_string=4000),
    "db2": Limits(identifier=128),
    "clickhouse": Limits(),
}


def limits(engine, version=None, **asked):
    """`engine`'s `Limits`: its defaults (`LIMITS`), what the server's
    version says of them - MariaDB and MySQL before 8.0 have no
    `utf8mb4_0900_bin`, only `utf8mb4_bin` - and whatever an engine read
    off the server itself (`asked`, by field) over both."""
    got = LIMITS.get(dialect(engine), Limits())
    text = str(version or "").lower()
    if dialect(engine) == "mysql" and text and (
            "mariadb" in text or _re.match(r"\s*[1-7]\.", text)):
        got = got._replace(binary_collation="utf8mb4_bin")
    return got._replace(**{k: v for k, v in asked.items() if v is not None})


def ddl_type(engine, cls, numbers=(), key=False, bounds=None):
    """The column type to create for a class on this engine - for a column
    of the table's key where `key`.

    Raises for an engine or class with no mapping rather than falling back to
    something plausible: a table created with the wrong column type is harder
    to notice than one that was never created.

    `bounds`, the target server's `Limits` where an engine asked it, sizes
    what the defaults cannot know: a key's text by the key bytes and the
    bytes a character takes there (191 characters of utf8mb4 on a COMPACT
    InnoDB, not 768), and Oracle's unbounded text by its MAX_STRING_SIZE.
    """
    engine = dialect(engine)
    table = DDL.get(engine)
    if table is None:
        raise ValueError(f"no DDL types for engine {engine!r}")
    pair = table.get(cls)
    if cls == "json" and tuple(numbers[:1]) == ("array",) \
            and engine in ARRAY_DDL:
        element = numbers[1] if len(numbers) > 1 else None
        if element not in ARRAY_HOLDS[engine]:
            raise ValueError(f"no array of {element or 'that'} on {engine}"
                             " that holds each element as it is")
        return ARRAY_DDL[engine].format(
            ddl_type(engine, element, tuple(numbers[2:])))
    if pair is None:
        raise ValueError(f"no DDL type for class {cls!r} on {engine}")
    if key and cls == "text" and not numbers and engine in KEY_TEXT:
        if bounds and bounds.key_bytes and bounds.char_bytes and \
                engine in ("mysql", "mssql"):
            chars = bounds.key_bytes // bounds.char_bytes
            return (f"nvarchar({chars})" if engine == "mssql"
                    else f"varchar({chars})")
        return KEY_TEXT[engine]
    if cls == "text" and not numbers and engine == "oracle" and bounds \
            and bounds.max_string and bounds.max_string > 4000:
        return f"VARCHAR2({bounds.max_string} CHAR)"
    if cls == "float":
        return (FLOAT4_DDL.get(engine, pair[0]) if tuple(numbers) == (4,)
                else pair[0])
    if cls == "integer" and tuple(numbers) == (32,):
        return INT4_DDL.get(engine, pair[0])
    if cls == "json" and tuple(numbers[:1]) == ("array",):
        return pair[0]
    if cls == BSON:
        if key and engine in JSON_UNKEYED:
            raise ValueError(
                f"{engine} cannot key {JSON_UNKEYED[engine]}, which is what"
                " keeps a MongoDB value's type")
        return pair[1] if tuple(numbers) == ("document",) and not key \
            else pair[0]
    if cls == GEOMETRY:
        return ("geography" if engine == "postgres"
                and tuple(numbers) == ("geography",) else pair[0])
    if cls == VECTOR and not numbers and engine in ("duckdb", "cassandra"):
        raise ValueError(f"{engine} builds a vector of a fixed number of"
                         " elements, and the source's does not say how many")
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


def change(op, table, key, values=None, before=None, txn=None):
    """One change record, checked at the point it is made.

    Not a bare dict: an op this file does not know is a log format that
    changed under migkit, and finding that out where the record is built is
    cheaper than finding it out as a row that never arrived.

    `before` is the whole row as it was, where the log keeps it (a binlog
    with its full row image, a table with REPLICA IDENTITY FULL): what a
    two-way tail holds the target's row to, to tell a row changed on both
    sides from one only this side changed.

    `txn` is the transaction the change committed in, as the source names
    it (a PostgreSQL xid, a MySQL GTID), where the log says: what the tail
    holds to the table copier's marks (`Engine.mark_covers`). The tail
    takes it off before anything is applied, with the transaction's end
    (`ended`).
    """
    if op not in CHANGE_OPS:
        raise ValueError(f"unknown change op {op!r}, expected one of"
                         f" {', '.join(CHANGE_OPS)}")
    if not key:
        raise ValueError(f"a {op} on {table} with no key cannot be applied -"
                         " migkit will not guess which row it meant")
    out = {"op": op, "table": str(table), "key": dict(key),
           "values": dict(values or {})}
    if before is not None:
        out["before"] = dict(before)
    if txn is not None:
        out["txn"] = txn
    return out


#: what a change record carries from its source's log and no writer
#: applies: the transaction it was made in, whether it is that
#: transaction's last change the reader handed back, the position just
#: after its commit and the time the source committed it
FROM_THE_LOG = ("txn", "txn_end", "txn_at", "txn_ts")

#: on a change, the columns its log left out as unchanged - a large value
#: stored out of line that an update did not touch (`pgslot.change`):
#: not written, so the target keeps what it holds, and counted by the
#: tail's accounts
UNCHANGED = "unchanged"

#: on a `create`, the table it was renamed from in another database: it
#: arrives with that table's rows, which no row change carries
#: (`tailtables.TableSet.ddl` holds it as a rename into the hop's scope)
CAME_FROM = "came_from"

#: on a delete, that migkit made it of a row that left the hop's row
#: filter (`hetero._in_scope`) - the source deleted nothing, so a two-way
#: tail holds it to no conflict (`twoway.resolve`)
FILTERED = "left_the_filter"


def ended(out, begun, at=None, ts=None):
    """At a transaction's commit: mark the last change of it in `out` as
    its end (`txn_end`), where it handed back any - `begun` is how many
    changes `out` held when the transaction began. A batch cut after a
    change so marked holds whole source transactions and never half of
    one, which is what the connector's transaction metadata gives its
    readers too. A transaction the reader left all of out - on tables the
    hop excludes - marks nothing.

    `at` is the position a read started from there goes on after the
    transaction - what the tail saves where it ends a batch at this
    commit (`hetero.tail_apply`); `ts` the time the source committed it,
    in seconds since the epoch - what the tail's beat says it has applied
    through (`tailctl.beat`)."""
    if len(out) > begun:
        out[-1]["txn_end"] = True
        if at is not None:
            out[-1]["txn_at"] = at
        if ts is not None:
            out[-1]["txn_ts"] = float(ts)


def ddl(table, kind, sql=None, to=None, txn=None):
    """A change to a table's shape or its whole contents, at its place in
    the source's log: `truncate`, `drop`, `rename` (to `to`), `create`,
    `alter`, or `reload` - rows changed with no row event for them (a
    partition truncated, dropped or exchanged: `logddl`), so the table is
    copied again wherever an emptied one would be, and never emptied in
    place - a DDL the log carries, as the reader found it (`sql` where it
    has the statement's text). Not a row change: no writer applies one,
    and the tail takes each out of its batch before anything else sees it
    (`hetero.tail_apply`), cutting that table's changes there."""
    if kind not in DDL_KINDS:
        raise ValueError(f"unknown DDL kind {kind!r}, expected one of"
                         f" {', '.join(DDL_KINDS)}")
    out = {"op": "ddl", "table": str(table), "kind": kind}
    if sql:
        out["sql"] = str(sql)
    if to:
        out["to"] = str(to)
    if txn is not None:
        out["txn"] = txn
    return out


DDL_KINDS = ("truncate", "drop", "rename", "create", "alter", "reload")


def is_ddl(change):
    return change.get("op") == "ddl"


#: classes whose values reach a writer as numbers or times, never as a
#: mapping or a buffer, so `sql_value` has nothing to do for them
PLAIN = frozenset({"integer", "decimal", "float", "boolean", "date",
                   "timestamp", "time"})


#: Python's own scalars, which `write_value` hands to a driver as they are
#: for every class but those with a form of their own (`_writer`)
_AS_THEY_ARE = frozenset({str, bytes, int, float, bool})


def _writer(engine, cls):
    """What a value of one column goes through on its way into `engine`,
    chosen once for the column: `write_value`, with the values it hands on
    untouched - a `str`, `bytes`, a number - let through without the call
    where the class has no form of its own there."""
    if str(cls).startswith(bsontype_prefixes()) or cls in (
            INTERVAL, SPARSE, VECTOR, GEOMETRY) or (
            dialect(engine) == "mssql" and cls in ("timestamp", "time")):
        return lambda v: write_value(engine, cls, v)
    plain = _AS_THEY_ARE
    return lambda v: v if type(v) in plain else write_value(engine, cls, v)


def sql_rows(classes, rows, keep=(), engine=None):
    """`rows` with `write_value` applied where a column can need it: a
    million rows of eight columns called it eight million times, and half
    of those were numbers and times it passes through untouched. The
    columns at `keep` are handed on as they are (an array column, which
    takes a list as the array it is). `engine` is the one written to, for
    the classes it takes a form of its own of. A writer is chosen once a
    column (`_writer`): measured 2026-10-01, 8 columns (4 text or bytes)
    took 2.12 us a row with `write_value` called on every value, 0.15 us
    so."""
    need = [(i, _writer(engine, c)) for i, c in enumerate(classes)
            if c not in PLAIN and i not in keep]
    if not need:
        return rows
    out = []
    for r in rows:
        r = list(r)
        for i, write in need:
            r[i] = write(r[i])
        out.append(r)
    return out


#: How many digits after the second SQL Server's `datetime2` and `time`
#: take in their text: its driver sends a Python time at three (measured
#: with pymssql 2.4, `datetime(2024, 1, 1, 0, 0, 0, 123456)` went as
#: `'2024-01-01 00:00:00.123'`), so the fourth to seventh were lost on
#: every write, and the check found them after it
MSSQL_DIGITS = 7


def write_value(engine, cls, value):
    """One value on its way into `engine`'s driver, for a column of class
    `cls` there: `sql_value`, and for the classes that engine keeps in a
    form of its own - an interval, a vector, a shape, an array in an
    engine that has arrays, a MongoDB value in a JSON column - that form.
    One place, so the copy, a repair and a key looked up all send a value
    the same way."""
    if value is None or value is ABSENT:
        return value
    engine = dialect(engine)
    if isinstance(cls, str) and cls.startswith(bsontype_prefixes()):
        from . import bsontype
        if cls.startswith(bsontype.MONGO):
            # into MongoDB: the type the other side's column holds it as
            return bsontype.as_bson(cls[len(bsontype.MONGO):], value)
        if cls == bsontype.OTHER + "json":
            if engine in ("cassandra", "clickhouse") and \
                    isinstance(value, (dict, list, tuple)):
                # a collection column there (a list, a set, a map, an
                # Array) takes the collection, its numbers as Python's
                return bsontype.plain(value)
            # a MongoDB value into a JSON column: its canonical Extended
            # JSON - a document's fields in its own order, a number with
            # its type, a string as the JSON string it is. `json_write`
            # sorted a plain document's fields
            return bsontype.ejson(value)
        cls = {"string": "text", "uuid": "uuid",
               "binData": "bytes"}.get(cls[len(bsontype.OTHER):], cls)
    if engine == "mssql" and cls in ("timestamp", "time"):
        return mssql_time_text(value)
    if engine != "mongodb":
        if hasattr(value, "to_decimal"):
            # BSON's Decimal128, as the decimal it holds: no SQL driver
            # takes one
            value = value.to_decimal()
        elif type(value).__name__ == "Int64" and isinstance(value, int):
            # BSON's 64-bit integer: an int, which pymysql's escape
            # table, keyed by exact type, does not know it as
            value = int(value)
    if cls == INTERVAL:
        from . import intervals
        if engine == "cassandra":
            from cassandra.util import Duration
            got = intervals.of(value)
            return Duration(got.months, got.days, got.nanos)
        if engine in ("postgres", "duckdb"):
            return intervals.postgres_text(value) or intervals.text(value)
        if engine == "oracle":
            # its driver's own forms: months alone as a year-to-month, days
            # and time as a day-to-second's `timedelta`; the part the other
            # has no room for is refused before the move (`interval-part`)
            got = intervals.of(value)
            if not got.days and not got.nanos:
                import oracledb
                years, months = divmod(abs(got.months), 12)
                sign = -1 if got.months < 0 else 1
                return oracledb.IntervalYM(sign * years, sign * months)
            return intervals.timedelta_of(got) or intervals.text(got)
        return intervals.text(value)
    if cls == SPARSE:
        from . import vectors
        return vectors.sparse_text(value)
    if cls == VECTOR:
        from . import vectors
        if engine == "mysql":
            return vectors.packed(value)
        if engine == "postgres":
            return vectors.pg_text(value)
        if engine == "oracle":
            # its driver binds a VECTOR from an array of 32-bit floats
            import array
            return array.array("f", vectors.of(value))
        return vectors.of(value)
    if cls == GEOMETRY:
        from . import geo
        if engine == "mysql":
            return geo.to_mysql(value)
        if engine == "mongodb":
            return geo.to_geojson(value)
        return geo.ewkb_hex(value)
    if engine == "mongodb":
        # MongoDB takes a document, an ObjectId, any BSON value as it is
        return sql_value(value) if isinstance(value, (memoryview,
                                                      bytearray)) else value
    if cls in ("json", BSON, JSON_ONE):
        if _bson_value(value):
            from . import bsontype
            return bsontype.ejson(value)
        if engine in ARRAY_DDL and isinstance(value, (list, tuple)):
            # an array column of an engine that has them takes the list
            return list(value)
        if engine == "cassandra" and isinstance(value, dict):
            # and a map, the mapping
            return dict(value)
    if cls in ("text", "uuid") and not isinstance(value, str) \
            and _bson_value(value):
        # a MongoDB value into a text column the hop lets it change into
        # (`accept_changes`): its own text, an ObjectId's hexadecimal
        from . import bsontype
        got = bsontype.tag(value)
        if got == "uuid":
            import uuid
            return str(uuid.UUID(bytes=bytes(value)))
        return str(value)
    return sql_value(value)


def has_own_form(cls):
    """Whether a value of this class on its way into an engine takes a
    form of its own there (`write_value`) - an interval, a vector, a
    shape, a MongoDB-typed value - rather than the driver's reading of
    it as it came."""
    return cls in (INTERVAL, VECTOR, GEOMETRY) or \
        str(cls).startswith(bsontype_prefixes())


def _bson_value(value):
    """Whether a value is, or holds, one of BSON's own types - which JSON
    writes only as Extended JSON."""
    if isinstance(value, dict):
        return any(_bson_value(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_bson_value(v) for v in value)
    mod = type(value).__module__ or ""
    return mod.startswith("bson")


def mssql_time_text(value):
    """A time as SQL Server reads its `datetime2`/`time` text, every digit
    it carries up to the seventh (`MSSQL_DIGITS`) - what its drivers cut:
    the statements' to three, the bulk load's conversion to six. An
    instant as its UTC reading, as every engine here renders one; any
    other value as `sql_value` sends it."""
    import datetime
    if not isinstance(value, (datetime.datetime, datetime.time)):
        return sql_value(value)
    ns = value.microsecond * 1000 + (getattr(value, "nanosecond", 0) or 0)
    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        value = value.astimezone(datetime.timezone.utc)
    head = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    if isinstance(value, datetime.datetime):
        head = f"{value.year:04d}-{value.month:02d}-{value.day:02d} {head}"
    # without the zeros at the end: a `datetime` takes no more than three
    # digits in its text, whatever they are
    frac = f"{ns:09d}"[:MSSQL_DIGITS].rstrip("0")
    return f"{head}.{frac}" if frac else head


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
        return json_write(value)
    if isinstance(value, (memoryview, bytearray)):
        # psycopg2 hands a `bytea` column back as a memoryview, and pymysql
        # has no escape rule for one - measured, it stored the *text* of the
        # object: `<memory at 0x10ad03dc0>`, 23 bytes where the source held
        # 3. No error, the right number of rows, and the bytes replaced by
        # an address. The digest is what caught it.
        return bytes(value)
    return value


def json_write(value):
    """A document as JSON text for a driver to send, every number written
    as the digits it holds - a Decimal `1.50` as `1.50`, not a float and
    not a string.

    `json.dumps(..., default=str)` was here: it wrote a Decimal as the
    string `"1.50"`, a datetime and bytes as strings of their Python text,
    and the target stored a string where the source held a number. What
    JSON has no form for - NaN, an infinity, a date, bytes - is refused by
    name rather than written as something else."""
    import json
    import math
    from decimal import Decimal

    def one(x):
        if x is None:
            return "null"
        if x is True:
            return "true"
        if x is False:
            return "false"
        if isinstance(x, int):
            return str(x)
        if isinstance(x, float):
            if not math.isfinite(x):
                raise ValueError(f"{x!r} has no JSON form")
            return repr(x)
        if hasattr(x, "to_decimal"):
            # BSON's Decimal128, as the decimal it holds
            x = x.to_decimal()
        if isinstance(x, Decimal):
            if not x.is_finite():
                raise ValueError(f"{x} has no JSON form")
            return str(x)
        if isinstance(x, str):
            return json.dumps(x, ensure_ascii=False)
        if isinstance(x, dict):
            return "{" + ", ".join(
                f"{json.dumps(str(k), ensure_ascii=False)}: {one(v)}"
                for k, v in sorted(x.items(), key=lambda kv: str(kv[0])))\
                + "}"
        if isinstance(x, (list, tuple)):
            return "[" + ", ".join(one(e) for e in x) + "]"
        raise ValueError(f"a {type(x).__name__} inside a document has no"
                         f" JSON form: {x!r}")
    return one(value)


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
    if isinstance(cls, str) and cls.startswith(bsontype_prefixes()):
        # `long` and 5: MongoDB's value by its type, or the other side's
        # column's value as it holds that type
        from . import bsontype
        got = bsontype.value_of(text)
        if cls.startswith(bsontype.OTHER):
            # the other side's value is looked up as it is written there
            # (`write_value`): a JSON column's from the BSON value
            t = cls[len(bsontype.OTHER):]
            if t == "json":
                return got
            if t in ("int", "long"):
                return int(got)
            if t == "string":
                return str(got)
            if t == "uuid":
                import uuid
                return str(uuid.UUID(bytes=bytes(got)))
        return got
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
        return _bytea_escaped(raw)
    return text


def _bytea_escaped(raw):
    """PostgreSQL's other text for bytes (`bytea_output = escape`): a
    printable byte as its character, a backslash doubled, anything else
    `\\ooo` in octal. Read as the characters' own UTF-8, `\\000\\377A`
    was nine bytes where the value is three. Text that is not in that form
    is taken as the characters' bytes, as before."""
    out, i = bytearray(), 0
    while i < len(raw):
        ch = raw[i]
        if ch == "\\":
            if raw[i + 1:i + 2] == "\\":
                out.append(92)
                i += 2
                continue
            digits = raw[i + 1:i + 4]
            if len(digits) == 3 and all(d in "01234567" for d in digits):
                out.append(int(digits, 8))
                i += 4
                continue
            return raw.encode()
        if not " " <= ch <= "~":
            return raw.encode()
        out.append(ord(ch))
        i += 1
    return bytes(out)


def fold_rows(classes, rows, total=0):
    """(rows folded, running digest) over rows of values, rendered by
    their classes and folded one row at a time as `digest_step` folds -
    the one fold every engine that digests in this process uses, and the
    number the SQL engines' own digests are held to. Each column's
    rendering is chosen once (`render.digest_renderer`): a wide value as
    its length and md5, as every server writes it (`WIDE`)."""
    from . import render, rowtext
    fns = [render.digest_renderer(c) for c in classes]
    n = 0
    for row in rows:
        total = digest_step(total, rowtext.encode(
            [f(v) for f, v in zip(fns, row)]))
        n += 1
    return n, total


def digest_step(total, text):
    """Fold one row's text into a running digest, the same way the SQL does.

    Separate from the aggregate that calls it so the arithmetic can be tested
    without a database, and so an engine that has to accumulate in Python
    cannot drift from the one that accumulates in SQL.
    """
    got = _hashlib.md5(("" if text is None else str(text)).encode(),
                       usedforsecurity=False).digest()
    # the first DIGEST_HEX hex digits, read off the bytes: the same number
    # as `int(hexdigest()[:DIGEST_HEX], 16)`, without writing the hex out
    # (0.385 -> 0.36 us a row, measured 2026-10-01)
    return total + (int.from_bytes(got[:_DIGEST_BYTES], "big")
                    >> _DIGEST_SHIFT)


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


#: What a temporal column is *for*, which is a different question from how
#: to render it. Two columns can share a canonical class - and therefore
#: compare correctly value by value - while one records an instant in time
#: and the other records digits off a wall clock. Moving data from one to
#: the other is a silent conversion that no row count and no checksum of
#: the values as they now stand will show.
#:
#: The word `timestamp` means **opposite things** in the two engines
#: mapped here, which is why this table exists rather than a rule of thumb.
#: Measured, not assumed:
#:
#:   postgres  '2020-11-01 01:05:00+04' -> timestamp    2020-11-01 01:05:00
#:                                      -> timestamptz  2020-10-31 21:05:00+00
#:   mysql     written at time_zone '+00:00', read at '+07:00'
#:                        datetime   2026-07-01 12:00:00  (unchanged)
#:                        timestamp  2026-07-01 19:00:00  (converted)
INSTANT, WALL = "instant", "wall clock"

TIME_MEANING = {
    "postgres": {
        "timestamp with time zone": INSTANT, "timestamptz": INSTANT,
        "time with time zone": INSTANT, "timetz": INSTANT,
        "timestamp without time zone": WALL, "timestamp": WALL,
        "time without time zone": WALL, "time": WALL, "date": WALL,
    },
    "mysql": {
        "timestamp": INSTANT,
        "datetime": WALL, "date": WALL, "time": WALL, "year": WALL,
    },
    # From each engine's own definition of the type: whether it keeps a
    # point in time (stored as one, shown in some zone) or the digits of a
    # clock with no zone. Oracle's LOCAL TIME ZONE keeps the instant and
    # shows it in the session's zone. ClickHouse, MongoDB and Cassandra
    # have only the instant, and a wall clock moved into one is written as
    # that clock read at a zone migkit pins; which zone that was is the
    # meaning the hop carries, so they are not in this table
    "mssql": {"datetime": WALL, "datetime2": WALL, "smalldatetime": WALL,
              "date": WALL, "time": WALL, "datetimeoffset": INSTANT},
    "oracle": {"date": WALL, "timestamp": WALL,
               "timestamp with time zone": INSTANT,
               "timestamp with local time zone": INSTANT},
    "duckdb": {"timestamp": WALL, "timestamp_ns": WALL, "timestamp_ms": WALL,
               "timestamp_s": WALL, "timestamp with time zone": INSTANT,
               "date": WALL, "time": WALL},
    "bigquery": {"datetime": WALL, "timestamp": INSTANT, "date": WALL,
                 "time": WALL},
    "snowflake": {"timestamp_ntz": WALL, "timestamp_ltz": INSTANT,
                  "timestamp_tz": INSTANT, "date": WALL, "time": WALL},
    "redshift": {"timestamp without time zone": WALL,
                 "timestamp with time zone": INSTANT, "date": WALL,
                 "time without time zone": WALL},
    "db2": {"timestamp": WALL, "date": WALL, "time": WALL},
    "db2i": {"timestamp": WALL, "date": WALL, "time": WALL},
    "db2zos": {"timestamp": WALL, "date": WALL, "time": WALL},
    # by each format's specification: Iceberg's `timestamp` has no zone
    # and `timestamptz` is adjusted to UTC; Delta's `timestamp` is
    # adjusted to UTC and `timestamp_ntz` has no zone
    "iceberg": {"timestamp": WALL, "timestamp_ns": WALL,
                "timestamptz": INSTANT, "timestamptz_ns": INSTANT,
                "date": WALL, "time": WALL},
    "delta": {"timestamp": INSTANT, "timestamp_ntz": WALL, "date": WALL},
    # by Teradata's definition of its types, OData's (a DateTime has no
    # zone, a DateTimeOffset is an instant) and the words `engines.saas`
    # gives each kind's fields
    "teradata": {"timestamp": WALL, "timestamp with time zone": INSTANT,
                 "time": WALL, "time with time zone": INSTANT,
                 "date": WALL},
    "sapodp": {"edm.datetime": WALL, "edm.date": WALL, "edm.time": WALL,
               "edm.datetimeoffset": INSTANT},
    "saas": {"timestamp": WALL, "timestamptz": INSTANT, "date": WALL,
             "time": WALL},
}


def time_meaning(engine, declared):
    """`INSTANT`, `WALL`, or None when this is not a temporal column - or
    when migkit has not measured what this engine means by it, which the
    caller has to tell apart from "they agree"."""
    if not declared:
        return None
    # `timestamp(6) with time zone` and `datetime(3)` have to land on the
    # same key as the bare spelling, and this file carries no imports
    name, depth = [], 0
    for ch in str(declared).lower():
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            name.append(ch)
    return TIME_MEANING.get(engine, {}).get(" ".join("".join(name).split()))


#: How much a column can hold. A narrower target is not a schema difference
#: worth arguing about until a row actually exceeds it - and then it is the
#: thing that stops the load halfway through.
INT_RANGES = {
    "postgres": {
        "smallint": (-32768, 32767), "int2": (-32768, 32767),
        "integer": (-2147483648, 2147483647),
        "int4": (-2147483648, 2147483647),
        "bigint": (-9223372036854775808, 9223372036854775807),
        "int8": (-9223372036854775808, 9223372036854775807),
    },
    "mysql": {
        "tinyint": (-128, 127), "tinyint unsigned": (0, 255),
        "smallint": (-32768, 32767), "smallint unsigned": (0, 65535),
        "mediumint": (-8388608, 8388607),
        "mediumint unsigned": (0, 16777215),
        "int": (-2147483648, 2147483647),
        "int unsigned": (0, 4294967295),
        "bigint": (-9223372036854775808, 9223372036854775807),
        "bigint unsigned": (0, 18446744073709551615),
    },
}

_I64 = (-2 ** 63, 2 ** 63 - 1)
_I32 = (-2 ** 31, 2 ** 31 - 1)
_I16 = (-2 ** 15, 2 ** 15 - 1)
INT_RANGES.update({
    "mssql": {"tinyint": (0, 255), "smallint": _I16, "int": _I32,
              "bigint": _I64},
    "clickhouse": {**{f"int{b}": (-2 ** (b - 1), 2 ** (b - 1) - 1)
                      for b in (8, 16, 32, 64, 128, 256)},
                   **{f"uint{b}": (0, 2 ** b - 1)
                      for b in (8, 16, 32, 64, 128, 256)}},
    "duckdb": {"tinyint": (-128, 127), "smallint": _I16, "integer": _I32,
               "bigint": _I64, "hugeint": (-2 ** 127, 2 ** 127 - 1),
               "utinyint": (0, 255), "usmallint": (0, 65535),
               "uinteger": (0, 2 ** 32 - 1), "ubigint": (0, 2 ** 64 - 1),
               "uhugeint": (0, 2 ** 128 - 1)},
    # `varint` holds any integer and `counter` a signed 64-bit one
    "cassandra": {"tinyint": (-128, 127), "smallint": _I16, "int": _I32,
                  "bigint": _I64, "counter": _I64},
    "db2": {"smallint": _I16, "integer": _I32, "bigint": _I64},
    "db2i": {"smallint": _I16, "integer": _I32, "bigint": _I64},
    "db2zos": {"smallint": _I16, "integer": _I32, "bigint": _I64},
    "ase": {"tinyint": (0, 255), "smallint": _I16, "int": _I32,
            "integer": _I32, "bigint": _I64,
            "unsigned smallint": (0, 65535),
            "unsigned int": (0, 2 ** 32 - 1),
            "unsigned bigint": (0, 2 ** 64 - 1)},
    "redshift": {"smallint": _I16, "integer": _I32, "bigint": _I64},
    "bigquery": {"int64": _I64},
    "parquet": {"int64": _I64},
    "snowflake": {"integer": (-(10 ** 38 - 1), 10 ** 38 - 1)},
    # SQLite and the drivers of MongoDB and Cassandra store a whole number in
    # at most 64 bits, whatever the column or field is declared
    "sqlite": {n: _I64 for n in ("int", "integer", "tinyint", "smallint",
                                 "mediumint", "bigint", "int2", "int8")},
    "mongodb": {"int": _I32, "long": _I64},
    "teradata": {"byteint": (-128, 127), "smallint": _I16, "integer": _I32,
                 "bigint": _I64},
    "sapodp": {"edm.byte": (0, 255), "edm.sbyte": (-128, 127),
               "edm.int16": _I16, "edm.int32": _I32, "edm.int64": _I64},
})

CHAR_TYPES = {
    "postgres": ("character varying", "varchar", "character", "char",
                 "bpchar"),
    "mysql": ("varchar", "char"),
    # its length in characters as `DECLARED` writes it: an n-type's bytes
    # halved
    "mssql": ("varchar", "char", "nvarchar", "nchar"),
}

#: Types with no character limit worth counting. `("chars", None)` rather
#: than None, because "holds anything" and "migkit never measured this" are
#: different answers and only one of them is safe to ignore. SQL Server's
#: `(max)` is one: `varchar(max)` has no number to count against
UNLIMITED_CHARS = {"postgres": ("text", "citext"),
                   "mssql": ("varchar", "nvarchar", "text", "ntext")}

#: MySQL's TEXT family is limited in **bytes**, not characters, so a
#: utf8mb4 string of 20,000 characters can overflow a 65,535-byte TEXT.
#: Kept as its own kind so it is never quietly compared against a
#: character limit.
BYTE_TYPES = {
    "mysql": {"tinytext": 255, "text": 65535, "mediumtext": 16777215,
              "longtext": 4294967295},
}

NUMERIC_TYPES = {
    "postgres": ("numeric", "decimal"),
    "mysql": ("decimal", "numeric"),
    "mssql": ("decimal", "numeric"),
}


def _split_declared(declared):
    """('varchar', [50]) from 'varchar(50)', without importing anything."""
    name, nums, depth, digits = [], [], 0, []
    for ch in str(declared).lower():
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if digits:
                nums.append(int("".join(digits)))
                digits = []
        elif depth:
            if ch.isdigit():
                digits.append(ch)
            elif ch == "," and digits:
                nums.append(int("".join(digits)))
                digits = []
        else:
            name.append(ch)
    return " ".join("".join(name).split()), nums


def capacity(engine, declared):
    """What this column can hold, or None when migkit has not measured it.

    ("chars", n) - at most n characters
    ("int", lo, hi) - a whole number in that range
    ("numeric", precision, scale)

    None is returned both for an unlimited type and for one nobody mapped,
    and the caller treats it the same way: nothing to compare against, so
    no claim is made.
    """
    name, nums = _split_declared(declared)
    if name in CHAR_TYPES.get(engine, ()) and nums:
        return ("chars", nums[0])
    if name in UNLIMITED_CHARS.get(engine, ()):
        return ("chars", None)
    if name in BYTE_TYPES.get(engine, {}):
        return ("bytes", BYTE_TYPES[engine][name])
    if name in INT_RANGES.get(engine, {}):
        return ("int",) + INT_RANGES[engine][name]
    if name in NUMERIC_TYPES.get(engine, ()) and len(nums) >= 1:
        return ("numeric", nums[0], nums[1] if len(nums) > 1 else 0)
    return None


def narrower(src, dst):
    """Can a value that fits `src` fail to fit `dst`.

    Two different kinds are not compared here - a type that changed class
    is `comparable`'s business, and answering both questions in two places
    is how they end up disagreeing.
    """
    if src is None or dst is None or src[0] != dst[0]:
        return False
    if src[0] == "chars":
        if dst[1] is None:
            return False
        return src[1] is None or src[1] > dst[1]
    if src[0] == "bytes":
        return src[1] > dst[1]
    if src[0] == "int":
        return src[1] < dst[1] or src[2] > dst[2]
    if src[0] == "numeric":
        return src[1] - src[2] > dst[1] - dst[2] or src[2] > dst[2]
    raise ValueError(f"no rule for capacity kind {src[0]!r}")


# Values that would not arrive as themselves.
#
# A cross-engine move can change a value without an error: MySQL rounds a
# decimal's extra fraction digits even in strict mode (Note 1265 only),
# ClickHouse writes 0 or '' for a NULL into a column that is not Nullable,
# the PostgreSQL driver reads `infinity` as 9999-12-31 and `24:00:00` as
# 00:00:00, a MySQL JSON column turns a number it cannot hold as a 64-bit
# integer into a double. Or the target refuses a row halfway through: a
# NUL character into PostgreSQL text, an unsigned 64-bit integer into a
# bigint, a NaN into MySQL. Either way the move is the wrong place to find
# out. Each such case is a *kind* here - a question the source can be asked
# about one column before anything is read to be moved, answered with a
# count and the keys of a few rows, so the operator decides what each value
# should become instead of the move deciding it silently.
#
# `unfit` says which kinds a column pair needs asking; `unfit_sql` asks the
# source in its own SQL where it has any; `unfit_value` asks a value read
# into this process, for every other engine and for what SQL cannot see.
# The two are held to the same answers by the tests.

#: kinds, and what a row of each is, as the report says it
UNFIT_WORDS = {
    "nonfinite": "infinity or -infinity, which {dst} has no value for (read"
                 " through a driver it becomes 9999-12-31)",
    "years": "a year before 1 AD or after 9999, which {dst} cannot hold",
    "time24": "the time 24:00:00, which is read as 00:00:00 on its way to"
              " {dst}",
    "timeday": "a TIME outside 00:00:00 to 23:59:59.999999 - a duration,"
               " which a time of day on {dst} cannot hold",
    "fraction": "more than {arg} digits of a second, which {dst} rounds or"
                " cuts to {arg}",
    "numeric-nonfinite": "NaN or an infinity in a decimal, which {dst} has no"
                         " value for",
    "float-nonfinite": "NaN or an infinity, which {dst} refuses",
    "scale": "more than {arg} digits after the point, which {dst} rounds to"
             " {arg} without an error",
    "digits": "more digits before the point than {dst}'s decimal({arg[0]},"
              "{arg[1]}) holds",
    "sigdigits": "more than {arg} significant digits, which {dst} cannot hold"
                 " exactly",
    "magnitude": "a number outside the range {dst} holds ({arg[0]} to"
                 " {arg[1]})",
    "int-range": "a whole number outside {arg[0]}..{arg[1]}, the range of"
                 " the {dst} column",
    "chars": "more than {arg} characters, the {dst} column's limit",
    "bytes": "more than {arg} bytes, the {dst} column's limit",
    "utf16": "more than {arg} UTF-16 code units, the {dst} column's limit (a"
             " character outside the Basic Multilingual Plane takes two)",
    "nul": "the character U+0000, which {dst} text refuses",
    "supplementary": "a character outside the Basic Multilingual Plane (an"
                     " emoji, say), which the {arg} column on {dst} cannot"
                     " hold",
    "charset": "a character the {arg} column on {dst} has no code for",
    "inet-mask": "an address with a mask, which the {dst} column keeps"
                 " without it",
    "array-bounds": "an array that does not start at 1, which a JSON array"
                    " on {dst} cannot say",
    "json-form": "a value inside a document that JSON has no form for - a"
                 " date, bytes, NaN, an object id - which {dst}'s JSON"
                 " cannot hold as it is",
    "json-number": "a JSON number {dst} stores as a double that is not the"
                   " same number - a whole number past 64 bits, or a"
                   " decimal of more than 15 significant digits its JSON"
                   " reads as another",
    "null": "NULL, which the {dst} column is not Nullable for and writes as"
            " 0 or ''",
    "not-null": "NULL, which the {dst} column does not take - the load would"
                " stop there",
    "merge": "keys distinct here that {dst}'s {arg} takes for one key - the"
             " later row would overwrite the earlier",
    "signed-zero": "keys that differ only by the sign of a zero, which {dst}"
                   " takes for one key - the later row would overwrite the"
                   " earlier",
    "bson-type": "a value of another BSON type than the {arg} the {dst}"
                 " column holds - it would come back to MongoDB a {arg}",
    "bson-order": "a document of more than one field, whose fields {dst}'s"
                  " JSON puts in an order of its own",
    "bson-form": "a value MongoDB's {arg} has no form for",
    "bson-none": "a {arg}, which MongoDB has no type for",
    "vector-width": "a vector whose elements a {arg}-bit float on {dst}"
                    " does not hold exactly",
    "vector-dims": "a vector of other than the {arg} dimensions the {dst}"
                   " column holds",
    "vector-nonfinite": "a vector holding NaN or an infinity, which {dst}"
                        " refuses",
    "geometry-geojson": "a shape GeoJSON cannot say - an SRID other than"
                        " 4326, or a measure (M) - which {dst} keeps as"
                        " GeoJSON",
    "geometry-form": "a value that is no GeoJSON shape, which the {dst}"
                     " column holds shapes only",
    "empty-collection": "an empty {arg}, which {dst} stores as NULL - the"
                        " two are one there",
    "interval-part": "an interval with {arg}, which the {dst} interval type"
                     " has no part for",
    "set-order": "an array out of order or holding a value twice, which a"
                 " set on {dst} keeps sorted and once each",
    "no-writer": "a {arg}, which the row copy has no way to write into"
                 " {dst}",
    "null-element": "a NULL inside a collection, which {dst} refuses",
}


#: the most digits after the second each engine's types keep, whatever a
#: declaration asks for: PostgreSQL takes `timestamp(9)` and keeps six
MOST_DIGITS = {"postgres": 6, "mysql": 6, "mssql": 7, "redshift": 6,
               "bigquery": 6, "parquet": 6, "db2": 12, "teradata": 6}


def _temporal_digits(engine, declared):
    """How many digits after the second a temporal type keeps, or None
    where migkit has not measured it."""
    got = _declared_digits(engine, declared)
    most = MOST_DIGITS.get(engine)
    return min(got, most) if got is not None and most else got


def _declared_digits(engine, declared):
    engine = dialect(engine)
    name, nums = _split_declared(declared)
    base = name.split(" ")[0]
    if type_class(engine, declared) == INTERVAL:
        # an interval's time: microseconds in PostgreSQL and DuckDB,
        # nanoseconds in a Cassandra `duration`, as declared in Oracle's
        # day-to-second (six where it says nothing)
        if engine == "oracle":
            return (nums[-1] if name.endswith("second") and nums else 6)
        return {"postgres": 6, "duckdb": 6, "cassandra": 9}.get(engine)
    if engine == "postgres":
        return nums[0] if nums else 6
    if engine == "mysql":
        return nums[0] if nums else 0
    if engine == "mssql":
        if base in ("datetime2", "time", "datetimeoffset"):
            return nums[0] if nums else 7
        return {"datetime": 2, "smalldatetime": 0}.get(base)
    if engine == "clickhouse":
        return (nums[0] if nums else 3) if base == "datetime64" else \
            0 if base == "datetime" else None
    if engine in ("oracle", "db2"):
        return (nums[0] if nums else 6) if base == "timestamp" else \
            0 if base == "date" else None
    if engine == "duckdb":
        return {"timestamp_ns": 9, "timestamp_ms": 3,
                "timestamp_s": 0}.get(base, 6)
    if engine in ("mongodb", "cassandra"):
        return 9 if (engine, base) == ("cassandra", "time") else 3
    if engine == "snowflake":
        return nums[0] if nums else 9
    if engine in ("bigquery", "redshift", "parquet"):
        return 6
    return None


def _fixed_decimal(engine, declared):
    """(precision, scale) a decimal column is held to, or None where it
    keeps what it is given (PostgreSQL's `numeric` with no typmod)."""
    engine = dialect(engine)
    name, nums = _split_declared(declared)
    if engine == "clickhouse" and name in ("decimal32", "decimal64",
                                           "decimal128", "decimal256"):
        return ({"decimal32": 9, "decimal64": 18, "decimal128": 38,
                 "decimal256": 76}[name], nums[0] if nums else 0)
    if engine in ("mssql", "ase") and name in ("money", "smallmoney"):
        return (19, 4) if name == "money" else (10, 4)
    if engine == "bigquery" and name in ("numeric", "bignumeric"):
        return (38, 9) if name == "numeric" else (76, 38)
    if name in ("decimal", "numeric", "number", "dec") and nums:
        return (nums[0], nums[1] if len(nums) > 1 else 0)
    if engine == "mysql" and name in ("decimal", "numeric"):
        return (10, 0)
    return None


def written_as(src, declared, dst):
    """What a value of a source column is stored as on an engine that
    makes a table on the first write, so has no column type to be read
    before - MongoDB: the BSON type the column's values are written as
    (`bsontype.column_tag`), a date keeping milliseconds, a whole number
    by its column's width (a `bigint` a `long`: pymongo's own choice was
    32 bits wherever a value fit), a decimal a Decimal128 of 34 digits.
    None where the engine has no type for it."""
    if dst != "mongodb":
        return None
    from . import bsontype
    cls = type_class(src, declared)
    if cls in (VECTOR, GEOMETRY):
        return cls
    return bsontype.column_tag(src, declared, cls) if cls else None

#: The widest decimal each engine builds: (precision, scale)
DECIMAL_MOST = {"mysql": (65, 30), "mssql": (38, 38), "clickhouse": (76, 76),
                "db2": (31, 31), "duckdb": (38, 38), "ase": (38, 38),
                "redshift": (38, 37), "snowflake": (38, 37),
                "bigquery": (76, 38), "teradata": (38, 38)}

#: decimal types that hold at most this many significant digits and keep
#: no scale of their own
SIGNIFICANT = {"dynamodb": 38, "mongodb": 34, "oracle": 38,
               "snowflake": 38}

#: engines that refuse a float's NaN and infinities
NO_NONFINITE_FLOAT = {"mysql", "mssql", "dynamodb", "db2", "ase",
                      "opensearch", "teradata"}

#: engines whose decimal holds NaN and the infinities
NONFINITE_DECIMAL = {"postgres", "mongodb"}

#: MySQL character sets that hold the Basic Multilingual Plane and nothing
#: past it
BMP_CHARSETS = {"utf8mb3", "utf8", "ucs2"}

#: MySQL's single-byte character sets, by the codec that holds exactly
#: what each does (MySQL's `latin1` is Windows-1252, not ISO 8859-1)
CODECS = {"latin1": "cp1252", "latin2": "iso8859_2", "ascii": "ascii",
          "cp1250": "cp1250", "cp1251": "cp1251", "cp1256": "cp1256",
          "cp1257": "cp1257", "greek": "iso8859_7", "hebrew": "iso8859_8",
          "latin5": "iso8859_9", "latin7": "iso8859_13", "koi8r": "koi8_r",
          "koi8u": "koi8_u", "tis620": "tis_620"}

#: address types that keep no mask
ADDRESS_ONLY = {"clickhouse": ("ipv4", "ipv6"), "cassandra": ("inet",)}

#: DynamoDB's number range
DYNAMO_RANGE = ("1e-130", "1e126")


#: JSON types that keep an object's fields in an order of their own
JSON_SORTS_KEYS = {("mysql", "json"), ("postgres", "jsonb")}

#: vector types by the width of float each element is kept in
VECTOR_WIDTH = {"postgres": {"vector": 32, "halfvec": 16, "sparsevec": 32},
                "mysql": {"vector": 32}, "clickhouse": {"array": 32},
                "cassandra": {"vector": 32}, "duckdb": {"float": 32,
                                                        "real": 32,
                                                        "float4": 32}}

#: engines whose vector refuses NaN and the infinities
NO_NONFINITE_VECTOR = {"postgres"}

#: engines a shape goes into only through their own constructor, which
#: the row copy does not call: SQL Server takes no binary but its own
#: serialisation into a `geography` or a `geometry`
NO_SHAPE_WRITER = {"mssql"}


def vector_width(engine, declared):
    """16, 32 or 64: the float each element of a vector column is kept in,
    or None where the column says nothing (a MongoDB array holds doubles,
    and a double need not be a float)."""
    base = str(declared or "").strip().lower()
    for cut in ("(", "<", "["):
        base = base.split(cut)[0].strip()
    if engine == "oracle":
        return 64 if "float64" in str(declared).lower() else 32
    return VECTOR_WIDTH.get(engine, {}).get(base)


def _unfit_bson(src, src_declared, scls, dst, dst_declared):
    """The kinds a MongoDB value has on its way into another engine, or
    another engine's on its way into MongoDB (`bsontype`)."""
    from . import bsontype
    if src == "mongodb" and dst != "mongodb":
        got = bson_pair(src, src_declared, dst, dst_declared)
        if not got or not got[1] or not got[1].startswith(bsontype.OTHER):
            return []
        t = got[1][len(bsontype.OTHER):]
        # what the field holds, by `$type` over every document: a field
        # holding only the column's type has no value of another to count
        seen = {x.strip() for x in str(src_declared or "").split("|")} \
            - {"null", "missing", ""}
        if t != "json":
            want = {"number": "decimal"}.get(t, t)
            return [] if seen == {want} else [("bson-type", t)]
        base = str(dst_declared or "").strip().lower().split("(")[0]
        sorts = (dst, base) in JSON_SORTS_KEYS or \
            collection_kind(dst, dst_declared) == "map"
        return ([("bson-order", None)] if sorts
                and seen & {"object", "array"} else [])
    if dst == "mongodb" and src != "mongodb" and scls:
        if scls in (VECTOR, GEOMETRY):
            return []
        t = bsontype.column_tag(src, src_declared, scls)
        if t is None:
            return [("bson-none", scls)]
        if t == "bool":
            return [("bson-form", t)]
    return []


def _unfit_shapes(src, src_declared, scls, dst, dst_declared, dcls,
                  src_facts):
    """The kinds of a vector, a shape and a collection on their way."""
    if src == "mongodb" and dst != "mongodb":
        got = bson_pair(src, src_declared, dst, dst_declared)
        if got and got[0] in (VECTOR, GEOMETRY, INTERVAL):
            scls = got[0]
    out = []
    if scls == VECTOR and dcls == VECTOR and dst != "mongodb":
        mine, theirs = (vector_width(src, src_declared),
                        vector_width(dst, dst_declared))
        if theirs and (mine is None or mine > theirs):
            out.append(("vector-width", theirs))
        n = vector_dims(dst, dst_declared)
        if n and vector_dims(src, src_declared) != n:
            out.append(("vector-dims", n))
        if dst in NO_NONFINITE_VECTOR and src not in NO_NONFINITE_VECTOR:
            out.append(("vector-nonfinite", None))
    if scls == GEOMETRY and dst == "mongodb":
        out.append(("geometry-geojson", None))
    if scls == GEOMETRY and src == "mongodb":
        # a document is a shape only where it is GeoJSON
        out.append(("geometry-form", None))
    if scls == GEOMETRY and dcls == GEOMETRY and dst in NO_SHAPE_WRITER:
        out.append(("no-writer", "shape"))
    kind = collection_kind(dst, dst_declared)
    if kind and src != dst and (scls in ("json", BSON) or src == "mongodb"):
        # a collection empty on the source and NULL on the target read
        # alike to nobody, and differently to the application
        out.append(("empty-collection", kind))
        if kind in ("list", "set"):
            out.append(("null-element", None))
        if kind == "set":
            out.append(("set-order", None))
    if dst == "clickhouse" and str(dst_declared or "").strip().lower() \
            .startswith("array(") and (src_facts or {}).get("null") \
            is not False:
        # an Array cannot be Nullable: a NULL is written as an empty one
        out.append(("null", None))
    return out


def unfit(src, src_declared, dst, dst_declared, facts=None,
          src_facts=None):
    """[(kind, arg)] to ask of one source column before it is moved into
    one target column - `dst_declared` is the target's type as it stands,
    or as migkit is about to build it. `facts` is what the target says
    of the column beyond its type: `null` (False where it takes none),
    `charset`; `src_facts` the same of the source's column.

    Only kinds that can happen for this pair: asking a question whose
    answer is always nothing costs a scan and says nothing."""
    facts = facts or {}
    src, dst = dialect(src), dialect(dst)
    if dst == "mongodb" and src != "mongodb" and src_declared:
        # into MongoDB a value is written as the type its source column is
        # held as there (`bsontype.column_tag`), whatever the field
        # already holds: held to the field's type, a `bigint` beside a
        # field of 32-bit integers was refused past 2**31 and would have
        # been written as a 64-bit one
        dst_declared = written_as(src, src_declared, dst) or dst_declared
    scls, dcls = type_class(src, src_declared), type_class(dst, dst_declared)
    out = []
    if dcls == "json" and src not in ("postgres", "mysql", "mongodb"):
        # a document store's value is a document of its own types, of
        # which JSON holds only some - a field migkit has no class for as
        # well as one it has. MongoDB's go as their Extended JSON
        out.append(("json-form", None))
    out += _unfit_bson(src, src_declared, scls, dst, dst_declared)
    out += _unfit_shapes(src, src_declared, scls, dst, dst_declared, dcls,
                         src_facts)
    if not scls:
        return out
    other = src != dst
    if scls in ("timestamp", "date") and src == "postgres" and other:
        out += [("nonfinite", None), ("years", None)]
    if scls == "time" and other:
        if src == "postgres":
            out.append(("time24", None))
        if src == "mysql":
            out.append(("timeday", None))
    if scls == dcls == INTERVAL:
        mine, theirs = (_declared_digits(src, src_declared),
                        _declared_digits(dst, dst_declared))
        if theirs is not None and (mine is None or mine > theirs):
            out.append(("fraction", theirs))
        # Oracle keeps an interval as one of two types, each with no room
        # for the other's parts
        held = _split_declared(dst_declared)[0] if dst == "oracle" else ""
        if held.endswith("to second"):
            out.append(("interval-part", "months"))
        elif held.endswith("to month"):
            out.append(("interval-part", "days or a time"))
    if scls in ("timestamp", "time") and dcls in ("timestamp", "time"):
        mine, theirs = (_temporal_digits(src, src_declared),
                        _temporal_digits(dst, dst_declared))
        if theirs is not None and (mine is None or mine > theirs):
            out.append(("fraction", theirs))
    if scls == "decimal" and src in NONFINITE_DECIMAL \
            and dst not in NONFINITE_DECIMAL:
        out.append(("numeric-nonfinite", None))
    if scls == "float" and dst in NO_NONFINITE_FLOAT:
        out.append(("float-nonfinite", None))
    if scls in ("integer", "decimal", "float") and dcls in ("integer",
                                                            "decimal"):
        rng = INT_RANGES.get(dst, {}).get(_split_declared(dst_declared)[0])
        fixed = _fixed_decimal(dst, dst_declared) if dcls == "decimal" \
            else None
        if dcls == "integer" and scls != "integer":
            # a fraction into a whole number is rounded away
            out.append(("scale", 0))
        if dcls == "integer" and rng:
            mine = INT_RANGES.get(src, {}).get(
                _split_declared(src_declared)[0])
            if scls != "integer" or not mine or mine[0] < rng[0] \
                    or mine[1] > rng[1]:
                out.append(("int-range", rng))
        elif fixed:
            mine = _fixed_decimal(src, src_declared) \
                if scls == "decimal" else None
            if scls != "integer" and (mine is None or mine[1] > fixed[1]):
                out.append(("scale", fixed[1]))
            wide = INT_RANGES.get(src, {}).get(
                _split_declared(src_declared)[0])
            if scls == "integer" and wide:
                fits = max(-wide[0], wide[1]) < 10 ** (fixed[0] - fixed[1])
            else:
                fits = mine is not None and \
                    mine[0] - mine[1] <= fixed[0] - fixed[1] and \
                    mine[1] <= fixed[1]
            if not fits:
                out.append(("digits", fixed))
    # a whole number of a known range cannot pass either limit: 38 digits
    # hold every 64-bit integer, and 1e126 is past every one of them
    bounded = scls == "integer" and INT_RANGES.get(src, {}).get(
        _split_declared(src_declared)[0], (None, 10 ** 38))[1] < 10 ** 38
    if scls in ("integer", "decimal") and dst in SIGNIFICANT \
            and dcls in ("decimal", None) and not bounded \
            and not _fixed_decimal(dst, dst_declared):
        out.append(("sigdigits", SIGNIFICANT[dst]))
    if scls in ("integer", "decimal", "float") and dst == "dynamodb" \
            and not bounded:
        out.append(("magnitude", DYNAMO_RANGE))
    if scls == "text" and dcls == "text":
        cap = capacity(dst, dst_declared)
        mine = capacity(src, src_declared)
        if cap and cap[0] in ("chars", "bytes") and cap[1] is not None \
                and (not mine or mine[0] != cap[0] or narrower(mine, cap)
                     or mine[1] is None):
            out.append((cap[0], cap[1]))
        name, nums = _split_declared(dst_declared)
        if dst == "mssql" and name in ("nvarchar", "nchar") and nums:
            out.append(("utf16", nums[0]))
        if dst == "postgres" and src != "postgres":
            out.append(("nul", None))
        charset = str(facts.get("charset") or "").lower()
        if charset in BMP_CHARSETS:
            out.append(("supplementary", charset))
        elif charset in CODECS:
            out.append(("charset", charset))
    if scls == "json" and dst == "mysql" and src != "mysql":
        out.append(("json-number", None))
    if scls == "json" and src == "postgres" and dst != "postgres" \
            and array_element(src, src_declared) is not None:
        out.append(("array-bounds", None))
    if scls == "inet" and dcls == "inet" \
            and str(dst_declared or "").strip().lower() in ADDRESS_ONLY.get(
                dst, ()):
        out.append(("inet-mask", None))
    if facts.get("null") is False \
            and (src_facts or {}).get("null") is not False:
        out.append(("null" if dst == "clickhouse" else "not-null", None))
    # each kind asked once, whichever rule found it
    return list(dict.fromkeys(out))


def unfit_words(kind, arg, dst):
    """What a row of `kind` is, in a sentence about the target."""
    return UNFIT_WORDS[kind].format(dst=dst, arg=arg)


def _pow10(n):
    """10**n as an exact numeric literal both SQL engines read as a
    decimal: MySQL reads `1e40` as a double."""
    return "1" + "0" * n if n >= 0 else "0." + "0" * (-n - 1) + "1"


def _unfit_sql_postgres(kind, c, arg, cls=None):
    # a number as the decimal it is: a double through its own text, which
    # is exact where `float8::numeric` keeps fifteen digits; anything else
    # through `numeric`, which a `money`'s text is not
    n = f"({c}::text::numeric)" if cls == "float" else f"({c}::numeric)"
    t = f"({c}::text)"

    def finite(p):
        # asked only of a finite number, in that order: `and` in SQL does
        # not promise to look at its left side first
        return (f"case when {c}::text in ('NaN', 'Infinity', '-Infinity')"
                f" then false else {p} end")
    if kind == "nonfinite":
        return f"not isfinite({c})"
    if kind == "years":
        return (f"isfinite({c}) and ({c} < '0001-01-01'"
                f" or {c} >= '10000-01-01')")
    if kind == "time24":
        return f"{c} = '24:00:00'"
    if kind == "fraction":
        if arg >= 6:
            return "false"
        return (f"(extract(microseconds from {c})::bigint"
                f" % {10 ** (6 - arg)}) <> 0")
    if kind == "numeric-nonfinite":
        return f"{c}::text in ('NaN', 'Infinity', '-Infinity')"
    if kind == "float-nonfinite":
        return (f"{c}::float8 in ('NaN'::float8, 'Infinity'::float8,"
                f" '-Infinity'::float8)")
    if kind == "scale":
        return finite(f"{n} <> round({n}, {arg})")
    if kind == "digits":
        return finite(f"abs(round({n}, {arg[1]}))"
                      f" >= {_pow10(arg[0] - arg[1])}")
    if kind == "sigdigits":
        return finite(f"length(trim(both '0' from"
                      f" replace(abs({n})::text, '.', ''))) > {arg}")
    if kind == "magnitude":
        return finite(f"{n} <> 0 and (abs({n}) >= {arg[1]}::numeric"
                      f" or abs({n}) < {arg[0]}::numeric)")
    if kind == "int-range":
        return f"{c} < {arg[0]} or {c} > {arg[1]}"
    # the text kinds through `::text`: a `tsvector`, an `inet` or a `uuid`
    # is compared as its text, and `char_length` takes none of them
    if kind == "chars":
        return f"char_length({t}) > {arg}"
    if kind == "bytes":
        return f"octet_length(convert_to({t}, 'UTF8')) > {arg}"
    if kind == "utf16":
        return (f"char_length({t}) + char_length(regexp_replace({t},"
                f" '[^\\U00010000-\\U0010FFFF]', '', 'g')) > {arg}")
    if kind == "nul":
        return "false"
    if kind == "supplementary":
        return f"{t} ~ '[\\U00010000-\\U0010FFFF]'"
    if kind == "json-number":
        # a row holding a number that can change (`json_candidate`); the
        # target then says which of them do
        return (f"exists (select 1 from jsonb_path_query(to_jsonb({c}),"
                f" 'strict $.**') v where jsonb_typeof(v) = 'number' and"
                f" case when v::text ~ '^-?[0-9]+$'"
                f" then v::text::numeric not between -9223372036854775808"
                f" and 18446744073709551615"
                f" else length(trim(both '0' from regexp_replace("
                f"split_part(lower(v::text), 'e', 1), '[^0-9]', '', 'g')))"
                f" > 15 end)")
    if kind == "array-bounds":
        # PostgreSQL writes an array whose first index is not 1 with its
        # bounds in front: `[0:1]={5,6}`
        return f"{c}::text like '[%'"
    if kind == "inet-mask":
        return (f"masklen({c}) < case family({c}) when 4 then 32"
                f" else 128 end")
    if kind in ("null", "not-null"):
        return f"{c} is null"
    return None


def _unfit_sql_mysql(kind, c, arg, cls=None):
    if kind == "timeday":
        return f"{c} < '00:00:00' or {c} >= '24:00:00'"
    if kind == "fraction":
        if arg >= 6:
            return "false"
        return f"microsecond({c}) % {10 ** (6 - arg)} <> 0"
    if kind in ("nonfinite", "years", "time24", "numeric-nonfinite",
                "float-nonfinite"):
        # MySQL holds none of these
        return "false"
    if kind == "scale":
        return f"{c} <> round({c}, {arg})"
    if kind == "digits":
        return f"abs(round({c}, {arg[1]})) >= {_pow10(arg[0] - arg[1])}"
    if kind == "sigdigits":
        return (f"length(trim(both '0' from replace(cast(abs({c}) as char),"
                f" '.', ''))) > {arg}")
    if kind == "magnitude":
        return (f"{c} <> 0 and (abs({c}) >= {arg[1]}"
                f" or abs({c}) < {arg[0]})")
    if kind == "int-range":
        return f"{c} < {arg[0]} or {c} > {arg[1]}"
    if kind == "chars":
        return f"char_length({c}) > {arg}"
    if kind == "bytes":
        return f"octet_length({c}) > {arg}"
    if kind == "nul":
        # in its bytes: under a `_ci` collation the search matched a row
        # with no NUL in it (measured on 8.4)
        return f"instr(cast({c} as binary), x'00') > 0"
    if kind == "supplementary":
        # converted into a character set that has only the first plane,
        # a character outside it becomes `?`: any byte that changed is one
        return (f"cast({c} as binary) <> cast(convert({c} using utf8mb3)"
                f" as binary)")
    if kind in ("null", "not-null"):
        return f"{c} is null"
    return None


def _unfit_sql_clickhouse(kind, c, arg, cls=None):
    """ClickHouse's: a source of billions of rows is not read into this
    process to be asked."""
    if kind in ("nonfinite", "years", "time24", "timeday",
                "numeric-nonfinite"):
        return "false"
    if kind == "fraction":
        if arg >= 9:
            return "false"
        return (f"toUnixTimestamp64Nano(toDateTime64({c}, 9))"
                f" % {10 ** (9 - arg)} != 0")
    if kind == "float-nonfinite":
        return f"isNaN({c}) or isInfinite({c})"
    if kind == "scale":
        return f"{c} != truncate({c}, {arg})"
    if kind == "int-range":
        return f"{c} < {arg[0]} or {c} > {arg[1]}"
    if kind == "chars":
        return f"lengthUTF8({c}) > {arg}"
    if kind == "bytes":
        return f"length({c}) > {arg}"
    if kind == "nul":
        return f"position({c}, char(0)) > 0"
    if kind == "supplementary":
        return f"match({c}, '[\\\\x{{10000}}-\\\\x{{10FFFF}}]')"
    if kind in ("null", "not-null"):
        return f"{c} is null"
    return None


UNFIT_SQL = {"postgres": _unfit_sql_postgres, "mysql": _unfit_sql_mysql,
             "clickhouse": _unfit_sql_clickhouse}


def unfit_sql(engine, kind, quoted, arg, cls=None):
    """A predicate true for the rows of `kind`, in this engine's SQL, or
    None where it has none and the rows are to be read and asked here.
    `cls` is the column's class, where the SQL depends on it."""
    build = UNFIT_SQL.get(engine)
    return build(kind, quoted, arg, cls) if build else None


def _json_numbers(value):
    """Every number in a JSON document, as the Decimal or int it was
    written as."""
    import json
    from decimal import Decimal
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value, parse_float=Decimal, parse_int=int)
    stack = [value]
    while stack:
        x = stack.pop()
        if isinstance(x, dict):
            stack.extend(x.values())
        elif isinstance(x, (list, tuple)):
            stack.extend(x)
        elif isinstance(x, bool) or x is None or isinstance(x, str):
            continue
        elif isinstance(x, (int, float, Decimal)):
            yield x


def unfit_value(kind, value, arg):
    """Whether one value read into this process is a row of `kind`. What
    a driver cannot hand over at all (an `infinity`, a year before
    Christ) is never one here: those kinds are asked in SQL."""
    import datetime
    import math
    from decimal import Decimal, localcontext
    if kind in ("null", "not-null"):
        return value is None
    if value is None or value is ABSENT:
        return False
    if kind.startswith(("bson-", "vector-", "geometry-")) \
            or kind in ("empty-collection", "null-element", "set-order"):
        return _unfit_typed(kind, value, arg)
    if kind == "no-writer":
        return True
    if kind == "interval-part":
        from . import intervals
        try:
            got = intervals.of(value)
        except ValueError:
            return True
        return bool(got.months) if arg == "months" \
            else bool(got.days or got.nanos)
    if kind == "fraction" and isinstance(value, str) \
            and value.startswith(UNCOMPARABLE):
        # digits past the ninth, which its reader could not keep
        return True
    if kind == "fraction" and _interval_like(value):
        from . import intervals
        try:
            return intervals.of(value).nanos % (10 ** (9 - arg)) != 0
        except ValueError:
            return False
    if hasattr(value, "to_decimal"):
        value = value.to_decimal()
    if kind == "timeday":
        return isinstance(value, datetime.timedelta) and not (
            datetime.timedelta(0) <= value < datetime.timedelta(days=1))
    if kind == "fraction":
        ns = getattr(value, "nanosecond", 0) or 0
        us = getattr(value, "microsecond", None)
        if us is None and isinstance(value, datetime.timedelta):
            us = value.microseconds
        if us is None:
            return False
        return (us * 1000 + ns) % (10 ** (9 - arg)) != 0
    if kind == "numeric-nonfinite":
        return isinstance(value, Decimal) and not value.is_finite()
    if kind == "float-nonfinite":
        if isinstance(value, float):
            return not math.isfinite(value)
        return isinstance(value, Decimal) and not value.is_finite()
    if kind in ("scale", "digits", "sigdigits", "magnitude"):
        if isinstance(value, bool):
            return False
        if isinstance(value, float):
            if not math.isfinite(value):
                return False
            value = Decimal(repr(value))
        if isinstance(value, int):
            value = Decimal(value)
        if not isinstance(value, Decimal) or not value.is_finite():
            return False
        with localcontext() as ctx:
            ctx.prec = 400
            if kind == "scale":
                return value != value.quantize(Decimal(1).scaleb(-arg))
            if kind == "digits":
                return abs(value.quantize(Decimal(1).scaleb(-arg[1]))) \
                    >= Decimal(10) ** (arg[0] - arg[1])
            if kind == "sigdigits":
                return len(value.normalize().as_tuple().digits) > arg \
                    and value != 0
            return value != 0 and (abs(value) >= Decimal(arg[1])
                                   or abs(value) < Decimal(arg[0]))
    if kind == "int-range":
        try:
            n = int(value)
        except (TypeError, ValueError):
            return False
        return n < arg[0] or n > arg[1]
    if kind in ("chars", "bytes", "utf16", "nul", "supplementary",
                "charset"):
        if not isinstance(value, str):
            return False
        if kind == "charset":
            try:
                value.encode(CODECS[arg])
            except UnicodeEncodeError:
                return True
            return False
        if kind == "chars":
            return len(value) > arg
        if kind == "bytes":
            return len(value.encode("utf-8", "surrogatepass")) > arg
        if kind == "utf16":
            return len(value.encode("utf-16-le", "surrogatepass")) // 2 > arg
        if kind == "nul":
            return "\x00" in value
        return any(ord(ch) > 0xFFFF for ch in value)
    if kind == "json-number":
        try:
            return any(json_candidate(n) for n in _json_numbers(value))
        except (ValueError, TypeError):
            return False
    if kind == "inet-mask":
        import ipaddress
        try:
            iface = ipaddress.ip_interface(str(value).strip())
        except ValueError:
            return False
        return iface.network.prefixlen < iface.max_prefixlen
    if kind == "json-form":
        if isinstance(value, (str, bytes, bytearray, memoryview)):
            return False
        try:
            json_write(value)
        except ValueError:
            return True
        return False
    return False


def _interval_like(value):
    """An interval as a reader hands it over: one, a Cassandra `Duration`,
    or the ISO text PostgreSQL's and DuckDB's reads give."""
    from . import intervals
    return (isinstance(value, intervals.Interval)
            or hasattr(value, "nanoseconds")
            or isinstance(value, str) and value[:1] == "P")


def _json_value(value):
    """A JSON column's value as the Python it is: MySQL's comes as text."""
    import json
    from decimal import Decimal
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value, parse_float=Decimal)
        except ValueError:
            return value
    return value


def _unfit_typed(kind, value, arg):
    """`unfit_value` for the kinds of a MongoDB value, a vector, a shape
    and a collection. A value that is not one of its class at all is
    one that would not arrive as it is."""
    from . import bsontype, geo, vectors
    if kind == "bson-type":
        return not bsontype.matches(arg, value)
    if kind == "bson-order":
        def fields(x):
            if isinstance(x, dict):
                return len(x) > 1 or any(fields(v) for v in x.values())
            if isinstance(x, (list, tuple)):
                return any(fields(v) for v in x)
            return False
        return fields(value)
    if kind == "bson-form":
        try:
            bsontype.as_bson(arg, value)
        except (ValueError, TypeError, ArithmeticError):
            return True
        return False
    if kind == "bson-none":
        return True
    if kind.startswith("vector-"):
        try:
            got = vectors.of(value)
        except (ValueError, TypeError):
            return True
        if kind == "vector-width":
            return vectors.width(got) > arg
        if kind == "vector-dims":
            return len(got) != arg
        return not vectors.finite(got)
    if kind in ("geometry-geojson", "geometry-form"):
        import struct
        try:
            got = geo.of(value)
            if kind == "geometry-geojson":
                geo.to_geojson(got)
        except (ValueError, TypeError, KeyError, IndexError, AttributeError,
                struct.error):
            return True
        return False
    got = _json_value(value)
    if kind == "empty-collection":
        return isinstance(got, (list, tuple, dict, set)) and not got
    if kind == "null-element":
        return isinstance(got, (list, tuple, set)) and any(
            x is None for x in got)
    if kind == "set-order":
        if not isinstance(got, (list, tuple)):
            return False
        try:
            return list(got) != sorted(set(got))
        except TypeError:
            # elements of kinds that do not order among themselves
            return True
    return False


def json_candidate(n):
    """Whether a JSON number can come back from MySQL's JSON as another
    number: a whole one outside what it keeps as a 64-bit integer, or a
    decimal past the 15 significant digits every double holds. Measured
    on 8.4, `9088544342.689999` - a double's own shortest text - came back
    `9088544342.69`, and `123456789.123456789` `123456789.1234568`; which
    of these longer ones change is the target's to say
    (`json_rewritten`), so this only picks what to ask it."""
    from decimal import Decimal
    if isinstance(n, bool):
        return False
    if isinstance(n, int):
        return not -2 ** 63 <= n <= 2 ** 64 - 1
    if isinstance(n, float):
        n = Decimal(repr(n))
    if not isinstance(n, Decimal) or not n.is_finite():
        return True
    if n.as_tuple().exponent >= 0:
        return not -2 ** 63 <= int(n) <= 2 ** 64 - 1
    return len(n.normalize().as_tuple().digits) > 15


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


#: How a pair of columns of each class is compared, in the words a verdict
#: says after "as": what the rendering keeps of a value and what it lets go
#: (problems D15)
AS = {
    "integer": "integers",
    "decimal": "decimals, digit for digit",
    NUMBER: "numbers by value (1.5 and 1.50 are one)",
    "float": "floating-point numbers to twenty places after the point",
    "boolean": "true or false",
    "text": "text",
    "bytes": "bytes",
    "date": "dates",
    "timestamp": "timestamps, instants in UTC, to the nanosecond held",
    "time": "times of day",
    "json": "JSON documents with their keys in order",
    JSON_ONE: "the engine's own normal form of JSON (spacing and key order"
              " not compared)",
    OWN: "the engine's own text of the value",
    "xml": "XML documents in canonical form",
    "uuid": "UUIDs",
    "inet": "network addresses",
    INTERVAL: "durations (months, days and nanoseconds apart)",
    VECTOR: "vectors, float for float",
    GEOMETRY: "shapes (SRID and binary, longitude first)",
    BSON: "values with their BSON type",
}

#: Classes whose rendering is the whole of what a column of one type on
#: one engine holds: two such columns compared in it are compared exactly.
#: The others let something go - a float past twenty places, JSON's spacing,
#: a decimal's trailing zeros - even where the type is the same.
WHOLE = frozenset({"integer", "decimal", "boolean", "text", "bytes", "date",
                   "timestamp", "time", OWN, "uuid", INTERVAL, VECTOR,
                   GEOMETRY, BSON})


def as_words(cls):
    """What a column of `cls` is compared as, in words."""
    if cls in AS:
        return AS[cls]
    if str(cls).startswith(bsontype_prefixes()):
        return AS[BSON]
    return str(cls)


def how_compared(src_engine, src_declared, src_cls, dst_engine,
                 dst_declared, dst_cls):
    """("exactly" | "by value", why) for one compared pair of columns.

    Exactly: one engine, one declared type, and a class whose rendering is
    the whole of the value (`WHOLE`). By value: anything else, compared as
    the class both sides were brought to - two types, two engines, or a
    rendering that lets part of the value go - and the words say as what."""
    same = (src_engine == dst_engine
            and str(src_declared).strip().lower()
            == str(dst_declared).strip().lower())
    if same and src_cls == dst_cls and src_cls in WHOLE:
        return "exactly", "one engine, one type"
    words = as_words(src_cls) if src_cls == dst_cls else (
        f"{as_words(src_cls)} against {as_words(dst_cls)}")
    if not same:
        return "by value", (f"{src_declared} and {dst_declared} compared as"
                            f" {words}")
    return "by value", f"compared as {words}"


# Engines that produce the canonical text in this process rather than in a
# SQL expression. Not a lesser arrangement - SQLite is in both lists, because
# it runs here anyway - but for MongoDB it means the documents cross the
# network to be folded, which `check` reports rather than leaves implied.
IN_PROCESS = {"mongodb", "sqlite", "mssql", "parquet", "clickhouse",
              "dynamodb", "oracle", "db2", "opensearch", "cassandra",
              "redshift", "snowflake", "bigquery", "ase", "duckdb",
              "copybook", "db2i", "db2zos", "iceberg", "delta",
              "cockroachdb", "dsql", "couchbase", "cosmosdb", "teradata",
              "sapodp", "saas"}


def renders(engine):
    """Whether migkit can produce canonical text for this engine at all."""
    return engine in BUILDERS or engine in IN_PROCESS


# The digest two different engines can both compute, over the canonical row
# text above.
#
# migkit's own per-engine checksums cannot be used for this: PostgreSQL folds
# rows with `sum(...::bit(64)::bigint)` and MySQL sums a 32-bit prefix, which
# are different functions of the same data and never meet. What both can
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
#: `digest_step`'s DIGEST_HEX hex digits as whole bytes and a shift
_DIGEST_BYTES = (DIGEST_HEX + 1) // 2
_DIGEST_SHIFT = 8 * _DIGEST_BYTES - 4 * DIGEST_HEX
DIGEST_BITS = DIGEST_HEX * 4


def digest_expr(engine, row_expr):
    """Aggregate yielding one number for a whole table, comparable across
    engines. Order-independent, so neither side has to sort."""
    engine = dialect(engine)
    if engine == "mysql":
        return (f"coalesce(sum(cast(conv(substr(md5({row_expr}),1,"
                f"{DIGEST_HEX}),16,10) as decimal(65,0))), 0)")
    if engine == "postgres":
        # the row's UTF-8 bytes, as MySQL and this process hash: `md5(text)`
        # hashes the database's own encoding, and in a LATIN1 database `é`
        # is one byte there and two everywhere else
        return (f"coalesce(sum(('x'||substr(md5(convert_to({row_expr},"
                f" 'UTF8')),1,{DIGEST_HEX}))"
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
    engine = dialect(engine)
    if engine == "mysql":
        return rowtext.mysql_row_from(parts)
    if engine == "postgres":
        return rowtext.postgres_row_from(parts)
    if engine == "sqlite":
        return rowtext.sqlite_row_from(parts)
    raise ValueError(f"no canonical row text for engine {engine!r}")


# ---- engines on another's wire (`dialect`) ---------------------------------

#: CockroachDB's own names for its types, as its catalogue's
#: `crdb_sql_type` gives them, beside PostgreSQL's, which `format_type`
#: gives. Its `INT` is 64 bits, and `format_type` says `bigint`
TYPES["cockroachdb"] = {
    **TYPES["postgres"],
    "string": "text", "int": "integer", "int8": "integer",
    "int4": "integer", "int2": "integer", "int64": "integer",
    "float": "float", "decimal": "decimal", "bytes": "bytes",
    "timestamptz": "timestamp", "jsonb": "json", "json": "json",
}


def _speak_as():
    """Each engine-keyed table holds a wire engine's dialect's entry under
    the engine's own name too, where the engine has none of its own - so
    TiDB builds, writes, sizes and refuses a value as MySQL does, and
    CockroachDB and DSQL as PostgreSQL does. `BUILDERS` and `IN_PROCESS`
    are not among them: where a rendering runs is each engine's own."""
    from .engines import WIRE
    tables = (TYPES, DDL, FLOAT4, FLOAT4_DDL, INT4_DDL, ARRAY_DDL,
              ARRAY_HOLDS, JSON_UNKEYED, KEY_TEXT, TIME_MEANING, INT_RANGES,
              CHAR_TYPES, UNLIMITED_CHARS, BYTE_TYPES, NUMERIC_TYPES,
              MOST_DIGITS, DECIMAL_MOST, SIGNIFICANT, ADDRESS_ONLY,
              VECTOR_WIDTH, UNFIT_SQL)
    sets = (KEEPS_SCALE, NO_NONFINITE_FLOAT, NONFINITE_DECIMAL,
            NO_NONFINITE_VECTOR, NO_SHAPE_WRITER)
    for engine, speaks in WIRE.items():
        for table in tables:
            if speaks in table and engine not in table:
                table[engine] = table[speaks]
        for group in sets:
            if speaks in group:
                group.add(engine)
        for owner, typ in list(JSON_SORTS_KEYS):
            if owner == speaks:
                JSON_SORTS_KEYS.add((engine, typ))


_speak_as()
