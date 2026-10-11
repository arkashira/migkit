"""The types only the commercial engines have (backlog W6; the type-fidelity
research, docs/research/type-fidelity-2026-09-28.md, sections 6, 8 and 11):
what each one is on its way to another engine, decided before a row moves.

Each is one of four things:

* **carried exactly** - its value in a class every engine renders alike
  (`canon`): a `hierarchyid` as its canonical path (`/1/3.5/`), an
  XMLType and a Db2 XML as the document, an SDO_GEOMETRY as its SRID and
  well-known binary (`geo`), a DECFLOAT and an Oracle FLOAT as the
  decimals they are - never a double, which held fifteen of FLOAT(126)'s
  thirty-eight digits - money at its four places and inside its range, a
  datetimeoffset or a TIMESTAMP WITH TIME ZONE as its instant *and* its
  offset where both sides keep one (`OFFSET`);
* **carried as**, said and accepted - a value the target has no type for,
  kept as what says everything it held: a `sql_variant` as its base type
  and its value (`Variant`), an Oracle object or collection as the JSON
  of its attributes or elements, a BFILE as the bytes of its file, an
  instant whose offset the target drops, sixteen bytes read as a UUID.
  Each is counted on the source and refused before the move unless the
  hop's `accept_changes` names the column (`canon.unfit`);
* **said, not data** - a row version or a row's address (`NOT_DATA`): the
  target writes its own, so it is neither carried nor compared, and the
  report says so beside the columns it did compare;
* **refused by name** - a type migkit has no exact form of on another
  engine (a CLR type, an ANYDATA, a Teradata UDT): counted and named
  before the move, and carried as its driver reads it only where the hop
  accepts that; between two servers of its own engine it is carried as
  the engine reads it.

Each class's comparison and the DDL each target builds are `canon`'s; what
is here is what only these engines' types need: how SQL Server writes a
`sql_variant` and a shape back, which zone a time was written in, and
which of their types hold what.
"""
import datetime
import re
import uuid
from decimal import Decimal

#: the engines whose own types are spoken for here
ENGINES = frozenset({"mssql", "oracle", "db2", "db2i", "db2zos", "teradata",
                     "ase"})


def base(declared):
    """A declared type's name in small letters, its numbers left out:
    `datetimeoffset(7)` is `datetimeoffset`, `xml(dbo.orders)` is `xml`,
    `TIMESTAMP(6) WITH TIME ZONE` is `timestamp with time zone`."""
    return " ".join(re.sub(r"\([^)]*\)", " ", str(declared or ""))
                    .lower().split())


# ---- said, not data -------------------------------------------------------

_VERSION = ("a row version, which the target's server writes anew into"
            " every row it is given - not data, so neither carried nor"
            " compared")
_ADDRESS = ("a row's address on the source's disk, which points at nothing"
            " on the target - not data, so neither carried nor compared")
_ROW_ID = ("a row identifier Db2 makes for each row, which the target makes"
           " its own of - not data, so neither carried nor compared")

#: {engine: {type: what it is}}: SQL Server's and ASE's `timestamp` (SQL
#: Server's `rowversion`, which its catalogue names `timestamp`), Oracle's
#: ROWID and UROWID, Db2's ROWID
NOT_DATA = {
    "mssql": {"timestamp": _VERSION, "rowversion": _VERSION},
    "ase": {"timestamp": _VERSION},
    "oracle": {"rowid": _ADDRESS, "urowid": _ADDRESS},
    **{name: {"rowid": _ROW_ID} for name in ("db2", "db2i", "db2zos")},
}


def not_data(engine, declared):
    """What a column of this type is, where it is not data (`NOT_DATA`);
    None for any other."""
    return NOT_DATA.get(engine, {}).get(base(declared))


# ---- refused by name, and carried as ----------------------------------------

def _title(engine):
    from .engines import TITLES
    return TITLES.get(engine, engine)


def refused(engine, declared, dst):
    """Words for a source column of one of `ENGINES` whose type migkit has
    no class for, on its way into another engine - a CLR type, an
    ANYDATA, a Teradata UDT: counted and refused before the move
    (`canon.unfit`, `refused-type`). None for any other."""
    from . import canon
    if engine not in ENGINES or not declared \
            or canon.dialect(dst) == engine or not_data(engine, declared) \
            or canon.type_class(engine, declared) is not None:
        return None
    return f"{_title(engine)} {declared}"


def oracle_object(declared):
    """Whether an Oracle column's declared type is an object type or a
    collection of a schema's own (`"OWNER"."TYPE"`, as the Oracle engine
    names one)."""
    return str(declared or "").strip().startswith('"')


def as_json(engine, declared, dst):
    """What a source column is, in words, where the target has no type for
    it and keeps it as the JSON of what it holds - a `sql_variant` as its
    base type and value (`variant_json`) beside any other engine, an
    Oracle object or collection as its attributes or elements
    (`oracle_value`) beside any other; None for any other."""
    from . import canon
    if canon.dialect(dst) == engine:
        return None
    if engine == "mssql" and base(declared) == "sql_variant":
        return "a SQL Server sql_variant"
    if engine == "oracle" and oracle_object(declared):
        return "an Oracle object or collection"
    return None


def own_type(engine, declared):
    """The type a table built on the source's own engine gives a column:
    the source's, but where that holds no data of its own - a BFILE, which
    points at a file on the source server's disk - the type that holds
    what it is carried as."""
    if engine == "oracle" and base(declared) == "bfile":
        return "BLOB"
    return declared


def xml_collection(engine, declared):
    """The schema collection a SQL Server `xml` column is typed by
    (`xml(dbo.orders)`, as the engine declares one), or None."""
    if engine != "mssql" or base(declared) != "xml":
        return None
    got = re.search(r"\(([^)]*)\)", str(declared))
    return got.group(1).strip() if got else None


def built_without(engine, declared, facts, dst):
    """What a column of the source is that a table built for it on `dst`
    does not keep, in words - a value it holds is carried as it is - or
    None: a SQL Server `xml` column's schema collection on any other
    engine, a FILESTREAM column's store anywhere (the table holds the
    bytes)."""
    from . import canon
    coll = xml_collection(engine, declared)
    if coll and canon.dialect(dst) != engine:
        return (f"its documents are checked against the schema collection"
                f" {coll} on the source; here they are held unchecked")
    if engine == "mssql" and (facts or {}).get("filestream"):
        return ("its bytes are kept in the source's FILESTREAM store;"
                " here they are held in the table")
    return None


# ---- SQL Server's sql_variant ----------------------------------------------

class Variant(tuple):
    """(declared base type, value, the server's text of it): a
    `sql_variant`'s value with the type it was stored as. `int` 5,
    `bigint` 5 and `nvarchar(1)` '5' are three values: read through the
    driver, or as the base value alone, all three were 5 to every
    comparison."""
    __slots__ = ()

    def __new__(cls, declared, value, text):
        return super().__new__(cls, (str(declared), value, str(text)))

    declared = property(lambda self: self[0])
    value = property(lambda self: self[1])
    text = property(lambda self: self[2])

    def __str__(self):
        return variant_text(self)


#: a `sql_variant` read as one text SQL Server writes whole: its base type,
#: precision, scale and length, then its value in a form the server reads
#: back exactly - a binary as hexadecimal, a float at its seventeen digits
#: (style 3), a time at every digit (style 121), money at its four places
#: (style 2). `{c}` is the column
VARIANT_SQL = (
    "case when {c} is null then null else"
    " cast(sql_variant_property({c}, 'BaseType') as nvarchar(128)) + N'|'"
    " + isnull(cast(sql_variant_property({c}, 'Precision') as"
    " nvarchar(12)), N'') + N'|'"
    " + isnull(cast(sql_variant_property({c}, 'Scale') as nvarchar(12)),"
    " N'') + N'|'"
    " + isnull(cast(sql_variant_property({c}, 'MaxLength') as"
    " nvarchar(12)), N'') + N'|'"
    " + case cast(sql_variant_property({c}, 'BaseType') as nvarchar(128))"
    " when 'binary' then convert(nvarchar(max), cast({c} as varbinary(8000)),"
    " 2)"
    " when 'varbinary' then convert(nvarchar(max), cast({c} as"
    " varbinary(8000)), 2)"
    " when 'float' then convert(nvarchar(64), cast({c} as float), 3)"
    " when 'real' then convert(nvarchar(64), cast(cast({c} as real) as"
    " float), 3)"
    " when 'datetime' then convert(nvarchar(64), cast({c} as datetime), 121)"
    " when 'smalldatetime' then convert(nvarchar(64), cast({c} as"
    " smalldatetime), 120)"
    " when 'datetime2' then convert(nvarchar(64), cast({c} as"
    " datetime2(7)), 121)"
    " when 'datetimeoffset' then convert(nvarchar(64), cast({c} as"
    " datetimeoffset(7)), 121)"
    " when 'date' then convert(nvarchar(64), cast({c} as date), 23)"
    " when 'time' then convert(nvarchar(64), cast({c} as time(7)))"
    " when 'money' then convert(nvarchar(64), cast({c} as money), 2)"
    " when 'smallmoney' then convert(nvarchar(64), cast({c} as money), 2)"
    " else convert(nvarchar(max), {c}) end end")

_INTEGERS = ("tinyint", "smallint", "int", "bigint")
#: how SQL Server reads each base type back out of the text above
_STYLE = {"datetime": 121, "smalldatetime": 120, "datetime2": 121,
          "datetimeoffset": 121, "date": 23}


def _variant_declared(name, precision, scale, length):
    """A base type with the numbers its values are held at."""
    if name in ("decimal", "numeric"):
        return f"{name}({precision},{scale})"
    if name in ("char", "varchar", "binary", "varbinary"):
        return f"{name}({length})"
    if name in ("nchar", "nvarchar"):
        return f"{name}({int(length) // 2})"
    if name in ("datetime2", "time", "datetimeoffset"):
        return f"{name}({scale})"
    return name


def _typed(declared, text):
    """A base type's value out of the server's text of it."""
    from . import nanotime
    name = base(declared)
    if name in _INTEGERS:
        return int(text)
    if name == "bit":
        return text.strip() in ("1", "True", "true")
    if name in ("decimal", "numeric", "money", "smallmoney"):
        return Decimal(text.strip())
    if name in ("float", "real"):
        return float(text)
    if name == "date":
        return datetime.date.fromisoformat(text.strip())
    if name in ("datetime", "smalldatetime", "datetime2", "time"):
        return nanotime.parse(text)
    if name == "datetimeoffset":
        return offset_time(text)
    if name == "uniqueidentifier":
        return uuid.UUID(text.strip())
    if name in ("binary", "varbinary"):
        return bytes.fromhex(text.strip())
    return text


def variant_of(text):
    """A `Variant` from what `VARIANT_SQL` reads."""
    if text is None:
        return None
    name, precision, scale, length, rest = str(text).split("|", 4)
    declared = _variant_declared(name.strip().lower(), precision.strip(),
                                 scale.strip(), length.strip())
    return Variant(declared, _typed(declared, rest), rest)


def variant(value):
    """A `Variant` from what a reader hands over: one already, or its JSON
    (`variant_json`) as another engine keeps it - a document, or its text.
    Raises for anything else."""
    import json
    if isinstance(value, Variant):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    if isinstance(value, dict) and set(value) == {"type", "value"} \
            and isinstance(value["value"], str):
        declared = str(value["type"]).strip().lower()
        return Variant(declared, _typed(declared, value["value"]),
                       value["value"])
    raise ValueError(f"not a sql_variant: {value!r}")


def variant_text(value):
    """The canonical text: the base type, then the value as every engine
    renders a value of that type - a datetimeoffset with its offset."""
    from . import canon
    v = variant(value)
    if base(v.declared) == "datetimeoffset":
        shown = canon.render_value(canon.OFFSET, v.value)
    else:
        cls = canon.type_class("mssql", v.declared)
        shown = (canon.render_value(cls, v.value) if cls is not None
                 else v.text)
    return f"{v.declared}:{shown}"


def variant_json(value):
    """A `Variant` as the JSON another engine keeps it in: its base type
    and the server's text of its value, which SQL Server reads back as the
    same value of the same type (`variant_mark`)."""
    v = variant(value)
    return {"type": v.declared, "value": v.text}


def variant_write(engine, value):
    """A `Variant` on its way into `engine`'s driver: into SQL Server the
    base value alone, which a key compares by (a row is written through
    `variant_mark`); into MongoDB its document; into any other engine its
    JSON text."""
    from . import canon
    v = variant(value)
    if engine == "mssql":
        return v.value
    if engine == "mongodb":
        return variant_json(v)
    return canon.json_write(variant_json(v))


def variant_mark(value):
    """(the expression a `sql_variant` is written by, `{}` where its text
    goes; the text): converted from the server's own text into its base
    type, so the column holds the type the source held - written as a
    parameter, an `int` came back an `nvarchar`, or a `bigint`. None for
    a value that is no `sql_variant` (a plain value of another engine),
    which goes in as it is."""
    try:
        v = variant(value)
    except (ValueError, TypeError):
        return None
    name = base(v.declared)
    if name == "real":
        return "convert(real, convert(float, {}))", v.text
    if name in ("binary", "varbinary"):
        return (f"convert({v.declared}, convert(varchar(max), {{}}), 2)",
                v.text)
    style = _STYLE.get(name)
    if style is None:
        return f"convert({v.declared}, {{}})", v.text
    return f"convert({v.declared}, {{}}, {style})", v.text


# ---- an instant and the zone it was written in ------------------------------

#: types that keep each value's own offset or zone beside its instant
OFFSET_TYPES = {"mssql": ("datetimeoffset",),
                "oracle": ("timestamp with time zone",)}

#: the zones that are UTC by another name, as Oracle's region names have
#: them
UTC_NAMES = frozenset({"UTC", "GMT", "Z", "UCT", "ZULU", "UNIVERSAL",
                       "GREENWICH", "GMT0", "GMT+0", "GMT-0", "ETC/UTC",
                       "ETC/GMT", "ETC/UCT", "ETC/ZULU", "ETC/UNIVERSAL",
                       "ETC/GREENWICH", "ETC/GMT0", "ETC/GMT+0",
                       "ETC/GMT-0"})

_ZONE = re.compile(r"^([+-])(\d{1,2}):?(\d{2})$")


def keeps_offset(engine, declared):
    """Whether a column of this type keeps each value's own offset."""
    return base(declared) in OFFSET_TYPES.get(engine, ())


def _offset(delta):
    minutes = int(delta.total_seconds() // 60)
    sign = "-" if minutes < 0 else "+"
    hours, left = divmod(abs(minutes), 60)
    return f"{sign}{hours:02d}:{left:02d}"


def zone_text(zone):
    """A zone as it is compared: an offset `+07:00`, UTC by any of its
    names `+00:00`, a region by its name in capitals (Oracle's TZR)."""
    z = str(zone).strip()
    m = _ZONE.match(z)
    if m:
        sign, hours, minutes = m.groups()
        if int(hours) == 0 and int(minutes) == 0:
            return "+00:00"
        return f"{sign}{int(hours):02d}:{minutes}"
    return "+00:00" if z.upper() in UTC_NAMES else z.upper()


def zone_of(value):
    """The zone a time was written in: Oracle's own (`Zoned.zone`, an
    offset or a region), or a datetime's offset - None for a time that
    has none."""
    zone = getattr(value, "zone", None)
    if zone:
        return zone_text(zone)
    if getattr(value, "tzinfo", None) is None \
            or not isinstance(value, datetime.datetime):
        return None
    off = value.utcoffset()
    return None if off is None else _offset(off)


def is_region(zone):
    """Whether a zone (`zone_text`) is a region rather than an offset."""
    return bool(zone) and not _ZONE.match(zone)


def offset_render(value):
    """The canonical text of a time that keeps its zone: the instant as
    every engine renders one, in UTC, then the zone it was written in."""
    from . import canon
    return f"{canon.render_value('timestamp', value)} " \
        f"{zone_of(value) or '+00:00'}"


_TEXT = re.compile(r"\s*(.*?\d)\s*([+-]\d\d:\d\d)\s*$")


def offset_time(text):
    """`2024-01-01 07:00:00.1234567 +07:00` - SQL Server's own text of a
    `datetimeoffset` (style 121) - as the time at its offset, every digit
    kept (`nanotime`): its driver reads six of the seven."""
    from . import nanotime
    if text is None:
        return None
    m = _TEXT.match(str(text))
    if not m:
        return nanotime.parse(text)
    return nanotime.parse(m.group(1) + m.group(2))


def offset_at(value):
    """The offset a time was written at, as a `timedelta`: its own, or
    the one its region had at that instant. Raises for a region this
    machine's zone database does not know."""
    zone = zone_of(value)
    if zone is None:
        return datetime.timedelta(0)
    m = _ZONE.match(zone)
    if m:
        sign, hours, minutes = m.groups()
        delta = datetime.timedelta(hours=int(hours), minutes=int(minutes))
        return -delta if sign == "-" else delta
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    raw = str(getattr(value, "zone", zone)).strip()
    for name in (raw, raw.title(), "/".join(
            p.upper() if len(p) <= 3 else p.title() for p in raw.split("/"))):
        try:
            got = ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError):
            continue
        return value.astimezone(got).utcoffset()
    raise ValueError(f"the zone {raw} is not one this machine knows")


def mssql_offset_text(value):
    """A time as SQL Server's `datetimeoffset` takes it: its clock at its
    own offset, every digit to the seventh, then the offset - which its
    drivers drop, writing the instant at UTC. A time with no zone is UTC;
    one written in a region at the offset the region had then. Text, and
    anything else, as it is."""
    if not isinstance(value, datetime.datetime):
        return value
    ns = value.microsecond * 1000 + (getattr(value, "nanosecond", 0) or 0)
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    off = offset_at(value)
    t = value.astimezone(datetime.timezone(off))
    seven = f"{ns:09d}"[:7]
    return (f"{t.year:04d}-{t.month:02d}-{t.day:02d} {t.hour:02d}:"
            f"{t.minute:02d}:{t.second:02d}.{seven} {_offset(off)}")


# ---- decimals of no fixed scale, and money ----------------------------------

#: decimal types that keep each value's own digits rather than a declared
#: scale, by (engine, type): (significant digits, (smallest magnitude,
#: largest) as text). Oracle's NUMBER with no precision and Teradata's
#: hold 38 digits from 1E-130 to below 1E126; Db2's DECFLOAT(16) 16 from
#: 1E-398 to below 1E385, DECFLOAT(34) 34 from 1E-6176 to below 1E6145
FLOATING = {
    ("oracle", "number"): (38, ("1e-130", "1e126")),
    ("teradata", "number"): (38, ("1e-130", "1e126")),
}
DECFLOAT = {16: (16, ("1e-398", "1e385")), 34: (34, ("1e-6176", "1e6145"))}


def _numbers(declared):
    got = re.search(r"\(([^)]*)\)", str(declared or ""))
    if not got:
        return []
    return [int(p) for p in got.group(1).split(",")
            if p.strip().isdigit()]


def floating(engine, declared):
    """(significant digits, (smallest, largest)) a decimal column of no
    fixed scale holds, or None for one of a fixed scale. Oracle's FLOAT(p)
    is a NUMBER of p binary digits, about p x 0.30103 decimal ones - a
    FLOAT(126) 38 - not a double."""
    name, nums = base(declared), _numbers(declared)
    if engine in ("db2", "db2i", "db2zos") and name == "decfloat":
        return DECFLOAT[16 if nums == [16] else 34]
    if engine == "oracle" and name == "float":
        digits = max(1, round((nums[0] if nums else 126) * 0.30103))
        return (digits, FLOATING[("oracle", "number")][1])
    if (engine, name) in FLOATING and not nums:
        return FLOATING[(engine, name)]
    return None


def holds_nonfinite(engine, declared):
    """Whether a decimal column of this type holds NaN and the
    infinities: Db2's DECFLOAT does."""
    return engine in ("db2", "db2i", "db2zos") and \
        base(declared) == "decfloat"


#: SQL Server's and ASE's money types: four places, and the range of a
#: 64-bit count of ten-thousandths (32-bit for smallmoney)
MONEY = {"money": (Decimal("-922337203685477.5808"),
                   Decimal("922337203685477.5807")),
         "smallmoney": (Decimal("-214748.3648"), Decimal("214748.3647"))}


def money_range(engine, declared):
    """(least, most) a money column holds, or None for any other."""
    if engine not in ("mssql", "ase"):
        return None
    return MONEY.get(base(declared))


# ---- shapes -----------------------------------------------------------------

#: the shape types that read a polygon by the order of its rings' points
#: (`geo.misoriented`), and what they make of one drawn the other way
RING_RULES = {
    ("mssql", "geography"): "SQL Server's geography takes the inside to lie"
                            " on a ring's left, so it would hold the rest"
                            " of the globe",
    ("oracle", "sdo_geometry"): "Oracle holds such a polygon as not valid",
}


def ring_rule(engine, declared):
    return RING_RULES.get((engine, base(declared)))


def mssql_shape_mark(kind, value):
    """(the expression a shape is written into a SQL Server `geography`
    or `geometry` column by, `{}` where its value goes; the value): its
    well-known binary and SRID, through the type's own constructor - the
    row copy's binary otherwise met SQL Server's own serialisation, which
    nothing else writes. A shape with Z or M as well-known text, which
    SQL Server reads Z and M from (`geo.wkt`). The SRID at its EPSG number
    (`geo.as_epsg`); None for a value that is no shape."""
    from . import geo
    try:
        s = geo.as_epsg(value)
    except (ValueError, TypeError, KeyError, IndexError):
        return None
    if s[2] or s[3]:
        return f"{kind}::STGeomFromText({{}}, {int(s.srid)})", geo.wkt(s)
    return f"{kind}::STGeomFromWKB({{}}, {int(s.srid)})", geo.wkb(s)


def mssql_mark(declared):
    """How a value is written into a SQL Server column of this declared
    type where a parameter alone would not hold it - f(value) giving (the
    expression, `{}` where the value goes; the value bound), or None from
    f where the value goes in as it is - or None for a column of any other
    type."""
    name = base(declared)
    if name == "sql_variant":
        return variant_mark
    if name in ("geography", "geometry"):
        return lambda v: mssql_shape_mark(name, v)
    if name == "datetimeoffset":
        def offset(v):
            if not isinstance(v, datetime.datetime):
                return None
            return "convert(datetimeoffset(7), {}, 121)", \
                mssql_offset_text(v)
        return offset
    return None


# ---- Oracle's objects and collections ----------------------------------------

def oracle_value(value):
    """An Oracle object or collection, as its driver hands it over, as the
    JSON of it: an object by its attributes' names, a collection (a
    VARRAY, a nested table) as the list of its elements - at every depth,
    with a time as its ISO text and bytes as their hexadecimal, which JSON
    has no form of its own for. Any other value as it is."""
    kind = getattr(value, "type", None)
    if kind is not None and hasattr(kind, "attributes"):
        if getattr(kind, "iscollection", False):
            return [oracle_value(v) for v in value.aslist()]
        return {a.name: oracle_value(getattr(value, a.name))
                for a in kind.attributes}
    if isinstance(value, (datetime.datetime, datetime.date,
                          datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex().upper()
    return value


# ---- UUIDs kept as sixteen bytes ----------------------------------------------

def sixteen_bytes(engine, declared):
    """Whether a column holds bytes sixteen at a time: SQL Server's,
    MySQL's and ASE's `binary(16)`, Oracle's RAW(16), Db2's sixteen bytes,
    Teradata's BYTE(16) - where a UUID is kept as bytes."""
    name, nums = base(declared), _numbers(declared)
    return nums == [16] and name in ("binary", "raw", "byte", "varbinary",
                                     "char for bit data")


# ---- XML -----------------------------------------------------------------------

#: engines whose XML type holds one document and nothing else
DOCUMENT_ONLY = frozenset({"oracle", "db2", "db2i", "db2zos"})


def fragment(value):
    """Whether an XML value is content rather than one document - several
    elements, or text beside one - or no XML at all: what an XML type of
    one document refuses."""
    import xml.etree.ElementTree as ET
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    try:
        ET.fromstring(str(value))
    except ET.ParseError:
        return True
    return False
