"""A MongoDB value compared as its BSON type and its value, both.

BSON keeps apart what every SQL engine collapses: a 32-bit and a 64-bit
integer (`5` and `NumberLong(5)`), an ObjectId and the 24 hexadecimal
digits of it, a UUID (binary subtype 4) and the same sixteen bytes of any
other subtype, a date and a replication `Timestamp`, and types SQL has
nothing for at all - a regular expression, MinKey and MaxKey. Compared by
the rendering each value's class gives, those pairs were equal - an
ObjectId rendered as its hex beside a text column holding the hex, 5 as
5 beside `NumberLong(5)` - so a document that went to PostgreSQL as text
and `bigint` came back with its `_id` a string and its `int`s `long`s,
and every check on the way said nothing (type-fidelity G5).

So wherever MongoDB is one side of a pair and the other side is not,
each value is rendered with its type in front - `long` and 5, `objectId`
and 65a1... - on both sides: MongoDB's value by the type it has (`tag`),
the other side's by the type MongoDB holds that column's values as
(`TAGS`, through `as_bson`, which is also what a move into MongoDB
writes). A type that changes on the way is then a difference, not a
match.

What the other side can hold exactly it holds as itself: a 32-bit
integer in a 32-bit column, a 64-bit one in a `bigint`, a double, a
Decimal128 in a `numeric`, a UUID in a `uuid`. What it has no type for is
kept in a JSON column as its canonical Extended JSON (MongoDB's own
specification of every BSON type as JSON), which `as_bson` reads back
into the same value: `{"$oid": "65a1..."}`, `{"$numberLong": "5"}`.
"""
import datetime
import json

#: the class of a MongoDB field compared with a column of another engine,
#: the column's own tag after the slash: `bson/long`
MONGO = "bson/"
#: the class of another engine's column compared with a MongoDB field:
#: `bson:long`
OTHER = "bson:"

#: between a value's type and its value in its text: a control character
#: no rendering writes, so a report can show the value alone (`shown`)
#: and a text key holding a colon is not taken for a type
SEP = "\x1e"


def shown(text):
    """A typed text as a person reads it: the value, without its type."""
    return text.split(SEP, 1)[-1] if isinstance(text, str) else text


def tag(value):
    """The BSON type of a value as pymongo hands it over, by MongoDB's own
    alias for it (`$type`), a binary by its subtype: `uuid` for subtype
    4, `binData` for 0, `binData3` and so on for the rest."""
    from bson import Code, Int64, MaxKey, MinKey, ObjectId, Regex, \
        Timestamp
    from bson.binary import Binary
    from bson.decimal128 import Decimal128
    import re
    import uuid
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, Int64):
        return "long"
    if isinstance(value, int):
        return "int" if -2 ** 31 <= value < 2 ** 31 else "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, Decimal128):
        return "decimal"
    if isinstance(value, str):
        return "string"
    if isinstance(value, datetime.datetime) or \
            type(value).__name__ == "DatetimeMS":
        return "date"
    if isinstance(value, Binary):
        return {0: "binData", 4: "uuid"}.get(value.subtype,
                                             f"binData{value.subtype}")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "binData"
    if isinstance(value, uuid.UUID):
        return "uuid"
    if isinstance(value, ObjectId):
        return "objectId"
    if isinstance(value, Timestamp):
        return "timestamp"
    if isinstance(value, (Regex, re.Pattern)):
        return "regex"
    if isinstance(value, MinKey):
        return "minKey"
    if isinstance(value, MaxKey):
        return "maxKey"
    if isinstance(value, Code):
        return "javascript"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, (list, tuple)):
        return "array"
    from decimal import Decimal
    if isinstance(value, Decimal):
        return "decimal"
    return type(value).__name__


def _options():
    from bson.json_util import CANONICAL_JSON_OPTIONS
    return CANONICAL_JSON_OPTIONS


def ejson(value):
    """A BSON value as its canonical Extended JSON text, a document's
    fields in the order it keeps them."""
    from bson import json_util
    return json_util.dumps(value, json_options=_options())


def from_ejson(text):
    """The BSON value an Extended JSON text (canonical or relaxed, or
    plain JSON) stands for: `{"$numberLong": "5"}` is a 64-bit 5, `5` a
    32-bit one, as pymongo writes a whole number that fits."""
    from bson import json_util
    if isinstance(text, (bytes, bytearray, memoryview)):
        text = bytes(text).decode("utf-8")
    return json_util.loads(text, json_options=_options())


def _payload(t, value, by_value):
    from .canon import NUMBER, render_value
    if t in ("int", "long"):
        return str(int(value))
    if t == "double":
        return render_value("float", value)
    if t == "decimal":
        return render_value(NUMBER if by_value else "decimal", value)
    if t == "string":
        return value
    if t == "bool":
        return "1" if value else "0"
    if t == "date":
        return render_value("timestamp", value)
    if t == "uuid":
        import uuid
        return str(value if isinstance(value, uuid.UUID)
                   else uuid.UUID(bytes=bytes(value)))
    if t.startswith("binData"):
        return bytes(value).hex().upper()
    if t == "objectId":
        return str(value)
    if t == "timestamp":
        return f"{value.time}.{value.inc}"
    if t == "regex":
        return ejson(value)
    if t in ("minKey", "maxKey", "null"):
        return ""
    return ejson(value)


def render(value, by_value=False):
    """The value's BSON type, `SEP`, and the value as its class renders
    it; None for NULL. `by_value` compares a Decimal128 by its value
    (`1.5` and `1.50` one), where the other side does not keep each
    value's own scale (`canon.decimal_class`)."""
    from .canon import ABSENT
    if value is None or value is ABSENT:
        return None
    t = tag(value)
    return f"{t}{SEP}{_payload(t, value, by_value)}"


#: what each class of another engine's column is held as in MongoDB, where
#: it is one thing: a whole number by its width (`width_tag`), a decimal
#: as a Decimal128, a time as a date, JSON as the document it is
TAGS = {"float": "double", "decimal": "decimal", "number": "decimal",
        "boolean": "bool", "text": "string", "xml": "string",
        "inet": "string", "uuid": "uuid", "bytes": "binData",
        "timestamp": "date", "date": "date", "json": "json",
        "json on one engine": "json", "bson": "json", "vector": "vector",
        "geometry": "geometry"}


def column_tag(engine, declared, cls):
    """What MongoDB holds a value of this column as: a BSON type's alias,
    or `json` (a document, read from its Extended JSON), `vector` (an
    array of doubles) or `geometry` (GeoJSON). None where MongoDB has no
    type for it - a time of day, an interval."""
    from . import canon
    if cls == "integer":
        name, _ = canon._split_declared(declared)
        if engine == "mysql" and name == "tinyint" and \
                canon.params(declared)[:1] == (1,):
            # `BOOLEAN` in MySQL is `tinyint(1)`: what a MongoDB boolean
            # moved there is built as, and read back as
            return "bool"
        rng = canon.INT_RANGES.get(engine, {}).get(name)
        return "int" if rng and rng[0] >= -2 ** 31 and rng[1] < 2 ** 31 \
            else "long"
    return TAGS.get(cls)


def as_bson(t, value):
    """A value of another engine's column as MongoDB holds it, by the
    column's tag: what a move into MongoDB writes, and what the value is
    rendered as beside MongoDB's own. Raises where the value has no form
    of that type - a 2 in a MySQL boolean, a whole number past 32 bits in
    a 32-bit column."""
    from decimal import Decimal
    if value is None:
        return None
    if t == "int":
        n = int(value)
        if not -2 ** 31 <= n < 2 ** 31:
            raise ValueError(f"{n} does not fit a 32-bit integer")
        return n
    if t == "long":
        from bson import Int64
        return Int64(int(value))
    if t == "double":
        return float(value)
    if t in ("decimal", "number"):
        from bson.decimal128 import Decimal128
        if hasattr(value, "to_decimal"):
            return value
        return Decimal128(value if isinstance(value, Decimal)
                          else Decimal(str(value)))
    if t == "bool":
        if isinstance(value, bool):
            return value
        if value in (0, 1):
            return bool(value)
        raise ValueError(f"{value!r} is no boolean")
    if t == "string":
        return value if isinstance(value, str) else str(value)
    if t == "date":
        if isinstance(value, datetime.datetime):
            if value.tzinfo is not None:
                value = value.astimezone(datetime.timezone.utc)
            return datetime.datetime(value.year, value.month, value.day,
                                     value.hour, value.minute, value.second,
                                     value.microsecond)
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        raise ValueError(f"{value!r} is no date")
    if t == "binData":
        return bytes(value)
    if t == "uuid":
        import uuid

        from bson.binary import Binary
        u = value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        return Binary.from_uuid(u)
    if t == "json":
        from .canon import _bson_value, json_write
        if isinstance(value, (dict, list, tuple)):
            if _bson_value(value):
                return value
            value = json_write(value)
        elif not isinstance(value, (str, bytes, bytearray, memoryview)):
            # a value no JSON column hands over as text is one already
            return value
        return from_ejson(value)
    if t == "vector":
        from . import vectors
        return vectors.of(value)
    if t == "geometry":
        from . import geo
        return geo.to_geojson(value)
    raise ValueError(f"MongoDB has no type for {value!r}")


def render_other(t, value, by_value=False):
    """Another engine's value, rendered as MongoDB would hold it."""
    if value is None:
        return None
    try:
        held = as_bson(t, value)
    except (ValueError, TypeError, ArithmeticError) as e:
        from .canon import uncomparable
        return uncomparable(f"no BSON form: {e}")
    return render(held, by_value)


def matches(t, value):
    """Whether a MongoDB value is of the type a column of tag `t` holds -
    anything is, in a column holding documents as their Extended JSON."""
    got = tag(value)
    if t == "json" or got == "null":
        return True
    if t in ("decimal", "number"):
        return got == "decimal"
    return got == t


def value_of(text):
    """The value back out of its typed text (`render`), for a key a
    repair addresses a document by."""
    from bson import Int64, ObjectId
    from decimal import Decimal
    t, _, payload = str(text).partition(SEP)
    if t == "int":
        return int(payload)
    if t == "long":
        return Int64(int(payload))
    if t == "double":
        return float(payload)
    if t == "decimal":
        from bson.decimal128 import Decimal128
        return Decimal128(Decimal(payload))
    if t == "string":
        return payload
    if t == "bool":
        return payload == "1"
    if t == "objectId":
        return ObjectId(payload)
    if t == "uuid":
        import uuid

        from bson.binary import Binary
        return Binary.from_uuid(uuid.UUID(payload))
    if t == "binData":
        return bytes.fromhex(payload)
    if t == "date":
        from . import nanotime
        return nanotime.parse(payload)
    return from_ejson(payload)


def plain(value):
    """A document or an array with BSON's number types as Python's: a
    64-bit integer an int, a Decimal128 its decimal - what a driver of a
    collection column takes. A type MongoDB alone has stays as it is,
    and the driver refuses it by name."""
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    if hasattr(value, "to_decimal"):
        return value.to_decimal()
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    return value
