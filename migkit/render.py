"""The canonical text of a column's values, with the rendering chosen once
for the column rather than once for every value.

`canon.render_value` decides by the class of each value it is handed: a
row of eight columns is eight walks down its list of classes, and a
timestamp is found at the eighth test. A table's columns do not change
class from one row to the next, so the choice is made here once a column
(`renderer`), and each value then goes by the type it arrives as: the
types a driver hands back for that class are written straight, and any
other - a NULL, a subclass, a year before 1000, a zone - goes to
`canon.render_value` itself. Nothing here is a rule of its own: each
straight path is the one `render_value` takes for that exact type, and
`tests/test_a_column_renders_as_canon_renders.py` holds every class to it
over generated values.

Measured, 200,000 rows folded in this process (`canon.fold_rows`, best of
five, the same total before and after): eight columns of the bench's
kinds 0.58s before, 0.39s after; an integer and two timestamps 0.58s,
0.31s; four integers 0.22s, 0.19s; an integer and two texts 0.20s, 0.17s;
an integer and two decimals 0.23s, 0.20s. Floats and JSON stay at 0.58s
and 0.64s: their rendering is the work.
"""
import datetime
from decimal import Decimal

_DATETIME, _DATE, _TIME = datetime.datetime, datetime.date, datetime.time


def renderer(cls):
    """f(value) giving what `canon.render_value(cls, value)` gives."""
    from . import canon
    slow = canon.render_value

    if cls == "integer":
        def one(v):
            return str(v) if type(v) is int else slow(cls, v)
    elif cls in ("text", canon.OWN):
        def one(v):
            return v if type(v) is str else slow(cls, v)
    elif cls == "decimal":
        def one(v):
            return format(v, "f") if type(v) is Decimal else slow(cls, v)
    elif cls == "boolean":
        def one(v):
            if type(v) is bool:
                return "1" if v else "0"
            return slow(cls, v)
    elif cls == "bytes":
        def one(v):
            return v.hex().upper() if type(v) is bytes else slow(cls, v)
    elif cls == "timestamp":
        # the ISO form is `%Y-%m-%d %H:%M:%S.%f` for a naive instant; how a
        # platform pads a year below 1000 is left to `render_value`
        def one(v):
            if type(v) is _DATETIME and v.tzinfo is None and v.year >= 1000:
                return v.isoformat(" ", "microseconds")
            return slow(cls, v)
    elif cls == "date":
        def one(v):
            if type(v) is _DATE and v.year >= 1000:
                return v.isoformat()
            return slow(cls, v)
    elif cls == "time":
        def one(v):
            if type(v) is _TIME and v.tzinfo is None:
                return v.isoformat("microseconds")
            return slow(cls, v)
    else:
        def one(v):
            return slow(cls, v)
    return one


def digest_renderer(cls):
    """f(value) giving what `canon.digest_value(cls, value)` gives: the
    column's `renderer`, and a text or byte value wider than `canon.WIDE`
    as its length and md5, as every server's digest writes it - one the
    server hashed where it lies (`pieces.Hashed`) as what it wrote."""
    from . import canon
    from .pieces import Hashed
    one = renderer(cls)
    if cls == "bytes":
        def wide(v):
            if isinstance(v, (bytes, bytearray, memoryview)) \
                    and len(v) > canon.WIDE:
                return canon.wide_bytes(v)
            if type(v) is Hashed:
                return v.text()
            return one(v)
        return wide
    if cls == "text":
        def wide(v):
            if type(v) is str and len(v) * 4 > canon.WIDE:
                got = canon.wide_text(v)
                if got is not None:
                    return got
            if type(v) is Hashed:
                return v.text()
            return one(v)
        return wide
    return one

