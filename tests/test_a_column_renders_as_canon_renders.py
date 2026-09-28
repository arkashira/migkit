"""A column's renderer (`render.renderer`) gives exactly what
`canon.render_value` gives, for every class and whatever a value arrives
as.

The renderer writes the types a driver hands back for a class straight
and sends every other to `render_value`; a straight path that differed by
a byte would make the in-process fold disagree with the servers' SQL
digests - a table reported different that is not, or, worse, two
renderings that differ in the same way on both sides of a pair. So every
class is held to `render_value` over values of the types it is meant for
and of the types it is not: the subclasses (`bool` of `int`, `datetime`
of `date`), years before 1000, zones, NULL, and values `render_value`
refuses, which the renderer must refuse the same way.
"""
import datetime
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from migkit import canon, render, rowtext


class _Sub(datetime.datetime):
    """A datetime of a driver's own, as a dataframe library hands back."""


class _Decimal128:
    """BSON's decimal, which is asked for its Decimal."""

    def __init__(self, d):
        self.d = d

    def to_decimal(self):
        return self.d


_zones = st.builds(lambda m: datetime.timezone(datetime.timedelta(minutes=m)),
                   st.integers(-14 * 60 + 1, 14 * 60 - 1))
_naive = st.datetimes(min_value=datetime.datetime(1, 1, 1))
_aware = st.datetimes(min_value=datetime.datetime(2, 1, 1),
                      max_value=datetime.datetime(9998, 12, 31),
                      timezones=_zones)
_decimals = st.decimals(allow_nan=True, allow_infinity=True) | st.decimals(
    places=12, allow_nan=False, allow_infinity=False)
_json = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text()
    | st.decimals(allow_nan=False, allow_infinity=False),
    lambda inner: st.lists(inner, max_size=4)
    | st.dictionaries(st.text(max_size=5), inner, max_size=4), max_leaves=8)

#: what each class is handed: what a driver gives for it, and what else
#: may come
VALUES = {
    "integer": st.integers() | st.booleans() | st.binary(max_size=8)
    | st.integers(-10**6, 10**6).map(float) | _decimals
    | st.text(max_size=4),
    "decimal": _decimals | st.integers() | st.floats()
    | _decimals.map(_Decimal128) | st.text(max_size=6),
    "float": st.floats() | st.integers(-10**20, 10**20) | _decimals
    | st.booleans(),
    "boolean": st.booleans() | st.integers() | st.text(max_size=3)
    | st.floats(),
    "text": st.text() | st.integers() | st.binary(max_size=4) | _decimals,
    "own text": st.text() | st.integers() | st.floats(),
    "bytes": st.binary() | st.binary().map(bytearray)
    | st.binary().map(memoryview) | st.text(max_size=3),
    "date": st.dates() | _naive | _aware | st.text(max_size=3),
    "timestamp": _naive | _aware | _naive.map(
        lambda d: _Sub(d.year, d.month, d.day, d.hour, d.minute, d.second,
                       d.microsecond)) | st.dates() | st.text(max_size=3),
    "time": st.times() | st.times(timezones=_zones) | _naive
    | st.text(max_size=3),
    "json": _json | _json.map(lambda v: str(v)) | st.text(max_size=6),
}


def _either(fn, *args):
    try:
        return "text", fn(*args)
    except Exception as e:  # noqa: BLE001 - the refusal is the answer
        return "refused", type(e)


def test_every_class_is_held_to_the_rendering():
    assert set(VALUES) == set(canon.CLASSES)


UTC7 = datetime.timezone(datetime.timedelta(hours=7))
#: where a straight path and `render_value` part first, each always tried
EDGES = [
    ("time", datetime.time(1, 2, 3)), ("time", datetime.time(0, 0)),
    ("time", datetime.time(1, 2, 3, tzinfo=UTC7)),
    ("timestamp", datetime.datetime(2026, 1, 2)),
    ("timestamp", datetime.datetime(999, 12, 31, 23, 59, 59, 999999)),
    ("timestamp", datetime.datetime(1000, 1, 1)),
    ("timestamp", datetime.datetime(2026, 1, 2, 3, tzinfo=UTC7)),
    ("timestamp", _Sub(2026, 1, 2)), ("timestamp", datetime.date(2026, 1, 2)),
    ("date", datetime.date(999, 1, 1)), ("date", datetime.date(1000, 1, 1)),
    ("date", datetime.datetime(2026, 1, 2, 23, 59)),
    ("decimal", Decimal("1E-10")), ("decimal", Decimal("-0.000")),
    ("decimal", Decimal("1.500")), ("decimal", Decimal("NaN")),
    ("decimal", _Decimal128(Decimal("1E-10"))), ("decimal", 5),
    ("integer", True), ("integer", b"\x01\x00"), ("integer", 2 ** 70),
    ("boolean", 0), ("boolean", "f"), ("bytes", bytearray(b"\x00\xff")),
    ("bytes", memoryview(b"\x0a")), ("text", 5), ("own text", 1.5),
    ("float", -0.0), ("float", 1e45), ("float", 5), ("json", '{"b": 1}'),
]


def test_where_the_straight_paths_end_they_render_as_canon_does():
    for cls, value in EDGES:
        for v in (value, None):
            assert _either(render.renderer(cls), v) == \
                _either(canon.render_value, cls, v), (cls, v)


@settings(max_examples=400, deadline=None)
@given(st.data())
def test_a_column_renders_each_value_as_canon_does(data):
    cls = data.draw(st.sampled_from(sorted(VALUES)))
    value = data.draw(st.none() | VALUES[cls])
    assert _either(render.renderer(cls), value) == \
        _either(canon.render_value, cls, value), (cls, value)


# values each class takes without refusing: what a fold is handed
FOLDED = {
    "integer": st.integers() | st.booleans() | st.binary(max_size=8),
    "decimal": st.decimals(allow_nan=False, allow_infinity=False),
    "float": st.floats(),
    "boolean": st.booleans(),
    "text": st.text(),
    "own text": st.text(),
    "bytes": st.binary() | st.binary().map(memoryview),
    "date": st.dates(),
    "timestamp": _naive | _aware,
    "time": st.times() | st.times(timezones=_zones),
    # a str is read as the JSON text it holds
    "json": _json.filter(lambda v: not isinstance(v, str)),
}


@settings(max_examples=200, deadline=None)
@given(st.data())
def test_a_fold_of_rows_is_the_sum_it_always_was(data):
    classes = data.draw(st.lists(st.sampled_from(sorted(FOLDED)),
                                 min_size=1, max_size=6))
    rows = data.draw(st.lists(st.tuples(*[st.none() | FOLDED[c]
                                          for c in classes]), max_size=8))
    want = 0
    for row in rows:
        want = canon.digest_step(want, rowtext.encode(
            [canon.render_value(c, v) for c, v in zip(classes, row)]))
    assert canon.fold_rows(classes, rows, 7) == (len(rows), 7 + want)


def test_the_straight_paths_are_the_ones_taken():
    """The renderer is not `render_value` under another name: the types
    it writes straight do not reach it."""
    seen = []
    real = canon.render_value

    def counted(cls, value):
        seen.append(cls)
        return real(cls, value)
    canon.render_value = counted
    try:
        for cls, value in (("integer", 7), ("text", "x"),
                           ("decimal", Decimal("1.50")),
                           ("boolean", True), ("bytes", b"\x01"),
                           ("timestamp", datetime.datetime(2026, 1, 2)),
                           ("date", datetime.date(2026, 1, 2)),
                           ("time", datetime.time(1, 2, 3))):
            render.renderer(cls)(value)
        assert seen == []
        render.renderer("timestamp")(datetime.datetime(999, 1, 2))
        render.renderer("integer")(True)
        assert seen == ["timestamp", "integer"]
    finally:
        canon.render_value = real
