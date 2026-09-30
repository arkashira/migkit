"""An interval as the three parts every engine that has one keeps apart:
months, days, and a time of nanoseconds.

A month is not thirty days and a day is not twenty-four hours - across a
month end or a clock change they differ - so PostgreSQL, DuckDB and
Cassandra keep the three apart, and `interval '1 mon'` is not
`interval '30 days'`. The drivers do not: psycopg2 and DuckDB's library
hand an interval over as a Python `timedelta`, which has no months -
measured on DuckDB 1.5, `1 year 2 months 3 days 04:05:06.5` came back as
423 days and 04:05:06.5 - so a move would write another value, and every
rendering through the `timedelta` would take `1 mon` and `30 days` for
one. Here an interval is read as its three parts (`canon.read_expr`, the
engines' own reads) and kept as them.

Its text (`text`) is ISO 8601 with the three parts written apart -
`P14M3DT14706.5S` - which PostgreSQL reads back as the same value.
"""
import datetime
import re
from decimal import Decimal

#: nanoseconds in a second, a microsecond
SECOND, MICRO = 10 ** 9, 10 ** 3


class Interval(tuple):
    """(months, days, nanoseconds), each signed on its own."""
    __slots__ = ()

    def __new__(cls, months=0, days=0, nanos=0):
        return super().__new__(cls, (int(months), int(days), int(nanos)))

    months = property(lambda self: self[0])
    days = property(lambda self: self[1])
    nanos = property(lambda self: self[2])

    def __repr__(self):
        return f"Interval({self[0]}, {self[1]}, {self[2]})"

    def __str__(self):
        return text(self)


def _seconds(nanos):
    """Nanoseconds as seconds, the digits it has and no more."""
    if not nanos:
        return "0"
    return format(Decimal(nanos).scaleb(-9).normalize(), "f")


def text(value):
    """The canonical text: `P{months}M{days}DT{seconds}S`."""
    months, days, nanos = of(value)
    return f"P{months}M{days}DT{_seconds(nanos)}S"


def postgres_text(value):
    """What PostgreSQL and DuckDB both read as the same three parts - each
    unit said, the time in microseconds. None where the value carries a
    digit past the microsecond, which neither keeps (`unfit`)."""
    months, days, nanos = of(value)
    if nanos % MICRO:
        return None
    return f"{months} months {days} days {nanos // MICRO} microseconds"


_ISO = re.compile(r"P(?:(?P<Y>[-+]?\d+(?:\.\d+)?)Y)?"
                  r"(?:(?P<Mo>[-+]?\d+(?:\.\d+)?)M)?"
                  r"(?:(?P<W>[-+]?\d+(?:\.\d+)?)W)?"
                  r"(?:(?P<D>[-+]?\d+(?:\.\d+)?)D)?"
                  r"(?:T(?:(?P<H>[-+]?\d+(?:\.\d+)?)H)?"
                  r"(?:(?P<Mi>[-+]?\d+(?:\.\d+)?)M)?"
                  r"(?:(?P<S>[-+]?\d+(?:\.\d+)?)S)?)?$")

_UNITS = {"year": ("m", 12), "years": ("m", 12), "mon": ("m", 1),
          "mons": ("m", 1), "month": ("m", 1), "months": ("m", 1),
          "week": ("d", 7), "weeks": ("d", 7), "day": ("d", 1),
          "days": ("d", 1), "hour": ("n", 3600 * SECOND),
          "hours": ("n", 3600 * SECOND), "min": ("n", 60 * SECOND),
          "mins": ("n", 60 * SECOND), "minute": ("n", 60 * SECOND),
          "minutes": ("n", 60 * SECOND), "sec": ("n", SECOND),
          "secs": ("n", SECOND), "second": ("n", SECOND),
          "seconds": ("n", SECOND), "millisecond": ("n", 10 ** 6),
          "milliseconds": ("n", 10 ** 6), "microsecond": ("n", MICRO),
          "microseconds": ("n", MICRO), "nanosecond": ("n", 1),
          "nanoseconds": ("n", 1)}


def _whole(number, scale):
    """`number` (text, maybe with a fraction) times `scale`, exactly - a
    fraction of a unit that does not come out whole is refused rather
    than rounded."""
    got = Decimal(number) * scale
    if got != got.to_integral_value():
        raise ValueError(f"{number} does not come to a whole number of the"
                         " part it is kept in")
    return int(got)


def parse(raw):
    """An interval from its text: ISO 8601 (`P1Y2M3DT4H5M6.5S`, each part
    signed on its own), or the unit-by-unit form PostgreSQL and DuckDB
    print (`1 year 2 mons 3 days 04:05:06.5`, `-1 mons +2 days
    -00:00:01`)."""
    s = str(raw).strip()
    m = _ISO.fullmatch(s)
    if m and s != "P":
        g = m.groupdict()
        months = _whole(g["Y"] or 0, 12) + _whole(g["Mo"] or 0, 1)
        days = _whole(g["W"] or 0, 7) + _whole(g["D"] or 0, 1)
        nanos = (_whole(g["H"] or 0, 3600 * SECOND)
                 + _whole(g["Mi"] or 0, 60 * SECOND)
                 + _whole(g["S"] or 0, SECOND))
        return Interval(months, days, nanos)
    body = s[1:].strip() if s.startswith("@") else s
    ago = body.endswith(" ago")
    if ago:
        body = body[:-4]
    parts = {"m": 0, "d": 0, "n": 0}
    tokens = body.split()
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        clock = re.fullmatch(r"([-+]?)(\d+):(\d\d)(?::(\d\d(?:\.\d+)?))?", tok)
        if clock:
            sign = -1 if clock.group(1) == "-" else 1
            parts["n"] += sign * (int(clock.group(2)) * 3600 * SECOND
                                  + int(clock.group(3)) * 60 * SECOND
                                  + _whole(clock.group(4) or 0, SECOND))
            i += 1
            continue
        if i + 1 >= len(tokens) or tokens[i + 1].lower() not in _UNITS:
            raise ValueError(f"not an interval: {raw!r}")
        kind, scale = _UNITS[tokens[i + 1].lower()]
        parts[kind] += _whole(tok, scale)
        i += 2
    out = Interval(parts["m"], parts["d"], parts["n"])
    return Interval(-out[0], -out[1], -out[2]) if ago else out


def of(value):
    """An `Interval` from what a reader hands over, or None for NULL: one
    already, its text, Cassandra's `Duration`, Oracle's year-to-month, or
    a `timedelta` - Oracle's day-to-second, whose days and time share one
    sign as Oracle keeps them."""
    if value is None:
        return None
    if isinstance(value, Interval):
        return value
    if isinstance(value, (str, bytes)):
        return parse(value.decode() if isinstance(value, bytes) else value)
    if all(hasattr(value, a) for a in ("months", "days", "nanoseconds")):
        return Interval(value.months, value.days, value.nanoseconds)
    if hasattr(value, "years") and hasattr(value, "months"):
        return Interval(value.years * 12 + value.months, 0, 0)
    if isinstance(value, datetime.timedelta):
        us = (value.days * 86400 + value.seconds) * 10 ** 6 \
            + value.microseconds
        days, rest = divmod(abs(us), 86400 * 10 ** 6)
        sign = -1 if us < 0 else 1
        return Interval(0, sign * days, sign * rest * MICRO)
    if isinstance(value, (tuple, list)) and len(value) == 3:
        return Interval(*value)
    raise ValueError(f"not an interval: {value!r}")


def timedelta_of(value):
    """The value as a `timedelta` - what Oracle's day-to-second takes -
    or None where it has months, or a digit past the microsecond, which
    a `timedelta` cannot hold."""
    months, days, nanos = of(value)
    if months or nanos % MICRO:
        return None
    return datetime.timedelta(days=days, microseconds=nanos // MICRO)
