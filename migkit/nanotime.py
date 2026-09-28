"""A timestamp or a time of day that keeps the digits past the microsecond.

Python's own date and time types stop at six digits after the second, and
every engine migkit reads into this process hands its values over as them.
SQL Server's `datetime2(7)`, ClickHouse's `DateTime64(9)`, Oracle's
`TIMESTAMP(9)` and DuckDB's `TIMESTAMP_NS` hold more: measured on DuckDB
1.x, `2024-01-01 00:00:00.123456789` read back as `...00.123456`. The cut
happened on both sides of a comparison alike, so a move that dropped the
digits compared equal. These carry the three digits after the sixth as
`nanosecond`, so a reading keeps them and the rendering writes all nine.
"""
import datetime


def of(value, ns):
    """`value` - a datetime or a time - carrying `ns` (0 to 999) digits
    past its microsecond; the value itself where there are none."""
    if not ns or value is None:
        return value
    if isinstance(value, datetime.datetime):
        out = Datetime9(value.year, value.month, value.day, value.hour,
                        value.minute, value.second, value.microsecond,
                        value.tzinfo, fold=value.fold)
    else:
        out = Time9(value.hour, value.minute, value.second,
                    value.microsecond, value.tzinfo, fold=value.fold)
    out.nanosecond = int(ns)
    return out


class Datetime9(datetime.datetime):
    """A datetime and the three digits after its microsecond. Anything that
    makes a new value from it (`replace`, `astimezone`) makes a plain one
    without them, so a reader takes `nanosecond` first."""
    nanosecond = 0

    def __reduce_ex__(self, protocol):
        return (of, (datetime.datetime(
            self.year, self.month, self.day, self.hour, self.minute,
            self.second, self.microsecond, self.tzinfo, fold=self.fold),
            self.nanosecond))

    def __repr__(self):
        return f"{super().__repr__()[:-1]}, nanosecond={self.nanosecond})"

    def isoformat(self, sep="T", timespec="auto"):
        """Every digit, which is what a driver that writes a value as its
        text then sends."""
        return _more(super().isoformat(sep, "microseconds"),
                     self.nanosecond)

    def __str__(self):
        return self.isoformat(" ")


class Time9(datetime.time):
    """A time of day and the three digits after its microsecond."""
    nanosecond = 0

    def isoformat(self, timespec="auto"):
        return _more(super().isoformat("microseconds"), self.nanosecond)

    def __str__(self):
        return self.isoformat()

    def __reduce_ex__(self, protocol):
        return (of, (datetime.time(self.hour, self.minute, self.second,
                                   self.microsecond, self.tzinfo,
                                   fold=self.fold), self.nanosecond))


def _more(text, ns):
    """An ISO text at six digits with the three after them put in, ahead
    of any offset."""
    at = text.index(".") + 7
    return f"{text[:at]}{ns:03d}{text[at:]}"


def parse(text):
    """`2024-01-01 00:00:00.123456789` or `12:00:00.5` - a server's own text
    of a timestamp or a time with up to nine digits after the second - as
    the value, keeping all nine. None is None."""
    import re
    if text is None:
        return None
    if isinstance(text, (bytes, bytearray)):
        text = bytes(text).decode("ascii")
    head, frac, zone = re.fullmatch(r"(.*?\d\d:\d\d(?::\d\d)?)(?:\.(\d+))?(.*)",
                                    str(text).strip()).groups()
    frac = ((frac or "") + "0" * 9)[:9]
    us, ns = int(frac[:6]), int(frac[6:])
    if re.fullmatch(r"\d{1,2}:\d\d(:\d\d)?", head):
        value = datetime.time.fromisoformat(head + zone)
    else:
        value = datetime.datetime.fromisoformat(head + zone)
    return of(value.replace(microsecond=us), ns)


def epoch_ticks(value, digits, zone=None):
    """A timestamp as a whole number of 10**-digits seconds since the
    epoch, with every digit it carries: what ClickHouse's `DateTime64`
    and Arrow's timestamps are written as. A value without a zone is read
    in `zone` (UTC where none is given)."""
    ns = getattr(value, "nanosecond", 0) or 0
    if value.tzinfo is None:
        value = value.replace(tzinfo=zone or datetime.timezone.utc)
    since = value - datetime.datetime(1970, 1, 1,
                                      tzinfo=datetime.timezone.utc)
    whole = since // datetime.timedelta(microseconds=1) * 1000 + ns
    return whole // 10 ** (9 - digits)
