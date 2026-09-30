"""Where two ways do the same write - a batch loaded or inserted, a run
staged or sent as statements, a stage copied binary or as text - each is
tried once and the one that did a row in less time is taken from then on,
kept apart by table and by the run's size, climbed in `decide` as every
choice is: a tie goes to the way that leaves less behind. What was
measured is data the decision layer reads.

And the values the binary COPY must not take: each one a text COPY lands
differently or that the packer would read as another type.
"""
import datetime
import decimal
import uuid

import pytest

from migkit.config import Endpoint, Hop


def _engine():
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="h", port=1, user="u", password="p")
    return PostgresEngine(Hop(name="x", engine="postgres", source=ep,
                              target=ep, databases=["d"]))


def test_each_way_once_then_the_cheaper():
    eng = _engine()
    ways = eng.UPSERT_WAYS
    key = eng._work_key("upserts", "d.t", 1500)
    assert eng._cheaper(key, ways) == "staged"
    eng._took(key, "staged", 3.0, 1500)
    assert eng._cheaper(key, ways) == "statements"
    eng._took(key, "statements", 1.5, 1500)
    assert eng._cheaper(key, ways) == "statements"
    # the way taken grows dearer than the other's last cost: the other
    eng._took(key, "statements", 4.5, 1500)
    assert eng._cheaper(key, ways) == "staged"


def test_a_tie_goes_to_the_way_that_leaves_less_behind():
    """Staged makes a temporary table; within what timing a busy server
    twice can give, the statements are taken."""
    eng = _engine()
    key = eng._work_key("upserts", "d.t", 1500)
    eng._took(key, "staged", 1.00, 1000)
    eng._took(key, "statements", 1.04, 1000)
    assert eng._cheaper(key, eng.UPSERT_WAYS) == "statements"
    eng._took(key, "statements", 1.10, 1000)
    assert eng._cheaper(key, eng.UPSERT_WAYS) == "staged"


def test_runs_of_other_sizes_and_tables_are_kept_apart():
    eng = _engine()
    ways = eng.UPSERT_WAYS
    small = eng._work_key("upserts", "d.t", 1500)
    eng._took(small, "staged", 1.0, 1500)
    eng._took(small, "statements", 9.0, 1500)
    # the same power of two, the same costs
    assert eng._work_key("upserts", "d.t", 2047) == small
    assert eng._cheaper(small, ways) == "staged"
    for key in (eng._work_key("upserts", "d.t", 2048),
                eng._work_key("upserts", "d.u", 1500),
                eng._work_key("rows", "d.t", 1500)):
        assert eng._cheaper(key, ways) == "staged"
        eng._took(key, "staged", 1.0, 1500)
        assert eng._cheaper(key, ways) == "statements"


def test_what_was_measured_is_data():
    eng = _engine()
    assert eng.measured_ways() == []
    key = eng._work_key("stage", "public.t", 5000)
    eng._cheaper(key, eng.STAGE_WAYS)
    eng._took(key, "binary", 0.5, 5000)
    assert eng.measured_ways() == [
        {"work": "stage", "table": "public.t", "rows_from": 4096,
         "seconds_a_row": {"binary": 0.0001}, "chosen": None}]
    eng._took(key, "text", 1.0, 5000)
    assert eng.measured_ways()[0]["chosen"] == "binary"
    eng._took(key, "binary", 2.0, 5000)
    assert eng.measured_ways()[0]["chosen"] == "text"


TZ = datetime.timezone.utc


@pytest.mark.parametrize("kind,value", [
    # a time or instant of the other kind: text reads an offset into a
    # `timestamp` and drops it, and a naive one into `timestamptz` in the
    # session's zone
    ("timestamp", datetime.datetime(2026, 1, 1, tzinfo=TZ)),
    ("timestamptz", datetime.datetime(2026, 1, 1)),
    ("time", datetime.time(1, 2, tzinfo=TZ)),
    ("time", datetime.timedelta(hours=1)),
    # an instant into a date column, a date subclass
    ("date", datetime.datetime(2026, 1, 1)),
    # numbers of another kind than the column's
    ("int2", 1 << 15), ("int4", -(1 << 31) - 1), ("int8", 1 << 63),
    ("int8", True), ("int8", decimal.Decimal(3)), ("int4", 3.0),
    ("numeric", 1.5), ("float8", 3), ("float8", decimal.Decimal("1.5")),
    ("bool", 1),
    # text, bytes and identifiers of the other kind
    ("text", b"x"), ("bytea", "x"), ("uuid", str(uuid.UUID(int=1))),
    ("jsonb", {"a": 1}), ("text", 5),
    # columns sent as text whatever they hold
    ("float4", 1.5), ("interval", datetime.timedelta(1)), ("money", 5),
    ("_int4", [1]), (None, 1),
])
def test_a_value_binary_would_not_land_as_text_does_goes_as_text(kind,
                                                                  value):
    from migkit.engines.postgres import _copy_binary
    # refused before anything is packed: no connection is needed to say so
    assert _copy_binary(None, ["int4", kind], [[1, None], [2, value]]) \
        is None
