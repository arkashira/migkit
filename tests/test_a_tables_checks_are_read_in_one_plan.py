"""Every aggregate the checks ask of a table is read in one plan
(backlog P0-VERIFY-EXECUTION, `scanplan`): one bounded read a side or a
key range, not one a column a check; a failed fused read falls to reads
apart that name the failing column; values kept under unchanged markers
are not read again; the reads share the run's slots.

Pure: the servers' answers are given in-process.
"""
import threading
import time
from types import SimpleNamespace

import pytest

from migkit import ranges, scanplan, sizing
from migkit.scanplan import Aggregate

ROWS = {"src": [(1, None, 1.5), (2, "", 2.5), (3, "x", None)],
        "dst": [(1, None, 1.5), (2, None, 2.5), (3, "x", None)]}


def _value(expr, rows):
    if expr == "nulls":
        return sum(r[1] is None for r in rows)
    if expr == "empties":
        return sum(r[1] == "" for r in rows)
    if expr == "fsum":
        return sum(r[2] or 0 for r in rows)
    if expr == "fmax":
        return max((abs(r[2]) for r in rows if r[2] is not None), default=0)
    raise AssertionError(expr)


def _query(asked, fail=None, delay=0.0, lock=None, live=None):
    def q(side, sql):
        exprs = sql.split("select count(*), ")[1].split(" from ")[0]
        exprs = exprs.split(", ")
        if lock is not None:
            with lock:
                live["now"] += 1
                live["peak"] = max(live["peak"], live["now"])
        try:
            asked.append((side, sql))
            time.sleep(delay)
            if fail and fail in exprs:
                raise RuntimeError("Lost connection while reading")
            rows = ROWS[side]
            if " where id < 2" in sql:
                rows = [r for r in rows if r[0] < 2]
            elif " where id >= 2" in sql:
                rows = [r for r in rows if r[0] >= 2]
            return [(len(rows),) + tuple(_value(e, rows) for e in exprs)]
        finally:
            if lock is not None:
                with lock:
                    live["now"] -= 1
    return q


AGGS = [Aggregate("nullempty", "t", "c", ("nulls", "empties")),
        Aggregate("float", "t", "f", ("fsum", "fmax"), ("sum", "max")),
        Aggregate("other", "t", "c", ("nulls",))]


@pytest.fixture(autouse=True)
def _plan(monkeypatch):
    monkeypatch.setattr(sizing, "estimate",
                        lambda *a, **k: sizing.Plan(2, 4, {}))
    monkeypatch.setattr(sizing, "resources", lambda *a, **k: {})


def _eng():
    return SimpleNamespace(hop=SimpleNamespace(slice=10))


def test_every_check_of_a_table_is_one_read_a_side(tmp_path):
    asked = []
    got = scanplan.run(_eng(), "app", AGGS, source=lambda s, t: t,
                       query=_query(asked), progress=tmp_path / "p.json")
    assert len(asked) == 2 and {s for s, _ in asked} == {"src", "dst"}
    # the expression two checks ask is read once
    assert asked[0][1].count("nulls") == 1
    assert got.value("src", AGGS[0]) == (1, 1)
    assert got.value("dst", AGGS[0]) == (2, 0)
    assert got.ways["t"] == "fused" and not got.failed


def test_ranges_are_read_side_by_side_and_folded():
    asked, lock, live = [], threading.Lock(), {"now": 0, "peak": 0}
    got = scanplan.run(
        _eng(), "app", AGGS, source=lambda s, t: t,
        query=_query(asked, delay=0.02, lock=lock, live=live),
        parts=lambda t: ["id < 2", "id >= 2"])
    assert len(asked) == 4 and live["peak"] >= 2
    assert got.ways["t"] == "ranges"
    assert got.value("src", AGGS[1]) == (4.0, 2.5)
    assert got.rows[("src", "t")] == 3


def test_a_failed_fused_read_names_the_column_and_never_says_zero():
    asked = []
    got = scanplan.run(_eng(), "app", AGGS, source=lambda s, t: t,
                       query=_query(asked, fail="fmax"))
    assert got.ways["t"] == "apart"
    assert got.value("src", AGGS[1]) is None
    assert set(got.failures("float")) == {"t.f"}
    assert got.value("dst", AGGS[0]) == (2, 0)
    assert not got.failures("nullempty")


def test_kept_values_are_not_read_again_while_the_markers_hold(tmp_path):
    marks = {"src": "m1", "dst": "m2"}

    def run(asked, held="", m=marks):
        return scanplan.run(
            _eng(), "app", AGGS, source=lambda s, t: t, query=_query(asked),
            markers=lambda s, t: m[s], held=held, scope="s1",
            evidence=scanplan.Evidence(tmp_path / "ev.json"))
    first = []
    run(first)
    assert len(first) == 2
    again = []
    got = run(again)
    assert again == [] and got.ways["t"] == "kept"
    assert got.value("dst", AGGS[0]) == (2, 0)
    held = []
    run(held, held="the proof goes into a cutover")
    assert len(held) == 2
    moved = []
    run(moved, m={"src": "m1", "dst": "m3"})
    assert len(moved) == 2


def test_the_reads_borrow_the_runs_slots_rather_than_multiply_them():
    asked, lock, live = [], threading.Lock(), {"now": 0, "peak": 0}
    outer = ranges.Slots(1)

    def check(_):
        scanplan.run(_eng(), "app", AGGS, source=lambda s, t: t,
                     query=_query(asked, delay=0.01, lock=lock, live=live),
                     parts=lambda t: ["id < 2", "id >= 2"])
    outer.each(["db"], check)
    assert len(asked) == 4 and live["peak"] == 1


def test_only_ranges_that_cover_the_table_are_used():
    assert scanplan.covering([(None, 5), (5, None)]) == [(None, 5), (5, None)]
    assert scanplan.covering([(1, 5), (5, None)]) == [(None, None)]
    assert scanplan.covering([(None, 5), (6, None)]) == [(None, None)]


def test_no_way_of_reading_is_less_than_every_row():
    assert all("every row" in r.gives for r in scanplan.READS)
    assert scanplan.NEED == ("every row",)
