"""A table's ranges are cut again while the copy runs, from the rows the
finished ones held (`ranges.finished` -> `ranges.refine`): only ranges no
copy has taken, only once the measured rows are off the plan by more than
`ranges.OFF_BY`, and never where the plan was read whole. A cross-engine
copy keeps the plan's range size for it (`into=st`). Fakes only."""
import ast
from pathlib import Path

from migkit import ranges

EDGES = list(range(100, 1000, 100))


def _plan(every=100):
    st, saves = {}, []
    todo = ranges.plan(st, 1, 1000, lambda: EDGES, lambda: saves.append(1))
    st["every"] = every
    return st, todo, saves


def _copy(st, todo, density, workers=1):
    copied = []

    def one(rng):
        a, u = rng
        ranges.started(st, a, None, lambda: None)
        copied.append((a, u))
        ranges.finished(st, a, lambda: None, rows=(u - a) * density)
    ranges.Slots(workers).each(todo, one)
    return copied


def _covers(copied, lo=0, hi=1000):
    spans = sorted(copied)
    assert spans[0][0] == lo and spans[-1][1] == hi
    assert all(a[1] == b[0] for a, b in zip(spans, spans[1:]))


def test_ranges_three_times_the_plan_split_the_pending_ones():
    st, todo, _ = _plan()
    assert len(todo) == 10
    copied = _copy(st, todo, density=3)
    # the first range came back 300 rows against 100 planned: every range
    # not yet taken was split, and the copy took them as they now stand
    assert copied[0] == (0, 100)
    assert len(copied) > 10
    assert max(u - a for a, u in copied[1:]) < 100
    _covers(copied)
    assert sorted(tuple(r) for r in st["ranges"]) == sorted(copied)
    assert st["last"] == 1000


def test_ranges_the_size_planned_leave_the_plan_alone():
    st, todo, _ = _plan()
    copied = _copy(st, todo, density=1)
    assert copied == [(a, a + 100) for a in range(0, 1000, 100)]


def test_ranges_far_under_the_plan_merge_the_pending_ones():
    st, todo, _ = _plan(every=1000)
    copied = _copy(st, todo, density=1)
    assert len(copied) < 10
    _covers(copied)


def test_a_taken_range_is_never_cut_again():
    st, todo, _ = _plan()
    first, held = todo.take(), todo.take()
    ranges.finished(st, first[0], lambda: None, rows=300)
    # the second was taken before the first finished: kept whole
    assert [held[0], held[1]] in st["ranges"]
    assert [200, 300] not in st["ranges"]
    assert any(r[0] == 200 and r[1] < 300 for r in st["ranges"])


def test_a_plan_read_whole_is_copied_as_read():
    st, todo, _ = _plan()
    listed = list(todo)
    _copy(st, listed, density=3)
    assert [tuple(r) for r in st["ranges"]] == listed


def test_the_plan_is_weighed_about_log_n_times():
    st, todo, _ = _plan(every=10 ** 9)
    asked = []
    real = ranges.refine
    try:
        ranges.refine = lambda st, keep=(): asked.append(1) or False
        # off the plan on every range: weighed at 1, 2, 4 and 8 ranges
        _copy(st, todo, density=1)
    finally:
        ranges.refine = real
    assert len(asked) == 4


def test_a_cross_engine_copy_keeps_its_range_size_for_refine():
    tree = ast.parse(Path(ranges.__file__).with_name("engines").joinpath(
        "hetero.py").read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and n.func.attr == "cut"
             and getattr(n.func.value, "id", None) == "ranges"]
    assert calls and all(any(k.arg == "into" for k in c.keywords)
                         for c in calls)
