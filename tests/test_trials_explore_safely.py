"""The ranges of a table explore the ways safely and re-plan when the world
changes (backlog P0-SMART, items 3 and 5).

`decide.Trials` with a cost model tries a way not measured yet only on a
unit no larger than the median, never on a deadline's critical path,
never where its prediction's best is slower than what was measured, and
within `EXPLORE_BUDGET` of the work's time; a unit whose time leaves its
spread, or a resource whose limit the pace cut, re-climbs what is left
and says the switch in one line. Without a model it is what it was: each
way never timed once, then the cheapest (pinned in
`test_a_table_times_each_way_on_its_own_ranges.py`). No server.
"""
import numpy as np
import pytest

from migkit import costmodel as cm
from migkit import decide as d
from migkit import sizing

R = d.Rung
MB = 2 ** 20


@pytest.fixture(autouse=True)
def own_knowledge(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_KNOWLEDGE", str(tmp_path / "knowledge.json"))
    cm._KNOWLEDGE.clear()
    yield
    cm._KNOWLEDGE.clear()


def _unit(rows):
    return {"rows": rows, "bytes": rows * 200.0}


def _trials(ways, ahead=None, critical=False, log=None, limits=None,
            predicted=None):
    m = cm.CostModel("public.big", ways, knowledge=False,
                     unit=_unit(100_000))
    for name, per_krow in (predicted or {}).items():
        # a prediction sure of itself: a tenth of its cost a row either
        # way, nothing else to it
        mu = np.zeros(cm.D)
        mu[2] = per_krow
        sd = np.array(cm.LOOSE_SD) * 1e-3
        sd[2] = 0.1 * per_krow
        m.fits[name] = cm.Fit(mu, sd, "the resource model")
    return d.Trials("public.big", ways, model=m, unit=_unit(100_000),
                    ahead=ahead, critical=critical, log=log, limits=limits)


def test_a_way_never_measured_is_not_tried_on_a_range_above_the_median():
    ways = [R("pass", "server to server"), R("pipe", "through migkit")]
    t = _trials(ways, ahead=[10_000, 20_000, 30_000, 900_000])
    first = t.pick(_unit(10_000))
    t.took(first.path, 10_000, 0.1, _unit(10_000))
    # the largest range: committing to what was measured, the other way
    # not tried on it
    big = t.pick(_unit(900_000))
    assert big.path == first.path
    # a small one: the other way gets its trial
    small = t.pick(_unit(20_000))
    assert small.path != first.path


def test_nothing_is_explored_on_a_deadlines_critical_path():
    ways = [R("pass", "server to server"), R("pipe", "through migkit")]
    t = _trials(ways, critical=True)
    got = t.pick(_unit(10_000))
    t.took(got.path, 10_000, 0.1, _unit(10_000))
    for _ in range(5):
        assert t.pick(_unit(10_000)).path == got.path


def test_a_way_predicted_slower_beyond_its_spread_is_never_tried():
    ways = [R("fast", "the fast way"), R("slow", "the slow way")]
    # both predicted; the slow one at ten times the fast one's cost
    t = _trials(ways, predicted={"fast": 0.01, "slow": 0.1})
    got = t.pick(_unit(100_000))
    assert got.path == "fast"
    for _ in range(4):
        t.took("fast", 100_000, 1.0, _unit(100_000))
    for _ in range(10):
        got = t.pick(_unit(50_000))
        assert got.path == "fast", got
    assert "slow" not in t._timing


def test_an_undecided_way_is_timed_again_on_a_small_unit():
    """Two ways measured once each within reach of each other: the slower
    is timed once more before the work settles - its lead or its lag not
    yet outside the spread."""
    ways = [R("a", "way a"), R("b", "way b")]
    t = _trials(ways)
    for name, secs in (("a", 1.0), ("b", 1.05)):
        assert t.pick(_unit(100_000)).path == name
        t.took(name, 100_000, secs, _unit(100_000))
    picked = [t.pick(_unit(50_000)).path for _ in range(1)]
    assert picked == ["b"]
    t.took("b", 50_000, 0.6, _unit(50_000))
    # both have two units now: the cheaper by what was measured
    assert t.pick(_unit(50_000)).path == "a"


def test_exploring_stays_within_its_budget():
    """Once exploring has cost more than `EXPLORE_BUDGET` of the work's
    time over the best way, nothing new is tried: the measured best is
    taken, even for a way nothing says anything of."""
    ways = [R("a", "way a"), R("b", "way b"), R("c", "way c")]
    t = _trials(ways)
    assert t.pick(_unit(100_000)).path == "a"
    t.took("a", 100_000, 1.0, _unit(100_000))
    t._on = ways[0]
    # b ran once and cost far more than a would have
    t.took("b", 100_000, 10.0, _unit(100_000))
    assert t._extra == pytest.approx(9.0, rel=0.2)
    assert t._extra > d.EXPLORE_BUDGET * t._spent
    got = t.pick(_unit(100_000))
    assert got.path == "a", got


def test_a_unit_outside_its_spread_re_plans_and_says_the_switch():
    said = []
    ways = [R("a", "server to server"), R("b", "through migkit")]
    t = _trials(ways, log=said.append)
    for name, secs in (("a", 1.0), ("b", 2.0)):
        t.pick(_unit(100_000))
        t.took(name, 100_000, secs, _unit(100_000))
    for _ in range(3):
        assert t.pick(_unit(100_000)).path == "a"
        t.took("a", 100_000, 1.0, _unit(100_000))
    # the source got busy: a range of the same size takes ten times longer
    t.took("a", 100_000, 10.0, _unit(100_000))
    t.took("a", 100_000, 10.0, _unit(100_000))
    got = t.pick(_unit(100_000))
    assert got.path == "b", got
    assert len(said) == 1, said
    line = said[0]
    assert line.startswith("public.big: through migkit for what is left")
    assert "was predicted" in line and "server to server" in line


def test_a_resource_cut_slows_the_ways_that_load_it():
    """The pace cut the source's limit to a quarter: the way that reads
    the source is costed at four times its time and the one that does not
    is taken, said in one line."""
    said = []
    ways = [R("pass", "server to server", loads={"source": 2}),
            R("pipe", "through migkit", loads={"target": 1})]
    lim = sizing.Limit("the source's reads", lambda: None, 8)
    t = _trials(ways, log=said.append,
                limits={"the source's reads": lim})
    for name, secs in (("pass", 1.0), ("pipe", 2.0)):
        t.pick(_unit(100_000))
        t.took(name, 100_000, secs, _unit(100_000))
    for _ in range(2):
        assert t.pick(_unit(100_000)).path == "pass"
        t.took("pass", 100_000, 1.0, _unit(100_000))
        t.took("pipe", 100_000, 2.0, _unit(100_000))
    lim.value = 2
    got = t.pick(_unit(100_000))
    assert got.path == "pipe", got
    assert said and "the source's reads held to 2 of 8" in said[-1]
    # the limit given back: the faster way again
    lim.value = 8
    assert t.pick(_unit(100_000)).path == "pass"


def test_what_the_trials_learned_is_kept():
    ways = [R("pass", "server to server")]
    m = cm.CostModel("public.big", ways, pair="postgres->postgres",
                     knowledge=cm.Knowledge())
    t = d.Trials("public.big", ways, model=m, unit=_unit(100_000))
    for rows in (10_000, 50_000, 90_000):
        t.pick(_unit(rows))
        t.took("pass", rows, rows / 100_000, _unit(rows))
    t.give_up("pass", "the source would not export a snapshot")
    t.save()
    kb = cm.Knowledge()
    assert kb.fit("pass", "postgres->postgres").n == pytest.approx(3)
    assert kb.failures("pass", "postgres->postgres") == (4, 1)
