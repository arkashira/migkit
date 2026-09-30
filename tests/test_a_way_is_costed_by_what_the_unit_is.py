"""A way's cost is a function of the unit, with its spread (backlog
P0-SMART, items 1, 2, 6 and learning across hops).

`costmodel.CostModel` fits each way's seconds as a line through what the
unit is - its bytes, rows, large values, keyless bytes, round trips and
the server's load - from every unit it ran, predicts the ways never run
from this hop's earlier runs, other hops here, the resource model or
migkit's own benchmarks, and a measurement always beats a prediction when
the climb ranks them. No server: the units are made up for the purpose,
and each test's knowledge base is its own.
"""
import math

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from migkit import costmodel as cm
from migkit import decide as d

R = d.Rung
MB = 2 ** 20


@pytest.fixture(autouse=True)
def own_knowledge(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_KNOWLEDGE", str(tmp_path / "knowledge.json"))
    cm._KNOWLEDGE.clear()
    yield
    cm._KNOWLEDGE.clear()


def _unit(mb, rows=None, lob=0.0, rtt=0.0, load=0.0, keyless=False):
    rows = rows if rows is not None else int(mb * MB / 200)
    return {"bytes": mb * MB, "rows": rows, "lob_bytes": lob * mb * MB,
            "rtt": rtt, "load": load, "keyless": keyless}


def _seconds(coef, u):
    return float(cm.features(u) @ __import__("numpy").array(coef))


# --- the line and its spread --------------------------------------------------

@settings(max_examples=40, deadline=None)
@given(start=st.floats(0.0, 2.0), per_mb=st.floats(0.001, 0.05),
       per_krow=st.floats(0.0, 0.01))
def test_a_line_is_learned_from_units_of_different_sizes(start, per_mb,
                                                          per_krow):
    """Units of several sizes on a known line: the fitted line predicts a
    unit ten times the largest within 5%, and says it measured it."""
    coef = (start, per_mb, per_krow, 0, 0, 0, 0)
    f = cm.Fit()
    for mb, rows in ((1, 2_000), (4, 30_000), (16, 20_000), (64, 400_000),
                     (8, 90_000), (32, 50_000)):
        f.update(cm.features(_unit(mb, rows)), _seconds(coef, _unit(mb,
                                                                   rows)))
    big = _unit(640, 2_000_000)
    mean, sd, _ = f.predict(cm.features(big))
    truth = _seconds(coef, big)
    assert mean == pytest.approx(truth, rel=0.05)
    assert sd > 0


def test_the_spread_narrows_as_units_come_in():
    coef = (0.2, 0.01, 0.002, 0, 0, 0, 0)
    f = cm.Fit()
    x = cm.features(_unit(10))
    sds = []
    for mb in (1, 3, 9, 27, 2, 5, 11, 17):
        u = _unit(mb)
        f.update(cm.features(u), _seconds(coef, u))
        sds.append(f.predict(x)[1])
    assert sds[-1] < sds[1] / 2, sds


def test_a_unit_far_off_its_line_is_a_surprise_and_halves_the_past():
    m = cm.CostModel("t", [R("a", "a way")], knowledge=False,
                     unit=_unit(10))
    for _ in range(6):
        z = m.saw("a", _unit(10)["rows"], 1.0, _unit(10))
    assert abs(z) < 1.0
    before = m.fit("a").n
    z = m.saw("a", _unit(10)["rows"], 10.0, _unit(10))
    assert z > cm.SURPRISE
    # what was learned before the change weighs half now
    assert m.fit("a").n < before


# --- measured, predicted, never measured --------------------------------------

def test_nothing_measured_or_predicted_is_no_estimate():
    m = cm.CostModel("t", [R("a", "a way")], knowledge=False)
    assert m.estimate("a") is None and m.get("a") is None
    m.saw("a", 1000, 0.5, _unit(0.2, 1000))
    got = m.estimate("a", _unit(0.2, 1000))
    assert got.measured and got.n == 1 and got.source == "this run"


def test_a_way_the_resource_model_sees_is_predicted_with_a_wide_spread():
    from migkit.model import Model, Stage
    model = Model([Stage("source", "the source's reads", per_stream=50e6,
                         total=200e6, truth="measured"),
                   Stage("target", "the target's writes", per_stream=20e6,
                         total=60e6, truth="measured")], measured=True)
    ways = [R("pass", "server to server", loads={"source": 2, "target": 1},
              startup=0.5)]
    m = cm.CostModel("t", ways, model=model, knowledge=False)
    got = m.estimate("pass", _unit(100))
    assert got is not None and not got.measured
    assert got.source == "the resource model"
    # a byte through the source twice at 50 MB/s, the slower of the two
    assert got.mean == pytest.approx(0.5 + 100 * max(2 * MB / 50e6,
                                                     MB / 20e6), rel=0.05)
    # the model is a prior with its own coefficient's width of spread
    assert got.high() > got.mean * 1.3


def test_migkits_benchmark_seeds_the_mysql_bulk_ways():
    """The MySQL table ladder's rungs' two measured rates solve for a
    second a MB and a second a thousand rows: the dump and load is
    predicted, never measured, on a MySQL pair."""
    from migkit.engines.mysql import PLAIN, MySQLEngine
    seeds = cm.seeds()
    rung = MySQLEngine.TABLE_WAYS[0]
    mu, source = seeds[(rung.name, "mysql->mysql")]
    assert "benchmark" in source
    narrow = {"bytes": 41.5e6, "rows": 500_000}
    got = float(cm.features(narrow) @ mu)
    # the narrow table's measured time: its start, its rows and bytes
    assert got == pytest.approx(rung.startup + 41.5e6
                                / rung.rates[PLAIN], rel=0.02)
    m = cm.CostModel("t", [R(rung.name, rung.said)], pair="mysql->mysql",
                     knowledge=False)
    e = m.estimate(rung.name, narrow)
    assert e is not None and e.truth == cm.PREDICTED


def test_other_hops_here_are_a_prior_for_the_next():
    kb = cm.Knowledge()
    first = cm.CostModel("t", [R("pipe", "through migkit")],
                         pair="postgres->postgres", knowledge=kb)
    for mb in (1, 2, 4, 8, 16):
        u = _unit(mb)
        first.saw("pipe", u["rows"], 0.1 + 0.02 * mb, u)
    first.save()
    nxt = cm.CostModel("t", [R("pipe", "through migkit")],
                       pair="postgres->postgres", knowledge=cm.Knowledge())
    got = nxt.estimate("pipe", _unit(32))
    assert got is not None and got.truth == cm.PREDICTED
    assert got.source == "other hops here"
    assert got.mean == pytest.approx(0.1 + 0.02 * 32, rel=0.25)
    # another pair learned nothing from it
    other = cm.CostModel("t", [R("pipe", "through migkit")],
                         pair="mysql->mysql", knowledge=cm.Knowledge())
    assert other.estimate("pipe", _unit(32)) is None


def test_the_hops_own_earlier_runs_count_as_measured(tmp_path):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"), databases=["appdb"])
    hop.report_dir = lambda db="", _p=tmp_path: _p
    run = cm.CostModel("copy", [R("pipe", "through migkit")], hop=hop,
                       knowledge=False)
    for mb in (1, 3, 9):
        run.saw("pipe", _unit(mb)["rows"], 0.05 * mb, _unit(mb))
    run.save()
    again = cm.CostModel("copy", [R("pipe", "through migkit")], hop=hop,
                         knowledge=False)
    got = again.estimate("pipe", _unit(9))
    assert got.measured and got.source == "this hop's earlier runs"
    assert got.mean == pytest.approx(0.45, rel=0.2)


# --- the climb on a model -----------------------------------------------------

def _model(measured=None, predicted=None, knowledge=False, pair=""):
    """A cost model whose ways cost what the test says: measured ones by
    three units each, predicted ones by a prior."""
    import numpy as np
    ways = [R(n, f"the way {n}") for n in list(measured or {})
            + list(predicted or {})]
    m = cm.CostModel("t", ways, knowledge=knowledge, pair=pair,
                     unit=_unit(10, 50_000))
    for n, secs in (measured or {}).items():
        for _ in range(3):
            m.saw(n, 50_000, secs, _unit(10, 50_000))
    for n, (mean, share) in (predicted or {}).items():
        mu = np.zeros(cm.D)
        mu[0] = mean
        sd = np.array(cm.LOOSE_SD) * 1e-6
        sd[0] = share * mean
        m.fits[n] = cm.Fit(mu, sd, "the resource model")
    return ways, m


def test_a_prediction_whose_best_is_slower_than_what_was_measured_waits():
    """The lead is outside its spread: the measured way is taken, the
    predicted one is not tried, and the choice says why not."""
    ways, m = _model({"a": 10.0}, {"b": (20.0, 0.1)})
    got = d.climb("t", ways, costs=m, unit=_unit(10, 50_000))
    assert got.path == "a"
    assert got.runner_up.name == "b"
    assert got.why_not.startswith("predicted slower")
    assert "outside its spread" in got.why_not


def test_a_prediction_that_may_be_faster_is_tried_first():
    ways, m = _model({"a": 10.0}, {"b": (8.0, 0.5)})
    got = d.climb("t", ways, costs=m, unit=_unit(10, 50_000))
    assert got.path == "b"
    # committing, a prediction is taken over a measurement only where it
    # is faster even at its p90
    got = d.climb("t", ways, costs=m, unit=_unit(10, 50_000), explore=False)
    assert got.path == "a"
    ways, m = _model({"a": 10.0}, {"b": (5.0, 0.1)})
    got = d.climb("t", ways, costs=m, unit=_unit(10, 50_000), explore=False)
    assert got.path == "b"


def test_where_nothing_is_measured_the_predictions_rank():
    ways, m = _model({}, {"slow": (20.0, 0.2), "fast": (5.0, 0.2)})
    got = d.climb("t", ways, costs=m)
    assert got.path == "fast"
    assert "predicted the fastest here" in got.reason


def test_two_measured_ways_within_their_noise_go_to_the_lighter():
    import dataclasses
    ways, m = _model({"heavy": 10.0, "light": 10.3})
    ways = [dataclasses.replace(ways[0], footprint=2),
            dataclasses.replace(ways[1], footprint=0)]
    got = d.climb("t", ways, costs=m, unit=_unit(10, 50_000))
    assert got.path == "light"
    assert got.why_not.startswith("within the noise")
    # far apart, the faster one whatever it leaves
    ways2, m2 = _model({"heavy": 5.0, "light": 10.3})
    ways2 = [dataclasses.replace(ways2[0], footprint=2),
             dataclasses.replace(ways2[1], footprint=0)]
    assert d.climb("t", ways2, costs=m2,
                   unit=_unit(10, 50_000)).path == "heavy"


def test_a_way_that_fails_here_costs_its_thrown_away_work():
    """Two ways measured alike: the one whose history here is failure
    after failure is ranked after the other (its expected time counts the
    work a failure throws away), and attempt keeps each failure."""
    ways, m = _model({"flaky": 10.0, "sound": 10.0},
                     knowledge=cm.Knowledge(), pair="postgres->postgres")
    for _ in range(12):
        m.outcome("flaky", False, "the server closed the connection")
    m.outcome("sound", True)
    # this run's tries count before they are kept
    assert m.risk("flaky") > m.risk("sound")
    got = d.climb("t", ways, costs=m, unit=_unit(10, 50_000))
    assert got.path == "sound"
    m.save()
    # and once kept, the next run of these engines knows them
    ways2, again = _model({"flaky": 10.0, "sound": 10.0},
                          knowledge=cm.Knowledge(), pair="postgres->postgres")
    assert again.risk("flaky") == pytest.approx(cm.risk(12, 12))
    assert again.risk("sound") == pytest.approx(cm.risk(1, 0))
    assert d.climb("t", ways2, costs=again,
                   unit=_unit(10, 50_000)).path == "sound"
    assert cm.risked(10.0, 0.5, 0.5) == pytest.approx(12.5)
    assert cm.risked(10.0, 0.0) == 10.0


def test_attempt_keeps_each_failure_for_the_next_climb():
    ways, m = _model({"a": 1.0, "b": 2.0})

    def run(choice):
        if choice.path == "a":
            raise OSError("refused")
        return "done"
    got, result = d.attempt("t", ways, run, costs=m)
    assert got.path == "b" and result == "done"
    assert m._tries["a"][1] == 1 and m._tries["b"] == [1, 0, ""]


def test_a_number_a_unit_ranks_as_it_always_did():
    """`Costs` - one number a rung - is ranked by the old rule: never
    timed first, then the cheapest, a 5% tie to the footprint."""
    c = d.Costs({"a": 1.0})
    ways = [R("a", "a"), R("b", "b")]
    assert d.climb("t", ways, costs=c).path == "b"
    c["b"] = 1.04
    assert d.climb("t", ways, costs=c).path == "a"


def test_the_estimate_says_its_time_in_words():
    e = cm.Estimate(120.0, 30.0, cm.MEASURED_TRUTH, n=4, dof=10)
    assert e.low() < e.mean < e.high()
    said = e.said()
    assert said.startswith("~2m00s") and "measured" in said
    assert math.isclose(e.scaled(2).mean, 240.0)
