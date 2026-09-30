"""Numbers set by reasoning are tuned from what runs measure, and the pace
holds the hop's limits as targets (backlog P0-DEPTH, items 3, 4 and 5,
built on P0-SMART's knowledge base).

Each reasoned number is declared with today's value as its prior
(`tuning.TUNABLES`); a run that measured what it should have been moves
it - per machine, and per hop where the hop measured its own - never
past `SPAN` times its prior. The probe floor and the tie read their tuned
values. The pace holds the hop's share of the source and its replica lag
as targets (`sizing.Held`), not only strains it backs off from. No
server.
"""
import math

import pytest

from migkit import costmodel as cm
from migkit import sizing, tuning
from migkit.config import Endpoint, Hop


@pytest.fixture(autouse=True)
def own_knowledge(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_KNOWLEDGE", str(tmp_path / "knowledge.json"))
    monkeypatch.delenv("MIGKIT_PROBE_FROM", raising=False)
    cm._KNOWLEDGE.clear()
    yield
    cm._KNOWLEDGE.clear()


def _hop(tmp_path, **options):
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"), databases=["appdb"],
              options=options)
    hop.report_dir = lambda db="", _p=tmp_path: _p
    return hop


def test_every_reasoned_number_is_declared_with_its_prior():
    from migkit import canon, decide, model, ranges, throttle
    from migkit.engines.postgres import PostgresEngine
    priors = {"model.PROBE_FROM": model.PROBE_FROM,
              "decide.TIE": decide.TIE, "canon.WIDE": canon.WIDE,
              "ranges.BUCKET_ROWS": ranges.BUCKET_ROWS,
              "throttle.REPLICA_RISE": throttle.REPLICA_RISE,
              "postgres.DIGEST_MEMORY": PostgresEngine.DIGEST_MEMORY}
    for name, prior in priors.items():
        t = tuning.TUNABLES[name]
        assert t.prior() == float(prior), name
        assert len(t.how.split()) >= 6, name
        # nothing measured yet: the prior itself
        assert tuning.value(name) == pytest.approx(float(prior))


def test_what_runs_measure_moves_a_number_within_its_span(tmp_path):
    prior = tuning.value("decide.TIE")
    for _ in range(3):
        tuning.observe("decide.TIE", 0.10)
    # three runs against a prior weighing three: the geometric mean
    assert tuning.value("decide.TIE") == pytest.approx(
        math.sqrt(prior * 0.10), rel=1e-6)
    for _ in range(200):
        tuning.observe("decide.TIE", 100.0)
    assert tuning.value("decide.TIE") == pytest.approx(prior * tuning.SPAN)
    # nothing unknown or not positive is kept
    tuning.observe("no such number", 1.0)
    tuning.observe("decide.TIE", 0.0)
    tuning.observe("decide.TIE", -1.0)
    assert tuning.value("decide.TIE") == pytest.approx(prior * tuning.SPAN)


def test_a_hops_own_measurement_is_its_own(tmp_path):
    hop = _hop(tmp_path)
    prior = tuning.value("model.PROBE_FROM")
    tuning.observe("model.PROBE_FROM", prior * 4, hop)
    mine = tuning.value("model.PROBE_FROM", hop)
    assert mine > prior
    # this machine learned it too, the hop with its own weight
    assert tuning.value("model.PROBE_FROM") == pytest.approx(mine)
    other = _hop(tmp_path / "other")
    tuning.observe("model.PROBE_FROM", prior / 4, other)
    assert tuning.value("model.PROBE_FROM", other) < prior
    assert tuning.value("model.PROBE_FROM", hop) == pytest.approx(mine)


def test_the_probe_floor_reads_its_tuned_value_unless_the_operator_sets_it(
        tmp_path, monkeypatch):
    from migkit import model
    hop = _hop(tmp_path)
    assert model.probe_floor(hop) == pytest.approx(model.PROBE_FROM)
    m = model.Model(per_row=100.0)
    m.probe_seconds = 9.0
    # the move carried 10,000 rows a second of 100 bytes: 9 s of probes
    # is 9 MB of its work
    model.tune_probe_floor(hop, m, 1_000_000, 100.0)
    tuned = model.probe_floor(hop)
    assert tuned < model.PROBE_FROM
    monkeypatch.setenv("MIGKIT_PROBE_FROM", "12345")
    assert model.probe_floor(hop) == model.PROBE_FROM


def test_the_resource_models_readings_are_a_wide_prior():
    import numpy as np
    mu = np.array([0.5, 0.01, 0, 0, 0, 0, 0.01])
    wide = cm._spread(mu, cm.MODEL_SHARE)
    narrow = cm._spread(mu)
    assert wide[1] == pytest.approx(0.01) and narrow[1] == pytest.approx(
        0.005)
    assert cm.MODEL_SHARE >= 2 * cm.PRIOR_SHARE


# --- the pace holds a target --------------------------------------------------

class _Clock:
    t = 0.0

    def __call__(self):
        return self.t


def _run(pace, clock, windows, per_worker=10.0):
    seen = []
    for _ in range(windows):
        clock.t += 1.0
        pace.done(weight=per_worker * pace.limit, seconds=1.0)
        seen.append(pace.limit)
    return seen


def test_the_pace_holds_the_hops_share_of_the_source():
    """The source's reads carry 100 a second, a worker 10: the hop allows
    30%, so three workers. Held as a target, the pace sits at three most
    of the time; a strain that only backs off at the same threshold spends
    most of it below."""
    clock = _Clock()
    held = sizing.Held("the move's share of the source's reads",
                       lambda h: h.pace.rate / 100.0 if h.pace.rate
                       else None, 0.3)
    pace = sizing.Pace(8, 16, clock=clock, resources={"share": held})
    seen = _run(pace, clock, 400)[50:]
    at = seen.count(3) / len(seen)
    assert at >= 0.6, at
    assert max(seen) <= 4

    clock2 = _Clock()
    box = {}
    pace2 = sizing.Pace(8, 16, clock=clock2, resources={
        "share": lambda: "busy" if box["p"].rate
        and box["p"].rate / 100.0 > 0.3 else None})
    box["p"] = pace2
    seen2 = _run(pace2, clock2, 400)[50:]
    assert seen2.count(3) / len(seen2) < at


def test_a_target_under_its_reading_asks_the_number_down_in_proportion():
    h = sizing.Held("lag", lambda h: 40.0, 20.0,
                    lambda x: f"{x:.0f}s of replica lag")
    got = h()
    assert got.factor == pytest.approx(0.5)
    assert str(got) == "40s of replica lag, past the 20s of replica lag" \
        " the hop allows"
    under = sizing.Held("lag", lambda h: 10.0, 20.0)
    assert under() is None
    assert under.room(2) is True and under.room(1) is True
    at = sizing.Held("lag", lambda h: 19.0, 20.0)
    assert at() is None and at.room(3) is False


def test_the_hops_limits_become_the_paces_targets(tmp_path):
    from migkit.model import Model, Stage
    hop = _hop(tmp_path, limits={"source_load": "25%", "replica_lag": 30})
    m = Model([Stage("source", "the source's reads", 50e6, 200e6,
                     "measured")], per_row=100.0, measured=True)

    class Health:
        lag_seconds = 45.0

    got = sizing.held(hop, None, "appdb", m, lambda e, s, d: Health())
    assert set(got) == {"the hop's share of the source",
                        "the source's replica lag"}
    assert got["the hop's share of the source"].target == 0.25
    lag = got["the source's replica lag"]
    assert lag().factor == pytest.approx(30 / 45)
    assert sizing.held(_hop(tmp_path), None, "appdb", m, None) == {}
    assert sizing.stage_of("the hop's share of the source") == "source"


def test_a_cut_is_not_floored_one_short_by_its_floating_point():
    lim = sizing.Limit("x", lambda: sizing.Strain("over", factor=0.3 / 0.4),
                       8)
    lim.update(4, 0.7)
    assert lim.value == 3
