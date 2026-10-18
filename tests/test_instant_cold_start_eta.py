"""Zero-warmup instant cold-start ETA (FIELD-OPS04).

Measured in batch1-mysql.data.log: migkit reported 'eta not enough evidence yet:
1 of 3 units measured' across 50% of the run because the task had only 2 units,
leaving operators blind. `forecast` now scales effective warmup to min(warmup, len(units))
and supports instant cold-start ETA (eta.initial) upon first unit completion.
"""
from migkit.util import Unit, forecast, human_secs
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("util", "instant_eta_rungs", "byte_weighted_ewma_converged", "holds"),
    ("util", "instant_eta_rungs", "byte_weighted_ewma_converged", "refuses"),
    ("util", "instant_eta_rungs", "cold_start_initial_unit_projection", "holds"),
    ("util", "instant_eta_rungs", "cold_start_initial_unit_projection", "refuses"),
    ("util", "instant_eta_rungs", "p90_confidence_interval_bounded", "holds"),
    ("util", "instant_eta_rungs", "p90_confidence_interval_bounded", "refuses"),
    ("util", "instant_eta_rungs", "historical_rate_bootstrap", "holds"),
    ("util", "instant_eta_rungs", "historical_rate_bootstrap", "refuses"),
    ("util", "instant_eta_rungs", "refuse_insufficient_sample_extrapolation", "holds"),
    ("util", "instant_eta_rungs", "refuse_insufficient_sample_extrapolation", "refuses"),
]


def test_instant_eta_rungs_hold_and_refuse():
    from migkit import util

    with watched() as seen:
        c = util.decide_instant_eta({"measured": 5, "warmup": 3, "has_byte_rates": True})
    assert c.rung.name == "byte_weighted_ewma_converged"
    assert branch(seen, "byte_weighted_ewma_converged") == [HOLDS]

    with watched() as seen:
        c = util.decide_instant_eta({"measured": 1, "warmup": 3, "has_byte_rates": False, "instant": True})
    assert branch(seen, "byte_weighted_ewma_converged") == [REFUSES]
    assert c.rung.name == "cold_start_initial_unit_projection"
    assert branch(seen, "cold_start_initial_unit_projection") == [HOLDS]

    with watched() as seen:
        c = util.decide_instant_eta({"measured": 0, "warmup": 3, "instant": False, "lo_seconds": 10.0, "hi_seconds": 20.0, "confidence": 0.5})
    assert branch(seen, "cold_start_initial_unit_projection") == [REFUSES]
    assert c.rung.name == "p90_confidence_interval_bounded"
    assert branch(seen, "p90_confidence_interval_bounded") == [HOLDS]

    with watched() as seen:
        c = util.decide_instant_eta({"measured": 0, "warmup": 3, "instant": False, "kept_rate": 1000.0})
    assert branch(seen, "p90_confidence_interval_bounded") == [REFUSES]
    assert c.rung.name == "historical_rate_bootstrap"
    assert branch(seen, "historical_rate_bootstrap") == [HOLDS]

    with watched() as seen:
        c = util.decide_instant_eta({"measured": 0, "warmup": 3, "instant": False})
    assert branch(seen, "historical_rate_bootstrap") == [REFUSES]
    assert c.rung.name == "refuse_insufficient_sample_extrapolation"
    assert branch(seen, "refuse_insufficient_sample_extrapolation") == [HOLDS]

    with watched() as seen:
        c = util.decide_instant_eta({"measured": 10, "warmup": 3, "has_byte_rates": False, "instant": False, "lo_seconds": None})
    assert branch(seen, "refuse_insufficient_sample_extrapolation") == [REFUSES]



def test_two_unit_task_gives_eta_after_first_unit():
    # A task with 2 databases: after unit 1 finishes, unit 2 should be forecast immediately!
    units = [
        Unit("db1", state="done", seconds=20.0, rows=1000.0, ended=100.0),
        Unit("db2", state="pending", rows=1000.0),
    ]
    got = forecast(units, kind="check", workers=1, now=100.0)
    assert got.seconds is not None, f"Expected calculated ETA seconds, got None: {got.formatted}"
    assert got.basis in ("measured", "initial")
    assert 10.0 <= got.seconds <= 30.0
    assert "left" in got.operator_line()


def test_instant_flag_provides_cold_start_eta_with_one_measured_unit():
    # Multi-unit task with instant=True: first unit provides initial estimate
    units = [
        Unit("u1", state="done", seconds=15.0, rows=500.0, ended=50.0),
        Unit("u2", state="pending", rows=500.0),
        Unit("u3", state="pending", rows=500.0),
        Unit("u4", state="pending", rows=500.0),
    ]
    got = forecast(units, kind="check", workers=1, now=50.0, instant=True)
    assert got.seconds is not None, f"Expected cold-start ETA, got {got.formatted}"
    assert got.confidence >= 0.2
    assert any("stands on the first measured units of this run" in ev for ev in got.evidence)
