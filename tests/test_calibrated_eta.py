import time
import pytest
from migkit.util import Timer, CalibratedETA, ETA


def test_warmup_behavior():
    timer = CalibratedETA(t0=0.0)
    # Zero done
    assert timer.eta(0, 10) == "? (warmup)"
    res0 = timer.eta_detailed(0, 10)
    assert res0.formatted == "? (warmup)"
    assert res0.confidence < 0.5
    assert res0.seconds is None

    # Sample 1
    timer.update(1, timestamp=5.0)
    assert timer.eta(1, 10) == "? (warmup)"
    res1 = timer.eta_detailed(1, 10)
    assert res1.formatted == "? (warmup)"
    assert res1.confidence < 0.5

    # Sample 2
    timer.update(2, timestamp=10.0)
    assert timer.eta(2, 10) == "? (warmup)"
    res2 = timer.eta_detailed(2, 10)
    assert res2.formatted == "? (warmup)"
    assert res2.confidence < 0.5

    # Sample 3: warmup complete
    timer.update(3, timestamp=15.0)
    assert timer.eta(3, 10) != "? (warmup)"
    res3 = timer.eta_detailed(3, 10)
    assert "(warmup)" not in res3.formatted
    assert res3.seconds is not None
    assert 34.0 <= res3.seconds <= 36.0


def test_steady_throughput_estimation():
    timer = CalibratedETA(t0=0.0)
    # 1 item every 2 seconds -> rate = 0.5 items/sec
    for i in range(1, 7):
        timer.update(i, timestamp=i * 2.0)

    # 6 done out of 10 -> 4 remaining -> 4 / 0.5 = 8 seconds
    res = timer.eta_detailed(6, 10)
    assert res.seconds is not None
    assert pytest.approx(8.0, rel=0.05) == res.seconds
    assert res.confidence >= 0.75
    assert res.lower_bound <= res.seconds <= res.upper_bound
    assert timer.eta(6, 10) == "8s"


def test_varying_throughput_with_smoothing():
    timer = CalibratedETA(t0=0.0, alpha=0.3)
    # Fast phase: 4 items at 1 item/sec
    for i in range(1, 5):
        timer.update(i, timestamp=float(i))

    # Slow phase: 3 items at 0.2 item/sec (5 sec per item)
    t = 4.0
    for i in range(5, 8):
        t += 5.0
        timer.update(i, timestamp=t)

    # Total 12 items, 7 done, 5 remaining
    res = timer.eta_detailed(7, 12)
    # With smoothing, smoothed rate should be between 0.2 and 1.0
    expected_pure_slow_sec = 5 / 0.2  # 25s
    expected_pure_fast_sec = 5 / 1.0  # 5s
    assert expected_pure_fast_sec < res.seconds < expected_pure_slow_sec

    # Variance is high, so confidence interval widens and formatted contains bounds indicator
    assert res.lower_bound < res.seconds < res.upper_bound
    assert "~" in res.formatted
    assert "±" in res.formatted


def test_confidence_interval_bounds():
    # Steady timer
    steady = CalibratedETA(t0=0.0)
    for i in range(1, 10):
        steady.update(i, timestamp=i * 1.0)
    steady_eta = steady.eta_detailed(9, 20)

    # Noisy timer
    noisy = CalibratedETA(t0=0.0)
    t = 0.0
    delays = [0.1, 4.0, 0.2, 5.0, 0.1, 3.5, 0.2, 4.5, 0.1]
    for i, d in enumerate(delays, 1):
        t += d
        noisy.update(i, timestamp=t)
    noisy_eta = noisy.eta_detailed(9, 20)

    assert steady_eta.confidence > noisy_eta.confidence
    steady_spread = steady_eta.upper_bound - steady_eta.lower_bound
    noisy_spread = noisy_eta.upper_bound - noisy_eta.lower_bound
    assert steady_spread < noisy_spread
    assert noisy_eta.lower_bound <= noisy_eta.seconds <= noisy_eta.upper_bound
    assert noisy_eta.lower_bound >= 0.0


def test_remaining_work_weights_cost():
    timer = CalibratedETA(t0=0.0)
    # 3 chunks finished, each 100 MB took 2 seconds -> 50 MB/sec
    timer.update(1, cost=100.0, timestamp=2.0)
    timer.update(2, cost=200.0, timestamp=4.0)
    timer.update(3, cost=300.0, timestamp=6.0)

    # 500 MB remaining
    res = timer.eta_detailed(3, 5, remaining_cost=500.0)
    assert res.seconds is not None
    assert pytest.approx(10.0, rel=0.05) == res.seconds
    assert timer.eta(3, 5, remaining_cost=500.0) == "10s"


def test_backward_compatibility_timer_eta():
    t = Timer()
    assert isinstance(t.t0, float)
    assert t.elapsed() >= 0.0
    assert t.eta(0, 0) == "?"
    assert t.eta(5, 0) == "?"
    assert t.eta(10, 10) == "0s"
    assert isinstance(t.eta(1, 10), str)


def test_completion_and_zero_remaining():
    timer = CalibratedETA(t0=0.0)
    timer.update(10, timestamp=10.0)
    res = timer.eta_detailed(10, 10)
    assert res.seconds == 0.0
    assert res.confidence == 1.0
    assert res.lower_bound == 0.0
    assert res.upper_bound == 0.0
    assert res.formatted == "0s"

    res_cost = timer.eta_detailed(5, 10, remaining_cost=0)
    assert res_cost.seconds == 0.0
    assert res_cost.confidence == 1.0


def test_timer_auto_update_on_eta_call():
    timer = Timer(t0=0.0)
    # Direct calls to eta without update()
    assert timer.eta(1, 10, timestamp=2.0) == "? (warmup)"
    assert timer.eta(2, 10, timestamp=4.0) == "? (warmup)"
    # Third sample finishes warmup: rate is 0.5 items/sec
    eta_str = timer.eta(3, 10, timestamp=6.0)
    assert "(warmup)" not in eta_str
    # 7 remaining -> 14s
    assert eta_str == "14s"


def test_eta_structured_tuple():
    timer = CalibratedETA(t0=0.0)
    for i in range(1, 6):
        timer.update(i, timestamp=float(i))
    eta = timer.eta_detailed(5, 10)
    # Unpackable as namedtuple
    sec, lb, ub, conf, fmt = eta
    assert sec == eta.seconds
    assert lb == eta.lower_bound
    assert ub == eta.upper_bound
    assert conf == eta.confidence
    assert fmt == eta.formatted
    assert str(eta) == eta.formatted


def test_timer_edge_cases():
    timer = CalibratedETA(t0=100.0)
    # total is None or 0 or negative
    assert timer.eta(0, 0) == "?"
    assert timer.eta(5, -1) == "?"
    res_neg = timer.eta_detailed(5, -1)
    assert res_neg.seconds is None
    assert res_neg.formatted == "?"

    # negative remaining cost
    res_neg_cost = timer.eta_detailed(2, 10, remaining_cost=-5)
    assert res_neg_cost.seconds == 0.0
    assert res_neg_cost.formatted == "0s"

