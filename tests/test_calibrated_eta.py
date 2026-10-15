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
    # Unpackable as namedtuple: the first five fields keep their order
    sec, lb, ub, conf, fmt = eta[:5]
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


# ---- the work left (backlog P0-ETA-ALL) -------------------------------------
# When a task ends is told from the work left in the ledger, not elapsed
# time against phases done: rows and bytes left at the recent units' rate,
# the workers and the dependency chain counted once, retries and cooldowns
# in; a range, a confidence, when, evidence and scope beside the time; no
# time where the evidence holds none; a tail's catch-up kept apart and never
# promised while changes arrive faster than they are applied; the estimate
# and the finished units kept so a resumed run goes on from them.

from migkit import util  # noqa: E402
from migkit.engines.base import Engine  # noqa: E402
from migkit.throttle import Health  # noqa: E402
from migkit.util import (EstimateHistory, Unit, catch_up,  # noqa: E402
                         forecast, schedule)


def _finished(n, rows=1000, seconds=10.0, start=100.0, attempts=1):
    return [Unit(f"t{i}", rows=rows, seconds=seconds, state="done",
                 ended=start + seconds * (i + 1), attempts=attempts)
            for i in range(n)]


def test_parallel_work_is_counted_once_and_the_chain_bounds_it():
    assert schedule({k: 10.0 for k in "abcd"}, {}, 2) == (20.0, 10.0, 40.0)
    assert schedule({"a": 10.0, "b": 10.0, "c": 10.0},
                    {"b": ("a",), "c": ("b",)}, 8) == (30.0, 30.0, 30.0)
    assert schedule({"b": 5.0}, {"b": ("gone",)}, 1)[0] == 5.0


def test_a_cycle_does_not_hang_the_schedule():
    assert schedule({"a": 1.0, "b": 2.0}, {"a": ("b",), "b": ("a",)},
                    1)[0] == 3.0


def test_the_time_comes_from_rows_left_at_the_measured_rate():
    units = _finished(5) + [Unit(f"p{i}", rows=1000) for i in range(4)]
    got = forecast(units, kind="move", workers=2, now=200.0)
    assert got.basis == "measured"
    # 4000 rows at 100 rows a second a worker, two workers
    assert got.seconds == 20.0
    assert got.lower_bound < got.seconds < got.upper_bound
    assert got.active_seconds == 40.0
    assert got.known_left == {"units": 4, "rows": 4000.0, "bytes": 0,
                              "unmeasured_units": 0}
    assert got.measured_at.endswith("+00:00")
    assert got.scope == "the copy"
    assert any("2 at a time" in e for e in got.evidence)
    assert 0 < got.confidence <= 1


def test_a_unit_half_done_owes_only_its_rows_left():
    units = _finished(5) + [Unit("p", rows=1000, done_rows=500,
                                 state="running")]
    assert forecast(units, workers=1, now=160.0).seconds == 5.0


def test_retries_and_cooldowns_make_it_longer():
    left = [Unit(f"p{i}", rows=1000) for i in range(2)]
    plain = forecast(_finished(6) + left, workers=1, now=170.0)
    retried = forecast(_finished(6, attempts=2) + left, workers=1, now=170.0)
    cooled = forecast(_finished(6) + [left[0]._replace(cooldown=30.0),
                                      left[1]], workers=1, now=170.0)
    assert retried.seconds == plain.seconds * 2
    assert any("extra attempts" in e for e in retried.evidence)
    assert cooled.seconds == plain.seconds + 30.0
    assert any("cooldown" in e for e in cooled.evidence)


def test_warmup_says_insufficient_evidence_not_a_time():
    got = forecast(_finished(1) + [Unit("p", rows=1000)], now=120.0)
    assert got.basis == "insufficient"
    assert got.seconds is None and got.upper_bound is None
    assert "not enough evidence" in got.formatted
    assert "2 more units" in got.next_step
    assert forecast([Unit("p")], now=1.0).seconds is None


def test_an_earlier_runs_rate_is_read_only_where_needed():
    asked = []

    def kept():
        asked.append(1)
        return 50.0

    measured = forecast(_finished(5) + [Unit("p", rows=1000)], now=200.0,
                        kept_rate=kept)
    assert asked == [] and measured.basis == "measured"
    got = forecast([Unit("p", rows=1000)], now=1.0, kept_rate=kept)
    assert asked == [1]
    assert got.basis == "conditional" and got.seconds == 20.0
    assert got.lower_bound < got.seconds < got.upper_bound
    assert got.confidence <= 0.25


def test_unmeasured_work_is_told_apart_from_known_work():
    units = _finished(5) + [Unit("p", rows=1000), Unit("x")]
    got = forecast(units, workers=1, now=200.0)
    assert got.basis == "conditional"
    assert got.unmeasured == 1 and got.known_left["unmeasured_units"] == 1
    assert got.formatted.startswith("if the 1 units not yet measured")
    assert got.seconds == 20.0


def test_undiscovered_work_has_no_upper_bound():
    got = forecast(_finished(5) + [Unit("p", rows=1000)], workers=1,
                   now=200.0, undiscovered=True)
    assert got.upper_bound is None and got.undiscovered
    assert "at least" in got.formatted


def test_a_stall_is_said_not_smoothed_over():
    units = _finished(5) + [Unit("r", rows=1000, state="running")]
    got = forecast(units, now=200.0)
    assert got.basis == "stalled" and got.seconds is None
    assert "wait on" in got.next_step
    assert forecast(units, now=160.0).basis == "measured"


def test_a_changing_workload_lowers_the_confidence():
    fast = [Unit(f"f{i}", rows=1000, seconds=2.0, state="done",
                 ended=10.0 + i) for i in range(4)]
    slow = [Unit(f"s{i}", rows=1000, seconds=20.0, state="done",
                 ended=100.0 + 20 * i) for i in range(4)]
    left = [Unit("p", rows=1000)]
    steady = forecast(_finished(8) + left, workers=1, now=190.0)
    got = forecast(fast + slow + left, workers=1, now=190.0)
    assert any("workload is changing" in e for e in got.evidence)
    assert got.confidence < steady.confidence


def test_pressure_on_a_side_widens_the_range_and_says_why():
    units = _finished(6) + [Unit("p", rows=1000)]
    calm = forecast(units, workers=1, now=170.0, health=[Health(0.1)])
    busy = forecast(units, workers=1, now=170.0, health=[Health(0.99)])
    assert busy.upper_bound > calm.upper_bound
    assert busy.confidence < calm.confidence
    assert any("under pressure" in e for e in busy.evidence)


def test_catch_up_is_never_promised_while_ingest_outruns_apply():
    behind = catch_up(1000, ingest=50.0, applied=40.0, now=1.0)
    assert behind.basis == "behind" and behind.seconds is None
    assert "apply more room" in behind.next_step
    assert catch_up(1000, ingest=50.0, applied=50.0, now=1.0).seconds is None
    got = catch_up(1000, ingest=50.0, applied=60.0, now=1.0,
                   spread=(55.0, 58.0, 62.0, 65.0))
    assert got.seconds == 100.0
    assert got.lower_bound < 100.0 < got.upper_bound
    assert got.scope == "the change catch-up"
    assert catch_up(1000, 50.0, 60.0, now=1.0,
                    spread=(45.0, 70.0)).upper_bound is None
    assert catch_up(None, 1.0, 2.0, now=1.0).basis == "insufficient"


def test_a_resumed_run_goes_on_from_the_units_kept(tmp_path):
    history = EstimateHistory(tmp_path / "eta.jsonl")
    for u in _finished(5):
        history.done("s", u)
    left = [Unit(f"p{i}", rows=1000) for i in range(4)]
    got = forecast(left, scope="s", history=history, workers=2, now=200.0)
    assert got.basis == "measured" and got.seconds == 20.0
    assert history.last("s")["seconds"] == 20.0
    history.forget("s", ["t0"])
    assert sorted(u.name for u in history.finished("s")) == \
        ["t1", "t2", "t3", "t4"]


def test_a_revision_and_a_stale_estimate_are_said(tmp_path):
    history = EstimateHistory(tmp_path / "eta.jsonl", stale_after=60.0)
    for u in _finished(5):
        history.done("s", u)
    left = [Unit(f"p{i}", rows=1000) for i in range(4)]
    first = forecast(left, scope="s", history=history, workers=4, now=200.0)
    assert first.revised == "" and not first.stale
    later = forecast(left, scope="s", history=history, workers=1, now=201.0)
    assert later.revised.startswith("revised later by")
    old = forecast(left, scope="s", history=history, workers=1, now=900.0)
    assert old.stale
    assert any("measured again" in e for e in old.evidence)


def test_an_earlier_time_of_a_unit_owed_again_is_rate_not_progress(tmp_path):
    history = EstimateHistory(tmp_path / "eta.jsonl")
    for u in _finished(4):
        history.done("s", u)
    got = forecast([Unit(f"t{i}", rows=1000) for i in range(4)], scope="s",
                   history=history, workers=1, now=200.0)
    assert got.known_left["units"] == 4
    assert got.seconds == 40.0


class _Hop:
    name = "fake"


class _Listed(Engine):
    def __init__(self, tables=None):
        self.hop, self.tables = _Hop(), tables

    def neutral_tables(self, side, db):
        if self.tables is None:
            raise RuntimeError("no listing")
        return self.tables

    def in_scope(self, db, table):
        return table != "c"

    def _rows_of(self, side, db, table):
        return {"a": 1000}.get(table)


def test_every_task_kind_goes_through_the_engine_contract(monkeypatch):
    import migkit.planner as planner
    kept = {}
    monkeypatch.setattr(planner, "measured_rate",
                        lambda hop, path: kept.get(path))
    monkeypatch.setattr(planner, "record_rate",
                        lambda hop, path, rows, seconds, **k:
                        kept.__setitem__(path, rows / seconds))
    eng = _Listed(["a", "b", "c"])
    for kind in ("move", "check", "verify", "deep"):
        got = eng.forecast(kind, "db1", now=1.0)
        assert got.scope == f"{util.TASK_KINDS[kind]} of db1"
        assert got.known_left["units"] == 2
        assert got.known_left["unmeasured_units"] == 1
        assert got.basis == "insufficient"
    assert eng.forecast("move", "db1", units=_finished(3),
                        now=200.0).basis == "done"
    assert kept["eta.move"] == 100.0
    again = eng.forecast("move", "db1", units=[Unit("a", rows=500)], now=1.0)
    assert again.basis == "conditional" and again.seconds == 5.0
    assert eng.forecast("follow", "db1", backlog=10, ingest=5.0, applied=1.0,
                        now=1.0).basis == "behind"
    lost = _Listed(None).forecast("check", "db1", now=1.0)
    assert lost.undiscovered and lost.seconds is None

