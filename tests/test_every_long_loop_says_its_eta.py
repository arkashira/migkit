"""Every long loop says its remaining time through the one forecast
(backlog P0-ETA-ALL): the move's progress, the tail's catch-up, the deep
and verify checks and the report - each held to the attempt ledger, so a
unit that has made no progress for long is said stalled. A table, a
partition or a range is a unit of its own, not its statements; estimates
are held to how their task really ended; a sick side widens the range."""
import threading
import time
from types import SimpleNamespace

import pytest

from migkit import attempts, cli, planner, report
from migkit.engines.base import Engine
from migkit.util import (ETA, EstimateHistory, Unit, forecast, human_secs,
                         merge_units)


class Clock:
    def __init__(self, t=100.0):
        self.t = t

    def __call__(self):
        return self.t


def ledger_with(units, stuck=(), wall0=1_000_000.0):
    """A ledger whose `units` each ran 10 s one after another, then the
    `stuck` ones began and never progressed; times on a monotonic clock
    whose zero is `wall0` on the wall."""
    clock = Clock(0.0)
    mon = attempts.Monitor(None, attempts.Ledger(), clock=clock,
                           thread=False, wall=lambda: wall0 + clock.t)
    for name in units:
        a = mon.begin("src", "copy", name)
        clock.t += 10.0
        mon.done(a, True)
    for name in stuck:
        mon.begin("src", "copy", name)
    return mon.ledger, wall0 + clock.t


def test_the_ledger_times_each_unit_and_says_how_long_a_running_one_waits():
    ledger, now = ledger_with(["a", "b"], stuck=["c"])
    got = {u.name: u for u in ledger.units("move", now=now + 50.0)}
    assert got["a"].state == "done" and got["a"].seconds == 10.0
    assert got["b"].ended == now
    assert got["c"].state == "running" and got["c"].idle == 50.0
    assert ledger.units("move", operation="other") == []


def test_a_failed_attempt_leaves_its_unit_owed_and_an_old_record_untimed():
    clock = Clock(0.0)
    mon = attempts.Monitor(None, attempts.Ledger(), clock=clock,
                           thread=False, wall=lambda: 5.0 + clock.t)
    mon.done(mon.begin("src", "copy", "x"), False, "connection lost")
    (u,) = mon.ledger.units()
    assert u.state == "failed"
    mon.ledger.attempts["old"] = {"unit": "y", "stage": "done"}
    assert [u.name for u in mon.ledger.units()] == ["x"]


def test_listed_units_take_their_attempts_times_and_keep_their_rows():
    listed = [Unit("a", rows=100.0), Unit("b", rows=50.0)]
    measured = [Unit("a", state="done", seconds=10.0, ended=1.0),
                Unit("z", state="running", idle=3.0)]
    got = {u.name: u for u in merge_units(listed, measured)}
    assert got["a"].rows == 100.0 and got["a"].done_rows == 100.0
    assert got["a"].state == "done" and got["a"].seconds == 10.0
    assert got["b"].state == "pending"
    assert got["z"].idle == 3.0


def test_every_running_unit_stalled_says_stalled_and_no_time():
    ledger, now = ledger_with(["a", "b", "c", "d"], stuck=["e"])
    units = merge_units([Unit(n) for n in "abcde"],
                        ledger.units(now=now + 400.0))
    got = forecast(units, now=now + 400.0)
    assert got.basis == "stalled" and got.seconds is None
    assert got.formatted.startswith("stalled: e made no progress")
    assert "look at what the stalled units wait on" in got.operator_line()


def test_one_stalled_unit_of_two_lowers_the_confidence_and_opens_the_range():
    ledger, now = ledger_with(["a", "b", "c", "d"], stuck=["e", "f"])
    units = ledger.units(now=now + 1.0)
    calm = forecast(units, now=now + 1.0)
    # e waits long, f has just made progress
    units = [u._replace(idle=400.0) if u.name == "e" else u for u in units]
    got = forecast(units, now=now + 1.0)
    assert calm.basis == "measured" and calm.upper_bound is not None
    assert got.basis == "conditional" and got.upper_bound is None
    assert got.confidence < calm.confidence
    assert got.formatted.endswith("1 stalled")
    assert any(ev.endswith(": e") and "made no progress" in ev
               for ev in got.evidence)


def test_warming_up_says_not_enough_evidence_and_what_to_wait_for():
    ledger, now = ledger_with(["a"], stuck=["b"])
    got = forecast(ledger.units(now=now) + [Unit("c")], now=now)
    line = got.operator_line()
    assert line.startswith("not enough evidence yet: 1 of 3")
    assert "wait for 2 more units to finish" in line


def pace_of(rows, rate, seconds):
    return SimpleNamespace(recent=rate, total=rows, took=lambda: seconds,
                           _clock=lambda: seconds, started=0.0,
                           held_by=lambda: None, limit=1,
                           history=[(0.0, 0)])


def test_the_move_progress_line_is_the_forecast(tmp_path):
    ledger, now = ledger_with(["s.a", "s.b", "s.c"])
    units = merge_units([Unit(n, rows=1000.0) for n in
                         ("s.a", "s.b", "s.c", "s.d", "s.e")],
                        ledger.units(now=now))
    planner.progress(tmp_path, pace_of(3000, 100.0, 30.0), 5000,
                     eta=lambda: forecast(units, workers=1, now=now))
    p = planner.read_progress(tmp_path)
    assert p["eta"]["basis"] == "measured" and p["eta"]["seconds"] == 20.0
    hop = SimpleNamespace(report_dir=lambda *a: tmp_path)
    (said,) = [s for s in cli._move_said(hop, "d") if s.startswith("move")]
    assert f"the copy left: {human_secs(20.0)} (" in said
    assert "% confidence" in said and "rows/s" not in said


def test_the_move_progress_is_kept_when_the_estimate_cannot_be_read(tmp_path):
    def broken():
        raise RuntimeError("no catalogue")
    planner.progress(tmp_path, pace_of(10, 10.0, 1.0), 100, eta=broken)
    p = planner.read_progress(tmp_path)
    assert "eta" not in p and p["eta_copy"] == 9.0


class FollowEngine:
    forecast = Engine.forecast


def catch_up_after(arrived, applied, behind):
    clock = Clock(0.0)
    seen = {"arrived": arrived[0], "applied": applied[0]}
    ahead = planner.CatchUp(lambda: seen["arrived"], lambda: seen["applied"],
                            behind=lambda: behind, clock=clock)
    ahead.sample()
    clock.t = 10.0
    seen.update(arrived=arrived[1], applied=applied[1])
    ahead.sample()
    return ahead


def test_the_tail_says_falling_behind_where_arrivals_outrun_the_apply():
    ahead = catch_up_after((0, 500), (0, 200), 9000)
    said = cli._follow_said(FollowEngine(), "d", ahead)
    assert said.startswith("catch-up: falling behind")
    assert "give the apply more room" in said


def test_the_tail_says_its_catch_up_time_where_the_apply_gains():
    ahead = catch_up_after((0, 100), (0, 300), 4000)
    said = cli._follow_said(FollowEngine(), "d", ahead)
    # 4000 behind, gaining 30 - 10 = 20 a second
    assert said.startswith(f"catch-up: {human_secs(200.0)} (")
    assert "% confidence" in said and "the tail is catching up" in said


def test_no_tail_says_no_catch_up():
    ahead = planner.CatchUp(lambda: None, lambda: None)
    ahead.sample()
    assert cli._follow_said(FollowEngine(), "d", ahead) == ""


def test_the_checks_loop_names_its_task_by_what_it_runs():
    assert cli._checks_kind(["counts", "data"]) == "verify"
    assert cli._checks_kind(["counts", "deep"]) == "deep"
    assert cli._checks_kind(["counts", "schema"]) == "check"


def test_the_report_says_the_time_left_of_each_task(tmp_path):
    history = EstimateHistory(tmp_path / "eta-history.jsonl")
    units = [Unit(n, state="done", seconds=5.0, ended=float(i))
             for i, n in enumerate("abc")] + [Unit("d")]
    forecast(units, kind="verify", scope="the verification of d",
             history=history, now=10.0)
    hop = SimpleNamespace(report_dir=lambda *a: tmp_path)
    got = report._time_left_section(hop)
    assert "time left" in got and "the verification of d" in got
    assert "% confidence" in got and "stale" in got
    assert report._time_left_section(
        SimpleNamespace(report_dir=lambda *a: tmp_path / "none")) == ""


# --- the unit level, the accuracy gate, the tail's own line, side health ---

def monitor(wall0=1_000_000.0, path=None):
    clock = Clock(0.0)
    mon = attempts.Monitor(None, attempts.Ledger(path), clock=clock,
                           thread=False, wall=lambda: wall0 + clock.t)
    return mon, clock


def test_a_table_between_two_of_its_statements_is_running_not_done():
    mon, clock = monitor()
    with mon.unit("s.a"):
        mon.done(mon.begin("src", "the copy", "s.a"), True)
        clock.t += 5.0
        (u,) = mon.ledger.units("move", now=1_000_005.0)
        # every statement so far ended well, and still the table is not done
        assert u.state == "running" and u.idle is None
        a = mon.begin("src", "the copy", "s.a")
        clock.t += 7.0
        (u,) = mon.ledger.units("move", now=1_000_012.0)
        # idle since the statement in flight was sent, at 5 s
        assert u.state == "running" and u.idle == 7.0
        mon.done(a, True)
    (u,) = mon.ledger.units("move")
    assert u.state == "done" and u.seconds == 12.0 and u.attempts == 1


def test_the_next_statement_after_a_good_one_is_no_retry():
    mon, _ = monitor()
    mon.done(mon.begin("src", "the copy", "s.a"), True)
    assert mon.begin("src", "the copy", "s.a").retries == 0
    mon.done(mon.begin("src", "the copy", "s.b"), False, "OperationalError")
    assert mon.begin("src", "the copy", "s.b").retries == 1


def test_a_failing_unit_is_owed_again_and_keeps_only_the_class():
    mon, _ = monitor()
    with pytest.raises(RuntimeError):
        with mon.unit("s.a"):
            raise RuntimeError("select secret from t")
    (u,) = mon.ledger.units("move")
    assert u.state == "failed"
    assert mon.ledger.records["s.a"]["reason"] == "RuntimeError"
    with mon.unit("s.a"):
        pass
    (u,) = mon.ledger.units("move")
    assert u.state == "done" and u.attempts == 2


def test_a_unit_an_earlier_run_left_open_is_owed_again(tmp_path):
    path = tmp_path / "attempts.json"
    first, _ = monitor(path=path)
    first.ledger.unit_opened("s.a", at=10.0)
    first.begin("src", "the copy", "s.a")
    again = attempts.Ledger(path)
    (u,) = again.units("move", now=20.0)
    # the earlier run's open table and statement were cut off, not running
    assert u.state == "failed" and u.idle is None
    again.unit_opened("s.a", at=30.0)
    assert again.records["s.a"]["opens"] == 2


def test_a_unit_cut_into_parts_rolls_up_with_the_share_closed():
    led = attempts.Ledger()
    led.unit_opened("s.t", parts=4, at=0.0)
    for i in (1, 2):
        led.unit_opened(f"s.t#{i}", level="range", parent="s.t", at=float(i))
        led.unit_closed(f"s.t#{i}", True, at=float(i) + 1.0)
    led.unit_opened("s.t#3", level="range", parent="s.t", at=4.0)
    (u,) = led.units("move", now=5.0)
    assert u.name == "s.t" and u.state == "running" and u.share == 0.5
    (got,) = merge_units([Unit("s.t", rows=1000.0)], [u])
    assert got.done_rows == 500.0 and got.state == "running"
    # parts all closed while the table is open: not done until it closes
    led.unit_closed("s.t#3", True, at=6.0)
    assert led.units("move", now=7.0)[0].state == "running"
    led.unit_closed("s.t", True, at=8.0)
    (u,) = led.units("move", now=9.0)
    assert u.state == "done" and u.seconds == 8.0


def test_a_unit_of_another_task_is_not_this_ones():
    led = attempts.Ledger()
    led.unit_opened("shop", kind="verify", at=0.0)
    led.unit_opened("s.a", kind="move", at=0.0)
    assert [u.name for u in led.units("move")] == ["s.a"]
    got = merge_units([Unit("s.a"), Unit("s.b")],
                      [Unit("shop", "verify", state="running"),
                       Unit("s.c", "", state="running")])
    assert [u.name for u in got] == ["s.a", "s.b"]


class UnitEngine:
    attempt_unit = Engine.attempt_unit
    unit_now = Engine.unit_now
    work_unit = Engine.work_unit
    session_health = Engine.session_health

    def __init__(self, mon):
        self.mon = mon

    def attempts(self):
        return self.mon


def test_a_work_unit_holds_its_statements_and_its_record():
    mon, _ = monitor()
    eng = UnitEngine(mon)
    with eng.work_unit("move", "s.a"):
        assert eng.unit_now() == ("the copy", "s.a", "")
        assert mon.ledger.records["s.a"]["state"] == "open"
    assert eng.unit_now() is None
    assert mon.ledger.records["s.a"]["state"] == "done"


def scored(history, scope, said_at, ends_at, conf=0.9):
    """Estimates of 10 s (8 to 12) said at `said_at`, the task then found
    done at `ends_at`."""
    for t in said_at:
        history.keep(ETA(10.0, 8.0, 12.0, conf, "10s", basis="measured",
                         scope=scope), t)
    history.keep(ETA(0.0, 0.0, 0.0, 1.0, "0s", basis="done", scope=scope),
                 ends_at)


def test_a_finished_task_scores_the_estimates_said_of_it(tmp_path):
    history = EstimateHistory(tmp_path / "eta.jsonl")
    scored(history, "the copy of a", (0.0, 1.0, 2.0), 11.0)
    cal = history.calibration("move")
    assert cal["n"] == 3 and cal["inside"] == 3 and cal["coverage"] == 1.0
    assert round(cal["stated"], 2) == 0.9
    scored(history, "the copy of b", (100.0, 101.0, 102.0), 300.0)
    cal = history.calibration("move")
    # only the second task's three are new; the first's are not scored twice
    assert cal["n"] == 6 and cal["inside"] == 3
    assert history.calibration("verify") is None
    assert set(history.calibrations()) == {"move"}


def test_a_task_scores_at_most_a_few_estimates_spread_over_its_run(tmp_path):
    from migkit.util import SCORED_A_TASK
    history = EstimateHistory(tmp_path / "eta.jsonl")
    scored(history, "the copy of a", [float(i) for i in range(35)], 40.0)
    assert history.calibration("move", last=100)["n"] <= SCORED_A_TASK


def test_estimates_that_missed_their_range_lower_the_next_confidence(
        tmp_path):
    units = [Unit(f"t{i}", rows=1000, seconds=10.0, state="done",
                  ended=100.0 + 10 * i) for i in range(6)]
    units.append(Unit("p", rows=1000))
    fresh = forecast(units, workers=1, now=170.0, scope="the copy of x",
                     history=EstimateHistory(tmp_path / "fresh.jsonl"))
    missed = EstimateHistory(tmp_path / "missed.jsonl")
    scored(missed, "the copy of y", (0.0, 1.0, 2.0), 90.0)
    got = forecast(units, workers=1, now=170.0, scope="the copy of x",
                   history=missed)
    assert got.confidence < fresh.confidence
    assert got.upper_bound > fresh.upper_bound
    assert any("ended inside their range 0 times" in e
               for e in got.evidence)
    held = EstimateHistory(tmp_path / "held.jsonl")
    scored(held, "the copy of y", (0.0, 1.0, 2.0), 10.0, conf=0.5)
    kept = forecast(units, workers=1, now=170.0, scope="the copy of x",
                    history=held)
    assert kept.confidence == fresh.confidence
    assert not any("ended inside their range" in e for e in kept.evidence)


def test_a_loop_that_asks_often_keeps_a_short_journal(tmp_path):
    path = tmp_path / "eta.jsonl"
    history = EstimateHistory(path, keep_every=30.0)
    for t in (0.0, 1.0, 2.0):
        history.keep(ETA(10.0 - t, 8.0, 12.0, 0.9, "x", basis="measured",
                         scope="the copy of a"), t)
    history.keep(ETA(0.0, 0.0, 0.0, 1.0, "0s", basis="done",
                     scope="the copy of a"), 3.0)
    lines = path.read_text().splitlines()
    assert len([ln for ln in lines if '"estimate"' in ln]) == 2


def test_the_report_says_how_the_estimates_held(tmp_path):
    history = EstimateHistory(tmp_path / "eta-history.jsonl")
    scored(history, "the copy of d", (0.0, 1.0, 2.0), 90.0)
    hop = SimpleNamespace(report_dir=lambda *a: tmp_path)
    got = report._time_left_section(hop)
    assert "the copy: 0 of the last 3 estimates ended inside their range" \
        in got
    assert "new estimates say less" in got


def test_the_running_tail_says_its_own_catch_up_line(tmp_path):
    ahead = catch_up_after((0, 100), (0, 300), 4000)
    hop = SimpleNamespace(report_dir=lambda *a: tmp_path)
    said = []
    with cli._tail_said(hop, FollowEngine(), "d", said.append, every=0.01,
                        ahead=ahead):
        deadline = time.time() + 5.0
        while not said and time.time() < deadline:
            time.sleep(0.01)
        time.sleep(0.05)
    assert said and said[0].startswith(f"  d: catch-up: {human_secs(200.0)}")
    # the same line is not said again while nothing changes
    assert len(said) == 1
    assert (tmp_path / "eta-history.jsonl").exists()


def test_a_tail_with_nothing_measured_says_nothing_and_goes_on(tmp_path):
    ahead = planner.CatchUp(lambda: None, lambda: None)
    hop = SimpleNamespace(report_dir=lambda *a: tmp_path)
    said, ran = [], threading.Event()
    with cli._tail_said(hop, FollowEngine(), "d", said.append, every=0.01,
                        ahead=ahead):
        time.sleep(0.05)
        ran.set()
    assert ran.is_set() and said == []


def test_a_move_estimate_that_fails_is_said_with_its_reason(tmp_path):
    def broken():
        raise RuntimeError("no catalogue\nsecond line")
    planner.progress(tmp_path, pace_of(10, 10.0, 1.0), 100, eta=broken)
    p = planner.read_progress(tmp_path)
    assert p["eta_error"] == "RuntimeError: no catalogue"
    hop = SimpleNamespace(report_dir=lambda *a: tmp_path)
    (said,) = [s for s in cli._move_said(hop, "d") if s.startswith("move")]
    assert "no time estimate: RuntimeError: no catalogue" in said
    assert "rows/s" in said


def sick_ledger():
    mon, clock = monitor()
    for i in range(4):
        mon.done(mon.begin("src", "the copy", f"s.{i}"), True)
    lost = mon.begin("src", "the copy", "s.lost")
    lost.closes = 1
    mon.done(lost, False, "OperationalError")
    mon.done(mon.begin("src", "the copy", "s.bare",
                       transport={"error": "keepalive unreadable"}), True)
    mon.done(mon.begin("dst", "the copy", "d.0"), True)
    return mon


def test_a_sick_side_says_why_from_its_sessions():
    mon = sick_ledger()
    src, dst = UnitEngine(mon).session_health()
    why = src.stressed()
    assert "1 of its last 6 answers were lost" in why
    assert "keepalives could not be set on 1" in why
    assert why.startswith("the source's sessions")
    assert dst.stressed() == ""
    assert src.widen > 1.5 and src.trust < 0.7


def test_a_sick_side_widens_the_move_estimate_and_says_why():
    units = [Unit(f"t{i}", rows=1000, seconds=10.0, state="done",
                  ended=100.0 + 10 * i) for i in range(6)]
    units.append(Unit("p", rows=1000))
    health = UnitEngine(sick_ledger()).session_health()
    calm = forecast(units, workers=1, now=170.0)
    sick = forecast(units, workers=1, now=170.0, health=health)
    assert sick.upper_bound > calm.upper_bound
    assert sick.confidence < calm.confidence
    assert any("the source's sessions" in e and "range opened" in e
               for e in sick.evidence)
