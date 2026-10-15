"""Every long loop says its remaining time through the one forecast
(backlog P0-ETA-ALL): the move's progress, the tail's catch-up, the deep
and verify checks and the report - each held to the attempt ledger, so a
unit that has made no progress for long is said stalled."""
from types import SimpleNamespace

from migkit import attempts, cli, planner, report
from migkit.engines.base import Engine
from migkit.util import (EstimateHistory, Unit, forecast, human_secs,
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
