"""A hop that runs on a schedule of its own (`migkit.schedule`): due once
a slot, a slot missed past its catch-up window skipped, never started
while the last run goes on, paused after failures in a row, retried within
its window and ended at its maximum - whatever fires it.
"""
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from migkit import schedule

ROOT = Path(__file__).resolve().parent.parent


def _at(text, zone="UTC"):
    import datetime
    from zoneinfo import ZoneInfo
    return datetime.datetime.fromisoformat(text).replace(
        tzinfo=ZoneInfo(zone)).timestamp()


def test_a_slot_is_due_once_and_not_after_its_window():
    spec = {"cron": "0 */6 * * *", "catchup": 1800}
    slot, _ = schedule.decide(spec, {}, _at("2026-09-27 06:10:00"))
    assert slot == _at("2026-09-27 06:00:00")
    assert schedule.decide(spec, {"last_slot": slot},
                           _at("2026-09-27 06:20:00")) == (
        None, "this slot has run")
    assert schedule.decide(spec, {}, _at("2026-09-27 06:45:00"))[1] == \
        "the slot was missed by more than its catch-up window"


def test_a_slot_is_read_in_the_hops_time_zone():
    spec = {"cron": "0 9 * * *", "timezone": "Asia/Bangkok"}
    slot, _ = schedule.decide(spec, {}, _at("2026-09-27 02:05:00"))
    assert slot == _at("2026-09-27 09:00:00", "Asia/Bangkok")


def test_a_run_still_going_or_failing_holds_the_next(monkeypatch):
    spec = {"cron": "* * * * *", "pause_after_failures": 2}
    now = time.time()
    assert schedule.decide(spec, {"running": os.getpid()}, now)[1] == \
        "the last slot's run is still going"
    assert schedule.decide(spec, {"running": 999999}, now)[0] is not None
    why = schedule.decide(spec, {"failures": 2}, now)[1]
    assert why.startswith("paused after 2 slots in a row failed"), why
    monkeypatch.setenv("MIGKIT_SCHEDULE_RESUME", "1")
    assert schedule.decide(spec, {"failures": 2}, now)[0] is not None


class _Hop:
    name = "sched"

    def __init__(self, tmp):
        self.tmp = tmp

    def report_dir(self, db=None):
        return self.tmp


def test_a_failed_run_is_tried_again_within_its_window(tmp_path):
    marks = tmp_path / "tries"
    cmd = [sys.executable, "-c",
           f"p = {str(marks)!r}; open(p, 'a').write('x');"
           " import sys; sys.exit(0 if len(open(p).read()) >= 3 else 1)"]
    slept = []
    ok = schedule.run(_Hop(tmp_path), {"retry_window": 3600,
                                       "max_duration": 3600}, argv=cmd,
                      sleeper=slept.append)
    assert ok and marks.read_text() == "xxx" and slept == [30.0, 60.0]
    # past its window it stops trying
    marks.unlink()
    ok = schedule.run(_Hop(tmp_path), {"retry_window": 0,
                                       "max_duration": 3600}, argv=cmd,
                      sleeper=slept.append)
    assert not ok and marks.read_text() == "x"


def test_a_run_past_its_maximum_is_ended(tmp_path):
    began = time.time()
    ok = schedule.run(_Hop(tmp_path), {"max_duration": 2}, argv=[
        sys.executable, "-c", "import time; time.sleep(60)"])
    assert not ok and time.time() - began < 30


def test_a_command_fired_every_minute_runs_once_a_slot(tmp_path):
    for name in ("a.db", "b.db"):
        conn = sqlite3.connect(tmp_path / name)
        conn.execute("create table t (id integer primary key, v text)")
        conn.execute("insert into t values (1, 'x')")
        conn.commit()
        conn.close()
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  sq:\n    engine: sqlite\n"
        f"    source: {{path: {tmp_path / 'a.db'}}}\n"
        f"    target: {{path: {tmp_path / 'b.db'}}}\n"
        "    databases: [main]\n"
        "    options:\n      schedule: {run: check, cron: '0 * * * *',"
        " catchup: 3600}\n")
    env = dict(os.environ, MIGKIT_CONF=str(tmp_path / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp_path / "reports"), COLUMNS="200",
               MIGKIT_SCHEDULED="1")

    def fire(*args):
        return subprocess.run([sys.executable, "-c",
                               "from migkit.cli import main; main()",
                               "check", "sq", *args], env=env, cwd=ROOT,
                              capture_output=True, text=True, timeout=300)
    first = fire()
    assert first.returncode == 0, first.stdout[-1500:] + first.stderr[-800:]
    assert "checking sq" in first.stdout
    state = json.loads((tmp_path / "reports" / "sq" /
                        "schedule.json").read_text())
    assert state["last_result"] == "ok" and "running" not in state
    again = fire()
    assert again.returncode == 0
    assert "not due - this slot has run" in again.stdout
    assert "checking sq" not in again.stdout
    # by hand, never held back
    env.pop("MIGKIT_SCHEDULED")
    assert "checking sq" in fire().stdout
