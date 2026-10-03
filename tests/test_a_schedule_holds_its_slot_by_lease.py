"""A schedule gates every command once, holds its slot by a lease, counts
a difference found as work done, and keeps going when one hop cannot
(audit K4, K5, K6, V6).

Before: only `check` and `move` gated, inside themselves, so `sync --mode
verify` ran a check whose gate ended it silently with 0 - compared nothing,
said nothing; `MIGKIT_SCHEDULED=false` counted as on; the slot's liveness
was a process number, and in a container every run is process 1; the
scheduled check counted exit 1 as done while `report --serve`'s scheduler
counted it a failure and paused the schedule after three differences; and
the first hop whose cron did not parse ended the scheduler for every hop.
"""
import os
import sqlite3
import threading
import time

import pytest

from migkit import schedule, verdict


class _Hop:
    def __init__(self, tmp, name="sched", spec=None):
        self.tmp, self.name = tmp, name
        self.options = {"schedule": spec} if spec else {}

    def report_dir(self, db=None):
        return self.tmp


EVERY_MINUTE = {"cron": "* * * * *", "catchup": 3600}


def test_a_slot_is_held_by_its_lease_not_by_a_process_number(tmp_path):
    hop = _Hop(tmp_path / "a")
    said = []
    first = schedule.take(hop, EVERY_MINUTE, said.append)
    assert first is not None
    # a second firing in the same minute - another pod - does not run it
    assert schedule.take(hop, EVERY_MINUTE, said.append) is None
    assert said[-1].endswith("still going"), said
    first.finished(True)
    # the slot has run: the next firing in it says so
    assert schedule.take(hop, EVERY_MINUTE, said.append) is None
    assert said[-1].endswith("this slot has run"), said
    # the record names who ran it, and no process number decides anything:
    # a stale one that happens to be alive held every later slot
    state = schedule._state(hop.report_dir() / schedule.STATE)
    assert state["host"] and state["pid"] == os.getpid()
    assert "running" not in state
    assert schedule.decide(EVERY_MINUTE, {"running": 1}, time.time())[0] \
        is not None


def test_a_difference_found_is_work_done_and_is_not_run_again(tmp_path):
    import sys
    tries = tmp_path / "tries"
    cmd = [sys.executable, "-c",
           f"open({str(tries)!r}, 'a').write('x'); import sys;"
           f" sys.exit({verdict.VERDICTS['different'].exit})"]
    slept = []
    ok = schedule.run(_Hop(tmp_path), {"retry_window": 3600,
                                       "max_duration": 3600}, argv=cmd,
                      sleeper=slept.append)
    assert ok and tries.read_text() == "x" and slept == []
    # could not compare: a failure, tried again
    cmd[-1] = cmd[-1].replace(
        f"sys.exit({verdict.VERDICTS['different'].exit})",
        f"sys.exit({verdict.EXIT['failed']})")
    tries.unlink()
    ok = schedule.run(_Hop(tmp_path), {"retry_window": 0,
                                       "max_duration": 3600}, argv=cmd,
                      sleeper=slept.append)
    assert not ok


@pytest.fixture
def sq(tmp_path, monkeypatch):
    import migkit.config as cfg
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key, v text)")
        con.execute("insert into t values (1, 'x')")
        con.commit()
        con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  sq:\n    engine: sqlite\n"
        f"    source: {{path: {tmp_path / 'a.db'}}}\n"
        f"    target: {{path: {tmp_path / 'b.db'}}}\n"
        "    databases: [main]\n"
        "    options:\n      schedule: {run: check, cron: '* * * * *',"
        " catchup: 3600}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _run(*argv):
    from click.testing import CliRunner

    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, got.output


def test_every_command_is_gated_once_and_says_why_not(sq, monkeypatch):
    """`sync --mode verify` ran; the check it invoked ended it with 0 and
    no word, and the verify read as passed."""
    monkeypatch.setenv(schedule.SLOT_ENDS, "")
    monkeypatch.setenv("MIGKIT_SCHEDULED", "1")
    got, said = _run("sync", "sq", "--mode", "verify")
    assert got.exit_code == 0, said
    assert "not due - its schedule runs check, not sync" in said, said
    assert "checking sq" not in said, said
    # the check its schedule runs is run, once, and the slot kept
    got, said = _run("check", "sq")
    assert got.exit_code == 0 and "checking sq" in said, said
    state = schedule._state(sq / "reports" / "sq" / schedule.STATE)
    assert state["last_result"] == "ok", state
    got, said = _run("check", "sq")
    assert "not due - this slot has run" in said, said


def test_a_variable_that_says_off_is_off(sq, monkeypatch):
    monkeypatch.setenv(schedule.SLOT_ENDS, "")
    monkeypatch.setenv("MIGKIT_SCHEDULED", "1")
    got, said = _run("check", "sq")
    assert "checking sq" in said, said
    # the slot has run; "false" is a command run by hand, never held back
    monkeypatch.setenv("MIGKIT_SCHEDULED", "false")
    got, said = _run("check", "sq")
    assert "checking sq" in said and "not due" not in said, said
    monkeypatch.setenv("MIGKIT_SCHEDULED", "maybe")
    got, said = _run("check", "sq")
    assert got.exit_code == verdict.EXIT["usage"], said


def test_one_hop_s_mistake_leaves_the_others_running(tmp_path):
    ran, both = [], threading.Barrier(2, timeout=10)

    def runner(hop, spec):
        ran.append(hop.name)
        # each hop on its own: two slots run side by side, where one ran
        # after the other and a long one made the next miss its window
        both.wait()
        return True
    hops = {"bad": _Hop(tmp_path / "bad", "bad", {"cron": "61 * * * *"}),
            "one": _Hop(tmp_path / "one", "one", EVERY_MINUTE),
            "two": _Hop(tmp_path / "two", "two", EVERY_MINUTE)}
    stop, said = threading.Event(), []
    t = threading.Thread(target=schedule.loop,
                         args=(lambda: hops, stop, said.append, 0.05,
                               runner), daemon=True)
    t.start()
    end = time.time() + 20
    while len(ran) < 2 and time.time() < end:
        time.sleep(0.05)
    stop.set()
    t.join(5)
    assert sorted(ran) == ["one", "two"], (ran, said)
    assert any(s.startswith("bad: its schedule cannot be read") for s in
               said), said
    assert not t.is_alive()
