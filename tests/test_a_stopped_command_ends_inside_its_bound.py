"""W16N-BASELINE: with the workers pinned to 1 the check of the rows waited
for a slot its own caller held (a borrowed lease read as already lent, so
the third level down never ran), and SIGTERM left the command there: the
stop's `finally` waited on those threads for good.

The unit's slot is ridden on at any depth (`ranges._Lease.lent`), and a
stop's cleanup is bounded (`decide.stop_grace`, `cli._arm_stop_bound`)."""
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

from migkit import decide, ranges, runner

ROOT = str(Path(__file__).resolve().parent.parent)


def test_one_slot_runs_work_three_levels_deep():
    """The shape the check has: a database's unit, its tables, each
    table's ranges - every level waiting on the one below with one slot."""
    slots = ranges.Slots(1)
    seen = []

    def leaf(k):
        seen.append(k)

    def table(t):
        slots.each([f"{t}.{k}" for k in range(3)], leaf)

    def db(d):
        slots.each([f"{d}.t{i}" for i in range(2)], table)
    t = threading.Thread(target=slots.each, args=(["a", "b"], db),
                         daemon=True)
    t.start()
    t.join(20)
    assert not t.is_alive(), "the work waited on its own slot"
    assert len(seen) == 12 and len(set(seen)) == 12
    # every slot back: the next unit takes one at once
    lease = slots._take()
    assert slots._busy == 1 and not lease.borrowed
    slots._give(lease)
    assert slots._busy == 0


def test_a_lent_slot_is_not_lent_twice_at_once():
    """Siblings started by one unit share its slot one at a time: two
    never run on it together, so one slot still means one at a time."""
    slots = ranges.Slots(1)
    now, most = [0], [0]
    lock = threading.Lock()

    def leaf(k):
        with lock:
            now[0] += 1
            most[0] = max(most[0], now[0])
        time.sleep(0.05)
        with lock:
            now[0] -= 1

    def parent(p):
        import concurrent.futures as cf
        import contextvars
        with cf.ThreadPoolExecutor(4) as pool:
            futs = [pool.submit(contextvars.copy_context().run,
                                slots.each, [f"{p}{i}"], leaf)
                    for i in range(4)]
            for f in futs:
                f.result(timeout=20)
    slots.each(["p"], parent)
    assert most[0] == 1


def test_the_stop_bound_is_decided_from_what_runs():
    with_programs, why = decide.stop_grace({"programs": 2, "grace": 15})
    assert why.path == "programs" and with_programs == 30
    alone, why = decide.stop_grace({"programs": 0, "grace": 15})
    assert why.path == "statement" and alone == 15
    # a statement measured slower than the grace is given twice its time
    slow, _ = decide.stop_grace({"programs": 0, "grace": 15,
                                 "slowest": 40})
    assert slow == 80


def test_sigterm_ends_a_command_whose_cleanup_waits_for_good(tmp_path):
    """A thread that never ends (the hang the harness found) holds the
    exit: SIGTERM ends the command inside the bound, code 130, said."""
    script = tmp_path / "held.py"
    script.write_text(textwrap.dedent("""
        import sys, threading, time
        from migkit import cli, runner
        runner.GRACE = 1
        never = threading.Event()
        threading.Thread(target=never.wait).start()
        with cli._term_as_interrupt():
            print("ready", flush=True)
            try:
                time.sleep(60)
            finally:
                never.wait()
    """))
    p = subprocess.Popen([sys.executable, str(script)], cwd=ROOT,
                         env={**os.environ, "PYTHONPATH": ROOT},
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True)
    try:
        assert p.stdout.readline().strip() == "ready"
        t0 = time.monotonic()
        p.send_signal(signal.SIGTERM)
        p.wait(timeout=20)
        took = time.monotonic() - t0
    finally:
        p.kill()
    err = p.stderr.read()
    assert p.returncode == 130, err
    assert took < 10, took
    assert "did not end in 1s" in err and "next run reconciles" in err


def test_a_cleanup_that_ends_takes_the_bound_off():
    """In a process that goes on (this one), a stop whose cleanup ended
    leaves no bound behind to end it later."""
    from migkit import cli
    try:
        with cli._term_as_interrupt():
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(30)
    except KeyboardInterrupt:
        pass
    assert cli._STOP_BOUND["armed"] is None
    time.sleep(0.2)
    assert not [t for t in threading.enumerate() if t.name == "stop-bound"
                and t.is_alive()]
    assert cli._arm_stop_bound not in runner.ON_STOP
