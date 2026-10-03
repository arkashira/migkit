"""Every program migkit runs goes through one runner (audit 2026-10-01,
W1-W3, W9, W12, W19, W20).

Before it, 62 places started a program each its own way: 15 with a time
limit, 2 in a process group of their own, 7 listed where the next run could
find them. Here each promise the runner makes is held on a real program: a
stopped program's children stop with it, a program that runs out of time
or passes nothing is ended and said, a listing is by the moment a process
started and not its number, a failure is of one of three kinds, a secret
never reaches a command line or the run's record, a script on another
machine is held by its lock.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time

import pytest

from migkit import runner, stall


def _alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _script(tmp_path, name, body):
    path = tmp_path / name
    path.write_text("#!/bin/sh\n" + body + "\n")
    path.chmod(0o755)
    return str(path)


# ---- started, and stopped with what it started -----------------------------

def test_a_program_runs_in_a_group_of_its_own(tmp_path):
    proc = runner.start(["sleep", "30"], stdin=subprocess.DEVNULL)
    try:
        with proc:
            assert os.getpgid(proc.pid) == proc.pid
            assert os.getpgid(proc.pid) != os.getpgid(0)
            proc.stop(grace=5)
        assert proc.poll() is not None
    finally:
        if proc.poll() is None:
            proc.popen.kill()


def test_stopping_a_program_stops_what_it_started(tmp_path):
    """Only the direct child used to be signalled: a shell's own children
    went on writing."""
    marker = tmp_path / "child.pid"
    prog = _script(tmp_path, "parent",
                   f"sleep 300 & echo $! > {marker}; wait")
    proc = runner.start([prog], stdin=subprocess.DEVNULL)
    with proc:
        for _ in range(50):
            if marker.exists() and marker.read_text().strip():
                break
            time.sleep(0.1)
        child = int(marker.read_text())
        assert _alive(child)
        proc.stop(grace=5)
    time.sleep(0.3)
    assert not _alive(child), "the program's own child outlived it"


def test_an_exception_while_it_runs_stops_it_and_unlists_it(tmp_path):
    listing = tmp_path / "running-programs.json"
    with runner.listing(listing):
        proc = runner.start(["sleep", "30"], stdin=subprocess.DEVNULL)
        with pytest.raises(RuntimeError):
            with proc:
                got = json.loads(listing.read_text())
                assert got[str(proc.pid)]["program"] == "sleep"
                assert got[str(proc.pid)]["created"] is not None
                raise RuntimeError("the caller failed")
    assert proc.poll() is not None
    assert not listing.exists()


def test_kill_is_taken_as_ctrl_c_while_a_program_runs(tmp_path):
    marker = tmp_path / "pid"
    child = subprocess.Popen([sys.executable, "-c", textwrap.dedent(f"""
        import pathlib, sys
        from migkit import runner
        prog = pathlib.Path({str(tmp_path / 'p')!r})
        prog.write_text("#!/bin/sh\\necho $$ > {marker}\\nexec sleep 300\\n")
        prog.chmod(0o755)
        print("started", flush=True)
        try:
            runner.run([str(prog)])
        except KeyboardInterrupt:
            print("interrupted", flush=True)
            sys.exit(130)
    """)], stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "started"
    for _ in range(100):
        if marker.exists() and marker.read_text().strip():
            break
        time.sleep(0.1)
    pid = int(marker.read_text())
    child.send_signal(signal.SIGTERM)
    assert child.wait(timeout=30) == 130
    assert "interrupted" in child.stdout.read()
    time.sleep(0.3)
    assert not _alive(pid), "the program outlived the run that was killed"


# ---- the listing: by the moment a process started --------------------------

def test_a_listed_process_is_the_one_that_started_then(tmp_path):
    listing = tmp_path / "running-programs.json"
    other = subprocess.Popen(["sleep", "30"])
    try:
        import psutil
        created = psutil.Process(other.pid).create_time()
        host = socket.gethostname()
        listing.write_text(json.dumps({str(other.pid): {
            "host": host, "program": "sleep", "since": time.time(),
            "created": round(created, 2)}}))
        assert [p for p, _, _ in runner.still_running(listing)] == \
            [str(other.pid)]
        # the same number, started at another moment: another process
        listing.write_text(json.dumps({str(other.pid): {
            "host": host, "program": "sleep", "since": time.time(),
            "created": round(created - 1000, 2)}}))
        assert runner.still_running(listing) == []
        assert not listing.exists()
    finally:
        other.kill()
        other.wait()


def test_a_container_a_gone_run_left_is_removed(tmp_path, monkeypatch):
    listing = tmp_path / "running-programs.json"
    listing.write_text(json.dumps({"999999": {
        "host": socket.gethostname(), "program": "docker",
        "since": time.time(), "created": 1.0,
        "container": "migkit-20261001T000000-abc123-1"}}))
    removed, said = [], []
    monkeypatch.setattr(runner, "_remove_container", removed.append)
    assert runner.still_running(listing, said.append) == []
    assert removed == ["migkit-20261001T000000-abc123-1"]
    assert said and "removed" in said[0], said


# ---- time limits, stalls, output ------------------------------------------------

def test_a_program_past_its_time_is_ended_and_said(tmp_path):
    began = time.monotonic()
    with pytest.raises(runner.Transient) as e:
        runner.run(["sleep", "30"], timeout=1, what="the copy")
    assert time.monotonic() - began < 15
    assert "the copy: did not finish in 1s" in str(e.value), e.value


def test_a_program_that_passes_nothing_is_ended_by_the_stall_limit(
        tmp_path):
    prog = _script(tmp_path, "quiet", "echo 'row 1'; sleep 30")
    seen = []

    def progress(line):
        seen.append(line)
        return line.strip()
    began = time.monotonic()
    with pytest.raises(stall.Stalled) as e:
        runner.run([prog], progress=progress, merge=True,
                   stall=stall.Watch(2), what="the copy")
    assert time.monotonic() - began < 20
    assert "nothing moved for 2s" in str(e.value), e.value
    assert seen == ["row 1\n"], seen


def test_a_program_that_keeps_moving_is_not_stalled(tmp_path):
    prog = _script(tmp_path, "busy",
                   "for i in 1 2 3 4 5 6 7 8; do echo row $i; sleep 0.5;"
                   " done")
    said = []
    got = runner.run([prog], progress=lambda line: line.strip(),
                     log=said.append, merge=True, stall=stall.Watch(2))
    assert got.returncode == 0
    assert said == [f"row {i}" for i in range(1, 9)], said


def test_both_streams_are_read_as_they_come(tmp_path):
    """A program that fills the pipe nobody reads is held up for good:
    each stream is read on a thread of its own."""
    prog = _script(tmp_path, "loud",
                   "i=0; while [ $i -lt 4000 ]; do"
                   " echo 'a line of what it says to the error stream' >&2;"
                   " echo 'a line of what it says'; i=$((i+1)); done")
    got = runner.run([prog], timeout=60)
    assert got.stdout.count("\n") == 4000
    assert got.stderr.count("\n") == 4000


# ---- one way to say a failure ----------------------------------------------------

@pytest.mark.parametrize("said,kind", [
    ("FATAL:  password authentication failed for user \"app\"",
     runner.Refused),
    ("ERROR 1045 (28000): Access denied for user 'app'@'10.0.0.1'"
     " (using password: YES)", runner.Refused),
    ("ERROR:  permission denied for table orders", runner.Refused),
    ("ERROR 1044 (42000): Access denied for user 'app'@'%' to database"
     " 'shop'", runner.Refused),
    ("[TARGET 1] [53100] ERROR:  could not extend file", runner.Refused),
    ("ERROR:  cannot execute INSERT in a read-only transaction",
     runner.Refused),
    ("server closed the connection unexpectedly", runner.Transient),
    ("could not connect to server: Connection refused", runner.Transient),
    ("ERROR:  duplicate key value violates unique constraint",
     runner.Failed),
])
def test_a_failure_is_of_one_of_three_kinds(said, kind):
    got = runner.classify(1, said, "the copy")
    assert type(got) is kind, (type(got), got)
    assert str(got).startswith("the copy: "), got


def test_a_refusal_stops_every_way_and_the_rest_switch():
    """The decision layer switches on a way that could not run, and stops
    on a refusal every way would meet (`decide.operational`)."""
    from migkit import decide
    assert not decide.operational(runner.classify(
        1, "password authentication failed"))
    assert decide.operational(runner.classify(1, "connection refused"))
    assert decide.operational(runner.classify(1, "some fault of its own"))
    assert decide.operational(runner.classify("missing", ""))


def test_a_program_ended_by_a_signal_is_said_so():
    got = runner.classify(-9, "")
    assert "SIGKILL" in str(got) and "memory" in str(got), got


def test_a_failure_never_carries_the_command_line(tmp_path):
    prog = _script(tmp_path, "fails", "echo 'it went wrong' >&2; exit 2")
    with pytest.raises(runner.Failed) as e:
        runner.run([prog, "--host", "10.0.0.7", "--token", "CHANGE_ME_t"])
    said = str(e.value)
    assert "it went wrong" in said, said
    assert "10.0.0.7" not in said and "CHANGE_ME_t" not in said, said


def test_a_failure_hides_the_runs_secrets(tmp_path):
    prog = _script(tmp_path, "leaks",
                   "echo 'connecting as app with CHANGE_ME_pw' >&2; exit 2")
    with pytest.raises(runner.Failed) as e:
        runner.run([prog], secrets=("CHANGE_ME_pw",))
    assert "CHANGE_ME_pw" not in str(e.value), e.value
    assert "***" in str(e.value), e.value


# ---- the record, and secrets never on a command line --------------------------------

def test_the_command_line_goes_to_the_runs_record_hidden(tmp_path):
    from migkit.wording import DebugLog
    with runner.recording(DebugLog(tmp_path / "commands.log"),
                          ("CHANGE_ME_a",)):
        runner.run(["sh", "-c", "true", "x", "CHANGE_ME_a"],
                   {"PGPASSWORD": "CHANGE_ME_b"})
    body = (tmp_path / "commands.log").read_text()
    assert "sh -c true x ***" in body, body
    assert "CHANGE_ME" not in body, body
    assert runner._RUN["debug"] is None


def test_a_containers_secrets_reach_it_through_a_private_file(
        tmp_path, monkeypatch):
    seen = {}

    def fake_run(argv, env=None, **kw):
        seen["argv"], seen["env"] = list(argv), dict(env or {})
        at = argv.index("--env-file")
        seen["file"] = argv[at + 1]
        seen["body"] = open(argv[at + 1]).read()
        seen["mode"] = os.stat(argv[at + 1]).st_mode & 0o777
        seen["container"] = kw.get("container")
        return subprocess.CompletedProcess(argv, 0, "", "")
    removed = []
    monkeypatch.setattr(runner, "run", fake_run)
    monkeypatch.setattr(runner, "_remove_container", removed.append)
    runner.container("example/image:1", ["copy", "--jobs", "4"],
                     secrets={"SOURCE_URI": "postgresql://u:CHANGE_ME@h/d"},
                     passthrough={"PROPS": "a = 1\nb = CHANGE_ME_2\n"})
    argv = " ".join(seen["argv"])
    assert "CHANGE_ME" not in argv, argv
    assert seen["body"] == "SOURCE_URI=postgresql://u:CHANGE_ME@h/d\n"
    assert seen["mode"] == 0o600
    assert not os.path.exists(seen["file"]), "the file outlived the run"
    # a value of many lines, through the client's own environment
    assert "-e PROPS" in argv and seen["env"]["PROPS"].startswith("a = 1")
    name = seen["container"]
    assert name.startswith("migkit-") and f"--name {name}" in argv
    assert "--init" in seen["argv"] and "--rm" in seen["argv"]
    # removed however it ended
    assert removed == [name]


# ---- the flags, on the command line about to start ----------------------------------

def test_a_flag_the_build_does_not_take_is_refused_before_it_starts(
        tmp_path, monkeypatch):
    from migkit import toolversion
    ran = tmp_path / "ran"
    prog = _script(tmp_path, "mydumper", f"touch {ran}")
    monkeypatch.setattr(toolversion, "options",
                        lambda program, *sub, **kw: frozenset({"--threads"}))
    monkeypatch.setattr(toolversion, "version", lambda program: "0.9.1")
    with pytest.raises(runner.Blocked) as e:
        runner.run([prog, "--threads", "4", "--trx-tables"])
    assert "--trx-tables" in str(e.value), e.value
    assert not ran.exists(), "it was started"
    # every flag it takes: it runs
    runner.run([prog, "--threads", "4"])
    assert ran.exists()


# ---- a script on another machine, under its lock ----------------------------------

def test_a_script_on_another_machine_is_held_by_its_lock(tmp_path):
    lock = tmp_path / "data.migkit-lock"
    first = subprocess.Popen(
        ["sh", "-c", runner.locked_script("sleep 5", str(lock))])
    try:
        for _ in range(50):
            if (tmp_path / "data.migkit-lock.pid").exists():
                break
            time.sleep(0.1)
        second = subprocess.run(
            ["sh", "-c", runner.locked_script("echo ran", str(lock))],
            capture_output=True, text=True, timeout=30)
        assert second.returncode == 75, second
        assert "migkit-lock" in second.stderr and "ran" not in second.stdout
    finally:
        first.kill()
        first.wait()
    # its holder gone, the next one runs
    again = subprocess.run(
        ["sh", "-c", runner.locked_script("echo ran", str(lock))],
        capture_output=True, text=True, timeout=30)
    assert again.returncode == 0 and "ran" in again.stdout, again


# ---- the helper every engine calls, through the same runner ------------------------

def test_the_engines_run_helper_goes_through_the_runner(tmp_path):
    from migkit import util
    listing = tmp_path / "running-programs.json"
    prog = _script(tmp_path, "look", f"cat {listing}")
    with runner.listing(listing):
        got = util.run([prog])
    assert '"program": "look"' in got.stdout, got.stdout
    with pytest.raises(subprocess.TimeoutExpired):
        util.run(["sleep", "30"], timeout=0.5)
    with pytest.raises(OSError):
        util.run(["/nonexistent/program"])
