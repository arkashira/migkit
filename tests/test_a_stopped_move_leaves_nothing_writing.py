"""A move that is stopped does not leave its programs writing (backlog 44).

Measured on MySQL, 1,500,000 rows: migkit killed while the load ran, and
the load went on without it - 375,000 rows when migkit died, all of them
by the time anyone looked. A move started again at once emptied the tables
under it and stopped on `Duplicate entry '425000'`, naming neither the
cause nor the program. The same with PostgreSQL: the copy program finished
the table after migkit was gone, and the target was left with the rows and
without its keys.

Now:
* `kill` (SIGTERM) stops what the move started, the way ctrl-c does
* `kill -9` cannot be answered by anybody, so the programs a move runs are
  listed while they run, and the next move finds one still running and
  stops before it writes, naming the process to wait for
"""
import json
import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop


def _hop(tmp_path):
    hop = Hop(name="k", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="127.0.0.1", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False


CHILD = textwrap.dedent("""
    import sys
    from pathlib import Path
    from migkit import movers
    from migkit.config import Endpoint, Hop
    where = Path(sys.argv[1])
    hop = Hop(name="k", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="127.0.0.1", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["app"])
    hop.report_dir = lambda db=None: where
    # the bulk path, standing in: one program that runs until stopped
    movers._movers = lambda: {"mydumper": lambda h, d, w, g, l:
                              movers._sh(["sleep", "300"])}
    movers.options_missing = lambda h, d, v: {}
    movers.which = lambda p: "/bin/" + p
    print("started", flush=True)
    movers.run_via("mydumper", hop, "app", 1, True, None)
""")


def _start(tmp_path):
    child = subprocess.Popen([sys.executable, "-c", CHILD, str(tmp_path)],
                             stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "started"
    listing = tmp_path / "running-programs.json"
    for _ in range(50):
        if listing.exists():
            break
        time.sleep(0.1)
    got = json.loads(listing.read_text())
    assert [r["program"] for r in got.values()] == ["sleep"], got
    return child, listing, next(iter(got))


def test_kill_stops_what_the_move_started(tmp_path):
    child, listing, pid = _start(tmp_path)
    assert _alive(pid)
    child.send_signal(signal.SIGTERM)
    child.wait(timeout=30)
    time.sleep(0.5)
    assert not _alive(pid), "the program outlived the move"
    assert not listing.exists()


def test_after_kill_9_the_next_move_waits_for_the_program(tmp_path):
    child, listing, pid = _start(tmp_path)
    child.send_signal(signal.SIGKILL)
    child.wait(timeout=30)
    try:
        # nothing could answer that: the program runs on, and is listed
        assert _alive(pid)
        assert listing.exists()
        if not all(movers.which(p) for p in movers.PROGRAMS["mydumper"]):
            pytest.skip("the bulk path's programs are not installed here")
        with pytest.raises(SystemExit) as e:
            movers.run_via("mydumper", _hop(tmp_path), "app", 1, True, None)
        said = str(e.value)
        assert f"still running (process {pid}" in said, said
        assert f"kill {pid}" in said and "Nothing has been written" in said
        assert "sleep" not in said and "mydumper" not in said, said
    finally:
        os.kill(int(pid), signal.SIGKILL)
    time.sleep(0.5)
    # gone now: the listing is dropped and the move may go on
    assert movers._still_running(listing) == []
    assert not listing.exists()


def test_a_number_reused_by_another_program_is_not_the_one_listed(tmp_path):
    """A process number comes round again. One that now belongs to some
    other program is not the move's, and does not hold the next one up."""
    listing = tmp_path / "running-programs.json"
    other = subprocess.Popen(["sleep", "30"])
    try:
        import socket
        listing.write_text(json.dumps({str(other.pid): {
            "host": socket.gethostname(), "program": "myloader",
            "since": time.time()}}))
        assert movers._still_running(listing) == []
        assert not listing.exists()
    finally:
        other.kill()
        other.wait()


def test_a_program_listed_by_another_machine_counts(tmp_path):
    """Its process cannot be asked about from here; guessing it stopped is
    the guess that writes beside it."""
    listing = tmp_path / "running-programs.json"
    listing.write_text(json.dumps({"4242": {
        "host": "another-machine.example.com", "program": "myloader",
        "since": time.time()}}))
    assert [p for p, _, _ in movers._still_running(listing)] == ["4242"]
