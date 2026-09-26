"""A write operation holds its hop by a lease with a heartbeat (backlog 30).

It held it by a file with a process number in it. A process on another
machine could not tell whether that one still ran, and a holder that died
left the file saying it did. Now the holder renews its lease while it
runs; a lease not renewed for its whole term is taken over, and the
operation that takes it over says from whom.
"""
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import textwrap
import time

import pytest
from click.testing import CliRunner

from migkit.lease import Held, Lease


def test_a_held_lease_is_refused_with_its_holder(tmp_path):
    first = Lease(tmp_path / "lease.json", "move").acquire()
    try:
        with pytest.raises(Held) as e:
            Lease(tmp_path / "lease.json").acquire()
        said = str(e.value)
        assert "another migkit move is running on this hop" in said, said
        assert f"process {os.getpid()}" in said, said
    finally:
        first.release()
    assert not (tmp_path / "lease.json").exists()
    Lease(tmp_path / "lease.json").acquire().release()


def test_the_heartbeat_keeps_it_and_a_lapsed_one_is_taken(tmp_path,
                                                          monkeypatch):
    monkeypatch.setenv("MIGKIT_LEASE_SECONDS", "1.5")
    path = tmp_path / "lease.json"
    held = Lease(path).acquire()
    try:
        first = json.loads(path.read_text())["expires"]
        time.sleep(2.5)
        # renewed while it runs: longer than its term, and still held
        assert json.loads(path.read_text())["expires"] > first
        with pytest.raises(Held):
            Lease(path).acquire()
    finally:
        held.release()
    # another machine's, lapsed: taken over, and said
    path.write_text(json.dumps({"holder": "x", "host": "far.example.com",
                                "pid": 1, "since": time.time() - 99,
                                "expires": time.time() - 1}))
    got = Lease(path).acquire()
    try:
        assert got.took_over["host"] == "far.example.com"
    finally:
        got.release()
    # another machine's, current: held, however long ago it started
    path.write_text(json.dumps({"holder": "x", "host": "far.example.com",
                                "pid": 1, "since": time.time() - 99,
                                "expires": time.time() + 30}))
    with pytest.raises(Held):
        Lease(path).acquire()


def test_a_holder_killed_on_this_machine_is_not_waited_for(tmp_path):
    path = tmp_path / "lease.json"
    child = subprocess.Popen([sys.executable, "-c", textwrap.dedent(f"""
        import time
        from pathlib import Path
        from migkit.lease import Lease
        Lease(Path({str(path)!r})).acquire()
        print("held", flush=True)
        time.sleep(300)
    """)], stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "held"
    with pytest.raises(Held):
        Lease(path).acquire()
    child.send_signal(signal.SIGKILL)
    child.wait()
    got = Lease(path).acquire()
    try:
        assert got.took_over["pid"] == child.pid
        assert got.took_over["host"] == socket.gethostname()
    finally:
        got.release()


def test_one_taken_over_stops_renewing(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_LEASE_SECONDS", "1")
    path = tmp_path / "lease.json"
    old = Lease(path).acquire()
    # someone else holds it now - the old holder was not heard from
    path.write_text(json.dumps({"holder": "new", "host": "far.example.com",
                                "pid": 2, "since": time.time(),
                                "expires": time.time() + 60}))
    time.sleep(1.2)
    assert json.loads(path.read_text())["holder"] == "new"
    old.release()
    # and letting go does not remove someone else's
    assert json.loads(path.read_text())["holder"] == "new"


def test_a_move_on_a_held_hop_is_refused(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import cli
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table t (id integer primary key)")
        con.commit()
        con.close()
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    held = Lease(tmp_path / "reports" / "lite" / "lease.json",
                 "repair").acquire()
    try:
        got = CliRunner().invoke(cli.main, ["move", "lite", "--mode", "full",
                                            "--go"])
        said = " ".join((got.output + str(got.exception)).split())
        assert got.exit_code != 0, said
        assert "another migkit repair is running on this hop" in said, said
    finally:
        held.release()
    got = CliRunner().invoke(cli.main, ["move", "lite", "--mode", "full",
                                        "--go"])
    assert got.exit_code == 0, got.output
