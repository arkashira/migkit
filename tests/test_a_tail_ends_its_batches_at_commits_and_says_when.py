"""A tail's batch ends at a source commit, and its beat says when the
source committed what it applied last (backlog F7).

* A batch is cut after the last change that ended a source transaction
  where a read stopped inside the next one (`_commit_cut`): the position
  saved is that commit's, and the rest is read with the next batch - a
  transaction that fits a batch is never half on the target. One larger
  than a read is carried in parts, as before.
* The beat carries the source's commit time of the last change applied
  and how long it took from there to the target (`tailctl.beat`), and
  `/metrics` says both.
"""
import json
import socket
import subprocess
import threading
import time

import pytest

from migkit import canon, tailctl
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine
from tests.conftest import needs_docker


def _c(i, end=False, at=None):
    c = canon.change("insert", "t", {"id": i}, {"id": i})
    if end:
        c["txn_end"] = True
        if at is not None:
            c["txn_at"] = at
    return c


def test_a_batch_is_cut_after_its_last_commit():
    cut = HeteroEngine._commit_cut
    # ends at a commit already: whole
    assert cut([_c(1), _c(2, True, "p2")]) is None
    # a commit, then part of the next transaction: cut after the commit
    assert cut([_c(1, True, "p1"), _c(2), _c(3)]) == 1
    assert cut([_c(1), _c(2, True, "p2"), _c(3, True, "p3"), _c(4)]) == 3
    # one transaction larger than the read: carried in parts
    assert cut([_c(1), _c(2), _c(3)]) is None
    # an end that says nowhere to read on from cannot be cut at
    assert cut([_c(1, True), _c(2)]) is None
    assert cut([]) is None


def test_the_beat_says_when_the_source_committed(tmp_path):
    from migkit import ui
    where = tmp_path / "db"
    where.mkdir()
    with tailctl.Running(where):
        tailctl.beat(where, time.time(), 5, None, through=time.time() - 3,
                     lag=2.5, tables={"live": 3, "parked": 1,
                                      "set_aside_bytes": 120})
        st = tailctl.state(where)
        assert st["lag"] == 2.5 and st["through"] < time.time() - 2
        lines = ui._tail_metrics(tmp_path, 'hop="h"')
    got = "\n".join(lines)
    assert 'migkit_tail_commit_lag_seconds{hop="h",db="db"} 2.500' in got
    assert "migkit_tail_applied_through_age_seconds" in got
    assert 'migkit_tail_tables{hop="h",db="db",state="parked"} 1' in got
    assert 'migkit_tail_set_aside_bytes{hop="h",db="db"} 120' in got


MY, PORT = "migkit-test-livetail-commit-my", 16263


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{PORT}:3306", "mysql:8.4", "--server-id=7",
                    "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _pair(where, name):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name=name, engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"}, workers=1,
              options={"exact_batches": False})
    where.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: where
    return MySQLEngine(hop)._as_pair()


def _run(pair, where, seconds=20):
    lines, done = [], threading.Event()

    def run():
        try:
            pair.tail_apply("cx", True, where / "tail-token.json",
                            lines.append)
        except BaseException as e:  # noqa: BLE001 - said in the lines
            lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    threading.Thread(target=run, daemon=True).start()
    done.wait(seconds)
    if not done.is_set():
        tailctl.stop(where)
        done.wait(60)
    return lines


@needs_docker
@pytest.mark.usefixtures("mysql_server")
def test_a_transaction_that_fits_is_never_split(tmp_path, monkeypatch):
    _my("drop database if exists cx; drop database if exists cy;"
        " create database cx; create database cy;"
        " create table cx.t (id int primary key, v int);"
        " create table cy.t like cx.t")
    from migkit import failpoint
    where = tmp_path / "r"
    pair = _pair(where, "cut")
    # a first run saves the position; nothing to read yet
    _run(pair, where, seconds=5)
    # A: three rows, B: five rows one statement each; a read of five
    # stops inside B
    _my("begin; insert into cx.t values (1,0),(2,0),(3,0); commit;"
        " begin; insert into cx.t values (4,0); insert into cx.t values"
        " (5,0); insert into cx.t values (6,0); insert into cx.t values"
        " (7,0); insert into cx.t values (8,0); commit;")
    monkeypatch.setattr(pair, "TAIL_BATCH", 5)
    monkeypatch.setattr(failpoint, "_counts", {})
    monkeypatch.setenv("MIGKIT_FAILPOINT", "tail.saved:1:raise")
    lines = _run(pair, where)
    assert any("failpoint tail.saved" in line for line in lines), lines
    # the first batch was A alone: B was not split, and the position saved
    # is where A committed
    assert _my("select count(*) from cy.t") == "3"
    token = json.loads((where / "tail-token.json").read_text())["token"]
    assert "skip_rows" not in token, token
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    monkeypatch.setattr(pair, "TAIL_BATCH", 1000)
    lines = _run(pair, where, seconds=8)
    assert _my("select count(*) from cy.t") == "8", lines


@needs_docker
@pytest.mark.usefixtures("mysql_server")
def test_the_beat_carries_the_source_commit_time(tmp_path):
    _my("drop database if exists cx; drop database if exists cy;"
        " create database cx; create database cy;"
        " create table cx.t (id int primary key, v int);"
        " create table cy.t like cx.t")
    where = tmp_path / "r"
    pair = _pair(where, "beat")
    seen = {}
    lines, done = [], threading.Event()

    def run():
        try:
            pair.tail_apply("cx", True, where / "tail-token.json",
                            lines.append)
        finally:
            done.set()
    threading.Thread(target=run, daemon=True).start()
    try:
        end = time.time() + 60
        while not (where / "tail.pid").exists() and time.time() < end:
            time.sleep(0.2)
        before = time.time()
        _my("insert into cx.t values (1, 1)")
        while time.time() < end:
            st = tailctl.state(where) or {}
            if st.get("through"):
                seen = st
                break
            time.sleep(0.3)
    finally:
        tailctl.stop(where)
        done.wait(60)
    assert seen, lines
    # the source's own time, and how long it took to reach the target
    assert before - 5 <= seen["through"] <= time.time() + 5, seen
    assert 0 <= seen["lag"] < 60, seen
