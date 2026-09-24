"""MongoDB confirms a difference before calling it one.

PostgreSQL and MySQL already fence: they note where the source is, wait for
the target to get there, and read the suspect keys again. MongoDB settled on
elapsed time or not at all, so a document the tail had not applied yet was
reported as a difference. The fence is now migkit's own tail: it saves the
position it has applied up to, idle or not, and the confirm pass waits for
that position to reach the source's cluster time before it reads again.
"""
import ctypes
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop

MG, PORT = "migkit-test-mgfence", 15695


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(), reason="docker not available")]


def mongosh(script, db="cx"):
    return subprocess.run(["docker", "exec", MG, "mongosh", "--quiet", db,
                           "--eval", script], capture_output=True, text=True)


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"{PORT}:27017", "mongo:7", "--replSet", "rs0",
                    "--bind_ip_all"], check=True, capture_output=True)
    try:
        for _ in range(60):
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        for _ in range(40):
            if mongosh("db.runCommand({ping:1}).ok", "admin").returncode == 0:
                break
            time.sleep(2)
        mongosh('rs.initiate({_id:"rs0",members:'
                '[{_id:0,host:"127.0.0.1:27017"}]})', "admin")
        for _ in range(30):
            if "PRIMARY" in mongosh(
                    "rs.status().myState === 1 ? 'PRIMARY' : 'no'",
                    "admin").stdout:
                break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)


def _engine(tmp_path):
    from migkit.engines.mongodb import MongoEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="", password="",
                  options={"uri_options": "directConnection=true"})
    hop = Hop(name="mgf", engine="mongodb", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"},
              options={"fence_timeout": 60})
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


def _seed():
    mongosh("db.getSiblingDB('cx').dropDatabase();"
            " db.getSiblingDB('cy').dropDatabase();"
            " db.getSiblingDB('cx').t.insertMany("
            "[{_id: 1, v: 'a'}, {_id: 2, v: 'b'}]);"
            " db.getSiblingDB('cy').t.insertMany("
            "[{_id: 1, v: 'a'}, {_id: 2, v: 'b'}]);")


def _tail(eng, path, after=0.0):
    """migkit's own tail in a thread, started `after` seconds from now."""
    stop = threading.Event()

    def run():
        time.sleep(after)
        try:
            eng.tail_apply("cx", True, path, lambda m: None)
        except BaseException:
            pass
        finally:
            stop.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    def halt():
        if not stop.is_set():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            stop.wait(20)
    return halt


def _data(eng):
    got = [r for r in eng.check_data("cx") if r.check == "data"]
    return {r.scope: r for r in got}


@pytest.mark.usefixtures("server")
def test_a_change_still_arriving_is_not_called_a_difference(tmp_path):
    eng = _engine(tmp_path)
    _seed()
    path = eng.hop.report_dir("cx") / "tail-token.json"
    halt = _tail(eng, path)
    time.sleep(4)        # running and idle: it has saved where it is
    halt()
    # the source moves on while nothing carries it
    assert mongosh("db.t.updateOne({_id: 1}, {$set: {v: 'new'}})"
                   ).returncode == 0
    halt = _tail(eng, path, after=3)
    try:
        got = _data(eng)["cx.t"]
    finally:
        halt()
    assert got.status == "ok", got.detail
    assert "still arriving" in got.detail, got.detail


@pytest.mark.usefixtures("server")
def test_a_difference_on_the_target_alone_is_still_one(tmp_path):
    eng = _engine(tmp_path)
    _seed()
    path = eng.hop.report_dir("cx") / "tail-token.json"
    halt = _tail(eng, path)
    try:
        time.sleep(4)
        assert mongosh("db.t.updateOne({_id: 2}, {$set: {v: 'target only'}})",
                       "cy").returncode == 0
        got = _data(eng)["cx.t"]
    finally:
        halt()
    assert got.status == "diff", got.detail


@pytest.mark.usefixtures("server")
def test_without_a_tail_there_is_nothing_to_fence_on(tmp_path):
    """No saved position: the pass cannot tell in flight from wrong, and
    says the difference as it found it rather than waiting blind."""
    eng = _engine(tmp_path)
    _seed()
    assert mongosh("db.t.updateOne({_id: 1}, {$set: {v: 'new'}})"
                   ).returncode == 0
    got = _data(eng)["cx.t"]
    assert got.status == "diff", got.detail


def test_the_token_says_its_cluster_time():
    """Measured tokens from this session: the time sits right after the
    first byte."""
    from bson.timestamp import Timestamp

    from migkit.engines.mongodb import MongoEngine
    got = MongoEngine._token_time("826AB465E4000000652B042C0100296E5A10")
    assert got == Timestamp(0x6AB465E4, 0x65), got
    assert MongoEngine._token_time("00ff") is None
