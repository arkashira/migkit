"""The cross-engine confirm pass, from a MongoDB source.

A resume token carries the cluster time it stands at, and the change
stream's own token moves on with the cluster when nothing arrives. So the
fence is the source's cluster time now, reached when the tail's saved
token stands at or past it. A change the tail has read and not applied yet
reads `ok ... still arriving`.
"""
import ctypes
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MG, PG = "migkit-test-xmongo-mg", "migkit-test-xmongo-pg"
MG_PORT, PG_PORT = 15742, 15743


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def mongosh(script, db="cx"):
    return subprocess.run(["docker", "exec", MG, "mongosh", "--quiet", db,
                           "--eval", script], capture_output=True, text=True)


def pg(sql):
    got = subprocess.run(["docker", "exec", PG, "psql", "-U", "postgres",
                          "-d", "cx", "-At", "-c", sql], capture_output=True,
                         text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    for n in (MG, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"{MG_PORT}:27017", "mongo:7", "--replSet", "rs0",
                    "--bind_ip_all"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    try:
        for _ in range(40):
            if mongosh("db.runCommand({ping:1}).ok", "admin"
                       ).returncode == 0:
                break
            time.sleep(2)
        mongosh('rs.initiate({_id:"rs0",members:'
                '[{_id:0,host:"127.0.0.1:27017"}]})', "admin")
        for _ in range(30):
            if "PRIMARY" in mongosh("rs.status().myState === 1 ?"
                                    " 'PRIMARY' : 'no'", "admin").stdout:
                break
            time.sleep(2)
        for _ in range(40):
            if subprocess.run(["docker", "exec", PG, "psql", "-U",
                               "postgres", "-c", "create database cx"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        with socket.socket() as s:
            s.settimeout(5)
            assert s.connect_ex(("127.0.0.1", MG_PORT)) == 0
        mongosh("db.t.insertMany([{_id: 1, v: 'a'}, {_id: 2, v: 'b'}])")
        pg('create table t ("_id" bigint primary key, v text);'
           " insert into t values (1, 'a'), (2, 'b')")
        yield
    finally:
        for n in (MG, PG):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _eng(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="xm", engine="hetero",
              options={"source_engine": "mongodb",
                       "target_engine": "postgres", "fence_timeout": 60},
              source=Endpoint(host="127.0.0.1", port=MG_PORT, user="",
                              password="",
                              options={"uri_options":
                                       "directConnection=true"}),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_a_quiet_stream_reaches_the_cluster_time(pair, tmp_path):
    src = _eng(tmp_path).src_engine
    start = src.change_point("src", "cx")
    now = src.log_position("src", "cx")
    time.sleep(1)
    _, token = src.neutral_changes("src", "cx", start)
    assert src.position_reached(token, now) is True, (token, now)


def test_a_mongo_change_still_arriving_is_confirmed(pair, tmp_path,
                                                    monkeypatch):
    from migkit import tailctl
    from migkit.engines.postgres import PostgresEngine
    eng = _eng(tmp_path)
    real = PostgresEngine.neutral_apply
    held = {"once": False}

    def slow(self, side, db, changes):
        if not held["once"]:
            held["once"] = True
            time.sleep(6)
        return real(self, side, db, changes)

    monkeypatch.setattr(PostgresEngine, "neutral_apply", slow)
    thread = threading.Thread(
        target=lambda: eng.tail_apply("cx", True, tmp_path /
                                      "tail-token.json", lambda m: None),
        daemon=True)
    thread.start()
    try:
        for _ in range(30):
            if tailctl.alive(tmp_path) and \
                    (tmp_path / "tail-token.json").exists():
                break
            time.sleep(0.5)
        assert mongosh("db.t.insertOne({_id: 3, v: 'c'})").returncode == 0
        time.sleep(2)
        got = [r for r in eng.check_data("cx") if r.check == "data"]
        assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]
        assert "still arriving" in got[0].detail, got[0].detail
        assert pg("select v from t where _id = 3") == "c"
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            thread.join(20)
