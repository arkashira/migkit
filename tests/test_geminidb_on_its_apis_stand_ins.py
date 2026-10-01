"""Huawei GeminiDB's Redis API as a side of a pair (backlog P0-CLOUDS),
against Redis 7 standing in for the API it speaks - no GeminiDB instance
is here. What GeminiDB does differently is what it lacks: it is never a
replica and serves none, so a GeminiDB side is copied key by key with each
key's time to live, compared, and followed by nothing; that is held here
by the engine having no way that makes or reads a replica.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, SRC_PORT = "migkit-test-gemini-src", 16450
DST, DST_PORT = "migkit-test-gemini-dst", 16451


def _wait(port, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module")
def pair():
    import redis
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"127.0.0.1:{port}:6379", "redis:7"], check=True,
                       capture_output=True)
    try:
        assert _wait(SRC_PORT) and _wait(DST_PORT)
        s, t = redis.Redis(port=SRC_PORT), redis.Redis(port=DST_PORT)
        for c in (s, t):
            for _ in range(30):
                try:
                    c.ping()
                    break
                except Exception:
                    time.sleep(1)
        yield s, t
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _engine(tmp_path):
    from migkit.engines.geminidb import GeminiRedisEngine
    hop = Hop(name="gem", engine="geminidb-redis",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT),
              target=Endpoint(host="127.0.0.1", port=DST_PORT),
              databases=["0"])
    hop.report_dir = lambda db=None: tmp_path
    return GeminiRedisEngine(hop)


def test_a_keyspace_is_copied_key_by_key_with_its_times_to_live(pair,
                                                                tmp_path):
    from migkit.cli import _Checkpoint
    s, t = pair
    s.flushall()
    t.flushall()
    for i in range(50):
        s.set(f"k:{i}", f"v{i}")
    s.hset("h", mapping={"a": "1"})
    s.set("ttl", "soon", px=600000)
    eng = _engine(tmp_path)
    assert not hasattr(eng, "replicate_sql")
    eng.move_table("0", "", "0", 20, _Checkpoint(tmp_path / "move.json"),
                   lambda m: None)
    assert t.get("k:7") == b"v7" and 0 < t.pttl("ttl") <= 600000
    got = {r.scope: r.status for r in eng.check_data("0")
           if r.check == "data"}
    assert got and set(got.values()) == {"ok"}, got
    t.set("k:7", "changed")
    got = {r.scope: r.status for r in _engine(tmp_path).check_data("0")
           if r.check == "data"}
    assert "diff" in got.values(), got
