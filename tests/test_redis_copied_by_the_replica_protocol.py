"""A Redis keyspace copied by the replica protocol's own copy (backlog
F7.rshake): the snapshot the source hands a replica, a scan where the
source refuses one, and the source's backup file where the hop names it.

Each way carries every type with what is left of its time to live, leaves
out what the hop leaves out (the target's own keys of that pattern stay
where they are), and is held to the source by migkit's check after.
Skipped where the program is not installed: `migkit doctor --install`
puts it in place.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.util import which
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.bulk", "redis")]

pytestmark = [needs_docker, pytest.mark.skipif(
    not which("redis-shake"), reason="the replica-protocol copy is not"
                                     " installed here")]

SRC, DST = "migkit-test-rshake-src", "migkit-test-rshake-dst"
SRC_PORT, DST_PORT = 16286, 16287


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
                except Exception:  # noqa: BLE001 - not up yet
                    time.sleep(1)
        yield s, t
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _fill(s, t):
    s.flushall()
    t.flushall()
    pipe = s.pipeline()
    for i in range(2000):
        pipe.set(f"str:{i}", f"v{i}")
    pipe.hset("hash:1", mapping={"a": "1", "b": "2"})
    pipe.rpush("list:1", "x", "y", "z")
    pipe.sadd("set:1", "p", "q")
    pipe.zadd("zset:1", {"m": 1.5, "n": 2})
    pipe.xadd("stream:1", {"f": "v"})
    pipe.set("ttl:1", "soon", px=600_000)
    pipe.set("tmp:1", "left out")
    pipe.set(b"\xff\xfebin", b"\x00\x01")
    pipe.execute()
    # the target's own, of a pattern the hop leaves out
    t.set("tmp:mine", "stays")
    t.set("stray", "goes")


def _hop(tmp_path, **src_opts):
    hop = Hop(name="rshake", engine="redis",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password="", options=src_opts),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              databases=["0"], exclude=["tmp:*"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _held(tmp_path, hop, s, t):
    from migkit.engines.redis import RedisEngine
    eng = RedisEngine(hop)
    bad = [r for r in eng.check_data("0") if r.status != "ok"]
    assert not bad, [(r.scope, r.detail) for r in bad]
    assert t.get("tmp:mine") == b"stays"
    assert t.get("tmp:1") is None
    assert t.get("stray") is None
    assert t.get(b"\xff\xfebin") == b"\x00\x01"
    left = t.pttl("ttl:1")
    assert 590_000 < left <= 600_000, left
    kept = {k for k in s.scan_iter() if not k.startswith(b"tmp:")}
    got = {k for k in t.scan_iter() if not k.startswith(b"tmp:")}
    assert kept == got


def test_the_snapshot_a_replica_gets_is_the_copy(pair, tmp_path):
    from migkit import redisshake
    s, t = pair
    _fill(s, t)
    hop = _hop(tmp_path)
    assert redisshake.reader_for(hop, "0").path == "sync"
    said = []
    redisshake.move(hop, "0", 2, True, said.append)
    assert any("the snapshot the source hands a replica" in str(x)
               for x in said), said
    _held(tmp_path, hop, s, t)


def test_a_source_refusing_a_replica_is_scanned(pair, tmp_path,
                                                monkeypatch):
    from migkit import redisshake
    s, t = pair
    _fill(s, t)
    monkeypatch.setattr(redisshake, "replica_refused",
                        lambda hop: "the source refuses a replica here")
    hop = _hop(tmp_path)
    got = redisshake.reader_for(hop, "0")
    assert got.path == "scan"
    assert "refuses a replica" in got.reason
    redisshake.move(hop, "0", 2, True, None)
    _held(tmp_path, hop, s, t)


def test_the_sources_backup_file_is_read_in_its_place(pair, tmp_path):
    from migkit import redisshake
    s, t = pair
    _fill(s, t)
    s.save()
    rdb = tmp_path / "dump.rdb"
    subprocess.run(["docker", "cp", f"{SRC}:/data/dump.rdb", str(rdb)],
                   check=True, capture_output=True)
    hop = _hop(tmp_path, rdb_file=str(rdb))
    assert redisshake.reader_for(hop, "0").path == "rdb"
    redisshake.move(hop, "0", 2, True, None)
    _held(tmp_path, hop, s, t)
