"""Redis copies a keyspace key for key, resumably (backlog 0e).

Each value moves in the server's own serialised form with what is left of
its time to live, as the repair already moved them. The copy is stopped
after its first batch and resumed from the scan's cursor. What the hop
leaves out stays where it is on both sides, and a key the target held
that is not the source's is gone.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-rcp-src", "migkit-test-rcp-dst"
SRC_PORT, DST_PORT = 15816, 15817


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


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
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:6379", "redis:7"], check=True,
                       capture_output=True)
    try:
        assert _wait(SRC_PORT) and _wait(DST_PORT)
        s = redis.Redis(port=SRC_PORT)
        t = redis.Redis(port=DST_PORT)
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


def _engine(tmp_path):
    from migkit.engines.redis import RedisEngine
    hop = Hop(name="rcp", engine="redis",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              databases=["0"], exclude=["local:*"])
    hop.report_dir = lambda db=None: tmp_path
    return RedisEngine(hop)


class _Stop(Exception):
    pass


def test_stopped_and_resumed_every_kept_key_arrives(pair, tmp_path):
    from migkit.cli import _Checkpoint
    s, t = pair
    for i in range(300):
        s.set(f"k:{i}", f"v{i}")
    s.hset("h", mapping={"a": "1", "b": "2"})
    s.rpush("l", "x", "y", "x")
    s.sadd("set", "p", "q")
    s.zadd("z", {"m": 1.5, "n": -2})
    s.xadd("stream", {"f": "v"})
    s.set("ttl", "soon", px=600000)
    s.set(b"\xff\xfe raw", b"\x00\x01")
    s.set("local:src-only", "the source's own")
    t.set("local:dst-only", "the target's own")
    eng = _engine(tmp_path)
    # the target's own key is left out, so the target holds nothing
    assert eng.moved_nothing("0") == ["db0"]
    t.set("stray", "not the source's")
    ck = _Checkpoint(tmp_path / "move.json")
    said = []

    def stop_after_one(line):
        said.append(line)
        if line.startswith("db0: ") and "emptied" not in line:
            raise _Stop()
    with pytest.raises(_Stop):
        eng.move_table("0", "", "0", 50, ck, stop_after_one)
    first = _Checkpoint(tmp_path / "move.json")["db0"]
    assert first["cursor"] != 0 and 0 < first["moved"] < 307, first
    eng.move_table("0", "", "0", 50, _Checkpoint(tmp_path / "move.json"),
                   said.append)

    kept = sorted(k for k in s.keys() if not k.startswith(b"local:"))
    assert len(kept) == 307
    assert sorted(k for k in t.keys() if not k.startswith(b"local:")) == kept
    for k in kept:
        assert t.dump(k) == s.dump(k), k
    assert 0 < t.pttl("ttl") <= 600000
    assert t.pttl("h") == -1
    # what the hop leaves out: not carried, not removed
    assert t.get("local:src-only") is None
    assert t.get("local:dst-only") == b"the target's own"
    assert "db0: emptied 1 keys the target held before the copy" in said, \
        said
    assert eng.moved_nothing("0") == []
    assert not [x for x in said if "redis" in x.lower()], said
    # the check reads the key that is not UTF-8 too, and finds it equal
    got = [r for r in eng.check_data("0") if r.check == "data"]
    assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]
    t.set(b"\xff\xfe raw", b"\x00\x02")
    got = [r for r in eng.check_data("0") if r.check == "data"]
    assert [r.status for r in got] == ["diff"], [r.__dict__ for r in got]
    t.set(b"\xff\xfe raw", b"\x00\x01")
    # run again after the source changed: not skipped on the earlier word
    s.set("k:1", "changed")
    said = []
    eng.move_table("0", "", "0", 50, _Checkpoint(tmp_path / "move.json"),
                   said.append)
    assert said[0] == ("db0: done earlier; a keyspace cannot be asked"
                       " whether it still matches, so it is copied again"), \
        said
    assert t.get("k:1") == b"changed"


def test_the_restore_point_says_what_the_target_held(pair, tmp_path):
    _engine(tmp_path).snapshot_state("0", tmp_path)
    got = (tmp_path / "dst-shape.txt").read_text()
    assert "('hash', 1)" in got and "('stream', 1)" in got, got
    assert "('string', 302)" in got, got
