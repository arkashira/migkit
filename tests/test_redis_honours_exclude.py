"""`exclude` on Redis: a target-owned key pattern is left alone.

Redis read no exclude list at all, so a pattern the operator excluded - a
cache the application on the target fills for itself - was counted,
compared, and removed by `repair` as keys the source does not have.
Patterns match `db.key` and `key`, with shell wildcards, the same rule
every other engine uses for its tables.
"""
import pathlib
import socket
import subprocess
import tempfile
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-rex-src", "migkit-test-rex-dst"
SRC_PORT, DST_PORT = 15653, 15654


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


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def _engine(exclude=()):
    hop = Hop(name="r", engine="redis",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              db_map={"0": "0"}, exclude=list(exclude),
              options={"deep": True})
    hop.report_dir = lambda db=None, _p=pathlib.Path(tempfile.mkdtemp()): _p
    from migkit.engines.redis import RedisEngine
    return RedisEngine(hop)


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
        yield (redis.Redis(port=SRC_PORT, decode_responses=True),
               redis.Redis(port=DST_PORT, decode_responses=True))
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def seeded(pair):
    s, t = pair
    for c in (s, t):
        c.flushdb()
    for i in range(10):
        s.set(f"order:{i}", f"v{i}")
        t.set(f"order:{i}", f"v{i}")
    s.delete("order:9")                 # one stray key the target has
    t.set("cache:user:1", "target-owned")
    t.set("cache:user:2", "target-owned", ex=3600)
    return s, t


def _said(results):
    return " | ".join(f"{r.scope} {r.status} {r.detail}" for r in results)


def test_the_check_leaves_an_excluded_pattern_alone(seeded):
    eng = _engine(["cache:*"])
    counts = eng.check_counts("0")
    data = eng.check_data("0")
    said = _said(counts + data + eng.check_deep("0"))
    assert "cache:" not in said, said
    # and the check still judged what it may look at: one stray order key
    assert counts[0].status == "diff" and "src=9 dst=10" in said, said
    assert "order:9" in said, said


def test_repair_keeps_the_target_owned_keys(seeded):
    _, t = seeded
    eng = _engine(["cache:*"])
    eng.check_data("0")
    plan = eng.repair_plan("0", "rows")
    assert plan, "the stray order key is still a finding"
    for action in plan:
        eng.apply("0", action)
    assert t.get("cache:user:1") == "target-owned"
    assert t.get("cache:user:2") == "target-owned"
    assert t.get("order:9") is None


def test_without_an_exclude_list_nothing_changes(seeded):
    said = _said(_engine().check_counts("0") + _engine().check_data("0"))
    assert "src=9 dst=12" in said, said
    assert "cache:user:1" in said, said


def test_server_settings_are_compared_and_secrets_left_out(pair, tmp_path):
    """An eviction policy decides which keys vanish under memory pressure;
    a target that evicts differently loses different keys."""
    s, t = pair
    t.config_set("maxmemory-policy", "allkeys-lru")
    s.config_set("requirepass", "")
    try:
        eng = _engine()
        eng.hop.report_dir = lambda db=None, _p=tmp_path: _p
        got = eng.check_params("0")
        said = " | ".join(f"{r.status} {r.detail}" for r in got)
        assert any(r.status == "diff" for r in got), said
        assert "maxmemory-policy" in said, said
        dumped = " ".join(p.read_text() for p in tmp_path.rglob("*.json"))
        # the dump was written, so its not holding a secret means something
        assert "maxmemory-policy" in dumped, dumped[:200]
        assert "requirepass" not in dumped and "masterauth" not in dumped
    finally:
        t.config_set("maxmemory-policy", "noeviction")


def test_a_kind_of_value_the_copy_left_behind_is_named(pair, tmp_path):
    """A stream on the source and none on the target is the shape of a copy
    that dropped what it did not handle; the key counts can still match."""
    s, t = pair
    for c in (s, t):
        c.flushdb()
    s.set("a", "1")
    s.xadd("events", {"k": "v"})
    t.set("a", "1")
    t.set("events", "flattened")
    eng = _engine()
    eng.hop.report_dir = lambda db=None, _p=tmp_path: _p
    got = {r.scope: r for r in eng.check_schema("0")}
    kinds = got["db0 kinds"]
    assert kinds.status == "diff" and "stream" in kinds.detail, kinds.detail
    assert got["db0 modules"].status == "ok", got["db0 modules"].detail
