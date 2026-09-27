"""Redis verifies only the keys the source wrote since the last cycle.

Measured before: `watch` on a Redis hop had no way to verify what changed
- the capability was declared missing - so a long follow was checked by
sampling the whole keyspace again, or not at all. The source now says
which keys it wrote (key tracking in broadcast mode, redirected to a
connection that listens: nothing of the server's is changed), and only
those are compared.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-rdelta-src", 15925), ("migkit-test-rdelta-dst",
                                                15926)


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
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:6379", "redis:7"], check=True,
                       capture_output=True)
    try:
        clients = []
        for _, port in (SRC, DST):
            assert _wait(port)
            c = redis.Redis(port=port)
            for _ in range(30):
                try:
                    c.ping()
                    break
                except Exception:
                    time.sleep(1)
            clients.append(c)
        yield clients
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def eng(pair, tmp_path):
    from migkit.engines.redis import RedisEngine
    s, t = pair
    for c in (s, t):
        c.flushall()
    for i in range(100):
        s.set(f"k{i}", i)
        t.set(f"k{i}", i)
    hop = Hop(name="rd", engine="redis",
              source=Endpoint(host="127.0.0.1", port=SRC[1]),
              target=Endpoint(host="127.0.0.1", port=DST[1]),
              databases=["0"])
    hop.report_dir = lambda db=None: tmp_path
    e = RedisEngine(hop)
    yield e
    e.delta_teardown("0")


def _settle():
    # the invalidations are pushed as the writes happen; the reader wakes
    # at least once a second
    time.sleep(1.5)


def test_only_the_written_keys_are_compared_until_they_match(pair, eng):
    s, t = pair
    first = eng.delta_verify("0")
    assert first[0].status == "ok" and "listening" in first[0].detail
    s.set("k1", "changed")
    s.set("new", "x")
    s.delete("k2")
    _settle()
    got = eng.delta_verify("0")
    assert got[0].status == "diff", got[0].detail
    assert got[0].detail.startswith("3 of 3 written keys differ: 1 missing"
                                    " on the target, 1 on the target only,"
                                    " 1 with another value"), got[0].detail
    # carried over by hand; nothing written on the source since, and the
    # three are compared again because they differed
    t.set("k1", "changed")
    t.set("new", "x")
    t.delete("k2")
    got = eng.delta_verify("0")
    assert got[0].status == "ok", got[0].detail
    assert got[0].detail == "3 written keys, every one the same on the"\
        " target", got[0].detail
    # and then they are not compared again
    got = eng.delta_verify("0")
    assert got[0].detail.startswith("0 written keys"), got[0].detail


def test_a_value_changed_on_the_target_alone_is_not_seen_here(pair, eng):
    """What the source did not write is not this cycle's to compare: a
    target changed on its own is `check`'s to find, and this says only
    what it compared."""
    s, t = pair
    eng.delta_verify("0")
    t.set("k5", "drift")
    _settle()
    got = eng.delta_verify("0")
    assert got[0].status == "ok" and got[0].detail.startswith(
        "0 written keys"), got[0].detail


def test_a_listener_cut_off_is_said_not_passed(pair, eng):
    s, _ = pair
    eng.delta_verify("0")
    s.set("k7", "while nobody listened")
    s.client_kill_filter(_type="pubsub")
    _settle()
    got = eng.delta_verify("0")
    assert got[0].status == "error", got[0].detail
    assert "lost its connection" in got[0].detail, got[0].detail


def test_a_flushed_source_is_compared_whole(pair, eng):
    s, _ = pair
    eng.delta_verify("0")
    s.flushdb()
    _settle()
    got = eng.delta_verify("0")
    assert "compared whole" in got[0].detail, got[0].detail
    assert got[0].status == "diff", got[0].detail
