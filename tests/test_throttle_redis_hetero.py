"""The brake on the two paths that were running without one.

A Redis `SCAN` plus a pipeline of reads is the heaviest thing a verifier does
to a single-threaded server, and a cross-engine move reads a whole table. Both
ran at whatever speed the network allowed.

The measurement that shaped the Redis signal is the reason this file exists.
The obvious choice - `instantaneous_ops_per_sec` - is a rolling sample rather
than a reading of now:

    during 200k SET/sec   instantaneous_ops_per_sec = 287,248  latency 0.02 ms
    two seconds after     instantaneous_ops_per_sec = 287,187  load stopped

A throttle built on that would back off from a fast, healthy server and keep
going against a slow, struggling one. Saturation against the ceiling is what
the other engines use and what this uses too.
"""
import socket
import subprocess
import time

import pytest

from migkit.throttle import BUSY_RATIO

RD = "migkit-test-thr-redis"
RD_PORT = 16377


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


def rcli(*args, container=RD):
    return subprocess.run(["docker", "exec", container, "redis-cli", *args],
                          capture_output=True, text=True)


def _engine(port=RD_PORT):
    from migkit.config import Endpoint, Hop
    from migkit.engines.redis import RedisEngine
    ep = Endpoint(host="127.0.0.1", port=port, user="", password="")
    return RedisEngine(Hop(name="t", engine="redis", source=ep, target=ep,
                           db_map={"0": "0"}))


@pytest.fixture(scope="module")
def redis_server():
    subprocess.run(["docker", "rm", "-f", RD], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", RD, "-p",
                    f"{RD_PORT}:6379", "redis:7"], check=True,
                   capture_output=True)
    assert _wait(RD_PORT)
    for _ in range(30):
        if rcli("ping").returncode == 0:
            break
        time.sleep(1)
    else:
        pytest.fail("redis never answered")
    yield
    subprocess.run(["docker", "rm", "-f", RD], capture_output=True)


def test_an_idle_server_reads_as_idle(redis_server):
    health = _engine()._health("src")
    assert health is not None, "a reachable server must produce a reading"
    assert health.busy_ratio is not None
    assert health.busy_ratio < BUSY_RATIO, health.busy_ratio
    assert not health.stressed()


def test_the_ratio_is_clients_against_the_ceiling_not_throughput(
        redis_server):
    """Both numbers are read off the server, so the ratio is checked against
    them rather than against a value written beside it."""
    info = rcli("info", "clients").stdout.replace("\r", "")
    fields = dict(l.split(":", 1) for l in info.splitlines() if ":" in l)
    connected = float(fields["connected_clients"])
    ceiling = float(fields["maxclients"])
    health = _engine()._health("src")
    # migkit's own probe is a client too, so the count may be one higher
    assert abs(health.busy_ratio - connected / ceiling) < 2 / ceiling, (
        health.busy_ratio, connected, ceiling)


def test_throughput_would_have_been_the_wrong_signal(redis_server):
    """The measurement in this file's docstring, run again: the field stays
    high after the load has stopped, so it says nothing about now."""
    subprocess.run(["docker", "exec", "-d", RD, "sh", "-c",
                    "redis-benchmark -n 2000000 -c 50 -t set -q"
                    " > /tmp/b.log 2>&1"], check=True, capture_output=True)
    time.sleep(3)

    def ops():
        out = rcli("info", "stats").stdout.replace("\r", "")
        for line in out.splitlines():
            if line.startswith("instantaneous_ops_per_sec:"):
                return int(line.split(":", 1)[1])
        return None
    busy = ops()
    assert busy and busy > 1000, f"the benchmark did not load the server: {busy}"
    subprocess.run(["docker", "exec", RD, "sh", "-c",
                    "pkill redis-benchmark || true"], capture_output=True)
    time.sleep(2)
    after = ops()
    assert after and after > 1000, (
        "the field fell to zero immediately, which would make it usable -"
        f" re-measure before trusting it: {after}")
    # meanwhile the ratio migkit does use has come back down
    assert _engine()._health("src").busy_ratio < BUSY_RATIO
    rcli("flushall")


def test_a_background_save_counts_as_busy(redis_server):
    """Redis forks to write the RDB, and a fork over a large keyspace is the
    one moment when adding a full scan is genuinely unkind."""
    assert rcli("debug", "populate", "2000000").returncode == 0
    assert rcli("bgsave").returncode == 0
    saw_busy = False
    for _ in range(20):
        info = rcli("info", "persistence").stdout.replace("\r", "")
        running = "rdb_bgsave_in_progress:1" in info
        health = _engine()._health("src")
        if running:
            saw_busy = True
            assert health.stressed(), (health.busy_ratio, health.note)
            assert "rewriting to disk" in health.note
            break
        time.sleep(0.2)
    assert saw_busy, "the background save finished before it could be sampled"
    rcli("flushall")


def test_an_unreachable_server_is_unknown_rather_than_idle(redis_server):
    """An engine that cannot answer must not be mistaken for a quiet one -
    that would remove the brake exactly when something is wrong."""
    assert _engine(port=1)._health("src") is None


def test_the_scan_is_gated_now(redis_server):
    """Without the gate this loop ran at whatever speed the network allowed.
    The check still has to work - a brake that stops the verifier is not an
    improvement."""
    for i in range(50):
        rcli("set", f"k{i}", f"v{i}")
    eng = _engine()
    res = eng.check_data("0")
    assert res and res[0].status == "ok", [(r.status, r.detail) for r in res]
    assert "keys value-equal" in res[0].detail, res[0].detail
    rcli("flushall")


def test_hetero_borrows_the_health_of_whichever_side_it_is_reading():
    """A cross-engine hop has no server of its own. Before this it was the
    one configuration with no brake at all, while doing the heaviest read."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    ep = Endpoint(host="127.0.0.1", port=RD_PORT, user="", password="")
    hop = Hop(name="h", engine="hetero", source=ep, target=ep,
              options={"source_engine": "redis", "target_engine": "redis"})
    eng = HeteroEngine(hop)
    seen = eng._health("src")
    assert seen is not None and seen.busy_ratio is not None, seen
    # and a side whose engine cannot say comes back as unknown, not idle
    sqlite_hop = Hop(name="h", engine="hetero",
                     source=Endpoint(host="/tmp/x.db", port=0, user="",
                                     password=""),
                     target=ep,
                     options={"source_engine": "sqlite",
                              "target_engine": "redis"})
    assert HeteroEngine(sqlite_hop)._health("src") is None
