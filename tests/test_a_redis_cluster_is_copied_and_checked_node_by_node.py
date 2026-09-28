"""Redis: every node of a cluster copied and checked, each key's expiry
carried as the moment it falls due, and how recently or often it was read.

A SCAN of one node of a cluster sees the keys of that node's slots and no
others, and a DUMP or RESTORE of a key another node serves is refused with
`MOVED`. The copier and every check read the node the hop names: a third
of a three-master cluster was copied, the counts compared one node against
the other side, and a key that differed on either of the other two was
never looked at - a check that said equal over two thirds it did not read.

The expiry was sent as the time left, read a pipeline before the write, so
a key landed living longer than on the source by however long the write
took; and a restored key started with no access history, so the target
evicted by the order of the copy rather than the application's use.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

BOX, OLD = "migkit-test-out-rc", "migkit-test-out-r6"
NODES = (16090, 16091, 16092)
ALONE, OLD_PORT = 16093, 16094


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

#: three masters and a server on its own, all in one container: the
#: cluster's nodes announce 127.0.0.1 and the ports the host reaches them
#: on, so a client outside finds every node where the cluster says it is
START = " ; ".join(
    [f"redis-server --port {p} --cluster-enabled yes --cluster-config-file"
     f" /data/n{p}.conf --cluster-announce-ip 127.0.0.1 --save ''"
     f" --appendonly no --daemonize yes --logfile /data/r{p}.log"
     for p in NODES]
    + [f"redis-server --port {ALONE} --save '' --daemonize yes"
       f" --logfile /data/r{ALONE}.log", "sleep 1",
       "redis-cli --cluster create " + " ".join(f"127.0.0.1:{p}"
                                                for p in NODES)
       + " --cluster-replicas 0 --cluster-yes", "tail -f /dev/null"])


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module")
def servers():
    import redis
    from redis.cluster import RedisCluster
    for name in (BOX, OLD):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", BOX, "-p",
                        f"127.0.0.1:{NODES[0]}-{ALONE}:{NODES[0]}-{ALONE}",
                        "redis:7", "sh", "-c", START], check=True,
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", OLD, "-p",
                        f"127.0.0.1:{OLD_PORT}:6379", "redis:6"], check=True,
                       capture_output=True)
        for port in NODES + (ALONE, OLD_PORT):
            assert _wait(port), port
        end = time.time() + 60
        while time.time() < end:
            got = subprocess.run(["docker", "exec", BOX, "redis-cli", "-p",
                                  str(NODES[0]), "cluster", "info"],
                                 capture_output=True, text=True).stdout
            if "cluster_state:ok" in got:
                break
            time.sleep(1)
        else:
            pytest.fail("the cluster never came up")
        cluster = RedisCluster(host="127.0.0.1", port=NODES[0])
        alone = redis.Redis(port=ALONE)
        old = redis.Redis(port=OLD_PORT)
        for c in (alone, old):
            for _ in range(30):
                try:
                    c.ping()
                    break
                except Exception:
                    time.sleep(1)
        yield cluster, alone, old
    finally:
        for name in (BOX, OLD):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def clean(servers):
    cluster, alone, old = servers
    cluster.flushall()
    alone.flushall()
    old.flushall()
    for c in (alone, old):
        c.config_set("maxmemory-policy", "noeviction")
    for node in cluster.get_primaries():
        node.redis_connection.config_set("maxmemory-policy", "noeviction")
    return servers


def _engine(tmp_path, src, dst, **options):
    from migkit.engines.redis import RedisEngine

    def ep(port):
        return Endpoint(host="127.0.0.1", port=port, user="", password="")
    hop = Hop(name="rc", engine="redis", source=ep(src), target=ep(dst),
              databases=["0"], options={"deep": True, **options})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return RedisEngine(hop)


class _Ck(dict):
    def save(self):
        pass


def _size(cluster):
    """Every master's keys: the cluster client's own `dbsize` asks one."""
    return sum(n.redis_connection.dbsize() for n in cluster.get_primaries())


def _owner(cluster, key):
    return cluster.get_node_from_key(key).port


def _fill(client, n=600):
    for i in range(n):
        client.set(f"k:{i}", f"v{i}")
    client.hset("h:1", mapping={"a": "1", "b": "2"})
    client.rpush("l:1", "x", "y")
    client.set("ttl:1", "soon", px=600000)


def test_a_cluster_source_is_copied_from_every_node(clean, tmp_path):
    cluster, alone, _ = clean
    _fill(cluster)
    owners = {_owner(cluster, f"k:{i}") for i in range(600)}
    assert owners == set(NODES), owners
    eng = _engine(tmp_path, NODES[0], ALONE)
    eng.move_table("0", "", "0", 100, _Ck(), lambda m: None)
    assert alone.dbsize() == _size(cluster) == 603
    assert alone.get("k:599") == b"v599"
    assert [r.status for r in eng.check_counts("0")] == ["ok"]
    assert [r.status for r in eng.check_data("0")] == ["ok"]


def test_a_difference_on_any_node_is_found(clean, tmp_path):
    """The verdict, able to say different wherever the difference is: a
    key changed and a key missing on the target, both served by nodes
    other than the one the hop names."""
    cluster, alone, _ = clean
    _fill(cluster)
    eng = _engine(tmp_path, NODES[0], ALONE)
    eng.move_table("0", "", "0", 100, _Ck(), lambda m: None)
    far = [f"k:{i}" for i in range(600)
           if _owner(cluster, f"k:{i}") != NODES[0]]
    alone.set(far[0], "changed on the target")
    alone.delete(far[1])
    counts = eng.check_counts("0")
    assert counts[0].status == "diff", counts[0].detail
    data = eng.check_data("0")
    assert data[0].status == "diff", data[0].detail
    assert "1 missing on the target" in data[0].detail
    assert "1 with a different value" in data[0].detail
    # and the other way: the cluster as the target, a stray on a far node
    rev = _engine(tmp_path, ALONE, NODES[0])
    alone.set(far[1], f"v{far[1][2:]}")
    cluster.set("stray:x", "the target's only")
    got = rev.check_data("0")
    assert any(r.status == "diff" and "stray:x" in r.detail for r in got), \
        [(r.scope, r.detail) for r in got]


def test_a_cluster_target_takes_the_keys_of_every_slot(clean, tmp_path):
    cluster, alone, _ = clean
    _fill(alone)
    cluster.set("stale:1", "the target's before the copy")
    eng = _engine(tmp_path, ALONE, NODES[0])
    eng.move_table("0", "", "0", 100, _Ck(), lambda m: None)
    assert _size(cluster) == 603 and cluster.get("stale:1") is None
    assert [r.status for r in eng.check_data("0")] == ["ok"]


class _Slow:
    """A client whose pipelines take two seconds to write: a slow link."""

    def __init__(self, c):
        self.c = c

    def __getattr__(self, name):
        return getattr(self.c, name)

    def pipeline(self, *a, **k):
        return _SlowPipe(self.c.pipeline(*a, **k))


class _SlowPipe:
    def __init__(self, p):
        self.p = p

    def __getattr__(self, name):
        return getattr(self.p, name)

    def execute(self, *a, **k):
        time.sleep(2)
        return self.p.execute(*a, **k)


def _slow_target(eng):
    keys, client = eng._keys, eng._client
    eng._keys = lambda side, db=0, decode=True: (
        _Slow(keys(side, db, decode)) if side == "dst"
        else keys(side, db, decode))
    eng._client = lambda side, db=0, decode=True: (
        _Slow(client(side, db, decode)) if side == "dst"
        else client(side, db, decode))


def test_the_expiry_lands_as_the_moment_it_falls_due(clean, tmp_path):
    """Measured with the relative TTL this sent before: over a link that
    takes two seconds to write, every key lived two seconds longer on the
    target than on the source."""
    cluster, alone, _ = clean
    at = int(time.time() * 1000) + 600_000
    for i in range(20):
        cluster.set(f"e:{i}", "x")
        cluster.pexpireat(f"e:{i}", at)
    eng = _engine(tmp_path, NODES[0], ALONE)
    _slow_target(eng)
    eng.move_table("0", "", "0", 100, _Ck(), lambda m: None)
    for i in range(20):
        assert alone.pexpiretime(f"e:{i}") == at, (
            i, alone.pexpiretime(f"e:{i}") - at)


def test_a_source_before_7_is_carried_by_its_own_clock(clean, tmp_path):
    """No `PEXPIRETIME` before 7.0: the moment is the source's clock plus
    the time left, both read once, before the write."""
    _, alone, old = clean
    at = int(time.time() * 1000) + 600_000
    for i in range(20):
        old.set(f"e:{i}", "x")
        old.pexpireat(f"e:{i}", at)
    eng = _engine(tmp_path, OLD_PORT, ALONE)
    _slow_target(eng)
    eng.move_table("0", "", "0", 100, _Ck(), lambda m: None)
    for i in range(20):
        late = alone.pexpiretime(f"e:{i}") - at
        assert abs(late) <= 50, (i, late)


def test_how_long_since_a_key_was_read_is_carried(clean, tmp_path):
    cluster, alone, _ = clean
    alone.set("cold", "untouched for a while")
    alone.set("hot", "just read")
    time.sleep(3)
    alone.get("hot")
    cold = alone.object("idletime", "cold")
    assert cold >= 3
    eng = _engine(tmp_path, ALONE, NODES[0])
    eng.move_table("0", "", "0", 100, _Ck(), lambda m: None)
    got = cluster.object("idletime", "cold")
    assert got >= cold, (got, cold)
    assert cluster.object("idletime", "hot") < cold


def test_how_often_a_key_was_read_is_carried(clean, tmp_path):
    """Under an LFU policy on both sides, the counter the source kept is
    the target's. A fresh key starts at 5 (`LFU_INIT_VAL`)."""
    cluster, alone, _ = clean
    alone.config_set("maxmemory-policy", "allkeys-lfu")
    for node in cluster.get_primaries():
        node.redis_connection.config_set("maxmemory-policy", "allkeys-lfu")
    alone.set("popular", "x")
    for _ in range(3000):
        alone.get("popular")
    freq = alone.object("freq", "popular")
    assert freq > 5, freq
    eng = _engine(tmp_path, ALONE, NODES[0])
    eng.move_table("0", "", "0", 100, _Ck(), lambda m: None)
    assert cluster.object("freq", "popular") == freq


def test_written_keys_are_heard_from_every_node(clean, tmp_path):
    """The check of what changed listened to the node the hop names: a
    key written on either of the other two was never compared."""
    cluster, alone, _ = clean
    _fill(cluster, 60)
    eng = _engine(tmp_path, NODES[0], ALONE)
    eng.move_table("0", "", "0", 100, _Ck(), lambda m: None)
    try:
        first = eng.delta_verify("0")
        assert first[0].status == "ok", first[0].detail
        far = next(f"w:{i}" for i in range(100)
                   if _owner(cluster, f"w:{i}") == NODES[2])
        cluster.set(far, "written after the copy")
        time.sleep(1)
        got = eng.delta_verify("0")
        assert got[0].status == "diff", got[0].detail
        assert "1 missing on the target" in got[0].detail
    finally:
        eng.delta_teardown("0")


def test_a_server_in_the_clear_is_said_to_be(clean):
    """The TLS probe (`migkit.tls`) against a port with no TLS on it."""
    from migkit import tls
    tls._SEEN.clear()
    assert tls.probe("127.0.0.1", ALONE, "direct")[0] == "none"


#: a stand-in for Redis's own copy tool that writes down what it was given
FAKE = """#!{python}
import json, os, sys
json.dump({{"argv": sys.argv[1:],
           "env": {{k: v for k, v in os.environ.items()
                   if k.startswith("RIOT_")}}}}, open("{seen}", "w"))
"""


def test_redis_products_tool_is_handed_every_node(clean, tmp_path,
                                                  monkeypatch):
    """Where its terms allow it, Redis's own copy tool gets the keyspace:
    the target emptied of what it held, each side's address and whether
    it is a cluster in its environment. (A stand-in here: installing the
    tool is accepting its terms, which is the operator's to do.)"""
    import json
    import os
    import sys

    from migkit import movers
    cluster, alone, _ = clean
    alone.set("stale", "the target's before the copy")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    seen = tmp_path / "seen.json"
    fake = bin_dir / "riotx"
    fake.write_text(FAKE.format(python=sys.executable, seen=seen))
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    hop = _engine(tmp_path, NODES[0], ALONE).hop
    hop.target.options = {"redis_product": "community"}
    assert movers.fitted(hop, "redis", "riotx") == ("riotx", None)
    movers.riotx_move(hop, "0", 2, True, lambda m: None)
    got = json.loads(seen.read_text())
    assert got["argv"][:3] == ["replicate", "--mode", "scan"], got
    assert got["env"]["RIOT_SOURCE_URI"] == \
        f"redis://127.0.0.1:{NODES[0]}/0"
    assert got["env"]["RIOT_SOURCE_CLUSTER"] == "true"
    assert "RIOT_TARGET_CLUSTER" not in got["env"]
    assert alone.dbsize() == 0
