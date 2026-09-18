"""`migkit assess` on Redis - the same command, answering for a second kind
of database.

The findings are Redis's own, but the shape is the one every engine gives:
a version comparison from the base, then whatever this engine knows to look
at. The point of the exercise is that `assess` stops saying "not implemented"
the moment an engine can report a version.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-ar-src", "migkit-test-ar-dst"
SRC_PORT, DST_PORT = 16379, 16380


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
def pair():
    for n, p, extra in ((SRC, SRC_PORT, []),
                        (DST, DST_PORT, ["--maxmemory-policy", "allkeys-lru",
                                         "--save", ""])):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-p",
                        f"{p}:6379", "redis:7"] + extra,
                       check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    time.sleep(1)
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.redis import RedisEngine
    hop = Hop(name="r", engine="redis",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT),
              target=Endpoint(host="127.0.0.1", port=DST_PORT))
    hop.report_dir = lambda db=None: tmp_path
    return RedisEngine(hop)


def _rows(tmp_path):
    items = _engine(tmp_path).assess()
    assert items, "assess returned nothing at all"
    return items


def test_assess_no_longer_says_not_implemented(pair, tmp_path):
    items = _rows(tmp_path)
    assert not any("not implemented" in i["item"] for i in items), items


def test_the_version_comparison_comes_from_the_base(pair, tmp_path):
    """Both sides are the same image, so this is the pass case - and it
    proves the engine actually reported a version rather than None."""
    hit = [i for i in _rows(tmp_path) if i["item"] == "server version match"]
    assert hit, _rows(tmp_path)
    assert hit[0]["level"] == "pass", hit
    assert "src 7" in hit[0]["detail"], hit[0]["detail"]


def test_an_evicting_target_is_a_failure_not_a_note(pair, tmp_path):
    """`allkeys-lru` drops keys under memory pressure, which arrives as a
    migration that lost data with nothing in any log."""
    hit = [i for i in _rows(tmp_path)
           if i["item"] == "target eviction policy"]
    assert hit, _rows(tmp_path)
    assert hit[0]["level"] == "fail", hit
    assert "allkeys-lru" in hit[0]["detail"]
    assert "silently drop keys" in hit[0]["detail"]


def test_a_memory_only_target_is_reported(pair, tmp_path):
    """Started with `--save ''` and no AOF: a restart before cutover loses
    everything."""
    hit = [i for i in _rows(tmp_path)
           if i["item"] == "target keeps the data on disk"]
    assert hit, _rows(tmp_path)
    assert hit[0]["level"] == "warn", hit
    assert "memory only" in hit[0]["detail"], hit[0]["detail"]


def test_the_persistence_signal_is_the_configuration_not_the_save_time(pair,
                                                                       tmp_path):
    """`rdb_last_save_time` is set at startup whether or not snapshots are
    configured - measured, identical on a default server and on one started
    with `--save ""`. Reading it as evidence reported a memory-only target as
    safe, which is the worst direction for this check to be wrong in."""
    import redis as _r
    a = _r.Redis(host="127.0.0.1", port=SRC_PORT, socket_timeout=5)
    b = _r.Redis(host="127.0.0.1", port=DST_PORT, socket_timeout=5)
    assert a.info("persistence").get("rdb_last_save_time") is not None
    assert b.info("persistence").get("rdb_last_save_time") is not None, \
        "the misleading field is gone, so this test no longer guards anything"
    assert a.config_get("save").get("save"), "source should schedule saves"
    assert not b.config_get("save").get("save", "").strip(), \
        "target should have no save schedule"
    hit = [i for i in _rows(tmp_path)
           if i["item"] == "target keeps the data on disk"][0]
    assert "save=(none)" in hit["detail"], hit["detail"]


def test_an_unreachable_side_is_unknown_not_clean(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.redis import RedisEngine
    hop = Hop(name="r", engine="redis",
              source=Endpoint(host="127.0.0.1", port=1),
              target=Endpoint(host="127.0.0.1", port=1))
    hop.report_dir = lambda db=None: tmp_path
    items = RedisEngine(hop).assess()
    assert items, "assess said nothing about a server it could not reach"
    assert any("unknown, not clean" in i["detail"] for i in items), items
