"""Keys the target has that the source never did.

Scanning the source finds a key that is missing on the target. Nothing finds
the reverse, and the count check cannot stand in for it: measured, one key
deleted on the target and one stray key added there leaves `dbsize` at 20 on
both sides.

    counts  ok    db0          keys 20==20
    data    diff  db0          1/20 keys differ (sample 20)
                               ... and no mention of the stray key at all

A target still holding keys from an earlier attempt is exactly that shape, and
every other engine names what the target has and the source does not - mysql
says "extra tables on target", mongodb says "extra collections on target".
This is the same command, so redis says it too.
"""
import pathlib
import socket
import subprocess
import tempfile
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-rx-src", "migkit-test-rx-dst"
SRC_PORT, DST_PORT = 16403, 16404


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


def rcli(container, *args):
    return subprocess.run(["docker", "exec", container, "redis-cli", *args],
                          capture_output=True, text=True)


def _engine(**options):
    hop = Hop(name="r", engine="redis",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              db_map={"0": "0"}, options=options)
    hop.report_dir = lambda db=None: pathlib.Path(tempfile.mkdtemp())
    from migkit.engines.redis import RedisEngine
    return RedisEngine(hop)


@pytest.fixture(scope="module")
def pair():
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:6379", "redis:7"], check=True,
                       capture_output=True)
    assert _wait(SRC_PORT) and _wait(DST_PORT)
    for name in (SRC, DST):
        for _ in range(30):
            if "PONG" in rcli(name, "ping").stdout:
                break
            time.sleep(1)
        else:
            pytest.fail(f"{name} never answered")
    yield
    for name in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


@pytest.fixture
def loaded(pair):
    """The same twenty keys on both sides, and nothing else."""
    for name in (SRC, DST):
        rcli(name, "flushall")
        rcli(name, "eval",
             "for i=1,20 do redis.call('set', 'k'..i, 'v'..i) end return 1",
             "0")
    assert rcli(SRC, "dbsize").stdout.strip() == "20"
    assert rcli(DST, "dbsize").stdout.strip() == "20"
    yield


def _by_scope(results):
    return {r.scope: r for r in results}


def test_identical_sides_pass_and_say_both_directions_were_looked_at(loaded):
    res = _engine().check_data("0")
    assert [r.status for r in res] == ["ok"], [(r.scope, r.detail)
                                               for r in res]
    assert "20 keys value-equal" in res[0].detail
    assert "20 target keys all present on the source" in res[0].detail


def test_a_stray_key_on_the_target_is_found_when_the_counts_match(loaded):
    """The measurement this file opens with. The count check is asked too,
    so the test carries the reason the reverse pass has to exist rather than
    only asserting that it works."""
    rcli(DST, "del", "k7")
    rcli(DST, "set", "stray_from_an_earlier_attempt", "v")
    assert rcli(SRC, "dbsize").stdout == rcli(DST, "dbsize").stdout

    eng = _engine()
    counts = eng.check_counts("0")
    assert counts[0].status == "ok", counts[0].detail
    assert "20==20" in counts[0].detail, counts[0].detail

    res = _by_scope(eng.check_data("0"))
    extra = res.get("db0 extra keys")
    assert extra is not None, [(k, v.detail) for k, v in res.items()]
    assert extra.status == "diff"
    assert "stray_from_an_earlier_attempt" in extra.detail
    assert "1 of 20 keys on the target" in extra.detail, extra.detail
    assert "earlier attempt" in extra.fix_hint
    # the forward direction still reports the key that went missing
    assert res["db0"].status == "diff"
    assert "1/20 keys differ" in res["db0"].detail


def test_a_key_only_the_source_has_is_still_reported(loaded):
    rcli(SRC, "set", "only_on_source", "v")
    res = _by_scope(_engine().check_data("0"))
    assert res["db0"].status == "diff", res["db0"].detail
    assert "1/21 keys differ" in res["db0"].detail, res["db0"].detail
    assert "db0 extra keys" not in res, res["db0 extra keys"].detail


def test_a_value_that_differs_is_not_confused_with_a_missing_key(loaded):
    rcli(DST, "set", "k3", "something else")
    res = _by_scope(_engine().check_data("0"))
    assert res["db0"].status == "diff"
    assert "db0 extra keys" not in res, "a changed value is not an extra key"


def test_the_sample_cap_bounds_the_reverse_pass_too(loaded):
    """Otherwise the new pass would walk a whole production keyspace while
    the forward one stopped at the sample."""
    res = _engine(sample=5).check_data("0")
    assert [r.status for r in res] == ["ok"], [(r.scope, r.detail)
                                               for r in res]
    assert "5 keys value-equal" in res[0].detail, res[0].detail
    assert "5 target keys" in res[0].detail, res[0].detail
    assert "sample 5" in res[0].detail, res[0].detail

    deep = _engine(deep=True).check_data("0")
    assert "full scan" in deep[0].detail
    assert "20 keys value-equal" in deep[0].detail
    assert "20 target keys" in deep[0].detail
