"""`migkit sync --kind rows` on Redis, which could not repair anything.

The other stores have been able to put a target right for a while; redis could
only tell you it was wrong. The check already knew which keys were involved -
it counted them and then dropped the names - so this is mostly a matter of
writing the same drilldown files the rest of the estate writes, and acting on
them.

DUMP and RESTORE carry the type and the expiry with the value, so one path
covers every type. Measured across two Redis 7 servers: a hash came back a
hash, a list came back in order, and a key with 600 seconds left came back
with 599992 ms. Measured across versions, it refuses rather than guessing:

    payload from Redis 7.4.11 -> RESTORE on Redis 6.2.24
    ResponseError: DUMP payload version or checksum are wrong

so the repair reports that instead of falling back to re-issuing values with
type-specific commands, which would quietly change what some types hold.
"""
import base64
import json
import pathlib
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST, OLD = ("migkit-test-rr-src", "migkit-test-rr-dst",
                 "migkit-test-rr-old")
SRC_PORT, DST_PORT, OLD_PORT = 16421, 16422, 16423


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


@pytest.fixture(scope="module")
def servers():
    spec = ((SRC, SRC_PORT, "redis:7"), (DST, DST_PORT, "redis:7"),
            (OLD, OLD_PORT, "redis:6"))
    for name, port, image in spec:
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:6379", image], check=True,
                       capture_output=True)
    for _, port, _ in spec:
        assert _wait(port)
    for name, _, _ in spec:
        for _ in range(30):
            r = subprocess.run(["docker", "exec", name, "redis-cli", "ping"],
                               capture_output=True, text=True)
            if "PONG" in r.stdout:
                break
            time.sleep(1)
        else:
            pytest.fail(f"{name} never answered")
    yield
    for name, _, _ in spec:
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def _client(port, decode=True):
    import redis
    return redis.Redis(host="127.0.0.1", port=port, decode_responses=decode)


def _engine(tmp_path, dst_port=DST_PORT):
    from migkit.engines.redis import RedisEngine
    hop = Hop(name="r", engine="redis",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=dst_port, user="",
                              password=""),
              db_map={"0": "0"})
    hop.report_dir = lambda db=None: tmp_path
    return RedisEngine(hop)


@pytest.fixture
def seeded(servers):
    """A spread of types on the source, copied to the target, so what the
    tests break afterwards is the only difference there is.

    The string carries a quote, a colon and brackets: a repair that went
    through a text encoding somewhere would show up here.
    """
    src, dst = _client(SRC_PORT), _client(DST_PORT)
    for client in (src, dst, _client(OLD_PORT)):
        client.flushall()
    src.set("plain", 'he said "x:y[1]"')
    src.hset("h", mapping={"a": "1", "b": "2"})
    src.rpush("l", "x", "y", "x")
    src.sadd("st", "p", "q")
    src.zadd("z", {"m": 1.5, "n": -2})
    src.set("with_ttl", "v", ex=600)
    raw_src, raw_dst = _client(SRC_PORT, False), _client(DST_PORT, False)
    for key in src.keys("*"):
        payload = raw_src.dump(key.encode())
        ttl = raw_src.pttl(key.encode())
        raw_dst.restore(key.encode(), ttl if ttl > 0 else 0, payload,
                        replace=True)
    assert sorted(src.keys("*")) == sorted(dst.keys("*"))
    yield src, dst


def _repair(eng, db="0"):
    actions = eng.repair_plan(db, "rows")
    for action in actions:
        eng.apply(db, action)
    return actions


def test_the_three_kinds_of_difference_are_put_right(seeded, tmp_path):
    src, dst = seeded
    dst.delete("l")                       # missing
    dst.set("plain", "wrong")             # changed, and a type change with it
    dst.set("stray_from_an_earlier_load", "x")   # extra

    eng = _engine(tmp_path)
    before = eng.check_data("0")
    assert any(r.status == "diff" for r in before), before
    assert sorted(p.name for p in tmp_path.glob("data-db0.*")) == [
        "data-db0.changed", "data-db0.extra", "data-db0.missing"]

    actions = _repair(eng)
    assert len(actions) == 1, actions
    assert "1 missing, 1 changed, 1 extra" in actions[0].note
    assert any(s.startswith("RESTORE") for s in actions[0].statements)
    assert any(s.startswith("DEL") for s in actions[0].statements)

    after = eng.check_data("0")
    assert [r.status for r in after] == ["ok"], [(r.scope, r.detail)
                                                 for r in after]
    assert dst.type("l") == "list"
    assert dst.lrange("l", 0, -1) == ["x", "y", "x"]
    assert dst.get("plain") == 'he said "x:y[1]"'
    assert not dst.exists("stray_from_an_earlier_load")


def test_the_expiry_comes_with_the_value(seeded, tmp_path):
    """A repair that dropped the ttl would leave a key serving stale data
    forever, and the data check does not compare expiries."""
    src, dst = seeded
    dst.set("with_ttl", "stale")          # different value, and no expiry
    assert dst.ttl("with_ttl") == -1

    eng = _engine(tmp_path)
    eng.check_data("0")
    _repair(eng)
    assert dst.get("with_ttl") == "v"
    assert 500 < dst.ttl("with_ttl") <= 600, dst.ttl("with_ttl")


def test_everything_it_overwrites_or_deletes_goes_to_undo_first(seeded,
                                                                tmp_path):
    src, dst = seeded
    dst.set("plain", "target value worth keeping")
    dst.set("stray", "also worth keeping")
    eng = _engine(tmp_path)
    eng.check_data("0")
    _repair(eng)

    undo = tmp_path / "undo" / "db0.keys.jsonl"
    saved = {json.loads(l)["key"]: json.loads(l)
             for l in undo.read_text().splitlines()}
    assert set(saved) == {"plain", "stray"}, sorted(saved)
    # the saved payloads are the target's old values, and they go back
    raw = _client(DST_PORT, False)
    for key, row in saved.items():
        raw.restore(key.encode(), 0, base64.b64decode(row["dump"]),
                    replace=True)
    assert dst.get("plain") == "target value worth keeping"
    assert dst.get("stray") == "also worth keeping"


def test_a_key_missing_on_the_target_has_nothing_to_undo(seeded, tmp_path):
    """There was no old value, so the undo file must not claim there was."""
    src, dst = seeded
    dst.delete("h")
    eng = _engine(tmp_path)
    eng.check_data("0")
    _repair(eng)
    assert dst.hgetall("h") == {"a": "1", "b": "2"}
    undo = tmp_path / "undo" / "db0.keys.jsonl"
    keys = [json.loads(l)["key"] for l in undo.read_text().splitlines()] \
        if undo.exists() else []
    assert "h" not in keys, keys


def test_nothing_to_repair_when_the_two_sides_agree(seeded, tmp_path):
    eng = _engine(tmp_path)
    got = eng.check_data("0")
    assert [r.status for r in got] == ["ok"], [(r.scope, r.detail)
                                               for r in got]
    assert not list(tmp_path.glob("data-db0.*")), list(tmp_path.iterdir())
    assert eng.repair_plan("0", "rows") == []


def test_a_stale_drilldown_from_an_earlier_run_is_not_acted_on(seeded,
                                                               tmp_path):
    """The file is the repair's input, so a clean run has to clear it -
    otherwise `sync` would delete keys that a later check found to be fine."""
    src, dst = seeded
    dst.set("stray", "x")
    eng = _engine(tmp_path)
    eng.check_data("0")
    assert (tmp_path / "data-db0.extra").exists()

    dst.delete("stray")                   # put right by hand, then re-check
    eng.check_data("0")
    assert not (tmp_path / "data-db0.extra").exists()
    assert eng.repair_plan("0", "rows") == []


def test_only_the_rows_kind_is_answered(seeded, tmp_path):
    src, dst = seeded
    dst.delete("l")
    eng = _engine(tmp_path)
    eng.check_data("0")
    assert eng.repair_plan("0", "sequences") == []
    assert eng.repair_plan("0", "rows")
    assert eng.repair_plan("0", "all")


def test_an_older_target_is_refused_rather_than_half_written(seeded,
                                                             tmp_path):
    """The measurement in this file's docstring, through the repair itself.
    Nothing may be deleted on the way out."""
    src, _ = seeded
    old = _client(OLD_PORT)
    old.set("stray_on_the_old_one", "x")
    eng = _engine(tmp_path, dst_port=OLD_PORT)
    eng.check_data("0")
    actions = eng.repair_plan("0", "rows")
    assert actions, "nothing to repair, so the refusal cannot be shown"

    with pytest.raises(SystemExit) as caught:
        eng.apply("0", actions[0])
    said = str(caught.value)
    assert "payload version" in said, said
    assert "older Redis" in said, said
    assert "riot replicate" in said or "redis-shake" in said, said
    # the deletions come last, so the stray is still there to be seen
    assert old.exists("stray_on_the_old_one")
