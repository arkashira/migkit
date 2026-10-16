"""A Redis source followed without loss past the copy (LT.C6,
P0-PARITY.tail on Redis): migkit reads the replica protocol's own stream -
the snapshot first, then every write with its offset - and lands each key
as the source holds it; a stop resumes from the offset saved, said as a
partial resync where the source's backlog still holds it and a full one
where it does not, and the target ends equal to the source key by key and
value by value either way. A source that refuses the stream to the account
(a managed service that keeps replication to itself) is followed by the
idle-time sweep instead, chosen by the decision engine on the grant it
read, and the sweep says the one thing it cannot see: a change made and
undone between two rounds.

Against real servers started from `redis-server` on this machine
(`tests/redis_local.py`), never a real database."""
import threading
import time

from migkit import decide
from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)

PROVES = [("tail.follows", "redis"), ("tail.table-states", "redis")]

SKIP = {b"migkit:origin", b"wake"}


def _writes(c, lo, hi):
    """Writes `lo`..`hi` on the source, one command each: strings set and
    deleted, counters stepped, set members and hash fields added, expiries
    put on."""
    p = c.pipeline(transaction=False)
    for i in range(lo, hi):
        kind = i % 5
        if kind == 0:
            p.set(f"s{i % 3000}", f"v{i}")
        elif kind == 1:
            p.incr(f"n{i % 500}")
        elif kind == 2:
            p.sadd(f"set{i % 200}", i)
        elif kind == 3:
            p.hset(f"h{i % 300}", f"f{i % 7}", i)
        elif i % 10 == 9:
            p.delete(f"s{(i // 10) % 3000}")
        else:
            p.expire(f"h{i % 300}", 1000 + i)
        if len(p) >= 500:
            p.execute()
    p.execute()


def _value(c, k):
    t = c.type(k)
    if t == b"string":
        v = c.get(k)
    elif t == b"set":
        v = sorted(c.smembers(k))
    elif t == b"hash":
        v = sorted(c.hgetall(k).items())
    elif t == b"list":
        v = c.lrange(k, 0, -1)
    elif t == b"zset":
        v = c.zrange(k, 0, -1, withscores=True)
    else:
        v = None
    return t, v, c.ttl(k) > 0


def _same(src, dst):
    a, b = src.client(), dst.client()
    keys = {k for k in a.scan_iter(count=1000)} - SKIP
    held = {k for k in b.scan_iter(count=1000)} - SKIP
    assert keys == held, (len(keys - held), len(held - keys))
    differ = [k for k in keys if _value(a, k) != _value(b, k)]
    assert not differ, differ[:5]
    return len(keys)


def _follow(eng, state, said, stop):
    run = threading.Thread(target=lambda: eng._stream_rounds(
        "0", state, lambda: None, said.append, stop.is_set), daemon=True)
    run.start()
    return run


def _reached(c, state, before=None, wait=60):
    want = int(c.info("replication")["master_repl_offset"])
    end = time.time() + wait
    while time.time() < end:
        at = (state.get("keyspaces") or {}).get("offset") or 0
        if at >= want and at != before:
            return at
        time.sleep(0.05)
    raise AssertionError(f"the follow did not reach offset {want}")


def _end(c, run, stop):
    stop.set()
    c.set("wake", 1)
    run.join(30)
    assert not run.is_alive()


def _start(c, state):
    end = time.time() + 30
    # the stream begun and its start saved (a source whose backlog was
    # made by this replica stands at offset 0)
    while time.time() < end and not (state.get("keyspaces") or {}).get(
            "replid"):
        time.sleep(0.05)


@needs_server
def test_ten_thousand_writes_across_a_stop_and_a_partial_resume_land_whole(
        clean, tmp_path):
    src, _ = clean
    c = src.client()
    _writes(c, 0, 1000)
    state, said = {}, []
    eng, stop = engine(clean, tmp_path), threading.Event()
    run = _follow(eng, state, said, stop)
    _start(c, state)
    _writes(c, 1000, 5000)
    _reached(c, state)
    _end(c, run, stop)
    eng.close_streams()
    stopped_at = state["keyspaces"]["offset"]
    # written while nothing follows: the source's backlog holds it
    _writes(c, 5000, 7500)
    again, stop = engine(clean, tmp_path), threading.Event()
    run = _follow(again, state, said, stop)
    _writes(c, 7500, 11000)
    _reached(c, state, before=stopped_at)
    _end(c, run, stop)
    again.close_streams()
    assert any(f"resumed at offset {stopped_at:,}" in s
               and "partial resync" in s for s in said), said
    assert _same(*clean) > 700


@needs_server
def test_a_resume_past_the_backlog_is_said_as_a_full_resync_and_lands_whole(
        clean, tmp_path):
    src, _ = clean
    c = src.client()
    c.config_set("repl-backlog-size", 16384)
    _writes(c, 0, 500)
    state, said = {}, []
    eng, stop = engine(clean, tmp_path), threading.Event()
    run = _follow(eng, state, said, stop)
    _start(c, state)
    _writes(c, 500, 2000)
    _reached(c, state)
    _end(c, run, stop)
    eng.close_streams()
    stopped_at = state["keyspaces"]["offset"]
    # more written than the backlog keeps
    p = c.pipeline(transaction=False)
    for i in range(400):
        p.set(f"big{i}", "x" * 200)
    p.execute()
    _writes(c, 2000, 4000)
    again, stop = engine(clean, tmp_path), threading.Event()
    run = _follow(again, state, said, stop)
    _start(c, state)
    _writes(c, 4000, 5000)
    _reached(c, state, before=stopped_at)
    _end(c, run, stop)
    again.close_streams()
    assert any(f"no longer holds offset {stopped_at:,}" in s
               and "full resync" in s for s in said), said
    assert _same(*clean) > 700


@needs_server
def test_a_source_refusing_the_stream_is_swept_and_says_what_a_sweep_misses(
        clean, tmp_path):
    src, dst = clean
    c = src.client()
    c.execute_command("ACL", "SETUSER", "follower", "on", ">CHANGE_ME", "~*",
                      "&*", "+@all", "-psync", "-sync")
    eng = engine(clean, tmp_path)
    eng.hop.source.user, eng.hop.source.password = "follower", "CHANGE_ME"
    choice = eng.live_way()
    assert choice.rung.name == "idle-time sweep"
    assert eng.live_facts()["replica grant"] is False
    _writes(c, 0, 2000)
    state, said = {}, []
    eng._sweep_rounds("0", state, lambda: None, said.append, lambda: False,
                      rounds=1)
    _same(src, dst)
    time.sleep(1.1)
    _writes(c, 2000, 4000)
    c.set("brief", 1)
    c.delete("brief")
    eng._sweep_rounds("0", state, lambda: None, said.append, lambda: False,
                      rounds=1)
    assert _same(src, dst) > 700
    assert not dst.client().exists("brief")
    bound = [s for s in said if "made and undone between two rounds" in s]
    assert bound and "written then deleted" in bound[0]


def test_the_stream_is_ranked_over_the_sweep_only_on_the_grant_and_room():
    base = {"replica grant": True, "cluster": False, "idletime": True,
            "headroom": 10 ** 9, "stream needs": 10 ** 6}
    assert decide.redis_live(base).rung.name == "replica stream"
    refused = decide.redis_live(dict(base, **{"replica grant": False}))
    assert refused.rung.name == "idle-time sweep"
    tight = decide.redis_live(dict(base, headroom=10))
    assert tight.rung.name == "idle-time sweep"
    lfu = decide.redis_live(dict(base, **{"replica grant": False,
                                          "idletime": False}))
    assert lfu.rung is None and "idle times" in lfu.reason
