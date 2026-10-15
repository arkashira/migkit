"""A Redis tail kept per key space at the exact seam of the source's
replication offset, its live way chosen between the replica stream and the
idle-time sweep, and two sites both written made alike by rules chosen from
the measured overlap (backlog P0-PARITY.tail on Redis, LT.C6, LT.C4).

The stream's reader, the key-space states, the choices and the two-site
rules are proved here without a server, over scripted bytes; the stream
against a real source, its offsets, its resume by offset and the tail's
end-to-end landing on a real target, where `redis-server` is on the machine
(`tests/redis_local.py`)."""
import threading
import time

import pytest

from migkit import decide, twoway
from migkit.engines import redis as R
from tests.redis_local import (  # noqa: F401 - fixtures
    clean, engine, needs_server, pair)


def resp(*argv):
    out = b"*%d\r\n" % len(argv)
    for a in argv:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Sock:
    """A source's side of the connection, its bytes handed out in small
    pieces so a command split across reads is met."""

    def __init__(self, data, piece=7):
        self.data, self.piece, self.sent = bytearray(data), piece, b""

    def recv(self, n):
        out = bytes(self.data[:min(n, self.piece)])
        del self.data[:len(out)]
        return out

    def sendall(self, b):
        self.sent += b

    def getsockname(self):
        return ("127.0.0.1", 40000)

    def close(self):
        pass


def _read(stream, n):
    got = []
    for argv in stream.commands():
        got.append((stream.offset, argv))
        if len(got) == n:
            return got


def test_a_full_start_counts_each_command_from_the_snapshot_offset():
    cmds = [resp("SELECT", 0), resp("SET", "k", "v"), resp("RENAME", "k", "j")]
    s = R.ReplicaStream(Sock(b"+OK\r\n+OK\r\n\n+FULLRESYNC abc 100\r\n\n"
                             b"$5\r\nREDIS" + b"".join(cmds)))
    snap = []
    got = s.begin(sink=snap.append)
    assert got == {"kind": "full", "replid": "abc", "offset": 100}
    assert b"".join(snap) == b"REDIS"
    assert b"PSYNC\r\n$1\r\n?\r\n$2\r\n-1" in s.sent
    read = _read(s, 3)
    ends = [100 + sum(len(c) for c in cmds[:i + 1]) for i in range(3)]
    assert [o for o, _ in read] == ends
    assert read[2][1] == [b"RENAME", b"k", b"j"]


def test_a_saved_offset_continues_from_the_byte_after_it():
    s = R.ReplicaStream(Sock(b"+OK\r\n+OK\r\n+CONTINUE abc\r\n"
                             + resp("DEL", "x")))
    got = s.begin("abc", 150)
    assert got["kind"] == "continue" and got["offset"] == 150
    assert resp("PSYNC", "abc", 151) in s.sent
    assert _read(s, 1) == [(150 + len(resp("DEL", "x")), [b"DEL", b"x"])]


def test_a_diskless_snapshot_is_read_to_its_end_mark():
    mark = b"m" * 40
    s = R.ReplicaStream(Sock(b"+OK\r\n+OK\r\n+FULLRESYNC r 7\r\n$EOF:" + mark
                             + b"\r\nPAYLOAD-BYTES" + mark
                             + resp("SET", "a", "1"), piece=5))
    snap = []
    s.begin(sink=snap.append)
    assert b"".join(snap) == b"PAYLOAD-BYTES"
    assert _read(s, 1)[0][0] == 7 + len(resp("SET", "a", "1"))


def test_a_source_that_refuses_the_stream_says_so_as_a_fact():
    s = R.ReplicaStream(Sock(b"+OK\r\n+OK\r\n-NOPERM no psync\r\n"))
    with pytest.raises(R.LiveRefused) as e:
        s.begin()
    assert e.value.facts == {"replica grant": False}


def test_a_getack_is_not_a_write_but_moves_the_offset():
    ack = resp("REPLCONF", "GETACK", "*")
    s = R.ReplicaStream(Sock(b"+OK\r\n+OK\r\n+CONTINUE\r\n" + ack
                             + resp("SET", "a", "1")))
    s.begin("r", 0)
    off, argv = _read(s, 1)[0]
    assert argv[0] == b"SET" and off == len(ack) + len(resp("SET", "a", "1"))


SPEC = {b"set": (1, 1, 1), b"del": (1, -1, 1), b"rename": (1, 2, 1),
        b"expire": (1, 1, 1), b"sadd": (1, 1, 1)}


def spec(argv):
    got = SPEC.get(argv[0].lower())
    if got is None:
        return None
    first, last, step = got
    end = len(argv) + last if last < 0 else last
    return list(argv[first:end + 1:step])


def _spaces(said=None):
    state = {}
    return state, R.KeyspaceStates(state, (said or []).append)


def test_a_space_joins_at_its_seam_and_its_changes_go_by_kind():
    state, ks = _spaces()
    assert ks.boundary({"0": 3}, 10) == ["0"]
    assert ks.spaces["0"]["state"] == "joining"
    assert ks.spaces["0"]["seam"] == 10
    ks.copied("0", {})
    assert ks.spaces["0"]["state"] == "live"
    ks.route(20, [b"SET", b"a", b"1"], spec)
    ks.route(30, [b"RENAME", b"a", b"b"], spec)
    ks.route(40, [b"EXPIRE", b"b", b"9"], spec)
    assert ks.take() == {"0": {b"a": "rename", b"b": "ttl"}}
    ks.landed("0", {"added": 1, "renamed": 1, "ttl": 1})
    ks.reached(40)
    assert state["offset"] == 40
    assert "1 renamed" in ks.said() and "1 ttl" in ks.said()


def test_a_transaction_lands_whole():
    _, ks = _spaces()
    ks.boundary({"0": 1}, 0)
    ks.copied("0", {})
    ks.route(1, [b"MULTI"], spec)
    ks.route(2, [b"SET", b"a", b"1"], spec)
    assert not ks.ready()
    ks.route(3, [b"EXEC"], spec)
    assert ks.ready() and ks.take() == {"0": {b"a": "write"}}


def test_a_flush_or_an_unknown_write_parks_the_space_to_be_copied_again():
    _, ks = _spaces()
    ks.boundary({"0": 1, "1": 1}, 0)
    for d in ("0", "1"):
        ks.copied(d, {})
    ks.route(5, [b"SET", b"a", b"1"], spec)
    ks.route(6, [b"FLUSHDB"], spec)
    ks.route(7, [b"SELECT", b"1"], spec)
    ks.route(8, [b"WEIRDWRITE", b"x"], spec)
    assert ks.parked() == ["0", "1"]
    assert ks.take() == {}
    assert ks.rejoin(9) == ["0", "1"]
    assert all(ks.spaces[d]["seam"] == 9 for d in ("0", "1"))


def test_a_space_back_in_the_scope_reads_only_its_changes_since():
    _, ks = _spaces()
    ks.boundary({"0": 2, "1": 10}, 0)
    for d in ("0", "1"):
        ks.copied(d, {})
    ks.boundary({"0": 2}, 5)
    assert ks.spaces["1"]["state"] == "left"
    ks.route(6, [b"SELECT", b"1"], spec)
    ks.route(7, [b"DEL", b"a", b"b"], spec)
    assert ks.boundary({"0": 2, "1": 8}, 8) == []
    assert ks.spaces["1"]["state"] == "live"
    assert ks.take() == {"1": {"a": "write", "b": "write"}}


def test_a_space_that_changed_more_than_it_held_is_copied_whole():
    _, ks = _spaces()
    ks.boundary({"0": 1, "1": 2}, 0)
    for d in ("0", "1"):
        ks.copied(d, {})
    ks.boundary({"0": 1}, 5)
    ks.route(6, [b"SELECT", b"1"], spec)
    ks.route(7, [b"DEL", b"a", b"b", b"c"], spec)
    assert ks.boundary({"0": 1, "1": 2}, 9) == ["1"]
    assert ks.spaces["1"]["state"] == "joining"


def test_a_stream_that_cannot_continue_copies_every_space_again():
    state, ks = _spaces()
    ks.boundary({"0": 1}, 0)
    ks.copied("0", {})
    state.update(replid="r", offset=50)
    again = ks.started({"kind": "full", "replid": "s", "offset": 90})
    assert again == ["0"] and ks.spaces["0"]["seam"] == 90
    assert state["replid"] == "s"
    _, fresh = _spaces()
    assert fresh.started({"kind": "full", "replid": "s", "offset": 1}) == []


def test_each_change_is_told_by_the_types_before_and_after():
    assert R.key_event("write", "none", "string") == "added"
    assert R.key_event("write", "string", "none") == "removed"
    assert R.key_event("write", "string", "hash") == "retyped"
    assert R.key_event("rename", "set", "set") == "renamed"
    assert R.key_event("ttl", "zset", "zset") == "ttl"
    assert R.key_event("write", "none", "none") == "written"


LIVE = {"replica grant": True, "cluster": False, "headroom": 10 << 20,
        "stream needs": 1 << 20, "idletime": True}


def test_the_stream_is_chosen_where_it_holds_and_the_sweep_where_not():
    assert decide.redis_live(LIVE).rung.name == "replica stream"
    for change in ({"replica grant": False}, {"cluster": True},
                   {"headroom": 1}, {"stream needs": None},
                   {"continued": False}):
        assert decide.redis_live(dict(LIVE, **change)).rung.name \
            == "idle-time sweep", change
    none = decide.redis_live(dict(LIVE, cluster=True, idletime=False))
    assert none.rung is None
    assert "idle times" in none.reason and "cluster" in none.reason


def test_a_refused_stream_is_decided_again_with_what_it_showed(monkeypatch):
    eng = object.__new__(R.RedisEngine)
    monkeypatch.setattr(R.RedisEngine, "live_facts", lambda self: dict(LIVE))

    def refuse(self, *a, **k):
        raise R.LiveRefused("refused", **{"replica grant": False})
    monkeypatch.setattr(R.RedisEngine, "_stream_rounds", refuse)
    monkeypatch.setattr(R.RedisEngine, "_sweep_rounds",
                        lambda self, *a, **k: "swept")
    said = []
    assert eng.follow_live("0", {}, lambda: None, said.append) == "swept"
    assert any("choosing again" in s for s in said)


def test_a_stream_that_breaks_twice_at_one_offset_stops_there(monkeypatch):
    eng = object.__new__(R.RedisEngine)
    monkeypatch.setattr(R.RedisEngine, "live_facts", lambda self: dict(LIVE))

    def broken(self, *a, **k):
        raise R.LiveRefused("broke", continued=True)
    monkeypatch.setattr(R.RedisEngine, "_stream_rounds", broken)
    with pytest.raises(SystemExit, match="fails again"):
        eng.follow_live("0", {"keyspaces": {"offset": 7}}, lambda: None,
                        lambda s: None)


def ch(key, kind, t, **more):
    return dict(db="0", key=key, kind=kind, time=t, offset=t, **more)


def _settled_or_logged(out, a, b):
    both = {(c["db"], c["key"]) for c in a} & {(c["db"], c["key"]) for c in b}
    seen = set(out["merged"]) | set(out["newest"]) | {
        (c["db"], c["key"]) for c in out["conflicts"]}
    assert both == seen


def test_no_overlap_carries_each_sites_writes_across():
    a, b = [ch("x", "value", 1, value="1")], [ch("y", "removed", 2)]
    out = twoway.redis_two_site(a, b, log=lambda s: None)
    assert out["choice"].rung.name == "apart"
    assert out["to_b"] == [("0", "x", ("put", "1"))]
    assert out["to_a"] == [("0", "y", ("del",))]


def test_counters_sum_and_sets_keep_a_concurrent_add():
    a = [ch("n", "counter", 1, delta=3), ch("n", "counter", 2, delta=2),
         ch("s", "set", 3, added=["m"], gone=["q"])]
    b = [ch("n", "counter", 4, delta=-1), ch("s", "set", 5, gone=["m"])]
    out = twoway.redis_two_site(a, b, log=lambda s: None)
    assert out["choice"].rung.name == "merged"
    assert ("0", "n", ("incrby", -1)) in out["to_a"]
    assert ("0", "n", ("incrby", 5)) in out["to_b"]
    # b removed m while a added it: the add stays on both
    assert ("0", "s", ("set", [], [])) in out["to_a"]
    assert ("0", "s", ("set", ["m"], ["q"])) in out["to_b"]
    _settled_or_logged(out, a, b)


def test_the_later_write_wins_only_beyond_the_measured_skew():
    a = [ch("k", "value", 1000, value="a"), ch("j", "value", 1000,
                                               value="a")]
    b = [ch("k", "value", 1600, value="b"), ch("j", "value", 1100,
                                               value="b")]
    said = []
    out = twoway.redis_two_site(a, b, skew_ms=200, log=said.append)
    assert out["choice"].rung.name == "merged, else newest"
    assert out["to_a"] == [("0", "k", ("put", "b"))]
    assert out["to_b"] == []
    assert [c["key"] for c in out["conflicts"]] == ["j"]
    assert any("j: a conflict" in s for s in said)
    _settled_or_logged(out, a, b)


def test_unmeasured_clocks_hold_every_unmergeable_key_and_log_it():
    a = [ch("k", "value", 1, value="a"), ch("n", "counter", 1, delta=1)]
    b = [ch("k", "set", 9000, added=["z"]), ch("n", "counter", 2, delta=1)]
    said = []
    out = twoway.redis_two_site(a, b, log=said.append)
    assert out["choice"].rung.name == "merged, else held"
    assert [c["key"] for c in out["conflicts"]] == ["k"]
    assert not any(k[1] == "k" for k in out["to_a"] + out["to_b"])
    assert out["merged"] == [("0", "n")]
    assert len(said) == 1
    _settled_or_logged(out, a, b)


def test_a_list_written_on_both_sites_is_never_overruled():
    a = [ch("q", "list", 1, value=["a"])]
    b = [ch("q", "list", 5000, value=["b"])]
    out = twoway.redis_two_site(a, b, skew_ms=10, log=lambda s: None)
    assert out["to_a"] == out["to_b"] == []
    assert "order" in out["conflicts"][0]["why"]


# --- against a real source and target ------------------------------------

def _stream_to(src):
    import socket
    return R.ReplicaStream(socket.create_connection(("127.0.0.1", src.port),
                                                    timeout=15))


def _until(stream, want):
    got = []
    for argv in stream.commands():
        got.append(argv)
        if stream.offset >= want:
            return got


@needs_server
def test_the_stream_offsets_match_the_source_and_resume_by_offset(clean):
    src, _ = clean
    c = src.client()
    s = _stream_to(src)
    first = s.begin()
    c.set("a", 1)
    c.incr("a")
    c.rename("a", "b")
    want = int(c.info("replication")["master_repl_offset"])
    got = _until(s, want)
    assert s.offset == want
    assert [b"RENAME", b"a", b"b"] in got
    s.close()
    c.sadd("later", "m")
    again = _stream_to(src)
    resumed = again.begin(first["replid"], want)
    assert resumed["kind"] == "continue"
    for argv in again.commands():
        if argv[0].upper() != b"PING" and argv[0].upper() != b"SELECT":
            assert argv == [b"SADD", b"later", b"m"]
            break
    again.close()


@needs_server
def test_a_followed_space_lands_every_change_and_counts_it(clean, tmp_path):
    src, dst = clean
    eng = engine(clean, tmp_path)
    c = src.client()
    c.set("gone", 1)
    c.set("kept", 1)
    c.set("retype", "s")
    state, said, stop = {}, [], threading.Event()
    run = threading.Thread(target=lambda: eng._stream_rounds(
        "0", state, lambda: None, said.append, stop.is_set), daemon=True)
    run.start()
    end = time.time() + 20
    while time.time() < end and not state.get("keyspaces", {}).get("offset"):
        time.sleep(0.1)
    c.delete("gone")
    c.rename("kept", "renamed")
    c.expire("renamed", 500)
    c.delete("retype")
    c.sadd("retype", "m")
    c.set("new", 1)
    want = int(c.info("replication")["master_repl_offset"])
    while time.time() < end and (state["keyspaces"].get("offset") or 0) < want:
        time.sleep(0.1)
    stop.set()
    c.set("wake", 1)
    run.join(20)
    d = dst.client()
    assert not d.exists("gone") and not d.exists("kept")
    assert d.get("renamed") == b"1" and 0 < d.ttl("renamed") <= 500
    assert d.type("retype") == b"set" and d.get("new") == b"1"
    counts = state["keyspaces"]["spaces"]["0"]["counts"]
    assert counts["removed"] >= 2 and counts["added"] >= 2
    assert counts["ttl"] + counts["renamed"] >= 1 and counts["retyped"] >= 1


@needs_server
def test_the_sweep_reads_again_what_was_written_since_and_removes(clean,
                                                                  tmp_path):
    src, dst = clean
    eng = engine(clean, tmp_path)
    c = src.client()
    for k in ("old", "drop"):
        c.set(k, 1)
        dst.client().set(k, 1)
    since = int(c.time()[0])
    c.set("fresh", 2)
    c.delete("drop")
    tally = eng._sweep_once("0", since, lambda s: None)
    assert dst.client().get("fresh") == b"2"
    assert not dst.client().exists("drop")
    assert tally["added"] == 1 and tally["removed"] == 1
