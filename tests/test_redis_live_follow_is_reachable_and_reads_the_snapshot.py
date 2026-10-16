"""A Redis tail a run starts follows live (`tail_apply` -> `follow_live`),
fills a full start from the snapshot the source sends its replica stream
(read by `RdbReader`, the scan the fallback `decide.redis_seed` names),
reads its sweep without touching the source's access history, follows a
cluster one stream a shard at offsets of their own, and reads each site's
writes into the records the two-site rules take (backlog P0-PARITY.tail on
Redis, LT.C4, LT.C6, W12C's leftovers). Proved over scripted bytes and
fakes, no server."""
import io
import struct

import pytest

from migkit import decide, twoway
from migkit.engines import redis as R


def ln(n):
    if n < 64:
        return bytes([n])
    if n < 16384:
        return bytes([0x40 | (n >> 8), n & 0xFF])
    return b"\x80" + n.to_bytes(4, "big")


def st(b):
    return ln(len(b)) + b


def listpack(items):
    body = b""
    for x in items:
        entry = bytes([0x80 | len(x)]) + x
        body += entry + bytes([len(entry)])
    blob = body + b"\xff"
    return (len(blob) + 6).to_bytes(4, "little") + \
        len(items).to_bytes(2, "little") + blob


def rdb(*parts, version=11, good=True):
    body = b"REDIS%04d" % version + b"".join(parts) + b"\xff"
    crc = R.crc64(body)
    return body + (crc if good else crc ^ 1).to_bytes(8, "little")


SNAPSHOT = rdb(
    b"\xfa" + st(b"redis-ver") + st(b"7.2.4"),
    b"\xfe" + ln(0), b"\xfb" + ln(4) + ln(1),
    b"\xfc" + (1893456000000).to_bytes(8, "little") + b"\x00" + st(b"s")
    + st(b"hello"),
    b"\xf8" + ln(42) + b"\x00" + st(b"n") + b"\xc0\x7b",
    b"\x02" + st(b"plain") + ln(2) + st(b"a") + st(b"b"),
    b"\x05" + st(b"z") + ln(1) + st(b"m") + struct.pack("<d", 2.5),
    b"\x10" + st(b"h") + st(listpack([b"f", b"v"])),
    b"\xfe" + ln(1),
    b"\x0b" + st(b"ints") + st((2).to_bytes(4, "little")
                               + (2).to_bytes(4, "little")
                               + (5).to_bytes(2, "little", signed=True)
                               + (-7).to_bytes(2, "little", signed=True)),
    b"\x14" + st(b"lp") + st(listpack([b"x", b"y"])))


def test_the_checksum_is_redis_own():
    assert R.crc64(b"123456789") == 0xE9C6D914C4B8D9CA


def test_a_compressed_string_comes_back_whole():
    assert R.lzf_decompress(b"\x02abc\x20\x02", 6) == b"abcabc"
    with pytest.raises(R.RdbUnread):
        R.lzf_decompress(b"\x02abc\x20\x02", 7)


def test_the_snapshot_reads_into_keys_with_types_ttls_and_values():
    r = R.RdbReader(io.BytesIO(SNAPSHOT))
    got = {(e["db"], e["key"]): e for e in r.entries()}
    assert r.version == 11
    assert set(got) == {(0, b"s"), (0, b"n"), (0, b"plain"), (0, b"z"),
                        (0, b"h"), (1, b"ints"), (1, b"lp")}
    s = got[(0, b"s")]
    assert (s["kind"], s["value"], s["expire"]) == ("string", b"hello",
                                                    1893456000000)
    assert got[(0, b"n")]["value"] == b"123"
    assert got[(0, b"n")]["idle"] == 42 and got[(0, b"n")]["expire"] is None
    assert got[(0, b"z")]["value"] == [(b"m", 2.5)]
    assert got[(0, b"h")]["value"] == [(b"f", b"v")]
    assert got[(1, b"ints")]["value"] == [b"5", b"-7"]
    assert got[(1, b"lp")] ["kind"] == "set"
    assert got[(1, b"lp")]["value"] == [b"x", b"y"]
    # the serialised form RESTORE takes: type, value, version, checksum
    raw = got[(0, b"s")]["raw"]
    assert raw == b"\x00" + st(b"hello")
    p = R.dump_payload(raw, 11)
    assert p[-10:-8] == (11).to_bytes(2, "little")
    assert int.from_bytes(p[-8:], "little") == R.crc64(p[:-8])


def test_a_snapshot_not_read_whole_is_said_and_never_half_trusted():
    with pytest.raises(R.RdbUnread, match="checksum"):
        list(R.RdbReader(io.BytesIO(rdb(b"\x00" + st(b"k") + st(b"v"),
                                        good=False))).entries())
    with pytest.raises(R.RdbUnread, match="type 6"):
        list(R.RdbReader(io.BytesIO(rdb(b"\x06" + st(b"k")))).entries())
    with pytest.raises(R.RdbUnread, match="past"):
        list(R.RdbReader(io.BytesIO(rdb(b"\x00" + st(b"k") + st(b"v" * 90))),
                         most=64).entries())


FIT = {"snapshot bytes": 10 ** 6, "local free": 10 ** 9,
       "local memory": 10 ** 9, "memory needs": 10 ** 8}


def test_the_first_fill_is_decided_by_size_memory_and_what_read():
    assert decide.redis_seed(FIT).rung.name == "snapshot"
    short = decide.redis_seed(dict(FIT, **{"local free": 10}))
    assert short.rung.name == "scan" and "free" in short.reason
    blind = decide.redis_seed(dict(FIT, **{"local memory": None}))
    assert blind.rung.name == "scan" and "not read" in blind.reason
    unread = decide.redis_seed(dict(FIT, unread="its checksum does not"
                                    " match what was read"))
    assert unread.rung.name == "scan"


def test_the_spool_lets_go_past_the_room_measured(tmp_path):
    sp = R._Spool(tmp_path, 10, None)
    sp.write(b"12345")
    assert sp.why is None
    sp.write(b"123456")
    assert "past the 10 bytes" in sp.why
    sp.close()


class Pipe:
    def __init__(self, log, refuse=None):
        self.log, self.refuse, self.q = log, refuse, []

    def __getattr__(self, name):
        return lambda *a, **k: self.q.append((name, a, k))

    def execute(self, raise_on_error=True):
        self.log += self.q
        return [self.refuse if self.refuse and n == "execute_command"
                else 1 for n, _, _ in self.q]


def test_a_target_refusing_the_serialised_form_is_given_typed_writes(
        monkeypatch):
    eng = object.__new__(R.RedisEngine)
    eng.__dict__["_is_cluster"] = {"src": False, "dst": False}
    sent, pipes = [], []

    class Dst:
        def pipeline(self, transaction=False):
            pipes.append(Pipe(sent, Exception("DUMP payload version or"
                                              " checksum are wrong")
                              if len(pipes) == 0 else None))
            return pipes[-1]
    monkeypatch.setattr(R.RedisEngine, "_keys", lambda self, *a, **k: Dst())
    batch = [e for e in R.RdbReader(io.BytesIO(SNAPSHOT)).entries()
             if e["db"] == 0]
    out = {"0": dict.fromkeys(R.KEY_EVENTS, 0)}
    eng._land("0", batch, {"abs": True, "history": "idletime"}, 11, out)
    names = [n for n, _, _ in sent]
    assert names.count("execute_command") == len(batch)   # RESTORE first
    assert {"set", "sadd", "zadd", "hset", "pexpireat"} <= set(names)
    assert eng.__dict__["_plain_seed"] is True
    assert out["0"]["added"] == len(batch)


def test_a_stream_resumed_knows_the_database_it_was_in():
    state = {}
    a = R.KeyspaceStates(state, lambda m: None)
    a.route(10, [b"select", b"3"], lambda argv: [])
    a.reached(10)
    assert R.KeyspaceStates(state, lambda m: None).db == "3"


def test_the_run_reaches_follow_live(monkeypatch, tmp_path):
    from migkit import tailctl
    assert hasattr(R.RedisEngine, "tail_apply")
    eng = object.__new__(R.RedisEngine)
    eng.hop = type("Hop", (), {"source": type("E", (), {"password": ""}),
                               "target": type("E", (), {"password": ""})})()
    called = []

    def follow(self, db, state, save, log, stop=lambda: False, rounds=None):
        called.append(db)
        state["rounds"] = 3
    monkeypatch.setattr(R.RedisEngine, "follow_live", follow)

    def loop(where, db, look, log, noun, **k):
        st, went, caught = look(None)
        assert went == 3 and caught is True
    monkeypatch.setattr(tailctl, "run_loop", loop)
    eng.tail_apply("0", True, tmp_path / "tail.json", lambda m: None)
    assert called == ["0"]


def test_the_sweep_says_when_its_reads_touch_the_source(monkeypatch):
    eng = object.__new__(R.RedisEngine)
    eng.__dict__["_untouched"] = {"src": False}

    class Src:
        def time(self):
            return (1000, 0)
    monkeypatch.setattr(R.RedisEngine, "_client", lambda self, *a, **k: Src())
    monkeypatch.setattr(R.RedisEngine, "_sweep_once",
                        lambda self, db, since, log: {})
    said = []
    eng._sweep_rounds("0", {}, lambda: None, said.append, lambda: False,
                      rounds=1)
    assert any("access" in s and "7.2" in s for s in said)
    eng2 = object.__new__(R.RedisEngine)
    eng2.__dict__["_untouched"] = {"src": True}
    said = []
    eng2._sweep_rounds("0", {}, lambda: None, said.append, lambda: False,
                       rounds=1)
    assert not any("access" in s for s in said)


def test_a_cluster_is_followed_one_stream_a_shard(monkeypatch):
    eng = object.__new__(R.RedisEngine)
    eng.__dict__["_is_cluster"] = {"src": True}
    Node = type("N", (), {})
    a, b = Node(), Node()
    a.host, a.port, b.host, b.port = "10.0.0.1", 7000, "10.0.0.2", 7000
    cache = {0: [a], 1: [a], 8192: [b]}
    rc = type("RC", (), {"nodes_manager": type("NM", (), {
        "slots_cache": cache})()})()
    monkeypatch.setattr(R.RedisEngine, "_keys", lambda self, *a, **k: rc)
    assert [s[:3] for s in eng._shards()] == [("0", "10.0.0.1", 7000),
                                               ("8192", "10.0.0.2", 7000)]
    monkeypatch.setattr(R.RedisEngine, "_node_client",
                        lambda self, h, p, decode=True: (h, p))
    monkeypatch.setattr(R.RedisEngine, "live_way",
                        lambda self, f=None, client=None:
                        decide.redis_live({"replica grant": True,
                                           "headroom": 10, "stream needs": 1,
                                           "idletime": True}))
    ran = []

    def stream(self, db, state, save, log, stop, rounds=None, node=None,
               guard=None):
        with guard:
            state.setdefault("keyspaces", {})["offset"] = 100 + int(node[0])
            ran.append(node[0])
            save()
    monkeypatch.setattr(R.RedisEngine, "_stream_rounds", stream)
    state = {"shards": {"16000": {"keyspaces": {"offset": 5}}}}
    eng.follow_live("0", state, lambda: None, lambda m: None)
    assert sorted(ran) == ["0", "8192"]
    assert state["shards"]["0"]["keyspaces"]["offset"] == 100
    assert state["shards"]["8192"]["keyspaces"]["offset"] == 8292
    assert "16000" not in state["shards"]


def spec(argv):
    return [argv[1]]


spec.categories = lambda name: {"@list"} if name == b"rpush" else set()


def test_each_site_stream_becomes_the_two_site_records():
    c = R.SiteChanges(spec, 1000, 0)
    c.clock = lambda: 1000
    for at, argv in enumerate(([b"select", b"0"], [b"incrby", b"c", b"5"],
                               [b"sadd", b"s", b"x"], [b"rpush", b"l", b"1"],
                               [b"set", b"v", b"1"], [b"del", b"g"],
                               [b"flushdb"])):
        c.route(at, argv)
    kinds = {r["key"]: r["kind"] for r in c.records}
    assert kinds == {b"c": "counter", b"s": "set", b"l": "list",
                     b"v": "value", b"g": "removed"}
    assert [r["delta"] for r in c.records if r["kind"] == "counter"] == [5]
    assert len(c.held) == 1 and "FLUSHDB" in c.held[0]["why"]
    c.valued(lambda d, keys: {b"v": b"payload", b"l": None})
    assert [r for r in c.records if r["key"] == b"v"][0]["value"] == \
        b"payload"
    assert c.records[-1]["key"] == b"l" and c.records[-1]["kind"] == \
        "removed"
    other = R.SiteChanges(spec, 1000, 0)
    other.route(9, [b"incrby", b"c", b"2"])
    out = twoway.redis_two_site(c.records, other.records, skew_ms=1,
                                log=lambda m: None)
    assert ("0", b"c", ("incrby", 2)) in out["to_a"]
    assert ("0", b"c", ("incrby", 5)) in out["to_b"]


def test_a_hop_keeping_deletes_is_filled_by_scan():
    kept = decide.redis_seed(dict(FIT, **{"every change carried": False}))
    assert kept.rung.name == "scan" and "keeps" in kept.reason
