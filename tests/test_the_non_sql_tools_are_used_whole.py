"""The non-SQL tools are used whole, and chosen by measurement (backlog F7:
the replica-protocol Redis copy, Kafka's cluster mirror, the Cassandra
bulk loader, OpenSearch's snapshot reader, DynamoDB's export and import).

Each wrapped tool's account (`migkit/tooluse.py`) is held against its
pinned release's documented surface both ways, and against what migkit
really passes it: an option written into a configuration or onto a
command line and not called used (or topped up), or called used and
passed nowhere, fails. The Redis and Kafka ladders are climbed on the
hop's measured rates, a way never timed first; a need a way lacks passes
it over with the reason. The per-table ladder of the native engines falls
a rung where a tool fails on a table and never skips a table. None of
this needs a server: the ones that do are in their own files.
"""
import pytest

from migkit import tooluse
from migkit.config import Endpoint, Hop

NEW = ("redis-shake", "connect-mirror-maker.sh", "dsbulk",
       "reindex-from-snapshot", "dynamodb-s3")


def _hop(engine, tmp_path, **kw):
    src_opts = kw.pop("src_opts", {})
    dst_opts = kw.pop("dst_opts", {})
    hop = Hop(name="f7", engine=engine,
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME", options=src_opts),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME", options=dst_opts),
              **kw)
    hop.report_dir = lambda db=None: (tmp_path / db if db else tmp_path)
    return hop


# ---- every account against the surface ------------------------------------

@pytest.mark.parametrize("name", NEW)
def test_every_option_is_used_or_explained(name):
    have = tooluse.surface(name)
    # a surface of nothing would pass for the wrong reason
    assert sum(len(v) for v in have.values()) >= 10, have
    assert tooluse.unexplained(name) == []
    assert tooluse.stale(name) == []
    row = tooluse.tools_used()[name]
    assert row["licence"] in ("MIT", "Apache-2.0")
    assert row["terms"] == "open"


def test_a_missing_reason_on_a_new_tool_is_caught(monkeypatch):
    tool = tooluse.REGISTRY["redis-shake"]
    options = dict(tool.options)
    del options[("advanced", "status_port")]
    monkeypatch.setattr(tool, "options", options)
    assert tooluse.unexplained("redis-shake") == [("advanced",
                                                    "status_port")]


def _used(name, where, states=(tooluse.USED,)):
    return {o for (c, o), (state, _) in tooluse.REGISTRY[name].options
            .items() if c == where and state in states}


# ---- the cluster mirror ---------------------------------------------------

def test_the_mirror_is_given_what_its_account_says(tmp_path):
    from migkit import mirror
    hop = _hop("kafka", tmp_path,
               src_opts={"security_protocol": "SASL_SSL",
                         "sasl_mechanism": "SCRAM-SHA-512",
                         "ssl_cafile": "/etc/ca.pem"})
    props = mirror.properties(hop, ["orders.v1", "audit"], 4, 3)
    written = {mirror.entry_of(k) for k in props}
    for where in ("flow", "worker"):
        got = {o for w, o in written if w == where}
        used = _used("connect-mirror-maker.sh", where)
        allowed = _used("connect-mirror-maker.sh", where,
                        (tooluse.USED, tooluse.TOPPED))
        assert used <= got, (where, used - got)
        assert got <= allowed, (where, got - allowed)
    # nothing is written to the source: its syncs on the target, no
    # heartbeat, and only committed messages read
    assert props["offset-syncs.topic.location"] == "target"
    # the flow back, off, would still run its heartbeat into the source
    # unless its heartbeat is off too (measured)
    assert props["source->target.emit.heartbeats.enabled"] == "false"
    assert props["target->source.emit.heartbeats.enabled"] == "false"
    assert props["emit.heartbeats.enabled"] == "false"
    assert props["target->source.enabled"] == "false"
    assert props["source.consumer.isolation.level"] == "read_committed"
    # a dot in a topic's name is a dot, not any character
    assert props["source->target.topics"] == r"audit,orders\.v1"
    assert "CHANGE_ME" in props["source.sasl.jaas.config"]
    text = mirror.properties_text(props)
    assert "orders\\\\.v1" in text


def test_the_mirrors_own_topics_are_not_the_applications():
    from migkit import mirror
    for t in ("mm2-offset-syncs.source.internal",
              "source.checkpoints.internal", "mm2-offsets.source.internal",
              "mm2-configs.source.internal", "mm2-status.source.internal",
              "mm2-offsets.target.internal", "east.checkpoints.internal"):
        assert mirror.internal(t), t
    for t in ("orders", "checkpoints.internal", "mm2-offsets",
              "heartbeats"):
        assert not mirror.internal(t), t


def test_the_mirrors_tables_are_read_as_its_binary_schema_writes_them():
    """An offset sync's key is a topic and a partition, its value two
    offsets; a checkpoint's value starts with a version. Built here as
    Kafka's `Struct` writes them (a string is its length in two bytes,
    then its bytes)."""
    import json
    import struct

    from migkit import mirror

    def s(text):
        raw = text.encode()
        return struct.pack(">h", len(raw)) + raw
    key = s("orders") + struct.pack(">i", 3)
    assert mirror.offset_sync(key, struct.pack(">qq", 120, 57)) == \
        ("orders", 3, 120, 57)
    ck_key = s("billing") + s("orders") + struct.pack(">i", 1)
    ck_val = struct.pack(">h", 0) + struct.pack(">qq", 40, 12) + s("")
    assert mirror.checkpoint(ck_key, ck_val) == ("billing", "orders", 1,
                                                 40, 12)
    pos = json.dumps(["MirrorSourceConnector",
                      {"cluster": "source", "partition": 2,
                       "topic": "orders"}]).encode()
    assert mirror.saved_position(pos, b'{"offset": 99}') == \
        (("orders", 2), 99)
    other = json.dumps(["MirrorCheckpointConnector", {}]).encode()
    assert mirror.saved_position(other, b'{"offset": 1}') is None


def test_a_sync_is_a_candidate_counted_on_from_the_last_before_it():
    from migkit import mirror
    syncs = [(0, 0), (100, 90), (250, 230)]
    assert mirror.downstream(syncs, 105) == 95
    assert mirror.downstream(syncs, 250) == 230
    assert mirror.downstream(syncs, 400) == 380
    assert mirror.downstream([(10, 5)], 3) is None
    assert mirror.downstream(None, 3) is None


def test_the_mirror_refuses_what_it_cannot_carry(tmp_path):
    from migkit import mirror
    iam = _hop("kafka", tmp_path, src_opts={
        "security_protocol": "SASL_SSL", "sasl_mechanism": "AWS_MSK_IAM"})
    assert "cloud's own credentials" in mirror.unfit(iam)
    both = _hop("kafka", tmp_path,
                src_opts={"schema_registry": "http://10.0.0.1:8081"},
                dst_opts={"schema_registry": "http://10.0.0.2:8081"})
    assert "schema registry of its own" in mirror.unfit(both)
    one = _hop("kafka", tmp_path,
               src_opts={"schema_registry": "http://10.0.0.1:8081"})
    assert mirror.unfit(one) is None
    assert mirror.unfit(_hop("kafka", tmp_path)) is None


# ---- the bulk loader ------------------------------------------------------

def test_the_loader_is_passed_what_its_account_says():
    from migkit import dsbulk
    acct = tooluse.REGISTRY["dsbulk"].options
    for command in ("unload", "load", "count"):
        got = {"-f"}
        for resume in ((None, "/w/logs/LOAD_1/checkpoint.csv")
                       if command != "count" else (None,)):
            argv = dsbulk.args(command, "ks", "t", "/w", 4, resume)
            assert argv[:2] == ["dsbulk", command]
            got |= {a for a in argv[2:] if a.startswith("-")}
            # never an address or a password on its command line
            assert not {"-h", "-u", "-p", "-port"} & set(argv)
        used = {o for (c, o), (st, _) in acct.items()
                if c == command and st == tooluse.USED}
        assert got == used, (command, got ^ used)


def test_the_loaders_closing_lines_are_read():
    from migkit import dsbulk
    out = ("Operation directory: /w/logs/UNLOAD_1\n"
           "total | failed | rows/s | p50ms | p99ms | p999ms\n"
           "1,250 |      2 |    500 |  1.23 |  4.56 |   7.89\n"
           "Operation UNLOAD_1 completed with 2 errors in 2 seconds.\n")
    assert dsbulk.summary(out) == (1250, 2)
    assert dsbulk.summary("no table here\n") is None
    assert dsbulk.counted("Operation directory: x\n1250\n"
                          "Operation COUNT_1 completed successfully\n") \
        == 1250
    assert dsbulk.counted("nothing counted\n") is None


# ---- the replica-protocol copy of Redis -----------------------------------

KEYS = ["user:1", "user:2", "tmp.x", "0.tmp", "cache:a", "0", "0.cache:b",
        "session.9", "a.b.c", "sess", "migkit_origin"]


@pytest.mark.parametrize("exclude", [
    ["user:*"], ["0.cache:*"], ["tmp.*", "sess"], ["a.b.c", "cache:?"],
    ["[st]*"], ["0.tmp"]])
def test_a_filter_says_exactly_what_the_hops_pattern_says(tmp_path,
                                                          exclude):
    """Each key is left out by the program's filters exactly where the hop
    leaves it out - held key by key against `Hop.excluded`."""
    import re

    from migkit import redisshake
    hop = _hop("redis", tmp_path, exclude=exclude)
    regexes, why = redisshake.key_filters(hop, "0")
    if regexes is None:
        # a pattern that could match across the database's name is not
        # guessed at, and says so
        assert "could match across" in why
        assert exclude[0][:1] in "*?[0"
        return
    for key in KEYS:
        blocked = key == "migkit_origin" or any(
            re.fullmatch(r, key) for r in regexes)
        assert blocked == hop.excluded("0", key), (exclude, key, regexes)


def test_a_pattern_that_crosses_the_databases_name_is_not_guessed(tmp_path):
    from migkit import redisshake
    for pat in ("*.tmp", "?.x", "[0-9].cache"):
        got, why = redisshake.key_filters(_hop("redis", tmp_path,
                                               exclude=[pat]), "0")
        assert got is None and pat in why, (pat, why)
    # a database named otherwise cannot start a pattern that starts with 0
    got, why = redisshake.key_filters(_hop("redis", tmp_path,
                                           exclude=["0.tmp"]), "3")
    assert got is not None, why


class _Stub:
    """What `redisshake.config` asks of the Redis engine, answered."""

    def __init__(self, hop, cluster=(False, False)):
        self.hop, self.cl = hop, cluster

    def _cluster(self, side):
        return self.cl[0 if side == "src" else 1]

    def _client(self, side, db=0, decode=True):
        class C:
            def config_get(self, name):
                return {name: {"proto-max-bulk-len": "536870912",
                               "client-query-buffer-limit": "1073741824",
                               "repl-diskless-sync": "yes"}[name]}

            def module_list(self):
                return [{"name": "bf", "ver": 20812}]
        return C()


def test_the_copy_is_given_what_its_account_says(tmp_path, monkeypatch):
    """Every reader, a cluster target, a database other than 0 and a
    target that refuses the source's form: what the configuration file
    carries is what the account calls used (or topped up), and every
    option called used is carried by one of them."""
    import tomllib

    from migkit import redisshake
    from migkit.engines import redis as redis_engine
    hop = _hop("redis", tmp_path, exclude=["tmp:*"],
               src_opts={"rdb_file": "/x/dump.rdb",
                         "aof_file": "/x/appendonly.aof",
                         "aof_timestamp": 1700000000})
    monkeypatch.setattr(redis_engine, "RedisEngine",
                        lambda h: _Stub(h, (True, True)))
    written = set()
    for reader in ("sync", "scan", "rdb", "aof"):
        for refused in (False, True):
            for db in ("0", "3"):
                conf = redisshake.config(hop, db, reader, "/w", 16289, 4,
                                         refused)
                for section, keys in conf.items():
                    written |= {(section, k) for k in keys}
                assert conf["advanced"]["target_redis_proto_max_bulk_len"] \
                    == (0 if refused else 536870912)
                if db == "3":
                    # a cluster target holds database 0 alone
                    assert conf["filter"]["function"].startswith(
                        "shake.call(0")
                else:
                    assert "function" not in conf["filter"]
                # the file the program reads is the configuration as built
                assert tomllib.loads(redisshake.toml_text(conf)) == conf
    acct = tooluse.REGISTRY["redis-shake"].options
    sections = {s for s, _ in written}
    used = {k for k, (st, _) in acct.items()
            if st == tooluse.USED and k[0] in sections}
    assert written <= {k for k, (st, _) in acct.items()
                       if st in (tooluse.USED, tooluse.TOPPED)}, \
        written - set(acct)
    assert used <= written, used - written


def test_a_later_serialised_form_is_named(tmp_path, monkeypatch):
    from migkit import redisshake, variants
    from migkit.engines.redis import RedisEngine
    cases = [(("redis", "7.4.1"), ("valkey", "8.0.2"), True),
             (("redis", "7.2.5"), ("valkey", "8.0.2"), False),
             (("redis", "7.4.1"), ("redis", "7.2.4"), True),
             (("redis", "7.2.4"), ("redis", "7.4.1"), False),
             (("valkey", "9.0.0"), ("redis", "8.0.1"), True),
             (("redis", "9.9.9"), ("redis", "7.2.4"), False)]
    for (sn, sv), (dn, dv), refused in cases:
        brands = (variants.Brand(sn, "redis", sv),
                  variants.Brand(dn, "redis", dv))
        monkeypatch.setattr(RedisEngine, "_brands", lambda self, b=brands: b)
        got = redisshake.older_target(_hop("redis", tmp_path))
        assert bool(got) == refused, ((sn, sv), (dn, dv), got)


# ---- one Redis ladder, one Kafka ladder, on measured rates ---------------

def _redis_ladder(tmp_path, monkeypatch, **facts):
    from migkit import movers, redisshake
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    monkeypatch.setattr(movers, "which", lambda n: f"/bin/{n}")
    monkeypatch.setattr(movers, "_riotx_unfit",
                        lambda hop: facts.get("terms"))
    for name in ("checks_certificates", "evicts", "older_target"):
        monkeypatch.setattr(redisshake, name,
                            lambda hop, n=name: facts.get(n, False) or None
                            if n == "older_target" else facts.get(n, False))
    return _hop("redis", tmp_path, databases=["0"])


def test_the_redis_ladder_tries_each_way_once_then_the_fastest(
        tmp_path, monkeypatch):
    from migkit import bulkways, planner
    hop = _redis_ladder(tmp_path, monkeypatch)
    assert bulkways.fitted(hop, "redis", "riotx") == ("riotx", None)
    planner.record_rate(hop, "riotx", 100_000, 10.0)
    # the way never timed goes before one that was
    assert bulkways.fitted(hop, "redis", "riotx")[0] == "redisshake"
    planner.record_rate(hop, "redisshake", 100_000, 4.0)
    assert bulkways.fitted(hop, "redis", "riotx")[0] == "builtin"
    planner.record_rate(hop, "builtin", 100_000, 20.0)
    # every way timed: the fastest a key
    assert bulkways.fitted(hop, "redis", "riotx")[0] == "redisshake"
    planner.record_rate(hop, "redisshake", 100_000, 30.0)
    assert bulkways.fitted(hop, "redis", "riotx")[0] == "riotx"


def test_a_need_passes_a_way_over_with_its_reason(tmp_path, monkeypatch):
    from migkit import bulkways, planner
    hop = _redis_ladder(tmp_path, monkeypatch, evicts=True)
    for way in ("riotx", "redisshake"):
        planner.record_rate(hop, way, 100_000, 1.0)
    planner.record_rate(hop, "builtin", 100_000, 50.0)
    way, why = bulkways.fitted(hop, "redis", "riotx")
    # the copier is slower and still taken: it alone keeps the history
    assert way == "builtin"
    assert "evicts keys by how recently" in why
    # the Redis products' own copy, the fastest, is left for its terms
    hop = _redis_ladder(tmp_path, monkeypatch,
                        terms="the target is not one of Redis's products")
    got = bulkways.choose(hop, "redis")
    assert got.path == "redisshake", got.reason
    assert "not one of Redis's products" in got.reason
    hop = _redis_ladder(tmp_path, monkeypatch, older_target="later form")
    got = bulkways.choose(hop, "redis")
    assert got.path == "redisshake", got.reason
    assert "refuses the source's serialised values" in got.reason


def test_a_copy_stopped_part_way_goes_on_by_the_copier(tmp_path,
                                                       monkeypatch):
    import json

    from migkit import bulkways
    hop = _redis_ladder(tmp_path, monkeypatch)
    (tmp_path / "0").mkdir()
    (tmp_path / "0" / "move.json").write_text(json.dumps(
        {"db0": {"cursor": 81, "node": "", "moved": 1000}}))
    assert bulkways.fitted(hop, "redis", "riotx")[0] == "builtin"
    (tmp_path / "0" / "move.json").write_text(json.dumps(
        {"db0": {"cursor": 0, "moved": 5000, "done": True}}))
    assert bulkways.fitted(hop, "redis", "riotx")[0] == "riotx"


def test_the_kafka_ladder_keeps_the_copier_where_the_header_is_asked(
        tmp_path, monkeypatch):
    from migkit import bulkways, mirror
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    monkeypatch.setattr(mirror, "runner", lambda: "image")
    hop = _hop("kafka", tmp_path)
    assert bulkways.choose(hop, "kafka").path == "mirror"
    hop = _hop("kafka", tmp_path, options={"offset_header": True})
    got = bulkways.choose(hop, "kafka")
    assert got.path == "builtin"
    assert "carries its source position" in got.reason
    monkeypatch.setattr(mirror, "runner", lambda: "")
    assert bulkways.choose(_hop("kafka", tmp_path), "kafka").path == \
        "builtin"


def test_a_forced_mover_is_not_climbed(tmp_path, monkeypatch):
    from migkit import bulkways
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    assert not bulkways.measured("redis")
    monkeypatch.delenv("MIGKIT_MOVER")
    assert bulkways.measured("redis") and bulkways.measured("kafka")
    assert not bulkways.measured("postgres")


# ---- the per-table ladder of the native engines --------------------------

class _Eng:
    """An engine's cost keeping, as `tableways` reads it."""

    def __init__(self, hop):
        self.hop = hop

    from migkit.engines.base import Engine
    _layered = Engine._layered
    _work_key = staticmethod(Engine._work_key)


def _rungs():
    from migkit.decide import Need, Rung
    return (Rung("tool", "the tool", needs=(
                Need("the tool here", lambda f: f["here"], "not here"),)),
            Rung("own", "migkit's own"))


def test_a_tool_that_fails_on_a_table_gives_it_to_the_next_way(tmp_path):
    from migkit import tableways
    from migkit.decide import Facts
    said, ran = [], []

    def run(way, t):
        ran.append((way, t))
        if way == "tool" and t == "b":
            raise RuntimeError("the export ended failed")
        return 10
    # b first, so the tool is the way never timed when b is climbed
    got = tableways.each(_Eng(_hop("dynamodb", tmp_path)), "db",
                         ["b", "a"], _rungs(),
                         lambda t: Facts(here=lambda: True), run,
                         said.append)
    assert got["b"] == "own"
    assert ran[:2] == [("tool", "b"), ("own", "b")]
    assert any("the tool could not run (the export ended failed) -"
               " switching to migkit's own" in s for s in said), said


def test_a_table_no_way_carried_stops_the_copy(tmp_path):
    from migkit import tableways
    from migkit.decide import Facts

    def run(way, t):
        raise RuntimeError(f"{way} broke")
    # every way's reason, in one stop
    with pytest.raises(SystemExit, match="tool broke.*own broke"):
        tableways.each(_Eng(_hop("dynamodb", tmp_path)), "db", ["a"],
                       _rungs(), lambda t: Facts(here=lambda: True), run,
                       None)


def test_the_way_measured_cheaper_carries_the_next_table(tmp_path,
                                                         monkeypatch):
    import itertools

    from migkit import tableways
    from migkit.decide import Facts
    clock = itertools.count()
    # the tool takes 100 ticks a table, migkit's own 1
    ticks = {"tool": 100, "own": 1}
    now = [0.0]
    monkeypatch.setattr(tableways.time, "monotonic", lambda: now[0])

    def run(way, t):
        next(clock)
        now[0] += ticks[way]
        return 10
    got = tableways.each(_Eng(_hop("dynamodb", tmp_path)), "db",
                         ["a", "b", "c", "d"], _rungs(),
                         lambda t: Facts(here=lambda: True), run, None)
    # each tried once, in the list's order, then the cheaper
    assert [got[t] for t in "abcd"] == ["tool", "own", "own", "own"]
