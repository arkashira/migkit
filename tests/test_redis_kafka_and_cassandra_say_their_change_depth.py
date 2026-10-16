"""Redis, Kafka and Cassandra say how deeply their changes can be read
(LT.R1), each from what its source reports: Redis from the live way the
decision engine chose, a topic from its cleanup policy, a ScyllaDB table
from its change log's options - with the statement that raises it, and
no depth guessed where the source would not say. Run on readers that
answer as the servers do; the lanes' own servers are not asked here.
"""
from types import SimpleNamespace as NS

from migkit import changedepth, decide
from migkit.engines.cassandra import CassandraEngine
from migkit.engines.kafka import KafkaEngine
from migkit.engines.redis import RedisEngine


def test_the_redis_depth_is_the_live_ways():
    assert changedepth.redis("replica stream") == ("D3", "")
    depth, fix = changedepth.redis(
        "idle-time sweep", "needs an account the source lets replicate")
    assert depth == "D2" and "+psync" in fix
    assert changedepth.redis(None)[0] == "D0"
    eng = object.__new__(RedisEngine)
    eng.databases = lambda: [0, 3]
    eng.live_way = lambda: decide.redis_live(
        {"replica grant": False, "idletime": True})
    got = eng.change_depths()
    assert set(got) == {"db0", "db3"} and got["db0"][0] == "D2"
    assert "+psync" in got["db0"][1]


def test_a_compacted_topic_keeps_only_the_latest_of_each_key():
    assert changedepth.kafka("t", "delete") == ("D4", "")
    depth, fix = changedepth.kafka("t", "compact,delete")
    assert depth == "D3" and "cleanup.policy=delete" in fix
    assert changedepth.kafka("t", None) is None
    eng = object.__new__(KafkaEngine)
    eng._consumer = lambda side: NS(close=lambda: None)
    eng._topics = lambda c: ["a", "b"]
    eng._topic_configs = lambda side, topics: {
        "a": {"cleanup.policy": "delete"}, "b": {"cleanup.policy": "compact"}}
    assert {t: d for t, (d, _) in eng.change_depths().items()} == {
        "a": "D4", "b": "D3"}
    eng._topic_configs = lambda side, topics: None
    assert eng.change_depths() is None


def test_a_scylla_table_is_as_deep_as_its_change_log():
    assert changedepth.cassandra(True, {"enabled": "false"}, False,
                                 "k.t")[1].startswith("ALTER TABLE k.t")
    assert changedepth.cassandra(True, {"enabled": "true"}, False)[0] == "D3"
    assert changedepth.cassandra(True, {"enabled": "true", "preimage":
                                        "true", "postimage": "true"},
                                 False) == ("D4", "")
    assert changedepth.cassandra(False, None, False) == ("D0", "")
    assert changedepth.cassandra(True, {"enabled": "true"}, True)[0] == "D0"
    eng = object.__new__(CassandraEngine)
    eng.__dict__["_is_scylla"] = {"src": True}
    eng.databases = lambda: ["k"]
    eng.neutral_tables = lambda side, db: ["t", "u"]
    eng._columns = lambda side, db, t: [NS(type="int")]
    eng._session = lambda side: NS(execute=lambda sql, args: [
        NS(table_name="t", cdc={"enabled": "true", "preimage": "true",
                                "postimage": "true"}),
        NS(table_name="u", cdc={"enabled": "false"})])
    got = eng.change_depths()
    assert got["k.t"] == ("D4", "") and got["k.u"][0] == "D0"
    assert "cdc" in got["k.u"][1]
