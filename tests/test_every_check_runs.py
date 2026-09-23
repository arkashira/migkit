"""Every check each engine has, called against live servers.

This exists because of one that never ran. `check --check deep` on Kafka
called `list_consumer_groups`, a kafka-python 2.x name, against the 3.x client
migkit installs - so it raised `AttributeError` every single time it was asked,
and nothing noticed, because `deep` is not in Kafka's default set of checks and
no test had ever called it.

A `hasattr` sweep of the client libraries came back clean afterwards, which is
worth exactly as much as the day it was run: the next version bump can take a
method away again. This is the standing version of that sweep. It asks each
engine for every check it has - the default ones and the ones an operator has
to name explicitly - and requires that each one *answers*.

Answering badly is allowed here. A check that reports `error` because a side is
unreadable has done its job; this file is only about the difference between
reporting and falling over, which is the same distinction the rest of the
project keeps making.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

RD_SRC, RD_DST = "migkit-test-all-rd-src", "migkit-test-all-rd-dst"
RD_SRC_PORT, RD_DST_PORT = 16431, 16432
MG = "migkit-test-all-mg"
MG_PORT = 27091
KF = "migkit-test-all-kf"
KF_PORT = 19421

#: the checks the CLI can ask for, whether or not an engine lists them in its
#: own default set - the ones it does not list are exactly where this rots
ALL_CHECKS = ("schema", "counts", "autoinc", "data", "deep", "params")


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _run(name, *args):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, *args],
                   check=True, capture_output=True)


@pytest.fixture(scope="module")
def servers():
    for name, port in ((RD_SRC, RD_SRC_PORT), (RD_DST, RD_DST_PORT)):
        _run(name, "-p", f"{port}:6379", "redis:7")
    _run(MG, "-p", f"{MG_PORT}:27017", "mongo:7")
    _run(KF, "-p", f"{KF_PORT}:9092",
         "-e", "KAFKA_NODE_ID=1",
         "-e", "KAFKA_PROCESS_ROLES=broker,controller",
         "-e", "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,"
               "CONTROLLER://0.0.0.0:9093",
         "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{KF_PORT}",
         "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
         "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
         "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,"
               "PLAINTEXT:PLAINTEXT",
         "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
         "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
         "apache/kafka:3.8.0")
    for port in (RD_SRC_PORT, RD_DST_PORT, MG_PORT, KF_PORT):
        if not _wait(port):
            pytest.skip("a server did not come up in this sandbox")
    time.sleep(8)
    yield
    for name in (RD_SRC, RD_DST, MG, KF):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def _seed_redis():
    import redis
    for port in (RD_SRC_PORT, RD_DST_PORT):
        client = redis.Redis(host="127.0.0.1", port=port,
                             decode_responses=True)
        client.flushall()
        for i in range(20):
            client.set(f"k{i}", f"v{i}", ex=600)


def _seed_mongo():
    from pymongo import MongoClient
    client = MongoClient(f"mongodb://127.0.0.1:{MG_PORT}/"
                         "?directConnection=true")
    for name in ("mk_src", "mk_dst"):
        client.drop_database(name)
        client[name]["t"].insert_many([{"_id": i, "v": f"v{i}"}
                                       for i in range(20)])
    client.close()


def _seed_kafka():
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{KF_PORT}")
    try:
        admin.create_topics([NewTopic("orders", num_partitions=1,
                                      replication_factor=1)])
    except Exception:
        pass
    admin.close()
    producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{KF_PORT}")
    for i in range(20):
        producer.send("orders", value=f"v{i}".encode())
    producer.flush()
    producer.close()


def _engines(pg_pair, tmp_path):
    """One live engine of each kind, with both sides seeded the same."""
    import sqlite3

    from migkit.engines.kafka import KafkaEngine
    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.postgres import PostgresEngine
    from migkit.engines.redis import RedisEngine
    from migkit.engines.sqlite import SQLiteEngine
    out = {}

    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table t (id bigint primary key, v text);"
                   " insert into t select g, 'v' from generate_series(1,20) g;")
    out["postgres"] = PostgresEngine(Hop(
        name="pg", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                        user="postgres", password="test"),
        target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                        user="postgres", password="test"),
        databases=["postgres"]))

    def ep(path):
        return Endpoint(host=str(path), port=0, user="", password="")
    for name in ("a.db", "b.db"):
        conn = sqlite3.connect(tmp_path / name)
        conn.execute("create table t (id integer primary key, v text)")
        conn.executemany("insert into t values (?,?)",
                         [(i, f"v{i}") for i in range(20)])
        conn.commit()
        conn.close()
    out["sqlite"] = SQLiteEngine(Hop(name="lite", engine="sqlite",
                                     source=ep(tmp_path / "a.db"),
                                     target=ep(tmp_path / "b.db")))

    _seed_redis()
    out["redis"] = RedisEngine(Hop(
        name="rd", engine="redis",
        source=Endpoint(host="127.0.0.1", port=RD_SRC_PORT, user="",
                        password=""),
        target=Endpoint(host="127.0.0.1", port=RD_DST_PORT, user="",
                        password=""),
        db_map={"0": "0"}))

    _seed_mongo()
    mongo_ep = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="",
                        options={"uri_options": "directConnection=true"})
    out["mongodb"] = MongoEngine(Hop(name="mg", engine="mongodb",
                                     source=mongo_ep, target=mongo_ep,
                                     databases=["mk_src"],
                                     db_map={"mk_src": "mk_dst"}))

    _seed_kafka()
    kafka_ep = Endpoint(host="127.0.0.1", port=KF_PORT, user="", password="")
    out["kafka"] = KafkaEngine(Hop(name="kf", engine="kafka",
                                   source=kafka_ep, target=kafka_ep,
                                   db_map={"cluster": "cluster"}))

    # the two that are made of other engines rather than a driver of their
    # own, and so are the easiest to leave out of a sweep like this
    from migkit.engines.hetero import HeteroEngine
    out["hetero"] = HeteroEngine(Hop(
        name="het", engine="hetero",
        source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                        user="postgres", password="test"),
        target=ep(tmp_path / "b.db"), databases=["postgres"],
        options={"source_engine": "postgres", "target_engine": "sqlite"}))

    from migkit.engines.generic import GenericEngine
    out["generic"] = GenericEngine(Hop(
        name="gen", engine="generic",
        source=Endpoint(host="x", port=0, user="", password="", options={
            "url": f"postgresql://postgres:test@127.0.0.1:{pg_pair['src']}"
                   "/postgres"}),
        target=Endpoint(host="x", port=0, user="", password="", options={
            "url": f"postgresql://postgres:test@127.0.0.1:{pg_pair['dst']}"
                   "/postgres"}),
        options={"tables": ["t"], "key": "id"}))

    for engine in out.values():
        engine.hop.report_dir = lambda db=None: tmp_path
    return out


def _db_of(name):
    return {"postgres": "postgres", "sqlite": "main", "redis": "0",
            "mongodb": "mk_src", "kafka": "cluster", "hetero": "postgres",
            "generic": "-"}[name]


def test_every_check_every_engine_has_answers_rather_than_raising(
        pg_pair, servers, tmp_path):
    engines = _engines(pg_pair, tmp_path)
    broken = []
    ran = 0
    for name, engine in sorted(engines.items()):
        db = _db_of(name)
        for check in ALL_CHECKS:
            method = getattr(type(engine), f"check_{check}", None)
            if method is None or method is getattr(Engine, f"check_{check}"):
                continue      # the base answers `skip`, which is an answer
            ran += 1
            try:
                got = getattr(engine, f"check_{check}")(db)
            except Exception as e:
                broken.append(f"{name}.check_{check}:"
                              f" {type(e).__name__}: {str(e)[:110]}")
                continue
            if not isinstance(got, list) or not got:
                broken.append(f"{name}.check_{check} answered {got!r}")
    assert ran >= 15, f"only {ran} checks were exercised at all"
    assert not broken, broken


def test_every_delta_verify_answers_too(pg_pair, servers, tmp_path):
    """`delta` is the other one an operator has to ask for by name."""
    engines = _engines(pg_pair, tmp_path)
    broken = []
    asked = 0
    for name, engine in sorted(engines.items()):
        if not hasattr(engine, "delta_verify"):
            continue
        asked += 1
        try:
            got = engine.delta_verify(_db_of(name))
        except Exception as e:
            broken.append(f"{name}.delta_verify:"
                          f" {type(e).__name__}: {str(e)[:110]}")
            continue
        if not isinstance(got, list) or not got:
            broken.append(f"{name}.delta_verify answered {got!r}")
    # every engine here that the capability matrix says verifies deltas was
    # asked. This read `asked >= 4`, and Redis was the fourth: its method
    # did nothing but return an error, and is gone
    from migkit import capabilities
    have = [n for n in engines if capabilities.implemented(n, "delta")]
    assert asked == len(have) >= 3, (asked, have)
    assert not broken, broken


def test_the_client_libraries_still_have_what_the_engines_call():
    """The one-off sweep, kept. It needs no server, so it fails fast on the
    day a dependency renames something rather than the day someone asks for
    the check that uses it."""
    import redis
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient
    from pymongo import MongoClient
    from pymongo.collection import Collection
    from pymongo.database import Database
    expected = {
        KafkaAdminClient: ("alter_group_offsets", "describe_configs",
                           "describe_topics", "list_group_offsets",
                           "list_groups", "close"),
        KafkaConsumer: ("assign", "beginning_offsets", "end_offsets",
                        "partitions_for_topic", "poll", "seek", "topics",
                        "commit", "close"),
        redis.Redis: ("dump", "restore", "pttl", "scan", "delete", "pipeline",
                      "info", "config_get", "dbsize", "type", "memory_usage",
                      "exists"),
        Collection: ("aggregate", "bulk_write", "count_documents",
                     "delete_one", "find", "find_one", "list_indexes",
                     "update_one", "replace_one", "watch"),
        Database: ("list_collection_names", "list_collections", "command",
                   "watch"),
        MongoClient: ("list_database_names", "watch", "close"),
    }
    missing = [f"{cls.__name__}.{name}"
               for cls, names in expected.items()
               for name in names if not hasattr(cls, name)]
    assert not missing, missing


# ---- the other things the CLI asks an engine for ------------------------

#: `migkit sync --kind X` passes each of these straight through
REPAIR_KINDS = ("rows", "sequences", "schema", "all")


def test_assess_answers_on_every_engine(pg_pair, servers, tmp_path):
    """`migkit assess` is the first command anyone runs, and it is the one
    that has to work when a side is misconfigured."""
    broken = []
    for name, engine in sorted(_engines(pg_pair, tmp_path).items()):
        try:
            got = engine.assess()
        except Exception as e:
            broken.append(f"{name}.assess:"
                          f" {type(e).__name__}: {str(e)[:110]}")
            continue
        if not isinstance(got, list) or not got:
            broken.append(f"{name}.assess answered {got!r}")
            continue
        shape = [row for row in got
                 if not {"level", "scope", "item", "detail"} <= set(row)]
        if shape:
            broken.append(f"{name}.assess row missing fields: {shape[0]}")
    assert not broken, broken


def test_watch_sample_answers_on_every_engine(pg_pair, servers, tmp_path):
    """`migkit watch` calls this in a loop, so it is the one place a raise
    becomes a crash in front of somebody watching a cutover."""
    broken = []
    for name, engine in sorted(_engines(pg_pair, tmp_path).items()):
        try:
            got = engine.watch_sample(_db_of(name))
        except Exception as e:
            broken.append(f"{name}.watch_sample:"
                          f" {type(e).__name__}: {str(e)[:110]}")
            continue
        if not isinstance(got, dict) or "ts" not in got:
            broken.append(f"{name}.watch_sample answered {got!r}")
        elif "error" not in got and "src_rows" not in got:
            broken.append(f"{name}.watch_sample gave neither a count nor a"
                          f" reason: {got!r}")
    assert not broken, broken


def test_every_repair_kind_answers_on_every_engine(pg_pair, servers,
                                                   tmp_path):
    """Including the kinds an engine has nothing to say about: an empty list
    is a fine answer, an exception is not."""
    broken = []
    for name, engine in sorted(_engines(pg_pair, tmp_path).items()):
        for kind in REPAIR_KINDS:
            try:
                got = engine.repair_plan(_db_of(name), kind)
            except Exception as e:
                broken.append(f"{name}.repair_plan({kind}):"
                              f" {type(e).__name__}: {str(e)[:110]}")
                continue
            if not isinstance(got, list):
                broken.append(f"{name}.repair_plan({kind}) answered {got!r}")
    assert not broken, broken


def test_setup_and_replication_plans_answer(pg_pair, servers, tmp_path):
    from migkit.engines import engines_with
    broken = []
    engines = _engines(pg_pair, tmp_path)
    for name, engine in sorted(engines.items()):
        try:
            got = engine.setup_target_plan(_db_of(name))
        except SystemExit as e:
            # migkit's own refusal channel: a sentence an operator can read
            # is an answer, an exception from four frames down is not
            if not str(e).strip():
                broken.append(f"{name}.setup_target_plan exited silently")
        except Exception as e:
            broken.append(f"{name}.setup_target_plan:"
                          f" {type(e).__name__}: {str(e)[:110]}")
        else:
            if not isinstance(got, list):
                broken.append(f"{name}.setup_target_plan answered {got!r}")
    for name in engines_with("replicate_sql"):
        engine = engines.get(name)
        if engine is None:
            continue
        try:
            plan = engine.replicate_sql(_db_of(name))
        except Exception as e:
            broken.append(f"{name}.replicate_sql:"
                          f" {type(e).__name__}: {str(e)[:110]}")
            continue
        if not {"src", "dst", "status"} <= set(plan):
            broken.append(f"{name}.replicate_sql answered {sorted(plan)}")
    assert not broken, broken
