"""Large values on every engine that holds them (backlog P0-PARITY,
`migkit.pieces`): values twice what one batch holds - or as large as the
engine takes one - moved by migkit held to the memory its model says, never
killed, checked equal held to the same, the same on both sides as read
here, and refused before anything is copied where the memory is not there
(`large_values_rig`). Redis strings and collections past a piece, MongoDB
documents at BSON's limit and a file in GridFS, Kafka messages at their
topic's bound, ClickHouse strings, OpenSearch documents, and the pair's
copier between the file engines - SQLite into DuckDB into Parquet into
Delta into Iceberg, each digest held to the other engine's. SQL Server, Db2
and SAP ASE run on x86 only and have files of their own
(`test_large_values_on_sql_server.py`, `..._on_db2.py`,
`..._on_sap_ase.py`).

Every test runs through the docker lock:
`python tools/with_docker_lock.py python -m pytest -q <this file>`.
"""
import os
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import engine_named
from tests import large_values_rig as rig
from tests.cap_pairs import remove
from tests.conftest import needs_docker

pytestmark = needs_docker

MB = rig.MB


def _hop(engine, src, dst, db, **extra):
    """The same hop as this machine reaches its servers: 127.0.0.1 and the
    ports they are published on."""
    return Hop(name=rig.HOP, engine=engine, source=src, target=dst,
               databases=[db], **extra)


def _yaml(engine, src, dst, db, extra=""):
    """The hop as the runner reaches its servers: by their names."""
    return (f"hops:\n  {rig.HOP}:\n    engine: {engine}\n"
            f"    source: {src}\n    target: {dst}\n"
            f"    databases: [{db}]\n" + extra)


# --- Redis -----------------------------------------------------------------

REDIS = {"src": ("migkit-test-lv-redis-src", 16370),
         "dst": ("migkit-test-lv-redis-dst", 16371)}


@pytest.fixture
def redis_pair():
    import redis
    rig.network()
    try:
        for name, port in REDIS.values():
            rig.start(name, port, 6379, "redis:7.4")
        for _, port in REDIS.values():
            rig.until(lambda port=port: redis.Redis(port=port).ping(),
                      f"redis on {port}")
        yield
    finally:
        remove(*(n for n, _ in REDIS.values()))


def _redis(side):
    import redis
    return redis.Redis(port=REDIS[side][1], socket_timeout=300)


def _redis_values(side):
    """{key: (type, md5 of what it holds)} of one side, read here."""
    r = _redis(side)
    out = {}
    for k in r.scan_iter(count=1000):
        kind = r.type(k).decode()
        if kind == "string":
            got = [r.get(k)]
        elif kind == "hash":
            got = [x for f, v in sorted(r.hgetall(k).items())
                   for x in (f, v)]
        elif kind == "list":
            got = r.lrange(k, 0, -1)
        elif kind == "set":
            got = sorted(r.smembers(k))
        else:
            got = [x for m, s in r.zrange(k, 0, -1, withscores=True)
                   for x in (m, repr(s))]
        out[k] = (kind, rig.md5_of(got))
    return out


def test_redis_keys_past_a_piece_are_carried_and_compared_in_pieces(
        redis_pair):
    """A 128 MB string, a hash, a list, a set and a sorted set of 128 MB
    each, one due to expire, and two thousand small keys: a key past a
    piece is read, written and compared a piece at a time, so one worker
    holds a piece of it; the expiry goes with it; a byte changed on the
    target is found by the comparison in pieces."""
    r = _redis("src")
    r.set(b"lv:string", os.urandom(rig.WIDE))
    r.hset(b"lv:hash", mapping={b"f%d" % i: os.urandom(2 * MB)
                                for i in range(64)})
    r.rpush(b"lv:list", *[os.urandom(2 * MB) for _ in range(64)])
    r.sadd(b"lv:set", *[os.urandom(MB) for _ in range(128)])
    r.zadd(b"lv:zset", {os.urandom(MB): i / 3 for i in range(128)})
    r.set(b"lv:due", os.urandom(20 * MB), px=3_600_000)
    pipe = r.pipeline(transaction=False)
    for i in range(2000):
        pipe.set(b"lv:small:%d" % i, b"v%d" % i)
    pipe.execute()
    work = rig.work_dir()
    eng = engine_named("redis", _hop(
        "redis", Endpoint(host="127.0.0.1", port=REDIS["src"][1]),
        Endpoint(host="127.0.0.1", port=REDIS["dst"][1]), "0"))
    hops = _yaml("redis", f"{{host: {REDIS['src'][0]}, port: 6379}}",
                 f"{{host: {REDIS['dst'][0]}, port: 6379}}", "'0'")
    image = rig.runner_image("redis")

    def after(cap):
        # the moment it falls due, carried with the key written in pieces
        assert _redis("dst").pttl(b"lv:due") > 3_000_000
        _redis("dst").setrange(b"lv:string", rig.WIDE // 2, b"!")
        code, out, killed, _ = rig.run(work, cap, "check", rig.HOP,
                                       "--only", "data", image=image)
        assert not killed and code != 0, out[-3000:]
        assert "with a different value" in out, out[-3000:]
    one = rig.held_to_its_memory(work, hops, eng, "0", image,
                                 lambda: _redis("dst").flushall(),
                                 _redis_values, after=after)
    # what one worker holds of a key is a piece of it: less than the widest
    # key held twice over, as a copier of whole keys would
    assert one < 64 * MB + 2 * rig.WIDE


# --- MongoDB ---------------------------------------------------------------

MONGO = {"src": ("migkit-test-lv-mongo-src", 16372),
         "dst": ("migkit-test-lv-mongo-dst", 16373)}


@pytest.fixture
def mongo_pair():
    from pymongo import MongoClient
    rig.network()
    try:
        for name, port in MONGO.values():
            rig.start(name, port, 27017, "mongo:7.0")
        for _, port in MONGO.values():
            rig.until(lambda port=port: MongoClient(
                port=port, serverSelectionTimeoutMS=2000).admin.command(
                "ping"), f"mongodb on {port}")
        yield
    finally:
        remove(*(n for n, _ in MONGO.values()))


def _mongo(side):
    from pymongo import MongoClient
    return MongoClient(port=MONGO[side][1])


def _mongo_documents(side):
    """{collection: md5 of its documents as the server keeps them, in
    `_id` order} of one side, read here."""
    from bson.codec_options import CodecOptions
    from bson.raw_bson import RawBSONDocument
    db = _mongo(side)["lv"]
    raw = CodecOptions(document_class=RawBSONDocument)
    return {name: rig.md5_of(d.raw for d in db.get_collection(
                name, codec_options=raw).find().sort("_id", 1))
            for name in sorted(db.list_collection_names())}


def test_mongodb_documents_at_their_limit_and_a_gridfs_file(mongo_pair):
    """Twenty-four documents of 15 MB - BSON's limit is 16 - a file of
    128 MB in GridFS and two thousand small documents: every read ends at
    `pieces.BATCH_BYTES` of BSON, the file goes chunk by chunk as the
    documents it is."""
    import gridfs
    from bson.binary import Binary
    db = _mongo("src")["lv"]
    for i in range(24):
        db.docs.insert_one({"_id": i, "v": Binary(os.urandom(15 * MB))})
    db.small.insert_many([{"_id": i, "v": f"v{i}"} for i in range(2000)])
    gridfs.GridFS(db).put(os.urandom(rig.WIDE), filename="lv.bin")
    work = rig.work_dir()
    eng = engine_named("mongodb", _hop(
        "mongodb", Endpoint(host="127.0.0.1", port=MONGO["src"][1]),
        Endpoint(host="127.0.0.1", port=MONGO["dst"][1]), "lv"))
    hops = _yaml("mongodb", f"{{host: {MONGO['src'][0]}, port: 27017}}",
                 f"{{host: {MONGO['dst'][0]}, port: 27017}}", "lv")
    rig.held_to_its_memory(work, hops, eng, "lv", rig.runner_image("pymongo"),
                           lambda: _mongo("dst").drop_database("lv"),
                           _mongo_documents)


# --- Kafka -----------------------------------------------------------------

KAFKA = {"src": ("migkit-test-lv-kafka-src", 16374),
         "dst": ("migkit-test-lv-kafka-dst", 16375)}


@pytest.fixture
def kafka_pair():
    from kafka import KafkaAdminClient
    rig.network()
    try:
        for name, port in KAFKA.values():
            rig.start(name, port, port, "redpandadata/redpanda:latest",
                      "redpanda", "start", "--overprovisioned", "--smp",
                      "1", "--memory", "1G", "--reserve-memory", "0M",
                      "--node-id", "0", "--check=false", "--kafka-addr",
                      f"internal://0.0.0.0:9092,external://0.0.0.0:{port}",
                      "--advertise-kafka-addr",
                      f"internal://{name}:9092,external://127.0.0.1:{port}")
        for _, port in KAFKA.values():
            rig.until(lambda port=port: KafkaAdminClient(
                bootstrap_servers=f"127.0.0.1:{port}").list_topics() is not
                None, f"kafka on {port}")
        yield
    finally:
        remove(*(n for n, _ in KAFKA.values()))


def _topic(side, name, most):
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    admin = KafkaAdminClient(
        bootstrap_servers=f"127.0.0.1:{KAFKA[side][1]}")
    try:
        admin.create_topics([NewTopic(name, 1, 1, topic_configs={
            "max.message.bytes": str(most)})])
    finally:
        admin.close()


def _kafka_messages(side):
    """{topic: (messages, md5 of their keys and values in order)} of one
    side, read here; the broker's own topics left out."""
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{KAFKA[side][1]}",
                      enable_auto_commit=False, consumer_timeout_ms=10000,
                      fetch_max_bytes=64 * MB,
                      max_partition_fetch_bytes=32 * MB)
    try:
        out = {}
        for topic in sorted(t for t in c.topics() if not t.startswith("_")):
            tp = TopicPartition(topic, 0)
            c.assign([tp])
            end = c.end_offsets([tp])[tp]
            c.seek_to_beginning(tp)
            parts, n = [], 0
            while c.position(tp) < end:
                for msgs in c.poll(timeout_ms=5000).values():
                    for m in msgs:
                        parts += [m.key or b"", m.value or b""]
                        n += 1
            if n:
                out[topic] = (n, rig.md5_of(parts))
        return out
    finally:
        c.close()


def _kafka_fill(most):
    from kafka import KafkaProducer
    _topic("src", "lv", most)
    p = KafkaProducer(bootstrap_servers=f"127.0.0.1:{KAFKA['src'][1]}",
                      max_request_size=most + MB, buffer_memory=96 * MB,
                      acks="all")
    sent = [p.send("lv", key=b"s%d" % i, value=b"v%d" % i)
            for i in range(500)]
    # the wide ones last: the check reads the last messages of a partition
    sent += [p.send("lv", key=b"w%d" % i, value=os.urandom(16 * MB))
             for i in range(12)]
    p.flush()
    for s in sent:
        s.get(timeout=60)
    p.close()


def _kafka_empty():
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(
        bootstrap_servers=f"127.0.0.1:{KAFKA['dst'][1]}")
    try:
        admin.delete_topics(["lv"])
    finally:
        admin.close()
    time.sleep(3)


def _kafka_engine():
    return engine_named("kafka", _hop(
        "kafka", Endpoint(host="127.0.0.1", port=KAFKA["src"][1]),
        Endpoint(host="127.0.0.1", port=KAFKA["dst"][1]), "cluster"))


def _kafka_yaml():
    return _yaml("kafka", f"{{host: {KAFKA['src'][0]}, port: 9092}}",
                 f"{{host: {KAFKA['dst'][0]}, port: 9092}}", "cluster")


def test_kafka_messages_at_their_topics_bound(kafka_pair):
    """Twelve messages of 16 MB in a topic that takes 20: the producer is
    sized to the topic's bound, the target's topic made with it, and the
    fetches go by bytes."""
    _kafka_fill(20 * MB)
    rig.held_to_its_memory(rig.work_dir(), _kafka_yaml(), _kafka_engine(),
                           "cluster", rig.runner_image("kafka-python"),
                           _kafka_empty, _kafka_messages)


def test_a_message_the_target_takes_no_more_of_stops_the_copy(kafka_pair):
    """The target's topic made beforehand taking 1 MB a message: the copy
    stops at the first message it refuses, naming it, and does not move
    past it as if it had landed."""
    _kafka_fill(20 * MB)
    _topic("dst", "lv", MB)
    work = rig.work_dir()
    (work / "hops.yaml").write_text(_kafka_yaml())
    code, out, killed, _ = rig.run(work, 2 * 2 ** 30, "move", rig.HOP,
                                   "--go",
                                   image=rig.runner_image("kafka-python"))
    assert not killed and code != 0, out[-3000:]
    assert "refused the message at offset 500" in out, out[-3000:]
    held = _kafka_messages("dst").get("lv", (0, ""))[0]
    assert held == 500, held


# --- ClickHouse ------------------------------------------------------------

CH = ("migkit-test-lv-clickhouse", 16376)


def _ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(host="127.0.0.1", port=CH[1],
                                         username="default",
                                         password="test")


@pytest.fixture
def clickhouse():
    rig.network()
    try:
        rig.start(CH[0], CH[1], 8123, "clickhouse/clickhouse-server:24.8",
                  env=["CLICKHOUSE_PASSWORD=test"],
                  extra=["--ulimit", "nofile=262144:262144"])
        rig.until(lambda: _ch().command("select 1") == 1, "clickhouse")
        yield
    finally:
        remove(CH[0])


def _ch_rows(side):
    """[(id, bytes, md5)] of the table on one side, read here."""
    db = "lvsrc" if side == "src" else "lvdst"
    client = _ch()
    if not client.command(f"select count() from system.tables where"
                          f" database = '{db}' and name = 'big'"):
        return []
    return [tuple(r) for r in client.query(
        f"select id, length(v), lower(hex(MD5(v))) from {db}.big"
        " order by id").result_rows]


def test_clickhouse_strings_twice_a_batch(clickhouse):
    """Four strings of 128 MB and a thousand small: sized by the server,
    read in blocks sized by the widest row, hashed where they lie by the
    check."""
    client = _ch()
    client.command("create database lvsrc")
    client.command("create table lvsrc.big (id UInt64, v String) engine ="
                   " MergeTree order by id")
    for i in range(4):
        client.insert("big", [[i, os.urandom(rig.WIDE)]],
                      column_names=["id", "v"], database="lvsrc")
    client.insert("big", [[100 + i, b"v%d" % i] for i in range(1000)],
                  column_names=["id", "v"], database="lvsrc")
    ep = Endpoint(host="127.0.0.1", port=CH[1], user="default",
                  password="test")
    eng = engine_named("clickhouse", _hop("clickhouse", ep, ep, "lvsrc",
                                          db_map={"lvsrc": "lvdst"}))
    side = (f"{{host: {CH[0]}, port: 8123, user: default,"
            " password: test}")
    hops = _yaml("clickhouse", side, side, "lvsrc",
                 "    db_map: {lvsrc: lvdst}\n")
    rig.held_to_its_memory(rig.work_dir(), hops, eng, "lvsrc",
                           rig.runner_image("clickhouse-connect"),
                           lambda: _ch().command(
                               "drop database if exists lvdst"),
                           _ch_rows)


# --- OpenSearch ------------------------------------------------------------

OS = ("migkit-test-lv-opensearch", 16377)


def _os(method, path, body=None):
    import json
    import urllib.request
    req = urllib.request.Request(
        f"http://127.0.0.1:{OS[1]}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=300) as r:
        raw = r.read()
    return json.loads(raw) if raw else {}


@pytest.fixture
def opensearch():
    rig.network()
    try:
        rig.start(OS[0], OS[1], 9200, "opensearchproject/opensearch:2.19.1",
                  env=["discovery.type=single-node",
                       "DISABLE_SECURITY_PLUGIN=true",
                       "DISABLE_INSTALL_DEMO_CONFIG=true",
                       "OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g"])
        rig.until(lambda: _os("GET", "/_cluster/health").get("status")
                  in ("green", "yellow"), "opensearch")
        yield
    finally:
        remove(OS[0])


def _os_documents(side):
    """{id: md5 of its source} of the index on one side, read here."""
    import json
    import urllib.error
    index = "lvsrc.docs" if side == "src" else "lvdst.docs"
    try:
        _os("POST", f"/{index}/_refresh")
    except urllib.error.HTTPError:
        return {}
    out = {}
    got = _os("POST", f"/{index}/_search?scroll=2m",
              {"size": 2, "sort": ["_doc"]})
    while got["hits"]["hits"]:
        for h in got["hits"]["hits"]:
            out[h["_id"]] = rig.md5_of([json.dumps(h["_source"],
                                                   sort_keys=True)])
        got = _os("POST", "/_search/scroll",
                  {"scroll": "2m", "scroll_id": got["_scroll_id"]})
    return out


def test_opensearch_documents_of_many_megabytes(opensearch):
    """Eight documents of 24 MB and five hundred small: read in scroll
    pages sized by the widest document, written in bulk requests cut by
    bytes."""
    import base64
    _os("PUT", "/lvsrc.docs", {"mappings": {"properties": {
        "k": {"type": "long"}, "v": {"type": "binary"}}}})
    for i in range(8):
        _os("PUT", f"/lvsrc.docs/_doc/w{i}", {
            "k": i, "v": base64.b64encode(os.urandom(24 * MB)).decode()})
    for i in range(500):
        _os("PUT", f"/lvsrc.docs/_doc/s{i}",
            {"k": 100 + i, "v": base64.b64encode(b"v%d" % i).decode()})
    ep = Endpoint(host="127.0.0.1", port=OS[1])
    eng = engine_named("opensearch", _hop("opensearch", ep, ep, "lvsrc",
                                          db_map={"lvsrc": "lvdst"}))
    side = f"{{host: {OS[0]}, port: 9200}}"
    hops = _yaml("opensearch", side, side, "lvsrc",
                 "    db_map: {lvsrc: lvdst}\n")
    rig.held_to_its_memory(rig.work_dir(), hops, eng, "lvsrc",
                           rig.runner_image("opensearch-py"),
                           lambda: _os("DELETE", "/lvdst.docs"),
                           _os_documents)


# --- the pair's copier between the file engines ----------------------------

FILES = ("duckdb==1.5.5", "pyarrow==25.0.1", "deltalake==1.6.6",
         "pyiceberg[sql-sqlite]==0.12.0")


def _sqlite_source(work):
    """A SQLite file of three rows holding 128 MB byte values and 48 MB
    texts, and a thousand small ones."""
    import sqlite3
    conn = sqlite3.connect(work / "src.db")
    conn.execute("create table big (id integer primary key, b blob, t text)")
    for i in range(3):
        conn.execute("insert into big values (?, ?, ?)",
                     (i, os.urandom(rig.WIDE), "é" * (24 * MB)))
    conn.executemany("insert into big values (?, ?, ?)",
                     [(100 + i, b"v%d" % i, f"t{i}") for i in range(1000)])
    conn.commit()
    conn.close()


def _pair(src_engine, dst_engine, src, dst, host_src, host_dst):
    """(the pair as this machine reaches its files, as the runner does)."""
    eng = engine_named("hetero", _hop(
        "hetero", host_src, host_dst, "main",
        options={"source_engine": src_engine, "target_engine": dst_engine}))
    hops = _yaml("hetero", src, dst, "main",
                 f"    options: {{source_engine: {src_engine},"
                 f" target_engine: {dst_engine}}}\n")
    return eng, hops


def _duck_rows(path):
    import duckdb
    if not os.path.exists(path):
        return []
    con = duckdb.connect(str(path), read_only=True)
    try:
        return [tuple(r) for r in con.execute(
            "select id, octet_length(b), md5(b), md5(t) from main.big"
            " order by id").fetchall()]
    except duckdb.Error:
        return []
    finally:
        con.close()


def _sqlite_rows(path):
    import hashlib
    import sqlite3
    conn = sqlite3.connect(path)
    try:
        return [(i, len(b), hashlib.md5(b).hexdigest(),
                 hashlib.md5(t.encode()).hexdigest())
                for i, b, t in conn.execute(
                    "select id, b, t from big order by id")]
    finally:
        conn.close()


def test_sqlite_into_duckdb_through_the_pairs_copier():
    """A SQLite source's wide rows into DuckDB: each read ending at its
    bytes, and the two engines' digests - one folded by SQLite's function
    here, one in DuckDB's SQL, each writing a wide value as its length and
    md5 - the same."""
    import shutil
    rig.network()
    work = rig.work_dir()
    _sqlite_source(work)
    eng, hops = _pair("sqlite", "duckdb", "{host: /work/src.db}",
                      "{host: /work/dst.duckdb}",
                      Endpoint(host=str(work / "src.db")),
                      Endpoint(host=str(work / "dst.duckdb")))

    def rows(side):
        return (_sqlite_rows(work / "src.db") if side == "src"
                else _duck_rows(work / "dst.duckdb"))

    def empty():
        for f in ("dst.duckdb", "dst.duckdb.wal"):
            if (work / f).exists():
                os.remove(work / f)
        shutil.rmtree(work / "reports", ignore_errors=True)
    rig.held_to_its_memory(work, hops, eng, "main",
                           rig.runner_image(*FILES), empty, rows)


def _duck_source(work):
    """A DuckDB file of the rows `_sqlite_source` makes."""
    import duckdb
    con = duckdb.connect(str(work / "src.duckdb"))
    con.execute("create table main.big (id bigint primary key, b blob,"
                " t varchar)")
    for i in range(3):
        con.execute("insert into main.big values (?, ?, ?)",
                    (i, os.urandom(rig.WIDE), "é" * (24 * MB)))
    con.executemany("insert into main.big values (?, ?, ?)",
                    [(100 + i, b"v%d" % i, f"t{i}") for i in range(1000)])
    con.close()


def test_duckdb_into_parquet_into_delta_into_iceberg():
    """The same rows from DuckDB into Parquet, from there into Delta, from
    there into Iceberg: each file engine's reads cut by bytes and made
    values a slice at a time, each pair's digests the same."""
    import shutil
    rig.network()
    work = rig.work_dir()
    _duck_source(work)
    image = rig.runner_image(*FILES)
    legs = [
        ("duckdb", "parquet", "{host: /work/src.duckdb}",
         "{host: /work/parquet}", Endpoint(host=str(work / "src.duckdb")),
         Endpoint(host=str(work / "parquet")), work / "parquet"),
        ("parquet", "delta", "{host: /work/parquet}", "{host: /work/delta}",
         Endpoint(host=str(work / "parquet")),
         Endpoint(host=str(work / "delta")), work / "delta"),
        ("delta", "iceberg", "{host: /work/delta}",
         "{catalog: 'sqlite:////work/cat.db', warehouse: /work/wh}",
         Endpoint(host=str(work / "delta")),
         Endpoint(options={"catalog": f"sqlite:///{work}/cat.db",
                           "warehouse": str(work / "wh")}), None),
    ]
    for src_e, dst_e, src, dst, host_src, host_dst, made in legs:
        eng, hops = _pair(src_e, dst_e, src, dst, host_src, host_dst)

        def empty(made=made):
            if made is not None:
                shutil.rmtree(made, ignore_errors=True)
            else:
                for f in ("cat.db", "wh"):
                    target = work / f
                    if target.is_dir():
                        shutil.rmtree(target, ignore_errors=True)
                    elif target.exists():
                        os.remove(target)
            shutil.rmtree(work / "reports", ignore_errors=True)
        # the Iceberg catalog keeps its files by the runner's paths: that
        # leg is held to the pair's own check, not read back here
        rig.held_to_its_memory(work, hops, eng, "main", image, empty)
        if dst_e != "iceberg":
            # moved again for the next leg to read
            (work / "hops.yaml").write_text(hops)
            code, out, killed, _ = rig.run(work, 3 * 2 ** 30, "move",
                                           rig.HOP, "--go", image=image)
            assert code == 0 and not killed, out[-3000:]
