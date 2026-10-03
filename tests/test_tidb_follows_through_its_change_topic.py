"""TiDB as a side of a pair (backlog ER4), against a TiDB server of its own
(`pingcap/tidb`, which keeps its data in the process) and a Kafka broker
the change topic is published on.

TiDB has no binlog: its changes are TiCDC's canal-json messages on a
topic, which carry every value as text. The messages here are made as
TiCDC makes them - each transaction's commit TSO from the server's own
`tidb_last_txn_info`, a watermark on every partition - so what is proved
is the reader against TiDB's own clock: rows read back as they stood at
the watermark, a key the feed names wrongly stopping the tail, and the
copy's snapshot mark leaving out exactly what the copy saw.
"""
import ctypes
import json
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.fence", "tidb"),
    ("tail.follows", "tidb"),
    ("move.table-copy", "tidb"),
    ("move.create-missing", "tidb"),
    ("cutover.counters", "tidb"),
    ("cutover.drain", "tidb"),
]

pytestmark = needs_docker

TIDB, TIDB_PORT = "migkit-test-tidb", 16340
KAFKA, KAFKA_PORT = "migkit-test-changes-kafka", 16347
TOPIC = "ticdc-shop"


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
    for name in (TIDB, KAFKA):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", TIDB, "-p",
                    f"127.0.0.1:{TIDB_PORT}:4000", "pingcap/tidb:v8.5.1"],
                   check=True, capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--name", KAFKA, "-p",
         f"127.0.0.1:{KAFKA_PORT}:{KAFKA_PORT}",
         "redpandadata/redpanda:latest", "redpanda", "start",
         "--overprovisioned", "--smp", "1", "--memory", "384M",
         "--reserve-memory", "0M", "--node-id", "0", "--check=false",
         "--kafka-addr", f"PLAINTEXT://0.0.0.0:{KAFKA_PORT}",
         "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{KAFKA_PORT}"],
        check=True, capture_output=True)
    try:
        assert _wait(TIDB_PORT) and _wait(KAFKA_PORT)
        import pymysql
        for _ in range(60):
            try:
                pymysql.connect(host="127.0.0.1", port=TIDB_PORT,
                                user="root").close()
                break
            except Exception:
                time.sleep(1)
        from kafka.admin import KafkaAdminClient, NewTopic
        for _ in range(30):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=f"127.0.0.1:{KAFKA_PORT}")
                admin.create_topics([NewTopic(TOPIC, num_partitions=2,
                                              replication_factor=1)])
                admin.close()
                break
            except Exception:
                time.sleep(1)
        yield
    finally:
        for name in (TIDB, KAFKA):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def sql(statements, db=None):
    """Run statements; the last one's rows, and the commit TSO of each
    transaction that wrote (`tidb_last_txn_info`)."""
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=TIDB_PORT, user="root",
                           database=db, autocommit=True)
    rows, tsos = [], []
    try:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
                rows = cur.fetchall()
                if s.split()[0].lower() in ("insert", "update", "delete"):
                    cur.execute("select @@tidb_last_txn_info")
                    info = json.loads(cur.fetchone()[0])
                    tsos.append(int(info["commit_ts"]))
    finally:
        conn.close()
    return rows, tsos


def _begin_tso():
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=TIDB_PORT, user="root")
    try:
        with conn.cursor() as cur:
            cur.execute("start transaction with consistent snapshot")
            cur.execute("select @@tidb_current_ts")
            return int(cur.fetchone()[0])
    finally:
        conn.rollback()
        conn.close()


def publish(messages, partition=None):
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=f"127.0.0.1:{KAFKA_PORT}")
    for key, value, part in messages:
        p.send(TOPIC, key=key, value=json.dumps(value).encode(),
               partition=part if partition is None else partition)
    p.flush()
    p.close()


def canal(kind, ts, rows, old=None, table="orders"):
    return {"database": "tsrc", "table": table, "isDdl": False,
            "type": kind, "pkNames": ["id"], "data": rows, "old": old,
            "_tidb": {"commitTs": ts}}


def watermark(ts):
    return [(None, {"type": "TIDB_WATERMARK", "_tidb": {"watermarkTs": ts}},
             p) for p in (0, 1)]


def _engine(tmp_path, **changes):
    from migkit.engines.tidb import TiDBEngine
    ep = Endpoint(host="127.0.0.1", port=TIDB_PORT, user="root", password="",
                  options={"changes": {"servers": f"127.0.0.1:{KAFKA_PORT}",
                                       "topic": TOPIC, **changes}})
    hop = Hop(name="tidb", engine="tidb", source=ep,
              target=Endpoint(host="127.0.0.1", port=TIDB_PORT, user="root",
                              password=""),
              databases=["tsrc"], db_map={"tsrc": "tdst"})
    hop.report_dir = lambda db=None: tmp_path
    return TiDBEngine(hop)


@pytest.fixture
def shop(servers):
    sql(["drop database if exists tsrc", "drop database if exists tdst",
         "create database tsrc", "create database tdst"])
    sql(["create table orders (id bigint primary key, qty int,"
         " price decimal(12,2), note varchar(40), made datetime(6),"
         " blob_ varbinary(8), doc json)",
         "insert into orders values (1, 2, 10.50, 'a', '2024-01-01"
         " 00:00:00.123456', x'00ff', '{\"b\": 1, \"a\": [1, 2]}'),"
         " (2, 3, 0.10, null, null, null, null)"], db="tsrc")
    return "tsrc"


def _move(eng, tmp_path):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    eng.create_missing("tsrc", lambda m: None)
    for sch, t in eng.list_move_tables("tsrc"):
        eng.move_table("tsrc", sch, t, 1000, ck, lambda m: None)


def _data(eng):
    return {r.scope: r.status for r in eng.check_data("tsrc")
            if r.check == "data"}


def test_a_tidb_database_copied_and_compared_on_the_server(shop, tmp_path):
    eng = _engine(tmp_path)
    _move(eng, tmp_path)
    got = _data(eng)
    assert got and set(got.values()) == {"ok"}, got
    sql(["update orders set price = 10.51 where id = 1"], db="tdst")
    got = _data(_engine(tmp_path))
    assert "diff" in got.values(), got


def test_a_row_is_read_back_as_it_stood_at_the_watermark(shop, tmp_path):
    from migkit.changetopic import Row
    eng = _engine(tmp_path)
    at = _begin_tso()
    sql(["update orders set note = 'changed' where id = 1"], db="tsrc")
    (r, key, values), = eng._read_back(
        "tsrc", "orders", [Row("tsrc", "orders", "upsert", {"id": "1"},
                               None, at)], at)
    assert key == {"id": 1} and values["note"] == "a"
    # a row the feed says was written by then and is not there: stopped,
    # never applied as a delete
    with pytest.raises(SystemExit) as e:
        eng._read_back("tsrc", "orders",
                       [Row("tsrc", "orders", "upsert", {"id": "77"},
                            None, at)], at)
    assert "is not the key here" in str(e.value)


def test_the_copys_mark_leaves_out_exactly_what_it_saw(shop, tmp_path):
    eng = _engine(tmp_path)
    _, (before,) = sql(["insert into orders (id, qty) values (3, 1)"],
                       db="tsrc")
    mark = eng.snapshot_mark("src", "tsrc")
    _, (after,) = sql(["insert into orders (id, qty) values (4, 1)"],
                      db="tsrc")
    assert eng.mark_covers("src", "tsrc", mark, {"txn": before}) is True
    assert eng.mark_covers("src", "tsrc", mark, {"txn": after}) is False


def test_the_next_id_on_the_target_is_read_where_tidb_keeps_it(shop,
                                                               tmp_path):
    sql(["create table seq (id bigint auto_increment primary key, v int)",
         "insert into seq (id, v) values (1000, 1)"], db="tsrc")
    sql(["create table seq (id bigint auto_increment primary key, v int)"],
        db="tdst")
    eng = _engine(tmp_path)
    got = {r.scope: r.status for r in eng.check_autoinc("tsrc")}
    assert got["tsrc.seq"] == "diff"
    sql(["insert into seq (id, v) values (1000, 1)"], db="tdst")
    got = {r.scope: r.status for r in _engine(tmp_path).check_autoinc(
        "tsrc")}
    assert got["tsrc.seq"] == "ok"


def _tail(eng, tmp_path):
    ended = {}

    def run():
        try:
            eng.tail_apply("tsrc", True, tmp_path / "tail-token.json",
                           lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, ended


def _stop(thread):
    if thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    thread.join(timeout=30)


def test_a_copy_and_its_change_topic_end_equal(shop, tmp_path):
    eng = _engine(tmp_path)
    token = tmp_path / "tail-token.json"
    eng.tail_start("tsrc", token)
    _move(eng, tmp_path)
    _, (t1, t2, t3, t4) = sql([
        "insert into orders (id, qty, note) values (10, 5, 'new')",
        "update orders set qty = 9, doc = '{\"z\": 1.10}' where id = 1",
        "update orders set id = 20 where id = 2",
        "delete from orders where id = 10"], db="tsrc")
    # as TiCDC publishes them: the key's changes on one partition, a
    # watermark on every partition; the row values as TiCDC's text
    publish([(b"10", canal("INSERT", t1, [{"id": "10", "qty": "5"}]), 0),
             (b"1", canal("UPDATE", t2, [{"id": "1", "qty": "9"}]), 1),
             (b"20", canal("UPDATE", t3, [{"id": "20"}],
                           old=[{"id": "2"}]), 0),
             (b"10", canal("DELETE", t4, [{"id": "10"}]), 0)])
    publish(watermark(_begin_tso()))
    thread, ended = _tail(eng, tmp_path)
    try:
        time.sleep(2)
        at = eng.src_lsn("tsrc")
        assert at is not None
        publish(watermark(_begin_tso()))
        for _ in range(30):
            if eng.fence_wait("tsrc", at, timeout=2):
                break
            publish(watermark(_begin_tso()))
        assert eng.fence_wait("tsrc", at, timeout=5) is True
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    src = sql(["select id, qty, price, note, made, blob_, doc from orders"
               " order by id"], db="tsrc")[0]
    dst = sql(["select id, qty, price, note, made, blob_, doc from orders"
               " order by id"], db="tdst")[0]
    assert dst == src and [r[0] for r in dst] == [1, 20]


def test_a_table_without_a_key_is_not_followed(shop, tmp_path):
    sql(["create table bare (v int)"], db="tsrc")
    with pytest.raises(SystemExit) as e:
        _engine(tmp_path).change_point("src", "tsrc")
    assert "bare" in str(e.value) and "primary key" in str(e.value)
