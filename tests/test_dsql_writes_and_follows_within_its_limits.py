"""Aurora DSQL as a side of a pair (backlog ER4).

There is no DSQL to run here: it is AWS's alone. What can be proved on
this machine is proved: the batches a DSQL transaction takes (3,000 rows,
9 MiB), each CDC record's values as DSQL's documentation writes them,
and the stream read out of commit order across its shards - against a
Kinesis of this machine (`instructure/kinesalite`) and a PostgreSQL
server standing in for DSQL's wire, where the rows are written and read
back. Against a real cluster, `migkit doctor` and `check` are the proof.
"""
import base64
import ctypes
import datetime
import json
import socket
import subprocess
import threading
import time
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import dsql
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.fence", "dsql"),
    ("tail.follows", "dsql"),
    ("cutover.drain", "dsql"),
]

PG, PG_PORT = "migkit-test-dsql-wire", 16349
KS, KS_PORT = "migkit-test-dsql-stream", 16348
STREAM = "dsql-cdc"


# ---- what a transaction may hold ----------------------------------------

def test_a_batch_goes_in_transactions_dsql_takes():
    eng = dsql.DsqlEngine(Hop(name="d", engine="dsql", source=Endpoint(),
                              target=Endpoint()))
    small = [[i, "x"] for i in range(7000)]
    assert [len(p) for p in eng._pieces(small)] == [3000, 3000, 1000]
    big = [[i, "y" * (4 * 2 ** 20)] for i in range(5)]
    got = [len(p) for p in eng._pieces(big)]
    # two 4 MiB rows fit 9 MiB, a third does not
    assert got == [2, 2, 1]
    for part in eng._pieces(big):
        assert sum(eng._bytes(r) for r in part) <= dsql.WRITE_BYTES


def test_a_record_is_read_as_dsqls_documentation_writes_it():
    ts = dsql._decoder("timestamp", "timestamp(6) without time zone")
    assert ts(1710510600123456) == datetime.datetime(
        2024, 3, 15, 13, 50, 0, 123456)
    tz = dsql._decoder("timestamp", "timestamp(6) with time zone")
    assert tz(0).tzinfo is not None
    assert dsql._decoder("date", "date")(19797) == datetime.date(2024, 3, 15)
    assert dsql._decoder("time", "time without time zone")(52200123456) \
        == datetime.time(14, 30, 0, 123456)
    assert dsql._decoder("decimal", "numeric(12,2)")("29.90") == \
        Decimal("29.90")
    assert dsql._decoder("bytes", "bytea")(
        base64.b64encode(b"\x00\xff").decode()) == b"\x00\xff"
    # a `real` as the 32-bit float it is, as the driver reads one
    assert dsql._decoder("float", "real")(0.1) == 0.10000000149011612
    assert dsql._decoder("float", "double precision")(0.1) == 0.1
    # an infinity's stand-in is no value: its row is read back instead
    with pytest.raises(dsql._Infinite):
        dsql._decoder("timestamp", "timestamp")(dsql.TS_INFINITE[0])


# ---- against a stand-in for its wire and a stream of this machine ------


def _wait(port, timeout=90):
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
    import boto3
    for name in (PG, KS):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p",
                    f"127.0.0.1:{PG_PORT}:5432", "postgres:16"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", KS, "-p",
                    f"127.0.0.1:{KS_PORT}:4567",
                    "instructure/kinesalite:latest"],
                   check=True, capture_output=True)
    try:
        assert _wait(PG_PORT) and _wait(KS_PORT)
        time.sleep(3)
        client = boto3.client("kinesis", region_name="us-east-1",
                              endpoint_url=f"http://127.0.0.1:{KS_PORT}",
                              aws_access_key_id="local",
                              aws_secret_access_key="local")
        for _ in range(30):
            try:
                client.create_stream(StreamName=STREAM, ShardCount=2)
                break
            except Exception:
                time.sleep(1)
        client.get_waiter("stream_exists").wait(StreamName=STREAM)
        yield client
    finally:
        for name in (PG, KS):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def run(statements, db="postgres"):
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=PG_PORT, user="postgres",
                            password="test", dbname=db)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        rows = []
        for s in statements:
            cur.execute(s)
            rows = cur.fetchall() if cur.description else []
        return rows
    finally:
        conn.close()


def _engine(tmp_path):
    def ep(**changes):
        return Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                        password="test",
                        options={"sslmode": "disable", **({"changes": {
                            "stream": STREAM, "region": "us-east-1",
                            "endpoint_url": f"http://127.0.0.1:{KS_PORT}",
                            "user": "local", "password": "local",
                            "order_window": 2}} if changes else {})})
    hop = Hop(name="dsql", engine="dsql", source=ep(on=True), target=ep(),
              databases=["dsrc"], db_map={"dsrc": "ddst"})
    hop.report_dir = lambda db=None: tmp_path
    return dsql.DsqlEngine(hop)


@pytest.fixture
def shop(servers):
    run(["drop database if exists dsrc", "drop database if exists ddst",
         "create database dsrc", "create database ddst"])
    for db in ("dsrc", "ddst"):
        run(["create table orders (id bigint primary key, price"
             " numeric(12,2), note text, made timestamp(6), raw bytea)",
             "create table spans (id bigint primary key, took interval)"],
            db=db)
    return servers


def put(client, records):
    """Records as DSQL puts them: each on a shard of its random key, in
    whatever order they arrive."""
    for i, (rec, shard_key) in enumerate(records):
        client.put_record(StreamName=STREAM, Data=json.dumps(rec).encode(),
                          PartitionKey=shard_key)


def rec(op, table, ts, after=None, before=None):
    return {"type": "full", "op": op, "before": before, "after": after,
            "source": {"version": "1.0", "ts_ms": ts // 10 ** 6,
                       "ts_ns": ts, "txId": f"t{ts}", "schema": "public",
                       "table": table, "db": "postgres",
                       "cluster": "example"},
            "ts_ms": ts // 10 ** 6, "ts_ns": ts}


@needs_docker
def test_a_write_lands_on_itself_in_pieces_dsql_takes(shop, tmp_path):
    eng = _engine(tmp_path)
    rows = [[i, Decimal("1.50"), "n", None, None] for i in range(7001)]
    cols = [("id", "integer"), ("price", "decimal"), ("note", "text"),
            ("made", "timestamp"), ("raw", "bytes")]
    eng.neutral_write("dst", "dsrc", "public.orders", cols, rows)
    eng.neutral_write("dst", "dsrc", "public.orders", cols, rows[:10])
    assert run(["select count(*) from orders"], db="ddst")[0][0] == 7001
    gone = eng.neutral_empty("dst", "dsrc", "public.orders")
    assert gone == 7001
    assert run(["select count(*) from orders"], db="ddst")[0][0] == 0


def _tail(eng, tmp_path):
    ended = {}

    def go():
        try:
            eng.tail_apply("dsrc", True, tmp_path / "tail-token.json",
                           lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=go, daemon=True)
    thread.start()
    return thread, ended


def _stop(thread):
    if thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    thread.join(timeout=30)


@needs_docker
def test_the_stream_applied_in_commit_order_across_its_shards(shop,
                                                              tmp_path):
    eng = _engine(tmp_path)
    eng.tail_start("dsrc", tmp_path / "tail-token.json")
    now = eng._now_ns("src", "dsrc")
    # the source's rows as they end up, and the records DSQL would send
    # for them - out of order, and one of them twice
    run(["insert into orders values (1, 3.00, 'last', '2024-01-01"
         " 00:00:00.5', '\\x00ff'), (2, 1.00, null, null, null)",
         "insert into spans values (7, '1 mon 2 days')"], db="dsrc")
    later = [rec("u", "orders", now + 3000, after={
                 "id": 1, "price": "3.00", "note": "last",
                 "made": 1704067200500000,
                 "raw": base64.b64encode(b"\x00\xff").decode()}),
             rec("c", "orders", now + 1000, after={
                 "id": 1, "price": "2.00", "note": "first", "made": None,
                 "raw": None}),
             rec("c", "orders", now + 2000, after={
                 "id": 2, "price": "1.00", "note": None, "made": None,
                 "raw": None}),
             rec("c", "orders", now + 1500, after={
                 "id": 3, "price": "9.00", "note": None, "made": None,
                 "raw": None}),
             rec("d", "orders", now + 2500, before={"id": 3}),
             # an interval's months are not in the record: read back
             rec("c", "spans", now + 1800, after={"id": 7,
                                                  "took": 2802603000000})]
    put(shop, [(later[0], "a"), (later[4], "b"), (later[1], "c"),
               (later[2], "d"), (later[3], "e"), (later[0], "f"),
               (later[5], "g")])
    thread, ended = _tail(eng, tmp_path)
    try:
        time.sleep(2)
        at = eng.src_lsn("dsrc")
        assert at is not None
        assert eng.fence_wait("dsrc", at, timeout=90) is True
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    assert run(["select id, price, note, made, raw from orders order by"
                " id"], db="ddst") == run(
        ["select id, price, note, made, raw from orders order by id"],
        db="dsrc")
    assert run(["select took::text from spans"], db="ddst") == \
        [("1 mon 2 days",)]
