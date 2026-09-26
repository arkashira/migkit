"""A change stream delivered into Amazon Kinesis Data Streams (backlog 33),
against a local stand-in for the service.

The messages are the same as Kafka's, built by the same code. A key's
records go to one shard in the source's order. A stream the target does
not have stops the delivery unless the hop says to create it, since a
stream costs money for as long as it exists. When Kinesis keeps some
records of a batch and refuses others, a refused record goes again before
its key's next change: a batch holds each key once.
"""
import ctypes
import json
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

KS, KS_PORT = "migkit-test-kinesis", 15843


@pytest.fixture(scope="module")
def kinesis():
    import boto3
    subprocess.run(["docker", "rm", "-f", "-v", KS], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", KS, "-p",
                    f"{KS_PORT}:4567", "instructure/kinesalite:latest"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 60
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", KS_PORT)) == 0:
                    break
            time.sleep(1)
        client = boto3.client("kinesis", region_name="us-east-1",
                              endpoint_url=f"http://127.0.0.1:{KS_PORT}",
                              aws_access_key_id="local",
                              aws_secret_access_key="local")
        for _ in range(30):
            try:
                client.list_streams()
                break
            except Exception:
                time.sleep(1)
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", KS], capture_output=True)


def _hop(pg_pair, name, **options):
    return Hop(name=name, engine="hetero",
               source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                               user="postgres", password="test"),
               target=Endpoint(user="local", password="local",
                               options={"endpoint_url":
                                        f"http://127.0.0.1:{KS_PORT}"}),
               databases=["postgres"],
               options={"source_engine": "postgres",
                        "target_engine": "kinesis", **options})


def _tail(hop, tmp_path, monkeypatch, write):
    """Run the tail, make the changes, stop it once they are delivered."""
    import migkit.config as cfg
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    eng = HeteroEngine(hop)
    said, ended = [], {}

    def run():
        try:
            eng.tail_apply("postgres", True,
                           hop.report_dir("postgres") / "tail-token.json",
                           said.append)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(4)
    try:
        write()
        for _ in range(60):
            if any(m.endswith("changes") for m in said) or ended:
                break
            time.sleep(0.5)
        time.sleep(2)
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    return said, ended


def _read(client, stream):
    shards = client.describe_stream(StreamName=stream)[
        "StreamDescription"]["Shards"]
    out = []
    for shard in shards:
        it = client.get_shard_iterator(
            StreamName=stream, ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        got = client.get_records(ShardIterator=it, Limit=1000)
        out += [(shard["ShardId"], json.loads(r["PartitionKey"]),
                 json.loads(r["Data"])) for r in got["Records"]]
    return out


@pytest.fixture
def table(pg_pair):
    psql(pg_pair["src"], "drop table if exists public.o;"
                         " create table public.o (id int primary key,"
                         " region text, amount numeric(8,2), note text)")
    yield
    psql(pg_pair["src"], "select pg_drop_replication_slot(slot_name) from"
                         " pg_replication_slots where not active")
    psql(pg_pair["src"], "drop table if exists public.o")


def _changes(pg_pair):
    def write():
        psql(pg_pair["src"], "insert into public.o values (1, 'eu', 1.50,"
                             " 'a'), (2, 'us', 2.00, 'b')")
        psql(pg_pair["src"], "update public.o set amount = 9.25 where id = 1")
        psql(pg_pair["src"], "delete from public.o where id = 2")
    return write


def test_a_stream_it_may_not_create_stops_the_delivery(kinesis, pg_pair,
                                                       table, tmp_path,
                                                       monkeypatch):
    _, ended = _tail(_hop(pg_pair, "knone", topic="nostream-{table}"),
                     tmp_path, monkeypatch, _changes(pg_pair))
    assert isinstance(ended.get("e"), SystemExit), ended
    assert "no stream nostream-o on the target" in str(ended["e"])
    assert "nostream-o" not in kinesis.list_streams()["StreamNames"]


def test_every_change_in_the_sources_order(kinesis, pg_pair, table,
                                           tmp_path, monkeypatch):
    hop = _hop(pg_pair, "kjson", topic="cdc-{table}", create_streams=True,
               shards=2)
    _tail(hop, tmp_path, monkeypatch, _changes(pg_pair))
    got = _read(kinesis, "cdc-o")
    ops = sorted(((shard, v["op"], v["key"]["id"]) for shard, _, v in got),
                 key=lambda r: r[0])
    by_key = {}
    for shard, op, key in ops:
        by_key.setdefault(key, []).append((shard, op))
    # one key, one shard, its changes in the order they were made
    assert [op for _, op in by_key[1]] == ["insert", "update"], got
    assert [op for _, op in by_key[2]] == ["insert", "delete"], got
    assert all(len({s for s, _ in v}) == 1 for v in by_key.values()), got
    first = next(v for _, _, v in got if v["op"] == "insert"
                 and v["key"]["id"] == 1)
    assert first["values"] == {"id": 1, "region": "eu", "amount": "1.50",
                               "note": "a"}, first


def test_a_refused_record_goes_again_ahead_of_its_keys_next(kinesis):
    """The service keeps some records of a batch and refuses others. A
    batch holds each key once, so the refused record goes again before
    its key's next change is put: each key's changes arrive in order, and
    none twice."""
    from migkit.engines.kinesis import KinesisEngine
    kinesis.create_stream(StreamName="retry", ShardCount=1)
    kinesis.get_waiter("stream_exists").wait(StreamName="retry")
    refused = {"once": True}

    class Refusing:
        def __init__(self, client):
            self.client = client

        def put_records(self, StreamName, Records):
            if refused["once"]:
                # the service takes the rest, and refuses key b's record
                refused["once"] = False
                kept = [r for r in Records
                        if r["PartitionKey"] != json.dumps("b")]
                self.client.put_records(StreamName=StreamName, Records=kept)
                return {"FailedRecordCount": len(Records) - len(kept),
                        "Records": [{"ErrorCode": "ProvisionedThroughput"
                                                  "Exceeded"}
                                    if r["PartitionKey"] == json.dumps("b")
                                    else
                                    {"SequenceNumber": "1"}
                                    for r in Records]}
            return self.client.put_records(StreamName=StreamName,
                                           Records=Records)
    records = [{"Data": json.dumps({"key": k, "n": n}).encode(),
                "PartitionKey": json.dumps(k)}
               for n, k in [(0, "a"), (0, "b"), (1, "a"), (1, "b"),
                            (2, "a")]]
    KinesisEngine._put(Refusing(kinesis), "retry", records)
    got = [(v["key"], v["n"]) for _, _, v in _read(kinesis, "retry")]
    assert [n for k, n in got if k == "a"] == [0, 1, 2], got
    assert [n for k, n in got if k == "b"] == [0, 1], got
