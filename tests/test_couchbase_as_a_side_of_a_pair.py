"""Couchbase as a side of a pair (backlog ER4), against a Couchbase server
of its own and a Kafka broker its connector's topic is published on.

A collection is read by the key-value range scan, written by key and
compared document by document. Its changes are the connector's messages:
made here as the connector makes them, each with the vBucket, UUID and
sequence number the server itself gave the write (the SDK's mutation
token) - so the fence, the server's own high sequence numbers read over
memcached's binary protocol, is proved against what the server says.
"""
import base64
import ctypes
import json
import socket
import subprocess
import threading
import time
import urllib.parse
import urllib.request

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.fence", "couchbase"),
    ("tail.follows", "couchbase"),
    ("schema.create", "couchbase"),
    ("move.table-copy", "couchbase"),
    ("cutover.drain", "couchbase"),
]

pytestmark = needs_docker

CB, KV, MGMT = "migkit-test-couchbase", 16342, 16343
KAFKA, KAFKA_PORT = "migkit-test-changes-kafka", 16347
TOPIC = "couchbase-shop"
USER, PW = "migkit", "migkit-pw"


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def rest(path, data=None, method=None, auth=True):
    req = urllib.request.Request(
        f"http://127.0.0.1:{MGMT}{path}",
        data=urllib.parse.urlencode(data).encode() if data else None,
        method=method)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(
            f"{USER}:{PW}".encode()).decode())
    with urllib.request.urlopen(req, timeout=30) as got:
        body = got.read()
        return json.loads(body) if body.strip().startswith((b"{", b"[")) \
            else body


@pytest.fixture(scope="module")
def servers():
    for name in (CB, KAFKA):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", CB,
                    "-p", f"127.0.0.1:{MGMT}:8091",
                    "-p", f"127.0.0.1:{KV}:11210",
                    "couchbase:community-7.6.2"],
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
        assert _wait(MGMT) and _wait(KAFKA_PORT)
        for _ in range(60):
            try:
                rest("/pools", auth=False)
                break
            except Exception:
                time.sleep(2)
        rest("/nodes/self/controller/settings", {"path": "/opt/couchbase/"
                                                 "var/lib/couchbase/data"},
             auth=False)
        rest("/node/controller/setupServices", {"services": "kv"},
             auth=False)
        rest("/pools/default", {"memoryQuota": 512}, auth=False)
        rest("/settings/web", {"username": USER, "password": PW,
                               "port": "SAME"}, auth=False)
        # the node as this machine reaches it
        rest("/node/controller/setupAlternateAddresses/external",
             {"hostname": "127.0.0.1", "mgmt": MGMT, "kv": KV},
             method="PUT")
        for b in ("cbsrc", "cbdst"):
            rest("/pools/default/buckets", {"name": b, "ramQuota": 200,
                                            "bucketType": "couchbase",
                                            "flushEnabled": 1})
        time.sleep(10)
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
        for name in (CB, KAFKA):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _ep(**changes):
    return Endpoint(host="127.0.0.1", port=KV, user=USER, password=PW,
                    options={"network": "external", "mgmt_port": MGMT,
                             **({"changes": {
                                 "servers": f"127.0.0.1:{KAFKA_PORT}",
                                 "topic": TOPIC}} if changes else {})})


def _engine(tmp_path):
    from migkit.engines.couchbase import CouchbaseEngine
    hop = Hop(name="cb", engine="couchbase", source=_ep(on=True),
              target=_ep(), databases=["cbsrc"], db_map={"cbsrc": "cbdst"})
    hop.report_dir = lambda db=None: tmp_path
    return CouchbaseEngine(hop)


def _coll(eng, side, db, table="inv.items"):
    return eng._collection(side, db, table)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    # where a collection migkit made is described: this test's own
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


@pytest.fixture
def shop(servers, tmp_path):
    eng = _engine(tmp_path)
    src = eng._bucket("src", "cbsrc").collections()
    if "inv" not in {s.name for s in src.get_all_scopes()}:
        src.create_scope("inv")
        src.create_collection("inv", "items")
        time.sleep(2)
    for got in eng._scan("src", "cbsrc", "inv.items", ids_only=True):
        _coll(eng, "src", "cbsrc").remove(got.id)
    # the target's collection is migkit's to make, and its description
    dst = eng._bucket("dst", "cbsrc").collections()
    for scope in dst.get_all_scopes():
        if "items" in {c.name for c in scope.collections}:
            dst.drop_collection(scope.name, "items")
            time.sleep(2)
    src = _coll(eng, "src", "cbsrc")
    for i in range(40):
        src.upsert(f"item::{i}", {"n": i, "name": f"i{i}", "price": i * 1.5,
                                  "tags": ["a", "b"], "on": i % 2 == 0})
    return eng


def _move(eng, tmp_path):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing("cbsrc", lambda m: None)
    for sch, t in eng.list_move_tables("cbsrc"):
        eng.move_table("cbsrc", sch, t, 100, ck, lambda m: None)


def _data(eng):
    return {r.scope: r.status for r in eng.check_data("cbsrc")
            if r.check == "data"}


def test_a_collection_copied_and_compared_document_for_document(shop,
                                                                tmp_path):
    eng = shop
    _move(eng, tmp_path)
    got = _data(eng)
    assert got and set(got.values()) == {"ok"}, got
    # made from another side's table, the collection is in the target
    # bucket's default scope
    _coll(eng, "dst", "cbsrc", "_default.items").upsert(
        "item::3", {"n": 3, "name": "i3", "price": 4.5, "tags": ["a"],
                    "on": False})
    got = _data(_engine(tmp_path))
    assert "diff" in got.values(), got


def test_the_fence_is_the_servers_own_sequence_numbers(shop, tmp_path):
    eng = _engine(tmp_path)
    before = eng.seqnos("src", "cbsrc")
    assert len(before) >= 64
    got = _coll(eng, "src", "cbsrc").upsert("item::fence", {"n": -1})
    token = got.mutation_token()
    after = eng.seqnos("src", "cbsrc")
    vb = f"vb{token.partition_id}"
    assert after[vb][1] == token.sequence_number > before[vb][1]
    assert after[vb][0] == token.partition_uuid
    # a tail that read up to what came before has not reached this
    assert eng.position_reached({"seen": before}, {"seen": after}) is False
    assert eng.position_reached({"seen": after}, {"seen": after}) is True


def _publish(messages):
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=f"127.0.0.1:{KAFKA_PORT}")
    for key, value in messages:
        # the connector keys a document's changes by its key: one
        # partition, in order
        p.send(TOPIC, key=key.encode(), value=json.dumps(value).encode())
    p.flush()
    p.close()


def dcp(token, key, body=None, event="mutation"):
    return {"schema": {"type": "struct", "name": "DcpMessage"},
            "payload": {"event": event, "partition": token.partition_id,
                        "key": key, "bySeqno": token.sequence_number,
                        "vBucketUuid": token.partition_uuid,
                        "bucket": "cbsrc", "scope": "inv",
                        "collection": "items", "content": base64.b64encode(
                            json.dumps(body).encode()).decode()
                        if body is not None else None}}


def _tail(eng, tmp_path):
    ended = {}

    def go():
        try:
            eng.tail_apply("cbsrc", True, tmp_path / "tail-token.json",
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


def test_a_copy_and_the_connectors_topic_end_equal(shop, tmp_path):
    eng = shop
    eng.tail_start("cbsrc", tmp_path / "tail-token.json")
    _move(eng, tmp_path)
    src = _coll(eng, "src", "cbsrc")
    body = {"n": 5, "name": "changed", "price": 0.25, "tags": [],
            "on": True}
    t1 = src.upsert("item::5", body).mutation_token()
    t2 = src.upsert("item::new", {"n": 99}).mutation_token()
    t3 = src.remove("item::7").mutation_token()
    _publish([("item::5", dcp(t1, "item::5", body)),
              ("item::new", dcp(t2, "item::new", {"n": 99})),
              ("item::7", dcp(t3, "item::7", event="deletion")),
              # the first delivered again after a connector restart
              ("item::5", dcp(t1, "item::5", body))])
    thread, ended = _tail(eng, tmp_path)
    try:
        time.sleep(2)
        at = eng.src_lsn("cbsrc")
        assert at is not None
        assert eng.fence_wait("cbsrc", at, timeout=90) is True
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    got = _data(_engine(tmp_path))
    assert set(got.values()) == {"ok"}, got
