"""A change stream delivered into Google Cloud Pub/Sub (backlog 33),
against the service's own emulator.

The messages are the same as Kafka's and Kinesis's, built by the same
code. Each carries its row's key as its ordering key, so a subscription
that asks for ordering gets a key's changes in the source's order.

Measured on the emulator before the delivery was written: a message
published to a topic with no subscription is accepted - the publish
returns its id - and a subscription made afterwards receives nothing. So
a topic without a subscription stops the delivery before anything is
sent to it, as a missing topic does.
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

PS, PS_PORT, PROJECT = "migkit-test-pubsub", 15852, "migkit-test"


@pytest.fixture(scope="module")
def emulator():
    subprocess.run(["docker", "rm", "-f", "-v", PS], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PS, "-p",
                    f"{PS_PORT}:8085",
                    "gcr.io/google.com/cloudsdktool/google-cloud-cli:"
                    "emulators", "gcloud", "beta", "emulators", "pubsub",
                    "start", "--host-port=0.0.0.0:8085",
                    f"--project={PROJECT}"], check=True, capture_output=True)
    try:
        import grpc
        from google.cloud import pubsub_v1
        from google.pubsub_v1.services.publisher.transports.grpc import \
            PublisherGrpcTransport
        from google.pubsub_v1.services.subscriber.transports.grpc import \
            SubscriberGrpcTransport
        end = time.time() + 90
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", PS_PORT)) == 0:
                    break
            time.sleep(1)
        at = f"127.0.0.1:{PS_PORT}"
        pub = pubsub_v1.PublisherClient(transport=PublisherGrpcTransport(
            channel=grpc.insecure_channel(at)))
        sub = pubsub_v1.SubscriberClient(transport=SubscriberGrpcTransport(
            channel=grpc.insecure_channel(at)))
        for _ in range(60):
            try:
                list(pub.list_topics(request={"project":
                                              f"projects/{PROJECT}"}))
                break
            except Exception:
                time.sleep(1)
        yield pub, sub
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PS], capture_output=True)


def _hop(pg_pair, name, **options):
    return Hop(name=name, engine="hetero",
               source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                               user="postgres", password="test"),
               target=Endpoint(host="127.0.0.1", port=PS_PORT, user="",
                               password="",
                               options={"project": PROJECT}),
               databases=["postgres"],
               options={"source_engine": "postgres",
                        "target_engine": "pubsub", **options})


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


def _topic(pub, sub, name, subscribed=True):
    path = pub.topic_path(PROJECT, name)
    pub.create_topic(request={"name": path})
    if subscribed:
        sub.create_subscription(request={
            "name": sub.subscription_path(PROJECT, name + "-reader"),
            "topic": path, "enable_message_ordering": True})


def _pull(sub, name):
    got = []
    path = sub.subscription_path(PROJECT, name + "-reader")
    for _ in range(10):
        reply = sub.pull(request={"subscription": path,
                                  "max_messages": 100}, timeout=5)
        if not reply.received_messages:
            break
        got += [(m.message.ordering_key, json.loads(m.message.data))
                for m in reply.received_messages]
        sub.acknowledge(request={"subscription": path, "ack_ids": [
            m.ack_id for m in reply.received_messages]})
    return got


def test_a_topic_that_is_not_there_stops_the_delivery(emulator, pg_pair,
                                                      table, tmp_path,
                                                      monkeypatch):
    _, ended = _tail(_hop(pg_pair, "pnone", topic="absent-{table}"),
                     tmp_path, monkeypatch, _changes(pg_pair))
    assert isinstance(ended.get("e"), SystemExit), ended
    assert str(ended["e"]).startswith(
        f"no topic absent-o in project {PROJECT}"), ended["e"]


def test_a_topic_nobody_reads_stops_the_delivery(emulator, pg_pair, table,
                                                 tmp_path, monkeypatch):
    pub, sub = emulator
    _topic(pub, sub, "unread-o", subscribed=False)
    _, ended = _tail(_hop(pg_pair, "punread", topic="unread-{table}"),
                     tmp_path, monkeypatch, _changes(pg_pair))
    assert isinstance(ended.get("e"), SystemExit), ended
    assert str(ended["e"]).startswith(
        "topic unread-o has no subscription"), ended["e"]
    assert "would be dropped" in str(ended["e"])


def test_every_change_in_the_sources_order(emulator, pg_pair, table,
                                           tmp_path, monkeypatch):
    pub, sub = emulator
    _topic(pub, sub, "cdc-o")
    _tail(_hop(pg_pair, "pjson", topic="cdc-{table}"), tmp_path,
          monkeypatch, _changes(pg_pair))
    got = _pull(sub, "cdc-o")
    by_key = {}
    for key, v in got:
        assert json.loads(key) == v["key"], (key, v)
        by_key.setdefault(v["key"]["id"], []).append(v["op"])
    assert by_key == {1: ["insert", "update"], 2: ["insert", "delete"]}, got
    first = next(v for _, v in got if v["op"] == "insert"
                 and v["key"]["id"] == 1)
    assert first["values"] == {"id": 1, "region": "eu", "amount": "1.50",
                               "note": "a"}, first


def test_a_publish_that_fails_stops_and_frees_its_key(monkeypatch,
                                                      tmp_path):
    """The client holds a key's later messages back once one of them
    fails. The delivery stops, and the key is released for the next
    start."""
    from migkit.engines.pubsub import PubSubEngine
    hop = Hop(name="pfail", engine="pubsub", source=Endpoint(),
              target=Endpoint(options={"project": PROJECT}),
              databases=["app"], options={"topic": "t-{table}"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PubSubEngine(hop)
    resumed = []

    class Refused:
        def result(self, timeout=None):
            raise RuntimeError("503 the service is unavailable")

    class Client:
        def topic_path(self, project, topic):
            return f"projects/{project}/topics/{topic}"

        def publish(self, path, data, ordering_key):
            return Refused()

        def resume_publish(self, path, key):
            resumed.append((path, key))
    eng.__dict__["_client"] = Client()
    eng.__dict__["_ready"] = {"t-o"}
    with pytest.raises(SystemExit) as e:
        eng.neutral_apply("dst", "app", [{"op": "insert", "table": "o",
                                          "key": {"id": 7},
                                          "values": {"id": 7}}])
    assert str(e.value).startswith("Pub/Sub did not take a change for topic"
                                   " t-o: 503 the service is unavailable")
    assert resumed == [(f"projects/{PROJECT}/topics/t-o", '{"id": 7}')]
