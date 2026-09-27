"""Changes delivered to Kafka as Avro through a schema registry, and Avro
topics copied between two clusters that each keep their own registry.

Measured before: the change stream went only as JSON, and a copy of an
Avro topic between clusters carried the source registry's schema ids -
the target's consumers looked them up in their own registry, where they
name other schemas or none. Each message is now framed as a registry's
clients frame it (a zero byte, the schema's id, the body), in Debezium's
envelope; a copy registers each schema in the target's registry and
changes the id; and messages compare across clusters by the schema's
fingerprint, since each registry numbers schemas its own way.
"""
import datetime
import decimal
import json
import socket
import subprocess
import time
import urllib.request

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

CLUSTERS = {"src": ("migkit-test-avro-src", 15893, 15895),
            "dst": ("migkit-test-avro-dst", 15894, 15896)}


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
def clusters():
    for name, kafka, reg in CLUSTERS.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", name, "-p",
             f"{kafka}:{kafka}", "-p", f"{reg}:8081",
             "redpandadata/redpanda:latest", "redpanda", "start",
             "--overprovisioned", "--smp", "1", "--memory", "384M",
             "--reserve-memory", "0M", "--node-id", "0", "--check=false",
             "--kafka-addr", f"PLAINTEXT://0.0.0.0:{kafka}",
             "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{kafka}",
             "--schema-registry-addr", "0.0.0.0:8081"],
            check=True, capture_output=True)
    try:
        for _, kafka, reg in CLUSTERS.values():
            assert _wait(kafka) and _wait(reg)
        for _, _, reg in CLUSTERS.values():
            end = time.time() + 60
            while time.time() < end:
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{reg}/subjects",
                                           timeout=3)
                    break
                except Exception:  # noqa: BLE001 - not up yet
                    time.sleep(1)
        yield
    finally:
        for name, _, _ in CLUSTERS.values():
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _ep(side):
    _, kafka, reg = CLUSTERS[side]
    return Endpoint(host="127.0.0.1", port=kafka,
                    options={"schema_registry": f"http://127.0.0.1:{reg}"})


def _engine(tmp_path, **options):
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="av", engine="kafka", source=_ep("src"),
              target=_ep("src"), databases=["cluster"],
              db_map={"cluster": "cluster"},
              options={"format": "avro", "topic": "{db}.{table}",
                       **options})
    hop.report_dir = lambda db=None: tmp_path
    return KafkaEngine(hop)


def _read(port, topic, n, timeout=30):
    from kafka import KafkaConsumer
    c = KafkaConsumer(topic, bootstrap_servers=f"127.0.0.1:{port}",
                      auto_offset_reset="earliest", enable_auto_commit=False,
                      consumer_timeout_ms=5000)
    out, end = [], time.time() + timeout
    try:
        while len(out) < n and time.time() < end:
            for m in c:
                out.append((m.key, m.value))
                if len(out) >= n:
                    break
    finally:
        c.close()
    return out


def _decode(side, raw):
    from migkit import avrostream, registry
    return avrostream.decode(registry.of(_ep(side)), raw)


def test_changes_arrive_as_avro_in_debeziums_envelope(clusters, tmp_path):
    eng = _engine(tmp_path)
    at = datetime.datetime(2024, 5, 1, 12, 30, 0, 123456)
    eng.neutral_apply("dst", "app", [
        {"op": "insert", "table": "orders", "key": {"id": 1},
         "values": {"id": 1, "total": decimal.Decimal("12.50"),
                    "raw": b"\x00\xff", "at": at,
                    "day": datetime.date(2024, 5, 1),
                    "doc": {"b": 1, "a": [1, 2]}, "note": None}},
        {"op": "update", "table": "orders", "key": {"id": 1},
         "values": {"id": 1, "total": decimal.Decimal("13.00"),
                    "note": "changed"}},
        {"op": "delete", "table": "orders", "key": {"id": 1}}])
    got = _read(CLUSTERS["src"][1], "app.orders", 4)
    assert len(got) == 4, got
    # a tombstone after the delete, under the same key
    assert got[3][1] is None and got[3][0] == got[2][0]
    first, second, third = (_decode("src", v) for _, v in got[:3])
    assert first["op"] == "c" and first["before"] is None
    assert first["after"]["total"] == "12.50"
    assert first["after"]["raw"] == b"\x00\xff"
    assert first["after"]["at"] == at.replace(
        tzinfo=datetime.timezone.utc)
    assert first["after"]["day"] == datetime.date(2024, 5, 1)
    assert json.loads(first["after"]["doc"]) == {"a": [1, 2], "b": 1}
    assert first["source"]["db"] == "app"
    assert second["op"] == "u" and second["after"]["note"] == "changed"
    assert third["op"] == "d" and third["after"] is None
    assert third["before"]["id"] == 1
    assert _decode("src", got[0][0]) == {"id": 1}


def test_a_new_column_is_a_new_version_the_registry_takes(clusters,
                                                         tmp_path):
    eng = _engine(tmp_path)
    eng.neutral_apply("dst", "app", [
        {"op": "insert", "table": "grows", "key": {"id": 1},
         "values": {"id": 1, "a": 1}}])
    eng.neutral_apply("dst", "app", [
        {"op": "insert", "table": "grows", "key": {"id": 2},
         "values": {"id": 2, "a": 2, "b": "new"}}])
    reg = CLUSTERS["src"][2]
    versions = json.loads(urllib.request.urlopen(
        f"http://127.0.0.1:{reg}/subjects/app.grows-value/versions").read())
    assert len(versions) == 2, versions
    got = _read(CLUSTERS["src"][1], "app.grows", 2)
    # the first message still reads under the schema it was written with
    assert [_decode("src", v)["after"] for _, v in got] == [
        {"id": 1, "a": 1}, {"id": 2, "a": 2, "b": "new"}]


def test_a_schema_the_registry_refuses_stops_and_says_so(clusters,
                                                         tmp_path):
    reg = CLUSTERS["src"][2]
    eng = _engine(tmp_path)
    eng.neutral_apply("dst", "app", [
        {"op": "insert", "table": "strict", "key": {"id": 1},
         "values": {"id": 1, "v": 1}}])
    req = urllib.request.Request(
        f"http://127.0.0.1:{reg}/config/app.strict-value", method="PUT",
        data=json.dumps({"compatibility": "FULL"}).encode(),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    urllib.request.urlopen(req).read()
    with pytest.raises(SystemExit, match="refused the table's new schema"):
        # the same field now holding text: a reader of the old schema
        # could not read it
        eng.neutral_apply("dst", "app", [
            {"op": "insert", "table": "strict", "key": {"id": 2},
             "values": {"id": 2, "v": "text"}}])


def test_an_avro_topic_copied_between_registries(clusters, tmp_path):
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic

    from migkit import avrostream, registry
    from migkit.cli import _Checkpoint
    from migkit.engines.kafka import KafkaEngine
    # the target's registry numbers schemas its own way: one it holds
    # already, so the ids differ
    registry.of(_ep("dst")).register("other-value", {
        "type": "record", "name": "Other", "fields": [
            {"name": "x", "type": "long"}]})
    admin = KafkaAdminClient(
        bootstrap_servers=f"127.0.0.1:{CLUSTERS['src'][1]}")
    try:
        admin.create_topics([NewTopic("copied", 1, 1)])
    finally:
        admin.close()
    enc = avrostream.Encoder(registry.of(_ep("src")))
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=f"127.0.0.1:{CLUSTERS['src'][1]}")
    for i in range(20):
        for key, value in enc.encode("copied", "app", {
                "op": "insert", "table": "copied", "key": {"id": i},
                "values": {"id": i, "v": f"row {i}"}}, 1000):
            producer.send("copied", key=key, value=value)
    producer.flush()
    producer.close()
    # the other tests' streams are not this copy's
    hop = Hop(name="cp", engine="kafka", source=_ep("src"),
              target=_ep("dst"), databases=["cluster"], exclude=["app.*"])
    hop.report_dir = lambda db=None: tmp_path
    eng = KafkaEngine(hop)
    eng.move_table("cluster", "", "copied", 1000,
                   _Checkpoint(tmp_path / "m.json"), [].append)
    src = _read(CLUSTERS["src"][1], "copied", 20)
    dst = _read(CLUSTERS["dst"][1], "copied", 20)
    assert len(dst) == 20
    # other ids on the target, the same records
    assert registry.frame_of(src[0][1])[0] != registry.frame_of(
        dst[0][1])[0]
    assert [_decode("dst", v) for _, v in dst] == \
        [_decode("src", v) for _, v in src]
    # and compared equal: the registries' own stores are not data
    got = eng.check_data("cluster")
    assert got and all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]
    assert "copied" in str(eng.list_move_tables("cluster"))
    assert "_schemas" not in str(eng.list_move_tables("cluster"))


def test_msk_iam_is_signed_in_over_sasl_ssl_only(tmp_path):
    from kafka.net.sasl import get_sasl_mechanism

    from migkit.engines.kafka import KafkaEngine
    ep = Endpoint(host="10.0.0.9", port=9098,
                  options={"security_protocol": "SASL_SSL",
                           "sasl_mechanism": "AWS_MSK_IAM"})
    eng = KafkaEngine(Hop(name="msk", engine="kafka", source=ep, target=ep))
    got = eng._connection("src")
    assert got["sasl_mechanism"] == "AWS_MSK_IAM"
    assert "sasl_plain_password" not in got
    assert get_sasl_mechanism("AWS_MSK_IAM") is not None
    ep.options["security_protocol"] = "SASL_PLAINTEXT"
    with pytest.raises(SystemExit, match="over SASL_SSL only"):
        eng._connection("src")
