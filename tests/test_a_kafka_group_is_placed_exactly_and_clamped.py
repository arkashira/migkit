"""A consumer group is put on the target at exactly the message it reads
next, and never past what the target holds (backlog F7.mm2, F7.kclamp).

The copier writes each message's source position in a header where the
hop asks (`offset_header`). Here the messages are all alike - one key, one
value, one time - so the message itself cannot say which is which: the
header is what puts the group at the right one, where a search by the
message puts it at the first look-alike. The target began with five
messages of its own, so no number means the same thing on both sides.

Then the source takes ten messages the target has not got yet. A group
that read everything the target holds is put at the target's end - after
the last message it read, so it reads none twice and skips none when the
rest arrive - and that is said; one that read past what the target holds
has nowhere to go and is left, said. At a cutover the plan is made then
(`repair_plan`), and going back puts the target's groups as they were,
a group migkit made removed again.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-test-kclamp-src", "migkit-test-kclamp-dst"
SRC_PORT, DST_PORT = 16282, 16283
AT = 1_700_000_000_000


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _broker(name, port):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run([
        "docker", "run", "-d", "--name", name,
        "-p", f"127.0.0.1:{port}:{port}",
        "-e", "KAFKA_NODE_ID=1",
        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", f"KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:{port},"
              "CONTROLLER://0.0.0.0:9093",
        "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,"
              "PLAINTEXT:PLAINTEXT",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
        "apache/kafka:3.8.0"], check=True, capture_output=True)


def _produce(port, n, value=b"same", key=b"k", at=AT):
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=f"127.0.0.1:{port}")
    for _ in range(n):
        p.send("orders", key=key, value=value, partition=0, timestamp_ms=at)
    p.flush()
    p.close()


def _commit(port, group, offset):
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}",
                      group_id=group, enable_auto_commit=False)
    tp = TopicPartition("orders", 0)
    c.assign([tp])
    c.commit({tp: OffsetAndMetadata(offset, "", -1)})
    c.close()


def _committed(port, group):
    from kafka.admin import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers=f"127.0.0.1:{port}")
    try:
        got = admin.list_group_offsets(group).get(group, {})
        groups = {g.get("group_id") if isinstance(g, dict) else g[0]
                  for g in admin.list_groups()}
    finally:
        admin.close()
    return ({tp.partition: m.offset for tp, m in got.items()
             if m.offset >= 0}, group in groups)


@pytest.fixture(scope="module")
def clusters():
    from kafka.admin import KafkaAdminClient, NewTopic
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        _broker(name, port)
    try:
        for port in (SRC_PORT, DST_PORT):
            if not _wait(port):
                pytest.skip("kafka did not come up in this sandbox")
        for port in (SRC_PORT, DST_PORT):
            for _ in range(30):
                try:
                    admin = KafkaAdminClient(
                        bootstrap_servers=f"127.0.0.1:{port}")
                    admin.create_topics([NewTopic("orders", 1, 1)])
                    admin.close()
                    break
                except Exception:  # noqa: BLE001 - not up yet
                    time.sleep(2)
        time.sleep(2)
        # the target's own five first, at another time and value
        _produce(DST_PORT, 5, value=b"the target's own", at=AT - 60_000)
        _produce(SRC_PORT, 50)
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path, header=True):
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="kclamp", engine="kafka",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              db_map={"cluster": "cluster"},
              options={"offset_header": header})
    hop.report_dir = lambda db=None: tmp_path
    return KafkaEngine(hop)


class _Ck(dict):
    def save(self):
        pass


def _headers(port):
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}",
                      enable_auto_commit=False)
    tp = TopicPartition("orders", 0)
    c.assign([tp])
    c.seek_to_beginning(tp)
    out, end = [], c.end_offsets([tp])[tp]
    while len(out) < end:
        batch = c.poll(timeout_ms=5000)
        if not batch:
            break
        out += [(m.offset, dict(m.headers or {})) for msgs in batch.values()
                for m in msgs]
    c.close()
    return out


def test_each_copied_message_carries_where_it_came_from(clusters,
                                                        tmp_path):
    eng = _engine(tmp_path)
    eng.move_table("cluster", "", "orders", 1000, _Ck(), lambda m: None)
    got = _headers(DST_PORT)
    assert len(got) == 55
    # the target's own carry none; each copied one its source position
    assert all(eng.HEADER not in h for _, h in got[:5])
    assert [h[eng.HEADER] for _, h in got[5:]] == \
        [f"0:{i}".encode() for i in range(50)]


def test_the_group_goes_to_the_very_message_by_its_header(clusters,
                                                          tmp_path):
    from kafka import TopicPartition
    eng = _engine(tmp_path)
    _commit(SRC_PORT, "billing", 20)
    rows, unsure, _ = eng.group_positions("cluster")
    row = next(r for r in rows if r["group"] == "billing")
    assert row["target"] == 25, row
    assert "source position" in row["how"], row
    # without the header the look-alikes are all the message can say: the
    # first of them, which is not the one the group reads next
    sc, dc = eng._consumer("src"), eng._consumer("dst")
    try:
        tp = TopicPartition("orders", 0)
        by_message = eng._find(dc, tp, eng._message_at(sc, tp, 20))
    finally:
        sc.close()
        dc.close()
    assert by_message == 5


def test_a_group_past_what_the_target_holds_is_clamped_or_left(clusters,
                                                               tmp_path):
    eng = _engine(tmp_path)
    _produce(SRC_PORT, 10, value=b"not copied yet")
    # read everything the target holds: put at its end, and said
    _commit(SRC_PORT, "late", 50)
    # read five the target has not got: nowhere to go
    _commit(SRC_PORT, "ahead", 55)
    rows, unsure, _ = eng.group_positions("cluster")
    late = next(r for r in rows if r["group"] == "late")
    assert late["target"] == 55, late
    assert "10 it reads next have not reached the target" in late["note"]
    assert not [r for r in rows if r["group"] == "ahead"]
    assert any(u.startswith("ahead orders[0]") and "not on the target" in u
               for u in unsure), unsure
    checked = {r.scope: r for r in eng.check_deep("cluster")}
    assert "have not reached the target" in \
        checked["group-offsets"].detail


def test_the_cutovers_plan_is_made_then_and_undone_as_it_was(clusters,
                                                             tmp_path):
    from migkit.engines.base import RepairAction
    eng = _engine(tmp_path)
    _commit(SRC_PORT, "billing", 20)
    _commit(DST_PORT, "billing", 3)
    _commit(SRC_PORT, "late", 50)
    before = _committed(DST_PORT, "late")
    assert before == ({}, False), before
    actions = eng.repair_plan("cluster", "sequences")
    assert len(actions) == 1
    rows = [json.loads(s) for s in actions[0].statements[1:]]
    assert {(r["group"], r["offset"]) for r in rows} >= {("billing", 25),
                                                         ("late", 55)}
    assert "have not reached the target" in actions[0].note
    eng.apply("cluster", actions[0])
    assert _committed(DST_PORT, "billing")[0] == {0: 25}
    assert _committed(DST_PORT, "late")[0] == {0: 55}
    # going back: the target's groups as they were, the one migkit made
    # gone
    eng.apply("cluster", RepairAction("cluster", "sequences",
                                      actions[0].undo, [], "undone"))
    assert _committed(DST_PORT, "billing")[0] == {0: 3}
    assert _committed(DST_PORT, "late") == ({}, False)
