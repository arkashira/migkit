"""A change stream delivered into Kafka topics, in the formats consumers
expect (backlog 35).

A hop `engine: hetero` with `target_engine: kafka` carries the source's
changes as messages: plain JSON, a Debezium envelope, or Canal's shape.
The topic comes from a template, the partition from the row's key or a
column, and a message larger than the limit is skipped and counted
rather than sent or lost without a word. Every change is a message, in
the source's order - not collapsed to a row's last state, as a table's
rows are.
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

RP, RP_PORT = "migkit-test-deliver-rp", 15795


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
def broker():
    subprocess.run(["docker", "rm", "-f", "-v", RP], capture_output=True)
    try:
        subprocess.run(
            ["docker", "run", "-d", "--name", RP, "-p",
             f"{RP_PORT}:{RP_PORT}", "redpandadata/redpanda:latest",
             "redpanda", "start", "--overprovisioned", "--smp", "1",
             "--memory", "512M", "--reserve-memory", "0M", "--node-id",
             "0", "--check=false",
             "--kafka-addr", f"PLAINTEXT://0.0.0.0:{RP_PORT}",
             "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{RP_PORT}"],
            check=True, capture_output=True)
        if not _wait(RP_PORT):
            pytest.fail("the broker never answered")
        time.sleep(5)
        yield RP_PORT
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", RP], capture_output=True)


def _hop(pg_pair, name, **options):
    return Hop(name=name, engine="hetero",
               source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                               user="postgres", password="test"),
               target=Endpoint(host="127.0.0.1", port=RP_PORT, user="",
                               password=""),
               databases=["postgres"],
               options={"source_engine": "postgres",
                        "target_engine": "kafka", **options})


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
            if any(m.endswith("changes") for m in said):
                break
            time.sleep(0.5)
        time.sleep(2)
        assert thread.is_alive(), (ended, said)
    finally:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident),
            ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    return said


def _read(topic):
    from kafka import KafkaConsumer
    c = KafkaConsumer(topic, bootstrap_servers=f"127.0.0.1:{RP_PORT}",
                      auto_offset_reset="earliest", consumer_timeout_ms=5000,
                      enable_auto_commit=False)
    try:
        return [(m.partition, json.loads(m.key), json.loads(m.value))
                for m in c]
    finally:
        c.close()


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


def test_every_change_as_json_in_the_sources_order(broker, pg_pair, table,
                                                   tmp_path, monkeypatch):
    hop = _hop(pg_pair, "kjson")
    _tail(hop, tmp_path, monkeypatch, _changes(pg_pair))
    got = _read("postgres.o")
    ops = [(v["op"], v["key"]["id"]) for _, _, v in got]
    # every change, not the rows' last states
    assert ops == [("insert", 1), ("insert", 2), ("update", 1),
                   ("delete", 2)], got
    first = got[0][2]
    assert first["values"] == {"id": 1, "region": "eu", "amount": "1.50",
                               "note": "a"}, first
    assert got[3][2]["values"] is None
    # one key, one partition, so its changes stay in order
    by_key = {}
    for part, key, _ in got:
        by_key.setdefault(key["id"], set()).add(part)
    assert all(len(p) == 1 for p in by_key.values()), by_key


def test_a_debezium_envelope_and_a_canal_message(broker, pg_pair, table,
                                                 tmp_path, monkeypatch):
    _tail(_hop(pg_pair, "kdbz", format="debezium", topic="cdc.{table}"),
          tmp_path / "d", monkeypatch, _changes(pg_pair))
    got = [v for _, _, v in _read("cdc.o")]
    assert [v["op"] for v in got] == ["c", "c", "u", "d"], got
    assert got[0]["before"] is None and got[0]["after"]["id"] == 1, got[0]
    assert got[3]["after"] is None and got[3]["before"] == {"id": 2}
    assert got[2]["source"]["table"] == "o" and got[2]["ts_ms"] > 0
    psql(pg_pair["src"], "delete from public.o")
    _tail(_hop(pg_pair, "kcanal", format="canal", topic="canal.{table}"),
          tmp_path / "c", monkeypatch, _changes(pg_pair))
    got = [v for _, _, v in _read("canal.o")]
    assert [v["type"] for v in got] == ["INSERT", "INSERT", "UPDATE",
                                        "DELETE"], got
    assert got[0]["pkNames"] == ["id"] and got[0]["database"] == "postgres"
    assert got[0]["data"][0]["region"] == "eu", got[0]


def test_partition_by_a_column(broker, pg_pair, table, tmp_path,
                               monkeypatch):
    _tail(_hop(pg_pair, "kpart", topic="by.{table}", partition_by="region"),
          tmp_path, monkeypatch, _changes(pg_pair))
    keys = [k for _, k, _ in _read("by.o")]
    assert keys[:2] == [{"region": "eu"}, {"region": "us"}], keys


def test_a_message_too_large_is_skipped_and_counted(broker, pg_pair, table,
                                                     tmp_path, monkeypatch):
    def write():
        psql(pg_pair["src"], "insert into public.o values (1, 'eu', 1,"
                             " repeat('x', 3000)), (2, 'us', 2, 'small')")
    hop = _hop(pg_pair, "kbig", topic="big.{table}", max_message_bytes=1000)
    _tail(hop, tmp_path, monkeypatch, write)
    got = _read("big.o")
    assert [v["key"]["id"] for _, _, v in got] == [2], got
    counted = json.loads((tmp_path / "kbig" / "postgres" /
                          "stream-skipped.json").read_text())
    assert counted == {"big.o": 1}, counted


def test_a_format_it_does_not_know_is_refused():
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="x", engine="kafka",
              source=Endpoint(host="h", port=1, user="", password=""),
              target=Endpoint(host="h", port=1, user="", password=""),
              options={"format": "avro"})
    with pytest.raises(SystemExit, match="avro is not one of"):
        KafkaEngine(hop)._stream_options()
