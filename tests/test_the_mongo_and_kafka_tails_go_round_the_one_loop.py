"""MongoDB's change stream and Kafka's topics are followed round the loop
every tail goes round (audit 2026-10-01, ST14, K14, SP19, LG2).

MongoDB's tail had no stop, no end once caught up, no retry, saved its
position every hundred events in place, and took SIGTERM as a crash;
Kafka's had no end once caught up. Both now go round `tailctl.run_loop`:
held here on one MongoDB replica set (the target a database of its own,
`db_map`) and two Redpanda brokers - each ends once caught up where the
hop says so, each stopped dead between the target taking a batch and its
position being saved goes on without a change lost or one written twice,
and MongoDB's deep check says what it could not read of the sharding
instead of "not a sharded cluster".
"""
import os
import socket
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from migkit import tailctl
from tests.conftest import needs_docker

pytestmark = needs_docker

MG, MG_PORT = "migkit-test-audb-mongo", 16492
KSRC, KDST = "migkit-test-audb-kafka-src", "migkit-test-audb-kafka-dst"
KSRC_PORT, KDST_PORT = 16493, 16494


def _mongosh(script, db="admin"):
    return subprocess.run(["docker", "exec", MG, "mongosh", "--quiet", db,
                           "--eval", script], capture_output=True, text=True)


@pytest.fixture(scope="module")
def mongo():
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"127.0.0.1:{MG_PORT}:27017", "mongo:7", "--replSet",
                    "rs0", "--bind_ip_all"], check=True, capture_output=True)
    try:
        for _ in range(40):
            if _mongosh("db.runCommand({ping:1}).ok").returncode == 0:
                break
            time.sleep(2)
        _mongosh('rs.initiate({_id:"rs0",members:'
                 '[{_id:0,host:"127.0.0.1:27017"}]})')
        for _ in range(30):
            if "PRIMARY" in _mongosh("rs.status().myState === 1 ?"
                                     " 'PRIMARY' : 'no'").stdout:
                break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)


_MONGO_HOP = textwrap.dedent("""
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine

    def engine(where, port, until="caught_up"):
        ep = Endpoint(host="127.0.0.1", port=port, user="", password="",
                      options={"uri_options": "directConnection=true"})
        hop = Hop(name="mloop", engine="mongodb", source=ep, target=ep,
                  databases=["app"], db_map={"app": "app_copy"},
                  options={"follow_until": until})
        hop.report_dir = lambda db=None: where
        return MongoEngine(hop)
""")


def _mongo_engine(where, until="caught_up"):
    space = {}
    exec(_MONGO_HOP, space)
    return space["engine"](where, MG_PORT, until)


def _sub(script, where, failpoint=None):
    env = dict(os.environ)
    env.pop("MIGKIT_FAILPOINT", None)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    if failpoint:
        env["MIGKIT_FAILPOINT"] = failpoint
    return subprocess.run([sys.executable, "-c", script, str(where)],
                          env=env, capture_output=True, text=True,
                          timeout=300)


_MONGO_TAIL = _MONGO_HOP + textwrap.dedent(f"""
    import sys
    from pathlib import Path
    where = Path(sys.argv[1])
    eng = engine(where, {MG_PORT})
    eng.tail_apply("app", True, where / "tail-token.json", print)
""")


def _docs(db):
    from pymongo import MongoClient
    c = MongoClient(f"mongodb://127.0.0.1:{MG_PORT}/?directConnection=true")
    try:
        return {d["_id"]: d.get("n") for d in c[db]["t"].find()}
    finally:
        c.close()


def _write(script):
    got = _mongosh(script, "app")
    assert got.returncode == 0, got.stderr


def test_the_mongo_tail_ends_once_caught_up_and_goes_on_from_a_stop(
        mongo, tmp_path):
    _write("db.t.drop(); db.getSiblingDB('app_copy').t.drop()")
    where = tmp_path
    # the position, taken before anything is written
    eng = _mongo_engine(where)
    lines = []
    eng.tail_apply("app", True, where / tailctl.TOKEN, lines.append)
    env = tailctl.position(where / tailctl.TOKEN)
    assert env["v"] == tailctl.V and "_data" in env["token"]
    _write("for (let i = 0; i < 30; i++) db.t.insertOne({_id: i, n: 0});"
           " for (let k = 1; k <= 3; k++) db.t.updateMany({}, {$inc: {n: 1}})")
    # stopped dead once the target took the first batch, its position not
    # saved
    got = _sub(_MONGO_TAIL, where, "tail.applied:1:exit")
    assert got.returncode == 137, got.stdout + got.stderr
    assert _docs("app_copy"), "the first batch reached the target"
    got = _sub(_MONGO_TAIL, where)
    assert got.returncode == 0, got.stdout + got.stderr
    assert "caught up with the source" in got.stdout, got.stdout
    assert _docs("app_copy") == _docs("app") == {i: 3 for i in range(30)}
    after = tailctl.position(where / tailctl.TOKEN)
    assert after["gen"] > env["gen"] and after["id"] == env["id"]
    assert not tailctl.alive(where) and tailctl.stopped(where) is None


def test_the_mongo_tail_stops_between_batches_when_asked(mongo, tmp_path):
    import threading
    _write("db.t.drop(); db.getSiblingDB('app_copy').t.drop()")
    eng = _mongo_engine(tmp_path, until="stopped")
    lines, done = [], threading.Event()

    def run():
        try:
            eng.tail_apply("app", True, tmp_path / tailctl.TOKEN,
                           lines.append)
        finally:
            done.set()
    threading.Thread(target=run, daemon=True).start()
    deadline = time.time() + 60
    while not tailctl.alive(tmp_path) and time.time() < deadline:
        time.sleep(0.1)
    _write("db.t.insertOne({_id: 'a', n: 1})")
    while _docs("app_copy").get("a") != 1 and time.time() < deadline:
        time.sleep(0.1)
    # the beat says how it is while it runs
    assert (tmp_path / tailctl.BEAT).exists()
    tailctl.stop(tmp_path)
    assert done.wait(30), lines[-5:]
    assert any("stopped as asked" in s for s in lines), lines
    assert _docs("app_copy") == {"a": 1}


def test_mongos_sharding_check_says_what_it_could_not_read(mongo,
                                                           tmp_path):
    _write("db.t.drop(); db.t.insertOne({_id: 1});"
           " db.getSiblingDB('app_copy').t.drop();"
           " db.getSiblingDB('app_copy').t.insertOne({_id: 1})")
    eng = _mongo_engine(tmp_path)
    real = eng._client

    class Refuses:
        def __init__(self, c):
            self._c = c

        def __getattr__(self, name):
            return getattr(self._c, name)

        def __getitem__(self, name):
            return self._c[name]

        @property
        def admin(self):
            class Admin:
                def command(self, *a, **kw):
                    raise RuntimeError("not authorized on admin")
            return Admin()

    eng._client = lambda side: Refuses(real(side)) if side == "dst" \
        else real(side)
    got = {r.scope: r for r in eng.check_deep("app")}["app sharding"]
    # the source answered, the target did not: not "not sharded", ok
    assert got.status in ("warn", "skip"), (got.status, got.detail)
    assert "not read" in got.detail and "dst topology" in got.detail


@pytest.fixture(scope="module")
def brokers():
    for name, port in ((KSRC, KSRC_PORT), (KDST, KDST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", name, "-p",
             f"127.0.0.1:{port}:{port}", "redpandadata/redpanda:latest",
             "redpanda", "start", "--overprovisioned", "--smp", "1",
             "--memory", "384M", "--reserve-memory", "0M", "--node-id", "0",
             "--check=false", "--kafka-addr", f"PLAINTEXT://0.0.0.0:{port}",
             "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{port}"],
            check=True, capture_output=True)
    try:
        for port in (KSRC_PORT, KDST_PORT):
            end = time.time() + 90
            while time.time() < end:
                with socket.socket() as s:
                    s.settimeout(2)
                    if s.connect_ex(("127.0.0.1", port)) == 0:
                        break
                time.sleep(1)
        time.sleep(5)
        yield
    finally:
        for name in (KSRC, KDST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


_KAFKA_TAIL = textwrap.dedent(f"""
    import sys
    from pathlib import Path
    from migkit.config import Endpoint, Hop
    from migkit.engines.kafka import KafkaEngine
    where = Path(sys.argv[1])
    hop = Hop(name="kloop", engine="kafka",
              source=Endpoint(host="127.0.0.1", port={KSRC_PORT}, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port={KDST_PORT}, user="",
                              password=""),
              db_map={{"cluster": "cluster"}},
              options={{"follow_until": "caught_up"}})
    hop.report_dir = lambda db=None: where
    KafkaEngine(hop).tail_apply("cluster", True,
                                where / "tail-token.json", print)
""")


def _count(port, topic):
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=f"127.0.0.1:{port}")
    try:
        tps = [TopicPartition(topic, p)
               for p in sorted(c.partitions_for_topic(topic) or [])]
        ends, begins = c.end_offsets(tps), c.beginning_offsets(tps)
        return sum(ends[tp] - begins[tp] for tp in tps)
    finally:
        c.close()


def test_the_kafka_tail_stopped_dead_writes_no_message_twice(brokers,
                                                             tmp_path):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{KSRC_PORT}")
    for i in range(12000):
        producer.send("orders", value=f"order {i}".encode(), partition=0)
    producer.flush()
    producer.close()
    # stopped dead once the target took the first batch of the first
    # round, before its position was saved (the first save is where the
    # partition's count began, before anything was sent)
    got = _sub(_KAFKA_TAIL, tmp_path, "tail.applied:2:exit")
    assert got.returncode == 137, got.stdout + got.stderr
    assert 0 < _count(KDST_PORT, "orders") < 12000
    got = _sub(_KAFKA_TAIL, tmp_path)
    assert got.returncode == 0, got.stdout + got.stderr
    assert "caught up with the source" in got.stdout, got.stdout
    # every message once: what reached the target after the last saved
    # position is counted, not sent again
    assert _count(KDST_PORT, "orders") == 12000
    env = tailctl.position(tmp_path / tailctl.TOKEN)
    assert env["v"] == tailctl.V and env["gen"] >= 1
