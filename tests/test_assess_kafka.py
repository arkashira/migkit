"""`migkit assess` on Kafka - the same command again, a third kind of store.

Kafka has no server version over the client protocol, so the base reports
that as unknown rather than inventing one. What it does have is the finding
that cannot be fixed after the fact: partition counts.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-ak-src", "migkit-test-ak-dst"
SRC_PORT, DST_PORT = 19092, 19093


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _rpk(name, *args):
    r = subprocess.run(["docker", "exec", name, "rpk"] + list(args),
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", n, "-p", f"{p}:{p}",
             "redpandadata/redpanda:latest",
             "redpanda", "start", "--overprovisioned", "--smp", "1",
             "--memory", "512M", "--reserve-memory", "0M", "--node-id", "0",
             "--check=false",
             "--kafka-addr", f"PLAINTEXT://0.0.0.0:{p}",
             "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{p}"],
            check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        if not _wait(p):
            pytest.skip("redpanda did not come up in this sandbox")
    time.sleep(6)
    # same name, deliberately different partition counts
    _rpk(SRC, "topic", "create", "orders", "-p", "6")
    _rpk(SRC, "topic", "create", "only_src", "-p", "1")
    _rpk(DST, "topic", "create", "orders", "-p", "3")
    assert "orders" in _rpk(SRC, "topic", "list")
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="k", engine="kafka",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT),
              target=Endpoint(host="127.0.0.1", port=DST_PORT))
    hop.report_dir = lambda db=None: tmp_path
    return KafkaEngine(hop)


def _rows(tmp_path):
    items = _engine(tmp_path).assess()
    assert items, "assess returned nothing at all"
    return items


def test_assess_answers_rather_than_saying_not_implemented(pair, tmp_path):
    assert not any("not implemented" in i["item"] for i in _rows(tmp_path))


def test_a_store_with_no_version_says_unknown_not_nothing(pair, tmp_path):
    """Kafka does not hand a version to a client the way a database does, and
    inventing one would be worse than admitting it."""
    hit = [i for i in _rows(tmp_path) if "version" in i["item"]]
    assert hit, _rows(tmp_path)
    assert "unknown, not clean" in hit[0]["detail"], hit[0]["detail"]


def test_a_missing_topic_is_a_failure(pair, tmp_path):
    hit = [i for i in _rows(tmp_path)
           if "every source topic exists" in i["item"]]
    assert hit and hit[0]["level"] == "fail", hit
    assert "only_src" in hit[0]["detail"], hit[0]["detail"]


def test_a_partition_count_change_is_a_failure_with_its_reason(pair,
                                                               tmp_path):
    """6 partitions to 3 cannot be fixed after the data lands: the key to
    partition mapping moves with the count, so a consumer gets a different
    key and nothing errors."""
    hit = [i for i in _rows(tmp_path) if "partition counts" in i["item"]]
    assert hit and hit[0]["level"] == "fail", hit
    assert "orders 6->3" in hit[0]["detail"], hit[0]["detail"]
    assert "different partition" in hit[0]["detail"]


def test_an_unreachable_broker_is_unknown_not_clean(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="k", engine="kafka",
              source=Endpoint(host="127.0.0.1", port=1),
              target=Endpoint(host="127.0.0.1", port=1))
    hop.report_dir = lambda db=None: tmp_path
    items = KafkaEngine(hop).assess()
    assert items, "said nothing about brokers it could not reach"
    assert any("unknown, not clean" in i["detail"] for i in items), items
