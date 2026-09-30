"""Cassandra: each cell's write time and time to live held to the
source's, counters by their value, and a Cassandra source's follow refused
with the reason.

Measured before: the data check compares values, and a row copied without
its time to live - the table copier writes every row as new - passed it,
though the row expires on the source and lives on the target for good;
a copy's later write time outranks a change the source makes after it,
and nothing said so either.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-eng-cs-src", 16015), ("migkit-test-eng-cs-dst",
                                               16016)


def _session(port, wait=300):
    from cassandra.cluster import Cluster
    end = time.time() + wait
    while True:
        try:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", port)) != 0:
                    raise OSError("not listening")
            return Cluster(["127.0.0.1"], port=port,
                           connect_timeout=10).connect()
        except Exception:
            if time.time() > end:
                raise
            time.sleep(3)


@pytest.fixture(scope="module")
def clusters():
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:9042", "-e", "MAX_HEAP_SIZE=256M", "-e",
                        "HEAP_NEWSIZE=64M", "cassandra:4.1"], check=True,
                       capture_output=True)
    try:
        sessions = [_session(port) for _, port in (SRC, DST)]
        for s in sessions:
            s.execute("create keyspace if not exists timed with replication"
                      " = {'class': 'SimpleStrategy', 'replication_factor':"
                      " 1}")
        yield sessions
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path):
    from migkit.engines.cassandra import CassandraEngine

    def ep(port):
        return Endpoint(host="127.0.0.1", port=port)
    hop = Hop(name="cstimes", engine="cassandra", source=ep(SRC[1]),
              target=ep(DST[1]), databases=["timed"])
    hop.report_dir = lambda db=None: tmp_path
    return CassandraEngine(hop)


def _verdict(results, scope):
    got = [r for r in results if r.scope == scope]
    assert len(got) == 1, [r.scope for r in results]
    return got[0]


def test_times_copied_with_the_rows_pass(clusters, tmp_path):
    src, dst = clusters
    src.execute("create table timed.kept (id int primary key, v text,"
                " tags set<text>)")
    for i in range(300):
        src.execute("insert into timed.kept (id, v, tags) values (%s, %s,"
                    " {'a'}) using ttl 7200 and timestamp %s",
                    (i, f"v{i}", 1_000_000 + i))
    eng = _engine(tmp_path)
    eng.native_bulk("timed", ["kept"], True, [].append)
    got = _verdict(eng.check_deep("timed"), "timed.kept cell times")
    assert got.status == "ok", got.detail
    assert got.detail == (
        "300 rows (every row) read on both sides: 300 cells' write times"
        " within 1 s and times to live within 60 s of the source's; tags"
        " hold(s) a collection or user type with no single write time,"
        " compared by value only"), got.detail


def test_rows_copied_as_new_are_named(clusters, tmp_path):
    """The table copier wrote each row as new: the values match and the
    data check passes, the times do not."""
    from migkit.cli import _Checkpoint
    src, dst = clusters
    src.execute("create table timed.lost (id int primary key, v text)")
    for i in range(50):
        src.execute("insert into timed.lost (id, v) values (%s, %s) using"
                    " ttl 3600 and timestamp %s", (i, f"v{i}", 5_000 + i))
    eng = _engine(tmp_path)
    eng.move_table("timed", "", "lost", 1000,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    data = [r for r in eng.check_data("timed", table="lost")
            if r.check == "data"]
    assert data and all(r.status == "ok" for r in data), \
        [r.detail for r in data]
    got = _verdict(eng.check_deep("timed"), "timed.lost cell times")
    assert got.status == "diff", got.detail
    assert "write time off by more than 1 s on 50 cells" in got.detail
    assert "time to live off by more than 60 s, or on one side only, on 50" \
        " cells" in got.detail, got.detail


def test_a_counter_is_compared_by_value(clusters, tmp_path):
    src, dst = clusters
    for s in (src, dst):
        s.execute("create table if not exists timed.hits (id int primary"
                  " key, n counter)")
    for i in range(5):
        src.execute("update timed.hits set n = n + %s where id = %s",
                    (i + 1, i))
        dst.execute("update timed.hits set n = n + %s where id = %s",
                    (i + 1, i))
    eng = _engine(tmp_path)
    got = _verdict(eng.check_deep("timed"), "timed.hits cell times")
    assert got.status == "ok", got.detail
    assert "1 counter column(s) compared by value - a counter is only ever" \
        " added to, and no copy writes one" in got.detail, got.detail
    dst.execute("update timed.hits set n = n + 1 where id = 3")
    got = _verdict(eng.check_deep("timed"), "timed.hits cell times")
    assert got.status == "diff" and "counter value differs on 1 (3.n)" \
        in got.detail, got.detail
    with pytest.raises(SystemExit, match="holds counters, which are added"):
        eng.native_bulk("timed", ["hits"], True, [].append)


def test_a_cassandra_source_is_not_followed(clusters, tmp_path):
    eng = _engine(tmp_path)
    with pytest.raises(SystemExit, match="commit log's change files"):
        eng.tail_start("timed", tmp_path / "tail-token.json")
    assert not (tmp_path / "tail-token.json").exists()


def test_two_clusters_of_one_name_are_told_apart(clusters, tmp_path):
    """Both clusters start with the image's name, `Test Cluster`: by the
    name alone the interlock would take the target for the source and
    refuse every write. Their nodes' ids tell them apart; one cluster
    asked as both sides is still the source itself."""
    import dataclasses

    from migkit import interlock
    eng = _engine(tmp_path)
    a, b = (interlock.identity(eng.hop, eng, s, "timed")
            for s in ("src", "dst"))
    assert a["name"] == b["name"] == "Test Cluster", (a, b)
    assert a["id"] != b["id"] and interlock.itself(a, b) is None, (a, b)
    one = _engine(tmp_path)
    one.hop = dataclasses.replace(one.hop, target=one.hop.source)
    one.hop.report_dir = lambda db=None: tmp_path
    c, d = (interlock.identity(one.hop, one, s, "timed")
            for s in ("src", "dst"))
    assert interlock.itself(c, d), (c, d)
