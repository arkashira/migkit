"""ScyllaDB to ScyllaDB: the target follows the source through each
table's change log - every stream of the generations in play read on its
own, behind a confidence window - and the fence, the confirm and the
verify of only what changed ride the same reading. Each change lands with
the source's write time, so the deep check holds the followed rows to the
source's times too.

Measured before: a ScyllaDB hop could be copied and checked, not
followed. There is no ScyllaDB change-log reader for Python; this one
reads the log tables the server documents (`<table>_scylla_cdc_log`,
`system_distributed.cdc_generation_timestamps`,
`system_distributed.cdc_streams_descriptions_v2`).
"""
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-eng-sc-src", 16017), ("migkit-test-eng-sc-dst",
                                               16018)
KEYSPACE = ("create keyspace if not exists {} with replication = {{'class':"
            " 'NetworkTopologyStrategy', 'replication_factor': 1}} and"
            " tablets = {{'enabled': false}}")


def _session(port, wait=300):
    from cassandra.cluster import Cluster
    end = time.time() + wait
    while True:
        try:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", port)) != 0:
                    raise OSError("not listening")
            got = Cluster(["127.0.0.1"], port=port,
                          connect_timeout=10).connect()
            got.execute("select now() from system.local")
            return got
        except Exception:
            if time.time() > end:
                raise
            time.sleep(3)


@pytest.fixture(scope="module")
def scylla():
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:9042", "scylladb/scylla:6.2", "--smp", "1",
                        "--memory", "750M", "--overprovisioned", "1",
                        "--developer-mode", "1"], check=True,
                       capture_output=True)
    try:
        sessions = [_session(port) for _, port in (SRC, DST)]
        for s in sessions:
            s.execute(KEYSPACE.format("shop"))
        yield sessions
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path, db="shop"):
    from migkit.engines.cassandra import CassandraEngine

    def ep(port):
        return Endpoint(host="127.0.0.1", port=port)
    hop = Hop(name="scf", engine="scylladb", source=ep(SRC[1]),
              target=ep(DST[1]), databases=[db],
              options={"cdc_confidence_seconds": 2})
    hop.report_dir = lambda d=None: tmp_path
    return CassandraEngine(hop)


def _tail(eng, db, tmp_path):
    ended = {}

    def run():
        try:
            eng.tail_apply(db, True, tmp_path / "tail-token.json",
                           lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, ended


def _stop(thread):
    import ctypes
    if thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    thread.join(timeout=30)


def _rows(session, table="shop.orders"):
    return sorted(tuple(r) for r in session.execute(
        f"select id, line, v, w, tags, writetime(v), writetime(w) from"
        f" {table}"))


def test_a_copy_and_its_follow_end_equal(scylla, tmp_path):
    src, dst = scylla
    src.execute("create table if not exists shop.orders (id int, line int,"
                " v text, w text, tags set<text>, primary key (id, line))"
                " with cdc = {'enabled': true}")
    for i in range(40):
        src.execute("insert into shop.orders (id, line, v, w, tags) values"
                    " (%s, 0, %s, 'w', {'a'}) using timestamp %s",
                    # a write time of its own for each row; not one from
                    # before the log's generation began, which a table with
                    # its log on refuses
                    (i, f"v{i}", int(time.time() * 1e6) + i))
    eng = _engine(tmp_path)
    eng.tail_start("shop", tmp_path / "tail-token.json")
    eng.native_bulk("shop", ["orders"], True, [].append)
    thread, ended = _tail(eng, "shop", tmp_path)
    try:
        src.execute("insert into shop.orders (id, line, v) values (1000, 0,"
                    " 'new') using ttl 3600")
        src.execute("update shop.orders set w = null where id = 1 and"
                    " line = 0")
        src.execute("delete from shop.orders where id = 2 and line = 0")
        for line in range(1, 6):
            src.execute("insert into shop.orders (id, line, v) values"
                        " (3, %s, 'p')", (line,))
            src.execute("insert into shop.orders (id, line, v) values"
                        " (4, %s, 'r')", (line,))
        src.execute("delete from shop.orders where id = 3")
        src.execute("delete from shop.orders where id = 4 and line >= 2 and"
                    " line < 4")
        src.execute("update shop.orders set tags = tags + {'z'} where id = 5"
                    " and line = 0")
        src.execute("update shop.orders set tags = {'only'} where id = 6 and"
                    " line = 0")
        at = eng.src_lsn("shop")
        assert at is not None
        assert eng.fence_wait("shop", at, timeout=120) is True
        # written while the follow is behind: the check waits on it and
        # compares again
        src.execute("update shop.orders set v = 'late' where id = 7 and"
                    " line = 0")
        got = [r for r in eng.check_data("shop", table="orders")
               if r.check == "data"]
        assert got and all(r.status == "ok" for r in got), \
            [r.detail for r in got]
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    assert _rows(src) == _rows(dst)
    theirs = {(r[0], r[1]): r for r in _rows(dst)}
    assert (2, 0) not in theirs and not [k for k in theirs if k[0] == 3]
    # line 0 from the first rows; 2 and 3 went with the range
    assert sorted(k[1] for k in theirs if k[0] == 4) == [0, 1, 4, 5]
    assert theirs[(5, 0)][4] == {"a", "z"} and theirs[(6, 0)][4] == {"only"}
    assert theirs[(1, 0)][3] is None
    ttl = dst.execute("select ttl(v) from shop.orders where id = 1000 and"
                      " line = 0").one()[0]
    assert ttl and 3500 < ttl <= 3600, ttl
    deep = [r for r in eng.check_deep("shop")
            if r.scope == "shop.orders cell times"]
    assert deep and deep[0].status == "ok", [r.detail for r in deep]
    # the target's table was made without the source's log - a log on
    # refuses the copy's back-dated writes - and the settings say so
    made = {r.table_name for r in dst.execute(
        "select table_name from system_schema.tables where keyspace_name ="
        " 'shop'")}
    assert made == {"orders"}, made
    said = eng.check_params("shop")[0]
    assert said.status == "diff" and "orders.change_log src=on dst=off" \
        in said.detail, said.detail


def test_only_what_the_log_names_is_verified(scylla, tmp_path):
    src, dst = scylla
    for s in (src, dst):
        s.execute(KEYSPACE.format("dv"))
    src.execute("create table if not exists dv.t (id int primary key, v"
                " text) with cdc = {'enabled': true}")
    dst.execute("create table if not exists dv.t (id int primary key, v"
                " text)")
    for s in (src, dst):
        for i in range(10):
            s.execute("insert into dv.t (id, v) values (%s, 'x')", (i,))
    eng = _engine(tmp_path, "dv")
    # past the confidence window, so the baseline is after them
    time.sleep(3)
    first = eng.delta_verify("dv")
    assert first[0].status == "ok" and "baseline" in first[0].detail
    src.execute("update dv.t set v = 'moved on' where id = 4")
    time.sleep(3)
    got = eng.delta_verify("dv")
    assert "diff" in [r.status for r in got], [r.detail for r in got]
    dst.execute("update dv.t set v = 'moved on' where id = 4")
    got = eng.delta_verify("dv")
    assert all(r.status == "ok" for r in got), [r.detail for r in got]
    assert "1 changed rows verified equal" in " ".join(
        r.detail for r in got), [r.detail for r in got]


def test_a_table_with_its_log_off_is_refused_not_switched_on(scylla,
                                                             tmp_path):
    src, _ = scylla
    src.execute(KEYSPACE.format("quiet"))
    src.execute("create table if not exists quiet.t (id int primary key,"
                " v text)")
    eng = _engine(tmp_path, "quiet")
    with pytest.raises(SystemExit, match="ALTER TABLE quiet.t WITH cdc"):
        eng.tail_start("quiet", tmp_path / "tail-token.json")
    assert not (tmp_path / "tail-token.json").exists()
    left = {r.table_name for r in src.execute(
        "select table_name from system_schema.tables where keyspace_name ="
        " 'quiet'")}
    assert left == {"t"}, left


def test_a_counter_table_is_refused_by_name(scylla, tmp_path):
    src, _ = scylla
    src.execute(KEYSPACE.format("counted"))
    src.execute("create table if not exists counted.hits (id int primary"
                " key, n counter)")
    eng = _engine(tmp_path, "counted")
    with pytest.raises(SystemExit, match="counted.hits holds counters"):
        eng.tail_start("counted", tmp_path / "tail-token.json")
    assert not (tmp_path / "tail-token.json").exists()
