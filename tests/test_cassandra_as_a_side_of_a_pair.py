"""Cassandra as one side of a pair (backlog 34).

PostgreSQL moves into Cassandra. The keyspace is made only with the
replication the target endpoint names, since how many copies a keyspace
keeps is the operator's decision. A batch written again replaces itself.
A null is left unset, not written as a tombstone. A value changed on the
target is a difference.

A `timestamp` holds milliseconds. A source value carrying microseconds
arrives without them, and the check reports that as a difference: the
target really does hold less than the source. Then the rows move back.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

pytestmark = [pytest.mark.docker]

CS, CS_PORT = "migkit-test-cassandra", 15838


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def cassandra():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", CS], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", CS, "-p",
                    f"{CS_PORT}:9042", "-e", "MAX_HEAP_SIZE=384M", "-e",
                    "HEAP_NEWSIZE=64M", "cassandra:4.1"], check=True,
                   capture_output=True)
    try:
        from cassandra.cluster import Cluster
        end = time.time() + 240
        session = None
        while time.time() < end:
            try:
                with socket.socket() as s:
                    s.settimeout(1)
                    if s.connect_ex(("127.0.0.1", CS_PORT)) != 0:
                        raise OSError
                session = Cluster(["127.0.0.1"], port=CS_PORT).connect()
                break
            except Exception:
                time.sleep(3)
        assert session is not None, "cassandra never answered"
        yield session
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", CS], capture_output=True)


@pytest.fixture(scope="module")
def source(pg_pair):
    port = pg_pair["src"]
    psql(port, "drop database if exists cssrc")
    assert psql(port, "create database cssrc").returncode == 0
    made = psql(port, """
        create table orders (region text, id bigint, total numeric(12,2),
          paid boolean, at timestamp(3), day date, note text, blob bytea,
          primary key (region, id));
        insert into orders
          select 'r' || (g % 4), g, g * 2.25, g % 2 = 0,
                 timestamp '2024-02-29 10:00:00' + g * interval '1.5 ms',
                 date '2024-01-01' + g, 'note ' || g,
                 decode(lpad(to_hex(g), 4, '0'), 'hex')
            from generate_series(1, 900) g;
        insert into orders values ('r0', 0, null, null, null, null, null,
          null)""", db="cssrc")
    assert made.returncode == 0, made.stderr
    return port


def _pair(src, dst, src_ep, dst_ep, tmp_path, **extra):
    from migkit.engines.hetero import HeteroEngine
    extra.setdefault("options", {"source_engine": src, "target_engine": dst})
    hop = Hop(name="cs", engine="hetero",
              source=src_ep, target=dst_ep, databases=["cssrc"], **extra)
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _cs(**options):
    return Endpoint(host="127.0.0.1", port=CS_PORT, options=options)


def _move(eng, tmp_path, name="move.json"):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / name)
    said = []
    made = eng.dst_engine.prepare_target("cssrc")
    if made:
        said.append(made)
    eng.create_missing("cssrc", said.append)
    for sch, t in eng.list_move_tables("cssrc"):
        eng.move_table("cssrc", sch, t, 300, ck, said.append)
    return ck, said


def _data(eng):
    return {r.scope: r for r in eng.check_data("cssrc")
            if r.check == "data"}


def test_the_keyspace_is_not_made_without_its_replication(cassandra,
                                                          source, tmp_path):
    pg = Endpoint(host="127.0.0.1", port=source, user="postgres",
                  password="test")
    eng = _pair("postgres", "cassandra", pg, _cs(), tmp_path)
    with pytest.raises(SystemExit) as e:
        eng.dst_engine.prepare_target("cssrc")
    assert "not migkit's to choose" in str(e.value)


def test_postgresql_into_cassandra(cassandra, source, tmp_path):
    pg = Endpoint(host="127.0.0.1", port=source, user="postgres",
                  password="test")
    eng = _pair("postgres", "cassandra", pg, _cs(
        replication="{'class': 'SimpleStrategy', 'replication_factor': 1}"),
        tmp_path)
    ck, said = _move(eng, tmp_path)
    got = _data(eng)
    assert [r.status for r in got.values()] == ["ok"], \
        [r.__dict__ for r in got.values()]
    # the whole key is the partition key
    keys = {r.column_name: r.kind for r in cassandra.execute(
        "select column_name, kind from system_schema.columns where"
        " keyspace_name = 'cssrc' and table_name = 'orders'")}
    assert keys["region"] == keys["id"] == "partition_key", keys
    # nothing written for the nulls
    row = cassandra.execute("select writetime(note) from cssrc.orders"
                            " where region = 'r0' and id = 0").one()
    assert row[0] is None
    ck["cssrc.orders"] = {}
    ck.save()
    eng.move_table("cssrc", "public", "orders", 300, ck, said.append)
    assert cassandra.execute("select count(*) from cssrc.orders").one()[0] \
        == 901
    deep = {r.scope: r for r in eng.dst_engine.check_deep("cssrc")}
    assert deep["cssrc target replication"].status == "warn", deep
    cassandra.execute("update cssrc.orders set note = 'changed' where"
                      " region = 'r1' and id = 5")
    assert _data(eng)["cssrc.orders"].status == "diff"
    cassandra.execute("update cssrc.orders set note = 'note 5' where"
                      " region = 'r1' and id = 5")
    assert _data(eng)["cssrc.orders"].status == "ok"


def test_microseconds_the_target_cannot_hold_are_a_difference(
        cassandra, source, pg_pair, tmp_path):
    port = source
    psql(port, "create table if not exists fine (id int primary key,"
               " at timestamp(6)); insert into fine values"
               " (1, '2024-02-29 10:00:00.123456') on conflict do nothing",
         db="cssrc")
    pg = Endpoint(host="127.0.0.1", port=port, user="postgres",
                  password="test")
    replication = "{'class': 'SimpleStrategy', 'replication_factor': 1}"
    eng = _pair("postgres", "cassandra", pg, _cs(replication=replication),
                tmp_path)
    # read back as it is written: the move stops at the batch, naming the
    # column the target cannot hold as it was given
    with pytest.raises(SystemExit) as e:
        _move(eng, tmp_path, "fine.json")
    assert str(e.value).startswith("cssrc.fine: written twice, 0 rows of a"
                                   " batch are not on the target and 1 read"
                                   " back different from the source - the"
                                   " first by key 1, in at."), e.value
    # and with the hop's read-back turned off, `check` finds it after
    eng = _pair("postgres", "cassandra", pg, _cs(replication=replication),
                tmp_path, options={"source_engine": "postgres",
                                   "target_engine": "cassandra",
                                   "verify_batches": False})
    _move(eng, tmp_path, "fine-unchecked.json")
    got = _data(eng)
    assert got["cssrc.fine"].status == "diff", got["cssrc.fine"].__dict__
    psql(port, "drop table fine", db="cssrc")
    cassandra.execute("drop table cssrc.fine")


def test_cassandra_moves_back_into_postgresql(cassandra, source, pg_pair,
                                              tmp_path):
    back = pg_pair["dst"]
    psql(back, "drop database if exists csback")
    assert psql(back, "create database csback").returncode == 0
    eng = _pair("cassandra", "postgres", _cs(),
                Endpoint(host="127.0.0.1", port=back, user="postgres",
                         password="test"), tmp_path,
                db_map={"cssrc": "csback"})
    _move(eng, tmp_path, "back.json")
    assert [r.status for r in _data(eng).values()] == ["ok"]
    assert psql(back, "select count(*), sum(total), max(at) from orders",
                db="csback").stdout.strip() == \
        "901|912262.50|2024-02-29 10:00:01.35"
