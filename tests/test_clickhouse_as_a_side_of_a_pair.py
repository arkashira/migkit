"""ClickHouse as one side of a pair (backlog 33).

PostgreSQL moves into ClickHouse, and the check compares the two through
the in-process renderer. The copy is then made to write the same batches
again, as a restart after a crash between a write and its checkpoint
would: a MergeTree keeps a second row with the same key, so without the
delete that goes first every row of those batches would be there twice.
A value changed on the target is a difference. Then the table moves back.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

pytestmark = [pytest.mark.docker]

CH, CH_PORT = "migkit-test-clickhouse", 15826


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def clickhouse():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", CH], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", CH, "-e",
                    "CLICKHOUSE_PASSWORD=test", "-p", f"{CH_PORT}:8123",
                    "--ulimit", "nofile=262144:262144",
                    "clickhouse/clickhouse-server:24.8"], check=True,
                   capture_output=True)
    try:
        import clickhouse_connect
        end = time.time() + 90
        client = None
        while time.time() < end:
            try:
                client = clickhouse_connect.get_client(
                    host="127.0.0.1", port=CH_PORT, username="default",
                    password="test")
                client.command("select 1")
                break
            except Exception:
                time.sleep(1)
        assert client is not None
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", CH], capture_output=True)


@pytest.fixture(scope="module")
def source(pg_pair):
    port = pg_pair["src"]
    psql(port, "drop database if exists chsrc")
    assert psql(port, "create database chsrc").returncode == 0
    made = psql(port, """
        create table events (id bigint primary key, amount numeric(12,3),
          kind text, ok boolean, at timestamp(6), day date,
          payload bytea, score double precision);
        insert into events
          select g, g * 1.125, 'kind ' || (g % 5), g % 3 = 0,
                 timestamp '2024-02-29 00:00:00' + g * interval '1 minute',
                 date '1999-12-31' + g, decode(lpad(to_hex(g), 6, '0'),
                 'hex'), g / 7.0
            from generate_series(1, 3000) g;
        insert into events values (0, null, null, null, null, null, null,
          null)""", db="chsrc")
    assert made.returncode == 0, made.stderr
    return port


def _pair(src, dst, tmp_path, src_ep, dst_ep, db_map=None):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="ch", engine="hetero",
              options={"source_engine": src, "target_engine": dst},
              source=src_ep, target=dst_ep, databases=["chsrc"],
              db_map=db_map or {})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _pg(port):
    return Endpoint(host="127.0.0.1", port=port, user="postgres",
                    password="test")


def _ch():
    return Endpoint(host="127.0.0.1", port=CH_PORT, user="default",
                    password="test")


def _move(eng, tmp_path, name="move.json"):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / name)
    said = []
    made = eng.dst_engine.prepare_target("chsrc")
    if made:
        said.append(made)
    eng.create_missing("chsrc", said.append)
    for sch, t in eng.list_move_tables("chsrc"):
        eng.move_table("chsrc", sch, t, 700, ck, said.append)
    return ck, said


def _data(eng):
    return {r.scope: r for r in eng.check_data("chsrc")
            if r.check == "data"}


def test_postgresql_into_clickhouse_and_a_replay_does_not_double(
        clickhouse, source, tmp_path):
    eng = _pair("postgres", "clickhouse", tmp_path, _pg(source), _ch())
    ck, said = _move(eng, tmp_path)
    got = _data(eng)
    assert [r.status for r in got.values()] == ["ok"], \
        [r.__dict__ for r in got.values()]
    assert clickhouse.command("select count() from chsrc.events") == 3001
    # the copy writes its last batches again, from a checkpoint saved
    # before them, as a restart after a crash would
    ck["chsrc.events"] = {"last": [1400], "moved": 1401}
    ck.save()
    eng.move_table("chsrc", "public", "events", 700, ck, said.append)
    assert clickhouse.query("select count(), uniqExact(id) from"
                            " chsrc.events").result_rows == [(3001, 3001)]
    assert any("1,600 of" in s or "3,001" in s for s in said), said
    assert [r.status for r in _data(eng).values()] == ["ok"]
    deep = {r.scope: r.status for r in eng.dst_engine.check_deep("chsrc")}
    assert deep["chsrc target mutations"] == "ok", deep
    clickhouse.command("alter table chsrc.events update kind = 'changed'"
                       " where id = 77 settings mutations_sync = 2")
    got = _data(eng)["chsrc.events"]
    assert got.status == "diff", got.__dict__
    clickhouse.command("alter table chsrc.events update kind = 'kind 2'"
                       " where id = 77 settings mutations_sync = 2")
    assert [r.status for r in _data(eng).values()] == ["ok"]


def test_types_are_the_ones_clickhouse_readers_expect(clickhouse, source,
                                                      tmp_path):
    got = dict(clickhouse.query(
        "select name, type from system.columns where database = 'chsrc'"
        " and table = 'events'").result_rows)
    assert got["id"] == "Int64" and got["amount"] == \
        "Nullable(Decimal(12, 3))" and got["at"] == \
        "Nullable(DateTime64(6))", got
    assert clickhouse.command("select sorting_key from system.tables where"
                              " database = 'chsrc' and name = 'events'") \
        == "id"


def test_a_clickhouse_table_moves_into_postgresql(clickhouse, pg_pair,
                                                  tmp_path):
    """A table ClickHouse made itself: unsigned keys, a low-cardinality
    string, a nullable decimal, millisecond times."""
    clickhouse.command("create database if not exists chnative")
    clickhouse.command("drop table if exists chnative.metrics")
    clickhouse.command(
        "create table chnative.metrics (id UInt32, name"
        " LowCardinality(String), v Nullable(Decimal(10, 2)), at"
        " DateTime64(3), day Date) engine = MergeTree order by id")
    clickhouse.command(
        "insert into chnative.metrics select number, concat('m',"
        " toString(number % 7)), if(number % 10 = 0, null, number / 4),"
        " toDateTime64('2024-02-29 00:00:00', 3) + number / 1000,"
        " toDate('2024-01-01') + number from numbers(1, 500)")
    back = pg_pair["dst"]
    psql(back, "drop database if exists chback")
    assert psql(back, "create database chback").returncode == 0
    eng = _pair("clickhouse", "postgres", tmp_path, _ch(), _pg(back),
                {"chnative": "chback"})
    eng.hop.databases = ["chnative"]
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "back.json")
    said = []
    eng.create_missing("chnative", said.append)
    for sch, t in eng.list_move_tables("chnative"):
        eng.move_table("chnative", sch, t, 200, ck, said.append)
    got = [r.status for r in eng.check_data("chnative") if r.check == "data"]
    assert got == ["ok"], said
    assert psql(back, "select count(*), sum(v), count(v) from metrics",
                db="chback").stdout.strip() == "500|28125.00|450"
