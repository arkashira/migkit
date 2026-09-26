"""Parquet files, on a disk and under an S3 prefix, as one side of a pair
(backlog 34).

PostgreSQL moves into Parquet, and the check compares the two through the
renderer every engine without a server shares. A part file removed after
the move is a difference; a part cut off halfway is a deep finding. Then
the files move back into PostgreSQL, read in one pass, since files keep
no order to resume by.
"""
import datetime
import socket
import subprocess
import time
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

pytestmark = [pytest.mark.docker]

S3, S3_PORT = "migkit-test-parquet-s3", 15825


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def s3(pg_pair):
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", S3], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", S3, "-e",
                    "MINIO_ROOT_USER=minioadmin", "-e",
                    "MINIO_ROOT_PASSWORD=minioadmin", "-p",
                    f"{S3_PORT}:9000", "bitnamilegacy/minio:latest"],
                   check=True, capture_output=True)
    try:
        import boto3
        end = time.time() + 60
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", S3_PORT)) == 0:
                    break
            time.sleep(1)
        client = boto3.client("s3",
                              endpoint_url=f"http://127.0.0.1:{S3_PORT}",
                              aws_access_key_id="minioadmin",
                              aws_secret_access_key="minioadmin",
                              region_name="us-east-1")
        for _ in range(30):
            try:
                client.create_bucket(Bucket="lake")
                break
            except Exception:
                time.sleep(1)
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", S3], capture_output=True)


@pytest.fixture(scope="module")
def source(pg_pair):
    port = pg_pair["src"]
    psql(port, "drop database if exists lakesrc")
    assert psql(port, "create database lakesrc").returncode == 0
    made = psql(port, """
        create table orders (id bigint primary key, amount numeric(10,2),
          loose numeric, name text, paid boolean, at timestamp(6),
          day date, raw bytea, ratio double precision);
        insert into orders
          select g, (g * 1.25)::numeric(10,2), g / 7.0, 'order ' || g,
                 g % 2 = 0, timestamp '2024-02-29 12:00:00'
                 + g * interval '1 second', date '2024-01-01' + g,
                 decode(lpad(to_hex(g), 4, '0'), 'hex'), g / 3.0
            from generate_series(1, 2500) g;
        insert into orders values (0, null, null, null, null, null, null,
          null, null);
        create table notes (body text)""", db="lakesrc")
    assert made.returncode == 0, made.stderr
    psql(port, "insert into notes select 'note ' || g"
               " from generate_series(1, 30) g", db="lakesrc")
    return port


def _hop(src_port, target, tmp_path, name="lake"):
    hop = Hop(name=name, engine="hetero",
              options={"source_engine": "postgres",
                       "target_engine": "parquet"},
              source=Endpoint(host="127.0.0.1", port=src_port,
                              user="postgres", password="test"),
              target=target, databases=["lakesrc"])
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _move(hop):
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    eng = HeteroEngine(hop)
    ck = _Checkpoint(hop.report_dir() / "move.json")
    said = []
    eng.create_missing("lakesrc", said.append)
    for sch, t in eng.list_move_tables("lakesrc"):
        eng.move_table("lakesrc", sch, t, 700, ck, said.append)
    return eng, said


def _data(eng):
    return {r.scope: r for r in eng.check_data("lakesrc")
            if r.check == "data"}


@pytest.mark.parametrize("where", ["disk", "bucket"])
def test_a_move_into_parquet_is_checked_and_its_losses_found(
        where, source, s3, tmp_path):
    target = (Endpoint(options={"path": str(tmp_path / "lake")})
              if where == "disk" else
              # the local stand-in's own sample sign-in
              Endpoint(user="minioadmin", password="minioadmin",  # sample
                       options={"url": f"s3://lake/{tmp_path.name}",
                                "endpoint_url":
                                    f"http://127.0.0.1:{S3_PORT}"}))
    hop = _hop(source, target, tmp_path)
    eng, said = _move(hop)
    got = _data(eng)
    assert {r.status for r in got.values()} == {"ok"}, \
        [r.__dict__ for r in got.values()]
    # every column compared, the unconstrained numeric included
    assert "loose" in json_columns(eng, "orders"), said
    facts = eng.dst_engine.table_facts("dst", "lakesrc")
    assert facts["orders"]["rows"] == 2501 and facts["notes"]["rows"] == 30
    deep = [r for r in eng.dst_engine.check_deep("lakesrc")
            if r.scope == "lakesrc target files"]
    assert [r.status for r in deep] == ["ok"], [r.__dict__ for r in deep]
    fs, parts = eng.dst_engine._parts("dst", "lakesrc", "orders")
    assert len(parts) == 4, parts
    # one part gone: rows missing, and said to be
    fs.delete_file(parts[0])
    got = _data(eng)
    assert got["lakesrc.orders"].status == "diff", got["lakesrc.orders"]
    # a part cut off halfway reads as a file that is not whole
    with fs.open_input_file(parts[1]) as f:
        head = f.read(200)
    with fs.open_output_stream(parts[1]) as f:
        f.write(head)
    deep = [r for r in eng.dst_engine.check_deep("lakesrc")
            if r.scope == "lakesrc target files"]
    assert [r.status for r in deep] == ["diff"], [r.__dict__ for r in deep]


def json_columns(eng, table):
    return dict(eng.dst_engine.neutral_columns("dst", "lakesrc", table))


def test_values_arrive_as_arrow_types_readers_expect(source, tmp_path):
    import pyarrow.parquet as pq
    hop = _hop(source, Endpoint(options={"path": str(tmp_path / "lake")}),
               tmp_path)
    eng, _ = _move(hop)
    cols = json_columns(eng, "orders")
    assert cols["amount"] == "decimal128(10, 2)" and \
        cols["loose"] == "decimal_text" and cols["at"] == "timestamp[us]", cols
    _, parts = eng.dst_engine._parts("dst", "lakesrc", "orders")
    rows = {r["id"]: r for p in parts
            for r in pq.read_table(p).to_pylist()}
    assert rows[8]["amount"] == Decimal("10.00")
    assert rows[8]["at"] == datetime.datetime(2024, 2, 29, 12, 0, 8)
    assert rows[8]["raw"] == b"\x00\x08" and rows[8]["paid"] is True
    assert rows[0]["name"] is None


def test_parquet_moves_back_into_postgresql(source, pg_pair, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    lake = tmp_path / "lake"
    _move(_hop(source, Endpoint(options={"path": str(lake)}), tmp_path))
    back_port = pg_pair["dst"]
    psql(back_port, "drop database if exists lakeback")
    assert psql(back_port, "create database lakeback").returncode == 0
    hop = Hop(name="back", engine="hetero",
              options={"source_engine": "parquet",
                       "target_engine": "postgres"},
              source=Endpoint(options={"path": str(lake)}),
              target=Endpoint(host="127.0.0.1", port=back_port,
                              user="postgres", password="test"),
              databases=["lakesrc"], db_map={"lakesrc": "lakeback"})
    hop.report_dir = lambda db=None: tmp_path / "back"
    (tmp_path / "back").mkdir()
    from migkit.cli import _Checkpoint
    eng = HeteroEngine(hop)
    ck = _Checkpoint(tmp_path / "back" / "move.json")
    said = []
    eng.create_missing("lakesrc", said.append)
    for sch, t in eng.list_move_tables("lakesrc"):
        eng.move_table("lakesrc", sch, t, 700, ck, said.append)
    assert any("in one pass" in s for s in said if "orders" in s), said
    got = {r.scope: r.status for r in eng.check_data("lakesrc")
           if r.check == "data"}
    assert set(got.values()) == {"ok"}, got
    assert psql(back_port, "select count(*), sum(amount) from orders",
                db="lakeback").stdout.strip() == "2501|3907812.50"
