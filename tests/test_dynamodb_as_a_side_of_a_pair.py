"""DynamoDB as one side of a pair (backlog 34), against DynamoDB Local.

DynamoDB keeps a number without the scale it was written with and has no
time type. Measured through the service: `1.50` comes back `1.5`. So the
table migkit creates carries each column's class in its tags, and values
are read back as that. Without it every decimal with a trailing zero, and
every time, would read as a difference.

A composite key goes to the partition and sort keys. A table keyed by
three columns has nowhere to go, and is refused by name. Then the items
move back into PostgreSQL.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql

pytestmark = [pytest.mark.docker]

DDB, DDB_PORT = "migkit-test-dynamodb", 15834


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def dynamo():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", DDB], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", DDB, "-p",
                    f"{DDB_PORT}:8000", "amazon/dynamodb-local:latest",
                    "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"],
                   check=True, capture_output=True)
    try:
        import boto3
        end = time.time() + 60
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", DDB_PORT)) == 0:
                    break
            time.sleep(1)
        client = boto3.client("dynamodb",
                              endpoint_url=f"http://127.0.0.1:{DDB_PORT}",
                              region_name="us-east-1",
                              aws_access_key_id="local",
                              aws_secret_access_key="local")
        for _ in range(30):
            try:
                client.list_tables()
                break
            except Exception:
                time.sleep(1)
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", DDB], capture_output=True)


@pytest.fixture(scope="module")
def this_machine(tmp_path_factory):
    """One report root for the module: the machine both hops run on."""
    return tmp_path_factory.mktemp("reports")


@pytest.fixture(autouse=True)
def _reports(this_machine, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", this_machine)


@pytest.fixture(scope="module")
def source(pg_pair):
    port = pg_pair["src"]
    psql(port, "drop database if exists ddbsrc")
    assert psql(port, "create database ddbsrc").returncode == 0
    made = psql(port, """
        create table accounts (id bigint primary key, balance numeric(12,2),
          name text, active boolean, opened timestamp(6), born date,
          photo bytea, rate double precision);
        insert into accounts
          select g, g * 1.5, 'name ' || g, g % 2 = 0,
                 timestamp '2024-02-29 00:00:00' + g * interval '1 hour',
                 date '1990-01-01' + g, decode('00ff' || lpad(to_hex(g), 4,
                 '0'), 'hex'), g / 3.0
            from generate_series(1, 700) g;
        insert into accounts values (0, 0.10, '', null, null, null, null,
          null);
        create table lines (region text, seq int, qty int,
          primary key (region, seq));
        insert into lines select 'r' || (g % 3), g, g * 2
          from generate_series(1, 90) g;
        create table wide (a int, b int, c int, v text,
          primary key (a, b, c));
        insert into wide values (1, 2, 3, 'x')""", db="ddbsrc")
    assert made.returncode == 0, made.stderr
    return port


def _ddb():
    return Endpoint(user="local", password="local",
                    options={"endpoint_url": f"http://127.0.0.1:{DDB_PORT}"})


def _pair(src, dst, src_ep, dst_ep, tmp_path, **extra):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="ddb", engine="hetero",
              options={"source_engine": src, "target_engine": dst},
              source=src_ep, target=dst_ep, databases=["ddbsrc"], **extra)
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _move(eng, tmp_path, name="move.json"):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / name)
    said = []
    eng.create_missing("ddbsrc", said.append)
    for sch, t in eng.list_move_tables("ddbsrc"):
        eng.move_table("ddbsrc", sch, t, 250, ck, said.append)
    return ck, said


def _data(eng):
    return {r.scope: r for r in eng.check_data("ddbsrc")
            if r.check == "data"}


def test_postgresql_into_dynamodb(dynamo, source, tmp_path):
    pg = Endpoint(host="127.0.0.1", port=source, user="postgres",
                  password="test")
    eng = _pair("postgres", "dynamodb", pg, _ddb(), tmp_path,
                exclude=["wide"])
    ck, said = _move(eng, tmp_path)
    got = _data(eng)
    assert {r.status for r in got.values()} == {"ok"}, \
        [r.__dict__ for r in got.values()]
    assert set(got) == {"ddbsrc.accounts", "ddbsrc.lines"}, got
    # what the service kept, before migkit reads it back at its scale
    item = dynamo.get_item(TableName="ddbsrc.accounts",
                           Key={"id": {"N": "1"}})["Item"]
    assert item["balance"] == {"N": "1.5"}, item
    assert item["opened"] == {"S": "2024-02-29 01:00:00.000000"}, item
    keys = dynamo.describe_table(TableName="ddbsrc.lines")["Table"][
        "KeySchema"]
    assert [(k["AttributeName"], k["KeyType"]) for k in keys] == [
        ("region", "HASH"), ("seq", "RANGE")]
    # a batch written again lands on itself
    ck["ddbsrc.accounts"] = {}
    ck.save()
    eng.move_table("ddbsrc", "public", "accounts", 250, ck, said.append)
    assert dynamo.scan(TableName="ddbsrc.accounts", Select="COUNT")[
        "Count"] == 701
    # one value changed on the target is a difference
    dynamo.update_item(TableName="ddbsrc.accounts", Key={"id": {"N": "5"}},
                       UpdateExpression="set #n = :v",
                       ExpressionAttributeNames={"#n": "name"},
                       ExpressionAttributeValues={":v": {"S": "changed"}})
    assert _data(eng)["ddbsrc.accounts"].status == "diff"
    dynamo.update_item(TableName="ddbsrc.accounts", Key={"id": {"N": "5"}},
                       UpdateExpression="set #n = :v",
                       ExpressionAttributeNames={"#n": "name"},
                       ExpressionAttributeValues={":v": {"S": "name 5"}})
    assert _data(eng)["ddbsrc.accounts"].status == "ok"


def test_three_key_columns_have_no_table_to_go_to(dynamo, source, tmp_path):
    pg = Endpoint(host="127.0.0.1", port=source, user="postgres",
                  password="test")
    eng = _pair("postgres", "dynamodb", pg, _ddb(), tmp_path)
    with pytest.raises(SystemExit) as e:
        eng.create_missing("ddbsrc", lambda m: None)
    assert "keyed by 3 columns" in str(e.value), str(e.value)


def test_dynamodb_moves_back_into_postgresql(dynamo, source, pg_pair,
                                             tmp_path):
    back = pg_pair["dst"]
    psql(back, "drop database if exists ddbback")
    assert psql(back, "create database ddbback").returncode == 0
    eng = _pair("dynamodb", "postgres", _ddb(),
                Endpoint(host="127.0.0.1", port=back, user="postgres",
                         password="test"), tmp_path,
                db_map={"ddbsrc": "ddbback"})
    _, said = _move(eng, tmp_path, "back.json")
    assert {r.status for r in _data(eng).values()} == {"ok"}, said
    # read back as what they were: a decimal at its scale, a time a time
    assert psql(back, "select count(*), sum(balance), min(opened),"
                      " max(born) from accounts",
                db="ddbback").stdout.strip() == \
        "701|368025.10|2024-02-29 01:00:00|1991-12-02"
    assert psql(back, "select format_type(atttypid, atttypmod) from"
                      " pg_attribute where attrelid = 'accounts'::regclass"
                      " and attname = 'opened'",
                db="ddbback").stdout.strip().startswith("timestamp")
    got = psql(back, "select encode(photo, 'hex'), active from accounts"
                     " where id = 10", db="ddbback").stdout.strip()
    assert got == "00ff000a|t", got
