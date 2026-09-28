"""Numbers DynamoDB refuses are counted before the move (type-fidelity
G27).

DynamoDB's number holds 38 significant digits between 1E-130 and
9.99E+125 and no NaN or infinity. Measured before, against DynamoDB Local:
a PostgreSQL `numeric` of 40 digits, a `double precision` NaN and 5e-324
each stopped the load with a ValidationException, the items before them
already written.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql
from tests.typepair import Checkpoint

pytestmark = [pytest.mark.docker]

DDB, DDB_PORT = "migkit-test-f0t-ddb", 16064


@pytest.fixture(scope="module")
def dynamo():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
    except Exception:
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", DDB], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", DDB, "-p",
                    f"127.0.0.1:{DDB_PORT}:8000",
                    "amazon/dynamodb-local:latest", "-jar",
                    "DynamoDBLocal.jar", "-inMemory", "-sharedDb"],
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


def test_numbers_dynamodb_cannot_hold_are_refused(dynamo, pg_pair,
                                                  tmp_path):
    port = pg_pair["src"]
    psql(port, "drop database if exists ddbn")
    assert psql(port, "create database ddbn").returncode == 0
    assert psql(port, "create table n (id bigint primary key, v numeric,"
                      " f double precision); insert into n values"
                      " (1, 1234567890123456789012345678901234567890, 1),"
                      " (2, 1.5, 'NaN'), (3, 2, 5e-324), (4, 1e-131, 2.5),"
                      " (5, 1e40, 1e300)", db="ddbn").returncode == 0
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="f0tddb", engine="hetero",
              options={"source_engine": "postgres",
                       "target_engine": "dynamodb"},
              source=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              target=Endpoint(user="local", password="local", options={
                  "endpoint_url": f"http://127.0.0.1:{DDB_PORT}"}),
              databases=["ddbn"])
    hop.report_dir = lambda db=None: tmp_path
    eng = HeteroEngine(hop)
    with pytest.raises(SystemExit) as got:
        eng.move_table("ddbn", "public", "n", 100, Checkpoint(),
                       lambda m: None)
    said = " ".join(str(got.value).split())
    # 1e40 is one significant digit, which DynamoDB holds
    assert "v: 1 row holds more than 38 significant digits" in said \
        and "id 1" in said, said
    assert "v: 1 row holds a number outside the range dynamodb holds" \
        in said and "id 4" in said, said
    assert "f: 1 row holds NaN or an infinity, which dynamodb refuses" \
        in said and "id 2" in said, said
    assert "f: 2 rows hold a number outside the range dynamodb holds" \
        in said and "id 3, 5" in said, said
    assert not any(t.endswith("n") for t in
                   dynamo.list_tables()["TableNames"]), said
