"""Cassandra keeps -0.0 and 0.0 apart as two keys; every other engine here
compares a float by value and takes them for one (type-fidelity G29).

Measured before, Cassandra 4.1 into PostgreSQL 16: the move stopped
part-way on `ON CONFLICT DO UPDATE command cannot affect row a second
time`, the two keys being one to PostgreSQL - with the float rendering
writing both as 0, nothing before it had said so. Now the pair is named
before the first row is written.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import psql
from tests.typepair import Checkpoint

pytestmark = [pytest.mark.docker]

CS, CS_PORT = "migkit-test-f0t-cs", 16066


@pytest.fixture(scope="module")
def cassandra():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
    except Exception:
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", CS], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", CS, "-p",
                    f"127.0.0.1:{CS_PORT}:9042", "-e", "MAX_HEAP_SIZE=384M",
                    "-e", "HEAP_NEWSIZE=64M", "cassandra:4.1"], check=True,
                   capture_output=True)
    try:
        from cassandra.cluster import Cluster
        end, session = time.time() + 240, None
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


def test_keys_that_differ_by_the_sign_of_a_zero_are_refused(cassandra,
                                                            pg_pair,
                                                            tmp_path):
    cassandra.execute("create keyspace if not exists csz with replication ="
                      " {'class': 'SimpleStrategy', 'replication_factor': 1}")
    cassandra.execute("create table if not exists csz.z (k double primary"
                      " key, v text)")
    for k, v in ((0.0, "plus"), (-0.0, "minus"), (1.5, "other")):
        cassandra.execute("insert into csz.z (k, v) values (%s, %s)", (k, v))
    # the premise: two rows, one per sign
    assert cassandra.execute("select count(*) from csz.z").one()[0] == 3
    port = pg_pair["dst"]
    psql(port, "drop database if exists csz")
    assert psql(port, "create database csz").returncode == 0
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="f0tcs", engine="hetero",
              options={"source_engine": "cassandra",
                       "target_engine": "postgres"},
              source=Endpoint(host="127.0.0.1", port=CS_PORT),
              target=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"), databases=["csz"])
    hop.report_dir = lambda db=None: tmp_path
    with pytest.raises(SystemExit) as got:
        HeteroEngine(hop).move_table("csz", "", "z", 100, Checkpoint(),
                                     lambda m: None)
    said = " ".join(str(got.value).split())
    assert "z.k: 1 group of keys that differ only by the sign of a zero" \
        in said, said
    assert "0.0" in said and "-0.0" in said, said
    # and a key column without both signs moves
    cassandra.execute("delete from csz.z where k = %s", (-0.0,))
    assert cassandra.execute("select count(*) from csz.z").one()[0] == 2
    HeteroEngine(hop).move_table("csz", "", "z", 100, Checkpoint(),
                                 lambda m: None)
    assert psql(port, "select count(*) from z", db="csz").stdout.strip() \
        == "2"
