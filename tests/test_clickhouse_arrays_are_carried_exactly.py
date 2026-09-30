"""Arrays into ClickHouse `Array` columns and back, each element as it was
(type-fidelity section 8.2).

Before: a PostgreSQL array was a JSON array with no ClickHouse type to
build (`no DDL type for class 'json' on clickhouse`), and into a table
made by hand every list went through the writer's JSON text - a string,
into an `Array(Int64)`. An `Array(String)`'s elements were read as
bytes and rendered as a Python bytes object's text. Now an array of
whole numbers, floats or text is an `Array(Nullable(T))` of the same
elements, a NULL inside one kept; and a NULL array, which an `Array`
cannot hold (ClickHouse writes an empty one in its place), is refused
before the move.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

CH, PG = "migkit-test-ty2-ch", "migkit-test-ty2-hpg"
CH_PORT, PG_PORT = 16216, 16217

SEED = """
create table t (id int primary key, a bigint[] not null, s text[] not null,
                f float8[] not null, r real[] not null);
insert into t values
  (1, '{1,NULL,-9223372036854775808}', '{"a b",é,NULL}', '{1.5,-0,1e300}',
   '{0.1,3.4028235e38}'),
  (2, '{}', '{}', '{}', '{}');
create table holes (id int primary key, a bigint[]);
insert into holes values (1, '{1}'), (2, null), (3, null);
"""


def _up(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def pg(sql, db="cx"):
    got = subprocess.run(["docker", "exec", "-i", PG, "psql", "-U",
                          "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for n in (CH, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", CH, "-e",
                        "CLICKHOUSE_PASSWORD=test", "-p",
                        f"127.0.0.1:{CH_PORT}:8123", "--ulimit",
                        "nofile=262144:262144",
                        "clickhouse/clickhouse-server:24.8"], check=True,
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_PORT}:5432", "postgres:16"],
                       check=True, capture_output=True)
        assert _up(PG_PORT) and _up(CH_PORT)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(2)
        time.sleep(2)
        pg("create database cx", db="postgres")
        pg(SEED)
        import clickhouse_connect
        end, client = time.time() + 120, None
        while time.time() < end:
            try:
                client = clickhouse_connect.get_client(
                    host="127.0.0.1", port=CH_PORT, username="default",
                    password="test")
                client.command("select 1")
                break
            except Exception:  # noqa: BLE001 - not up yet
                client = None
                time.sleep(1)
        assert client is not None
        yield client
    finally:
        for n in (CH, PG):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _hop(tmp_path, source="postgres", target="clickhouse"):
    post = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")
    ch = Endpoint(host="127.0.0.1", port=CH_PORT, user="default",
                  password="test")
    hop = Hop(name="cha", engine="hetero",
              source=post if source == "postgres" else ch,
              target=ch if target == "clickhouse" else post,
              databases=["cx"],
              db_map={} if target == "clickhouse" else {"cx": "back"},
              options={"source_engine": source, "target_engine": target})
    hop.report_dir = lambda db=None: tmp_path
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(hop)


def _verdict(eng, table):
    got = [r for r in eng.check_data("cx")
           if r.check == "data" and r.scope.endswith(f".{table}")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


@pytest.fixture(scope="module")
def moved(servers, tmp_path_factory):
    eng = _hop(tmp_path_factory.mktemp("ch"))
    eng.dst_engine.prepare_target("cx")
    eng.move_table("cx", "public", "t", 100, Checkpoint(), [].append)
    return eng


def test_arrays_land_in_array_columns_element_for_element(servers, moved):
    types = dict(servers.query("select name, type from system.columns where"
                               " database = 'cx' and table = 't'")
                 .result_rows)
    assert types["a"] == "Array(Nullable(Int64))", types
    assert types["s"] == "Array(Nullable(String))", types
    assert types["f"] == "Array(Nullable(Float64))", types
    rows = dict(servers.query("select id, a from cx.t").result_rows)
    assert rows == {1: [1, None, -9223372036854775808], 2: []}, rows
    got = servers.query("select toString(s), toString(f), toString(r)"
                        " from cx.t where id = 1").result_rows[0]
    assert got[0] == "['a b','é',NULL]", got
    assert got[1] == "[1.5,-0,1e300]", got
    got = _verdict(moved, "t")
    assert got.status == "ok", got.detail


def test_an_element_changed_in_clickhouse_is_found(servers, moved):
    servers.command("alter table cx.t update s = ['a b', 'e', NULL]"
                    " where id = 1", settings={"mutations_sync": 2})
    try:
        got = _verdict(moved, "t")
        assert got.status == "diff", got.detail
    finally:
        servers.command("alter table cx.t update s = ['a b', 'é', NULL]"
                        " where id = 1", settings={"mutations_sync": 2})
    assert _verdict(moved, "t").status == "ok"


def test_arrays_come_back_to_postgresql_as_they_went(servers, moved,
                                                     tmp_path):
    pg("create database back", db="postgres")
    back = _hop(tmp_path, "clickhouse", "postgres")
    back.move_table("cx", "", "t", 100, Checkpoint(), [].append)
    assert pg("select a::text || ' ' || s::text from t order by id",
              db="back") == ('[1, null, -9223372036854775808] ["a b", "é",'
                             ' null]\n[] []')
    got = _verdict(back, "t")
    assert got.status == "ok", got.detail


def test_a_null_array_is_refused_not_emptied(servers, tmp_path):
    eng = _hop(tmp_path)
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "public", "holes", 100, Checkpoint(),
                       [].append)
    said = str(refused.value)
    assert "holes.a: 2 rows hold NULL, which the clickhouse column is not" \
           " Nullable for" in said, said
