"""PostgreSQL's intervals, arrays and pgvector's vectors into Cassandra 5
and back, each as the same value (type-fidelity sections 8-9, G29).

Before: an interval, an array and a vector had no class across engines
(an array was a JSON array with no Cassandra type to build), so the table
could not be built and the move stopped - and Cassandra keeps an empty
list, set or map as NULL, which nothing counted: `'{}'` on the source
and NULL on the target read alike to nobody and differently to the
application. Now an interval is a `duration` of the same months, days
and time, an array a `list` of its elements, a vector a `vector<float,
n>` of the same floats, and an empty collection is refused before the
move, by table and column, with how many rows.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

CS, PG = "migkit-test-ty2-cs", "migkit-test-ty2-cpg"
CS_PORT, PG_PORT = 16214, 16215
REPLICATION = "{'class': 'SimpleStrategy', 'replication_factor': 1}"

SEED = """
create extension vector;
create table t (id int primary key, dur interval, a int[], s text[],
                e vector(3));
insert into t values
  (1, '1 mon 2 days 00:00:01.5', '{1,2,3}', '{"a b",c}',
   '[0.1,-0,3.4028235e38]'),
  (2, '-1 mon', '{7,8,9}', null, '[1,2,3]'),
  (3, null, null, '{x}', null);
create table hole (id int primary key, a int[], m text[]);
insert into hole values (1, '{}', '{x}'), (2, '{1}', '{}'), (3, null, '{}'),
  (4, '{5,NULL}', '{y}');
"""


def _up(port, timeout=240):
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
    for n in (CS, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", CS, "-p",
                        f"127.0.0.1:{CS_PORT}:9042", "-e",
                        "MAX_HEAP_SIZE=512M", "-e", "HEAP_NEWSIZE=96M",
                        "cassandra:5.0"], check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_PORT}:5432",
                        "pgvector/pgvector:pg16"], check=True,
                       capture_output=True)
        assert _up(PG_PORT)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(2)
        time.sleep(2)
        pg("create database cx", db="postgres")
        pg(SEED)
        from cassandra.cluster import Cluster
        end, session = time.time() + 300, None
        while time.time() < end and session is None:
            try:
                session = Cluster(["127.0.0.1"], port=CS_PORT).connect()
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(4)
        assert session is not None, "cassandra never answered"
        yield session
    finally:
        for n in (CS, PG):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _hop(tmp_path, source="postgres", target="cassandra", **options):
    post = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")
    cass = Endpoint(host="127.0.0.1", port=CS_PORT,
                    options={"replication": REPLICATION})
    hop = Hop(name="cs2", engine="hetero",
              source=post if source == "postgres" else cass,
              target=cass if target == "cassandra" else post,
              databases=["cx"],
              db_map={"cx": "cx"} if target == "cassandra" else {
                  "cx": "back"},
              options={"source_engine": source, "target_engine": target,
                       **options})
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
    tmp = tmp_path_factory.mktemp("cs")
    eng = _hop(tmp)
    eng.dst_engine.prepare_target("cx")
    eng.move_table("cx", "public", "t", 100, Checkpoint(), [].append)
    return eng


def test_each_value_lands_as_the_type_that_holds_it(servers, moved):
    types = {r.column_name: r.type for r in servers.execute(
        "select column_name, type from system_schema.columns where"
        " keyspace_name = 'cx' and table_name = 't'")}
    assert types == {"id": "bigint", "dur": "duration", "a": "list<bigint>",
                     "s": "list<text>", "e": "vector<float, 3>"}, types
    row = servers.execute("select dur, a, s, e from cx.t where id = 1").one()
    assert (row.dur.months, row.dur.days, row.dur.nanoseconds) \
        == (1, 2, 1_500_000_000)
    assert row.a == [1, 2, 3] and row.s == ["a b", "c"]
    import struct
    assert [struct.pack("<f", x) for x in row.e] == [
        struct.pack("<f", 0.1), struct.pack("<f", -0.0),
        struct.pack("<f", 3.4028235e38)]
    got = _verdict(moved, "t")
    assert got.status == "ok", got.detail


def test_a_month_that_became_thirty_days_is_found(servers, moved):
    from cassandra.util import Duration
    servers.execute("update cx.t set dur = %s where id = 2",
                    (Duration(0, -30, 0),))
    try:
        got = _verdict(moved, "t")
        assert got.status == "diff", got.detail
    finally:
        servers.execute("update cx.t set dur = %s where id = 2",
                        (Duration(-1, 0, 0),))
    assert _verdict(moved, "t").status == "ok"


def test_a_float_one_bit_away_is_found(servers, moved):
    import struct
    near = struct.unpack("<f", struct.pack("<I", struct.unpack(
        "<I", struct.pack("<f", 2.0))[0] + 1))[0]
    servers.execute("update cx.t set e = %s where id = 2", ([1.0, near, 3.0],))
    try:
        got = _verdict(moved, "t")
        assert got.status == "diff", got.detail
    finally:
        servers.execute("update cx.t set e = %s where id = 2",
                        ([1.0, 2.0, 3.0],))


def test_an_empty_collection_is_refused_not_turned_into_null(servers,
                                                             tmp_path):
    eng = _hop(tmp_path)
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "public", "hole", 100, Checkpoint(),
                       [].append)
    said = str(refused.value)
    assert "hole.a: 1 row holds an empty list" in said, said
    assert "hole.m: 2 rows hold an empty list" in said, said
    assert "hole.a: 1 row holds a NULL inside a collection" in said, said
    assert "nothing was copied" in said


def test_an_empty_list_and_a_null_are_two_values_to_the_check(servers,
                                                              moved):
    pg("update t set s = '{}' where id = 2")
    try:
        got = _verdict(moved, "t")
        assert got.status == "diff", got.detail
    finally:
        pg("update t set s = null where id = 2")


def test_durations_come_back_and_a_nanosecond_is_refused(servers, moved,
                                                         tmp_path):
    pg("create database back", db="postgres")
    # the vector column is built as pgvector's
    pg("create extension vector", db="back")
    back = _hop(tmp_path, "cassandra", "postgres")
    back.move_table("cx", "", "t", 100, Checkpoint(), [].append)
    assert pg("select coalesce(dur::text, '-') from t order by id",
              db="back") == "1 mon 2 days 00:00:01.5\n-1 mons\n-"
    got = _verdict(back, "t")
    assert got.status == "ok", got.detail
    from cassandra.util import Duration
    servers.execute("update cx.t set dur = %s where id = 3",
                    (Duration(0, 0, 1),))
    pg("drop table t", db="back")
    again = _hop(tmp_path, "cassandra", "postgres")
    with pytest.raises(SystemExit) as refused:
        again.move_table("cx", "", "t", 100, Checkpoint(), [].append)
    assert "t.dur: 1 row holds more than 6 digits of a second" \
        in str(refused.value), str(refused.value)
