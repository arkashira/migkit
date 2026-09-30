"""pgvector's vectors into MySQL 9's `VECTOR` and back, every element the
same 32-bit float (type-fidelity section 8.9).

Before: a pgvector column was classed as text, so a move into MySQL built
a `varchar` and wrote the vector's text into it - a string where the
source held an embedding - and the check compared the two texts. Now a
vector is a `VECTOR(n)` of the same floats, written as MySQL keeps them
(little-endian 32-bit floats, not its six-digit text), compared by the
bits of each element; a `halfvec` target that would round them, and a
target of other dimensions, are refused before the move.
"""
import socket
import struct
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

PG, MY = "migkit-test-ty2-vpg", "migkit-test-ty2-vmy"
PG_PORT, MY_PORT = 16210, 16211

SEED = """
create extension vector;
create table v (id int primary key, e vector(3), h halfvec(3));
insert into v values
  (1, '[0.1,-0,3.4028235e38]', '[0.5,1,2]'),
  (2, '[1e-7,0.33333334,-2]', '[0.33325195,1,-1]'),
  (3, null, null);
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


def my(sql, db="cx"):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B",
                          *(["-D", db] if db else [])],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_PORT}:5432",
                        "pgvector/pgvector:pg16"], check=True,
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"127.0.0.1:{MY_PORT}:3306", "mysql:9.4"],
                       check=True, capture_output=True)
        assert _up(PG_PORT) and _up(MY_PORT)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(2)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(2)
        time.sleep(3)
        pg("create database cx", db="postgres")
        pg(SEED)
        my("create database cx", db=None)
        yield
    finally:
        for n in (PG, MY):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _hop(tmp_path, source="postgres", target="mysql", db_map=None):
    post = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")
    mysql = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                     password="test")
    hop = Hop(name="vec", engine="hetero",
              source=post if source == "postgres" else mysql,
              target=mysql if target == "mysql" else post,
              databases=["cx"], db_map=db_map or {},
              options={"source_engine": source, "target_engine": target})
    hop.report_dir = lambda db=None: tmp_path
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(hop)


def _verdict(eng, table):
    got = [r for r in eng.check_data("cx")
           if r.check == "data" and r.scope.endswith(f".{table}")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def _f32(*xs):
    return struct.pack(f"<{len(xs)}f", *xs).hex().upper()


@pytest.fixture(scope="module")
def moved(servers, tmp_path_factory):
    eng = _hop(tmp_path_factory.mktemp("vec"))
    eng.move_table("cx", "public", "v", 100, Checkpoint(), [].append)
    return eng


def test_a_vector_lands_as_the_floats_it_holds(moved):
    assert my("select column_type from information_schema.columns where"
              " table_schema = 'cx' and table_name = 'v' and column_name"
              " = 'e'") == "vector(3)"
    assert my("select hex(e) from v where id = 1") \
        == _f32(0.1, -0.0, 3.4028235e38)
    # a halfvec's floats, widened to 32 bits as they are
    assert my("select hex(h) from v where id = 2") \
        == _f32(0.33325195, 1, -1)
    got = _verdict(moved, "v")
    assert got.status == "ok", got.detail


def test_one_bit_of_one_element_is_a_difference(moved):
    bits = struct.unpack("<I", struct.pack("<f", 0.1))[0] + 1
    near = struct.pack("<I", bits).hex()
    my(f"update v set e = unhex(concat('{near}', substr(hex(e), 9)))"
       " where id = 1")
    try:
        got = _verdict(moved, "v")
        assert got.status == "diff", got.detail
    finally:
        my(f"update v set e = unhex('{_f32(0.1, -0.0, 3.4028235e38)}')"
           " where id = 1")
    assert _verdict(moved, "v").status == "ok"


def test_vectors_come_back_to_pgvector_bit_for_bit(moved, tmp_path):
    pg("create database back", db="postgres")
    pg("create extension vector", db="back")
    back = _hop(tmp_path, "mysql", "postgres", {"cx": "back"})
    back.move_table("cx", "", "v", 100, Checkpoint(), [].append)
    assert pg("select e::text from v where id = 1", db="back") \
        == pg("select e::text from v where id = 1")
    got = _verdict(back, "v")
    assert got.status == "ok", got.detail


def test_a_target_that_would_round_them_is_refused(moved, tmp_path):
    """MySQL's 32-bit floats into a `halfvec` made by hand: 0.1 has no
    16-bit float, and the target would keep another number."""
    pg("create table narrow (id int primary key, e halfvec(3))", db="back")
    my("create table narrow (id int primary key, e vector(3));"
       " insert into narrow select id, e from v")
    eng = _hop(tmp_path, "mysql", "postgres", {"cx": "back"})
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "", "narrow", 100, Checkpoint(), [].append)
    said = str(refused.value)
    assert "narrow.e: 2 rows hold a vector whose elements a 16-bit float" \
           in said, said
