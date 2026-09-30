"""PostGIS shapes into MySQL 9 and back, each point where it was (type-
fidelity section 8.6), and an interval refused where MySQL has no type
for one.

Before: a `geometry` had no class across engines, so the table could not
be built and the move stopped; carried as its bytes it would have
crossed the axes, since for SRID 4326 MySQL reads the first number of a
point as the latitude (EPSG's order) where PostGIS, GeoJSON and SQL
Server's binary put the longitude first. Now a shape is its SRID and its
longitude-first binary on both sides, MySQL is written in its own order,
and a point that moved - or whose axes were swapped - is a difference.

The `postgis/postgis` image is built for x86 only: this runs in the VM
`with_docker_lock.py --vm migkit` starts, under Rosetta.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

PG, MY = "migkit-test-ty2-gpg", "migkit-test-ty2-gmy"
PG_PORT, MY_PORT = 16210, 16211

SEED = """
create extension postgis;
create table g (id int primary key, p geometry(Point, 4326),
                shape geometry, sphere geography);
insert into g values
  (1, 'SRID=4326;POINT(100.5 13.75)',
   'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 2))',
   'SRID=4326;LINESTRING(100.5 13.75, 101 14)'),
  (2, 'SRID=4326;POINT(-73.9857 40.7484)',
   'SRID=3857;MULTIPOINT((1 2), (3 4))', null),
  (3, null, null, null);
create table spans (id int primary key, took interval);
insert into spans values (1, '1 mon 2 days');
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
                          "-ptest", "--default-character-set=utf8mb4",
                          "-N", "-B", *(["-D", db] if db else [])],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        # x86 only: under Rosetta in the separate VM (`with_docker_lock.py
        # --vm migkit`)
        subprocess.run(["docker", "run", "-d", "--name", PG, "--platform",
                        "linux/amd64", "-e", "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_PORT}:5432",
                        "postgis/postgis:16-3.4"], check=True,
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


def _hop(tmp_path, source="postgres", target="mysql"):
    post = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")
    mysql = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                     password="test")
    hop = Hop(name="geo", engine="hetero",
              source=post if source == "postgres" else mysql,
              target=mysql if target == "mysql" else post,
              databases=["cx"],
              db_map={} if target == "mysql" else {"cx": "back"},
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
    eng = _hop(tmp_path_factory.mktemp("geo"))
    eng.move_table("cx", "public", "g", 100, Checkpoint(), [].append)
    return eng


def test_a_point_lands_at_its_own_latitude_and_longitude(moved):
    assert my("select st_srid(p), st_latitude(p), st_longitude(p) from g"
              " where id = 1") == "4326\t13.75\t100.5"
    assert my("select st_srid(shape), st_astext(shape) from g where id = 2") \
        == "3857\tMULTIPOINT((1 2),(3 4))"
    assert my("select st_astext(shape) from g where id = 1") \
        == "POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,3 2,3 3,2 2))"
    got = _verdict(moved, "g")
    assert got.status == "ok", got.detail


def test_a_point_whose_axes_were_swapped_is_found(moved):
    my("update g set p = st_srid(point(100.5, 13.75), 4326) where id = 1")
    try:
        assert my("select st_latitude(p) from g where id = 1") == "100.5"
        got = _verdict(moved, "g")
        assert got.status == "diff", got.detail
    finally:
        my("update g set p = st_geomfromtext('POINT(13.75 100.5)', 4326)"
           " where id = 1")
    assert _verdict(moved, "g").status == "ok"


def test_shapes_come_back_to_postgis_where_they_were(moved, tmp_path):
    pg("create database back", db="postgres")
    pg("create extension postgis", db="back")
    back = _hop(tmp_path, "mysql", "postgres")
    back.move_table("cx", "", "g", 100, Checkpoint(), [].append)
    assert pg("select st_x(p) || ' ' || st_y(p) || ' ' || st_srid(p)"
              " from g where id = 1", db="back") == "100.5 13.75 4326"
    assert pg("select st_asewkt(shape) from g where id = 2", db="back") \
        == "SRID=3857;MULTIPOINT((1 2),(3 4))"
    got = _verdict(back, "g")
    assert got.status == "ok", got.detail


def test_an_interval_has_nowhere_to_go_on_mysql(servers, tmp_path):
    eng = _hop(tmp_path)
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "public", "spans", 100, Checkpoint(),
                       [].append)
    said = str(refused.value)
    assert "spans is not on the target and migkit cannot build it" in said
    assert "took (interval)" in said and "no DDL type for class" \
        " 'interval' on mysql" in said, said
