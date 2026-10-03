"""Oracle's own types carried, compared and refused exactly (backlog W6):
an XMLType as its document, an SDO_GEOMETRY as its SRID and binary - at
EPSG's number for Oracle's own 8307 - a BINARY_DOUBLE's NaN and infinity,
a FLOAT(126) at all thirty-eight of its digits, an object type and a
VARRAY as the JSON of what they hold where the hop accepts that, a time
with its zone, a ROWID left out and said, a BFILE refused and carried as
its file's bytes where accepted; PostGIS shapes and XML into Oracle
through the types' own constructors, a polygon drawn clockwise and XML
that is no single document refused before the move.

Before: a FLOAT was read as a double, so thirty-eight digits arrived as
seventeen and compared equal to them; XMLType, SDO_GEOMETRY, BFILE and
object types had no class, so a table holding one could not be built and
a column of one was compared nowhere.

Oracle Free's full image (the slim one has no Spatial) and PostGIS run in
the VM `with_docker_lock.py --vm migkit` starts.
"""
import datetime
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

ORA, ORA_PORT, PW = "migkit-test-w6-ora", 16542, "CHANGE_ME_w6ora1"
PG, PG_PORT = "migkit-test-w6-orapg", 16543
IMAGE = "gvenzl/oracle-free:full-faststart"

SEED = [
    "create type address_t as object (street varchar2(40), since date)",
    "create type phones_t as varray(3) of varchar2(20)",
    "create table o (id number(10) primary key, doc xmltype,"
    " shape sdo_geometry, f binary_double, big float(126), n number,"
    " a address_t, p phones_t, tz timestamp(6) with time zone, r rowid)",
    "insert into o values (1, xmltype('<a b=\"1\"><c>x</c></a>'),"
    " sdo_geometry(2001, 8307, sdo_point_type(100.5, 13.75, null), null,"
    " null), binary_double_nan,"
    " 0.12345678901234567890123456789012345678, 1.5,"
    " address_t('1 Main', date '2024-01-02'), phones_t('1', '2'),"
    " timestamp '2024-01-01 07:00:00 +07:00', null)",
    "insert into o values (2, null, sdo_geometry(2003, 8307, null,"
    " sdo_elem_info_array(1, 1003, 1), sdo_ordinate_array(0, 0, 10, 0,"
    " 10, 10, 0, 10, 0, 0)), binary_double_infinity, 1.5e125, -0.001,"
    " null, phones_t(), timestamp '2024-01-01 00:00:00 +00:00', null)",
    "insert into o (id) values (3)",
    "update o set r = rowid",
    "create table bf (id number(10) primary key, f bfile)",
    "insert into bf values (1, bfilename('W6DIR', 'w6.bin'))",
    "insert into bf values (2, null)",
]

PG_SEED = """
create extension postgis;
create table pshape (id int primary key, g geometry(Polygon, 4326), d xml);
insert into pshape values
  (1, 'SRID=4326;POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', '<r><s/></r>'),
  (2, null, null);
create table pbad (id int primary key, g geometry(Polygon, 4326), d xml);
insert into pbad values
  (1, 'SRID=4326;POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', '<a/><b/>');
"""


def _docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def ora(sql, user="app"):
    import oracledb
    oracledb.defaults.fetch_lobs = False
    conn = oracledb.connect(user=user, password=PW,
                            dsn=f"127.0.0.1:{ORA_PORT}/FREEPDB1")
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()] if cur.description \
            else None
    finally:
        conn.close()


def pg(sql, db="cx"):
    got = subprocess.run(["docker", "exec", "-i", PG, "psql", "-U",
                          "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for n in (ORA, PG):
        _docker("rm", "-f", "-v", n)
    try:
        since = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        made = _docker("run", "-d", "--name", ORA, "-p",
                       f"127.0.0.1:{ORA_PORT}:1521", "-e",
                       f"ORACLE_PASSWORD={PW}", "-e", "APP_USER=app", "-e",
                       f"APP_USER_PASSWORD={PW}", "--shm-size=1g", IMAGE)
        assert made.returncode == 0, made.stderr
        # x86 only: under Rosetta in the separate VM
        made = _docker("run", "-d", "--name", PG, "--platform",
                       "linux/amd64", "-e", "POSTGRES_PASSWORD=test", "-p",
                       f"127.0.0.1:{PG_PORT}:5432", "postgis/postgis:16-3.4")
        assert made.returncode == 0, made.stderr
        end = time.time() + 1200
        while time.time() < end:
            logs = _docker("logs", "--since", since, ORA)
            if "DATABASE IS READY TO USE" in logs.stdout + logs.stderr:
                break
            time.sleep(5)
        else:
            pytest.fail("Oracle Free never said it was ready")
        for _ in range(60):
            if _docker("exec", PG, "pg_isready", "-U",
                       "postgres").returncode == 0:
                break
            time.sleep(2)
        time.sleep(3)
        # a file on the server's own disk for the BFILE to point at
        assert _docker("exec", ORA, "sh", "-c",
                       "printf abc > /tmp/w6.bin").returncode == 0
        ora("create directory w6dir as '/tmp'", user="system")
        ora("grant read on directory w6dir to app", user="system")
        for s in SEED:
            ora(s)
        pg("create database cx", db="postgres")
        pg(PG_SEED)
        yield
    finally:
        for n in (ORA, PG):
            _docker("rm", "-f", "-v", n)


def _pair(tmp_path, src, dst, **options):
    from migkit.engines.hetero import HeteroEngine
    oracle = Endpoint(host="127.0.0.1", port=ORA_PORT, user="system",
                      password=PW, options={"service": "FREEPDB1"})
    post = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")
    if src == "oracle":
        hop = Hop(name="o2p", engine="hetero", source=oracle, target=post,
                  databases=["app"], db_map={"app": "cx"},
                  options={"source_engine": "oracle",
                           "target_engine": "postgres", **options})
    else:
        hop = Hop(name="p2o", engine="hetero", source=post, target=oracle,
                  databases=["cx"], db_map={"cx": "app"},
                  options={"source_engine": "postgres",
                           "target_engine": "oracle", **options})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _verdict(eng, db, table):
    got = [r for r in eng.check_data(db)
           if r.check == "data" and r.scope.endswith(table)]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def test_objects_and_a_zone_go_to_postgresql_only_as_accepted(servers,
                                                               tmp_path):
    eng = _pair(tmp_path, "oracle", "postgres")
    with pytest.raises(SystemExit) as refused:
        eng.move_table("app", "", "o", 100, Checkpoint(), [].append)
    said = str(refused.value)
    assert "o.a: 1 row holds an Oracle object or collection" in said, said
    assert "o.p: 2 rows hold an Oracle object or collection" in said, said
    assert "o.tz: 1 row holds a time written at an offset of its own" \
        in said, said
    eng = _pair(tmp_path, "oracle", "postgres",
                accept_changes=["o.a", "o.p", "o.tz"])
    log = []
    eng.move_table("app", "", "o", 100, Checkpoint(), log.append)
    assert pg("select st_srid(shape), st_astext(shape) from o order by id") \
        == "4326|POINT(100.5 13.75)\n4326|POLYGON((0 0,10 0,10 10,0 10,0" \
           " 0))\n|"
    assert pg("select coalesce(f::text, '-') from o order by id") == \
        "NaN\nInfinity\n-"
    # all thirty-eight digits: as a double they were seventeen
    assert pg("select big::text from o where id = 1") == \
        "0.12345678901234567890123456789012345678"
    assert pg("select a->>'STREET', a->>'SINCE' from o where id = 1") == \
        "1 Main|2024-01-02T00:00:00"
    assert pg("select coalesce(p::text, '-') from o order by id") == \
        '["1", "2"]\n[]\n-'
    assert pg("select to_char(tz at time zone 'UTC', 'YYYY-MM-DD HH24:MI')"
              " from o where id = 1") == "2024-01-01 00:00"
    # the row's address is no data of the target's
    assert pg("select count(*) from information_schema.columns where"
              " table_name = 'o' and column_name = 'r'") == "0"
    assert any("not carried" in m and "r, a row's address" in m
               for m in log), log
    got = _verdict(eng, "app", ".o")
    assert got.status == "ok", got.detail
    pg("update o set big = 0.12345678901234567890123456789012345679"
       " where id = 1")
    assert _verdict(eng, "app", ".o").status == "diff"


def test_a_bfile_is_carried_as_its_files_bytes_only_where_accepted(
        servers, tmp_path):
    eng = _pair(tmp_path, "oracle", "postgres")
    with pytest.raises(SystemExit) as refused:
        eng.move_table("app", "", "bf", 100, Checkpoint(), [].append)
    assert "bf.f: 1 row holds a BFILE" in str(refused.value)
    eng = _pair(tmp_path, "oracle", "postgres", accept_changes=["bf.f"])
    eng.move_table("app", "", "bf", 100, Checkpoint(), [].append)
    assert pg("select coalesce(encode(f, 'hex'), '-') from bf order by"
              " id") == "616263\n-"
    got = _verdict(eng, "app", ".bf")
    assert got.status == "ok", got.detail


def test_shapes_and_xml_go_into_oracle_through_their_constructors(
        servers, tmp_path):
    eng = _pair(tmp_path, "postgres", "oracle")
    eng.move_table("cx", "public", "pshape", 100, Checkpoint(), [].append)
    assert ora("select t.g.sdo_srid, sdo_geom.validate_geometry_with_context("
               "t.g, 0.005), xmlserialize(content t.d as varchar2(200))"
               " from pshape t where t.id = 1") == [
        (4326, "TRUE", "<r><s/></r>")]
    got = _verdict(eng, "cx", ".pshape")
    assert got.status == "ok", got.detail
    ora("update pshape t set t.g = sdo_geometry(2003, 4326, null,"
        " sdo_elem_info_array(1, 1003, 1), sdo_ordinate_array(0, 0, 11, 0,"
        " 11, 10, 0, 10, 0, 0)) where t.id = 1")
    assert _verdict(eng, "cx", ".pshape").status == "diff"


def test_a_polygon_drawn_clockwise_and_xml_of_two_documents_are_refused(
        servers, tmp_path):
    eng = _pair(tmp_path, "postgres", "oracle")
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "public", "pbad", 100, Checkpoint(), [].append)
    said = str(refused.value)
    assert "pbad.g: 1 row holds a polygon whose outer ring runs clockwise" \
        in said, said
    assert "pbad.d: 1 row holds XML that is not one document" in said, said
    assert ora("select count(*) from user_tables where table_name ="
               " 'PBAD'") == [(0,)]
