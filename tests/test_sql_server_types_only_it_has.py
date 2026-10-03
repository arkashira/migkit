"""SQL Server's own types carried, compared and refused exactly (backlog
W6): a `sql_variant` with its base type, a `hierarchyid` as its path, a
`datetimeoffset` with its offset, money to its last place, a row version
left out and said, PostGIS shapes written into a `geography` through its
constructor - a polygon drawn clockwise refused, which the geography would
hold as the rest of the globe - and XML refused where the target column is
typed by a schema collection.

Before: a `sql_variant` read through the driver was its base value alone,
so an `int` 5 written back as a `bigint` 5 compared equal; a
`datetimeoffset` was unmapped, compared nowhere and written at UTC; a
shape into SQL Server was refused outright.

SQL Server 2022 itself (SQL Edge has neither spatial types nor
`hierarchyid`) and PostGIS are x86 only: both run under Rosetta in the VM
`with_docker_lock.py --vm migkit` starts; it skips, saying so, only where
SQL Server cannot start at all.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.typepair import Checkpoint

pytestmark = needs_docker

MS, MS_PORT, PW = "migkit-test-w6-mssql", 16540, "CHANGE_ME-W6-Str0ng!"
PG, PG_PORT = "migkit-test-w6-pg", 16541
IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"

VARIANTS = """
create table dbo.v (id int primary key, v sql_variant, h hierarchyid,
                    at datetimeoffset(7), rv rowversion, m money);
insert into dbo.v (id, v, h, at, m) values
 (1, cast(5 as int), hierarchyid::Parse('/1/'),
  '2024-02-29 13:14:15.1234567 +07:00', 922337203685477.5807),
 (2, cast(5 as bigint), hierarchyid::Parse('/1/3.5/'),
  '2024-01-01 00:00:00 +00:00', -1.5),
 (3, cast(N'5' as nvarchar(1)), hierarchyid::Parse('/'), null, null),
 (4, cast(1.50 as decimal(10,2)), null,
  '0001-01-01 00:00:00.0000001 -14:00', 0),
 (5, cast('2024-01-01 00:00:00.003' as datetime), null, null, null),
 (6, cast(0x00ff as varbinary(8)), null, null, null),
 (7, cast(0.1 as float), null, null, null),
 (8, null, null, null, null)
"""

#: the same kinds into PostgreSQL, at the six digits it keeps
SIX = """
create table dbo.w (id int primary key, v sql_variant, h hierarchyid,
                    at datetimeoffset(7), rv rowversion);
insert into dbo.w (id, v, h, at) values
 (1, cast(5 as int), hierarchyid::Parse('/1/'),
  '2024-02-29 13:14:15.123456 +07:00'),
 (2, cast(N'5' as nvarchar(1)), hierarchyid::Parse('/1/3.5/'),
  '2024-01-01 00:00:00 +00:00'),
 (3, cast(1.50 as decimal(10,2)), null, null),
 (4, null, null, null)
"""

TYPED = """
create xml schema collection dbo.nums as N'<xs:schema
 xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="n"
 type="xs:decimal"/></xs:schema>'
"""

SHAPES = """
create extension postgis;
create table s (id int primary key, g geography);
insert into s values
  (1, 'SRID=4326;POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'),
  (2, 'SRID=4326;POINT(100.5 13.75)'),
  (3, null);
create table cw (id int primary key, g geography);
insert into cw values (1, 'SRID=4326;POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
create table x (id int primary key, d xml);
insert into x values (1, '<n>1.50</n>');
"""


def _ms(db, *statements):
    import pymssql
    conn = pymssql.connect(server="127.0.0.1", port=str(MS_PORT), user="sa",
                           password=PW, database=db, autocommit=True,
                           tds_version="7.4", login_timeout=10)
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = [tuple(r) for r in cur.fetchall()] if cur.description \
                else None
        return out
    finally:
        conn.close()


def pg(sql, db="cx"):
    got = subprocess.run(["docker", "exec", "-i", PG, "psql", "-U",
                          "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def servers():
    for n in (MS, PG):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        run = subprocess.run(
            ["docker", "run", "-d", "--name", MS, "--platform",
             "linux/amd64", "-p", f"127.0.0.1:{MS_PORT}:1433", "-e",
             "ACCEPT_EULA=Y", "-e", f"MSSQL_SA_PASSWORD={PW}", "-e",
             "MSSQL_MEMORY_LIMIT_MB=2048", "-e",
             "MSSQL_TELEMETRY_ENABLED=false", IMAGE],
            capture_output=True, text=True)
        if run.returncode:
            pytest.skip("SQL Server 2022 cannot run on this machine:"
                        f" {run.stderr.strip()[-300:]}")
        # x86 only: under Rosetta in the separate VM
        subprocess.run(["docker", "run", "-d", "--name", PG, "--platform",
                        "linux/amd64", "-e", "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PG_PORT}:5432",
                        "postgis/postgis:16-3.4"], check=True,
                       capture_output=True)
        end = time.time() + 420
        while time.time() < end:
            if _up(MS_PORT, 5):
                try:
                    _ms("master", "select 1")
                    break
                except Exception:  # noqa: BLE001 - not up yet
                    pass
            time.sleep(3)
        else:
            pytest.skip("SQL Server 2022 did not answer within 7 minutes"
                        " on this machine")
        assert _up(PG_PORT)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(2)
        time.sleep(3)
        _ms("master", "create database cx", "create database cx2")
        _ms("cx", VARIANTS, SIX, TYPED,
            "create table dbo.x (id int primary key, d xml(dbo.nums))")
        pg("create database cx", db="postgres")
        pg(SHAPES)
        yield
    finally:
        for n in (MS, PG):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _ep(engine):
    if engine == "mssql":
        return Endpoint(host="127.0.0.1", port=MS_PORT, user="sa",
                        password=PW)
    return Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                    password="test")


def _pair(tmp_path, src, dst, **options):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name=f"{src}2{dst}", engine="hetero", source=_ep(src),
              target=_ep(dst), databases=["cx"],
              db_map={"cx": "cx2"} if src == dst else {},
              options={"source_engine": src, "target_engine": dst,
                       **options})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _verdict(eng, table):
    got = [r for r in eng.check_data("cx")
           if r.check == "data" and r.scope.endswith(f".{table}")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


BASE = ("select id, cast(sql_variant_property(v, 'BaseType') as"
        " varchar(20)), convert(nvarchar(64), v), h.ToString(),"
        " convert(varchar(34), at, 121), m from dbo.v order by id")


@pytest.fixture(scope="module")
def copied(servers, tmp_path_factory):
    eng = _pair(tmp_path_factory.mktemp("ms2ms"), "mssql", "mssql")
    said = []
    eng.move_table("cx", "dbo", "v", 100, Checkpoint(), said.append)
    return eng, said


def test_a_variant_keeps_its_base_type_and_an_offset_its_offset(copied):
    eng, said = copied
    assert _ms("cx2", BASE) == _ms("cx", BASE)
    # every digit of the seventh, and the offset each was written at
    assert _ms("cx2", "select convert(varchar(34), at, 121) from dbo.v"
                      " where id = 1") == [
        ("2024-02-29 13:14:15.1234567 +07:00",)]
    # the row version is the target's own, and said
    assert any(m.startswith("v: not carried, not data") and m.endswith(
        ": rv") for m in said), said
    got = _verdict(eng, "v")
    assert got.status == "ok", got.detail


def test_a_base_type_changed_under_the_same_value_is_found(copied):
    eng, _ = copied
    _ms("cx2", "update dbo.v set v = cast(5 as bigint) where id = 1")
    try:
        assert _ms("cx2", "select convert(int, v) from dbo.v where id = 1") \
            == [(5,)]
        got = _verdict(eng, "v")
        assert got.status == "diff", got.detail
    finally:
        _ms("cx2", "update dbo.v set v = cast(5 as int) where id = 1")
    assert _verdict(eng, "v").status == "ok"


def test_one_instant_at_another_offset_is_another_value(copied):
    eng, _ = copied
    _ms("cx2", "update dbo.v set at = '2024-02-29 06:14:15.1234567 +00:00'"
               " where id = 1")
    try:
        got = _verdict(eng, "v")
        assert got.status == "diff", got.detail
    finally:
        _ms("cx2", "update dbo.v set at ="
                   " '2024-02-29 13:14:15.1234567 +07:00' where id = 1")


def test_a_variant_and_an_offset_go_to_postgresql_only_as_accepted(
        servers, tmp_path):
    eng = _pair(tmp_path, "mssql", "postgres")
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "dbo", "w", 100, Checkpoint(), [].append)
    said = str(refused.value)
    assert "w.v: 3 rows hold a SQL Server sql_variant" in said, said
    assert "w.at: 1 row holds a time written at an offset of its own" \
        in said, said
    eng = _pair(tmp_path, "mssql", "postgres",
                accept_changes=["w.v", "w.at"])
    eng.move_table("cx", "dbo", "w", 100, Checkpoint(), [].append)
    assert pg("select v->>'type', v->>'value' from w order by id") == \
        "int|5\nnvarchar(1)|5\ndecimal(10,2)|1.50\n|"
    assert pg("select h from w where id = 2") == "/1/3.5/"
    assert pg("select to_char(at at time zone 'UTC',"
              " 'YYYY-MM-DD HH24:MI:SS.US') from w where id = 1") == \
        "2024-02-29 06:14:15.123456"
    # the row version is not built there: the target writes none
    assert pg("select count(*) from information_schema.columns where"
              " table_name = 'w' and column_name = 'rv'") == "0"
    got = _verdict(eng, "w")
    assert got.status == "ok", got.detail
    pg("""update w set v = '{"type": "bigint", "value": "5"}' where id = 1""")
    assert _verdict(eng, "w").status == "diff"


def test_a_shape_goes_into_a_geography_through_its_constructor(servers,
                                                                tmp_path):
    eng = _pair(tmp_path, "postgres", "mssql")
    eng.move_table("cx", "public", "s", 100, Checkpoint(), [].append)
    assert _ms("cx", "select id, g.STSrid, g.STAsText() from dbo.s"
                     " order by id") == [
        (1, 4326, "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"),
        (2, 4326, "POINT (100.5 13.75)"), (3, None, None)]
    # the small square it was, not the rest of the globe
    area = _ms("cx", "select g.STArea() from dbo.s where id = 1")[0][0]
    assert 1e12 < area < 2e12, area
    got = _verdict(eng, "s")
    assert got.status == "ok", got.detail


def test_a_polygon_drawn_clockwise_is_refused_by_a_geography(servers,
                                                              tmp_path):
    eng = _pair(tmp_path, "postgres", "mssql")
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "public", "cw", 100, Checkpoint(), [].append)
    said = str(refused.value)
    assert "cw.g: 1 row holds a polygon whose outer ring runs clockwise" \
        in said, said
    assert _ms("cx", "select count(*) from sys.tables where name = 'cw'") \
        == [(0,)]


def test_xml_into_a_typed_column_is_refused_and_its_collection_read(
        servers, tmp_path):
    from migkit.engines.mssql import MSSQLEngine
    ms = MSSQLEngine(Hop(name="ms", engine="mssql", source=_ep("mssql"),
                         target=_ep("mssql"), databases=["cx"]))
    assert ms.neutral_columns("src", "cx", "dbo.x") == [
        ("id", "int"), ("d", "xml(dbo.nums)")]
    eng = _pair(tmp_path, "postgres", "mssql")
    with pytest.raises(SystemExit) as refused:
        eng.move_table("cx", "public", "x", 100, Checkpoint(), [].append)
    said = str(refused.value)
    assert "against its schema collection dbo.nums" in said, said
    assert _ms("cx", "select count(*) from dbo.x") == [(0,)]
