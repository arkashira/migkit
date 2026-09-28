"""SQL Server tables holding `geography`, `geometry` or `hierarchyid` are
compared, not reported as an error.

The row hash was `FOR JSON` over `t.*`, and `FOR JSON` refuses CLR types
outright (Msg 13604): every such table came back "refused a statement",
never compared, whatever it held. Each is now rendered through its own
canonical form inside the hash - a shape as its SRID and its well-known
binary with Z and M, a hierarchy node as its path - so equal tables pass and
a point moved by a billionth of a degree, a changed SRID, elevation or node
are each found.

SQL Server 2022 itself: SQL Edge, the build that runs on arm64, has no
spatial types and no hierarchyid. x86-only, so on Apple Silicon it runs
under Rosetta in the separate VM (`with_docker_lock.py --vm migkit`); it
skips, saying so, only where the engine cannot start at all.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-f0v-mssql22", 16055, "CHANGE_ME-Str0ng!"
IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"


def _sql(db, *statements):
    import pymssql
    conn = pymssql.connect(server="127.0.0.1", port=str(PORT), user="sa",
                           password=PW, database=db, autocommit=True,
                           tds_version="7.4", login_timeout=10)
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = cur.fetchall() if cur.description else None
        return out
    finally:
        conn.close()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)
    run = subprocess.run(
        ["docker", "run", "-d", "--name", MS, "--platform", "linux/amd64",
         "-p", f"127.0.0.1:{PORT}:1433", "-e", "ACCEPT_EULA=Y", "-e",
         f"MSSQL_SA_PASSWORD={PW}", "-e", "MSSQL_MEMORY_LIMIT_MB=2048",
         "-e", "MSSQL_TELEMETRY_ENABLED=false", IMAGE],
        capture_output=True, text=True)
    if run.returncode:
        pytest.skip("SQL Server 2022 cannot run on this machine:"
                    f" {run.stderr.strip()[-300:]}")
    try:
        end = time.time() + 420
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(1)
                up = s.connect_ex(("127.0.0.1", PORT)) == 0
            if up:
                try:
                    _sql("master", "select 1")
                    break
                except Exception:
                    pass
            state = subprocess.run(["docker", "inspect", "-f",
                                    "{{.State.Running}}", MS],
                                   capture_output=True, text=True)
            if state.stdout.strip() == "false":
                logs = subprocess.run(["docker", "logs", "--tail", "5", MS],
                                      capture_output=True, text=True)
                pytest.skip("SQL Server 2022 cannot run on this machine:"
                            f" {(logs.stdout + logs.stderr)[-300:]}")
            time.sleep(3)
        else:
            pytest.skip("SQL Server 2022 did not answer within 7 minutes"
                        " on this machine")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)


TABLE = ("create table dbo.s (id int primary key, g geography,"
         " m geometry, h hierarchyid)")
ROWS = [
    "(1, geography::STGeomFromText('POINT(100.5 13.75)', 4326),"
    " geometry::STGeomFromText('LINESTRING(0 0, 1 1)', 0),"
    " hierarchyid::Parse('/1/2/'))",
    "(2, geography::Parse('POINT(100.5 13.75 10 2)'),"
    " geometry::STGeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))', 3857),"
    " hierarchyid::Parse('/1/'))",
    "(3, null, null, null)",
]


@pytest.fixture
def eng(server, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for db in ("shop", "shop_new"):
        _sql("master", f"if db_id('{db}') is not null begin alter database"
                       f" {db} set single_user with rollback immediate;"
                       f" drop database {db} end", f"create database {db}")
        _sql(db, TABLE, "insert into dbo.s values " + ", ".join(ROWS))
    from migkit.engines.mssql import MSSQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    return MSSQLEngine(Hop(name="f0v", engine="mssql", source=ep, target=ep,
                           databases=["shop"], db_map={"shop": "shop_new"}))


def _data(eng):
    got = [r for r in eng.check_data("shop") if r.check == "data"]
    assert len(got) == 1, got
    return got[0]


def test_the_server_refuses_json_over_these_types(eng):
    """Why the hash had to change: the old rendering, on this server."""
    with pytest.raises(Exception) as e:
        _sql("shop", "select (select t.* for json path) from dbo.s t")
    assert "13604" in str(e.value), e.value


def test_equal_tables_of_shapes_and_hierarchies_are_ok(eng):
    r = _data(eng)
    assert r.status == "ok", r.detail


@pytest.mark.parametrize("change", [
    # a point moved by a billionth of a degree
    "update dbo.s set g = geography::STGeomFromText("
    "'POINT(100.500000001 13.75)', 4326) where id = 1",
    # the same shape in another reference system
    "update dbo.s set m = geometry::STGeomFromText("
    "'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))', 0) where id = 2",
    # another elevation, the rest alike
    "update dbo.s set g = geography::Parse('POINT(100.5 13.75 11 2)')"
    " where id = 2",
    # another node
    "update dbo.s set h = hierarchyid::Parse('/1/3/') where id = 1",
    # a value where the source has none
    "update dbo.s set h = hierarchyid::Parse('/9/') where id = 3",
])
def test_a_changed_shape_or_node_is_found(eng, change):
    _sql("shop_new", change)
    r = _data(eng)
    assert r.status == "diff", r.detail
    assert "changed=1" in r.detail, r.detail
