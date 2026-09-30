"""SQL Server's `geography` and `geometry` read as the shapes they are, the
longitude first, for a move to another engine and for its check (type-
fidelity section 8.6).

Before: both had no class across engines - named and never compared,
and a table holding one could not be built on a target - and the driver
hands a value over in SQL Server's own serialisation, which no other
engine reads. Now a shape is read through the rendering the row hash
already reads it by (its SRID and its well-known binary with Z and M)
and is the same text as PostGIS's or MySQL's reading of the same shape.

SQL Server 2022 itself: SQL Edge, the build that runs on arm64, has no
spatial types, so this runs in the x86 VM (`with_docker_lock.py --vm
migkit`), and skips, saying so, only where the engine cannot start.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-ty2-ms22", 16219, "CHANGE_ME-Str0ng!"
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
                except Exception:  # noqa: BLE001 - not up yet
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
        _sql("master", "create database cx")
        _sql("cx", "create table dbo.s (id int primary key, g geography,"
                   " m geometry)",
             "insert into dbo.s values"
             " (1, geography::Point(13.75, 100.5, 4326),"
             " geometry::STGeomFromText('LINESTRING(0 0, 1 1)', 0)),"
             " (2, geography::Parse('POINT(100.5 13.75 10 2)'),"
             " geometry::STGeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2,"
             " 0 0))', 3857)),"
             " (3, null, null)")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MS], capture_output=True)


def _engine():
    from migkit.engines.mssql import MSSQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    return MSSQLEngine(Hop(name="ms22", engine="hetero", source=ep,
                           target=ep, databases=["cx"],
                           options={"source_engine": "mssql",
                                    "target_engine": "postgres"}))


def test_a_shape_is_read_as_its_srid_and_longitude_first_binary(server):
    from migkit import canon, geo
    eng = _engine()
    cols = [("id", "integer"), ("g", "geometry"), ("m", "geometry")]
    assert [canon.type_class("mssql", t) for _, t in
            eng.neutral_columns("src", "cx", "dbo.s")] \
        == ["integer", "geometry", "geometry"]
    rows, _ = eng.neutral_read("src", "cx", "dbo.s", cols)
    got = {r[0]: r for r in rows}
    # geography::Point takes the latitude first; the shape is longitude
    # first, as PostGIS and GeoJSON have it
    assert geo.to_geojson(got[1][1]) == {"type": "Point",
                                         "coordinates": [100.5, 13.75]}
    assert canon.render_value("geometry", got[1][1]) == geo.text(
        "0101000020E610000000000000002059400000000000802B40")
    assert canon.render_value("geometry", got[1][2]) == geo.text(
        geo.from_geojson({"type": "LineString",
                          "coordinates": [[0, 0], [1, 1]]}, 0))
    # Z and M kept, and an SRID other than 4326
    assert geo.of(got[2][1])[2:4] == (True, True)
    assert geo.of(got[2][2]).srid == 3857
    assert got[3][1] is None and got[3][2] is None
    # and the same rows digest as they render
    n, _ = eng.neutral_digest("src", "cx", "dbo.s", cols)
    assert n == 3
