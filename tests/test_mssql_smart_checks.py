"""Smart checks for the SQL Server engine. IDENTITY collision: SQL Server does
NOT auto-advance the seed after an IDENTITY_INSERT load, so a migration that
copies identity values without a follow-up DBCC CHECKIDENT RESEED leaves the
seed behind max(col) - the first normal insert then duplicate-keys. no-PK: a
table without a pk/unique loses its updates/deletes under CDC. Both are the SQL
Server versions of the checks the other engines already have.

Gated: SQL Server's image is amd64-only, so on Apple Silicon it runs under
emulation (slow). Skips cleanly when docker or the image is absent."""
import shutil
import socket
import subprocess
import time

import pytest


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _have_image():
    r = subprocess.run(["docker", "image", "ls", "--format", "{{.Repository}}"],
                       capture_output=True, text=True)
    return "mcr.microsoft.com/mssql/server" in r.stdout


pytestmark = [
    pytest.mark.skipif(not _docker(), reason="docker not available"),
    pytest.mark.skipif(not shutil.which("sqlcmd"), reason="sqlcmd not installed"),
    pytest.mark.skipif(not _have_image(),
                       reason="mssql image not pulled (amd64, ~1.5GB)"),
]

SRC, DST = "migkit-test-mssql-src", "migkit-test-mssql-dst"
SRC_PORT, DST_PORT = 14433, 14434
SA = "Str0ng!Passw0rd"


def _sqlcmd(port, sql, db="master"):
    return subprocess.run(
        ["sqlcmd", "-S", f"127.0.0.1,{port}", "-U", "sa", "-P", SA,
         "-d", db, "-C", "-b", "-Q", sql], capture_output=True, text=True)


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                if _sqlcmd(port, "select 1").returncode == 0:
                    return True
        time.sleep(3)
    return False


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "--platform",
                        "linux/amd64", "-e", "ACCEPT_EULA=Y", "-e",
                        f"MSSQL_SA_PASSWORD={SA}", "-p", f"{p}:1433",
                        "mcr.microsoft.com/mssql/server:2022-latest"],
                       check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        if not _wait(p):
            pytest.skip("SQL Server did not come up (emulation too slow)")
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _engine():
    from migkit.config import Endpoint, Hop
    from migkit.engines.mssql import MSSQLEngine
    hop = Hop(name="s", engine="mssql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="sa",
                              password=SA),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="sa",
                              password=SA),
              databases=["shop"])
    return MSSQLEngine(hop)


def test_mssql_identity_collision_and_nopk(pair):
    seed = ("create database shop;")
    _sqlcmd(SRC_PORT, seed)
    _sqlcmd(DST_PORT, seed)
    tbls = ("create table orders(id int identity primary key, v int);"
            " insert into orders(v) values (1),(2),(3),(4),(5);"
            " create table events(a int, b int);")
    _sqlcmd(SRC_PORT, tbls, "shop")
    _sqlcmd(DST_PORT, tbls, "shop")
    # target: seed knocked behind max(id) (the post-load-without-reseed trap)
    _sqlcmd(DST_PORT, "dbcc checkident('orders', reseed, 1)", "shop")

    eng = _engine()
    ai = eng.check_autoinc("shop")
    usable = [r for r in ai if r.scope.endswith("usable")]
    assert usable and usable[0].status == "diff", [r.__dict__ for r in ai]
    assert "orders" in usable[0].detail and "collide" in usable[0].detail.lower()

    deep = eng.check_deep("shop")
    keys = [r for r in deep if r.scope.endswith("keys")]
    assert keys and keys[0].status == "diff", [r.__dict__ for r in deep]
    assert "events" in keys[0].detail
