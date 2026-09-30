"""An edition change on SQL Server (backlog P0-SCENARIOS, the owner's "SQL
Server Web edition to Datacenter (Enterprise)"): SQL Server 2022 Developer
into Express, on the x86 VM (`tools/with_docker_lock.py --vm migkit`).

Each side's edition read from its own server; a database the source holds
with transparent data encryption - a feature SQL Server itself records it
as holding (`sys.dm_db_persisted_sku_features`), and Express has not -
refused before anything is written, the move said as the licence-driven
one it is, and the refusal lifted where the hop accepts the loss; SQL
Server's own copy-only backup refused as the way for it, since the
restored database would not open there, and taken for a database that
holds nothing Express lacks: proved by its checksums, restored, its rows
the source's and its change tracking version the source's at the backup.
"""
import subprocess
import time

import pytest

from tests.conftest import MSSQL_IMAGE, needs_docker

pytestmark = [needs_docker, pytest.mark.skipif(
    "azure-sql-edge" in MSSQL_IMAGE,
    reason="the editions are SQL Server 2022's: the x86 VM runs them")]

SRC = ("migkit-test-edition-src", 16408, "Developer")
DST = ("migkit-test-edition-dst", 16409, "Express")
VOLUME = "migkit-test-edition-backup"
BACKUP = "/var/opt/mssql/backup"
PASSWORD = "CHANGE_ME-Ed1tion!x"


def _conn(port, db="master"):
    import pymssql
    return pymssql.connect(server="127.0.0.1", port=str(port), user="sa",
                           password=PASSWORD, database=db, autocommit=True,
                           login_timeout=15)


def run(port, sql, db="master"):
    conn = _conn(port, db)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        try:
            return cur.fetchall()
        except Exception:
            return []
    finally:
        conn.close()


@pytest.fixture(scope="module")
def servers():
    for name, _, _ in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "volume", "rm", "-f", VOLUME],
                   capture_output=True)
    for name, port, pid in (SRC, DST):
        # as root: both write and read the backup directory they share
        subprocess.run(["docker", "run", "-d", "--name", name, "--platform",
                        "linux/amd64", "--user", "root", "-p",
                        f"127.0.0.1:{port}:1433", "-e", "ACCEPT_EULA=Y",
                        "-e", f"MSSQL_SA_PASSWORD={PASSWORD}", "-e",
                        f"MSSQL_PID={pid}", "-e",
                        "MSSQL_MEMORY_LIMIT_MB=1024", "-v",
                        f"{VOLUME}:{BACKUP}", MSSQL_IMAGE], check=True,
                       capture_output=True)
    try:
        for _, port, _ in (SRC, DST):
            for _ in range(120):
                try:
                    run(port, "select 1")
                    break
                except Exception:
                    time.sleep(3)
        s = SRC[1]
        run(s, "create database app")
        run(s, "create table dbo.t (id int primary key, v nvarchar(20));"
               " insert into dbo.t values (1, N'a'), (2, N'b')", "app")
        run(s, "create master key encryption by password ="
               " 'CHANGE_ME-Mk1!x'; create certificate tde with subject ="
               " 'tde'")
        run(s, "create database encryption key with algorithm = aes_256"
               " encryption by server certificate tde", "app")
        run(s, "alter database app set encryption on")
        for _ in range(60):
            got = run(s, "select encryption_state from"
                         " sys.dm_database_encryption_keys where"
                         " database_id = db_id('app')")
            if got and int(got[0][0]) == 3:
                break
            time.sleep(1)
        run(s, "create database shop")
        run(s, "create table dbo.t (id int primary key, v nvarchar(20));"
               " insert into dbo.t values (1, N'a'), (2, N'b'), (3, N'c')",
            "shop")
        run(s, "alter database shop set change_tracking = on"
               " (change_retention = 2 days, auto_cleanup = on)")
        run(s, "alter table dbo.t enable change_tracking", "shop")
        # a change the tracking holds: its version is where a tail goes on
        run(s, "insert into dbo.t values (4, N'd')", "shop")
        yield
    finally:
        for name, _, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "volume", "rm", "-f", VOLUME],
                       capture_output=True)


def _hop(tmp_path, dbs, options=None, name="edition"):
    from migkit.config import Endpoint, Hop
    hop = Hop(name=name, engine="mssql", goal="cutover",
              source=Endpoint(host="127.0.0.1", port=SRC[1], user="sa",
                              password=PASSWORD),
              target=Endpoint(host="127.0.0.1", port=DST[1], user="sa",
                              password=PASSWORD),
              databases=list(dbs), options=options or {})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _eng(hop):
    from migkit.engines.mssql import MSSQLEngine
    return MSSQLEngine(hop)


def test_what_express_lacks_of_the_source_is_refused_first(servers,
                                                           tmp_path):
    from migkit import case, editions, physical
    hop = _hop(tmp_path, ["app"])
    src, dst = editions.pair(_eng(hop))
    assert (src.name, dst.name) == ("developer", "express"), (src, dst)
    by = {r["item"]: r for r in case.checks(hop, _eng(hop), ["app"],
                                            quick=True)}
    named = by["what the two servers are"]["detail"]
    assert "an edition down: SQL Server Developer to SQL Server Express" \
        in named, named
    assert "licence-driven" in named
    tde = by["transparent data encryption"]
    assert tde["level"] == "fail", tde
    assert "lost on the target" in tde["detail"]
    assert by["the edition's size"]["level"] == "pass"
    # the loss accepted by the hop: said, and not refused
    ok = _hop(tmp_path, ["app"], {"edition": {
        "accept": ["TransparentDataEncryption"]}})
    by = {r["item"]: r for r in case.checks(ok, _eng(ok), ["app"])}
    assert by["transparent data encryption"]["level"] == "warn"
    # the database's own backup would not open on Express: not the way
    phys = _hop(tmp_path, ["app"], {"physical": {"backup": BACKUP}})
    rung, why = physical.choose(phys, _eng(phys), "full")
    assert rung is None
    assert "does not have what the source's databases hold" in why, why


def test_a_database_express_can_hold_is_restored_from_its_own_backup(
        servers, tmp_path):
    from migkit import physical
    hop = _hop(tmp_path, ["shop"], {"physical": {"backup": BACKUP}})
    rung, why = physical.choose(hop, _eng(hop), "full")
    assert rung is not None and rung.name == "physical-backup", why
    said = []
    assert physical.carry(hop, _eng(hop), "shop", "full", True, said.append)
    assert run(DST[1], "select count(*) from dbo.t", "shop")[0][0] == 4
    got = run(DST[1], "select v from dbo.t where id = 4", "shop")[0][0]
    assert got == "d"
    rec = physical.seeded(hop)
    assert rec["rung"] == "physical-backup"
    version = rec["databases"]["shop"]["version"]
    source_now = run(SRC[1], "select change_tracking_current_version()",
                     "shop")[0][0]
    assert int(source_now) >= 1
    assert version is not None and int(version) == int(source_now)
    # where the tail goes on from: the version the copy holds
    kept = (tmp_path / "copy-position.json").read_text()
    assert f'"before": {int(version)}' in kept, kept
    # copy-only: the source's own chain of backups is as it was
    only = run(SRC[1], "select top 1 is_copy_only from msdb.dbo.backupset"
                       " where database_name = 'shop' order by"
                       " backup_finish_date desc")[0][0]
    assert bool(only)
    # another hop finds the database there already: refused, never replaced
    other = _hop(tmp_path / "other", ["shop"], {"physical": {
        "backup": BACKUP}}, name="edition-other")
    (tmp_path / "other").mkdir()
    rung, why = physical.choose(other, _eng(other), "full")
    assert rung is None and "already holds shop" in why, why
