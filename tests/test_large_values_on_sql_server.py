"""Large values on SQL Server (backlog P0-PARITY, `migkit.pieces`): a
`varbinary(max)` of 128 MB - twice what one batch holds - and an
`nvarchar(max)` of 16 MB, moved by migkit held to the memory its model says
(`large_values_rig`): a byte value past a piece is read by `substring` of
it and every `(max)` value past a piece written through `.write`, a piece
a statement - the driver writes a parameter into the statement's own text,
a byte value as hex twice its size - and a wide byte value is hashed where
it lies (`hashbytes`) when a digest is folded. Never killed, checked equal,
the same on both sides as the server hashes them, and refused before it
starts where the memory is not there.

SQL Edge on an arm64 machine, SQL Server 2022 in the x86 workflow
(`conftest.MSSQL_IMAGE`); through the docker lock, `--vm migkit` for 2022.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import engine_named
from tests import large_values_rig as rig
from tests.cap_pairs import remove
from tests.conftest import mssql_run, needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.large-values", "mssql"), ("move.sized", "mssql")]

pytestmark = needs_docker

MS, PORT, PW = "migkit-test-lv-mssql", 16378, "CHANGE_ME-lv1a"


def _sql(db, sql):
    import pymssql
    conn = pymssql.connect(server="127.0.0.1", port=str(PORT), user="sa",
                           password=PW, database=db, login_timeout=10,
                           autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()] if cur.description \
            else None
    finally:
        conn.close()


@pytest.fixture
def server():
    rig.network()
    cmd = mssql_run(MS, PORT, PW, memory_mb=2048)
    # on the shared network, where the runner reaches it by its name
    cmd[3:3] = ["--network", rig.NETWORK]
    remove(MS)
    import subprocess
    got = subprocess.run(cmd, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr[-2000:]
    try:
        rig.until(lambda: _sql("master", "select 1") == [(1,)],
                  "SQL Server", seconds=600)
        yield
    finally:
        remove(MS)


#: 1 MiB of random bytes made in the server, doubled to 128 MiB: the value
#: never crosses from here
MAKE = """
declare @s varbinary(max) = 0x, @i int = 0;
while @i < 131 begin set @s = @s + crypt_gen_random(8000); set @i += 1 end;
set @s = @s + crypt_gen_random(576);
while datalength(@s) < 134217728 set @s = @s + @s;
insert into dbo.big values
  (1, @s + 0x01, replicate(cast(N'é' as nvarchar(max)), 8388608)),
  (2, 0x02 + @s, N'short'),
  (3, null, null);
"""


def _hashes(side):
    """[(id, bytes, md5 of the bytes, bytes of the text, md5 of it)] of
    one side, hashed by the server."""
    db = "lvsrc" if side == "src" else "lvdst"
    try:
        return _sql(db, "select id, datalength(b), convert(varchar(32),"
                        " hashbytes('MD5', b), 2), datalength(t),"
                        " convert(varchar(32), hashbytes('MD5', t), 2)"
                        " from dbo.big order by id")
    except Exception:  # noqa: BLE001 - not there: nothing copied
        return []


def test_sql_server_values_past_a_piece_are_read_and_written_in_pieces(
        server):
    for db in ("lvsrc", "lvdst"):
        _sql("master", f"if db_id('{db}') is not null drop database {db};"
                       f" create database {db}")
    _sql("lvsrc", "create table dbo.big (id int primary key,"
                  " b varbinary(max), t nvarchar(max))")
    _sql("lvsrc", MAKE)
    _sql("lvsrc", "insert into dbo.big select 100 + n, cast(n as"
                  " varbinary(8)), N't' from (select top 1000"
                  " row_number() over (order by (select 0)) as n from"
                  " sys.all_objects a cross join sys.all_objects b) x")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    eng = engine_named("mssql", Hop(name=rig.HOP, engine="mssql", source=ep,
                                    target=ep, databases=["lvsrc"],
                                    db_map={"lvsrc": "lvdst"}))
    side = f"{{host: {MS}, port: 1433, user: sa, password: {PW}}}"
    hops = (f"hops:\n  {rig.HOP}:\n    engine: mssql\n"
            f"    source: {side}\n    target: {side}\n"
            "    databases: [lvsrc]\n    db_map: {lvsrc: lvdst}\n")

    def empty():
        _sql("lvdst", "if object_id('dbo.big') is not null drop table"
                      " dbo.big")
    rig.held_to_its_memory(rig.work_dir(), hops, eng, "lvsrc",
                           rig.runner_image("pymssql"), empty, _hashes)
