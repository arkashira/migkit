"""Large values on SAP ASE (backlog P0-PARITY, `migkit.pieces`): image and
text values of 12 MB, past a piece, moved from one database into another by
migkit held to the memory its model says (`large_values_rig`). ASE hands a
session 32,768 bytes of a text or image value unless the session says
otherwise, and cuts the rest off without a word: a copy and a check read
through that cut would each see the same first 32 KB and agree. Every
session migkit opens takes the whole value, which is what arrives, sized by
the server (`datalength`) and read in batches cut by bytes. Never killed,
checked equal, the same on both sides as read here, and refused before it
starts where the memory is not there.

JetBrains' ASE image runs on x86-64 only: through the docker lock with
`--vm migkit` here, natively in the x86 workflow.
"""
import os

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import engine_named
from tests import large_values_rig as rig
from tests.cap_pairs import remove
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.large-values", "ase"), ("move.sized", "ase")]

pytestmark = needs_docker

ASE, PORT, PW = "migkit-test-lv-ase", 16378, "myPassword"
IMAGE = "datagrip/sybase:16.0"
#: a value past a piece
SIZE = 12 * rig.MB


def _sql(db, statement, args=()):
    import pyodbc

    from migkit.engines.ase import DRIVERS
    driver = next((p for p in DRIVERS if os.path.exists(p)), None)
    if driver is None:
        pytest.skip("FreeTDS's ODBC driver is not installed")
    conn = pyodbc.connect(f"DRIVER={{{driver}}};SERVER=127.0.0.1;"
                          f"PORT={PORT};TDS_Version=5.0;UID=sa;PWD={PW};"
                          f"DATABASE={db};ClientCharset=UTF-8",
                          timeout=10, autocommit=True)
    try:
        cur = conn.cursor()
        # read here whole, as migkit's sessions read it
        cur.execute("set textsize 2147483647")
        cur.execute(statement, *args)
        return [tuple(r) for r in cur.fetchall()] if cur.description \
            else None
    finally:
        conn.close()


@pytest.fixture(scope="module")
def server():
    rig.network()
    rig.start(ASE, PORT, 5000, IMAGE, extra=["-t", "--platform",
                                             "linux/amd64"])
    try:
        rig.until(lambda: _sql("master", "select 1") == [(1,)], "ASE",
                  seconds=900)
        if not _sql("master", "select name from sysdevices where name ="
                              " 'migkit_lv'"):
            _sql("master", "disk init name = 'migkit_lv', physname ="
                           " '/tmp/migkit_lv.dat', size = '400M'")
        for db in ("lvsrc", "lvdst"):
            if _sql("master", f"select db_id('{db}')")[0][0] is None:
                _sql("master", f"create database {db} on migkit_lv ="
                               " '150M'")
        yield
    finally:
        remove(ASE)


def _values(side):
    """[(id, bytes of the image, its md5, of the text, its md5)] of one
    side, read here whole."""
    db = "lvsrc" if side == "src" else "lvdst"
    try:
        rows = _sql(db, "select id, b, t from dbo.big order by id")
    except Exception:  # noqa: BLE001 - not there: nothing copied
        return []
    return [(i, None if b is None else len(b),
             None if b is None else rig.md5_of([b]),
             None if t is None else len(t),
             None if t is None else rig.md5_of([t])) for i, b, t in rows]


def test_ase_text_and_image_values_arrive_whole(server):
    for db in ("lvsrc", "lvdst"):
        _sql(db, "if object_id('dbo.big') is not null drop table dbo.big")
        _sql(db, "create table dbo.big (id int not null primary key,"
                 " b image null, t text null)")
    for i in range(3):
        _sql("lvsrc", "insert into dbo.big values (?, ?, ?)",
             (i, os.urandom(SIZE), "x" * SIZE))
    for i in range(200):
        _sql("lvsrc", "insert into dbo.big values (?, ?, ?)",
             (100 + i, b"v%d" % i, f"t{i}"))
    got = _values("src")
    # the values went in whole: what the test reads back is 12 MB, not
    # the 32 KB a session is handed by default
    assert got[0][1] == SIZE and got[0][3] == SIZE, got[0]
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    eng = engine_named("ase", Hop(name=rig.HOP, engine="ase", source=ep,
                                  target=ep, databases=["lvsrc"],
                                  db_map={"lvsrc": "lvdst"}))
    side = f"{{host: {ASE}, port: 5000, user: sa, password: {PW}}}"
    hops = (f"hops:\n  {rig.HOP}:\n    engine: ase\n"
            f"    source: {side}\n    target: {side}\n"
            "    databases: [lvsrc]\n    db_map: {lvsrc: lvdst}\n")
    rig.held_to_its_memory(
        rig.work_dir(), hops, eng, "lvsrc",
        rig.runner_image("pyodbc", apt=("unixodbc", "tdsodbc")),
        lambda: _sql("lvdst", "delete from dbo.big"), _values)
