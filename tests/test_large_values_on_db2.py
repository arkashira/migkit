"""Large values on Db2 (backlog P0-PARITY, `migkit.pieces`): a BLOB of
128 MB - twice what one batch holds - and a CLOB of 16 MB, moved from one
schema into another by migkit held to the memory its model says
(`large_values_rig`): sized by the server off each LOB's descriptor, a
byte value past a piece read by `SUBSTR` of it a piece at a time, a wide
value hashed as its pieces pass when a digest is folded. Never killed,
checked equal, the same on both sides as read here, and refused before it
starts where the memory is not there.

IBM's Db2 Community image runs on x86-64 only: through the docker lock
with `--vm migkit` here, natively in the x86 workflow. The LOB columns are
not logged, as a LOB this size is loaded: the image's logs hold far less.
"""
import os
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import engine_named
from tests import large_values_rig as rig
from tests.cap_pairs import remove
from tests.conftest import needs_docker

pytestmark = needs_docker

DB2, PORT, PW = "migkit-test-lv-db2", 16379, "CHANGE_ME-db2"
IMAGE = "icr.io/db2_community/db2:latest"


def _connect():
    import ibm_db_dbi
    return ibm_db_dbi.connect(f"DATABASE=TESTDB;HOSTNAME=127.0.0.1;"
                              f"PORT={PORT};PROTOCOL=TCPIP;UID=db2inst1;"
                              f"PWD={PW};CONNECTTIMEOUT=10;", "", "")


def _sql(statement, args=()):
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(statement, tuple(args))
        out = [tuple(r) for r in cur.fetchall()] if cur.description \
            else None
        conn.commit()
        return out
    finally:
        conn.close()


@pytest.fixture(scope="module")
def server():
    rig.network()
    remove(DB2)
    got = subprocess.run(
        ["docker", "run", "-d", "--name", DB2, "--network", rig.NETWORK,
         "--platform", "linux/amd64", "--privileged", "-p",
         f"127.0.0.1:{PORT}:50000", "-e", "LICENSE=accept", "-h",
         "db2server", "-e", "DB2INSTANCE=db2inst1", "-e",
         f"DB2INST1_PASSWORD={PW}", "-e", "DBNAME=testdb", "-e",
         "TO_CREATE_SAMPLEDB=false", "-e", "REPODB=false", "-e",
         "ARCHIVE_LOGS=false", "-e", "AUTOCONFIG=false", IMAGE],
        capture_output=True, text=True)
    assert got.returncode == 0, got.stderr[-2000:]
    try:
        end = time.time() + 2400
        while time.time() < end:
            said = subprocess.run(["docker", "logs", DB2],
                                  capture_output=True, text=True)
            if "Setup has completed" in said.stdout + said.stderr:
                break
            time.sleep(10)
        rig.until(lambda: _sql("values 1") == [(1,)], "Db2", seconds=600)
        yield
    finally:
        remove(DB2)


def _values(side):
    """[(id, bytes, md5 of the BLOB, md5 of the CLOB)] of one side, read
    here."""
    schema = "lvsrc" if side == "src" else "lvdst"
    try:
        rows = _sql(f"select id, b, t from {schema}.big order by id")
    except Exception:  # noqa: BLE001 - not there: nothing copied
        return []
    return [(i, None if b is None else len(b),
             None if b is None else rig.md5_of([b]),
             None if t is None else rig.md5_of([t])) for i, b, t in rows]


def test_db2_lobs_past_a_piece_are_read_in_pieces(server):
    for schema in ("lvsrc", "lvdst"):
        try:
            _sql(f"drop table {schema}.big")
        except Exception:  # noqa: BLE001 - not there yet
            pass
        _sql(f"create table {schema}.big (id integer not null primary key,"
             " b blob(256M) not logged, t clob(64M) not logged)")
    for i in range(2):
        _sql("insert into lvsrc.big values (?, ?, ?)",
             (i, os.urandom(rig.WIDE), "x" * (16 * rig.MB)))
    for i in range(200):
        _sql("insert into lvsrc.big values (?, ?, ?)",
             (100 + i, b"v%d" % i, f"t{i}"))
    ep = Endpoint(host="127.0.0.1", port=PORT, user="db2inst1", password=PW,
                  options={"database": "TESTDB"})
    eng = engine_named("db2", Hop(name=rig.HOP, engine="db2", source=ep,
                                  target=ep, databases=["lvsrc"],
                                  db_map={"lvsrc": "lvdst"}))
    side = (f"{{host: {DB2}, port: 50000, user: db2inst1, password: {PW},"
            " database: TESTDB}")
    hops = (f"hops:\n  {rig.HOP}:\n    engine: db2\n"
            f"    source: {side}\n    target: {side}\n"
            "    databases: [lvsrc]\n    db_map: {lvsrc: lvdst}\n")
    rig.held_to_its_memory(rig.work_dir(), hops, eng, "lvsrc",
                           rig.runner_image("ibm_db", apt=("libxml2",)),
                           lambda: _sql("delete from lvdst.big"), _values)
