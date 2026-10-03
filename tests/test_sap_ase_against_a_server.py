"""SAP ASE against a server: one side of a pair, then the other.

SAP publishes no ASE image; JetBrains' (`datagrip/sybase`, ASE 16.0, for
their own tests) runs on x86-64 only - here under Rosetta
(`tools/with_docker_lock.py --vm migkit`), in the x86 workflow natively.
Reached through FreeTDS's ODBC driver at protocol 5.0. A table is copied
from one database into another by the table copier, the target's table
made by migkit; what arrives is held to the source by the in-process
digest, and the same comparison says so when one value on the target
differs.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.table-copy", "ase")]

pytestmark = needs_docker

ASE, PORT, PW = "migkit-test-ms-ase", 16133, "myPassword"
IMAGE = "datagrip/sybase:16.0"


def _sql(db, *statements):
    import os

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
        out = None
        for s in statements:
            cur.execute(s)
            out = [tuple(r) for r in cur.fetchall()] if cur.description \
                else None
        return out
    finally:
        conn.close()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", ASE], capture_output=True)
    subprocess.run(["docker", "run", "-d", "-t", "--name", ASE, "--platform",
                    "linux/amd64", "-p", f"127.0.0.1:{PORT}:5000", IMAGE],
                   check=True, capture_output=True)
    try:
        # it answers the port before it answers a query, and now and then
        # not at all on its first start
        end = time.time() + 900
        while time.time() < end:
            try:
                _sql("master", "select 1")
                break
            except pytest.skip.Exception:
                raise
            except Exception:
                time.sleep(5)
        else:
            pytest.fail("ASE never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", ASE], capture_output=True)


@pytest.fixture
def pair(server, tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    # the image's master device has no room for another database: a device
    # of the test's own to make them on
    if not _sql("master", "select name from sysdevices where name ="
                          " 'migkit_dev'"):
        _sql("master", "disk init name = 'migkit_dev', physname ="
                       " '/tmp/migkit_dev.dat', size = '200M'")
    for db in ("shop", "shop_new"):
        if _sql("master", f"select db_id('{db}')")[0][0] is None:
            _sql("master", f"create database {db} on migkit_dev = '48M'")
        _sql(db, "if object_id('dbo.orders') is not null drop table"
                 " dbo.orders")
    _sql("shop", "create table dbo.orders (id int not null primary key,"
                 " code char(5) null, amount numeric(10,2) null,"
                 " made datetime null, note varchar(40) null,"
                 " raw varbinary(8) null)",
         "insert into dbo.orders values (1, 'ab', 12.50,"
         " '2024-02-29 13:14:15.123', 'first', 0x00ff)",
         "insert into dbo.orders values (2, '', 0.00, '1900-01-01', '',"
         " 0x00)",
         "insert into dbo.orders values (3, null, null, null, null, null)")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="sa", password=PW)
    eng = HeteroEngine(Hop(name="ase", engine="hetero",
                           options={"source_engine": "ase",
                                    "target_engine": "ase"},
                           source=ep, target=ep, databases=["shop"],
                           db_map={"shop": "shop_new"}))
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng


class _Kept(dict):
    def save(self):
        pass


def test_a_table_copied_between_databases_is_the_same_and_a_change_is_not(
        pair):
    said = []
    pair.move_table("shop", "dbo", "orders", 1000, _Kept(), said.append)
    q = ("select id, rtrim(code), amount, note, raw from dbo.orders"
         " order by id")
    assert _sql("shop_new", q) == _sql("shop", q), said
    got = [(r.status, r.detail) for r in pair.check_data("shop")
           if r.check == "data"]
    assert got and all(s == "ok" for s, _ in got), got
    _sql("shop_new", "update dbo.orders set amount = 12.51 where id = 1")
    got = [r.status for r in pair.check_data("shop") if r.check == "data"]
    assert "diff" in got, got
