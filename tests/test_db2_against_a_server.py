"""Db2 against a server: one side of a pair, then the other.

IBM's Db2 Community image runs on x86-64 only. It runs here under Rosetta
(`tools/with_docker_lock.py --vm migkit`) and in the x86 workflow natively;
its first start builds the instance and the database, which takes minutes.
A table is copied from one schema into another by the table copier, the
target's table made by migkit; what arrives is held to the source by the
in-process digest, and the same comparison says so when one value on the
target differs.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

DB2, PORT, PW = "migkit-test-ms-db2", 16132, "CHANGE_ME-db2"
IMAGE = "icr.io/db2_community/db2:latest"


def _connect():
    import ibm_db_dbi
    return ibm_db_dbi.connect(f"DATABASE=TESTDB;HOSTNAME=127.0.0.1;"
                              f"PORT={PORT};PROTOCOL=TCPIP;UID=db2inst1;"
                              f"PWD={PW};CONNECTTIMEOUT=10;", "", "")


def _sql(*statements):
    conn = _connect()
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = [tuple(r) for r in cur.fetchall()] if cur.description \
                else None
        conn.commit()
        return out
    finally:
        conn.close()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", DB2], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", DB2, "--platform",
                    "linux/amd64", "--privileged", "-p",
                    f"127.0.0.1:{PORT}:50000", "-e", "LICENSE=accept",
                    "-h", "db2server", "-e", "DB2INSTANCE=db2inst1",
                    "-e", f"DB2INST1_PASSWORD={PW}", "-e", "DBNAME=testdb",
                    "-e", "TO_CREATE_SAMPLEDB=false", "-e", "REPODB=false",
                    "-e", "ARCHIVE_LOGS=false", "-e", "AUTOCONFIG=false",
                    IMAGE], check=True, capture_output=True)
    try:
        # the instance and the database are made on the first start; a
        # setup that ends without a server answering has failed, and says
        # why in its log
        end, done = time.time() + 2400, None
        while time.time() < end:
            logs = subprocess.run(["docker", "logs", DB2],
                                  capture_output=True, text=True)
            said = logs.stdout + logs.stderr
            if "Setup has completed" in said:
                done = done or time.time()
                try:
                    _sql("values 1")
                    break
                except Exception:
                    if time.time() - done > 180:
                        made = subprocess.run(
                            ["docker", "exec", DB2, "sh", "-c",
                             "tail -n 5 /tmp/db2icrt.log*"],
                            capture_output=True, text=True).stdout
                        pytest.fail("Db2's setup ended and it does not"
                                    " answer: " + " | ".join(
                                        line for line in said.splitlines()
                                        if "not found" in line
                                        or "not been started" in line)[:400]
                                    + " | instance: " + made[-400:])
            time.sleep(10)
        else:
            pytest.fail("Db2 never finished setting up")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", DB2], capture_output=True)


@pytest.fixture
def pair(server, tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    for schema in ("APP", "APP_NEW"):
        for (t,) in _sql("select tabname from syscat.tables where tabschema"
                         f" = '{schema}' and type = 'T'"):
            _sql(f'drop table "{schema}"."{t.strip()}"')
    _sql("create table app.orders (id integer not null primary key,"
         " code char(5), amount decimal(10,2), made timestamp(6),"
         " note varchar(40), raw varchar(8) for bit data)",
         "insert into app.orders values"
         " (1, 'ab', 12.50, '2024-02-29-13.14.15.123456', 'first', x'00ff'),"
         " (2, '', 0.00, '0001-01-01-00.00.00.000000', '', x''),"
         " (3, null, null, null, null, null)")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="db2inst1", password=PW,
                  options={"database": "TESTDB"})
    eng = HeteroEngine(Hop(name="db2", engine="hetero",
                           options={"source_engine": "db2",
                                    "target_engine": "db2"},
                           source=ep, target=ep, databases=["app"],
                           db_map={"app": "app_new"}))
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng


class _Kept(dict):
    def save(self):
        pass


def test_a_table_copied_between_schemas_is_the_same_and_a_change_is_not(
        pair):
    said = []
    pair.move_table("app", "", "orders", 1000, _Kept(), said.append)
    # the target's table is migkit's, CHAR(5) made VARCHAR(5): the
    # values are what is held to the source, not the padding
    q = ("select id, rtrim(code), amount, note, hex(raw) from {}.orders"
         " order by id")
    assert _sql(q.format("app_new")) == _sql(q.format("app"))
    got = [(r.status, r.detail) for r in pair.check_data("app")
           if r.check == "data"]
    assert got and all(s == "ok" for s, _ in got), got
    _sql("update app_new.orders set amount = 12.51 where id = 1")
    got = [r.status for r in pair.check_data("app") if r.check == "data"]
    assert "diff" in got, got
