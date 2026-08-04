"""Prove repair is fully reversible: after repair rewrites rows to match the
source, restore_rows puts the target back exactly as it was before, using the
undo captured at apply time.

Two real mysql servers (src + dst), same db name, covering all three row-diff
kinds at once: missing (repair inserts), extra (repair deletes), changed
(repair overwrites).
"""
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


from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-test-restore-src", "migkit-test-restore-dst"
SRC_PORT, DST_PORT = 13398, 13399


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _ready(name):
    for _ in range(40):
        if subprocess.run(["docker", "exec", name, "mysql", "-uroot", "-ptest",
                           "-e", "select 1"],
                          capture_output=True).returncode == 0:
            time.sleep(1)
            return
        time.sleep(2)


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        _ready(n)
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _sql(name, sql):
    return subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                           "-ptest"], input=sql, capture_output=True, text=True)


def test_repair_then_restore_is_exact(pair, tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine

    _sql(SRC, "create database shop;"
              " create table shop.t (id int primary key, v varchar(20));"
              " insert into shop.t values (1,'a'),(2,'b'),(3,'c'),(4,'d');")
    _sql(DST, "create database shop;"
              " create table shop.t (id int primary key, v varchar(20));"
              " insert into shop.t values (1,'a'),(3,'CHANGED'),(4,'d'),"
              "(9,'extra');")
    # dst vs src -> missing=2, extra=9, changed=3

    hop = Hop(name="r", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["shop"])
    import migkit.config as cfg
    cfg.REPORTS = tmp_path / "reports"
    eng = MySQLEngine(hop)
    d = eng.hop.report_dir("shop")
    (d / "data-t.missing").write_text("2\n")
    (d / "data-t.extra").write_text("9\n")
    (d / "data-t.changed").write_text("3\n")

    before = _sql(DST, "select * from shop.t order by id").stdout

    undo = tmp_path / "undo"
    eng._apply_rows("shop", "t", str(undo))

    after = _sql(DST, "select * from shop.t order by id").stdout
    assert "extra" not in after          # id 9 deleted
    assert "2\tb" in after               # id 2 inserted from src
    assert "3\tc" in after               # id 3 overwritten to src value
    assert "CHANGED" not in after

    # the undo file captured all three touched pks
    undo_body = (undo / "rows-t.jsonl").read_text().splitlines()
    assert len(undo_body) == 3

    # restore must return dst to the exact pre-repair bytes
    n = eng.restore_rows("shop", str(undo))
    assert n == 3
    restored = _sql(DST, "select * from shop.t order by id").stdout
    assert restored == before, f"\nbefore={before!r}\nafter ={restored!r}"
