"""The tail writes without checking foreign keys where every parent a
table in scope points at is in scope too (backlog R2.5): the parent and
its child then need not share a lane or keep each other's order, and each
table's rows go as runs.

Measured before, MySQL 10 ms away, 6,000 parents and 6,000 children
written alternately as an application writes them: 154s - the two tables
were held together in source order, so every statement was one row - and
1.4s with the keys off. Where the hop leaves a parent out, the keys stay
on: what a child points at would arrive by nothing. The deep check's
orphan scan is what finds a child whose parent never came.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MY, PORT = "migkit-test-keysoff", 15948


@pytest.fixture(scope="module")
def mysql():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8.4"], check=True, capture_output=True)
    try:
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _sql(*statements, db="app"):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=PORT, user="root",
                           password="test", autocommit=True,
                           database=db or None)
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = cur.fetchall()
        return out
    finally:
        conn.close()


class _Counted:
    def __init__(self, conn, sent):
        self._conn, self._sent = conn, sent

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def cursor(self, *a, **k):
        real, sent = self._conn.cursor(*a, **k), self._sent

        class Cursor:
            def __getattr__(self, name):
                return getattr(real, name)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                real.close()
                return False

            def execute(self, *a, **k):
                sent.append(1)
                return real.execute(*a, **k)

            def executemany(self, *a, **k):
                sent.append(1)
                return real.executemany(*a, **k)
        return Cursor()


def _engine(exclude=()):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    return MySQLEngine(Hop(name="ko", engine="mysql", source=ep, target=ep,
                           databases=["app"], workers=4,
                           exclude=list(exclude)))


@pytest.fixture
def tables(mysql):
    _sql("drop database if exists app", "create database app", db="")
    _sql("create table p (id int primary key, v varchar(20))",
         "create table c (id int primary key, pid int, v varchar(20),"
         " foreign key (pid) references p (id))")


def _batch(n):
    out = []
    for i in range(n):
        out.append({"op": "insert", "table": "p", "key": {"id": i},
                    "values": {"id": i, "v": f"p{i}"}})
        out.append({"op": "insert", "table": "c", "key": {"id": i},
                    "values": {"id": i, "pid": i, "v": f"c{i}"}})
    return out


def _sent(eng, monkeypatch, batch):
    from migkit.engines.mysql import MySQLEngine
    sent, real = [], MySQLEngine._open_writer
    monkeypatch.setattr(MySQLEngine, "_open_writer",
                        lambda self, side, db: _Counted(
                            real(self, side, db), sent))
    eng.neutral_apply("dst", "app", batch)
    monkeypatch.undo()
    return len(sent)


def test_a_parent_and_its_child_go_as_runs(tables, monkeypatch):
    eng = _engine()
    n = _sent(eng, monkeypatch, _batch(3000))
    assert _sql("select count(*) from c")[0][0] == 3000
    # a statement a table a lane, and each lane's own setting - not one a
    # row as the source wrote them
    assert n < 40, n


def test_a_parent_left_out_keeps_the_keys_on(tables, monkeypatch):
    eng = _engine(exclude=["p"])
    _sql("set session cte_max_recursion_depth = 10000",
         "insert into p select * from (with recursive g(i) as (select 0"
         " union all select i + 1 from g where i < 2999) select i,"
         " concat('p', i) from g) x")
    batch = [c for c in _batch(3000) if c["table"] == "c"]
    assert not eng._keys_off("dst", "app")
    eng.neutral_apply("dst", "app", batch)
    assert _sql("select count(*) from c")[0][0] == 3000


def test_a_child_whose_parent_never_came_is_found(tables):
    eng = _engine()
    eng.neutral_apply("dst", "app", [
        {"op": "insert", "table": "c", "key": {"id": 1},
         "values": {"id": 1, "pid": 99, "v": "orphan"}}])
    found = eng._fk_orphans("app")
    assert found is not None and found.status == "diff", found
