"""A fork change, MySQL 8.4 into MariaDB 10.11 (backlog P0-SCENARIOS): the
two brands read from the servers, the plan saying "a fork change", and what
MySQL's source uses that MariaDB has not refused before anything is
written - its default collation (`utf8mb4_0900_ai_ci`, which MariaDB 10.11
does not take), an index on an expression - with MariaDB's JSON kept as
text and its own transaction ids said.

Before: nothing named the fork; the move stopped on `ERROR 1273: Unknown
collation` at the first table, and the index on an expression the first
time MariaDB was asked to make it.
"""
import subprocess
import time

import pytest

from tests.conftest import needs_docker

pytestmark = needs_docker

MY = ("migkit-test-fork-my84", 16402, "mysql:8.4")
MARIA = ("migkit-test-fork-maria1011", 16403, "mariadb:10.11")


def sql(name, client, text):
    got = subprocess.run(["docker", "exec", "-i", name, client, "-uroot",
                          "-ptest", "-N", "-B"], input=text,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for name, _, _ in (MY, MARIA):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY[0], "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{MY[1]}:3306", MY[2]], check=True,
                   capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MARIA[0], "-e",
                    "MARIADB_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{MARIA[1]}:3306", MARIA[2]], check=True,
                   capture_output=True)
    try:
        for name, client in ((MY[0], "mysql"), (MARIA[0], "mariadb")):
            for _ in range(120):
                ok = subprocess.run(["docker", "exec", name, client,
                                     "-uroot", "-ptest", "-e", "select 1"],
                                    capture_output=True)
                if ok.returncode == 0:
                    break
                time.sleep(2)
        sql(MY[0], "mysql", "create database app; create table app.plain"
                            " (id int primary key, v varchar(20));"
                            " create table app.doc (id int primary key,"
                            " body json); create index by_len on app.plain"
                            " ((length(v)));")
        sql(MARIA[0], "mariadb", "create database app")
        yield
    finally:
        for name, _, _ in (MY, MARIA):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="fork-my", engine="mysql", goal="cutover",
              source=Endpoint(host="127.0.0.1", port=MY[1], user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=MARIA[1], user="root",
                              password="test"),
              databases=["app"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def test_mysqls_source_into_mariadb_is_held_to_what_mariadb_lacks(
        servers, tmp_path):
    from migkit import case, ready
    from migkit.engines.mysql import MySQLEngine
    hop = _hop(tmp_path)
    rows = case.checks(hop, MySQLEngine(hop), ["app"], quick=True)
    by = {}
    for r in rows:
        by.setdefault(r["item"], []).append(r)
    named = by["what the two servers are"][0]["detail"]
    assert named.startswith("a fork change: MySQL 8.4"), named
    assert "to MariaDB 10.11" in named
    coll = by["collations"][0]
    assert coll["level"] == "fail", coll
    assert "utf8mb4_0900_ai_ci" in coll["detail"]
    expr = by["indexes on expressions"][0]
    assert expr["level"] == "fail" and "plain.by_len" in expr["detail"]
    assert "MariaDB" in expr["detail"] and "has none" in expr["detail"]
    text = by["JSON as text"][0]
    assert text["level"] == "warn" and "app.doc.body" in text["detail"]
    assert by["transaction ids"][0]["level"] == "pass"
    # the same, before the first write: the move is not started
    stops = [str(i) for i in ready.stops(hop, MySQLEngine(hop), ["app"],
                                         "full", False)]
    assert any("utf8mb4_0900_ai_ci" in s for s in stops), stops
    assert any("indexes on expressions" in s for s in stops), stops
    lines = case.plan_lines(hop, MySQLEngine(hop), "app")
    assert lines[0].startswith("the case: a fork change: MySQL 8.4"), lines
