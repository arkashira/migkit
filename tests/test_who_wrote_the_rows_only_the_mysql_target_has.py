"""Rows only the target has: who wrote them, on MySQL (backlog 15).

PostgreSQL answers from transaction ids and commit times, MongoDB from its
ObjectIds. MySQL has neither, and has its binary log: a move records where
the target's log was as it began, and the data check reads the target's
log from there for the rows only the target has. A row written since is
in it, with the server that wrote it; one that is not was there before the
move - a target that was not emptied.
"""
import json
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MY, PORT = "migkit-test-whowrote-my", 15809


def _sql(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        _sql("set global binlog_row_metadata = 'FULL'")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def test_before_and_since_the_move_began(server, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="who", engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"})
    hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    _sql("drop database if exists cx; drop database if exists cy;"
         " create database cx; create database cy;"
         " create table cx.t (id int primary key, v varchar(20));"
         " create table cy.t (id int primary key, v varchar(20));"
         " insert into cx.t values (1, 'a'), (2, 'b');"
         " insert into cy.t values (1, 'a'), (2, 'b'),"
         " (50, 'left from before');")
    (tmp_path / "move-began.json").write_text(json.dumps(
        {"at": "the test's move", **eng.target_mark("cx")}))
    _sql("insert into cy.t values (60, 'the application'),"
         " (61, 'the application')")
    got = [r for r in eng.check_data("cx") if r.scope.endswith(".t")][0]
    assert got.status == "diff", got.detail
    assert "extra=3" in got.detail, got.detail
    assert "1 not written since the move of the test's move began - the" \
           " target held them before it" in got.detail, got.detail
    assert "2 written since it began, 2 by the target's own sessions" \
        in got.detail, got.detail


def test_a_log_that_is_gone_is_said_not_guessed(server, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    eng = MySQLEngine(Hop(name="who", engine="mysql", source=ep, target=ep,
                          db_map={"cx": "cy"}))
    said = eng.who_wrote("cx", "t", ["2:60"],
                         {"log_file": "binlog.999999", "log_pos": 4})
    assert "cannot be told" in said and "gone" in said, said
