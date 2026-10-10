"""A source that logs statements (binlog_format STATEMENT or MIXED) is not
read as if it had logged nothing (LT.R8).

The text of an INSERT, UPDATE, DELETE or LOAD names its table, so the
tail sees that table changed and has it copied again (a `reload`, depth
D1). A statement that writes where its text does not say - a CALL, a
join, a table with a trigger or a cascading key - stops the tail, saying
to set ROW.
"""
import socket
import subprocess
import time

import pytest

from migkit import logddl
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker


def test_the_statements_that_name_one_table_are_read():
    assert logddl.dml("insert into t values (1)", "app") == (
        [("app", "t")], False)
    assert logddl.dml("UPDATE `x`.`t` AS a SET a.v = 1", "app") == (
        [("x", "t")], False)
    assert logddl.dml("delete from t where id = 1", "app") == (
        [("app", "t")], False)
    assert logddl.dml("LOAD DATA INFILE '/f' INTO TABLE t", "app") == (
        [("app", "t")], False)


def test_what_changes_rows_by_a_way_the_text_does_not_name_is_unread():
    for q in ("call p()", "update a join b using (id) set a.v = 1",
              "delete a from a join b using (id)", "do sleep(1)"):
        assert logddl.dml(q, "app") == ([], True), q
    for q in ("BEGIN", "COMMIT", "set @@session.x = 1",
              "create table t (a int)", "grant select on *.* to u"):
        assert logddl.dml(q, "app") == ([], False), q


MY, MY_PORT = "migkit-test-lt-r8-my", 16584


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{MY_PORT}:3306", "mysql:8.4",
                    "--server-id=7", "--binlog-row-metadata=FULL",
                    "--binlog-format=STATEMENT"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _engine(exclude=()):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    return MySQLEngine(Hop(name="ltr8", engine="mysql", source=ep, target=ep,
                           databases=["appdb"], exclude=list(exclude)))


@needs_docker
def test_a_statement_log_gives_the_tables_it_changed(mysql_server):
    _my("create database appdb; create table appdb.t (id int primary key,"
        " v int); create table appdb.own (id int primary key)")
    assert _my("select @@binlog_format") == "STATEMENT"
    token = _engine(["appdb.own"]).change_point("src", "appdb")
    _my("insert into appdb.t values (1, 1), (2, 2);"
        " update appdb.t set v = 5 where id = 1;"
        " insert into appdb.own values (1)")
    changes, _ = _engine(["appdb.own"]).neutral_changes("src", "appdb", token)
    assert [(c["op"], c.get("kind"), c["table"]) for c in changes] == [
        ("ddl", "reload", "t"), ("ddl", "reload", "t")], changes


@needs_docker
def test_a_statement_that_writes_where_its_text_does_not_say_stops_it(
        mysql_server):
    _my("create database if not exists appdb;"
        " create table if not exists appdb.u (id int primary key);"
        " create table if not exists appdb.log (id int)")
    token = _engine().change_point("src", "appdb")
    _my("create trigger appdb.u_log after insert on appdb.u for each row"
        " insert into appdb.log values (new.id)")
    _my("insert into appdb.u values (7)")
    with pytest.raises(SystemExit, match="binlog_format = 'ROW'"):
        _engine().neutral_changes("src", "appdb", token)
