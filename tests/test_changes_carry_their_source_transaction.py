"""The last change of each source transaction migkit reads out of a log
says it ended (backlog 0f).

The connector's `provide.transaction.metadata` gives its readers the same:
a batch cut only after a change that ends a transaction holds whole
source transactions, and a target never shows half of one. Delivered as
data on the records (`txn_end`, beside the transaction's own name in
`txn`); what a writer does with it is the writer's, and the tail takes
both off before anything is applied.
"""
import socket
import subprocess
import time

import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def test_a_transactions_last_change_is_marked_at_its_commit():
    out = [canon.change("insert", "t", {"id": 1}, {"id": 1}, txn=7)]
    canon.ended(out, 0)
    assert out[0]["txn_end"] is True
    # a transaction that handed back nothing marks nothing
    out.append(canon.change("insert", "t", {"id": 2}, txn=8))
    canon.ended(out, 2)
    assert "txn_end" not in out[1]
    canon.ended(out, 1)
    assert out[1]["txn_end"] is True
    assert "txn_end" not in canon.change("delete", "t", {"id": 3})


def test_the_tail_takes_the_logs_marks_off_before_applying():
    from migkit.engines.hetero import _CopiedRanges
    ranges = _CopiedRanges.__new__(_CopiedRanges)
    ranges.off = True
    got, left = ranges.leave_out([
        dict(canon.change("insert", "t", {"id": 1}, {"id": 1}, txn=7),
             txn_end=True)])
    assert got == [{"op": "insert", "table": "t", "key": {"id": 1},
                    "values": {"id": 1}}], got
    assert not left


def _ends(changes):
    return [bool(c.get("txn_end")) for c in changes]


@needs_docker
def test_postgresql_changes_say_where_a_transaction_ends(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["src"]
    psql(port, "create table t (id int primary key, v text);"
               " create table own (id int primary key)")
    ep = Endpoint(host="127.0.0.1", port=port, user="postgres",
                  password="test")
    eng = PostgresEngine(Hop(name="txn0f", engine="postgres", source=ep,
                             target=ep, databases=["postgres"],
                             exclude=["own"]))
    try:
        _, token = eng.neutral_changes("src", "postgres")
        psql(port, "begin; insert into t values (1, 'a');"
                   " insert into t values (2, 'b'); update t set v = 'c'"
                   " where id = 1; insert into own values (1); commit")
        psql(port, "insert into t values (3, 'd')")
        # a transaction on the table the hop leaves alone only
        psql(port, "insert into own values (2)")
        psql(port, "insert into t values (4, 'e')")
        changes, _ = eng.neutral_changes("src", "postgres", token)
        assert [c["key"]["id"] for c in changes] == [1, 2, 1, 3, 4], changes
        # the last change handed back of each transaction, and only it -
        # the first one's after its change to the excluded table was left
        # out, and nothing for the transaction left out whole
        assert _ends(changes) == [False, False, True, True, True], changes
        xids = [c["txn"] for c in changes]
        assert xids[0] == xids[1] == xids[2] != xids[3] != xids[4], xids
    finally:
        psql(port, f"select pg_drop_replication_slot('{eng.slot_name()}')")


MY, MY_PORT = "migkit-test-0f-my", 16005


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
                    "--server-id=7", "--binlog-row-metadata=FULL"],
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


@needs_docker
def test_mysql_changes_say_where_a_transaction_ends(mysql_server):
    from migkit.engines.mysql import MySQLEngine
    _my("create database appdb; create table appdb.t (id int primary key,"
        " v varchar(20)); create table appdb.own (id int primary key)")
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")

    def eng():
        return MySQLEngine(Hop(name="txn0f", engine="mysql", source=ep,
                               target=ep, databases=["appdb"],
                               exclude=["appdb.own"]))
    token = eng().change_point("src", "appdb")
    _my("begin; insert into appdb.t values (1, 'a'), (2, 'b');"
        " update appdb.t set v = 'c' where id = 1;"
        " insert into appdb.own values (1); commit;"
        " insert into appdb.own values (2);"
        " insert into appdb.t values (3, 'd')")
    changes, _ = eng().neutral_changes("src", "appdb", token)
    assert [c["key"]["id"] for c in changes] == [1, 2, 1, 3], changes
    assert _ends(changes) == [False, False, True, True], changes
    # cut inside the transaction, the batch's last change does not end
    # it; the rest of it, read from where the cut left off, does
    part, after = eng().neutral_changes("src", "appdb", token, limit=2)
    assert _ends(part) == [False, False], part
    rest, _ = eng().neutral_changes("src", "appdb", after)
    assert [c["key"]["id"] for c in rest] == [1, 3], rest
    assert _ends(rest) == [True, True], rest
