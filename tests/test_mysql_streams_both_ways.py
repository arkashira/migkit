"""MySQL both ways (backlog 36): each side a replica of the other, rows
written on both at once, and nothing going round.

Before this, `topology: two_way` was PostgreSQL 16's only: a MySQL hop had
no word on whether a change would come back to where it was made, or
whether two sides writing at once would make the same key. What stops a
loop is each side's own server id and GTIDs on both; what keeps two
sides' new rows apart is each handing out its own auto-increment values.
Both are asked before anything is set up, and each missing one named.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

NET = "migkit-test-mytwo-net"
A, B = ("migkit-test-mytwo-a", 15897), ("migkit-test-mytwo-b", 15898)


def sql(node, text):
    got = subprocess.run(["docker", "exec", "-i", node[0], "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=text,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    for i, node in enumerate((A, B), 1):
        subprocess.run(["docker", "rm", "-f", "-v", node[0]],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", node[0], "--network",
                        NET, "-e", "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"{node[1]}:3306", "mysql:8.4", f"--server-id={i}",
                        "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                        "--auto-increment-increment=2",
                        f"--auto-increment-offset={i}"],
                       check=True, capture_output=True)
    try:
        for node in (A, B):
            end = time.time() + 180
            while time.time() < end:
                ok = subprocess.run(["docker", "exec", node[0], "mysql",
                                     "-uroot", "-ptest", "-h127.0.0.1",
                                     "--protocol=tcp", "-e", "select 1"],
                                    capture_output=True).returncode == 0
                with socket.socket() as s:
                    s.settimeout(2)
                    if ok and s.connect_ex(("127.0.0.1", node[1])) == 0:
                        break
                time.sleep(2)
            else:
                pytest.fail(f"{node[0]} never answered")
        yield
    finally:
        for node in (A, B):
            subprocess.run(["docker", "rm", "-f", "-v", node[0]],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _eng():
    from migkit.engines.mysql import MySQLEngine
    return MySQLEngine(Hop(
        name="two", engine="mysql",
        source=Endpoint(host="127.0.0.1", port=A[1], user="root",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=B[1], user="root",
                        password="test"),
        databases=["app"], options={"topology": "two_way"}))


def _run(plan, on, other):
    """A plan's statements run where they go, the other server named as
    the replica sees it on the network the two share."""
    for s in plan:
        sql(on, s.replace("'127.0.0.1'", f"'{other[0]}'")
            .replace(f"= {other[1]}", "= 3306"))


def test_both_ways_nothing_goes_round(pair):
    for node in (A, B):
        sql(node, "create database if not exists app; create table if not"
                  " exists app.t (id int auto_increment primary key, v"
                  " varchar(20), at_node varchar(2))")
    eng = _eng()
    assert eng.loops_prevented("app") == ""
    forward = eng.replicate_sql("app", False, "pw")
    back = type(eng)(eng.hop.reversed()).replicate_sql("app", False, "pw")
    _run(forward["src"], A, B)
    _run(forward["dst"], B, A)
    _run(back["src"], B, A)
    _run(back["dst"], A, B)
    # both sides write at once, new rows by auto-increment on each
    for i in range(50):
        sql(A, f"insert into app.t (v, at_node) values ('a{i}', 'A')")
        sql(B, f"insert into app.t (v, at_node) values ('b{i}', 'B')")
    sql(A, "update app.t set v = concat(v, '!') where at_node = 'B'"
           " and id < 40")
    sql(B, "delete from app.t where at_node = 'A' and id > 80")
    q = ("select count(*), md5(group_concat(concat_ws('|', id, v, at_node)"
         " order by id)) from app.t")
    for _ in range(60):
        if sql(A, q) == sql(B, q):
            break
        time.sleep(1)
    assert sql(A, q) == sql(B, q)
    # and it stays still: a change going round would keep the executed
    # sets growing
    gtids = (sql(A, "select @@gtid_executed"), sql(B, "select @@gtid_executed"))
    time.sleep(5)
    assert (sql(A, "select @@gtid_executed"),
            sql(B, "select @@gtid_executed")) == gtids
    for node in (A, B):
        status = sql(node, "select service_state from performance_schema"
                           ".replication_applier_status")
        assert status == "ON", status
        err = sql(node, "select last_error_number from performance_schema"
                        ".replication_applier_status_by_worker where"
                        " last_error_number <> 0")
        assert err == "", err


def test_what_would_go_round_or_collide_is_named(pair):
    for node in (A, B):
        sql(node, "create database if not exists app; create table if not"
                  " exists app.t (id int auto_increment primary key, v"
                  " varchar(20), at_node varchar(2))")
    eng = _eng()
    sql(B, "set global auto_increment_offset = 1")
    try:
        why = eng.loops_prevented("app")
        assert why.startswith("both sides hand out the same auto-increment"
                              " values (increment 2 and 2, offset 1 and"
                              " 1)"), why
    finally:
        sql(B, "set global auto_increment_offset = 2")
    sql(B, "set global server_id = 1")
    try:
        assert _eng().loops_prevented("app") == (
            "both sides have server_id 1: each would take the other's"
            " changes for its own and drop them")
    finally:
        sql(B, "set global server_id = 2")


def test_a_row_both_sides_change_at_once_ends_different(pair):
    """Why the warning is there: measured, each side ends with the other's
    value, and the check is what finds it."""
    for node in (A, B):
        sql(node, "create database if not exists app; create table if not"
                  " exists app.t (id int auto_increment primary key, v"
                  " varchar(20), at_node varchar(2))")
    sql(A, "insert into app.t (id, v, at_node) values (1001, 'start', 'A')")
    for _ in range(30):
        if sql(B, "select v from app.t where id = 1001") == "start":
            break
        time.sleep(1)
    # both replicas held, both sides change the row, both let go
    for node in (A, B):
        sql(node, "stop replica sql_thread")
    sql(A, "update app.t set v = 'from A' where id = 1001")
    sql(B, "update app.t set v = 'from B' where id = 1001")
    for node in (A, B):
        sql(node, "start replica sql_thread")
    time.sleep(5)
    got = (sql(A, "select v from app.t where id = 1001"),
           sql(B, "select v from app.t where id = 1001"))
    assert got == ("from B", "from A"), got
