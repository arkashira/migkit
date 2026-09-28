"""MariaDB's own flag as a two-way mark (backlog R3, the rungs).

A session under `skip_replication` writes every event of its transactions
with a flag in the event's header, and migkit's binlog reader finds it
there: the tail reading a MariaDB side leaves those transactions out and
carries the application's written between them. Apart only - a hop that
counts stands on the table - and it needs SUPER. It is not free of
consequences on the server, and `doctor` says them: the target's own
replicas that filter such events never receive migkit's writes, and a
transaction the application marks so itself is left out too.
"""
import subprocess
import time

import pymysql
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MA, MA_PORT = "migkit-test-rung-maria", 15963


def _connect(user="root", password="test", db="app"):
    return pymysql.connect(host="127.0.0.1", port=MA_PORT, user=user,
                           password=password, database=db)


@pytest.fixture(scope="module")
def mariadb():
    subprocess.run(["docker", "rm", "-f", "-v", MA], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MA, "-e",
                    "MARIADB_ROOT_PASSWORD=test", "-p", f"{MA_PORT}:3306",
                    "mariadb:11", "--log-bin", "--binlog-format=ROW",
                    "--binlog-row-metadata=FULL", "--binlog-row-image=FULL",
                    "--server-id=1"], check=True, capture_output=True)
    try:
        for _ in range(90):
            try:
                _connect(db=None).close()
                break
            except pymysql.err.MySQLError:
                time.sleep(2)
        conn = _connect(db=None)
        with conn.cursor() as cur:
            cur.execute("create database app")
            cur.execute("create table app.acct (id int primary key,"
                        " balance int, note varchar(20))")
            cur.execute("insert into app.acct values (1, 100, 'n1'),"
                        " (2, 100, 'n2')")
        conn.commit()
        conn.close()
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MA], capture_output=True)


def _side(name, server_id, user="root", password="test", **two_way):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=MA_PORT, user=user,
                  password=password)
    hop = Hop(name=name, engine="mysql", source=ep, target=ep,
              databases=["app"],
              options={"two_way": dict(two_way or {"on_conflict": "error"}),
                       "server_id": server_id})
    return MySQLEngine(hop)


def test_the_flag_proves_and_only_migkits_transactions_are_left_out(mariadb):
    from migkit import canon
    applier, reader = _side("maapply", 4801), _side("maread", 4802)
    assert applier.mark_facts("dst", "app")["mariadb"]
    assert applier.mark_prove("dst", "app", "skip_flag") is None
    token = reader.change_point("src", "app")
    app = _connect()
    try:
        for k in range(2):
            applier._mark_rung = "skip_flag"
            applier.neutral_apply("dst", "app", [canon.change(
                "update", "acct", {"id": 1},
                {"id": 1, "balance": 100, "note": f"mine {k}"})])
            with app.cursor() as cur:
                cur.execute(f"update acct set note = 'app {k}' where id = 2")
            app.commit()
    finally:
        app.close()
    got, _ = reader.neutral_changes("src", "app", token)
    assert [(c["key"]["id"], c["values"]["note"]) for c in got] == [
        (2, "app 0"), (2, "app 1")], got
    assert {m["kind"] for m in reader._marks_seen} == {"skip_flag"}


def test_the_climb_and_what_doctor_says_of_the_flag(mariadb, tmp_path):
    import json

    from migkit import marks
    side = _side("maclimb", 4803)
    ranked, dropped = marks.choose_rung(
        side, dict(side.mark_facts("dst", "app"), exact=False))
    assert {r.name for r in ranked} == {"skip_flag", "table"}
    assert {r.name for r, _ in dropped} == {"gtid_tag", "comment"}
    got = marks.climb(side, "app", False, print)
    assert got == ranked[0].name
    # a hop that counts stands on the table: the flag says nothing of
    # which batch was committed
    assert marks.climb(side, "app", True, print) == "table"
    token = tmp_path / "tail-token.json"
    token.write_text(json.dumps({"token": {}, "rung": "skip_flag"}))
    said = marks.said(side, token, "app")
    assert "replicate_events_marked_for_" in said, said
    assert "the application itself" in said, said


def test_a_user_who_may_set_the_flag_but_not_read_the_binlog_proves_nothing(
        mariadb):
    from migkit import marks
    conn = _connect()
    with conn.cursor() as cur:
        cur.execute("create user if not exists plain identified by 'plain'")
        cur.execute("grant all on app.* to plain")
    conn.commit()
    conn.close()
    side = _side("maplain", 4804, user="plain", password="plain")
    ranked, _ = marks.choose_rung(
        side, dict(side.mark_facts("dst", "app"), exact=False))
    # allowed: setting the flag needs no global privilege (measured)
    assert "skip_flag" in [r.name for r in ranked]
    # but the way back reads this side's binlog, and this user cannot: the
    # proof says so rather than trusting a mark nobody can read
    why = side.mark_prove("dst", "app", "skip_flag")
    assert why and "cannot be read" in why, why
