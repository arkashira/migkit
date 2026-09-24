"""What an open snapshot costs the source, said where the operator looks.

`check --consistent` holds one snapshot per side for the whole pass, and
while it is open the source cleans up nothing newer: vacuum on PostgreSQL,
purge on InnoDB. Nothing said how long it was held, or whether something
else on the source was holding one already.

* the consistent pass says how long it held the source's snapshot
* `assess` names the oldest snapshot on a PostgreSQL source (its age in
  transactions, how long, whose), and InnoDB's unpurged history on MySQL
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _pg(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="held", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    return PostgresEngine(hop)


def _hold(seconds=12):
    """Another session holding a repeatable-read snapshot open."""
    return subprocess.Popen(
        ["docker", "exec", "-e", "PGPASSWORD=test", "-e",
         "PGAPPNAME=report-job", "migkit-test-pg-src", "psql", "-U",
         "postgres", "-c", "begin isolation level repeatable read;"
         f" select count(*) from pg_class; select pg_sleep({seconds});"
         " commit;"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def test_the_oldest_snapshot_is_named_with_its_holder(pg_pair):
    eng = _pg(pg_pair)
    holder = _hold()
    try:
        time.sleep(3)
        psql(pg_pair["src"], "create table if not exists churn (v int);"
                             " insert into churn values (1)")
        got = eng.oldest_snapshot("src")
        assert got is not None, got
        age, secs, who = got
        assert who == "report-job", got
        assert secs >= 2 and age >= 1, got
        said = " ".join(f"{i['item']}: {i['detail']}"
                        for i in eng.assess())
        assert "the oldest snapshot on the source" in said, said
        assert "report-job" in said, said
    finally:
        holder.wait(30)
    assert eng.oldest_snapshot("src") is None


def test_the_consistent_pass_says_how_long_it_held_it(pg_pair):
    eng = _pg(pg_pair)
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.t (id int primary key, v int);"
                   " insert into public.t values (1, 1)")
    said = []
    got = eng.check_data("postgres", stream=said.append, consistent=True)
    data = [r for r in got if r.check == "data"][0]
    assert "held on the source" in data.detail, data.detail
    assert any(line.startswith("source snapshot held") for line in said), said


@pytest.fixture(scope="module")
def mysql():
    import socket
    name, port = "migkit-test-held-my", 15696
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                    "mysql:8"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(2)
        from migkit.engines.mysql import MySQLEngine
        ep = Endpoint(host="127.0.0.1", port=port, user="root",
                      password="test")
        yield MySQLEngine(Hop(name="held", engine="mysql", source=ep,
                              target=ep, databases=["mysql"]))
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def test_mysql_says_how_much_undo_waits_to_be_purged(mysql):
    got = mysql.purge_backlog("src")
    assert isinstance(got, int) and got >= 0, got
    said = " ".join(f"{i['item']}: {i['detail']}" for i in mysql.assess())
    assert "undo the source has not purged yet" in said, said
