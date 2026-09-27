"""What `assess` says before a load about the target and the source's code:
standbys and replicas the target ships every loaded row to, stored code
written under another collation than the target database's, and key-less
tables a replica can only apply by reading them whole.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, MY_PORT = "migkit-test-assess-my", 15890
MY_REPLICA = "migkit-test-assess-myreplica"
PG_STANDBY = "migkit-test-assess-standby"


def _ip(name):
    return subprocess.run(["docker", "inspect", "-f",
                           "{{range .NetworkSettings.Networks}}"
                           "{{.IPAddress}}{{end}}", name],
                          capture_output=True, text=True).stdout.strip()


def _items(eng, item):
    return [i for i in eng.assess() if i["item"] == item]


def _pg(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(Hop(
        name="a", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                        user="postgres", password="test"),
        target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                        user="postgres", password="test"),
        databases=["postgres"]))


def test_a_standby_of_the_target_is_named(pg_pair):
    item = "no standbys stream from the target during the load"
    assert [i["level"] for i in _items(_pg(pg_pair), item)] == ["pass"]
    subprocess.run(["docker", "exec", "migkit-test-pg-dst", "sh", "-c",
                    "echo 'host replication all all scram-sha-256' >>"
                    " /var/lib/postgresql/data/pg_hba.conf"], check=True)
    psql(pg_pair["dst"], "select pg_reload_conf()")
    subprocess.run(["docker", "rm", "-f", "-v", PG_STANDBY],
                   capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--name", PG_STANDBY, "-e", "PGPASSWORD=test",
         "--entrypoint", "sh", "postgres:16", "-c",
         "mkdir -p /var/lib/postgresql/data && chown postgres"
         " /var/lib/postgresql/data && chmod 700 /var/lib/postgresql/data &&"
         f" su postgres -c 'pg_basebackup -h {_ip('migkit-test-pg-dst')} -U"
         " postgres -D /var/lib/postgresql/data -R -X stream"
         " --checkpoint=fast' && exec su postgres -c 'postgres -D"
         " /var/lib/postgresql/data'"], check=True, capture_output=True)
    try:
        end = time.time() + 90
        got = []
        while time.time() < end:
            got = _items(_pg(pg_pair), item)
            if got and got[0]["level"] == "warn":
                break
            time.sleep(2)
        assert [i["level"] for i in got] == ["warn"], got
        assert got[0]["detail"].startswith("1 standbys stream from the"
                                           " target"), got
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG_STANDBY],
                       capture_output=True)


def my(sql, name=MY):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _wait_my(name, port=None):
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp", "-e",
                             "select 1"], capture_output=True).returncode == 0
        if ok and port is None:
            return
        if ok:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module")
def mysql_pair():
    for name in (MY, MY_REPLICA):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8.4", "--server-id=1"], check=True,
                   capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY_REPLICA, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "mysql:8.4",
                    "--server-id=2"], check=True, capture_output=True)
    try:
        _wait_my(MY, MY_PORT)
        _wait_my(MY_REPLICA)
        yield
    finally:
        for name in (MY, MY_REPLICA):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _my_eng():
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    return MySQLEngine(Hop(name="a", engine="mysql", source=ep, target=ep,
                           databases=["app"], db_map={"app": "app_copy"}))


def test_code_written_under_another_collation_is_named(mysql_pair):
    my("drop database if exists app; drop database if exists app_copy;"
       " create database app collate utf8mb4_0900_ai_ci;"
       " create database app_copy collate utf8mb4_general_ci;"
       " create procedure app.find(in n varchar(20)) select n = 'a';"
       " create table app.t (id int primary key, v varchar(10));"
       " create trigger app.t_stamp before insert on app.t for each row"
       " set new.v = lower(new.v)")
    got = _items(_my_eng(), "stored code written under the target's"
                            " collation")
    assert [i["level"] for i in got] == ["warn"], got
    assert "2 of 2 were written under another collation" in got[0]["detail"]
    assert "PROCEDURE find (utf8mb4_0900_ai_ci)" in got[0]["detail"]
    my("drop database app_copy; create database app_copy collate"
       " utf8mb4_0900_ai_ci")
    got = _items(_my_eng(), "stored code written under the target's"
                            " collation")
    assert [i["level"] for i in got] == ["pass"], got


def test_a_replica_of_the_target_is_named(mysql_pair):
    item = "no replicas read from the target during the load"
    my("create database if not exists app; create database if not exists"
       " app_copy")
    assert [i["level"] for i in _items(_my_eng(), item)] == ["pass"]
    my(f"change replication source to source_host = '{_ip(MY)}',"
       " source_user = 'root', source_password = 'test',"
       " get_source_public_key = 1; start replica", name=MY_REPLICA)
    end = time.time() + 60
    got = []
    while time.time() < end:
        got = _items(_my_eng(), item)
        if got and got[0]["level"] == "warn":
            break
        time.sleep(2)
    assert [i["level"] for i in got] == ["warn"], got
    assert got[0]["detail"].startswith("1 replicas read from the target")


def test_a_keyless_table_a_replica_reads_whole_is_named(mysql_pair):
    my("drop database if exists app; create database app; create database"
       " if not exists app_copy; create table app.events (at datetime,"
       " body json); create table app.plain (a int, b int)")
    got = [r for r in _my_eng().check_deep("app")
           if r.scope == "app keys"]
    assert [r.status for r in got] == ["diff"], got
    assert "2 tables have no pk/unique" in got[0].detail
    assert "1 of them hold JSON, spatial or large columns" in got[0].detail
    assert got[0].detail.endswith(": events"), got[0].detail


def test_large_values_are_sized_before_a_move(pg_pair, mysql_pair):
    got = psql(pg_pair["src"], """
        drop table if exists public.docs;
        create table public.docs (id int primary key, body text);
        alter table public.docs alter column body set storage external;
        insert into public.docs select g, repeat(md5(g::text), 400)
          from generate_series(1, 2000) g;
        select lo_from_bytea(0, decode(repeat('ab', 50000), 'hex'))
          from generate_series(1, 3)""")
    assert got.returncode == 0, got.stderr
    item = [i for i in _pg(pg_pair).assess()
            if i["item"] == "values stored outside the rows sized"]
    assert item and item[0]["detail"].startswith("3 large objects in"), item
    assert "MB of values stored out of line" in item[0]["detail"], item
    my("drop database if exists app; create database app; create database"
       " if not exists app_copy; create table app.files (id int primary"
       " key, body longblob); set session cte_max_recursion_depth = 10000;"
       " insert into app.files with recursive g(n) as (select 1 union all"
       " select n + 1 from g where n < 300) select n, repeat('x', n * 100)"
       " from g")
    item = [i for i in _my_eng().assess() if i["item"] == "large values"
            " sized" and i["scope"] == "app"]
    assert item, item
    assert "the largest value seen is 29.3 KB, in files.body" in \
        item[0]["detail"], item
