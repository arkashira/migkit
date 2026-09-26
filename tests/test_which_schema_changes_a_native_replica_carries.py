"""Which schema changes a native MySQL replica carries under the hop's
filters (backlog 5).

With the same database name on both sides, every table, view, routine,
trigger and index statement in the hop's database arrives, and what is
outside it does not: another database, accounts, grants.

A renamed database is different, measured on 8.4. The replica's rewrite
applies to the database in use, not to one a statement names. So
`alter table cx.t add column ...`, run with another database in use or
none, never reaches the renamed target. The replica keeps running, and the
next row arrives without the added columns' values. So migkit does not set
up a native replica for a hop that renames a database, and says why.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

NET = "migkit-test-ddlrep-net"
SRC, DST = "migkit-test-ddlrep-src", "migkit-test-ddlrep-dst"
SRC_PORT, DST_PORT = 15820, 15821


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _wait(name, port):
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp",
                             "-e", "select 1"],
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    for name, port, sid in ((SRC, SRC_PORT, 51), (DST, DST_PORT, 52)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "--network",
                        NET, "-e", "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"{port}:3306", "mysql:8.4", f"--server-id={sid}"],
                       check=True, capture_output=True)
    try:
        _wait(SRC, SRC_PORT)
        _wait(DST, DST_PORT)
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _eng(renamed):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="ddl", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["cx"], db_map={"cx": "cy"} if renamed else {})
    return MySQLEngine(hop)


def _replica(renamed):
    """The plan's own statements, run on the target, as `--go` runs them."""
    tdb = "cy" if renamed else "cx"
    subprocess.run(["docker", "exec", "-i", DST, "mysql", "-uroot", "-ptest"],
                   input="stop replica; reset replica all;",
                   capture_output=True, text=True)
    my(SRC, "drop database if exists cx; drop database if exists other;"
            " create database cx; create database other;"
            " create table cx.t (id int primary key, v int);"
            " drop user if exists 'migkit_repl'@'%'; drop user if exists u1;"
            " create user 'migkit_repl'@'%' identified by 'pw';"
            " grant replication slave on *.* to 'migkit_repl'@'%'")
    my(DST, "drop database if exists cx; drop database if exists cy;"
            f" drop database if exists other; create database {tdb};"
            f" create database other; create table {tdb}.t"
            " (id int primary key, v int); drop user if exists u1")
    for stmt in _eng(renamed).replicate_sql("cx", False, "pw")["dst"]:
        my(DST, stmt.replace("SOURCE_HOST = '127.0.0.1'",
                             f"SOURCE_HOST = '{SRC}'")
           .replace(f"SOURCE_PORT = {SRC_PORT}", "SOURCE_PORT = 3306"))
    return tdb


def _arrived(sql, probe):
    my(SRC, sql)
    time.sleep(1.5)
    return my(DST, probe) != "0"


def _column(tdb, name):
    return ("select count(*) from information_schema.columns where"
            f" table_schema = '{tdb}' and table_name = 't' and"
            f" column_name = '{name}'")


def _running():
    said = my(DST, "select service_state from performance_schema"
                   ".replication_applier_status")
    return said == "ON", said


def test_under_the_same_name_the_databases_changes_arrive(pair):
    tdb = _replica(False)
    carried = {
        "create table cx.t3, another database in use":
            ("use other; create table cx.t3 (id int primary key)",
             "select count(*) from information_schema.tables where"
             " table_schema = 'cx' and table_name = 't3'"),
        "alter table cx.t, no database in use":
            ("alter table cx.t add column e int", _column(tdb, "e")),
        "view": ("use cx; create view vw as select id from t",
                 "select count(*) from information_schema.views where"
                 " table_schema = 'cx' and table_name = 'vw'"),
        "procedure": ("use cx; create procedure p1() select 1",
                      "select count(*) from information_schema.routines"
                      " where routine_schema = 'cx' and routine_name ="
                      " 'p1'"),
        "trigger": ("use cx; create trigger tr1 before insert on t for each"
                    " row set new.v = 1",
                    "select count(*) from information_schema.triggers where"
                    " trigger_schema = 'cx' and trigger_name = 'tr1'"),
        "index": ("use cx; create index iv on t (v)",
                  "select count(*) from information_schema.statistics where"
                  " table_schema = 'cx' and index_name = 'iv'"),
    }
    for what, (sql, probe) in carried.items():
        assert _arrived(sql, probe), what
    left = {
        "a table in another database":
            ("create table other.o1 (id int primary key)",
             "select count(*) from information_schema.tables where"
             " table_schema = 'other' and table_name = 'o1'"),
        "an account": ("create user u1 identified by 'x'",
                       "select count(*) from mysql.user where user = 'u1'"),
    }
    for what, (sql, probe) in left.items():
        assert not _arrived(sql, probe), what
    assert _running()[0], _running()[1]
    assert _eng(False).native_replica_unsafe() is None


def test_a_renamed_database_loses_a_qualified_change_and_its_values(pair):
    tdb = _replica(True)
    assert _arrived("use cx; alter table t add column c int",
                    _column(tdb, "c"))
    assert not _arrived("use other; alter table cx.t add column d int",
                        _column(tdb, "d"))
    my(SRC, "insert into cx.t (id, v, c, d) values (7, 1, 2, 3)")
    time.sleep(2)
    # the row arrived without the value of the column that never did,
    # and the replica says nothing is wrong
    assert my(DST, f"select * from {tdb}.t where id = 7") == "7\t1\t2"
    assert _running()[0], _running()[1]
    why = _eng(True).native_replica_unsafe()
    assert why and "renames cx to cy" in why, why


def test_the_move_refuses_the_replica_for_a_renamed_database(pair, tmp_path,
                                                              monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  ddl:\n    engine: mysql\n"
        f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: root,"
        " password: test}\n"
        "    databases: [cx]\n    db_map: {cx: cy}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    got = CliRunner().invoke(cli.main, ["replicate", "ddl", "--no-copy-data"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code != 0, said
    assert "Nothing was set up" in said and "--mode cdc" in said, said
