"""MySQL to MySQL `move --mode full+cdc`: the copy, then the changes made
while it ran, and after.

It used to copy and print a replica plan whose binlog position was taken
before the copy. Nothing started the replica. Measured on 8.4 by starting it
by hand from that position: the copy is not a snapshot at any position, so
the replica stopped on the first row the copy had already carried

    Replica_SQL_Running: No   Last_SQL_Errno: 1062

and nothing written afterwards reached the target. The same hop now runs the
change tail the cross-engine hops use, from a position taken before the copy
and applying by key, so the stretch the copy and the log share converges
instead of stopping.
"""
import socket
import subprocess
import time

import pytest

pytestmark = [pytest.mark.docker]

MY, PORT = "migkit-test-myfcdc", 15685


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytestmark.append(pytest.mark.skip(reason="docker not available"))


def my(sql):
    got = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                          "-h127.0.0.1", "--protocol=tcp", "-N", "-B", "-e",
                          sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8", "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        for _ in range(90):
            try:
                my("select 1")
                break
            except AssertionError:
                time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture
def hop(server, tmp_path, monkeypatch):
    """Two databases on the one server stand in for the two sides."""
    import migkit.config as cfg
    my("drop database if exists cx; drop database if exists cy;"
       " create database cx; create database cy;"
       " create table cx.t (id int primary key, v varchar(20));"
       " create table cy.t (id int primary key, v varchar(20));"
       " insert into cx.t values " + ", ".join(f"({i}, 'v{i}')"
                                            for i in range(1, 21)))
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  mm:\n    engine: mysql\n"
        f"    source: {{host: 127.0.0.1, port: {PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {PORT}, user: root,"
        " password: test}\n"
        "    databases: [cx]\n    db_map: {cx: cy}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _move(monkeypatch, during=None):
    from click.testing import CliRunner

    from migkit import cli
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.mysql import MySQLEngine
    real_move = MySQLEngine.move_table

    def copy_then_write(self, db, sch, tbl, chunk, ck, log):
        real_move(self, db, sch, tbl, chunk, ck, log)
        if during:
            during()

    monkeypatch.setattr(MySQLEngine, "move_table", copy_then_write)
    real_tail = HeteroEngine.tail_apply

    def tail_until_quiet(self, db, go, token_path, log):
        engine = self.src_engine
        real_changes = type(engine).neutral_changes

        def changes(side, db, token=None, limit=1000):
            got, token = real_changes(engine, side, db, token, limit)
            if not got:
                raise KeyboardInterrupt
            return got, token

        monkeypatch.setattr(engine, "neutral_changes", changes)
        return real_tail(self, db, go, token_path, log)

    monkeypatch.setattr(HeteroEngine, "tail_apply", tail_until_quiet)
    got = CliRunner().invoke(cli.main, ["move", "mm", "--mode", "full+cdc",
                                        "--db", "cx", "--go"])
    return got, " ".join((got.output + str(got.exception or "")).split())


def _rows(db):
    return my(f"select concat(id, ':', v) from {db}.t order by id")


@pytest.mark.usefixtures("hop")
def test_what_changed_during_the_copy_reaches_the_target(monkeypatch):
    def during():
        my("update cx.t set v = 'changed' where id = 1;"
           " delete from cx.t where id = 2;"
           " insert into cx.t values (100, 'new')")
    got, said = _move(monkeypatch, during)
    assert got.exit_code == 0, said
    assert "stopped after" in said, said
    assert _rows("cy") == _rows("cx")
    assert my("select v from cy.t where id = 1") == "changed"


@pytest.mark.usefixtures("hop")
def test_the_plan_that_stopped_on_a_duplicate_is_not_printed(monkeypatch):
    got, said = _move(monkeypatch)
    assert got.exit_code == 0, said
    assert "change replication source" not in said.lower(), said
    assert "CHANGE_ME" not in said, said
