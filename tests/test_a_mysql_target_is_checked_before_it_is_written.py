"""The interlock on the target, on two MySQL servers (backlog R23, 3).

MySQL says which server it is by `@@server_uuid`, what it calls itself
by `@@hostname`: shown in the plan and held to at `--go`. The source given
as the target, a target holding rows the hop says nothing about, and a
target the hop was pointed at after the plan are refused; a move into an
empty target goes on as it always has.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker

pytestmark = needs_docker

SERVERS = {"a": ("migkit-test-lock-mysql-a", 16240),
           "b": ("migkit-test-lock-mysql-b", 16241)}


def _my(which, sql):
    got = subprocess.run(["docker", "exec", SERVERS[which][0], "mysql",
                          "-uroot", "-ptest", "-N", "-B", "-e", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for name, port in SERVERS.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"127.0.0.1:{port}:3306", "mysql:8.4"],
                       check=True, capture_output=True)
    try:
        for name, _ in SERVERS.values():
            for _ in range(90):
                if subprocess.run(["docker", "exec", name, "mysql",
                                   "-uroot", "-ptest", "-h127.0.0.1",
                                   "--protocol=tcp", "-e", "select 1"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(2)
            else:
                pytest.fail(f"{name} never answered")
        yield
    finally:
        for name, _ in SERVERS.values():
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


TABLES = ("create table t (id int primary key, v varchar(20));"
          " create table u (id int primary key, v varchar(20));")


@pytest.fixture
def pair(servers, tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    for which in SERVERS:
        _my(which, "drop database if exists app; drop database if exists"
                   " other; create database app; create database other;"
                   f" use app; {TABLES} use other; {TABLES}")
    _my("a", "insert into app.t values (1, 'x'), (2, 'y');"
             " insert into app.u values (5, 'z')")


def _hop(tmp_path, src="a", dst="b", host="127.0.0.1", target_db="",
         options=""):
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  il:\n    engine: mysql\n"
        f"    source: {{host: 127.0.0.1, port: {SERVERS[src][1]},"
        " user: root, password: test}\n"
        f"    target: {{host: {host}, port: {SERVERS[dst][1]}, user: root,"
        " password: test}\n    databases: [app]\n"
        + (f"    db_map: {{app: {target_db}}}\n" if target_db else "")
        + (f"    options: {{{options}}}\n" if options else ""))


def _move(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "il", *argv])
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_the_plan_shows_both_servers_and_a_move_goes_on(pair, tmp_path):
    _hop(tmp_path)
    got, said = _move()
    assert got.exit_code == 0, said
    for which in SERVERS:
        assert f"server {_my(which, 'select @@server_uuid')}" in said, said
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _my("b", "select count(*) from app.t") == "2"


def test_the_source_as_the_target_is_refused(pair, tmp_path):
    _hop(tmp_path, dst="a", host="localhost")
    got, said = _move("--go")
    assert got.exit_code != 0, said
    assert "the target is the source itself" in said, said
    assert _my("a", "select count(*) from app.t") == "2"
    # the same server into another database is a target like any other
    _hop(tmp_path, dst="a", target_db="other")
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _my("a", "select count(*) from other.t") == "2"


def test_a_target_holding_rows_is_refused_until_the_hop_says(pair,
                                                            tmp_path):
    _my("b", "insert into app.t values (99, 'theirs')")
    _hop(tmp_path)
    got, said = _move("--go")
    assert got.exit_code != 0, said
    assert "already holds rows on the target: t." in said, said
    assert _my("b", "select group_concat(id) from app.t") == "99"
    assert _my("b", "select count(*) from app.u") == "0"
    _hop(tmp_path, options="target_rows: keep")
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _my("b", "select group_concat(id) from app.t") == "99"
    assert _my("b", "select count(*) from app.u") == "1"
    _hop(tmp_path, options="target_rows: replace")
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _my("b", "select group_concat(id order by id) from app.t") == \
        "1,2"


def test_a_target_changed_between_the_plan_and_go_is_refused(pair,
                                                            tmp_path):
    _hop(tmp_path)
    assert _move()[0].exit_code == 0
    # pointed at the other server between the plan and --go
    _hop(tmp_path, dst="a", target_db="other")
    got, said = _move("--go")
    assert got.exit_code != 0, said
    assert "not the server the plan showed" in said, said
    assert _my("a", "select count(*) from other.t") == "0"
    assert _move()[0].exit_code == 0
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _my("a", "select count(*) from other.t") == "2"
