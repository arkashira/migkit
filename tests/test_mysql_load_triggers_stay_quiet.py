"""A MySQL load does not let the target's triggers rewrite what it loads.

MySQL has no way to keep a trigger from firing for one session. Measured on
8.4, a `BEFORE INSERT` trigger setting `updated_at = now()` on the target:
rows the source dated 2001 and 2002 landed dated the day of the move,
through the bulk load and the table copier both, and the move said
complete. The triggers now come off for the load, definitions saved first,
and go back on afterwards; one the load's user could not put back as its
definer stops the move before anything is loaded.
"""
import socket
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import movers

pytestmark = [pytest.mark.docker]

SRC, DST = "migkit-test-mytrig-src", "migkit-test-mytrig-dst"
SRC_PORT, DST_PORT = 15746, 15747


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


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
    try:
        for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
            for _ in range(90):
                if subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                                   "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                   "-e", "select 1"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(2)
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def seeded(pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    my(SRC, "drop database if exists appdb; create database appdb;"
            " create table appdb.notes (id int primary key, body text,"
            " updated_at datetime); insert into appdb.notes values"
            " (1, 'a', '2001-01-01 00:00:00'), (2, 'b', '2002-02-02 00:00:00')")
    my(DST, "drop database if exists appdb; create database appdb;"
            " create table appdb.notes (id int primary key, body text,"
            " updated_at datetime);"
            " create trigger appdb.stamp before insert on appdb.notes"
            " for each row set new.updated_at = now()")

    def hop(user="root", password="test"):
        conf = tmp_path / "hops.yaml"
        conf.write_text(
            "hops:\n  mt:\n    engine: mysql\n"
            f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: root,"
            " password: test}\n"
            f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: {user},"
            f" password: {password}}}\n"
            "    databases: [appdb]\n")
        monkeypatch.setattr(cfg, "CONF", str(conf))
        monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return hop


def _move(monkeypatch, via):
    from migkit import cli
    monkeypatch.setenv("MIGKIT_MOVER", via)
    got = CliRunner().invoke(cli.main, ["move", "mt", "--go"])
    return got, " ".join((got.output + str(got.exception or "")).split())


@pytest.mark.parametrize("via", ["mydumper", "builtin"])
def test_the_rows_land_as_the_source_dated_them(seeded, monkeypatch, via):
    if via == "mydumper" and not movers.which("mydumper"):
        pytest.skip("the MySQL dump programs are not installed")
    seeded()
    got, said = _move(monkeypatch, via)
    assert got.exit_code == 0, said
    assert my(DST, "select group_concat(updated_at order by id) from"
                   " appdb.notes") == "2001-01-01 00:00:00,2002-02-02 00:00:00"
    # and the trigger is back for the application
    assert my(DST, "select action_statement from information_schema.triggers"
                   " where trigger_name = 'stamp'").lower() == \
        "set new.updated_at = now()"


def test_a_trigger_it_could_not_put_back_stops_the_move(seeded,
                                                        monkeypatch):
    my(DST, "drop user if exists 'loader'@'%';"
            " create user 'loader'@'%' identified by 'pw';"
            " grant all on appdb.* to 'loader'@'%'")
    seeded(user="loader", password="pw")
    got, said = _move(monkeypatch, "builtin")
    assert got.exit_code != 0, said
    assert "stamp on notes (defined by root@" in said, said
    assert "Nothing has been loaded" in said, said
    assert my(DST, "select count(*) from appdb.notes") == "0"
    assert my(DST, "select count(*) from information_schema.triggers"
                   " where trigger_name = 'stamp'") == "1"
