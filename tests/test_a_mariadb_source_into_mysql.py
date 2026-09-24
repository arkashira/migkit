"""What a MariaDB source holds that MySQL has no home for is named before
the move, and what it can carry is carried (A4).

Measured on MariaDB 11 into MySQL 8.4, before:
* `move` said `complete`, and `check` read `counts OK 1 tables`, over a
  database that also held a system-versioned table and a sequence. Neither
  is a `BASE TABLE`, so every list of tables left both out. The
  schema-aware comparison saw neither either, and its reading demoted the
  dump diff, which did show the missing table, to "cosmetic".
* MariaDB's default collation, `utf8mb4_uca1400_ai_ci`, stopped the move
  on `ERROR 1273: Unknown collation`, and nothing said so beforehand.
* A system-versioned table's hidden `row_end` is part of its primary key
  in the catalogue, and a check that selected it stopped on `Unknown
  column 'row_end'`.

Now a system-versioned table is carried as a plain table, with the rows it
holds now; its history is said to stay behind. `assess` fails the
sequences, the versioned tables, the MariaDB-only column types and the
collations the target does not have. A table on one side only is never
demoted to cosmetic.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

pytestmark = [pytest.mark.docker]

MARIA, MARIA_PORT = "migkit-test-fork-maria", 15764
MY, MY_PORT = "migkit-test-fork-my", 15765


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def sql(name, client, text):
    got = subprocess.run(["docker", "exec", "-i", name, client, "-uroot",
                          "-ptest", "-N", "-B"], input=text,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    for name in (MARIA, MY):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MARIA, "-e",
                        "MARIADB_ROOT_PASSWORD=test", "-p",
                        f"{MARIA_PORT}:3306", "mariadb:11"],
                       check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for name, client in ((MARIA, "mariadb"), (MY, "mysql")):
            for _ in range(90):
                if subprocess.run(["docker", "exec", name, client, "-uroot",
                                   "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                   "-e", "select 1"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(2)
            else:
                pytest.fail(f"{name} never answered")
        yield
    finally:
        for name in (MARIA, MY):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def hop(servers, tmp_path, monkeypatch):
    import migkit.config as cfg
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  fk:\n    engine: mysql\n"
        f"    source: {{host: 127.0.0.1, port: {MARIA_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {MY_PORT}, user: root,"
        " password: test}\n"
        "    databases: [cx]\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    sql(MY, "mysql", "drop database if exists cx")

    def source(collate):
        sql(MARIA, "mariadb",
            "drop database if exists cx;"
            f" create database cx collate {collate};"
            " create table cx.plain (id int primary key, v text);"
            " insert into cx.plain values (1, 'a');"
            " create table cx.hist (id int primary key, v text)"
            "  with system versioning;"
            " insert into cx.hist values (1, 'old');"
            " update cx.hist set v = 'new' where id = 1;"
            " create sequence cx.seq start with 100;")
    return source


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_assess_names_what_has_no_home(hop):
    hop("utf8mb4_uca1400_ai_ci")
    sql(MARIA, "mariadb", "create table cx.u (id int primary key, x uuid)")
    got, said = _run("assess", "fk")
    assert "fail brand MariaDB sequences 1 with no home" in said, said
    assert "MariaDB system-versioned tables 1 with no home" in said, said
    assert "cx.u.x (uuid)" in said, said
    assert "collations the target does not have 1 used in scope" in said, \
        said
    assert "utf8mb4_uca1400_ai_ci" in said, said


def test_a_versioned_table_is_carried_and_compared(hop):
    hop("utf8mb4_general_ci")
    got, said = _run("move", "fk", "--go")
    assert got.exit_code == 0, said
    assert "hist: made without its history" in said, said
    assert sql(MY, "mysql", "select concat(id, '=', v) from cx.hist") == \
        "1=new"
    got, said = _run("check", "fk", "--only", "counts,data")
    assert got.exit_code == 0, said
    assert "cx.hist: OK rows 1==1" in said, said
    # a table on one side only is a difference, whatever else read clean
    sql(MY, "mysql", "drop table cx.hist")
    got, said = _run("check", "fk", "--only", "schema")
    assert got.exit_code != 0, said
    assert "cosmetic" not in said, said
