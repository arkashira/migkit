"""A MySQL bulk move creates the tables the target does not have yet.

The bulk path loads rows, not tables. Measured onto a target without them:
the move stopped at the load on `ERROR 1146: Table 'appdb.small' doesn't
exist`, where the cross-engine copier creates what is missing. It now creates exactly the tables the target lacks, from the
source's own definition, and the database too when it is not there. A
table the target already has is not touched, and neither is one the hop
excludes.
"""
import socket
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import movers

pytestmark = [pytest.mark.docker]

MY, PORT = "migkit-test-mycreate", 15709


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytestmark.append(pytest.mark.skip(reason="docker not available"))
if not (movers.which("mydumper") and movers.which("myloader")):
    pytestmark.append(pytest.mark.skip(reason="bulk copy not installed"))


def my(sql, ok=True):
    got = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                          "-h127.0.0.1", "--protocol=tcp", "-N", "-B", "-e",
                          sql], capture_output=True, text=True)
    if ok:
        assert got.returncode == 0, got.stderr
    return got if not ok else got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8.4"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        for _ in range(90):
            if my("select 1", ok=False).returncode == 0:
                break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture
def source(server, tmp_path, monkeypatch):
    """`cx` is the source; the target is whatever `cx` maps to."""
    import migkit.config as cfg
    my("drop database if exists cx; drop database if exists cy;"
       " drop database if exists cz;"
       " create database cx character set latin1 collate latin1_swedish_ci;"
       " create table cx.z_parent (id int primary key, v varchar(20));"
       # a child that sorts, and so is created, before the table it
       # references
       " create table cx.a_child (id int primary key, parent int,"
       "  foreign key (parent) references cx.z_parent (id));"
       " create table cx.kept (id int primary key, v varchar(20));"
       " create table cx.audit (id int primary key);"
       " insert into cx.z_parent values (1, 'p1'), (2, 'p2');"
       " insert into cx.a_child values (10, 1), (11, 2);"
       " insert into cx.kept values (1, 'k');"
       " insert into cx.audit values (1);")

    def hop(target):
        conf = tmp_path / "hops.yaml"
        conf.write_text(
            "hops:\n  mc:\n    engine: mysql\n"
            f"    source: {{host: 127.0.0.1, port: {PORT}, user: root,"
            " password: test}\n"
            f"    target: {{host: 127.0.0.1, port: {PORT}, user: root,"
            " password: test}\n"
            f"    databases: [cx]\n    db_map: {{cx: {target}}}\n"
            "    exclude: [cx.audit]\n")
        monkeypatch.setattr(cfg, "CONF", str(conf))
        monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return hop


def _move(*extra):
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "mc", "--mode", "full",
                                        *extra])
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_the_tables_the_target_lacks_are_created_and_loaded(source):
    source("cy")
    my("create database cy;"
       " create table cy.kept (id int primary key, v varchar(20))"
       "  comment 'the target made this one'")
    got, said = _move()
    assert "create the tables the target does not have yet, from the" \
        " source's definition: 2" in said, said
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert my("select group_concat(id order by id) from cy.a_child") == \
        "10,11"
    assert my("select group_concat(id order by id) from cy.z_parent") == \
        "1,2"
    assert my("select count(*) from information_schema.referential_"
              "constraints where constraint_schema = 'cy'") == "1"
    # the table it had is loaded, and left as the target made it
    assert my("select v from cy.kept") == "k"
    assert my("select table_comment from information_schema.tables"
              " where table_schema = 'cy' and table_name = 'kept'") == \
        "the target made this one"
    # what the hop excludes is not created
    assert my("select count(*) from information_schema.tables"
              " where table_schema = 'cy' and table_name = 'audit'") == "0"


def test_a_missing_database_is_created_as_the_source_has_it(source):
    source("cz")
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert my("select concat(default_character_set_name, ' ',"
              " default_collation_name) from information_schema.schemata"
              " where schema_name = 'cz'") == "latin1 latin1_swedish_ci"
    assert my("select count(*) from cz.a_child") == "2"


def test_a_default_the_source_accepted_is_accepted_on_the_target(source):
    """The source kept a zero date default from a laxer time; a strict
    target refuses the same definition typed in plainly."""
    source("cy")
    my("set session sql_mode = '';"
       " create table cx.legacy (id int primary key,"
       "  at datetime not null default '0000-00-00 00:00:00');"
       " insert into cx.legacy (id) values (1)")
    my("create database cy")
    plain = my("create table cy.probe (id int primary key,"
               " at datetime not null default '0000-00-00 00:00:00')",
               ok=False)
    assert plain.returncode != 0 and "Invalid default value" in \
        plain.stderr, plain.stderr
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert my("select count(*) from cy.legacy") == "1"


def test_a_table_the_target_does_not_have_received_nothing(source):
    """The guard after a move asked the target about every source table,
    failed on the first one it did not have, and gave up on the move."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    source("cy")
    my("create database cy; create table cy.kept (id int primary key,"
       " v varchar(20)); insert into cy.kept values (1, 'k')")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="mc", engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"})
    assert MySQLEngine(hop).moved_nothing("cx") == ["a_child", "audit",
                                                    "z_parent"]
