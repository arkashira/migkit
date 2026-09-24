"""A schema fix and its undo are rehearsed before the fix is applied.

The undo written beside every schema fix had only ever run in tests, so
the first time anyone relied on one would have been on the day, on the
target. Now `sync --kind schema --apply` first runs the fix and then the
undo on a scratch database on the target's own server, holding a copy of
the target's schema, and compares the schema with what it was:
* a fix that fails there is not applied to the target
* an undo that does not return the schema exactly is said, with the
  lines in `rehearsal.diff`
* the scratch database is dropped afterwards, whatever happened
"""
import os
import shutil

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.base import RepairAction
from tests.conftest import needs_docker, psql

pytestmark = [needs_docker, pytest.mark.skipif(
    not shutil.which("atlas", path=os.environ.get("PATH", "")
                     + ":/opt/homebrew/bin:/usr/local/bin"),
    reason="the schema differ is not installed")]


def _eng(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="reh", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _scratch_left(pg_pair):
    return psql(pg_pair["dst"], "select count(*) from pg_database where"
                " datname like 'migkit_rehearsal%'").stdout.strip()


def _repair(eng, monkeypatch):
    from migkit import cli
    shown = []
    monkeypatch.setattr(cli.console, "print",
                        lambda *a, **k: shown.append(" ".join(map(str, a))))
    cli._repair_one(eng.hop, eng, "postgres", "schema", True)
    return " | ".join(shown)


def test_a_fix_is_rehearsed_before_it_is_applied(pg_pair, tmp_path,
                                                 monkeypatch):
    psql(pg_pair["src"], "create table public.t (id int primary key, v int,"
                         " extra text); create index t_v on public.t (v)")
    psql(pg_pair["dst"], "create table public.t (id int primary key,"
                         " v int)")
    said = _repair(_eng(pg_pair, tmp_path), monkeypatch)
    assert "rehearsed on a copy of the target's schema: the fix applies," \
        " and the undo returns the schema exactly" in said, said
    assert "applied" in said, said
    assert psql(pg_pair["dst"], "select count(*) from pg_indexes where"
                " indexname = 't_v'").stdout.strip() == "1"
    assert _scratch_left(pg_pair) == "0"


def test_a_fix_that_fails_on_the_copy_is_not_applied(pg_pair, tmp_path,
                                                     monkeypatch):
    psql(pg_pair["dst"], "create table public.t (id int primary key)")
    eng = _eng(pg_pair, tmp_path)
    monkeypatch.setattr(eng, "repair_plan", lambda db, kind: [RepairAction(
        db, "schema", ["alter table public.t add column a int;",
                       "alter table public.nowhere add column b int;"],
        ["alter table public.t drop column a;"], "")])
    with pytest.raises(SystemExit) as e:
        _repair(eng, monkeypatch)
    assert "the fix fails on a copy of the target's schema" in str(e.value)
    # nothing reached the target
    assert psql(pg_pair["dst"], "select count(*) from information_schema"
                ".columns where table_name = 't' and column_name = 'a'"
                ).stdout.strip() == "0"
    assert _scratch_left(pg_pair) == "0"


def test_an_undo_that_does_not_return_it_is_said(pg_pair, tmp_path,
                                                 monkeypatch):
    psql(pg_pair["dst"], "create table public.t (id int primary key,"
                         " n int not null default 0)")
    eng = _eng(pg_pair, tmp_path)
    # the undo puts the column back without what it had
    monkeypatch.setattr(eng, "repair_plan", lambda db, kind: [RepairAction(
        db, "schema", ["alter table public.t drop column n;"],
        ["alter table public.t add column n int;"], "")])
    said = _repair(eng, monkeypatch)
    assert "the undo does NOT return the schema exactly" in said, said
    assert (tmp_path / "rehearsal.diff").exists()
    assert _scratch_left(pg_pair) == "0"


# ---- MySQL, whose DDL is not transactional: a fix that fails part of the
# way through on the target is left half applied there ----

MY, MY_PORT = "migkit-test-rehearse-my", 15758


def my(sql):
    import subprocess
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_server():
    import subprocess
    import time
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _my_eng(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    hop = Hop(name="reh", engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _my_repair(eng, monkeypatch):
    from migkit import cli
    shown = []
    monkeypatch.setattr(cli.console, "print",
                        lambda *a, **k: shown.append(" ".join(map(str, a))))
    cli._repair_one(eng.hop, eng, "cx", "schema", True)
    return " | ".join(shown)


def _my_scratch_left():
    return my("select count(*) from information_schema.schemata where"
              " schema_name like 'migkit_rehearsal%'")


@pytest.fixture
def my_dbs(mysql_server):
    my("drop database if exists cx; drop database if exists cy;"
       " create database cx; create database cy;"
       " create table cx.t (id int primary key, v int, extra text,"
       "  index t_v (v));"
       " create table cy.t (id int primary key, v int)")


def test_mysql_a_fix_is_rehearsed_before_it_is_applied(my_dbs, tmp_path,
                                                       monkeypatch):
    said = _my_repair(_my_eng(tmp_path), monkeypatch)
    assert "rehearsed on a copy of the target's schema: the fix applies," \
        " and the undo returns the schema exactly" in said, said
    assert my("select count(*) from information_schema.statistics where"
              " table_schema = 'cy' and index_name = 't_v'") == "1"
    assert _my_scratch_left() == "0"


def test_mysql_a_fix_that_fails_part_way_is_not_applied(my_dbs, tmp_path,
                                                        monkeypatch):
    eng = _my_eng(tmp_path)
    monkeypatch.setattr(eng, "repair_plan", lambda db, kind: [RepairAction(
        db, "schema", ["alter table t add column a int;",
                       "alter table nowhere add column b int;"],
        ["alter table t drop column a;"], "")])
    with pytest.raises(SystemExit) as e:
        _my_repair(eng, monkeypatch)
    assert "the fix fails on a copy of the target's schema" in str(e.value)
    # the first statement would have stayed on the target
    assert my("select count(*) from information_schema.columns where"
              " table_schema = 'cy' and column_name = 'a'") == "0"
    assert _my_scratch_left() == "0"


def test_mysql_an_undo_that_does_not_return_it_is_said(my_dbs, tmp_path,
                                                       monkeypatch):
    my("alter table cy.t add column n int not null default 0")
    eng = _my_eng(tmp_path)
    monkeypatch.setattr(eng, "repair_plan", lambda db, kind: [RepairAction(
        db, "schema", ["alter table t drop column n;"],
        ["alter table t add column n int;"], "")])
    said = _my_repair(eng, monkeypatch)
    assert "the undo does NOT return the schema exactly" in said, said
    assert (tmp_path / "rehearsal.diff").exists()
    assert _my_scratch_left() == "0"
