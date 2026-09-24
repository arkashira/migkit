"""Columns kept, dropped and renamed on a PostgreSQL or MySQL hop (backlog 9).

`mapping.columns` was read by the cross-engine copier and comparison only.
A PostgreSQL or MySQL hop copies and hashes whole rows on its own paths, so
the mapping was refused there. Now a table whose columns the hop maps goes
through the pair machinery that reads the mapping:
* the plan routes it out of the bulk copy
* the pair's copier builds and fills it
* the pair compares it, and repairs it
* the pair's change tail follows it
Every other table still takes the fast path. The whole-database schema
comparers leave the mapped tables to the pair's column-by-column check, so
a dropped or renamed column is not reported as a difference.

The tail used to stop on the first change to a mapped table, on `Unknown
column 'name'`. With both columns on the target, it would have written the
wrong one.
"""
import subprocess
import threading
import time

import pytest
from click.testing import CliRunner

from migkit import movers
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

SOURCE = ("create table public.people (id int primary key, full_name text,"
          " secret text, age int);"
          " insert into public.people values (1, 'Ann', 'x', 30),"
          " (2, 'Bo', 'y', 41);"
          " create table public.plain (id int primary key, v text);"
          " insert into public.plain values (1, 'p'), (2, 'q')")

MAPPING = ("    mapping:\n      columns:\n        people:\n"
           "          drop: [secret]\n          rename: {full_name: name}\n")


@pytest.fixture
def pg_hop(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  cm:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n" + MAPPING)
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    psql(pg_pair["src"], SOURCE)
    yield tmp_path
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "select pg_drop_replication_slot(slot_name)"
                   " from pg_replication_slots;"
                   " drop table if exists public.people, public.plain")


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _dst(pg_pair, sql):
    got = psql(pg_pair["dst"], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.mark.parametrize("via", ["pgdump", "builtin"])
def test_postgres_moves_checks_and_repairs_through_the_mapping(
        pg_hop, pg_pair, monkeypatch, via):
    if via == "pgdump" and not movers.which("pg_dump"):
        pytest.skip("the PostgreSQL dump programs are not installed")
    monkeypatch.setenv("MIGKIT_MOVER", via)
    got, said = _run("move", "cm", "--go")
    assert got.exit_code == 0, said
    assert _dst(pg_pair, "select string_agg(column_name, ',' order by"
                         " column_name) from information_schema.columns"
                         " where table_name = 'people'") == "age,id,name"
    assert _dst(pg_pair, "select string_agg(id || '=' || name || '/' || age,"
                         " ',' order by id) from people") == "1=Ann/30,2=Bo/41"
    assert _dst(pg_pair, "select count(*) from plain") == "2"
    got, said = _run("check", "cm", "--only", "schema,data")
    assert got.exit_code == 0, said
    assert "secret" not in said and "full_name" not in said, said
    # a renamed value that differs is still one
    psql(pg_pair["dst"], "update people set name = 'Anne' where id = 1")
    got, said = _run("check", "cm", "--only", "data")
    assert got.exit_code != 0 and "people" in said, said
    got, said = _run("sync", "cm", "--db", "postgres", "--kind", "rows",
                     "--apply")
    assert got.exit_code == 0, said
    assert _dst(pg_pair, "select name from people where id = 1") == "Ann"
    got, said = _run("check", "cm", "--only", "data")
    assert got.exit_code == 0, said


def test_a_target_made_by_hand_is_compared_through_the_mapping(
        pg_hop, pg_pair, monkeypatch):
    """The usual shape: the target's table was made the way the mapping
    says, and has its own index on the renamed column."""
    psql(pg_pair["dst"], "create table public.people (id int primary key,"
                         " name text, age int); create index people_name on"
                         " public.people (name);"
                         " create table public.plain (id int primary key,"
                         " v text)")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    got, said = _run("move", "cm", "--go")
    assert got.exit_code == 0, said
    got, said = _run("check", "cm", "--only", "schema,data")
    assert got.exit_code == 0, said
    # and a column the target lacks is still a difference
    psql(pg_pair["dst"], "alter table public.people drop column age")
    got, said = _run("check", "cm", "--only", "schema")
    assert got.exit_code != 0 and "age" in said, said


def test_postgres_changes_follow_the_mapping(pg_hop, pg_pair, monkeypatch):
    from migkit import cli
    from migkit.config import get_hop
    from migkit.engines import get_engine
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")
    got, said = _run("move", "cm", "--go")
    assert got.exit_code == 0, said
    hop = get_hop("cm")
    eng = get_engine(hop)
    # the server's own replication would apply whole rows
    with pytest.raises(SystemExit) as e:
        cli._replicate(hop, eng, "postgres", False, False, False)
    assert "maps columns" in str(e.value)
    tailer = cli._tail_pair(eng)
    lines, done = [], threading.Event()

    def run():
        try:
            cli._tail(hop, tailer, "postgres", True)
        except BaseException as err:
            lines.append(repr(err))
        finally:
            done.set()
    import ctypes
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(4)
    psql(pg_pair["src"], "insert into people values (3, 'Cy', 'z', 5);"
                         " update people set full_name = 'Bob' where id = 2")
    time.sleep(6)
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    assert done.wait(timeout=30)
    assert _dst(pg_pair, "select string_agg(id || '=' || name, ','"
                         " order by id) from people") == "1=Ann,2=Bob,3=Cy", \
        lines


def test_the_delta_loop_compares_through_the_mapping(pg_hop, pg_pair,
                                                     monkeypatch):
    """It hashed whole rows under the source's names, which the target's
    mapped table does not have."""
    from migkit.config import get_hop
    from migkit.engines import get_engine
    monkeypatch.setenv("MIGKIT_MOVER", "pgdump")
    got, said = _run("move", "cm", "--go")
    assert got.exit_code == 0, said
    eng = get_engine(get_hop("cm"))
    eng.delta_verify("postgres")
    psql(pg_pair["src"], "update people set full_name = 'Bob' where id = 2;"
                         " update people set secret = 'q' where id = 1")
    got = {r.scope: r for r in eng.delta_verify("postgres")}
    people = got["postgres.public.people"]
    assert people.status == "diff" and "changed=1" in people.detail, \
        [r.__dict__ for r in got.values()]
    # the dropped column changing is not a difference; the renamed one
    # arriving is the end of one
    psql(pg_pair["dst"], "update people set name = 'Bob' where id = 2")
    got = {r.scope: r for r in eng.delta_verify("postgres")}
    assert got["postgres.public.people"].status == "ok", \
        [r.__dict__ for r in got.values()]
    eng.delta_teardown("postgres")


MY, MY_PORT = "migkit-test-colmap-my", 15754


def my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_server():
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


@pytest.fixture
def my_hop(mysql_server, tmp_path, monkeypatch):
    import migkit.config as cfg
    my("drop database if exists cx; drop database if exists cy;"
       " create database cx; create table cx.people (id int primary key,"
       " full_name text, secret text, age int);"
       " insert into cx.people values (1, 'Ann', 'x', 30), (2, 'Bo', 'y', 41);"
       " create table cx.plain (id int primary key, v text);"
       " insert into cx.plain values (1, 'p'), (2, 'q')")
    ep = (f"{{host: 127.0.0.1, port: {MY_PORT}, user: root,"
          " password: test}")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  mm:\n    engine: mysql\n"
        f"    source: {ep}\n    target: {ep}\n"
        "    databases: [cx]\n    db_map: {cx: cy}\n" + MAPPING)
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


@pytest.mark.parametrize("via", ["mydumper", "builtin"])
def test_mysql_moves_checks_and_repairs_through_the_mapping(
        my_hop, monkeypatch, via):
    if via == "mydumper" and not movers.which("mydumper"):
        pytest.skip("the MySQL dump programs are not installed")
    monkeypatch.setenv("MIGKIT_MOVER", via)
    got, said = _run("move", "mm", "--go")
    assert got.exit_code == 0, said
    assert my("select group_concat(column_name order by column_name) from"
              " information_schema.columns where table_schema = 'cy'"
              " and table_name = 'people'") == "age,id,name"
    assert my("select group_concat(concat(id, '=', name, '/', age)"
              " order by id) from cy.people") == "1=Ann/30,2=Bo/41"
    assert my("select count(*) from cy.plain") == "2"
    got, said = _run("check", "mm", "--only", "schema,data")
    assert got.exit_code == 0, said
    assert "secret" not in said and "full_name" not in said, said
    my("update cy.people set name = 'Anne' where id = 1")
    got, said = _run("check", "mm", "--only", "data")
    assert got.exit_code != 0 and "people" in said, said
    got, said = _run("sync", "mm", "--db", "cx", "--kind", "rows", "--apply")
    assert got.exit_code == 0, said
    assert my("select name from cy.people where id = 1") == "Ann"
