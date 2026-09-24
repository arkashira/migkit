"""After the table copier, the target's sequences are past its rows and its
statistics are read.

The table copier writes each row with the key the source gave it. A
PostgreSQL target's sequence does not move for a key it did not hand out.
The application's first insert after cutover then collides with a row the
copy wrote, on `duplicate key value violates unique constraint`. That held
for every path that copies table by table into PostgreSQL:
* PostgreSQL's own copier
* the copier between engines, and the column-mapping pair
The bulk paths already carried the source's sequences. A target never
analysed also plans every query on no statistics.

Now the copy ends by settling the target, whichever path wrote it:
* the source's sequence values are carried where the source is
  PostgreSQL, never below a row the target holds
* from any other engine, every sequence that owns a column is raised past
  that column's largest value
* the loaded tables are analysed
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _dst(pg_pair, sql):
    got = psql(pg_pair["dst"], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _conf(tmp_path, monkeypatch, text):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(text)
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


@pytest.fixture
def pg_tables(pg_pair):
    psql(pg_pair["src"], "create table public.t (id serial primary key,"
                         " v text); insert into public.t (v) values ('a'),"
                         " ('b'), ('c'); select setval('public.t_id_seq', 10)")
    psql(pg_pair["dst"], "create table public.t (id serial primary key,"
                         " v text)")
    yield
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.t")


def test_postgres_own_copier_carries_the_sequence(pg_pair, pg_tables,
                                                  tmp_path, monkeypatch):
    _conf(tmp_path, monkeypatch,
          "hops:\n  sq:\n    engine: postgres\n"
          f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
          " user: postgres, password: test}\n"
          f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
          " user: postgres, password: test}\n"
          "    databases: [postgres]\n")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    got, said = _run("move", "sq", "--go")
    assert got.exit_code == 0, said
    # the source's value, which is past its rows
    assert _dst(pg_pair, "insert into public.t (v) values ('new')"
                         " returning id").splitlines()[0] == "11", said
    assert float(_dst(pg_pair, "select reltuples from pg_class where"
                               " oid = 'public.t'::regclass")) >= 3


MY, MY_PORT = "migkit-test-settle-my", 15759


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


@pytest.mark.parametrize("via", ["builtin"])
def test_between_engines_the_sequence_is_raised_past_the_rows(
        mysql_server, pg_pair, tmp_path, monkeypatch, via):
    my("drop database if exists cx; create database cx;"
       " create table cx.t (id int auto_increment primary key, v text);"
       " insert into cx.t (v) values ('a'), ('b'), ('c')")
    psql(pg_pair["dst"], "drop table if exists public.t;"
                         " create table public.t (id serial primary key,"
                         " v text)")
    try:
        _conf(tmp_path, monkeypatch,
              "hops:\n  mp:\n    engine: hetero\n"
              f"    source: {{host: 127.0.0.1, port: {MY_PORT}, user: root,"
              " password: test}\n"
              f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
              " user: postgres, password: test}\n"
              "    databases: [cx]\n    db_map: {cx: postgres}\n"
              "    options: {source_engine: mysql, target_engine: postgres}\n")
        monkeypatch.setenv("MIGKIT_MOVER", via)
        got, said = _run("move", "mp", "--go")
        assert got.exit_code == 0, said
        assert _dst(pg_pair, "select count(*) from public.t") == "3", said
        assert _dst(pg_pair, "insert into public.t (v) values ('new')"
                             " returning id").splitlines()[0] == "4", said
        assert float(_dst(pg_pair, "select reltuples from pg_class where"
                                   " oid = 'public.t'::regclass")) >= 3
    finally:
        psql(pg_pair["dst"], "drop table if exists public.t")


def test_the_check_and_the_repair_catch_what_the_tail_wrote(
        mysql_server, pg_pair, tmp_path, monkeypatch):
    """The tail writes each change with the source's key too. What it
    inserted after the copy leaves the sequence behind again, and the
    cutover would collide on it: `check` says so, and `sync --kind
    sequences` raises it."""
    my("drop database if exists cx; create database cx;"
       " create table cx.t (id int auto_increment primary key, v text);"
       " insert into cx.t (v) values ('a')")
    psql(pg_pair["dst"], "drop table if exists public.t;"
                         " create table public.t (id serial primary key,"
                         " v text)")
    try:
        _conf(tmp_path, monkeypatch,
              "hops:\n  mp:\n    engine: hetero\n"
              f"    source: {{host: 127.0.0.1, port: {MY_PORT}, user: root,"
              " password: test}\n"
              f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
              " user: postgres, password: test}\n"
              "    databases: [cx]\n    db_map: {cx: postgres}\n"
              "    options: {source_engine: mysql, target_engine: postgres}\n")
        got, said = _run("check", "mp", "--only", "autoinc")
        assert got.exit_code == 0, said
        # what a tail does: the source's key, written as it is
        psql(pg_pair["dst"], "insert into public.t values (7, 'from the"
                             " tail')")
        got, said = _run("check", "mp", "--only", "autoinc")
        assert got.exit_code != 0 and "public.t_id_seq" in said, said
        assert "largest public.t.id is 7" in said, said
        got, said = _run("sync", "mp", "--db", "cx", "--kind", "sequences",
                         "--apply")
        assert got.exit_code == 0, said
        assert _dst(pg_pair, "insert into public.t (v) values ('new')"
                             " returning id").splitlines()[0] == "8", said
    finally:
        psql(pg_pair["dst"], "drop table if exists public.t")
