"""A table migkit builds on another engine keeps its columns' rules.

Measured before, a MySQL table built on PostgreSQL:
    status varchar(10) not null default 'new'   ->   status varchar(10)
    id int auto_increment                        ->   id integer
The application's first insert after cutover that left out `status` stored
NULL, and one that left out `id` was refused. The check compared column
names and kinds, so it called the two tables the same.

Now the table is built with:
* NOT NULL where the source has it
* each default, translated into the target's SQL and asked of the target
  before it is used; one the target does not take is named and left off
* the engine numbering the column where the source's did
And the check compares those rules too.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, MY_PORT = "migkit-test-rules-my", 15760


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


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


def _dst(pg_pair, sql):
    got = psql(pg_pair["dst"], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _hop(tmp_path, monkeypatch, src, dst, pg_pair, db_map):
    import migkit.config as cfg
    ends = {"mysql": f"{{host: 127.0.0.1, port: {MY_PORT}, user: root,"
                     " password: test}",
            "postgres": f"{{host: 127.0.0.1, port: {pg_pair['dst']},"
                        " user: postgres, password: test}"}
    if src == "postgres":
        ends["postgres"] = (f"{{host: 127.0.0.1, port: {pg_pair['src']},"
                            " user: postgres, password: test}")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  rules:\n    engine: hetero\n"
        f"    source: {ends[src]}\n    target: {ends[dst]}\n"
        f"    databases: [{list(db_map)[0]}]\n    db_map: {db_map}\n"
        f"    options: {{source_engine: {src}, target_engine: {dst}}}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")


def test_mysql_to_postgres(mysql_server, pg_pair, tmp_path, monkeypatch):
    my("drop database if exists cx; create database cx;"
       " create table cx.t (id int auto_increment primary key,"
       "  name varchar(20) not null,"
       "  status varchar(10) not null default 'new',"
       "  created datetime default current_timestamp,"
       "  token varchar(36) default (uuid()), score int default 0,"
       "  unique key t_name (name), key t_score (score),"
       "  key t_prefix (token(8)));"
       " insert into cx.t (name) values ('a'), ('b')")
    psql(pg_pair["dst"], "drop table if exists public.t")
    try:
        _hop(tmp_path, monkeypatch, "mysql", "postgres", pg_pair,
             {"cx": "postgres"})
        got, said = _run("move", "rules", "--go")
        assert got.exit_code == 0, said
        row = _dst(pg_pair, "insert into public.t (name) values ('z')"
                            " returning id, status, created is not null,"
                            " token is not null, score").splitlines()[0]
        assert row == "3|new|t|t|0", (row, said)
        refused = psql(pg_pair["dst"], "insert into public.t (id)"
                                       " values (9)")
        assert "not-null" in refused.stderr, refused.stderr
        # the source's indexes, after the rows; a prefix is not an index
        # another engine can be given, and is named
        assert _dst(pg_pair, "select string_agg(indexname, ',' order by"
                             " indexname) from pg_indexes where tablename ="
                             " 't' and indexname <> 't_pkey'") == \
            "t_name,t_score", said
        assert "index t_prefix not carried" in said, said
        dup = psql(pg_pair["dst"], "insert into public.t (name) values"
                                   " ('a')")
        assert "duplicate key" in dup.stderr, dup.stderr
        got, said = _run("check", "rules", "--only", "schema")
        assert got.exit_code == 0, said
        # a unique index the target lost is a difference
        psql(pg_pair["dst"], "drop index public.t_name")
        got, said = _run("check", "rules", "--only", "schema")
        assert got.exit_code != 0, said
        assert "unique over (name) on the source and not on the target" \
            in said, said
        psql(pg_pair["dst"], "create unique index t_name on public.t"
                             " (name)")
        # a default the target lost is a difference
        psql(pg_pair["dst"], "alter table public.t alter column status"
                             " drop default")
        got, said = _run("check", "rules", "--only", "schema")
        assert got.exit_code != 0, said
        assert "status defaults to 'new' on the source and to nothing" \
            in said, said
    finally:
        psql(pg_pair["dst"], "drop table if exists public.t")


def test_postgres_to_mysql(mysql_server, pg_pair, tmp_path, monkeypatch):
    psql(pg_pair["src"], "drop table if exists public.u;"
                         " create table public.u (id serial primary key,"
                         " name text not null,"
                         " status text not null default 'new',"
                         " created timestamptz default now(),"
                         " token uuid default gen_random_uuid(),"
                         " code varchar(20));"
                         " create unique index u_code on public.u (code);"
                         " insert into public.u (name) values ('a'), ('b')")
    my("drop database if exists cy; create database cy")
    try:
        _hop(tmp_path, monkeypatch, "postgres", "mysql", pg_pair,
             {"postgres": "cy"})
        got, said = _run("move", "rules", "--go")
        assert got.exit_code == 0, said
        my("insert into cy.u (name) values ('z')")
        assert my("select concat_ws('|', id, status, created is not null,"
                  " token is not null) from cy.u where name = 'z'") == \
            "3|new|1|1", said
        assert my("select count(*) from information_schema.statistics"
                  " where table_schema = 'cy' and index_name = 'u_code'"
                  " and non_unique = 0") == "1", said
        got, said = _run("check", "rules", "--only", "schema")
        assert got.exit_code == 0, said
    finally:
        psql(pg_pair["src"], "drop table if exists public.u")
