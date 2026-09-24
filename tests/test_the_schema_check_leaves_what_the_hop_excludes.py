"""The schema check leaves out the tables the hop excludes.

An excluded table is the target's own: the move leaves it alone, and the
data check neither reads nor repairs it. The whole-database schema
comparers still compared it. A target keeping its own `audit` table, shaped
differently from the source's, therefore had a schema difference that
nothing would ever clear. The fix DDL they generated would also have
rebuilt that table in the source's shape. They now leave it out, the way
they leave out the tables the pair compares through a column mapping.
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


@pytest.fixture
def pg_hop(pg_pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  ex:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    exclude: [public.audit]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.orders (id int primary key, v text)")
    psql(pg_pair["src"], "create table public.audit (id int primary key,"
                         " at timestamptz); create index audit_at on"
                         " public.audit (at)")
    psql(pg_pair["dst"], "create table public.audit (n bigint, note text)")
    yield tmp_path
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.orders, public.audit")


def test_postgres_leaves_the_excluded_table_alone(pg_hop, pg_pair):
    got, said = _run("check", "ex", "--only", "schema")
    assert got.exit_code == 0, said
    # and a difference on a table it does compare is still one
    psql(pg_pair["dst"], "alter table public.orders add column w int")
    got, said = _run("check", "ex", "--only", "schema")
    assert got.exit_code != 0 and "orders" in said, said
    # the fix touches that table and not the excluded one
    got, said = _run("sync", "ex", "--db", "postgres", "--kind", "schema")
    assert "audit" not in said, said


MY, MY_PORT = "migkit-test-schemaex-my", 15757


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


def test_mysql_leaves_the_excluded_table_alone(mysql_server, tmp_path,
                                               monkeypatch):
    import migkit.config as cfg
    my("drop database if exists cx; drop database if exists cy;"
       " create database cx; create database cy;"
       " create table cx.orders (id int primary key, v text);"
       " create table cy.orders (id int primary key, v text);"
       " create table cx.audit (id int primary key, at datetime,"
       "  index audit_at (at));"
       " create table cy.audit (n bigint, note text)")
    ep = f"{{host: 127.0.0.1, port: {MY_PORT}, user: root, password: test}}"
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  mx:\n    engine: mysql\n"
        f"    source: {ep}\n    target: {ep}\n"
        "    databases: [cx]\n    db_map: {cx: cy}\n    exclude: [audit]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    got, said = _run("check", "mx", "--only", "schema")
    assert got.exit_code == 0, said
    my("alter table cy.orders add column w int")
    got, said = _run("check", "mx", "--only", "schema")
    assert got.exit_code != 0 and "`w` int" in said, said
