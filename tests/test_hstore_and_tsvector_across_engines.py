"""A key/value set and a text search vector, carried and compared across
engines (backlog 19).

Both were left out of a cross-engine comparison as types with no shared
rendering. A text search vector prints its lexemes sorted and once each,
so its text is the value itself; a key/value set is a JSON object of
strings, which PostgreSQL's extension casts to jsonb and MySQL keeps as
JSON. An interval still has no counterpart - MySQL has no type holding
months and seconds apart - and is still left out, and said.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MY, PORT = "migkit-test-hstore-my", 15810


def _my(sql):
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
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def test_carried_and_compared_and_a_change_is_seen(mysql_server, pg_pair,
                                                   tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import cli
    got = psql(pg_pair["src"], "create extension if not exists hstore;"
                               " drop table if exists public.kv;"
                               " create table public.kv (id int primary key,"
                               " h hstore, tsv tsvector);"
                               " insert into public.kv values"
                               " (1, 'b=>2, a=>1', 'the cats sat'::tsvector),"
                               " (2, 'k=>\"with space\"', 'a b a'::tsvector),"
                               " (3, null, null)")
    assert got.returncode == 0, got.stderr
    _my("drop database if exists kvdb; create database kvdb")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  kv:\n    engine: hetero\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {PORT}, user: root,"
        " password: test}\n"
        "    databases: [postgres]\n    db_map: {postgres: kvdb}\n"
        "    options: {source_engine: postgres, target_engine: mysql}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    try:
        got = CliRunner().invoke(cli.main, ["move", "kv", "--go"])
        said = " ".join((got.output + str(got.exception or "")).split())
        assert got.exit_code == 0, said
        assert _my("select json_extract(h, '$.a'), tsv from kvdb.kv"
                   " where id = 1") == "\"1\"\t'cats' 'sat' 'the'", said
        got = CliRunner().invoke(cli.main, ["check", "kv", "--only",
                                            "data"])
        said = " ".join((got.output + str(got.exception or "")).split())
        assert got.exit_code == 0, said
        assert "no canonical rendering" not in said, said
        # a change to either is a difference
        _my("update kvdb.kv set h = json_object('k', 'changed')"
            " where id = 2")
        got = CliRunner().invoke(cli.main, ["check", "kv", "--only",
                                            "data"])
        assert got.exit_code != 0, got.output
        _my("update kvdb.kv set h = json_object('k', 'with space'),"
            " tsv = '''a'' ''c''' where id = 2")
        got = CliRunner().invoke(cli.main, ["check", "kv", "--only",
                                            "data"])
        assert got.exit_code != 0, got.output
    finally:
        psql(pg_pair["src"], "drop table if exists public.kv")
