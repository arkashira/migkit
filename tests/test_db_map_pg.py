"""Prove db_map end to end: a source database and a differently-named target
database compare, diff, and repair correctly through the mapping (source name
in config + report paths, target name only at the dst connection)."""
import os
import subprocess
import textwrap

from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _migkit(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [os.path.join(base, ".venv", "bin", "migkit"), *args],
        capture_output=True, text=True, env=env)


def test_pg_source_and_target_have_different_db_names(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [appsrc]
            db_map: {{appsrc: appdst}}
    """))
    try:
        psql(pg_pair["src"], "create database appsrc")
        psql(pg_pair["dst"], "create database appdst")
        for port, db in ((pg_pair["src"], "appsrc"), (pg_pair["dst"], "appdst")):
            psql(port, "create table orders (id bigint primary key, v int);"
                 " insert into orders select g, g*2 from generate_series(1,500) g;"
                 " create sequence s; select setval('s', 500);", db=db)

        # identical data in differently-named dbs -> clean
        r = _migkit(conf, "check", "t", "--only", "counts,data,autoinc")
        assert r.returncode == 0, r.stdout + r.stderr
        assert "DIFF" not in r.stdout
        assert "500" in r.stdout

        # seed a diff on the TARGET db, detect + repair across the name map
        psql(pg_pair["dst"], "delete from orders where id = 42;"
             " select setval('s', 999)", db="appdst")
        # data check writes the pk-level diff files that rows repair consumes
        r = _migkit(conf, "check", "t", "--only", "counts,data,autoinc")
        assert r.returncode == 1
        assert "DIFF" in r.stdout

        r = _migkit(conf, "sync", "t", "--db", "appsrc", "--kind", "rows",
                    "--apply")
        assert "applied" in r.stdout
        r = _migkit(conf, "sync", "t", "--db", "appsrc", "--kind",
                    "sequences", "--apply")
        assert "applied" in r.stdout

        # target row + sequence restored to match source
        n = psql(pg_pair["dst"], "select count(*) from orders",
                 db="appdst").stdout.strip()
        assert n == "500"
        r = _migkit(conf, "check", "t", "--only", "counts,data,autoinc")
        assert r.returncode == 0, r.stdout
    finally:
        psql(pg_pair["src"], "drop database if exists appsrc")
        psql(pg_pair["dst"], "drop database if exists appdst")
