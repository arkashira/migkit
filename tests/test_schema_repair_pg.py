"""Prove --kind schema repair: atlas-generated DDL brings the target's
objects (a missing column and index here) into line with the source, and
re-check goes clean. Structure only - no data touched."""
import os
import shutil
import subprocess
import textwrap

import pytest

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

if not shutil.which("atlas", path=os.environ.get("PATH", "")
                     + ":/opt/homebrew/bin:/usr/local/bin"):
    pytest.skip("atlas not installed", allow_module_level=True)


def _migkit(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [os.path.join(base, ".venv", "bin", "migkit"), *args],
        capture_output=True, text=True, env=env)


def test_schema_repair_aligns_objects(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    # source: column `extra` + index on v; target missing both
    psql(pg_pair["src"], "create table t (id int primary key, v int,"
         " extra text); create index t_v_idx on t(v);"
         " insert into t values (1,10,'a')")
    psql(pg_pair["dst"], "create table t (id int primary key, v int);"
         " insert into t values (1,10)")

    r = _migkit(conf, "check", "t", "--only", "schema")
    assert r.returncode == 1
    assert "DIFF" in r.stdout

    # dry-run shows atlas DDL, no change yet
    r = _migkit(conf, "sync", "t", "--db", "postgres", "--kind", "schema")
    assert "atlas" in r.stdout.lower()
    cols = psql(pg_pair["dst"], "select count(*) from information_schema.columns"
                " where table_name='t'").stdout.strip()
    assert cols == "2"  # still missing `extra`

    r = _migkit(conf, "sync", "t", "--db", "postgres", "--kind", "schema",
                "--apply")
    assert "applied" in r.stdout

    # target now has the column and the index
    cols = psql(pg_pair["dst"], "select count(*) from information_schema.columns"
                " where table_name='t'").stdout.strip()
    assert cols == "3"
    idx = psql(pg_pair["dst"], "select count(*) from pg_indexes"
               " where tablename='t' and indexname='t_v_idx'").stdout.strip()
    assert idx == "1"

    r = _migkit(conf, "check", "t", "--only", "schema")
    assert r.returncode == 0, r.stdout
