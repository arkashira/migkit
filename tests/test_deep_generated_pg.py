"""A stored generated column whose expression drifted between source and
target (or that is generated on one side and plain on the other) silently
diverges going forward. The deep check compares the generation expression and,
for stored columns, verifies the stored value still equals its expression."""
import os
import subprocess
import textwrap

from tests.conftest import needs_docker, psql, MIGKIT

pytestmark = needs_docker


def _migkit(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [MIGKIT, *args],
        capture_output=True, text=True, env=env)


def test_generation_expression_drift_flagged(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    # same column name, different generation expression (a*2 vs a*3)
    psql(pg_pair["src"], "create table t(id int primary key, a int,"
         " s int generated always as (a * 2) stored);"
         " insert into t(id,a) values (1,5)")
    psql(pg_pair["dst"], "create table t(id int primary key, a int,"
         " s int generated always as (a * 3) stored);"
         " insert into t(id,a) values (1,5)")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "generated" in out and "expression differs" in out, r.stdout
    assert "t.s" in r.stdout, r.stdout
