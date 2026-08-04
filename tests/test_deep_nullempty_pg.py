"""Oracle collapses '' to NULL while Postgres keeps them distinct, so a
migration can silently turn NULLs into empty strings (or the reverse),
flipping IS NULL checks and unique behavior. The counts stay 'present' either
way, so the deep check compares the NULL-vs-empty split per text column."""
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


def test_null_vs_empty_flip_flagged(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    # source: two NULLs; target: those became empty strings (Oracle-style flip)
    psql(pg_pair["src"], "create table t(id int primary key, name text);"
         " insert into t values (1,null),(2,null),(3,'a')")
    psql(pg_pair["dst"], "create table t(id int primary key, name text);"
         " insert into t values (1,''),(2,''),(3,'a')")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "null" in out and "empty" in out, r.stdout
    assert "t.name" in r.stdout, r.stdout
