"""If a unique/pk text column lands on a case- or accent-insensitive collation
on the target, rows that were distinct on the source collapse into duplicates
on load - silent data loss the row counts can't reveal until it's too late.
The deep check groups the source under the target collation and counts the
collisions."""
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


def test_unique_collapse_under_ci_collation_flagged(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    ci = ("create collation ci (provider = icu, locale = 'und-u-ks-level2',"
          " deterministic = false);")
    # source: case-sensitive unique key holding 'Foo' and 'foo' (distinct)
    psql(pg_pair["src"], ci + " create table t(id int primary key,"
         " name text unique); insert into t values (1,'Foo'),(2,'foo')")
    # target: same unique key but on a case-insensitive collation -> collapse
    psql(pg_pair["dst"], ci + " create table t(id int primary key,"
         " name text collate ci unique); insert into t values (1,'Foo')")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "collation" in out and "collapse" in out, r.stdout
    assert "t.name" in r.stdout, r.stdout
