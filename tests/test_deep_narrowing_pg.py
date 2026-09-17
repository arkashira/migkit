"""A target column narrower than the source silently truncates, rounds, or
overflows values while the migration reports success. The deep check must
call this out on its own (higher severity than cosmetic drift), auto-detected
from the catalog: shorter varchar, less numeric scale/precision, smaller int."""
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


def test_narrowing_detected_by_deep(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    psql(pg_pair["src"], "create table t(id int primary key,"
         " name varchar(100), price numeric(12,4), big bigint, note text)")
    # target: name capped shorter, price fewer decimals, big smaller int,
    # note capped from unlimited text -> varchar
    psql(pg_pair["dst"], "create table t(id int primary key,"
         " name varchar(50), price numeric(12,2), big integer,"
         " note varchar(200))")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "narrower" in out or "truncation" in out, r.stdout
    assert "name" in r.stdout and "50" in r.stdout, r.stdout          # varchar
    assert "scale" in out, r.stdout                                    # numeric
    assert "bigint" in r.stdout and "integer" in r.stdout, r.stdout    # int
