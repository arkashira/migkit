"""DEFERRABLE / INITIALLY DEFERRED constraint properties are easy to lose when
a schema is copied; code that reorders rows inside one transaction then fails
on a constraint that was deferred on the source. The deep check diffs the
deferral flags per constraint, auto-detected from the catalog."""
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


def test_deferral_drift_flagged_by_deep(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    # source FK is deferrable; target's is the default (immediate) - same name
    psql(pg_pair["src"], "create table p(id int primary key);"
         " create table c(id int primary key, pid int references p(id)"
         " deferrable initially deferred)")
    psql(pg_pair["dst"], "create table p(id int primary key);"
         " create table c(id int primary key, pid int references p(id))")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    assert "deferr" in r.stdout.lower(), r.stdout
    assert "c_pid_fkey" in r.stdout, r.stdout
