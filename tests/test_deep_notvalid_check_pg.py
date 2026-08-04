"""A CHECK constraint added NOT VALID enforces new writes but was never
scanned against existing rows, and the planner won't trust it - a load-time
speed hack teams forget to finish. The fk path already scans orphans; this
covers the check-constraint blind spot, auto-detected from the catalog."""
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


def test_not_valid_check_flagged_by_deep(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    psql(pg_pair["src"], "create table t(id int primary key, age int)")
    # target got the constraint the fast way and never validated it
    psql(pg_pair["dst"], "create table t(id int primary key, age int);"
         " alter table t add constraint age_pos check (age >= 0) not valid")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    assert "not validated" in r.stdout.lower(), r.stdout
    assert "age_pos" in r.stdout, r.stdout
