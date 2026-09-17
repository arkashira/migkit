"""Row-level security is a silent-data-loss trap: a table with RLS enabled but
NO policies is default-deny (reads as empty to non-owners), and a dump taken by
a role subject to RLS exports only a filtered subset. The deep check must
surface RLS tables on its own, auto-detected from the catalog."""
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


def test_rls_zero_policy_flagged_by_deep(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, "create table secret(id int primary key, v int);"
                " insert into secret select g, g from generate_series(1,10) g;"
                " alter table secret enable row level security")  # no policy

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "rls" in out and "zero policies" in out, r.stdout
    assert "secret" in r.stdout, r.stdout
