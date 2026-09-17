"""A table with no primary key or unique index is a silent migration trap:
DMS and GoldenGate drop its UPDATE/DELETE during CDC, duplicate it on
full+CDC, and it can't be verified or repaired by key. The smart deep check
must surface it on its own, before it bites at cutover."""
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


def test_no_pk_table_flagged_by_deep(pg_pair, tmp_path):
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
        psql(p, "create table orders(id serial primary key, amt int);"
                " create table events(a int, b int)")  # events has no key

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    assert "no pk/unique" in r.stdout.lower(), r.stdout
    assert "events" in r.stdout, r.stdout
