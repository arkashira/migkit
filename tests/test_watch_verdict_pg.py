"""End-to-end: watch on a quiesced, caught-up pair reaches the SAFE TO CUT
OVER verdict on its own - the operator does not have to reason about it."""
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


def test_watch_reaches_safe_verdict_on_quiesced_pair(pg_pair, tmp_path):
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
        psql(p, "create table orders(id int primary key, amt int);"
                " insert into orders select g, g from generate_series(1,50) g;"
                " analyze orders")  # n_live_tup needs stats to be populated

    r = _migkit(conf, "watch", "t", "--count", "3", "--interval", "1")
    assert "safe to cut over" in r.stdout.lower(), r.stdout
