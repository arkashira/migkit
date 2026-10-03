"""End-to-end: on a quiesced, caught-up pair the counts `watch` samples say
they converge - estimates, never "safe" (audit V14) - and a verifying
cycle of `watch --verify --delta` says SAFE TO CUT OVER on its own, the
operator not having to reason about it."""
import os
import subprocess
import textwrap

from tests.conftest import needs_docker, psql, migkit_cmd

pytestmark = needs_docker


def _migkit(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    return subprocess.run(
        migkit_cmd() + list(args),
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
    assert "counts converging (estimates)" in r.stdout, r.stdout
    assert "safe to cut over" not in r.stdout.lower(), r.stdout
    r = _migkit(conf, "watch", "t", "--verify", "--delta", "--count", "3",
                "--interval", "2")
    assert "SAFE TO CUT OVER" in r.stdout, r.stdout + r.stderr
    assert r.returncode == 0, r.stdout + r.stderr
    _migkit(conf, "watch", "t", "--teardown")
