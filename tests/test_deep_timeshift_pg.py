"""When a mover applies timestamps under a non-UTC session, every row's
timestamp shifts by the same constant offset - it looks like data but it is a
systematic timezone bug. Both sides are read TimeZone=UTC pinned, so a correct
migration compares as delta 0; a uniform shift is the tell the deep check must
name (distinct from random row corruption, which the data checksum handles)."""
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


def test_uniform_timezone_shift_flagged(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    psql(pg_pair["src"], "create table ev(id int primary key, ts timestamp);"
         " insert into ev select g, timestamp '2026-01-01 07:00'"
         " + (g||' days')::interval from generate_series(1,6) g")
    # target stored the same rows 8 hours ahead: a non-UTC session on load
    psql(pg_pair["dst"], "create table ev(id int primary key, ts timestamp);"
         " insert into ev select g, timestamp '2026-01-01 15:00'"
         " + (g||' days')::interval from generate_series(1,6) g")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "timezone offset" in out or "shifted" in out, r.stdout
    assert "8.0h" in r.stdout or "28800" in r.stdout, r.stdout
