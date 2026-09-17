"""A partitioned table can migrate its parent rows while a mover misses a
child partition - the rows then fall into the DEFAULT catch-all instead of
their real partition, silently changing routing. The deep check compares the
bound set and counts rows stranded in the default partition."""
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


def test_missing_partition_and_stranded_rows_flagged(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    base = ("create table events(id int, ts date, primary key(id, ts))"
            " partition by range (ts);"
            " create table events_2025 partition of events"
            " for values from ('2025-01-01') to ('2026-01-01');")
    # source has the 2026 partition; target does not, so the 2026 row strands
    psql(pg_pair["src"], base
         + " create table events_2026 partition of events"
           " for values from ('2026-01-01') to ('2027-01-01');"
           " create table events_def partition of events default;"
           " insert into events values (1,'2025-06-01'),(2,'2026-06-01')")
    psql(pg_pair["dst"], base
         + " create table events_def partition of events default;"
           " insert into events values (1,'2025-06-01'),(2,'2026-06-01')")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "partitions" in out, r.stdout
    assert "stranded" in out or "missing" in out, r.stdout
    assert "events" in r.stdout, r.stdout
