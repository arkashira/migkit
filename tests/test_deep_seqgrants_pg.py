"""A SERIAL column's sequence has its own ACL. Granting the table but not its
sequence is a classic trap: reads work, then the app's first INSERT fails with
'permission denied for sequence'. Table-grant parity misses it, so the deep
check diffs sequence grants separately."""
import os
import subprocess
import textwrap

from tests.conftest import needs_docker, psql, MIGKIT

pytestmark = needs_docker

_ROLE = ("do $$ begin if not exists (select from pg_roles"
         " where rolname='seqapp') then create role seqapp; end if; end $$;")


def _migkit(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [MIGKIT, *args],
        capture_output=True, text=True, env=env)


def test_missing_sequence_grant_flagged(pg_pair, tmp_path):
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
        psql(p, _ROLE + " create table t(id serial primary key, v int)")
    # source granted the sequence to the app role; target did not
    psql(pg_pair["src"], "grant usage, select on sequence t_id_seq to seqapp")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "sequence grants missing" in out or "seq-grants" in out, r.stdout
    assert "seqapp" in r.stdout, r.stdout
