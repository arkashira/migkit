"""sync --mode orchestration end to end: validate exits on diff, and full
loads + reconciles an empty target to green in one command."""
import os
import shutil
import subprocess
import textwrap

import pytest

from tests.conftest import needs_docker, psql, MIGKIT

pytestmark = needs_docker

_ATLAS = shutil.which("atlas", path=os.environ.get("PATH", "")
                      + ":/opt/homebrew/bin:/usr/local/bin")


def _migkit(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [MIGKIT, *args],
        capture_output=True, text=True, env=env)


def _conf(tmp_path, ports):
    c = tmp_path / "hops.yaml"
    c.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {ports['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {ports['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    return c


def test_mode_verify_exits_on_diff(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    psql(pg_pair["src"], "create table o (id int primary key, v int);"
         " insert into o select g, g from generate_series(1,50) g")
    psql(pg_pair["dst"], "create table o (id int primary key, v int);"
         " insert into o select g, g from generate_series(1,40) g")
    r = _migkit(conf, "sync", "t", "--mode", "verify")
    assert r.returncode == 1  # diff -> non-zero, no writes


def test_mode_seed_loads_and_greens(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    psql(pg_pair["src"], "create table people (id bigint primary key, v int,"
         " note text); insert into people select g, g*2, 'n'||g"
         " from generate_series(1,2000) g;"
         " create sequence people_seq; select setval('people_seq', 2000)")
    # target: same schema, empty
    psql(pg_pair["dst"], "create table people (id bigint primary key, v int,"
         " note text); create sequence people_seq")

    r = _migkit(conf, "sync", "t", "--mode", "seed", "--go")
    assert "seed" in r.stdout.lower()

    n = psql(pg_pair["dst"], "select count(*) from people").stdout.strip()
    assert n == "2000", r.stdout + r.stderr
    seq = psql(pg_pair["dst"], "select last_value from people_seq").stdout.strip()
    assert seq == "2000"

    r = _migkit(conf, "sync", "t", "--mode", "verify")
    assert r.returncode == 0, r.stdout
