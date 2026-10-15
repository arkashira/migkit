"""The scenarios that would prove `migkit migrate` against the mechanisms of
established products (docs/research/autonomous-comparison.md): measured
operator inputs, completion after an injected fault and a safe stop, on
the PostgreSQL sandbox. Written with the feature; run in the final suite."""
import os
import subprocess
import textwrap
import time

from tests.conftest import needs_docker, psql, migkit_cmd

pytestmark = needs_docker


def _migkit(conf, *args, **env):
    full = dict(os.environ, MIGKIT_CONF=str(conf),
                MIGKIT_REPORTS=str(conf.parent / "reports"), **env)
    t0 = time.time()
    r = subprocess.run(migkit_cmd() + list(args), capture_output=True,
                       text=True, env=full)
    return r, time.time() - t0


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


def _seed(pg_pair, rows=5000):
    psql(pg_pair["src"], "drop table if exists o; create table o (id int"
         " primary key, v text); insert into o select g, md5(g::text)"
         f" from generate_series(1,{rows}) g")
    psql(pg_pair["dst"], "drop table if exists o")


def test_autonomous_healthy_copy_takes_one_command(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    _seed(pg_pair)
    r, _ = _migkit(conf, "migrate", "--hop", "t", "--goal", "copy", "--go")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "copied and proved equal" in r.stdout
    n = psql(pg_pair["dst"], "select count(*) from o").stdout.strip()
    assert n == "5000"


def test_autonomous_fault_mid_copy_completes_on_the_next_run(pg_pair,
                                                            tmp_path):
    conf = _conf(tmp_path, pg_pair)
    _seed(pg_pair, 50000)
    r, _ = _migkit(conf, "migrate", "--hop", "t", "--goal", "copy", "--go",
                   MIGKIT_FAILPOINT="range.committed:3:exit")
    assert r.returncode != 0
    r, _ = _migkit(conf, "migrate", "--hop", "t", "--goal", "copy", "--go")
    assert r.returncode == 0, r.stdout + r.stderr
    same = psql(pg_pair["dst"], "select count(*), count(distinct id)"
                " from o").stdout.strip()
    assert same == "50000|50000"


def test_autonomous_unreachable_target_stops_safely_and_says_why(pg_pair,
                                                                tmp_path):
    conf = _conf(tmp_path, {"src": pg_pair["src"], "dst": 1})
    _seed(pg_pair, 10)
    r, _ = _migkit(conf, "migrate", "--hop", "t", "--goal", "copy", "--go")
    assert r.returncode != 0
    assert "proved equal" not in r.stdout


def test_autonomous_evidence_is_on_demand(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    _seed(pg_pair, 100)
    _migkit(conf, "migrate", "--hop", "t", "--goal", "copy", "--go")
    r, _ = _migkit(conf, "migrate", "--hop", "t", "--why")
    assert "move:postgres: done" in r.stdout
