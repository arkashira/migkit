"""The proof harness of `migkit migrate` (docs/research/autonomous-comparison.md):
healthy, recoverable-failure and unrecoverable runs on the MySQL and the
PostgreSQL sandboxes, each measured the same way - the operator's inputs
and interventions, completion, recovery success, retry waste and time -
and written as one JSON line a scenario to `MIGKIT_MEASURE_OUT` where it
is set, so the comparison table is filled from runs, never from claims.
Written with the feature; run in the final suite."""
import json
import os
import subprocess
import textwrap
import time

import pytest

from tests import mysql_pair
from tests.conftest import needs_docker, psql, migkit_cmd
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

pytestmark = needs_docker

ROWS = 20000


def _migkit(conf, *args, **env):
    full = dict(os.environ, MIGKIT_CONF=str(conf),
                MIGKIT_REPORTS=str(conf.parent / "reports"), **env)
    t0 = time.time()
    r = subprocess.run(migkit_cmd() + list(args), capture_output=True,
                       text=True, env=full, stdin=subprocess.DEVNULL)
    return r, time.time() - t0


def _conf(tmp_path, engine, ports, password="test"):
    user, db = (("postgres", "postgres") if engine == "postgres"
                else ("root", "w9c"))
    c = tmp_path / "hops.yaml"
    c.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: {engine}
            source: {{host: 127.0.0.1, port: {ports['src']}, user: {user}, password: test}}
            target: {{host: 127.0.0.1, port: {ports['dst']}, user: {user}, password: {password}}}
            databases: [{db}]
    """))
    return c


class Side:
    """One engine's sandbox: seed the source, count the target."""

    def __init__(self, engine, ports):
        self.engine, self.ports = engine, ports

    def seed(self, rows=ROWS):
        if self.engine == "postgres":
            psql(self.ports["src"], "drop table if exists o; create table o"
                 " (id int primary key, v text); insert into o select g,"
                 f" md5(g::text) from generate_series(1,{rows}) g")
            psql(self.ports["dst"], "drop table if exists o")
            return
        my("src", "drop database if exists w9c; create database w9c;"
           " create table w9c.o (id int primary key, v text);"
           " insert into w9c.o with recursive g(n) as (select 1 union all"
           f" select n + 1 from g where n < {rows}) select n, md5(n) from g;")
        my("dst", "drop database if exists w9c; create database w9c;")

    def counts(self):
        """(rows, distinct keys) on the target, or None where unread."""
        sql = "select count(*), count(distinct id) from o"
        if self.engine == "postgres":
            out = psql(self.ports["dst"], sql).stdout.strip()
        else:
            got = mysql_pair._side("dst").exec(
                "mysql", "-uroot", "-ptest", "-N", "-B", "w9c", input=sql)
            out = got.stdout.strip().replace("\t", "|")
        try:
            a, b = out.split("|")
            return int(a), int(b)
        except ValueError:
            return None


def _decisions(conf):
    for p in (conf.parent / "reports").rglob("workflow.json"):
        return json.loads(p.read_text()).get("decisions") or []
    return []


def _measure(name, engine, conf, runs, side, rows=ROWS):
    """The scenario's numbers, kept where the final suite gathers them."""
    decisions = _decisions(conf)
    rec = [d for d in decisions if d.get("what") == "recover"]
    held = [d for d in rec if d.get("held") is True]
    got = side.counts()
    last = runs[-1][0]
    m = {"scenario": name, "engine": engine,
         # the arguments of the first command, and every run after it the
         # operator had to start
         "inputs": len(runs[0][2]), "interventions": len(runs) - 1,
         "completed": last.returncode == 0 and got == (rows, rows),
         "recoveries": len(rec),
         "recovered": len(held) / len(rec) if rec else None,
         # tries a recovery spent that failed again
         "retry_waste": len(rec) - len(held),
         "seconds": round(sum(t for _, t, _ in runs), 2),
         "claimed": "proved equal" in last.stdout}
    out = os.environ.get("MIGKIT_MEASURE_OUT")
    if out:
        with open(out, "a") as f:
            f.write(json.dumps(m) + "\n")
    return m


def _run(conf, *args, **env):
    r, took = _migkit(conf, *args, **env)
    return r, took, args


@pytest.fixture(params=["postgres", "mysql"])
def side(request):
    ports = request.getfixturevalue(
        "pg_pair" if request.param == "postgres" else "my_pair")
    return Side(request.param, ports)


GO = ("migrate", "--hop", "t", "--goal", "copy", "--go")


def test_a_healthy_copy_completes_in_one_command(side, tmp_path):
    conf = _conf(tmp_path, side.engine, side.ports)
    side.seed()
    runs = [_run(conf, *GO)]
    m = _measure("healthy", side.engine, conf, runs, side)
    assert runs[0][0].returncode == 0, runs[0][0].stdout + runs[0][0].stderr
    assert m["completed"] and m["interventions"] == 0
    assert "copied and proved equal" in runs[0][0].stdout
    assert m["retry_waste"] == 0


def test_a_fault_mid_copy_is_recovered_inside_the_run(side, tmp_path):
    conf = _conf(tmp_path, side.engine, side.ports)
    side.seed()
    runs = [_run(conf, *GO,
                 MIGKIT_FAILPOINT="range.committed:2:fault=connectivity")]
    m = _measure("recoverable in the run", side.engine, conf, runs, side)
    r = runs[0][0]
    assert r.returncode == 0, r.stdout + r.stderr
    # every key once: the work after the last kept position was not doubled
    assert m["completed"] and m["interventions"] == 0
    assert m["recovered"] in (None, 1.0)


def test_a_crash_mid_copy_completes_on_the_next_run(side, tmp_path):
    conf = _conf(tmp_path, side.engine, side.ports)
    side.seed()
    runs = [_run(conf, *GO, MIGKIT_FAILPOINT="range.committed:3:exit")]
    assert runs[0][0].returncode != 0
    assert "proved equal" not in runs[0][0].stdout
    runs.append(_run(conf, *GO))
    m = _measure("recoverable by a run again", side.engine, conf, runs, side)
    assert runs[-1][0].returncode == 0, runs[-1][0].stdout
    assert m["completed"] and m["interventions"] == 1


def test_a_refused_target_stops_safely_and_claims_nothing(side, tmp_path):
    conf = _conf(tmp_path, side.engine, side.ports, password="CHANGE_ME")
    side.seed(10)
    runs = [_run(conf, *GO)]
    m = _measure("unrecoverable", side.engine, conf, runs, side, rows=10)
    r = runs[0][0]
    assert r.returncode != 0
    assert not m["claimed"] and not m["completed"]
    # stopped, not retried into a storm
    assert m["retry_waste"] <= 1
    assert "migrate --hop t --why" in r.stdout


def test_the_evidence_is_on_demand(pg_pair, tmp_path):
    conf = _conf(tmp_path, "postgres", pg_pair)
    Side("postgres", pg_pair).seed(100)
    _migkit(conf, *GO)
    r, _ = _migkit(conf, "migrate", "--hop", "t", "--why")
    assert "move:postgres: done" in r.stdout
