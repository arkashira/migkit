"""End-to-end postgres tests against throwaway docker containers.

Covers the whole loop: seed a diff, check detects it, repair --apply fixes
it, re-check is clean, and a resumable move survives a simulated crash.
"""
import subprocess
import textwrap

import pytest

from tests.conftest import needs_docker, psql, MIGKIT

pytestmark = needs_docker


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


def _migkit(conf, *args):
    import os
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return subprocess.run(
        [MIGKIT, *args],
        capture_output=True, text=True, env=env)


def test_counts_and_data_detect_and_repair(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    psql(pg_pair["src"], "create table orders (id bigint primary key, v int);"
         " insert into orders select g, g*2 from generate_series(1, 5000) g;"
         " delete from orders where id = 99;")
    psql(pg_pair["dst"], "create table orders (id bigint primary key, v int);"
         " insert into orders select g, g*2 from generate_series(1, 5000) g;")

    r = _migkit(conf, "check", "t", "--only", "counts")
    assert r.returncode == 1
    assert "DIFF" in r.stdout

    r = _migkit(conf, "check", "t", "--only", "data")
    assert "DIFF" in r.stdout

    r = _migkit(conf, "repair", "t", "--db", "postgres", "--kind", "rows",
                "--apply")
    assert "applied" in r.stdout

    r = _migkit(conf, "check", "t", "--only", "counts,data")
    assert r.returncode == 0
    assert "DIFF" not in r.stdout


def test_sequence_repair_matches_source_exactly(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, "create table t2 (id int primary key);"
             " create sequence s2;")
    psql(pg_pair["src"], "select setval('s2', 3000)")

    r = _migkit(conf, "check", "t", "--only", "autoinc")
    assert r.returncode == 1

    _migkit(conf, "repair", "t", "--db", "postgres", "--kind", "sequences",
            "--apply")
    val = psql(pg_pair["dst"], "select last_value from s2").stdout.strip()
    assert val == "3000"

    r = _migkit(conf, "check", "t", "--only", "autoinc")
    assert r.returncode == 0


def test_resumable_move_survives_crash(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    psql(pg_pair["src"], "create table big (id bigint primary key, v int);"
         " insert into big select g, g from generate_series(1, 30000) g;")
    psql(pg_pair["dst"], "create table big (id bigint primary key, v int);")

    r = _migkit(conf, "move", "t", "--table", "big", "--go", "--chunk", "10000")
    assert "move complete" in r.stdout

    # simulate crash mid-run: wipe a middle chunk and rewind the checkpoint
    # to the end of chunk 1, exactly the state after dying inside chunk 2
    import json
    psql(pg_pair["dst"], "delete from big where id between 10001 and 20000")
    ck_path = tmp_path / "reports" / "t" / "postgres" / "move.json"
    ck = json.loads(ck_path.read_text())
    ck["public.big"] = {"last": 10000}
    ck_path.write_text(json.dumps(ck))

    r = _migkit(conf, "move", "t", "--table", "big", "--go", "--chunk", "10000")
    assert "move complete" in r.stdout
    # resume must NOT redo chunk 1 (10,000 already checkpointed)
    assert "id=10,000" not in r.stdout
    assert "id=20,000" in r.stdout

    r = _migkit(conf, "check", "t", "--only", "counts")
    assert r.returncode == 0
    n = psql(pg_pair["dst"], "select count(*) from big").stdout.strip()
    assert n == "30000"


def test_check_is_read_only(pg_pair, tmp_path):
    conf = _conf(tmp_path, pg_pair)
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, "create table ro (id int primary key);"
             " insert into ro values (1), (2)")
    before = psql(pg_pair["dst"], "select count(*) from ro").stdout.strip()
    _migkit(conf, "check", "t")
    after = psql(pg_pair["dst"], "select count(*) from ro").stdout.strip()
    assert before == after == "2"
