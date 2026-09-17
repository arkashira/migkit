"""FLOAT/DOUBLE are IEEE approximations; a mover that changes precision drifts
them while a row checksum can't tell real drift from bit-level noise. The deep
check compares float columns per row with a relative tolerance, catching drift
beyond tolerance without false-flagging a faithful copy."""
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


def test_float_drift_beyond_tolerance_flagged(pg_pair, tmp_path):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """))
    psql(pg_pair["src"], "create table t(id int primary key, v double precision);"
         " insert into t values (1, 1.2345678901234), (2, 9.5)")
    # target: row 1 drifted well beyond tolerance, row 2 identical
    psql(pg_pair["dst"], "create table t(id int primary key, v double precision);"
         " insert into t values (1, 1.9999), (2, 9.5)")

    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 1, r.stdout
    out = r.stdout.lower()
    assert "float" in out and "tolerance" in out, r.stdout
    assert "t.v" in r.stdout, r.stdout


def test_identical_floats_pass(pg_pair, tmp_path):
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
        psql(p, "create table t(id int primary key, v double precision);"
                " insert into t values (1, 1.2345678901234), (2, 9.5)")

    # deep is green: a faithful float copy is within tolerance (no false diff)
    r = _migkit(conf, "check", "t", "--only", "deep")
    assert r.returncode == 0, r.stdout
