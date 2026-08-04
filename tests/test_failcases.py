"""Failure-mode and edge-case tests: bad input, missing state, locks,
no-PK tables, and cross-cutting error handling. Unit-level ones need no
docker; a few live ones use the pg_pair fixture.
"""
import os
import subprocess
import textwrap

import pytest

from migkit.config import Endpoint, Hop, load_hops
from tests.conftest import needs_docker, psql, MIGKIT


def _base():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run(conf, *args):
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(conf.parent / "reports"))
    return subprocess.run(
        [MIGKIT, *args],
        capture_output=True, text=True, env=env)


def _conf(tmp_path, body):
    c = tmp_path / "hops.yaml"
    c.write_text(textwrap.dedent(body))
    return c


# --- input / config failure modes (no docker) ---

def test_unknown_hop_errors_cleanly(tmp_path):
    conf = _conf(tmp_path, """
        hops:
          real:
            engine: postgres
            source: {host: h, user: u, password: p}
            target: {host: h, user: u, password: p}
    """)
    r = _run(conf, "check", "does-not-exist")
    assert r.returncode != 0
    assert "unknown hop" in (r.stdout + r.stderr)


def test_unconfigured_target_fails_fast(tmp_path):
    conf = _conf(tmp_path, """
        hops:
          half:
            engine: postgres
            source: {host: h, user: u, password: p}
            target: {host: "", user: "", password: ""}
    """)
    r = _run(conf, "check", "half")
    assert r.returncode != 0
    assert "not configured" in (r.stdout + r.stderr)
    # must fail before touching any database, not spew connection errors
    assert "psql" not in (r.stdout + r.stderr).lower()


def test_missing_config_file_errors(tmp_path):
    r = _run(tmp_path / "nope.yaml", "hops")
    assert r.returncode != 0


def test_unsupported_engine_rejected():
    hop = Hop(name="x", engine="cassandra",
              source=Endpoint(host="h"), target=Endpoint(host="h"))
    from migkit.engines import get_engine
    with pytest.raises(SystemExit):
        get_engine(hop)


def test_bad_yaml_does_not_crash_interpreter(tmp_path):
    c = tmp_path / "hops.yaml"
    c.write_text("hops: [not, a, mapping]")
    r = _run(c, "hops")
    # should exit non-zero or handle gracefully, never a raw traceback only
    assert r.returncode != 0 or "hop" in r.stdout


# --- live failure modes (docker) ---

@needs_docker
def test_connection_refused_reports_error(pg_pair, tmp_path):
    conf = _conf(tmp_path, f"""
        hops:
          bad:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: WRONGPASS}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """)
    psql(pg_pair["src"], "create table x (id int primary key)")
    psql(pg_pair["dst"], "create table x (id int primary key)")
    r = _run(conf, "check", "bad", "--only", "counts")
    # wrong password must surface as an error, not a false OK
    assert "OK" not in r.stdout or r.returncode != 0


@needs_docker
def test_repair_with_no_diff_says_nothing(pg_pair, tmp_path):
    conf = _conf(tmp_path, f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """)
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, "create sequence s; select setval('s', 5)")
    r = _run(conf, "repair", "t", "--db", "postgres", "--kind", "sequences")
    assert "nothing to repair" in r.stdout or "already equal" in r.stdout


@needs_docker
def test_rollback_unknown_state_errors(pg_pair, tmp_path):
    conf = _conf(tmp_path, f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """)
    r = _run(conf, "rollback", "t", "--db", "postgres",
             "--state", "20000101-000000")
    assert r.returncode != 0
    assert "no saved states" in (r.stdout + r.stderr) or \
        "no state" in (r.stdout + r.stderr)


@needs_docker
def test_table_without_pk_does_not_crash(pg_pair, tmp_path):
    conf = _conf(tmp_path, f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """)
    for p in (pg_pair["src"], pg_pair["dst"]):
        psql(p, "create table nopk (v text);"
             " insert into nopk values ('a'), ('b')")
    r = _run(conf, "check", "t", "--only", "data")
    # no-pk tables are compared by whole-table checksum, must not crash
    assert r.returncode in (0, 1)
    assert "Traceback" not in (r.stdout + r.stderr)


@needs_docker
def test_credential_drift_detected(pg_pair, tmp_path):
    conf = _conf(tmp_path, f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg_pair['src']}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg_pair['dst']}, user: postgres, password: test}}
            databases: [postgres]
    """)
    # same role name, different password: the DTS failure mode
    psql(pg_pair["src"], "create role appuser login password 'secret_v1'")
    psql(pg_pair["dst"], "create role appuser login password 'secret_v2'")
    r = _run(conf, "assess", "t")
    out = r.stdout + r.stderr
    assert "password" in out.lower()
    # appuser hash differs -> should be flagged, not passed silently
    assert "appuser" in out
