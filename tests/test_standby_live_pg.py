"""Verifying from a real streaming standby that has fallen behind.

The setup is a genuine primary and a genuine replica, not a simulation: the
point is that a replica which has not replayed produces `rows-extra`, and
`rows-extra` is the shape migkit teaches people to treat as someone writing
to the target. If the verdict does not say the source was a replica, the two
are indistinguishable.
"""
import socket
import subprocess
import time

import pytest

NET = "migkit-test-rep-net"
PRI, SBY = "migkit-test-pri", "migkit-test-sby"
PRI_PORT, SBY_PORT = 15437, 15438


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _sql(name, db, sql, check=True):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                        "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
                       capture_output=True, text=True)
    if check:
        assert r.returncode == 0, r.stderr
    return r.stdout.strip()


def _up(name, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                            "psql", "-U", "postgres", "-c", "select 1"],
                           capture_output=True)
        if r.returncode == 0:
            return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def pair():
    for n in (SBY, PRI):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PRI, "--network", NET,
                    "-e", "POSTGRES_PASSWORD=test", "-p", f"{PRI_PORT}:5432",
                    "postgres:16", "-c", "wal_level=replica",
                    "-c", "max_wal_senders=4", "-c", "hot_standby=on"],
                   check=True, capture_output=True)
    assert _wait(PRI_PORT) and _up(PRI)
    _sql(PRI, "postgres", "create role rep with replication login"
                          " password 'rep'")
    _sql(PRI, "postgres", "create database app")
    _sql(PRI, "app", "create table t (id int primary key, v text)")
    _sql(PRI, "app", "insert into t select g,'x'||g"
                     " from generate_series(1,100) g")
    assert _sql(PRI, "app", "select count(*) from t") == "100"
    subprocess.run(["docker", "exec", PRI, "bash", "-c",
                    "echo 'host replication rep all md5' >>"
                    " /var/lib/postgresql/data/pg_hba.conf"], check=True,
                   capture_output=True)
    _sql(PRI, "postgres", "select pg_reload_conf()")
    subprocess.run(["docker", "run", "-d", "--name", SBY, "--network", NET,
                    "-e", "PGPASSWORD=rep", "-p", f"{SBY_PORT}:5432",
                    "--entrypoint", "bash", "postgres:16", "-c",
                    "rm -rf /var/lib/postgresql/data/* &&"
                    f" pg_basebackup -h {PRI} -U rep"
                    " -D /var/lib/postgresql/data -R -X stream &&"
                    " chmod 700 /var/lib/postgresql/data &&"
                    " exec docker-entrypoint.sh postgres"],
                   check=True, capture_output=True)
    if not (_wait(SBY_PORT) and _up(SBY)):
        logs = subprocess.run(["docker", "logs", SBY], capture_output=True,
                              text=True).stdout
        pytest.fail(f"standby never came up: {logs[-500:]}")
    assert _sql(SBY, "postgres", "select pg_is_in_recovery()") == "t"
    assert _sql(SBY, "app", "select count(*) from t") == "100", \
        "the standby did not replicate the seed, so nothing below is tested"
    yield
    for n in (SBY, PRI):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _engine(tmp_path, src_port, dst_port):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="r", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=src_port,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=dst_port,
                              user="postgres", password="test"),
              db_map={"app": "app"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    return eng


def test_a_lagging_standby_source_is_named_in_the_verdict(pair, tmp_path):
    """The measurement this exists for: five rows on the primary that the
    paused standby has not replayed read as `rows-extra by=5`."""
    _sql(SBY, "postgres", "select pg_wal_replay_pause()", check=False)
    try:
        _sql(PRI, "app", "insert into t select g,'new'||g"
                         " from generate_series(101,105) g")
        time.sleep(2)
        assert _sql(PRI, "app", "select count(*) from t") == "105"
        assert _sql(SBY, "app", "select count(*) from t") == "100", \
            "replay did not actually pause, so the lag is not being tested"

        eng = _engine(tmp_path, SBY_PORT, PRI_PORT)
        rc, out = eng._data_fast_native("app")
        assert rc == 1, out
        line = [l for l in out.splitlines() if l.startswith("public.t:")][0]
        assert "rows-extra by=5" in line, line
        assert "the source is a replica" in line, line
        assert "re-read from the writer" in line, line
        # and the run says it once as well, so it is visible without a diff
        assert any("source read while in recovery" in l
                   for l in out.splitlines()), out
    finally:
        _sql(SBY, "postgres", "select pg_wal_replay_resume()", check=False)


def test_the_caveat_does_not_declare_the_finding_false(pair, tmp_path):
    """Someone writing to the target produces the same shape. migkit cannot
    tell them apart from one comparison and must not pretend to."""
    _sql(SBY, "postgres", "select pg_wal_replay_pause()", check=False)
    try:
        _sql(PRI, "app", "insert into t select g,'m'||g"
                         " from generate_series(200,203) g")
        time.sleep(2)
        _, out = _engine(tmp_path, SBY_PORT, PRI_PORT)._data_fast_native("app")
        line = [l for l in out.splitlines() if l.startswith("public.t:")][0]
        assert ": DIFF" in line, line
        for word in ("false positive", "ignore", "harmless"):
            assert word not in line.lower(), line
    finally:
        _sql(SBY, "postgres", "select pg_wal_replay_resume()", check=False)


def test_a_writer_source_gets_no_replica_note(pair, tmp_path):
    """Primary against primary: nothing to caveat, and the note must not
    appear where it does not apply."""
    eng = _engine(tmp_path, PRI_PORT, PRI_PORT)
    rc, out = eng._data_fast_native("app")
    assert rc == 0, out
    assert "in recovery" not in out, out
    assert "the source is a replica" not in out, out


def test_a_caught_up_standby_still_says_it_is_one(pair, tmp_path):
    """Equal data, so no finding to caveat - but the run still reports that
    the source was read from a replica, because freshness is a property of
    the answer even when the answer is 'equal'."""
    _sql(SBY, "postgres", "select pg_wal_replay_resume()", check=False)
    for _ in range(30):
        if _sql(SBY, "app", "select count(*) from t") == \
                _sql(PRI, "app", "select count(*) from t"):
            break
        time.sleep(1)
    else:
        pytest.fail("standby never caught up")
    rc, out = _engine(tmp_path, SBY_PORT, PRI_PORT)._data_fast_native("app")
    assert rc == 0, out
    assert any("source read while in recovery" in l
               for l in out.splitlines()), out
