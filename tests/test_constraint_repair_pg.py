"""Finishing the validation the load skipped, on a real PostgreSQL.

This came off the real backlog: a migrated database carrying 20 check
constraints left NOT VALID. They enforce new writes, they were never checked
against the rows already there, and the planner will not use them - so nobody
knows whether the data satisfies a rule the schema claims to hold.

The safety property that makes this repairable without a pre-scan was
measured: PostgreSQL refuses to validate a constraint some row violates, with
`check constraint "..." of relation "..." is violated by some row`, and
changes nothing.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-validate"
PORT = 15459


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


def _sql(db, sql, check=True):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", NAME,
                        "psql", "-U", "postgres", "-d", db, "-At",
                        "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    if check:
        assert r.returncode == 0, r.stderr
    return r.stdout.strip() if r.returncode == 0 else r.stderr.strip()


@pytest.fixture(scope="module")
def pg():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(PORT)
    for _ in range(45):
        r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", NAME,
                            "psql", "-U", "postgres", "-c", "select 1"],
                           capture_output=True)
        if r.returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never accepted a connection")
    yield
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="v", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"srcdb": "dstdb"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    return eng


def _fresh(dst_rows="(1,5),(2,7)", not_valid=True):
    """The source is always clean - a violating row there could not have got
    past its own validated constraint. The target is where a load that skipped
    validation leaves one."""
    for db, rows in (("srcdb", "(1,5),(2,7)"), ("dstdb", dst_rows)):
        _sql("postgres", "select pg_terminate_backend(pid) from"
                         " pg_stat_activity where datname = %r"
                         " and pid <> pg_backend_pid()" % db)
        _sql("postgres", f"drop database if exists {db}")
        _sql("postgres", f"create database {db}")
        _sql(db, f"create table t (id int primary key, n int);"
                 f" insert into t values {rows}")
    _sql("srcdb", "alter table t add constraint ck_pos check (n > 0)")
    tail = " not valid" if not_valid else ""
    _sql("dstdb", f"alter table t add constraint ck_pos check (n > 0){tail}")
    assert _sql("dstdb", "select count(*) from t") == "2", "empty seed"


def _validated(db="dstdb"):
    return _sql(db, "select convalidated from pg_constraint"
                    " where conname = 'ck_pos'")


def _plan(tmp_path):
    acts = [a for a in _engine(tmp_path).repair_plan("srcdb", "all")
            if a.kind == "constraints"]
    return acts[0] if acts else None


def test_an_unvalidated_constraint_is_validated_and_can_be_put_back(pg,
                                                                    tmp_path):
    _fresh()
    assert _validated() == "f", "the seed did not leave it NOT VALID"
    act = _plan(tmp_path)
    assert act is not None, "no constraint repair was planned"
    assert "VALIDATE CONSTRAINT" in act.statements[0], act.statements

    _engine(tmp_path).apply("srcdb", act)
    assert _validated() == "t"

    # PostgreSQL has no un-validate, so the undo drops and re-adds it
    for s in act.undo:
        _sql("dstdb", s)
    assert _validated() == "f"


def test_a_violating_row_makes_it_fail_loudly_and_change_nothing(pg,
                                                                 tmp_path):
    """The property that makes this safe without a pre-scan of our own."""
    _fresh(dst_rows="(1,5),(2,-3)")
    assert _validated() == "f"
    act = _plan(tmp_path)
    assert act is not None
    with pytest.raises(Exception) as e:
        _engine(tmp_path).apply("srcdb", act)
    assert "violated" in str(e.value).lower(), str(e.value)
    assert _validated() == "f", "it half-applied"


def test_an_already_validated_constraint_plans_nothing(pg, tmp_path):
    _fresh(not_valid=False)
    assert _validated() == "t", "the seed left it NOT VALID"
    assert _plan(tmp_path) is None


def test_the_check_and_the_repair_read_the_same_computation(pg, tmp_path):
    _fresh()
    eng = _engine(tmp_path)
    found = eng._unvalidated_checks("srcdb")
    assert len(found) == 1
    tbl, name, definition = found[0]
    assert name == "ck_pos"
    # the definition carries NOT VALID, which is what makes the undo exact
    assert "NOT VALID" in definition.upper(), definition
    assert len(_plan(tmp_path).statements) == len(found)


def test_the_lock_report_says_validation_blocks_nothing(pg, tmp_path):
    """Operators run this against a target that is serving traffic, so the
    cost has to be stated, not assumed."""
    from migkit.locks import classify
    mode, meaning, _ = classify(
        'ALTER TABLE t VALIDATE CONSTRAINT "ck_pos";')
    assert mode == "ShareUpdateExclusiveLock"
    assert "without blocking reads or writes" in meaning
