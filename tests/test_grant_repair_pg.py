"""Re-granting what the mover did not carry, on a real PostgreSQL.

migkit has always been able to say which grants were missing. Until now it
could not apply them, which is an odd place to stop for a tool that already
repairs sequences, rows and schema - and a grant is the difference between an
application that connects and one that works.

Sequence grants get their own attention because they are the confusing case:
the table grants look complete, `SELECT` works, and every `INSERT` fails.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-grantfix"
PORT = 15465


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


def _sql(db, sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", NAME,
                        "psql", "-U", "postgres", "-d", db, "-At",
                        "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pg():
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)
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
    _sql("postgres", "create role app login")
    yield
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="g", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"srcdb": "dstdb"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    return eng


SCHEMA = ("create table t (id serial primary key, v text);"
          " create table u (id int primary key);")


def _fresh(src_grants="", dst_grants=""):
    for db in ("srcdb", "dstdb"):
        _sql("postgres", "select pg_terminate_backend(pid) from"
                         " pg_stat_activity where datname = %r"
                         " and pid <> pg_backend_pid()" % db)
        _sql("postgres", f"drop database if exists {db}")
        _sql("postgres", f"create database {db}")
        _sql(db, SCHEMA)
    if src_grants:
        _sql("srcdb", src_grants)
    if dst_grants:
        _sql("dstdb", dst_grants)


def _held(db, obj):
    return _sql(db, "select coalesce(string_agg(distinct a.privilege_type,"
                    " ',' order by a.privilege_type), '(none)')"
                    " from pg_class c cross join lateral aclexplode(c.relacl) a"
                    f" where c.relname = '{obj}'"
                    " and pg_get_userbyid(a.grantee) = 'app'")


def _plan(tmp_path):
    acts = [a for a in _engine(tmp_path).repair_plan("srcdb", "all")
            if a.kind == "grants"]
    return acts[0] if acts else None


def test_a_missing_table_grant_is_applied_and_undone(pg, tmp_path):
    _fresh(src_grants="grant select, insert on t to app;")
    assert _held("srcdb", "t") == "INSERT,SELECT"
    assert _held("dstdb", "t") == "(none)", "the seed already granted it"

    eng = _engine(tmp_path)
    act = _plan(tmp_path)
    assert act is not None, "no grant repair was planned"
    assert any("GRANT SELECT" in s for s in act.statements), act.statements
    eng.apply("srcdb", act)
    assert _held("dstdb", "t") == "INSERT,SELECT"

    for s in act.undo:
        _sql("dstdb", s)
    assert _held("dstdb", "t") == "(none)"


def test_a_missing_sequence_grant_is_applied(pg, tmp_path):
    """The one that makes every INSERT fail while the table grants look
    complete."""
    _fresh(src_grants="grant select, insert on t to app;"
                      " grant usage on sequence t_id_seq to app;",
           dst_grants="grant select, insert on t to app;")
    assert _held("srcdb", "t_id_seq") == "USAGE"
    assert _held("dstdb", "t_id_seq") == "(none)"

    act = _plan(tmp_path)
    assert act is not None
    seq = [s for s in act.statements if "SEQUENCE" in s]
    assert seq, act.statements
    _engine(tmp_path).apply("srcdb", act)
    assert _held("dstdb", "t_id_seq") == "USAGE"


def test_matching_grants_plan_nothing(pg, tmp_path):
    g = "grant select on t to app; grant usage on sequence t_id_seq to app;"
    _fresh(src_grants=g, dst_grants=g)
    assert _held("dstdb", "t") == "SELECT"       # the seed really ran
    assert _plan(tmp_path) is None


def test_extra_grants_on_the_target_are_not_revoked(pg, tmp_path):
    """Removing a privilege somebody added on purpose is a different decision
    from restoring one the migration dropped. The check reports it; the repair
    does not act on it."""
    _fresh(src_grants="grant select on t to app;",
           dst_grants="grant select on t to app; grant delete on u to app;")
    assert _held("dstdb", "u") == "DELETE"
    act = _plan(tmp_path)
    assert act is None or not any("REVOKE" in s for s in act.statements)
    assert _held("dstdb", "u") == "DELETE"


def test_a_grant_to_a_role_the_target_lacks_is_not_attempted(pg, tmp_path):
    """It would simply fail. The users check is what reports the missing
    role, and `migkit users create` is what fixes it."""
    _sql("postgres", "drop role if exists ghost")
    _sql("postgres", "create role ghost login")
    _fresh(src_grants="grant select on t to ghost;")
    _sql("postgres", "revoke all on database dstdb from ghost")
    # the role exists cluster-wide here, so build the check's view directly
    gaps = _engine(tmp_path)._grant_gaps("srcdb")
    assert gaps is not None
    miss = gaps[0]
    assert any("ghost" in m for m in miss), miss    # visible while it exists
    # `drop owned by` runs in the database holding the objects, not in
    # `postgres` - the role owns a grant inside srcdb
    for db in ("srcdb", "dstdb"):
        _sql(db, "drop owned by ghost")
    _sql("postgres", "drop role ghost")
    gaps2 = _engine(tmp_path)._grant_gaps("srcdb")
    assert not any("ghost" in m for m in gaps2[0]), gaps2[0]


def test_the_check_and_the_repair_read_the_same_computation(pg, tmp_path):
    """Two copies of "which grants are missing" would be two chances to grant
    the wrong thing. The check reports exactly what the repair applies."""
    _fresh(src_grants="grant select, insert on t to app;"
                      " grant usage on sequence t_id_seq to app;")
    eng = _engine(tmp_path)
    miss, _extra, smiss, _, _ = eng._grant_gaps("srcdb")
    act = _plan(tmp_path)
    assert act is not None
    assert len(act.statements) == len(miss) + len(smiss)
    assert len(act.undo) == len(act.statements)
