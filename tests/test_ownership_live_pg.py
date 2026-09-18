"""Object owners, against a real PostgreSQL.

The structural differ compares definitions, and an owner is not part of one -
measured: a table owned by `appowner` on the source arrived owned by
`dts_migration` and the generated fix contained no `OWNER TO` statement at
all. The owner is the role that may ALTER or DROP the object, so a target
where the application no longer owns its own tables looks correct right up to
the first migration the application runs on itself.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-own-pg"
PORT = 15469


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
    # roles are cluster-wide, so they are made once
    _sql("postgres", "create role appowner login; create role migrator login")
    yield
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)


def _engine(tmp_path, dst_port=PORT):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="o", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=dst_port, user="postgres",
                              password="test"),
              db_map={"srcdb": "dstdb"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    return eng


def _load(src_sql, dst_sql, n=3):
    for db in ("srcdb", "dstdb"):
        _sql("postgres", "select pg_terminate_backend(pid) from"
                         " pg_stat_activity where datname = %r"
                         " and pid <> pg_backend_pid()" % db)
        _sql("postgres", f"drop database if exists {db}")
        _sql("postgres", f"create database {db}")
    _sql("srcdb", src_sql)
    _sql("dstdb", dst_sql)
    # a seed that half-applied would make "no drift" pass for nothing
    for db in ("srcdb", "dstdb"):
        got = int(_sql(db, "select count(*) from pg_class c join pg_namespace n"
                           " on n.oid = c.relnamespace"
                           " where n.nspname = 'public' and c.relkind = 'r'"))
        assert got == n, f"{db} has {got} tables, expected {n}"


def _row(tmp_path, dst_port=PORT):
    out = _engine(tmp_path, dst_port)._deep_ownership("srcdb")
    assert len(out) == 1, out
    return out[0]


def test_a_reassigned_owner_is_named(pg, tmp_path):
    _load("create table t1 (id int); alter table t1 owner to appowner;"
          " create table t2 (id int); create table t3 (id int);",
          "create table t1 (id int); alter table t1 owner to migrator;"
          " create table t2 (id int); create table t3 (id int);")
    r = _row(tmp_path)
    assert r.status == "diff", r.detail
    assert "appowner -> migrator" in r.detail
    assert "public.t1" in r.detail
    assert "t2" not in r.detail and "t3" not in r.detail


def test_matching_owners_report_ok_with_a_count(pg, tmp_path):
    sql = ("create table t1 (id int); create table t2 (id int);"
           " create table t3 (id int);"
           " alter table t1 owner to appowner;")
    _load(sql, sql)
    r = _row(tmp_path)
    assert r.status == "ok", r.detail
    assert "keep their owner" in r.detail


def test_a_whole_estate_reassigned_reads_as_one_line(pg, tmp_path):
    """The mover reassigning everything it created is one fact. Printing it
    once per object would bury the object that drifted on its own."""
    src = "".join(f"create table t{i} (id int);"
                  f" alter table t{i} owner to appowner;" for i in range(8))
    dst = "".join(f"create table t{i} (id int);"
                  f" alter table t{i} owner to migrator;" for i in range(8))
    _load(src, dst, n=8)
    r = _row(tmp_path)
    assert r.status == "diff"
    assert r.detail.count("->") == 1, r.detail
    assert "on 8:" in r.detail
    assert "and 4 more" in r.detail
    assert "most likely the mover" in r.fix_hint


def test_owners_of_routines_and_schemas_are_compared_too(pg, tmp_path):
    _load("create table t1 (id int); create table t2 (id int);"
          " create table t3 (id int);"
          " create schema app; alter schema app owner to appowner;"
          " create function f() returns int language sql as 'select 1';"
          " alter function f() owner to appowner;",
          "create table t1 (id int); create table t2 (id int);"
          " create table t3 (id int);"
          " create schema app;"
          " create function f() returns int language sql as 'select 1';")
    r = _row(tmp_path)
    assert r.status == "diff"
    assert "schema app" in r.detail
    assert "routine public.f" in r.detail


def test_security_definer_is_left_to_the_structural_diff(pg, tmp_path):
    """It is part of a function's definition, so the differ already catches
    it. Reporting it here too is how two copies drift apart."""
    _load("create table t1 (id int); create table t2 (id int);"
          " create table t3 (id int);"
          " create function f() returns int language sql"
          " security definer as 'select 1';",
          "create table t1 (id int); create table t2 (id int);"
          " create table t3 (id int);"
          " create function f() returns int language sql as 'select 1';")
    r = _row(tmp_path)
    # the owner is the same on both sides here, so this check has nothing
    assert r.status == "ok", r.detail
    assert "SECURITY" not in r.detail.upper()
    # and the structural diff does have it
    res = _engine(tmp_path).check_structural_diff("srcdb")
    fix = tmp_path / "structural-fix.sql"
    assert fix.exists(), res.detail
    assert "SECURITY DEFINER" in fix.read_text().upper()


def test_an_unreadable_target_is_unknown_and_not_ok(pg, tmp_path):
    _load("create table t1 (id int); create table t2 (id int);"
          " create table t3 (id int);",
          "create table t1 (id int); create table t2 (id int);"
          " create table t3 (id int);")
    r = _row(tmp_path, dst_port=1)
    assert r.status == "warn", r.detail
    assert "unknown, not clean" in r.detail
