"""PostgreSQL as the source and MySQL as the target, which used to exit.

`hetero.py` accepted exactly one pairing and refused the rest with "not built
yet". Nothing about comparing two tables was ever specific to the direction -
what was specific was that the comparison had been written twice, once per
engine, in the same function. This runs the real thing against real servers in
the direction that was impossible.
"""
import socket
import subprocess
import time

import pytest

PG, MY = "migkit-test-rev-pg", "migkit-test-rev-my"
PG_PORT, MY_PORT = 15483, 13383

DDL_PG = """create table shape (
    id bigint primary key, name varchar(50), amount numeric(12,4),
    ratio double precision, flag boolean, made timestamp(6))"""
DDL_MY = """create table shape (
    id bigint primary key, name varchar(50), amount decimal(12,4),
    ratio double, flag tinyint(1), made datetime(6))"""
ROWS_PG = ("(1,'café',-0.05,1e20,true,'2026-01-01 00:00:00'),"
           "(2,'',0.0001,0.000001,false,'2026-09-18 13:45:06.123456'),"
           "(3,null,null,null,null,null)")
ROWS_MY = ("(1,'café',-0.05,1e20,1,'2026-01-01 00:00:00'),"
           "(2,'',0.0001,0.000001,0,'2026-09-18 13:45:06.123456'),"
           "(3,null,null,null,null,null)")


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


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def pg_sql(sql, db="cx"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


def my_sql(sql, db=None):
    cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
           "--default-character-set=utf8mb4", "-N", "-B"]
    if db:
        cmd += ["-D", db]
    return subprocess.run(cmd + ["-e", sql], capture_output=True, text=True)


@pytest.fixture(scope="module")
def engine():
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    assert _wait(PG_PORT) and _wait(MY_PORT)
    for _ in range(40):
        if pg_sql("select 1", "postgres").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never accepted a connection")
    for _ in range(60):
        if my_sql("select 1").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never accepted a connection")

    assert pg_sql("create database cx", "postgres").returncode == 0
    assert my_sql("create database cx").returncode == 0
    assert pg_sql(DDL_PG).returncode == 0
    assert my_sql(DDL_MY, "cx").returncode == 0
    assert pg_sql(f"insert into shape values {ROWS_PG}").returncode == 0
    assert my_sql(f"insert into shape values {ROWS_MY}", "cx").returncode == 0
    # a half-written seed would make every comparison below meaningless
    assert pg_sql("select count(*) from shape").stdout.strip() == "3"
    assert my_sql("select count(*) from shape", "cx").stdout.strip() == "3"

    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="rev", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=MY_PORT,
                              user="root", password="test"),
              db_map={"cx": "cx"},
              options={"source_engine": "postgres",
                       "target_engine": "mysql"})
    yield HeteroEngine(hop)
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _one(engine, table="shape"):
    got = [r for r in engine.check_data("cx")
           if r.scope.endswith(f".{table}")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def test_the_same_data_in_two_engines_comes_out_equal(engine):
    r = _one(engine)
    assert r.status == "ok", r.detail
    assert "rows 3" in r.detail
    assert "postgres/mysql" in r.detail


def test_the_counts_check_agrees_and_names_the_pair(engine):
    out = engine.check_counts("cx")
    assert len(out) == 1 and out[0].status == "ok", \
        [(r.scope, r.status, r.detail) for r in out]
    assert "postgres/mysql" in out[0].detail


def test_a_changed_value_is_a_content_difference_not_a_row_difference(engine):
    assert my_sql("update shape set name='cafe' where id=1",
                  "cx").returncode == 0
    try:
        r = _one(engine)
        assert r.status == "diff", r.detail
        assert "rows 3 match but the contents do not" in r.detail
    finally:
        assert my_sql("update shape set name='café' where id=1",
                      "cx").returncode == 0
    assert _one(engine).status == "ok"


def test_a_missing_row_is_reported_as_a_row_difference(engine):
    assert my_sql("delete from shape where id=3", "cx").returncode == 0
    try:
        r = _one(engine)
        assert r.status == "diff", r.detail
        assert "rows src=3 dst=2" in r.detail
    finally:
        assert my_sql(f"insert into shape values {ROWS_MY.split('),')[-1]}",
                      "cx").returncode == 0
    assert _one(engine).status == "ok"


def test_a_column_on_only_one_side_is_named_rather_than_dropped(engine):
    """The remaining columns still compare, and the uncompared one is in the
    line. Silently ignoring it would report a target that is missing a column
    as a clean migration."""
    assert my_sql("alter table shape add column extra int", "cx").returncode \
        == 0
    try:
        r = _one(engine)
        assert r.status == "ok", r.detail
        assert "columns only on the target, not compared: extra" in r.detail
    finally:
        assert my_sql("alter table shape drop column extra",
                      "cx").returncode == 0


def test_a_table_on_only_one_side_is_a_difference(engine):
    assert pg_sql("create table orphan (id int)").returncode == 0
    try:
        got = {r.scope: r for r in engine.check_data("cx")}
        assert "cx.orphan" in got, list(got)
        assert got["cx.orphan"].status == "diff"
        assert "not on the target" in got["cx.orphan"].detail
        assert got["cx.shape"].status == "ok"
    finally:
        assert pg_sql("drop table orphan").returncode == 0


def test_a_type_with_no_rendering_is_named_and_the_rest_still_compare(engine):
    """A PostgreSQL `tsvector` has no MySQL counterpart and no canonical
    rendering. The table is still compared on everything else, and the column
    that was left out is in the line - which is the difference between a
    partial answer and a wrong one."""
    assert pg_sql("alter table shape add column doc tsvector").returncode == 0
    assert my_sql("alter table shape add column doc text", "cx").returncode \
        == 0
    try:
        r = _one(engine)
        assert "doc:" in r.detail and "no canonical rendering" in r.detail
        assert r.status == "ok", r.detail
    finally:
        pg_sql("alter table shape drop column doc")
        my_sql("alter table shape drop column doc", "cx")
