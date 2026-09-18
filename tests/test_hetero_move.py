"""Moving rows between two different engines, then checking they arrived.

The pairing is PostgreSQL to MySQL, which until now refused both operations.
Rows go across through the read/write contract, and the same digest that
compares any pair is what says they landed - so the move is not trusted on the
strength of having finished without an error.

The middle test is the one that matters: the move is interrupted and restarted
from the checkpoint, and has to converge rather than duplicate.
"""
import socket
import subprocess
import time

import pytest

PG, MY = "migkit-test-mv-pg", "migkit-test-mv-my"
PG_PORT, MY_PORT = 15471, 13371
ROWS = 250


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


class _Checkpoint(dict):
    """The shape `move` passes in: a dict that can save itself."""

    def save(self):
        pass


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
        pytest.fail("postgres never answered")
    for _ in range(60):
        if my_sql("select 1").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never answered")
    assert pg_sql("create database cx", "postgres").returncode == 0
    assert my_sql("create database cx").returncode == 0
    assert pg_sql(
        "create table t (id bigint primary key, name text,"
        " ratio double precision, flag boolean);"
        " insert into t select g, 'row ' || g || ' café',"
        " g::float8 / 7, g % 2 = 0"
        f" from generate_series(1, {ROWS}) g").returncode == 0
    assert pg_sql("select count(*) from t").stdout.strip() == str(ROWS)
    assert my_sql("create table t (id bigint primary key, name text,"
                  " ratio double, flag tinyint(1))", "cx").returncode == 0

    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="mv", engine="hetero",
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


def _verdict(eng):
    got = [r for r in eng.check_data("cx") if r.scope.endswith(".t")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def test_the_target_starts_empty_and_the_check_says_so(engine):
    """Without this the pass after the move could be a pass over nothing."""
    assert my_sql("select count(*) from t", "cx").stdout.strip() == "0"
    r = _verdict(engine)
    assert r.status == "diff", r.detail
    assert f"rows src={ROWS:,} dst=0" in r.detail


def test_the_tables_to_move_are_listed_for_this_pair(engine):
    got = engine.list_move_tables("cx")
    assert ("public", "t") in got, got


def test_rows_cross_and_the_digest_confirms_they_landed(engine):
    """Finishing without an error is not evidence. The digest is."""
    ck = _Checkpoint()
    engine.move_table("cx", "public", "t", 100, ck, lambda m: None)
    assert my_sql("select count(*) from t", "cx").stdout.strip() == str(ROWS)
    r = _verdict(engine)
    assert r.status == "ok", r.detail
    assert "postgres/mysql" in r.detail


def test_a_restart_converges_instead_of_duplicating(engine):
    """A move that was interrupted is resumed from the checkpoint, and a move
    that was already finished is run again from scratch. Neither may leave
    more rows than the source has."""
    ck = _Checkpoint()
    ck["cx.t"] = {"last": [100], "moved": 100}
    engine.move_table("cx", "public", "t", 40, ck, lambda m: None)
    assert my_sql("select count(*) from t", "cx").stdout.strip() == str(ROWS)
    assert _verdict(engine).status == "ok"

    engine.move_table("cx", "public", "t", 40, _Checkpoint(),
                      lambda m: None)
    assert my_sql("select count(*) from t", "cx").stdout.strip() == str(ROWS)
    assert _verdict(engine).status == "ok"


def test_a_row_changed_on_the_source_is_carried_by_a_rerun(engine):
    """The write replaces by key rather than inserting beside, so a second
    pass repairs rather than doubles."""
    assert pg_sql("update t set name = 'changed' where id = 7").returncode \
        == 0
    try:
        assert _verdict(engine).status == "diff"
        engine.move_table("cx", "public", "t", 100, _Checkpoint(),
                          lambda m: None)
        assert _verdict(engine).status == "ok"
        assert my_sql("select name from t where id=7",
                      "cx").stdout.strip() == "changed"
    finally:
        pg_sql("update t set name = 'row 7 café' where id = 7")
        engine.move_table("cx", "public", "t", 100, _Checkpoint(),
                          lambda m: None)
    assert _verdict(engine).status == "ok"


def test_a_column_the_target_lacks_is_named_rather_than_dropped_silently(
        engine):
    lines = []
    assert pg_sql("alter table t add column note text").returncode == 0
    try:
        engine.move_table("cx", "public", "t", 100, _Checkpoint(),
                          lines.append)
        assert any("the target has no such column: note" in m
                   for m in lines), lines
    finally:
        pg_sql("alter table t drop column note")


def test_a_table_missing_on_the_target_is_refused_not_invented(engine):
    assert pg_sql("create table only_here (id int primary key)").returncode \
        == 0
    try:
        with pytest.raises(SystemExit) as e:
            engine.move_table("cx", "public", "only_here", 100,
                              _Checkpoint(), lambda m: None)
        assert "not on the target" in str(e.value)
    finally:
        pg_sql("drop table only_here")
