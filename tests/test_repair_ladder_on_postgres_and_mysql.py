"""The repair ladder on PostgreSQL and MySQL's own paths (backlog P0-DEPTH
1): a span of the key copied again by the table copier's own range copy -
deleted and copied under one transaction, as a replica on PostgreSQL so
the target's triggers do not rewrite what lands - the table copied again
whole by the move's table ladder, each put back exactly by a rollback;
and a PostgreSQL row repair refused where the load would be refused,
before a row is written.

Two containers an engine, on 127.0.0.1 only, removed when the module
ends.
"""
import subprocess
import time

import pytest

from migkit import costmodel, repairlog, repairways
from migkit.config import Endpoint, Hop
from migkit.repairways import RANGES, RELOAD, ROWS

pytestmark = pytest.mark.docker


@pytest.fixture(autouse=True)
def own_knowledge(tmp_path, monkeypatch):
    """What one test's repairs teach the machine's knowledge is not the
    next test's prior."""
    monkeypatch.setenv("MIGKIT_KNOWLEDGE", str(tmp_path / "knowledge.json"))
    costmodel._KNOWLEDGE.clear()
    yield
    costmodel._KNOWLEDGE.clear()

PG = {"src": ("migkit-test-rl-pg-src", 16380),
      "dst": ("migkit-test-rl-pg-dst", 16381)}
MY = {"src": ("migkit-test-rl-my-src", 16382),
      "dst": ("migkit-test-rl-my-dst", 16383)}


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _rm(pair):
    for name, _ in pair.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _until(ready, what, timeout=300):
    end = time.time() + timeout
    while time.time() < end:
        if ready():
            return
        time.sleep(2)
    pytest.fail(f"{what} never answered")


# --- PostgreSQL ---------------------------------------------------------------

@pytest.fixture(scope="module")
def pg():
    if not _docker():
        pytest.skip("docker not available")
    _rm(PG)
    try:
        for name, port in PG.values():
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "POSTGRES_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:5432", "postgres:16"],
                           check=True, capture_output=True)
        for name, _ in PG.values():
            _until(lambda n=name: subprocess.run(
                ["docker", "exec", n, "psql", "-U", "postgres", "-h",
                 "127.0.0.1", "-Atc", "select 1"],
                capture_output=True).returncode == 0, name)
        yield
    finally:
        _rm(PG)


def pg_sql(side, sql, db="app"):
    got = subprocess.run(["docker", "exec", "-i", PG[side][0], "psql",
                          "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1",
                          "-At"], input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _fresh_pg(rows):
    for side in ("src", "dst"):
        pg_sql(side, "drop database if exists app with (force)",
               db="postgres")
        pg_sql(side, "create database app", db="postgres")
        pg_sql(side, "create table t (id int primary key, v text);"
                     " insert into t select g, 'v' || g from"
                     f" generate_series(1, {rows}) g; analyze t;")


def _pg(tmp_path, user="postgres", **options):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="rlpg", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PG["src"][1],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=PG["dst"][1],
                              user=user, password="test"),
              databases=["app"], options=options)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _pg_rows(side):
    return pg_sql(side, "select md5(string_agg(id || ':' || v, ','"
                        " order by id)) from t")


def _ladder(eng, db, table, copy=False):
    from migkit import cli
    [r for r in eng.check_data(db)]
    [action] = [a for a in eng.repair_plan(db, "rows")
                if a.kind == "rows" and a.table == table]
    return repairways.Ladder(eng, db, action,
                             copy_table=cli._table_copier(eng.hop, eng, db)
                             if copy else None)


def _rolled_back(eng, db, tmp_path):
    for path in repairlog.to_reverse(tmp_path / "undo"):
        repairlog.reverse(path, eng.repair_io(db, repairlog.read(path)[0]))


def test_postgres_copies_a_span_again_in_one_transaction_as_a_replica(
        pg, tmp_path):
    _fresh_pg(20000)
    pg_sql("dst", "delete from t where id between 5001 and 8000;"
                  " update t set v = 'x' where id in (7, 15000, 19999);")
    # a trigger on the target rewrites what it is given: a repair that let
    # it fire would leave every row it wrote apart from the source's
    pg_sql("dst", "create function bang() returns trigger language plpgsql"
                  " as $$ begin new.v := new.v || '!'; return new; end $$;"
                  " create trigger bang before insert or update on t"
                  " for each row execute function bang();")
    damaged = _pg_rows("dst")
    eng = _pg(tmp_path)
    ladder = _ladder(eng, "app", "public.t")
    spans, left, _ = ladder._spans()
    assert [(p.lo, p.hi) for p in spans] == [(5001, 8001)], spans
    assert sum(len(v) for v in left.values()) == 3, left
    out = ladder.run()
    assert (out.way, out.proof) == (RANGES, "ok"), out
    assert _pg_rows("dst") == _pg_rows("src")
    assert pg_sql("dst", "select count(*) from t where v like '%!'") == "0"
    # put back as it was, the trigger held quiet on the way back too
    _rolled_back(eng, "app", tmp_path)
    assert _pg_rows("dst") == damaged


def test_postgres_copies_a_table_most_of_which_differs_again_whole(
        pg, tmp_path):
    _fresh_pg(5000)
    pg_sql("dst", "update t set v = 'x' where id % 10 <> 0")
    damaged = _pg_rows("dst")
    eng = _pg(tmp_path, target_rows="replace")
    ladder = _ladder(eng, "app", "public.t", copy=True)
    assert [r.name for r in ladder.order()][0] == RELOAD
    out = ladder.run()
    assert (out.way, out.proof) == (RELOAD, "ok"), out
    assert _pg_rows("dst") == _pg_rows("src")
    [path] = repairlog.records(tmp_path / "undo")
    assert repairlog.read(path)[0]["form"] == "snapshot"
    _rolled_back(eng, "app", tmp_path)
    assert _pg_rows("dst") == damaged


def test_postgres_refuses_a_repair_where_it_would_refuse_the_load(
        pg, tmp_path):
    """The target's user may not write as a replica and the table has a
    trigger: the load stops before it empties anything, and the repair
    used to go on with the trigger firing. It stops the same way now,
    before a row is written."""
    _fresh_pg(100)
    pg_sql("dst", "delete from t where id = 5;"
                  " create function bang() returns trigger language plpgsql"
                  " as $$ begin new.v := new.v || '!'; return new; end $$;"
                  " create trigger bang before insert or update on t"
                  " for each row execute function bang();"
                  " do $$ begin if not exists (select from pg_roles where"
                  " rolname = 'rl_app') then create role rl_app login"
                  " password 'test'; end if; end $$;"
                  " grant select, insert, update, delete on t to rl_app;")
    # checked as the server's own user, repaired as the application's
    [r for r in _pg(tmp_path).check_data("app")]
    eng = _pg(tmp_path, user="rl_app")
    [action] = [a for a in eng.repair_plan("app", "rows")
                if a.kind == "rows" and a.table == "public.t"]
    ladder = repairways.Ladder(eng, "app", action)
    assert [r.name for r in ladder.order()][0] == ROWS
    with pytest.raises(SystemExit, match="have triggers on the target"):
        ladder.run()
    assert pg_sql("dst", "select count(*) from t where id = 5") == "0"
    assert pg_sql("dst", "select count(*) from t where v like '%!'") == "0"


# --- MySQL ----------------------------------------------------------------------

@pytest.fixture(scope="module")
def my():
    if not _docker():
        pytest.skip("docker not available")
    _rm(MY)
    try:
        for name, port in MY.values():
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:3306", "mysql:8.4"],
                           check=True, capture_output=True)
        for name, _ in MY.values():
            _until(lambda n=name: subprocess.run(
                ["docker", "exec", n, "mysql", "-uroot", "-ptest",
                 "-h127.0.0.1", "--protocol=tcp", "-e", "select 1"],
                capture_output=True).returncode == 0, name)
        yield
    finally:
        _rm(MY)


def my_sql(side, sql):
    got = subprocess.run(["docker", "exec", "-i", MY[side][0], "mysql",
                          "-uroot", "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my_rows(side):
    return my_sql(side, "set session group_concat_max_len = 100000000;"
                        " select md5(group_concat(concat(id, ':', v)"
                        " order by id separator ',')) from app.t;")


def test_mysql_copies_a_span_again_in_one_transaction(my, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    for side in ("src", "dst"):
        my_sql(side, "drop database if exists app; create database app;"
                     " create table app.t (id int primary key,"
                     " v varchar(50));"
                     " set session cte_max_recursion_depth = 100000;"
                     " insert into app.t with recursive g(i) as (select 1"
                     " union all select i + 1 from g where i < 20000)"
                     " select i, concat('v', i) from g;"
                     " analyze table app.t;")
    my_sql("dst", "delete from app.t where id between 5001 and 8000;"
                  " update app.t set v = 'x' where id in (7, 15000, 19999);")
    damaged = _my_rows("dst")
    hop = Hop(name="rlmy", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=MY["src"][1],
                              user="root", password="test"),
              target=Endpoint(host="127.0.0.1", port=MY["dst"][1],
                              user="root", password="test"),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    ladder = _ladder(eng, "app", "t")
    out = ladder.run()
    assert (out.way, out.proof) == (RANGES, "ok"), out
    assert _my_rows("dst") == _my_rows("src")
    _rolled_back(eng, "app", tmp_path)
    assert _my_rows("dst") == damaged
