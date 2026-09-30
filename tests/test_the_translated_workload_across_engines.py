"""The source's own statements after a change of engine (backlog R23, 4).

The statements come from the source's statement statistics in its own
SQL; each is translated into the target's by the one translator migkit
carries, planned on both sides, timed where it can be run as it is, and -
across engines, or across two major versions of one - held to the same
answer. A write is planned on both and never run on the source. A table
the source reaches through an index and the target reads whole is a
finding, and the fix names the index as the target makes it.

The pure parts - answers compared as values, plans matched across engines
through the hop's renames, a SQL Server plan read - need no server; the
last part runs MySQL 8.4 into PostgreSQL 16.
"""
import datetime
import decimal
import subprocess
import time

import pytest

from tests.conftest import needs_docker

MY, PG = ("migkit-test-xwork-my", 16332), ("migkit-test-xwork-pg", 16333)


# --- answers, as values two engines agree on ------------------------------------

def test_two_answers_are_the_same_rows_in_any_order():
    from migkit import workload
    assert workload.alike([(1, "a"), (2, "b")], [(2, "b"), (1, "a")]) is True
    # a number by its digits, a moment in UTC, a boolean as one or zero
    utc7 = datetime.timezone(datetime.timedelta(hours=7))
    assert workload.alike(
        [(decimal.Decimal("1.50"), datetime.datetime(2026, 9, 1, 3, 0), 1,
          decimal.Decimal("2.00"))],
        [(1.5, datetime.datetime(2026, 9, 1, 10, 0, tzinfo=utc7), True,
          2)]) is True


def test_an_answer_that_differs_is_said():
    from migkit import workload
    said = workload.alike([(1,)], [(2,)])
    assert said == "1 of its 1 rows answered otherwise on the target", said
    assert workload.alike([(1,)], [(1,), (1,)]) == \
        "2 rows on the target against 1"
    # a row twice on one side is not the same answer as once
    assert workload.alike([(1,), (2,)], [(1,), (1,)]) is not True


# --- plans matched across engines ---------------------------------------------

def test_a_table_read_whole_is_matched_through_the_hops_renames():
    from migkit import workload
    from migkit.config import Endpoint, Hop

    class Eng:
        hop = Hop(name="w", engine="hetero", source=Endpoint(),
                  target=Endpoint(), mapping={"tables": {"orders": "sales"}})
    lost = workload._read_whole(Eng(), "app", [
        {"orders": "index o_k", "items": None},
        {"public.sales": None, "public.items": None}], True)
    # the renamed table read whole on the target is found by its new name;
    # a table read whole on both sides is not a finding
    assert lost == ["public.sales"], lost
    # one engine: by name, as it always was
    assert workload._read_whole(Eng(), "app", [
        {"t": "index t_k"}, {"t": None}], False) == ["t"]
    assert workload._read_whole(Eng(), "app", [
        {"t": "index t_k"}, {"t": "index t_k"}], False) == []


def test_the_planners_settings_that_differ_are_said():
    from migkit import workload

    class Side:
        def __init__(self, got):
            self.got = got

        def planner_settings(self, side, db):
            return self.got
    same = workload.settings_differ(Side({"jit": "on"}), Side({"jit": "on"}),
                                    "app")
    assert same == []
    said = workload.settings_differ(
        Side({"work_mem": "4096kB", "jit": "on"}),
        Side({"work_mem": "65536kB", "jit": "on"}), "app")
    assert said == ["settings the planner reads differ - work_mem: the"
                    " source's 4096kB, the target's 65536kB"], said


def test_writes_are_timed_only_on_a_rehearsals_own_target(tmp_path,
                                                         monkeypatch):
    import json

    from migkit import cutover, workload
    from migkit.config import Endpoint, Hop
    hop = Hop(name="r", engine="postgres", source=Endpoint(),
              target=Endpoint(), goal="copy")
    hop.report_dir = lambda db=None: tmp_path
    assert workload._writes_timed(hop) is False
    hop.goal = "rehearse"
    assert workload._writes_timed(hop) is True
    (tmp_path / "cutover-runs.jsonl").write_text(
        json.dumps({"state": "flipped", "outage": 1.0}) + "\n")
    # after its cutover the target is live: nothing is run there
    assert cutover.last_run(hop)["state"] == "flipped"
    assert workload._writes_timed(hop) is False


def test_a_sql_server_plan_and_statement_are_read():
    from migkit.engines.mssql import _without_parameters, plan_tables
    assert _without_parameters(
        "(@P1 int,@P2 nvarchar(50))select * from t where a = @P1") == \
        "select * from t where a = @P1"
    assert _without_parameters("select 1") == "select 1"
    ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"
    xml = (f'<ShowPlanXML xmlns="{ns}"><BatchSequence><Batch><Statements>'
           '<StmtSimple><QueryPlan>'
           '<RelOp PhysicalOp="Index Seek"><IndexScan>'
           '<Object Table="[orders]" Index="[o_k]"/></IndexScan></RelOp>'
           '<RelOp PhysicalOp="Clustered Index Scan"><IndexScan>'
           '<Object Table="[items]" Index="[pk_items]"/></IndexScan></RelOp>'
           '<RelOp PhysicalOp="Table Scan"><TableScan>'
           '<Object Table="[heap]"/></TableScan></RelOp>'
           '</QueryPlan></StmtSimple></Statements></Batch></BatchSequence>'
           '</ShowPlanXML>')
    assert plan_tables(xml) == {"orders": "index o_k", "items": None,
                                "heap": None}


# --- MySQL into PostgreSQL ------------------------------------------------------

def _my(sql, db=None):
    cmd = ["docker", "exec", "-i", MY[0], "mysql", "-uroot", "-ptest", "-N",
           "-B"] + (["-D", db] if db else [])
    got = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pg(sql, db="app"):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG[0],
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", sql],
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    try:
        for name, port, image in ((MY[0], MY[1], "mysql:8.4"),
                                  (PG[0], PG[1], "postgres:16")):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            env = (["-e", "MYSQL_ROOT_PASSWORD=test"] if "mysql" in image
                   else ["-e", "POSTGRES_PASSWORD=test"])
            subprocess.run(["docker", "run", "-d", "--name", name, *env,
                            "-p", f"127.0.0.1:{port}:"
                            + ("3306" if "mysql" in image else "5432"),
                            image], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY[0], "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        for _ in range(60):
            if subprocess.run(["docker", "exec", PG[0], "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                break
            time.sleep(1)
        time.sleep(2)
        _my("create database app; use app;"
            " create table t (id int primary key, k int, pad varchar(80),"
            " key t_k (k));"
            " set session cte_max_recursion_depth = 1000000;"
            " insert into t with recursive s(g) as (select 1 union all"
            " select g + 1 from s where g < 300000)"
            " select g, g % 50000, repeat('x', 60) from s;"
            " analyze table t;")
        _pg("create database app", db="postgres")
        _pg("create table public.t (id int primary key, k int,"
            " pad varchar(80));"
            " insert into public.t select g, g % 50000, repeat('x', 60)"
            " from generate_series(1, 300000) g;"
            " create index t_k on public.t (k); analyze public.t;")
        # the application's statements, as the source keeps them
        for _ in range(30):
            _my("select count(*) from t where k = 777;"
                " select max(k) from t;", db="app")
        for _ in range(5):
            _my("update t set pad = 'y' where k = 5;", db="app")
        yield
    finally:
        for name in (MY[0], PG[0]):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="xwork", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=MY[1], user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG[1], user="postgres",
                              password="test"),
              databases=["app"],
              options={"source_engine": "mysql", "target_engine": "postgres"})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


@needs_docker
def test_the_translated_statements_answer_alike(pair, tmp_path):
    from migkit import workload
    got = workload.compare(_engine(tmp_path), "app")
    assert got.status == "ok", got.detail
    assert "of the source's busiest reads planned on both sides" \
        in got.detail, got.detail
    # the reads were run on both engines and answered alike, every run
    assert "runs answered alike, weighted by how often each is run" \
        in got.detail, got.detail
    assert "of its busiest writes planned on both sides, never run on" \
        " the source" in got.detail, got.detail
    # the update is not run anywhere: the pad it would write is not there
    assert _pg("select count(*) from public.t where pad = 'y'") == "0"
    # the check the hetero pair runs carries it
    deep = _engine(tmp_path).check_deep("app")
    assert any(r.scope == "app performance" for r in deep), \
        [r.scope for r in deep]


@needs_docker
def test_an_index_the_target_lacks_is_found_and_written_for_it(pair,
                                                               tmp_path):
    from migkit import workload
    _pg("drop index public.t_k")
    try:
        got = workload.compare(_engine(tmp_path), "app")
        assert got.status == "warn", got.detail
        assert "reads t whole on the target where the source uses index" \
               " t_k" in got.detail, got.detail
        # the literal a sample ran with is not shown
        assert "777" not in got.detail, got.detail
        assert 'create index "t_k" on "public"."t" ("k")' in got.fix_hint, \
            got.fix_hint
        assert (tmp_path / "for-the-target.sql").read_text().count(
            'create index "t_k"') == 1
    finally:
        _pg("create index t_k on public.t (k); analyze public.t")


@needs_docker
def test_a_translated_read_that_answers_otherwise_is_found(pair, tmp_path):
    from migkit import workload
    _pg("delete from public.t where k = 777")
    try:
        got = workload.compare(_engine(tmp_path), "app")
        assert got.status == "warn", got.detail
        assert "answered otherwise on the target" in got.detail, got.detail
    finally:
        _pg("insert into public.t select g, g % 50000, repeat('x', 60)"
            " from generate_series(1, 300000) g where g % 50000 = 777")
