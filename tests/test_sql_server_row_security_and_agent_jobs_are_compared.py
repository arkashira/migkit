"""A SQL Server move's deep check reads the row-level security policies
and the Agent jobs of both sides and compares them (backlog MSSQL-x86,
item 27): a policy the data arrives without leaves every row visible to
everyone; a job that arrives without its schedule never runs. Neither is
said by an error.

On the `sqlserver` lane the policies, the character sets and the
rendering of the types that render differently are read off the edge
edition itself, a source database and a target database on one server.
The Agent's jobs and schedules are replayed from the rows its `msdb`
tables hold as the documentation shows them: the edge edition has no
Agent. Built, unproved on a full SQL Server (`mssql.UNPROVED_ON`).
"""
import subprocess
import time

from migkit import jobs
from migkit.config import Endpoint, Hop
from migkit.engines.mssql import MSSQLEngine
from tests import servers
from tests.conftest import mssql_run, needs_docker

#: the lane run of 2026-10-16 failed (hierarchyid needs the CLR, which
#: the edge edition has off): nothing is claimed proved here yet

# msdb.dbo.sysjobs / sysjobsteps / sysschedules as a full server keeps a
# nightly job of two steps, run at 02:00 every day
STEPS = [("nightly", 1, 1, "load", "TSQL", "exec dbo.load", 3, 2),
         ("nightly", 1, 2, "purge", "TSQL", "delete from dbo.stage", 1, 2)]
SCHEDULES = [("nightly", 1, 4, 1, 1, 0, 0, 0, 20000, 235959)]


class _Agent:
    def __init__(self, steps, schedules):
        self.steps, self.schedules = steps, schedules

    def _rows(self, side, db, sql, args=()):
        if "sysschedules" in sql:
            return self.schedules[side]
        if "sysjobsteps" in sql:
            return self.steps[side]
        raise AssertionError(sql)


def test_agent_jobs_are_compared_step_by_step_and_schedule_by_schedule():
    alike = _Agent({"src": STEPS, "dst": STEPS},
                   {"src": SCHEDULES, "dst": SCHEDULES})
    src, dst = jobs.shapes(alike, "app", "src"), jobs.shapes(alike, "app",
                                                             "dst")
    assert jobs.compare(src, dst) == ([], [])
    # the job arrived without its schedule, and its second step changed
    lost = _Agent({"src": STEPS, "dst": [STEPS[0], STEPS[1][:5]
                                         + ("delete from dbo.stage2", 1, 2)]},
                  {"src": SCHEDULES, "dst": []})
    bad, _ = jobs.compare(jobs.shapes(lost, "app", "src"),
                          jobs.shapes(lost, "app", "dst"))
    assert bad == ["job nightly: step 2 differs",
                   "job nightly: its schedules differ (1 on the source, 0"
                   " on the target)"]
    # switched off on the target (one side at a time): said, not failed
    off = _Agent({"src": STEPS, "dst": [s[:1] + (0,) + s[2:] for s in STEPS]},
                 {"src": SCHEDULES, "dst": SCHEDULES})
    assert jobs.compare(jobs.shapes(off, "app", "src"),
                        jobs.shapes(off, "app", "dst")) == (
        [], ["job nightly: on only on the source"])
    # a whitespace change in a command is not a difference
    spaced = _Agent({"src": STEPS, "dst": [STEPS[0][:5] + (
        "exec   dbo.load\n", 3, 2), STEPS[1]]},
        {"src": SCHEDULES, "dst": SCHEDULES})
    assert jobs.compare(jobs.shapes(spaced, "app", "src"),
                        jobs.shapes(spaced, "app", "dst"))[0] == []


@needs_docker
def test_the_deep_check_finds_a_lost_policy_charset_and_rendering():
    import pymssql
    name, port = servers.name("migkit-test-mssql-rls"), \
        servers.private_port(9)
    pw = servers.STRONG
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(mssql_run(name, port, pw), check=True,
                   capture_output=True)
    try:
        def sql(db, *qs):
            conn = pymssql.connect(server="127.0.0.1", port=str(port),
                                   user="sa", password=pw, database=db,
                                   autocommit=True, tds_version="7.4",
                                   login_timeout=10)
            try:
                cur, out = conn.cursor(), None
                for q in qs:
                    cur.execute(q)
                    out = cur.fetchall() if cur.description else None
                return out
            finally:
                conn.close()
        for _ in range(150):
            try:
                sql("master", "select 1")
                break
            except Exception:  # noqa: BLE001 - starting
                time.sleep(2)
        sql("master", "create database r", "create database rt")
        for d, text in (("r", "nvarchar(20)"), ("rt", "varchar(20)")):
            sql(d, f"create table dbo.t (id int primary key, name {text}"
                " null, h hierarchyid null, tenant int not null)",
                "create function dbo.mine(@t int) returns table with"
                " schemabinding as return select 1 as ok where @t = 1")
        sql("r", "insert into dbo.t values (1, N'ไทย', '/1/', 1)")
        sql("rt", "insert into dbo.t values (1, 'x', '/2/', 1)")
        sql("r", "create security policy dbo.tenants add filter predicate"
            " dbo.mine(tenant) on dbo.t with (state = on)")

        class Lane(MSSQLEngine):
            def _connect(self, side, db):
                return super()._connect(side, db if side == "src" else
                                        db + "t")
        ep = Endpoint(host="127.0.0.1", port=port, user="sa", password=pw)
        eng = Lane(Hop(name="lane", engine="mssql", source=ep, target=ep,
                       databases=["r"]))
        got = eng._deep_row_security("r")
        assert got.status == "diff" and "policy dbo.tenants: missing on" \
            " target" in got.detail, got
        sql("rt", "create security policy dbo.tenants add filter predicate"
            " dbo.mine(tenant) on dbo.t with (state = off)")
        got = eng._deep_row_security("r")
        assert got.status == "diff" and "switched OFF" in got.detail, got
        sql("rt", "alter security policy dbo.tenants with (state = on)")
        got = eng._deep_row_security("r")
        assert got.status == "ok" and "1 security policies match" in \
            got.detail, got
        got = eng._deep_charset("r")
        assert got.status != "ok" and "DOWNGRADE" in got.detail, got
        got = eng._deep_render("r")
        assert got.status == "diff" and "dbo.t.h: renders differently" in \
            got.detail, got
        # the edge edition has no Agent: what its msdb answers is said as
        # it is, never as a clean comparison of jobs it does not run
        got = eng._deep_jobs("r")
        assert (got.status, got.detail) == EDGE_JOBS or (
            got.status == "skip" and "unknown, not clean" in got.detail), got
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


#: what the edge edition's msdb answered for the jobs (2026-10-16): its
#: tables are there and hold no job, the Agent that would run one is not
EDGE_JOBS = ("ok", "no Agent jobs run in it")
