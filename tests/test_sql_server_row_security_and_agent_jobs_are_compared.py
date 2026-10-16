"""A SQL Server move's deep check reads the row-level security policies
and the Agent jobs of both sides and compares them (backlog MSSQL-x86,
item 27): a policy the data arrives without leaves every row visible to
everyone; a job that arrives without its schedule never runs. Neither is
said by an error.

On the `sqlserver` lane the policies, the character sets, the rendering
of the types that render differently, the row filter
and the table names a hop gives are read off the edge edition itself, a
source database and a target database on one server. The Agent's jobs
and schedules are replayed from the rows its `msdb` tables hold as the
documentation shows them: the edge edition's Agent is stopped and its
`msdb` keeps only the capture jobs the server makes. Built, unproved on
a full SQL Server (`mssql.UNPROVED_ON`).
"""
import contextlib
import subprocess
import time

import pytest

from migkit import jobs
from migkit.config import Endpoint, Hop
from migkit.engines.mssql import MSSQLEngine
from tests import servers
from tests.conftest import mssql_run, needs_docker

#: what this module proves, item by engine (`capabilities.proofs`): the
#: lane tests below run each against the edge edition
PROVES = [
    ("verify.structure.row-security", "mssql"),
    ("verify.value.charset", "mssql"),
    ("verify.value.rendering", "mssql"),
    ("scope.row-filter", "mssql"),
    ("scope.names", "mssql"),
]

#: what the edge edition answered to a `hierarchyid` value
#: (2026-10-16, the `sqlserver` lane): error 10347, the CLR is not on, so
#: a column of that type is made and holds nothing; `xml` and `sql_variant`
#: hold values and render
EDGE_CLR_REFUSAL = "10347"

#: what the edge edition answered to the grants read (2026-10-16): error
#: 207, invalid column name, in the catalogue's key view
EDGE_GRANTS_REFUSAL = "asym_key_id"

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


@contextlib.contextmanager
def _edge(slot, label):
    """A SQL Server edge edition on the lane and `sql(db, *queries)` to ask
    it; the container is removed after."""
    import pymssql
    name, port = servers.name(f"{servers.PREFIX}-mssql-{label}"), \
        servers.private_port(slot)
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
        yield sql, port, pw
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _pair(port, pw, **kw):
    """An engine whose target is the source's database with a `t` after
    its name, on the one server."""
    class Lane(MSSQLEngine):
        def _connect(self, side, db):
            return super()._connect(side, db if side == "src" else
                                    db + "t")
    ep = Endpoint(host="127.0.0.1", port=port, user="sa", password=pw)
    return Lane(Hop(name="lane", engine="mssql", source=ep, target=ep,
                    databases=["r"], **kw))


@needs_docker
def test_the_deep_check_finds_a_lost_policy_charset_and_rendering():
    with _edge(9, "rls") as (sql, port, pw):
        sql("master", "create database r", "create database rt")
        for d, text in (("r", "nvarchar(20)"), ("rt", "varchar(20)")):
            sql(d, f"create table dbo.t (id int primary key, name {text}"
                " null, x xml null, tenant int not null)",
                "create function dbo.mine(@t int) returns table with"
                " schemabinding as return select 1 as ok where @t = 1",
                "create user app without login")
        # the CLR is off in the edge edition: no hierarchyid value is made
        sql("master", "create database z")
        sql("z", "create table dbo.h (a hierarchyid null)")
        with pytest.raises(Exception) as clr:
            sql("z", "insert into dbo.h values ('/1/')")
        assert EDGE_CLR_REFUSAL in str(clr.value), clr.value
        sql("r", "insert into dbo.t (id, name, x, tenant) values"
            " (1, N'ไทย', N'<a>1</a>', 1)")
        sql("rt", "insert into dbo.t (id, name, x, tenant) values"
            " (1, 'x', N'<a>2</a>', 1)")
        sql("r", "create security policy dbo.tenants add filter predicate"
            " dbo.mine(tenant) on dbo.t with (state = on)")
        eng = _pair(port, pw)
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
        assert got.status == "diff" and "dbo.t.x: renders differently" in \
            got.detail, got
        # the table's grants are read by a catalogue query the edge
        # edition cannot answer: it has no `sys.asymmetric_keys.asym_key_id`
        # (so `verify.access.table-grants` stays declared, not proved)
        sql("r", "grant select on dbo.t to app")
        with pytest.raises(Exception) as grants:
            eng._deep_grants("r")
        assert EDGE_GRANTS_REFUSAL in str(grants.value), grants.value
        # the Agent is stopped and its msdb keeps no job of this database
        # (the capture jobs belong to a database that captures): said as
        # it is, never as a clean comparison of jobs it does not run
        got = eng._deep_jobs("r")
        assert (got.status, got.detail) == EDGE_JOBS, got


@needs_docker
def test_the_rows_a_filter_selects_and_the_names_a_hop_gives_are_counted():
    with _edge(8, "scope") as (sql, port, pw):
        sql("master", "create database r", "create database rt")
        for d in ("r", "rt"):
            sql(d, "create table dbo.orders (id int primary key, tenant"
                " int not null)")
        sql("r", "insert into dbo.orders values (1, 1), (2, 2), (3, 1)")
        sql("rt", "insert into dbo.orders values (1, 1), (3, 1)")
        eng = _pair(port, pw, mapping={"where": {"dbo.orders":
                                                 "tenant = 1"}})
        # 3 rows against 2 would differ: counted by the filter, 2 and 2
        (got,) = eng.check_counts("r")
        assert got.status == "ok", got
        # the move puts no row outside the filter on the target
        sql("rt", "insert into dbo.orders values (4, 2)")
        (got,) = eng.check_counts("r")
        assert got.status == "diff" and "dbo.orders dst holds 1 rows the" \
            " hop's row filter excludes" in got.detail, got
        # a table the hop renames is the target's under its new name
        sql("rt", "exec sp_rename 'dbo.orders', 'orders_new'")
        eng = _pair(port, pw, mapping={"tables": {
            "dbo.orders": "dbo.orders_new"}})
        assert eng._target_name("r", "dbo.orders") == "dbo.orders_new"
        (got,) = eng.check_counts("r")
        assert got.status == "ok", got
        sql("rt", "delete from dbo.orders_new where id = 4")
        (got,) = eng.check_counts("r")
        assert got.status == "diff" and "dbo.orders src=3 dst=2" in \
            got.detail, got
        sql("r", "delete from dbo.orders where id = 2")
        (got,) = eng.check_counts("r")
        assert got.status == "ok", got


#: what the edge edition's msdb answered for the jobs of a database that
#: captures nothing (2026-10-16): its tables are there and hold the jobs
#: of the databases that capture, the Agent that would run one is stopped
EDGE_JOBS = ("ok", "no Agent jobs run in it")
