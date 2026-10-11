"""The SQL Server Agent jobs with a step in the database run on one side at
a time (P0-PARITY.all, `jobs`): each the source ran is switched off there
before it is frozen and on at the target at the flip, each switch recorded
before it is made and undone from the record; a job that writes only
tables the hop leaves out is left as it is.

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
catalogue reads and keeps the statements sent. Every statement here is new
and has not run on a server: `cutover.jobs` waits in
`capabilities.UNPROVED`. The partitioned table's way is the deep check
(`decide.WAYS`), whose statements are as new: `move.partitioned` waits too.
"""
from migkit import jobs
from tests.mssql_stub import Server, engine

STEPS = [("nightly_close", 1, "exec dbo.close_day"),
         ("nightly_close", 1, "update dbo.orders set closed = 1"),
         ("purge_audit", 1, "delete from dbo.audit_log where old = 1"),
         ("report", 0, "select * from dbo.orders")]


def _server():
    s = Server()
    s.on("msdb\\.dbo\\.sysjobs", STEPS)
    s.on("from sys\\.tables t", [("dbo.orders",), ("dbo.audit_log",)])
    s.on("o\\.type in \\('P', 'FN'", [("close_day",)])
    return s


def test_the_jobs_of_the_database_are_read_with_what_they_run(monkeypatch,
                                                               tmp_path):
    eng = engine(monkeypatch, tmp_path, _server())
    assert jobs.inventory(eng.hop, eng, "appdb", "src") == [
        ("nightly_close", True), ("purge_audit", True), ("report", False)]


def test_a_job_that_writes_only_what_the_hop_leaves_out_is_left_alone(
        monkeypatch, tmp_path):
    eng = engine(monkeypatch, tmp_path, _server(),
                 exclude=["appdb.dbo.audit_log"])
    # the job on the left-out table is the target's own; the one that runs a
    # routine may write anything, and stays the hop's
    assert jobs.inventory(eng.hop, eng, "appdb", "src") == [
        ("nightly_close", True), ("report", False)]
    assert jobs.left_alone(eng.hop, eng, "appdb", "src") == ["purge_audit"]


def test_a_switch_is_recorded_first_and_undone_from_the_record(monkeypatch,
                                                                tmp_path):
    s = _server()
    eng = engine(monkeypatch, tmp_path, s)
    said = []
    seen = []

    def switch(side, sql):
        # recorded before the switch it records
        seen.append((sql, jobs.recorded(eng.hop)))
        return []
    s.rules.insert(0, (None, __import__("re").compile("sp_update_job"),
                       switch))
    n = jobs.switch(eng.hop, eng, "appdb", "src",
                    ["nightly_close", "purge_audit", "report"], False,
                    said.append)
    # the one already off is not switched, and is not recorded
    assert n == 2
    assert [q for _, q in s.asked if "sp_update_job" in q] == [
        "exec msdb.dbo.sp_update_job @job_name = N'nightly_close',"
        " @enabled = 0",
        "exec msdb.dbo.sp_update_job @job_name = N'purge_audit',"
        " @enabled = 0"]
    assert [r["name"] for r in seen[0][1]] == ["nightly_close"]
    assert [r["was"] for r in jobs.recorded(eng.hop)] == [True, True]
    assert any("2 jobs switched off on the source" in m for m in said)
    s.asked.clear()
    jobs.undo(eng.hop, eng, lambda m: None)
    # last first, each as it was
    assert [q for _, q in s.asked if "sp_update_job" in q] == [
        "exec msdb.dbo.sp_update_job @job_name = N'purge_audit',"
        " @enabled = 1",
        "exec msdb.dbo.sp_update_job @job_name = N'nightly_close',"
        " @enabled = 1"]
    assert jobs.recorded(eng.hop) == []


def test_a_job_name_with_a_quote_stays_a_name():
    assert jobs._ms_enable("it's nightly", True) == (
        "exec msdb.dbo.sp_update_job @job_name = N'it''s nightly',"
        " @enabled = 1")


def test_a_server_with_no_agent_is_unread_not_empty(monkeypatch, tmp_path):
    def refuse(side, sql):
        raise RuntimeError("the source refused a statement: Invalid object"
                           " name 'msdb.dbo.sysjobs'")
    eng = engine(monkeypatch, tmp_path, Server().on("msdb", refuse))
    assert jobs.inventory(eng.hop, eng, "appdb", "src") is None


def test_a_partitioned_table_has_its_way(monkeypatch, tmp_path):
    from migkit import decide
    assert decide.cell("mssql", "partitioned") == ("yes", "@check_deep")
    assert "mssql" not in [n for n, row in decide.coverage().items()
                           if row["partitioned"] in ("undeclared", "stale")]
