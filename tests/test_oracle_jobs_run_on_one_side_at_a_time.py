"""Scheduled jobs inside an Oracle database run on one side at a time
(P0-PARITY, `cutover.jobs`), without a server: the scheduler's jobs and the
old queue's read from the catalogue, switched off on the source before it
is held and on at the flip, each switch recorded before it is made and
undone from the record.

The statements are not proved here (`capabilities.UNPROVED`): none has run
against an Oracle server. What is held is what is migkit's own - which jobs
are the hop's, what is recorded, that a job already as wanted is not
touched, and that a job put back is put back as it was.
"""
import datetime
import tempfile
from pathlib import Path

import pytest

from migkit import jobs
from migkit.config import Endpoint, Hop
from migkit.engines.oracle import OracleEngine
from tests.oracle_catalogue import cat  # noqa: F401

EP = Endpoint(host="10.0.0.5", port=1521, user="app", password="CHANGE_ME")
DUE = datetime.datetime(2026, 10, 12, 3, 0, 0)


class Conn:
    """A connection that keeps what it was told to run."""

    def __init__(self, ran):
        self.ran = ran

    def cursor(self):
        return self

    def execute(self, sql, params=()):
        self.ran.append((" ".join(sql.split()), list(params)))

    def commit(self):
        self.ran.append(("commit", []))

    def close(self):
        pass


@pytest.fixture
def ran(monkeypatch):
    got = []
    monkeypatch.setattr(OracleEngine, "_connect",
                        lambda self, side, db: Conn(got))
    return got


def _hop(**kw):
    hop = Hop(name="t", engine="oracle", source=EP, target=EP,
              databases=["app"], **kw)
    where = Path(tempfile.mkdtemp())
    hop.report_dir = lambda db=None: where
    return hop


def _jobs(cat, scheduler, queue=()):
    cat.on("from all_scheduler_jobs j", scheduler)
    cat.on("from all_jobs", list(queue))
    cat.on("from all_objects", [("REFRESH_TOTALS",)])
    cat.on("from all_tables", [("ORDERS",), ("AUDIT_LOG",)])


def test_the_schedulers_jobs_and_the_old_queue_are_the_schemas_jobs(cat):
    _jobs(cat, [("NIGHTLY", "TRUE", "insert into orders select 1 from dual"),
                ("PAUSED", "FALSE", "begin cleanup; end;")],
          [(7, "N", "update orders set x = 1;", DUE)])
    hop = _hop()
    eng = OracleEngine(hop)
    assert jobs.inventory(hop, eng, "app", "src") == [
        ("NIGHTLY", True), ("PAUSED", False), ("dbms_job 7", True)]


def test_a_job_that_writes_only_what_the_hop_leaves_out_is_left_alone(cat):
    _jobs(cat, [("PURGE_AUDIT", "TRUE", "delete from audit_log"),
                ("NIGHTLY", "TRUE", "insert into orders select 1 from dual"),
                ("OPAQUE", "TRUE", "begin refresh_totals; end;")])
    hop = _hop(exclude=["audit_log"])
    eng = OracleEngine(hop)
    assert jobs.left_alone(hop, eng, "app", "src") == ["PURGE_AUDIT"]
    names = [n for n, _ in jobs.inventory(hop, eng, "app", "src")]
    assert names == ["NIGHTLY", "OPAQUE"]


def test_a_catalogue_the_account_cannot_read_is_unknown_not_none(cat):
    cat.on("from all_scheduler_jobs j", RuntimeError("ORA-00942"))
    hop = _hop()
    assert jobs.inventory(hop, OracleEngine(hop), "app", "src") is None


def test_a_job_is_recorded_then_switched_off_and_put_back_as_it_was(
        cat, ran):
    _jobs(cat, [("NIGHTLY", "TRUE", "insert into orders select 1 from dual"),
                ("PAUSED", "FALSE", "begin cleanup; end;")],
          [(7, "N", "update orders set x = 1;", DUE)])
    hop = _hop()
    eng = OracleEngine(hop)
    said = []
    n = jobs.switch(hop, eng, "app", "src",
                    ["NIGHTLY", "PAUSED", "dbms_job 7"], False, said.append)
    # the one already off is not touched
    assert n == 2
    assert [sql for sql, _ in ran if sql != "commit"] == [
        "begin dbms_scheduler.disable(:1, force => true); end;",
        "begin dbms_job.broken(:1, true); end;"]
    assert ran[0][1] == ['"APP"."NIGHTLY"'] and ran[2][1] == [7]
    record = jobs.recorded(hop)
    assert [(e["name"], e["was"]) for e in record] == [
        ("NIGHTLY", True), ("dbms_job 7", True)]
    # put back as they were: the scheduler's job on, the queue's job due at
    # the time it was
    ran.clear()
    jobs.undo(hop, eng, said.append, "src")
    assert [sql for sql, _ in ran if sql != "commit"] == [
        "begin dbms_job.broken(:1, false, :2); end;",
        "begin dbms_scheduler.enable(:1); end;"]
    assert ran[0][1] == [7, DUE]
    assert jobs.recorded(hop) == []


def test_a_job_on_a_target_is_switched_on_and_a_far_due_time_is_not_kept(
        cat, ran):
    _jobs(cat, [("NIGHTLY", "FALSE", "insert into orders select 1 from dual")],
          [(7, "Y", "update orders set x = 1;",
            datetime.datetime(4000, 1, 1))])
    hop = _hop()
    eng = OracleEngine(hop)
    n = jobs.switch(hop, eng, "app", "dst", ["NIGHTLY", "dbms_job 7"], True,
                    lambda m: None)
    assert n == 2
    assert [sql for sql, _ in ran if sql != "commit"] == [
        "begin dbms_scheduler.enable(:1); end;",
        "begin dbms_job.broken(:1, false); end;"]


def test_a_job_that_is_not_there_is_not_made_up(cat, ran):
    _jobs(cat, [])
    hop = _hop()
    assert jobs.switch(hop, OracleEngine(hop), "app", "dst", ["GONE"], True,
                       lambda m: None) == 0
    assert ran == []
