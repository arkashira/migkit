"""The PostgreSQL and SQL Server engines keep every grant, version,
setting and slot probe they run in the hop's probe accumulator, under the
keys the cutover reads (`diagnostics.where`), each held as long as its kind
holds (`diagnostics.HOLDS`); the PostgreSQL copier takes a key that is not
one whole number by ranges of that key, and every range it copies weighs
the ones not yet taken (`ranges.finished(rows=)`). Fakes only."""
import re
from types import SimpleNamespace

import pytest

from migkit import diagnostics, ranges
from migkit.engines.mssql import MSSQLEngine
from migkit.engines.postgres import PostgresEngine


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _acc():
    clock = Clock()
    return diagnostics.ProbeAccumulator(clock), clock


@pytest.fixture
def hop():
    got = SimpleNamespace(name="w14a-probes", options={})
    yield got
    diagnostics.forget(got)


def _bare(cls, hop):
    eng = object.__new__(cls)
    eng.hop = hop
    return eng


def test_a_grant_holds_until_the_consumer_bound_and_a_slot_for_one_read():
    acc, clock = _acc()
    assert acc.probed("grant", diagnostics.where("main", "src", "replication"),
                      lambda: True) is True
    assert acc.probed("slot", diagnostics.where("main", "src", "s1"),
                      lambda: "pgoutput") == "pgoutput"
    clock.now += 3600
    assert acc.stale("grant", "main:src:replication") == ""
    assert "usable for 30.0s" in acc.stale("grant", "main:src:replication",
                                           max_age=30)
    # a slot's state is a per-read fact: it is measured again, not reused
    assert "usable for 0.0s" in acc.stale("slot", "main:src:s1")
    calls = []
    assert acc.read("slot", "main:src:s1",
                    lambda: calls.append(1) or "pgoutput") == "pgoutput"
    assert calls == [1]


def test_a_failed_probe_is_kept_failed_and_raised_on():
    acc, _ = _acc()

    def broken():
        raise OSError("permission denied for pg_roles")
    with pytest.raises(OSError):
        acc.probed("grant", "main:src:replication", broken)
    assert "permission denied" in acc.stale("grant", "main:src:replication")
    assert acc.refusals([("grant", "main:src:replication")], 30) == [
        "grant of main:src:replication: could not be read (permission"
        " denied for pg_roles)"]


def test_answers_handed_in_close_together_do_not_shrink_the_bound():
    acc, clock = _acc()
    acc.record("lag", "main", ("0/16", True, 0.1, ""))
    clock.now += 0.001
    acc.record("lag", "main", ("0/16", False, 1.0, ""))
    clock.now += 1
    assert acc.get("lag", "main").moves == []
    assert acc.stale("lag", "main", max_age=30) == ""
    # measured twice and changed: paced, but never below one read's time
    acc.probed("rate", "src", lambda: 1, holds=None)
    acc.get("rate", "src").took = 2.0
    clock.now += 0.001
    acc.probed("rate", "src", lambda: 2, holds=None)
    got = acc.get("rate", "src")
    assert got.moves == [2.0] and acc.allowed_age(got, 60) == 1.0


def test_two_readers_of_one_fact_in_two_shapes_keep_one_answer():
    acc, _ = _acc()
    on = lambda r: bool(r and int(r[0][0] or 0))  # noqa: E731
    acc.probed("feature", "app:src:change tracking", lambda: [(1,)], keep=on)
    assert acc.get("feature", "app:src:change tracking").value is True
    acc.probed("feature", "app:src:change tracking", lambda: [["0"]],
               keep=on)
    assert acc.get("feature", "app:src:change tracking").value is False


def test_the_postgres_replication_grant_is_kept_where_the_cutover_reads(hop):
    eng = _bare(PostgresEngine, hop)
    asked = []
    eng._slot_rows = lambda side, db, sql: asked.append(sql) or [(True,)]
    eng._change_read("src", "main")
    got = diagnostics.accumulator(hop).get(
        "grant", diagnostics.where("main", "src", "replication"))
    assert got is not None and got.ok and got.value is True
    assert "rolreplication" in asked[0]
    # a second owner reads it without asking the server
    assert diagnostics.accumulator(hop).read(
        "grant", "main:src:replication",
        lambda: pytest.fail("measured again"), 60) is True


def test_the_postgres_version_and_wal_level_are_kept(hop):
    eng = _bare(PostgresEngine, hop)

    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            self.sql = sql

        def fetchone(self):
            return (170002,)
    eng._connect = lambda side, db, why: SimpleNamespace(
        cursor=Cur, close=lambda: None)
    assert eng._server_version("dst", "main") == 170002
    acc = diagnostics.accumulator(hop)
    assert acc.get("version", "main:dst").value == 170002
    assert acc.get("version", "main:dst").holds is None
    ask = {"select plugin": "", "show wal_level": "replica"}

    def answer(sql):
        return next(v for k, v in ask.items() if sql.startswith(k))
    with pytest.raises(SystemExit, match="wal_level is replica"):
        eng.slot_name = lambda: "migkit_w14a"
        eng._slot_ready("src", "main", ask=answer)
    assert acc.get("setting", "main:src:wal_level").value == "replica"
    assert acc.get("slot", "main:src:migkit_w14a").value == ""


def test_the_sql_server_version_and_change_tracking_are_kept(hop):
    eng = _bare(MSSQLEngine, hop)
    eng._rows = lambda side, db, sql: (
        [("16.0.4135.4",)] if "ProductVersion" in sql else [(0,)])
    assert eng._server_versions() == ("16.0.4135.4", "16.0.4135.4")
    acc = diagnostics.accumulator(hop)
    assert acc.get("version", "master:src").value == "16.0.4135.4"
    assert acc.get("version", "master:dst").value == "16.0.4135.4"
    rows = eng._assess_tracking("app")
    assert rows and rows[0]["item"] == "change tracking"
    got = acc.get("feature", "app:src:change tracking")
    assert got.ok and got.value is False


def _job(st, copied, density=1):
    return SimpleNamespace(
        db="main", key="public.t", st=st,
        ck=SimpleNamespace(save=lambda: None), log=lambda line: None,
        qt='"public"."t"', pk=None, cols=["id", "v"], rf=None, chunk=1000,
        later=SimpleNamespace(callback=lambda fn: None))


def _copier(hop, copied, rows_of=lambda pred: 1):
    eng = _bare(PostgresEngine, hop)
    eng._copy_ways = lambda *a: None
    eng.snapshot_mark = lambda side, db: None

    def checked(db, qt, cols, pred, pre, what, log, way=None, proved=None):
        copied.append((pred, pre))
        proved(rows_of(pred))
        return SimpleNamespace(n=rows_of(pred))
    eng._copy_checked = checked
    eng._copy_in_spans = lambda *a: pytest.fail("copied by where rows are")
    return eng


UUIDS = ["3f1c0000-0000-4000-8000-000000000000",
         "7a2b0000-0000-4000-8000-000000000000",
         "c4d50000-0000-4000-8000-000000000000"]


def test_a_uuid_keyed_table_copies_by_ranges_of_its_key(hop, monkeypatch):
    copied = []
    eng = _copier(hop, copied)
    eng.table_facts = lambda side, db: {"public.t": {"rows": 4000}}
    monkeypatch.setattr(ranges, "active", ranges.Slots(1))
    monkeypatch.setattr(ranges, "cut", lambda *a, **k: list(UUIDS))
    st = {"keyed_by": ["id"]}
    assert eng._copy_ranges(_job(st, copied), None) is True
    preds = [p for p, _ in copied]
    assert len(preds) == len(UUIDS) + 1
    # open below and open above: every key falls in one range
    bounds = [[u for u in UUIDS if f"'{u}'" in p] for p in preds]
    assert bounds == [UUIDS[:1], UUIDS[:2], UUIDS[1:], UUIDS[2:]]
    assert preds[0].startswith("not (") and "not" not in preds[-1]
    assert all(pre.startswith('delete from "public"."t" where')
               for _, pre in copied)
    assert st["keyed"] and st["done"]
    assert len(st["ranges_done"]) == len(st["ranges"]) == len(UUIDS) + 1
    # a resume plans nothing again and copies nothing twice
    copied.clear()
    st.pop("done")
    assert eng._copy_ranges(_job(st, copied), None) is True
    assert copied == []


def test_a_composite_key_copies_by_tuples(hop, monkeypatch):
    copied = []
    eng = _copier(hop, copied)
    eng.table_facts = lambda side, db: {"public.t": {"rows": 4000}}
    monkeypatch.setattr(ranges, "active", ranges.Slots(1))
    monkeypatch.setattr(ranges, "cut",
                        lambda *a, **k: [["a", "2024-01-01"],
                                         ["m", "2024-06-01"]])
    st = {"keyed_by": ["region", "day"]}
    assert eng._copy_ranges(_job(st, copied), None) is True
    assert len(copied) == 3
    assert '"region"' in copied[1][0] and '"day"' in copied[1][0]


def test_a_table_without_a_key_is_not_taken_by_key_ranges(hop):
    eng = _bare(PostgresEngine, hop)
    assert eng._copy_keyed(_job({"keyed_by": []}, [])) is False
    # a plan of another kind already begun is not replaced
    assert eng._copy_keyed(_job({"keyed_by": ["id"],
                                 "ranges": [[0, 10]]}, [])) is False


def test_the_postgres_copy_cuts_its_pending_ranges_again_as_they_finish(
        hop, monkeypatch):
    copied = []

    def dense(pred):
        # every range holds three times the rows it was planned for
        lo, hi = (int(x) for x in re.findall(r"\d+", pred)[:2])
        return (hi - lo) * 3
    eng = _copier(hop, copied, dense)
    eng._psql = lambda side, db, sql: "1|1000|1" if "min(" in sql else "0"
    eng.table_facts = lambda side, db: {"public.t": {"rows": 10 ** 6}}
    monkeypatch.setattr(ranges, "active", ranges.Slots(1))

    def cut(*a, into=None, **k):
        into["every"] = 100
        return list(range(100, 1000, 100))
    monkeypatch.setattr(ranges, "cut", cut)
    st = {}
    job = _job(st, copied)
    job.pk = "id"
    assert eng._copy_ranges(job, None) is True
    assert len(copied) > 10
    assert len(st["ranges"]) > 10 and st["last"] == 1000
