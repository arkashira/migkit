"""Verification runs under the run's own budget and chooses its ways by
what they measured (backlog P0-VERIFY-EXECUTION): `leaves`' reads borrow
the run's slots, the delta loop's databases are verified side by side,
row parity climbs a costed ladder in `decide`, and PostgreSQL and SQL
Server cut a deep check's tables into key ranges from their statistics.

Pure: the servers' answers are given in-process.
"""
import inspect
import threading
import time
from types import SimpleNamespace

import pytest

from migkit import decide, leaves, ranges, scanplan, sizing, verifiers
from migkit.engines.mssql import MSSQLEngine
from migkit.engines.postgres import PostgresEngine

PROVES = [("verify.value.one-plan", ("postgres", "mssql"))]


def _peak(fn_items, most, parent=None):
    """The most calls of `together` in flight at once."""
    lock, now, peak = threading.Lock(), [0], [0]

    def fn(x):
        with lock:
            now[0] += 1
            peak[0] = max(peak[0], now[0])
        time.sleep(0.05)
        with lock:
            now[0] -= 1
        return x * 2
    token = ranges._current_coordinator_var.set(parent)
    try:
        got = scanplan.together(fn_items, fn, most)
    finally:
        ranges._current_coordinator_var.reset(token)
    return got, peak[0]


def test_reads_together_keep_their_order_and_run_side_by_side():
    got, peak = _peak([1, 2, 3, 4], 4)
    assert got == [2, 4, 6, 8]
    assert peak > 1


def test_reads_together_are_held_to_the_run_budget_they_borrow():
    got, peak = _peak([1, 2, 3, 4], 4, parent=ranges.Slots(1))
    assert got == [2, 4, 6, 8]
    assert peak == 1


def test_a_failed_read_together_is_raised():
    def fn(x):
        if x == 2:
            raise RuntimeError("refused")
        return x
    with pytest.raises(RuntimeError, match="refused"):
        scanplan.together([1, 2, 3], fn, 3)


def test_leaves_open_no_pool_of_their_own():
    src = inspect.getsource(leaves)
    assert "ThreadPoolExecutor" not in src
    seen = []
    a, b = SimpleNamespace(n="a"), SimpleNamespace(n="b")
    assert leaves.both(lambda s, k: seen.append(s.n) or s.n + k,
                       a, b, "!") == ("a!", "b!")
    assert sorted(seen) == ["a", "b"]


def test_ranges_cut_from_statistics_cover_every_key_and_null():
    preds = scanplan.bounded("k", ["10", "20"])
    assert preds == ["(k < 10 or k is null)", "k >= 10 and k < 20",
                     "k >= 20"]

    def holds(p, v):
        if v is None:
            return "is null" in p
        p = p.replace(" or k is null", "").strip("()")
        return eval(p.replace("k", str(v)))
    for v in (None, -5, 9, 10, 19, 20, 10 ** 12):
        assert sum(holds(p, v) for p in preds) == 1, v
    assert scanplan.bounded("k", []) == [""]


def test_a_table_is_cut_no_finer_than_the_plan_or_a_range_holds():
    assert scanplan.cuts(10 * ranges.LEAST, 4) == 4
    assert scanplan.cuts(2 * ranges.LEAST, 8) == 2
    assert scanplan.cuts(ranges.LEAST - 1, 8) == 1
    assert scanplan.cuts(None, 8) == 1
    assert scanplan.spread(list(range(100)), 4) == [25, 50, 75]
    assert scanplan.spread([1, 1, 1], 3) == [1]
    assert scanplan.spread([5], 1) == []


def _pg(stats, versions, hist):
    eng = PostgresEngine.__new__(PostgresEngine)
    eng._server_version = lambda side, db: versions[side]
    eng._psql = lambda side, db, sql: stats if "relpages" in sql else hist
    return eng


def test_postgres_cuts_block_ranges_where_both_servers_read_them_alone():
    eng = _pg(f"1000|{8 * ranges.LEAST}", {"src": 160000, "dst": 150000},
              "id|1,2,3")
    got = eng.scan_cuts("app", "public.orders", 4)
    assert got == ["ctid < '(250,0)'::tid",
                   "ctid >= '(250,0)'::tid and ctid < '(500,0)'::tid",
                   "ctid >= '(500,0)'::tid and ctid < '(750,0)'::tid",
                   "ctid >= '(750,0)'::tid"]


def test_postgres_cuts_its_key_histogram_on_an_older_server():
    eng = _pg(f"1000|{8 * ranges.LEAST}", {"src": 160000, "dst": 130000},
              "id|" + ",".join(str(i * 10) for i in range(100)))
    got = eng.scan_cuts("app", "public.orders", 2)
    assert got == ['"id" < 500', '"id" >= 500']


def test_postgres_reads_whole_what_its_statistics_cannot_cut():
    assert _pg("1000|0", {"src": 160000, "dst": 160000}, "").scan_cuts(
        "app", "public.orders", 4) == [""]
    assert _pg(f"1000|{8 * ranges.LEAST}", {"src": 130000, "dst": 130000},
               "").scan_cuts("app", "public.orders", 4) == [""]


def test_sql_server_cuts_its_key_histogram_from_partition_stats():
    eng = MSSQLEngine.__new__(MSSQLEngine)
    asked = []

    def cmd(side, db, sql):
        asked.append(sql)
        if "dm_db_partition_stats" in sql:
            return [[str(3 * ranges.LEAST)]]
        return [["id", str(i)] for i in range(0, 300, 10)]
    eng._cmd = cmd
    got = eng.scan_cuts("app", "dbo.orders", 3)
    assert len(got) == 3 and got[0].endswith("< 100")
    assert got[-1].endswith(">= 200")
    assert "dm_db_stats_histogram" in asked[1]
    eng._cmd = lambda side, db, sql: [["0"]]
    assert eng.scan_cuts("app", "dbo.orders", 3) == [""]


def test_the_plan_reads_each_range_the_engine_cuts(tmp_path, monkeypatch):
    monkeypatch.setattr(scanplan, "plan_most", lambda eng, db: 2)
    sqls = []
    eng = SimpleNamespace(
        hop=SimpleNamespace(report_dir=lambda db=None: tmp_path),
        scan_cuts=lambda db, t, most: ["k < 5", "k >= 5"])
    agg = scanplan.Aggregate("nullempty", "t", "c", ("sum(1)",))

    def query(side, sql):
        sqls.append(sql)
        return [(3, 3)]
    got = scanplan.for_engine(eng, "app", [agg], source=lambda s, t: t,
                              query=query)
    assert got.value("src", agg) == (6,) and got.ways["t"] == "ranges"
    assert any("where k < 5" in q for q in sqls)
    many = SimpleNamespace(
        hop=eng.hop, scan_cuts=lambda db, t, most: ["a", "b", "c"])
    sqls.clear()
    got = scanplan.for_engine(many, "app", [agg], source=lambda s, t: t,
                              query=query)
    assert got.value("src", agg) == (3,)
    assert not any(" where " in q for q in sqls)


def test_row_parity_goes_the_way_measured_cheaper():
    costs = decide.Costs({"server digest": 2.0, "key-range digest": 1.0})
    facts = decide.Facts({"server digest": True, "key": "id"})
    assert decide.row_parity("t", lambda c: c.rung.name, facts,
                             costs=costs)[1] == "key-range digest"
    costs = decide.Costs({"server digest": 1.0, "key-range digest": 2.0})
    assert decide.row_parity("t", lambda c: c.rung.name, facts,
                             costs=costs)[1] == "server digest"


def test_ranges_kept_by_an_earlier_run_are_resumed_whatever_they_cost():
    costs = decide.Costs({"server digest": 1.0, "key-range digest": 9.0})
    facts = decide.Facts({"server digest": True, "key": "id"})
    assert decide.row_parity("t", lambda c: c.rung.name, facts,
                             need=("resume by chunk",),
                             costs=costs)[1] == "key-range digest"


def test_a_way_that_fails_switches_and_none_left_is_said():
    facts = decide.Facts({"server digest": True, "key": "id"})
    tried = []

    def run(c):
        tried.append(c.rung.name)
        if c.rung.name == "server digest":
            raise RuntimeError("statement timeout")
        return "ok"
    costs = decide.Costs({"server digest": 1.0, "key-range digest": 2.0})
    assert decide.row_parity("t", run, facts, costs=costs)[1] == "ok"
    assert tried == ["server digest", "key-range digest"]
    with pytest.raises(decide.Exhausted):
        decide.row_parity("t", run, decide.Facts({"server digest": False}))


def test_the_delta_loop_verifies_its_databases_side_by_side(monkeypatch):
    monkeypatch.setattr(sizing, "estimate",
                        lambda *a, **k: sizing.Plan(3, 3, {}))
    monkeypatch.setattr(sizing, "resources", lambda *a, **k: {})
    met = threading.Barrier(3, timeout=5)

    def fn(db):
        met.wait()
        if db == "b":
            raise RuntimeError("gone")
        return db.upper()
    got = verifiers.each_database(SimpleNamespace(workers=3), object(),
                                  ["a", "b", "c"], fn)
    assert [d for d, _ in got] == ["a", "b", "c"]
    assert got[0][1] == "A" and got[2][1] == "C"
    assert isinstance(got[1][1], RuntimeError)
    assert verifiers.each_database(SimpleNamespace(workers=3), object(),
                                   [], fn) == []
