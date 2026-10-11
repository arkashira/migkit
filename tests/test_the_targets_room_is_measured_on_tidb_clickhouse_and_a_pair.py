"""The room a side has, as the server reports it (P0-PARITY.assess,
`assess.capacity`), without a server: the CPUs it gives a query, the
connections it has free and the queries running now.

* TiDB answers as a MySQL server does for its connections, and says nothing
  of the CPUs or a replica it does not report - a figure it cannot give is
  left out, never made up;
* ClickHouse reads its own settings and metrics, and a metric it cannot
  answer is left out;
* a pair asks each side's own engine, so a target's room is the target's.
"""
from types import SimpleNamespace

from migkit.config import Endpoint, Hop
from migkit.engines.clickhouse import ClickHouseEngine
from migkit.engines.hetero import HeteroEngine
from migkit.engines.tidb import TiDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("assess.capacity", ("tidb", "clickhouse", "hetero"))]

EP = Endpoint(host="10.0.0.9", port=1, user="u", password="p")


def _hop(name):
    return Hop(name="t", engine=name, source=EP, target=EP, databases=["d"])


def test_tidb_reports_its_free_connections_and_what_runs():
    eng = TiDBEngine(_hop("tidb"))
    seen = []

    def q(side, sql, *args, **kw):
        seen.append(sql)
        if "show global status" in sql:
            return [("Threads_connected", "10"), ("Threads_running", "3")]
        if "@@max_connections" in sql:
            return [(151,)]
        raise RuntimeError("not reported by TiDB")
    eng._q = q
    got = eng.capacity("dst", "d")
    assert got == {"free_connections": 141, "running": 2}, got
    # nothing it could not ask is claimed
    assert "cpus" not in got and "replica" not in got


def test_tidb_with_every_figure_missing_reports_what_it_could():
    eng = TiDBEngine(_hop("tidb"))

    def q(side, sql, *args, **kw):
        if "show global status" in sql:
            return []
        if "@@max_connections" in sql:
            return [(100,)]
        raise RuntimeError("no")
    eng._q = q
    assert eng.capacity("src", "d") == {"free_connections": 100,
                                        "running": 0}


def _clickhouse(answers):
    eng = ClickHouseEngine(_hop("clickhouse"))

    def rows(side, db, sql, *a, **k):
        for word, got in answers.items():
            if word in sql:
                if isinstance(got, Exception):
                    raise got
                return got
        return []
    eng._rows = rows
    return eng


def test_clickhouse_reports_the_threads_it_gives_a_query_and_its_slots():
    got = _clickhouse({"'max_threads'": [("auto(8)",)],
                       "metric = 'Query'": [(4,)],
                       "max_concurrent_queries": [(100,)]}
                      ).capacity("dst", "d")
    assert got == {"cpus": 8, "running": 4, "free_connections": 96}, got


def test_clickhouse_leaves_out_what_it_cannot_answer():
    got = _clickhouse({"'max_threads'": [("16",)],
                       "metric = 'Query'": RuntimeError("no access"),
                       "max_concurrent_queries": [(0,)]}
                      ).capacity("dst", "d")
    assert got == {"cpus": 16}, got


def test_a_pair_asks_each_side_s_own_engine():
    src = SimpleNamespace(capacity=lambda side, db: {"side": "source's"})
    dst = SimpleNamespace(capacity=lambda side, db: {"side": "target's"})
    pair = SimpleNamespace(src_engine=src, dst_engine=dst)
    assert HeteroEngine.capacity(pair, "src", "d") == {"side": "source's"}
    assert HeteroEngine.capacity(pair, "dst", "d") == {"side": "target's"}
