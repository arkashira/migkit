"""TiDB reads only the tables the hop takes (P0-PARITY.scope), without a
server: the engine's own listings, checks and counters over a catalogue
that holds a table the hop excludes (`audit`, the target's own) and an
online schema change's working table (`_orders_gho`, which goes when it
swaps in).

Each is read through the one scope answer (`Engine.in_scope`), so none of
the statements a check sends names a table the hop leaves out.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import mysql
from migkit.engines.tidb import TiDBEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.listings", "tidb"), ("scope.copy", "tidb"),
          ("scope.deep", "tidb"), ("scope.counters", "tidb")]

EP = Endpoint(host="x", port=1, user="x", password="x")
LEFT_OUT = ("audit", "_orders_gho")


@pytest.fixture
def asked(monkeypatch):
    """Every statement the engine sends, with its arguments, answered from
    a catalogue of three tables."""
    log = []

    def q(self, side, sql, args=None, fresh=False):
        log.append((side, " ".join(sql.split()), tuple(args or ())))
        low = sql.lower()
        if "count(" in low:
            return [(3,)]
        if "auto_increment%" in low:
            return [("orders", "id"), ("audit", "id")]
        if "max(" in low:
            return [(7,)]
        if low.startswith("show table"):
            return [("d", "orders", "id", "100", "AUTO_INCREMENT")]
        return []
    monkeypatch.setattr(mysql.MySQLEngine, "_q", q)
    # the clock a check's consistent read begins at is not this test's
    monkeypatch.setattr(TiDBEngine, "_tso", lambda self, side="src": 1)
    monkeypatch.setattr(mysql.MySQLEngine, "_all_tables",
                        lambda self, side, db: ["orders", "audit",
                                                "_orders_gho"])
    return log


def _engine(tmp_path, **kw):
    hop = Hop(name="t", engine="tidb", source=EP, target=EP,
              databases=["d"], exclude=["audit"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return TiDBEngine(hop)


def _names(log, table):
    return [s for s in log if table in s[1] or table in s[2]]


def test_the_copy_lists_only_the_tables_the_hop_takes(tmp_path, asked):
    assert _engine(tmp_path).list_move_tables("d") == [("", "orders")]
    assert _engine(tmp_path).neutral_tables("src", "d") == ["orders"]


def test_the_counts_the_data_and_the_schema_read_the_same_tables(
        tmp_path, asked):
    eng = _engine(tmp_path)
    for check in (eng.check_counts, eng.check_data, eng.check_schema):
        asked.clear()
        got = check("d")
        assert got, check.__name__
        scopes = {r.scope for r in got}
        assert not any(w in s for s in scopes for w in LEFT_OUT), scopes
        for table in LEFT_OUT:
            assert _names(asked, table) == [], (check.__name__, table)
    # what it does read is the table the hop takes
    asked.clear()
    eng.check_counts("d")
    assert _names(asked, "orders")


def test_the_deep_check_asks_the_server_only_of_the_taken_tables(
        tmp_path, asked):
    got = _engine(tmp_path).check_deep("d")
    assert [(r.scope, r.status) for r in got] == [
        ("d target indexes", "ok"), ("d partitions", "ok")]
    checks = [s[1] for s in asked if s[1].startswith("admin check table")]
    assert checks == ["admin check table `d`.`orders`"]
    # and the partitions are asked of no table the hop leaves out
    for table in LEFT_OUT:
        assert _names(asked, table) == [], table


def test_the_counters_are_only_of_the_taken_tables(tmp_path, asked):
    got = _engine(tmp_path).check_autoinc("d")
    assert [r.scope for r in got] == ["d.orders"]
    assert _names(asked, "`audit`") == []
