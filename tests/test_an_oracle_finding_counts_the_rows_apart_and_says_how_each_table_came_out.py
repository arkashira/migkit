"""An Oracle check says, table by table, how many rows are apart and how
each table came out (P0-PARITY.verify), without a server: the pair's walk
over the rows of both sides, and the findings of a database's one answer.

* a table that differs says how many of its rows are missing on the
  target, only on it, and changed - counted one by one, and marked exact
  only where the walk went through every row; where it stopped short it
  says so and counts none as exact;
* a database's counts say how each of its tables came out, those counted
  alike, those that differ and those not compared.

The statements are the ones the engine already sent (the digest, the read
in key order, the read by key): they are stubbed here, and this module
holds what the pair makes of their answers.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine
from migkit.engines.hetero import HeteroEngine
from migkit.engines.oracle import OracleEngine
from tests.oracle_catalogue import Catalogue, cat  # noqa: F401

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.rows-apart", "oracle"), ("verify.tables", "oracle")]

EP = Endpoint(host="10.0.0.5", port=1521, user="app", password="CHANGE_ME")
COLUMNS = [("id", "NUMBER(10,0)"), ("v", "VARCHAR2(10 CHAR)")]

#: the rows of ORDERS on each side: 1 only on the source, 6 only on the
#: target, 3 with another value
ROWS = {"src": [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")],
        "dst": [(2, "b"), (3, "CHANGED"), (4, "d"), (5, "e"), (6, "f")]}


@pytest.fixture
def oracle(monkeypatch, tmp_path, cat):  # noqa: F811
    cat.on("from all_tables", [("ORDERS",)])
    monkeypatch.setattr(OracleEngine, "neutral_columns",
                        lambda self, side, db, table: list(COLUMNS))
    monkeypatch.setattr(OracleEngine, "neutral_key",
                        lambda self, side, db, table: ["id"])
    monkeypatch.setattr(OracleEngine, "canonical_type",
                        lambda self, side, db, declared: "decimal"
                        if declared.startswith("NUMBER") else "text")
    monkeypatch.setattr(OracleEngine, "_mojibake_repair",
                        lambda self, db: None)

    def no_log(self, side, db):
        raise RuntimeError("no redo is read in this test")
    monkeypatch.setattr(OracleEngine, "log_position", no_log)
    monkeypatch.setattr(
        OracleEngine, "same_engine_digest",
        lambda self, side, db, table, cols, where=None, salt=None:
        (len(ROWS[side]), "equal" if ROWS["src"] == ROWS["dst"] else side))

    def unreadable(*a, **k):
        raise RuntimeError("the leaves cannot be read")
    # the rows are then found by walking the table by key, a few at a time
    from migkit import leaves
    monkeypatch.setattr(leaves, "drill", unreadable)

    def read(self, side, db, table, columns, after=None, limit=1000,
             where=None):
        rest = [r for r in ROWS[side] if after is None or r[0] > after[0]]
        got = rest[:limit]
        return [list(r) for r in got], (
            (got[-1][0],) if len(got) < len(rest) else None)
    monkeypatch.setattr(OracleEngine, "neutral_read", read)

    def by_key(self, side, db, table, columns, key, keys, where=None):
        held = {r[0]: list(r) for r in ROWS[side]}
        return {HeteroEngine._key_of(columns, key, list(k)): held[k[0]]
                for k in keys if k[0] in held}
    monkeypatch.setattr(OracleEngine, "neutral_rows_by_key", by_key)
    hop = Hop(name="t", engine="oracle", source=EP, target=EP,
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return OracleEngine(hop)


def test_a_table_that_differs_counts_its_rows_apart_one_by_one(oracle):
    got = [r for r in oracle.check_data("app") if r.status == "diff"]
    assert len(got) == 1
    assert got[0].rows_apart == {"orders": {
        "missing": 1, "extra": 1, "changed": 1, "exact": True}}
    assert got[0].table == "orders"
    assert "missing=1 extra=1 changed=1" in got[0].detail


def test_a_walk_that_stopped_short_counts_none_as_exact(oracle, monkeypatch):
    monkeypatch.setattr(Engine, "DRILL_CAP", 3)
    got = [r for r in oracle.check_data("app") if r.status == "diff"]
    assert got[0].rows_apart["orders"]["exact"] is False
    assert "stopped after 3 rows" in got[0].detail


def test_a_table_that_is_alike_counts_none(oracle, monkeypatch):
    monkeypatch.setitem(ROWS, "dst", list(ROWS["src"]))
    got = oracle.check_data("app")
    assert [r.status for r in got] == ["ok"]
    assert got[0].rows_apart == {}


def test_a_databases_counts_say_how_each_table_came_out(oracle, cat,
                                                        monkeypatch):
    monkeypatch.setitem(ROWS, "dst", list(ROWS["src"]))
    got = oracle.check_counts("app")
    assert [(r.scope, r.status) for r in got] == [("app", "ok")]
    assert got[0].tables == {"orders": "ok"}


def test_the_tables_that_count_differently_are_said_with_the_ones_that_do_not(
        oracle, cat, monkeypatch):
    cat.answers.clear()
    names = [f"T{i:02d}" for i in range(13)]
    cat.on("from all_tables", [(n,) for n in names])
    monkeypatch.setattr(
        OracleEngine, "same_engine_digest",
        lambda self, side, db, table, cols, where=None, salt=None:
        (3 if side == "src" or table == "t00" else 4, "x"))
    got = oracle.check_counts("app")
    more = [r for r in got if r.scope == "app"]
    assert len(more) == 1 and "more tables" in more[0].detail
    assert more[0].tables["t00"] == "ok"
    assert set(more[0].tables.values()) == {"ok", "diff"}
    assert len(more[0].tables) == 13
    # a table is named once, the way the source names it, in the finding
    # about it and in the database's
    assert {r.table for r in got if r.scope != "app"} <= set(more[0].tables)
