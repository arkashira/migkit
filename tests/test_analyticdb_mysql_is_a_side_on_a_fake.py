"""AnalyticDB for MySQL as a side, on a fake answering its catalogue (backlog
P0-CLOUDS.more): spread, external-table load and per-table binary log as rungs on its facts. No server is reached."""
import types

import pytest

from migkit import decide, managed, users
from migkit.engines import analyticdb


@pytest.fixture
def eng(monkeypatch):
    got = analyticdb.AnalyticDBMySQLEngine.__new__(analyticdb.AnalyticDBMySQLEngine)
    got.hop = types.SimpleNamespace(
        engine="analyticdb-mysql", source=types.SimpleNamespace(options={}),
        target=types.SimpleNamespace(options={}))
    got.said = []
    monkeypatch.setattr(decide, "record",
                        lambda hop, choice, log=None, **kw: got.said.append(
                            choice))
    monkeypatch.setattr(type(got), "_d", lambda self, side, db: db,
                        raising=False)
    return got


def test_the_catalogue_row_is_kept_for_the_engine():
    assert "alibaba-analyticdb-mysql" in managed.services()


def test_its_accounts_are_read_as_its_wires():
    hop = types.SimpleNamespace(engine="analyticdb-mysql")
    assert users._speaking(hop).engine == "mysql"


def test_a_small_table_is_kept_whole_and_a_large_one_hashed(eng):
    eng._q = lambda side, sql, args=None: [(10,)]
    assert eng.spread_of("app", "t", ["id"]) == "distributed by broadcast"
    eng._q = lambda side, sql, args=None: [(10 ** 7,)]
    assert eng.spread_of("app", "t", ["id"]) == "distributed by hash(`id`)"
    assert eng.spread_of("app", "t", []) == ""


def test_it_is_followed_only_where_every_table_keeps_a_binary_log(eng):
    eng.neutral_tables = lambda side, db: ["a", "b"]
    kept = {"a"}
    eng._q = lambda side, sql, args=None: [(
        "t", "create table x (id int) binlog=true" if any(
            f"`{t}`" in sql for t in kept) else "create table x (id int)")]
    got = eng.follow_way("app")
    assert got.path is None and "only 1 of the 2 tables" in got.reason
    with pytest.raises(SystemExit, match="no way to follow"):
        eng.tail_start("app", None)
    kept.add("b")
    assert eng.follow_way("app").path == "binlog"


def test_the_external_table_load_is_taken_on_facts_and_said_otherwise(eng):
    eng._table_rows = lambda side, db, t: 5 if side == "src" else 0
    eng._may_load = lambda db: True
    assert eng.load_way("app", "t").path == "copier"
    assert "no landing store" in eng.said[-1].reason
    eng.hop.target.options = {"stage": "oss://bucket/in"}
    eng._landing_writable = lambda url: True
    assert eng.load_way("app", "t").path == "landing"
    eng._table_rows = lambda side, db, t: 5
    assert eng.load_way("app", "t").path == "copier"
    assert "twice" in eng.said[-1].reason
    sql = eng.landing_sql("app", "t", ["id", "v"], "oss://bucket/in/t.csv")
    assert "engine='OSS'" in sql[0] and sql[1].startswith("insert into")
    assert sql[2].startswith("drop table")


def test_a_landed_value_keeps_null_apart_from_its_text():
    from migkit.engines.analyticdb import landing_text
    assert landing_text(None) == "\\N"
    assert landing_text("\\N") == "\\\\N"
    assert landing_text(b"\x01") == "01"
