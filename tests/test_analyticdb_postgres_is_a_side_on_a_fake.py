"""AnalyticDB for PostgreSQL as a side, on a fake answering its catalogue (backlog
P0-CLOUDS.more): distribution and external-table load as rungs, no follow with the reason. No server is reached."""
import types

import pytest

from migkit import decide, managed, users
from migkit.engines import analyticdb


@pytest.fixture
def eng(monkeypatch):
    got = analyticdb.AnalyticDBPostgresEngine.__new__(analyticdb.AnalyticDBPostgresEngine)
    got.hop = types.SimpleNamespace(
        engine="analyticdb-postgres", source=types.SimpleNamespace(options={}),
        target=types.SimpleNamespace(options={}))
    got.said = []
    monkeypatch.setattr(decide, "record",
                        lambda hop, choice, log=None, **kw: got.said.append(
                            choice))
    monkeypatch.setattr(type(got), "_d", lambda self, side, db: db,
                        raising=False)
    return got


def test_the_catalogue_row_is_kept_for_the_engine():
    assert "alibaba-analyticdb-postgres" in managed.services()


def test_its_accounts_are_read_as_its_wires():
    hop = types.SimpleNamespace(engine="analyticdb-postgres")
    assert users._speaking(hop).engine == "postgres"


def test_a_table_is_distributed_by_its_key_or_randomly(eng):
    eng._scalar = lambda side, db, sql: 4
    assert eng.spread_of("app", "t", ["id", "k"]) == \
        'distributed by ("id", "k")'
    assert eng.spread_of("app", "t", []) == "distributed randomly"


def test_as_a_source_it_is_refused_with_why(eng):
    got = eng.follow_way("app")
    assert got.path is None and "decodes no logical replication" in \
        got.reason
    with pytest.raises(SystemExit, match="no way to follow"):
        eng.tail_start("app", None)


def test_the_load_reads_a_readable_external_table(eng):
    eng.hop.target.options = {"stage": "oss://b/p"}
    eng._landing_writable = lambda url: True
    eng._may_load = lambda db: None
    eng._table_rows = lambda side, db, t: 3 if side == "src" else 0
    assert eng.load_way("app", "t").path == "copier"
    assert "grants were not read" in eng.said[-1].reason
    sql = eng.landing_sql("app", "t", ["id"], "oss://b/p/t.csv")
    assert sql[0].startswith("create readable external table")
    assert "null '\\N'" in sql[0] and sql[2].startswith("drop external")
