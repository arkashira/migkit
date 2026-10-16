"""SingleStore as a side, on a fake answering its catalogue (backlog
P0-CLOUDS.more): shard key, pipeline load and no follow, each on its facts. No server is reached."""
import types

import pytest

from migkit import decide, managed, users
from migkit.engines import singlestore


@pytest.fixture
def eng(monkeypatch):
    got = singlestore.SingleStoreEngine.__new__(singlestore.SingleStoreEngine)
    got.hop = types.SimpleNamespace(
        engine="singlestore", source=types.SimpleNamespace(options={}),
        target=types.SimpleNamespace(options={}))
    got.said = []
    monkeypatch.setattr(decide, "record",
                        lambda hop, choice, log=None, **kw: got.said.append(
                            choice))
    monkeypatch.setattr(type(got), "_d", lambda self, side, db: db,
                        raising=False)
    return got


def test_the_catalogue_row_is_kept_for_the_engine():
    assert "singlestore-helios" in managed.services()


def test_its_accounts_are_read_as_its_wires():
    hop = types.SimpleNamespace(engine="singlestore")
    assert users._speaking(hop).engine == "mysql"


def test_a_table_is_sharded_by_its_key(eng, monkeypatch):
    from migkit.engines.distmysql import OnMySQLWire
    monkeypatch.setattr(OnMySQLWire, "neutral_create_sql",
                        lambda self, side, db, t, cols, key=():
                        "create table `t` (`id` int, primary key (`id`))",
                        raising=False)
    got = eng.neutral_create_sql("dst", "app", "t", [], ["id"])
    assert got == ("create table `t` (`id` int, primary key (`id`),"
                   " shard key (`id`))")
    assert "shard key ()" in eng.neutral_create_sql("dst", "app", "t", [], [])


def test_as_a_source_it_is_refused_with_why(eng):
    got = eng.follow_way("app")
    assert got.path is None and "keeps no binary log" in got.reason


def test_a_pipeline_loads_where_the_account_may_make_one(eng):
    eng.hop.target.options = {"stage": "s3://b/p", "stage_region": "x-1"}
    eng._landing_writable = lambda url: True
    eng._table_rows = lambda side, db, t: 9 if side == "src" else 0
    eng._q = lambda side, sql, args=None: [("GRANT SELECT ON *.* TO a",)]
    assert eng.load_way("app", "t").path == "copier"
    assert "may not run" in eng.said[-1].reason
    eng._q = lambda side, sql, args=None: [("GRANT CREATE PIPELINE ON *.*",)]
    assert eng.load_way("app", "t").path == "landing"
    sql = eng.landing_sql("app", "t", ["id"], "s3://b/p/t.csv")
    assert sql[0].startswith("create pipeline") and "b/p/t.csv" in sql[0]
    assert sql[1].endswith("foreground") and sql[2].startswith("drop")
