"""The move into an OpenSearch cluster, on a model of it that needs no
server (P0-PARITY.all): a load holds the target's indexes off from
replicating and refreshing and gives both back - even when the process
that held them died, from a record it wrote first; an index the target
lacks is made like the source's before any is filled; the documents are
sized where they lie; and the rules a SQL column has that an index cannot
keep are named (`tests/opensearch_fake`).
"""
import json
import os
import socket
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from tests.opensearch_fake import Cluster, installed

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.load-window", "opensearch"),
          ("move.create-missing", "opensearch"),
          ("move.sized", "opensearch"), ("schema.defaults", "opensearch")]

PROPS = {"title": {"type": "text", "analyzer": "english"},
         "n": {"type": "long"}, "tags": {"type": "keyword"}}


def _engine(tmp_path, **kw):
    from migkit.engines.opensearch import OpenSearchEngine
    hop = Hop(name="osos", engine="opensearch",
              source=Endpoint(host="src.example.com", port=9200),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return OpenSearchEngine(hop)


def _target():
    c = Cluster()
    c.index("shop.orders", PROPS, {"1": {"title": "a", "n": 1}}, replicas=2,
            settings={"refresh_interval": "5s"})
    c.index("shop.lines", PROPS, {}, replicas=1)
    return c


def _setting(cluster, index, key):
    return cluster.indexes[index]["settings"].get(key)


# ---- the load window ----

def test_a_load_takes_the_replicas_and_the_refresh_and_gives_them_back(
        tmp_path):
    dst = _target()
    with installed(Cluster(), dst):
        eng = _engine(tmp_path)
        with eng.load_window("shop", None, {"orders", "lines"}):
            for index in ("shop.orders", "shop.lines"):
                assert _setting(dst, index, "number_of_replicas") == "0"
                assert _setting(dst, index, "refresh_interval") == "-1"
            # written down before anything was taken off
            assert list(tmp_path.glob("load-window.*.json"))
        assert _setting(dst, "shop.orders", "number_of_replicas") == "2"
        assert _setting(dst, "shop.orders", "refresh_interval") == "5s"
        assert _setting(dst, "shop.lines", "number_of_replicas") == "1"
        # an index that kept the default refresh goes back to the default
        assert _setting(dst, "shop.lines", "refresh_interval") is None
        assert not list(tmp_path.glob("load-window.*.json"))


def test_they_go_back_when_the_load_fails(tmp_path):
    dst = _target()
    with installed(Cluster(), dst):
        eng = _engine(tmp_path)
        with pytest.raises(RuntimeError):
            with eng.load_window("shop", None, {"orders"}):
                raise RuntimeError("the load stopped")
        assert _setting(dst, "shop.orders", "number_of_replicas") == "2"
        assert _setting(dst, "shop.orders", "refresh_interval") == "5s"
        assert not list(tmp_path.glob("load-window.*.json"))


def test_only_the_indexes_the_load_writes_are_held_off(tmp_path):
    dst = _target()
    with installed(Cluster(), dst):
        eng = _engine(tmp_path)
        with eng.load_window("shop", None, {"orders", "ghost"}):
            assert _setting(dst, "shop.orders", "refresh_interval") == "-1"
            assert _setting(dst, "shop.lines", "number_of_replicas") == "1"
        # no table named: every index the hop takes
        with _engine(tmp_path, exclude=["shop.lines"]).load_window(
                "shop", None):
            assert _setting(dst, "shop.orders", "refresh_interval") == "-1"
            assert _setting(dst, "shop.lines", "refresh_interval") is None


def _a_load_that_died(tmp_path, index, restore):
    """The record a process left that is not running any more."""
    path = tmp_path / f"load-window.{socket.gethostname()}.999999.1.json"
    path.write_text(json.dumps({index: restore}))
    return path


def test_a_load_that_died_in_the_window_is_found_and_given_back(tmp_path):
    dst = _target()
    # what it did: took the replicas and the refresh, and died
    dst.indexes["shop.orders"]["settings"].update(
        number_of_replicas="0", refresh_interval="-1")
    record = _a_load_that_died(tmp_path, "shop.orders",
                               {"number_of_replicas": "2",
                                "refresh_interval": "5s"})
    with installed(Cluster(), dst):
        eng = _engine(tmp_path)
        found = eng.set_aside("shop")
        assert found.status == "diff", found
        assert "shop.orders" in found.detail
        # the next load into the database gives them back first - and
        # takes the index's settings as they were, not as the dead load
        # left them
        with eng.load_window("shop", None, {"lines"}):
            assert _setting(dst, "shop.orders", "number_of_replicas") == "2"
            assert _setting(dst, "shop.orders", "refresh_interval") == "5s"
        assert not record.exists()
        assert eng.set_aside("shop") is None


def test_a_load_still_running_is_held_off_on_purpose(tmp_path):
    dst = _target()
    path = tmp_path / (f"load-window.{socket.gethostname()}"
                       f".{os.getpid()}.1.json")
    path.write_text(json.dumps({"shop.orders": {
        "number_of_replicas": "2", "refresh_interval": "5s"}}))
    with installed(Cluster(), dst):
        found = _engine(tmp_path).set_aside("shop")
    assert found.status == "ok" and "still running" in found.detail
    assert path.exists()


def test_the_copy_of_documents_as_they_are_runs_inside_the_same_window(
        tmp_path):
    src, dst = Cluster(), _target()
    src.index("shop.orders", PROPS, {str(i): {"title": f"t{i}", "n": i}
                                     for i in range(30)}, shards=2)
    src.index("shop.fresh", PROPS, {"a": {"title": "x", "n": 1}})
    said = []
    with installed(src, dst):
        eng = _engine(tmp_path)
        eng.native_bulk("shop", ["orders", "fresh"], True, said.append)
    assert len(dst.indexes["shop.orders"]["docs"]) == 30
    assert len(dst.indexes["shop.fresh"]["docs"]) == 1
    # an index that was there keeps its own replicas and refresh; one
    # made for the copy has the source's
    assert _setting(dst, "shop.orders", "number_of_replicas") == "2"
    assert _setting(dst, "shop.orders", "refresh_interval") == "5s"
    assert _setting(dst, "shop.fresh", "refresh_interval") is None
    assert not list(tmp_path.glob("load-window.*.json"))
    assert any("fresh: not on the target" in m for m in said), said


def test_a_deep_check_names_a_window_a_dead_load_left_open(tmp_path):
    src, dst = Cluster(), _target()
    src.index("shop.orders", PROPS, {})
    src.index("shop.lines", PROPS, {})
    _a_load_that_died(tmp_path, "shop.orders",
                      {"number_of_replicas": "2", "refresh_interval": "5s"})
    with installed(src, dst):
        for engine in ("opensearch", "elasticsearch"):
            eng = _engine(tmp_path)
            eng.hop.engine = engine
            found = [r for r in eng.check_deep("shop")
                     if r.scope.endswith("load windows set aside")]
            assert [r.status for r in found] == ["diff"], (engine, found)
            # and the comparison of the twins ran under either name
            assert any(r.scope.endswith("columns")
                       for r in eng.check_deep("shop")), engine


# ---- an index the target lacks ----

def test_an_index_the_target_lacks_is_made_like_the_sources(tmp_path):
    src, dst = Cluster(), Cluster()
    src.index("shop.orders", PROPS, {"1": {"title": "a", "n": 1}}, shards=3,
              settings={"analysis.analyzer.body.type": "english"})
    src.index("shop.audit", PROPS, {})
    dst.index("shop.lines", PROPS, {})
    src.index("shop.lines", PROPS, {})
    said = []
    with installed(src, dst):
        eng = _engine(tmp_path, exclude=["shop.audit"])
        made = eng.create_missing("shop", said.append)
        assert made == ["orders"], made
        like = dst.indexes["shop.orders"]
        assert like["mappings"]["properties"] == PROPS
        assert like["settings"]["number_of_shards"] == "3"
        assert like["settings"]["analysis.analyzer.body.type"] == "english"
        assert "shop.audit" not in dst.indexes
        assert not like["docs"]
        assert said and "orders: not on the target" in said[0]
        assert eng.create_missing("shop", said.append) == []


# ---- the documents sized where they lie ----

def test_the_documents_are_sized_where_they_lie(tmp_path):
    c = Cluster()
    docs = {str(i): {"title": "x" * (10 + i), "n": i} for i in range(20)}
    docs["wide"] = {"title": "w" * 5000, "n": 0}
    c.index("shop.orders", PROPS, docs)
    c.index("shop.empty", PROPS, {})
    with installed(c):
        got = _engine(tmp_path).row_sizes("src", "shop")
    assert set(got) == {"orders"}, got            # an empty index has none
    sized = got["orders"]
    assert sized["exact"] is True and sized["rows"] == 21
    assert 5000 < sized["largest"] < 5100
    assert sized["mean"] < sized["largest"]


def test_a_large_index_is_sampled_and_says_it_is_not_exact(tmp_path,
                                                          monkeypatch):
    from migkit import pieces
    monkeypatch.setattr(pieces, "EXACT_UP_TO", 100)
    c = Cluster()
    c.index("shop.orders", PROPS, {str(i): {"title": "x" * 40, "n": i}
                                   for i in range(50)})
    with installed(c):
        eng = _engine(tmp_path)
        eng.SIZE_SAMPLE = 7
        sized = eng.row_sizes("src", "shop", ["orders"])["orders"]
    assert sized["exact"] is False and sized["rows"] == 50


# ---- the rules a column has that an index cannot keep ----

def test_a_mapping_says_every_field_takes_null_but_the_key(tmp_path):
    c = Cluster()
    c.index("shop.orders", PROPS, {"1": {"title": "a", "n": 1}})
    with installed(c):
        rules = _engine(tmp_path).neutral_column_rules("src", "shop",
                                                       "orders")
    assert rules["_id"] == {"null": False, "default": None,
                            "identity": False}
    assert rules["n"] == {"null": True, "default": None, "identity": False}


def _lite(path):
    conn = sqlite3.connect(path)
    conn.execute("create table items (id integer primary key, title text"
                 " not null, n integer default 5)")
    conn.executemany("insert into items values (?,?,?)",
                     [(1, "a", 1), (2, "b", 2)])
    conn.commit()
    conn.close()


def test_what_the_index_cannot_keep_of_a_sql_column_is_named(tmp_path):
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    lite = tmp_path / "src.db"
    _lite(lite)
    dst = Cluster()
    with installed(Cluster(), dst):
        hop = Hop(name="sqos", engine="hetero",
                  source=Endpoint(host=str(lite), port=0, user="",
                                  password=""),
                  target=Endpoint(host="dst.example.com", port=9200),
                  databases=["main"],
                  options={"source_engine": "sqlite",
                           "target_engine": "opensearch"})
        hop.report_dir = lambda db=None: tmp_path
        eng = HeteroEngine(hop)
        eng.create_missing("main", lambda m: None)
        ck = _Checkpoint(tmp_path / "move.json")
        for sch, t in eng.list_move_tables("main"):
            eng.move_table("main", sch, t, 500, ck, lambda m: None)
        got = eng.check_schema("main")
    drift = [r for r in got if r.status == "diff"]
    assert drift, got
    assert "title is NOT NULL on the source and takes NULL on the target" \
        in drift[0].detail
    assert "n defaults to 5 on the source and to nothing on the target" \
        in drift[0].detail
