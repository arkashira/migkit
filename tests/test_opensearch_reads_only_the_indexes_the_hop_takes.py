"""The indexes an OpenSearch hop takes are the only ones its listings, its
copy, its deep check, its repairs and its row filter read - asked of the one
scope answer (`Engine.in_scope`), on a model of the cluster that needs no
server (P0-PARITY.scope).

A model answers only the requests the engine makes (`tests/opensearch_fake`),
so a listing that went round the scope would still be read here: the
indexes of other applications sit beside the hop's own on every cluster.
"""
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from tests.opensearch_fake import Cluster, installed

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.listings", "opensearch"), ("scope.deep", "opensearch"),
          ("scope.copy", "opensearch"), ("scope.repair", "opensearch"),
          ("scope.row-filter", "opensearch"), ("scope.names", "opensearch")]

PROPS = {"title": {"type": "text"}, "n": {"type": "long"}}


def _hop(tmp_path, **kw):
    hop = Hop(name="osos", engine="opensearch",
              source=Endpoint(host="src.example.com", port=9200),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _engine(tmp_path, **kw):
    from migkit.engines.opensearch import OpenSearchEngine
    return OpenSearchEngine(_hop(tmp_path, **kw))


def _cluster(*names, red=()):
    c = Cluster()
    for name in names:
        c.index(name, PROPS, {str(i): {"title": f"t{i}", "n": i}
                              for i in range(5)})
    for name in red:
        c.indexes[name]["health"] = "red"
    return c


def test_the_listings_are_the_hops_indexes_only(tmp_path):
    src = _cluster("shop.orders", "shop.audit", "shop.noise",
                   "other.orders")
    with installed(src):
        eng = _engine(tmp_path, exclude=["shop.audit"])
        assert eng.neutral_tables("src", "shop") == ["noise", "orders"]
        only = _engine(tmp_path, include=["shop.orders"])
        assert only.neutral_tables("src", "shop") == ["orders"]


def test_the_copy_lists_only_what_the_hop_takes(tmp_path):
    src = _cluster("shop.orders", "shop.audit", "shop.noise")
    with installed(src, _cluster()):
        eng = _engine(tmp_path, exclude=["shop.audit", "shop.noise"])
        assert eng.list_move_tables("shop") == [("", "orders")]
        everything = _engine(tmp_path)
        assert [t for _, t in everything.list_move_tables("shop")] == [
            "audit", "noise", "orders"]


def test_the_deep_check_reads_no_index_the_hop_leaves_out(tmp_path):
    dst = _cluster("shop.orders", "shop.audit", red=["shop.audit"])
    with installed(_cluster("shop.orders"), dst):
        left_out = _engine(tmp_path, exclude=["shop.audit"])
        health = [r for r in left_out.check_deep("shop")
                  if r.scope.endswith("target index health")]
        assert [r.status for r in health] == ["ok"], health
        taken = _engine(tmp_path)
        health = [r for r in taken.check_deep("shop")
                  if r.scope.endswith("target index health")]
        assert [r.status for r in health] == ["diff"], health
        assert "shop.audit" in health[0].detail


def test_a_repair_names_no_table_the_hop_no_longer_takes(tmp_path):
    """A drilldown left by a check that ran under another scope: its table
    is the target's own now, and the plan says so and carries nothing."""
    src = _cluster("shop.orders", "shop.audit")
    dst = _cluster("shop.orders", "shop.audit")
    dst.write("shop.audit", "99", {"title": "only here", "n": 99})
    dst.write("shop.orders", "98", {"title": "only here", "n": 98})
    with installed(src, dst):
        wide = _engine(tmp_path)
        found = [r for r in wide.check_data("shop") if r.status == "diff"]
        assert sorted(r.scope for r in found) == ["shop.audit",
                                                  "shop.orders"], found
        narrow = _engine(tmp_path, exclude=["shop.audit"])
        plan = narrow.repair_plan("shop", "rows")
        assert [a.scope for a in plan if a.kind == "rows"] == [
            "shop.orders"], plan
        left = [a for a in plan if a.kind == "left out"]
        assert left and "audit" in left[0].note, plan


def _lite(path, rows):
    conn = sqlite3.connect(path)
    conn.execute("create table items (id integer primary key, title text,"
                 " n integer)")
    conn.executemany("insert into items values (?,?,?)", rows)
    conn.commit()
    conn.close()


def _pair(tmp_path, lite, **kw):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="sqos", engine="hetero",
              source=Endpoint(host=str(lite), port=0, user="", password=""),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["main"],
              options={"source_engine": "sqlite",
                       "target_engine": "opensearch"}, **kw)
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _copied(eng):
    from migkit.cli import _Checkpoint
    eng.create_missing("main", lambda m: None)
    ck = _Checkpoint(eng.hop.report_dir("main") / "move.json")
    for sch, t in eng.list_move_tables("main"):
        eng.move_table("main", sch, t, 500, ck, lambda m: None)


def test_a_table_the_hop_renames_is_copied_and_checked_under_its_new_name(
        tmp_path):
    """sqlite into OpenSearch: the index is made and filled under the name
    the hop maps, and the check lines the two up by the same rule."""
    lite = tmp_path / "src.db"
    _lite(lite, [(i, f"t{i}", i) for i in range(1, 11)])
    dst = Cluster()
    with installed(Cluster(), dst):
        eng = _pair(tmp_path, lite, mapping={"tables": {"items": "things"}})
        _copied(eng)
        assert sorted(dst.indexes) == ["main.things"], sorted(dst.indexes)
        assert len(dst.indexes["main.things"]["docs"]) == 10
        got = {r.scope: r for r in eng.check_data("main")}
        assert [r.status for r in got.values()] == ["ok"], [
            r.detail for r in got.values()]
        # a document someone else put in the index is a difference the
        # same check names, under the same new name
        dst.write("main.things", "5:rogue", {"title": "r", "n": 1})
        again = eng.check_data("main")
        assert [r.status for r in again] == ["diff"], [r.detail
                                                       for r in again]


def test_a_row_filter_is_read_by_the_copy_and_the_check_and_never_dropped(
        tmp_path):
    """A filter is SQL, and OpenSearch takes none: the copy and the check
    both stop on it and say so - neither moves, or compares, the rows
    the filter was meant to keep out as if there were none."""
    lite = tmp_path / "src.db"
    _lite(lite, [(i, f"t{i}", i) for i in range(1, 11)])
    dst = Cluster()
    with installed(Cluster(), dst):
        eng = _pair(tmp_path, lite, mapping={"where": {"items": "n > 6"}})
        eng.create_missing("main", lambda m: None)
        with pytest.raises(SystemExit) as stopped:
            _copied(eng)
        assert "row filter" in str(stopped.value)
        assert "takes none" in str(stopped.value)
        assert not dst.indexes["main.items"]["docs"]
        got = eng.check_data("main")
        assert [r.status for r in got] == ["error"], got
        assert "row filter" in got[0].detail
