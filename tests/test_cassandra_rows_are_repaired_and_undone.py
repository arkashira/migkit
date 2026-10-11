"""A Cassandra target repaired row by row from the last check's drilldown,
and every repair undone exactly (P0-PARITY.verify).

The check counts the rows apart, table by table; the repair writes the
rows that are missing or changed, then removes the extra ones, and keeps
what the target held first. Two things were not true of a write on this
engine: a null was left unset, so a column the source holds null stayed
filled on the target and the table still differed after the repair; and a
row could not be deleted by its key at all. A repair writes a null as one
(the cell cleared) and the undo does the same.

No server: the cluster is `tests/cassandra_fake.py`. The delete is a
statement no server has yet been sent by this engine, so the repair itself
is built and not yet proved (`capabilities.UNPROVED`); the count of the
rows apart is read from the drilldown the pair already writes, and is
proved here.
"""
from migkit import repairlog
from migkit.config import Endpoint, Hop
from migkit.engines.cassandra import CassandraEngine
from tests import cassandra_fake as fake

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.rows-apart", "cassandra")]

COLS = {"id": "int", "name": "text", "note": "text"}


def _engine(tmp_path, monkeypatch, src_rows, dst_rows, **kw):
    src, dst = fake.Cluster(), fake.Cluster()
    src.table("shop", "orders", COLS, ["id"], rows=src_rows)
    dst.table("shop", "orders", COLS, ["id"], rows=dst_rows)
    fake.install(monkeypatch, src, dst)
    hop = Hop(name="cs", engine="cassandra", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return CassandraEngine(hop), dst


SRC = [{"id": 1, "name": "a", "note": "x"},
       {"id": 2, "name": "b", "note": None},
       {"id": 3, "name": "c", "note": "z"}]
DST = [{"id": 1, "name": "a", "note": "x"},
       {"id": 2, "name": "b", "note": "stale"},
       {"id": 9, "name": "only here", "note": "q"}]


def _rows(dst):
    return {r["id"]: (r["name"], r.get("note"))
            for r in fake.rows_of(dst.keyspaces["shop"]["orders"])}


def test_a_finding_counts_the_rows_apart_key_by_key(tmp_path, monkeypatch):
    eng, _ = _engine(tmp_path, monkeypatch, SRC, DST)
    got = {r.scope: r for r in eng.check_data("shop")}["shop.orders"]
    assert got.status == "diff"
    assert got.rows_apart == {"orders": {"missing": 1, "extra": 1,
                                         "changed": 1, "exact": True}}


def test_a_table_that_is_equal_counts_none(tmp_path, monkeypatch):
    eng, _ = _engine(tmp_path, monkeypatch, SRC, SRC)
    got = {r.scope: r for r in eng.check_data("shop")}["shop.orders"]
    assert got.status == "ok" and got.rows_apart == {}


def test_a_repair_makes_the_target_equal_and_clears_a_null(
        tmp_path, monkeypatch):
    eng, dst = _engine(tmp_path, monkeypatch, SRC, DST)
    eng.check_data("shop")
    for action in eng.repair_plan("shop", "rows"):
        eng.apply("shop", action)
    assert _rows(dst) == {1: ("a", "x"), 2: ("b", None), 3: ("c", "z")}
    again = {r.scope: r for r in eng.check_data("shop")}["shop.orders"]
    assert again.status == "ok", again.detail


def test_a_repair_is_undone_exactly(tmp_path, monkeypatch):
    eng, dst = _engine(tmp_path, monkeypatch, SRC, DST)
    eng.check_data("shop")
    before = _rows(dst)
    for action in eng.repair_plan("shop", "rows"):
        eng.apply("shop", action)
    assert _rows(dst) != before
    paths = repairlog.to_reverse(tmp_path / "undo")
    assert paths
    for path in paths:
        repairlog.reverse(path, eng.repair_io("shop",
                                              repairlog.read(path)[0]))
    assert _rows(dst) == before


def test_rows_a_hop_keeps_by_its_ops_are_left_as_they_are(
        tmp_path, monkeypatch):
    """A hop that does not carry deletes keeps the target's extra row."""
    eng, dst = _engine(tmp_path, monkeypatch, SRC, DST,
                       mapping={"ops": {"orders": "insert,update"}})
    eng.check_data("shop")
    for action in eng.repair_plan("shop", "rows"):
        eng.apply("shop", action)
    assert 9 in _rows(dst) and 3 in _rows(dst)


def test_a_change_from_another_engine_is_written_over_the_row_column_by_column(
        tmp_path, monkeypatch):
    """The target's contract for a tail that applies changes: an upsert
    writes the columns it carries and clears one set to null; a delete
    removes the row by its key."""
    eng, dst = _engine(tmp_path, monkeypatch, SRC, DST)
    eng._apply_upserts("dst", "shop", "orders", [
        ({"id": 2}, {"name": "renamed", "note": None}),
        ({"id": 5}, {"name": "five", "note": "n"})])
    eng._apply_upsert("dst", "shop", "orders", {"id": 1}, {"name": "one"})
    eng._apply_delete("dst", "shop", "orders", {"id": 9})
    assert _rows(dst) == {1: ("one", "x"), 2: ("renamed", None),
                          5: ("five", "n")}


def test_a_table_the_hop_renames_is_not_copied_by_the_copy_that_keeps_times(
        tmp_path, monkeypatch):
    import pytest
    eng, dst = _engine(tmp_path, monkeypatch, SRC, DST,
                       mapping={"tables": {"orders": "orders_v2"}})
    with pytest.raises(SystemExit) as e:
        eng.native_bulk("shop", ["orders"], False, [].append)
    assert "orders: renamed, or with columns the hop maps" in str(e.value)
