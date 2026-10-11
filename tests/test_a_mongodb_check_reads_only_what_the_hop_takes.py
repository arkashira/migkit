"""A MongoDB hop's copy and checks read the collections the hop takes, by
the names the hop gives them, and the views its `objects` carries - and
nothing else (P0-PARITY.scope).

An excluded collection is not so much as looked at: counts, the data
comparison, the schema comparison and the deep checks each ask the one
scope answer (`Engine.in_scope`); a collection the hop renames
(`mapping.tables`) is compared with its new name instead of reported
missing and extra; a view the hop's `objects` leaves out is not compared.
Run on a server in memory (`tests/mongo_fake.py`): the statements are the
engine's own, the answers the server's.
"""
import pytest

from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.listings", "mongodb"), ("scope.deep", "mongodb"),
          ("scope.copy", "mongodb"), ("scope.names", "mongodb"),
          ("scope.objects", "mongodb"), ("verify.tables", "mongodb")]


def _seed(src, dst, extra=()):
    for side in (src, dst):
        db = side.db("shop")
        db.add("orders", [{"_id": 1, "n": 1}, {"_id": 2, "n": 2}])
        db.add("scratch", [{"_id": 1}])
    for name, docs in extra:
        for side in (src, dst):
            side.db("shop").add(name, docs)


def _eng(tmp_path, **hop):
    eng, src, dst = engine(hop, tmp_path)
    _seed(src, dst)
    return eng, src, dst


def _read(side, name):
    return [t for t in side.touched if t[1] == name]


@pytest.mark.parametrize("hop", [{"exclude": ["shop.scratch"]},
                                 {"include": ["shop.orders"]}])
def test_the_copy_lists_only_the_collections_the_hop_takes(tmp_path, hop):
    eng, _, _ = _eng(tmp_path, **hop)
    assert [t for _, t in eng.list_move_tables("shop")] == ["orders"]


def test_counts_data_and_schema_never_read_a_collection_left_out(tmp_path):
    eng, src, dst = _eng(tmp_path, exclude=["shop.scratch"])
    # the one left out is different, and is no one's business
    dst.db("shop").colls["scratch"].docs.append({"_id": 9})
    for check in (eng.check_counts, eng.check_data, eng.check_schema):
        got = check("shop")
        assert all(r.status == "ok" for r in got), (check, got)
    assert not _read(src, "scratch") and not _read(dst, "scratch")
    assert _read(src, "orders") and _read(dst, "orders")


def test_the_deep_checks_read_only_the_collections_the_hop_takes(tmp_path):
    eng, src, dst = _eng(tmp_path, exclude=["shop.scratch"])
    got = eng.check_deep("shop")
    assert got and not [r for r in got if r.status == "error"]
    assert not _read(src, "scratch") and not _read(dst, "scratch")
    assert _read(src, "orders") and _read(dst, "orders")


def test_a_collection_left_in_is_still_found_missing_on_the_target(tmp_path):
    eng, _, dst = _eng(tmp_path, exclude=["shop.scratch"])
    del dst.db("shop").colls["orders"]
    got = eng.check_counts("shop")
    assert [r.status for r in got] == ["diff"]
    assert "missing collections on target: ['orders']" in got[0].detail
    assert got[0].tables == {"orders": "diff"}


def test_a_renamed_collection_is_compared_by_its_new_name(tmp_path):
    eng, src, dst = engine(
        {"mapping": {"tables": {"shop.orders": "orders_v2"}}}, tmp_path)
    src.db("shop").add("orders", [{"_id": 1}, {"_id": 2}])
    dst.db("shop").add("orders_v2", [{"_id": 1}, {"_id": 2}])
    assert eng._there("shop", "orders") == "orders_v2"
    for check in (eng.check_counts, eng.check_data, eng.check_schema):
        got = check("shop")
        assert all(r.status == "ok" for r in got), (check, got)
    # and one that really differs is found under the source's name
    dst.db("shop").colls["orders_v2"].docs.append({"_id": 3})
    counts = eng.check_counts("shop")[0]
    assert counts.status == "diff" and "orders src=2 dst=3" in counts.detail
    data = eng.check_data("shop")[0]
    assert data.status == "diff" and data.scope == "shop.orders"
    assert data.table == "orders"


def test_the_copy_writes_a_renamed_collection_under_its_new_name(tmp_path):
    from migkit.cli import _Checkpoint
    eng, src, dst = engine(
        {"mapping": {"tables": {"shop.orders": "orders_v2"}}}, tmp_path)
    src.db("shop").add("orders", [{"_id": i, "n": i} for i in range(5)])
    eng.move_table("shop", "", "orders", 2,
                   _Checkpoint(tmp_path / "move.json"), lambda line: None)
    assert [d["_id"] for d in dst.db("shop").colls["orders_v2"].docs] \
        == list(range(5))
    assert "orders" not in dst.db("shop").list_collection_names()


def test_a_view_the_hop_does_not_carry_is_not_compared(tmp_path):
    eng, src, dst = engine({"objects": {"views": False}}, tmp_path)
    src.db("shop").add("orders", [{"_id": 1}])
    dst.db("shop").add("orders", [{"_id": 1}])
    # a view only the source has
    src.db("shop").add("recent", [], view=True)
    got = eng.check_schema("shop")
    assert [(r.scope, r.status) for r in got] == [
        ("shop", "ok"), ("shop objects", "ok")]
    # carried, it is missing on the target
    carried, _, _ = engine({}, tmp_path)
    carried._client = eng._client
    got = carried.check_schema("shop")
    assert [r.status for r in got] == ["diff", "diff"]
    assert "recent: missing on target" in got[0].detail


def test_a_copy_that_cannot_mask_does_not_run_for_a_collection_to_mask(
        tmp_path):
    """`scope.transforms` is not claimed: the copier carries each document
    as stored. What it does instead is stop, before a byte is copied."""
    from migkit.cli import _Checkpoint
    eng, src, dst = engine(
        {"options": {"mask_on_write": {"orders.email": "null"}}}, tmp_path)
    src.db("shop").add("orders", [{"_id": 1, "email": "a@example.com"}])
    with pytest.raises(SystemExit, match="masks columns of this collection"):
        eng.move_table("shop", "", "orders", 2,
                       _Checkpoint(tmp_path / "move.json"), lambda line: None)
    assert "orders" not in dst.db("shop").list_collection_names()


def test_the_counts_say_how_each_collection_came_out(tmp_path):
    eng, src, dst = _eng(tmp_path, exclude=["shop.scratch"])
    dst.db("shop").colls["orders"].docs.pop()
    got = eng.check_counts("shop")
    assert got[0].status == "diff" and got[0].tables == {"orders": "diff"}
    for r in eng.check_data("shop"):
        assert r.tables == {"orders": r.status}
