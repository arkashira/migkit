"""The snapshot a repair takes of a MongoDB target before it writes
(backlog P0-PARITY.all, `cutover.rollback`) is set down whole or not at
all, and holds what a rollback needs of the target's shape - each
collection's type, options and indexes, a view among them - held without
a server.

A document store keeps no counters to put back; the rows a repair writes
are put back by its own undo. Nothing here proves a rollback end to end,
so the cell waits in `capabilities.UNPROVED` for a repair on a real
replica set.
"""
import ast

from migkit.config import Endpoint, Hop
from migkit.engines.mongodb import MongoEngine
from tests.mongo_model import Server


def _eng(tmp_path, dst):
    h = Hop(name="m", engine="mongodb", source=Endpoint(host="s", port=1),
            target=Endpoint(host="t", port=2), databases=["app"])
    h.report_dir = lambda db=None: tmp_path
    eng = MongoEngine(h)
    eng._client = lambda side: dst.client()
    return eng


def test_the_snapshot_holds_the_targets_shape_and_leaves_no_partial_file(
        tmp_path):
    dst = Server()
    orders = dst.db("app").create_collection("orders")
    orders.create_index([("sku", 1)], name="sku_1", unique=True)
    dst.db("app").create_collection("paid", viewOn="orders", pipeline=[])
    # `_shape` asks the server what each collection was made with
    eng = _eng(tmp_path, dst)
    state = tmp_path / "state"
    state.mkdir()
    eng.snapshot_state("app", state, "rows")
    got = ast.literal_eval((state / "dst-shape.txt").read_text())
    assert got == eng._shape("dst", "app")
    assert got["orders"]["type"] == "collection"
    assert "sku_1" in got["orders"]["indexes"]
    assert got["paid"]["type"] == "view"
    assert [p.name for p in state.iterdir()] == ["dst-shape.txt"]
    # taken again over what changed, the file is the new one whole
    orders.create_index([("at", 1)], name="at_1")
    eng.snapshot_state("app", state, "rows")
    assert "at_1" in ast.literal_eval(
        (state / "dst-shape.txt").read_text())["orders"]["indexes"]
