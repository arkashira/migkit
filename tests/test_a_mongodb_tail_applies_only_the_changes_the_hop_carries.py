"""A MongoDB tail applies the changes the hop's `mapping.ops` carries and
no others, to the collections it takes, under the names it gives them
(P0-PARITY.scope).

A move told to keep what the source deletes (an archive on the target)
does not delete when the stream says a document was; one that carries
updates and not inserts does not put a document there the target never
had. The position moves past every event either way - what is not carried
is not an event waiting - and the check holds the collection to what is
carried (`verdict.hold`).
"""
from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.ops", "mongodb")]


def _event(op, coll, _id, doc=None):
    return {"_id": {"_data": f"82{_id:02x}"}, "operationType": op,
            "ns": {"db": "shop", "coll": coll},
            "documentKey": {"_id": _id},
            **({"fullDocument": doc} if doc is not None else {})}


def _tail(tmp_path, ops=None, **hop):
    mapping = {"ops": ops} if ops else {}
    eng, src, dst = engine({"mapping": mapping, **hop}, tmp_path)
    held = dst.db("shop").add("orders", [{"_id": 1, "v": "old"},
                                         {"_id": 2, "v": "old"}])
    return eng, dst.db("shop"), held


def _apply(eng, target, *events):
    return [eng._tail_event("shop", target, ev, True) for ev in events]


def test_every_change_is_carried_unless_the_hop_says_fewer(tmp_path):
    eng, target, held = _tail(tmp_path)
    got = _apply(eng, target,
                 _event("insert", "orders", 3, {"_id": 3, "v": "new"}),
                 _event("update", "orders", 1, {"_id": 1, "v": "new"}),
                 _event("replace", "orders", 2, {"_id": 2, "v": "new"}),
                 _event("delete", "orders", 1))
    assert got == [1, 1, 1, 1]
    assert {d["_id"]: d["v"] for d in held.docs} == {2: "new", 3: "new"}


def test_a_hop_that_keeps_deletes_does_not_delete(tmp_path):
    eng, target, held = _tail(tmp_path, {"orders": ["insert", "update"]})
    got = _apply(eng, target, _event("delete", "orders", 1),
                 _event("update", "orders", 2, {"_id": 2, "v": "new"}))
    assert got == [0, 1]
    assert {d["_id"]: d["v"] for d in held.docs} == {1: "old", 2: "new"}


def test_a_hop_that_carries_updates_only_adds_no_document(tmp_path):
    eng, target, held = _tail(tmp_path, {"orders": "update"})
    got = _apply(eng, target,
                 _event("insert", "orders", 3, {"_id": 3, "v": "new"}),
                 _event("update", "orders", 4, {"_id": 4, "v": "new"}),
                 _event("update", "orders", 1, {"_id": 1, "v": "new"}),
                 _event("delete", "orders", 2))
    assert got == [0, 1, 1, 0]
    # the one it had is updated; none is put where there was none
    assert {d["_id"]: d["v"] for d in held.docs} == {1: "new", 2: "old"}


def test_a_hop_that_carries_inserts_only_leaves_a_document_as_it_was(
        tmp_path):
    eng, target, held = _tail(tmp_path, {"orders": "insert"})
    got = _apply(eng, target,
                 _event("insert", "orders", 3, {"_id": 3, "v": "new"}),
                 _event("update", "orders", 1, {"_id": 1, "v": "new"}))
    assert got == [1, 0]
    assert {d["_id"]: d["v"] for d in held.docs} == {1: "old", 2: "old",
                                                     3: "new"}


def test_the_rule_of_one_collection_is_not_the_rule_of_another(tmp_path):
    eng, target, held = _tail(tmp_path, {"shop.orders": ["insert", "update"],
                                         "*": "insert"})
    target.add("logs", [{"_id": 1}])
    got = _apply(eng, target, _event("delete", "orders", 1),
                 _event("delete", "logs", 1))
    assert got == [0, 0]
    got = _apply(eng, target, _event("update", "orders", 2, {"_id": 2}))
    assert got == [1]


def test_a_change_is_written_under_the_name_the_hop_gives_the_collection(
        tmp_path):
    eng, src, dst = engine(
        {"mapping": {"tables": {"shop.orders": "orders_v2"}}}, tmp_path)
    renamed = dst.db("shop").add("orders_v2", [])
    eng._tail_event("shop", dst.db("shop"),
                    _event("insert", "orders", 1, {"_id": 1}), True)
    assert renamed.docs == [{"_id": 1}]
    assert "orders" not in dst.db("shop").list_collection_names()


def test_without_go_a_change_is_counted_and_nothing_is_written(tmp_path):
    eng, target, held = _tail(tmp_path)
    assert eng._tail_event("shop", target, _event("delete", "orders", 1),
                           False) == 1
    assert len(held.docs) == 2


def test_the_tail_goes_round_its_loop_over_a_stream_of_all_kinds(tmp_path):
    """The tail itself, stream to target: what the hop carries lands, what
    it leaves out of its scope is not read, and the position still moves
    past every event until it has caught up."""
    from tests.mongo_fake import FakeStream
    eng, src, dst = engine(
        {"options": {"follow_until": "caught_up"}, "exclude": ["shop.scratch"],
         "mapping": {"ops": {"orders": ["insert", "update"]}}}, tmp_path)
    held = dst.db("shop").add("orders", [{"_id": 1, "v": "old"}])
    src.stream = FakeStream([
        _event("insert", "orders", 2, {"_id": 2, "v": "new"}),
        _event("delete", "orders", 1),
        _event("insert", "scratch", 9, {"_id": 9}),
        _event("update", "orders", 2, {"_id": 2, "v": "newer"})])
    lines = []
    eng.tail_apply("shop", True, tmp_path / "tail-token.json", lines.append)
    assert {d["_id"]: d["v"] for d in held.docs} == {1: "old", 2: "newer"}
    assert ("shop", "scratch") not in dst.touched
    assert any("caught up with the source" in line for line in lines)
    assert src.stream.events == []
