"""The mark migkit's own writes carry on MongoDB, written in the engine's
atomic unit (a multi-document transaction) and read back from the change stream.

Proved on a server: the rung is chosen by the decision engine from a probe
of the side itself (a probe ran, the Choice names the cost of the rung it
took), a unit migkit writes carries the mark and an application's write
beside it carries none, a reader that applies what it reads applies the
application's and leaves migkit's out (counts asserted), a unit that fails
in the middle leaves neither its mark nor its rows, `doctor` names the rung,
and teardown takes away what the mark left.
"""
import pytest

from migkit import engines, failpoint, marks
from migkit.config import Endpoint, Hop
from tests import servers
from tests.conftest import needs_docker

PROVES = [("cutover.two-way", ("mongodb",))]

pytestmark = needs_docker

space = servers.fixture("mongo")

DB = "w15h_mongo"


def _eng(space, **opts):
    hop = Hop(name="w15h", engine="mongodb", databases=[DB],
              source=space.src.endpoint(), target=space.dst.endpoint(),
              options={"two_way": True, **opts})
    return engines.get_engine(hop)


def _drain(stream, idle=2.0):
    import time
    out, last = [], time.time()
    while time.time() - last < idle:
        ev = stream.try_next()
        if ev is not None:
            out.append(ev)
            last = time.time()
    return out


def test_the_rung_is_chosen_from_a_probe_and_the_choice_names_its_cost(space):
    eng = _eng(space)
    marks.MEASURED.clear()
    got = marks.choice(eng, "dst", DB)
    assert got.path == "txn_doc" and eng._mark_rung == "txn_doc"
    assert got.cost().measured and got.cost().mean >= 0.0
    assert ("mongodb", "dst", "txn_doc") in marks.MEASURED        # a probe ran
    # the field rung needs pre-images that this database does not have
    assert any(r.name == "field" for r, _ in got.passed)
    assert "pre-images" in dict((r.name, w) for r, w in got.passed)["field"]
    assert eng.mark_prove("dst", DB, "txn_doc") is None


def test_a_unit_carries_the_mark_and_the_reader_skips_only_its_own(space):
    writer, reader = _eng(space), _eng(space)
    reader.hop = Hop(name="w15h-back", engine="mongodb", databases=[DB],
                     source=space.dst.endpoint(), target=space.src.endpoint(),
                     options={"two_way": True})
    reader = engines.get_engine(reader.hop)
    client = space.dst.client()
    client[DB].drop_collection("docs")
    client[DB].create_collection("docs")
    with client[DB].watch(full_document="updateLookup") as stream:
        writer.mark_unit("dst", DB, "txn_doc", 5,
                         [("write", "docs", {"_id": i, "by": "migkit"})
                          for i in range(3)], run_id="r1")
        client[DB]["docs"].insert_one({"_id": 100, "by": "app"})
        client[DB]["docs"].insert_one({"_id": 101, "by": "app"})
        events = _drain(stream)
    own = [writer.mark_own(e) for e in events]
    assert sum(own) == 4 and len(events) == 6      # origin + 3 rows / 2 app
    # in the transaction: the origin document and the rows share one
    origin = [e for e in events if e["ns"]["coll"] == "migkit_origin"]
    assert len(origin) == 1 and origin[0]["fullDocument"]["_migkit_marker"][
        "batch"] == 5
    dst = space.src.client()[reader._d("dst", DB)]
    dst.drop_collection("docs")
    applied = sum(reader._tail_event(DB, dst, e, True) for e in events)
    assert applied == 2                              # the application's only
    assert sorted(d["_id"] for d in dst["docs"].find()) == [100, 101]
    assert "migkit_origin" not in dst.list_collection_names()


def test_a_unit_that_fails_in_the_middle_leaves_neither_mark_nor_rows(
        space, monkeypatch):
    eng = _eng(space)
    there = space.dst.client()[DB]
    there.drop_collection("docs")
    there.drop_collection("migkit_origin")
    there.create_collection("docs")
    ops = [("write", "docs", {"_id": i}) for i in range(4)]
    monkeypatch.setenv("MIGKIT_FAILPOINT", "mark.unit:3:raise")
    failpoint.reset()
    with pytest.raises(RuntimeError):
        eng.mark_unit("dst", DB, "txn_doc", 6, ops, run_id="r2")
    assert there["docs"].count_documents({}) == 0
    assert there["migkit_origin"].count_documents({}) == 0
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    failpoint.reset()
    eng.mark_unit("dst", DB, "txn_doc", 6, ops, run_id="r2")
    assert there["docs"].count_documents({}) == 4
    assert there["migkit_origin"].count_documents({"_id": "r2:6"}) == 1


def test_field_rung_marks_inserts_and_deletes_when_pre_images_are_on(space):
    eng = _eng(space)
    db = "w15h_mongo_pre"
    c = space.dst.client()[db]
    c.drop_collection("docs")
    c.create_collection("docs",
                        changeStreamPreAndPostImages={"enabled": True})
    facts = eng.mark_facts("dst", db)
    assert facts["pre_images"] and not marks.rung("mongodb", "field").needs(
        facts)
    assert eng.mark_prove("dst", db, "field") is None


def test_doctor_names_the_rung_and_teardown_removes_the_origin(
        space, tmp_path):
    eng = _eng(space)
    marks.choice(eng, "dst", DB)
    token = str(tmp_path / "tail.token")
    marks._keep_rung(token, eng._mark_rung)
    assert "migkit_origin" in marks.said(eng, token, DB)
    eng.mark_unit("dst", DB, "txn_doc", 9,
                  [("write", "docs", {"_id": "t"})])
    there = space.dst.client()[DB]
    assert "migkit_origin" in there.list_collection_names()
    marks.teardown(eng, "dst", DB)
    assert "migkit_origin" not in there.list_collection_names()
    assert "_mark_rung" not in eng.__dict__

def test_the_rung_is_taken_once_per_tail_and_again_at_a_boundary(
        tmp_path, monkeypatch):
    """No server: the tail takes its rung from the side's facts once
    (`marks.tail_start`), keeps it while the facts hold, and takes it again
    through the same probe and decision at a boundary where they changed."""
    hop = Hop(name="w16f", engine="mongodb", databases=[DB],
              source=Endpoint(host="127.0.0.1", port=1),
              target=Endpoint(host="127.0.0.1", port=1),
              options={"two_way": True})
    eng = engines.get_engine(hop)
    facts = [{"family": "mongodb", "version": (7, 0), "transactions": True, "pre_images": False, "can_create": True}]
    monkeypatch.setattr(eng, "mark_facts", lambda side, db: dict(facts[0]))
    monkeypatch.setattr(eng, "mark_prove", lambda *a: None)
    monkeypatch.setattr(eng, "mark_timed",
                        lambda side, db, rung, rounds: [1e-5] * rounds)
    said, token = [], tmp_path / "tail.token"
    first = marks.tail_start(eng, DB, True, token, said.append)
    assert first == 'txn_doc' and eng._mark_rung == first
    assert any("carry" in m for m in said)
    said.clear()
    assert marks.tail_round(eng, DB, token, said.append) == first
    assert not said                      # the facts held: nothing chosen
    facts[0] = {"family": "mongodb", "version": (7, 0), "transactions": False, "pre_images": True, "can_create": True}
    again = marks.tail_round(eng, DB, token, said.append)
    assert any("choosing the mark again" in m for m in said)
    assert again == 'field' == eng._mark_rung
    assert marks.kept_rung(token) == again
    assert marks.tail_start(eng, DB, False, token, said.append) is None


def test_the_tails_round_is_one_transaction_with_the_origin_document_first(
        space, tmp_path):
    from bson.raw_bson import RawBSONDocument
    import bson
    eng = _eng(space)
    marks.tail_start(eng, DB, True, tmp_path / "tail.token", lambda m: None)
    assert eng._mark_rung == "txn_doc"
    dst = eng._client("dst")[eng._d("dst", DB)]
    dst["w16f"].delete_many({})
    dst[eng.MARK_COLLECTION].delete_many({})
    docs = [RawBSONDocument(bson.encode({"_id": i, "v": i})) for i in range(3)]
    evs = [{"operationType": "insert", "ns": {"coll": "w16f"},
            "documentKey": {"_id": i}, "fullDocument": d}
           for i, d in enumerate(docs)]
    with dst["w16f"].watch([], **eng._expanded_events()) as stream:
        with eng._tail_unit(dst) as unit:
            for ev in evs:
                assert eng._tail_event(DB, dst, ev, True, unit) == 1
        app = dst["w16f"].insert_one({"_id": 99})
        got = _drain(stream)
    assert dst["w16f"].count_documents({}) == 4
    assert dst[eng.MARK_COLLECTION].count_documents({}) == 1
    # read in the stream's order, as the tail reads it: the origin
    # document is asked first and names the transaction
    seen = [(e["ns"]["coll"], eng.mark_own(e)) for e in got
            if e["operationType"] == "insert"]
    assert [own for coll, own in seen if coll == "w16f"] == [
        True, True, True, False]
    assert seen[0] == (eng.MARK_COLLECTION, True)
    # a round that fails leaves neither the mark nor its writes
    dst["w16f"].delete_many({})
    dst[eng.MARK_COLLECTION].delete_many({})
    try:
        with eng._tail_unit(dst) as unit:
            eng._tail_event(DB, dst, evs[0], True, unit)
            raise RuntimeError("stop")
    except RuntimeError:
        pass
    assert dst["w16f"].count_documents({}) == 0
    assert dst[eng.MARK_COLLECTION].count_documents({}) == 0
