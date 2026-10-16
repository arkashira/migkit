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

# `cutover.two-way` on mongodb is proved here once the depth probe (capabilities.py) reads `mark_unit` as the way

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
