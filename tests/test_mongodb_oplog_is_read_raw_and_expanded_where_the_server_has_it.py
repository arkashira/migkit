"""MongoDB's changes read raw and expanded where the server has them
(LT.C1): the oplog read as the bytes the server stored (`local.oplog.rs`,
`oplog_events`) gives the same changes as the change stream for the same
writes, value for value; where every collection followed keeps its pre-
and post-images on 6.0 or later, the stream carries the document as of the
change and the one before it, chosen by the decision engine (`decide.
mongo_change_images`) on the collections' own options, and otherwise the
document is looked up; a standalone is still followed by buckets.

Against the `mongo` sandbox profile (a one-member replica set), never a
real database."""
import bson
from bson import Decimal128

from migkit import decide
from migkit.config import Hop
from migkit.engines.mongodb import MongoEngine, oplog_events
from tests import servers

mongo = servers.fixture("mongo")


def plain(x):
    return bson.decode(bson.encode({"v": x}))["v"]


def test_the_images_rung_needs_6_0_and_every_collection_keeping_them():
    got = decide.mongo_change_images({"version": (5, 0), "collections": ["a"],
                                      "without images": []})
    assert got.rung.name == "looked up"
    got = decide.mongo_change_images({"version": (7, 0),
                                      "collections": ["a", "b"],
                                      "without images": ["b"]})
    assert got.rung.name == "looked up"
    assert decide.mongo_change_images({"version": (7, 0), "collections": [
        "a"], "without images": []}).rung.name == "images"


def test_a_standalone_is_still_followed_by_buckets():
    got = MongoEngine.follow_choice({"replica set": False,
                                     "stream opens": False})
    assert got.rung.name == "bucket sweep"


def test_raw_oplog_events_equal_the_stream_and_images_are_read(mongo):
    c = mongo.src.client()
    db = c[mongo.name]
    for name in ("img", "plain"):
        db.drop_collection(name)
    db.create_collection("img", changeStreamPreAndPostImages={"enabled": True})
    db.create_collection("plain")
    eng = MongoEngine(Hop(name="o", engine="mongodb",
                          source=mongo.src.endpoint(),
                          target=mongo.dst.endpoint(),
                          databases=[mongo.name]))
    assert eng.image_facts(mongo.name)["without images"] == ["plain"]
    assert eng.image_options(mongo.name) == {"full_document": "updateLookup"}
    db.command("collMod", "plain",
               changeStreamPreAndPostImages={"enabled": True})
    said = []
    opts = eng.image_options(mongo.name, said.append)
    assert opts == {"full_document": "required",
                    "full_document_before_change": "whenAvailable"}
    assert said and "as of the change" in said[0]
    last = eng._oplog("src", {}, limit=1, sort=-1)[0]["ts"]
    with db.watch(**opts) as stream:
        db.img.insert_one({"_id": 1, "a": 1, "d": Decimal128("1.10")})
        db.img.update_one({"_id": 1}, {"$set": {"a": 2}})
        db.img.delete_one({"_id": 1})
        events = [stream.next() for _ in range(3)]
    entries = eng._oplog("src", {"ns": f"{mongo.name}.img",
                                 "ts": {"$gt": last}}, stored=True)
    txns = {}
    raw = [e for entry in entries
           for e in oplog_events(entry, mongo.name, txns)]
    assert [e["operationType"] for e in raw] == \
        [e["operationType"] for e in events] == ["insert", "update", "delete"]
    assert [plain(e["documentKey"]) for e in raw] == \
        [plain(e["documentKey"]) for e in events]
    assert plain(raw[0]["fullDocument"]) == plain(events[0]["fullDocument"])
    assert plain(raw[0]["fullDocument"])["d"] == Decimal128("1.10")
    # the stream's own update, as of the change, and the document before it
    assert events[1]["fullDocument"]["a"] == 2
    assert events[1]["fullDocumentBeforeChange"]["a"] == 1
    assert events[2]["fullDocumentBeforeChange"]["a"] == 2
    assert plain(raw[1]["modifiers"]) == {"$set": {"a": 2}}
