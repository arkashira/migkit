"""Amazon DocumentDB's pre-flight: change streams per collection, the
retention window against the move's forecast (a blocker carrying the
exact command and setting the DBA applies), and schema changes followed by
catalogue snapshots at every change-stream batch, since its change stream
emits none. The DocumentDB answers are fakes (`variants.UNPROVED_ON`); the
catalogue snapshots run on the sandbox's MongoDB."""
import pytest

from migkit import decide, variants
from migkit.engines.mongodb import MongoEngine


DOCDB = variants.identify("mongodb", {"version": "5.0.0", "_asked": "True",
                                      "list_change_streams": "[]",
                                      "_endpoint": "documentdb"})


def test_a_collection_without_its_change_stream_is_a_blocker_with_the_command():
    rows = MongoEngine.documentdb_preflight(
        "app", ["orders", "users"], [{"database": "app",
                                      "collection": "users"}],
        86400, 3600, DOCDB)
    fail = rows[0]
    assert fail.status == "fail" and "orders" in fail.detail
    assert fail.fix_hint == ('db.adminCommand({modifyChangeStreams: 1,'
                             ' database: "app", collection: "orders",'
                             ' enable: true})')
    # the whole database on, or the whole cluster: no blocker
    for on in ({"database": "app", "collection": ""},
               {"database": "", "collection": ""}):
        assert MongoEngine.documentdb_preflight(
            "app", ["orders"], [on], 86400, 3600, DOCDB)[0].status == "ok"


def test_retention_shorter_than_the_forecast_is_a_blocker_with_the_setting():
    rows = MongoEngine.documentdb_preflight(
        "app", ["a"], [{"database": "", "collection": ""}], 10800, 30000.2,
        DOCDB)
    ret = rows[1]
    assert ret.status == "fail" and "10800s" in ret.detail
    assert ret.fix_hint == ("set change_stream_log_retention_duration to"
                            " 30001 in the cluster parameter group")
    # longer than the longest it keeps: the move is to be split
    long = MongoEngine.documentdb_preflight(
        "app", ["a"], [{"database": "", "collection": ""}], 604800, 900000,
        DOCDB)[1]
    assert "split" in long.detail and "604800" in long.fix_hint
    ok = MongoEngine.documentdb_preflight(
        "app", ["a"], [{"database": "", "collection": ""}], 10800, 600,
        DOCDB)[1]
    assert ok.status == "ok"
    # unread is said as unknown, never clean
    unk = MongoEngine.documentdb_preflight(
        "app", ["a"], [{"database": "", "collection": ""}], None, 600,
        DOCDB)[1]
    assert unk.status == "warn" and "not read" in unk.detail


def test_the_catalogue_difference_is_said():
    before = {"a": ("[]", (("_id_", (("_id", "1"),), False),))}
    after = {"a": ("[]", (("_id_", (("_id", "1"),), False),
                          ("by_x", (("x", "1"),), True))),
             "b": ("[]", ())}
    assert MongoEngine.catalogue_changes(before, after) == [
        "collection b was created", "index by_x was added on a"]
    assert MongoEngine.catalogue_changes(after, before) == [
        "collection b was dropped", "index by_x was dropped from a"]


def test_a_brand_not_told_apart_follows_by_snapshots():
    way = decide.ddl_follow({"told": False, "expanded_events": True})
    assert way.rung.name == "snapshots"
    way = decide.ddl_follow({"told": True, "expanded_events": True,
                             "brand": "mongodb"})
    assert way.rung.name == "events"
    assert decide.ddl_follow({"told": True, "brand": "documentdb",
                              "expanded_events": False}).rung.name \
        == "snapshots"


@pytest.mark.docker
def test_a_schema_change_between_two_batches_is_seen_by_the_snapshot():
    import pathlib
    import tempfile
    from tests import servers
    from migkit.config import Endpoint, Hop
    with servers.space("mongo", __name__, own=False) as got:
        ep = lambda s: Endpoint(host=s.host, port=s.port,  # noqa: E731
                                user=s.user or "", password=s.password or "")
        hop = Hop(name="d", engine="mongodb", source=ep(got.src),
                  target=ep(got.dst), db_map={"w17g": "w17g"})
        hop.report_dir = lambda db=None: pathlib.Path(tempfile.mkdtemp())
        eng = MongoEngine(hop)
        eng._brand_cache = (DOCDB, DOCDB)      # the source as DocumentDB
        db = eng._client("src")["w17g"]
        db.drop_collection("t")
        db["t"].insert_one({"x": 1})
        held = {}
        assert eng.ddl_at_batch("src", "w17g", held) == []
        db["t"].insert_one({"x": 2})           # a batch of rows: no DDL
        assert eng.ddl_at_batch("src", "w17g", held) == []
        db["t"].create_index("x", name="by_x")  # DDL between two batches
        assert eng.ddl_at_batch("src", "w17g", held) == [
            "index by_x was added on t"]
        eng._client("src").drop_database("w17g")
