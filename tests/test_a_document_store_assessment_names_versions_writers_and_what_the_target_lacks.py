"""`migkit assess` on a MongoDB hop says what every engine's does
(backlog P0-PARITY.all), held without a server:

* both servers' versions read from what each says of itself and held to
  each other - and a side that would not say is unknown, not clean;
* the checks that predict trouble run before the move: whether the source
  can be followed after the copy (a change stream needs a replica set, and
  an oplog that reaches back), and whether the target runs a server new
  enough for the wildcard, hidden, time-series and clustered features the
  source uses;
* what else writes into the target's database named, from the operations
  running on it and the writes the server counts on it - and a target that
  would not say is unknown, not clean.

The calls the engine makes are answered by `tests/mongo_fake.py`.
"""
import pytest

from migkit import capabilities as caps
from migkit.config import Endpoint, Hop
from migkit.engines.mongodb import MongoEngine
from tests.mongo_fake import Server

#: what this module proves, item by engine (`capabilities.proofs`): the
#: versions and the pre-flight use calls the engine has always made
#: (`buildInfo`, `hello`, the oplog, the collection listings). The writers
#: are found by `$currentOp` and `top`, which no server has answered yet:
#: that cell waits in `capabilities.UNPROVED`
PROVES = [("assess.versions", "mongodb"), ("assess.preflight", "mongodb")]


def _eng(tmp_path, src, dst):
    h = Hop(name="m", engine="mongodb", source=Endpoint(host="s", port=1),
            target=Endpoint(host="t", port=2), databases=["app"])
    h.report_dir = lambda db=None: tmp_path
    eng = MongoEngine(h)
    eng._client = lambda side: (src if side == "src" else dst).client()
    eng.WRITE_WATCH = 0.0
    return eng


def _rows(eng, item):
    return [r for r in eng.assess() if item in r["item"]]


@pytest.fixture
def quiet(monkeypatch):
    """The parts of an assessment that look for programs on this machine
    are not this test's."""
    monkeypatch.setattr(MongoEngine, "_bulk_path_rows", lambda self: [])


# ---- the versions --------------------------------------------------------

def test_both_servers_versions_are_read_and_held_to_each_other(tmp_path,
                                                               quiet):
    src, dst = Server(version="7.0.5"), Server(version="7.0.12")
    eng = _eng(tmp_path, src, dst)
    assert eng._server_versions() == ("7.0.5", "7.0.12")
    row, = _rows(eng, "server version match")
    assert row["level"] == "pass" and "src 7.0.5 / dst 7.0.12" in row[
        "detail"]
    # another major version is a warning, said with both
    eng = _eng(tmp_path, Server(version="6.0.9"), Server(version="7.0.5"))
    row, = _rows(eng, "server version match")
    assert row["level"] == "warn" and "6.0.9" in row["detail"]


def test_a_side_that_will_not_say_its_version_is_unknown_not_clean(tmp_path,
                                                                   quiet):
    eng = _eng(tmp_path, Server(version="7.0.5"), Server(version=None))
    row, = _rows(eng, "server version match")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]


# ---- the pre-flight ------------------------------------------------------

def test_a_standalone_source_cannot_be_followed_and_the_assessment_says_so(
        tmp_path, quiet):
    eng = _eng(tmp_path, Server(replica_set=None), Server())
    row, = _rows(eng, "change stream")
    assert row["scope"] == "before the move" and row["level"] == "warn"
    assert "standalone" in row["detail"] and "replica set" in row["detail"]


def test_a_replica_set_source_says_how_far_back_its_oplog_reaches(tmp_path,
                                                                  quiet):
    src = Server(oplog_from=1_000, now=1_000 + 36_000)
    row, = _rows(_eng(tmp_path, src, Server()), "change stream")
    assert row["level"] == "pass"
    assert "10.0 hours" in row["detail"] and "rs0" in row["detail"]


def test_an_oplog_this_account_cannot_read_is_unknown_not_clean(tmp_path,
                                                                quiet):
    src = Server()
    src.oplog = []
    row, = _rows(_eng(tmp_path, src, Server()), "change stream")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]


def test_a_sharded_source_is_followed_through_its_router(tmp_path, quiet):
    row, = _rows(_eng(tmp_path, Server(replica_set=None, sharded=True),
                      Server()), "change stream")
    assert row["level"] == "pass" and "router" in row["detail"]


def _features(src):
    c = src.db("app").create_collection("events")
    c.create_index([("$**", 1)], name="wild")
    c.create_index([("a", 1)], name="hid", hidden=True)
    src.db("app").create_collection("plain")
    src.db("app").create_collection("ts", timeseries={"timeField": "t"})


def test_what_the_source_uses_and_an_older_target_lacks_fails_the_assessment(
        tmp_path, quiet):
    src = Server(version="7.0.5")
    _features(src)
    row, = _rows(_eng(tmp_path, src, Server(version="4.0.28")),
                 "features the target lacks")
    assert row["level"] == "fail"
    for what in ("wildcard index (events.wild) needs 4.2",
                 "hidden index (events.hid) needs 4.4",
                 "time-series collection (ts) needs 5.0"):
        assert what in row["detail"], row["detail"]
    assert "plain" not in row["detail"]
    # a target of 4.4 lacks the time-series one and no more
    row, = _rows(_eng(tmp_path, src, Server(version="4.4.1")),
                 "features the target lacks")
    assert "time-series" in row["detail"] and "wildcard" not in row["detail"]


def test_a_target_that_has_them_passes_and_one_unread_is_unknown(tmp_path,
                                                                quiet):
    src = Server(version="7.0.5")
    _features(src)
    row, = _rows(_eng(tmp_path, src, Server(version="7.0.5")),
                 "features the target lacks")
    assert row["level"] == "pass"
    row, = _rows(_eng(tmp_path, src, Server(version=None)),
                 "features the target lacks")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]


def test_the_preflight_checks_are_what_the_matrix_reads():
    assert MongoEngine.PREFLIGHT
    for name in MongoEngine.PREFLIGHT:
        assert callable(getattr(MongoEngine, name))
    for item in ("assess.versions", "assess.preflight", "assess.writers"):
        assert caps.depth_has("mongodb", item), item


# ---- the writers ---------------------------------------------------------

def test_nothing_else_writing_into_the_target_is_none_found(tmp_path, quiet):
    eng = _eng(tmp_path, Server(), Server())
    assert eng.stream_writers("app") == []
    row, = _rows(eng, "no other replication writes into the target")
    assert row["level"] == "pass" and row["detail"] == "none found"


def test_a_client_writing_into_the_target_is_named(tmp_path, quiet):
    dst = Server()
    dst.running = lambda: [
        {"op": "update", "ns": "app.orders", "appName": "sync",
         "client": "10.0.0.7:5555"},
        {"op": "getmore", "ns": "app.orders", "appName": "reader"},
        {"op": "command", "ns": "app.lines", "client": "10.0.0.8:1",
         "command": {"insert": "lines"}},
        {"op": "update", "ns": "other.orders", "appName": "elsewhere"}]
    eng = _eng(tmp_path, Server(), dst)
    named = [w for w, pausable in eng.stream_writers("app")]
    assert named == ["a client at 10.0.0.8 writing to app.lines",
                     "sync at 10.0.0.7 writing to app.orders"]
    assert not any(p for _, p in eng.stream_writers("app"))
    row, = _rows(eng, "no other replication writes into the target")
    assert row["level"] == "fail" and "sync at 10.0.0.7" in row["detail"]


def test_writes_the_server_counts_with_no_client_to_name_are_said(tmp_path,
                                                                  quiet):
    dst = Server()
    counts = iter([{"app.orders": {"insert": {"count": 4}}},
                   {"app.orders": {"insert": {"count": 49}},
                    "other.x": {"insert": {"count": 900}}}])
    dst.top = lambda: next(counts)
    eng = _eng(tmp_path, Server(), dst)
    got = eng.stream_writers("app")
    assert len(got) == 1 and got[0][1] is False
    assert got[0][0].startswith("a writer the server does not name (45 writes")


def test_migkits_own_tail_is_expected_to_write_and_can_be_paused(
        tmp_path, quiet, monkeypatch):
    from migkit import tailctl
    monkeypatch.setattr(tailctl, "alive", lambda where: True)
    dst = Server()
    counts = iter([{"app.orders": {"insert": {"count": 1}}},
                   {"app.orders": {"insert": {"count": 99}}}])
    dst.top = lambda: next(counts)
    eng = _eng(tmp_path, Server(), dst)
    assert eng.stream_writers("app") == [("migkit's change tail", True)]


def test_a_target_that_will_not_say_who_writes_is_unknown_not_clean(
        tmp_path, quiet):
    dst = Server()
    dst.running = None
    eng = _eng(tmp_path, Server(), dst)
    # a repair goes on as it did, with what it could ask
    assert eng.stream_writers("app") == []
    row, = _rows(eng, "no other replication writes into the target")
    assert row["level"] == "warn" and "unknown, not clean" in row["detail"]
