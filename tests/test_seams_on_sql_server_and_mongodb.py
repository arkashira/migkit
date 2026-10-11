"""The exact seam between a copy's range and the changes after it, on SQL
Server and MongoDB (backlog L3): each change carries its transaction as
the source names it - the change tracking version, the cluster time -
and a range's mark is what the source had made visible before the range
was read, so `mark_covers` leaves a change out only where the range's
read holds it. A mark from another database or replica set covers
nothing. A multi-document transaction's last event says it ended, and a
read the limit cut inside one does not (`canon.ended`, F7).
"""
import socket
import subprocess
import time

import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker


def _hop(engine):
    ep = Endpoint(host="x", port=1, user="x", password="x")
    return Hop(name="seam", engine=engine, source=ep, target=ep,
               databases=["d"])


def test_a_sql_server_change_is_held_to_the_version_of_its_mark():
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql"))
    eng.__dict__["_marks_on"] = {("src", "d"): "srv/5/2026-01-01"}
    mark = "srv/5/2026-01-01|40"
    older = canon.change("update", "dbo.t", {"id": 1}, {"id": 1}, txn=40)
    newer = canon.change("update", "dbo.t", {"id": 1}, {"id": 1}, txn=41)
    assert eng.mark_covers("src", "d", mark, older) is True
    assert eng.mark_covers("src", "d", mark, newer) is False
    # another database's mark, or a change with no version: not known
    assert eng.mark_covers("src", "d", "srv/6/2026-01-01|40", older) is None
    assert eng.mark_covers("src", "d", mark,
                           canon.change("delete", "dbo.t", {"id": 1})) \
        is None


def test_a_mongodb_change_is_held_to_the_cluster_time_of_its_mark():
    from bson.timestamp import Timestamp

    from migkit.engines.mongodb import MongoEngine
    eng = MongoEngine(_hop("mongodb"))
    eng.stream_identity = lambda side, db: {"replica set": "rs0"}
    mark = "rs0|100:3"
    for (t, i), want in (((99, 9), True), ((100, 3), True),
                         ((100, 4), False), ((101, 0), False)):
        c = canon.change("insert", "c", {"_id": 1}, {"_id": 1},
                         txn=Timestamp(t, i))
        assert eng.mark_covers("src", "d", mark, c) is want, (t, i)
    other = canon.change("insert", "c", {"_id": 1}, txn=Timestamp(1, 1))
    assert eng.mark_covers("src", "d", "rs1|100:3", other) is None


class _Stream:
    def __init__(self, events):
        self.events = list(events)
        self.resume_token = None

    def try_next(self):
        if not self.events:
            return None
        ev = self.events.pop(0)
        self.resume_token = ev["_id"]
        return ev

    def close(self):
        pass


class _Client:
    def __init__(self, events):
        self.events = events

    def __getitem__(self, name):
        return self

    def watch(self, pipeline, **kwargs):
        return _Stream(self.events)


def _event(n, txn=None, op="insert"):
    from bson.timestamp import Timestamp
    ev = {"_id": {"_data": f"82{n:08x}"}, "operationType": op,
          "ns": {"db": "d", "coll": "c"}, "documentKey": {"_id": n},
          "fullDocument": {"_id": n, "v": n},
          "clusterTime": Timestamp(1000, n)}
    if txn is not None:
        ev.update(txnNumber=txn, lsid={"id": "s"})
    return ev


def test_a_mongodb_transaction_ends_at_its_last_event():
    from migkit.engines.mongodb import MongoEngine
    eng = MongoEngine(_hop("mongodb"))
    events = [_event(1), _event(2, 7), _event(3, 7), _event(4)]
    eng._client = lambda side: _Client(list(events))
    got, _ = eng.neutral_changes("src", "d", "82", limit=10)
    assert [bool(c.get("txn_end")) for c in got] == [True, False, True,
                                                     True]
    assert got[2]["txn_at"] == "8200000003"
    assert got[0]["txn"].inc == 1
    # the limit cuts inside the transaction: its last event read does not
    # say it ended, and a tail ends the batch before it
    eng._client = lambda side: _Client(list(events))
    got, _ = eng.neutral_changes("src", "d", "82", limit=2)
    assert [bool(c.get("txn_end")) for c in got] == [True, False]


def test_a_collection_event_is_a_ddl_at_its_place():
    from migkit.engines.mongodb import MongoEngine
    eng = MongoEngine(_hop("mongodb"))
    drop = _event(2, op="drop")
    rename = _event(3, op="rename")
    rename["to"] = {"db": "d", "coll": "c2"}
    eng._client = lambda side: _Client([_event(1), drop, rename])
    got, _ = eng.neutral_changes("src", "d", "82", limit=10)
    assert [(c["op"], c.get("kind"), c.get("to")) for c in got] == [
        ("insert", None, None), ("ddl", "drop", None),
        ("ddl", "rename", "c2")]
    # under the hop's default `ddl` a collection event stops the tail as
    # it always did
    assert "drop" in eng.DDL_STOPS


MONGO, MONGO_PORT = "migkit-test-livetail-seam-mongo", 16262


@pytest.fixture(scope="module")
def mongo_server():
    subprocess.run(["docker", "rm", "-f", "-v", MONGO], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MONGO, "-p",
                    f"127.0.0.1:{MONGO_PORT}:27017", "mongo:7",
                    "--replSet", "rs0", "--bind_ip_all"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", MONGO_PORT)) == 0:
                    break
            time.sleep(1)
        subprocess.run(["docker", "exec", MONGO, "mongosh", "--quiet",
                        "--eval", "rs.initiate({_id: 'rs0', members:"
                        " [{_id: 0, host: '127.0.0.1:27017'}]})"],
                       capture_output=True)
        time.sleep(5)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MONGO],
                       capture_output=True)


@needs_docker
def test_a_mongodb_mark_covers_what_was_written_before_it(mongo_server):
    from migkit.engines.mongodb import MongoEngine
    ep = Endpoint(host="127.0.0.1", port=MONGO_PORT, user="", password="",
                  options={"uri_options": "directConnection=true"})
    eng = MongoEngine(Hop(name="seam", engine="mongodb", source=ep,
                            target=ep, databases=["d"]))
    client = eng._client("src")
    client["d"]["c"].delete_many({})
    token = eng.change_point("src", "d")
    client["d"]["c"].insert_one({"_id": 1})
    time.sleep(1)
    mark = eng.snapshot_mark("src", "d")
    assert mark and mark.startswith("rs0|"), mark
    client["d"]["c"].insert_one({"_id": 2})
    changes, _ = eng.neutral_changes("src", "d", token)
    by = {c["key"]["_id"]: c for c in changes}
    assert eng.mark_covers("src", "d", mark, by[1]) is True
    assert eng.mark_covers("src", "d", mark, by[2]) is False
