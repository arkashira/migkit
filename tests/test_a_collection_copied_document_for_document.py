"""MongoDB copies collection by collection, resumably, document for document
(backlog 0e).

The table copier every other engine shares carries values in the classes
another engine can hold, and a nested document or an array has none. So
MongoDB's collection copier moves each document as stored, and resumes by
`_id` from the checkpoint.

The resume is where a copier like this goes wrong without saying so. A
plain `{_id: {$gt: last}}` matches only `_id`s of the same type as the
last one, so a collection whose `_id`s are numbers and then strings loses
every string once a batch ends on a number. The copy is stopped halfway,
on a number, and resumed.
"""
import datetime
import socket
import subprocess
import time
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-mcp-src", "migkit-test-mcp-dst"
SRC_PORT, DST_PORT = 15814, 15815


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module")
def pair():
    import pymongo
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:27017", "mongo:7"], check=True,
                       capture_output=True)
    try:
        assert _wait(SRC_PORT) and _wait(DST_PORT)
        s = pymongo.MongoClient(port=SRC_PORT, serverSelectionTimeoutMS=60000)
        t = pymongo.MongoClient(port=DST_PORT, serverSelectionTimeoutMS=60000)
        s.admin.command("ping")
        t.admin.command("ping")
        yield s, t
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path):
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="mcp", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


class _Stop(Exception):
    pass


def test_stopped_on_a_number_and_resumed_it_loses_no_string(pair, tmp_path):
    from bson import BSON, Binary, Decimal128, ObjectId

    from migkit.cli import _Checkpoint
    s, t = pair
    docs = [{"_id": i, "k": f"n{i}", "nested": {"a": [i, {"b": i}]}}
            for i in range(1, 8)]
    docs += [{"_id": "a", "k": "sa", "d": Decimal128(Decimal("1.50"))},
             {"_id": "b", "k": "sb",
              "when": datetime.datetime(2024, 2, 29, 12, 0)},
             {"_id": ObjectId("65f000000000000000000001"), "k": "o",
              "raw": Binary(b"\x00\x01", 128)}]
    s.app.things.insert_many(docs)
    s.app.things.create_index("k", unique=True, name="k_unique")
    s.app.stale.insert_one({"_id": 1, "v": "source"})
    t.app.stale.insert_one({"_id": 99, "v": "a document of the target's"})

    eng = _engine(tmp_path)
    assert eng.list_move_tables("app") == [("", "stale"), ("", "things")]
    ck = _Checkpoint(tmp_path / "move.json")
    said = []

    def stop_after_two(line):
        said.append(line)
        if "things" in line and len([x for x in said if "things" in x]) == 2:
            raise _Stop()
    with pytest.raises(_Stop):
        eng.move_table("app", "", "things", 3, ck, stop_after_two)
    # the second batch of three ended on the number 6
    assert _Checkpoint(tmp_path / "move.json")["app.things"]["last"] == "6"
    ck = _Checkpoint(tmp_path / "move.json")
    eng.move_table("app", "", "things", 3, ck, said.append)
    eng.move_table("app", "", "stale", 3, ck, said.append)

    def stored(coll):
        return sorted(BSON.encode(d) for d in coll.find())
    assert stored(t.app.things) == stored(s.app.things)
    assert t.app.things.count_documents({}) == 10
    # the target's own document is not this copy's
    assert stored(t.app.stale) == stored(s.app.stale), said
    assert "app.stale: emptied 1 documents the target held before the copy" \
        in said, said
    # a collection the copy made gets the source's indexes
    got = t.app.things.index_information()
    assert got["k_unique"]["unique"] is True, got
    assert not [x for x in said if "mongo" in x.lower()], said


def test_a_finished_collection_is_asked_again_before_it_is_skipped(
        pair, tmp_path):
    """Skipped only while both sides still hold the same documents: an
    earlier run's word was taken whatever the source had done since."""
    from migkit.cli import _Checkpoint
    s, t = pair
    s.app.again.drop()
    t.app.again.drop()
    s.app.again.insert_many([{"_id": i, "v": i} for i in range(5)])
    eng = _engine(tmp_path)
    eng.move_table("app", "", "again", 3,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    said = []
    eng.move_table("app", "", "again", 3,
                   _Checkpoint(tmp_path / "move.json"), said.append)
    assert said == ["app.again: done earlier, and both sides still hold the"
                    " same rows - skipped"], said
    s.app.again.update_one({"_id": 2}, {"$set": {"v": 20}})
    said = []
    eng.move_table("app", "", "again", 3,
                   _Checkpoint(tmp_path / "move.json"), said.append)
    assert said[0] == ("app.again: done earlier, and the two sides no longer"
                       " hold the same rows - copying it again"), said
    assert t.app.again.find_one({"_id": 2})["v"] == 20
