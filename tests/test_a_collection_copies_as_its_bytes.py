"""A MongoDB collection copied as the bytes the server sent: never decoded
and encoded again, inserted where the copy emptied the collection, read
past the last `_id` by the index where every `_id` is of one type - and,
where they are not, by the comparison that orders across types.

Measured before: 200,000 documents in 3.4s, each decoded into Python and
replaced one by one; now 0.7-1.0s. What must not change with it: a
collection whose `_id`s are numbers and strings loses none after a stop,
and a batch written again after a stop replaces what is there instead of
stopping on the duplicate.
"""
import socket
import subprocess
import time

import pytest
from bson import ObjectId

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = ("migkit-test-mgraw-src", 15941), ("migkit-test-mgraw-dst", 15942)


@pytest.fixture(scope="module")
def clients():
    import pymongo
    for name, port in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                        f"{port}:27017", "mongo:7"], check=True,
                       capture_output=True)
    try:
        out = []
        for _, port in (SRC, DST):
            end = time.time() + 90
            while time.time() < end:
                with socket.socket() as s:
                    s.settimeout(1)
                    if s.connect_ex(("127.0.0.1", port)) == 0:
                        break
                time.sleep(1)
            c = pymongo.MongoClient(port=port, serverSelectionTimeoutMS=30000)
            c.admin.command("ping")
            out.append(c)
        yield out
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path):
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="mgraw", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=SRC[1]),
              target=Endpoint(host="127.0.0.1", port=DST[1]),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


def _stop_after(monkeypatch, n):
    """The checkpoint saved `n` times, then the run stopped - as a kill
    between a batch written and its position saved."""
    from migkit.cli import _Checkpoint
    real, seen = _Checkpoint.save, []

    def save(self):
        seen.append(1)
        if len(seen) == n:
            raise KeyboardInterrupt
        return real(self)
    monkeypatch.setattr(_Checkpoint, "save", save)


@pytest.mark.parametrize("ids", ["one type", "numbers and strings"])
def test_stopped_and_resumed_every_document_once(clients, tmp_path,
                                                 monkeypatch, ids):
    from migkit.cli import _Checkpoint
    src, dst = clients
    name = "c" + ids.replace(" ", "")
    src.app[name].drop()
    dst.app[name].drop()
    keys = list(range(300))
    if ids != "one type":
        keys += [f"k{i:03}" for i in range(300)] + [ObjectId()
                                                    for _ in range(50)]
    src.app[name].insert_many([{"_id": k, "v": {"n": i, "a": [i, "x"]}}
                               for i, k in enumerate(keys)])
    eng = _engine(tmp_path)
    ck = tmp_path / f"{name}.json"
    _stop_after(monkeypatch, 4)
    with pytest.raises(KeyboardInterrupt):
        eng.move_table("app", "", name, 50, _Checkpoint(ck), [].append)
    monkeypatch.undo()
    got_before = dst.app[name].count_documents({})
    assert 0 < got_before < len(keys)
    eng.move_table("app", "", name, 50, _Checkpoint(ck), [].append)
    assert dst.app[name].count_documents({}) == len(keys)
    have = sorted(map(str, (d["_id"] for d in dst.app[name].find({}, {
        "_id": 1}))))
    assert have == sorted(map(str, keys))
    one = dst.app[name].find_one({"_id": keys[7]})
    assert one["v"] == {"n": 7, "a": [7, "x"]}
