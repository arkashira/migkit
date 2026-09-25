"""The MongoDB bulk load reports each collection as it lands, and does not
call a load complete when documents failed to load.

The load finishes each collection with a line naming how many documents
landed and how many failed. migkit read none of it, and printed one "copied"
line per database when the whole thing was over.

The case that matters is a collection whose validator was added after some
of its documents were written. The source keeps those documents, since a
validator does not look back. The load, though, creates the collection
with its validator first and then inserts, so each such document is
refused.
"""
import shutil
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-mgl-src", "migkit-test-mgl-dst"
SRC_PORT, DST_PORT = 15687, 15688


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not _docker(), reason="docker not available"),
    pytest.mark.skipif(not (shutil.which("mongodump")
                            and shutil.which("mongorestore")),
                       reason="the MongoDB dump programs are not installed"),
]


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


def _hop():
    return Hop(name="mgl", engine="mongodb",
               source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                               password=""),
               target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                               password=""),
               databases=["app"], db_map={"app": "app"})


def _move(tmp_path):
    from migkit import movers
    hop = _hop()
    hop.report_dir = lambda db=None: tmp_path
    said = []
    movers.mongodump_move(hop, "app", 2, True, said.append)
    return [str(s) for s in said]


def test_each_collection_is_reported_as_it_lands(pair, tmp_path):
    s, t = pair
    s.drop_database("app")
    t.drop_database("app")
    s.app.orders.insert_many([{"_id": i} for i in range(5)])
    s.app.people.insert_many([{"_id": i} for i in range(3)])
    said = " | ".join(_move(tmp_path))
    assert "app.orders: loaded" in said, said
    assert "app.people: loaded" in said, said
    assert t.app.orders.count_documents({}) == 5


def test_documents_older_than_their_validator_still_land(pair, tmp_path):
    """The source holds them, so the target must: the copy carries what is
    there, and `check` is what judges it."""
    s, t = pair
    s.drop_database("app")
    t.drop_database("app")
    s.app.orders.insert_many([{"_id": 1, "qty": "not a number"},
                              {"_id": 2, "qty": 5}])
    s.app.command({"collMod": "orders", "validator": {
        "qty": {"$type": "int"}}})
    _move(tmp_path)
    assert t.app.orders.count_documents({}) == 2


def test_a_load_that_reports_failures_is_not_called_copied(monkeypatch,
                                                           tmp_path):
    """Whatever the reason, a document the load counts as failed stops the
    move. The program itself exits 0 over it - measured with the validator
    case above, before the load was told to keep such documents."""
    import io

    from migkit import movers

    class Fake:
        def __init__(self, cmd, **kw):
            self.returncode = 0
            said = (b"finished restoring `app.orders` (3 documents,"
                    b" 2 failures)\n" if cmd[0] == "mongorestore" else b"")
            self.stdout = io.BytesIO(b"")
            self.stderr = io.BytesIO(said)

        def wait(self):
            return 0

        # finished, as a process that has exited says
        def poll(self):
            return self.returncode

        def communicate(self):
            return b"", b""

    monkeypatch.setattr(movers.subprocess, "Popen", Fake)
    hop = _hop()
    hop.report_dir = lambda db=None: tmp_path
    with pytest.raises(RuntimeError) as e:
        movers.mongodump_move(hop, "app", 2, True, lambda m: None)
    assert "app.orders 2 of 5" in str(e.value), e.value
