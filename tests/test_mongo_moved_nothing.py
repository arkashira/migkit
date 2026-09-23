"""MongoDB notices a move that moved nothing.

The SQL engines have had this guard since a bulk copy was measured
finishing without an error and leaving the target empty. MongoDB had none,
so `move` printed its success line over an empty target and could only
add that the engine "cannot confirm the rows landed".
"""
import pathlib
import socket
import subprocess
import tempfile
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-mgn-src", "migkit-test-mgn-dst"
SRC_PORT, DST_PORT = 15659, 15660


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


def _engine(exclude=(), db_map=None, dst_port=DST_PORT):
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="m", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=dst_port, user="",
                              password=""),
              databases=["app"], exclude=list(exclude),
              db_map=db_map or {"app": "app"})
    hop.report_dir = lambda db=None, _p=pathlib.Path(tempfile.mkdtemp()): _p
    return MongoEngine(hop)


@pytest.fixture
def seeded(pair):
    s, t = pair
    for c in (s, t):
        for name in ("app", "app_new"):
            c.drop_database(name)
    s.app.orders.insert_many([{"_id": i} for i in (1, 2)])
    s.app.people.insert_many([{"_id": 1}])
    s.app.audit.insert_many([{"_id": 1}])
    s.app.create_collection("unused")
    t.app.orders.insert_many([{"_id": 1}])
    t.app.create_collection("people")
    return s, t


def test_a_collection_that_arrived_empty_or_not_at_all_is_named(seeded):
    got = _engine().moved_nothing("app")
    # people is there and empty; audit never arrived; orders has a row;
    # unused is empty on the source too
    assert got == ["audit", "people"], got


def test_an_excluded_collection_is_not_reported(seeded):
    assert _engine(["audit"]).moved_nothing("app") == ["people"]


def test_it_looks_in_the_targets_name_for_the_database(seeded):
    s, t = seeded
    for c in ("orders", "people", "audit"):
        t.app_new[c].insert_one({"_id": 1})
    assert _engine(db_map={"app": "app_new"}).moved_nothing("app") == []


def test_a_target_it_cannot_reach_is_not_a_clean_bill(seeded):
    """None, not an empty list: the caller says it could not confirm."""
    assert _engine(dst_port=1).moved_nothing("app") is None
