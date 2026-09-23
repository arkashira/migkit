"""The MongoDB bulk path leaves an excluded collection alone.

The restore drops each collection before loading it, and the path read no
exclude list: a collection the hop excluded - one the target owns - was
dropped and replaced by the source's collection of the same name. The
restore also ignored `db_map` and loaded into the source's database name.
"""
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

from migkit.config import Endpoint, Hop

SRC, DST = "migkit-test-mgx-src", "migkit-test-mgx-dst"
SRC_PORT, DST_PORT = 15655, 15656


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


def _hop(exclude=(), db_map=None):
    hop = Hop(name="m", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="",
                              password=""),
              databases=["app"], exclude=list(exclude),
              db_map=db_map or {"app": "app"})
    hop.report_dir = lambda db=None, _p=pathlib.Path(tempfile.mkdtemp()): _p
    return hop


def _seed(s, t, target_db="app"):
    for c in (s, t):
        for name in ("app", "app_new"):
            c.drop_database(name)
    s.app.orders.insert_many([{"_id": i} for i in (1, 2, 3)])
    s.app.audit.insert_many([{"_id": 1, "from": "source"}])
    t[target_db].audit.insert_many([{"_id": 7, "from": "target"}])


def test_an_excluded_collection_is_not_dropped_or_replaced(pair):
    from migkit import movers
    s, t = pair
    _seed(s, t)
    movers.mongodump_move(_hop(["audit"]), "app", 2, True, None)
    assert sorted(d["_id"] for d in t.app.orders.find()) == [1, 2, 3]
    assert list(t.app.audit.find({}, {"_id": 1, "from": 1})) == \
        [{"_id": 7, "from": "target"}]


def test_without_an_exclude_list_the_collection_is_copied(pair):
    """The same seed, no exclude: the source's audit replaces the target's,
    which is what the drop is for."""
    from migkit import movers
    s, t = pair
    _seed(s, t)
    movers.mongodump_move(_hop(), "app", 2, True, None)
    assert list(t.app.audit.find({}, {"_id": 1, "from": 1})) == \
        [{"_id": 1, "from": "source"}]


def test_the_restore_goes_to_the_targets_name_for_the_database(pair):
    from migkit import movers
    s, t = pair
    _seed(s, t, target_db="app_new")
    movers.mongodump_move(_hop(db_map={"app": "app_new"}), "app", 2, True,
                          None)
    assert sorted(d["_id"] for d in t.app_new.orders.find()) == [1, 2, 3]
    assert "app" not in t.list_database_names()
