"""Mover leftovers in a real MongoDB source.

Same question as the SQL engines ask, in the one place nothing on the target
can answer it: what is in this source that the application did not put there?

Nothing MongoDB leaves behind is urgent the way a PostgreSQL replication slot
is - there is no storage being pinned - so every finding here is a warn, and
the check leans hard towards saying too little. The source belongs to the
application.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-leftover-mg"
PORT = 15457


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
        time.sleep(2)
    return False


def _client():
    import pymongo
    return pymongo.MongoClient(f"mongodb://127.0.0.1:{PORT}",
                               serverSelectionTimeoutMS=8000)


@pytest.fixture(scope="module")
def mongo():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"{PORT}:27017", "mongo:7"], check=True,
                   capture_output=True)
    assert _wait(PORT)
    time.sleep(3)
    yield
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="l", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=PORT),
              target=Endpoint(host="127.0.0.1", port=PORT),
              databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


def _clean():
    c = _client()
    for db in c.list_database_names():
        if db not in ("admin", "local", "config"):
            c.drop_database(db)
    c["shop"]["orders"].insert_one({"x": 1})
    assert "shop" in c.list_database_names()


def _rows(tmp_path):
    return _engine(tmp_path)._mover_leftovers()


def test_a_clean_source_says_so_with_a_real_count(mongo, tmp_path):
    _clean()
    rows = _rows(tmp_path)
    assert len(rows) == 1, rows
    assert rows[0]["level"] == "pass"
    # one database plus one collection is the floor for the seed above, so a
    # "clean" verdict here cannot mean "we looked at nothing"
    n = int(rows[0]["detail"].split()[0])
    assert n >= 2, rows[0]["detail"]


def test_a_leftover_collection_is_found_and_the_mover_named(mongo, tmp_path):
    _clean()
    c = _client()
    c["shop"]["awsdms_apply_exceptions"].insert_one({"x": 1})
    try:
        rows = _rows(tmp_path)
        warn = [r for r in rows if r["level"] == "warn"]
        assert warn, rows
        assert "AWS DMS" in warn[0]["detail"]
        assert "awsdms_apply_exceptions" in warn[0]["detail"]
    finally:
        c["shop"].drop_collection("awsdms_apply_exceptions")


def test_a_leftover_database_is_found(mongo, tmp_path):
    _clean()
    c = _client()
    c["__tencentdb__"]["meta"].insert_one({"x": 1})
    try:
        rows = _rows(tmp_path)
        warn = [r for r in rows if r["level"] == "warn"]
        assert warn, rows
        assert "database __tencentdb__" in warn[0]["detail"]
    finally:
        c.drop_database("__tencentdb__")


def test_server_bookkeeping_is_never_reported(mongo, tmp_path):
    """`system.*` belongs to MongoDB, not to a mover, and nobody can act on
    it. Listing it would be noise in the one check that cannot afford any."""
    _clean()
    c = _client()
    c["shop"].create_collection("ts", timeseries={"timeField": "t"})
    try:
        # the server really did make a system collection to hide
        assert any(n.startswith("system.")
                   for n in c["shop"].list_collection_names())
        rows = _rows(tmp_path)
        assert all(r["level"] == "pass" for r in rows), rows
    finally:
        c["shop"].drop_collection("ts")


def test_an_application_collection_is_not_called_litter(mongo, tmp_path):
    _clean()
    c = _client()
    c["shop"]["orders_new"].insert_one({"x": 1})
    try:
        rows = _rows(tmp_path)
        assert all(r["level"] == "pass" for r in rows), rows
    finally:
        c["shop"].drop_collection("orders_new")


def test_an_unreachable_source_is_unknown_not_clean(mongo, tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="l", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=1),
              target=Endpoint(host="127.0.0.1", port=PORT),
              databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    rows = MongoEngine(hop)._mover_leftovers()
    assert rows and rows[0]["level"] == "warn", rows
    assert "unknown, not clean" in rows[0]["detail"]
