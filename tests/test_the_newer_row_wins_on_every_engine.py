"""Newer-row-wins on the engines that lacked it (backlog 10.more), and the
statement-level record of their repairs (C3.repair).

`newer_wins: <column>` kept the target's row where its column was later
there - on PostgreSQL and MySQL only. Every engine with a key-addressed
target keeps it now: every pair through the one repair all of them share
(`test_a_repair_is_recorded_statement_by_statement.py`), SQL Server whose
row repair was a note to do it by hand, MongoDB by a field of the
document, Redis by a field of a hash. A key with no such field on either
side is overwritten: the source is the truth where the values cannot say
otherwise.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit import repairlog
from migkit.config import Endpoint, Hop
from tests.conftest import _have_docker

pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _have_docker(),
                                 reason="docker not available")]

MONGO, MONGO_PORT = "migkit-test-vd-mongo", 16274
RD_SRC, RD_DST = ("migkit-test-vd-rd-src", 16275), ("migkit-test-vd-rd-dst",
                                                    16279)


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
def mongo():
    subprocess.run(["docker", "rm", "-f", "-v", MONGO], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MONGO, "-p",
                        f"127.0.0.1:{MONGO_PORT}:27017", "mongo:7"],
                       check=True, capture_output=True)
        assert _wait(MONGO_PORT)
        import pymongo
        client = pymongo.MongoClient("127.0.0.1", MONGO_PORT,
                                     serverSelectionTimeoutMS=60000)
        client.admin.command("ping")
        yield client
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MONGO],
                       capture_output=True)


def _mongo_engine(tmp_path, **opts):
    from migkit.engines.mongodb import MongoEngine
    ep = Endpoint(host="127.0.0.1", port=MONGO_PORT, user="", password="")
    hop = Hop(name="m", engine="mongodb", source=ep, target=ep,
              databases=["a"], db_map={"a": "b"}, options=opts)
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


def _mongo_seed(client, tmp_path):
    from bson.json_util import dumps
    for name in ("a", "b"):
        client.drop_database(name)
    client.a.docs.insert_many([{"_id": 1, "v": "one", "at": 10},
                               {"_id": 2, "v": "two", "at": 10},
                               {"_id": 3, "v": "three", "at": 10}])
    client.b.docs.insert_many([{"_id": 1, "v": "OLD", "at": 5},
                               {"_id": 2, "v": "NEWER", "at": 99},
                               {"_id": 4, "v": "stray", "at": 1}])
    (tmp_path / "data-docs.changed").write_text(
        dumps(1) + "\n" + dumps(2) + "\n")
    (tmp_path / "data-docs.missing").write_text(dumps(3) + "\n")
    (tmp_path / "data-docs.extra").write_text(dumps(4) + "\n")


def test_mongodb_keeps_the_newer_document_and_records_the_rest(
        mongo, tmp_path):
    _mongo_seed(mongo, tmp_path)
    eng = _mongo_engine(tmp_path, newer_wins="at")
    for action in eng.repair_plan("a", "rows"):
        eng.apply("a", action)
    got = {d["_id"]: d["v"] for d in mongo.b.docs.find()}
    assert got == {1: "one", 2: "NEWER", 3: "three"}, got
    assert (tmp_path / "data-docs.kept-newer").read_text().strip() == "2"
    [path] = repairlog.records(tmp_path / "undo")
    head, stmts, upto = repairlog.read(path)
    assert head["form"] == "document", head
    assert len(stmts) == upto == 3, (stmts, upto)
    # put back where the target still holds what the repair wrote
    done, _, left = repairlog.reverse(path, eng.repair_io(
        "a", head))
    assert done == 3 and not left, (done, left)
    got = {d["_id"]: (d["v"], d["at"]) for d in mongo.b.docs.find()}
    assert got == {1: ("OLD", 5), 2: ("NEWER", 99), 4: ("stray", 1)}, got


def test_mongodb_without_the_field_on_either_side_overwrites(mongo,
                                                            tmp_path):
    _mongo_seed(mongo, tmp_path)
    eng = _mongo_engine(tmp_path, newer_wins="updated")
    for action in eng.repair_plan("a", "rows"):
        eng.apply("a", action)
    got = {d["_id"]: d["v"] for d in mongo.b.docs.find()}
    assert got == {1: "one", 2: "two", 3: "three"}, got


@pytest.fixture(scope="module")
def redis_pair():
    for name, _ in (RD_SRC, RD_DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    try:
        for name, port in (RD_SRC, RD_DST):
            subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                            f"127.0.0.1:{port}:6379", "redis:7"],
                           check=True, capture_output=True)
        for _, port in (RD_SRC, RD_DST):
            assert _wait(port)
        import redis
        yield (redis.Redis(host="127.0.0.1", port=RD_SRC[1],
                           decode_responses=True),
               redis.Redis(host="127.0.0.1", port=RD_DST[1],
                           decode_responses=True))
    finally:
        for name, _ in (RD_SRC, RD_DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _redis_engine(tmp_path, **opts):
    from migkit.engines.redis import RedisEngine
    hop = Hop(name="r", engine="redis",
              source=Endpoint(host="127.0.0.1", port=RD_SRC[1], user="",
                              password=""),
              target=Endpoint(host="127.0.0.1", port=RD_DST[1], user="",
                              password=""),
              db_map={"0": "0"}, options=opts)
    hop.report_dir = lambda db=None: tmp_path
    return RedisEngine(hop)


def test_redis_keeps_the_newer_hash_and_overwrites_the_rest(redis_pair,
                                                            tmp_path):
    src, dst = redis_pair
    for c in (src, dst):
        c.flushall()
    src.hset("user:1", mapping={"name": "ann", "at": "100"})
    src.hset("user:2", mapping={"name": "bob", "at": "100"})
    src.set("plain", "source")
    dst.hset("user:1", mapping={"name": "OLD", "at": "50"})
    dst.hset("user:2", mapping={"name": "NEWER", "at": "900"})
    dst.set("plain", "target")
    (tmp_path / "data-db0.changed").write_text(
        "".join(json.dumps(k) + "\n" for k in ("user:1", "user:2",
                                               "plain")))
    eng = _redis_engine(tmp_path, newer_wins="at")
    for action in eng.repair_plan("0", "rows"):
        eng.apply("0", action)
    assert dst.hget("user:1", "name") == "ann"
    assert dst.hget("user:2", "name") == "NEWER"
    # a string has no field to tell by: the source's
    assert dst.get("plain") == "source"
    assert (tmp_path / "data-db0.kept-newer").read_text().strip() \
        == "user:2"
    [path] = repairlog.records(tmp_path / "undo")
    head, stmts, upto = repairlog.read(path)
    assert head["form"] == "dump" and len(stmts) == upto == 2, (head, stmts)
    done, _, left = repairlog.reverse(path, eng.repair_io("0", head))
    assert done == 2 and not left, (done, left)
    assert dst.hget("user:1", "name") == "OLD" and dst.get("plain") \
        == "target"
