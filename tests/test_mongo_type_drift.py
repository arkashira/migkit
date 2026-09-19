"""Numeric type drift: what dbHash sees, verified rather than assumed.

An earlier attempt at this concluded that `dbHash` was blind to
int32 -> double. That was wrong, and the mistake is worth recording: the
experiment was run in mongosh, where JavaScript has a single number type and
an integral literal like `7.0` is sent as **int32**. So it compared int32
with int32 and never tested a double at all.

Driven from pymongo, with the stored types confirmed on the server
(`{$type: ...}` reports `int` and `double`), `dbHash` does distinguish them.
Both layers therefore cover this class: the data check sees it in the hash,
and the deep check names the field and the direction. These tests hold both
of those true.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-mongotype-src", "migkit-test-mongotype-dst"
SRC_PORT, DST_PORT = 15492, 15493


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


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-p",
                        f"{p}:27017", "mongo:7"], check=True,
                       capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    time.sleep(3)
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine():
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="g", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT),
              target=Endpoint(host="127.0.0.1", port=DST_PORT),
              databases=["shop"])
    return MongoEngine(hop)


def _client(port):
    import pymongo
    return pymongo.MongoClient(f"mongodb://127.0.0.1:{port}",
                               serverSelectionTimeoutMS=8000)


def _seed_type_drift(n=40):
    """int32 on the source, double on the target - the blind pair.

    A Python int inside 32 bits encodes as BSON int32, and a Python float
    encodes as BSON double, so this is the exact shape a JSON-mediated
    migration produces.
    """
    src = _client(SRC_PORT)["shop"]
    dst = _client(DST_PORT)["shop"]
    src.drop_collection("t")
    dst.drop_collection("t")
    src.t.insert_many([{"_id": i, "qty": i} for i in range(n)])
    dst.t.insert_many([{"_id": i, "qty": float(i)} for i in range(n)])
    assert src.t.count_documents({}) == n
    assert dst.t.count_documents({}) == n
    return src, dst


def test_the_seed_really_is_int32_against_double(pair):
    """Ask the server, because the client library and the shell each have
    their own opinion about what an integral literal means."""
    src, dst = _seed_type_drift()
    def kind(coll):
        return list(coll.t.aggregate(
            [{"$project": {"k": {"$type": "$qty"}}}, {"$limit": 1}]))[0]["k"]
    assert kind(src) == "int"
    assert kind(dst) == "double"


def test_dbhash_does_distinguish_int32_from_double(pair):
    src, dst = _seed_type_drift()
    a = src.command("dbHash")["collections"]["t"]
    b = dst.command("dbHash")["collections"]["t"]
    assert a != b, ("dbHash stopped distinguishing int32 from double - the"
                    " data check would then be blind to this class and the"
                    " deep field-type check becomes the only cover")


def test_the_deep_check_also_names_the_field_and_direction(pair):
    _seed_type_drift()
    res = _engine().check_deep("shop")
    hits = [r for r in res if r.scope.endswith("bson-types")]
    assert hits, [r.scope for r in res]
    r = hits[0]
    assert r.status == "diff", r.detail
    assert r.category == "value.type-drift"
    assert "qty" in r.detail, r.detail


def test_matching_types_are_not_flagged(pair):
    src = _client(SRC_PORT)["shop"]
    dst = _client(DST_PORT)["shop"]
    for c in (src, dst):
        c.drop_collection("t")
        c.t.insert_many([{"_id": i, "qty": i} for i in range(25)])
        assert c.t.count_documents({}) == 25

    hits = [r for r in _engine().check_deep("shop")
            if r.scope.endswith("bson-types")]
    assert hits and hits[0].status == "ok", hits[0].detail if hits else "none"
