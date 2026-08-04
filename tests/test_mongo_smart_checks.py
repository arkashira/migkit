"""Smart checks for the mongo engine. BSON type fidelity: a migration through
mongoexport/mongoimport or a careless mover silently changes a field's type -
Decimal128 becomes double (precision loss), Int64 becomes Int32 (overflow),
ObjectId becomes string (_id breaks). check_deep compares the dominant BSON type
per field per side and flags drift; a row/dbHash compare would miss it because
the field is still 'present'."""
import socket
import subprocess
import time

import pytest


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-test-mongo-src", "migkit-test-mongo-dst"
SRC_PORT, DST_PORT = 47017, 47018


def _wait(port, timeout=90):
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
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-p",
                        f"{p}:27017", "mongo:7"], check=True,
                       capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    import pymongo
    for p in (SRC_PORT, DST_PORT):
        for _ in range(30):
            try:
                pymongo.MongoClient(f"mongodb://127.0.0.1:{p}/",
                                    serverSelectionTimeoutMS=1000).admin.command("ping")
                break
            except Exception:
                time.sleep(1)
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _engine():
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="m", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT),
              target=Endpoint(host="127.0.0.1", port=DST_PORT),
              databases=["shop"])
    return MongoEngine(hop)


def test_mongo_bson_type_drift_flagged(pair):
    import pymongo
    from bson.decimal128 import Decimal128
    src = pymongo.MongoClient(f"mongodb://127.0.0.1:{SRC_PORT}/")["shop"]
    dst = pymongo.MongoClient(f"mongodb://127.0.0.1:{DST_PORT}/")["shop"]
    src.orders.delete_many({})
    dst.orders.delete_many({})
    # source stores price as Decimal128; target flattened it to double
    src.orders.insert_many([{"_id": i, "price": Decimal128(f"{i}.50")}
                            for i in range(1, 6)])
    dst.orders.insert_many([{"_id": i, "price": float(i) + 0.5}
                            for i in range(1, 6)])

    deep = _engine().check_deep("shop")
    bt = [r for r in deep if r.scope.endswith("bson-types")]
    assert bt and bt[0].status == "diff", [r.__dict__ for r in deep]
    assert "orders.price" in bt[0].detail, bt[0].detail
    assert "decimal" in bt[0].detail and "double" in bt[0].detail, bt[0].detail


def test_mongo_null_vs_missing_flagged(pair):
    import pymongo
    src = pymongo.MongoClient(f"mongodb://127.0.0.1:{SRC_PORT}/")["nmtest"]
    dst = pymongo.MongoClient(f"mongodb://127.0.0.1:{DST_PORT}/")["nmtest"]
    src.orders.delete_many({})
    dst.orders.delete_many({})
    # source: field present but explicitly null; target: field absent
    src.orders.insert_many([{"_id": i, "note": None} for i in range(1, 4)])
    dst.orders.insert_many([{"_id": i} for i in range(1, 4)])

    deep = _engine().check_deep("nmtest")
    nm = [r for r in deep if r.scope.endswith("null-missing")]
    assert nm and nm[0].status == "diff", [r.__dict__ for r in deep]
    assert "orders.note" in nm[0].detail, nm[0].detail


def test_mongo_capped_size_drift_flagged(pair):
    import pymongo
    src = pymongo.MongoClient(f"mongodb://127.0.0.1:{SRC_PORT}/")["captest"]
    dst = pymongo.MongoClient(f"mongodb://127.0.0.1:{DST_PORT}/")["captest"]
    src.drop_collection("logs")
    dst.drop_collection("logs")
    # target capped smaller than source -> old docs silently roll off the head
    src.create_collection("logs", capped=True, size=100000)
    dst.create_collection("logs", capped=True, size=20000)

    deep = _engine().check_deep("captest")
    cap = [r for r in deep if r.scope.endswith("capped")]
    assert cap and cap[0].status == "diff", [r.__dict__ for r in deep]
    assert "logs" in cap[0].detail and "100000" in cap[0].detail, cap[0].detail


def test_mongo_unique_index_collation_collapse_flagged(pair):
    import pymongo
    src = pymongo.MongoClient(f"mongodb://127.0.0.1:{SRC_PORT}/")["cidb"]
    dst = pymongo.MongoClient(f"mongodb://127.0.0.1:{DST_PORT}/")["cidb"]
    src.drop_collection("users")
    dst.drop_collection("users")
    # source: case-sensitive unique holding 'Foo@x' and 'foo@x' (distinct)
    src.users.insert_many([{"email": "Foo@x"}, {"email": "foo@x"}])
    src.users.create_index("email", unique=True)
    # target: same unique key but on a case-insensitive collation -> collapse
    dst.users.create_index("email", unique=True,
                           collation={"locale": "en", "strength": 2})

    deep = _engine().check_deep("cidb")
    ix = [r for r in deep if r.scope.endswith("indexes")]
    assert ix and ix[0].status == "diff", [r.__dict__ for r in deep]
    assert "collapse" in ix[0].detail.lower(), ix[0].detail
    assert "users.email" in ix[0].detail, ix[0].detail


def test_mongo_ttl_index_drift_flagged(pair):
    import pymongo
    src = pymongo.MongoClient(f"mongodb://127.0.0.1:{SRC_PORT}/")["ttldb"]
    dst = pymongo.MongoClient(f"mongodb://127.0.0.1:{DST_PORT}/")["ttldb"]
    src.drop_collection("events")
    dst.drop_collection("events")
    src.events.create_index("createdAt")
    # target added a TTL that the source does not have -> deletes data
    dst.events.create_index("createdAt", expireAfterSeconds=3600)

    deep = _engine().check_deep("ttldb")
    ix = [r for r in deep if r.scope.endswith("indexes")]
    assert ix and ix[0].status == "diff", [r.__dict__ for r in deep]
    assert "ttl" in ix[0].detail.lower(), ix[0].detail
    assert "createdAt" in ix[0].detail, ix[0].detail


def test_mongo_sharding_check_skips_cleanly_on_standalone(pair):
    # the sharding check must detect a non-sharded topology and skip cleanly
    # (report ok), never error or emit a false positive
    deep = _engine().check_deep("shop")
    sh = [r for r in deep if r.scope.endswith("sharding")]
    assert sh and sh[0].status == "ok", [r.__dict__ for r in deep]
    assert "not a sharded cluster" in sh[0].detail.lower(), sh[0].detail
