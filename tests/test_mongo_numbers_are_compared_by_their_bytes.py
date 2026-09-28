"""A MongoDB collection whose documents differ only in a number is found
different, not reported ok.

`dbHash` saw the difference and the check then asked each document's
`$toHashedIndexKey` where, and that hasher turns every number into a
64-bit integer before hashing: 2.3 and 2.9 hash alike, and so do
NumberLong(5), 5 and 5.0. Measured on 7.0 before the fix: a collection
holding 2.3 on the source and 2.9 on the target, and another holding
NumberLong(5) against 5.0, were each reported `ok` - "docs 3 compared by id
hash" - while `dbHash` differed. When the id hash finds nothing the same
ranges are walked again by each document's stored bytes, and a difference
nothing localizes is said, never called ok. The delta check read documents
decoded and compared them in Python, where 5 == 5.0 and 1 == True and
field order does not count; it compares the bytes too.

One server holds both sides (`shop` and `shop_new`), a one-member replica
set so the delta check has a change stream.
"""
import socket
import subprocess
import time

import pytest

from tests.conftest import needs_docker

pytestmark = needs_docker

MG, PORT = "migkit-test-f0v-mongo", 16050


def _up():
    import pymongo
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"127.0.0.1:{PORT}:27017", "mongo:7", "--replSet", "rs0",
                    "--bind_ip_all"], check=True, capture_output=True)
    end = time.time() + 90
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", PORT)) == 0:
                break
        time.sleep(1)
    c = pymongo.MongoClient(port=PORT, directConnection=True,
                            serverSelectionTimeoutMS=60000)
    for _ in range(60):
        try:
            c.admin.command("replSetInitiate", {
                "_id": "rs0", "members": [{"_id": 0,
                                           "host": "127.0.0.1:27017"}]})
            break
        except Exception as e:
            if "already initialized" in str(e):
                break
            time.sleep(1)
    for _ in range(60):
        if c.admin.command("hello").get("isWritablePrimary"):
            return c
        time.sleep(1)
    pytest.fail("the replica set never elected a primary")


@pytest.fixture(scope="module")
def server():
    c = _up()
    try:
        yield c
    finally:
        c.close()
        subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)


@pytest.fixture
def eng(server, tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    for d in ("shop", "shop_new"):
        server.drop_database(d)
    ep = Endpoint(host="127.0.0.1", port=PORT,
                  options={"uri_options": "directConnection=true"})
    hop = Hop(name="f0v", engine="mongodb", source=ep, target=ep,
              databases=["shop"], db_map={"shop": "shop_new"})
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


def _seed(server, src_docs, dst_docs, name="t"):
    src, dst = server.shop[name], server.shop_new[name]
    src.insert_many(src_docs)
    dst.insert_many(dst_docs)
    # a seed that silently stopped short would pass for the wrong reason
    assert src.count_documents({}) == len(src_docs)
    assert dst.count_documents({}) == len(dst_docs)
    return src, dst


def _data(eng, name="t"):
    got = [r for r in eng.check_data("shop") if r.check == "data"
           and r.scope == f"shop.{name}"]
    assert len(got) == 1, [(r.scope, r.status) for r in got]
    return got[0]


def _premise(server, name, _id):
    """What the id hash cannot see: `dbHash` differs, and the document's
    `$toHashedIndexKey` is the same on both sides."""
    a = server.shop.command("dbHash", collections=[name])["collections"]
    b = server.shop_new.command("dbHash", collections=[name])["collections"]
    assert a[name] != b[name], "the premise failed: dbHash agrees"
    pipe = [{"$match": {"_id": _id}},
            {"$project": {"h": {"$toHashedIndexKey": "$$ROOT"}}}]
    ha = list(server.shop[name].aggregate(pipe))[0]["h"]
    hb = list(server.shop_new[name].aggregate(pipe))[0]["h"]
    assert ha == hb, "the premise failed: the id hash sees this one"


def test_a_fraction_that_changed_is_found(server, eng):
    rest = [{"_id": i, "price": 1.25 * i} for i in range(2, 6)]
    _seed(server, [{"_id": 1, "price": 2.3}] + rest,
          [{"_id": 1, "price": 2.9}] + [dict(d) for d in rest])
    _premise(server, "t", 1)
    r = _data(eng)
    assert r.status == "diff", r.detail
    assert "changed=1" in r.detail and "missing=0" in r.detail, r.detail
    assert (eng.hop.report_dir("shop") / "data-t.changed").read_text() \
        .split() == ["1"]


@pytest.mark.parametrize("was, now", [
    ("long", "double"), ("int", "long"), ("int", "decimal")])
def test_a_number_that_changed_its_type_is_found(server, eng, was, now):
    from bson import Decimal128, Int64
    make = {"int": lambda: 5, "long": lambda: Int64(5),
            "double": lambda: 5.0, "decimal": lambda: Decimal128("5")}
    _seed(server, [{"_id": 1, "n": {"deep": [make[was]()]}}],
          [{"_id": 1, "n": {"deep": [make[now]()]}}])
    _premise(server, "t", 1)
    r = _data(eng)
    assert r.status == "diff", r.detail
    assert "changed=1" in r.detail, r.detail


def test_equal_collections_of_every_number_type_are_ok(server, eng):
    from bson import Decimal128, Int64
    docs = [{"_id": 1, "a": 5, "b": Int64(5), "c": 5.0, "d": Decimal128("5"),
             "e": -0.0, "f": {"g": [2.3, True, None]}}]
    _seed(server, docs, [dict(d) for d in docs])
    r = _data(eng)
    assert r.status == "ok", r.detail


@pytest.fixture
def no_dbhash(monkeypatch):
    """A server that will not give `dbHash` - a mongos, or a user without
    the right to it."""
    from pymongo.database import Database
    from pymongo.errors import OperationFailure
    real = Database.command

    def command(self, command, *args, **kw):
        name = command if isinstance(command, str) else next(iter(command))
        if name == "dbHash":
            raise OperationFailure("not authorized on shop to execute"
                                   " command { dbHash: 1 }", 13)
        return real(self, command, *args, **kw)
    monkeypatch.setattr(Database, "command", command)


def test_without_dbhash_the_bytes_decide(server, eng, no_dbhash):
    _seed(server, [{"_id": 1, "price": 2.3}, {"_id": 2, "k": "same"}],
          [{"_id": 1, "price": 2.9}, {"_id": 2, "k": "same"}])
    r = _data(eng)
    assert r.status == "diff", r.detail
    assert "changed=1" in r.detail and "by their bytes" in r.detail, r.detail


def test_without_dbhash_equal_collections_are_ok(server, eng, no_dbhash):
    docs = [{"_id": i, "v": 0.5 * i} for i in range(20)]
    _seed(server, docs, [dict(d) for d in docs])
    r = _data(eng)
    assert r.status == "ok", r.detail
    assert "docs 20 compared by their bytes" in r.detail, r.detail


def test_a_field_order_that_changed_is_found(server, eng, no_dbhash):
    from bson.son import SON
    _seed(server, [SON([("_id", 1), ("a", 1), ("b", 2)])],
          [SON([("_id", 1), ("b", 2), ("a", 1)])])
    r = _data(eng)
    assert r.status == "diff", r.detail


def _lying_dbhash(monkeypatch, times):
    """`dbHash` of the target reports a different md5 for the first
    `times` asks - a difference no document holds, as a write between the
    reads leaves."""
    from pymongo.database import Database
    real = Database.command
    asked = {"n": 0}

    def command(self, command, *args, **kw):
        got = real(self, command, *args, **kw)
        name = command if isinstance(command, str) else next(iter(command))
        if name == "dbHash" and self.name == "shop_new":
            asked["n"] += 1
            if asked["n"] <= times:
                got["collections"] = {k: "0" * 32
                                      for k in got["collections"]}
        return got
    monkeypatch.setattr(Database, "command", command)
    return asked


def test_a_difference_nothing_localizes_is_not_ok(server, eng, monkeypatch):
    docs = [{"_id": i, "v": i} for i in range(10)]
    _seed(server, docs, [dict(d) for d in docs])
    asked = _lying_dbhash(monkeypatch, times=10 ** 6)
    r = _data(eng)
    assert asked["n"] >= 2, "dbHash was not asked again"
    assert r.status == "diff", r.detail
    assert r.detail.startswith("differs, not localized"), r.detail


def test_a_difference_that_settles_is_ok_and_says_so(server, eng,
                                                     monkeypatch):
    docs = [{"_id": i, "v": i} for i in range(10)]
    _seed(server, docs, [dict(d) for d in docs])
    _lying_dbhash(monkeypatch, times=1)
    r = _data(eng)
    assert r.status == "ok", r.detail
    assert "equal asked again" in r.detail, r.detail


def test_the_delta_check_compares_the_bytes(server, eng):
    from bson import Int64
    from bson.son import SON
    _seed(server, [{"_id": 1, "n": Int64(5)}, {"_id": 2, "flag": 1},
                   SON([("_id", 3), ("a", 1), ("b", 2)])],
          [{"_id": 1, "n": Int64(5)}, {"_id": 2, "flag": 1},
           SON([("_id", 3), ("a", 1), ("b", 2)])])
    first = eng.delta_verify("shop")
    assert first[0].status == "ok" and "recorded" in first[0].detail, first
    src = server.shop.t
    src.update_one({"_id": 1}, {"$set": {"n": 5.0}})
    src.update_one({"_id": 2}, {"$set": {"flag": True}})
    src.replace_one({"_id": 3}, SON([("_id", 3), ("b", 2), ("a", 1)]))
    time.sleep(1)
    got = eng.delta_verify("shop")
    assert got[0].status == "diff", [(r.scope, r.detail) for r in got]
    per = [r for r in got if r.scope == "shop.t"]
    assert per and "3 differ" in per[0].detail, per[0].detail
