"""Comparing a collection by id, one `_id` range at a time.

This replaced a version that built a dict of every id and hash for both sides
at once, and therefore refused to run past five million documents and told the
operator to go and use a different tool. Ranges make the memory cost
proportional to the range, and make the comparison restartable.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-mongorange-src", "migkit-test-mongorange-dst"
SRC_PORT, DST_PORT = 15487, 15488


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
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-p",
                        f"{p}:27017", "mongo:7"], check=True,
                       capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    time.sleep(3)
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _client(port):
    import pymongo
    return pymongo.MongoClient(f"mongodb://127.0.0.1:{port}",
                               serverSelectionTimeoutMS=8000)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="g", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT),
              target=Endpoint(host="127.0.0.1", port=DST_PORT),
              databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


def _seed(n=600, mixed=True):
    """Mixed `_id` types on purpose: an ObjectId collection is the easy case,
    and the arithmetic split the SQL engines use cannot handle this at all."""
    from bson import ObjectId
    src = _client(SRC_PORT)["shop"]
    dst = _client(DST_PORT)["shop"]
    src.drop_collection("t")
    dst.drop_collection("t")
    docs = [{"_id": ObjectId(), "v": i} for i in range(n)]
    if mixed:
        docs += [{"_id": i, "v": i} for i in range(40)]
        docs += [{"_id": f"s{i:03d}", "v": i} for i in range(40)]
    src.t.insert_many(docs)
    dst.t.insert_many([dict(d) for d in docs])
    total = len(docs)
    assert src.t.count_documents({}) == total
    assert dst.t.count_documents({}) == total
    return src, dst, total


def test_identical_collections_pass_over_several_ranges(pair, tmp_path,
                                                        monkeypatch):
    from migkit.engines import mongodb
    _, _, total = _seed()
    monkeypatch.setattr(mongodb, "DRILL_RANGE_DOCS", 100)

    r = _engine(tmp_path)._drilldown("shop", "t")
    assert r.status == "ok", r.detail
    assert f"docs {total:,}" in r.detail, r.detail
    # it really did split, rather than quietly doing one pass
    ranges = int(r.detail.split(", ")[1].split()[0])
    assert ranges > 1, r.detail


def test_a_difference_is_found_in_whichever_range_holds_it(pair, tmp_path,
                                                           monkeypatch):
    from migkit.engines import mongodb
    src, dst, total = _seed()
    monkeypatch.setattr(mongodb, "DRILL_RANGE_DOCS", 100)
    # one changed, one missing, one extra - spread across the keyspace
    victim = dst.t.find_one({"v": 5})
    dst.t.update_one({"_id": victim["_id"]}, {"$set": {"v": -1}})
    dst.t.delete_one({"_id": 7})
    dst.t.insert_one({"_id": "zzz-extra", "v": 0})

    r = _engine(tmp_path)._drilldown("shop", "t")
    assert r.status == "diff", r.detail
    assert "changed=1" in r.detail, r.detail
    assert "missing=1" in r.detail, r.detail
    assert "extra=1" in r.detail, r.detail


def test_a_clean_run_leaves_no_partials_owed(pair, tmp_path, monkeypatch):
    import json
    from migkit.engines import mongodb
    _seed()
    monkeypatch.setattr(mongodb, "DRILL_RANGE_DOCS", 100)
    r = _engine(tmp_path)._drilldown("shop", "t")
    assert r.status == "ok", r.detail
    left = json.loads((tmp_path / "checkpoint.json").read_text())
    assert not (left["tables"].get("shop.t") or {}).get("done")


def test_a_collection_smaller_than_one_range_still_works(pair, tmp_path):
    """No boundaries means one whole range, which must behave normally."""
    src = _client(SRC_PORT)["shop"]
    dst = _client(DST_PORT)["shop"]
    for c in (src, dst):
        c.drop_collection("small")
        c.small.insert_many([{"_id": i, "v": i} for i in range(5)])
        assert c.small.count_documents({}) == 5

    r = _engine(tmp_path)._drilldown("shop", "small")
    assert r.status == "ok", r.detail
    assert "1 ranges" in r.detail, r.detail


def test_every_document_is_inside_some_range_even_with_mixed_id_types(
        pair, tmp_path, monkeypatch):
    """The bug this guards against.

    MongoDB sorts across BSON types but its query comparison operators are
    type-bracketed, so one ordered list of boundaries taken across a
    mixed-type `_id` leaves whole types outside every range. The first version
    of this code compared 600 of 680 documents and reported them identical.
    """
    from migkit.engines import mongodb
    src, _, total = _seed()
    monkeypatch.setattr(mongodb, "DRILL_RANGE_DOCS", 100)
    eng = _engine(tmp_path)

    plan = eng._id_plan(src.t, total)
    assert len(plan) > 1, plan
    covered = sum(src.t.count_documents(eng._range_filter(ty, lo, hi))
                  for ty, lo, hi in plan)
    assert covered == total, f"{covered} of {total} documents are in a range"
    assert eng._coverage_ok(src.t, plan, total)


def test_a_plan_that_misses_documents_is_rejected(pair, tmp_path):
    """The guard, not just the fix: a plan is only used if it accounts for
    every document, so a future partitioning mistake degrades to one pass
    instead of silently skipping data."""
    from bson import ObjectId
    src, _, total = _seed()
    eng = _engine(tmp_path)
    # a deliberately broken plan: one ObjectId-typed range only
    broken = [("objectId", None, ObjectId("0" * 24))]
    assert eng._coverage_ok(src.t, broken, total) is False


def test_a_crash_partway_resumes_and_still_counts_everything(
        pair, tmp_path, monkeypatch):
    """Crash it for real rather than hand-writing a checkpoint.

    A test that fabricates the checkpoint proves the reader works; making the
    run die mid-collection proves the writer and the reader agree, which is
    the part that breaks.
    """
    from migkit.engines import mongodb
    from migkit.engines.mongodb import MongoEngine
    _, _, total = _seed()
    monkeypatch.setattr(mongodb, "DRILL_RANGE_DOCS", 100)
    eng = _engine(tmp_path)
    plan = eng._id_plan(eng._client("src")["shop"]["t"], total)
    assert len(plan) > 3, plan

    real = MongoEngine._range_hashes
    calls = {"n": 0}

    def flaky(self, coll, flt):
        calls["n"] += 1
        # two ranges is two calls per range (source and target)
        if calls["n"] > 4:
            raise RuntimeError("connection reset")
        return real(self, coll, flt)

    monkeypatch.setattr(MongoEngine, "_range_hashes", flaky)
    with pytest.raises(RuntimeError):
        eng._drilldown("shop", "t")

    # what the dead process left behind
    import json
    left = json.loads((tmp_path / "checkpoint.json").read_text())
    done = (left["tables"].get("shop.t") or {}).get("done") or {}
    assert done, "a crash left nothing to resume from"
    partial = len(done)

    monkeypatch.setattr(MongoEngine, "_range_hashes", real)
    r = _engine(tmp_path)._drilldown("shop", "t")
    assert r.status == "ok", r.detail
    assert f"resumed {partial}/{len(plan)}" in r.detail, r.detail
    # and the resumed total still accounts for every document
    assert f"docs {total:,}" in r.detail, r.detail
