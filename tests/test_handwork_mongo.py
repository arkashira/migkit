"""The manual-work inventory, against a real MongoDB pair.

The MongoDB entries share one shape: an insert-by-insert copy does not fail,
it succeeds into the wrong kind of collection. A capped collection lands
uncapped and grows without a bound; a time-series collection lands as an
ordinary one holding the same documents. Nothing errors, so nothing surfaces
unless something counts these before the load.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-hwmg-src", "migkit-test-hwmg-dst"
SRC_PORT, DST_PORT = 15497, 15498


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


def _client(port):
    import pymongo
    return pymongo.MongoClient(f"mongodb://127.0.0.1:{port}",
                               serverSelectionTimeoutMS=8000)


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
    d = _client(SRC_PORT)["shop"]
    d.create_collection("cap", capped=True, size=8192)
    d.create_collection("ts", timeseries={"timeField": "t"})
    d.create_collection("coll8", collation={"locale": "en", "strength": 2})
    d["plain"].insert_one({"x": 1})
    d.command("create", "vw", viewOn="plain", pipeline=[])
    d["ttlcoll"].create_index([("t", 1)], expireAfterSeconds=60)
    # a seed that half-failed would let the assertions below pass for nothing
    assert {"cap", "ts", "coll8", "plain", "vw", "ttlcoll"} <= \
        set(d.list_collection_names())
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine(tmp_path, src_port=SRC_PORT):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="g", engine="mongodb",
              source=Endpoint(host="127.0.0.1", port=src_port),
              target=Endpoint(host="127.0.0.1", port=DST_PORT),
              databases=["shop"])
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


@pytest.fixture(scope="module")
def inv(pair, tmp_path_factory):
    return _engine(tmp_path_factory.mktemp("hwmg"))._handwork()


def _row(inv, kind):
    from migkit.handwork import KINDS
    hits = [r for r in inv.rows() if r["item"] == KINDS[kind][1]]
    assert hits, f"no row for {kind}: {[r['item'] for r in inv.rows()]}"
    return hits[0]


def test_a_capped_collection_must_be_created_before_the_load(inv):
    d = _row(inv, "target-prereq")["detail"]
    assert "cap (capped, size 8192)" in d, d


def test_a_time_series_collection_names_its_time_field(inv):
    """Only settable at creation, so a copy into a fresh target produces an
    ordinary collection holding the same documents and no error."""
    d = _row(inv, "target-prereq")["detail"]
    assert "ts (timeField t)" in d, d


def test_a_non_simple_collation_is_listed_with_its_locale(inv):
    d = _row(inv, "target-prereq")["detail"]
    assert "coll8 (en)" in d, d


def test_a_ttl_index_needs_a_decision_not_a_copy(inv):
    d = _row(inv, "decide-then-apply")["detail"]
    assert "ttlcoll.t_1" in d, d


def test_server_internal_collections_are_not_listed_as_work(inv):
    """`system.views` and `system.buckets.*` are the server's own
    bookkeeping; nobody can act on them, so listing them is noise."""
    text = " ".join(r["detail"] for r in inv.rows())
    assert "system." not in text, text


def test_views_are_left_to_the_schema_check(inv):
    """`_shape` records a collection's type, so a view that arrived as a
    collection is already reported there. Saying it twice would let the two
    copies drift."""
    text = " ".join(r["detail"] for r in inv.rows())
    assert "vw" not in text.split(), text


def test_a_standalone_reports_no_sharded_collections_rather_than_unknown(inv):
    """A standalone genuinely has none - that is a measured zero, and
    flattening it into unknown would cry wolf."""
    rows = [r for r in inv.rows() if "UNKNOWN" in r["detail"]]
    assert rows == [], [r["detail"] for r in rows]
    assert inv.unknowns() == 0


def test_an_unreachable_source_reports_unknown_not_zero(pair, tmp_path):
    inv2 = _engine(tmp_path, src_port=1)._handwork()
    assert inv2.unknowns() >= 1
    assert any("Not zero" in r["detail"] for r in inv2.rows())
    assert inv2.total() == 0


def test_assess_ends_with_the_inventory_and_no_estimate(pair, tmp_path):
    from migkit.handwork import KINDS
    items = _engine(tmp_path).assess()
    assert KINDS["target-prereq"][1] in {i["item"] for i in items}
    closing = [i for i in items if i["scope"] == "manual work"]
    assert closing and "does not estimate" in closing[0]["detail"]
