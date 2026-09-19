"""One vocabulary for the shape of a difference, including MongoDB.

MongoDB reaches the answer differently from the SQL engines: it compares
`_id` sets, so it *knows* how many documents are missing, extra and changed
rather than inferring the shape from two hashes. The words it reports have to
be the same words anyway - a report that says `rows-missing` for MySQL and
`docs-absent` for MongoDB cannot be aggregated, and a reader would have to
learn two vocabularies to read one run.

These assertions are deliberately the same strings asserted in
`test_diff_kind_pg.py` and `test_diff_kind_mysql.py`.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-mongokind-src", "migkit-test-mongokind-dst"
SRC_PORT, DST_PORT = 15493, 15494


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


ROWS = 40


def _fresh():
    src, dst = _client(SRC_PORT)["shop"], _client(DST_PORT)["shop"]
    for d in (src, dst):
        d.drop_collection("t")
        d["t"].insert_many([{"_id": i, "payload": f"v{i}"}
                            for i in range(1, ROWS + 1)])
        # a silently empty seed would let every assertion below pass for the
        # wrong reason
        assert d["t"].count_documents({}) == ROWS
    return src, dst


def _detail(tmp_path):
    r = _engine(tmp_path)._drilldown("shop", "t")
    return r.status, r.detail


def test_identical_collections_claim_no_kind(pair, tmp_path):
    _fresh()
    status, detail = _detail(tmp_path)
    assert status == "ok", detail
    assert "kind=" not in detail, detail


def test_an_edited_document_is_named_values_changed(pair, tmp_path):
    _, dst = _fresh()
    dst["t"].update_one({"_id": 3}, {"$set": {"payload": "tampered"}})
    status, detail = _detail(tmp_path)
    assert status == "diff"
    assert "kind=values-changed" in detail, detail


def test_a_deleted_document_is_named_rows_missing_with_a_count(pair, tmp_path):
    _, dst = _fresh()
    dst["t"].delete_many({"_id": {"$in": [4, 5]}})
    status, detail = _detail(tmp_path)
    assert status == "diff"
    assert "kind=rows-missing by=2" in detail, detail


def test_an_added_document_is_named_rows_extra(pair, tmp_path):
    _, dst = _fresh()
    dst["t"].insert_one({"_id": 9999, "payload": "ghost"})
    status, detail = _detail(tmp_path)
    assert status == "diff"
    assert "kind=rows-extra by=1" in detail, detail


def test_a_swapped_id_is_named_rows_replaced(pair, tmp_path):
    """The case a document count cannot see at all: same number of documents,
    different ids."""
    _, dst = _fresh()
    dst["t"].delete_one({"_id": 6})
    dst["t"].insert_one({"_id": 600, "payload": "v6"})
    status, detail = _detail(tmp_path)
    assert status == "diff"
    assert "kind=rows-replaced" in detail, detail


def test_unequal_losses_and_gains_report_both_sides(pair, tmp_path):
    """Two gone and five arrived is not a replacement, and saying so would
    hide half of what happened."""
    _, dst = _fresh()
    dst["t"].delete_many({"_id": {"$in": [7, 8]}})
    dst["t"].insert_many([{"_id": 700 + i, "payload": "x"} for i in range(5)])
    status, detail = _detail(tmp_path)
    assert status == "diff"
    assert "kind=rows-missing by=2,rows-extra by=5" in detail, detail
