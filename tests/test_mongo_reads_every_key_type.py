"""A chunked MongoDB read reaches every document, whatever its key's type.

MongoDB's `$gt` in a query compares within one type only. The chunked read
resumed from the last key with it, so a read that ended a chunk on a number
never reached a string or an ObjectId. Measured: seven documents keyed 1,
2, 2.5, "a", "b" and two ObjectIds, read two at a time, came back as the
three numbers, and the copier and the row walk built on it were three of
seven without a word. It resumes in the order the sort uses now, across
types, and a document keyed null no longer reads as the end.

The digest read one cursor over the whole collection; sustained cursors
stalled over a tunnel where single reads did not. It reads in chunks
resumed the same way now, and gives the same answer at any chunk size.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

NAME, PORT = "migkit-test-mongo-keytypes", 15741


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def coll():
    if not _docker():
        pytest.skip("docker not available")
    import pymongo
    from bson import ObjectId
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"{PORT}:27017", "mongo:7"], check=True,
                   capture_output=True)
    try:
        client = pymongo.MongoClient(f"mongodb://127.0.0.1:{PORT}/",
                                     serverSelectionTimeoutMS=2000)
        for _ in range(60):
            try:
                client.admin.command("ping")
                break
            except Exception:
                time.sleep(1)
        client.app.docs.insert_many([
            {"_id": 1, "v": 1}, {"_id": 2, "v": 2}, {"_id": 2.5, "v": 7},
            {"_id": "a", "v": 3}, {"_id": "b", "v": 4},
            {"_id": ObjectId(), "v": 5}, {"_id": ObjectId(), "v": 6},
            {"_id": None, "v": 0}])
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


def _eng():
    from migkit.engines.mongodb import MongoEngine
    ep = Endpoint(host="127.0.0.1", port=PORT)
    return MongoEngine(Hop(name="kt", engine="mongodb", source=ep, target=ep,
                           databases=["app"]))


@pytest.mark.parametrize("size", [1, 2, 3, 1000])
def test_every_document_is_read_whatever_the_chunk(coll, size):
    eng, seen, after = _eng(), [], None
    while True:
        rows, after = eng.neutral_read("src", "app", "docs",
                                       [("_id", "x"), ("v", "x")], after,
                                       size)
        if not rows:
            break
        seen += [r[1] for r in rows]
    assert sorted(seen) == [0, 1, 2, 3, 4, 5, 6, 7], seen


def test_the_digest_is_the_same_at_any_chunk_size(coll):
    eng = _eng()
    whole = eng.neutral_digest("src", "app", "docs", [("v", "integer")])
    assert whole[0] == 8, whole
    eng.DIGEST_CHUNK = 2
    assert eng.neutral_digest("src", "app", "docs",
                              [("v", "integer")]) == whole
