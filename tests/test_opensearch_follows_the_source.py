"""OpenSearch to OpenSearch: the target follows the source by each
shard's sequence numbers, each document written with the source's own
version of it, the fence waits on the follow and the check confirms a
difference still arriving.

Measured before: an OpenSearch hop could be copied and verified, not
followed; a copy emptied the target index first and wrote every document
unversioned. Shown here by hand as well: a change carried after a newer
one overwrites it unless the write carries a version, and an index emptied
and written again at the same versions refuses the documents for a minute
(the versions of deletes it keeps), so the copy would have lost them.
"""
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

OS, PORT = "migkit-test-eng-os", 16012


def _http(method, path, body=None, ndjson=False, ok=(200, 201)):
    data = None
    if body is not None:
        data = (body if ndjson else json.dumps(body)).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}{path}", method=method, data=data,
        headers={"Content-Type": "application/x-ndjson" if ndjson
                 else "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        if e.code in ok:
            raw = e.read()
        else:
            return {"status": e.code, **json.loads(e.read() or b"{}")}
    return json.loads(raw) if raw else {}


@pytest.fixture(scope="module")
def cluster():
    subprocess.run(["docker", "rm", "-f", "-v", OS], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", OS, "-p",
                    f"{PORT}:9200", "-e", "discovery.type=single-node",
                    "-e", "DISABLE_SECURITY_PLUGIN=true", "-e",
                    "DISABLE_INSTALL_DEMO_CONFIG=true", "-e",
                    "OPENSEARCH_JAVA_OPTS=-Xms384m -Xmx384m",
                    "opensearchproject/opensearch:2.19.1"], check=True,
                   capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            try:
                if _http("GET", "/_cluster/health").get("status") in (
                        "green", "yellow"):
                    break
            except Exception:
                pass
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", OS], capture_output=True)


def _engine(tmp_path, db):
    from migkit.engines.opensearch import OpenSearchEngine
    ep = Endpoint(host="127.0.0.1", port=PORT)
    hop = Hop(name="osf", engine="opensearch", source=ep, target=ep,
              databases=[db], db_map={db: f"{db}copy"})
    hop.report_dir = lambda d=None: tmp_path
    return OpenSearchEngine(hop)


def _index(name, n, refresh="1s"):
    _http("PUT", f"/{name}", {
        "settings": {"number_of_shards": 2, "number_of_replicas": 0,
                     "refresh_interval": refresh},
        "mappings": {"properties": {"title": {"type": "text"},
                                    "n": {"type": "integer"}}}})
    lines = []
    for i in range(n):
        lines += [json.dumps({"index": {"_index": name, "_id": str(i)}}),
                  json.dumps({"title": f"doc {i}", "n": i})]
    if lines:
        _http("POST", "/_bulk?refresh=true", "\n".join(lines) + "\n",
              ndjson=True)


def _docs(name):
    _http("POST", f"/{name}/_refresh")
    got = _http("POST", f"/{name}/_search", {"size": 10000,
                                              "sort": ["_doc"]})
    return {h["_id"]: h["_source"] for h in got["hits"]["hits"]}


def _tail(eng, db, tmp_path):
    ended = {}

    def run():
        try:
            eng.tail_apply(db, True, tmp_path / "tail-token.json",
                           lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, ended


def _stop(thread):
    import ctypes
    if thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    thread.join(timeout=30)


def test_a_copy_and_its_follow_end_equal(cluster, tmp_path):
    _index("shop.orders", 500)
    eng = _engine(tmp_path, "shop")
    token = tmp_path / "tail-token.json"
    eng.tail_start("shop", token)
    # after the position, before the copy: the follow carries it too
    _http("PUT", "/shop.orders/_doc/5", {"title": "during", "n": -5})
    said = []
    eng.native_bulk("shop", ["orders"], True, said.append)
    assert said == ["orders: 500 documents copied in 4 slices"], said
    thread, ended = _tail(eng, "shop", tmp_path)
    try:
        _http("PUT", "/shop.orders/_doc/7", {"title": "after", "n": 70})
        _http("DELETE", "/shop.orders/_doc/3")
        _http("PUT", "/shop.orders/_doc/1000?routing=r1",
              {"title": "routed", "n": 1000})
        at = eng.src_lsn("shop")
        assert at is not None
        assert eng.fence_wait("shop", at, timeout=120) is True
        # written while the follow is looking: a difference still arriving,
        # which the check waits on and compares again
        _http("PUT", "/shop.orders/_doc/8?refresh=true",
              {"title": "late", "n": 80})
        got = eng.check_data("shop", table="orders")
        assert [r.status for r in got] == ["ok"], [r.detail for r in got]
        assert _docs("shopcopy.orders")["8"]["title"] == "late"
    finally:
        _stop(thread)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
    src, dst = _docs("shop.orders"), _docs("shopcopy.orders")
    assert src == dst
    assert "3" not in dst and dst["7"]["title"] == "after"
    assert dst["5"]["title"] == "during"
    routed = _http("GET", "/shopcopy.orders/_doc/1000?routing=r1")
    assert routed["found"] and routed["_routing"] == "r1", routed
    # the target holds each document at the source's own place in history
    one = _http("GET", "/shop.orders/_doc/7")
    mine = _http("GET", "/shopcopy.orders/_doc/7")
    assert mine["_version"] == (one["_primary_term"] << 43) | one["_seq_no"]


def test_a_change_carried_late_is_left_by_the_target(cluster, tmp_path):
    """By hand first: a document written again without a version is
    replaced, whatever came first. With the source's version, the older
    change is refused and the newer one stays."""
    _index("late.t", 1)
    _http("PUT", "/late.t/_doc/0", {"title": "newer", "n": 1})
    newer = _http("GET", "/late.t/_doc/0?_source=true")
    older = dict(newer, _seq_no=newer["_seq_no"] - 1,
                 _source={"title": "older", "n": 0})
    eng = _engine(tmp_path, "late")
    _http("PUT", "/latecopy.t", {"settings": {"number_of_replicas": 0}})
    _http("PUT", "/latecopy.t/_doc/0", {"title": "newer", "n": 1})
    _http("PUT", "/latecopy.t/_doc/0", {"title": "older", "n": 0})
    assert _http("GET", "/latecopy.t/_doc/0")["_source"]["title"] == "older"
    assert eng._bulk("dst", [eng._versioned(newer, "latecopy.t")]) == (1, 0)
    assert eng._bulk("dst", [eng._versioned(older, "latecopy.t")]) == (0, 1)
    assert _http("GET", "/latecopy.t/_doc/0")["_source"]["title"] == "newer"


def test_a_copy_run_again_keeps_the_target_and_takes_what_is_gone(cluster,
                                                                  tmp_path):
    _index("again.t", 50)
    eng = _engine(tmp_path, "again")
    eng.native_bulk("again", ["t"], True, [].append)
    _http("DELETE", "/again.t/_doc/2?refresh=true")
    said = []
    eng.native_bulk("again", ["t"], True, said.append)
    assert said == ["t: 49 documents copied in 4 slices, 1 the source does"
                    " not hold taken from the target"], said
    assert _docs("again.t") == _docs("againcopy.t")


def test_a_document_deleted_on_the_target_refuses_its_version(cluster,
                                                              tmp_path):
    """By hand first: a document deleted and written again at the version
    it had is refused - the index keeps the delete's version for a minute.
    A copy that emptied the index before writing at the source's versions
    would lose every document that way, so the copy does not empty it; and
    where the target deleted one itself, the copy says so and stops rather
    than calling itself complete."""
    _index("gcd.t", 20)
    eng = _engine(tmp_path, "gcd")
    eng.native_bulk("gcd", ["t"], True, [].append)
    one = _http("GET", "/gcd.t/_doc/1")
    _http("POST", "/gcdcopy.t/_delete_by_query?refresh=true",
          {"query": {"ids": {"values": ["1"]}}})
    assert eng._bulk("dst", [eng._versioned(one, "gcdcopy.t")]) == (0, 1)
    assert not _http("GET", "/gcdcopy.t/_doc/1", ok=(200, 404))["found"]
    with pytest.raises(SystemExit, match="1 documents deleted on the target"
                                         " in the last minute refuse"):
        eng.native_bulk("gcd", ["t"], True, [].append)


def test_an_index_that_never_refreshes_is_refused(cluster, tmp_path):
    _index("still.t", 5, refresh="-1")
    eng = _engine(tmp_path, "still")
    with pytest.raises(SystemExit, match="refreshes only when asked"):
        eng.tail_start("still", tmp_path / "tail-token.json")
    assert not (tmp_path / "tail-token.json").exists()


def test_users_where_neither_cluster_runs_the_security_plugin(cluster,
                                                              tmp_path):
    from migkit import users
    out, _ = users.compare(_engine(tmp_path, "shop").hop, [].append)
    assert out["plugin"] == {"source": "absent", "target": "absent"}, out
    assert out["result"] == "pass" and out["missing_on_target"] == []
