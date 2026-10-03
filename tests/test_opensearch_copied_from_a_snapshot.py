"""An OpenSearch index copied from the source's own snapshot (backlog
F7.rfs), beside migkit's sliced scroll.

The source takes a snapshot into a directory this machine can read (the
test's own doing, on the source it set up). The hop names it; the index
is copied out of the snapshot's files - the source cluster itself is not
searched - with every document's id, routing and `_source` as they were,
the target's replicas and refreshes put back after, and the reader's
working index gone. An index the target would hold under another name,
or one the snapshot does not hold, goes by the scroll, with the reason.
Skipped where the snapshot reader is not built: `migkit doctor --install`
builds it.
"""
import json
import os
import subprocess
import time
import urllib.request

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.bulk", "opensearch")]

SRC, DST = ("migkit-test-rfs-src", 16280), ("migkit-test-rfs-dst", 16281)


def _built():
    from migkit import fromsnapshot
    return fromsnapshot.program() is not None


pytestmark = [needs_docker, pytest.mark.skipif(
    not _built(), reason="the snapshot reader is not built here")]


def _http(port, method, path, body=None, ndjson=False):
    data = None
    if body is not None:
        data = (body if ndjson else json.dumps(body)).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=method, data=data,
        headers={"Content-Type": "application/x-ndjson" if ndjson
                 else "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
    return json.loads(raw) if raw else {}


@pytest.fixture(scope="module")
def clusters(tmp_path_factory):
    snaps = tmp_path_factory.mktemp("snapshots")
    os.chmod(snaps, 0o777)
    for (name, port), mount in ((SRC, True), (DST, False)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", name, "-p",
             f"127.0.0.1:{port}:9200", "-e", "discovery.type=single-node",
             "-e", "DISABLE_SECURITY_PLUGIN=true", "-e",
             "DISABLE_INSTALL_DEMO_CONFIG=true", "-e",
             "OPENSEARCH_JAVA_OPTS=-Xms256m -Xmx256m"]
            + (["-e", "path.repo=/snapshots", "-v", f"{snaps}:/snapshots"]
               if mount else [])
            + ["opensearchproject/opensearch:2.19.1"], check=True,
            capture_output=True)
    try:
        for _, port in (SRC, DST):
            end = time.time() + 180
            while time.time() < end:
                try:
                    if _http(port, "GET", "/_cluster/health").get(
                            "status") in ("green", "yellow"):
                        break
                except Exception:  # noqa: BLE001 - not up yet
                    pass
                time.sleep(2)
        port = SRC[1]
        _http(port, "PUT", "/app.books", {
            "settings": {"number_of_shards": 2, "number_of_replicas": 0},
            "mappings": {"properties": {"title": {"type": "text"},
                                        "n": {"type": "integer"}}}})
        lines = []
        for i in range(500):
            action = {"_index": "app.books", "_id": f"b{i}"}
            if i % 10 == 0:
                action["routing"] = "r1"
            lines += [json.dumps({"index": action}),
                      json.dumps({"title": f"book {i}", "n": i})]
        _http(port, "POST", "/_bulk?refresh=true", "\n".join(lines) + "\n",
              ndjson=True)
        _http(port, "PUT", "/app.later", {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0}})
        _http(port, "PUT", "/_snapshot/repo", {
            "type": "fs", "settings": {"location": "/snapshots"}})
        _http(port, "PUT", "/_snapshot/repo/snap-1?wait_for_completion=true",
              {"indices": "app.books", "include_global_state": True})
        yield snaps
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path, snaps, db_map=None):
    from migkit.engines.opensearch import OpenSearchEngine
    hop = Hop(name="rfs", engine="opensearch",
              source=Endpoint(host="127.0.0.1", port=SRC[1], options={
                  "snapshot": {"name": "snap-1", "repository": "repo",
                               "local_dir": str(snaps)}}),
              target=Endpoint(host="127.0.0.1", port=DST[1]),
              databases=["app"], db_map=db_map or {})
    hop.report_dir = lambda db=None: tmp_path
    return OpenSearchEngine(hop)


def _docs(port, index):
    got = _http(port, "POST", f"/{index}/_search", {
        "size": 1000, "query": {"match_all": {}}})
    return sorted((h["_id"], h.get("_routing"), json.dumps(
        h["_source"], sort_keys=True)) for h in got["hits"]["hits"])


def test_an_index_is_copied_out_of_the_sources_snapshot(clusters,
                                                        tmp_path):
    eng = _engine(tmp_path, clusters)
    said = []
    eng.native_bulk("app", ["books", "later"], True, said.append)
    assert any("books: 500 documents copied out of the source's snapshot"
               in m for m in said), said
    # not in the snapshot: the scroll carries it, and says why
    assert any(m.startswith("later:") and "does not hold app.later" in m
               for m in said), said
    assert _docs(SRC[1], "app.books") == _docs(DST[1], "app.books")
    settings = _http(DST[1], "GET", "/app.books/_settings")[
        "app.books"]["settings"]["index"]
    assert settings.get("refresh_interval") in (None, "1s")
    # the reader's working index is not left on the target
    names = [i["index"] for i in _http(DST[1], "GET",
                                       "/_cat/indices?format=json&h=index")]
    assert "app.books" in names
    assert not [n for n in names if n.startswith(".migrations")], names


def test_an_index_renamed_on_the_target_goes_by_the_scroll(clusters,
                                                           tmp_path):
    from migkit import fromsnapshot
    eng = _engine(tmp_path, clusters, {"app": "copy"})
    why = fromsnapshot.unfit(eng, "app", "books")
    assert "copy.books on the target" in why
