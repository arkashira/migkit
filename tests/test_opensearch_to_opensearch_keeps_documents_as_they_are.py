"""OpenSearch to OpenSearch: documents copied as they are held, settings
compared, a snapshot taken by the cluster, and only what was written since
the last run verified.

Measured before: an OpenSearch hop could not be moved at all - the table
copier refused the first index on its `_id` ("a own text column (_id) has
no mapping migkit writes") - and it compared no settings (an index
analysed another way finds other words for the same text), took no
snapshot and had no verify of what changed.
"""
import json
import subprocess
import time
import urllib.request

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

OS, PORT = "migkit-test-osos", 15930


def _http(method, path, body=None, ndjson=False):
    data = None
    if body is not None:
        data = (body if ndjson else json.dumps(body)).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}{path}", method=method, data=data,
        headers={"Content-Type": "application/x-ndjson" if ndjson
                 else "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
    return json.loads(raw) if raw else {}


@pytest.fixture(scope="module")
def cluster():
    subprocess.run(["docker", "rm", "-f", "-v", OS], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", OS, "-p",
                    f"{PORT}:9200", "-e", "discovery.type=single-node",
                    "-e", "DISABLE_SECURITY_PLUGIN=true", "-e",
                    "DISABLE_INSTALL_DEMO_CONFIG=true", "-e",
                    "OPENSEARCH_JAVA_OPTS=-Xms256m -Xmx256m", "-e",
                    "path.repo=/usr/share/opensearch/snapshots",
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
    hop = Hop(name="osos", engine="opensearch", source=ep, target=ep,
              databases=[db], db_map={db: f"{db}copy"})
    hop.report_dir = lambda d=None: tmp_path
    return OpenSearchEngine(hop)


def _index(name, docs, analyzer="standard"):
    _http("PUT", f"/{name}", {
        "settings": {"number_of_shards": 2, "number_of_replicas": 0,
                     "analysis": {"analyzer": {"body": {
                         "type": analyzer}}}},
        "mappings": {"properties": {
            "title": {"type": "text", "analyzer": "body"},
            "tags": {"type": "keyword"},
            "author": {"properties": {"name": {"type": "text"},
                                      "age": {"type": "integer"}}}}}})
    lines = []
    for i, d in docs:
        lines += [json.dumps({"index": {"_index": name, "_id": str(i)}}),
                  json.dumps(d)]
    if lines:
        _http("POST", "/_bulk?refresh=true", "\n".join(lines) + "\n",
              ndjson=True)


DOCS = [(i, {"title": f"a story {i}", "tags": ["x", f"t{i % 3}"],
             "author": {"name": "Ann", "age": 30 + i % 5}})
        for i in range(3000)]


def test_documents_cross_as_they_are(cluster, tmp_path, monkeypatch):
    import migkit.config as cfg
    from click.testing import CliRunner

    from migkit import cli
    _index("docs.posts", DOCS)
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  osos:\n    engine: opensearch\n"
        f"    source: {{host: 127.0.0.1, port: {PORT}, user: '',"
        " password: ''}\n"
        f"    target: {{host: 127.0.0.1, port: {PORT}, user: '',"
        " password: ''}\n"
        "    databases: [docs]\n    db_map: {docs: docscopy}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    got = CliRunner().invoke(cli.main, ["move", "osos", "--mode", "full",
                                        "--go"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code == 0, said
    assert "posts: 3,000 documents copied" in said, said
    one = _http("GET", "/docscopy.posts/_doc/7")["_source"]
    assert one == dict(DOCS[7][1]), one
    made = _http("GET", "/docscopy.posts")["docscopy.posts"]
    assert made["mappings"]["properties"]["author"]["properties"][
        "age"] == {"type": "integer"}
    assert made["settings"]["index"]["analysis"]["analyzer"]["body"][
        "type"] == "standard"
    # the load's own settings given back
    assert made["settings"]["index"].get("refresh_interval") != "-1"


def test_another_analyzer_is_named(cluster, tmp_path):
    _index("ana.t", [], analyzer="standard")
    _index("anacopy.t", [], analyzer="whitespace")
    got = _engine(tmp_path, "ana").check_params("ana")
    assert got[0].status == "diff", got[0].detail
    assert "t:index.analysis.analyzer.body.type src=standard" \
        " dst=whitespace" in got[0].detail, got[0].detail


def test_a_snapshot_is_taken_by_the_cluster(cluster, tmp_path):
    _index("snap.t", [])
    _index("snapcopy.t", DOCS[:10])
    point = tmp_path / "20260927-0202"
    point.mkdir()
    _engine(tmp_path, "snap").snapshot_state("snap", point)
    got = json.loads((point / "dst-indexes.json").read_text())
    assert got["snapshot"]["state"] == "SUCCESS", got
    assert got["indexes"]["snapcopy.t"]["count"] == 10


def test_only_what_was_written_is_verified(cluster, tmp_path):
    _index("dv.t", DOCS[:200])
    _index("dvcopy.t", DOCS[:200])
    eng = _engine(tmp_path, "dv")
    first = eng.delta_verify("dv")
    assert "baseline" in first[0].detail
    _http("PUT", "/dv.t/_doc/5?refresh=true", {"title": "rewritten"})
    _http("DELETE", "/dv.t/_doc/6?refresh=true")
    got = eng.delta_verify("dv")
    row = [r for r in got if r.scope == "dv.t"][0]
    assert row.status == "diff", row.detail
    assert row.detail == "1 written, 1 deleted on the source and still on" \
        " the target: 2 differ", row.detail
    _http("PUT", "/dvcopy.t/_doc/5?refresh=true", {"title": "rewritten"})
    _http("DELETE", "/dvcopy.t/_doc/6?refresh=true")
    got = eng.delta_verify("dv")
    assert got[0].status == "ok", [r.detail for r in got]
    assert "advanced" in got[0].detail
