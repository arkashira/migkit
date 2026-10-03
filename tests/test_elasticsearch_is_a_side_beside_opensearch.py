"""Elasticsearch as a side, beside OpenSearch (backlog P0-SCENARIOS): the
OpenSearch engine reads an Elasticsearch server as it reads its own -
documents as JSON, basic authentication, the same bulk and scroll - with
what differs between the two held to: each named by what only it says of
itself (`GET /`), the plan saying "a fork change", and a field type one
has and the other has not refused before anything is written.

Before: an Elasticsearch side was taken as OpenSearch without a word, and
an index holding a `flattened` field stopped the copy when OpenSearch
refused to make its mapping.
"""
import json
import subprocess
import time
import urllib.request

import pytest

from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("assess.versions", "opensearch"), ("assess.brands", "opensearch")]

pytestmark = needs_docker

ES = ("migkit-test-side-es", 16406, "elasticsearch:8.15.3")
OS = ("migkit-test-side-os", 16407, "opensearchproject/opensearch:2.11.1")


def _http(port, method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read() or b"{}")


@pytest.fixture(scope="module")
def servers():
    for name, _, _ in (ES, OS):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", ES[0], "-p",
                    f"127.0.0.1:{ES[1]}:9200", "-e",
                    "discovery.type=single-node", "-e",
                    "xpack.security.enabled=false", "-e",
                    "ES_JAVA_OPTS=-Xms512m -Xmx512m", ES[2]], check=True,
                   capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", OS[0], "-p",
                    f"127.0.0.1:{OS[1]}:9200", "-e",
                    "discovery.type=single-node", "-e",
                    "plugins.security.disabled=true", "-e",
                    "OPENSEARCH_INITIAL_ADMIN_PASSWORD=CHANGE_ME-Os9!admin",
                    "-e", "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m", OS[2]],
                   check=True, capture_output=True)
    try:
        for _, port, _ in (ES, OS):
            for _ in range(120):
                try:
                    _http(port, "GET", "/")
                    break
                except Exception:
                    time.sleep(2)
        _http(ES[1], "PUT", "/app.events", {"mappings": {"properties": {
            "attrs": {"type": "flattened"}, "name": {"type": "keyword"}}}})
        _http(ES[1], "PUT", "/app.plain", {"mappings": {"properties": {
            "name": {"type": "keyword"}, "n": {"type": "long"}}}})
        for i in range(3):
            _http(ES[1], "PUT", f"/app.plain/_doc/{i}?refresh=true",
                  {"name": f"n{i}", "n": i})
        yield
    finally:
        for name, _, _ in (ES, OS):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(tmp_path):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="es-side", engine="elasticsearch", goal="cutover",
              source=Endpoint(host="127.0.0.1", port=ES[1]),
              target=Endpoint(host="127.0.0.1", port=OS[1]),
              databases=["app"])
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def test_elasticsearch_is_named_read_and_held_to_what_opensearch_lacks(
        servers, tmp_path):
    from migkit import case
    from migkit.engines.opensearch import OpenSearchEngine
    hop = _hop(tmp_path)
    eng = OpenSearchEngine(hop)
    src, dst = eng._brands()
    assert (src.name, dst.name) == ("elasticsearch", "opensearch")
    assert src.version.startswith("8.15") and dst.version.startswith("2.11")
    # read as OpenSearch is read
    assert "plain" in eng.neutral_tables("src", "app")
    cols = eng.neutral_columns("src", "app", "plain")
    rows = [r for batch in eng.neutral_batches("src", "app", "plain", cols)
            for r in batch]
    assert len(rows) == 3
    by = {r["item"]: r for r in case.checks(hop, OpenSearchEngine(hop),
                                            ["app"])}
    named = by["what the two servers are"]["detail"]
    assert named.startswith("a fork change: Elasticsearch 8.15"), named
    assert "to OpenSearch 2.11" in named
    flat = by["flattened fields"]
    assert flat["level"] == "fail" and "events.attrs" in flat["detail"]
    assert "OpenSearch 2.11.1 has none" in flat["detail"], flat
    assert by["users and roles"]["level"] == "pass"
    lines = case.plan_lines(hop, OpenSearchEngine(hop), "app")
    assert lines[0].startswith("the case: a fork change: Elasticsearch")
