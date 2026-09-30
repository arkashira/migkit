"""An OpenSearch source held at cutover, read back, and given back
(`freeze.OpenSearchHold`): each of the hop's indices closed to writes by
its own block, what each had set given back; with every index the hop's,
the whole cluster read-only as well, lifted first on the way back. An
index the hop does not move goes on taking writes where only the hop's
are held. The read back says when an index is open.
"""
import json
import subprocess
import time
import urllib.error
import urllib.request

import pytest

from migkit import freeze
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

NAME, PORT = "migkit-test-hold-opensearch", 16354


def _http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{PORT}{path}",
                                 data=data, method=method,
                                 headers={"Content-Type":
                                          "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
    return json.loads(raw) if raw else {}


def _write(index, doc_id):
    """True where the document was taken, False where a block refused."""
    try:
        _http("PUT", f"/{index}/_doc/{doc_id}?refresh=true", {"v": doc_id})
        return True
    except urllib.error.HTTPError as e:
        if e.code in (403, 429):
            return False
        raise


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-p",
                    f"127.0.0.1:{PORT}:9200", "-e",
                    "discovery.type=single-node", "-e",
                    "DISABLE_SECURITY_PLUGIN=true", "-e",
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
        else:
            pytest.fail("opensearch never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


@pytest.fixture
def shop(server, tmp_path):
    _http("PUT", "/_cluster/settings",
          {"persistent": {"cluster.blocks.read_only": None}})
    for index in ("shop.orders", "shop.carts", "logs"):
        try:
            _http("DELETE", f"/{index}")
        except urllib.error.HTTPError:
            pass
        _http("PUT", f"/{index}", {"settings": {"number_of_replicas": 0}})
        _write(index, "seed")
    # one index set so already, to be given back as it was
    _http("PUT", "/shop.carts/_settings", {"index.blocks.write": False})
    ep = Endpoint(host="127.0.0.1", port=PORT)
    hop = Hop(name="oh", engine="opensearch", source=ep, target=ep,
              databases=["shop"], options={"cutover": True})
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    from migkit.engines.opensearch import OpenSearchEngine
    yield hop, OpenSearchEngine(hop)
    _http("PUT", "/_cluster/settings",
          {"persistent": {"cluster.blocks.read_only": None}})


def _setting(index):
    got = _http("GET", f"/{index}/_settings?flat_settings=true")
    return got[index]["settings"].get("index.blocks.write")


def test_the_hops_indices_are_blocked_read_back_and_given_back(shop):
    hop, eng = shop
    got = freeze.choose(hop, eng, ["shop"])
    assert got.rung.name == "os_indices" and "logs" in got.reason, got
    said = []
    freeze.freeze_source(hop, eng, ["shop"], said.append, "os_indices")
    assert not _write("shop.orders", "during")
    assert not _write("shop.carts", "during")
    assert _write("logs", "during"), "an index the hop does not move"
    held = freeze.proof(hop, eng, "shop", "os_indices")
    assert all(ok for ok, _ in held), held
    # an index opened behind the hold's back reads back as open
    _http("PUT", "/shop.orders/_settings", {"index.blocks.write": False})
    held = freeze.proof(hop, eng, "shop", "os_indices")
    assert (False, "open to writes: shop.orders") in held, held
    freeze.thaw_source(hop, eng, ["shop"], said.append)
    assert _setting("shop.orders") is None
    assert _setting("shop.carts") == "false", "given back as it was"
    assert _write("shop.orders", "after") and _write("shop.carts", "after")


def test_the_whole_cluster_is_read_only_where_it_is_all_the_hops(shop):
    hop, eng = shop
    _http("DELETE", "/logs")
    got = freeze.choose(hop, eng, ["shop"])
    assert got.rung.name == "os_cluster", got
    said = []
    freeze.freeze_source(hop, eng, ["shop"], said.append, "os_cluster")
    assert not _write("shop.orders", "during")
    # an index made during the cutover is held as well
    with pytest.raises(urllib.error.HTTPError):
        _http("PUT", "/shop.new", {"settings": {"number_of_replicas": 0}})
    held = freeze.proof(hop, eng, "shop", "os_cluster")
    assert all(ok for ok, _ in held), held
    freeze.thaw_source(hop, eng, ["shop"], said.append)
    got = _http("GET", "/_cluster/settings?flat_settings=true")
    assert "cluster.blocks.read_only" not in got.get("persistent", {})
    assert _write("shop.orders", "after")
    assert freeze.server_state(hop) is None
