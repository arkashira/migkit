"""An OpenSearch source's writes are held at cutover on a server of the test
lanes: a second session's write to an index of the hop is refused while its
indices are blocked (`os_indices`) or the cluster is read-only
(`os_cluster`), an index the hop does not move goes on taking writes, a
hold whose process was killed outright is found and lifted by `leftovers`,
and `doctor` names the rung."""
import json
import urllib.error
import urllib.request

from migkit import freeze
from tests import holdkit, servers, waits
from tests.conftest import needs_docker

PROVES = [("cutover.freeze", "opensearch"),
          ("cutover.read-only-target", "opensearch")]

pytestmark = needs_docker

pair = servers.fixture("opensearch")


def _http(pair, method, path, body=None):
    req = urllib.request.Request(
        f"http://{pair.src.host}:{pair.src.port}{path}",
        data=None if body is None else json.dumps(body).encode(),
        method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def _write(pair, index, doc):
    return _http(pair, "PUT", f"/{index}/_doc/{doc}?refresh=true",
                 {"v": doc}) in (200, 201)


def _setup(pair, tmp_path, with_other):
    from migkit.engines.opensearch import OpenSearchEngine
    names = ["shop.orders", "shop.carts"] + (["logs"] if with_other else [])
    for n in names:
        _http(pair, "DELETE", f"/{n}")
        assert _write(pair, n, "seed")
    hop = holdkit.hop_of("opensearch", pair.src, pair.dst, ["shop"],
                         tmp_path)
    return hop, OpenSearchEngine(hop), names


def _lift_all(pair, names):
    _http(pair, "PUT", "/_cluster/settings",
          {"persistent": {"cluster.blocks.read_only": None}})
    for n in names:
        _http(pair, "PUT", f"/{n}/_settings", {"index.blocks.write": False})
        _http(pair, "DELETE", f"/{n}")


def test_a_second_session_is_refused_on_the_hops_indices_only(pair, tmp_path):
    hop, eng, names = _setup(pair, tmp_path, True)
    try:
        assert freeze.choose(hop, eng, ["shop"]).rung.name == "os_indices"
        freeze.freeze_source(hop, eng, ["shop"], lambda m: None,
                             "os_indices")
        assert not _write(pair, "shop.orders", "during")
        assert not _write(pair, "shop.carts", "during")
        assert _write(pair, "logs", "during"), "an index the hop does not move"
        freeze.thaw_source(hop, eng, ["shop"], lambda m: None)
        assert _write(pair, "shop.orders", "after")
        assert freeze.state(hop, "shop", "src") is None
    finally:
        _lift_all(pair, names)


def test_the_cluster_is_read_only_where_it_is_all_the_hops(pair, tmp_path):
    hop, eng, names = _setup(pair, tmp_path, False)
    try:
        assert freeze.choose(hop, eng, ["shop"]).rung.name == "os_cluster"
        freeze.freeze_source(hop, eng, ["shop"], lambda m: None,
                             "os_cluster")
        assert not _write(pair, "shop.orders", "during")
        freeze.thaw_source(hop, eng, ["shop"], lambda m: None)
        assert _write(pair, "shop.orders", "after")
        assert freeze.server_state(hop) is None
    finally:
        _lift_all(pair, names)


def test_a_hold_whose_process_was_killed_is_found_and_lifted(pair, tmp_path):
    hop, eng, names = _setup(pair, tmp_path, True)
    try:
        def refuses():
            assert not _write(pair, "shop.orders", "during")

        def accepts():
            waits.until(lambda: _write(pair, "shop.orders", "after"), 30,
                        "the write is taken again")
        holdkit.lifted_after_a_crash(hop, eng, "os_indices", ["shop"],
                                     accepts, refuses)
    finally:
        _lift_all(pair, names)
