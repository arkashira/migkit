"""Qdrant's settings compared on both servers (backlog R16b): each
collection's configuration read through the server's own API and every
difference that changes recall, size or throughput said with both values,
how far the reading goes (the collections, their shards, the cluster) chosen
by the decision engine from the collections and shards measured.

The first half runs against the sandbox's `qdrant` server pair; the second
against a server in this process that lacks an endpoint, which is said as a
gap, not raised.
"""
import json
import urllib.request

import pytest

from migkit import decide
from migkit.config import Endpoint, Hop
from tests import servers
from tests.conftest import needs_docker
from tests.test_qdrant_and_neo4j_go_deeper import DeepQdrant


pair = servers.fixture("qdrant")


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _engine(src, dst, tmp_path):
    from migkit.engines.qdrant import QdrantEngine
    hop = Hop(name="vec", engine="qdrant", source=src, target=dst,
              databases=["qdrant"])
    hop.report_dir = lambda db=None: tmp_path
    return QdrantEngine(hop)


def _put(server, name, **config):
    body = {"vectors": {"size": 4, "distance": "Cosine"}, **config}
    req = urllib.request.Request(
        f"http://{server.host}:{server.port}/collections/{name}",
        method="PUT", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        assert json.loads(r.read())["result"] is True


def _drop(server, name):
    req = urllib.request.Request(
        f"http://{server.host}:{server.port}/collections/{name}",
        method="DELETE")
    urllib.request.urlopen(req, timeout=30).read()


def _server_engine(pair, tmp_path):
    return _engine(pair.src.endpoint(tls=False), pair.dst.endpoint(tls=False),
                   tmp_path)


@needs_docker
def test_a_distance_and_an_hnsw_m_that_differ_are_findings_with_both_values(
        pair, tmp_path):
    _put(pair.src, "cmp_a", hnsw_config={"m": 16})
    _put(pair.dst, "cmp_a", hnsw_config={"m": 32},
         vectors={"size": 4, "distance": "Dot"})
    try:
        got = _server_engine(pair, tmp_path).check_params("qdrant")
    finally:
        _drop(pair.src, "cmp_a")
        _drop(pair.dst, "cmp_a")
    by = {r.scope: r for r in got}
    print(sorted((k, v.status, v.detail[:80]) for k, v in by.items()))
    dist = by["qdrant cmp_a params.vectors.distance"]
    assert dist.status == "diff" and "recall" in dist.detail
    assert "'Cosine'" in dist.detail and "'Dot'" in dist.detail
    m = by["qdrant cmp_a hnsw_config.m"]
    assert m.status == "diff" and "16" in m.detail and "32" in m.detail
    assert m.table == "cmp_a"
    assert not [r for r in got if r.status == "ok"]


@needs_docker
def test_equal_collections_are_no_finding_and_the_depth_names_what_was_counted(
        pair, tmp_path):
    for s in (pair.src, pair.dst):
        _put(s, "cmp_b", hnsw_config={"m": 24})
    try:
        eng = _server_engine(pair, tmp_path)
        got = eng.check_params("qdrant")
        choice, facts = eng.settings_depth("qdrant")
    finally:
        _drop(pair.src, "cmp_b")
        _drop(pair.dst, "cmp_b")
    assert [r.status for r in got] == ["ok"]
    assert "1 collections, 2 shards" in got[0].detail
    # one shard a collection and a server alone: the configuration says it all
    assert choice.path == "collections"
    assert facts["collections"] == 1 and facts["shards"] == 2
    assert facts["peers"] == 1 and facts["rtt"] > 0
    assert {"collections", "shards", "peers"} <= set(choice.asked)
    assert [w for w, _ in [(r.name, why) for r, why in choice.passed]] == [
        "cluster", "shards"]


@needs_docker
def test_a_collection_split_in_shards_is_read_shard_by_shard(pair, tmp_path):
    _put(pair.src, "cmp_c", shard_number=3)
    _put(pair.dst, "cmp_c", shard_number=1)
    try:
        eng = _server_engine(pair, tmp_path)
        got = eng.check_params("qdrant")
        choice, facts = eng.settings_depth("qdrant")
    finally:
        _drop(pair.src, "cmp_c")
        _drop(pair.dst, "cmp_c")
    assert choice.path == "shards"
    assert facts["collections"] == 1 and facts["shards"] == 4
    by = {r.scope: r for r in got}
    count = by["qdrant cmp_c params.shard_number"]
    assert count.status == "warn" and "throughput" in count.detail
    assert "3" in count.detail and "1" in count.detail
    laid = by["qdrant cmp_c layout.shards"]
    assert "source 3, target 1" in laid.detail


@needs_docker
def test_a_setting_the_servers_report_that_no_effect_is_known_for_is_counted(
        pair, tmp_path):
    _put(pair.src, "cmp_d", optimizers_config={"deleted_threshold": 0.3},
         wal_config={"wal_capacity_mb": 64})
    _put(pair.dst, "cmp_d", optimizers_config={"deleted_threshold": 0.3},
         wal_config={"wal_capacity_mb": 64})
    try:
        got = _server_engine(pair, tmp_path).check_params("qdrant")
        params = json.loads((tmp_path / "params.json").read_text())
    finally:
        _drop(pair.src, "cmp_d")
        _drop(pair.dst, "cmp_d")
    assert [r.status for r in got] == ["ok"]
    assert params["cmp_d optimizer_config.deleted_threshold"] == {
        "src": 0.3, "dst": 0.3}


# ---- a server in this process ---------------------------------------------

class Sharded(DeepQdrant):
    """The fake with a shard count and the cluster layout of its own, or not
    answering for them."""

    def __init__(self, shards=1, layout=True, peers=0, cluster=True):
        super().__init__()
        self.shards, self.layout, self.peers = shards, layout, peers
        self.cluster = cluster

    def _answer(self, method, path, body):
        parts = path.split("?")[0].strip("/").split("/")
        if parts == ["cluster"] and not self.cluster:
            return 404
        if parts == ["cluster"]:
            return ({"status": "enabled",
                     "peers": {str(i): {} for i in range(self.peers)}}
                    if self.peers else {"status": "disabled"})
        if len(parts) == 3 and parts[2] == "cluster":
            return ({"local_shards": [{"shard_id": i, "state": "Active"}
                                      for i in range(self.shards)],
                     "remote_shards": []} if self.layout else 404)
        got = super()._answer(method, path, body)
        if len(parts) == 2 and method == "GET" and isinstance(got, dict):
            got["config"]["params"]["shard_number"] = self.shards
        return got


def _fakes(tmp_path, src, dst):
    for s in (src, dst):
        s.make("docs", {"size": 3, "distance": "Cosine"})
    ep = lambda s: Endpoint(host="127.0.0.1", port=s.port,
                            options={"tls": False})
    return _engine(ep(src), ep(dst), tmp_path)


def test_a_server_without_the_shard_endpoint_is_a_gap_not_an_exception(
        tmp_path):
    src, dst = Sharded(shards=2, layout=False), Sharded(shards=2,
                                                        layout=False)
    eng = _fakes(tmp_path, src, dst)
    got = eng.check_params("qdrant")
    choice, _ = eng.settings_depth("qdrant")
    assert choice.path == "collections"
    gap = [r for r in got if r.scope.endswith("params depth")]
    assert len(gap) == 1 and gap[0].status == "skip"
    assert "shards are not read" in gap[0].detail
    assert "404" in gap[0].detail
    # what could be read is still compared
    assert [r.status for r in got if r.check == "params"
            and r.status != "skip"] == ["ok"]


def test_a_server_without_the_cluster_endpoint_is_said_not_raised(tmp_path):
    src = Sharded(shards=2, cluster=False)
    dst = Sharded(shards=2, cluster=False)
    eng = _fakes(tmp_path, src, dst)
    got = eng.check_params("qdrant")
    assert eng.settings_depth("qdrant")[0].path == "shards"
    assert [r.status for r in got] == ["ok"]


def test_the_cluster_is_read_where_the_servers_are_one_and_answer(tmp_path):
    src, dst = Sharded(shards=2, peers=3), Sharded(shards=2, peers=2)
    eng = _fakes(tmp_path, src, dst)
    got = {r.scope: r for r in eng.check_params("qdrant")}
    choice, facts = eng.settings_depth("qdrant")
    assert choice.path == "cluster" and facts["peers"] == 3
    peers = got["qdrant cluster cluster.peers"]
    assert peers.status == "warn" and "source 3, target 2" in peers.detail


def test_the_choice_is_the_decision_engines_and_is_costed_from_the_counts():
    facts = dict(collections=40, shards=200, peers=3, rtt=1.0, timeout=120,
                 shards_answer=None, cluster_answer=None)
    # 40 collections on two servers: 160 requests to read their shards, 244
    # with the cluster; a second a request, neither passes a 120s call
    got = decide.settings_depth(decide.Facts(facts))
    assert got.path == "collections"
    assert any("would pass the 120s" in why for _, why in got.passed)
    facts["rtt"] = 0.5
    assert decide.settings_depth(decide.Facts(facts)).path == "shards"
    facts["rtt"] = 0.05
    assert decide.settings_depth(decide.Facts(facts)).path == "cluster"
    facts["collections"] = 3
    got = decide.settings_depth(decide.Facts(dict(facts, shards=6, peers=1)))
    assert got.path == "collections"
    assert [r.name for r, _ in got.passed] == ["cluster", "shards"]
