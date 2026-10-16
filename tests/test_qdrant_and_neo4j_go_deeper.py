"""Qdrant and Neo4j one step deeper (backlog R16b, R16c).

Qdrant against a server in this process: a collection made with the
source's HNSW graph and quantization, a deep check that fails when the
target's differ, a recall at k scored against the source's exact nearest
points (a target whose search finds fewer is a difference, one still
indexing a warning), and the choice between a collection's pages and its
snapshot from the measured link. Neo4j against answers shaped as the
server gives them: the dump rung refused unless both sides may stop their
database, users carried without a readable password, and a change position
counted beside an opaque change id.
"""
import json
import math

import pytest

from migkit import decide
from migkit.config import Endpoint, Hop
from tests.test_qdrant_and_neo4j_reach_the_depth_matrix import FakeQdrant

PROVES = [
    ("move.table-copy", "qdrant"),
    ("tail.follows", "neo4j"),
]


def _cos(a, b):
    return sum(x * y for x, y in zip(a, b)) / (
        math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


class DeepQdrant(FakeQdrant):
    """The fake with settings on its collections and a query that is exact
    or, for a server told to be `blind`, misses the nearest points."""

    def __init__(self, blind=False, status="green"):
        self.blind, self.status = blind, status
        self.settings = {}
        super().__init__()

    def _answer(self, method, path, body):
        parts = path.split("?")[0].strip("/").split("/")
        if len(parts) == 2 and method == "PUT":
            self.settings[parts[1]] = {k: body[k] for k in (
                "hnsw_config", "quantization_config") if k in body}
        if len(parts) == 2 and method == "GET":
            got = super()._answer(method, path, body)
            if isinstance(got, dict):
                got["config"].update(self.settings.get(parts[1], {}))
                got["status"] = self.status
            return got
        if parts[2:] == ["points", "query"]:
            pts = list(self.cols[parts[1]]["points"].values())
            if (body["query"] or {}) == {"sample": "random"}:
                return {"points": pts[:body["limit"]]}
            ranked = sorted(pts, key=lambda p: -_cos(body["query"],
                                                     p["vector"]))
            if self.blind and not body["params"]["exact"]:
                ranked = ranked[len(ranked) // 2:]
            return {"points": ranked[:body["limit"]]}
        return super()._answer(method, path, body)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _pair(tmp_path, dst_blind=False, dst_status="green"):
    from migkit.engines.qdrant import QdrantEngine
    src, dst = DeepQdrant(), DeepQdrant(blind=dst_blind, status=dst_status)
    src.make("docs", {"size": 3, "distance": "Cosine"}, {"lang": "keyword"})
    src.settings["docs"] = {
        "hnsw_config": {"m": 24, "ef_construct": 150},
        "quantization_config": {"scalar": {"type": "int8"}}}
    for i in range(1, 41):
        src.cols["docs"]["points"][i] = {
            "id": i, "vector": [math.sin(i), math.cos(i * 1.7), i / 40],
            "payload": {"n": i}}
    hop = Hop(name="vec", engine="qdrant",
              source=Endpoint(host="127.0.0.1", port=src.port,
                              options={"tls": False}),
              target=Endpoint(host="127.0.0.1", port=dst.port,
                              options={"tls": False, "recall_k": 5}),
              databases=["qdrant"])
    hop.report_dir = lambda db=None: tmp_path
    return QdrantEngine(hop), src, dst


def _copy(eng, src, dst):
    eng.neutral_create("dst", "qdrant", "docs", [])
    dst.cols["docs"]["points"].update(src.cols["docs"]["points"])
    dst.cols["docs"]["indexes"].update(src.cols["docs"]["indexes"])


def test_a_collection_is_made_with_the_sources_hnsw_and_quantization(
        tmp_path):
    eng, src, dst = _pair(tmp_path)
    eng.neutral_create("dst", "qdrant", "docs", [])
    assert dst.settings["docs"] == src.settings["docs"]


def test_the_deep_check_fails_a_target_with_other_hnsw_or_no_quantization(
        tmp_path):
    eng, src, dst = _pair(tmp_path)
    _copy(eng, src, dst)
    dst.settings["docs"] = {"hnsw_config": {"m": 16, "ef_construct": 150}}
    got = {r.scope: r for r in eng.check_deep("qdrant")}
    assert got["qdrant docs hnsw"].status == "diff"
    assert "16" in got["qdrant docs hnsw"].detail
    assert got["qdrant docs quantization"].status == "diff"


def test_a_target_that_states_more_than_the_source_still_holds_it(tmp_path):
    eng, src, dst = _pair(tmp_path)
    _copy(eng, src, dst)
    dst.settings["docs"]["hnsw_config"]["max_indexing_threads"] = 0
    got = {r.scope: r for r in eng.check_deep("qdrant")}
    assert got["qdrant docs hnsw"].status == "ok"
    assert got["qdrant docs quantization"].status == "ok"


def test_the_sample_resolves_recall_to_one_hit_in_k_and_never_exceeds_the_set():
    from migkit.engines.qdrant import QdrantEngine
    assert QdrantEngine.recall_sample(100_000, 10) == 97
    assert QdrantEngine.recall_sample(100_000, 20) == 384
    assert QdrantEngine.recall_sample(12, 10) == 11
    assert QdrantEngine.recall_sample(0, 10) == 0


def test_recall_of_an_equal_copy_is_the_same_on_both_sides(tmp_path):
    eng, src, dst = _pair(tmp_path)
    _copy(eng, src, dst)
    got = eng.recall_of("qdrant", "docs")
    assert got["k"] == 5 and got["n"] == 16
    assert got["source"] == got["target"] == 1.0
    res = [r for r in eng.check_recall("qdrant")]
    assert [r.status for r in res] == ["ok"]
    assert "recall at 5 over 16 queries" in res[0].detail


def test_a_target_whose_search_misses_the_nearest_points_is_a_difference(
        tmp_path):
    eng, src, dst = _pair(tmp_path, dst_blind=True)
    _copy(eng, src, dst)
    got = eng.recall_of("qdrant", "docs")
    assert got["target"] < got["source"] and got["behind"] > 0
    res = eng.check_recall("qdrant")[0]
    assert res.status == "diff" and res.fix_hint


def test_a_target_still_indexing_is_a_warning_not_a_failure(tmp_path):
    eng, src, dst = _pair(tmp_path, dst_blind=True, dst_status="yellow")
    _copy(eng, src, dst)
    res = eng.check_recall("qdrant")[0]
    assert res.status == "warn" and "still indexing" in res.detail


def test_an_empty_collection_has_no_recall_to_measure(tmp_path):
    eng, src, dst = _pair(tmp_path)
    src.cols["docs"]["points"].clear()
    _copy(eng, src, dst)
    assert eng.check_recall("qdrant")[0].status == "skip"


def _facts(snapshot_s, scroll_s, **kw):
    base = {"changed": False, "done": False, "fresh": True}
    base.update(kw)
    return decide.Facts(base, transfer=lambda: {
        "snapshot_s": snapshot_s, "scroll_s": scroll_s})


def test_a_large_collection_on_a_fast_stream_takes_the_snapshot(tmp_path):
    eng, _, _ = _pair(tmp_path)
    got = decide.climb("moving docs", eng._move_ways(), _facts(30.0, 900.0))
    assert got.path == "snapshot"


def test_a_small_collection_goes_by_its_pages(tmp_path):
    eng, _, _ = _pair(tmp_path)
    got = decide.climb("moving docs", eng._move_ways(), _facts(4.0, 0.5))
    assert got.path == "pages"
    assert "quicker here" in dict((r.name, w) for r, w in got.passed)[
        "snapshot"]


@pytest.mark.parametrize("fact,why", [
    ({"changed": True}, "mask or row filter"),
    ({"fresh": False}, "already holds"),
    ({"done": True}, "earlier run")])
def test_the_snapshot_is_passed_over_where_it_would_change_the_result(
        tmp_path, fact, why):
    eng, _, _ = _pair(tmp_path)
    got = decide.climb("moving docs", eng._move_ways(),
                       _facts(1.0, 900.0, **fact))
    assert got.path == "pages"
    assert why in dict((r.name, w) for r, w in got.passed)["snapshot"]


def test_the_transfer_is_measured_from_a_real_page(tmp_path):
    eng, _, _ = _pair(tmp_path)
    got = eng._transfer_facts("docs")
    assert got["rows"] == 40 and got["rtt"] > 0 and got["rate"] > 0
    assert got["scroll_s"] > 0 and got["snapshot_s"] > 0


# ---- Neo4j ------------------------------------------------------------------

@pytest.fixture
def neo():
    from migkit.engines.neo4j import Neo4jEngine
    hop = Hop(name="g", engine="neo4j",
              source=Endpoint(host="127.0.0.1", port=1, options={
                  "tls": False}),
              target=Endpoint(host="127.0.0.1", port=2, options={
                  "tls": False}), databases=["neo4j"])
    eng = Neo4jEngine(hop)
    eng.said = []
    return eng


def test_the_dump_is_passed_over_unless_both_sides_may_stop_the_database(
        neo):
    facts = decide.Facts(
        {"changed": False, "stoppable": False, "admin": True}, empty=lambda: True,
        sizes=lambda: {"ranges_s": 900.0, "bytes": 1e9}, disk=lambda: 2e8)
    got = decide.climb("moving neo4j", neo._move_ways(), facts)
    assert got.path == "ranges"
    assert "stop_database" in dict((r.name, w) for r, w in got.passed)["dump"]


def test_the_dump_needs_a_graph_the_ranges_take_longer_over(neo):
    def pick(ranges_s):
        return decide.climb("moving neo4j", neo._move_ways(), decide.Facts(
            {"changed": False, "stoppable": True, "admin": True}, empty=lambda: True,
            sizes=lambda: {"ranges_s": ranges_s, "bytes": 1e9},
            disk=lambda: 1e8)).path
    assert pick(5.0) == "ranges"
    assert pick(3600.0) == "dump"


def test_a_scoped_graph_keeps_to_the_ranges_whatever_its_size(neo):
    got = decide.climb("moving neo4j", neo._move_ways(), decide.Facts(
        {"changed": True, "stoppable": True, "admin": True},
        empty=lambda: True, sizes=lambda: {"ranges_s": 3600.0, "bytes": 1e9},
        disk=lambda: 1e8))
    assert got.path == "ranges"
    assert "label or type scope" in dict(
        (r.name, w) for r, w in got.passed)["dump"]


def test_a_target_holding_elements_keeps_to_the_ranges(neo):
    got = decide.climb("moving neo4j", neo._move_ways(), decide.Facts(
        {"changed": False, "stoppable": True, "admin": True}, empty=lambda: False,
        sizes=lambda: {"ranges_s": 3600.0, "bytes": 1e9}, disk=lambda: 1e8))
    assert got.path == "ranges"


def test_users_are_carried_without_their_passwords(neo, monkeypatch):
    ran = []

    def run(side, db, statement, **params):
        ran.append((side, db, statement, params))
        if statement.startswith("SHOW USERS"):
            return ([{"user": "neo4j", "roles": ["admin"], "suspended": False,
                      "home": None}] if side == "dst" else [
                {"user": "neo4j", "roles": ["admin"], "suspended": False,
                 "home": None},
                {"user": "ana", "roles": ["reader", "ghost"],
                 "suspended": True, "home": "sales"}])
        if "TO `ana`" in statement and "ghost" in statement:
            raise RuntimeError("Neo.ClientError: role does not exist")
        return []
    monkeypatch.setattr(neo, "_run", run)
    said = []
    made = neo.carry_database_objects("neo4j", said.append)
    assert made == ["ana"]
    create = [r for r in ran if "CREATE USER" in r[2]]
    assert len(create) == 1 and create[0][0] == "dst"
    assert "SET STATUS SUSPENDED" in create[0][2]
    assert "SET HOME DATABASE `sales`" in create[0][2]
    assert len(create[0][3]["pw"]) >= 24 and create[0][3]["pw"] not in " ".join(
        said)
    assert any("GRANT ROLE `reader` TO `ana`" in r[2] for r in ran)
    assert any("ana:ghost" in line for line in said)
    assert any("1 users made" in line for line in said)


def test_a_change_position_is_counted_beside_the_opaque_id(neo, monkeypatch):
    monkeypatch.setattr(neo, "_cdc_on", lambda db: None)
    rows = [{"id": f"opaque-{i}", "txId": i, "seq": 0, "event": {
        "eventType": "n", "operation": "c", "elementId": f"e{i}",
        "labels": ["P"], "state": {"after": {"labels": ["P"], "properties":
                                             {}}}}} for i in range(3)]
    monkeypatch.setattr(neo, "_run", lambda *a, **k: rows)
    out, token = neo.neutral_changes("src", "neo4j", {"cdc": "opaque-x",
                                                      "seen": 4})
    assert len(out) == 3
    assert token == {"cdc": "opaque-2", "seen": 7}
    assert neo.change_position(token) == 7
    assert neo.change_position(None) == 0


def test_the_changes_still_ahead_of_a_position_are_counted(neo, monkeypatch):
    monkeypatch.setattr(neo, "_run", lambda s, d, q, **k: [{"id": "a"}] * min(
        2, k["most"]))
    assert neo.changes_behind("src", "neo4j", {"cdc": "x"}, 10) == (2, True)
    assert neo.changes_behind("src", "neo4j", {"cdc": "x"}, 2) == (2, False)


def test_the_reach_says_the_change_id_is_opaque(neo):
    item = [i for i in neo.reach_items("src") if "changes" in i["item"]][0]
    assert "opaque" in item["detail"] and "counted" in item["detail"]
