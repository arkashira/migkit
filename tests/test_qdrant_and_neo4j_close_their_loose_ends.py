"""Qdrant and Neo4j: the loose ends closed (backlog R16b, R16c).

A column as these engines declare it (`string`, `json`) is folded by its
class, so their digests compare; every check they make says one of the
declared statuses; a collection holding only sparse vectors has its
recall measured through the sparse index; and a Neo4j fence waits on the
change log itself, comparing positions by the changes still ahead of each
because a change id says nothing of order.
"""
import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from migkit.engines.base import STATUSES
from tests.test_qdrant_and_neo4j_go_deeper import DeepQdrant


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


# ---- a declared type folds by its class --------------------------------------

def test_a_declared_string_folds_as_text_and_a_class_as_itself():
    assert canon.fold_class("qdrant", "string") == "text"
    assert canon.fold_class("neo4j", "string") == "text"
    assert canon.fold_class("neo4j", "json") == "json"
    assert canon.fold_class("qdrant", "integer") == "integer"
    assert canon.fold_class(None, "text") == "text"
    with pytest.raises(ValueError, match="'geohash'"):
        canon.fold_class("qdrant", "geohash", "where")


def test_a_qdrant_digest_is_the_digest_of_its_classes():
    from migkit.engines.qdrant import COLUMNS
    rows = [["1", [0.1, 0.2], {"lang": "en"}], ["2", [0.3, 0.4], {}]]
    declared = canon.fold_batches(COLUMNS, [rows], engine="qdrant")
    classed = canon.fold_batches(
        [("id", "text"), ("vector", "json"), ("payload", "json")], [rows])
    assert declared == classed and declared[0] == 2
    other = canon.fold_batches(COLUMNS, [[rows[0], ["2", [0.3, 0.5], {}]]],
                               engine="qdrant")
    assert other != declared
    with pytest.raises(ValueError):
        # without the engine the declared type is no class to render by
        canon.fold_batches(COLUMNS, [rows])


# ---- Qdrant ------------------------------------------------------------------

def _sparse_dot(a, b):
    w = dict(zip(b["indices"], b["values"]))
    return sum(v * w.get(i, 0.0) for i, v in zip(a["indices"], a["values"]))


class SparseQdrant(DeepQdrant):
    """The fake answering a query by a sparse vector: ranked by the dot
    product over shared indices, and for a `blind` server missing the
    nearest half when not asked to be exact."""

    def _answer(self, method, path, body):
        parts = path.split("?")[0].strip("/").split("/")
        q = (body or {}).get("query") if isinstance(body, dict) else None
        if parts[2:] == ["points", "query"] and isinstance(q, dict) \
                and "indices" in q:
            self.asked.append(body.get("using"))
            pts = list(self.cols[parts[1]]["points"].values())
            ranked = sorted(pts, key=lambda p: -_sparse_dot(
                q, p["vector"][body["using"]]))
            if self.blind and not body["params"]["exact"]:
                ranked = ranked[len(ranked) // 2:]
            return {"points": ranked[:body["limit"]]}
        return super()._answer(method, path, body)


def _sparse_pair(tmp_path, blind=False):
    from migkit.engines.qdrant import QdrantEngine
    src, dst = SparseQdrant(), SparseQdrant(blind=blind)
    for side in (src, dst):
        side.asked = []
        side.make("terms", {})
    for i in range(1, 31):
        src.cols["terms"]["points"][i] = {"id": i, "vector": {"text": {
            "indices": [i % 7, 7 + i % 5, 12 + i % 3],
            "values": [1.0 + i / 30, 0.5, 2.0 - i / 30]}}, "payload": {}}
    dst.cols["terms"]["points"].update(src.cols["terms"]["points"])
    hop = Hop(name="vec", engine="qdrant",
              source=Endpoint(host="127.0.0.1", port=src.port,
                              options={"tls": False}),
              target=Endpoint(host="127.0.0.1", port=dst.port,
                              options={"tls": False, "recall_k": 5}),
              databases=["qdrant"])
    hop.report_dir = lambda db=None: tmp_path
    return QdrantEngine(hop), src, dst


def test_a_sparse_only_collection_has_its_recall_measured(tmp_path):
    eng, src, dst = _sparse_pair(tmp_path)
    try:
        got = eng.recall_of("qdrant", "terms")
        assert isinstance(got, dict), got
        assert got["by"] == "sparse" and got["n"] > 0
        assert got["source"] == got["target"] == 1.0
        assert set(src.asked) == set(dst.asked) == {"text"}
        res = eng.check_recall("qdrant")
        assert [r.status for r in res] == ["ok"]
        assert "sparse vectors" in res[0].detail
    finally:
        src.server.shutdown()
        dst.server.shutdown()


def test_a_sparse_index_that_misses_the_nearest_points_is_a_difference(
        tmp_path):
    eng, src, dst = _sparse_pair(tmp_path, blind=True)
    try:
        got = eng.recall_of("qdrant", "terms")
        assert got["target"] < got["source"] and got["behind"] > 0
        res = eng.check_recall("qdrant")[0]
        assert res.status == "diff" and res.fix_hint
    finally:
        src.server.shutdown()
        dst.server.shutdown()


def test_every_qdrant_check_says_a_declared_status(tmp_path):
    from tests.test_qdrant_and_neo4j_go_deeper import _copy, _pair
    eng, src, dst = _pair(tmp_path, dst_blind=True)
    try:
        _copy(eng, src, dst)
        dst.settings["docs"]["hnsw_config"] = {"m": 8}
        dst.cols["docs"]["indexes"].clear()
        got = {r.scope: r.status for r in eng.check_deep("qdrant")}
        assert set(got.values()) <= set(STATUSES)
        assert got["qdrant docs hnsw"] == "diff"
        assert got["qdrant docs indexes"] == "diff"
        assert got["qdrant docs recall"] == "diff"
    finally:
        src.server.shutdown()
        dst.server.shutdown()


# ---- Neo4j -------------------------------------------------------------------

LOG = [f"c{i}" for i in range(1, 11)]


def _neo(tmp_path, lost=()):
    from migkit.engines.neo4j import Neo4jEngine
    hop = Hop(name="g", engine="neo4j",
              source=Endpoint(host="127.0.0.1", port=1, options={
                  "tls": False}),
              target=Endpoint(host="127.0.0.1", port=2, options={
                  "tls": False}), databases=["neo4j"])
    hop.report_dir = lambda db=None: tmp_path
    eng = Neo4jEngine(hop)
    eng.asked = []

    def run(side, db, statement, **params):
        eng.asked.append((statement, params))
        if "db.cdc.current" in statement:
            return [{"id": LOG[-1]}]
        if "db.cdc.query" in statement:
            if params["from"] in lost:
                raise RuntimeError("Neo.ClientError.ChangeDataCapture."
                                   "InvalidIdentifier: pruned")
            after = LOG[LOG.index(params["from"]) + 1:]
            return [{"id": i} for i in after[:params.get("most", 1)]]
        if "SHOW CONSTRAINTS" in statement or "SHOW INDEXES" in statement:
            return [{"name": "p_key"}] if side == "src" else []
        raise AssertionError(statement)
    eng._run = run
    return eng


def test_a_tail_is_past_the_fence_only_with_no_more_changes_ahead(tmp_path):
    eng = _neo(tmp_path)
    want = {"cdc": "c7"}
    assert eng.fence_reached("neo4j", {"cdc": "c5", "seen": 4}, want) is False
    assert eng.fence_reached("neo4j", {"cdc": "c8", "seen": 7}, want) is True
    # the tail's count stops one past the fence's: 3 ahead of c7, so 4
    assert eng.asked[-1][1]["most"] == 4
    eng.asked.clear()
    assert eng.fence_reached("neo4j", {"cdc": "c7"}, want) is True
    assert eng.asked == []


def test_a_tail_position_the_log_dropped_has_nothing_to_wait_on(tmp_path):
    eng = _neo(tmp_path, lost=("c2",))
    assert eng.fence_reached("neo4j", {"cdc": "c2"}, {"cdc": "c7"}) is None


def test_the_fence_waits_for_the_tail_to_reach_the_log(tmp_path, monkeypatch):
    from migkit import tailctl
    eng = _neo(tmp_path)
    tokens = iter([None, {"cdc": "c3"}, {"cdc": "c6"}, {"cdc": "c9"}])
    monkeypatch.setattr(tailctl, "saved", lambda path: next(tokens))
    slept = []
    monkeypatch.setattr("migkit.engines.neo4j.time.sleep", slept.append)
    assert eng.fence_wait("neo4j", {"cdc": "c8"}, timeout=60) is True
    assert len(slept) == 3 and min(slept) >= tailctl.POLL
    assert eng.fence_wait("neo4j", None) is None


def test_a_tail_that_never_arrives_times_out(tmp_path, monkeypatch):
    from migkit import tailctl
    eng = _neo(tmp_path)
    monkeypatch.setattr(tailctl, "saved", lambda path: {"cdc": "c1"})
    monkeypatch.setattr("migkit.engines.neo4j.time.sleep", lambda s: None)
    assert eng.fence_wait("neo4j", {"cdc": "c9"}, timeout=0.02) is False


def test_the_fence_point_is_where_the_log_is_while_a_tail_runs(
        tmp_path, monkeypatch):
    from migkit import tailctl
    eng = _neo(tmp_path)
    monkeypatch.setattr(tailctl, "alive", lambda where, hop=None: True)
    assert eng.src_lsn("neo4j") == {"cdc": "c10"}
    monkeypatch.setattr(tailctl, "alive", lambda where, hop=None: False)
    assert eng.src_lsn("neo4j") is None


def test_a_constraint_the_target_lacks_is_a_difference(tmp_path, monkeypatch):
    eng = _neo(tmp_path)
    monkeypatch.setattr(eng, "_constraints", lambda side, db: (
        ["UNIQUE P.k"] if side == "src" else []))
    monkeypatch.setattr(eng, "_indexes", lambda side, db: ["RANGE P.n"])
    got = {r.scope: r for r in eng.check_deep("neo4j")}
    assert {r.status for r in got.values()} <= set(STATUSES)
    assert got["neo4j constraints"].status == "diff"
    assert "UNIQUE P.k" in got["neo4j constraints"].detail
    assert got["neo4j indexes"].status == "ok"
