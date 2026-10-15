"""Qdrant and Neo4j as sides of a pair (backlog R16b, R16c).

Qdrant against a server in this process that answers its HTTP API as
documented - collections, payload indexes, scroll by offset, upsert,
retrieve, count, delete: a collection is made on the target as the
source's is, its points copied by id with their vectors and payload, a
payload changed on the target is a difference, and a payload index the
target lacks is a failed deep check.

Neo4j's change capture against answers shaped as its `db.cdc.query`
gives them: a node made, a relationship made, a node deleted, each one
change keyed by the source's id; a relationship type out of scope left
out; a database without enrichment refused, naming the setting.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from migkit.config import Endpoint, Hop

PROVES = [
    ("move.table-copy", "qdrant"),
    ("schema.create", "qdrant"),
    ("verify.structure.indexes", "qdrant"),
    ("tail.follows", "neo4j"),
]


class FakeQdrant:
    """Collections of points by id, answered as Qdrant's API answers."""

    def __init__(self):
        self.cols = {}
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        threading.Thread(target=self.server.serve_forever,
                         daemon=True).start()
        self.port = self.server.server_address[1]

    def make(self, name, vectors, indexes=None):
        self.cols[name] = {"vectors": vectors, "indexes": dict(indexes or {}),
                           "points": {}}

    def _answer(self, method, path, body):
        parts = path.split("?")[0].strip("/").split("/")
        if parts == ["collections"]:
            return {"collections": [{"name": n} for n in self.cols]}
        if parts == ["cluster"]:
            return {"status": "disabled"}
        name = parts[1]
        if len(parts) == 2 and method == "PUT":
            self.make(name, body["vectors"])
            return True
        col = self.cols.get(name)
        if col is None:
            return 404
        rest = parts[2:]
        if not rest:
            return {"config": {"params": {"vectors": col["vectors"]}},
                    "payload_schema": {k: {"data_type": v, "points": 0}
                                       for k, v in col["indexes"].items()}}
        pts = col["points"]
        if rest == ["index"]:
            col["indexes"][body["field_name"]] = body["field_schema"]
            return {"status": "acknowledged"}
        if rest == ["points", "count"]:
            return {"count": len(pts)}
        if rest == ["points", "scroll"]:
            ids = sorted(pts, key=lambda i: (isinstance(i, str), i))
            start = 0 if body.get("offset") is None else ids.index(
                body["offset"])
            page = ids[start:start + body["limit"]]
            nxt = ids[start + body["limit"]] \
                if start + body["limit"] < len(ids) else None
            return {"points": [pts[i] for i in page],
                    "next_page_offset": nxt}
        if rest == ["points"] and method == "PUT":
            for p in body["points"]:
                pts[p["id"]] = dict(p)
            return {"status": "completed"}
        if rest == ["points"] and method == "POST":
            return [pts[i] for i in body["ids"] if i in pts]
        if rest == ["points", "delete"]:
            for i in body["points"]:
                pts.pop(i, None)
            return {"status": "completed"}
        return 404

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                return None

            def _go(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"null")
                got = fake._answer(self.command, self.path, body)
                code, out = (404, {"status": {"error": "Not found"}}) \
                    if got == 404 else (200, {"result": got, "status": "ok"})
                data = json.dumps(out).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_PUT = do_POST = _go
        return Handler


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


@pytest.fixture
def qdrant(tmp_path):
    from migkit.engines.qdrant import QdrantEngine
    src, dst = FakeQdrant(), FakeQdrant()
    src.make("docs", {"size": 3, "distance": "Cosine"}, {"lang": "keyword"})
    for i in range(1, 8):
        src.cols["docs"]["points"][i] = {
            "id": i, "vector": [i / 10, 0.5, -i / 10],
            "payload": {"lang": "en" if i % 2 else "th", "n": i}}
    hop = Hop(name="vec", engine="qdrant",
              source=Endpoint(host="127.0.0.1", port=src.port,
                              options={"tls": False}),
              target=Endpoint(host="127.0.0.1", port=dst.port,
                              options={"tls": False}),
              databases=["qdrant"])
    hop.report_dir = lambda db=None: tmp_path
    yield QdrantEngine(hop), src, dst
    src.server.shutdown()
    dst.server.shutdown()


def _move(eng, tmp_path):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    for sch, t in eng.list_move_tables("qdrant"):
        eng.move_table("qdrant", sch, t, 3, ck, lambda m: None)


def test_a_collection_is_made_as_the_sources_and_its_points_copied(
        qdrant, tmp_path):
    eng, src, dst = qdrant
    _move(eng, tmp_path)
    got = dst.cols["docs"]
    assert got["vectors"] == {"size": 3, "distance": "Cosine"}
    assert got["indexes"] == {"lang": "keyword"}
    assert got["points"] == src.cols["docs"]["points"]
    cols = eng.neutral_columns("src", "qdrant", "docs")
    assert eng.neutral_digest("src", "qdrant", "docs", cols) == \
        eng.neutral_digest("dst", "qdrant", "docs", cols)
    dst.cols["docs"]["points"][4]["payload"]["n"] = 40
    assert eng.neutral_digest("src", "qdrant", "docs", cols) != \
        eng.neutral_digest("dst", "qdrant", "docs", cols)


def test_a_payload_index_the_target_lacks_fails_the_deep_check(qdrant):
    eng, src, dst = qdrant
    dst.make("docs", {"size": 3, "distance": "Cosine"})
    got = {r.scope: r for r in eng.check_deep("qdrant")}
    assert got["qdrant docs indexes"].status == "fail"
    assert "lang (keyword)" in got["qdrant docs indexes"].detail
    assert got["qdrant docs vectors"].status == "ok"
    dst.cols["docs"]["indexes"]["lang"] = "keyword"
    dst.cols["docs"]["vectors"] = {"size": 3, "distance": "Dot"}
    got = {r.scope: r for r in eng.check_deep("qdrant")}
    assert got["qdrant docs indexes"].status == "ok"
    assert got["qdrant docs vectors"].status == "fail"


def test_points_are_read_page_by_page_from_each_offset(qdrant):
    eng, src, dst = qdrant
    cols = eng.neutral_columns("src", "qdrant", "docs")
    pages = list(eng.neutral_batches("src", "qdrant", "docs", cols, size=3))
    assert [len(p) for p in pages] == [3, 3, 1]
    assert [r[0] for p in pages for r in p] == [str(i) for i in range(1, 8)]


# ---- Neo4j's change capture -------------------------------------------------

def _neo4j(tmp_path, answers, types=None):
    from migkit.engines.neo4j import Neo4jEngine
    opts = {"tls": False, **({"types": types} if types else {})}
    hop = Hop(name="graph", engine="neo4j",
              source=Endpoint(host="127.0.0.1", port=7474, options=opts),
              target=Endpoint(host="127.0.0.1", port=7475,
                              options={"tls": False}),
              databases=["neo4j"])
    hop.report_dir = lambda db=None: tmp_path
    eng = Neo4jEngine(hop)
    asked = []

    def run(side, db, statement, **params):
        asked.append((statement, params))
        for word, rows in answers.items():
            if word in statement:
                return rows(params) if callable(rows) else rows
        raise AssertionError(statement)
    eng._run = run
    return eng, asked


EVENTS = [
    {"id": "c1", "txId": 11, "seq": 0, "event": {
        "elementId": "4:x:1", "eventType": "n", "operation": "c",
        "labels": ["Person"],
        "state": {"before": None, "after": {
            "labels": ["Person"],
            "properties": {"_source_id": "p1", "name": "Ann"}}}}},
    {"id": "c2", "txId": 12, "seq": 0, "event": {
        "elementId": "5:x:9", "eventType": "r", "operation": "c",
        "type": "KNOWS",
        "start": {"elementId": "4:x:1", "labels": ["Person"],
                  "keys": {"Person": [{"_source_id": "p1"}]}},
        "end": {"elementId": "4:x:2", "labels": ["Person"],
                "keys": {}},
        "state": {"before": None, "after": {
            "properties": {"_source_id": "r1", "since": 2020}}}}},
    {"id": "c3", "txId": 13, "seq": 0, "event": {
        "elementId": "5:x:10", "eventType": "r", "operation": "c",
        "type": "LIKES", "start": {"elementId": "4:x:1"},
        "end": {"elementId": "4:x:2"},
        "state": {"before": None, "after": {"properties": {}}}}},
    {"id": "c4", "txId": 14, "seq": 0, "event": {
        "elementId": "4:x:3", "eventType": "n", "operation": "d",
        "labels": ["Person"],
        "state": {"before": {"labels": ["Person"],
                             "properties": {"_source_id": "p3"}},
                  "after": None}}},
]


def test_the_change_capture_is_followed_in_order_keyed_by_the_source_id(
        tmp_path):
    eng, asked = _neo4j(tmp_path, {
        "SHOW DATABASE": [{"options": {"txLogEnrichment": "FULL"}}],
        "db.cdc.current": [{"id": "c0"}],
        "db.cdc.query": EVENTS}, types=["KNOWS"])
    changes, token = eng.neutral_changes("src", "neo4j")
    assert changes == [] and token == {"cdc": "c0"}
    changes, token = eng.neutral_changes("src", "neo4j", token)
    assert token == {"cdc": "c4"}
    assert asked[-1][1]["from"] == "c0"
    got = [(c["op"], c["table"], c["key"]) for c in changes]
    assert got == [("insert", "nodes", {"id": "p1"}),
                   ("insert", "relationships", {"id": "r1"}),
                   ("delete", "nodes", {"id": "p3"})]
    node, rel = changes[0]["values"], changes[1]["values"]
    assert json.loads(node["properties"]) == {"name": "Ann"}
    assert node["labels"] == '["Person"]'
    assert (rel["from"], rel["to"], rel["type"]) == ("p1", "4:x:2", "KNOWS")
    assert json.loads(rel["properties"]) == {"since": 2020}


def test_a_database_without_enrichment_is_refused_naming_the_setting(
        tmp_path):
    eng, _ = _neo4j(tmp_path, {
        "SHOW DATABASE": [{"options": {"txLogEnrichment": "OFF"}}]})
    with pytest.raises(SystemExit) as e:
        eng.neutral_changes("src", "neo4j")
    assert "txLogEnrichment" in str(e.value)
