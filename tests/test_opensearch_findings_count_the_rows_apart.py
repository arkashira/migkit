"""An OpenSearch data finding counts the rows apart table by table, says how
each table came out and names its table as the source names it - and what
it finds is repaired, with the target's old documents written down first
(P0-PARITY.verify). On a model of the cluster that needs no server
(`tests/opensearch_fake`).

The repair deletes documents by a bulk request, a request nothing has sent
to a real cluster yet: its cells wait on a server proof (`UNPROVED`).
"""
import json

from migkit.config import Endpoint, Hop
from tests.opensearch_fake import Cluster, installed

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.rows-apart", "opensearch"),
          ("verify.tables", "opensearch"), ("verify.table", "opensearch")]

PROPS = {"title": {"type": "text"}, "n": {"type": "long"}}


def _engine(tmp_path, **kw):
    from migkit.engines.opensearch import OpenSearchEngine
    hop = Hop(name="osos", engine="opensearch",
              source=Endpoint(host="src.example.com", port=9200),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return OpenSearchEngine(hop)


def _clusters():
    src, dst = Cluster(), Cluster()
    docs = {str(i): {"title": f"t{i}", "n": i} for i in range(1, 9)}
    for c in (src, dst):
        c.index("shop.orders", PROPS, docs)
        c.index("shop.lines", PROPS, {"a": {"title": "x", "n": 1}})
    # on the target: one document missing, one changed, one more
    dst.delete("shop.orders", "3")
    dst.write("shop.orders", "5", {"title": "other", "n": 5})
    dst.write("shop.orders", "99", {"title": "extra", "n": 99})
    return src, dst


def test_a_finding_counts_the_rows_apart_table_by_table(tmp_path):
    src, dst = _clusters()
    with installed(src, dst):
        got = {r.scope: r for r in _engine(tmp_path).check_data("shop")}
    orders, lines = got["shop.orders"], got["shop.lines"]
    assert orders.status == "diff" and lines.status == "ok"
    assert orders.rows_apart == {"orders": {
        "missing": 1, "extra": 1, "changed": 1, "exact": True}}, \
        orders.rows_apart
    # the table is named as the source names it, and each table's outcome
    # is said beside the finding
    assert orders.table == "orders" and lines.table == "lines"
    assert orders.tables == {"orders": "diff"}
    assert lines.tables == {"lines": "ok"} and not lines.rows_apart


def test_a_repair_brings_the_target_level_and_writes_down_what_it_held(
        tmp_path):
    src, dst = _clusters()
    with installed(src, dst):
        eng = _engine(tmp_path)
        eng.check_data("shop")
        plan = [a for a in eng.repair_plan("shop", "rows")
                if a.kind == "rows"]
        assert [a.scope for a in plan] == ["shop.orders"], plan
        for action in plan:
            eng.apply("shop", action)
        assert dst.sources("shop.orders") == src.sources("shop.orders")
        assert [r.status for r in eng.check_data("shop")] == ["ok", "ok"]
    saved = {tuple(json.loads(l)["key"]): json.loads(l)["row"]
             for l in (tmp_path / "undo" / "orders.rows.jsonl"
                       ).read_text().splitlines()}
    # what the repair overwrote or removed, as the target held it: the
    # changed document and the extra one - not the one it only added
    assert sorted(saved) == [("5",), ("99",)], saved
    assert saved[("5",)]["title"] == "other"
    assert saved[("99",)]["title"] == "extra"
