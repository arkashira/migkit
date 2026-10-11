"""The deep check of an OpenSearch hop finds where an index and its twin
drift apart, as findings of the categories the other engines' deep checks
name (P0-PARITY.verify): a field that differs (structure.columns), a number
mapped narrower (value.narrowing), digits lost (value.precision), text
analysed another way (value.collation), a value the cluster computes
(value.generated), the index's own options (structure.collection-options),
a field of another kind of value (value.type-drift) and a null that became
empty or absent (value.null-empty).

On a model of the cluster that needs no server (`tests/opensearch_fake`):
only the requests the engine already makes are asked.
"""
from migkit.config import Endpoint, Hop
from tests.opensearch_fake import Cluster, installed

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.structure.columns", "opensearch"),
          ("verify.value.narrowing", "opensearch"),
          ("verify.value.precision", "opensearch"),
          ("verify.value.collation", "opensearch"),
          ("verify.value.generated", "opensearch"),
          ("verify.value.null-empty", "opensearch"),
          ("verify.value.type-drift", "opensearch"),
          ("verify.structure.collection-options", "opensearch")]

SAME = {"title": {"type": "text", "analyzer": "standard"},
        "n": {"type": "long"},
        "tag": {"type": "keyword", "ignore_above": 256},
        "price": {"type": "double"},
        "amount": {"type": "scaled_float", "scaling_factor": 100},
        "at": {"type": "date_nanos"},
        "note": {"type": "keyword"}}


def _engine(tmp_path, **kw):
    from migkit.engines.opensearch import OpenSearchEngine
    hop = Hop(name="osos", engine="opensearch",
              source=Endpoint(host="src.example.com", port=9200),
              target=Endpoint(host="dst.example.com", port=9200),
              databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return OpenSearchEngine(hop)


def _docs():
    return {str(i): {"title": f"t{i}", "n": i, "tag": "x", "price": 1.5,
                     "amount": 2.5, "at": "2024-01-01T00:00:00.123456789Z",
                     "note": "n" if i % 2 else None}
            for i in range(1, 7)}


def _deep(tmp_path, src, dst, **kw):
    with installed(src, dst):
        return {r.scope.split(" ", 1)[1]: r
                for r in _engine(tmp_path, **kw).check_deep("shop")}


def _clusters(src_map=None, dst_map=None, src_docs=None, dst_docs=None,
              src_settings=None, dst_settings=None, src_shards=1,
              dst_shards=1, src_extra=None, dst_extra=None):
    src, dst = Cluster(), Cluster()
    src.index("shop.orders", dict(SAME, **(src_map or {})),
              _docs() if src_docs is None else src_docs,
              shards=src_shards, settings=src_settings,
              mappings=src_extra)
    dst.index("shop.orders", dict(SAME, **(dst_map or {})),
              _docs() if dst_docs is None else dst_docs,
              shards=dst_shards, settings=dst_settings,
              mappings=dst_extra)
    return src, dst


def test_twins_that_agree_are_found_to_agree(tmp_path):
    got = _deep(tmp_path, *_clusters())
    drift = {k: (r.status, r.detail) for k, r in got.items()
             if r.status != "ok"}
    assert drift == {}, drift
    assert {r.category for r in got.values()} >= {
        "structure.columns", "value.narrowing", "value.precision",
        "value.collation", "value.generated",
        "structure.collection-options", "value.type-drift",
        "value.null-empty"}


def test_a_field_that_differs_is_a_columns_finding(tmp_path):
    got = _deep(tmp_path, *_clusters(
        dst_map={"title": {"type": "keyword"}, "extra": {"type": "long"}}))
    found = got["columns"]
    assert found.status == "diff" and found.category == "structure.columns"
    assert "orders.title: text -> keyword" in found.detail
    assert "orders.extra: only on the target" in found.detail
    assert all(got[k].status == "ok" for k in ("narrowing", "precision"))


def test_a_number_mapped_narrower_is_a_narrowing_finding(tmp_path):
    got = _deep(tmp_path, *_clusters(
        dst_map={"n": {"type": "integer"},
                 "tag": {"type": "keyword", "ignore_above": 64}}))
    found = got["narrowing"]
    assert found.status == "diff" and found.category == "value.narrowing"
    assert "orders.n: long -> integer" in found.detail
    assert "orders.tag: ignore_above 256 -> 64" in found.detail
    # a wider one on the target loses nothing: a difference, not a narrowing
    wider = _deep(tmp_path, *_clusters(src_map={"n": {"type": "integer"}}))
    assert wider["narrowing"].status == "ok"
    assert "orders.n: integer -> long" in wider["columns"].detail


def test_fewer_digits_kept_is_a_precision_finding(tmp_path):
    got = _deep(tmp_path, *_clusters(
        dst_map={"price": {"type": "float"},
                 "amount": {"type": "scaled_float", "scaling_factor": 10},
                 "at": {"type": "date"}}))
    found = got["precision"]
    assert found.status == "diff" and found.category == "value.precision"
    for words in ("orders.price: double -> float",
                  "orders.amount: scaling_factor 100 -> 10",
                  "orders.at: date_nanos -> date"):
        assert words in found.detail, found.detail


def test_text_analysed_another_way_is_a_collation_finding(tmp_path):
    got = _deep(tmp_path, *_clusters(
        dst_map={"title": {"type": "text", "analyzer": "english"}},
        src_settings={"analysis.analyzer.body.type": "standard"},
        dst_settings={"analysis.analyzer.body.type": "english"}))
    found = got["collation"]
    assert found.status == "diff" and found.category == "value.collation"
    assert "orders.title: analyzer 'standard' -> 'english'" in found.detail
    assert "analysis.analyzer.body.type" in found.detail


def test_a_value_the_cluster_computes_is_a_generated_finding(tmp_path):
    got = _deep(tmp_path, *_clusters(
        dst_map={"note": {"type": "keyword", "copy_to": "title"}},
        dst_settings={"default_pipeline": "stamp"}))
    found = got["generated"]
    assert found.status == "diff" and found.category == "value.generated"
    assert "default_pipeline None -> 'stamp'" in found.detail
    assert "orders.note: copy_to None -> 'title'" in found.detail


def test_the_options_of_the_index_are_compared(tmp_path):
    got = _deep(tmp_path, *_clusters(
        src_shards=2, dst_shards=1, src_extra={"dynamic": "strict"},
        dst_settings={"codec": "best_compression"}))
    found = got["index options"]
    assert found.status == "diff"
    assert found.category == "structure.collection-options"
    for words in ("number_of_shards '2' -> '1'", "mapping dynamic 'strict'",
                  "codec None -> 'best_compression'"):
        assert words in found.detail, found.detail


def test_a_field_of_another_kind_of_value_is_a_type_drift(tmp_path):
    dst_docs = _docs()
    dst_docs["2"]["n"] = "2"           # a number that became a string
    got = _deep(tmp_path, *_clusters(dst_docs=dst_docs))
    found = got["field types"]
    assert found.status == "diff" and found.category == "value.type-drift"
    assert "orders.n: integer 6 -> 5, string 0 -> 1" in found.detail
    assert got["null and empty"].status == "ok"


def test_null_empty_and_absent_are_told_apart(tmp_path):
    dst_docs = _docs()
    dst_docs["1"]["note"] = ""         # was "n": not null; here empty
    dst_docs["2"]["note"] = None       # was null; still null
    del dst_docs["4"]["note"]          # was null; now absent
    got = _deep(tmp_path, *_clusters(dst_docs=dst_docs))
    found = got["null and empty"]
    assert found.status == "diff" and found.category == "value.null-empty"
    assert "orders.note:" in found.detail
    assert "null 3 -> 2" in found.detail and "absent 0 -> 1" in found.detail
    assert "empty 0 -> 1" in found.detail


def test_twins_with_different_numbers_of_documents_are_not_compared(
        tmp_path):
    """Counts of kinds over different sets differ for that reason alone:
    the finding says it did not compare them, and is no ok."""
    dst_docs = _docs()
    del dst_docs["6"]
    got = _deep(tmp_path, *_clusters(dst_docs=dst_docs))
    for key in ("field types", "null and empty"):
        assert got[key].status == "skip", got[key]
        assert "6 documents against 5" in got[key].detail


def test_an_index_that_cannot_be_read_is_no_ok(tmp_path):
    src, dst = _clusters()

    def refuse(method, path, body, query):
        raise RuntimeError(f"{method} {path}: 503 unavailable")
    dst.answers["/shop.orders"] = ("GET", refuse)
    got = _deep(tmp_path, src, dst)
    assert got["columns"].status == "skip", got["columns"]
    assert "orders" in got["columns"].detail


def test_an_index_with_more_field_paths_than_can_be_counted_is_said(
        tmp_path, monkeypatch):
    from migkit.engines import opensearch
    monkeypatch.setattr(opensearch, "MOST_PATHS", 3)
    got = _deep(tmp_path, *_clusters())
    assert got["field types"].status == "skip"
    assert "more than 3 field paths" in got["field types"].detail


def test_only_the_indexes_the_hop_takes_are_compared(tmp_path):
    src, dst = _clusters(dst_map={"n": {"type": "integer"}})
    left = _deep(tmp_path, src, dst, exclude=["shop.orders"])
    assert left["narrowing"].status == "ok"
    assert "orders" not in left["narrowing"].detail
