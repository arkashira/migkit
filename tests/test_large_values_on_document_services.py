"""Large values on Cosmos DB and Couchbase (backlog P0-PARITY.lv.new):
writes cut by the bytes the service takes, measured on what is sent, and
an item or document past the service's bound named before anything is
sent. The SDK containers are stand-ins that record each request; no
server runs, so the move cells stay in `capabilities.UNPROVED`."""
import json

import pytest

from migkit.engines import cosmosdb, couchbase


def body(i, n, pk="p"):
    return {"id": str(i), "pk": pk, "v": "x" * n}


def test_a_batch_that_would_pass_2_mb_is_split_not_sent():
    items = [body(i, 700_000) for i in range(5)]
    got = list(cosmosdb.transactional_batches(items))
    batches = [b for b, _ in got if b]
    assert sum(len(b) for b in batches) == 5
    assert len(batches) == 3
    for b in batches:
        sent = json.dumps([{"operationType": "Upsert", "resourceBody": x}
                           for x in b], separators=(",", ":"))
        assert len(sent) <= cosmosdb.REQUEST_BYTES


def test_a_batch_is_cut_at_its_last_byte_exactly():
    one = cosmosdb.upsert_bytes(body(0, 10))
    most = 2 + 3 * one + 2
    got = [b for b, _ in cosmosdb.transactional_batches(
        [body(i, 10) for i in range(4)], most_bytes=most)]
    assert [len(b) for b in got] == [3, 1]
    got = [b for b, _ in cosmosdb.transactional_batches(
        [body(i, 10) for i in range(4)], most_bytes=most - 1)]
    assert [len(b) for b in got] == [2, 2]


def test_text_past_ascii_is_measured_as_its_escaped_form():
    wide = {"id": "1", "v": "ก" * 10}
    assert cosmosdb.upsert_bytes(wide) > cosmosdb.item_bytes(wide) > 30


def test_count_still_cuts_a_batch_of_small_items():
    got = [b for b, _ in cosmosdb.transactional_batches(
        [body(i, 1) for i in range(250)])]
    assert [len(b) for b in got] == [100, 100, 50]


def test_an_operation_no_batch_holds_is_written_by_itself():
    most = cosmosdb.upsert_bytes(body(0, 50)) + 1
    got = list(cosmosdb.transactional_batches(
        [body(0, 10), body(1, 50), body(2, 10)], most_bytes=most))
    assert ([], [body(1, 50)]) in got
    assert all(len(b) == 1 for b, _ in got if b)


class Container:
    def __init__(self):
        self.batches, self.single = [], []

    def execute_item_batch(self, ops, partition_key=None):
        self.batches.append([b for _, (b,) in ops])

    def upsert_item(self, b):
        self.single.append(b)


def cosmos(monkeypatch, container):
    eng = cosmosdb.CosmosEngine.__new__(cosmosdb.CosmosEngine)
    monkeypatch.setattr(couchbase, "forget_columns", lambda e: None)
    eng._container = lambda *a: container
    eng._pk_fields = lambda *a: ["pk"]
    return eng


def test_cosmos_writes_a_partition_key_value_in_batches_under_2_mb(
        monkeypatch):
    c = Container()
    eng = cosmos(monkeypatch, c)
    eng._upsert("dst", "db", "t", [body(i, 900_000) for i in range(4)])
    assert sorted(b["id"] for x in c.batches for b in x) == list("0123")
    assert all(len(json.dumps(x)) <= cosmosdb.REQUEST_BYTES
               for x in c.batches)
    assert len(c.batches) == 2


def test_an_item_past_2_mb_stops_the_write_before_anything_is_sent(
        monkeypatch):
    c = Container()
    eng = cosmos(monkeypatch, c)
    items = [body(0, 10), body(1, cosmosdb.REQUEST_BYTES), body(2, 10)]
    with pytest.raises(SystemExit) as e:
        eng._upsert("dst", "db", "t", items)
    assert "'1'" in str(e.value) and "nothing" in str(e.value)
    assert c.batches == [] and c.single == []


class Multi:
    all_ok = True
    exceptions = {}


class Collection:
    def __init__(self):
        self.sent = []

    def upsert_multi(self, docs, return_exceptions=True):
        self.sent.append(dict(docs))
        return Multi()


def bucket(monkeypatch, coll):
    eng = couchbase.CouchbaseEngine.__new__(couchbase.CouchbaseEngine)
    monkeypatch.setattr(couchbase, "forget_columns", lambda e: None)
    eng._collection = lambda *a: coll
    return eng


def test_couchbase_writes_are_cut_by_bytes(monkeypatch):
    from migkit import pieces
    monkeypatch.setattr(pieces, "BATCH_BYTES", 3_000)
    coll = Collection()
    eng = bucket(monkeypatch, coll)
    docs = {str(i): {"v": "x" * 1_000} for i in range(6)}
    eng._upsert_docs("dst", "b", "s.c", docs)
    assert [len(d) for d in coll.sent] == [2, 2, 2]
    assert {k for d in coll.sent for k in d} == set(docs)


def test_a_document_past_20_mb_stops_the_write_before_anything_is_sent(
        monkeypatch):
    coll = Collection()
    eng = bucket(monkeypatch, coll)
    docs = {"a": {"v": "x"}, "b": {"v": "x" * couchbase.DOC_BYTES}}
    with pytest.raises(SystemExit) as e:
        eng._upsert_docs("dst", "b", "s.c", docs)
    assert "document b" in str(e.value)
    assert coll.sent == []


def test_couchbase_reads_are_handed_on_cut_by_bytes(monkeypatch):
    from migkit import pieces
    monkeypatch.setattr(pieces, "BATCH_BYTES", 2_500)

    class Got:
        def __init__(self, i):
            self.id, self.content_as = str(i), {dict: {"v": "y" * 1_000}}
    eng = couchbase.CouchbaseEngine.__new__(couchbase.CouchbaseEngine)
    eng._classes = lambda *a: {}
    eng._scan = lambda *a, **k: (Got(i) for i in range(5))
    got = list(eng.neutral_batches("src", "b", "s.c",
                                   [("_key", "text"), ("v", "text")]))
    assert [len(b) for b in got] == [2, 2, 1]
