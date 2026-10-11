"""What a DynamoDB check says beyond the items' digest (backlog
P0-PARITY.verify, .all), against the service's own API (moto, no account):

* **NULL, empty, absent**: the service keeps `{"NULL": true}`, `""` and an
  attribute left out as three things; an item copied through the neutral
  writer loses its NULL (a null is an attribute left out), and the deep
  check names every attribute whose three counts moved;
* **kinds of value**: an attribute holding a string on one side and a
  number on the other is named;
* **indexes and options**: a secondary index the target lacks, a time to
  live only the source has (it expires other items), a stream, billing or
  table class that differs; a setting the endpoint will not answer is
  said as not compared;
* **the indexes are made** once the rows are in - a global one, never a
  unique one, never a local one (it is made with its table);
* **the assessment**: items sized where they lie, the settings a move
  leaves for a person counted, the stream every followed table needs, the
  throughput the target account has, and what else writes into a target.
"""
import json

import pytest

from tests import dynamodb_on_moto as ddbm
from tests.dynamodb_on_moto import items, put, table

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("verify.value.null-empty", "dynamodb"),
    ("verify.value.type-drift", "dynamodb"),
    ("verify.structure.indexes", "dynamodb"),
    ("verify.structure.collection-options", "dynamodb"),
    ("schema.indexes", "dynamodb"),
    ("move.create-missing", "dynamodb"),
    ("move.sized", "dynamodb"),
    ("assess.engine-checks", "dynamodb"),
    ("assess.preflight", "dynamodb"),
    ("assess.capacity", "dynamodb"),
    ("assess.writers", "dynamodb"),
    ("assess.handwork", "dynamodb"),
]


@pytest.fixture
def aws(tmp_path, monkeypatch):
    with ddbm.service(tmp_path, monkeypatch) as ddb:
        yield ddb


def _deep(eng, scope):
    return next(r for r in eng.check_deep("shop") if r.scope == f"shop {scope}")


def test_a_null_that_arrived_absent_is_named(aws, tmp_path):
    table(aws, "shop.t")
    put(aws, "shop.t", 1, note={"NULL": True}, e="")
    put(aws, "shop.t", 2, note="n", e="")
    put(aws, "shop.t", 3, e="x")
    eng = ddbm.engine(tmp_path)
    ddbm.copy(eng)
    got = _deep(eng, "null-missing")
    assert got.status == "diff" and got.category == "value.null-empty"
    # item 1's NULL is absent on the target; the empty string stayed empty
    assert "t.note: null/empty/absent 1/0/1 on the source, 0/0/2 on the" \
        " target" in got.detail, got.detail
    assert ".e:" not in got.detail, got.detail
    # carried as the service holds it, the NULL stays one
    aws.put_item(TableName="shopcopy.t", Item={
        "id": {"N": "1"}, "note": {"NULL": True}, "e": {"S": ""}})
    assert _deep(eng, "null-missing").status == "ok"


def test_an_empty_value_that_arrived_absent_is_named(aws, tmp_path):
    table(aws, "shop.t")
    table(aws, "shopcopy.t")
    put(aws, "shop.t", 1, e="")
    put(aws, "shopcopy.t", 1)
    got = _deep(ddbm.engine(tmp_path), "null-missing")
    assert got.status == "diff" and "t.e: null/empty/absent 0/1/0" \
        in got.detail, got.detail


def test_an_attribute_that_holds_another_kind_of_value_is_named(aws,
                                                               tmp_path):
    table(aws, "shop.t")
    table(aws, "shopcopy.t")
    put(aws, "shop.t", 1, price={"N": "10"}, tags={"SS": ["a", "b"]})
    put(aws, "shopcopy.t", 1, price="10", tags={"SS": ["a", "b"]})
    eng = ddbm.engine(tmp_path)
    got = _deep(eng, "field types")
    assert got.status == "diff" and got.category == "value.type-drift"
    assert got.detail.startswith("t.price: N -> S"), got.detail
    assert "tags" not in got.detail


def test_a_table_too_large_to_read_whole_is_not_called_clean(aws, tmp_path):
    table(aws, "shop.t")
    table(aws, "shopcopy.t")
    for i in range(1, 6):
        put(aws, "shop.t", i, v="x")
        put(aws, "shopcopy.t", i, v="x")
    eng = ddbm.engine(tmp_path)
    eng.DEEP_MOST = 3
    for scope in ("null-missing", "field types"):
        got = _deep(eng, scope)
        assert got.status == "warn" and "more than 3 items, not read whole" \
            in got.detail, got.detail


def test_a_different_number_of_items_is_left_to_the_data_check(aws,
                                                               tmp_path):
    table(aws, "shop.t")
    table(aws, "shopcopy.t")
    put(aws, "shop.t", 1, v="x")
    got = _deep(ddbm.engine(tmp_path), "null-missing")
    assert got.status == "warn" and "1 items against 0" in got.detail, \
        got.detail


def test_a_missing_index_and_a_different_ttl_are_named(aws, tmp_path):
    table(aws, "shop.t", indexes=[("by_kind", "kind")])
    table(aws, "shopcopy.t")
    aws.update_time_to_live(TableName="shop.t", TimeToLiveSpecification={
        "Enabled": True, "AttributeName": "expires"})
    eng = ddbm.engine(tmp_path)
    idx = _deep(eng, "indexes")
    assert idx.status == "diff" and "by_kind(kind/ALL) on the source, none" \
        in idx.detail, idx.detail
    opts = _deep(eng, "table options")
    assert opts.status == "diff" and opts.category == \
        "structure.collection-options"
    assert "t.ttl: expires on the source, off on the target" in opts.detail
    assert "expires other items" in opts.fix_hint


def test_a_stream_that_differs_is_a_warning_not_a_difference(aws, tmp_path):
    table(aws, "shop.t")
    table(aws, "shopcopy.t", stream=False)
    opts = _deep(ddbm.engine(tmp_path), "table options")
    assert opts.status == "warn" and "t.stream: NEW_AND_OLD_IMAGES on the" \
        " source, off on the target" in opts.detail, opts.detail


def test_equal_tables_are_equal_in_every_finding(aws, tmp_path):
    for side in ("shop", "shopcopy"):
        table(aws, f"{side}.t", indexes=[("by_kind", "kind")])
        put(aws, f"{side}.t", 1, kind="k", e="")
    eng = ddbm.engine(tmp_path)
    for scope in ("null-missing", "field types", "indexes", "table options"):
        assert _deep(eng, scope).status == "ok", scope


def test_a_setting_the_endpoint_will_not_answer_is_said_not_compared(
        aws, tmp_path, monkeypatch):
    table(aws, "shop.t")
    table(aws, "shopcopy.t")
    eng = ddbm.engine(tmp_path)
    real = eng._settings

    def settings(side, db):
        got = real(side, db)
        got["t.ttl"] = "not answered: not supported"
        return got
    monkeypatch.setattr(eng, "_settings", settings)
    opts = _deep(eng, "table options")
    assert opts.status == "warn" and "1 setting the endpoint would not" \
        " answer were not compared" in opts.detail, opts.detail


# ---- the indexes, made once the rows are in --------------------------------

def test_a_global_index_is_made_on_the_copy_once_its_rows_are_in(aws,
                                                                 tmp_path):
    table(aws, "shop.t", indexes=[("by_kind", "kind")])
    for i in range(1, 4):
        put(aws, "shop.t", i, kind=f"k{i % 2}")
    eng = ddbm.engine(tmp_path)
    ddbm.copy(eng)
    target = aws.describe_table(TableName="shopcopy.t")["Table"]
    assert not target.get("GlobalSecondaryIndexes")
    logged = []
    eng.finish_created("shop", logged.append)
    target = aws.describe_table(TableName="shopcopy.t")["Table"]
    index = target["GlobalSecondaryIndexes"][0]
    assert index["IndexName"] == "by_kind" and index["IndexStatus"] == \
        "ACTIVE" and index["KeySchema"] == [
            {"AttributeName": "kind", "KeyType": "HASH"}]
    assert eng.neutral_indexes("dst", "shop", "t") == [
        ("by_kind", False, ["kind"], True)]
    assert _deep(eng, "indexes").status == "ok"
    # and it serves the query it was made for
    got = aws.query(TableName="shopcopy.t", IndexName="by_kind",
                    KeyConditionExpression="kind = :k",
                    ExpressionAttributeValues={":k": {"S": "k1"}})
    assert sorted(i["id"]["N"] for i in got["Items"]) == ["1", "3"]


def test_an_index_the_service_cannot_make_is_refused_by_name(aws, tmp_path):
    table(aws, "shop.t")
    table(aws, "shopcopy.t")
    eng = ddbm.engine(tmp_path)
    for columns, unique, said in (
            (["kind"], True, "enforces no unique index"),
            (["a", "b", "c"], False, "partition attribute and at most one")):
        sql = eng.neutral_create_index_sql("dst", "shop", "t", "ix", unique,
                                           columns)
        with pytest.raises(RuntimeError, match=said):
            eng.execute_ddl("dst", "shop", sql)
    assert not aws.describe_table(TableName="shopcopy.t")["Table"].get(
        "GlobalSecondaryIndexes")


def test_a_local_index_is_read_and_is_not_made_after_the_table(aws,
                                                               tmp_path):
    aws.create_table(
        TableName="shop.t", BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"},
                   {"AttributeName": "at", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"},
                              {"AttributeName": "at", "AttributeType": "S"},
                              {"AttributeName": "by", "AttributeType": "S"}],
        LocalSecondaryIndexes=[{
            "IndexName": "by_by", "KeySchema": [
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "by", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "KEYS_ONLY"}}])
    eng = ddbm.engine(tmp_path)
    # read, with the projection it does not hold in full
    assert eng.neutral_indexes("src", "shop", "t") == [
        ("by_by", False, ["id", "by"], False)]


# ---- the move: made, sized ----------------------------------------------------

def test_a_table_the_target_lacks_is_made_with_the_sources_key(aws, tmp_path):
    table(aws, "shop.t", sort="at")
    aws.put_item(TableName="shop.t", Item={"id": {"N": "1"},
                                           "at": {"S": "a"}})
    eng = ddbm.engine(tmp_path)
    said = []
    eng.create_missing("shop", said.append)
    assert any("t: not on the target, created it" in m for m in said), said
    made = aws.describe_table(TableName="shopcopy.t")["Table"]
    assert [(k["AttributeName"], k["KeyType"]) for k in made["KeySchema"]] \
        == [("id", "HASH"), ("at", "RANGE")]
    spec = json.loads(said[0].split(" - ", 1)[1])
    assert spec["key"] == ["id", "at"]


def test_items_are_sized_where_they_lie(aws, tmp_path):
    table(aws, "shop.t")
    put(aws, "shop.t", 1, v="x" * 100)
    put(aws, "shop.t", 2, v="y" * 1000, n={"N": "123456"},
        doc={"M": {"a": {"S": "bb"}, "l": {"L": [{"BOOL": True}]}}})
    eng = ddbm.engine(tmp_path)
    sized = eng.row_sizes("src", "shop")["t"]
    # name + value: "id"(2) + 1-digit number(2) + "v"(1) + 100
    assert eng._item_bytes({"id": {"N": "1"}, "v": {"S": "x" * 100}}) == 105
    assert sized["largest"] > 1000 and sized["exact"] is True \
        and sized["rows"] == 2
    assert sized["mean"] < sized["largest"]
    # a sample of a larger table is no bound on the rest
    eng.SIZE_SAMPLE = 1
    got = eng.row_sizes("src", "shop", ["t"])["t"]
    assert got["exact"] is False


# ---- the assessment ------------------------------------------------------------

def test_a_followed_table_without_a_stream_is_named_before_the_copy(
        aws, tmp_path):
    table(aws, "shop.a")
    table(aws, "shop.b", stream=False)
    eng = ddbm.engine(tmp_path)
    got = eng._follow_readiness("shop")
    assert got.status == "warn" and "1 source tables have no stream on (b)" \
        in got.detail, got.detail
    assert [r for r in eng._preflight_items()
            if r["item"] == "shop change streams"][0]["level"] == "warn"
    table(aws, "shop.b2", stream=False)
    aws.delete_table(TableName="shop.b")
    aws.delete_table(TableName="shop.b2")
    assert eng._follow_readiness("shop").status == "ok"


def test_what_the_move_leaves_for_a_person_is_counted_and_named(aws,
                                                               tmp_path):
    table(aws, "shop.t")
    table(aws, "shop.plain", stream=False)
    aws.update_time_to_live(TableName="shop.t", TimeToLiveSpecification={
        "Enabled": True, "AttributeName": "expires"})
    aws.update_continuous_backups(
        TableName="shop.t",
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True})
    rows = ddbm.engine(tmp_path)._assess_extra()
    said = {r["item"]: r for r in rows}
    left = next(r for r in rows if "objects whose contents no data mover"
                in r["item"])
    assert left["level"] == "warn" and "time to live, which expires items:" \
        " 1 (t)" in left["detail"] and "streams, the change log of a way" \
        " back: 1 (t)" in left["detail"], left
    decide = next(r for r in rows if "settings that need a decision"
                  in r["item"])
    assert "point-in-time recovery: 1 (t)" in decide["detail"], decide
    assert "plain" not in left["detail"] + decide["detail"], said


def test_the_throughput_the_target_account_can_give_is_measured(aws,
                                                                tmp_path):
    aws.create_table(
        TableName="shop.big", BillingMode="PROVISIONED",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
        ProvisionedThroughput={"ReadCapacityUnits": 30,
                               "WriteCapacityUnits": 20})
    eng = ddbm.engine(tmp_path)
    room = eng.capacity("dst", "shop")
    assert room["read_units_free"] == 20000 - 30
    assert room["write_units_free"] == 20000 - 20
    assert room["table_write_units_most"] == 10000
    rows = [r for r in eng._assess_extra()
            if "room for provisioned" in r["item"]]
    assert rows[0]["level"] == "pass" and "provisions 30 read and 20 write" \
        in rows[0]["detail"], rows
    # an account too full for what the source provisions
    eng.capacity = lambda side, db: {"read_units_free": 10,
                                     "write_units_free": 5000}
    rows = [r for r in eng._assess_extra()
            if "room for provisioned" in r["item"]]
    assert rows[0]["level"] == "fail" and "read 30 units against 10 free" \
        in rows[0]["detail"], rows
    # too many tables to sum is unknown, never room
    eng.capacity = lambda side, db: {"table_read_units_most": 10000}
    rows = [r for r in eng._assess_extra()
            if "room for provisioned" in r["item"]]
    assert rows[0]["level"] == "warn" and "unknown, not clean" in \
        rows[0]["detail"]


def test_the_sum_of_an_account_with_too_many_tables_is_left_out(aws,
                                                                tmp_path):
    for i in range(3):
        table(aws, f"shop.t{i}")
    eng = ddbm.engine(tmp_path)
    eng.ACCOUNT_TABLES = 2
    room = eng.capacity("dst", "shop")
    assert "read_units_free" not in room and room["table_read_units_most"]


def test_what_else_writes_into_a_target_is_named(aws, tmp_path, monkeypatch):
    table(aws, "shopcopy.t")
    eng = ddbm.engine(tmp_path)
    assert eng.stream_writers("shop") == []
    aws.update_time_to_live(TableName="shopcopy.t",
                            TimeToLiveSpecification={
                                "Enabled": True, "AttributeName": "exp"})
    got = eng.stream_writers("shop")
    assert got == [("the service deleting t's expired items (time to live"
                    " on exp)", False)], got
    # a global table's replica in another region writes into it too
    real = eng._client("dst")

    class Client:
        def __getattr__(self, name):
            return getattr(real, name)

        def describe_table(self, **kw):
            got = real.describe_table(**kw)
            got["Table"]["Replicas"] = [{"RegionName": "eu-west-1"}]
            return got
    monkeypatch.setattr(eng, "_client", lambda side: Client())
    names = [w for w, pausable in eng.stream_writers("shop")]
    assert "t is a global table, replicated to and from eu-west-1" in names
    rows = eng._writer_items("shop")
    assert rows[0]["level"] == "fail" and "global table" in rows[0]["detail"]
