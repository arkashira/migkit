"""A DynamoDB hop reads one scope everywhere (backlog P0-PARITY.scope) and
says its findings table by table (P0-PARITY.verify), against the service's
own API (moto, no account).

* **the scope**: a table the hop leaves out (`exclude`) is not listed as
  one the hop has, not copied, not counted, compared or deep-checked, and
  not repaired - though it differs on the two sides and an earlier check
  left its drilldown;
* **the names**: a table the hop renames (`mapping.names`) is copied under
  its new name and checked against it, not reported missing;
* **the findings**: a table that differs says so by its name, counts the
  rows apart - missing, extra and changed, one by one - and the finding
  says how every table it compared came out;
* **the repair** makes the target equal and writes down every item it
  overwrites or removes first, so the repair can be turned back.
"""
import json

from tests import dynamodb_on_moto as ddbm
from tests.dynamodb_on_moto import items, put, table

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.listings", "dynamodb"),
    ("scope.deep", "dynamodb"),
    ("scope.copy", "dynamodb"),
    ("scope.repair", "dynamodb"),
    ("scope.names", "dynamodb"),
    ("verify.table", "dynamodb"),
    ("verify.rows-apart", "dynamodb"),
    ("verify.tables", "dynamodb"),
    ("verify.repair", "dynamodb"),
    ("verify.repair-undo", "dynamodb"),
]


import pytest


@pytest.fixture
def aws(tmp_path, monkeypatch):
    with ddbm.service(tmp_path, monkeypatch) as ddb:
        yield ddb


def _two_tables(ddb):
    """orders and audit on the source, the same on the target - and each
    differs there: the target has item 9 the source lacks, lacks 2 and
    holds 1 changed."""
    for side in ("shop", "shopcopy"):
        table(ddb, f"{side}.orders")
        table(ddb, f"{side}.audit")
    for t in ("orders", "audit"):
        for i in range(1, 5):
            put(ddb, f"shop.{t}", i, v=f"v{i}")
            put(ddb, f"shopcopy.{t}", i, v=f"v{i}")
        put(ddb, f"shopcopy.{t}", 9, v="only on the target")
        put(ddb, f"shopcopy.{t}", 1, v="changed on the target")
        ddb.delete_item(TableName=f"shopcopy.{t}", Key={"id": {"N": "2"}})


def test_a_table_the_hop_leaves_out_is_not_listed_copied_or_checked(
        aws, tmp_path):
    _two_tables(aws)
    whole = ddbm.engine(tmp_path)
    assert whole.neutral_tables("src", "shop") == ["audit", "orders"]
    # the premise: without the exclusion both tables differ
    bad = [r.table for r in whole.check_data("shop") if r.status == "diff"]
    assert sorted(bad) == ["audit", "orders"]

    eng = ddbm.engine(tmp_path, exclude=["audit"])
    assert eng.neutral_tables("src", "shop") == ["orders"]
    assert eng.neutral_tables("dst", "shop") == ["orders"]
    # the whole list stays for the scope's own entries to name
    assert eng.scope_tables("src", "shop") == ["audit", "orders"]
    assert eng.list_move_tables("shop") == [("", "orders")]

    for check in (eng.check_counts("shop"), eng.check_data("shop"),
                  eng.check_schema("shop")):
        text = " ".join(r.scope + " " + r.detail for r in check)
        assert "audit" not in text, text
    assert [r.table for r in eng.check_data("shop")] == ["orders"]
    deep = {r.scope: r for r in eng.check_deep("shop")}
    assert deep["shop source tables"].detail == "1 tables, every one active"
    assert deep["shop target tables"].detail == "1 tables, every one active"
    # and the shape and settings findings read one table too
    assert "1 tables" in deep["shop indexes"].detail


def test_a_renamed_table_is_copied_and_checked_under_its_new_name(
        aws, tmp_path):
    table(aws, "shop.orders")
    for i in range(1, 4):
        put(aws, "shop.orders", i, v=f"v{i}")
    eng = ddbm.engine(tmp_path, mapping={"names": {
        "tables": {"prefix": "x_"}}})
    ddbm.copy(eng)
    assert items(aws, "shopcopy.x_orders") == items(aws, "shop.orders")
    assert [r.status for r in eng.check_data("shop")] == ["ok"]
    assert [r.status for r in eng.check_counts("shop")] == ["ok"]
    put(aws, "shopcopy.x_orders", 2, v="drifted")
    bad = eng.check_data("shop")
    assert [r.status for r in bad] == ["diff"] and bad[0].table == "orders"


def test_a_finding_counts_the_rows_apart_table_by_table(aws, tmp_path):
    _two_tables(aws)
    table(aws, "shop.same")
    table(aws, "shopcopy.same")
    put(aws, "shop.same", 1, v="a")
    put(aws, "shopcopy.same", 1, v="a")
    eng = ddbm.engine(tmp_path)
    found = {r.table: r for r in eng.check_data("shop")}
    assert found["orders"].status == "diff"
    # one by one, the table named as the source names it
    assert found["orders"].rows_apart == {"orders": {
        "missing": 1, "extra": 1, "changed": 1, "exact": True}}
    assert found["same"].status == "ok" and not found["same"].rows_apart
    # every finding says how each table came out
    want = {"orders": "diff", "audit": "diff", "same": "ok"}
    assert all(r.tables == want for r in found.values())


def test_the_repair_makes_the_target_equal_and_keeps_what_it_overwrote(
        aws, tmp_path):
    _two_tables(aws)
    eng = ddbm.engine(tmp_path)
    assert {r.table for r in eng.check_data("shop") if r.status == "diff"} \
        == {"orders", "audit"}
    plan = eng.repair_plan("shop", "rows")
    assert sorted(a.table for a in plan) == ["audit", "orders"]
    for action in plan:
        eng.apply("shop", action)
    assert all(r.status == "ok" for r in eng.check_data("shop"))
    for t in ("orders", "audit"):
        assert items(aws, f"shopcopy.{t}") == items(aws, f"shop.{t}")
    # what the repair overwrote or removed was written down first
    undo = tmp_path / "undo" / "orders.rows.jsonl"
    saved = {json.loads(l)["key"][0]: json.loads(l)["row"]
             for l in undo.read_text().splitlines()}
    assert sorted(saved) == ["1", "9"], saved
    assert saved["9"]["v"] == "only on the target"
    assert saved["1"]["v"] == "changed on the target"
    assert eng.repair_plan("shop", "rows") == []


def test_a_repair_never_writes_into_a_table_the_hop_leaves_out(aws,
                                                                tmp_path):
    """A check run before `exclude: [audit]` left audit's drilldown; the
    repair under the scope it has now says so and leaves the table to the
    target."""
    from migkit.engines.base import LEFT_OUT
    _two_tables(aws)
    ddbm.engine(tmp_path).check_data("shop")
    eng = ddbm.engine(tmp_path, exclude=["audit"])
    plan = eng.repair_plan("shop", "rows")
    assert [a.table for a in plan if a.kind == "rows"] == ["orders"]
    said = [a for a in plan if a.kind == LEFT_OUT]
    assert said and "audit" in said[0].note
    for action in plan:
        if action.kind == "rows":
            eng.apply("shop", action)
    assert items(aws, "shopcopy.orders") == items(aws, "shop.orders")
    # audit is the target's own: it keeps the item it had
    assert 9 in items(aws, "shopcopy.audit")
