"""A Cosmos DB hop reads one scope everywhere (backlog P0-PARITY.scope) and
says its findings container by container (P0-PARITY.verify), against a
model of the service's SDK (`tests/cosmos_fake.py`: accounts, containers
with a partition key, feed ranges, a change feed - no real account):

* **the scope**: a container the hop leaves out (`exclude`) is not listed as
  one the hop has, not copied, not counted, compared or deep-checked, and
  not repaired - though it differs on the two sides and an earlier check
  left its drilldown;
* **the names**: a container the hop renames (`mapping.names`) is copied
  under its new name and checked against it;
* **the findings**: a container that differs says so by its name, counts the
  items apart - missing, extra and changed, one by one - and the finding
  says how every container it compared came out;
* **the repair** makes the target equal and writes down every item it
  overwrites or removes first;
* **a container the target lacks** is made with the source's partition key,
  and its settings are compared in one list.
"""
import json

import pytest

from tests import cosmos_fake as cf

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("scope.listings", "cosmosdb"),
    ("scope.deep", "cosmosdb"),
    ("scope.copy", "cosmosdb"),
    ("scope.repair", "cosmosdb"),
    ("scope.names", "cosmosdb"),
    ("verify.table", "cosmosdb"),
    ("verify.rows-apart", "cosmosdb"),
    ("verify.tables", "cosmosdb"),
    ("verify.repair", "cosmosdb"),
    ("verify.repair-undo", "cosmosdb"),
    ("move.create-missing", "cosmosdb"),
]


@pytest.fixture
def account(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    client = cf.installed(monkeypatch)
    client.create_database("csrc")
    client.create_database("cdst")
    return client


def _container(db, name, **kw):
    return db.create_container(name, cf.PartitionKey("/region"), **kw)


def _two(account):
    """orders and audit on the source and on the target - and each differs
    there: the target has item o9 the source lacks, lacks o2 and holds o1
    changed."""
    src = account.get_database_client("csrc")
    dst = account.get_database_client("cdst")
    for name in ("orders", "audit"):
        s, d = _container(src, name), _container(dst, name)
        for i in range(1, 5):
            cf.put(s, f"o{i}", region="eu", v=f"v{i}")
            cf.put(d, f"o{i}", region="eu", v=f"v{i}")
        cf.put(d, "o9", region="eu", v="only on the target")
        cf.put(d, "o1", region="eu", v="changed on the target")
        d.delete_item("o2", partition_key="eu")
    return src, dst


def _described(eng, db, name):
    """The target's containers are the ones migkit made: describe them as
    the source is, so the check can line the columns up."""
    return eng


def test_a_container_the_hop_leaves_out_is_not_listed_copied_or_checked(
        account, tmp_path):
    src, dst = _two(account)
    # migkit made neither; each is described by the items it holds
    whole = cf.engine(tmp_path)
    assert whole.neutral_tables("src", "csrc") == ["audit", "orders"]
    bad = [r.table for r in whole.check_data("csrc") if r.status == "diff"]
    assert sorted(bad) == ["audit", "orders"]

    eng = cf.engine(tmp_path, exclude=["audit"])
    assert eng.neutral_tables("src", "csrc") == ["orders"]
    assert eng.neutral_tables("dst", "csrc") == ["orders"]
    assert eng.scope_tables("src", "csrc") == ["audit", "orders"]
    assert eng.list_move_tables("csrc") == [("", "orders")]
    for check in (eng.check_counts("csrc"), eng.check_data("csrc"),
                  eng.check_schema("csrc")):
        text = " ".join(r.scope + " " + r.detail for r in check)
        assert "audit" not in text, text
    assert [r.table for r in eng.check_data("csrc")] == ["orders"]
    deep = {r.scope: r for r in eng.check_deep("csrc")}
    assert "1 containers" in deep["csrc indexes"].detail
    assert "audit" not in " ".join(r.detail for r in deep.values())


def test_a_renamed_container_is_copied_and_checked_under_its_new_name(
        account, tmp_path):
    src = account.get_database_client("csrc")
    s = _container(src, "orders")
    for i in range(1, 4):
        cf.put(s, f"o{i}", region="eu", v=f"v{i}")
    eng = cf.engine(tmp_path, mapping={"names": {"tables": {"prefix": "x_"}}})
    cf.copy(eng)
    dst = account.get_database_client("cdst")
    assert cf.items(dst.get_container_client("x_orders")) == cf.items(s)
    assert [r.status for r in eng.check_data("csrc")] == ["ok"]
    cf.put(dst.get_container_client("x_orders"), "o2", region="eu",
           v="drifted")
    bad = eng.check_data("csrc")
    assert [r.status for r in bad] == ["diff"] and bad[0].table == "orders"


def test_a_finding_counts_the_items_apart_container_by_container(
        account, tmp_path):
    src, dst = _two(account)
    s, d = _container(src, "same"), _container(dst, "same")
    cf.put(s, "o1", region="eu", v="a")
    cf.put(d, "o1", region="eu", v="a")
    found = {r.table: r for r in cf.engine(tmp_path).check_data("csrc")}
    assert found["orders"].status == "diff"
    assert found["orders"].rows_apart == {"orders": {
        "missing": 1, "extra": 1, "changed": 1, "exact": True}}
    assert found["same"].status == "ok" and not found["same"].rows_apart
    want = {"orders": "diff", "audit": "diff", "same": "ok"}
    assert all(r.tables == want for r in found.values())


def test_the_repair_makes_the_target_equal_and_keeps_what_it_overwrote(
        account, tmp_path):
    src, dst = _two(account)
    eng = cf.engine(tmp_path)
    assert {r.table for r in eng.check_data("csrc") if r.status == "diff"} \
        == {"orders", "audit"}
    plan = eng.repair_plan("csrc", "rows")
    assert sorted(a.table for a in plan) == ["audit", "orders"]
    for action in plan:
        eng.apply("csrc", action)
    assert all(r.status == "ok" for r in eng.check_data("csrc"))
    for t in ("orders", "audit"):
        assert cf.items(dst.get_container_client(t)) \
            == cf.items(src.get_container_client(t))
    undo = tmp_path / "undo" / "orders.rows.jsonl"
    saved = {json.loads(l)["key"][-1]: json.loads(l)["row"]
             for l in undo.read_text().splitlines()}
    assert sorted(saved) == ["o1", "o9"], saved
    assert saved["o9"]["v"] == "only on the target"
    assert saved["o1"]["v"] == "changed on the target"
    assert eng.repair_plan("csrc", "rows") == []


def test_a_repair_never_writes_into_a_container_the_hop_leaves_out(
        account, tmp_path):
    from migkit.engines.base import LEFT_OUT
    src, dst = _two(account)
    cf.engine(tmp_path).check_data("csrc")
    eng = cf.engine(tmp_path, exclude=["audit"])
    plan = eng.repair_plan("csrc", "rows")
    assert [a.table for a in plan if a.kind == "rows"] == ["orders"]
    said = [a for a in plan if a.kind == LEFT_OUT]
    assert said and "audit" in said[0].note
    for action in plan:
        if action.kind == "rows":
            eng.apply("csrc", action)
    assert cf.items(dst.get_container_client("orders")) \
        == cf.items(src.get_container_client("orders"))
    assert "o9" in cf.items(dst.get_container_client("audit"))


def test_a_container_the_target_lacks_is_made_with_the_sources_key(
        account, tmp_path):
    src = account.get_database_client("csrc")
    s = _container(src, "orders")
    cf.put(s, "o1", region="eu", v="a")
    eng = cf.engine(tmp_path)
    said = []
    eng.create_missing("csrc", said.append)
    assert any("orders: not on the target, created it" in m for m in said), \
        said
    made = account.get_database_client("cdst").get_container_client(
        "orders").read()
    assert made["partitionKey"]["paths"] == ["/region"]
    assert made["partitionKey"]["kind"] == "Hash"
    # described as the source is, so every column lines up
    assert eng.neutral_columns("dst", "csrc", "orders") \
        == eng.neutral_columns("src", "csrc", "orders")


def test_the_settings_are_one_list_and_a_different_key_is_named(account,
                                                                tmp_path):
    src = account.get_database_client("csrc")
    dst = account.get_database_client("cdst")
    _container(src, "orders", default_ttl=60)
    dst.create_container("orders", cf.PartitionKey("/id"))
    got = cf.engine(tmp_path).check_params("csrc")
    assert [type(r).__name__ for r in got] == ["Result"]
    assert got[0].status == "diff" and "orders partition key src=/region" \
        " dst=/id" in got[0].detail, got[0].detail
    assert "orders default ttl src=60 dst=None" in got[0].detail
