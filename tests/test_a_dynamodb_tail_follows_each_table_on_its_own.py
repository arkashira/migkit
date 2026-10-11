"""A DynamoDB table's stream is followed table by table (backlog
P0-PARITY.tail), against the service's own API (moto, no account):

* **a state of its own**: each table is live, or removed when it is dropped,
  or copied again when its stream is made again - while the others go on;
* **the hop's `ddl`**: a table dropped or made again is a DDL of the log; a
  hop whose `ddl` is stop stops there and says so, one that applies copies
  the table again and reads its new stream;
* **a table that comes back** gets only its changes since it left, read
  again from the stream where it left, where the service still holds them;
* **what the hop carries**: a hop whose `mapping.ops` carries no deletes
  keeps what the source deleted, and the check holds it to that;
* **a difference still arriving** is not a difference: a change the tail
  has read and not applied yet is confirmed, not called wrong.
"""
import ctypes
import threading
import time

import pytest

from migkit import tailctl, tailtables
from tests import dynamodb_on_moto as ddbm
from tests.dynamodb_on_moto import items, put, table

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("tail.table-states", "dynamodb"),
    ("tail.ddl", "dynamodb"),
    ("tail.readded", "dynamodb"),
    ("scope.ops", "dynamodb"),
    ("verify.confirm", "dynamodb"),
]


@pytest.fixture
def aws(tmp_path, monkeypatch):
    with ddbm.service(tmp_path, monkeypatch) as ddb:
        yield ddb


def _shop(ddb, tmp_path, **kw):
    """orders and items on the source, copied; the position taken first."""
    for t in ("orders", "items"):
        table(ddb, f"shop.{t}")
        for i in range(1, 4):
            put(ddb, f"shop.{t}", i, v=f"v{i}")
    eng = ddbm.engine(tmp_path, **kw)
    eng.tail_start("shop", tmp_path / "tail-token.json")
    ddbm.copy(eng)
    eng.finish_created("shop")
    return eng


def _state(tmp_path, t):
    return tailtables.states(tmp_path).get(t) or {}


def _wait(cond, what, seconds=60):
    end = time.time() + seconds
    while time.time() < end:
        if cond():
            return
        time.sleep(0.2)
    pytest.fail(f"never {what}")


def test_each_table_is_followed_in_a_state_of_its_own(aws, tmp_path):
    eng = _shop(aws, tmp_path, options={"ddl": "apply"})
    said = []
    with ddbm.Tail(eng, log=said.append) as tail:
        put(aws, "shop.orders", 7, v="new")
        aws.delete_item(TableName="shop.items", Key={"id": {"N": "1"}})
        assert tail.fence()
        assert items(aws, "shopcopy.orders") == items(aws, "shop.orders")
        assert items(aws, "shopcopy.items") == items(aws, "shop.items")
        assert {t: _state(tmp_path, t).get("state")
                for t in ("orders", "items")} == {"orders": "live",
                                                  "items": "live"}
        # items dropped on the source: out of the tail, its target left as
        # it is - and orders goes on
        aws.delete_table(TableName="shop.items")
        put(aws, "shop.orders", 8, v="after the drop")
        _wait(lambda: _state(tmp_path, "items").get("state") == "removed",
              "items removed")
        _wait(lambda: 8 in items(aws, "shopcopy.orders"), "orders goes on")
        assert "dropped on the source" in _state(tmp_path, "items")["why"]
        assert sorted(items(aws, "shopcopy.items")) == [2, 3]
        assert _state(tmp_path, "orders")["state"] == "live"
    assert not tail.ended or isinstance(tail.ended["e"], KeyboardInterrupt)


def test_a_table_made_again_is_copied_again_under_a_hop_that_applies(
        aws, tmp_path):
    eng = _shop(aws, tmp_path, options={"ddl": "apply"})
    with ddbm.Tail(eng, log=lambda m: None) as tail:
        put(aws, "shop.orders", 7, v="new")
        assert tail.fence()
        # dropped and made again: a new stream, the records between gone
        aws.delete_table(TableName="shop.orders")
        table(aws, "shop.orders")
        put(aws, "shop.orders", 1, v="made again")
        put(aws, "shop.orders", 9, v="after")
        _wait(lambda: items(aws, "shopcopy.orders")
              == items(aws, "shop.orders"), "orders level again")
        assert sorted(items(aws, "shopcopy.orders")) == [1, 9]
        _wait(lambda: _state(tmp_path, "orders").get("state") == "live",
              "orders live again")
        # and its new stream is followed
        put(aws, "shop.orders", 10, v="followed")
        _wait(lambda: 10 in items(aws, "shopcopy.orders"), "new stream read")
    assert not tail.ended or isinstance(tail.ended["e"], KeyboardInterrupt)


def test_a_table_made_again_stops_a_hop_whose_ddl_is_stop(aws, tmp_path):
    eng = _shop(aws, tmp_path)
    with ddbm.Tail(eng, log=lambda m: None) as tail:
        assert tail.fence()
        aws.delete_table(TableName="shop.orders")
        table(aws, "shop.orders")
        put(aws, "shop.orders", 1, v="made again")
        _wait(lambda: "e" in tail.ended, "the tail stops")
    said = str(tail.ended["e"])
    assert "orders was given or robbed of rows with no row event" in said \
        and "`ddl` is stop" in said, said
    # nothing of the new table reached the target
    assert sorted(items(aws, "shopcopy.orders")) == [1, 2, 3]
    assert items(aws, "shopcopy.orders")[1] == {"v": "v1"}


def test_a_table_dropped_between_the_listing_and_the_read_is_a_drop(
        aws, tmp_path, monkeypatch):
    eng = _shop(aws, tmp_path)
    token = eng.change_point("src", "shop")
    aws.delete_table(TableName="shop.items")
    real = eng.scope_tables
    # the listing still shows it: the drop is found at the read
    monkeypatch.setattr(eng, "scope_tables",
                        lambda side, db: sorted(set(real(side, db))
                                                | {"items"}))
    monkeypatch.setattr(eng, "neutral_tables",
                        lambda side, db: ["items", "orders"])
    got, after = eng.neutral_changes("src", "shop", token)
    assert [c for c in got if c["op"] == "ddl"] == [
        {"op": "ddl", "table": "items", "kind": "drop"}]
    assert "items" not in after["streams"] and "orders" in after["streams"]


def test_a_position_that_was_lost_is_said_not_read_on_from(aws, tmp_path):
    eng = _shop(aws, tmp_path)
    token = eng.change_point("src", "shop")
    assert eng.position_lost("src", "shop", token) is None
    # the stream turned off and on, or the table made again
    aws.delete_table(TableName="shop.items")
    table(aws, "shop.items")
    why = eng.position_lost("src", "shop", token)
    assert why and "items has a new stream since" in why, why
    aws.delete_table(TableName="shop.items")
    assert "items is no longer there" in eng.position_lost("src", "shop",
                                                           token)

    # and a record the service trimmed after its 24 hours
    put(aws, "shop.orders", 5, v="x")
    token = eng.change_point("src", "shop")
    streams = eng._streams("src")

    class Trimmed(Exception):
        pass

    class Client:
        exceptions = type("E", (), {
            "TrimmedDataAccessException": Trimmed,
            "ResourceNotFoundException": type("N", (Exception,), {})})

        def get_shard_iterator(self, **kw):
            raise Trimmed()
    eng._streams = lambda side: Client()
    why = eng.position_lost("src", "shop", {"streams": {
        "orders": token["streams"]["orders"]}})
    assert why and "no longer holds the records after" in why \
        and "24 hours passed" in why, why
    assert streams is not None


def test_a_table_comes_back_with_only_its_changes_since_it_left(
        aws, tmp_path, monkeypatch):
    import migkit.config
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n  ddb:\n    exclude: []\n")
    monkeypatch.setattr(migkit.config, "CONF", conf)
    eng = _shop(aws, tmp_path)
    pair = eng._as_pair()
    said, done = [], threading.Event()

    def run():
        try:
            pair.tail_apply("shop", True, tmp_path / "tail-token.json",
                            said.append)
        except BaseException as e:  # noqa: BLE001 - said in the lines
            said.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        _wait(lambda: (tmp_path / tailctl.PID).exists(), "running")
        put(aws, "shop.orders", 7, v="while in")
        _wait(lambda: 7 in items(aws, "shopcopy.orders"), "carried")
        # orders leaves the tail; the target keeps what it held
        conf.write_text("hops:\n  ddb:\n    exclude: [orders]\n")
        _wait(lambda: _state(tmp_path, "orders").get("state") == "removed",
              "orders removed")
        _wait(lambda: _state(tmp_path, "orders").get("digest"),
              "orders digested")
        put(aws, "shop.orders", 8, v="while out")
        aws.delete_item(TableName="shop.orders", Key={"id": {"N": "2"}})
        put(aws, "shop.items", 9, v="items goes on")
        _wait(lambda: 9 in items(aws, "shopcopy.items"), "items goes on")
        assert 8 not in items(aws, "shopcopy.orders")
        # and comes back: only what changed while it was out is read again
        conf.write_text("hops:\n  ddb:\n    exclude: []\n")
        _wait(lambda: items(aws, "shopcopy.orders")
              == items(aws, "shop.orders"), "orders level again")
        _wait(lambda: _state(tmp_path, "orders").get("state") == "live",
              "orders live")
        assert any("its changes since it left the tail, read again from"
                   " where it left" in line for line in said), said[-12:]
        put(aws, "shop.orders", 10, v="after")
        _wait(lambda: 10 in items(aws, "shopcopy.orders"), "followed again")
    finally:
        tailctl.stop(tmp_path)
        done.wait(60)
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            thread.join(20)
    assert not any(line.startswith("exit:") for line in said), said[-5:]


def test_a_hop_that_carries_no_deletes_keeps_what_the_source_deleted(
        aws, tmp_path):
    from migkit import verdict
    eng = _shop(aws, tmp_path,
                mapping={"ops": {"orders": "insert, update"}})
    with ddbm.Tail(eng, log=lambda m: None) as tail:
        aws.delete_item(TableName="shop.orders", Key={"id": {"N": "2"}})
        aws.delete_item(TableName="shop.items", Key={"id": {"N": "2"}})
        put(aws, "shop.orders", 5, v="new")
        assert tail.fence()
    # orders keeps item 2, items (carried in full) lost it
    assert sorted(items(aws, "shopcopy.orders")) == [1, 2, 3, 5]
    assert sorted(items(aws, "shopcopy.items")) == [1, 3]
    rs = [r.__dict__ for r in eng.check_data("shop")]
    assert verdict.decide(rs, eng.hop).status == "same", rs
    kept = [r for r in rs if r.get("kept_by_hop")]
    assert kept and "1 row the source deleted - deletes are not carried" \
        in kept[0]["kept_by_hop"], rs
    # a hop that carries every change finds it
    whole = ddbm.engine(tmp_path)
    assert verdict.decide([r.__dict__ for r in whole.check_data("shop")],
                          whole.hop).status == "different"


def test_a_change_the_tail_has_not_applied_is_still_arriving(
        aws, tmp_path, monkeypatch):
    from migkit.engines.dynamodb import DynamoDBEngine
    eng = _shop(aws, tmp_path)
    real = DynamoDBEngine._apply_upserts
    held = {"once": False}

    def slow(self, side, db, table, rows):
        if not held["once"]:
            held["once"] = True
            time.sleep(6)
        return real(self, side, db, table, rows)
    monkeypatch.setattr(DynamoDBEngine, "_apply_upserts", slow)
    with ddbm.Tail(eng, log=lambda m: None):
        _wait(lambda: tailctl.alive(tmp_path), "running")
        put(aws, "shop.orders", 4, v="read, held in its apply")
        time.sleep(2)
        got = [r for r in eng.check_data("shop") if r.check == "data"]
        assert [r.status for r in got] == ["ok", "ok"], [r.detail for r in got]
        assert any("still arriving" in r.detail for r in got), got
        # a difference made on the target alone is still one
        put(aws, "shopcopy.items", 1, v="wrong")
        got = [r for r in eng.check_data("shop") if r.check == "data"]
        assert "diff" in [r.status for r in got], [r.detail for r in got]
