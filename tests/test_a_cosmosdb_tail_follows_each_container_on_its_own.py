"""A Cosmos DB container's change feed is followed container by container
(backlog P0-PARITY.tail), against a model of the service's SDK
(`tests/cosmos_fake.py`, no real account):

* **a state of its own**: each container is live, or removed when it is
  dropped, while the others go on;
* **the hop's `ddl`**: a container made or dropped is a DDL of the feed; a
  hop that applies copies a new container and follows it;
* **a container that comes back** gets only its changes since it left, read
  again from its feed ranges' continuations where the service still takes
  them - and is copied instead where it does not;
* **what the hop carries**: a hop whose `mapping.ops` carries no deletes
  keeps what the source deleted, and the check holds it to that;
* **a difference still arriving** is not a difference;
* **only what changed** is verified, from the feed.
"""
import ctypes
import threading
import time

import pytest

from migkit import tailctl, tailtables
from tests import cosmos_fake as cf
from tests.dynamodb_on_moto import Tail

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("tail.table-states", "cosmosdb"),
    ("tail.ddl", "cosmosdb"),
    ("tail.readded", "cosmosdb"),
    ("scope.ops", "cosmosdb"),
    ("verify.confirm", "cosmosdb"),
    ("verify.delta", "cosmosdb"),
]


@pytest.fixture
def account(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    client = cf.installed(monkeypatch)
    client.create_database("csrc")
    client.create_database("cdst")
    return client


def _shop(account, tmp_path, **kw):
    """orders and items on the source, copied; the position taken first."""
    src = account.get_database_client("csrc")
    for t in ("orders", "items"):
        c = src.create_container(t, cf.PartitionKey("/region"))
        for i in range(1, 4):
            cf.put(c, f"{t[0]}{i}", region="eu", v=f"v{i}")
    eng = cf.engine(tmp_path, **kw)
    eng.tail_start("csrc", tmp_path / "tail-token.json")
    cf.copy(eng)
    return eng


def _src(account, t):
    return account.get_database_client("csrc").get_container_client(t)


def _dst(account, t):
    return account.get_database_client("cdst").get_container_client(t)


def _state(tmp_path, t):
    return tailtables.states(tmp_path).get(t) or {}


def _wait(cond, what, seconds=30):
    end = time.time() + seconds
    while time.time() < end:
        if cond():
            return
        time.sleep(0.1)
    pytest.fail(f"never {what}")


def _ids(account, t, side=_dst):
    return sorted(cf.items(side(account, t)))


def test_each_container_is_followed_in_a_state_of_its_own(account, tmp_path):
    eng = _shop(account, tmp_path, options={"ddl": "apply"})
    with Tail(eng, "csrc", log=lambda m: None) as tail:
        cf.put(_src(account, "orders"), "o7", region="eu", v="new")
        _src(account, "items").delete_item("i1", partition_key="eu")
        assert tail.fence()
        assert cf.items(_dst(account, "orders")) \
            == cf.items(_src(account, "orders"))
        assert cf.items(_dst(account, "items")) \
            == cf.items(_src(account, "items"))
        assert {t: _state(tmp_path, t).get("state")
                for t in ("orders", "items")} == {"orders": "live",
                                                  "items": "live"}
        # items dropped on the source: out of the tail, its target left as
        # it is - and orders goes on
        account.get_database_client("csrc").containers.pop("items")
        cf.put(_src(account, "orders"), "o8", region="eu", v="after")
        _wait(lambda: _state(tmp_path, "items").get("state") == "removed",
              "items removed")
        _wait(lambda: "o8" in cf.items(_dst(account, "orders")),
              "orders goes on")
        assert "dropped on the source" in _state(tmp_path, "items")["why"]
        assert _ids(account, "items") == ["i2", "i3"]
        assert _state(tmp_path, "orders")["state"] == "live"
    assert not tail.ended or isinstance(tail.ended["e"], KeyboardInterrupt)


def test_a_container_made_since_the_tail_began_is_copied_and_followed(
        account, tmp_path):
    eng = _shop(account, tmp_path, options={"ddl": "apply"})
    with Tail(eng, "csrc", log=lambda m: None) as tail:
        assert tail.fence()
        made = account.get_database_client("csrc").create_container(
            "fresh", cf.PartitionKey("/region"))
        for i in range(3):
            cf.put(made, f"f{i}", region="eu", v="x")
        _wait(lambda: _state(tmp_path, "fresh").get("state") == "live",
              "fresh followed")
        cf.put(made, "f9", region="eu", v="after")
        _wait(lambda: cf.items(_dst(account, "fresh")) == cf.items(made),
              "fresh level")
        assert sorted(cf.items(made)) == ["f0", "f1", "f2", "f9"]
    assert not tail.ended or isinstance(tail.ended["e"], KeyboardInterrupt)


def test_a_container_that_left_the_tail_comes_back_with_its_changes_since(
        account, tmp_path, monkeypatch):
    import migkit.config
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n  cosmos:\n    exclude: []\n")
    monkeypatch.setattr(migkit.config, "CONF", conf)
    eng = _shop(account, tmp_path)
    pair = eng._as_pair()
    said, done = [], threading.Event()

    def run():
        try:
            pair.tail_apply("csrc", True, tmp_path / "tail-token.json",
                            said.append)
        except BaseException as e:  # noqa: BLE001 - said in the lines
            said.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    orders = _src(account, "orders")
    try:
        _wait(lambda: (tmp_path / tailctl.PID).exists(), "running")
        cf.put(orders, "o7", region="eu", v="while in")
        _wait(lambda: "o7" in cf.items(_dst(account, "orders")), "carried")
        conf.write_text("hops:\n  cosmos:\n    exclude: [orders]\n")
        _wait(lambda: _state(tmp_path, "orders").get("state") == "removed",
              "orders removed")
        _wait(lambda: _state(tmp_path, "orders").get("digest"),
              "orders digested")
        cf.put(orders, "o8", region="eu", v="while out")
        orders.delete_item("o2", partition_key="eu")
        cf.put(_src(account, "items"), "i9", region="eu", v="items goes on")
        _wait(lambda: "i9" in cf.items(_dst(account, "items")),
              "items goes on")
        assert "o8" not in cf.items(_dst(account, "orders"))
        conf.write_text("hops:\n  cosmos:\n    exclude: []\n")
        _wait(lambda: cf.items(_dst(account, "orders")) == cf.items(orders),
              "orders level again")
        _wait(lambda: _state(tmp_path, "orders").get("state") == "live",
              "orders live")
        assert any("its changes since it left the tail, read again from"
                   " where it left" in line for line in said), said[-12:]
        cf.put(orders, "o10", region="eu", v="after")
        _wait(lambda: "o10" in cf.items(_dst(account, "orders")),
              "followed again")
    finally:
        tailctl.stop(tmp_path)
        done.wait(30)
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            thread.join(20)
    assert not any(line.startswith("exit:") for line in said), said[-5:]


def test_a_position_the_service_no_longer_takes_is_said(account, tmp_path):
    eng = _shop(account, tmp_path)
    token = eng.change_point("src", "csrc")
    assert eng.position_lost("src", "csrc", token) is None
    cf.put(_src(account, "orders"), "o5", region="eu", v="x")
    # the feed's retention passed it
    account.get_database_client("csrc").containers["orders"] \
        .expired_before = 10 ** 6
    why = eng.position_lost("src", "csrc", token)
    assert why and "change feed of orders no longer takes its saved" \
        " position" in why, why
    # a container that is gone is not what the position is lost for
    account.get_database_client("csrc").containers.pop("orders")
    assert eng.position_lost("src", "csrc", token) is None


def test_a_hop_that_carries_no_deletes_keeps_what_the_source_deleted(
        account, tmp_path):
    from migkit import verdict
    eng = _shop(account, tmp_path,
                mapping={"ops": {"orders": "insert, update"}})
    with Tail(eng, "csrc", log=lambda m: None) as tail:
        _src(account, "orders").delete_item("o2", partition_key="eu")
        _src(account, "items").delete_item("i2", partition_key="eu")
        cf.put(_src(account, "orders"), "o5", region="eu", v="new")
        assert tail.fence()
    assert _ids(account, "orders") == ["o1", "o2", "o3", "o5"]
    assert _ids(account, "items") == ["i1", "i3"]
    rs = [r.__dict__ for r in eng.check_data("csrc")]
    assert verdict.decide(rs, eng.hop).status == "same", rs
    kept = [r for r in rs if r.get("kept_by_hop")]
    assert kept and "1 row the source deleted - deletes are not carried" \
        in kept[0]["kept_by_hop"], rs
    whole = cf.engine(tmp_path)
    assert verdict.decide([r.__dict__ for r in whole.check_data("csrc")],
                          whole.hop).status == "different"


def test_a_change_the_tail_has_not_applied_is_still_arriving(
        account, tmp_path, monkeypatch):
    from migkit.engines.cosmosdb import CosmosEngine
    eng = _shop(account, tmp_path)
    real = CosmosEngine._apply_upserts
    held = {"once": False}

    def slow(self, side, db, table, rows):
        if not held["once"]:
            held["once"] = True
            time.sleep(4)
        return real(self, side, db, table, rows)
    monkeypatch.setattr(CosmosEngine, "_apply_upserts", slow)
    with Tail(eng, "csrc", log=lambda m: None):
        _wait(lambda: tailctl.alive(tmp_path), "running")
        cf.put(_src(account, "orders"), "o4", region="eu", v="held")
        time.sleep(1.5)
        got = [r for r in eng.check_data("csrc") if r.check == "data"]
        assert [r.status for r in got] == ["ok", "ok"], [r.detail for r in got]
        assert any("still arriving" in r.detail for r in got), got
        cf.put(_dst(account, "items"), "i1", region="eu", v="wrong")
        got = [r for r in eng.check_data("csrc") if r.check == "data"]
        assert "diff" in [r.status for r in got], [r.detail for r in got]


def test_only_what_the_feed_names_is_verified(account, tmp_path):
    eng = _shop(account, tmp_path)
    first = eng.delta_verify("csrc")
    assert first[0].status == "ok" and "baseline" in first[0].detail, \
        [r.detail for r in first]
    cf.put(_src(account, "orders"), "o2", region="eu", v="moved on")
    got = eng.delta_verify("csrc")
    assert "diff" in [r.status for r in got], [r.detail for r in got]
    cf.put(_dst(account, "orders"), "o2", region="eu", v="moved on")
    got = eng.delta_verify("csrc")
    assert all(r.status == "ok" for r in got), [r.detail for r in got]
