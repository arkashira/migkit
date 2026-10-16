"""A takeover reads the stopped mover's ledger from the machines' store,
the lease waits for its holder's measured jitter, and a topology reads its
key spaces, conflict rule and routing from the hops (backlog W1 `fleet`,
R3.topo).

Fake stores and fake hops only: the record is written over the version
read, as the machines' store does; no server is reached.
"""
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from migkit import attempts, claims, fleet, ha, hopkeys, topology
from migkit import lease as lease_mod
from migkit.config import Endpoint
from migkit.state import Taken

TABLE = "db.t"
SPANS = [[0, 100], [100, 200], [200, 300]]


class Store:
    def __init__(self):
        self.records, self.lock = {}, threading.Lock()

    def read_record(self, path, sealed=False):
        with self.lock:
            got = self.records.get(str(path))
        return (json.loads(json.dumps(got[0])), got[1]) if got else (None, None)

    def write_record(self, path, record, version, sealed=False):
        with self.lock:
            have = self.records.get(str(path))
            if (have[1] if have else None) != version:
                raise Taken()
            self.records[str(path)] = (json.loads(json.dumps(record)),
                                       (have[1] if have else 0) + 1)


def hop(root, name="h", src="a", dst="b", port=5432, **options):
    return SimpleNamespace(
        name=name, databases=["db"],
        options={"share_tables": "ranges", **options},
        source=Endpoint(host=src, port=port),
        target=Endpoint(host=dst, port=port),
        report_dir=lambda d="": Path(root) / name / d)


def shared_table(h, store):
    store.records[str(h.report_dir("db") / claims.INDEX)] = ({"keys": [TABLE]}, 1)
    t = claims.Table(store, claims.path(h, "db", TABLE), "m1:1", 30)
    t.plan(TABLE, 0, SPANS[-1][1], lambda: SPANS)
    return t


def stopped_movers_ledger(where, open_afters=(0, 100), done_afters=()):
    """What the machine that stopped left in its report directory."""
    Path(where).mkdir(parents=True, exist_ok=True)
    led = attempts.Ledger(Path(where) / "attempts.json")
    for a in (*open_afters, *done_afters):
        led.unit_opened(fleet.unit_name(TABLE, a), "move", "range", TABLE, 3)
    for a in done_afters:
        led.unit_closed(fleet.unit_name(TABLE, a), True)
    led.own("copy:op", state="running", table=TABLE, after=200)
    return led


# ---- the ledger rides in the store -----------------------------------------

def test_the_kept_ledger_reads_back_as_the_stopped_mover_left_it(tmp_path):
    led = stopped_movers_ledger(tmp_path / "gone")
    saved = json.loads(json.dumps(fleet.kept(tmp_path / "gone")))
    view = fleet.Departed.of(saved)
    assert fleet.opened(view, TABLE).keys() == fleet.opened(led, TABLE).keys()
    assert set(fleet.opened(view, TABLE)) == {0, 100, 200}
    assert view.range_units(TABLE)[0]["state"] == "open"
    assert saved["unread"] == []


def test_a_ledger_that_cannot_be_read_is_named_not_left_out(tmp_path):
    where = tmp_path / "gone"
    where.mkdir()
    (where / "attempts.json").write_text("{not json")
    (where / "attempts.json.bak").write_text("{also not")
    saved = fleet.kept(where)
    assert saved["unread"] == ["attempts.json"] and saved["ledgers"] == {}
    assert fleet.Departed.of(saved).unread == ("attempts.json",)
    assert fleet.Departed.of(None, {}).empty()


def test_a_later_opening_wins_where_two_ledgers_name_one_unit(tmp_path):
    name = fleet.unit_name(TABLE, 0)
    old = {"ledgers": {"a": {"units": {name: {
        "level": "range", "parent": TABLE, "state": "failed",
        "started": 1.0}}, "operations": {}}}}
    new = {"ledgers": {"b": {"units": {name: {
        "level": "range", "parent": TABLE, "state": "done",
        "started": 2.0}}, "operations": {}}}}
    assert fleet.Departed.of(old, new).range_units(TABLE)[0]["state"] == "done"
    assert fleet.Departed.of(new, old).range_units(TABLE)[0]["state"] == "done"


# ---- a takeover on another machine -----------------------------------------

def test_the_taker_reads_units_of_a_ledger_that_is_on_another_machine(
        tmp_path, monkeypatch):
    monkeypatch.setattr(ha, "mover", lambda: "m2:2")
    monkeypatch.setattr(ha, "term", lambda: 30)
    stopped_movers_ledger(tmp_path / "machine-a")
    saved = json.loads(json.dumps(fleet.kept(tmp_path / "machine-a")))
    mine = attempts.Ledger()
    gone = {"host": "m1", "pid": 1}
    store = Store()
    h = hop(tmp_path / "machine-b")
    t = shared_table(h, store)
    t.claim()
    t.claim()
    # this machine's own ledger alone knows nothing of the other's units
    blind = fleet.take_hop_over(h, gone, store, lambda s: None, mine)
    assert blind[("db", TABLE)].uncertain == {}
    store2 = Store()
    h2 = hop(tmp_path / "machine-d")
    t2 = shared_table(h2, store2)
    t2.claim()
    t2.claim()
    said = []
    view = fleet.Departed.of(saved).joined(mine)
    got = fleet.take_hop_over(h2, gone, store2, said.append,
                              view)[("db", TABLE)]
    assert set(got.uncertain) == {0, 100, 200}
    # range 200 was never claimed, but the stopped mover's ledger had an
    # operation on it in flight: it is not copied on a guess
    assert sorted(got.taken_back) == [0, 100, 200] and got.todo == []
    assert any("taken back from the mover that stopped" in s for s in said)


def test_the_machines_record_stays_the_truth_where_the_ledger_disagrees(
        tmp_path, monkeypatch):
    monkeypatch.setattr(ha, "mover", lambda: "m2:2")
    monkeypatch.setattr(ha, "term", lambda: 30)
    stopped_movers_ledger(tmp_path / "gone", open_afters=(), done_afters=(0, 100))
    view = fleet.Departed.of(fleet.kept(tmp_path / "gone"))
    store = Store()
    h = hop(tmp_path / "taker")
    t = shared_table(h, store)
    first = t.claim()
    assert first[0] == 0
    said = []
    got = fleet.take_hop_over(h, {"host": "m1", "pid": 1}, store,
                              said.append, view)[("db", TABLE)]
    assert got.done == []
    assert len(got.conflicts) == 2
    assert any("the record stands" in c and "after 0" in c for c in got.conflicts)
    rec = t.read()
    assert all(rec["state"][str(a)]["state"] != claims.DONE for a, _ in SPANS)
    assert sum("closed the range after" in s for s in said) == 2


def test_a_ledger_that_agrees_with_the_record_raises_no_conflict(tmp_path):
    stopped_movers_ledger(tmp_path / "gone", open_afters=(), done_afters=(0,))
    view = fleet.Departed.of(fleet.kept(tmp_path / "gone"))
    store = Store()
    h = hop(tmp_path / "taker")
    t = shared_table(h, store)
    first = t.claim()
    t.done(first[0], first[2], 100)
    rec = t.read()
    assert rec["state"]["0"]["state"] == claims.DONE
    assert fleet.conflicts(rec, view, TABLE) == []
    assert fleet.conflicts(rec, fleet.Departed(), TABLE) == []
    assert fleet.conflicts(None, view, TABLE) == []


def holding(tmp_path, took_over, store, **options):
    h = hop(tmp_path / "taker", **options)
    ls = SimpleNamespace(fence=2, me="m2", took_over=took_over)
    got = ha.Holding(h, ls, store, lambda s: None)
    got.dbs = ["db"]
    return h, got


def test_a_holding_saves_the_ledger_with_the_position_and_stamps_it(
        tmp_path, monkeypatch):
    stopped_movers_ledger(tmp_path / "taker" / "h")
    monkeypatch.setattr(ha, "bundle", lambda where, store: {
        "files": {}, "blobs": {}, "gen": 0})
    h, got = holding(tmp_path, None, Store())
    got.token = SimpleNamespace(number=2)
    before = got._stamps(h.report_dir("db"))
    record = got._record("db", None)
    units = record["ledgers"]["ledgers"]["attempts.json"]["units"]
    assert set(units) == {fleet.unit_name(TABLE, 0), fleet.unit_name(TABLE, 100)}
    assert "attempts.json" in dict(before)
    led = attempts.Ledger(h.report_dir() / "attempts.json")
    led.unit_opened(fleet.unit_name(TABLE, 200), "move", "range", TABLE, 3)
    assert got._stamps(h.report_dir("db")) != before


def test_the_holding_that_takes_over_resumes_from_the_departed_units(
        tmp_path, monkeypatch):
    monkeypatch.setattr(ha, "mover", lambda: "m2:2")
    monkeypatch.setattr(ha, "term", lambda: 30)
    stopped_movers_ledger(tmp_path / "machine-a")
    saved = json.loads(json.dumps(fleet.kept(tmp_path / "machine-a")))
    store = Store()
    h, got = holding(tmp_path, {"host": "m1", "pid": 1}, store)
    t = shared_table(h, store)
    t.claim()
    t.claim()
    got.left["db"] = saved
    got._take_ranges_over(SimpleNamespace())
    acct = got.accounts[("db", TABLE)]
    assert set(acct.uncertain) == {0, 100, 200}
    assert sorted(acct.taken_back) == [0, 100, 200]


def test_a_taker_without_the_departed_ledger_says_so_and_goes_on_by_the_record(
        tmp_path, monkeypatch):
    monkeypatch.setattr(ha, "mover", lambda: "m2:2")
    monkeypatch.setattr(ha, "term", lambda: 30)
    store = Store()
    h, got = holding(tmp_path, {"host": "m1", "pid": 1}, store)
    said = []
    got.say = said.append
    t = shared_table(h, store)
    t.claim()
    got._take_ranges_over(SimpleNamespace())
    assert any("ledger was not in the machines' store" in s for s in said)
    assert got.accounts[("db", TABLE)].taken_back == [0]


# ---- the lease waits for its holder's measured jitter -----------------------

def test_the_jitter_is_the_worst_late_renewal_and_its_write():
    j = lease_mod.Jitter()
    assert j.allowance() == 0.0
    j.beat(gap=10.0, due=10.0, wrote=0.2)
    j.beat(gap=13.0, due=10.0, wrote=0.5)
    j.beat(gap=9.0, due=10.0, wrote=0.1)
    assert j.allowance() == 3.5
    for _ in range(j.KEPT):
        j.beat(10.0, 10.0, 0.0)
    assert j.allowance() == 0.0


def record(now, jitter=None, renewed_ago=40.0, term=30.0, expired_ago=0.0):
    have = {"holder": "other:1:1", "host": "elsewhere", "pid": 1,
            "renewed": now - renewed_ago,
            "expires": now - renewed_ago + term, "since": now - 100}
    if expired_ago:
        have["expires"] = now - expired_ago
        have["renewed"] = have["expires"] - term
    if jitter is not None:
        have["jitter"] = jitter
    return have


def test_a_lapse_is_the_expiry_and_the_jitter_bounded_by_the_holders_term():
    now = time.time()
    have = record(now, expired_ago=1.0)
    assert lease_mod.lapse(have) == have["expires"]
    have["jitter"] = 5.0
    assert lease_mod.lapse(have) == have["expires"] + 5.0
    have["jitter"] = 1e9
    assert lease_mod.lapse(have) == have["expires"] + 30.0
    have["jitter"] = "garbage"
    assert lease_mod.lapse(have) == have["expires"]
    have["jitter"] = -3
    assert lease_mod.lapse(have) == have["expires"]
    assert lease_mod.lapse({"holder": None, "expires": 0}) == 0.0


def test_a_holder_that_is_only_late_is_not_taken_over_until_its_jitter_passes(
        tmp_path):
    now = time.time()
    path = tmp_path / "lease.json"
    late = record(now, jitter=5.0, expired_ago=1.0)
    path.write_text(json.dumps(late))
    assert lease_mod.liveness(late)
    with pytest.raises(lease_mod.Held):
        lease_mod.Lease(path, term=30).acquire()
    steady = record(now, jitter=0.0, expired_ago=1.0)
    assert not lease_mod.liveness(steady)
    path.write_text(json.dumps(steady))
    mine = lease_mod.Lease(path, term=30).acquire()
    try:
        assert mine.took_over["holder"] == "other:1:1"
    finally:
        mine.release()
    gone = record(now, jitter=5.0, expired_ago=7.0)
    assert not lease_mod.liveness(gone)
    path.write_text(json.dumps(gone))
    assert lease_mod.holder(path) is None
    path.write_text(json.dumps(late))
    assert lease_mod.holder(path)["holder"] == "other:1:1"


def test_a_holder_writes_the_jitter_it_measured_into_its_record(tmp_path):
    path = tmp_path / "lease.json"
    mine = lease_mod.Lease(path, term=3600).acquire()
    try:
        assert "jitter" not in json.loads(path.read_text()) or \
            json.loads(path.read_text())["jitter"] == 0.0
        mine.renewed = time.monotonic() - 2000.0
        assert mine._renew_once()
        assert mine._renew_once()
        got = json.loads(path.read_text())
        assert 700.0 < got["jitter"] < 900.0
    finally:
        mine.release()


# ---- a topology reads the hops' own keys -----------------------------------

TABLES = {"t": {"key": ["id"], "columns": ["id", "v"]}}


@pytest.fixture
def keys(monkeypatch):
    for name in (topology.SPACES_KEY, topology.CONFLICT_KEY,
                 topology.ROUTING_KEY):
        monkeypatch.setitem(hopkeys.HOP_OPTIONS, name,
                            hopkeys.Key(f"the topology's {name}", "any"))


def many(tmp_path, spaces, conflict=None, rates=None, route=None):
    hops = []
    for i, src in enumerate("xyz"):
        opts = {topology.SPACES_KEY: {"t": spaces[src]}} if spaces else {}
        if conflict:
            opts[topology.CONFLICT_KEY] = conflict
        if route:
            opts[topology.ROUTING_KEY] = route
        hops.append(hop(tmp_path, f"h{i}", src=src, dst="hub", **opts))
    rates = rates or {}
    return hops, lambda h: rates.get(h.name)


def test_key_spaces_come_from_the_hops_and_decide_the_overlap(tmp_path, keys):
    hops, speed = many(tmp_path, {"x": {"range": [1, 10]},
                                  "y": {"range": [11, 20]},
                                  "z": {"range": [21, 30]}},
                       rates={"h0": 100.0})
    facts, lay = topology.facts_of(hops, TABLES, speed=speed)
    assert facts["spaces"]["x:5432"] == {"t": {"range": [1, 10]}}
    assert topology.overlaps({s: facts["spaces"][s] for s in facts["sources"]},
                             TABLES) == []
    assert facts["speeds"] == {("x:5432", "hub:5432"): 100.0}
    assert facts["unmeasured"] == (("y:5432", "hub:5432"),
                                   ("z:5432", "hub:5432"))


def test_overlapping_spaces_read_from_the_hops_are_found(tmp_path, keys):
    hops, speed = many(tmp_path, {"x": {"range": [1, 10]},
                                  "y": {"range": [5, 20]},
                                  "z": {"range": [21, 30]}})
    facts, _ = topology.facts_of(hops, TABLES, speed=speed)
    assert topology.overlaps({s: facts["spaces"][s] for s in facts["sources"]},
                             TABLES)


def test_what_is_passed_in_stands_over_what_the_hops_say(tmp_path, keys):
    hops, speed = many(tmp_path, {"x": {"range": [1, 10]},
                                  "y": {"range": [11, 20]},
                                  "z": {"range": [21, 30]}},
                       conflict="keep_local")
    facts, _ = topology.facts_of(hops, TABLES, speed=speed)
    assert facts["conflict"] == "keep_local"
    facts, _ = topology.facts_of(
        hops, TABLES, spaces={"x:5432": {"t": {"range": [1, 3]}}},
        conflict="apply_remote", speed=speed)
    assert facts["conflict"] == "apply_remote"
    assert facts["spaces"]["x:5432"] == {"t": {"range": [1, 3]}}
    assert facts["spaces"]["y:5432"] == {"t": {"range": [11, 20]}}


def test_hops_that_give_two_conflict_rules_are_refused_by_name(tmp_path, keys):
    hops, speed = many(tmp_path, None, conflict="keep_local")
    hops[2].options[topology.CONFLICT_KEY] = "apply_remote"
    with pytest.raises(SystemExit) as e:
        topology.facts_of(hops, TABLES, speed=speed)
    assert "h0, h1: 'keep_local'" in str(e.value) and "h2" in str(e.value)


def test_a_fan_out_routing_is_joined_from_its_hops_and_a_clash_refused(
        tmp_path, keys):
    hops = [hop(tmp_path, "h0", src="s", dst="a", **{
                topology.ROUTING_KEY: {"by": "table", "tables": {"t": "a"}}}),
            hop(tmp_path, "h1", src="s", dst="b", **{
                topology.ROUTING_KEY: {"by": "table", "tables": {"u": "b"}}})]
    facts, _ = topology.facts_of(hops, TABLES, speed=lambda h: None)
    assert facts["route"] == {"by": "table", "tables": {"t": "a", "u": "b"}}
    hops[1].options[topology.ROUTING_KEY] = {"by": "table",
                                             "tables": {"t": "b"}}
    with pytest.raises(SystemExit) as e:
        topology.facts_of(hops, TABLES, speed=lambda h: None)
    assert "sends t to b" in str(e.value)
    hops[1].options[topology.ROUTING_KEY] = {"by": "hash"}
    with pytest.raises(SystemExit):
        topology.facts_of(hops, TABLES, speed=lambda h: None)


def test_a_bad_key_space_is_refused_where_it_is_read(tmp_path, keys):
    hops, speed = many(tmp_path, {"x": {"stride": ["a", "b"]},
                                  "y": {}, "z": {}})
    with pytest.raises(SystemExit) as e:
        topology.facts_of(hops, TABLES, speed=speed)
    assert "not a key space" in str(e.value)


def test_a_key_the_registry_lacks_reads_as_unsaid(tmp_path, monkeypatch):
    for name in (topology.SPACES_KEY, topology.CONFLICT_KEY,
                 topology.ROUTING_KEY):
        monkeypatch.delitem(hopkeys.HOP_OPTIONS, name, raising=False)
    hops, speed = many(tmp_path, None)
    facts, _ = topology.facts_of(hops, TABLES, speed=speed)
    assert facts["spaces"] == {} and facts["conflict"] is None
