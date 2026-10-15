"""The standby takeover is used, ranges are ledger units, and a topology
is read off the hops and run in order (backlog W1 `fleet`, R3.topo).

Fake stores and fake hops only: the record is written over the version
read, as the machines' store does; no server is reached.
"""
import contextlib
import copy
import threading
from pathlib import Path
from types import SimpleNamespace

from migkit import attempts, claims, fleet, ha, planner, topology
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
        return (copy.deepcopy(got[0]), got[1]) if got else (None, None)

    def write_record(self, path, record, version, sealed=False):
        with self.lock:
            have = self.records.get(str(path))
            if (have[1] if have else None) != version:
                raise Taken()
            self.records[str(path)] = (copy.deepcopy(record),
                                       (have[1] if have else 0) + 1)


def hop(name="h", share="ranges", src="a", dst="b", port=5432):
    root = Path("/nowhere") / name
    return SimpleNamespace(
        name=name, databases=["db"], options={"share_tables": share},
        source=Endpoint(host=src, port=port), target=Endpoint(host=dst, port=port),
        report_dir=lambda d="": root / d)


def shared_table(h, store, spans=SPANS, key=TABLE):
    store.records[str(h.report_dir("db") / claims.INDEX)] = (
        {"keys": [key]}, 1)
    t = claims.Table(store, claims.path(h, "db", key), "m1:1", 30)
    t.plan(key, 0, spans[-1][1], lambda: spans)
    return t


def test_taking_the_hop_over_puts_the_stopped_movers_ranges_back_at_once(
        monkeypatch):
    h, store = hop(), Store()
    old = shared_table(h, store)
    first = old.claim()
    second = old.claim()
    assert first and second
    monkeypatch.setattr(ha, "mover", lambda: "m2:2")
    monkeypatch.setattr(ha, "term", lambda: 30)
    said = []
    got = fleet.take_hop_over(h, {"host": "m1", "pid": 1}, store, said.append)
    rec = old.read()
    states = [rec["state"][str(a)]["state"] for a, _ in rec["ranges"]]
    assert states == [claims.TODO] * 3
    assert len(rec["lapsed"]) == 2
    account = got[("db", TABLE)]
    assert sorted(account.taken_back) == sorted([first[0], second[0]])
    assert account.held == [] and account.done == []
    assert any("taken back from the mover that stopped" in s for s in said)


def test_a_mover_still_in_its_term_keeps_its_claim(monkeypatch):
    h, store = hop(), Store()
    other = shared_table(h, store)
    other.claim()
    monkeypatch.setattr(ha, "mover", lambda: "m2:2")
    monkeypatch.setattr(ha, "term", lambda: 30)
    got = fleet.take_hop_over(h, {"host": "m9", "pid": 9}, store, lambda s: None)
    rec = other.read()
    assert rec["lapsed"] == []
    assert len(got[("db", TABLE)].held) == 1


def test_the_audit_finds_a_lost_range_and_a_whole_table_has_none():
    h, store = hop(), Store()
    shared_table(h, store, spans=[[0, 100], [200, 300]])
    found = fleet.audit_hop(h, store)
    assert list(found) == [("db", TABLE)]
    assert any("were lost" in w for w in found[("db", TABLE)])
    assert fleet.audit_lines(found)[0].startswith(f"{TABLE}: no range covers")
    clean, store2 = hop("g"), Store()
    shared_table(clean, store2)
    assert fleet.audit_hop(clean, store2) == {}


def test_the_evidence_of_a_table_carries_its_audit():
    h, store = hop(), Store()
    shared_table(h, store, spans=[[0, 100], [200, 300]])
    rows = claims.evidence(h, "db", store)
    audit = [r for r in rows if r["from"] == "audit"]
    assert audit and "lost" in audit[0]["to"]
    assert any("AUDIT" in line for line in claims.health(h, "db", store))
    h2, store2 = hop("g"), Store()
    shared_table(h2, store2)
    ok = [r for r in claims.evidence(h2, "db", store2) if r["from"] == "audit"]
    assert len(ok) == 1 and "not finished" in ok[0]["to"]


class Engine:
    def __init__(self, ledger):
        self.ledger = ledger

    @contextlib.contextmanager
    def work_unit(self, kind, name, level="table", parent="", parts=None):
        self.ledger.unit_opened(name, kind, level, parent, parts)
        ok = False
        try:
            yield
            ok = True
        finally:
            self.ledger.unit_closed(name, ok, "" if ok else "Boom")


def test_a_range_is_a_ledger_unit_under_the_fleets_name():
    ledger = attempts.Ledger()
    eng = Engine(ledger)
    with attempts.range_unit(eng, TABLE, 100, 3):
        assert fleet.unit_name(TABLE, 100) in ledger.records
        assert set(fleet.opened(ledger, TABLE)) == {100}
        assert ledger.range_units(TABLE)[100]["parts"] == 3
    assert fleet.opened(ledger, TABLE) == {}
    try:
        with attempts.range_unit(eng, TABLE, 200):
            raise RuntimeError
    except RuntimeError:
        pass
    got = ledger.range_units(TABLE)
    assert got[100]["state"] == "done" and got[200]["state"] == "failed"
    assert fleet.parse_unit(fleet.unit_name(TABLE, -5)) == (TABLE, -5)
    assert fleet.parse_unit("plain.table") is None
    assert attempts.ledger_of(object()) is None


def test_a_range_left_open_is_read_by_the_taker(monkeypatch):
    h, store = hop(), Store()
    old = shared_table(h, store)
    first = old.claim()
    ledger = attempts.Ledger()
    ledger.unit_opened(fleet.unit_name(TABLE, first[0]), "move", "range",
                       TABLE, 3)
    monkeypatch.setattr(ha, "mover", lambda: "m2:2")
    monkeypatch.setattr(ha, "term", lambda: 30)
    got = fleet.take_hop_over(h, {"host": "m1", "pid": 1}, store,
                              lambda s: None, ledger)
    assert first[0] in got[("db", TABLE)].uncertain
    assert first[0] in got[("db", TABLE)].taken_back


# ---- topology read off the hops --------------------------------------------

def sources(rates=None):
    rates = rates or {}
    hops = [hop(f"h{i}", src=s, dst="hub") for i, s in enumerate("xyz")]
    return hops, (lambda h: rates.get(h.name))


def test_the_layout_is_read_from_the_hops_endpoints_and_measured_links():
    hops, speed = sources({"h0": 900.0, "h1": 100.0})
    lay = topology.layout(hops, speed)
    assert lay.kind == "many-to-one" and lay.hops == 3
    assert lay.sites == ("hub:5432", "x:5432", "y:5432", "z:5432")
    assert lay.speed("x:5432", "hub:5432") == 900.0
    assert lay.speed("z:5432", "hub:5432") is None
    assert lay.slowest() == (("y:5432", "hub:5432"), 100.0)


def test_the_shape_names_what_the_links_draw_and_refuses_what_they_do_not():
    assert topology.shape([("a", "b"), ("a", "c")]) == "one-to-many"
    full = [(x, y) for x in "abc" for y in "abc" if x != y]
    assert topology.shape(full) == "mesh"
    assert topology.shape(full[:-1]) == "mesh"
    assert topology.shape([("a", "b"), ("b", "a")]) == ""
    assert topology.shape([("a", "b"), ("b", "c")]) == ""
    assert topology.shape([("a", "b")]) == ""


TABLES = {"t": {"key": ["id"], "columns": ["id", "v"]}}
SPACES = {"x:5432": {"t": {"range": [1, 10]}},
          "y:5432": {"t": {"range": [11, 20]}},
          "z:5432": {"t": {"range": [21, 30]}}}
ROWS = {"x:5432": {"t": {("1",): "d1"}}, "y:5432": {"t": {("11",): "d2"}},
        "z:5432": {"t": {("21",): "d3"}}}


def union(upto):
    got = {}
    for s in upto:
        for k, d in ROWS[s]["t"].items():
            got[k] = d
    return {"t": got}


def test_the_planner_runs_the_hops_in_the_decided_order_and_proves_each():
    hops, speed = sources({"h0": 1000.0, "h1": 10.0, "h2": 100.0})
    facts, lay = topology.facts_of(hops, TABLES, spaces=SPACES, speed=speed)
    ran = []
    rows = {"sources": ROWS, "target": {}, "applied": []}

    copied = []

    def run_hop(h, spec):
        ran.append(h.name)
        copied.append(spec["source"])
        rows["target"] = union(copied)
    out = planner.run_topology(hops, facts, lay, run_hop, lambda: rows,
                               rows_by_hop={"h0": 1000, "h1": 1000, "h2": 1000})
    assert ran == ["h1", "h2", "h0"]
    assert out.order == ("h1", "h2", "h0")
    assert out.ok and len(out.done) == 3
    assert sum("the whole" in x for x in out.lines) >= 1


def test_a_hop_whose_proof_fails_stops_the_hops_after_it():
    hops, speed = sources({"h0": 1000.0, "h1": 10.0, "h2": 100.0})
    facts, lay = topology.facts_of(hops, TABLES, spaces=SPACES, speed=speed)
    ran = []
    rows = {"sources": ROWS, "target": {}, "applied": []}

    def run_hop(h, spec):
        ran.append(h.name)
    out = planner.run_topology(hops, facts, lay, run_hop, lambda: rows,
                               rows_by_hop={"h0": 1, "h1": 1, "h2": 1})
    assert ran == [ran[0]]
    assert not out.ok and out.stopped[0] == ran[0]
    assert out.final is None and len(out.done) == 1
    assert "missing" in " ".join(out.lines)


def test_a_case_the_planner_refuses_runs_no_hop():
    hops, speed = sources()
    same = {s: {"t": {"range": [1, 10]}} for s in SPACES}
    facts, lay = topology.facts_of(hops, TABLES, spaces=same, speed=speed)
    ran = []
    out = planner.run_topology(hops, facts, lay,
                               lambda h, s: ran.append(h.name), lambda: {})
    assert ran == [] and not out.ok and out.stopped[1] == "the case was refused"


def test_a_plan_with_a_hop_the_configuration_lacks_runs_nothing():
    hops, speed = sources()
    facts, lay = topology.facts_of(hops, TABLES, spaces=SPACES, speed=speed)
    ran = []
    out = planner.run_topology(hops[:2], facts, lay,
                               lambda h, s: ran.append(h.name), lambda: {})
    assert ran == [] and not out.ok
    assert out.stopped[0] == "z:5432 to hub:5432"
