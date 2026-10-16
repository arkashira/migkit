"""A Redis stream whose buffer never empties still lands, saves its offset
and acknowledges it; a warehouse's versioned rows are never moved across a
delete of their key and its tail batch fits this machine's memory; a hop
whose `nodes:` names a topology moves through the planner's topology run,
a plain hop as before."""

import types
from pathlib import Path

from migkit import decide, planner, sizing
from migkit.config import Endpoint
from migkit.engines import redis as R
from migkit.engines import warehouse


# ---- Redis: landing under sustained writes ----------------------------------

class NeverEmpty:
    """A replica stream with bytes always buffered after each command."""

    def __init__(self, n):
        self.n, self.offset, self.argv, self.acks = n, 100, None, []

    def commands(self):
        for i in range(self.n):
            self.argv = [b"set", f"k{i}".encode(), b"v"]
            self.offset += 10
            yield self.argv

    def buffered(self):
        return True

    def ack(self, offset):
        self.acks.append(offset)

    def close(self):
        pass


def _stream_engine(stream, state, landed, clock=None):
    eng = object.__new__(R.RedisEngine)
    eng.hop = types.SimpleNamespace(source=types.SimpleNamespace(
        user=None, password=None))
    spaces = R.KeyspaceStates(state.setdefault("keyspaces", {}),
                              lambda m: None)
    spaces._enter("0", "live", 100, "test")
    eng._streams = {"": (stream, spaces, lambda argv: argv[1:2])}
    if clock is not None:
        eng._paces = {"": R._LandingPace(clock=clock)}
    eng._refresh_keys = lambda d, ops: landed.append(dict(ops)) or {}
    eng._copy_again = lambda d, log: None
    return eng


def test_a_stream_that_never_empties_still_lands_saves_and_acks():
    state, landed, saved = {}, [], []
    stream = NeverEmpty(50)
    eng = _stream_engine(stream, state, landed)
    eng._stream_rounds("0", state, lambda: saved.append(1), lambda s: None,
                       lambda: False)
    assert stream.acks and stream.acks[-1] == state["keyspaces"]["offset"]
    assert state["keyspaces"]["offset"] > 100 and saved
    assert sum(len(b) for b in landed) >= len(landed) and landed
    # every ack is at an offset whose keys landed before it was said
    assert stream.acks == sorted(stream.acks)


def test_a_slow_landing_is_batched_by_the_measured_rate():
    now = [0.0]

    def clock():
        return now[0]
    state, landed = {}, []
    stream = NeverEmpty(200)
    eng = _stream_engine(stream, state, landed, clock=clock)

    def refresh(d, ops):
        now[0] += 0.05  # the target's round trip and writes
        landed.append(dict(ops))
        return {}
    eng._refresh_keys = refresh
    orig = stream.commands

    def paced():
        # a key written every millisecond, buffered while a landing runs
        for i, argv in enumerate(orig()):
            now[0] = max(now[0], i * 0.001)
            yield argv
    stream.commands = paced
    eng._stream_rounds("0", state, lambda: None, lambda s: None,
                       lambda: False)
    assert len(landed) < 200 / 5, "each key landed on its own round trip"
    assert max(len(b) for b in landed) >= 20
    # what has not landed yet waits for the next boundary, kept
    waiting = eng._streams[""][1].pending.get("0", {})
    assert sum(len(b) for b in landed) + len(waiting) == 200
    assert stream.acks[-1] == state["keyspaces"]["offset"]


def test_the_elapsed_bound_lands_a_quiet_stream():
    pace = R._LandingPace(clock=lambda: 10.0)
    pace.took, pace.pace.at = 30.0, 10.0 - 1.0
    assert pace.due(True), "never longer than the most a stream waits"
    pace.took, pace.pace.at = 0.5, 10.0 - 0.2
    assert not pace.due(True) and pace.due(False)
    pace.pace.at = 10.0 - 0.5
    assert pace.due(True)


# ---- the warehouse's version order around deletes ----------------------------

def _wh(monkeypatch, versions=None):
    e = object.__new__(warehouse.RedshiftEngine)
    e.hop = types.SimpleNamespace(name="h", versions=versions)
    monkeypatch.setattr(e, "_endpoint", lambda side: types.SimpleNamespace(
        options={}, user="u"), raising=False)
    monkeypatch.setattr(e, "local_table", lambda t: t)
    monkeypatch.setattr(decide, "record", lambda *a, **k: None)
    return e


def up(v, x, key=1):
    return {"op": "upsert", "table": "t", "key": {"id": key},
            "values": {"id": key, "v": v, "x": x}}


DEL = {"op": "delete", "table": "t", "key": {"id": 1}, "values": {}}


def test_a_row_is_never_moved_across_its_keys_delete(monkeypatch):
    e = _wh(monkeypatch, {"t": "v"})
    got = e._in_version_order([up(3, "a"), DEL, up(2, "b")])
    assert [c.get("op") for c in got] == ["upsert", "delete", "upsert"]
    assert [c["values"].get("x") for c in got] == ["a", None, "b"]


def test_rows_between_deletes_are_ordered_and_other_keys_kept(monkeypatch):
    e = _wh(monkeypatch, {"t": "v"})
    ch = [up(5, "late"), up(4, "early"), DEL, up(9, "new"), up(7, "old"),
          up(2, "k2b", key=2), up(1, "k2a", key=2)]
    got = e._in_version_order(ch)
    assert [c["values"].get("x") for c in got] == [
        "early", "late", None, "old", "new", "k2a", "k2b"]


# ---- the warehouse's tail batch in memory -------------------------------------

def test_a_huge_batch_answer_is_capped_by_memory(monkeypatch):
    e = _wh(monkeypatch)
    monkeypatch.setattr(sizing, "host", lambda: {"memory": 4_000_000})
    e._measure_changes([{"key": {"id": 1}, "values": {"x": "y" * 1000}}])
    assert e._held_in_memory(10 ** 9, 500) == 4_000_000 // (4 * e._change_width)
    assert e._held_in_memory(10, 500) == 10
    monkeypatch.setattr(sizing, "host", lambda: {"memory": None})
    assert e._held_in_memory(10 ** 9, 500) == 10 ** 9


def test_tail_limit_never_answers_past_memory(monkeypatch):
    e = _wh(monkeypatch)
    monkeypatch.setattr(sizing, "host", lambda: {"memory": 1_000_000})
    e._fed = {"d": True}
    monkeypatch.setattr(e, "landing_facts", lambda side, rate: {})
    monkeypatch.setattr(e, "_landing_way", lambda side: "merge")
    monkeypatch.setattr(e, "_landing_probe", lambda *a: None)
    monkeypatch.setattr(decide, "warehouse_landing", lambda f: (
        types.SimpleNamespace(path="merge", said="merge"),
        (50_000_000, 1.0, 1.0)))
    monkeypatch.setattr(decide, "batch_for", lambda *a: (50_000_000, 1.0,
                                                          1.0))
    e._change_width = 100
    e.tail_limit("dst", "d", 1000, 100.0, 1000)
    got = e.tail_limit("dst", "d", 1000, 101.0, 1000)
    assert got == 1_000_000 // (4 * 100)


# ---- a topology's move goes through the planner's topology run ---------------

def hop(name, src, dst, nodes=None):
    root = Path("/nowhere") / name
    opts = {"nodes": nodes} if nodes else {}
    return types.SimpleNamespace(
        name=name, databases=["db"], options=opts,
        source=Endpoint(host=src, port=5432),
        target=Endpoint(host=dst, port=5432),
        report_dir=lambda d="": root / d)


TABLES = {"t": {"key": ["id"], "columns": ["id", "v"]}}
SPACES = {"x:5432": {"t": {"range": [1, 10]}},
          "y:5432": {"t": {"range": [11, 20]}}}


def test_a_plain_hop_moves_alone_as_before(monkeypatch):
    ran = []
    monkeypatch.setattr(planner, "run_topology",
                        lambda *a, **k: ran.append("topology"))
    one = hop("h", "a", "b")
    got = planner.move_topology(one, [one, hop("o", "c", "d")], TABLES,
                                lambda h, s: ran.append((h.name, s)),
                                lambda: {}, speed=lambda h: None)
    assert got is None and ran == [("h", None)]


def test_a_hop_naming_a_topology_runs_through_the_topology_run(monkeypatch):
    seen = {}

    def run(hops, facts, lay, run_hop, read_rows, **k):
        seen.update(hops=[h.name for h in hops], kind=lay.kind,
                    speeds=dict(facts["speeds"]))
        return "ran"
    monkeypatch.setattr(planner, "run_topology", run)
    nodes = {"spaces": SPACES}
    hops = [hop("h0", "x", "hub", nodes), hop("h1", "y", "hub", nodes),
            hop("far", "p", "q")]
    rates = {"h0": 100.0, "h1": 10.0}
    got = planner.move_topology(hops[0], hops, TABLES,
                                lambda h, s: None, lambda: {},
                                speed=lambda h: rates.get(h.name))
    assert got == "ran"
    assert seen["hops"] == ["h0", "h1"] and seen["kind"] == "many-to-one"
    assert seen["speeds"] == {("x:5432", "hub:5432"): 100.0,
                              ("y:5432", "hub:5432"): 10.0}
