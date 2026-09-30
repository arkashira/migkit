"""One table's ranges handed to every machine's workers through one record
written only over the version read (backlog W1, `claims`).

The record's changes are pure functions, so every interleaving of claims,
renewals, pauses past the term, copies and completions hypothesis finds
is driven here against a model of the target that holds each range's
number the way `fencing` does: a claim raises its range's number at its
first write, and a write under a lower number is refused. Whatever the
order, a range is done once, by the claim that holds it, and what stays
on the target for it is that claim's copy; one machine finishes the
table.
"""
import threading

import pytest
from hypothesis import given, settings, strategies as st

from migkit import claims as C
from migkit import shared

TERM = 5.0
SPANS = [[0, 10], [10, 20], [20, 30], [30, 40]]


class Target:
    """Each range's row and the number of the claim whose copy is on the
    target now."""

    def __init__(self):
        self.row, self.copy = {}, {}

    def write(self, after, number, raised):
        """A write of `after`'s range under `number`: raised first where
        this claim has not raised yet, then held - True where it is
        taken."""
        if not raised:
            self.row[after] = max(self.row.get(after, 0), number)
        if self.row.get(after) != number:
            return False
        self.copy[after] = number
        return True


WORKERS = [(m, k) for m in range(3) for k in range(2)]
OPS = st.lists(st.one_of(
    st.tuples(st.just("claim"), st.sampled_from(WORKERS)),
    st.tuples(st.just("write"), st.sampled_from(WORKERS)),
    st.tuples(st.just("done"), st.sampled_from(WORKERS)),
    st.tuples(st.just("renew"), st.integers(0, 2)),
    st.tuples(st.just("give back"), st.sampled_from(WORKERS)),
    st.tuples(st.just("wait"), st.floats(0.1, 8.0))), max_size=80)


def _me(m):
    return f"machine-{m}"


@settings(max_examples=400, deadline=None)
@given(OPS)
def test_each_range_is_done_once_by_the_claim_that_holds_it(ops):
    now = [100.0]
    rec = C.planned(None, "db.t", 1, 40, SPANS)
    target = Target()
    holding = {}        # worker -> [after, number, raised, wrote]
    numbers = []
    ever_done = set()
    accepted = {}       # after -> numbers whose writes the target took

    def step(op, arg):
        nonlocal rec
        if op == "wait":
            now[0] += arg
            return
        if op == "renew":
            mine = {h[0]: h[1] for w, h in holding.items() if w[0] == arg}
            rec = C.renew(rec, _me(arg), mine, now[0], TERM)
            return
        w = arg
        me = _me(w[0])
        if op == "claim":
            if w in holding:
                return
            rec, got = C.claim(rec, me, now[0], TERM)
            if got is None:
                return
            a, _, n = got
            assert not numbers or n > max(numbers)
            numbers.append(n)
            holding[w] = [a, n, False, False]
            return
        if w not in holding:
            return
        a, n, raised, wrote = holding[w]
        if op == "write":
            took = target.write(a, n, raised)
            holding[w][2] = True
            if took:
                holding[w][3] = True
                # once a newer claim wrote, no older one's write is taken
                assert all(n >= x for x in accepted.get(a, [])), (a, n)
                accepted.setdefault(a, []).append(n)
            else:
                # refused: its copy is not counted, and it lets go
                holding.pop(w)
            return
        if op == "give back":
            rec = C.give_back(rec, me, a, n)
            holding.pop(w)
            return
        if op == "done":
            if not wrote:
                return
            st_ = rec["state"][str(a)]
            holds = (st_["state"] == C.CLAIMED and st_["by"] == me
                     and st_["number"] == n)
            if holds:
                rec = C.done(rec, me, a, n, 10, now[0])
                assert a not in ever_done, "a range done twice"
                ever_done.add(a)
            else:
                with pytest.raises(C.Lost):
                    C.done(rec, me, a, n, 10, now[0])
            holding.pop(w)

    for op, arg in ops:
        before = {a: s["state"] for a, s in rec["state"].items()}
        step(op, arg)
        for a, s in before.items():
            if s == C.DONE:
                # done stays done
                assert rec["state"][a]["state"] == C.DONE
    # every machine keeps going until nothing is left: the rest are
    # claimed - a lapsed claim taken again - copied and done
    now[0] += 3 * TERM
    holding.clear()
    for _ in range(3 * len(SPANS)):
        rec, got = C.claim(rec, _me(0), now[0], TERM)
        if got is None:
            break
        a, _, n = got
        assert target.write(a, n, False)
        rec = C.done(rec, _me(0), a, n, 10, now[0])
    assert C.left(rec) == 0
    for a, s in rec["state"].items():
        # what stays on the target is the copy of the claim that is done
        assert target.copy[int(a)] == s["number"], (a, s, target.copy)
    finished = []
    for m in range(3):
        rec, mine = C.finish(rec, _me(m), now[0])
        finished.append(mine)
    assert finished.count(True) == 1


def test_a_lapsed_claim_is_taken_again_and_its_done_refused():
    rec = C.planned(None, "db.t", 1, 20, SPANS[:2])
    rec, (a, _, first) = C.claim(rec, "a", 0.0, TERM)
    # before the term ends nobody else takes it
    rec, got = C.claim(rec, "b", TERM - 0.1, TERM)
    assert got[0] != a
    rec, again = C.claim(rec, "b", TERM + 0.1, TERM)
    assert again[0] == a and again[2] > first
    assert rec["lapsed"] and rec["lapsed"][0]["by"] == "a"
    with pytest.raises(C.Lost):
        C.done(rec, "a", a, first, 5, TERM + 1)
    with pytest.raises(C.Lost):
        C.started(rec, "a", a, first, {"x": 1})
    rec = C.done(rec, "b", a, again[2], 5, TERM + 1)
    assert rec["state"][str(a)]["state"] == C.DONE


def test_a_renewed_claim_is_not_taken():
    rec = C.planned(None, "db.t", 1, 10, SPANS[:1])
    rec, (a, _, n) = C.claim(rec, "a", 0.0, TERM)
    rec = C.renew(rec, "a", {a: n}, TERM - 1, TERM)
    rec, got = C.claim(rec, "b", TERM + 1, TERM)
    assert got is None
    # renewing a claim another holds moves nothing of it
    rec = C.renew(rec, "b", {a: n}, TERM + 1, TERM)
    assert rec["state"][str(a)]["by"] == "a"


def test_machines_share_ranges_through_a_directory(tmp_path):
    """Two machines' workers, six threads, one record in a shared
    directory: each range done once, and no range held by two workers at
    once."""
    from migkit.config import Endpoint, Hop
    hop = Hop(name="share", engine="postgres", source=Endpoint(),
              target=Endpoint())
    hop.report_dir = lambda db=None: tmp_path / "reports" / (db or "")
    store = shared.Directory(hop, tmp_path / "shared")
    where = C.path(hop, "db", "db.t")
    spans = [[i * 10, i * 10 + 10] for i in range(30)]
    C.Table(store, where, "planner", TERM).plan("db.t", 1, 300,
                                                lambda: spans)
    lock = threading.Lock()
    holding, doubled, done = set(), [], []

    def worker(me):
        table = C.Table(store, where, me, TERM)
        while True:
            got = table.claim()
            if got is None:
                return
            a, _, n = got
            with lock:
                if a in holding:
                    doubled.append(a)
                holding.add(a)
            table.started(a, n, {"at": a})
            with lock:
                holding.discard(a)
            table.done(a, n, 1)
            with lock:
                done.append(a)
    threads = [threading.Thread(target=worker, args=(f"m{i % 2}:{i}",))
               for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not doubled
    assert sorted(done) == [s[0] for s in spans]
    rec = C.Table(store, where, "x", TERM).read()
    assert C.left(rec) == 0
    numbers = [s["number"] for s in rec["state"].values()]
    assert len(set(numbers)) == len(numbers) == 30


def test_the_share_of_a_machine_divides_the_servers_not_itself():
    from migkit import model as M
    stages = [M.Stage("source", "the source's reads", 10e6, 40e6,
                      M.MEASURED, side="src"),
              M.Stage("target", "the target's writes", 5e6, 20e6,
                      M.MEASURED, side="dst"),
              M.Stage("cpu", "this machine's CPUs", 50e6, 400e6,
                      M.MEASURED, side="")]
    m = M.Model(stages, measured=True)
    sides = {"src": {"free_connections": 90, "cpus": 8},
             "dst": {"free_connections": 40, "cpus": 4}}
    got, mine = C.per_machine(m, sides, 4)
    assert got.stages["target"].total == pytest.approx(5e6)
    assert got.stages["source"].total == pytest.approx(10e6)
    # this machine's own, whole
    assert got.stages["cpu"].total == pytest.approx(400e6)
    assert mine["src"] == {"free_connections": 22, "cpus": 2}
    assert mine["dst"] == {"free_connections": 10, "cpus": 1}
    # the servers' binding stage carries fewer workers for one of four
    assert got.little is not None and m.little is not None
    assert got.little < m.little, (got.little, m.little)
