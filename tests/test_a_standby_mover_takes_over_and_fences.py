"""Two movers share one table's ranges; one stops, the other takes the
hop's lease over and the stopped one's ranges back at once, and the late
writes of the one that stopped are refused (backlog W1, `fleet`).

On a store that writes only over the version read and a target that
keeps, per range, the highest number any claim raised it to - refusing a
write under a lower one, as `fencing.hold` makes a real target do. The
lease, the claims and the takeover are migkit's own; the clock is the
test's.
"""
import os
import socket
import threading

import pytest

from migkit import attempts, claims, fleet
from migkit import lease as L
from migkit.state import Taken

SPANS = [[0, 100], [100, 200], [200, 300], [300, 400]]
TABLE = "db.t"


class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def time(self):
        return self.now

    def monotonic(self):
        return self.now

    def sleep(self, s):
        self.now += s

    strftime = staticmethod(__import__("time").strftime)
    localtime = staticmethod(__import__("time").localtime)


class Store:
    """Records written only over the version read, in memory."""

    def __init__(self):
        self.records, self.lock = {}, threading.Lock()

    def read_record(self, path, sealed=False):
        with self.lock:
            got = self.records.get(str(path))
        return (__import__("copy").deepcopy(got[0]), got[1]) if got \
            else (None, None)

    def write_record(self, path, record, version, sealed=False):
        with self.lock:
            have = self.records.get(str(path))
            if (have[1] if have else None) != version:
                raise Taken()
            self.records[str(path)] = (__import__("copy").deepcopy(record),
                                       (have[1] if have else 0) + 1)


class Refused(Exception):
    pass


class Target:
    """Each range's row: the highest number raised to it, and the rows
    its span holds - emptied when a higher number first writes."""

    def __init__(self):
        self.number, self.rows, self.writes = {}, {}, []

    def raise_to(self, after, n):
        if self.number.get(after, 0) < n:
            self.number[after] = n
            self.rows[after] = []
        return self.number[after]

    def write(self, after, n, rows):
        if self.number.get(after, 0) > n:
            raise Refused(after, n)
        self.rows.setdefault(after, []).extend(rows)
        self.writes.append((after, n))


def _copy(target, after, upto, n, upto_part=None):
    target.raise_to(after, n)
    target.write(after, n, list(range(after + 1, (upto_part or upto) + 1)))


@pytest.fixture
def clock(monkeypatch):
    got = Clock()
    for mod in (L, claims, fleet):
        monkeypatch.setattr(mod, "time", got)
    return got


def _held(path, store, term=10):
    got = L.Lease(path, "move", remote=store, fenced=True, term=term)
    got.acquire()
    got._stop.set()
    got._beat.join()
    return got


def test_two_movers_a_takeover_no_range_twice_none_lost(tmp_path, clock):
    store, target = Store(), Target()
    lease_at, where = tmp_path / "lease.json", tmp_path / "ranges.json"
    a_lease = _held(lease_at, store)
    assert a_lease.fence == 1
    # the claims' term is longer than the lease's: without the takeover
    # the stopped mover's range would wait out its own term
    a_me = f"{socket.gethostname()}:{os.getpid()}"
    a = claims.Table(store, where, a_me, 30)
    b = claims.Table(store, where, "other-machine:7", 30)
    a.plan(TABLE, 0, 400, lambda: SPANS)

    after, upto, n = a.claim()
    _copy(target, after, upto, n)
    a.done(after, n, upto - after)
    a_after, _, a_n = a.claim()
    assert (a_after, a_n) == (100, 2)
    # half the range written, then the mover stops answering
    _copy(target, 100, 200, 2, upto_part=150)

    after, upto, n = b.claim()
    assert (after, n) == (200, 3)
    _copy(target, after, upto, n)
    b.done(after, n, upto - after)

    clock.now += 11
    b_lease = _held(lease_at, store)
    assert b_lease.fence == 2 and b_lease.took_over["holder"] == a_lease.me
    after, upto, n = b.claim()
    assert (after, n) == (300, 4)
    _copy(target, after, upto, n)
    b.done(after, n, upto - after)
    # range 100 is still in its claim's term: nobody may take it yet
    assert b.claim() is None

    ledger = attempts.Ledger(None)
    ledger.unit_opened(fleet.unit_name(TABLE, 100), level="range",
                       parent=TABLE)
    got = fleet.take_over(b, b_lease.took_over, ledger, name=TABLE)
    assert got.taken_back == [100] and 100 in got.uncertain
    assert got.done == [0, 200, 300] and not got.held and not got.todo
    assert any("taken back" in line for line in got.lines(TABLE))

    after, upto, n = b.claim()
    assert (after, n) == (100, 5)
    _copy(target, after, upto, n)
    b.done(after, n, upto - after)

    # the stopped mover wakes: its write, its done and its lease refused
    with pytest.raises(Refused):
        target.write(100, a_n, list(range(151, 201)))
    with pytest.raises(claims.Lost):
        a.done(100, a_n, 100)
    with pytest.raises(L.Lost):
        a_lease.check()

    rec, first = b.finish()
    assert first and not a.finish()[1]
    assert fleet.audit(rec) == []
    every = sorted(r for rows in target.rows.values() for r in rows)
    assert every == list(range(1, 401))
    for a_, _ in SPANS:
        assert target.number[a_] == rec["state"][str(a_)]["number"]
    assert {(x["after"], x["number"], x["by"]) for x in rec["lapsed"]} == {
        (100, 2, a_me)}


def test_a_mover_still_in_its_term_keeps_its_ranges(tmp_path, clock):
    store = Store()
    where = tmp_path / "ranges.json"
    a = claims.Table(store, where, "a:1", 30)
    b = claims.Table(store, where, "b:2", 30)
    a.plan(TABLE, 0, 400, lambda: SPANS)
    a.claim()
    # the lease taken over was someone else's: a's claim stands
    got = fleet.take_over(b, {"host": "c", "pid": 3}, None, name=TABLE)
    assert got.held == [0] and got.taken_back == []
    assert got.todo == [100, 200, 300]
    assert fleet.mover_of({"holder": None}) is None


def _done(spans, numbers, finished=None, lapsed=()):
    return {"ranges": spans, "numbers": max(numbers) if numbers else 0,
            "state": {str(a): {"state": claims.DONE, "by": "m",
                               "number": n} for (a, _), n in
                      zip(spans, numbers)},
            "lapsed": list(lapsed), "finished": finished}


def test_the_audit_names_what_would_be_lost_or_twice():
    assert fleet.audit(_done([[0, 100], [100, 200]], [1, 2])) == []
    gap = fleet.audit(_done([[0, 100], [150, 200]], [1, 2]))
    assert any("(100, 150]" in p and "lost" in p for p in gap)
    over = fleet.audit(_done([[0, 120], [100, 200]], [1, 2]))
    assert any("twice" in p for p in over)
    late = fleet.audit(_done([[0, 100], [100, 200]], [1, 2], lapsed=[
        {"after": 100, "number": 3, "by": "x"}]))
    assert any("not above the claim 3" in p for p in late)
    same = fleet.audit(_done([[0, 100], [100, 200]], [2, 2]))
    assert any("under one number" in p for p in same)
    rec = _done([[0, 100], [100, 200]], [1, 2], finished={"by": "m"})
    rec["state"]["100"] = {"state": claims.TODO}
    assert any("finished with the range after 100" in p
               for p in fleet.audit(rec))
    del rec["state"]["100"]
    assert any("no state" in p for p in fleet.audit(rec))
    assert fleet.audit(None)


def test_what_the_ledger_left_open_is_read_and_settled_by_the_record():
    ledger = attempts.Ledger(None)
    ledger.unit_opened(fleet.unit_name(TABLE, 0), level="range",
                       parent=TABLE)
    ledger.unit_opened(f"{TABLE} rows", level="range", parent=TABLE)
    ledger.unit_opened(fleet.unit_name("db.other", 0), level="range",
                       parent="db.other")
    ledger.unit_opened(fleet.unit_name(TABLE, 100), level="range",
                       parent=TABLE)
    ledger.unit_closed(fleet.unit_name(TABLE, 100), True)
    got = fleet.opened(ledger, TABLE)
    assert set(got) == {0, f"{TABLE} rows"}
    rec = _done([[0, 100], [100, 200]], [1, 2])
    acct = fleet.reconcile(rec, got, 0)
    assert acct.done == [0, 100] and not acct.taken_back
    said = " ".join(acct.lines(TABLE))
    assert "done in the machines' record" in said and "names no range" in said
