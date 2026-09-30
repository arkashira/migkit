"""A lease the machines of a hop take over from each other carries a
number, one higher each time it is taken and kept when it is let go, and
a holder knows when it may no longer be sure it holds (backlog W1,
`lease`).

The takeover logic is driven here through every interleaving hypothesis
finds, on a clock the test moves and a store that writes only over the
version read: at no moment do two holders both believe they may write,
the numbers only rise, and a holder that was taken over never renews its
way back.
"""
import threading
import time as _time

import pytest
from hypothesis import given, settings, strategies as st

from migkit import lease as L
from migkit.state import Taken


class Clock:
    """The lease's clock, moved by the test."""

    def __init__(self):
        self.now = 1_000_000.0

    def time(self):
        return self.now

    def monotonic(self):
        return self.now

    def sleep(self, s):
        self.now += s

    strftime = staticmethod(_time.strftime)
    localtime = staticmethod(_time.localtime)


class Store:
    """Records written only over the version read, in memory."""

    def __init__(self):
        self.records, self.lock = {}, threading.Lock()

    def read_record(self, path, sealed=False):
        with self.lock:
            got = self.records.get(str(path))
        return (dict(got[0]), got[1]) if got else (None, None)

    def write_record(self, path, record, version, sealed=False):
        with self.lock:
            have = self.records.get(str(path))
            if (have[1] if have else None) != version:
                raise Taken()
            self.records[str(path)] = (dict(record),
                                       (have[1] if have else 0) + 1)


@pytest.fixture
def clock(monkeypatch):
    got = Clock()
    monkeypatch.setattr(L, "time", got)
    return got


def _held(path, store, term=10):
    """A fenced lease taken, with no heartbeat of its own: the test renews
    it when it chooses."""
    got = L.Lease(path, "move", remote=store, fenced=True, term=term)
    got.acquire()
    got._stop.set()
    got._beat.join()
    return got


def test_the_number_rises_with_each_taking_and_survives_letting_go(
        tmp_path, clock):
    store = Store()
    path = tmp_path / "lease.json"
    a = _held(path, store)
    assert a.fence == 1
    a.release()
    rec, _ = store.read_record(path)
    # let go: nobody holds it, and the number stays for the next
    assert rec["holder"] is None and rec["fence"] == 1
    b = _held(path, store)
    assert b.fence == 2 and b.took_over is None
    clock.now += 11
    c = _held(path, store)
    assert c.fence == 3 and c.took_over["holder"] == b.me


def test_a_lease_that_is_not_fenced_is_let_go_as_before(tmp_path):
    path = tmp_path / "lease.json"
    got = L.Lease(path, "move").acquire()
    got.release()
    assert not path.exists()


def test_a_file_lease_keeps_its_number_too(tmp_path, clock):
    path = tmp_path / "lease.json"
    a = L.Lease(path, "move", fenced=True, term=10).acquire()
    a._stop.set()
    a.release()
    b = L.Lease(path, "move", fenced=True, term=10).acquire()
    b._stop.set()
    assert b.fence == 2
    b.release()


def test_a_holder_not_renewed_lately_checks_before_it_writes(tmp_path,
                                                             clock):
    store = Store()
    path = tmp_path / "lease.json"
    a = _held(path, store, term=9)
    assert a.fresh()
    clock.now += 6.5
    # past two thirds of its term: not sure it holds
    assert not a.fresh()
    # a renewal that succeeds makes it sure again
    a.check()
    assert a.fresh()
    clock.now += 10
    b = _held(path, store, term=9)
    # taken over: the check says so, and nothing renews it back
    with pytest.raises(L.Lost) as e:
        a.check()
    assert "taken over" in str(e.value)
    assert not a._renew_once()
    rec, _ = store.read_record(path)
    assert rec["holder"] == b.me


def test_bump_goes_past_what_the_target_holds(tmp_path, clock):
    store = Store()
    path = tmp_path / "lease.json"
    a = _held(path, store)
    assert a.bump(7) == 8 and a.fence == 8
    a.release()
    assert _held(path, store).fence == 9


OPS = st.lists(st.one_of(
    st.tuples(st.just("take"), st.integers(0, 3)),
    st.tuples(st.just("renew"), st.integers(0, 3)),
    st.tuples(st.just("let go"), st.integers(0, 3)),
    st.tuples(st.just("wait"), st.floats(0.1, 12.0))), max_size=60)


@settings(max_examples=300, deadline=None)
@given(OPS)
def test_one_holder_at_a_time_through_any_interleaving(ops):
    import pathlib
    clock = Clock()
    real = L.time
    L.time = clock
    try:
        store, path = Store(), pathlib.Path("/nowhere/lease.json")
        machines = {}
        numbers = []
        taken_from = set()
        # every lease made stays alive: a holder's name carries its
        # object's identity, which a collected one would hand on
        everyone = []
        for op, arg in ops:
            if op == "wait":
                clock.now += arg
            elif op == "take":
                if arg in machines and machines[arg].lost is None \
                        and machines[arg].fresh():
                    continue
                got = L.Lease(path, "move", remote=store, fenced=True,
                              term=9)
                everyone.append(got)
                try:
                    got.acquire()
                except L.Held:
                    continue
                got._stop.set()
                if got.took_over:
                    taken_from.add(got.took_over["holder"])
                # every taking goes above every number before it
                assert not numbers or got.fence > max(numbers), (numbers,
                                                                 got.fence)
                numbers.append(got.fence)
                machines[arg] = got
            elif op == "renew" and arg in machines:
                held = machines[arg]
                was_lost = held.lost is not None
                ok = held._renew_once()
                if was_lost or held.me in taken_from:
                    # one that was taken over never renews its way back
                    assert not ok
            elif op == "let go" and arg in machines:
                machines.pop(arg).release()
            # at no moment may two machines both believe they may write
            sure = [m for m in machines.values() if m.fresh()]
            assert len(sure) <= 1, [m.me for m in sure]
            # and the one that is sure is the one the record names
            if sure:
                rec, _ = store.read_record(path)
                assert rec["holder"] == sure[0].me
    finally:
        L.time = real
