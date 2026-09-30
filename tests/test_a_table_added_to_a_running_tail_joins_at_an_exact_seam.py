"""A table added to a running tail is copied beside the stream under its
own marks and joins at an exact seam (backlog L1, `tailtables`):
whatever the interleaving of the source's writes, the copy's ranges and
the tail's batches, the target ends equal to the source.

The tail has been reading all along and left the table's changes out
while it was not in scope; it joins at the position the tail had saved,
and what was logged before that is shown before the copy begins
(`Engine.visible_now`). Each range of the copy is begun (the source's
mark taken), read (the rows as a read sees them now, held by the copy)
and landed (written over its span on the target) in three steps, so a
batch of the tail can fall between a range's read and its write: a change
applied there would be written over by the older rows the copy read. The
tail's own rule - `TableSet.release` and `route` over `_verdict` - holds
such a change until its range lands, then holds it to the range's mark:
left out where the mark shows it committed, applied where it does not.
Transactions are logged in one order and shown in another, as PostgreSQL
shows them (`_Source`, from the lever-8 test); the property fails on a
rule that applies a change to a range being read
(`test_the_property_would_notice_a_change_applied_under_a_copy`).
"""
import tempfile
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from migkit import ranges, tailtables
from migkit.engines.hetero import HeteroEngine
from tests.test_a_copy_and_its_changes_interleave_safely import (
    _hop, _Memory)
from tests.test_the_tail_leaves_out_what_the_copy_read import (
    SPANS, _Source, write)

step = st.one_of(
    *[st.tuples(st.just("write"), write)] * 4,
    *[st.tuples(st.just("show"), st.integers(0, 9))] * 2,
    st.tuples(st.just("begin"), st.none()),
    st.tuples(st.just("read"), st.none()),
    st.tuples(st.just("land"), st.none()),
    *[st.tuples(st.just("tail"), st.integers(1, 8))] * 2)


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop, src):
        self.hop, self.src_engine = hop, src

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


class _Run:
    """The source, the target, the tail's tables and the added table's
    copy, and how far the tail has read."""

    def __init__(self, root):
        self.root = Path(root)
        self.src = _Source()
        self.target = _Memory(_hop())
        hop = _hop()
        hop.report_dir = lambda db=None: self.root
        self.tables = tailtables.TableSet(_Pair(hop, self.src), "d",
                                          self.root / "tail-token.json",
                                          lambda m: None)
        # a hop made here is in no hop file
        self.tables.file.said = None
        self.at = 0
        self.joined = False

    def join(self):
        """The table joins at the tail's position: what was logged before
        it is shown before the copy begins (`visible_now`), and the copy's
        ranges are planned."""
        for txn in [t for t in self.src.pending if t < self.at]:
            self.src.show_txn(txn)
        self.tables.fresh = False
        self.tables.tables = {"t": {"state": "copying", "name": "t",
                                    "way": "copy"}}
        self.ck = self.tables.copies()
        self.st = self.ck.setdefault("d.t", {})
        ranges.plan(self.st, 0, 9, lambda: [3, 6], self.ck.save,
                    table="t")
        self.st["key"] = "id"
        self.ck.save()
        self.todo, self.begun, self.held = list(SPANS), None, None
        self.joined = True

    def begin(self):
        if self.joined and self.begun is None and self.todo:
            self.begun = self.todo.pop(0)
            ranges.started(self.st, self.begun[0], self.src.snapshot_now(),
                           self.ck.save)

    def read(self):
        if self.begun is not None and self.held is None:
            after, upto = self.begun
            self.held = {k: self.src.seen(k)
                         for k in range(after + 1, upto + 1)}

    def land(self):
        if self.held is None:
            return
        # the copy empties its own span on the target and writes what it
        # read there
        for k, row in self.held.items():
            if row is None:
                self.target.rows.pop(k, None)
            else:
                self.target.rows[k] = row
        ranges.finished(self.st, self.begun[0], self.ck.save)
        self.begun, self.held = None, None
        if not self.todo:
            self.st["done"] = True
            self.ck.save()

    def tail(self, n):
        """One batch from where the tail has read: before the table joins
        its changes are left out, after it routed by the tables' states."""
        batch = [dict(c) for c in self.src.log[self.at:self.at + n]]
        token = self.at
        self.at += len(batch)
        if not self.joined:
            return len(batch)
        states = self.tables.begin()
        released, _, _ = self.tables.release(states)
        out, left = self.tables.route(states, batch, token)
        # every change of the batch applied, or left out and why - the
        # identity the tail's accounts hold it to
        assert len(out) + sum(sum(c.values()) for c in left.values()) \
            == len(batch), (batch, out, left)
        go = [{k: v for k, v in c.items() if k not in ("txn", "txn_end")}
              for c in released + out]
        if go:
            self.target.neutral_apply("dst", "d", go)
        self.tables.commit(states)
        return len(batch)


def _ends_as_the_source(before, steps, after):
    with tempfile.TemporaryDirectory() as root:
        run = _Run(root)
        for w in before:
            run.src.write(w)
        # the tail read what was there while the table was out of scope,
        # and saved its position; then the table joins
        run.tail(10 ** 6)
        run.join()
        for kind, arg in steps:
            if kind == "write":
                run.src.write(arg)
            elif kind == "show":
                run.src.show(arg)
            elif kind == "tail":
                run.tail(arg)
            else:
                getattr(run, kind)()
        while run.begun is not None or run.todo:
            run.begin()
            run.read()
            run.land()
        for w in after:
            run.src.write(w)
        run.src.pending.clear()
        while run.tail(5):
            pass
        # one more batch: what is still held once the copy is done goes
        run.tail(1)
        assert run.tables.tables["t"]["state"] == "live", run.tables.tables
        assert run.target.rows == run.src.rows, (run.target.rows,
                                                 run.src.rows)
        return run


@settings(max_examples=500, deadline=None,
          suppress_health_check=[HealthCheck.too_slow])
@given(before=st.lists(write, max_size=15),
       steps=st.lists(step, min_size=10, max_size=80),
       after=st.lists(write, max_size=10))
def test_the_added_table_ends_as_the_source(before, steps, after):
    _ends_as_the_source(before, steps, after)


def test_a_change_to_a_range_being_read_waits_for_it_to_land(tmp_path):
    """Read, then changed, then the tail's batch, then landed: the change
    is held while the range is read and applied once it has landed."""
    run = _Run(tmp_path)
    run.src.write(("insert", 1, 10, 0))
    run.src.pending.clear()
    run.tail(10)
    run.join()
    run.begin()
    run.read()
    run.src.write(("update", 1, 11, 0))
    run.src.pending.clear()
    run.tail(10)
    assert 1 not in run.target.rows, "applied under a range being read"
    assert run.tables.spool("t").changes(), "not held"
    while run.todo or run.begun is not None:
        run.begin()
        run.read()
        run.land()
    run.tail(1)
    assert run.target.rows[1] == {"id": 1, "a": 11, "b": -11}
    assert not run.tables.spool("t").changes()
    assert run.tables.tables["t"]["state"] == "live"


def test_the_property_would_notice_a_change_applied_under_a_copy(
        tmp_path, monkeypatch):
    """Teeth: a rule that applies a change to a range being read loses it
    - the copy writes the older rows it read over it - and the property
    says the target differs."""
    real = tailtables.TableSet._verdict

    def careless(self, leaf, st, change):
        got = real(self, leaf, st, change)
        entry = self.entries.get(self._entry(leaf)) or {}
        values = [change["key"]["id"]]
        if got == "held" and all(w == "reading" for w, _ in
                                 ranges.place(entry, values)):
            return "apply"
        return got
    monkeypatch.setattr(tailtables.TableSet, "_verdict", careless)
    steps = [("begin", None), ("read", None),
             ("write", ("update", 1, 11, 0)), ("show", 0),
             ("tail", 8), ("land", None)]
    with pytest.raises(AssertionError):
        _ends_as_the_source([("insert", 1, 10, 0)], steps, [])


def test_a_batch_read_again_is_not_set_aside_twice(tmp_path):
    """Set aside, then the batch not saved - a lost connection, a stop -
    and read again: what it set aside is dropped first (`forget`,
    `resumed`), so each change is held once."""
    run = _Run(tmp_path)
    run.tail(10)
    run.join()
    run.begin()
    run.src.write(("insert", 2, 20, 0))
    run.src.pending.clear()
    states = run.tables.begin()
    run.tables.route(states, [dict(run.src.log[0])], 0)
    assert len(run.tables.spool("t").changes()) == 1
    run.tables.forget(0)
    assert run.tables.spool("t").changes() == []
    states = run.tables.begin()
    run.tables.route(states, [dict(run.src.log[0])], 0)
    run.tables.commit(states)
    assert len(run.tables.spool("t").changes()) == 1
