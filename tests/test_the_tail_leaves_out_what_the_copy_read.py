"""The tail leaves out the changes the table copier already read (backlog
R19, lever 8), held to the same invariant as the copy and its changes
(`test_a_copy_and_its_changes_interleave_safely.py`): whatever the
interleaving, the target ends equal to the source.

Each range keeps the source's snapshot taken just before its read, in the
copy's own checkpoint; the tail leaves a change out where the snapshots
of every range it touches show it committed, or where the range has not
begun, the copy that planned it holds the move, and the source shows the
change committed now. This drives the tail's own rule
(`hetero._CopiedRanges` over `ranges.already_read`, reading the
checkpoint `cli._Checkpoint` writes and the lease `lease.Lease` keeps,
and PostgreSQL's own `mark_covers`) with Hypothesis choosing the source's
writes, when each becomes visible, where each range's snapshot is taken
and where it is read, the tail's batches between them, a copy that stops
and is resumed - or never resumed and the target put level some other
way - and a tail that reads again from behind its saved position.

A transaction is in the log from its commit record on, and visible to a
read only once the server shows it - on PostgreSQL not in the log's
order (a snapshot can show a later commit and not an earlier one), on
MySQL after the binlog is written. The one order kept is a row's: its
next writer waits for the transaction before it to show. So "a change
before the position read ahead of the range" is not "a change the range
read", and the property fails on that rule
(`test_the_property_would_notice_a_change_left_out_wrongly`).

A range's read and write are one moment here, and a change the tail
applied shows before the next range is read: a tail beside a copy that
applies a change the copy's read then does not see loses it, left out or
not - which is why the tail runs after the copy (`cli._move`).
"""
import json
import os
import socket
import tempfile
import time
from pathlib import Path

import pytest
from hypothesis import HealthCheck, Phase, given, settings
from hypothesis import strategies as st

from migkit import ranges
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine, _CopiedRanges
from migkit.engines.postgres import PostgresEngine
from tests.test_a_copy_and_its_changes_interleave_safely import (
    _apply_to_source, _hop, _Memory)

#: the table's key space, as three ranges (after, upto]
SPANS = [(-1, 3), (3, 6), (6, 9)]

write = st.tuples(
    st.sampled_from(["insert", "insert", "update", "partial", "delete",
                     "move"]),
    st.integers(0, 9), st.integers(0, 99), st.integers(0, 9))
#: one step of the source, the copy or the tail; writes the likeliest, so
#: changes fall on both sides of the ranges' reads
step = st.one_of(
    *[st.tuples(st.just("write"), write)] * 4,
    *[st.tuples(st.just("show"), st.integers(0, 9))] * 2,
    st.tuples(st.just("begin"), st.none()),
    st.tuples(st.just("read"), st.none()),
    st.tuples(st.just("tail"), st.integers(1, 8)),
    st.tuples(st.just("stop"), st.none()))


def _postgres():
    ep = Endpoint(host="x", port=1, user="x", password="x")
    eng = PostgresEngine(Hop(name="p", engine="postgres", source=ep,
                             target=ep, databases=["d"]))
    eng.__dict__["_marks_on"] = {"src": "cluster 1"}
    return eng


class _Source:
    """The source: its rows with every logged transaction applied, its log
    (a transaction's number is its place there), the transactions logged
    and not shown yet, and each row's history - what a read sees."""

    def __init__(self):
        self.rows, self.log, self.pending, self.history = {}, [], [], {}
        self.pg = _postgres()

    def write(self, w):
        kind, at, value, to = w
        touched = {at, to} if kind == "move" else {at}
        # a row's writer waits for the transaction before it on that row
        # to show: its lock is let go after that
        for k in touched:
            for txn, _ in self.history.get(k, []):
                self.show_txn(txn)
        n = len(self.log)
        _apply_to_source(self.rows, self.log, w)
        if len(self.log) == n:
            return
        self.log[-1]["txn"] = n
        for k in touched:
            self.history.setdefault(k, []).append(
                (n, dict(self.rows[k]) if k in self.rows else None))
        self.pending.append(n)

    def show(self, i):
        if self.pending:
            self.pending.pop(i % len(self.pending))

    def show_txn(self, txn):
        if txn in self.pending:
            self.pending.remove(txn)

    def seen(self, k):
        """Row `k` as a read now sees it."""
        for txn, row in reversed(self.history.get(k, [])):
            if txn not in self.pending:
                return dict(row) if row is not None else None
        return None

    def snapshot_now(self):
        """PostgreSQL's snapshot, over transaction numbers that are log
        places: `xmin:xmax:xip`."""
        top = len(self.log)
        return (f"cluster 1|{min(self.pending, default=top)}:{top}:"
                + ",".join(str(x) for x in self.pending))

    def snapshot_mark(self, side, db):
        return self.snapshot_now()

    def mark_covers(self, side, db, mark, change):
        return self.pg.mark_covers(side, db, mark, change)


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop, src):
        self.hop, self.src_engine = hop, src


class _Run:
    """The source, the target, the copy's checkpoint and lease, and the
    tail's saved position."""

    def __init__(self, root):
        from migkit.cli import _Checkpoint
        self.root = Path(root)
        hop = _hop()
        hop.report_dir = self._dir
        self.src = _Source()
        self.target = _Memory(_hop())
        self.copied = _CopiedRanges(_Pair(hop, self.src), "d",
                                    self._dir("d"))
        self.ck = _Checkpoint(self._dir("d") / "move.json")
        self.st = self.ck.setdefault("d.t", {})
        self.todo, self.begun, self.copying = list(SPANS), None, False
        self.position, self.at, self.left = 0, 0, 0
        # transactions the tail applied: they show before the next read
        self.applied = set()

    def _dir(self, db=None):
        d = self.root / (db or "")
        d.mkdir(parents=True, exist_ok=True)
        return d

    def hold(self, on):
        """The copy holds the move, as `cli._lock` does, or lets go."""
        path = self._dir() / "lease.json"
        if on:
            path.write_text(json.dumps(
                {"holder": "copy", "host": socket.gethostname(),
                 "pid": os.getpid(), "what": "a copy",
                 "since": time.time(), "expires": time.time() + 600}))
        else:
            path.unlink(missing_ok=True)
        self.copying = on

    def plan(self):
        ranges.plan(self.st, 0, 9, lambda: [3, 6], self.ck.save, table="t")
        self.st["key"] = "id"
        self.hold(True)

    def begin(self):
        if self.copying and self.begun is None and self.todo:
            self.begun = self.todo.pop(0)
            ranges.started(self.st, self.begun[0], self.src.snapshot_now(),
                           self.ck.save)

    def read(self):
        if self.begun is None:
            return
        for txn in self.applied:
            self.src.show_txn(txn)
        after, upto = self.begun
        # the copy empties its own span on the target and writes what a
        # read of the source sees there now
        for at in range(after + 1, upto + 1):
            row = self.src.seen(at)
            if row is not None:
                self.target.rows[at] = row
            else:
                self.target.rows.pop(at, None)
        ranges.finished(self.st, after, self.ck.save)
        self.begun = None

    def level(self):
        """The target put level with what a read of the source sees, some
        other way than the copy."""
        for txn in self.applied:
            self.src.show_txn(txn)
        self.target.rows = {k: self.src.seen(k) for k in range(10)
                            if self.src.seen(k) is not None}

    def stop(self):
        if self.copying:
            if self.begun is not None:
                # begun and never read: the next copy begins it again
                self.todo.insert(0, self.begun)
                self.begun = None
            self.hold(False)

    def tail(self, n, start=None):
        """One batch read from the saved position - or from `start`, as a
        tail started again reads from behind it - the copy's changes left
        out, the rest applied."""
        start = self.at if start is None else start
        # each numbered, to know which the tail kept
        batch = [dict(c, n=i) for i, c in enumerate(
            self.src.log[self.position + start:self.position + start + n])]
        keep, left = self.copied.leave_out(batch)
        assert all("txn" not in c for c in keep)
        assert sum(left.values()) + len(keep) == len(batch)
        self.left += sum(left.values())
        kept = {c.pop("n") for c in keep}
        self.applied |= {c["txn"] for c in batch if c["n"] in kept}
        if keep:
            self.target.neutral_apply("dst", "d", keep)
        self.at = start + len(batch)
        return len(batch)


@settings(max_examples=500, deadline=None,
          suppress_health_check=[HealthCheck.too_slow])
@given(before=st.lists(write, max_size=15),
       steps=st.lists(step, min_size=10, max_size=80),
       resume=st.booleans(), after=st.lists(write, max_size=15),
       back=st.integers(0, 10))
def test_the_target_ends_as_the_source_with_the_copys_changes_left_out(
        before, steps, resume, after, back):
    _the_target_ends_as_the_source(before, steps, resume, after, back)


def _the_target_ends_as_the_source(before, steps, resume, after, back):
    with tempfile.TemporaryDirectory() as root:
        run = _Run(root)
        for w in before:
            run.src.write(w)
        # the position, taken before the copy - and what was logged before
        # it shown by then: one logged before the position and shown only
        # after a range is read is in neither the copy nor the tail, left
        # out or not
        run.src.pending.clear()
        run.position = len(run.src.log)
        run.plan()
        for kind, arg in steps:
            if kind == "write":
                run.src.write(arg)
            elif kind == "show":
                run.src.show(arg)
            elif kind == "begin":
                run.begin()
            elif kind == "read":
                run.read()
            elif kind == "tail":
                run.tail(arg)
            else:
                run.stop()
        if run.begun is not None:
            run.read()
        if run.todo and not run.copying:
            if resume:
                # started again: the ranges not done, from the plan
                run.plan()
            else:
                # never resumed: the checkpoint left as the stopped copy
                # saved it
                run.level()
                run.todo = []
        while run.todo:
            run.begin()
            run.read()
        run.hold(False)
        for w in after:
            run.src.write(w)
        run.src.pending.clear()
        # caught up, stopped, and read again from `back` changes behind
        while run.tail(5):
            pass
        run.tail(10 ** 6, start=max(run.at - back, 0))
        assert run.target.rows == run.src.rows, (
            run.src.log[run.position:], run.target.rows, run.src.rows)
        return run


def test_changes_the_copy_read_are_left_out_and_the_rest_applied(tmp_path):
    """A change shown before a range was read is left out, one after it
    applied; one to a range not begun is left to the copy while it runs
    once the source shows it, and not before - nor once the copy has
    stopped."""
    run = _Run(tmp_path)
    run.plan()
    run.src.write(("insert", 1, 10, 0))      # shown before the first range
    run.src.pending.clear()
    run.begin()
    run.read()
    run.src.write(("update", 1, 11, 0))      # after it: applied
    run.src.write(("insert", 5, 50, 0))      # a range not begun, shown
    run.src.show_txn(2)
    run.src.write(("insert", 6, 60, 0))      # logged, not shown: applied
    assert run.tail(10) == 4 and run.left == 2, run.left
    assert run.target.rows[1] == {"id": 1, "a": 11, "b": -11}
    assert 5 not in run.target.rows and 6 in run.target.rows
    run.stop()
    run.src.write(("update", 5, 51, 0))      # no copy running: applied
    run.src.pending.clear()
    run.tail(10)
    assert run.left == 2
    assert run.target.rows[5] == {"id": 5, "a": 51, "b": -51}


def test_a_commit_logged_before_the_snapshot_and_not_shown_is_applied(
        tmp_path):
    """The position rule's loss: a transaction logged before the range's
    snapshot was taken, and not visible to it - the read does not have
    it, so the tail applies it."""
    run = _Run(tmp_path)
    run.plan()
    run.src.write(("insert", 1, 10, 0))      # logged first, shown last
    run.src.write(("insert", 7, 70, 0))
    run.src.show_txn(1)
    run.begin()
    run.read()                               # 0..3: row 1 not seen yet
    run.src.pending.clear()
    run.begin()
    run.read()
    run.begin()
    run.read()
    run.hold(False)
    run.tail(10)
    assert run.left == 1                     # 7, read with 7..9
    assert run.target.rows == run.src.rows


def test_a_key_moved_from_a_range_read_to_one_read_before_it_is_applied(
        tmp_path):
    run = _Run(tmp_path)
    run.plan()
    run.src.write(("insert", 7, 70, 0))
    run.src.pending.clear()
    run.begin()
    run.read()                               # 0..3, before the move
    run.src.write(("move", 7, 0, 2))         # 7 -> 2
    run.src.pending.clear()
    run.begin()
    run.read()                               # 4..6
    run.begin()
    run.read()                               # 7..9, after the move
    run.hold(False)
    run.tail(10)
    # the insert was read with 7..9; the move is shown there and not in
    # 0..3, so it is applied - and 2 arrives
    assert run.left == 1
    assert run.target.rows == run.src.rows == {2: {"id": 2, "a": 70,
                                                   "b": 70}}


def test_a_change_to_a_table_copied_with_no_ranges_is_applied(tmp_path):
    run = _Run(tmp_path)
    run.st.clear()
    run.st.update({"last": 9, "done": True})
    run.ck.save()
    run.src.write(("insert", 1, 1, 0))
    run.src.pending.clear()
    run.tail(10)
    assert run.left == 0 and run.target.rows == run.src.rows


def test_what_the_tail_is_told_to_apply_all_of_it_it_applies(
        tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_TAIL_APPLY_ALL", "1")
    run = _Run(tmp_path)
    run.plan()
    run.src.write(("insert", 1, 10, 0))
    run.src.pending.clear()
    run.begin()
    run.read()
    run.tail(10)
    assert run.left == 0


@pytest.mark.parametrize("broken", ["position", "uncovered", "any_mark",
                                    "not_running", "not_shown"])
def test_the_property_would_notice_a_change_left_out_wrongly(
        broken, monkeypatch):
    """The rule turned the wrong way each way it can be - Hypothesis finds
    a target that ends unlike its source every time:

    * position: a change is taken as read when it is before the position
      read ahead of the range (`xmax`), shown or not;
    * uncovered: changes the snapshot does not show left out;
    * any_mark: every range read taken as holding every change;
    * not_running: a range not begun left to a copy that is not running;
    * not_shown: a range not begun left to the copy for a change the
      source does not show yet."""
    real = ranges.already_read
    if broken == "position":
        monkeypatch.setattr(
            _Source, "mark_covers",
            lambda self, side, db, mark, change:
                change["txn"] < int(mark.split("|")[1].split(":")[1]))
    elif broken == "not_shown":
        # what the tail asks now, answered as though every logged change
        # showed; the copy's own snapshots stay true
        monkeypatch.setattr(
            _Source, "snapshot_mark",
            lambda self, side, db: f"cluster 1|{len(self.log)}:"
                                   f"{len(self.log)}:")
    else:
        def wrong(st_, values, covers, ahead=False):
            if broken == "uncovered":
                return real(st_, values, lambda m: covers(m) is not True,
                            ahead)
            if broken == "any_mark":
                return real(st_, values, lambda m: True, ahead)
            return real(st_, values, covers, ahead=True)
        monkeypatch.setattr(ranges, "already_read", wrong)

    # found, not shrunk: the failure is the answer here. A fixed search
    # (derandomize): drawn at random, 1,000 examples missed the
    # not_shown case about one run in six (measured), and a check of
    # the property's teeth that passes by luck is no check
    @settings(max_examples=3000, deadline=None, database=None,
              derandomize=True, phases=[Phase.generate],
              suppress_health_check=[HealthCheck.too_slow])
    @given(before=st.lists(write, max_size=15),
           steps=st.lists(step, min_size=10, max_size=80),
           resume=st.booleans(),
           after=st.lists(write, max_size=15), back=st.integers(0, 10))
    def prop(before, steps, resume, after, back):
        _the_target_ends_as_the_source(before, steps, resume, after, back)
    with pytest.raises(AssertionError):
        prop()


def test_a_postgresql_snapshot_covers_what_it_shows_committed():
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="x", port=1, user="x", password="x")
    eng = PostgresEngine(Hop(name="p", engine="postgres", source=ep,
                             target=ep, databases=["d"]))
    eng.__dict__["_marks_on"] = {"src": "cluster 7"}

    def covers(mark, xid):
        return eng.mark_covers("src", "d", mark, {"txn": xid})
    mark = "cluster 7|100:110:103,107"
    assert covers(mark, 99) is True            # below xmin
    assert covers(mark, 101) is True           # committed before it
    assert covers(mark, 103) is False          # in progress then
    assert covers(mark, 110) is False          # began after it
    assert covers("cluster 8|100:110:", 99) is None     # another server
    assert covers(mark, None) is None
    # the log's xid is 32 bits, the snapshot's 64: the epoch is the
    # snapshot's, and an xid just past a wraparound is still the later one
    epoch = 5 << 32
    wrapped = f"cluster 7|{epoch - 20}:{epoch + 3}:"
    assert covers(wrapped, (epoch - 21) & 0xFFFFFFFF) is True
    assert covers(wrapped, 1) is True
    assert covers(wrapped, 3) is False
    assert covers(wrapped, (epoch - 10) & 0xFFFFFFFF) is True


def test_a_gtid_set_covers_the_transactions_in_it():
    from migkit.engines.mysql import MySQLEngine
    uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
    other = "4e11fa47-71ca-11e1-9e33-c80aa9429562"
    mark = f"{uuid}:1-5:7,{other}:3:tag:1-2"

    def covers(gtid):
        return MySQLEngine.mark_covers("src", "d", mark, {"txn": gtid})
    assert covers(f"{uuid}:5") is True
    assert covers(f"{uuid}:6") is False
    assert covers(f"{uuid}:7") is True
    assert covers(f"{other}:3") is True
    assert covers(f"{other}:1") is False
    assert covers(f"{other}:tag:2") is True
    assert covers(f"{other}:tag:3") is False
    assert covers(f"{uuid.upper()}:2") is True
    assert MySQLEngine.mark_covers("src", "d", mark, {}) is None


def test_a_range_is_found_by_its_bounds_and_nothing_outside_the_plan():
    st_ = {"ranges": [[0, 10], [10, 20]], "ranges_done": [0, 10],
           "ranges_seen": {"0": 5, "10": None}}

    def yes(mark):
        return True
    assert ranges.already_read(st_, [1], yes)
    assert ranges.already_read(st_, [10], yes)          # (0, 10]
    assert not ranges.already_read(st_, [11], yes)      # read under no mark
    assert not ranges.already_read(st_, [0], yes)       # below the plan
    assert not ranges.already_read(st_, [21], yes)      # past it
    assert not ranges.already_read(st_, [1, 11], yes)   # a key moved
    assert not ranges.already_read(st_, [True], yes)
    assert not ranges.already_read(st_, ["1"], yes)
    assert not ranges.already_read({"last": 20}, [1], yes)
