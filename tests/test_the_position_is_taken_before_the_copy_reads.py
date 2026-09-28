"""The change position is taken before the copy reads anything, and every
change after it reaches the target - the rule from the RDS legs: the slot
first, the snapshot after, never the other way round. The gap between a
snapshot and a slot made after it is silent and cannot be recovered; the
counts do not see it, only the rows' contents do.

Held here as a property of the order `move --mode full+cdc` keeps, on a
model of a source and its change log where Hypothesis writes rows at every
point between the position, the copy's snapshot, each of its reads and the
tail's batches:

* a change enters the log in commit order and becomes visible to readers
  later, in an order of its own: PostgreSQL makes a commit visible when it
  leaves the ProcArray, which is not commit order - a commit waiting on a
  synchronous standby is in the log and out of sight while later ones are
  read - and MySQL writes a transaction to the binlog before the storage
  engine commits it. Only the changes to one row become visible in the
  order they were made, since the second waits on the first's lock.
* the copy reads either one snapshot for everything or each range as it is
  when that range is read
* the tail applies every change from the position on, through migkit's own
  applier (`Engine.neutral_apply`), in batches

Whatever the interleaving, the target ends equal to the source - as long as
the position is taken before the copy's first read and every change before
it is visible by then: PostgreSQL's slot made with its exported snapshot,
and migkit waiting until what that snapshot holds is visible to any read
(`test_rows_written_around_the_slot_arrive.py`). A change after the
position that the copy already saw is replayed over it, which converges.
Both halves of the condition are shown to matter: taken away, the model
loses a row, so the property is not true of every order by accident.
"""
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine


class _Memory(Engine):
    """A target that keeps its rows in a dict, written through the base's
    own apply loop."""

    def __init__(self):
        ep = Endpoint(host="x", port=1, user="x", password="x")
        super().__init__(Hop(name="m", engine="postgres", source=ep,
                             target=ep, databases=["d"]))
        self.rows = {}

    def _apply_upsert(self, side, db, table, key, values):
        at = key["id"]
        row = dict(self.rows.get(at, {}))
        row.update(values)
        self.rows[at] = row

    def _apply_delete(self, side, db, table, key):
        self.rows.pop(key["id"], None)


class _Source:
    """Rows, the log of every change in commit order with the rows each
    touches, and which of those changes a reader can see yet."""

    def __init__(self):
        self.rows, self.log, self.touched, self.shown = {}, [], [], set()

    def write(self, kind, at, value, to):
        rows, log = self.rows, self.log
        n = len(log)
        if kind == "insert" and at not in rows:
            rows[at] = {"id": at, "a": value, "b": value}
            log.append({"op": "insert", "table": "t", "key": {"id": at},
                        "values": dict(rows[at])})
        elif kind == "update" and at in rows:
            rows[at] = {"id": at, "a": value, "b": -value}
            log.append({"op": "update", "table": "t", "key": {"id": at},
                        "values": dict(rows[at])})
        elif kind == "partial" and at in rows:
            rows[at]["a"] = value
            log.append({"op": "update", "table": "t", "key": {"id": at},
                        "values": {"a": value}})
        elif kind == "delete" and at in rows:
            del rows[at]
            log.append({"op": "delete", "table": "t", "key": {"id": at}})
        elif kind == "move" and at in rows and to not in rows:
            row = dict(rows.pop(at), id=to)
            rows[to] = row
            log.append({"op": "update", "table": "t", "key": {"id": at},
                        "values": dict(row)})
            self.touched.append({at, to})
            return
        if len(log) > n:
            self.touched.append({at})

    def show(self, pick):
        """One change becomes visible: the `pick`th of those not yet - or,
        where an earlier change to one of its rows is still out of sight,
        that one first."""
        hidden = [i for i in range(len(self.log)) if i not in self.shown]
        if not hidden:
            return
        at = hidden[pick % len(hidden)]
        while True:
            before = [i for i in hidden
                      if i < at and self.touched[i] & self.touched[at]]
            if not before:
                break
            at = before[0]
        self.shown.add(at)

    def visible(self):
        """The rows as a reader sees them now: the visible changes, in
        commit order - for each row a prefix of its own changes."""
        seen = {}
        for i, change in enumerate(self.log):
            if i in self.shown:
                _Memory.neutral_apply(_Reader(seen), "dst", "d", [change])
        return seen

    def settle(self, upto=None):
        self.shown |= set(range(len(self.log) if upto is None else upto))


class _Reader(_Memory):
    def __init__(self, rows):
        super().__init__()
        self.rows = rows


def _position(source, waits):
    """Where the tail starts. A position taker that waits returns once every
    change before it is visible - PostgreSQL's slot with its exported
    snapshot and the wait after it. One that does not can hand back a point
    past a change nobody can read yet."""
    point = len(source.log)
    if waits:
        source.settle(point)
    return point


def _copy(source, target, span, snapshot):
    read = snapshot if snapshot is not None else source.visible()
    for at in span:
        if at in read:
            target.rows[at] = dict(read[at])
        else:
            target.rows.pop(at, None)


#: a write, or one more change of the log becoming visible
steps = st.lists(st.one_of(
    st.tuples(st.sampled_from(["insert", "update", "partial", "delete",
                               "move"]),
              st.integers(0, 9), st.integers(0, 99), st.integers(0, 9)),
    st.tuples(st.just("show"), st.integers(0, 50))), max_size=12)


def _run(source, gap):
    for step in gap:
        if step[0] == "show":
            source.show(step[1])
        else:
            source.write(*step)


def _move(before, gaps, one_snapshot, waits, batch_cuts, order="slot-first"):
    """One full+cdc on the model: `gaps` are what the source does before
    the position, between it and the snapshot, before each of the three
    reads, after the copy and between the tail's batches."""
    source, target = _Source(), _Memory()
    for step in before:
        if step[0] != "show":
            source.write(*step)
    source.settle()
    g = iter(gaps)
    # the move starts while the source is committing
    _run(source, next(g))
    if order == "slot-first":
        point = _position(source, waits)
        _run(source, next(g))
        snapshot = source.visible() if one_snapshot else None
    else:
        snapshot = source.visible() if one_snapshot else None
        _run(source, next(g))
        point = _position(source, waits)
    for span in (range(0, 4), range(4, 7), range(7, 10)):
        _run(source, next(g))
        _copy(source, target, span, snapshot)
    _run(source, next(g))
    # the tail, from the position, batch by batch while the source goes on
    start = point
    for cut in batch_cuts:
        end = min(start + cut, len(source.log))
        target.neutral_apply("dst", "d", source.log[start:end])
        start = end
        _run(source, next(g, []))
    source.settle()
    target.neutral_apply("dst", "d", source.log[start:])
    return target.rows, source.rows


@settings(max_examples=500, deadline=None,
          suppress_health_check=[HealthCheck.too_slow])
@given(before=steps, gaps=st.lists(steps, min_size=11, max_size=11),
       one_snapshot=st.booleans(),
       batch_cuts=st.lists(st.integers(1, 20), max_size=5))
def test_the_target_ends_as_the_source_when_the_position_comes_first(
        before, gaps, one_snapshot, batch_cuts):
    got, want = _move(before, gaps, one_snapshot, True, batch_cuts)
    assert got == want


# The same model, the order or the wait taken away: each loses a row, so
# the property above holds because of them.

def test_a_position_taken_after_the_snapshot_loses_the_row_between():
    # the snapshot, a row written, then the position: the snapshot does
    # not hold the row and the tail starts past it
    gaps = [[], [("insert", 1, 7, 0), ("show", 0)]] + [[] for _ in range(9)]
    got, want = _move([], gaps, True, True, [], order="snapshot-first")
    assert 1 in want and 1 not in got
    # slot first, the same write in the same place arrives
    got, want = _move([], gaps, True, True, [])
    assert got == want


def test_a_commit_before_the_position_out_of_sight_of_the_read_is_lost():
    """The shape the visibility race takes: T1 commits first and T2 after,
    T2 is visible and T1 is not - a snapshot sees T2 and misses T1 - and a
    position read beside that snapshot is past both. The tail skips T1
    and the copy never saw it. A position taker that waits for what is
    before it makes the copy read T1."""
    for waits, lost in ((False, True), (True, False)):
        source = _Source()
        source.write("insert", 1, 7, 0)      # T1
        source.write("insert", 2, 8, 0)      # T2, committed after T1
        source.show(1)                       # T2 visible first
        assert source.visible() == {2: {"id": 2, "a": 8, "b": 8}}
        point = _position(source, waits)
        target = _Memory()
        _copy(source, target, range(0, 10), source.visible())
        source.settle()
        target.neutral_apply("dst", "d", source.log[point:])
        assert (1 not in target.rows) is lost, (waits, target.rows)
        assert (target.rows == source.rows) is not lost


def test_a_commit_after_the_position_that_the_read_saw_is_replayed_safely():
    """The reverse: T2 commits after the position and the read already
    sees it; the tail applies it again over the copied row, and a later
    partial change of the same row lands on top."""
    source = _Source()
    source.write("insert", 1, 7, 0)
    source.settle()
    point = _position(source, True)
    source.write("update", 1, 9, 0)
    source.show(0)
    target = _Memory()
    _copy(source, target, range(0, 10), None)
    assert target.rows[1]["a"] == 9
    source.write("partial", 1, 11, 0)
    source.settle()
    target.neutral_apply("dst", "d", source.log[point:])
    assert target.rows == source.rows
