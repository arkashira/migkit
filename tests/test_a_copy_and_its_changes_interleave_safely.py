"""The copy and the changes around it, held to the invariants a chunked
copy under a change log has to keep (backlog R18; "Generalized DBLog",
2026): no change falls through a gap between the chunks and the log, and
a row read by the copy never overwrites a newer change or brings back a
deleted row.

migkit's shape of it: the log's position is taken before the copy, the
table is copied a chunk at a time while the source keeps changing - each
chunk seeing the table as it is when that chunk is read - and the tail
then applies every change since the position, in batches, collapsed per
row within a batch, and read again from the last saved position after a
stop. Whatever the interleaving, the target must end equal to the source.

This drives migkit's own applier (`neutral_apply`: the collapse, the
runs, updates that move a key, partial updates) against a target kept in
memory, with Hypothesis choosing the source's writes, where the chunks
fall between them, where batches end and where the tail stops and reads
again.
"""
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine


class _Memory(Engine):
    """A target that keeps its rows in a dict, applied through the base's
    own loop."""

    def __init__(self, hop):
        super().__init__(hop)
        self.rows = {}

    def _apply_upsert(self, side, db, table, key, values):
        at = key["id"]
        row = dict(self.rows.get(at, {}))
        row.update(values)
        self.rows[at] = row

    def _apply_delete(self, side, db, table, key):
        self.rows.pop(key["id"], None)


def _hop():
    ep = Endpoint(host="x", port=1, user="x", password="x")
    return Hop(name="m", engine="postgres", source=ep, target=ep,
               databases=["d"])


#: one write on the source: (kind, id, value, new id for a key move)
writes = st.lists(st.tuples(
    st.sampled_from(["insert", "update", "partial", "delete", "move"]),
    st.integers(0, 9), st.integers(0, 99), st.integers(0, 9)),
    max_size=40)


def _apply_to_source(source, log, write):
    """A write to the source model and the change its log records -
    None where the write does nothing on this state (a delete of a row
    that is not there writes nothing to a log)."""
    kind, at, value, to = write
    if kind == "insert":
        if at in source:
            return
        source[at] = {"id": at, "a": value, "b": value}
        log.append({"op": "insert", "table": "t", "key": {"id": at},
                    "values": dict(source[at])})
    elif kind in ("update", "partial"):
        if at not in source:
            return
        if kind == "update":
            source[at] = {"id": at, "a": value, "b": -value}
            values = dict(source[at])
        else:
            # a log that carries only the columns the statement changed
            source[at]["a"] = value
            values = {"a": value}
        log.append({"op": "update", "table": "t", "key": {"id": at},
                    "values": values})
    elif kind == "delete":
        if at not in source:
            return
        del source[at]
        log.append({"op": "delete", "table": "t", "key": {"id": at}})
    else:
        if at not in source or to in source:
            return
        row = source.pop(at)
        row = dict(row, id=to)
        source[to] = row
        log.append({"op": "update", "table": "t", "key": {"id": at},
                    "values": dict(row)})


@settings(max_examples=400, deadline=None,
          suppress_health_check=[HealthCheck.too_slow])
@given(before=writes, during=writes, after=writes,
       chunk_cuts=st.lists(st.integers(0, 40), max_size=4),
       batch_cuts=st.lists(st.integers(1, 40), max_size=6),
       stop_at=st.integers(0, 40), back=st.integers(0, 10))
def test_the_target_ends_as_the_source_whatever_the_interleaving(
        before, during, after, chunk_cuts, batch_cuts, stop_at, back):
    source, log = {}, []
    for w in before:
        _apply_to_source(source, log, w)
    # the position, taken before the copy
    position = len(log)
    # the copy: chunks of the key space, each read when its turn comes,
    # while the writes of `during` go on between them
    target = _Memory(_hop())
    chunks = [range(0, 4), range(4, 7), range(7, 10)]
    cuts = sorted(min(c, len(during)) for c in chunk_cuts)[:len(chunks)]
    cuts += [len(during)] * (len(chunks) - len(cuts))
    done = 0
    for span, cut in zip(chunks, cuts):
        for w in during[done:cut]:
            _apply_to_source(source, log, w)
        done = max(done, cut)
        for at in span:
            if at in source:
                target.rows[at] = dict(source[at])
            else:
                # the copy empties its own span on the target first
                target.rows.pop(at, None)
    for w in during[done:]:
        _apply_to_source(source, log, w)
    for w in after:
        _apply_to_source(source, log, w)
    # the tail: every change since the position, in batches; stopped once
    # after a batch was applied and before its position was saved, and
    # read again from `back` changes earlier than that
    pending = log[position:]
    edges = sorted({min(c, len(pending)) for c in batch_cuts}
                   | {len(pending)})
    saved, start = 0, 0
    stopped = False
    for edge in edges:
        batch = pending[start:edge]
        if batch:
            target.neutral_apply("dst", "d", batch)
        if not stopped and edge >= stop_at and edge < len(pending):
            stopped = True
            # the stop: the position saved lags what was applied
            start = max(saved - back, 0)
            target.neutral_apply("dst", "d", pending[start:edge])
        saved = start = edge
    assert target.rows == source, (log[position:], target.rows, source)
