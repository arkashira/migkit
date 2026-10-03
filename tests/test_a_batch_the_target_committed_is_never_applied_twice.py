"""A batch the target committed is never applied twice, however far the
saved position fell behind it (audit 2026-10-01, ST2).

The target's mark of the last batch it committed is written in the
batch's own transaction (`twoway.batch_seen`). A tail going on after a
stop asked it whether it was one batch ahead of the saved position, and
only one: a position a power loss rolled back further, or one another
machine kept a few seconds old (`state.TailMirror`), fell through and the
batches between were applied again - their counters added twice. Now the
table's mark is believed however far ahead it stands where it names this
run's first number (`run`), within `twoway.WINDOW` where an earlier mark
names none; a rung that keeps only where the batch was committed stops
where the target is past the one batch the tail wrote down. And the
tail's own state set down for the batch the target committed stands with
it (`hetero._committed_ahead`).
"""
import json

import pytest

from migkit import tailctl, tailtables, twoway
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine


class _Table:
    """A target whose table rung keeps the last mark committed."""

    def __init__(self, seen):
        self.seen = seen

    def origin_seen(self, side, db):
        return self.seen


def test_the_tables_mark_is_believed_however_far_ahead_for_this_run():
    base = 1 << 40
    # five batches ahead of a rolled-back position, this run's: gone on
    # after the fifth, none applied again
    got = twoway.committed_ahead(
        _Table({"token": "0/9", "batch": base + 5, "run": base}), "d",
        base, run=base)
    assert got == ("0/9", base + 5)
    # far beyond the window, still this run's
    assert twoway.committed_ahead(
        _Table({"token": "0/F", "batch": base + 10 ** 6, "run": base}), "d",
        base, run=base) == ("0/F", base + 10 ** 6)
    # another run's mark is never this run's batch
    assert twoway.committed_ahead(
        _Table({"token": "0/9", "batch": base + 1, "run": base + 7}), "d",
        base, run=base) is None
    # nothing past the saved batch: go on from the position
    assert twoway.committed_ahead(
        _Table({"token": "0/1", "batch": base, "run": base}), "d", base,
        run=base) is None
    assert twoway.committed_ahead(_Table(None), "d", base, run=base) is None


def test_a_mark_that_names_no_run_is_believed_within_the_window():
    base = 1 << 40
    assert twoway.committed_ahead(
        _Table({"token": "0/9", "batch": base + 3}), "d", base) == \
        ("0/9", base + 3)
    assert twoway.committed_ahead(
        _Table({"token": "0/9", "batch": base + twoway.WINDOW + 1}), "d",
        base) is None


class _Gtid:
    """A rung that says whether a batch was committed - every batch up to
    the last, as a GTID set does."""

    def __init__(self, last):
        self.last = last

    def mark_committed(self, side, db, rung, batch):
        return batch <= self.last


class _Origin(_Gtid):
    """A rung whose mark is the last batch only, as an origin's progress
    is."""

    def mark_committed(self, side, db, rung, batch):
        return batch == self.last


@pytest.mark.parametrize("rung", [_Gtid, _Origin])
def test_a_rung_that_keeps_no_end_stops_past_the_batch_written_down(rung):
    base = 1 << 40
    pending = {"token": "0/5", "batch": base + 1}
    # the one batch written down before it was applied: gone on after it
    assert twoway.committed_ahead(rung(base + 1), "d", base, "origin",
                                  pending, run=base - 3) == ("0/5", base + 1)
    # at the saved batch: from the position
    assert twoway.committed_ahead(rung(base), "d", base, "origin", None,
                                  run=base - 3) is None
    # past it: where those batches ended is known nowhere - stopped,
    # nothing applied
    with pytest.raises(SystemExit) as e:
        twoway.committed_ahead(rung(base + 4), "d", base, "origin", pending,
                               run=base - 3)
    assert "past the one after its saved position" in str(e.value)
    # a fresh run has committed nothing yet: nothing to hold it to
    assert twoway.committed_ahead(rung(17), "d", base, "origin", None,
                                  run=base) is None


class _Dst(_Table):
    CANON_ENGINE = "postgres"


def _pair(where, seen):
    ep = Endpoint(host="x", port=1, user="x", password="x")
    hop = Hop(name="ca", engine="postgres", source=ep, target=ep,
              databases=["d"])
    hop.report_dir = lambda db=None: where
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop, pair.dst_engine = hop, _Dst(seen)
    return pair


def test_the_state_set_down_for_the_committed_batch_stands_with_it(
        tmp_path):
    base = 1 << 40
    path = tmp_path / tailctl.TOKEN
    tailctl.keep(path, {"id": "s1", "token": "0/1", "gen": 3,
                        "batch": base, "run": base, "rung": "table"})
    st = tailtables.Store.of(tmp_path)
    st.reset("s1", gen=3)
    # the batch set down, then the target committed it, then the process
    # stopped before the position was written
    with st.batch(4) as s:
        s.put("tables", "a", {"state": "parked", "name": "a"})
        s.put("meta", "pending", {"batch": base + 1, "token": "0/2"})
    pair = _pair(tmp_path, {"token": "0/2", "batch": base + 1, "run": base})
    said = []
    token, batch = pair._committed_ahead("d", "0/1", base, path, said.append)
    assert (token, batch) == ("0/2", base + 1)
    env = tailctl.position(path)
    # the position stands where the store does: in step, nothing undone
    assert env["gen"] == 4 and env["token"] == "0/2"
    assert st.align(env, said.append) == "in step"
    assert st.tables()["a"]["state"] == "parked"
    assert any("had committed batch" in s for s in said), said


def test_a_jump_past_the_state_set_down_leaves_it_behind_and_said(tmp_path):
    base = 1 << 40
    path = tmp_path / tailctl.TOKEN
    tailctl.keep(path, {"id": "s1", "token": "0/1", "gen": 3,
                        "batch": base, "run": base, "rung": "table"})
    st = tailtables.Store.of(tmp_path)
    st.reset("s1", gen=3)
    with st.batch() as s:
        s.put("tables", "a", {"state": "parked", "name": "a"})
    # the position is three batches behind what the target committed - a
    # copy kept elsewhere a while ago
    pair = _pair(tmp_path, {"token": "0/7", "batch": base + 3, "run": base})
    said = []
    pair._committed_ahead("d", "0/1", base, path, said.append)
    env = tailctl.position(path)
    assert env["gen"] == 6 and env["batch"] == base + 3
    assert any("3 batches past it" in s for s in said), said
    # a table was parked: what the batches between set aside is not known
    with pytest.raises(SystemExit) as e:
        st.align(env, said.append, safe_behind=lambda: False)
    assert "not saved together" in str(e.value)


def test_each_batch_names_its_run_in_the_mark(tmp_path):
    path = tmp_path / tailctl.TOKEN
    tailctl.keep(path, {"id": "s1", "token": "0/1", "gen": 0})
    base = HeteroEngine._saved_batch(path, "0/1")
    env = json.loads(path.read_text())
    assert env["batch"] == base == env["run"]
    # kept: asked again, the same
    assert HeteroEngine._saved_batch(path, "0/1") == base
