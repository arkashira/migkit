"""A copy's ranges are a journal, not the whole checkpoint again (audit
2026-10-01, ST17).

A table copied in 20,000 ranges saved its whole checkpoint twice a range:
O(R^2) bookkeeping, about 14 minutes of saves one after the other. Now a
range begun or landed is a line of the checkpoint's journal
(`cli._Checkpoint.journal`, written under the lock the range was marked
under), and every reader - the copy going on, the tail leaving out what
the copy read, the dashboard, the evidence - reads the file and the
journal after it the one way (`checkpoint.read_move`). A line left over
from before the file was written whole again is not added to it.
"""
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from migkit import checkpoint, durable, ranges
from migkit.cli import _Checkpoint


def _plan(ck, key="db..t"):
    st = ck.setdefault(key, {"table": "public.t", "key": "id"})
    ranges.plan(st, 1, 400, lambda: [100, 200, 300], ck.save,
                table="public.t")
    return st


def test_a_range_begun_or_landed_is_a_line_and_reads_back(tmp_path):
    path = tmp_path / "move.json"
    ck = _Checkpoint(path)
    st = _plan(ck)
    whole = path.read_bytes()
    for after in (0, 100, 200):
        ranges.started(st, after, f"mark{after}", ck.save)
        ranges.finished(st, after, ck.save)
    # the file is as the plan left it; the six marks are lines after it
    assert path.read_bytes() == whole
    lines = durable.read_lines(checkpoint.journal_of(path), "journal")
    assert len(lines) == 6
    got = checkpoint.read_move(path)["db..t"]
    assert got["ranges_done"] == [0, 100, 200]
    assert got["ranges_seen"] == {"0": "mark0", "100": "mark100",
                                  "200": "mark200"}
    assert got["last"] == 300 and checkpoint.JOURNAL not in \
        checkpoint.read_move(path)
    # a copy going on reads the same
    again = _Checkpoint(path)
    assert again["db..t"]["ranges_done"] == [0, 100, 200]
    assert ranges.plan(again["db..t"], 1, 400, lambda: [], again.save) == \
        [(300, 400)]


def test_a_line_from_before_the_file_was_written_whole_is_not_added(
        tmp_path):
    path = tmp_path / "move.json"
    ck = _Checkpoint(path)
    st = _plan(ck)
    ranges.started(st, 0, "first", ck.save)
    old = checkpoint.journal_of(path).read_text()
    # the range begun again, and the file written whole since
    st["ranges_seen"]["0"] = "second"
    ck.save()
    assert not checkpoint.journal_of(path).exists()
    # a stop between the file and its old journal going: the old line
    # stays, under the old file's journal
    checkpoint.journal_of(path).write_text(old)
    assert checkpoint.read_move(path)["db..t"]["ranges_seen"] == \
        {"0": "second"}


_COPY = textwrap.dedent("""
    import sys
    from pathlib import Path
    from migkit import ranges
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(Path(sys.argv[1]))
    st = ck.setdefault("db..t", {"table": "public.t", "key": "id"})
    todo = ranges.plan(st, 1, 1000, lambda: list(range(50, 1000, 50)),
                       ck.save, table="public.t")
    for after, upto in todo:
        ranges.started(st, after, f"m{after}", ck.save)
        ranges.finished(st, after, ck.save)
""")


def test_a_copy_stopped_dead_goes_on_from_every_range_it_journaled(
        tmp_path):
    path = tmp_path / "move.json"
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    env["MIGKIT_FAILPOINT"] = "checkpoint.journaled:15:exit"
    got = subprocess.run([sys.executable, "-c", _COPY, str(path)], env=env,
                         capture_output=True, text=True, timeout=120)
    assert got.returncode == 137, got.stderr
    st = checkpoint.read_move(path)["db..t"]
    # 15 lines: eight ranges begun, seven landed - none lost, none made up
    assert len(st["ranges_seen"]) == 8 and len(st["ranges_done"]) == 7
    assert sorted(st["ranges_done"]) == [r[0] for r in st["ranges"]][:7]
    env.pop("MIGKIT_FAILPOINT")
    got = subprocess.run([sys.executable, "-c", _COPY, str(path)], env=env,
                         capture_output=True, text=True, timeout=120)
    assert got.returncode == 0, got.stderr
    st = checkpoint.read_move(path)["db..t"]
    assert sorted(st["ranges_done"]) == sorted(r[0] for r in st["ranges"])


def test_every_reader_reads_the_journal(tmp_path, monkeypatch):
    """The tail leaves out a change in a range only the journal says the
    copy read; the evidence and the dashboard count what it holds."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import _CopiedRanges
    path = tmp_path / "move.json"
    ck = _Checkpoint(path)
    st = _plan(ck)
    ranges.started(st, 100, "m", ck.save)
    ranges.finished(st, 100, ck.save)

    class Src:
        def mark_covers(self, side, db, mark, change):
            return mark == "m" and change["txn"] < 5

        def snapshot_mark(self, side, db):
            return None

    class Eng:
        def __init__(self):
            self.hop = Hop(name="j", engine="postgres", source=Endpoint(),
                           target=Endpoint())
            self.src_engine = Src()

        @staticmethod
        def _leaf(t):
            return str(t).split(".")[-1]
    copied = _CopiedRanges(Eng(), "db", tmp_path)
    change = {"op": "update", "table": "public.t", "key": {"id": 150},
              "values": {"id": 150}, "txn": 3}
    later = dict(change, txn=9)
    out, left = copied.leave_out([change, later])
    assert left == {"public.t": 1} and [c["key"]["id"] for c in out] == \
        [150]
    with pytest.raises(SystemExit):
        # a torn file is a stop for the copy and the tail
        path.write_text('{"db..t": {"ran')
        checkpoint.read_move(path)
    # the dashboard reads it as a cache
    assert checkpoint.read_move(path, "cache") == {}
    json.loads(json.dumps(checkpoint.read_move(path, "cache")))
