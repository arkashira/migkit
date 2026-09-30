"""The move's checkpoint (audit ST4, ST17, ST24, SP4).

A resumed `--mode full` copy took a new change position, so the later
`--mode cdc` missed what changed on the ranges the first run had
finished; the checkpoint was written indented - the encoder's slow path -
twice a range under one lock, every range rebuilding the set of those
done; and the retry around an entry changing under the encoder ended in
an unbound name on its last turn.
"""
import json

import pytest

from migkit import cli, ranges


class _Hop:
    name = "h"
    options = {}

    def __init__(self, tmp):
        self.tmp = tmp

    def report_dir(self, db=None):
        return self.tmp


def test_a_resumed_copy_goes_on_from_the_first_run_s_position(
        tmp_path, monkeypatch):
    hop = _Hop(tmp_path)
    ck = cli._Checkpoint(tmp_path / "move.json")
    now = {"at": "0/AAA"}
    monkeypatch.setattr(cli, "_position_before_copy",
                        lambda eng, d: now["at"])
    assert cli._kept_point(hop, None, "app", ck) == "0/AAA"
    # the first run copied part of a table, and stopped
    ck["app.public.t"] = {"ranges": [[0, 10]], "ranges_done": [0]}
    ck.save()
    now["at"] = "0/BBB"
    again = cli._Checkpoint(tmp_path / "move.json")
    assert cli._kept_point(hop, None, "app", again) == "0/AAA"


def test_a_copy_resumed_from_a_run_that_kept_none_says_it_has_none(
        tmp_path, monkeypatch):
    hop = _Hop(tmp_path)
    ck = cli._Checkpoint(tmp_path / "move.json")
    ck["app.public.t"] = {"ranges": [[0, 10]], "ranges_done": [0]}
    monkeypatch.setattr(cli, "_position_before_copy",
                        lambda eng, d: pytest.fail("a new position taken"))
    assert cli._kept_point(hop, None, "app", ck) is None


def test_the_checkpoint_is_written_compact(tmp_path):
    ck = cli._Checkpoint(tmp_path / "move.json")
    ck["app.public.t"] = {"ranges": [[0, 10], [10, 20]],
                          "ranges_done": [0], "last": 10}
    ck.save()
    text = (tmp_path / "move.json").read_text()
    assert "\n" not in text and ", " not in text, text
    assert json.loads(text) == dict(ck)


def test_a_checkpoint_that_keeps_changing_stops_in_words(tmp_path,
                                                         monkeypatch):
    ck = cli._Checkpoint(tmp_path / "move.json")
    ck["a"] = {"x": 1}

    def changing(*a, **k):
        raise RuntimeError("dictionary changed size during iteration")
    monkeypatch.setattr(cli.json, "dumps", changing)
    monkeypatch.setattr(cli.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit, match="kept changing"):
        ck.save()


def test_ranges_finishing_side_by_side_are_written_by_one_save(
        tmp_path, monkeypatch):
    from migkit import failpoint
    writes = []
    monkeypatch.setattr(failpoint, "hit",
                        lambda name: writes.append(name)
                        if name == "checkpoint.written" else None)
    ck = cli._Checkpoint(tmp_path / "move.json")
    st = ck.setdefault("app.public.t", {"ranges": [[0, 10], [10, 20],
                                                   [20, 30]],
                                        "ranges_done": []})
    ranges.started(st, 0, {"lsn": 1}, ck.save)
    assert writes == ["checkpoint.written"]
    # two changes made while nothing was saved: the save for the first
    # writes both, and the second's is not written again
    first, second = ck.changed(), ck.changed()
    ck.save(upto=first)
    ck.save(upto=second)
    assert len(writes) == 2, writes
    ranges.finished(st, 10, ck.save)
    assert st.get("last") is None           # the first range is not done
    ranges.finished(st, 0, ck.save)
    assert st["last"] == 20
    ranges.finished(st, 20, ck.save)
    assert st["last"] == 30
    kept = json.loads((tmp_path / "move.json").read_text())
    assert kept["app.public.t"]["ranges_seen"] == {"0": {"lsn": 1}}
    assert sorted(kept["app.public.t"]["ranges_done"]) == [0, 10, 20]


def test_how_far_every_range_is_done_follows_the_ranges_as_planned():
    """Out of order, and after the plan grows at an end, `last` is where
    every range from the start is done - as the rebuilt set said it."""
    st = {"ranges": [[0, 5], [5, 9], [9, 12]], "ranges_done": []}
    for after, want in ((5, None), (9, None), (0, 12)):
        ranges.finished(st, after, lambda: None)
        assert st.get("last") == want, (after, st)
    st2 = {"ranges": [[0, 5], [5, 9]], "ranges_done": []}
    ranges.finished(st2, 0, lambda: None)
    assert st2["last"] == 5
    # the source gained rows below the plan: a range of their own, first
    st2["ranges"].insert(0, [-3, 0])
    ranges.finished(st2, 5, lambda: None)
    assert st2["last"] == 5                 # that first range is not done
    ranges.finished(st2, -3, lambda: None)
    assert st2["last"] == 9
