"""The tail's reads go the way measured faster on this source: in a thread
beside the writer, or in a process of its own (`hetero._Reader`).

A thread shares the interpreter's lock with the writer, so a reader that
decodes in Python and a writer that builds its statements in Python take
turns; a process does not, and costs a process and the records' crossing.
Which is faster depends on the machine and the source, so each is timed
while the tail is behind and the faster kept - and a read is never lost or
repeated on the way from one to the other.
"""
import json
import time

import pytest

from migkit.engines import hetero


class _Way:
    """A reader that reads from the position asked, taking `delay` a read."""

    def __init__(self, name, delay, asked):
        self.name, self.delay, self.asked = name, delay, asked
        self.closed = False

    def next(self, token, limit):
        self.asked.append((self.name, token))
        time.sleep(self.delay)
        return list(range(token, token + limit)), token + limit

    def close(self):
        self.closed = True


class _Src:
    """A source engine as the reader sees it: a hop to hand over."""

    def __init__(self, hop="app"):
        self.hop = hop


def _reader(tmp_path, monkeypatch, delays, cpus=4, said=None, src=None):
    asked, made = [], {}
    monkeypatch.setattr(hetero._Reader, "_cpus", staticmethod(lambda: cpus))

    def rung(self, name):
        if name not in self.readers:
            self.readers[name] = made[name] = _Way(name, delays[name], asked)
        return self.readers[name]
    monkeypatch.setattr(hetero._Reader, "_rung", rung)
    got = hetero._Reader(src or _Src(), "app", tmp_path,
                         (said if said is not None else []).append, most=10)
    return got, asked, made


def _follow(reader, times, apply=0.0):
    """The tail's loop: read, apply, go on from where the read ended."""
    token, seen = 0, []
    for _ in range(times):
        changes, token = reader.next(token, 10)
        seen += changes
        time.sleep(apply)
    return seen, token


def test_the_faster_way_is_kept_and_nothing_is_read_twice_or_missed(
        tmp_path, monkeypatch):
    said = []
    reader, asked, made = _reader(tmp_path, monkeypatch,
                                  {"python-thread": 0.03,
                                   "python-process": 0.003}, said=said)
    seen, token = _follow(reader, 12)
    assert seen == list(range(token)) and token == 120
    assert reader.chosen == "python-process", reader.costs
    assert reader.costs["python-process"] < reader.costs["python-thread"]
    # timed in turn, then on the one kept: the thread's reader let go
    assert made["python-thread"].closed
    assert [n for n, _ in asked][-3:] == ["python-process"] * 3
    kept = json.loads((tmp_path / hetero._Reader.FILE).read_text())
    assert kept["read changes"] == "python-process"
    assert set(kept["seconds_per_change"]) == {"python-thread",
                                               "python-process"}
    assert any(line.startswith("changes are read in a process of its own:")
               and "as fast on this source as in a thread" in line
               for line in said), said


def test_the_slower_way_is_left_even_when_it_is_the_process(tmp_path,
                                                            monkeypatch):
    reader, _, made = _reader(tmp_path, monkeypatch,
                              {"python-thread": 0.003,
                               "python-process": 0.03})
    seen, token = _follow(reader, 12)
    assert seen == list(range(token))
    assert reader.chosen == "python-thread", reader.costs
    assert made["python-process"].closed


def test_a_tie_goes_to_the_way_that_starts_nothing(tmp_path, monkeypatch):
    reader, _, _ = _reader(tmp_path, monkeypatch, {"python-thread": 0,
                                                   "python-process": 0})
    reader.costs = {"python-thread": 1.04e-5, "python-process": 1.01e-5}
    assert [n for n, _ in reader.ranked()] == ["python-thread",
                                               "python-process"]
    reader.costs = {"python-thread": 2.1e-5, "python-process": 1.0e-5}
    assert [n for n, _ in reader.ranked()] == ["python-process",
                                               "python-thread"]
    reader.costs = {"python-thread": 2.1e-5}
    assert reader.ranked() == [("python-process", None),
                               ("python-thread", 2.1e-5)]


def test_small_batches_and_reads_asked_again_are_not_timed(tmp_path,
                                                           monkeypatch):
    reader, _, _ = _reader(tmp_path, monkeypatch, {"python-thread": 0,
                                                   "python-process": 0})
    token = 0
    for _ in range(6):
        # caught up: batches short of the largest size time nothing
        _, token = reader.next(token, 5)
    assert reader.timing == {}
    for _ in range(6):
        # a lost connection: the same position asked again each time
        reader.next(token, 10)
    assert reader.timing == {}


def test_one_processor_never_starts_a_process(tmp_path, monkeypatch):
    reader, asked, _ = _reader(tmp_path, monkeypatch,
                               {"python-thread": 0, "python-process": 0},
                               cpus=1)
    assert reader.names == ["python-thread"]
    _follow(reader, 8)
    assert {n for n, _ in asked} == {"python-thread"}


def test_a_hop_that_cannot_be_handed_to_a_process_is_read_in_a_thread(
        tmp_path, monkeypatch):
    reader, asked, _ = _reader(tmp_path, monkeypatch,
                               {"python-thread": 0, "python-process": 0},
                               src=_Src(hop=lambda: "made in this process"))
    assert reader.names == ["python-thread"]
    _follow(reader, 8)
    assert {n for n, _ in asked} == {"python-thread"}


def test_a_process_that_stops_is_left_and_the_read_asked_again(
        tmp_path, monkeypatch):
    from migkit.ranges import WorkerGone
    said = []
    reader, asked, made = _reader(tmp_path, monkeypatch,
                                  {"python-thread": 0,
                                   "python-process": 0}, said=said)
    reader._switch("python-process")
    reader.next(0, 10)

    def gone(token, limit):
        asked.append(("python-process", token))
        raise WorkerGone("the change reader's process stopped")
    made["python-process"].next = gone
    changes, token = reader.next(10, 10)
    assert changes == list(range(10, 20)) and token == 20
    assert asked[-2:] == [("python-process", 10), ("python-thread", 10)]
    assert reader.names == ["python-thread"] and reader.on == "python-thread"
    assert any("reading in a thread from the same position" in s
               for s in said), said


def test_a_tail_started_again_here_starts_on_the_way_kept(tmp_path,
                                                          monkeypatch):
    reader, _, _ = _reader(tmp_path, monkeypatch,
                           {"python-thread": 0.03, "python-process": 0.003})
    _follow(reader, 12)
    again, asked, _ = _reader(tmp_path, monkeypatch,
                              {"python-thread": 0, "python-process": 0})
    assert again.on == again.chosen == "python-process"
    _follow(again, 3)
    assert {n for n, _ in asked} == {"python-process"}
    # another machine, or this one with other processors: timed again
    other, _, _ = _reader(tmp_path, monkeypatch,
                          {"python-thread": 0, "python-process": 0}, cpus=3)
    assert other.chosen is None and other.on == "python-thread"


def _there_and_back(value):
    """`value` sent to a reader's process and handed back, pickled both
    ways as a batch of changes is."""
    from migkit.ranges import _Worker
    worker = _Worker()
    try:
        (back,) = worker.run(tuple, (value,))
    finally:
        worker.close()
    return back


@pytest.mark.parametrize("value,cls", [
    ("Decimal('1.00')", "decimal"), ("Decimal('1E-10')", "decimal"),
    ("Decimal('-0.000')", "decimal"), ("b'\\x00\\xffA'", "bytes"),
    ("-0.0", "float"), ("float('inf')", "float"), ("0.1", "float"),
    ("10 ** 30", "integer"), ("True", "boolean"), ("None", "text"),
    ("'caf\\u00e9 \\u2028 \\n'", "text"),
    ("datetime.datetime(2026, 1, 2, 3, 4, 5, 6)", "timestamp"),
    ("datetime.datetime(2026, 1, 2, 3, 4, 5, 6,"
     " tzinfo=datetime.timezone(datetime.timedelta(hours=7)))", "timestamp"),
    ("datetime.date(2026, 1, 2)", "date"),
    ("datetime.time(23, 59, 59, 999999)", "time"),
    ("datetime.timedelta(hours=-838, seconds=1)", "own text"),
    ("{'k': [1, Decimal('2.50'), None, {'n': 'x'}], 'a': 'y'}", "json")])
def test_every_kind_of_value_crosses_the_process_as_it_was(value, cls):
    """What the reader's process hands back is pickled: each kind of value
    a change log gives, sent there and back, is the same value of the same
    type with the same text."""
    import datetime  # noqa: F401 - read by eval
    from decimal import Decimal  # noqa: F401 - read by eval

    from migkit import canon
    sent = eval(value)
    back = _there_and_back(sent)
    assert type(back) is type(sent) and repr(back) == repr(sent)
    assert back == sent
    assert canon.render_value(cls, back) == canon.render_value(cls, sent)


def test_what_is_not_there_and_what_a_counter_adds_cross_as_themselves():
    from migkit import canon
    assert _there_and_back(canon.ABSENT) is canon.ABSENT
    added = _there_and_back(canon.Added(3, 10))
    assert isinstance(added, canon.Added) and added == canon.Added(3, 10)
    # a SET column's members, whose order no two processes need agree on
    assert _there_and_back({"a", "b", "c"}) == {"a", "b", "c"}
