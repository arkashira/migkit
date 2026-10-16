"""What CRIT6 left of a resumed MongoDB range and its drill: a range begun
before a stop is emptied only within the hop's row filter; a range the
mover taken over from began after the record the taker read of it is
emptied before it is copied again, so a document the source lost
meanwhile does not stay on the target; the verifier's drill asks as many
ranges as the copy cuts (`ranges.wanted`), with no ceiling of its own.

PostgreSQL's ranges (`delete from ... where <the range>` before each copy)
and the cross-engine ranges (`neutral_empty` of the range's scope) always
empty their span before copying it, so neither reads `ranges_seen` and
neither can leave a departed mover's rows: only MongoDB's copy keys its
delete on the record of what began. Fakes only."""
import socket

import pytest

from migkit import lease, ranges
from migkit.cli import _Checkpoint
from migkit.engines import mongodb
from tests.mongo_fake import FakeCollection, engine, no_real_server  # noqa: F401 - autouse

DOCS = [{"_id": i, "keep": 1} for i in range(2, 802, 2)]


@pytest.fixture
def four(monkeypatch):
    """Ranges of 100 documents cut in key order, one worker, so the order
    is the test's."""
    monkeypatch.setattr(ranges, "LEAST", 1)
    monkeypatch.setattr(ranges, "active", ranges.Slots(1))


def _move(eng, tmp_path, lines=None):
    ck = _Checkpoint(tmp_path / "move.json")
    eng.move_table("shop", "", "orders", 100, ck,
                   (lines if lines is not None else []).append)
    return ck


def _entry(ck):
    return next(v for v in ck.values() if isinstance(v, dict)
                and "ranges" in v)


def _stop_in_the_second_range(eng, tmp_path, monkeypatch, write_first):
    """The copy stopped in its second range, having written its
    documents there first where `write_first`."""
    real = FakeCollection.insert_many
    fault = {"on": True}

    def insert_many(self, docs, ordered=True):
        hit = fault["on"] and 202 < docs[0]["_id"] <= 402
        if hit and write_first:
            real(self, docs, ordered=ordered)
        if hit:
            raise RuntimeError("the target went away")
        return real(self, docs, ordered=ordered)
    monkeypatch.setattr(FakeCollection, "insert_many", insert_many)
    with pytest.raises(RuntimeError):
        _move(eng, tmp_path)
    fault["on"] = False


def _ids(dst):
    return [d["_id"] for d in dst.db("shop").colls["orders"].docs]


def test_a_filtered_range_resumed_deletes_only_what_the_filter_takes(
        tmp_path, monkeypatch, four):
    eng, src, dst = engine({"mapping": {"where": {"shop.orders":
                                                  "keep = 1"}}}, tmp_path)
    src.db("shop").add("orders", DOCS)
    _stop_in_the_second_range(eng, tmp_path, monkeypatch, write_first=True)
    st = _entry(_Checkpoint(tmp_path / "move.json"))
    # read back from the journal, the begin is named by its text, and
    # still read as the range's
    assert str(st["ranges"][1][0]) in st["ranges_seen"]
    assert ranges.ident(st["ranges"][1][0]) in ranges.begun_ranges(st)
    assert 250 in _ids(dst)
    # meanwhile the source lost one the stopped range wrote, and another
    # writer put a document the filter does not take inside its span
    src.db("shop").colls["orders"].docs = [
        d for d in src.db("shop").colls["orders"].docs if d["_id"] != 250]
    dst.db("shop").colls["orders"].docs.append({"_id": 251, "keep": 0})
    _move(eng, tmp_path)
    got = _ids(dst)
    want = sorted([d["_id"] for d in DOCS if d["_id"] != 250] + [251])
    assert sorted(got) == want and len(got) == len(set(got))


def _taken_over(tmp_path, monkeypatch, host):
    """The checkpoint as a taker reads it from the machines' store: the
    departed mover (`host`) planned the ranges and began the second after
    its last push, so the record holds no begin of it; it wrote three
    documents of that range on the target."""
    eng, src, dst = engine(tmp_path=tmp_path)
    src.db("shop").add("orders", DOCS)
    _stop_in_the_second_range(eng, tmp_path, monkeypatch, write_first=False)
    ck = _Checkpoint(tmp_path / "move.json")
    st = _entry(ck)
    second = st["ranges"][1][0]
    st["ranges_seen"] = {k: v for k, v in st["ranges_seen"].items()
                         if k not in (str(second), ranges.ident(second))}
    st["copying"] = {"host": host, "pid": 4242}
    ck.save()
    dst.db("shop").colls["orders"].docs += [
        {"_id": i, "keep": 1} for i in (204, 206, 208)]
    src.db("shop").colls["orders"].docs = [
        d for d in src.db("shop").colls["orders"].docs if d["_id"] != 206]
    return eng, src, dst


def test_a_range_the_departed_mover_began_is_emptied_before_it_is_copied(
        tmp_path, monkeypatch, four):
    eng, src, dst = _taken_over(tmp_path, monkeypatch, "elsewhere")
    lines = []
    _move(eng, tmp_path, lines)
    got = _ids(dst)
    assert sorted(got) == sorted(d["_id"] for d in
                                 src.db("shop").colls["orders"].docs)
    assert 206 not in got and len(got) == len(set(got))
    assert any("taken over from elsewhere" in line for line in lines)


def test_a_lease_taken_over_on_this_machine_counts_as_a_takeover(
        tmp_path, monkeypatch, four):
    eng, src, dst = _taken_over(tmp_path, monkeypatch, socket.gethostname())
    monkeypatch.setattr(lease, "taken_from",
                        lambda: {f"{socket.gethostname()}:4242"})
    _move(eng, tmp_path)
    got = _ids(dst)
    assert 206 not in got and len(got) == len(set(got))


def test_what_counts_as_begun():
    here = socket.gethostname()
    st = {"ranges": [[None, 10], [10, 20], [20, None]],
          "ranges_done": [None], "ranges_seen": {"null": None, "10": 7}}
    seen = {"null", "10"}
    # a begin the journal gave back by its text is the range's ident
    assert ranges.begun_ranges({**st, "ranges_seen": {"None": None}}) == \
        {"None", "null"}
    # this machine's own record of what began is exact
    assert ranges.begun_ranges(st) == seen
    assert ranges.begun_ranges(st, {"host": here, "pid": 1}) == seen
    # a mover taken over from, here or elsewhere: every range not done
    assert ranges.begun_ranges(st, {"host": here, "pid": 1},
                               {f"{here}:1"}) == {"null", "10", "20"}
    assert ranges.begun_ranges(st, {"host": "elsewhere", "pid": 1}) == \
        {"null", "10", "20"}


def test_the_movers_a_lease_was_taken_from(monkeypatch):
    held = type("L", (), {"took_over": {"host": "elsewhere", "pid": 9}})()
    free = type("L", (), {"took_over": None})()
    monkeypatch.setattr(lease, "_HOLDING", {held, free})
    assert lease.taken_from() == {"elsewhere:9"}


def test_the_drill_asks_as_many_ranges_as_the_copy_cuts(monkeypatch):
    class Coll:
        name = "c"

        def __init__(self, n):
            self.n = n

        def aggregate(self, pipeline, **kw):
            return [{"_id": "int"}]

        def count_documents(self, flt):
            return self.n

    eng = object.__new__(mongodb.MongoEngine)
    asked = []
    eng._id_edges = lambda coll, flt, n, per, ty: asked.append(per) or []
    for workers, docs in ((1, 10_000_000), (8, 10_000_000),
                          (40, 4_000_000), (8, 1_000_000_000)):
        monkeypatch.setattr(ranges, "active", ranges.Slots(workers))
        asked.clear()
        eng._id_plan(Coll(docs), docs)
        want = ranges.wanted(docs, mongodb.DRILL_RANGE_DOCS, workers)
        assert asked == [want]
    # coarse where the documents are few, finer with more workers, and no
    # ceiling of its own past what the documents need
    assert ranges.wanted(40_000, mongodb.DRILL_RANGE_DOCS, 1) == 1
    assert ranges.wanted(10_000_000, mongodb.DRILL_RANGE_DOCS, 8) == 50
    assert ranges.wanted(4_000_000, mongodb.DRILL_RANGE_DOCS, 40) == 80
    assert ranges.wanted(1_000_000_000, mongodb.DRILL_RANGE_DOCS, 8) == 5000
    assert eng._id_plan(Coll(40_000), 40_000) == [(None, None, None)]
