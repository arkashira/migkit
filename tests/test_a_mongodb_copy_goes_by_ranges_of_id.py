"""A MongoDB collection is copied as ranges of `_id` cut where every other
engine's ranges are cut (`ranges.cut_with`), side by side under the move's
workers, and resumed range by range from the checkpoint: a stop in the
second range copies the first again never."""
import pytest

from migkit import ranges
from migkit.cli import _Checkpoint
from migkit.engines.mongodb import _loads_key
from tests.mongo_fake import FakeCollection, engine, no_real_server  # noqa: F401 - autouse

DOCS = [{"_id": i, "n": i} for i in range(1, 401)]


@pytest.fixture
def four(monkeypatch):
    """Ranges of 100 documents, one worker, so the order is the test's."""
    monkeypatch.setattr(ranges, "LEAST", 1)
    monkeypatch.setattr(ranges, "active", ranges.Slots(1))


def _answers(monkeypatch, buckets=None, sample=None):
    def answer(self, pipeline):
        stages = [next(iter(s)) for s in pipeline]
        if "$bucketAuto" in stages and buckets is not None:
            return [dict(b) for b in buckets]
        if "$sample" in stages and sample is not None:
            return [{"_id": v} for v in sample]
        return []
    monkeypatch.setattr(FakeCollection, "answer", answer)


def _spy(monkeypatch):
    seen = []
    real = ranges.cut_with

    def spy(*a, **kw):
        got = real(*a, **kw)
        seen.append(got)
        return got
    monkeypatch.setattr(ranges, "cut_with", spy)
    return seen


def _move(eng, tmp_path, lines=None):
    ck = _Checkpoint(tmp_path / "move.json")
    eng.move_table("shop", "", "orders", 100, ck,
                   (lines if lines is not None else []).append)
    return ck


def _entry(ck):
    return next(v for k, v in ck.items() if isinstance(v, dict)
                and "ranges" in v)


def test_the_edges_are_the_servers_buckets_through_the_one_owner(
        tmp_path, monkeypatch, four):
    eng, src, dst = engine(tmp_path=tmp_path)
    src.db("shop").add("orders", DOCS)
    _answers(monkeypatch, buckets=[
        {"_id": {"min": lo, "max": lo + 99}, "count": 100}
        for lo in (1, 101, 201, 301)])
    seen = _spy(monkeypatch)
    st = _entry(_move(eng, tmp_path))
    assert len(seen) == 1 and st["edges_from"] == "statistics"
    # the edges the owner chose are the ones the plan holds
    assert [r[1] for r in st["ranges"][:-1]] == seen[0]
    assert [_loads_key(r[1]) for r in st["ranges"][:-1]] == [100, 200, 300]
    assert len(st["ranges"]) == 4 and len(st["ranges_done"]) == 4
    assert sorted(d["_id"] for d in dst.db("shop").colls["orders"].docs) \
        == list(range(1, 401))


def test_a_sample_cuts_the_ranges_where_the_buckets_are_too_coarse(
        tmp_path, monkeypatch, four):
    eng, src, dst = engine(tmp_path=tmp_path)
    src.db("shop").add("orders", DOCS)
    _answers(monkeypatch, buckets=[
        {"_id": {"min": 1, "max": 400}, "count": 400}],
        sample=list(range(1, 401, 7)))
    st = _entry(_move(eng, tmp_path))
    assert st["edges_from"] == "sample"
    assert len(st["ranges"]) == 4
    assert sorted(d["_id"] for d in dst.db("shop").colls["orders"].docs) \
        == list(range(1, 401))


def test_a_stop_in_the_second_range_resumes_without_the_first(
        tmp_path, monkeypatch, four):
    eng, src, dst = engine(tmp_path=tmp_path)
    src.db("shop").add("orders", DOCS)
    _answers(monkeypatch)
    real = FakeCollection.insert_many
    written, fault = [], {"on": True}

    def insert_many(self, docs, ordered=True):
        ids = [d["_id"] for d in docs]
        if fault["on"] and ids[0] > 101:
            raise RuntimeError("the target went away")
        written.append(ids)
        return real(self, docs, ordered=ordered)
    monkeypatch.setattr(FakeCollection, "insert_many", insert_many)
    with pytest.raises(RuntimeError):
        _move(eng, tmp_path)
    first = [i for ids in written for i in ids]
    assert first == list(range(1, 102))
    st = _entry(_Checkpoint(tmp_path / "move.json"))
    assert st["edges_from"] == "key order" and len(st["ranges"]) == 4
    assert st["ranges_done"] == [None]
    assert not st.get("done")
    fault["on"], written[:] = False, []
    lines = []
    ck = _move(eng, tmp_path, lines)
    again = [i for ids in written for i in ids]
    # the first range is not read or written again
    assert again == list(range(102, 401))
    got = [d["_id"] for d in dst.db("shop").colls["orders"].docs]
    assert sorted(got) == list(range(1, 401)) and len(got) == len(set(got))
    st = _entry(ck)
    assert st["done"] and len(st["ranges_done"]) == 4
    assert any("3 of 4 ranges" in line for line in lines)
    # a third run over a finished copy moves nothing
    written[:] = []
    _move(eng, tmp_path)
    assert written == []
