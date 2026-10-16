"""What CRIT6 asked of a resumed range, the cut of MongoDB's `_id` and the
cutover's seam: the `_id` buckets are as many as the ranges need, asked
again finer where the server answered coarse; a seam that cannot be read
back is written in the cutover's record with its cause; a range loaded
server to server commits before the copy hears it landed. Fakes only."""
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from migkit import cutover, drainmark
from tests.mongo_fake import engine, no_real_server  # noqa: F401 - autouse


class _Buckets:
    """A collection whose `$bucketAuto` answers at most `most` buckets of
    `n` documents keyed 0..n-1, recording each number of buckets asked."""

    def __init__(self, n, most=None):
        self.n, self.most, self.name, self.asked = n, most, "c", []

    def aggregate(self, pipeline, **kw):
        want = pipeline[-1]["$bucketAuto"]["buckets"]
        self.asked.append(want)
        got = min(want, self.most or want, self.n)
        size = -(-self.n // got)
        lo = 0
        while lo < self.n:
            hi = min(lo + size, self.n) - 1
            yield {"_id": {"min": lo, "max": hi}, "count": hi - lo + 1}
            lo = hi + 1


def test_the_id_buckets_are_as_many_as_the_ranges_need(tmp_path):
    eng, _, _ = engine(tmp_path=tmp_path)
    coll = _Buckets(20000)
    edges = eng._id_edges(coll, {}, 20000, 1000, 16)
    # no ceiling of its own: a thousand ranges ask a thousand buckets
    assert coll.asked == [1000]
    assert len(edges) == 999 and edges == sorted(edges)


def test_a_coarse_answer_is_asked_again_finer(tmp_path):
    eng, _, _ = engine(tmp_path=tmp_path)
    coll = _Buckets(1000, most=None)
    real = coll.aggregate
    answers = iter([400, None])

    def coarse_once(pipeline, **kw):
        cap = next(answers)
        coll.most = cap
        return real(pipeline, **kw)
    coll.aggregate = coarse_once
    edges = eng._id_edges(coll, {}, 1000, 8, 16)
    assert coll.asked[0] == 8
    assert len(coll.asked) == 1 and len(edges) == 7
    still = _Buckets(1000, most=4)
    assert eng._id_edges(still, {}, 1000, 8, 16) == []
    # asked once as the ranges need, once finer by the shortfall measured
    assert still.asked == [8, 16]


def test_a_seam_that_cannot_be_read_back_is_in_the_cutovers_record(
        tmp_path, monkeypatch):
    hop = SimpleNamespace(name="h", options={},
                          report_dir=lambda db=None: Path(tmp_path))

    def unread(*a, **kw):
        raise OSError("the stream's saved position is torn")
    monkeypatch.setattr(drainmark, "seam", unread)
    said = []
    leg = SimpleNamespace(fence=SimpleNamespace(CANON_ENGINE="postgres"))
    got = cutover._seam(hop, leg, "d", "0/20", 0.2, said.append,
                        rec={"id": "run-1"})
    assert got is None and "could not be read back" in said[0]
    line = json.loads((tmp_path / "changelog.jsonl").read_text()
                      .splitlines()[-1])
    assert line["op"] == "cutover" and line["run"] == "run-1"
    assert line["event"] == "seam unread" and line["position"] == "0/20"
    assert "torn" in line["why"]


def test_a_range_passed_server_to_server_commits_before_it_returns():
    from migkit.engines import pg_pass
    order = []

    class Conn:
        @contextmanager
        def transaction(self):
            order.append("begin")
            yield
            order.append("commit")

        def execute(self, sql):
            order.append(sql.split()[0])

        def cursor(self):
            conn = self

            class Cur:
                rowcount = 3

                def __enter__(self):
                    return self

                def __exit__(self, *e):
                    return False

                @contextmanager
                def copy(self, sql):
                    order.append("copy")
                    yield SimpleNamespace(write=lambda b: None)
            return Cur()

        def cancel_safe(self):
            pass
    proc = SimpleNamespace(kill=lambda: None, poll=lambda: 0,
                           wait=lambda: 0)
    stream = SimpleNamespace(read=lambda n: b"")
    tc = object.__new__(pg_pass.TableCopy)
    tc.eng = SimpleNamespace(
        _copy_select=lambda qt, cols, where: "select 1",
        _source_copy=lambda db, cmds: (proc, stream, None))
    tc.db, tc.qt, tc.cols, tc.collist = "d", '"t"', ["id"], '"id"'
    tc._target = lambda: Conn()
    tc._ended = lambda p, r: ""
    rows, lsn = tc.copy(SimpleNamespace(id="snap"), "id > 0",
                        'delete from "t" where id > 0', "t 1 to 3")
    order.append("finished")
    assert rows == 3 and lsn is None
    assert order == ["begin", "delete", "copy", "commit", "finished"]
