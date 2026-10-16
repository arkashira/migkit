"""A keyed read's page is sized by the one decision (`decide.page_rows`,
DEC.page) from what the read measured - its rate, its rows' width and the
free memory - and asked again before every page; nothing measured is the
lowest rung, said as such. No servers: the readers are fakes."""
from migkit import decide
from migkit.engines.base import PAGED, Engine

GIB = 2 ** 30
ROW = (1, "x" * 100)


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def _pages(free=8 * GIB):
    return decide.Pages(memory=lambda: free, clock=Clock())


def test_nothing_measured_is_the_lowest_rung_and_says_why():
    rows, choice = decide.page_rows({})
    assert rows == decide.PAGE_UNMEASURED
    assert choice.path == "unmeasured"
    assert "nothing measured yet" in choice.rung.because
    assert "no page of this read has been timed" in choice.reason


def test_the_page_grows_on_a_fast_reader_with_memory_to_spare():
    p = _pages()
    sizes = [p.size()]
    for _ in range(4):
        p.measured([ROW] * sizes[-1], 0.001)   # a million rows a second
        sizes.append(p.size())
    assert sizes == [1000, 2000, 4000, 8000, 16000]
    assert p.choice.path == "measured"


def test_the_page_shrinks_at_once_on_a_slow_reader():
    p = _pages()
    p.size()
    p.measured([ROW] * 1000, 0.001)
    assert p.size() == 2000
    p.measured([ROW] * 2000, 40.0)             # 50 rows a second
    assert p.size() == int(50 * decide.PAGE_SECONDS)


def test_the_free_memory_holds_a_wide_page_back():
    p = _pages(free=64 * 2 ** 20)
    p.size()
    wide = (1, b"x" * (2 ** 20))
    p.measured([wide] * 10, 0.001)             # fast, but a MiB a row
    held = 64 * 2 ** 20 // decide.PAGE_MEMORY_SHARE
    assert p.size() == held // (2 ** 20 + 9)
    rows, choice = decide.page_rows({"width": 2 ** 20 + 9,
                                     "free": 64 * 2 ** 20})
    assert choice.path == "by width" and rows == held // (2 ** 20 + 9)


def test_more_readers_at_once_share_the_memory():
    one, _ = decide.page_rows({"rate": 1e9, "width": 1000, "free": GIB})
    four, _ = decide.page_rows({"rate": 1e9, "width": 1000, "free": GIB,
                                "readers": 4})
    assert four == one // 4


class Fake(Engine):
    def __init__(self, total=20000):
        super().__init__(None)
        self.total, self.asked = total, []

    def neutral_read(self, side, db, table, columns, after=None, limit=None,
                     where=None):
        self.asked.append(limit)
        at = after or 0
        end = min(at + limit, self.total)
        return [ROW] * (end - at), (end if end < self.total else None)

    def neutral_changes(self, side, db, token=None, limit=None):
        self.asked.append(limit)
        return [{"op": "insert", "row": ROW}] * limit, token


def test_the_decision_is_asked_again_between_pages():
    eng, seen = Fake(), []

    def ask(facts):
        seen.append(dict(facts))
        return decide.page_rows(facts)
    eng.__dict__["_page_reads"] = {
        ("neutral_read", "src", "d", "t"): decide.Pages(ask=ask)}
    after, pages = None, 0
    while True:
        _, after = eng.neutral_read("src", "d", "t", [("id", "int")], after)
        pages += 1
        if after is None:
            break
    assert len(seen) == pages > 1
    assert seen[0]["rate"] is None and seen[1]["rate"] > 0
    assert seen[1]["width"] == decide._measure(ROW)
    assert eng.asked[0] == 1000 and eng.asked[1] > 1000


def test_a_page_the_caller_names_passes_as_it_is_and_is_not_measured():
    eng = Fake()
    eng.neutral_read("src", "d", "t", [], None, 7)
    eng.neutral_read("src", "d", "t", [], None, limit=9)
    assert eng.asked == [7, 9]
    assert not eng.__dict__.get("_page_reads")


def test_a_change_log_page_is_sized_by_the_same_decision():
    eng = Fake()
    eng.neutral_changes("src", "d", None)
    eng.neutral_changes("src", "d", None)
    assert eng.asked[0] == 1000 and eng.asked[1] == 2000
    assert ("neutral_changes", "src", "d") in eng._page_reads


def test_a_reader_below_an_engine_takes_what_it_is_handed():
    assert decide.page_given(None) == decide.PAGE_UNMEASURED
    assert decide.page_given(37) == 37


def test_an_engine_with_no_reader_of_its_own_keeps_the_one_it_inherits():
    class Plain(Engine):
        pass
    for name in PAGED:
        assert getattr(Plain, name) is getattr(Engine, name)
        assert name not in Plain.__dict__
        assert getattr(Fake, name).paged


def test_the_running_count_returns_to_nothing_after_a_failed_page():
    class Fails(Fake):
        def neutral_read(self, *a, **kw):
            raise OSError("gone")
    eng = Fails()
    try:
        eng.neutral_read("src", "d", "t", [])
    except OSError:
        pass
    assert eng.__dict__["_page_running"] == 0


def test_the_cloud_services_cells_reach_the_state():
    from migkit import capabilities as caps, cloud_readiness as cr, cloudapi
    assert "cloudapi.py" in caps.DECLARING
    for who in ("aws", "gcp"):
        why, cells = caps.UNPROVED_ELSEWHERE[who]
        assert why == cr.UNPROVED_ON[who] == cloudapi.UNPROVED_ON[who]
        assert "free.provider-metric" in cells and "managed.readiness" in cells
    assert cloudapi.Aws.UNPROVED_ITEMS == {"free": "free.provider-metric"}
    assert cloudapi.Gcp.UNPROVED_ITEMS == {"free": "free.provider-metric"}
    assert not any(caps.depth_failing().values())
