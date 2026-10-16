"""The keyed reads that passed a number as their page (READ.literal-pages) ask
the one decision for it (`decide.page_rows`, by way of `engines.base._paged`
or one `decide.Pages` of their own), asked again before every page: the first
page is the unmeasured rung and a reader that reads fast is given a larger
one, where it was a literal 1000, 5000 or 10,000 whatever it measured. No
servers: the engine is a fake that notes the page each read asks for."""
from types import SimpleNamespace

from migkit import decide, leaves, ranges, repairlog, repairways, subset
from migkit.engines.base import Engine

ROW_PAD = "x" * 100
FIRST = decide.PAGE_UNMEASURED


class Fake(Engine):
    RESUMES_BY_KEY = True

    def __init__(self, total=20000):
        super().__init__(None)
        self.total, self.asked, self.batched = total, [], []

    def neutral_read(self, side, db, table, columns, after=None, limit=None,
                     where=None):
        self.asked.append(limit)
        at = after or 0
        end = min(at + limit, self.total)
        return ([(i, ROW_PAD) for i in range(at, end)],
                end if end < self.total else None)

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        self.batched.append(size)
        yield [(0, ROW_PAD)]

    def neutral_key(self, side, db, table):
        return ["id"]

    def neutral_columns(self, side, db, table):
        return [("id", "integer"), ("v", "text")]

    def _quote_ident(self, name):
        return f'"{name}"'


COLS = [("id", "integer"), ("v", "text")]


def _grew(asked, total=20000):
    """The pages a read of `total` rows asked for: the first is the
    unmeasured rung, then each grows by the decision's step, not the one
    number it was."""
    assert asked[0] == FIRST
    assert asked[1] == FIRST * decide.PAGE_GROWTH
    assert asked[2] == FIRST * decide.PAGE_GROWTH ** 2
    assert len(set(asked)) > 2
    assert sum(min(a, total) for a in asked[:-1]) < total


def test_a_subset_scan_and_its_count_ask_the_decision_for_every_page():
    eng = Fake()
    assert len(list(subset._scan(eng, "d", "t", COLS))) == 20000
    _grew(eng.asked)
    eng = Fake()
    assert subset._count(eng, "src", "d", "t", COLS) == 20000
    _grew(eng.asked)
    eng = Fake()
    assert len(list(subset._scan_all(eng, "d", "t", COLS))) == 20000
    _grew(eng.asked)


def test_a_scan_that_stops_early_reads_no_page_past_it():
    eng = Fake()
    assert len(list(subset._scan(eng, "d", "t", COLS, stop=10))) == 10
    assert eng.asked == [FIRST]


def test_the_repair_pre_image_reads_its_span_by_the_decisions_pages():
    eng = Fake(total=9000)
    span = SimpleNamespace(lo=0, hi=9000)
    got = list(repairways._span_rows(eng, "src", "d", "t", COLS, "id", span))
    assert [k for k, _ in got] == list(range(9000))
    assert eng.asked[:2] == [FIRST, FIRST * decide.PAGE_GROWTH]
    assert not hasattr(repairways, "READ")


def test_the_leaf_reader_asks_the_decision_and_a_cursors_batch_once():
    eng = Fake()
    s = SimpleNamespace(eng=eng, side="src", db="d", table="t", cols=COLS,
                        where=None, key=["id"], as_text=lambda: ())
    assert sum(1 for _ in leaves._rows(s)) == 20000
    _grew(eng.asked)
    eng = Fake()
    s.eng, s.key = eng, []
    assert sum(1 for _ in leaves._rows(s)) == 1
    assert eng.batched == [decide.page_given(None)] and not eng.asked


def test_the_repair_snapshot_and_the_unfit_scan_ask_the_decision():
    eng = Fake()
    batches = list(repairlog._all_rows(eng, "src", "d", "t", COLS, ["id"]))
    assert sum(len(b) for b in batches) == 20000
    _grew(eng.asked)
    eng = Fake()
    assert sum(len(b) for b in eng._every_row("src", "d", "t", COLS)) == 20000
    _grew(eng.asked)
    eng = Fake()
    eng.RESUMES_BY_KEY = False
    list(eng._every_row("src", "d", "t", COLS))
    assert eng.batched == [decide.page_given(None)]


def test_the_sampled_check_walks_its_keys_by_the_decision_unless_told():
    eng = Fake()
    eng.CANON_ENGINE = "postgres"
    eng.neutral_rows_by_key = lambda *a, **k: {}
    ranges.sampled(eng, "d", "t", COLS, ["id"], 5)
    assert eng.asked[0] == FIRST and len(eng.asked) > 2
    eng = Fake()
    eng.CANON_ENGINE = "postgres"
    eng.neutral_rows_by_key = lambda *a, **k: {}
    ranges.sampled(eng, "d", "t", COLS, ["id"], 5, page=777)
    assert set(eng.asked) == {777}


def test_the_probe_read_sizes_its_pages_to_its_seconds_and_holds_its_budget():
    eng = Fake(total=10 ** 6)
    got = eng.probe_read("src", "d", "t", budget=300_000, seconds=1.5)
    assert eng.asked[0] == FIRST
    # once a row's width is known no page asks past the bytes left
    assert all(a * 110 <= 300_000 for a in eng.asked[1:])
    assert got["bytes"] <= 300_000 + max(eng.asked) * 120
    assert got["rows"] == sum(min(a, 10 ** 6) for a in eng.asked)


def test_the_drilldown_page_is_the_decisions_held_to_the_cap():
    """`_localise` walks a side by pages the decision gives, never past the
    rows its evidence cap leaves."""
    eng = Fake(total=10 ** 6)
    eng.DRILL_CAP = 3500
    eng.neutral_rows_by_key = lambda side, db, t, cols, key, raw, **k: {
        str(r[0]): (r[0], ROW_PAD) for r in raw}
    eng._key_of = lambda cols, key, row: str(row[0])
    eng._row_text = lambda cols, row: str(row)
    out = eng._localise("d", "t", eng, "t", COLS, eng, "t", COLS)
    assert eng.asked[0] == FIRST
    # each side's walk: 1000, 2000, then the 500 the cap leaves
    assert eng.asked == [1000, 2000, 500] * 2
    assert out[3]                           # capped, and says so
