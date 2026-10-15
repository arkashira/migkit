"""The generic engine, any SQL source reached through a standard driver,
compares each table the way its facts allow (`GenericEngine.compare_rungs`,
chosen by `decide.climb`): key ranges checksummed on both servers where one
key column names one row and the comparison component is installed, row
digests in this machine otherwise - a table without a key, a key of many
columns, a key that repeats - held to the memory the table would take.
On fakes: no server is reached."""
import decimal

import pytest

from migkit import decide
from migkit.config import Endpoint, Hop
from migkit.engines import generic
from migkit.engines.generic import GenericEngine

PROVES = [("move.large-values", "generic"), ("move.partitioned", "generic")]

COLUMNS = ("id", "name", "price")


class Fake(GenericEngine):
    """Each side's rows in memory; the facts as the test sets them."""

    def __init__(self, hop, src, dst, columns=COLUMNS, unique=True,
                 held=100, memory=10_000):
        super().__init__(hop)
        self.rows = {"src": src, "dst": dst}
        self.cols, self.uniq = columns, unique
        self.held_, self.memory_ = held, memory
        self.drills = {}

    def _side_columns(self, which, table):
        return tuple(self.cols)

    def _unique(self, table):
        return self.uniq

    def _held_bytes(self, table):
        return self.held_

    def _memory(self):
        return self.memory_

    def _side_rows(self, which, table, columns):
        at = [self.cols.index(c) for c in columns]
        return [tuple(r[i] for i in at) for r in self.rows[which]]

    def _write_drill(self, db, name, **kinds):
        self.drills[name] = kinds


def _hop(key="id"):
    return Hop(name="g", engine="generic",
               source=Endpoint(host="", port=0, user="", password="",
                               options={"url": "postgresql://u@10.0.0.1/a"}),
               target=Endpoint(host="", port=0, user="", password="",
                               options={"url": "postgresql://u@10.0.0.2/a"}),
               options={"tables": ["items"], "key": key})


ROWS = [(1, "a", decimal.Decimal("1.50")), (2, "b", decimal.Decimal("2")),
        (3, None, None)]


@pytest.fixture
def no_comparer(monkeypatch):
    monkeypatch.setattr(generic, "which", lambda name: None)


@pytest.fixture
def comparer(monkeypatch):
    monkeypatch.setattr(generic, "which", lambda name: "/usr/bin/" + name)


def test_one_unique_key_column_goes_by_key_ranges(comparer):
    choice, _ = Fake(_hop(), ROWS, ROWS).compare_way("items")
    assert choice.path == "key-ranges"


def test_a_repeated_key_is_refused_by_the_ranges_and_digested(comparer):
    choice, _ = Fake(_hop(), ROWS, ROWS, unique=False).compare_way("items")
    assert choice.path == "row-digests"
    assert "names one row" in choice.reason


def test_an_unread_uniqueness_leaves_the_ranges_to_the_key_check(comparer):
    choice, _ = Fake(_hop(), ROWS, ROWS, unique=None).compare_way("items")
    assert choice.path == "key-ranges"


def test_a_key_of_two_columns_is_digested(comparer):
    choice, _ = Fake(_hop(["id", "name"]), ROWS, ROWS).compare_way("items")
    assert choice.path == "row-digests"
    assert "2 key columns" in choice.reason


def test_a_table_without_the_key_needs_a_keyless_way(comparer):
    choice, _ = Fake(_hop("code"), ROWS, ROWS).compare_way("items")
    assert choice.path == "row-digests"
    assert "no key" in choice.reason


def test_without_the_comparison_component_the_rows_are_digested(no_comparer):
    choice, _ = Fake(_hop(), ROWS, ROWS).compare_way("items")
    assert choice.path == "row-digests"
    assert "not installed" in choice.reason


def test_a_table_too_large_to_hold_is_refused_not_read(no_comparer):
    eng = Fake(_hop(), ROWS, ROWS, held=10_000, memory=10_000)
    choice, _ = eng.compare_way("items")
    assert choice.rung is None
    got = eng.check_data("-")[0]
    assert got.status == "error"
    assert "only key ranges compare it" in got.detail


def test_unread_memory_or_size_is_never_a_yes(no_comparer):
    for held, memory in ((None, 10_000), (100, None)):
        choice, _ = Fake(_hop(), ROWS, ROWS, held=held,
                         memory=memory).compare_way("items")
        assert choice.rung is None


def test_equal_sides_are_ok_whatever_the_decimal_scale(no_comparer):
    other = [(1, "a", decimal.Decimal("1.5")), (2, "b", decimal.Decimal(2)),
             (3, None, None)]
    got = Fake(_hop(), ROWS, other).check_data("-")[0]
    assert got.status == "ok", got.detail
    assert "row digests compared in this machine" in got.detail


def test_keyed_differences_are_named_by_key(no_comparer):
    dst = [(1, "a", decimal.Decimal("1.50")), (2, "B", decimal.Decimal("2")),
           (4, "d", None)]
    eng = Fake(_hop(), ROWS, dst)
    got = eng.check_data("-")[0]
    assert got.status == "diff"
    assert "1 rows only on the source" in got.detail
    assert "1 rows only on the target" in got.detail
    assert "1 rows with different values" in got.detail
    assert eng.drills["items"] == {"missing": ["[\"3\"]"],
                                   "changed": ["[\"2\"]"],
                                   "extra": ["[\"4\"]"]}


def test_a_null_value_and_an_empty_text_are_told_apart(no_comparer):
    got = Fake(_hop(), [(1, None, None)], [(1, "", None)]).check_data("-")[0]
    assert got.status == "diff"
    assert "1 rows with different values" in got.detail


def test_a_keyless_table_compares_its_rows_as_a_multiset(no_comparer):
    src = [(1, "a", None), (1, "a", None), (2, "b", None)]
    dst = [(1, "a", None), (2, "b", None), (2, "b", None)]
    eng = Fake(_hop("code"), src, dst)
    got = eng.check_data("-")[0]
    assert got.status == "diff"
    assert "1 rows only on the source" in got.detail
    assert "1 rows only on the target" in got.detail
    assert "cannot have one row addressed" in got.fix
    assert eng.drills["items"] == {"missing": [], "changed": [], "extra": []}


def test_a_repeated_key_holding_other_rows_is_a_change(no_comparer):
    src = [(1, "a", None), (1, "b", None)]
    dst = [(1, "a", None), (1, "c", None)]
    got = Fake(_hop(), src, dst, unique=False).check_data("-")[0]
    assert got.status == "diff"
    assert "1 rows with different values" in got.detail


def test_every_shape_of_the_generic_engine_is_decided():
    for shape in decide.SHAPES:
        state, detail = decide.cell("generic", shape)
        assert state in ("yes", decide.NOT_APPLICABLE), (shape, state)
        assert detail
