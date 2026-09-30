"""The digest tree of a table in one scan a side, the differing rows read
only from the differing leaves, held to the totals, and a sketch of the
differences that decodes or says it did not (backlog R20.9, F1).

These run on an engine that reads rows into this process, so the arithmetic
is pinned without a server; the SQL engines are held to the same numbers in
`test_one_scan_leaves_on_servers.py`.
"""
import random

import pytest

from migkit import canon, leaves

COLS = [("id", "integer"), ("name", "text"), ("v", "integer")]


class Fake:
    """An engine of rows in memory, read the way the in-process path reads
    (`neutral_read` by key, `neutral_batches` without one)."""
    CANON_ENGINE = "fake"
    SQL_DIALECT = None
    RESUMES_BY_KEY = True

    def __init__(self, rows, key=("id",), cols=COLS):
        self.rows, self.key, self.cols = list(rows), list(key), list(cols)

    def _at(self, names):
        at = [c for c, _ in self.cols]
        return [at.index(n) for n in names]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        pos = self._at([n for n, _ in columns])
        kpos = self._at(self.key)
        rows = sorted(self.rows, key=lambda r: tuple(r[i] for i in kpos))
        if after is not None:
            rows = [r for r in rows if tuple(r[i] for i in kpos) > after]
        rows = rows[:limit]
        out = [[r[i] for i in pos] for r in rows]
        last = tuple(rows[-1][i] for i in kpos) if rows else None
        return out, last

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        pos = self._at([n for n, _ in columns])
        for i in range(0, len(self.rows), size):
            yield [[r[j] for j in pos] for r in self.rows[i:i + size]]

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        pos = self._at([n for n, _ in columns])
        kpos = self._at(key)
        cls = dict(columns)
        want = {tuple(k) for k in keys}
        out = {}
        for r in self.rows:
            if tuple(r[i] for i in kpos) in want:
                out[tuple(canon.render_value(cls[k], r[i])
                          for k, i in zip(key, kpos))] = [r[i] for i in pos]
        return out


def _sides(src_rows, dst_rows, key=("id",), cols=COLS):
    a = leaves.Side(Fake(src_rows, key, cols), "src", "db", "t", cols,
                    list(key))
    b = leaves.Side(Fake(dst_rows, key, cols), "dst", "db", "t", cols,
                    list(key))
    return a, b


def _table(n, seed=1):
    rnd = random.Random(seed)
    return [[i, f"n{rnd.randint(0, 10 ** 6)}", rnd.randint(0, 99)]
            for i in range(1, n + 1)]


def test_the_levels_found_from_the_top_are_the_leaves_that_differ():
    rnd = random.Random(7)
    for _ in range(50):
        a = leaves.Tree({b: (1, rnd.randint(0, 9), 0)
                         for b in rnd.sample(range(-40, 300), 120)})
        b = leaves.Tree(dict(a.leaves))
        for x in rnd.sample(sorted(b.leaves), 5):
            b.leaves[x] = (1, b.leaves[x][1] + 1, 0)
        for x in rnd.sample(range(-60, 400), 3):
            b.leaves.pop(x, None)
        want = sorted(x for x in set(a.leaves) | set(b.leaves)
                      if a.leaves.get(x) != b.leaves.get(x))
        assert a.differing(b) == want
        assert b.differing(a) == want
    # every level adds up to the root
    t = leaves.Tree({3: (2, 5, 7), 9: (1, 1, 1), -4: (3, 3, 3)})
    for level in t.levels():
        assert tuple(map(sum, zip(*level.values()))) == t.root


@pytest.mark.parametrize("kind", ["hash", "range"])
def test_the_rows_that_differ_are_found_and_add_up_to_the_totals(
        kind, monkeypatch):
    monkeypatch.setattr(leaves, "LEAF_ROWS", 100)
    src = _table(5000)
    dst = [list(r) for r in src]
    dst[1234][1] = "changed"          # id 1235 changed
    del dst[4000]                     # id 4001 missing on the target
    dst.append([7000, "extra", 1])    # id 7000 only on the target
    a, b = _sides(src, dst)
    spec = leaves.plan(len(src), kind, 1, 7000, salt="ab12")
    ta, tb = leaves.scan(a, spec), leaves.scan(b, spec)
    assert len(ta.leaves) >= 5
    found = leaves.localize(a, b, spec, ta, tb)
    assert found.complete, found.why
    assert found.changed == [("1235",)]
    assert found.missing == [("4001",)]
    assert found.extra == [("7000",)]
    # three leaves read back of the table's, not the table
    assert found.read <= 3 and found.rows < len(src) // 2, found
    assert found.found == found.delta
    cols, same = leaves.confirm(a, b, found)
    assert cols == {("1235",): ["name"]} and same == 0


def test_equal_tables_have_equal_trees_and_nothing_to_read():
    src = _table(3000)
    a, b = _sides(src, [list(r) for r in reversed(src)])
    spec = leaves.plan(len(src), "hash", salt="00")
    ta, tb = leaves.scan(a, spec), leaves.scan(b, spec)
    assert ta.leaves == tb.leaves
    found = leaves.localize(a, b, spec, ta, tb)
    assert found.complete and found.read == 0 and found.differ == 0


def test_a_text_composite_key_localizes_by_its_hash():
    cols = [("a", "text"), ("b", "text"), ("v", "integer")]
    src = [[f"k{i % 97}", f"x{i}", i] for i in range(4000)]
    dst = [list(r) for r in src]
    dst[17][2] = -1
    a, b = _sides(src, dst, key=("a", "b"), cols=cols)
    spec = leaves.plan(len(src), "hash", salt="1f")
    found = leaves.localize(a, b, spec, leaves.scan(a, spec),
                            leaves.scan(b, spec))
    assert found.complete and found.changed == [("k17", "x17")], found


def test_a_table_with_no_key_is_compared_as_a_multiset():
    # the same count on both sides and a row twice on each: XOR would
    # cancel both pairs, a sum does not
    src = [[1, "a", 0], [1, "a", 0], [5, "e", 5]]
    dst = [[2, "b", 0], [2, "b", 0], [5, "e", 5]]
    a, b = _sides(src, dst, key=())
    spec = leaves.plan(3, "row", salt="77")
    ta, tb = leaves.scan(a, spec), leaves.scan(b, spec)
    assert ta.root[0] == tb.root[0] and ta.root != tb.root
    found = leaves.localize(a, b, spec, ta, tb)
    assert found.complete, found.why
    assert (found.only_src, found.only_dst) == (2, 2)
    # a duplicate held once more on one side is one row, not zero
    a, b = _sides(src + [[1, "a", 0]], src, key=())
    found = leaves.localize(a, b, spec, leaves.scan(a, spec),
                            leaves.scan(b, spec))
    assert found.complete and (found.only_src, found.only_dst) == (1, 0)


def test_rows_that_do_not_add_up_are_never_called_localized(monkeypatch):
    src = _table(3000)
    dst = [list(r) for r in src]
    dst[10][2] += 1
    dst[2000][2] += 1
    a, b = _sides(src, dst)
    spec = leaves.plan(len(src), "hash", salt="aa")
    ta, tb = leaves.scan(a, spec), leaves.scan(b, spec)
    real = leaves.leaf_rows
    # a read-back that lost a row of the target: the leaf does not add up
    def short(s, spec, buckets):
        got = real(s, spec, buckets)
        if s.side == "dst" and got.keyed:
            got.keyed.pop(next(iter(got.keyed)))
        return got
    monkeypatch.setattr(leaves, "leaf_rows", short)
    found = leaves.localize(a, b, spec, ta, tb)
    assert not found.complete and "do not add up" in found.why, found
    monkeypatch.setattr(leaves, "leaf_rows", real)
    # a difference the rows cannot account for: the totals disagree
    tb.leaves[next(iter(tb.leaves))] = (0, 0, 0)
    found = leaves.localize(a, b, spec, ta, tb)
    assert not found.complete, found


def test_the_sketch_decodes_a_seeded_difference_set():
    rnd = random.Random(3)
    sa, sb = leaves.Sketch(64), leaves.Sketch(64)
    common = [(rnd.randrange(10 ** 9), rnd.getrandbits(60))
              for _ in range(5000)]
    for k, h in common:
        sa.add(1, k, h)
        sb.add(1, k, h)
    only_a = [(rnd.randrange(10 ** 9), rnd.getrandbits(60))
              for _ in range(30)]
    only_b = [(rnd.randrange(10 ** 9), rnd.getrandbits(60))
              for _ in range(20)]
    for k, h in only_a:
        sa.add(1, k, h)
    for k, h in only_b:
        sb.add(1, k, h)
    got, whole = sa.minus(sb).decode()
    assert whole
    assert sorted((k, h) for n, k, h in got if n == 1) == sorted(only_a)
    assert sorted((k, h) for n, k, h in got if n == -1) == sorted(only_b)
    # too many differences for its cells: it says so, never half an answer
    small = leaves.Sketch(4)
    for k, h in only_a:
        small.add(1, k, h)
    got, whole = small.decode()
    assert not whole


def test_the_sketch_carries_how_many_times_a_row_is_held():
    sa, sb = leaves.Sketch(32), leaves.Sketch(32)
    h1, h2 = (1 << 59) + 12345, 987654321
    for _ in range(3):
        sa.add(1, h1, h1)
    sb.add(1, h1, h1)
    sb.add(1, h2, h2)
    got, whole = sa.minus(sb).decode()
    assert whole and sorted(got) == sorted([(2, h1, h1), (-1, h2, h2)])


@pytest.mark.parametrize("key,cols", [
    (("id",), COLS),
    (("name", "id"), COLS),
    ((), COLS)])
def test_the_sketch_finds_the_rows_the_leaves_find(key, cols):
    src = _table(4000, seed=5)
    dst = [list(r) for r in src]
    dst[5][2] += 1
    del dst[100]
    dst.append([9001, "x", 1])
    a, b = _sides(src, dst, key=key, cols=cols)
    spec = leaves.plan(len(src), "hash" if key else "row", salt="5e")
    by_leaves = leaves.localize(a, b, spec, leaves.scan(a, spec),
                                leaves.scan(b, spec))
    by_sketch, why = leaves.by_sketch(a, b, spec, expected=3)
    assert why is None and by_sketch.complete
    assert by_leaves.complete
    for part in ("missing", "changed", "extra", "only_src", "only_dst"):
        x, y = getattr(by_leaves, part), getattr(by_sketch, part)
        assert (sorted(x) if isinstance(x, list) else x) == \
            (sorted(y) if isinstance(y, list) else y), part
    assert by_sketch.differ == (3 if key else 4)


def test_a_sketch_too_small_for_the_differences_falls_to_the_leaves():
    src = _table(2000)
    dst = [[r[0], r[1] + "!", r[2]] for r in src[:300]] + src[300:]
    a, b = _sides(src, dst)
    spec = leaves.plan(len(src), "hash", salt="01")
    got, why = leaves.by_sketch(a, b, spec, expected=2)
    assert got is None and "did not peel" in why


def _facts(**over):
    base = {"key": "integer", "sql": True, "kept": False, "expected": None,
            "disagreed": True, "two readings": False, "tail": False,
            "rtt_ms": 1.0, "row hash reader": False,
            "bisection reader": False}
    base.update(over)
    return base


def test_the_passes_are_chosen_from_what_the_table_and_link_are():
    s = leaves.strategy("t", _facts())
    assert s.names == {"prove": "scan", "leaves": "range",
                       "localize": "leaf reads", "confirm": "totals",
                       "keep": "again", "second": "none"}, s.names
    s = leaves.strategy("t", _facts(key="other", expected=10, rtt_ms=80,
                                    tail=True))
    assert s.names["leaves"] == "hash" and s.names["localize"] == \
        "sketch" and s.names["confirm"] == "fenced", s.names
    # a sketch that did not decode: the leaves, and why
    s = leaves.strategy("t", _facts(expected=10, rtt_ms=80,
                                    **{"sketch failed": "did not peel"}))
    assert s.names["localize"] == "leaf reads", s.names
    assert "did not prove itself: did not peel" in s.reasons["localize"]
    # the link is fast: nothing to gain from the sketch, and it says why
    s = leaves.strategy("t", _facts(expected=10, rtt_ms=0.4))
    assert s.names["localize"] == "leaf reads"
    assert "not the limit" in s.reasons["localize"], s.reasons
    # the kept leaves where the hop keeps them
    s = leaves.strategy("t", _facts(kept=True))
    assert s.names["prove"] == "kept" and s.names["keep"] == "kept"
    # a second, independent reading where the digest disagreed and a
    # reader is installed; none where none is
    s = leaves.strategy("t", _facts(**{"bisection reader": True}))
    assert s.names["second"] == "bisection", s.names
    s = leaves.strategy("t", _facts(disagreed=False,
                                    **{"bisection reader": True}))
    assert s.names["second"] == "none", s.names
    s = leaves.strategy("t", _facts(**{"row hash reader": True,
                                       "bisection reader": True}))
    assert s.names["second"] == "row hash", s.names


def test_the_measured_cheaper_reader_is_the_second_reading():
    from migkit import decide
    c = decide.Costs({"second/row hash": 9.0, "second/bisection": 1.0})
    s = leaves.strategy("t", _facts(**{"row hash reader": True,
                                       "bisection reader": True}), costs=c)
    assert s.names["second"] == "bisection", s.names


def test_the_bisecting_reader_localizes_where_it_measures_cheaper():
    from migkit import decide
    facts = _facts(**{"same engine": True, "same key": True,
                      "bisection reader": True})
    c = decide.Costs()
    # never timed: the leaves first, in the list's order
    s = leaves.strategy("t", facts, costs=c)
    assert s.names["localize"] == "leaf reads", s.names
    c.saw("localize/leaf reads", 1_000_000, 3.4)
    # then the one never timed
    s = leaves.strategy("t", facts, costs=c)
    assert s.names["localize"] == "bisection", s.names
    # and it is not asked again as the second reading
    assert s.names["second"] != "bisection", s.names
    c.saw("localize/bisection", 1_000_000, 2.0)
    s = leaves.strategy("t", facts, costs=c)
    assert s.names["localize"] == "bisection", s.names
    # what it named did not add up: the leaves, and why
    s = leaves.strategy("t", {**facts, "bisection failed": "short by one"},
                        costs=c)
    assert s.names["localize"] == "leaf reads", s.names
    assert "short by one" in s.reasons["localize"], s.reasons
    # two engines write a value's text apart: never across them
    s = leaves.strategy("t", {**facts, "same engine": False}, costs=c)
    assert s.names["localize"] == "leaf reads", s.names
