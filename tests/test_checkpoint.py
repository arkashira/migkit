"""Resuming a partial verify must give the same answer as never stopping.

That is the only property that matters here: if summing yesterday's ranges
with today's can produce a total that matches neither side, the feature is
worse than not having it.
"""
import json

from migkit.checkpoint import (Checkpoint, fingerprint, plan_ranges, where)

EXPR = "md5(a||b)"


def test_ranges_cover_the_entire_keyspace_not_just_the_observed_one():
    r = plan_ranges(1, 10, 4)
    assert r == [(None, 5), (5, 9), (9, None)]
    # open at both ends: boundaries come from the source, and a target row
    # below its minimum or above its maximum is precisely what we want to
    # catch, so no key may fall between two ranges
    assert r[0][0] is None and r[-1][1] is None
    # and the ranges are contiguous with no gap or overlap
    assert all(r[i][1] == r[i + 1][0] for i in range(len(r) - 1))


def test_degenerate_ranges_fall_back_to_the_whole_table():
    for args in ((None, 10, 5), (1, None, 5), (1, 10, 0), (10, 1, 5)):
        assert plan_ranges(*args) == [(None, None)]


def test_where_covers_each_shape():
    assert where('"id"', None, None) == ""
    assert where('"id"', 5, None) == '"id" >= 5'
    assert where('"id"', None, 5) == '"id" < 5'
    assert where('"id"', 5, 9) == '"id" >= 5 and "id" < 9'


def test_partials_sum_to_the_same_total_as_one_pass(tmp_path):
    """The point of the whole module."""
    ranges = plan_ranges(1, 300, 100)
    # the real sums are 64-bit-wide numerics; the arithmetic is what is under
    # test, so small numbers prove it just as well and read better
    parts = [(100, 7), (150, 11), (50, 13)]
    whole_rows = sum(r for r, _ in parts)
    whole_sum = sum(c for _, c in parts)

    cp = Checkpoint(str(tmp_path / "cp.json"))
    todo = cp.begin("public.t", EXPR, ranges)
    assert todo == ranges                       # nothing done yet
    for rng, (rows_in_range, csum) in zip(ranges, parts):
        cp.record("public.t", *rng, rows_in_range, csum)
    rows, total = cp.total("public.t")
    assert rows == whole_rows
    assert int(total) == whole_sum


def test_resume_skips_completed_ranges_only(tmp_path):
    path = str(tmp_path / "cp.json")
    ranges = plan_ranges(1, 300, 100)
    cp = Checkpoint(path)
    cp.begin("public.t", EXPR, ranges)
    cp.record("public.t", *ranges[0], 100, 5)

    resumed = Checkpoint(path)                  # new process
    todo = resumed.begin("public.t", EXPR, ranges)
    assert todo == ranges[1:]
    assert resumed.resumed("public.t") == 1
    assert resumed.total("public.t") == (100, "5")


def test_changing_the_checksum_expression_discards_partials(tmp_path):
    """Adding ranges hashed one way to ranges hashed another gives a total
    that is simply wrong. Throw them away instead."""
    path = str(tmp_path / "cp.json")
    ranges = plan_ranges(1, 200, 100)
    cp = Checkpoint(path)
    cp.begin("public.t", EXPR, ranges)
    cp.record("public.t", *ranges[0], 100, 5)

    other = Checkpoint(path)
    todo = other.begin("public.t", "md5(a||b||c)", ranges)
    assert todo == ranges                       # everything, again
    assert other.total("public.t") == (0, "0")


def test_rechunking_discards_partials(tmp_path):
    path = str(tmp_path / "cp.json")
    cp = Checkpoint(path)
    cp.begin("public.t", EXPR, plan_ranges(1, 200, 100))
    cp.record("public.t", 1, 101, 100, 5)

    other = Checkpoint(path)
    # different boundaries cover different rows - the old sums do not apply
    todo = other.begin("public.t", EXPR, plan_ranges(1, 200, 50))
    assert len(todo) == len(plan_ranges(1, 200, 50))
    assert other.total("public.t") == (0, "0")


def test_fingerprint_is_stable_and_sensitive():
    r = plan_ranges(1, 100, 10)
    assert fingerprint(EXPR, r) == fingerprint(EXPR, r)
    assert fingerprint(EXPR, r) != fingerprint(EXPR + " ", r)
    assert fingerprint(EXPR, r) != fingerprint(EXPR, plan_ranges(1, 100, 20))


def test_an_unreadable_checkpoint_starts_over_instead_of_crashing(tmp_path):
    p = tmp_path / "cp.json"
    p.write_text("{not json at all")
    cp = Checkpoint(str(p))
    ranges = plan_ranges(1, 100, 50)
    assert cp.begin("public.t", EXPR, ranges) == ranges


def test_a_checkpoint_from_a_future_format_is_ignored(tmp_path):
    p = tmp_path / "cp.json"
    p.write_text(json.dumps({"format_version": 99, "tables": {
        "public.t": {"fingerprint": "x", "done": {"1..2": [9, "9"]}}}}))
    cp = Checkpoint(str(p))
    assert cp.resumed("public.t") == 0


def test_writes_are_atomic_and_leave_no_temp_files(tmp_path):
    path = str(tmp_path / "sub" / "cp.json")
    cp = Checkpoint(path)
    cp.begin("public.t", EXPR, [(1, 2)])
    cp.record("public.t", 1, 2, 1, 1)
    assert json.loads(open(path).read())["tables"]
    assert [f.name for f in tmp_path.joinpath("sub").iterdir()] == ["cp.json"]


def test_clear_forgets_a_finished_table(tmp_path):
    path = str(tmp_path / "cp.json")
    cp = Checkpoint(path)
    cp.record("public.t", 1, 2, 1, 1)
    cp.clear("public.t")
    assert Checkpoint(path).total("public.t") == (0, "0")


def test_no_path_means_no_file_but_still_works_in_memory():
    cp = Checkpoint(None)
    cp.record("public.t", 1, 2, 5, 7)
    assert cp.total("public.t") == (5, "7")
