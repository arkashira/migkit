"""Where a key lies among a copy's ranges (`ranges.place`): read, being
read, not begun, or outside the plan - each with the mark its range was
begun under. The one reading of a checkpoint entry that both the tail
after a move (`ranges.already_read`) and a table copied beside a running
tail (`tailtables`) hold changes to; `already_read` answers as it did
before it was built on it.
"""
from hypothesis import given, settings
from hypothesis import strategies as st

from migkit import ranges


def _entry():
    st_ = {}
    ranges.plan(st_, 0, 9, lambda: [3, 6], lambda: None, table="t")
    return st_


def test_each_key_in_its_range_s_state():
    e = _entry()
    ranges.started(e, -1, "m0", lambda: None)
    ranges.finished(e, -1, lambda: None)
    ranges.started(e, 3, "m3", lambda: None)
    assert ranges.place(e, [2]) == [("read", "m0")]
    assert ranges.place(e, [5]) == [("reading", "m3")]
    assert ranges.place(e, [8]) == [("todo", None)]
    assert ranges.place(e, [42, "x", True]) == [("outside", None)] * 3
    assert ranges.place(e, [2, 8]) == [("read", "m0"), ("todo", None)]
    assert ranges.place({}, [1]) == []
    # a copy done has no range left to begin
    e["done"] = True
    assert ranges.place(e, [8]) == [("outside", None)]


def _old_already_read(st_, values, covers, ahead=False):
    """`already_read` as it read before `place`: the reference it still
    answers as."""
    import bisect
    rs = st_.get("ranges")
    if not rs or not values:
        return False
    seen = st_.get("ranges_seen") or {}
    for v in values:
        if not isinstance(v, int) or isinstance(v, bool):
            return False
        i = bisect.bisect_left(rs, v, key=lambda r: r[0]) - 1
        if i < 0 or v > rs[i][1]:
            return False
        at = str(rs[i][0])
        if at in seen:
            if seen[at] is None or covers(seen[at]) is not True:
                return False
        elif not ahead or st_.get("done") \
                or rs[i][0] in (st_.get("ranges_done") or ()):
            return False
    return True


@settings(max_examples=300, deadline=None)
@given(begun=st.lists(st.sampled_from([-1, 3, 6]), max_size=3),
       finished=st.lists(st.sampled_from([-1, 3, 6]), max_size=3),
       marks=st.lists(st.sampled_from([None, "yes", "no"]), min_size=3,
                      max_size=3),
       values=st.lists(st.integers(-2, 11), min_size=1, max_size=2),
       ahead=st.booleans(), done=st.booleans())
def test_already_read_answers_as_before(begun, finished, marks, values,
                                        ahead, done):
    e = _entry()
    for after in begun:
        ranges.started(e, after, marks[[-1, 3, 6].index(after)],
                       lambda: None)
    for after in finished:
        if after not in e["ranges_done"]:
            e["ranges_done"].append(after)
    if done:
        e["done"] = True

    def covers(mark):
        return mark == "yes"
    assert ranges.already_read(e, values, covers, ahead) \
        == _old_already_read(e, values, covers, ahead)
