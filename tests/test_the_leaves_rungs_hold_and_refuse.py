"""The rungs a digest's leaves, its localizer, its fence, what it keeps
between proofs, its second reading and its row hash are chosen by, each
held where its needs hold and refused with its own reason where one does
not, on fakes (backlog AUD-Z.rungs-outside-owed): no range of a key that
is not one integer, no sketch where nothing says how few differ, no
fence where no tail runs, and no hash a table without a key would read
apart."""
import pytest

from migkit import decide, leaves
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('leaves', 'HASH_WAYS', 'md5', 'holds'),
    ('leaves', 'PAIR_WAYS', 'md5', 'holds'),
    ('leaves', 'PAIR_WAYS', 'measured', 'holds'),
    ('leaves', 'PASSES', 'again', 'holds'),
    ('leaves', 'PASSES', 'bisection', 'holds'),
    ('leaves', 'PASSES', 'bisection', 'refuses'),
    ('leaves', 'PASSES', 'fenced', 'holds'),
    ('leaves', 'PASSES', 'fenced', 'refuses'),
    ('leaves', 'PASSES', 'generations', 'holds'),
    ('leaves', 'PASSES', 'generations', 'refuses'),
    ('leaves', 'PASSES', 'groups', 'holds'),
    ('leaves', 'PASSES', 'groups', 'refuses'),
    ('leaves', 'PASSES', 'hash', 'holds'),
    ('leaves', 'PASSES', 'kept', 'holds'),
    ('leaves', 'PASSES', 'kept', 'refuses'),
    ('leaves', 'PASSES', 'leaf reads', 'holds'),
    ('leaves', 'PASSES', 'leaf reads', 'refuses'),
    ('leaves', 'PASSES', 'none', 'holds'),
    ('leaves', 'PASSES', 'none', 'refuses'),
    ('leaves', 'PASSES', 'range', 'holds'),
    ('leaves', 'PASSES', 'range', 'refuses'),
    ('leaves', 'PASSES', 'row hash', 'holds'),
    ('leaves', 'PASSES', 'row hash', 'refuses'),
    ('leaves', 'PASSES', 'scan', 'holds'),
    ('leaves', 'PASSES', 'sketch', 'holds'),
    ('leaves', 'PASSES', 'sketch', 'refuses'),
    ('leaves', 'PASSES', 'totals', 'holds'),
    ('leaves', '_hash_way', '<way>', 'holds'),
    ('leaves', '_hash_way', '<way>', 'refuses'),
]


def _held_then_refused(rung, ok, bad):
    with watched() as seen:
        kept, _ = decide.prune([rung], decide.Facts(ok))
        _, out = decide.prune([rung], decide.Facts(bad))
    assert kept == [rung] and [r for r, _ in out] == [rung]
    assert branch(seen, rung.name) == [HOLDS, REFUSES], seen
    assert why(seen, rung.name) == out[0][1]
    return out[0][1]


def _passes(name):
    """Every pass of that name, part by part - two parts share 'kept'
    and 'bisection'."""
    got = [r for r in _ALL if r.name == name]
    assert got, name
    return got


#: every pass of every part, in the parts' order
_ALL = [r for part in leaves.PASSES for r in part.rungs]
SKETCH = {"expected": 10, "rtt_ms": leaves.SLOW_RTT_MS * 2}
GROUPS = {"row hash reader": True, "group column": "id"}
BISECT = {"same engine": True, "same key": True, "bisection reader": True}

#: (pass, which of that name, facts it holds on, facts it does not, said)
CASES = [
    ("kept", 0, {"kept": True}, {}, "needs the hop to keep its proof"),
    ("kept", 1, {"kept": True}, {"kept": False}, "keep_proof"),
    ("range", 0, {"key": "integer", "sql": True}, {"key": "text"},
     "not one integer on both sides"),
    ("range", 0, {"key": "integer", "sql": True},
     {"key": "integer", "sql": False}, "read into this process whole"),
    ("sketch", 0, SKETCH, dict(SKETCH, expected=None),
     "nothing says how many there are"),
    ("sketch", 0, SKETCH, dict(SKETCH, rtt_ms=1), "the link is not the limit"),
    ("sketch", 0, SKETCH, dict(SKETCH, localizer="groups"),
     "measured dearer here"),
    ("sketch", 0, SKETCH, dict(SKETCH, **{"wide sums": False}),
     "too narrow"),
    ("leaf reads", 0, {}, {"localizer": "sketch"}, "measured dearer here"),
    ("groups", 0, GROUPS, dict(GROUPS, **{"group column": None}),
     "source's statistics"),
    ("groups", 0, GROUPS, dict(GROUPS, **{"row hash reader": False}),
     "needs the second reader"),
    ("bisection", 0, BISECT, dict(BISECT, **{"same engine": False}),
     "which two engines write apart"),
    ("bisection", 0, BISECT, dict(BISECT, **{"bisection reader": False}),
     "the bisecting reader is not installed"),
    ("bisection", 1, {"two readings": True, "bisection reader": True},
     {"two readings": True}, "the bisecting reader is not installed"),
    ("generations", 0, {"written": True}, {}, "did not move"),
    ("fenced", 0, {"tail": True}, {"tail": False}, "no tail runs"),
    ("none", 0, {}, {"disagreed": True, "row hash reader": True},
     "migkit's own digest disagreed"),
    ("row hash", 0, {"two readings": True, "row hash reader": True},
     {"two readings": True}, "the second reader is not installed"),
]


@pytest.mark.parametrize("name,nth,ok,bad,said", CASES)
def test_a_pass_is_held_where_its_needs_hold_and_refused_where_not(
        name, nth, ok, bad, said):
    got = _held_then_refused(_passes(name)[nth], ok, bad)
    assert said in got, got


def test_every_pass_with_needs_is_proved_on_both_its_branches_here():
    proved = {(n, i) for n, i, *_ in CASES}
    asks = {(r.name, _passes(r.name).index(r)) for r in _ALL if r.needs}
    assert asks <= proved, sorted(asks - proved)


@pytest.mark.parametrize("name", ["scan", "hash", "totals", "again"])
def test_a_pass_with_no_need_is_held_wherever_it_is_offered(name):
    (rung,) = _passes(name)
    with watched() as seen:
        kept, _ = decide.prune([rung], decide.Facts({}))
    assert kept == [rung] and branch(seen, name) == [HOLDS]


def test_md5_is_held_wherever_no_other_hash_is_measured():
    for ladder in (leaves.HASH_WAYS, leaves.PAIR_WAYS):
        with watched() as seen:
            kept, out = decide.prune(ladder, decide.Facts({"keyed": False}))
        assert "md5" in [r.name for r in kept]
        assert all("only md5 is taken" in w for _, w in out)
        assert branch(seen, "md5") == [HOLDS]
    assert branch(seen, "measured") == [HOLDS]


def test_a_faster_hash_is_held_only_on_a_keyed_table_it_was_timed_on():
    rung = leaves._hash_way("xxh3", "xxh3 of each row's canonical text")
    timed = {"keyed": True, "probe": {"xxh3": (0.2, None)}}
    got = _held_then_refused(rung, timed, dict(timed, keyed=False))
    assert "only md5 is taken" in got
    got = _held_then_refused(rung, timed, dict(timed, probe={
        "xxh3": (None, "the target's SQL has no xxh3")}))
    assert got == "the target's SQL has no xxh3"
