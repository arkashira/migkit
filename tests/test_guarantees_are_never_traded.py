"""Guarantees are never traded for speed (backlog P0-SMART, the owner's
note C of 2026-09-30).

A strategy holds, of each guarantee, the weakest level any of its parts
holds, then whatever migkit tops up where the lift's ground holds
(`guarantees.of`); only the strategies that reach the strongest any
combination reaches are ranked, and a faster weaker one is left out with
why. The algebra is held to its laws by property tests; the climb and the
strategy space by rungs made for the purpose.
"""
from hypothesis import given, settings
from hypothesis import strategies as st

from migkit import decide as d
from migkit import guarantees as g

R = d.Rung
DIMS = list(g.ORDER)


def _levels():
    return st.fixed_dictionaries({}, optional={
        dim: st.sampled_from(g.LEVELS[dim]) for dim in DIMS})


def _rung(i, holds):
    return R(f"r{i}", f"way {i}", holds=holds)


rungs = st.lists(_levels(), min_size=1, max_size=5).map(
    lambda ls: [_rung(i, h) for i, h in enumerate(ls)])


@settings(max_examples=200, deadline=None)
@given(rungs, st.randoms())
def test_the_order_of_the_parts_does_not_matter(rs, rnd):
    shuffled = list(rs)
    rnd.shuffle(shuffled)
    assert g.of(rs) == g.of(shuffled)


@settings(max_examples=200, deadline=None)
@given(rungs)
def test_a_part_twice_is_the_part_once(rs):
    assert g.of(rs + rs) == g.of(rs)


@settings(max_examples=200, deadline=None)
@given(rungs, _levels())
def test_a_part_more_never_raises_a_guarantee_without_a_lift(rs, extra):
    before = g.of(rs)
    after = g.of(rs + [_rung(99, extra)])
    for dim in DIMS:
        assert after[dim] <= before[dim], (dim, before, after)


@settings(max_examples=200, deadline=None)
@given(rungs, st.integers(0, 4), st.sampled_from(DIMS))
def test_a_stronger_part_never_weakens_the_whole(rs, which, dim):
    """Raising one part's level of one guarantee never lowers any of the
    strategy's (monotone)."""
    which %= len(rs)
    before = g.of(rs)
    holds = dict(rs[which].holds)
    holds[dim] = g.LEVELS[dim][-1]
    stronger = list(rs)
    stronger[which] = _rung(which, holds)
    after = g.of(stronger)
    for x in DIMS:
        assert after[x] >= before[x], (x, before, after)


@settings(max_examples=200, deadline=None)
@given(rungs)
def test_the_whole_is_the_weakest_part(rs):
    got = g.of(rs)
    for dim in DIMS:
        said = [g.declared(r)[dim] for r in rs if dim in g.declared(r)]
        assert got[dim] == (min(said) if said else 0)


@settings(max_examples=200, deadline=None)
@given(rungs, st.lists(rungs, min_size=0, max_size=3))
def test_the_bound_is_never_below_what_a_completion_reaches(chosen, left):
    """`optimistic` - what the strategy space prunes by - is never below
    what any way of completing the parts reaches, so a subtree it leaves
    out holds nothing that could have been taken."""
    import itertools
    bound = g.optimistic(chosen, left)
    for completion in itertools.product(*left) if left else [()]:
        got = g.of(list(chosen) + list(completion))
        for dim in DIMS:
            assert bound[dim] >= got[dim], (dim, bound, got)


def test_a_lift_raises_only_where_its_ground_holds():
    lift = g.Lift("correct", "proved", (("idempotent", "converges"),))
    bulk = R("bulk", "a bulk program", holds={"correct": "unproved"})
    digest = R("digest", "every row digested and repaired", lifts=(lift,))
    # a repair needs writes that converge: without them, no lift
    assert g.of([bulk, digest])["correct"] == 0
    converging = R("bulk", "a bulk program", holds={
        "correct": "unproved", "idempotent": "converges"})
    got = g.of([converging, digest])
    assert got["correct"] == g.level("correct", "proved")
    assert g.said(got).startswith("every row proved")


def test_lifts_reach_a_fixed_point_through_each_other():
    a = g.Lift("idempotent", "converges", (("resumable", "goes on"),))
    b = g.Lift("correct", "proved", (("idempotent", "converges"),))
    rs = [R("x", "x", holds={"resumable": "goes on"}, lifts=(b,)),
          R("y", "y", lifts=(a,))]
    got = g.of(rs)
    assert got["idempotent"] == 1 and got["correct"] == 2


def test_the_climb_ranks_only_what_reaches_the_strongest():
    fast = R("fast", "the fast way", holds={"correct": "counted"})
    exact = R("exact", "the proved way", holds={"correct": "proved"})
    c = d.Costs({"fast": 1.0, "exact": 5.0})
    got = d.climb("t", [fast, exact], costs=c)
    assert got.path == "exact"
    assert got.reason == ("the fast way gives counted where proved is"
                          " reachable here (correct)")
    assert dict(got.guarantees)["correct"] == "proved"
    # where none reaches more, the fastest of them
    fast2 = R("fast", "the fast way", holds={"correct": "proved"})
    assert d.climb("t", [fast2, exact], costs=c).path == "fast"


def test_a_list_that_says_no_guarantees_is_ranked_as_before():
    """The rungs of the lists written before guarantees - which say
    nothing themselves - are not filtered by what their capabilities
    imply; the need does that, as it always did."""
    snap = R("snap", "one snapshot", gives=frozenset({"consistent as of"}))
    live = R("live", "read live")
    c = d.Costs({"snap": 5.0, "live": 1.0})
    assert d.climb("t", [snap, live], costs=c).path == "live"
    assert d.climb("t", [snap, live], costs=c,
                   need=("consistent as of",)).path == "snap"


def test_what_capabilities_say_is_read_as_guarantees():
    got = g.declared(R("k", "k", gives=frozenset({"resume by key",
                                                  "exact"})))
    assert got == {"resumable": 1, "exactly once": 1}
    assert g.lost(R("k", "k", gives=frozenset({"resume by key"})), 10) \
        == 0.05
    assert g.lost(R("w", "w"), 10) == 0.5


def test_the_strongest_is_the_first_guarantee_first():
    """Lexicographic: a stronger proof outranks going on after a stop."""
    proved = {"correct": 2, "consistent": 0, "idempotent": 0,
              "exactly once": 0, "resumable": 0}
    resumable = {"correct": 1, "consistent": 2, "idempotent": 1,
                 "exactly once": 1, "resumable": 1}
    assert g.strongest([proved, resumable]) == g.vector(proved)
    keep, out = g.reaching(["p", "r"], {"p": proved, "r": resumable}.get)
    assert keep == ["p"] and out[0][0] == "r"
    assert "correct" in out[0][1]
