"""Every way of a strategy is weighed (backlog P0-SMART, the owner's note A:
"compute every way").

`decide.space` walks every combination of the parts that fits, keeps the
ones reaching the strongest guarantees any reaches, costs each on the
unit and ranks them; a subtree is left out only where a bound proves
nothing in it can be taken, and it says how many were weighed, left out
and why. It agrees with `compose` where nothing says a guarantee, and
the listeners hear the strategy taken. No server.
"""
import itertools

from hypothesis import given, settings
from hypothesis import strategies as st

from migkit import decide as d
from migkit import guarantees as g

R = d.Rung


def _parts():
    same = d.Need("the same build on both sides", lambda f: f["same build"])
    return [
        d.Part("write", (R("bulk", "a bulk program", startup=1.0),
                         R("ranges", "ranges by key",
                           gives=frozenset({"resume by key"})))),
        d.Part("verify", (R("none", "no proof"),
                          R("hash", "the server's own row hash",
                            needs=(same,), holds={"correct": "proved"}),
                          R("digest", "a digest of every row",
                            holds={"correct": "proved"}))),
        d.Part("read", (R("snap", "one snapshot",
                          gives=frozenset({"consistent as of"})),
                        R("live", "read live"))),
    ]


def test_every_combination_is_weighed_and_counted():
    sp = d.space("public.t", _parts(), {"same build": True})
    c = sp.counts
    assert c["combinations"] == 2 * 3 * 2
    assert c["evaluated"] + c["bounded"] == c["combinations"]
    assert sp.best.names == {"write": "ranges", "verify": "hash",
                             "read": "snap"}
    assert sp.best.guarantees["correct"] == "proved"
    assert sp.best.guarantees["consistent"] == "as of one moment"
    assert sp.best.guarantees["resumable"] == "goes on"
    assert sp.strongest == sp.best.guarantees
    line = sp.said()
    assert line.startswith("public.t: 12 combinations")


def test_a_faster_weaker_strategy_is_left_out_with_why():
    c = d.Costs({"write/bulk": 1.0, "write/ranges": 9.0,
                 "verify/none": 0.0, "verify/hash": 1.0,
                 "verify/digest": 2.0, "read/snap": 1.0, "read/live": 0.1})
    sp = d.space("public.t", _parts(), {"same build": True}, costs=c)
    assert sp.best.names == {"write": "ranges", "verify": "hash",
                             "read": "snap"}
    assert sp.best.reasons["write"].startswith(
        "a bulk program gives starts over where goes on is reachable")
    assert sp.best.reasons["verify"].startswith("no proof gives unproved")
    # the runner-up reaches the same, costs more, and says so
    runner, why = sp.best.runners[0]
    assert runner.names["verify"] == "digest"
    assert why.startswith("measured")
    assert sp.best.estimate == 11.0


def test_where_nothing_says_a_guarantee_space_agrees_with_compose():
    parts = [d.Part("a", (R("a1", "a one"), R("a2", "a two"))),
             d.Part("b", (R("b1", "b one"), R("b2", "b two")))]
    c = d.Costs({"a/a1": 3.0, "a/a2": 1.0, "b/b1": 1.0, "b/b2": 2.0})
    assert d.space("t", parts, costs=c).best.names == \
        d.compose("t", parts, costs=c).names == {"a": "a2", "b": "b1"}
    assert d.space("t", parts).best.names == d.compose("t", parts).names


@settings(max_examples=60, deadline=None)
@given(st.lists(st.lists(st.tuples(
    st.sampled_from([None, "counted", "proved"]),
    st.sampled_from([None, "starts over", "goes on"]),
    st.floats(0.1, 10.0)), min_size=1, max_size=3), min_size=1, max_size=3))
def test_the_one_taken_is_the_cheapest_of_the_strongest(spec):
    """Against brute force: every combination, its guarantees and its
    cost - the space takes the cheapest (within the tie) of those whose
    guarantees are the strongest, whatever the bounds left out."""
    parts, costs = [], {}
    for i, rungs in enumerate(spec):
        rs = []
        for j, (correct, resumable, secs) in enumerate(rungs):
            holds = {k: v for k, v in (("correct", correct),
                                       ("resumable", resumable)) if v}
            rs.append(R(f"r{j}", f"way {i}.{j}", holds=holds))
            costs[f"p{i}/r{j}"] = secs
        parts.append(d.Part(f"p{i}", tuple(rs)))
    sp = d.space("t", parts, costs=d.Costs(costs))
    combos = list(itertools.product(*[p.rungs for p in parts]))
    best = max(g.vector(g.of(c)) for c in combos)
    top = [c for c in combos if g.vector(g.of(c)) == best]
    cheapest = min(sum(costs[f"{p.name}/{r.name}"] for p, r in
                       zip(parts, c)) for c in top)
    got = sp.best
    assert g.vector({k: g.level(k, v) for k, v in got.guarantees.items()}) \
        == best
    assert got.estimate <= cheapest * (1 + d.TIE) + 1e-9


def test_a_part_with_no_way_says_why():
    parts = [d.Part("a", (R("a1", "a one", needs=(
        d.Need("a grant", lambda f: False),)),))]
    sp = d.space("t", parts)
    assert not sp.best and sp.best.why_not == "a one needs a grant"


def test_the_listeners_hear_the_strategy_taken():
    heard = []
    d.LISTENERS.append(heard.append)
    try:
        sp = d.space("public.t", _parts(), {"same build": False})
    finally:
        d.LISTENERS.remove(heard.append)
    assert heard == [sp.best]
    assert sp.best.names["verify"] == "digest"


def test_the_walk_is_kept_for_the_same_parts():
    parts = _parts()
    d._SPACES.clear()
    d.space("t", parts, {"same build": True})
    n = len(d._SPACES)
    d.space("t", parts, {"same build": True})
    assert len(d._SPACES) == n == 1
    # another fact, another ladder: walked again
    d.space("t", parts, {"same build": False})
    assert len(d._SPACES) == 2


def test_the_evidence_names_no_label():
    sp = d.space("public.t", _parts(), {"same build": True})
    text = str(sp.as_evidence())
    assert "a digest of every row" in text or "the server's own row hash" \
        in text
    for label in ("'bulk'", "'ranges'", "'hash'", "'snap'"):
        assert label not in text, label
