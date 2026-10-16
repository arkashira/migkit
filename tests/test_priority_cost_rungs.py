"""The rung each item of `decide.rank_priority` is (`_priority_cost`) is
pruned before it is ranked (backlog AUD-Z.priced-rung): an item whose
prerequisite can neither come first nor is done, and one the window cannot
hold alone, are refused with why - kept in `refused`, a mandatory one in
`blocked`, never dropped silently - and what is held ranks as before."""
from migkit import decide as d
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("_priority_cost", "<item.name>", "holds"),
    ("_priority_cost", "<item.name>", "refuses"),
]


def test_an_item_whose_prerequisites_can_come_first_is_held():
    probe = d.PriorityItem("probe", cost=1.0)
    copy = d.PriorityItem("copy", cost=5.0, dependencies=("probe",))
    with watched() as seen:
        ranked = d.rank_priority([copy, probe])
    assert [i.name for i in ranked] == ["probe", "copy"]
    assert branch(seen, "copy") == [HOLDS]
    assert ranked.refused == [] and ranked.blocked == []


def test_a_prerequisite_neither_among_the_work_nor_done_refuses():
    verdict = d.PriorityItem("verdict", mandatory=True, risk="correctness",
                             dependencies=("copy",))
    with watched() as seen:
        ranked = d.rank_priority([verdict])
    assert list(ranked) == []
    assert branch(seen, "verdict") == [REFUSES]
    assert "waits on copy" in why(seen, "verdict")
    assert [i.name for i in ranked.blocked] == ["verdict"]
    assert "neither among the work" in ranked.blocked[0].why
    # said done, the same prerequisite holds
    with watched() as seen:
        ranked = d.rank_priority([verdict], done=("copy",))
    assert [i.name for i in ranked] == ["verdict"]
    assert branch(seen, "verdict") == [HOLDS]


def test_what_waits_on_a_refused_item_is_refused_after_it():
    """A refusal reaches what waits on it, and an irreversible item never
    runs where a mandatory check cannot."""
    check = d.PriorityItem("check", mandatory=True, dependencies=("gone",))
    after = d.PriorityItem("report", dependencies=("check",))
    drop = d.PriorityItem("drop_source", reversibility="irreversible",
                          risk="data_loss")
    free = d.PriorityItem("count", cost=1.0)
    with watched() as seen:
        ranked = d.rank_priority([check, after, drop, free])
    assert [i.name for i in ranked] == ["count"]
    assert {i.name for i in ranked.refused} == {"check", "report",
                                                 "drop_source"}
    assert "waits on check" in why(seen, "report")
    assert "waits on check" in why(seen, "drop_source")
    assert branch(seen, "count") == [HOLDS]


def test_an_item_the_window_cannot_hold_is_refused_a_required_one_held():
    costs = d.Costs()
    costs.saw("metrics", 1, 20.0)
    metrics = d.PriorityItem("metrics", cost=1.0)
    stats = d.PriorityItem("stats", cost=1.0)
    audit = d.PriorityItem("audit", mandatory=True, cost=30.0,
                           dependencies=("prep",))
    prep = d.PriorityItem("prep", cost=40.0)
    with watched() as seen:
        ranked = d.rank_priority([metrics, stats, audit, prep], max_cost=5.0,
                                 costs=costs)
    assert branch(seen, "metrics") == [REFUSES]
    assert "more than the whole window of 5 holds" in why(seen, "metrics")
    # what a mandatory item waits on is required: held past the window,
    # never dropped from under it
    names = [i.name for i in ranked]
    assert sorted(names) == ["audit", "prep", "stats"]
    assert names.index("prep") < names.index("audit")
    assert [i.name for i in ranked.refused] == ["metrics"]
    assert ranked.blocked == []
    # no window: nothing is refused for its cost
    with watched() as seen:
        assert "metrics" in [i.name for i in d.rank_priority(
            [metrics, stats], costs=costs)]
    assert branch(seen, "metrics") == [HOLDS]
