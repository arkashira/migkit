"""The rungs the guide's goal, the readiness check's depth, the copier a
standby holds and the planner's ways are chosen by, each reached on the
branches it has on fakes (backlog AUD-Z.rungs-outside-owed): a goal the
facts do not point to is refused with its own need, a full check the
servers do not answer falls to the hop's configuration and says why, and
a rung with no need is held wherever it is offered."""
from types import SimpleNamespace

from migkit import decide, guide, ha, planner, ready
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('guide', 'propose', 'copy', 'holds'),
    ('guide', 'propose', 'cutover', 'holds'),
    ('guide', 'propose', 'cutover', 'refuses'),
    ('guide', 'propose', 'verify', 'holds'),
    ('guide', 'propose', 'verify', 'refuses'),
    ('ha', '_copiers', '<own>', 'holds'),
    ('ha', '_copiers', 'builtin', 'holds'),
    ('planner', 'overlap', '<w>', 'holds'),
    ('planner', 'ways', '<BULK>', 'holds'),
    ('planner', 'ways', '<COPIER>', 'holds'),
    ('planner', 'ways', '<LEFT>', 'holds'),
    ('ready', 'depth', 'full', 'holds'),
    ('ready', 'depth', 'full', 'refuses'),
    ('ready', 'depth', 'quick', 'holds'),
]


def _found(on_target, follows):
    return {"sides": {"target": {"facts": {
                "public.orders": {"rows": 3} if on_target else {}}}},
            "tables": ["public.orders"],
            "needs": [SimpleNamespace(side="source", follow=True,
                                      ok=follows)]}


def test_the_guide_proves_what_the_target_holds_and_follows_what_it_can():
    with watched() as seen:
        assert guide.propose(_found(True, True)) == "verify"
        assert guide.propose(_found(False, False)) == "copy"
    assert branch(seen, "verify") == [HOLDS, REFUSES]
    assert branch(seen, "cutover") == [HOLDS, REFUSES]
    assert branch(seen, "copy") == [HOLDS]
    assert "target already holds tables" in why(seen, "verify")
    assert "source changes can be followed" in why(seen, "cutover")


def test_a_target_without_the_tables_is_cut_over_where_changes_follow():
    with watched() as seen:
        assert guide.propose(_found(False, True)) == "cutover"
    assert branch(seen, "verify") == [REFUSES]
    assert branch(seen, "cutover") == [HOLDS]


def test_the_full_check_is_held_only_where_the_servers_answer(monkeypatch):
    hop = SimpleNamespace(name="app")
    monkeypatch.setattr(ready, "_assessed", lambda h, e, m: [("ok", 1)])
    with watched() as seen:
        quick, rows, _ = ready.depth(hop, None)
    assert quick is False and rows == [("ok", 1)]

    def down(h, e, m):
        raise ConnectionError("connection refused by 10.0.0.1")
    monkeypatch.setattr(ready, "_assessed", down)
    with watched() as more:
        quick, rows, reason = ready.depth(hop, None)
    seen += more
    assert quick is True and rows is None
    assert branch(seen, "full") == [HOLDS, REFUSES]
    assert branch(seen, "quick") == [HOLDS]
    assert why(seen, "full").startswith("the servers did not answer: ")
    assert "connection refused" in why(seen, "full")


def test_the_standbys_copier_is_the_one_holding_every_write():
    ladder = ha._copiers("own", "the engine's own copier")
    with watched() as seen:
        kept, out = decide.prune(ladder, {})
        held, lacking = decide.prune(ladder, {}, need=(ha.HELD,))
    assert [r.name for r in kept] == ["own", "builtin"] and not out
    assert [r.name for r in held] == ["builtin"]
    assert [r.name for r, _ in lacking] == ["own"]
    assert branch(seen, "own")[0] == HOLDS
    assert branch(seen, "builtin") == [HOLDS]


def test_a_table_takes_the_bulk_copy_the_copier_or_is_left():
    ladder = planner.ways("builtin")
    with watched() as seen:
        kept, out = decide.prune(ladder, {})
    assert [r.name for r in kept] == [planner.BULK, planner.COPIER,
                                      planner.LEFT] and not out
    for name in (planner.BULK, planner.COPIER, planner.LEFT):
        assert branch(seen, name) == [HOLDS]
    with watched() as seen:
        kept, _ = decide.prune(ladder, {}, need=(planner.LEFT,))
    assert [r.name for r in kept] == [planner.LEFT]


def test_every_overlap_of_the_phases_is_offered_and_one_taken():
    with watched() as seen:
        got = planner.overlap(None, {"orders": 2.0, "lines": 1.0},
                              build={"orders": 0.5}, check={"lines": 0.2},
                              at_once=2)
    assert got.way in ("free", "waves", "serial")
    for w in ("free", "waves", "serial"):
        assert branch(seen, w) == [HOLDS]
