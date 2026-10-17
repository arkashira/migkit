"""The rungs `decide.retention_action` chooses by (backlog RCH16, row 624),
each held where its facts allow it and refused with its own reason."""

from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ('retention_action', 'stop_and_reseed', 'holds'),
    ('retention_action', 'stop_and_reseed', 'refuses'),
    ('retention_action', 'expand_capacity', 'holds'),
    ('retention_action', 'expand_capacity', 'refuses'),
    ('retention_action', 'throttle_safe_stages', 'holds'),
    ('retention_action', 'throttle_safe_stages', 'refuses'),
    ('retention_action', 'speed_copy', 'holds'),
    ('retention_action', 'speed_copy', 'refuses'),
    ('retention_action', 'steady', 'holds'),
    ('query_guard_threshold_rungs', 'strict', 'holds'),
    ('query_guard_threshold_rungs', 'strict', 'refuses'),
    ('query_guard_threshold_rungs', 'balanced', 'holds'),
    ('query_guard_threshold_rungs', 'balanced', 'refuses'),
    ('query_guard_threshold_rungs', 'permissive', 'holds'),
    ('query_guard_threshold_rungs', 'permissive', 'refuses'),
]

CASES = {
    "stop_and_reseed": {"stop_required": True},
    "expand_capacity": {"expansion_available": 10, "drain_needed": True},
    "throttle_safe_stages": {"throttle_safe": True, "drain_needed": True},
    "speed_copy": {"can_speed": True, "drain_needed": True},
}


def test_each_action_is_held_where_its_facts_allow_it_and_refused_otherwise():
    with watched() as seen:
        for name, facts in CASES.items():
            assert decide.retention_action(facts)[0] == name
        assert decide.retention_action({})[0] == "steady"
    for name in CASES:
        assert set(branch(seen, name)) == {HOLDS, REFUSES}, name
    assert branch(seen, "steady") == [HOLDS]


def test_a_drain_is_not_asked_for_without_a_need():
    with watched() as seen:
        assert decide.retention_action({"can_speed": True})[0] == "steady"
    assert "no drain is needed" in why(seen, "speed_copy")


def test_stop_comes_before_every_other_action():
    every = {k: v for f in CASES.values() for k, v in f.items()}
    assert decide.retention_action(every)[0] == "stop_and_reseed"


def test_the_query_guard_threshold_rung_is_held_by_its_profile_and_refused_otherwise():
    with watched() as seen:
        for profile, want in (("strict", (1.25, 100)), (None, (2.0, 50)),
                              ("permissive", (3.5, 20))):
            got = decide.query_guard_thresholds(options={"profile": profile})
            assert (got.latency_threshold, got.sample_size) == want
    for name in ("strict", "balanced", "permissive"):
        assert set(branch(seen, name)) == {HOLDS, REFUSES}, name
