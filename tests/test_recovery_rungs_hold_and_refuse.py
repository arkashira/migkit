"""The recovery ladder's rungs each held and refused on the facts that
decide them (backlog AUD-Z.rungs): a write whose effect is not known is
read before anything repeats it, a committed one goes on, and a retry or a
throttle stands only on a known effect with tries left. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("recovery", "reconcile", "holds"), ("recovery", "reconcile", "refuses"),
    ("recovery", "proceed", "holds"), ("recovery", "proceed", "refuses"),
    ("recovery", "retry", "holds"), ("recovery", "retry", "refuses"),
    ("recovery", "throttle", "holds"), ("recovery", "throttle", "refuses"),
    ("recovery", "pause", "holds"), ("recovery", "pause", "refuses"),
    ("recovery", "isolate", "holds"), ("recovery", "isolate", "refuses"),
    ("recovery", "stop", "holds"),
]

ALL = ("pause", "throttle", "retry", "replan", "isolate", "probe")


def _ranked(got):
    return [r.name for r in got.ranked]


def test_an_effect_not_known_is_read_before_any_retry():
    with watched() as seen:
        got = decide.recovery({"class": "timeout", "admits": ALL,
                               "transient": True, "tries_left": 3,
                               "uncertain": True, "can_check": True})
    assert got.path == "reconcile"
    assert branch(seen, "reconcile") == [HOLDS]
    # the effect not known keeps both repeats out, whatever they admit
    assert branch(seen, "retry") == [REFUSES]
    assert branch(seen, "throttle") == [REFUSES]
    assert "not known" in why(seen, "retry")
    assert "retry" not in _ranked(got) and "throttle" not in _ranked(got)


def test_an_effect_nothing_can_read_stops_rather_than_repeat_it():
    with watched() as seen:
        got = decide.recovery({"class": "timeout", "admits": ("retry",),
                               "transient": True, "tries_left": 3,
                               "uncertain": True, "can_check": False})
    assert got.path == "stop"
    assert branch(seen, "reconcile") == [REFUSES]
    assert "nothing can read whether the write committed" \
        in why(seen, "reconcile")
    assert branch(seen, "retry") == [REFUSES]
    assert branch(seen, "stop") == [HOLDS]


def test_a_write_read_as_committed_goes_on_and_is_not_read_again():
    with watched() as seen:
        got = decide.recovery({"class": "timeout", "admits": ALL,
                               "transient": True, "tries_left": 3,
                               "uncertain": True, "can_check": True,
                               "reconciled": "committed"})
    assert got.path == "proceed"
    assert branch(seen, "proceed") == [HOLDS]
    assert why(seen, "reconcile") == "it was read already"


def test_a_write_read_as_nothing_is_retried_not_proceeded():
    with watched() as seen:
        got = decide.recovery({"class": "timeout", "admits": ("retry",),
                               "transient": True, "tries_left": 2,
                               "uncertain": True, "can_check": True,
                               "reconciled": "nothing"})
    assert branch(seen, "proceed") == [REFUSES]
    assert why(seen, "proceed") == ("the work is not known to have taken"
                                    " effect")
    assert branch(seen, "retry") == [HOLDS]
    assert got.path == "retry"


def test_a_retry_needs_a_fault_that_clears_and_a_try_left():
    with watched() as seen:
        spent = decide.recovery({"class": "timeout", "admits": ("retry",),
                                 "transient": True, "tries_left": 0})
    assert spent.path == "stop"
    assert why(seen, "retry") == "every try this work may make is spent"
    with watched() as seen:
        lasting = decide.recovery({"class": "constraint",
                                   "admits": ("retry",),
                                   "transient": False, "tries_left": 3})
    assert lasting.path == "stop"
    assert why(seen, "retry") == "the fault does not clear by itself"


def test_a_throttle_stands_on_its_admission_and_a_known_effect():
    with watched() as seen:
        got = decide.recovery({"class": "throttled",
                               "admits": ("throttle", "retry"),
                               "transient": True, "tries_left": 2})
    assert branch(seen, "throttle") == [HOLDS]
    assert got.path == "throttle"
    with watched() as seen:
        got = decide.recovery({"class": "constraint", "admits": ("probe",),
                               "transient": True, "tries_left": 2})
    assert branch(seen, "throttle") == [REFUSES]
    assert why(seen, "throttle") == ("a constraint failure is not recovered"
                                     " by throttle")
    assert got.path == "probe"


def test_a_pause_waits_only_on_what_a_wait_clears():
    with watched() as seen:
        got = decide.recovery({"class": "unavailable",
                               "admits": ("pause", "retry"),
                               "transient": True, "tries_left": 1,
                               "breaker": "open"})
    assert got.path == "pause"
    assert branch(seen, "pause") == [HOLDS]
    with watched() as seen:
        got = decide.recovery({"class": "expired_credential",
                               "admits": ("pause",), "tries_left": 1,
                               "renewable": False, "breaker": "closed"})
    assert branch(seen, "pause") == [REFUSES]
    assert why(seen, "pause") == "nothing that a wait alone clears"
    assert got.path == "stop"


def test_a_unit_is_set_aside_only_where_its_fault_admits_it():
    with watched() as seen:
        got = decide.recovery({"class": "bad_row", "admits": ("isolate",),
                               "tries_left": 0})
    assert got.path == "isolate"
    assert branch(seen, "isolate") == [HOLDS]
    with watched() as seen:
        got = decide.recovery({"class": "corruption", "admits": (),
                               "tries_left": 3})
    assert branch(seen, "isolate") == [REFUSES]
    assert why(seen, "isolate") == ("a corruption failure is not recovered"
                                    " by isolate")
    assert got.path == "stop"
