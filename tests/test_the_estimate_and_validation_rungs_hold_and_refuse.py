"""The rungs an estimate stands on and a standalone validation is chosen
by, each reached on its branches on fakes (backlog AUD-Z.rungs-outside):
an estimate on this run's units once the warm-up is measured, an earlier
run's rate before, nothing said where neither is; a validation by the
servers' own digests, by bisection, by a second reader only where each is
there, and by row hashes and counts always."""
from migkit import decide, util, verifiers
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("util", "_estimate_rungs", "eta.window", "holds"),
    ("util", "_estimate_rungs", "eta.window", "refuses"),
    ("util", "_estimate_rungs", "eta.kept", "holds"),
    ("util", "_estimate_rungs", "eta.kept", "refuses"),
    ("util", "_estimate_rungs", "eta.none", "holds"),
    ("verifiers", "validation_ladder", "digest", "holds"),
    ("verifiers", "validation_ladder", "digest", "refuses"),
    ("verifiers", "validation_ladder", "bisection", "holds"),
    ("verifiers", "validation_ladder", "bisection", "refuses"),
    ("verifiers", "validation_ladder", "second_reader", "holds"),
    ("verifiers", "validation_ladder", "second_reader", "refuses"),
    ("verifiers", "validation_ladder", "row_hash", "holds"),
    ("verifiers", "validation_ladder", "count", "holds"),
]


def _eta(measured, kept):
    facts = decide.Facts({"measured": measured, "warmup": 3},
                         kept_rate=lambda: kept)
    with watched() as seen:
        got = decide.climb("when the copy ends", util._estimate_rungs(),
                           facts=facts)
    return got, seen


def test_a_warmed_up_run_estimates_on_its_own_units():
    got, seen = _eta(5, None)
    assert got.rung.name == "eta.window"
    assert branch(seen, "eta.window") == [HOLDS]
    assert branch(seen, "eta.kept") == [REFUSES]
    assert why(seen, "eta.kept") == "this run has measured enough of its own"
    assert branch(seen, "eta.none") == [HOLDS]


def test_before_the_warm_up_an_earlier_runs_rate_is_taken():
    got, seen = _eta(1, 2.5)
    assert got.rung.name == "eta.kept"
    assert why(seen, "eta.window") == "1 of 3 units measured here so far"
    assert branch(seen, "eta.kept") == [HOLDS]


def test_with_no_earlier_rate_nothing_is_said_and_both_say_why():
    got, seen = _eta(0, None)
    assert got.rung.name == "eta.none"
    assert why(seen, "eta.kept") == "no earlier run of this hop kept a rate"
    assert branch(seen, "eta.window") == [REFUSES]


THERE = {"can_digest": True, "reladiff_installed": True, "key": True,
         "dvt_installed": True}
NAMES = ("digest", "bisection", "second_reader", "row_hash", "count")


def test_every_validation_there_holds_and_the_digest_is_first():
    with watched() as seen:
        got = verifiers.plan_validation("app.t", THERE)
    assert got.rung.name == "digest"
    for name in NAMES:
        assert branch(seen, name) == [HOLDS], name


def test_what_is_not_there_is_refused_with_its_own_reason(monkeypatch):
    monkeypatch.setattr(util, "which", lambda name: None)
    with watched() as seen:
        got = verifiers.plan_validation("app.t", {
            "can_digest": False, "reladiff_installed": False, "key": True,
            "dvt_installed": False})
    assert got.rung.name in ("row_hash", "count")
    assert why(seen, "digest") == ("server does not support in-database"
                                   " digest hash")
    assert why(seen, "bisection") == "bisecting comparison tool not installed"
    assert why(seen, "second_reader") == ("independent second reader not"
                                          " installed")
    for name in ("digest", "bisection", "second_reader"):
        assert branch(seen, name) == [REFUSES], name
    assert branch(seen, "row_hash") == [HOLDS]
    assert branch(seen, "count") == [HOLDS]


def test_a_table_with_no_key_is_not_bisected():
    with watched() as seen:
        got = verifiers.plan_validation("app.t", dict(
            THERE, can_digest=False, key=False))
    assert got.rung.name == "second_reader"
    assert why(seen, "bisection") == "bisection requires key columns"
