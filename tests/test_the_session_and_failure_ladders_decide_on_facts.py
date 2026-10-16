"""The ladders that size a wait, act on a session the server was asked
about, recover from a failure, bound a stop, cut change windows, size a
copy's workers and time a token's renewal each held and refused on the
facts that decide them (backlog AUD-Z.rungs-owed): a way is taken only
where what it stands on was measured or read, and where it was not the
next way says why. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("session_patience", "measured", "holds"),
    ("session_patience", "measured", "refuses"),
    ("session_patience", "predicted", "holds"),
    ("session_patience", "predicted", "refuses"),
    ("session_patience", "deadline", "holds"),
    ("session_patience", "deadline", "refuses"),
    ("session_action", "close", "holds"),
    ("session_action", "close", "refuses"),
    ("session_action", "abandon", "holds"),
    ("session_action", "abandon", "refuses"),
    ("session_action", "report", "holds"),
    ("recovery", "replan", "holds"),
    ("recovery", "replan", "refuses"),
    ("recovery", "probe", "holds"),
    ("recovery", "probe", "refuses"),
    ("stop_grace", "programs", "holds"),
    ("stop_grace", "programs", "refuses"),
    ("stop_grace", "statement", "holds"),
    ("window_rungs", "<name>", "holds"),
    ("window_rungs", "<name>", "refuses"),
    ("worker_budget", "the flow's budget", "holds"),
    ("worker_budget", "the flow's budget", "refuses"),
    ("worker_budget", "the measured start", "holds"),
    ("worker_budget", "the measured start", "refuses"),
    ("worker_budget", "the measured carry", "holds"),
    ("worker_budget", "the measured carry", "refuses"),
    ("worker_budget", "one at a time", "holds"),
    ("token_renew", "measured", "holds"),
    ("token_renew", "measured", "refuses"),
    ("token_renew", "third", "holds"),
]


def test_a_wait_is_measured_then_predicted_then_the_deadline():
    with watched() as seen:
        seconds, got = decide.session_patience([2.0, 3.0, 9.0], predicted=5,
                                               deadline=30)
    assert got.path == "measured"
    # the longest wait and its spread above the middle: 9 + (9 - 3)
    assert seconds == 15.0
    for name in ("measured", "predicted", "deadline"):
        assert branch(seen, name) == [HOLDS]
    with watched() as seen:
        seconds, got = decide.session_patience([4.0], predicted=None,
                                               deadline=None)
    assert got.path is None and seconds is None
    assert "fewer than two waits" in why(seen, "measured")
    assert why(seen, "predicted") == "no wait was predicted for the unit"
    assert "read deadline was not read" in why(seen, "deadline")


def test_a_prediction_stands_where_too_few_waits_were_measured():
    with watched() as seen:
        seconds, got = decide.session_patience([1.0], predicted=7.5,
                                               deadline=30)
    assert (got.path, seconds) == ("predicted", 7.5)
    assert branch(seen, "measured") == [REFUSES]
    with watched() as seen:
        seconds, got = decide.session_patience((), deadline=20)
    assert (got.path, seconds) == ("deadline", 20.0)
    assert branch(seen, "predicted") == [REFUSES]


def _session(**over):
    facts = {"kind": "idle", "idle_seconds": 120, "patience": 30,
             "idle_reads": 2, "reads": True, "owned": True,
             "unprogressed": False, "can_abandon": True}
    facts.update(over)
    return facts


def test_an_idle_owned_read_is_closed_and_a_gone_one_abandoned():
    with watched() as seen:
        got = decide.session_action(_session())
    assert got.path == "close"
    assert branch(seen, "close") == [HOLDS]
    assert why(seen, "abandon") == "the server still has the session"
    assert branch(seen, "report") == [HOLDS]
    with watched() as seen:
        got = decide.session_action(_session(kind="gone"))
    assert got.path == "abandon"
    assert branch(seen, "abandon") == [HOLDS]
    assert why(seen, "close") == "the server is not idle on it"


def test_a_write_or_a_retry_without_progress_is_only_reported():
    with watched() as seen:
        got = decide.session_action(_session(reads=False))
    assert got.path == "report"
    assert "a write closed under it is left uncertain" in why(seen, "close")
    with watched() as seen:
        got = decide.session_action(_session(idle_seconds=10))
    assert "within the 30s measured" in why(seen, "close")
    with watched() as seen:
        got = decide.session_action(_session(kind="gone",
                                             unprogressed=True))
    assert got.path == "report"
    assert "retry made no progress" in why(seen, "abandon")


def test_a_failure_replans_or_probes_only_where_its_class_admits_it():
    with watched() as seen:
        got = decide.recovery({"class": "plan", "admits": ("replan",)})
    assert got.path == "replan"
    assert branch(seen, "replan") == [HOLDS]
    assert why(seen, "probe") == "a plan failure is not recovered by probe"
    with watched() as seen:
        got = decide.recovery({"class": "evidence", "admits": ("probe",)})
    assert got.path == "probe"
    assert branch(seen, "probe") == [HOLDS]
    assert why(seen, "replan") == ("a evidence failure is not recovered by"
                                   " replan")
    with watched() as seen:
        got = decide.recovery({"class": "unknown", "admits": ()})
    assert got.path == "stop"


def test_a_stop_waits_on_its_programs_or_its_statement():
    with watched() as seen:
        seconds, got = decide.stop_grace({"programs": 2, "grace": 5})
    assert got.path == "programs" and seconds == 10.0
    assert branch(seen, "programs") == [HOLDS]
    with watched() as seen:
        seconds, got = decide.stop_grace({"programs": 0, "grace": 5,
                                          "slowest": 8})
    assert got.path == "statement"
    # the statement measured slower than the floor is given twice its time
    assert seconds == 16.0
    assert branch(seen, "statement") == [HOLDS]
    assert why(seen, "programs").startswith("no program is running")
    assert "twice its time" in why(seen, "programs")


def test_a_window_narrower_or_wider_is_proved_only_on_its_rate_and_span():
    widths, rungs = decide.window_rungs(100, 10)
    assert widths["window x1"] == 1.0
    with watched() as seen:
        decide.climb("windows", rungs, decide.Facts({"rate": 10, "span": 5}),
                     prove=False)
    for name in widths:
        assert branch(seen, name) == [HOLDS], name
    with watched() as seen:
        decide.climb("windows", rungs, decide.Facts({"rate": 1, "span": 3}),
                     prove=False)
    assert branch(seen, "window x1") == [HOLDS]
    assert branch(seen, "window x2") == [HOLDS]
    assert "fewer than one change a window" in why(seen, "window x0.5")
    assert "spans less than one window" in why(seen, "window x4")


def test_a_copys_workers_come_from_the_first_measure_there_is():
    with watched() as seen:
        n, got = decide.worker_budget({"budget": 6, "start": 3, "little": 2,
                                       "most": 4})
    assert (got.path, n) == ("the flow's budget", 4)
    for name in ("the flow's budget", "the measured start",
                 "the measured carry", "one at a time"):
        assert branch(seen, name) == [HOLDS]
    with watched() as seen:
        n, got = decide.worker_budget({"start": 3, "little": 2})
    assert (got.path, n) == ("the measured start", 3)
    assert why(seen, "the flow's budget") == "no flow handed a budget down"
    with watched() as seen:
        n, got = decide.worker_budget({"little": 5, "scale": 0.4})
    assert (got.path, n) == ("the measured carry", 2)
    assert why(seen, "the measured start") == "nothing sized the start"
    with watched() as seen:
        n, got = decide.worker_budget({})
    assert (got.path, n) == ("one at a time", 1)
    assert why(seen, "the measured carry") == "the stages were not measured"


def test_a_slow_mint_is_renewed_twice_its_time_early():
    with watched() as seen:
        seconds, got = decide.token_renew(3600, mints=(700.0, 20.0))
    assert (got.path, seconds) == ("measured", 1400.0)
    assert branch(seen, "measured") == [HOLDS]
    with watched() as seen:
        seconds, got = decide.token_renew(3600, mints=(2.0,))
    assert (got.path, seconds) == ("third", 1200.0)
    assert branch(seen, "third") == [HOLDS]
    assert why(seen, "measured") == "minting is quick next to the lifetime"
