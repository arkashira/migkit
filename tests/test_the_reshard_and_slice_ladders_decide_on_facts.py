"""The reshard follow's read size and order, and a consolidation's slice
recheck, each held and refused on what was measured (backlog
AUD-Z.rungs-owed): a source is read twice as much only while it is behind
and a larger read gains, paced only off a full read timed, read behind
first only where the sources are timed and skewed, and a slice compared
again only while its differences fall past a follow. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("reshard_read", "grow", "holds"),
    ("reshard_read", "grow", "refuses"),
    ("reshard_read", "paced", "holds"),
    ("reshard_read", "paced", "refuses"),
    ("reshard_read", "as-asked", "holds"),
    ("reshard_order", "most-behind-first", "holds"),
    ("reshard_order", "most-behind-first", "refuses"),
    ("reshard_order", "as-named", "holds"),
    ("slice_recheck", "recheck", "holds"),
    ("slice_recheck", "recheck", "refuses"),
    ("slice_recheck", "settle", "holds"),
]


def test_a_source_behind_and_gaining_is_read_twice_as_much():
    with watched() as seen:
        got, size = decide.reshard_read({"rate": 900, "before": 600,
                                         "round": 2, "last": 500,
                                         "full": True})
    assert (got.path, size) == ("grow", 1000)
    assert branch(seen, "grow") == [HOLDS]
    assert branch(seen, "paced") == [HOLDS]
    assert branch(seen, "as-asked") == [HOLDS]


def test_a_read_that_gains_nothing_is_paced_and_a_short_one_kept():
    with watched() as seen:
        got, size = decide.reshard_read({"rate": 500, "before": 600,
                                         "round": 3, "last": 1000,
                                         "full": True})
    assert (got.path, size) == ("paced", 1500)
    assert why(seen, "grow") == ("the last read applied 500 changes a"
                                 " second, no more than the 600 of the"
                                 " smaller one before it")
    with watched() as seen:
        got, size = decide.reshard_read({"rate": 500, "round": 3,
                                         "last": 250, "full": False})
    assert (got.path, size) == ("as-asked", 250)
    assert "came back short" in why(seen, "grow")
    assert why(seen, "paced") == ("no full read of the source has been"
                                  " applied and timed yet")


def test_the_sources_behind_and_slowest_are_read_first_once_timed():
    with watched() as seen:
        got, order = decide.reshard_order({"took": [1.0, 4.0, 2.0],
                                           "full": [False, True, True]})
    assert got.path == "most-behind-first"
    assert order == [1, 2, 0]
    assert branch(seen, "as-named") == [HOLDS]
    with watched() as seen:
        got, order = decide.reshard_order({"took": [1.0, None],
                                           "full": [True, True]})
    assert (got.path, order) == ("as-named", [0, 1])
    assert why(seen, "most-behind-first") == ("a source's batch has not been"
                                              " timed yet")
    with watched() as seen:
        got, _ = decide.reshard_order({"took": [2.0, 2.0],
                                       "full": [True, True]})
    assert got.path == "as-named"
    assert branch(seen, "most-behind-first") == [REFUSES]
    assert "as far behind as the others" in why(seen, "most-behind-first")


def test_a_slice_is_compared_again_only_while_its_differences_fall():
    with watched() as seen:
        got = decide.slice_recheck({"unsettled": 3, "catch_up": True,
                                    "before": 8})
    assert got.path == "recheck"
    assert branch(seen, "settle") == [HOLDS]
    with watched() as seen:
        got = decide.slice_recheck({"unsettled": 8, "catch_up": True,
                                    "before": 8})
    assert got.path == "settle"
    assert "writes outrun the follow" in why(seen, "recheck")
    with watched() as seen:
        got = decide.slice_recheck({"unsettled": 3, "catch_up": False,
                                    "before": None})
    assert got.path == "settle"
    assert "no follow is there" in why(seen, "recheck")
    with watched() as seen:
        got = decide.slice_recheck({"unsettled": 0, "catch_up": True,
                                    "before": None})
    assert got.path == "settle"
    assert branch(seen, "recheck") == [REFUSES]
