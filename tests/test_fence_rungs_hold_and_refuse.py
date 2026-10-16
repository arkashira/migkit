"""The fence ladders' rungs each held and refused on the facts that decide
them (backlog AUD-Z.rungs): a target holds the mover's number only by a
compare it speaks and the account may use, the record goes where the
account may make it, and where neither holds no fence is assumed - the
choice is none, with the statement that would let it. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("fence_way", "compared-key", "holds"),
    ("fence_way", "compared-key", "refuses"),
    ("fence_way", "producer-epoch", "holds"),
    ("fence_way", "producer-epoch", "refuses"),
    ("fence_way", "replacing-row", "holds"),
    ("fence_way", "replacing-row", "refuses"),
    ("fence_way", "guarded-document", "holds"),
    ("fence_way", "guarded-document", "refuses"),
    ("fence_way", "lightweight-transaction", "holds"),
    ("fence_way", "lightweight-transaction", "refuses"),
    ("fence_place", "in", "holds"), ("fence_place", "in", "refuses"),
    ("fence_place", "beside", "holds"), ("fence_place", "beside", "refuses"),
]


def test_a_script_compare_holds_only_where_scripts_may_run():
    with watched() as seen:
        got = decide.fence_way({"ways": ("compared-key",), "scripts": True})
    assert got.path == "compared-key"
    assert branch(seen, "compared-key") == [HOLDS]
    with watched() as seen:
        got = decide.fence_way({"ways": ("compared-key",), "scripts": False})
    assert branch(seen, "compared-key") == [REFUSES]
    assert "may not run scripts" in why(seen, "compared-key")
    # nothing weaker is offered in its place: no fence is assumed
    assert got.path is None
    assert branch(seen, "replacing-row") == [REFUSES]


def test_a_script_refused_falls_to_the_weaker_hold_and_says_it():
    with watched() as seen:
        got = decide.fence_way({"ways": ("compared-key", "replacing-row"),
                                "scripts": False})
    assert branch(seen, "replacing-row") == [HOLDS]
    assert got.path == "replacing-row"
    assert "weaker" in got.said


def test_a_producer_epoch_needs_transactional_producers():
    with watched() as seen:
        got = decide.fence_way({"ways": ("producer-epoch",),
                                "transactions": True})
    assert got.path == "producer-epoch"
    assert branch(seen, "producer-epoch") == [HOLDS]
    with watched() as seen:
        got = decide.fence_way({"ways": ("producer-epoch",),
                                "transactions": False})
    assert got.path is None
    assert branch(seen, "producer-epoch") == [REFUSES]
    assert "no transactional producer" in why(seen, "producer-epoch")


def test_the_record_goes_in_the_target_where_the_account_may_make_it():
    with watched() as seen:
        got = decide.fence_place({"create_in": True, "create_beside": True})
    assert got.path == "in"
    assert branch(seen, "in") == [HOLDS]


def test_the_record_goes_beside_where_the_target_refuses_it():
    with watched() as seen:
        got = decide.fence_place({"create_in": False,
                                  "create_beside": True,
                                  "grant_in": "GRANT CREATE ON db"})
    assert got.path == "beside"
    assert branch(seen, "in") == [REFUSES]
    assert "GRANT CREATE ON db" in why(seen, "in")
    assert branch(seen, "beside") == [HOLDS]


def test_a_target_refusing_both_places_holds_no_number():
    with watched() as seen:
        got = decide.fence_place({"grant_in": "GRANT CREATE ON db"})
    assert got.path is None
    assert branch(seen, "in") == [REFUSES]
    assert branch(seen, "beside") == [REFUSES]
    # the statement is said for the place beside too
    assert "GRANT CREATE ON db" in why(seen, "beside")


def test_a_compare_the_target_does_not_speak_is_never_taken():
    with watched() as seen:
        got = decide.fence_way({"ways": ("guarded-document",)})
    assert got.path == "guarded-document"
    assert branch(seen, "guarded-document") == [HOLDS]
    assert why(seen, "lightweight-transaction") == ("the target has no"
                                                    " lightweight-transaction")
    with watched() as seen:
        got = decide.fence_way({"ways": ("lightweight-transaction",)})
    assert got.path == "lightweight-transaction"
    assert branch(seen, "lightweight-transaction") == [HOLDS]
    assert branch(seen, "guarded-document") == [REFUSES]
    with watched() as seen:
        got = decide.fence_way({})
    assert got.path is None
    assert {branch(seen, r)[0] for r in ("guarded-document", "compared-key",
            "lightweight-transaction", "producer-epoch",
            "replacing-row")} == {REFUSES}
