"""The seed's three carriers - a volume snapshot's changed blocks, a seed
shipped on media, the copy over the link - each held and refused on the
facts that decide it (backlog AUD-Z.rungs-owed): blocks only from a
snapshot whose parent the target holds and within the deadline, media
only where the hop names a carrier and within it, the link only within
it. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("SEED_BLOCKS", "blocks", "holds"), ("SEED_BLOCKS", "blocks", "refuses"),
    ("SEED_MEDIA", "media", "holds"), ("SEED_MEDIA", "media", "refuses"),
    ("SEED_NETWORK", "network", "holds"),
    ("SEED_NETWORK", "network", "refuses"),
]


def test_changed_blocks_go_first_where_the_target_holds_the_parent():
    with watched() as seen:
        got = decide.seed_carrier({"blocks ok": True, "blocks": 10,
                                   "ship": True, "media": 50,
                                   "network": 500, "deadline": 100})
    assert got.path == "blocks"
    assert branch(seen, "blocks") == [HOLDS]
    assert branch(seen, "media") == [HOLDS]
    assert branch(seen, "network") == [REFUSES]
    assert "past the deadline" in why(seen, "network")


def test_no_parent_snapshot_and_no_carrier_leave_the_link():
    with watched() as seen:
        got = decide.seed_carrier({
            "blocks ok": "the target holds no parent snapshot",
            "ship": False, "network": 20, "deadline": 100})
    assert got.path == "network"
    assert why(seen, "blocks") == "the target holds no parent snapshot"
    assert branch(seen, "media") == [REFUSES]
    assert "names no carrier" in why(seen, "media")
    assert branch(seen, "network") == [HOLDS]


def test_blocks_and_media_past_the_deadline_are_refused():
    with watched() as seen:
        got = decide.seed_carrier({"blocks ok": True, "blocks": 500,
                                   "ship": True, "media": 400,
                                   "network": 50, "deadline": 100})
    assert got.path == "network"
    assert branch(seen, "blocks") == [REFUSES]
    assert "past the deadline of 100s" in why(seen, "blocks")
    assert branch(seen, "media") == [REFUSES]
    assert "past the deadline" in why(seen, "media")
