"""The rungs a table's engine is changed by, a table added to a running
tail gets its rows by, and a move's tables fall to the copier by, each
reached on its branches on fakes (backlog AUD-Z.rungs-outside)."""
import pytest

from migkit import decide, tableengine, tableway, tailtables
from tests.rung_branches import (HOLDS, LACKS, REFUSES, branch, watched,
                                 why)

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("tableengine", "ways", "definition", "holds"),
    ("tableengine", "ways", "definition", "refuses"),
    ("tableengine", "ways", "in place", "holds"),
    ("tableengine", "ways", "in place", "refuses"),
    ("tableengine", "access_method_ways", "in place", "holds"),
    ("tableengine", "access_method_ways", "in place", "refuses"),
    ("tailtables", "_add_ways", "from-removal", "holds"),
    ("tailtables", "_add_ways", "from-removal", "refuses"),
    ("tailtables", "_add_ways", "copy", "holds"),
    ("tableway", "batch_or_copier", "copier", "holds"),
]


def _climb(rungs, facts):
    with watched() as seen:
        got = decide.climb("w", rungs, facts)
    return got, seen


def test_a_table_migkit_makes_takes_its_engine_in_the_definition():
    got, seen = _climb(tableengine.ways(), {"there": False})
    assert got.rung.name == "definition"
    assert branch(seen, "in place") == [REFUSES]
    assert why(seen, "in place") == "the target has no table to convert"


def test_a_table_the_target_has_is_converted_in_place():
    got, seen = _climb(tableengine.ways(), {"there": True})
    assert got.rung.name == "in place"
    assert branch(seen, "definition") == [REFUSES]
    assert why(seen, "definition") == "the target has the table already"
    assert branch(seen, "in place") == [HOLDS]


def test_an_access_method_is_altered_only_where_the_server_can():
    got, seen = _climb(tableengine.access_method_ways(),
                       {"alter_access_method": True, "there": True})
    assert got.rung.name == "in place"
    assert branch(seen, "in place") == [HOLDS]
    got, seen = _climb(tableengine.access_method_ways(),
                       {"alter_access_method": False, "there": True})
    assert got.rung is None and branch(seen, "in place") == [REFUSES]
    assert "SET ACCESS METHOD support" in why(seen, "in place")
    got, seen = _climb(tableengine.access_method_ways(),
                       {"alter_access_method": True, "there": False})
    assert got.rung is None and branch(seen, "in place") == [REFUSES]


LEFT = {"at": "0/16B3748", "rereads": True, "digest": "9f2c",
        "same_shape": True, "lost": None}


def test_a_table_that_left_exactly_is_read_again_from_where_it_left():
    got, seen = _climb(tailtables._add_ways(), LEFT)
    assert got.rung.name == "from-removal"
    assert branch(seen, "from-removal") == [HOLDS]
    assert branch(seen, "copy") == [HOLDS]


@pytest.mark.parametrize("change,said", [
    ({"at": None}, "to have left this tail before"),
    ({"rereads": False}, "not again from an older position"),
    ({"digest": None}, "when it left was not read"),
    ({"same_shape": False}, "its shape changed since it left"),
    ({"lost": "purged to 000123"},
     "no longer holds where it left: purged to 000123"),
])
def test_a_table_refused_its_changes_since_it_left_is_copied(change, said):
    got, seen = _climb(tailtables._add_ways(), dict(LEFT, **change))
    assert got.rung.name == "copy"
    assert branch(seen, "from-removal") == [REFUSES]
    assert said in why(seen, "from-removal") and said in got.reason


class _Eng:
    hop = None

    def __init__(self, masks):
        self.masks = masks
        self.TABLE_WAYS = (
            tableway.Way("bulk", "the bulk program", frozenset({"exact"}),
                         short="bulk", batch=True),
            tableway.Way("rows", "rows through migkit", frozenset({"exact"}),
                         short="rows"))

    def copier_masks(self):
        return self.masks


def test_the_copier_is_held_beside_a_program_that_carries_the_tables():
    ran = []
    with watched() as seen:
        fell = tableway.batch_or_copier(
            _Eng(False), "app", "bulk", ["a", "b"],
            lambda: ran.append("bulk") or "ok", lambda m: None)
    assert fell == [] and ran == ["bulk"]
    assert branch(seen, "copier") == [HOLDS]


def test_a_table_masked_on_the_way_falls_to_a_copier_that_masks():
    ran = []
    with watched() as seen:
        fell = tableway.batch_or_copier(
            _Eng(True), "app", "bulk", ["a", "b"],
            lambda: ran.append("bulk") or "ok", lambda m: None,
            masked=("a",))
    assert fell == ["a", "b"] and ran == []
    assert branch(seen, "bulk") == [LACKS]
    assert branch(seen, "copier") == [HOLDS]
