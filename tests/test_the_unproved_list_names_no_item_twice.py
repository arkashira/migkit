"""`capabilities.UNPROVED` is a dict literal: an item written twice keeps only
its last line, and the engines named on the first are lost without a sound
(two engineers each adding their engine to the same item is how it happens).
"""
import ast
import collections
from pathlib import Path

CAPS = Path(__file__).resolve().parent.parent / "migkit" / "capabilities.py"


def _unproved_keys():
    for node in ast.parse(CAPS.read_text()).body:
        if isinstance(node, ast.Assign) and any(
                getattr(t, "id", "") == "UNPROVED" for t in node.targets):
            return [k.value for k in node.value.keys]
    raise AssertionError("UNPROVED is not a dict literal in capabilities.py")


def test_no_item_is_listed_twice_in_unproved():
    seen = collections.Counter(_unproved_keys())
    assert [k for k, n in seen.items() if n > 1] == []
