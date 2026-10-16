"""A range begun before a stop is named in the journal by `ranges.ident`;
replaying the journal names it the same way, so a text key, a composite
key and the open first end are recognised as begun after a reload, not
only an integer."""
from migkit import checkpoint, ranges


def test_the_replayed_seen_keys_are_the_idents():
    lines = [{"k": "t", "seen": [None, "m0"]},
             {"k": "t", "seen": ["abc", "m1"]},
             {"k": "t", "seen": [[1, "x"], "m2"]},
             {"k": "t", "seen": [7, "m3"]}]
    entries = {}
    checkpoint.replay(entries, lines)
    seen = entries["t"]["ranges_seen"]
    assert set(seen) == {ranges.ident(None), ranges.ident("abc"),
                         ranges.ident([1, "x"]), ranges.ident(7)}
    assert seen[ranges.ident("abc")] == "m1"
    assert "None" not in seen and "abc" not in seen


def test_a_replayed_text_key_range_is_begun():
    entries = {}
    checkpoint.replay(entries, [{"k": "t", "seen": ["abc", "m1"]}])
    st = entries["t"]
    st["ranges"] = [("abc", "abd")]
    assert ranges.ident("abc") in ranges.begun_ranges(st)
