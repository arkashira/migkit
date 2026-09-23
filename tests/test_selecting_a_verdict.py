"""Picking a verdict out of a report, without picking the wrong one.

The deep battery answers a lot of questions about one database, and two of
them are about keys: `f"{db} keys"` asks whether a table has a primary or
unique key at all, and `f"{db} duplicate keys"` asks whether an existing
unique index is still enforcing. Both are real, both are worth having, and
the second was added after the tests were written.

Every test selecting with `r.scope.endswith("keys")` then started answering
with whichever the engine appended first - and the engine appends in the
order it runs the checks, which is not an order any test chose. Measured on
a live pair:

    skip   'shop duplicate keys'
    diff   'shop keys'

`keys[0]` was the *skip*, so a test written to prove a table with no key is
flagged was reading the verdict for a question it had not asked. It failed,
which is the lucky case: the two statuses differed. Two `ok`s and it would
have passed on the wrong record for as long as anyone cared to look.

`verdict()` takes the scope in full and refuses anything but one match, so
the failure lands on the selector instead of on whatever the wrong record
happened to say.
"""
import pytest

from tests.conftest import verdict


class R:
    def __init__(self, scope, status):
        self.scope = scope
        self.status = status


REPORT = [R("shop duplicate keys", "skip"), R("shop keys", "diff"),
          R("shop indexes", "ok")]


def test_it_returns_the_one_with_that_exact_scope():
    assert verdict(REPORT, "shop keys").status == "diff"
    assert verdict(REPORT, "shop duplicate keys").status == "skip"


def test_a_suffix_selector_would_have_taken_the_other_one():
    """The bug, written down: this is what every `endswith` selector did."""
    loose = [r for r in REPORT if r.scope.endswith("keys")]
    assert len(loose) == 2
    assert loose[0].status == "skip", "the wrong record, and it reads fine"


def test_a_scope_nothing_carries_is_an_error_not_an_empty_list():
    with pytest.raises(AssertionError) as e:
        verdict(REPORT, "shop nosuch")
    assert "0 verdicts" in str(e.value), str(e.value)


def test_the_failure_names_the_scopes_it_was_confused_by():
    """A bare "not found" sends the reader to the engine. Naming the
    neighbours sends them to the line that needs changing."""
    with pytest.raises(AssertionError) as e:
        verdict(REPORT, "shop missing keys")
    said = str(e.value)
    assert "shop keys" in said and "shop duplicate keys" in said, said


def test_two_records_with_one_scope_is_refused_too():
    """Not the bug above, but the same lie: a reader cannot tell which of
    two verdicts for one question is the answer."""
    with pytest.raises(AssertionError) as e:
        verdict(REPORT + [R("shop keys", "ok")], "shop keys")
    assert "2 verdicts" in str(e.value), str(e.value)


# ---- and the next one like it, caught before it is written ----

def _ambiguous_scope_tails():
    """Scope names that end with another scope name.

    Read from the engines rather than listed here, so a scope added
    tomorrow is covered by the same measurement. Four pairs today:
    `duplicate keys`/`keys`, `schema cross-check`/`cross-check`,
    `seq-grants`/`grants`, `large objects`/`objects`.
    """
    import pathlib
    import re
    scopes = set()
    for p in pathlib.Path(__file__).resolve().parents[1].joinpath(
            "migkit").rglob("*.py"):
        scopes |= set(re.findall(r'f"\{db\} ([a-z][a-z0-9 _-]*)"',
                                 p.read_text()))
    return {s for s in scopes
            if any(o != s and o.endswith(s) for o in scopes)}


def test_the_scan_finds_the_pair_this_file_is_about():
    """A scan that matched nothing would pass for the wrong reason."""
    tails = _ambiguous_scope_tails()
    assert "keys" in tails, sorted(tails)


def test_no_test_selects_a_verdict_by_an_ambiguous_suffix():
    """The guard. A suffix two scopes answer to picks whichever the engine
    ran first, and the test reads a verdict for a question it never asked.

    A line that means to be loose says so, in as many words.
    """
    import pathlib
    import re
    tails = _ambiguous_scope_tails()
    bad = []
    here = pathlib.Path(__file__).resolve()
    for p in sorted(here.parent.glob("*.py")):
        if p == here:
            continue                      # this file is about the trap
        for i, line in enumerate(p.read_text().splitlines(), 1):
            m = re.search(r'scope\.endswith\("([^"]+)"\)', line)
            if m and m.group(1) in tails and "deliberately loose" not in line:
                bad.append(f"{p.name}:{i} endswith({m.group(1)!r})")
    assert not bad, bad + ["use verdict(results, '<db> <scope>') instead"]
