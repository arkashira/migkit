"""The half DMS leaves out: the mapping drives the verification too.

A transformation layer that only the mover reads produces a target nobody
can check. Rename `public.orders` to `sales.orders_v2`, and a verifier that
pairs tables by name reports the source table as missing and the target
table as extra - two findings, both wrong, about a migration that worked.
AWS documents its own validation as unable to validate a transformed
target, which is the same problem from the other end.

So `match_tables` takes the hop's mapping. A mapped table is paired with
what it was renamed *to*; everything else still pairs by leaf name exactly
as before, which is what every hop in use does today.

The refusals stay refusals. A rename pointing at a table the target does
not have is still `src_only` - it really is missing - but the message says
the rename is why, because an operator hunting for `orders` on a target
that only ever had `orders_v2` is looking for the wrong thing.
"""
import pytest

from migkit.config import Endpoint, Hop


def _hetero(mapping=None):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="h", engine="hetero",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["appdb"], mapping=mapping or {},
              options={"source_engine": "mysql",
                       "target_engine": "postgres"})
    return HeteroEngine(hop)


def test_without_a_mapping_the_pairing_is_exactly_what_it_was():
    """Every hop in use has no mapping. `rename=None` must be the old
    function, not a new one that happens to agree today."""
    from migkit.engines.hetero import HeteroEngine
    src = ["orders", "people", "gone"]
    dst = ["public.orders", "public.people", "public.extra"]
    before = HeteroEngine.match_tables(src, dst)
    assert before[0] == [("orders", "public.orders"),
                         ("people", "public.people")], before
    assert before[1] == ["gone"] and before[2] == ["public.extra"], before


def test_a_renamed_table_is_paired_with_what_it_was_renamed_to():
    eng = _hetero({"tables": {"orders": "sales.orders_v2"}})
    pairs, src_only, dst_only, _ = eng.match_tables(
        ["orders", "people"],
        ["sales.orders_v2", "public.people"], eng._rename)
    assert ("orders", "sales.orders_v2") in pairs, pairs
    assert ("people", "public.people") in pairs, pairs
    # and neither side is reported as missing any more
    assert src_only == [] and dst_only == [], (src_only, dst_only)


def test_without_the_mapping_that_same_pair_reads_as_two_wrong_findings():
    """The control, and the argument for the feature: the same inputs
    without the mapping produce a missing table and an extra one."""
    from migkit.engines.hetero import HeteroEngine
    pairs, src_only, dst_only, _ = HeteroEngine.match_tables(
        ["orders", "people"], ["sales.orders_v2", "public.people"])
    assert ("orders", "sales.orders_v2") not in pairs, pairs
    assert src_only == ["orders"], src_only
    assert dst_only == ["sales.orders_v2"], dst_only


def test_a_rename_pointing_at_nothing_is_still_missing():
    """The mapping may not be used to make a real absence disappear."""
    eng = _hetero({"tables": {"orders": "sales.orders_v2"}})
    pairs, src_only, dst_only, _ = eng.match_tables(
        ["orders"], ["public.something_else"], eng._rename)
    assert pairs == [], pairs
    assert src_only == ["orders"], src_only


def test_the_message_says_the_rename_is_why_it_looks_missing(monkeypatch):
    eng = _hetero({"tables": {"orders": "sales.orders_v2"}})
    monkeypatch.setattr(eng.src_engine, "neutral_tables",
                        lambda side, db: ["orders"])
    monkeypatch.setattr(eng.dst_engine, "neutral_tables",
                        lambda side, db: ["public.other"])
    rows = eng._neutral_rows("appdb")
    said = " ".join(r[2] for r in rows)
    assert "mapped to sales.orders_v2" in said, said
    assert "the target does not have" in said, said


def test_an_unmapped_missing_table_keeps_the_plain_message(monkeypatch):
    """The new sentence must not replace the old one for tables the
    mapping never mentioned."""
    eng = _hetero({"tables": {"orders": "sales.orders_v2"}})
    monkeypatch.setattr(eng.src_engine, "neutral_tables",
                        lambda side, db: ["invoices"])
    monkeypatch.setattr(eng.dst_engine, "neutral_tables",
                        lambda side, db: [])
    said = " ".join(r[2] for r in eng._neutral_rows("appdb"))
    assert "is on the source and not on the target" in said, said
    assert "mapped to" not in said, said


def test_a_rename_does_not_consume_a_table_that_matched_by_name():
    """Two source tables must not end up pointed at one target table: the
    renamed one takes its target, and the namesake keeps its own."""
    eng = _hetero({"tables": {"archive": "public.orders"}})
    pairs, src_only, dst_only, _ = eng.match_tables(
        ["archive", "orders"], ["public.orders"], eng._rename)
    assert pairs == [("archive", "public.orders")], pairs
    assert src_only == ["orders"], src_only
    assert dst_only == [], dst_only


def test_ambiguity_is_still_refused_with_a_mapping_present():
    """The mapping resolves what it names. It does not license guessing
    about what it does not."""
    eng = _hetero({"tables": {"orders": "sales.orders_v2"}})
    pairs, _, _, ambiguous = eng.match_tables(
        ["orders", "a.people", "b.people"],
        ["sales.orders_v2", "x.people", "y.people"], eng._rename)
    assert ("orders", "sales.orders_v2") in pairs, pairs
    assert ambiguous, ambiguous
    assert all(p[0] != "a.people" for p in pairs), pairs
