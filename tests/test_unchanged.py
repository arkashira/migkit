"""Skipping a table that has not moved since it was last proved equal.

Everything here defends one direction. Re-reading a table that turned out to
be quiet costs a scan. Skipping one that actually changed reports it as equal
without ever looking, which is the failure a verifier cannot have - so every
ambiguous case must come out as "verify it".
"""
from migkit import unchanged as u


def test_a_marker_needs_every_piece():
    """A marker with a hole in it is not a marker: treating a missing piece
    as empty would let two different tables compare equal."""
    assert u.marker([1, 2, 3, "16384", 8192]) == "1|2|3|16384|8192"
    assert u.marker([1, None, 3]) is None
    assert u.marker([]) is None
    assert u.marker(None) is None


def test_identical_markers_are_the_only_unchanged_case():
    assert u.unchanged("1|2|3", "1|2|3")
    assert not u.unchanged("1|2|3", "1|2|4")


def test_a_counter_going_down_means_verify_not_relax():
    """A statistics reset or a crash lowers the counters. That is a reason to
    look again, not evidence that nothing happened."""
    assert not u.unchanged("100|1|1", "0|0|0")


def test_a_missing_marker_never_skips():
    for a, b in (("", "1|2|3"), ("1|2|3", ""), (None, "1|2|3"),
                 ("1|2|3", None), (None, None)):
        assert not u.unchanged(a, b), (a, b)


def test_both_sides_have_to_be_quiet():
    """A target somebody wrote to is exactly what the boundary check exists
    for. Skipping it because the source is quiet would hide it."""
    stored = {"src": "A", "dst": "B"}
    assert u.skippable(stored, "A", "B")
    assert not u.skippable(stored, "A", "B-changed")
    assert not u.skippable(stored, "A-changed", "B")


def test_nothing_stored_means_nothing_skipped():
    assert not u.skippable(None, "A", "B")
    assert not u.skippable({}, "A", "B")
    assert not u.skippable("not a dict", "A", "B")


def test_record_refuses_a_half_marker():
    assert u.record("A", "B") == {"src": "A", "dst": "B"}
    assert u.record("A", None) is None
    assert u.record(None, "B") is None
    assert u.record("", "B") is None


def test_statistics_before_postgres_15_are_not_trusted():
    """Before 15 the collector sent statistics over UDP and could drop a
    message under load. A dropped UPDATE is the exact false negative this
    module exists to avoid."""
    assert u.usable_postgres("16.15")
    assert u.usable_postgres("15.0")
    assert not u.usable_postgres("14.11")
    assert not u.usable_postgres("9.6")
    assert not u.usable_postgres(None)
    assert not u.usable_postgres("unknown")


def test_server_major_reads_the_shapes_engines_report():
    assert u.server_major("16.15") == 16
    assert u.server_major("8.0.30-txsql") == 8
    assert u.server_major(None) is None
