"""The rungs a large value is carried by and a failed unit is retuned
by, each reached on its branches on fakes (backlog AUD-Z.rungs-outside):
a value goes whole only on a certified bound every limit takes as sent,
in pieces only into a type that appends, streamed only through a load
that streams; a unit refused for a value's size is never run again as it
was - a smaller batch, pieces, a stream or another way, each only where
it answers the cause and differs from what failed - and one that timed
out is cut into smaller ranges, never pieces."""
import pytest

from migkit import decide, pieces, tuning
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("tuning", "payload_way", "whole", "holds"),
    ("tuning", "payload_way", "whole", "refuses"),
    ("tuning", "payload_way", "pieces", "holds"),
    ("tuning", "payload_way", "pieces", "refuses"),
    ("tuning", "payload_way", "stream", "holds"),
    ("tuning", "payload_way", "stream", "refuses"),
    ("tuning", "Retune", "smaller batch", "holds"),
    ("tuning", "Retune", "smaller batch", "refuses"),
    ("tuning", "Retune", "pieces", "holds"),
    ("tuning", "Retune", "pieces", "refuses"),
    ("tuning", "Retune", "stream", "holds"),
    ("tuning", "Retune", "stream", "refuses"),
    ("tuning", "Retune", "smaller ranges", "holds"),
    ("tuning", "Retune", "smaller ranges", "refuses"),
    ("tuning", "Retune", "other way", "holds"),
    ("tuning", "Retune", "other way", "refuses"),
]

KIB, MIB = 1 << 10, 1 << 20
PACKET = pieces.Limit("max_allowed_packet", 64 * MIB, "sent")
SMALL = tuning.Bound(4 * KIB, 8 * KIB, True, "4 KiB, from the catalogue")
WIDE = tuning.Bound(200 * MIB, 400 * MIB, True, "200 MiB, every row read")
SAMPLED = tuning.Bound(4 * KIB, 8 * KIB, False, "4 KiB, sampled")


def test_a_certified_bound_under_every_limit_goes_whole():
    with watched() as seen:
        got = tuning.payload_way(SMALL, [PACKET], appendable=True,
                                 streams=True)
    assert got.rung.name == "whole"
    for name in ("whole", "pieces", "stream"):
        assert branch(seen, name) == [HOLDS], name


def test_a_value_past_a_limit_is_written_in_pieces_where_it_appends():
    with watched() as seen:
        got = tuning.payload_way(WIDE, [PACKET], appendable=True)
    assert got.rung.name == "pieces"
    assert branch(seen, "whole") == [REFUSES]
    assert "bytes as sent, is past" in why(seen, "whole")
    assert branch(seen, "stream") == [REFUSES]
    assert why(seen, "stream") == "the target has no load that streams a value"


def test_a_sampled_bound_is_streamed_never_sent_whole():
    with watched() as seen:
        got = tuning.payload_way(SAMPLED, [PACKET], streams=True)
    assert got.rung.name == "stream"
    assert why(seen, "whole") == "no value is known to fit: 4 KiB, sampled"
    assert why(seen, "pieces") == ("the target's type takes no piece added"
                                   " to it")
    assert branch(seen, "stream") == [HOLDS]


def test_no_way_for_a_wide_value_is_a_blocker_naming_each():
    with watched() as seen, pytest.raises(decide.Blocked):
        tuning.payload_way(WIDE, [PACKET], engine="mysql")
    for name in ("whole", "pieces", "stream"):
        assert branch(seen, name) == [REFUSES], name


TOO_BIG = "Got a packet bigger than 'max_allowed_packet' bytes"


def test_a_value_too_large_is_answered_by_every_way_but_smaller_ranges():
    r = tuning.Retune("t.blobs", rows=10, nbytes=MIB, appendable=True,
                      streams=True, other=object())
    with watched() as seen:
        adj = r.failed("u1", TOO_BIG, sent=MIB, rows=10)
    assert adj.cause.kind == tuning.PACKET
    for name in ("smaller batch", "pieces", "stream", "other way"):
        assert branch(seen, name) == [HOLDS], name
    assert branch(seen, "smaller ranges") == [REFUSES]
    assert why(seen, "smaller ranges") == ("a shorter unit does not answer"
                                           " a size")


def test_a_wait_is_cut_into_ranges_and_never_into_pieces():
    r = tuning.Retune("t", rows=100, nbytes=MIB)
    with watched() as seen:
        adj = r.failed("u1", "statement timeout", sent=KIB, rows=100)
    assert adj.cause.kind == tuning.TIMEOUT
    assert branch(seen, "smaller ranges") == [HOLDS]
    assert why(seen, "pieces") == "pieces answer a value's size, not a wait"
    assert why(seen, "stream") == ("the target has no load that streams a"
                                   " value")
    assert why(seen, "other way") == "the work has no other way"
    assert adj.rung in ("smaller batch", "smaller ranges")


def test_one_row_that_timed_out_has_no_way_and_says_each():
    r = tuning.Retune("t", rows=1, nbytes=MIB)
    with watched() as seen, pytest.raises(decide.Blocked) as e:
        r.failed("u1", "statement timeout", sent=KIB, rows=1)
    assert why(seen, "smaller batch") == ("one row alone is past what the"
                                          " server takes")
    assert why(seen, "smaller ranges") == "the unit is one row"
    for name in ("smaller batch", "pieces", "stream", "smaller ranges",
                 "other way"):
        assert branch(seen, name) == [REFUSES], name
    assert "the unit is one row" in str(e.value)
