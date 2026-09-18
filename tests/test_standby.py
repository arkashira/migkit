"""Saying when a difference might just be a replica that has not caught up.

The shape a lagging source produces - the target appearing to have gained
rows - is the same shape someone writing to the target produces. migkit cannot
tell those apart from one comparison, so it must name both rather than pick.
"""
from migkit import standby as sb


def test_a_writer_gets_no_note():
    assert sb.note(False) == ""
    assert sb.note(False, 12) == ""


def test_lag_that_cannot_be_read_is_unknown_not_silence():
    """A replica of unknown freshness is more reason to say something."""
    assert "lag unknown" in sb.note(True, None)
    assert "lag unknown" in sb.note(True, "not a number")


def test_a_caught_up_replica_still_says_it_is_one():
    n = sb.note(True, 0)
    assert "in recovery" in n and "caught up" in n


def test_a_lagging_replica_reports_how_far():
    assert sb.note(True, 42.4) == "in recovery, 42s behind"


def test_only_the_target_ahead_shapes_can_be_lag():
    """A source that is behind shows fewer rows, so the target looks like it
    gained them. It cannot invent rows the target does not have."""
    n = sb.note(True, 30)
    assert sb.explains("rows-extra by=5", n)
    assert sb.explains("rows-replaced", n)
    assert not sb.explains("rows-missing by=5", n)
    assert not sb.explains("values-changed", n)
    assert not sb.explains("", n)


def test_nothing_is_explained_away_when_the_source_is_a_writer():
    assert not sb.explains("rows-extra by=5", "")


def test_the_caveat_names_both_possibilities_and_picks_neither():
    c = sb.caveat("rows-extra by=5", sb.note(True, 30))
    assert "replica" in c and "30s behind" in c
    assert "re-read from the writer" in c
    # it must not declare the finding false
    for word in ("false", "ignore", "harmless", "not a real"):
        assert word not in c.lower(), word


def test_no_caveat_where_lag_cannot_explain_it():
    assert sb.caveat("rows-missing by=5", sb.note(True, 30)) == ""
    assert sb.caveat("rows-extra by=5", "") == ""
