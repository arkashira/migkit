"""Reading a side that is a replica, and what that does to the answer.

Verifying from a read replica is attractive: the primary takes no load, so
the throttle has nothing to protect and the scan runs at full speed. It is
also the only way to verify a source that has been frozen for cutover,
because a replica is read-only by definition and migkit writes nothing.

But a replica that has not caught up is showing an older database than the
one the target was fed from, and the comparison says so in a way that reads
like a real fault. Measured, on a PostgreSQL 16 primary with a streaming
standby paused mid-stream:

    primary   105 rows
    standby   100 rows   (replay paused)

    source = standby, target = primary
    public.t: DIFF src=100|... dst=105|... kind=rows-extra by=5

Nothing is wrong with that data. The five rows exist on the primary and have
not reached the standby yet. But `rows-extra` is exactly the shape migkit
teaches operators to take seriously - it is what someone writing to the target
looks like - so a lagging replica manufactures the one finding that must never
be shrugged off.

migkit already knew the source was in recovery; the verdict just never said
so. This module exists so it does.

The rule is one-sided like the rest: the note is attached whenever a side is
in recovery, whether or not the lag is measurable. A replica whose lag cannot
be read is more of a reason to say something, not less - the answer there is
"in recovery, lag unknown", never silence.
"""


def note(in_recovery, lag_seconds=None):
    """One clause describing a side that is a replica, or ''.

    `lag_seconds` of None means the server would not say. That is reported as
    unknown rather than dropped: a replica of unknown freshness is exactly the
    case where a reader needs to be told to look.
    """
    if not in_recovery:
        return ""
    if lag_seconds is None:
        return "in recovery, replication lag unknown"
    try:
        lag = float(lag_seconds)
    except (TypeError, ValueError):
        return "in recovery, replication lag unknown"
    if lag <= 0:
        return "in recovery, caught up at the moment it was read"
    return f"in recovery, {lag:.0f}s behind"


def explains(kind, side_note):
    """Whether a replica on the source could have produced this difference.

    Only the target-ahead shapes: a source that is behind shows fewer rows
    than the target, so rows go missing from the *source* side of the
    comparison and the target looks like it gained them. A source that is
    behind cannot invent rows the target does not have, so `rows-missing` is
    never explained away by lag.
    """
    if not side_note:
        return False
    return "rows-extra" in str(kind) or "rows-replaced" in str(kind)


def caveat(kind, src_note):
    """The sentence to append to a finding, or ''.

    Deliberately not a verdict. Lag *could* produce this difference and so
    could a process writing to the target, and migkit cannot tell the two
    apart from one comparison - so it names both and leaves the judgement
    where it belongs.
    """
    if not explains(kind, src_note):
        return ""
    return (f"the source is a replica ({src_note}), which produces exactly"
            " this shape when it has not caught up - re-read from the writer,"
            " or after it catches up, before treating it as a real difference")
