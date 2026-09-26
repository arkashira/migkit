"""The time zone the data is written in, against the one the server
declares (backlog 23).

A column with no zone of its own - PostgreSQL's `timestamp`, MySQL's
`datetime` - holds whatever wall-clock time the application wrote. The
server's zone says nothing about it. An application writing Bangkok time
into a server set to UTC is common, and harmless until the move: a target
column that has a zone (`timestamptz`), or a target server in another
zone, reads those values as UTC and every one of them shifts by seven
hours, silently and consistently. That is the kind of difference a count
and a checksum over the same text do not see.

The evidence used is the only kind that does not guess: a column whose
latest value is in the future of the server's UTC clock is written in a
zone ahead of UTC, at least that far ahead. A latest value in the past
proves nothing - it may be an old row - so it is not read as a zone.
Only columns an index leads are read, so the latest value costs one probe
and never a scan.
"""

#: what counts as the future, in minutes: clocks drift a little
SLACK = 5


def infer(eng, db):
    """One `Result` for the database's source."""
    from .engines.base import Result
    evidence = getattr(eng, "zone_evidence", None)
    if evidence is None:
        return Result("deep", f"{db} time zone in use", "skip",
                      "this engine cannot read its columns' latest times")
    try:
        offset, utc_now, heads = evidence("src", db)
    except Exception as e:  # noqa: BLE001 - said, as the skip's reason
        return Result("deep", f"{db} time zone in use", "skip",
                      "the latest times could not be read:"
                      f" {(str(e).strip().splitlines() or [''])[0][:100]}")
    ahead = []
    for name, latest in heads:
        if latest is None:
            continue
        minutes = (latest - utc_now).total_seconds() / 60
        if minutes > SLACK:
            # at least this far ahead of UTC, to the half hour below it
            ahead.append((name, int(minutes // 30) * 30))
    if not ahead:
        return Result("deep", f"{db} time zone in use", "skip",
                      f"none of the {len(heads)} indexed zone-less time"
                      " columns holds a time ahead of UTC, which is the only"
                      " evidence of the zone they are written in")
    written = max(m for _, m in ahead)
    said = ", ".join(n for n, _ in ahead[:4])
    if written > offset + 30:
        return Result(
            "deep", f"{db} time zone in use", "warn",
            f"{said} hold times at least {_zone(written)} - written in a"
            f" zone ahead of the server's own ({_zone(offset)}). They carry"
            " no zone of their own: a target column that has one, or a"
            " target server in another zone, reads every one of them"
            f" {_hours(written - offset)} off", "",
            "keep the target's columns without a zone and its server in the"
            " same zone, or convert the values deliberately as they move")
    return Result("deep", f"{db} time zone in use", "ok",
                  f"{said} are written in the server's own zone"
                  f" ({_zone(offset)})")


def _zone(minutes):
    sign = "+" if minutes >= 0 else "-"
    h, m = divmod(abs(int(minutes)), 60)
    return f"UTC{sign}{h}" + (f":{m:02d}" if m else "")


def _hours(minutes):
    h, m = divmod(abs(int(minutes)), 60)
    return f"{h}h" + (f"{m:02d}m" if m else "")
