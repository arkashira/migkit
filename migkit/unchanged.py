"""Proof that a table has not moved since it was last proved equal.

Verifying a migration is not done once. During a CDC window it is done again
and again, and every run re-hashes every table from the beginning - including
the ones nobody has written to since the last time they came out equal. On an
estate where a handful of large tables are quiet and a couple of hundred small
ones are busy, that is the difference between hours and seconds.

The idea is cheap. The danger is not.

A marker that says "unchanged" when the table did change turns a skipped scan
into a table that was never verified but is reported as equal. That is a false
negative, and a verifier that produces one has failed at its only job. So the
rule here is stricter than anywhere else in migkit:

**Any doubt at all means re-verify.** The marker is compared for exact
equality, not for "has it grown" - a counter that went *down* (a statistics
reset, a crash) is as much a reason to look again as one that went up. An
engine that cannot produce a trustworthy marker returns `None`, which never
skips anything. A marker is never carried across a change of server version.

What the marker is made of was measured rather than assumed, on PostgreSQL 16:

    change      n_tup_ins/upd/del   relfilenode   size
    UPDATE      moves               same          same
    DELETE      moves               same          same
    TRUNCATE    **does not move**   changes       8192 -> 0
    stats reset drops to 0/0/0      same          same

TRUNCATE is the one that matters: it is invisible to the tuple counters, so a
marker built only from them would skip an emptied table and call it equal.
`relfilenode` catches it, and is in the marker for that reason.

One more limit, deliberately: **the final proof never skips.** The saving is
for the interim runs during the window, where the question is "has anything
drifted since the last look". When the source is frozen and the answer is
going into a cutover decision, every table is read.
"""

# PostgreSQL only publishes its per-table statistics through a collector that,
# before version 15, sent them over UDP and could silently drop a message
# under load. A dropped UPDATE is exactly the false negative this module
# exists to avoid, so the marker is refused on older servers rather than
# trusted. Shared-memory statistics landed in 15.
MIN_PG_MAJOR = 15


def server_major(text):
    """Major version from what the engine reported, or None."""
    if not text:
        return None
    import re
    m = re.match(r"\s*(\d+)", str(text))
    return int(m.group(1)) if m else None


def usable_postgres(server_version):
    """Whether this server's statistics can be trusted for a marker."""
    maj = server_major(server_version)
    return maj is not None and maj >= MIN_PG_MAJOR


def marker(parts):
    """One comparable string from the pieces an engine collected.

    `None` anywhere in the pieces makes the whole marker `None`: a marker with
    a hole in it is not a marker, and treating a missing piece as an empty one
    would let two different tables compare equal.
    """
    if parts is None:
        return None
    parts = list(parts)
    if not parts or any(p is None for p in parts):
        return None
    return "|".join(str(p) for p in parts)


def unchanged(before, after):
    """True only when both markers exist and are exactly equal.

    Exact equality, not ordering: a counter that went down means the server
    lost its statistics, which is a reason to read the table again rather than
    a reason to relax.
    """
    if not before or not after:
        return False
    return str(before) == str(after)


def skippable(stored, src_now, dst_now):
    """True when both sides prove they have not moved since `stored`.

    `stored` is what was recorded when the table last came out equal, as
    `{"src": marker, "dst": marker}`. Both sides have to match: a target that
    someone wrote to is exactly the case the boundary check exists for, and
    skipping it because the source is quiet would hide it.
    """
    if not isinstance(stored, dict):
        return False
    return (unchanged(stored.get("src"), src_now)
            and unchanged(stored.get("dst"), dst_now))


def record(src_now, dst_now):
    """What to store alongside a table that just came out equal, or None."""
    if not src_now or not dst_now:
        return None
    return {"src": src_now, "dst": dst_now}
