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

# Software whose statistics migkit has actually measured the marker against.
#
# Deliberately an allow-list rather than a list of brands to avoid. This
# module's failure mode is skipping a scan and reporting the table as proved
# equal, so "nothing is known against that brand" is not a good enough reason
# to trust it - and a wire-compatible fork is free to answer every question
# here plausibly and wrongly.
#
# Measured on CockroachDB v23.2.5, which speaks the PostgreSQL protocol:
# `pg_stat_all_tables` exists and is empty (`select count(*)` returns 0) and
# `pg_class.relfilenode` is 0 for every relation. A marker built there is the
# same string forever, so every table would skip its scan on every run. The
# only thing stopping that today is that CockroachDB reports `server_version`
# as 13.0.0 and the version gate refuses anything below 15 - a safety that is
# an accident of a number the brand is free to change.
TRUSTED_BRANDS = {"postgres"}


def usable_brand(brand):
    """Whether a marker from this software may be trusted at all.

    An unidentified server is refused for the same reason an unmeasured one
    is: the answer to "is this safe to skip" cannot be yes when the answer to
    "what is this" is unknown.
    """
    return getattr(brand, "name", None) in TRUSTED_BRANDS


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


class Proof:
    """Which tables were proved equal, and the marker they carried then.

    Kept beside the reports rather than in the checkpoint: a checkpoint holds
    partial work inside one run and is cleared the moment a table finishes,
    which is exactly when this needs to start remembering.

    A table is dropped from the store the moment it comes out different. A
    marker recorded against a table that does not match is not weaker
    evidence - it is evidence of the wrong thing, and keeping it would let the
    next run skip a table that is known to be wrong.
    """

    def __init__(self, path):
        import json
        import pathlib as _p
        self.path = _p.Path(path)
        self.data = {}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text())
                if isinstance(loaded, dict):
                    self.data = loaded
            except Exception:
                # an unreadable store proves nothing, and guessing at its
                # contents would be guessing about what was verified
                self.data = {}

    def get(self, table):
        v = self.data.get(str(table))
        return v if isinstance(v, dict) else None

    def set(self, table, src_now, dst_now):
        rec = record(src_now, dst_now)
        if rec:
            self.data[str(table)] = rec
        else:
            self.drop(table)

    def drop(self, table):
        self.data.pop(str(table), None)

    def save(self):
        import json
        import os
        import tempfile
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(self.data, f, indent=1, sort_keys=True)
            os.replace(tmp, self.path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
