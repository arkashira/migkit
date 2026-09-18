"""What a mover added to the source and never took away.

Every other check in migkit compares the source with the target. This one
looks at the source on its own and asks a different question: what is in here
that the application did not put there?

Movers are not read-only. They create bookkeeping in the database they are
reading from - a schema to hold checksums, event triggers to capture DDL, a
publication, a replication slot - and several of them do not remove any of it
when the task ends. migkit's own playbook has said so in prose for a long
time: "after cutover remove the leftovers on both sides". Prose is a note to
remember something. This counts it.

Three reasons it matters more than tidiness:

- **A replication slot left behind pins WAL.** The source keeps every segment
  the slot has not consumed, and a source that runs out of disk is an outage
  on the system you already migrated away from.
- **Event triggers fire on DDL.** A trigger the mover installed is still there
  the next time anybody runs a migration on the source, long after the mover
  is gone.
- **It is evidence.** A leftover schema is proof of which mover touched this
  database and when, which matters when two of them ran and only one is
  admitted to.

The patterns are named by the mover that leaves them, so a finding says who
was here rather than just that something is odd. migkit's own artifacts are in
the list too - a tool that reports everyone else's litter and not its own is
not worth reading.
"""

# (mover, [patterns]) - matched case-insensitively as a prefix or a whole
# name depending on the shape the mover uses. Kept as data so an engine only
# has to say what kind of object it found, never what it means.
SIGNATURES = [
    ("Tencent DTS", ["__tencentdb__", "tencentdb", "dts_"]),
    ("AWS DMS", ["awsdms_", "dms_"]),
    ("Debezium", ["debezium_", "dbz_"]),
    ("Maxwell", ["maxwell"]),
    ("Canal", ["canal_"]),
    # gh-ost and pt-osc wrap the original table name in BOTH a leading
    # underscore and a suffix. Matching the suffix alone would flag an
    # application's own `orders_new` as litter, which is the false positive
    # this module cannot afford.
    ("gh-ost", [("_", "_gho"), ("_", "_ghc"), ("_", "_del")]),
    ("pt-online-schema-change", [("_", "_new"), "pt_osc_", ("_", "_old")]),
    ("migkit", ["migkit_"]),
]


def whose(name):
    """Which mover leaves an object with this name, or '' if none known."""
    low = str(name).lower()
    for mover, pats in SIGNATURES:
        for pat in pats:
            if isinstance(pat, tuple):
                # both ends must match, so the original table name is wrapped
                pre, suf = pat
                if (low.startswith(pre) and low.endswith(suf)
                        and len(low) > len(pre) + len(suf)):
                    return mover
            elif low.startswith(pat) or low.endswith(pat) or low == pat:
                return mover
    return ""


def group(found):
    """{mover: [labels]} from (kind, name) pairs, keeping only known movers.

    An unrecognised object is not reported. The source belongs to the
    application, and guessing that an unfamiliar schema must be litter would
    turn this from a finding into noise - the one thing a check nobody asked
    for cannot afford to be.
    """
    out = {}
    for kind, name in found:
        mover = whose(name)
        if mover:
            out.setdefault(mover, []).append(f"{kind} {name}")
    return out


def describe(by_mover, show=4):
    parts = []
    for mover, items in sorted(by_mover.items()):
        items = sorted(set(items))
        shown = ", ".join(items[:show])
        if len(items) > show:
            shown += f" and {len(items) - show} more"
        parts.append(f"{mover}: {shown}")
    return "; ".join(parts)


def total(by_mover):
    return sum(len(set(v)) for v in by_mover.values())


# Leftovers that cost something while they sit there, as opposed to merely
# being untidy. Named by object kind, because every engine calls the object
# the same thing even when it spells the query differently.
COSTLY = {
    "slot": "pins WAL on the source until it is dropped - the source can run"
            " out of disk",
    "publication": "keeps the source publishing changes nobody reads",
    "event trigger": "still fires on the next DDL anyone runs on the source",
}


def urgent(by_mover):
    """The subset that is doing harm now, not just sitting there."""
    out = []
    # longest kind first: an object kind can be two words ("event trigger"),
    # and splitting on the first space matched "event" against nothing
    kinds = sorted(COSTLY, key=len, reverse=True)
    for items in by_mover.values():
        for label in set(items):
            for kind in kinds:
                if label.startswith(kind + " "):
                    out.append((label, COSTLY[kind]))
                    break
    return sorted(out)
