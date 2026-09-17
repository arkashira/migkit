"""What the migration will not do for you, counted and named.

Every mover leaves work behind, and the work is never in the mover's output -
it is in what the output quietly omits. A stored procedure, an unlogged table,
a collection whose validator has to be recreated: none of these produce an
error, so the first time anyone learns they were left behind is when something
downstream needs them.

This is the inventory of that residue. Three deliberate limits:

**It counts and names; it does not estimate.** ora2pg prints person-days, and
those numbers mean something because they sit on a cost table calibrated over
years of real migrations. migkit has no such table. Inventing one would be
fabrication wearing the clothes of a measurement, so the output stops at the
last defensible fact: how many, and which ones. An operator who knows their own
team can apply their own rate to a count. Nobody can apply a rate to a guess.

**A probe that could not run reports unknown, not zero.** A managed provider
that hides `mysql.proc`, a role without permission on `pg_proc`: in each case
the honest answer is that nobody looked. Reporting zero there would turn a
missing permission into a clean bill of health, which is the single worst
failure this file could have.

**The wording lives here, not in the engines.** An engine supplies facts -
these names, in this database. How a kind of manual work is phrased, ranked and
counted is decided once, so a PostgreSQL inventory and a MySQL one can be read
with the same eyes and added together without translation.
"""

# kind -> (level, what it is, why a human has to do something about it)
KINDS = {
    "no-row-key": (
        "warn",
        "tables with no primary key or unique index",
        "a mover cannot replicate updates or deletes for them, and"
        " verification falls back to whole-table checksums with no way to"
        " point at the offending row",
    ),
    "server-side-code": (
        "warn",
        "routines, triggers and views a data mover does not carry",
        "the data arrives and the behaviour attached to it does not; each one"
        " has to be recreated and its owner, definer or search path decided",
    ),
    "not-carried": (
        "warn",
        "objects whose contents no data mover copies",
        "they are invisible to the replication stream, so they are empty or"
        " stale on the target unless someone moves them separately",
    ),
    "target-prereq": (
        "fail",
        "things that must exist on the target before the load",
        "the load fails or silently changes meaning without them, and none of"
        " them can be created by the mover",
    ),
    "decide-then-apply": (
        "warn",
        "settings that need a decision rather than a copy",
        "copying the source value is sometimes right and sometimes an outage,"
        " so migkit will not choose on your behalf",
    ),
}
ORDER = ["target-prereq", "no-row-key", "not-carried", "server-side-code",
         "decide-then-apply"]
SHOW = 6            # names printed before the rest become a count


def names(items, show=SHOW):
    """A readable list that admits how much it is not showing."""
    items = sorted(str(i) for i in items)
    if len(items) <= show:
        return ", ".join(items)
    return f"{', '.join(items[:show])} and {len(items) - show} more"


class Inventory:
    """Facts in, assess rows out.

    Engines call `add` and `unknown`; nothing else here is theirs to decide.
    """

    def __init__(self):
        self._found = {}        # (kind, scope) -> {what: [names]}
        self._unknown = {}      # (kind, scope) -> [why]

    def add(self, kind, scope, what, found):
        """Record `found` (an iterable of names) as manual work of `kind`.

        An empty iterable is recorded too: "we looked and there were none" is
        a different statement from "nobody looked", and the report is only
        worth reading if it can tell them apart.
        """
        if kind not in KINDS:
            raise KeyError(f"unknown kind of manual work: {kind}")
        self._found.setdefault((kind, scope), {}).setdefault(what, [])
        self._found[(kind, scope)][what] += [str(f) for f in found]

    def unknown(self, kind, scope, why):
        """Record that a probe could not run. Never counts as zero."""
        if kind not in KINDS:
            raise KeyError(f"unknown kind of manual work: {kind}")
        self._unknown.setdefault((kind, scope), []).append(str(why))

    def total(self):
        return sum(len(v) for d in self._found.values() for v in d.values())

    def unknowns(self):
        return sum(len(v) for v in self._unknown.values())

    def rows(self):
        """assess-shaped rows: one per kind per scope, worst kind first."""
        out = []
        scopes = {s for _, s in self._found} | {s for _, s in self._unknown}
        for kind in ORDER:
            level, title, why = KINDS[kind]
            for scope in sorted(scopes):
                blocked = self._unknown.get((kind, scope))
                buckets = self._found.get((kind, scope), {})
                n = sum(len(v) for v in buckets.values())
                if blocked:
                    # unknown outranks whatever was counted: a partial count
                    # presented as a total is the thing this guards against
                    seen = f"; {n} found by the probes that did run" if n else ""
                    out.append({
                        "level": "warn", "scope": scope, "item": title,
                        "detail": f"UNKNOWN - {'; '.join(blocked)}{seen}."
                                  f" Not zero: nobody was able to look",
                    })
                    continue
                if not buckets:
                    continue
                if not n:
                    out.append({"level": "pass", "scope": scope,
                                "item": title, "detail": "none"})
                    continue
                parts = [f"{what}: {len(found)} ({names(found)})"
                         for what, found in sorted(buckets.items()) if found]
                out.append({"level": level, "scope": scope, "item": title,
                            "detail": f"{n} - {'; '.join(parts)}. {why}"})
        return out

    def summary(self):
        """One closing row, or nothing if there is nothing to close."""
        n, u = self.total(), self.unknowns()
        if not n and not u:
            return []
        bits = []
        if n:
            bits.append(f"{n} items need hands")
        if u:
            bits.append(f"{u} probes could not run (counted as unknown,"
                        f" not as zero)")
        return [{
            "level": "fail" if any(r["level"] == "fail" for r in self.rows())
                     else ("warn" if n or u else "pass"),
            "scope": "manual work", "item": "; ".join(bits),
            # said out loud so nobody waits for a number that is not coming
            "detail": "migkit counts and names this work and deliberately"
                      " does not estimate how long it takes - it has no"
                      " calibration to estimate from",
        }]
