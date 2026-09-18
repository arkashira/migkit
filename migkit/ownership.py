"""Who an object belongs to, and whose privileges it runs with.

Two engines say this with different words and mean the same thing. PostgreSQL
gives every object an owner - the role that may `ALTER` or `DROP` it. MySQL
gives views, routines, triggers and events a `DEFINER` and a `SQL SECURITY`
mode - the account whose privileges the object executes with. In both cases
the object can survive a migration looking perfect while the identity attached
to it has changed, and in both cases nothing errors until someone tries to use
or modify it.

Both were measured to be invisible to migkit before this existed.

**PostgreSQL.** Tencent's own migration notes say objects created by
`postgres` on the source change ownership to the migration account on the
target. Measured: a table owned by `appowner` on the source arrived owned by
`dts_migration`, and the structural diff produced no `OWNER TO` statement -
`results` compares definitions, and an owner is not part of one. (A function's
`SECURITY DEFINER` flag *is* part of its definition and was already caught, so
that is deliberately left alone here.)

**MySQL.** Measured on a pair where a view was `app@% / DEFINER` on the source
and `dts_migration@% / INVOKER` on the target: migkit reported `ok` on the
schema check, `ok` on the object check and `atlas diff clean`. That is not an
oversight in the differ - `_canon_ddl` strips `DEFINER=` and `SQL SECURITY`
before comparing, on purpose, because the mover rewrites them on every object
and leaving them in would bury every real finding under cosmetic noise. The
right answer is not to stop stripping them; it is to report them separately,
under their own name, where one line can describe a systematic change.

A `SQL SECURITY DEFINER` that became `INVOKER` deserves particular attention:
the routine now runs with the caller's privileges instead of the definer's. It
either stops working, or starts working for callers who should not have been
able to run it.

Reporting rule: group by the change, not by the object. A whole estate moved
to one new owner is one fact and should read as one line; a single object whose
owner slipped is a different fact and should stand out.
"""


def group(pairs):
    """{(src_identity, dst_identity): [object names]} from (name, a, b) rows.

    Only rows that actually differ are kept.
    """
    out = {}
    for name, a, b in pairs:
        if str(a) == str(b):
            continue
        out.setdefault((str(a), str(b)), []).append(str(name))
    return out


def describe(changes, show=4):
    """One line per distinct change, worst-case names spelled out."""
    lines = []
    for (a, b), names in sorted(changes.items(),
                                key=lambda kv: (-len(kv[1]), kv[0])):
        names = sorted(names)
        shown = ", ".join(names[:show])
        if len(names) > show:
            shown += f" and {len(names) - show} more"
        lines.append(f"{a} -> {b} on {len(names)}: {shown}")
    return "; ".join(lines)


def total(changes):
    return sum(len(v) for v in changes.values())


def systematic(changes):
    """True when every difference is the same substitution.

    A single mapping across many objects is one decision somebody made - or
    one thing the mover did to everything - and reads very differently from a
    handful of objects that each drifted their own way.
    """
    return len(changes) == 1 and total(changes) > 1
