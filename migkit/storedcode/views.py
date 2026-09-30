"""Views held to the source by their rows, on the hop's own servers.

Reading a view writes nothing, and the hop's own servers hold all the
rows, not a sample: a view is asked there, digested as a table is, and
held to the source's. What a view reads is the tables migkit moved and
checked, so a difference is the view's.
"""
from .prove import Verdict


def prove_views(pair, db):
    views = {pair._leaf(n): n for n, _ in
             pair.src_engine.neutral_views("src", db)}
    if not views:
        return []
    there = {pair._leaf(v) for v, _ in pair.dst_engine.neutral_views("dst",
                                                                     db)}
    out = []
    for name in views:
        said = f"view {name}"
        if name not in there:
            out.append(Verdict(said, "missing", "not on the target yet"))
            continue
        sc, dc, _ = pair._comparable_columns(db, pair.src_engine,
                                             views[name], pair.dst_engine,
                                             name)
        if not sc:
            out.append(Verdict(said, "not proved", "none of its columns can"
                               " be compared across the two engines"))
            continue
        try:
            a = pair.src_engine.neutral_digest("src", db, views[name], sc)
        except Exception as e:  # noqa: BLE001 - said, as not proved
            out.append(Verdict(said, "not proved", "it could not be read on"
                               " the source:"
                               f" {(str(e).splitlines() or [''])[0][:80]}"))
            continue
        try:
            b = pair.dst_engine.neutral_digest("dst", db, name, dc)
        except Exception as e:  # noqa: BLE001 - answered by failing
            b = ("failed", (str(e).splitlines() or [""])[0])
        if a == b:
            out.append(Verdict(said, "ok", "its rows digest the same on"
                               " both sides", calls=1,
                               coverage="every row it answers"))
        else:
            out.append(Verdict(said, "diff", "its rows differ", calls=1))
    return out
