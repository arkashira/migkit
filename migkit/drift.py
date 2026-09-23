"""Did the source's shape change while it was being moved.

A DDL on the source in the middle of a move is invisible until the next
schema check: the rows that moved before it and after it belong to two
different tables, and the check that follows compares them as one. The
paid services watch for it in their change stream. Where migkit reads no
stream, it reads the catalogue before the move and again after, through
the neutral contract every engine that moves data already speaks, and
says what changed.
"""


def shape(engine, side, db):
    """{table: [(column, type), ...]} as the engine sees it now, or None
    when it cannot be read - never an exception: a move is not stopped by
    the thing that watches it. An engine that can read its whole column
    catalogue in one query (`column_catalog`) is asked that way; the rest
    are read table by table.
    """
    try:
        whole = getattr(engine, "column_catalog", None)
        if whole is not None:
            got = whole(side, db)
        else:
            got = {t: sorted((str(n), str(ty)) for n, ty in
                             engine.neutral_columns(side, db, t))
                   for t in engine.neutral_tables(side, db)}
    except Exception:
        return None
    # a table the hop leaves alone is not moved, so its DDL is not the
    # move's business
    return {t: cols for t, cols in got.items()
            if not engine.hop.excluded(db, *str(t).split("."))}


def changes(before, after):
    """What differs between two shapes, one line per table, in words."""
    if before is None or after is None:
        return []
    out = []
    for t in sorted(set(before) - set(after)):
        out.append(f"{t}: dropped")
    for t in sorted(set(after) - set(before)):
        out.append(f"{t}: created")
    for t in sorted(set(before) & set(after)):
        a, b = dict(before[t]), dict(after[t])
        parts = [f"column {c} added" for c in sorted(set(b) - set(a))]
        parts += [f"column {c} dropped" for c in sorted(set(a) - set(b))]
        parts += [f"column {c} {a[c]} -> {b[c]}"
                  for c in sorted(set(a) & set(b)) if a[c] != b[c]]
        if parts:
            out.append(f"{t}: {', '.join(parts)}")
    return out
