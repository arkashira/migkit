"""Getting the indexes out of the way of a bulk load, and putting them back.

An index on a table being loaded is maintained one row at a time. Built after
the rows are already there, it is a sort - which is a different amount of
work, not a smaller amount of the same work.

Measured on PostgreSQL 16, 300,000 rows into a table with three secondary
indexes, on the same two-CPU machine:

    indexes present during the load     1.241s
    load into a bare table              0.230s
    build the three indexes afterwards  0.422s
                                        -----
                                        0.652s   (1.9x)

The gap widens with more indexes and more rows, because per-row maintenance
scales with both while a bulk build amortises the sort.

**The dangerous part is not the speed, it is the window.** Between the drop
and the rebuild the target has no indexes, and if the process dies there the
table is left slow and - worse - a unique index that was enforcing something
is simply gone. So the order here is fixed and not negotiable:

1. read the definitions and **write them to disk**
2. only then drop
3. load
4. rebuild, whether or not the load succeeded

Step 1 before step 2 is what makes step 4 possible from a different process,
an hour later, by a person. The file is the point.

**Constraints are never touched.** A primary key or a unique constraint is
enforcing something, not just making a query fast, and dropping one changes
what the database will accept - possibly while a foreign key elsewhere depends
on it. Only indexes that back no constraint are moved out of the way, which is
the majority of them and all of the ones that cost the most to maintain.
"""


def plan(rows):
    """(to_drop, ddl) from (name, definition, is_constraint) triples.

    An index whose definition could not be read is left alone: dropping
    something migkit cannot recreate is the one mistake this file must never
    make.
    """
    to_drop, ddl = [], {}
    for name, definition, is_constraint in rows:
        if is_constraint:
            continue
        if not name or not definition:
            continue
        to_drop.append(str(name))
        ddl[str(name)] = str(definition)
    return sorted(to_drop), ddl


def saved(path, ddl):
    """Write the definitions where a human can find them, and confirm it.

    Returns True only when the file is on disk and reads back with every
    definition in it. A caller that gets False must not drop anything.
    """
    import json
    import os
    import pathlib
    import tempfile
    if not ddl:
        return True
    path = pathlib.Path(path)
    try:
        # inside the guard: a caller that gets an exception here instead of
        # False may not handle it, and the whole contract is "could not save,
        # so do not drop"
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    except Exception:
        return False
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(ddl, f, indent=1, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False
    try:
        back = json.loads(path.read_text())
    except Exception:
        return False
    return back == ddl


def restore_from(path):
    """Definitions written by an earlier run, for rebuilding by hand."""
    import json
    import pathlib
    p = pathlib.Path(path)
    if not p.exists():
        return {}
    try:
        got = json.loads(p.read_text())
        return got if isinstance(got, dict) else {}
    except Exception:
        return {}


def summary(dropped, rebuilt, failed):
    """One line for the log, naming anything that did not come back."""
    if not dropped:
        return "no secondary indexes to move out of the way"
    if failed:
        return (f"{len(rebuilt)} of {len(dropped)} indexes rebuilt;"
                f" STILL MISSING: {', '.join(sorted(failed))}"
                f" - the definitions are on disk, recreate them before the"
                f" target is used")
    return f"{len(dropped)} indexes dropped for the load and rebuilt after"
