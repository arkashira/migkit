"""What `doctor` says of the ways a hop stands on: the way the whole
database moves, the way its changes are followed, and how deeply each
table's changes can be read (backlog P0-DE.2, LT.R1).

Each is read from the decision engine's own ladder, never a second
reading: the move from `movers.for_hop` and `movers.fitted` (the calls
the move makes), the changes from `cli._stream_climb` (the facts the
stream makes), the depth from the engine's `change_depths`. A way is
said with the reason it was taken, the footprint it leaves (`Rung.leaves`,
the words beside each rung; `footprint`, the number, only breaks a tie),
and what the choice stands on again when the move runs.

Nothing here runs a move or writes to a server: the climb asks the servers
read-only questions, and a way that can only prove itself by acting on a
server (`Rung.prove`) or needs a change on the source (`Rung.changes`) is
said to be proved or offered when the move goes. Where a choice could not
be made now, it is said to be made at run time, from the facts it reads.
"""

def rung_named(name):
    """The rung of the whole-database ladder called `name`, or None."""
    from . import movers
    return next((r for r in movers._ladder() if r.name == name), None)


def _leaves(rung):
    """What a way leaves, as a sentence's end; said plainly where the
    rung does not say."""
    if rung is None or not rung.leaves:
        return "it leaves what migkit has not said of it"
    return f"it leaves {rung.leaves}"


def _from(names):
    """The facts a choice reads that a server or the clock gives, in
    words (`factbook`), the ones given by the hop's own settings left
    out."""
    from . import factbook
    out = []
    for n in names:
        f = factbook.fact(n)
        if f is not None and f.how in (factbook.READ, factbook.MEASURED) \
                and f.what not in out:
            out.append(f.what)
    return out


def _later(rung, names):
    """The closing words of a way's line: chosen again when the move
    runs, from what it reads then; and what only the move itself can
    settle."""
    parts = ["chosen again when the move runs, from the servers as they"
             " are then and the rates measured on this hop"]
    read = _from(names)
    if read:
        parts[0] += " (read now: " + "; ".join(read[:5]) + (
            " ..." if len(read) > 5 else "") + ")"
    if rung is not None and rung.prove:
        parts.append("proved on the servers only when the move goes,"
                     " since proving it acts on them")
    if rung is not None and rung.changes:
        parts.append("the change it needs on the source is offered when"
                     " the move goes, never assumed")
    return "; ".join(parts)


def move_line(hop):
    """The line for the whole-database move of `hop`: the way, why, what
    it leaves, and what settles it at run time."""
    from . import movers
    from .engines import ALIASES
    engine = ALIASES.get(hop.engine, hop.engine)
    try:
        base, reason = movers.for_hop(hop, engine)
        via, why = movers.fitted(hop, engine, base)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said, not failed
        what = (str(e).splitlines() or [type(e).__name__])[0][:160]
        return ("whole-database move: no way chosen now (" + what + ");"
                " it is chosen when the move runs, from the programs on"
                " the machine, the servers' settings and the hop's"
                " scope")
    rung = rung_named(via)
    said = rung.said if rung is not None else "a way migkit does not name"
    because = why or reason or (rung.because if rung is not None else "")
    out = f"whole-database move: {said}"
    if because:
        out += f" - {because[:220]}"
    out += f"; {_leaves(rung)}"
    if via == "builtin":
        out += ("; each table's way is chosen when it is copied, from its"
                " size and key and the rates measured on this hop")
    return out + "; " + _later(rung, ())


def follow_lines(hop, eng, stream_climb):
    """The lines for the way `hop`'s changes follow its source.
    `stream_climb` is `cli._stream_climb`, which `move --mode cdc` reads
    its ladder from."""
    from . import capabilities, decide, movers
    gone = capabilities.unavailable(hop.engine, "stream")
    if gone:
        return [f"following changes: no way on {hop.engine} - {gone}"]
    if eng.maps_columns():
        rung = next((r for r in movers.stream_ways()
                     if r.name == "tail"), None)
        return [f"following changes: {rung.said} - the hop maps columns,"
                " which the servers' own replication does not carry;"
                f" {_leaves(rung)}"]
    try:
        facts, rungs, _, work = stream_climb(hop, eng, None, False)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said, not failed
        return [_stream_unasked(hop, e)]
    try:
        got = decide.climb(work, rungs, facts, costs=facts["costs"],
                           prove=False)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - said, not failed
        return [_stream_unasked(hop, e,
                                list(getattr(facts, "_readers", ())))]
    if got.rung is None:
        return [f"following changes: no way is set up on this machine - "
                f"{got.reason[:260]}"]
    out = (f"following changes: {got.rung.said} - {got.reason[:220]}; "
           f"{_leaves(got.rung)}")
    if got.runner_up is not None:
        out += (f"; next: {got.runner_up.said}"
                f" ({got.runner_up.leaves or 'its footprint not said'})")
    return [out + "; " + _later(got.rung, got.asked)]


def _stream_unasked(hop, error, names=()):
    what = (str(error).splitlines() or [type(error).__name__])[0][:140]
    read = _from(names)
    return ("following changes: not chosen now (" + what + "); it is"
            " chosen when the move runs, from the source's version and"
            " settings, what the hop asks of its tables, and the rates"
            " measured on this hop"
            + (f" ({'; '.join(read[:4])})" if read else ""))


def depth_lines(eng):
    """The lines for how deeply each source table's changes can be read
    (`changedepth`): the tables by depth, and for each below `BEST` the
    one thing the engine's code knows that raises it. An engine whose log
    settings migkit does not read is said to be unknown, not guessed."""
    from . import changedepth
    try:
        depths = eng.change_depths()
    except Exception as e:  # noqa: BLE001 - a probe never fails doctor
        what = (str(e).splitlines() or [type(e).__name__])[0][:140]
        return [f"change reading depth: not worked out now ({what})"]
    if depths is None:
        return ["change reading depth: not worked out on this engine -"
                " migkit does not read its log settings, so it names no"
                " depth and no statement that raises one"]
    if not depths:
        return ["change reading depth: no table in scope"]
    return [f"{item} - {detail}" for _, item, detail
            in changedepth.lines(depths)]


def lines(hop, eng, stream_climb, source_reached=True, both=True):
    """Every line for `hop`, each from its own probe: one that cannot be
    made says so and the others stand."""
    out = []
    if both:
        for fn in (lambda: [move_line(hop)],
                   lambda: follow_lines(hop, eng, stream_climb)):
            try:
                out += fn()
            except Exception as e:  # noqa: BLE001 - a probe never fails
                out.append("ways not worked out now: "
                           + (str(e).splitlines() or [""])[0][:120])
    if source_reached:
        out += depth_lines(eng)
    return out
