"""Where a quiet tail stands, from what a following engine's server
answers in one bounded round: the helpers an engine's `tail_where` is
built on. Below the engines (the workflow that prints the quiet line,
`tailctl`, reads these through its own names), so an engine calling them
imports nothing above itself."""


def log_moved(memo, token, wrote):
    """How far a counter the source keeps of its own log (bytes written,
    entries appended) moved since `token` was first asked about - the
    `moved` of a `tail_where` whose position cannot itself be measured in
    the log's bytes. `memo` is the engine's own dict for that database;
    a new token starts it again. None where the counter went back (the
    server restarted): what it wrote since then is not known."""
    import json
    key = json.dumps(token, sort_keys=True, default=str)
    if memo.get("token") != key or wrote is None:
        memo.update(token=key, base=wrote)
        return 0 if wrote is not None else None
    if wrote < memo["base"]:
        memo.update(base=wrote)
        return None
    return wrote - memo["base"]


#: what a quiet line says of a source that keeps no reader of the tail's
#: it could list: attached is not read, rather than assumed
NO_READER = "the source lists no reader of the tail's to ask"


def where_from(eng, db, token, wrote=None, unit=None, unread=NO_READER):
    """A `tail_where` from what a following engine's server answers in one
    bounded round, for a source with no slot or reader of the tail's to
    list: the position the tail is at, the source's now (its own
    `log_position`), how far a counter the source keeps of its own log or
    writes moved since the batch that last carried a change on the hop's
    tables (`wrote()`, through `log_moved`; `unit` names what it counts) -
    unread (None), never 0, where the server keeps no such counter or
    refuses it - and why the source lists no reader to ask (`unread`,
    `attached_unread`), so attached is never assumed. A position the
    server does not answer is detached, and why."""
    import json

    def said(v):
        return v if isinstance(v, str) else json.dumps(
            v, sort_keys=True, default=str)
    from .engines.base import Engine
    out = {"at": "" if token is None else said(token)}
    # the base engine's placeholder answers None: nothing to say of it
    if getattr(type(eng), "log_position", None) not in (
            None, Engine.log_position):
        try:
            now = eng.log_position("src", db)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said
            return {**out, "attached": False, "why_detached": (str(
                e).strip().splitlines() or [""])[0][:120]}
        if now is not None:
            out["server"] = said(now)
    if wrote is not None:
        try:
            count = wrote()
            count = None if count is None else int(count)
        except Exception:  # noqa: BLE001 - not granted: the move is unread
            count = None
        memo = eng.__dict__.setdefault("_tail_moved", {}).setdefault(db, {})
        # counted from the hop's last carried change, not from the
        # position: a tail whose position is a clock moves it while quiet
        since = eng.__dict__.get("_tail_carried", {}).get(db, 0)
        out["moved"] = log_moved(memo, {"carried": since}, count)
        if unit:
            out["moved_unit"] = unit
    else:
        out["moved"] = None
    if unread:
        out["attached_unread"] = unread
    return out
