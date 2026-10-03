"""The interlock on the target (backlog R23, 3): before a step writes to
a target - a copy, a stream, a repair, a cutover, users, a converted
schema, a restore, a stream back - migkit reads what each side says it is,
shows both in the plan, and does not write where:

* the target is the source itself - the same server by its own words
  and the same database, or the same address and database: a hop with its
  sides pasted the wrong way round would empty the source's tables to copy
  them into themselves;
* the target is not the server the plan showed: what it says of itself is
  kept with the plan and asked again at the start of every write step, so
  a DNS switch, a failover or an edited hop between the two is seen
  before a row lands. A restart reads as another server too - the
  process answering is part of what is kept - and showing the plan again
  takes the one there now;
* it matches one of the hop's `production` host patterns and the hop does
  not name it as its target:

    options:
      production: ["*.prod.example.com", "10.0.9.*"]
      production_target: db1.prod.example.com   # this one, on purpose

* a copy would write tables that already hold rows and the hop has not
  said what to do with them:

    options:
      target_rows: refuse    # the default: nothing is written, the tables
                             # are named
      target_rows: replace   # the copy writes them as it always has - what
                             # they held is removed first
      target_rows: keep      # they are left as they are and not copied

Rows this hop wrote into the same target on an earlier run - a move
stopped and run again - are its own and carry on as before: kept beside
the target's identity is the list of tables it left as they were, and a
table holding rows outside that list, on the same target, is one it wrote.
"""
import fnmatch
import hashlib
import json
import time

FILE = "target-identity.json"
CHOICES = ("refuse", "replace", "keep")
#: what says which server a side is, kept with the plan and held to at
#: every write step; the version and the size are shown, not held - a
#: write changes the size, and the plan says what it was
FIXED = ("engine", "address", "database", "id", "running", "name")
#: what says whose rows a table holds: the same data on the same server
#: through a restart or a failover to its own replica, where the plan's
#: `FIXED` asks for the plan to be shown again
LASTING = ("engine", "address", "database", "id")
_LOOPBACK = ("localhost", "127.0.0.1", "::1", "[::1]")


def side_engine(eng, side):
    return getattr(eng, "src_engine" if side == "src" else "dst_engine",
                   None) or eng


def _address(ep):
    o = ep.options or {}
    named = o.get("path") or o.get("url")
    if named:
        return str(named)
    hosts = o.get("hosts")
    if hosts:
        return hosts if isinstance(hosts, str) else ",".join(map(str, hosts))
    return f"{ep.host}:{ep.port}" if ep.port else str(ep.host)


def _plain(address):
    """An address as one server is reached: host names in one case, the
    loopback under one name, several hosts in one order, a file by where
    it really is."""
    import os
    if str(address).startswith(("/", "~", ".")):
        return os.path.realpath(os.path.expanduser(str(address)))
    out = []
    for part in str(address).split(","):
        part = part.strip().lower()
        host, sep, port = part.rpartition(":")
        if not sep or not port.isdigit():
            host, port = part, ""
        out.append(f"{'loopback' if host in _LOOPBACK else host}:{port}")
    return ",".join(sorted(out))


def _unreached(hop, side, ep):
    """Why nothing answers at the side's address, or None. Asked once a
    run, with a short wait: a plan does not wait on a server that is not
    there. Measured on this machine, a connect to an address nothing
    answers at waited the whole 20 seconds it was given; an engine's own
    connection waits 15 and tries again."""
    import socket
    said = hop.__dict__.setdefault("_interlock_unreached", {})
    if side not in said:
        o = ep.options or {}
        said[side] = None
        if ep.host and ep.port and not (o.get("path") or o.get("url")
                                        or o.get("hosts")):
            try:
                socket.create_connection((ep.host, int(ep.port)),
                                         timeout=5).close()
            except OSError as e:
                said[side] = (f"nothing answers at {ep.host}:{ep.port}"
                              f" ({e or type(e).__name__})")
    return said[side]


def reached(hop):
    """Whether something answers at both sides' addresses (asked once a
    run): where one does not, nothing is listed from it for the copy to
    be checked against - the step says why it stops soon enough."""
    return not any(_unreached(hop, side, ep) for side, ep in
                   (("src", hop.source), ("dst", hop.target)))


def identity(hop, eng, side, db):
    """Where the hop says a side is, and what its server says it is."""
    ep = hop.source if side == "src" else hop.target
    e = side_engine(eng, side)
    named = getattr(e, "side_database", None)
    # the database the engine's connections go to, not the name the hop
    # maps it to: PostgreSQL never maps its maintenance database, and a
    # label taken from the hop called the source itself another database
    database = (named(side, db) if named and db else
                hop.target_db(db) if side == "dst" and db else db)
    out = {"engine": getattr(e, "CANON_ENGINE", "")
           or type(e).__name__.removesuffix("Engine").lower(),
           "address": _address(ep), "database": str(database)}
    ask = getattr(e, "server_identity", None)
    try:
        away = _unreached(hop, side, ep) if ask else None
        said = {"unread": away} if away else (ask(side, db) if ask else {})
    except (Exception, SystemExit) as ex:  # noqa: BLE001 - shown as is
        said = {"unread": (str(ex).strip().splitlines() or
                           [type(ex).__name__])[0][:160]}
    said = dict(said or {})
    if ask and "unread" not in said:
        # which managed service runs it, by its address and its own word
        # (`clouds`): shown, not held - the server's id says which it is
        from . import clouds
        try:
            where = clouds.of(e, side, ep)
        except Exception:  # noqa: BLE001 - the plan goes on without it
            where = None
        if where is not None and where.cloud:
            said["service"] = where.label
    out.update({k: v for k, v in said.items() if v not in (None, "", {})})
    return out


def fingerprint(ident, keys=FIXED):
    basis = {k: ident.get(k) for k in keys}
    return hashlib.sha256(json.dumps(basis, sort_keys=True, default=str)
                          .encode()).hexdigest()[:16]


def itself(src, dst):
    """Why the target is the source itself, or None."""
    if src["engine"] != dst["engine"] or src["database"] != dst["database"]:
        return None
    if (src.get("id") or src.get("running")) and \
            all(src.get(k) == dst.get(k) for k in ("id", "running")):
        return ("it is the same server by what the server says of itself"
                f" ({_who(dst, ('id', 'running'))}), and the same database"
                f" {dst['database'] or '(the default)'}")
    if _plain(src["address"]) == _plain(dst["address"]):
        return (f"both are reached at {dst['address']}, database"
                f" {dst['database'] or '(the default)'}")
    return None


def _list(value):
    if not value:
        return []
    return [str(v) for v in (value if isinstance(value, (list, tuple))
                             else [value])]


def _names(hop, dst):
    """Every name the target answers to: its address as the hop gives it,
    what the server calls itself, and - where the side names it - its
    instance at its provider (`managed: {id: ...}`), which a managed
    service's private address does not carry."""
    from urllib.parse import urlsplit
    ep = hop.target
    o = ep.options or {}
    names = [ep.host]
    names += [h.rsplit(":", 1)[0] for part in _list(o.get("hosts"))
              for h in part.split(",")]
    if o.get("url"):
        names.append(urlsplit(str(o["url"])).hostname or "")
    names += [str(o.get("path") or ""), str(dst.get("name") or "")]
    managed = o.get("managed")
    if isinstance(managed, dict) and managed.get("id"):
        names.append(str(managed["id"]))
    return [n.strip().rstrip(".") for n in dict.fromkeys(names)
            if n and n.strip()]


def production(hop, dst):
    """(a name of the target that matches the hop's `production` list,
    the pattern, whether the hop names it as its target), or None."""
    opts = hop.options or {}
    patterns = _list(opts.get("production"))
    names = _names(hop, dst)
    hit = next(((n, p) for p in patterns for n in names
                if fnmatch.fnmatch(n.lower(), p.lower())), None)
    if hit is None:
        return None
    named = {n.lower() for n in _list(opts.get("production_target"))}
    return hit[0], hit[1], any(n.lower() in named for n in names)


def _who(ident, keys=None):
    got = []
    for k in keys or FIXED:
        v = ident.get(k)
        if isinstance(v, dict):
            got += [f"{a} {b}" for a, b in sorted(v.items())]
        elif v not in (None, ""):
            got.append(f"{'process started' if k == 'running' else k} {v}")
    return ", ".join(got)


def _line(role, ident):
    from .wording import human_bytes
    head = " ".join(x for x in (ident["engine"], str(ident.get("version")
                                                     or "")) if x)
    head += f" at {ident['address']}"
    if ident["database"]:
        head += f", database {ident['database']}"
    bits = [f"{a} {b}" for a, b in sorted((ident.get("id") or {}).items())]
    if ident.get("service"):
        bits.append(ident["service"])
    if ident.get("name") and ident["name"] not in ident["address"]:
        bits.append(f"answering as {ident['name']}")
    if ident.get("absent"):
        bits.append("the database is not there yet")
    elif ident.get("bytes") is not None:
        bits.append(human_bytes(ident["bytes"]))
    if ident.get("unread"):
        bits.append(f"it could not be asked more: {ident['unread']}")
    return f"{role}: {head}" + (f" ({'; '.join(bits)})" if bits else "")


def _path(hop, db):
    return (hop.report_dir(db) if db else hop.report_dir()) / FILE


def _mirror(hop, path):
    """The file through the hop's run state, where it has one: machines
    sharing a move read what the first one kept."""
    from .state import Mirror, run_state
    remote = run_state(hop)
    return Mirror(remote, [path]) if remote is not None else None


def _load(hop, db):
    path = _path(hop, db)
    shared = _mirror(hop, path)
    if shared:
        shared.fetch()
    try:
        return json.loads(path.read_text()), path
    except (OSError, ValueError):
        return {}, path


def _save(hop, rec, path):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(rec, indent=1, default=str))
    tmp.replace(path)
    shared = _mirror(hop, path)
    if shared:
        shared.push(force=True)


def _once(hop, what):
    """True the first time this run of the hop says `what`."""
    said = hop.__dict__.setdefault("_interlock_said", set())
    if what in said:
        return False
    said.add(what)
    return True


def identities(hop, eng, db):
    """(the source's identity, the target's) for `db`, asked once a run of
    the engine: a `move --go` asked both servers who they were, and the
    cloud they run in, three times a database - its guard, its copy's
    rows, its stream. Kept on the engine, which a run makes afresh."""
    from concurrent.futures import ThreadPoolExecutor

    from . import session
    asked = eng.__dict__.setdefault("_interlock_asked", {})
    key = (hop.name, db, session.current())
    got = asked.get(key)
    if got is None:
        # both at once: a side nothing answers at costs one wait, not two
        with ThreadPoolExecutor(2) as pool:
            got = tuple(pool.map(lambda side: identity(hop, eng, side, db),
                                 ("src", "dst")))
        asked[key] = got
    return got


def before_write(hop, eng, db, go, log):
    """Every write step starts here: both sides' identities, said once per
    run; the target held to the source, to the hop's production list and
    to what the plan showed. With `go`, a step that must not write stops
    before it does; without, the plan says so and keeps what it showed.
    Returns the target's identity and its fingerprint."""
    src, dst = identities(hop, eng, db)
    where = db or hop.name
    if _once(hop, ("shown", db)):
        log(_line("source", src))
        log(_line("target", dst))
    wrong = []
    same = itself(src, dst)
    if same:
        wrong.append(f"the target is the source itself: {same}. Writing"
                     " would copy the source over itself. Point the hop's"
                     " target at the server and database the rows go to")
    prod = production(hop, dst)
    if prod and not prod[2]:
        wrong.append(f"the target looks like production: {prod[0]} matches"
                     f" {prod[1]} in the hop's production list, and the hop"
                     " does not name it as its target. To write to it on"
                     " purpose, add production_target: "
                     f"{prod[0]} to the hop's options")
    elif prod and _once(hop, ("production", db)):
        log(f"the target is production ({prod[0]} matches {prod[1]}), and"
            " the hop names it as its target")
    rec, path = _load(hop, db)
    fp = fingerprint(dst)
    shown = rec.get("plan") or {}
    moved = bool(shown) and shown.get("fingerprint") != fp
    if go:
        if wrong:
            raise SystemExit(f"{where}: nothing was written - "
                             + "; ".join(wrong) + ".")
        if moved:
            raise SystemExit(
                f"{where}: nothing was written - the target is not the"
                " server the plan showed. Then: "
                f"{_who(shown.get('target') or {})}; now: {_who(dst)}. A"
                " DNS switch, a failover, a restart or an edited hop sends"
                " the write somewhere nobody looked at. Show the plan again"
                " (the same command without --go or --apply) to see this"
                " target; writing is then held to that one.")
        if shown:
            return dst, fp
    else:
        if _once(hop, ("plan", db)):
            for w in wrong:
                log(f"with --go this stops: {w}")
            if moved:
                log("this is not the target the last plan or write saw ("
                    f"{_who(shown.get('target') or {})}); writing is held to"
                    " this one from now on")
    rec["plan"] = {"fingerprint": fp, "target": {k: dst.get(k)
                                                   for k in FIXED},
                   "at": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "shown": not go}
    _save(hop, rec, path)
    return dst, fp


def choice(hop):
    got = str((hop.options or {}).get("target_rows", "") or "").lower()
    if got and got not in CHOICES:
        raise SystemExit(f"target_rows: {got} - {', '.join(CHOICES)}")
    return got or "refuse"


def _named(t):
    return ".".join(p for p in t if p)


def whose_rows(hop, eng, d, tables, dst):
    """(the tables holding rows this hop did not write, the tables holding
    rows an earlier run of it wrote into this same target) of `tables`, or
    None where the target cannot be asked. Reads only."""
    tables = [tuple(t) for t in tables if not hop.excluded(d, *t)]
    held = None if dst.get("unread") else eng.rows_held(d, tables)
    if held is None:
        return None
    rec, _ = _load(hop, d)
    wrote = rec.get("wrote") or {}
    ours = wrote.get("fingerprint") == fingerprint(dst, LASTING)
    kept_then = {tuple(t) for t in wrote.get("kept") or []}
    theirs = {t for t in held if not ours or t in kept_then}
    return theirs, set(held) - theirs


def refusals(hop, eng, d, tables=None):
    """[why a write step into `d` would stop] - the target the source
    itself, production the hop does not name, (for a copy of `tables`)
    rows it did not write that the hop does not say what to do with, and
    a target the hop's own `protect_target` asks to keep from the
    application that nothing here keeps - asked the way `before_write`,
    `before_copy` and `freeze.protect` ask, and nothing kept or said: what
    `ready` says before anything runs."""
    src, dst = identities(hop, eng, d)
    out = []
    same = itself(src, dst)
    if same:
        out.append(f"the target is the source itself: {same}")
    prod = production(hop, dst)
    if prod and not prod[2]:
        out.append(f"the target looks like production: {prod[0]} matches"
                   f" {prod[1]} in the hop's production list, and the hop"
                   " does not name it as its target (production_target)")
    if tables is not None and choice(hop) == "refuse":
        got = whose_rows(hop, eng, d, tables, dst)
        theirs = got[0] if got else set()
        if theirs:
            names = sorted(_named(t) for t in theirs)
            out.append(f"{len(theirs)} tables the copy writes already hold"
                       " rows on the target: " + ", ".join(names[:8])
                       + (" ..." if len(names) > 8 else "")
                       + " - and the hop does not say what to do with them"
                       " (target_rows: replace or keep)")
    from . import freeze
    why = freeze.not_protected(hop, eng, d)
    if why:
        out.append(f"protect_target is set, and {why}")
    return out


def before_copy(hop, eng, d, tables, go, log, whole=False):
    """`before_write` for a copy of `d` into the target, and the rows the
    target already holds in the tables it writes: the tables the hop keeps
    as they are (`target_rows: keep`), for the copy to leave out.

    `whole`: the server's own copy writes every table at once and cannot
    leave one out - a replica, a subscription."""
    dst, _ = before_write(hop, eng, d, go, log)
    decided = hop.__dict__.setdefault("_interlock_kept", {})
    if d in decided:
        # asked once a run: what it keeps is left out of the hop from then
        # on, and asking again would find nothing to keep
        return decided[d]
    fp = fingerprint(dst, LASTING)
    tables = [tuple(t) for t in tables if not hop.excluded(d, *t)]
    got = whose_rows(hop, eng, d, tables, dst)
    if got is None:
        if _once(hop, ("rows unknown", d)):
            log("whether the target already holds rows in the tables this"
                " copy writes could not be asked here")
        return set()
    theirs, mine = got
    rec, path = _load(hop, d)
    how = choice(hop)
    kept = set()
    if theirs:
        names = sorted(_named(t) for t in theirs)
        said = (f"{len(theirs)} of the {len(tables)} tables this copy"
                f" writes already {'holds' if len(theirs) == 1 else 'hold'}"
                " rows on the target: "
                + ", ".join(names[:8]) + (" ..." if len(names) > 8 else ""))
        if how == "refuse":
            ask = (f"{d}: {said}. Nothing was written. Say in the hop's"
                   " options what to do with them - target_rows: replace"
                   " (what they hold is removed and the source's rows"
                   " written)"
                   + ("" if whole else ", target_rows: keep (they are left"
                      " as they are and not copied)")
                   + " - or empty them, or point the hop at another target")
            if go:
                raise SystemExit(ask)
            if _once(hop, ("rows", d)):
                log(f"with --go this stops: {said}; the hop's options do not"
                    " say what to do with them (target_rows)")
        elif how == "keep" and whole:
            cannot = (f"{said}. target_rows: keep leaves them out, and the"
                      " server's own copy writes every table and cannot"
                      " leave one out")
            if go:
                raise SystemExit(f"{d}: {cannot}. Nothing was written. Use"
                                 " target_rows: replace, or empty them")
            if _once(hop, ("rows", d)):
                log(f"with --go this stops: {cannot}")
        elif how == "keep":
            kept = theirs
            if _once(hop, ("rows", d)):
                log(f"{said}; kept as they are and not copied (target_rows:"
                    " keep)")
        else:
            if _once(hop, ("rows", d)):
                log(f"{said}; what they hold is removed before the source's"
                    " rows are written (target_rows: replace)")
            if go and whole and not getattr(side_engine(eng, "dst"),
                                            "REPLICATES_THE_SERVER", False):
                # the server's own copy adds to a table and removes
                # nothing, where the table copier empties first
                target = side_engine(eng, "dst")
                for t in sorted(theirs):
                    gone = target.neutral_empty("dst", d, _named(t))
                    log(f"{_named(t)}: removed {gone:,} rows the target"
                        " held before the copy")
    if mine and _once(hop, ("mine", d)):
        log(f"{len(mine)} tables hold rows an earlier run of this hop wrote"
            " into this same target; the copy carries on with them")
    if go:
        rec["wrote"] = {"fingerprint": fp, "kept": sorted(kept),
                        "at": time.strftime("%Y-%m-%d %H:%M:%S")}
        _save(hop, rec, path)
    decided[d] = kept
    return kept


def leave_out(hop, d, kept):
    """The tables the hop keeps, left out of this run's copy of `d` the way
    the hop's own `exclude` leaves a table out, which every mover reads."""
    if kept:
        hop.exclude = list(hop.exclude or []) + [
            _named((d, *t)) for t in sorted(kept)]
