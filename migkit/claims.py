"""One table across machines: its ranges handed out through one record
each machine writes only over the version it read (backlog W1; R5
looked at this and left it).

A hop that says `share_tables: ranges` is moved by every machine that
runs it at once. A table leased whole goes to one machine at a time, as
`share_tables: true` did; a table large enough to be split (one integer
key, `hetero._splits`) is split once, by the first machine to reach it,
and its ranges are claimed one at a time by every machine's workers:

    todo      not copied, or given back
    claimed   one machine's worker copies it, under a number of its own
              and a term the machine renews every third of it; a claim
              not renewed within its term is claimable again, and the
              next claim of it has a higher number
    done      copied and read back, by the claim that holds it

Every change of the record is written only over the version read
(`shared`), so two workers never claim one range, and `done` is
accepted only from the claim that holds the range: a worker that lost
its claim - its machine paused past the term, and another took it - is
told so and its copy is not counted. It could not have written anything
that stays: each range is written under its claim's number, held by the
target in the range's own row (`fencing`, raised at the claim's first
write, which empties the range's span on the target before copying it),
so once a newer claim has written, the older one's transactions are
refused there. The range state machine is idempotent: a range copied
twice empties and copies its own span, and ends as the source's rows.

The first machine to find every range done finishes the table - the
target's rows outside the source's whole range removed, the table's
checkpoint entry written with every range, the mark each was read under
and who copied it - and only that one (`finish`, one more conditional
write).

How many workers a machine runs is the decision engine's, as it is for
one machine (`sizing.estimate` over the resource model, `model`): each
server's and link's rate divided among the machines that share the
table, this machine's own CPUs, memory and network whole. A machine
sizes itself again every third of the term from the machines it finds,
so one that joins later takes its share from the others.
"""
import copy as _copy
import os
import socket
import threading
import time

from . import failpoint, fencing, shared

TODO, CLAIMED, DONE = "todo", "claimed", "done"
#: the index of the tables a hop's machines share, per database
INDEX = "ranges/index.json"


class Lost(Exception):
    """This worker's claim on a range was taken by a newer one."""


def shared_ranges(hop):
    """Whether this hop's machines share a large table's ranges."""
    return str((hop.options or {}).get("share_tables") or "").lower() \
        == "ranges"


def path(hop, db, key):
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in key)
    return hop.report_dir(db) / "ranges" / f"{safe}.json"


def range_row(hop, key, after):
    """The target's row a range is held to."""
    import hashlib
    name = f"{hop.name}#r:{key}:{after}"
    if len(name) > shared.NAME_MOST:
        name = (f"{hop.name[:100]}#r:"
                + hashlib.sha256(name.encode()).hexdigest()[:40])
    return name


# ---- the record's changes, one function each (so the tests can drive
# every interleaving of them) ------------------------------------------------

def planned(rec, key, lo, hi, spans, way="ranges"):
    """The record of a table, made by the first machine to reach it:
    `spans` its ranges. A record already there stands."""
    if rec and rec.get("ranges") is not None:
        return rec
    return {"key": key, "lo": lo, "hi": hi, "way": way,
            "ranges": [[int(a), int(u)] for a, u in spans],
            "state": {str(int(a)): {"state": TODO, "claims": 0}
                      for a, _ in spans},
            "numbers": 0, "machines": {}, "finished": None, "lapsed": []}


def _held_by(st, me, number):
    return (st.get("state") == CLAIMED and st.get("by") == me
            and int(st.get("number") or -1) == int(number))


def claim(rec, me, now, term, host="", pid=0):
    """(the record, (after, upto, number)) for the first range this
    machine may take - one not claimed, or whose claim has lapsed - or
    (the record, None) where every range is done or claimed."""
    rec = _copy.deepcopy(rec)
    for a, u in rec["ranges"]:
        st = rec["state"][str(a)]
        if st["state"] == DONE:
            continue
        if st["state"] == CLAIMED and float(st.get("expires") or 0) > now:
            continue
        if st["state"] == CLAIMED:
            rec["lapsed"].append({"after": a, "by": st.get("by"),
                                  "host": st.get("host"),
                                  "number": st.get("number"), "at": now})
        rec["numbers"] = int(rec.get("numbers") or 0) + 1
        st.update(state=CLAIMED, by=me, host=host, pid=pid,
                  number=rec["numbers"], expires=now + term,
                  claims=int(st.get("claims") or 0) + 1, at=now)
        st.pop("mark", None)
        return rec, (a, u, rec["numbers"])
    return rec, None


def renew(rec, me, held, now, term, workers=None):
    """This machine's claims (`held`, {after: number}) given another term,
    and its line in the machines' record moved on."""
    rec = _copy.deepcopy(rec)
    for a, n in held.items():
        st = rec["state"].get(str(a))
        if st is not None and _held_by(st, me, n):
            st["expires"] = now + term
    line = dict((rec.get("machines") or {}).get(me) or {"since": now})
    line.update(beat=now, host=socket.gethostname(), pid=os.getpid())
    if workers is not None:
        line["workers"] = workers
    rec.setdefault("machines", {})[me] = line
    return rec


def started(rec, me, after, number, mark):
    """The source's mark taken just before the range is read, kept with
    its claim (`ranges.started`); `Lost` where the claim is not this
    worker's any more."""
    rec = _copy.deepcopy(rec)
    st = rec["state"][str(after)]
    if not _held_by(st, me, number):
        raise Lost(after)
    st["mark"] = mark
    return rec


def done(rec, me, after, number, rows, now):
    """The range copied, from the claim that holds it; `Lost` from any
    other."""
    rec = _copy.deepcopy(rec)
    st = rec["state"][str(after)]
    if not _held_by(st, me, number):
        raise Lost(after)
    st.update(state=DONE, rows=int(rows or 0), done_at=now)
    st.pop("expires", None)
    return rec


def give_back(rec, me, after, number):
    """A range this worker could not copy, claimable again at once."""
    rec = _copy.deepcopy(rec)
    st = rec["state"][str(after)]
    if _held_by(st, me, number):
        st.update(state=TODO)
        st.pop("expires", None)
    return rec


def left(rec):
    return sum(1 for st in rec["state"].values() if st["state"] != DONE)


def finish(rec, me, now):
    """(the record, True) where this machine is the one to finish the
    table - every range done, and no machine finished it yet."""
    if not rec or rec.get("finished") or left(rec):
        return rec, False
    rec = _copy.deepcopy(rec)
    rec["finished"] = {"by": me, "host": socket.gethostname(),
                       "pid": os.getpid(), "at": now}
    return rec, True


# ---- one machine's part -----------------------------------------------------

class Table:
    """A table's record in the machines' store, every change of it one
    of the functions above, written only over the version read."""

    def __init__(self, store, where, me, term):
        self.store, self.where, self.me, self.term = store, where, me, term

    def _do(self, fn):
        from . import ha
        from . import lease as _lease
        out = {}

        def change(have):
            got = fn(have)
            if isinstance(got, tuple):
                got, out["value"] = got
            else:
                out["value"] = None
            # nothing changed: nothing written, and no machine's write
            # waits on this one
            return _lease.KEEP if got == have else got
        rec = ha._change(self.store, self.where, change)
        return rec, out.get("value")

    def read(self):
        return self.store.read_record(self.where, sealed=True)[0]

    def plan(self, key, lo, hi, spans_fn, way="ranges"):
        have = self.read()
        if have and have.get("ranges") is not None:
            return have
        spans = spans_fn()
        return self._do(lambda rec: planned(rec, key, lo, hi, spans, way))[0]

    def claim(self):
        return self._do(lambda rec: claim(
            rec, self.me, time.time(), self.term, socket.gethostname(),
            os.getpid()))[1]

    def renew(self, held, workers=None):
        return self._do(lambda rec: renew(rec, self.me, held, time.time(),
                                          self.term, workers))[0]

    def started(self, after, number, mark):
        self._do(lambda rec: started(rec, self.me, after, number, mark))

    def done(self, after, number, rows):
        self._do(lambda rec: done(rec, self.me, after, number, rows,
                                  time.time()))

    def give_back(self, after, number):
        self._do(lambda rec: give_back(rec, self.me, after, number))

    def finish(self):
        return self._do(lambda rec: finish(rec, self.me, time.time()))


def way_of(hop, db, key):
    """"ranges" or "whole": how the hop's machines share the table, as
    the first one to reach it decided (`way`); None where they do not
    share its ranges, or nobody decided yet."""
    if not shared_ranges(hop):
        return None
    store = shared.store_for(hop)
    rec, _ = store.read_record(path(hop, db, key), sealed=True)
    return (rec or {}).get("way")


def way(hop, db, key, store, splits):
    """How the table is shared, decided once - by the first machine to
    reach it, `splits()` answering whether its ranges are - and kept, so
    every machine takes it the same way even where the table grew past
    the line between one machine and another's asking."""
    from . import ha
    table = Table(store, path(hop, db, key), ha.mover(), ha.term())
    have = table.read()
    if have and have.get("way"):
        return have["way"]
    chosen = "ranges" if splits() else "whole"

    def decide(rec):
        if rec and rec.get("way"):
            return rec
        if chosen == "whole":
            return planned(None, key, None, None, [[0, 0]], "whole")
        return {"key": key, "way": "ranges"}
    got, _ = table._do(decide)
    _index(hop, db, store, key)
    return got["way"]


def whole(hop, db, key, store, run, log):
    """A table leased whole to one machine at a time through its record -
    one range, claimed, renewed, taken again once it lapses - and copied
    by `run()` under the claim's number (`fencing`), which returns the
    rows it copied. False where another machine holds it, it is done, or
    this machine lost it on the way."""
    from . import ha
    term = ha.term()
    me = ha.mover()
    table = Table(store, path(hop, db, key), me, term)
    got = table.claim()
    if got is None:
        return False
    a, _, n = got
    stop = threading.Event()

    def renewing():
        while not stop.wait(term / 3):
            try:
                table.renew({a: n})
            except Exception:  # noqa: BLE001 - the next turn tries again
                continue
    beat = threading.Thread(target=renewing, daemon=True)
    beat.start()
    token = fencing.arm(hop, fencing.Token(range_row(hop, key, "whole"), n,
                                           me, store.ident(), lazy=True))
    try:
        moved = run()
    except (Lost, fencing.Fenced) as e:
        log(f"{key}: another machine took the table over ({_said(e)})")
        return False
    except BaseException:
        try:
            table.give_back(a, n)
        except Exception:  # noqa: BLE001 - it lapses anyway
            pass
        raise
    finally:
        fencing.disarm(hop, token)
        stop.set()
        beat.join(timeout=10)
    try:
        table.done(a, n, moved)
    except Lost:
        log(f"{key}: copied, and another machine had taken it over by then;"
            " its copy stands")
        return False
    table.finish()
    return True


def _said(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[0][:200]


def _index(hop, db, store, key):
    from . import ha
    from . import lease as _lease

    def add(have):
        have = dict(have or {"keys": []})
        if key in have["keys"]:
            return _lease.KEEP
        have["keys"] = have["keys"] + [key]
        return have
    ha._change(store, hop.report_dir(db) / INDEX, add, sealed=False)


def machines(rec, now, term):
    """The machines that renewed their line in the record within three
    terms."""
    return {k: v for k, v in (rec.get("machines") or {}).items()
            if now - float(v.get("beat") or 0) < 3 * term}


def per_machine(model, sides, n):
    """(the model, the servers' capacities) as one machine of `n`
    sharing the servers sees them: every server's and link's rate, its
    free connections and CPUs divided among the machines, this machine's
    own CPUs, memory and network whole."""
    from . import model as resource_model
    n = max(int(n or 1), 1)
    m = None
    if model is not None:
        d = model.to_dict()
        for s in d["stages"]:
            if s.get("side") in ("src", "dst") and s.get("total"):
                s["total"] = s["total"] / n
        m = resource_model.Model.from_dict(d)
    out = {}
    for side, c in (sides or {}).items():
        c = dict(c or {})
        if c.get("free_connections") is not None:
            c["free_connections"] = int(c["free_connections"]) // n
        if c.get("cpus"):
            c["cpus"] = max(1, int(c["cpus"]) // n)
        out[side] = c
    return m, out


def size(pair, db, units, n, model=None, sides=None):
    """Workers this machine runs on a shared table, one of `n` machines:
    `sizing.estimate` over the machine's share (`per_machine`)."""
    from . import sizing
    if sides is None:
        sides = {s: sizing.capacity(pair, s, db) for s in ("src", "dst")}
    m, mine = per_machine(model, sides, n)
    return sizing.estimate(pair.hop, pair, db, units, sides=mine,
                           model=m).start


def copy(pair, db, key, src_t, k, ck, lo, hi, edges, log, fn, item,
         outside):
    """A large table copied by every machine of the hop at once, this
    machine taking ranges while any is left (the module's words):
    `fn(item(after, upto, token))` copies one range in a process of its
    own, written under the claim's number (`fencing.Token.plain`). The
    checkpoint entry is left to the machine that finishes the table;
    `outside()` - the target's rows outside the source's whole range
    removed - is run by that machine alone. Returns True: the table is
    this path's, wherever its ranges were copied."""
    from . import ha, ranges, sizing
    from . import model as resource_model
    hop = pair.hop
    store = shared.store_for(hop, log)
    me = ha.mover()
    term = ha.term()
    table = Table(store, path(hop, db, key), me, term)
    rec = table.plan(key, lo, hi,
                     lambda: [list(r) for r in ranges.split(lo, hi, edges())])
    _index(hop, db, store, key)
    # the table's entry is the record's until one machine finishes it: an
    # entry this machine saved would be written over another's
    reload = getattr(ck, "reload", None)
    if reload is not None:
        reload(key)
    if rec.get("finished"):
        return True
    src = pair.src_engine
    try:
        model = resource_model.for_move(hop, pair, db, tables=[src_t])
    except Exception:  # noqa: BLE001 - sized from the servers' own words
        model = None
    sides = {s: sizing.capacity(pair, s, db) for s in ("src", "dst")}
    units = len(rec["ranges"])
    most = max(1, size(pair, db, units, 1, model, sides))
    allowed = {"n": most, "machines": 1}

    def resize(got):
        n = max(len(machines(got, time.time(), term)), 1)
        if n != allowed["machines"]:
            allowed["machines"] = n
            allowed["n"] = max(1, min(most, size(pair, db, units, n, model,
                                                 sides)))
            log(f"{key}: {n} machines share it; this one copies"
                f" {allowed['n']} ranges at once")
    resize(table.renew({}, most))
    log(f"{key}: {units} ranges, shared by every machine of the hop; this"
        f" one copies up to {allowed['n']} at once")
    held = {}
    cv = threading.Condition()
    stop = threading.Event()
    busy = {"n": 0}
    failed = []
    ident = store.ident()

    def renewing():
        while not stop.wait(term / 3):
            try:
                with cv:
                    mine = dict(held)
                resize(table.renew(mine, allowed["n"]))
            except Exception:  # noqa: BLE001 - the next turn tries again
                continue

    def one(proc):
        """One range claimed and copied: the worker process it used, or
        None where nothing is left for this machine."""
        got = table.claim()
        if got is None:
            if not left(table.read() or {"state": {}}):
                return None
            # the rest are claimed; one whose machine stops renewing is
            # claimable once its term lapses
            time.sleep(term / 3)
            return proc or False
        a, u, n = got
        with cv:
            held[a] = n
        try:
            table.started(a, n, src.snapshot_mark("src", db))
            tok = fencing.Token(range_row(hop, key, a), n, me, ident,
                                lazy=True)
            if not isinstance(proc, ranges._Worker) or not proc.alive():
                proc = ranges._Worker()
            got = proc.run(fn, item(a, u, tok.plain()))
            # the rows it copied, first where the copier says more
            moved = int((got[0] if isinstance(got, tuple) else got) or 0)
            # the places a shared range keeps what it did, as a range of
            # one machine's has them (`failpoint`)
            failpoint.hit("range.committed")
            table.done(a, n, moved)
            failpoint.hit("range.saved")
            now = table.read() or rec
            log(f"{key}: {k} {a + 1:,} to {u:,} copied, {moved:,} rows"
                f" ({units - left(now)} of {units} ranges done)")
        except (Lost, fencing.Fenced) as e:
            log(f"{key}: the range after {a:,} went to another machine"
                f" ({type(e).__name__}); its copy stands")
        except BaseException:
            try:
                table.give_back(a, n)
            except Exception:  # noqa: BLE001 - it lapses anyway
                pass
            raise
        finally:
            with cv:
                held.pop(a, None)
        return proc

    def worker():
        proc = False
        try:
            while not stop.is_set():
                with cv:
                    while busy["n"] >= allowed["n"] and not stop.is_set():
                        cv.wait(0.5)
                    if stop.is_set():
                        return
                    busy["n"] += 1
                try:
                    proc = one(proc)
                    if proc is None:
                        return
                finally:
                    with cv:
                        busy["n"] -= 1
                        cv.notify_all()
        except BaseException as e:  # noqa: BLE001 - raised below, once
            failed.append(e)
            stop.set()
        finally:
            if isinstance(proc, ranges._Worker):
                proc.close(wait=not failed)
    beat = threading.Thread(target=renewing, daemon=True)
    beat.start()
    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(most)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    beat.join(timeout=10)
    if failed:
        raise failed[0]
    rec, mine = table.finish()
    if not mine:
        return True
    outside()
    entry = {"table": src_t, "key": k, "ranges": rec["ranges"],
             "ranges_done": [int(a) for a, _ in rec["ranges"]],
             "ranges_seen": {a: st.get("mark")
                             for a, st in rec["state"].items()},
             "last": hi,
             "copying": {"host": socket.gethostname(), "pid": os.getpid()},
             "moved": sum(int(st.get("rows") or 0)
                          for st in rec["state"].values()),
             "shared": _by_machine(rec), "done": True}
    if reload is not None:
        reload(key)
    got = dict(ck.get(key) or {})
    got.update(entry)
    ck[key] = got
    ck.save()
    log(f"{key}: every range copied - " + "; ".join(
        f"{who} {n:,} rows" for who, n in entry["shared"].items()))
    return True


def _by_machine(rec):
    out = {}
    for st in rec["state"].values():
        who = f"{st.get('host')} process {st.get('pid')}"
        out[who] = out.get(who, 0) + int(st.get("rows") or 0)
    return out


# ---- health and evidence ----------------------------------------------------

def _records(hop, db, store):
    """(key, record) of every table the machines share that is planned:
    one whose ranges are not cut yet has nothing to say."""
    have, _ = store.read_record(hop.report_dir(db) / INDEX)
    for key in (have or {}).get("keys") or []:
        rec, _ = store.read_record(path(hop, db, key), sealed=True)
        if rec and rec.get("ranges") is not None:
            yield key, rec


def health(hop, db, store):
    """What `watch` says of the tables the hop's machines share: ranges
    done, and which machine copies how many now."""
    if not shared_ranges(hop):
        return []
    out = []
    now = time.time()
    try:
        for key, rec in _records(hop, db, store):
            copying = {}
            for st in rec["state"].values():
                if st["state"] == CLAIMED and float(st.get("expires") or 0) \
                        > now:
                    who = st.get("host")
                    copying[who] = copying.get(who, 0) + 1
            out.append(f"{key}: " + (
                ("copied whole" if not left(rec) else "copied whole, not"
                 " done") if rec.get("way") == "whole" else
                f"{len(rec['ranges']) - left(rec)} of"
                f" {len(rec['ranges'])} ranges done")
                       + ("; copying now: " + ", ".join(
                           f"{h} {n}" for h, n in sorted(copying.items()))
                          if copying else "")
                       + (f"; finished by {rec['finished'].get('host')}"
                          if rec.get("finished") else "")
                       + (f"; {len(rec['lapsed'])} claims lapsed and taken"
                          " again" if rec.get("lapsed") else ""))
    except Exception as e:  # noqa: BLE001 - said
        out.append(f"shared tables: not read ({type(e).__name__})")
    return out


def evidence(hop, db, store):
    """Rows of who copied each range of each shared table, under which
    number, and the claims that lapsed and were taken again."""
    if not shared_ranges(hop):
        return []
    out = []
    try:
        records = list(_records(hop, db, store))
    except Exception:  # noqa: BLE001 - nothing kept to say
        return []
    for key, rec in records:
        for a, u in rec["ranges"]:
            st = rec["state"][str(a)]
            out.append({"db": db, "what": f"{key}: the whole table"
                        if rec.get("way") == "whole" else
                        f"{key}: keys {a + 1:,} to {u:,}",
                        "by": f"{st.get('host')} process {st.get('pid')}",
                        "number": st.get("number"), "from": st.get("state"),
                        "to": f"{int(st.get('rows') or 0):,} rows"})
        for lapse in rec.get("lapsed") or []:
            out.append({"db": db, "what": f"{key}: the range after"
                                          f" {lapse.get('after')}",
                        "by": f"{lapse.get('host')}",
                        "number": lapse.get("number"),
                        "from": "claimed", "to": "lapsed, taken again"})
    return out
