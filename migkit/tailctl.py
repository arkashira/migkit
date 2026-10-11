"""A running change tail as the rest of migkit sees it, and the one loop
every tail runs.

The tail runs in its own `migkit move --mode cdc` process, often for days,
and it is a second writer on the target. A repair that writes rows beside it
races it: whichever lands last survives, and nothing says which. So the tail
says it is running, and it can be asked to pause, which it acknowledges
only once it has applied what it was holding and saved its position.
After the pause it resumes from that position, and the changes it replays
land on top of the repair by key. That is the order that converges.

Files in the database's report directory, nothing else:
* `tail-token.json` where the tail stands: one envelope for every engine
  (`position`), written whole and on the disk (`durable`) after the
  target committed what it covers - never before
* `tail.pid`     the tail's lease (`lease`): its host, process and term,
                 renewed by a heartbeat while it runs; a second tail of the
                 same database is refused, not run beside it
* `tail.pause`   written by whoever wants it paused, removed to resume
* `tail.paused`  written by the tail once it is holding still
* `tail.beat`    written by the tail every time round its loop
* `tail.stopped` what stopped it, when that was not a person
* `tail.stop`    written by whoever wants it to end: it applies what it
                 read, saves its position and ends between batches

How long it runs is the hop's `follow_until` (`until`): `stopped`, until
asked - ctrl-c, a service manager's stop, `tail.stop` - or `caught_up`,
until it has reached where the source's log was once it read to the end,
which is what a run its schedule started does unless the hop says
otherwise; and a run a schedule started ends at its slot's end at the
latest. Every engine's tail goes round the same loop (`run_loop`, audit
2026-10-01 ST14, K14): the cross-engine tail, the ones that follow a
source by looking at it again (ClickHouse, OpenSearch, Cassandra),
MongoDB's change stream and Kafka's topics - stop, until, the slot's end,
pause, the lease, the beat, a lost connection tried again from the last
saved position, SIGTERM as ctrl-c, and the position saved after the
target committed, the same for each.
"""
import json
import os
import socket
import sys
import time

from . import durable
from . import lease as _lease

PID, PAUSE, PAUSED = "tail.pid", "tail.pause", "tail.paused"
BEAT, STOPPED = "tail.beat", "tail.stopped"
STOP = "tail.stop"
#: where a position saved before the envelope carried its source kept it
SOURCE = "tail-source.json"
#: the tail's position, in the database's report directory
TOKEN = "tail-token.json"
UNTIL = ("stopped", "caught_up")
#: the envelope's version (`position`)
V = 1
#: how often the loop looks at the files a person writes to ask it to
#: pause or stop, and how often a waiter looks at the tail's: a fence
#: waited a second on average when both were polled once a second
#: (audit SP10)
POLL = 0.05
#: an idle tail waits this long before it reads again, doubling to
#: `IDLE_MOST` while nothing arrives and back at the next change: a flat
#: second put a second on every fence (cutover, a confirm pass, the kept
#: proof), where the first read after a change now comes within 20 ms
IDLE_LEAST, IDLE_MOST = 0.02, 1.0
#: a loop writes its beat at most this often: an idle tail goes round
#: every 20 ms at first, and the beat is for `/metrics` and the dashboard
BEAT_EVERY = 0.5


def until(hop, scheduled=None):
    """How long a follow runs (`follow_until`): `stopped` or `caught_up`.
    A run a schedule started ends once caught up unless the hop says
    otherwise: left running, it held its slot until the schedule's
    `max_duration` cut it off, and every slot counted as failed.
    `scheduled` None asks the schedule (`schedule.slot_end`) - the one
    reader every follow loop goes through."""
    if scheduled is None:
        from . import schedule
        scheduled = schedule.slot_end() is not None
    got = (hop.options or {}).get("follow_until")
    if got is None:
        return "caught_up" if scheduled else "stopped"
    got = str(got).strip().lower().replace("-", "_").replace(" ", "_")
    if got not in UNTIL:
        raise SystemExit(f"follow_until: {got} is not one of"
                         f" {', '.join(UNTIL)} - run until asked to stop,"
                         " or until caught up with the source")
    return got


def token_path(hop, db):
    """Where a tail of `db` keeps its position - computed here and nowhere
    else (it was in four places, audit ST1)."""
    if not db:
        raise SystemExit("cdc tail needs --db")
    return hop.report_dir(db) / TOKEN


# --- the position -------------------------------------------------------------

def position(path):
    """The envelope a tail saved at `path`, or None where none is saved:

        {"v": 1, "id": the stream it belongs to, "engine", "source": the
         source's identity it was taken on, "token": the position,
         "gen": how many times it moved, "batch", "rung", "next"}

    one shape for every engine (audit ST21). What earlier versions wrote -
    `{"token", "batch", "rung", "next"}` beside a `tail-source.json`, or
    MongoDB's bare resume token - reads as the same envelope. A file that
    cannot be read whole stops, naming it: a tail started from anywhere
    else skips changes, or cannot say it did not (`durable`)."""
    got = durable.read_state(path, "position")
    if got is None:
        return None
    if isinstance(got, dict) and got.get("v") == V and "token" in got:
        return got
    if isinstance(got, dict) and isinstance(got.get("v"), int) \
            and got["v"] > V:
        raise SystemExit(
            f"{path} was written by a newer migkit (envelope version"
            f" {got['v']}); this one reads version {V}. Run the newer"
            " migkit, or remove the file and move again with --mode"
            " full+cdc")
    if isinstance(got, dict) and "v" not in got:
        if "token" in got:
            env = {k: got[k] for k in ("token", "batch", "rung", "next")
                   if k in got}
        elif "_data" in got:
            # MongoDB's resume token, saved bare
            env = {"token": {"_data": got["_data"]}}
        else:
            env = None
        if env is not None:
            env.update(v=V, id="v0", gen=0)
            src = durable.read_state(path.parent / SOURCE, "state")
            if src is not None:
                env["source"] = src
            return env
    raise SystemExit(
        f"the saved position in {path} cannot be read, and a follow"
        " started from anywhere else either skips changes or cannot say it"
        " did not. Remove the file and move again with --mode full+cdc")


def saved(path):
    """The position a follow saved, None where there is none."""
    env = position(path)
    return None if env is None else env.get("token")


def keep(path, env):
    """The envelope written whole and on the disk - the one writer of a
    tail's position. Called after the target committed what `env`'s
    position covers, never before: a position saved first skips what the
    target never got, one saved after a crash applies it again, which the
    appliers are idempotent for - or which the target's mark of its last
    batch settles (`twoway.committed_ahead`)."""
    env = {**env, "v": V}
    env.setdefault("gen", 0)
    durable.write_json(path, env)
    return env


def moved(path, env, token, **fields):
    """`env` moved on to `token` and kept: its generation one higher, a
    batch's `next` gone with it, `fields` (batch, rung) set."""
    out = {k: v for k, v in (env or {}).items() if k != "next"}
    out.update(fields, token=token, gen=int(out.get("gen") or 0) + 1)
    return keep(path, out)


def new_stream(eng, token):
    """The envelope of a position taken now: a stream of its own
    (`id`), which the tail's own state beside it is held to
    (`tailtables.Store`)."""
    import secrets
    return {"v": V, "id": secrets.token_hex(8), "token": token, "gen": 0,
            "engine": getattr(eng, "CANON_ENGINE", None)
            or type(eng).__name__}


def begin(eng, db, path, point, fresh=None):
    """Save `point` as where a follow begins, the source it belongs to in
    the same envelope - one write, where there were three (the position,
    the old source's file removed, the new one written: a crash between
    them re-baselined the source, audit ST21). `fresh(env)` clears what
    the tail kept of an earlier stream, before the position is written."""
    env = new_stream(eng, point)
    said = eng.stream_identity("src", db)
    if said is not None:
        env["source"] = said
    if fresh is not None:
        fresh(env)
    keep(path, env)
    (path.parent / SOURCE).unlink(missing_ok=True)
    return env


def same_source(eng, db, token_path, token):
    """Stop before resuming from a position that is not the source's any
    more (backlog 44).

    A position belongs to one server's log. After a failover to a replica,
    or with the source rebuilt under the same address, a MySQL file and
    offset point into a different binlog, and a MongoDB resume token taken
    on another replica set was measured to be accepted without a word -
    either way the tail goes on, having skipped what it cannot know it
    skipped. So the source's identity is kept in the position's envelope
    when the position is first saved, and compared on every resume. Where
    the engine can say its log no longer holds the position, that stops
    the tail too, before the stream is opened rather than after it has
    read."""
    said = eng.stream_identity("src", db)
    if said is not None:
        env = position(token_path)
        have = (env or {}).get("source")
        if env is not None and (have is None or not token):
            keep(token_path, {**env, "source": said})
        elif env is not None and have != said:
            raise SystemExit(
                f"{db}: the saved change position belongs to another source"
                f" ({_who(have)}); the source is now {_who(said)}. A failover"
                " or a rebuilt server gives the same address a log of its"
                " own, and resuming in it skips what the old one held."
                " Nothing was applied. Move again with --mode full+cdc, or"
                f" remove {token_path.name} once the target is known to be"
                " level with the new source.")
    lost = eng.position_lost("src", db, token) if token else None
    if lost:
        raise SystemExit(f"{db}: {lost}. Nothing was applied. Move again"
                         " with --mode full+cdc.")


def _who(identity):
    return ", ".join(f"{k} {v}" for k, v in sorted(identity.items()))


# --- asking it -----------------------------------------------------------------

def stop(where):
    """Ask what follows here to end at its next batch, its position
    saved."""
    durable.write_atomic(where / STOP, str(time.time()))


def stop_asked(where):
    return (where / STOP).exists()


def _legacy(path, text):
    """A `tail.pid` an earlier version wrote - "host pid", no term - as a
    lease record: on another machine it counts as renewed when it last
    beat."""
    try:
        host, pid = text.split()
    except ValueError:
        return None
    at = (_read(path.parent / BEAT) or {}).get("at")
    if at is None:
        try:
            at = path.stat().st_mtime
        except OSError:
            at = 0.0
    return {"holder": f"{host}:{pid}:legacy", "host": host, "pid": pid,
            "expires": float(at) + _lease.term()}


def _remote(hop):
    """The store another machine's tail of this hop holds its lease in,
    or None: a hop whose run state is kept where every machine sees it,
    unless this process holds the hop as a standby's leader (`ha`),
    whose own lease is the tail's."""
    if hop is None:
        return None
    from . import ha, state
    if ha.held(hop) is not None:
        return None
    return state.run_state(hop)


class Running:
    """The tail's lease and its marks, for as long as the block lasts.

    One lease a tail's scope - its database's report directory - taken
    before anything of an earlier tail's is touched: two tails of one
    database both applied and both wrote the position, and the second
    one's end removed the first one's marks (audit ST8). A tail on another
    machine is refused where the hop's run state is shared (`hop`)."""

    def __init__(self, where, hop=None):
        self.where, self.hop = where, hop
        self.lease = self.remote = None
        self.was_stopped = None

    def __enter__(self):
        durable.private_dir(self.where)
        what = f"change tail of {self.where.name}"
        self.lease = _lease.Lease(self.where / PID, what,
                                  legacy=_legacy).acquire()
        try:
            remote = _remote(self.hop)
            if remote is not None:
                self.remote = _lease.Lease(self.where / PID, what,
                                           remote=remote).acquire()
        except BaseException:
            self.lease.release()
            self.lease = None
            raise
        # what stopped the last one, for whoever was told it had stopped
        self.was_stopped = _read(self.where / STOPPED)
        (self.where / STOPPED).unlink(missing_ok=True)
        # a stop asked of a run that has ended is not asked of this one
        (self.where / STOP).unlink(missing_ok=True)
        return self

    def check(self):
        """Between batches: `lease.Lost` where this tail no longer holds its
        lease - taken over, or not renewed within its term - or the run
        it is part of no longer holds the hop's, so it stops writing
        rather than going on beside another (audit ST11)."""
        for held in (self.lease, self.remote):
            if held is not None:
                held.check()
        _lease.check_held()

    def __exit__(self, kind=None, exc=None, tb=None):
        if self.lease is None:
            return False
        for name in (PAUSED, BEAT, STOP):
            (self.where / name).unlink(missing_ok=True)
        for held in (self.remote, self.lease):
            if held is not None:
                try:
                    held.release()
                except Exception:  # noqa: BLE001 - it lapses by its term
                    pass
        self.lease = self.remote = None
        if kind is not None and not issubclass(kind, KeyboardInterrupt):
            # ctrl-c and a service manager's stop are a person's; anything
            # else stopped a tail somebody expects to be running
            first = (str(exc).strip().splitlines() or [""])[0][:300]
            durable.write_json(self.where / STOPPED, {
                "at": time.time(), "kind": kind.__name__,
                "why": first if kind is SystemExit and first
                else f"{kind.__name__}: {first}" if first
                else kind.__name__})
        return False


def _read(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def beat(where, caught_up_at, changes, room=None, through=None, lag=None,
         tables=None, lag_unknown=None):
    """Written as the tail goes round its loop (`run_loop`, at most every
    `BEAT_EVERY`): when it last reached the end of the source's log, how
    many changes it has applied, and how much longer the source keeps
    what it has not read (`stream_room`). Written whole or not at all, so
    a reader never sees half of it; not synced, since it says how the
    tail is, not where it stands.

    `through` is when the source committed the last change applied (its
    log's own time, `canon.ended`), and `lag` how long that change took
    from the source's commit to the target's - GoldenGate's end-to-end
    latency and Q Replication's `END2END_LATENCY`, with no table on the
    source; skewed by as much as the two clocks are apart. `lag_unknown`
    is why a source gives no commit time to measure it from (a SQL Server
    account that cannot read its commit table): the lag is then unknown,
    never 0. `tables` is
    {state: tables in it} of a tail whose tables move on their own
    (`tailtables`), with `set_aside_bytes` - or what gives it, asked only
    when the beat is written."""
    now = time.time()
    if callable(tables):
        tables = tables()
    tmp = where / f".{BEAT}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps({"at": now,
                               "caught_up_at": caught_up_at,
                               "changes": changes, "room": room,
                               "through": through, "lag": lag,
                               "lag_unknown": lag_unknown,
                               "tables": tables}))
    os.replace(tmp, where / BEAT)


def stopped(where):
    """What stopped the tail here on an error, or None."""
    return _read(where / STOPPED)


def state(where):
    """The tail here as `/metrics` reports it, or None where no tail runs
    and none stopped on an error.

    `behind` is the seconds since the tail last read to the end of the
    source's log: about a second while it keeps up, and growing while it
    does not. `beat_age` is the seconds since it last went round its loop,
    which grows while it is stuck."""
    marked = (where / PID).exists()
    was = stopped(where)
    if not marked and was is None:
        return None
    out = {"running": alive(where) if marked else False,
           "marked": marked, "paused": (where / PAUSED).exists(),
           "stopped": was, "beat_age": None, "behind": None,
           "changes": None, "room": None, "lag": None, "through": None,
           "lag_unknown": None, "tables": None}
    b = _read(where / BEAT) if marked else None
    if b:
        now = time.time()
        out["beat_age"] = max(now - b["at"], 0)
        out["behind"] = max(now - b["caught_up_at"], 0)
        out["changes"] = b.get("changes")
        out["room"] = b.get("room")
        out["lag"] = b.get("lag")
        out["through"] = b.get("through")
        out["lag_unknown"] = b.get("lag_unknown")
        out["tables"] = b.get("tables")
    return out


def alive(where, hop=None):
    """True while a tail may be running here: its lease has a holder that
    `lease.liveness` says may still write - on this machine its process
    is there, on another its term, renewed by its heartbeat, has not run
    out. With `hop`, a tail of this database on another machine that
    shares the hop's run state counts too.

    A tail on another host counts as running for as long as its term:
    guessing that a writer has stopped is the mistake that costs rows,
    guessing that it runs costs a refusal - for one term, not for good."""
    rec = _lease._FileRecord(where / PID, _legacy).read()
    if _lease.liveness(rec):
        return True
    remote = _remote(hop)
    return remote is not None and _lease.holder(where / PID, remote) \
        is not None


def process_alive(host, pid):
    """True while that process may still be running: this host's is asked,
    another host's is assumed to be - where there is no lease record to
    hold it to a term."""
    if host != socket.gethostname():
        return True
    return _lease._pid_alive(pid) is not False


def _nap(where, seconds, names=(STOP, PAUSE)):
    """Sleep up to `seconds`, waking at once where one of `names` is
    written - a stop or a pause is not kept waiting for the nap."""
    end = time.monotonic() + seconds
    while True:
        left = end - time.monotonic()
        if left <= 0:
            return
        if any((where / n).exists() for n in names):
            return
        time.sleep(min(POLL, left))


def hold_if_asked(where, log=None):
    """Called by the tail between batches: while a pause is asked for, say
    so and wait. Returns True if it paused."""
    if not (where / PAUSE).exists():
        return False
    durable.write_atomic(where / PAUSED, str(time.time()))
    if log:
        log("paused for a repair; holding the position already saved")
    while (where / PAUSE).exists():
        time.sleep(POLL)
    (where / PAUSED).unlink(missing_ok=True)
    if log:
        log("resumed")
    return True


def pause(where, timeout):
    """Ask the tail here to pause; True once it has, False if it did not
    within `timeout` seconds (and the request is withdrawn)."""
    durable.write_atomic(where / PAUSE, str(time.time()))
    end = time.time() + timeout
    while time.time() < end:
        if (where / PAUSED).exists():
            return True
        if not alive(where):
            # it stopped rather than paused: nothing is writing now
            return True
        time.sleep(POLL)
    resume(where)
    return False


def resume(where):
    (where / PAUSE).unlink(missing_ok=True)


# --- the loop --------------------------------------------------------------------

def run_loop(where, db, look, log, noun, *, hop=None, go=True, token=None,
             save=None, reached=None, lost=None, beat_with=None,
             idle=(IDLE_LEAST, IDLE_MOST), secrets=(), until_=None,
             on_start=None, on_end=None):
    """The one follow loop (audit ST14, K14). Returns (how many `noun` it
    carried, why it ended: "asked", "slot", "caught up" or "interrupted").

    `look(token)` carries one batch from `token` and returns (where the
    next goes on from, how many it carried, whether it read to the end of
    what the source holds now). `save(after)` keeps the position once the
    batch is on the target - None where `look` saves its own, in the step
    that commits its batch. `reached(token)` says whether a follow that is
    to end once caught up (`until`) has come to where the source's log was
    when it first read to its end: True, False, or None where it cannot be
    compared - it then ends on an empty read. `lost(e, token)` is where to
    go on from after a lost connection (the last saved position where
    None). `beat_with(caught_up, seen)` replaces the plain beat.

    Around every batch, the same for each engine: the lease still held
    (`Running.check`), a pause held, a stop or the schedule's slot end
    obeyed with the position saved, a connection lost tried again after a
    wait that doubles to a minute, SIGTERM taken as ctrl-c, the beat
    written, and an idle wait that grows from 20 ms to `idle[1]` and is
    back at 20 ms at the next change. A run with `go` False writes
    nothing: no lease, no position, no beat."""
    from . import failpoint, notify, schedule
    from .util import is_transient, without_secret
    slot_ends = schedule.slot_end()
    if until_ is None:
        until_ = until(hop, scheduled=slot_ends is not None) \
            if hop is not None else "stopped"
    seen, caught_up, failures = 0, time.time(), 0
    wait = idle[0]
    ended = "asked"
    running = Running(where, hop) if go else None
    beaten = [0.0]

    def beat_now(force=False):
        if not go or (not force and time.monotonic() - beaten[0]
                      < BEAT_EVERY):
            return
        beaten[0] = time.monotonic()
        if beat_with is not None:
            beat_with(caught_up, seen)
        else:
            beat(where, caught_up, seen)
    # SIGTERM taken as ctrl-c, the one way every program migkit runs is
    # stopped (`runner.stopping`): the batch in hand is not saved, and what
    # the tail set aside on the target goes back
    from . import runner
    term = runner.stopping() if go else None
    if term is not None:
        term.__enter__()
    try:
        if running is not None:
            running.__enter__()
            if running.was_stopped and hop is not None:
                notify.tail_started(hop, db, log)
        try:
            if on_start is not None:
                on_start()
            while True:
                if running is not None:
                    running.check()
                    # between batches, never inside one: everything read
                    # so far is applied and its position saved before it
                    # holds still
                    hold_if_asked(where, log)
                    if stop_asked(where):
                        log(f"stopped as asked after {seen:,} {noun}; the"
                            " position is saved")
                        break
                if slot_ends is not None and time.time() >= slot_ends:
                    log(f"the schedule's slot has ended, after {seen:,}"
                        f" {noun}; the position is saved for the next")
                    ended = "slot"
                    break
                try:
                    after, n, settled = look(token)
                except Exception as e:  # noqa: BLE001 - classified here
                    if not is_transient(e):
                        raise
                    # a network blip or a server restarting: measured, the
                    # target paused for 30 seconds ended a running tail
                    # for good. Nothing after the saved position was kept,
                    # so it is read again from there - which the appliers
                    # are idempotent for, or the target's mark settles
                    failures += 1
                    pause_s = min(60, 2 ** min(failures, 6))
                    said = (str(e).strip().splitlines() or [""])[0]
                    for secret in secrets:
                        said = without_secret(said, secret)
                    log(f"lost a connection ({said[:100]}); trying again in"
                        f" {pause_s}s from the last saved position")
                    beat_now(force=True)
                    _nap(where, pause_s)
                    if lost is not None:
                        token = lost(e, token)
                    continue
                failures = 0
                if n:
                    # a batch carried: said at once, whatever the last beat
                    beaten[0] = 0.0
                if go and save is not None and (n or after != token):
                    if n:
                        failpoint.hit("tail.applied")
                    save(after)
                    if n:
                        failpoint.hit("tail.saved")
                token = after
                if settled:
                    caught_up = time.time()
                seen += n
                beat_now()
                if until_ == "caught_up" and settled:
                    got = reached(token) if reached is not None else None
                    if got or (got is None and not n):
                        log(f"caught up with the source, after {seen:,}"
                            f" {noun} - ended, as follow_until says; the"
                            " next run goes on from there")
                        ended = "caught up"
                        break
                if n or not settled:
                    wait = idle[0]
                    continue
                if running is not None:
                    _nap(where, wait)
                else:
                    time.sleep(wait)
                wait = min(wait * 2, idle[1])
        finally:
            if on_end is not None:
                on_end()
    except KeyboardInterrupt:
        log(f"stopped after {seen:,} {noun}; rerun to resume")
        ended = "interrupted"
    finally:
        if running is not None:
            kind = sys.exc_info()
            running.__exit__(*kind)
            why = stopped(where)
            if kind[0] is not None and why and hop is not None:
                notify.tail_stopped(hop, db, why, log)
        if term is not None:
            term.__exit__(None, None, None)
    return seen, ended


def follow(eng, db, go, token_path, log, noun, look, behind, every=1.0):
    """The loop of an engine that follows its source by looking at it
    again and again, the same for each: the saved position - taken now
    with `eng.tail_start` where there is none - held to the source it was
    taken on (`same_source`), then one `look(token)` after another until
    stopped (`run_loop`), the position saved after each. `look` returns
    the position after it, how many `noun` it carried, and whether it
    settled all it found - the time the follow last caught up, for
    `/metrics`. `behind` says, for a run without --go, what the source
    holds past the position. A connection lost is looked at again from
    the saved position, which carrying twice converges on; anything else
    stops the follow and says why."""
    token = saved(token_path)
    if token is None and go:
        eng.tail_start(db, token_path)
        token = saved(token_path)
    elif token is not None:
        same_source(eng, db, token_path, token)
    log(f"following the source's {noun}, ctrl-c to stop"
        + ("" if go else " (count-only, add --go to copy)"))
    # a way that fails as it carries a change is switched from by itself
    # and said here (`Engine._attempt`)
    lend = getattr(eng, "lend_log", None)
    if lend is not None:
        lend(log)
    if not go:
        log(behind(token))
        return
    env = [position(token_path)]
    carried = [0]

    def looking(at):
        after, went, settled = look(at)
        if went:
            carried[0] += went
            log(f"{carried[0]:,} {noun} carried")
        return after, went, settled

    def save(after):
        env[0] = moved(token_path, env[0], after)
    run_loop(token_path.parent, db, looking, log, noun, hop=eng.hop, go=go,
             token=token, save=save,
             secrets=(eng.hop.source.password, eng.hop.target.password),
             idle=(min(every, IDLE_MOST) / 8, every))
