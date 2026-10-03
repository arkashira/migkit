"""The cutover as one timed path that undoes itself.

`migkit move HOP --mode cdc --drop --go` has always been the cutover:
tearing the stream down is the moment the application moves. What came
before it was a runbook of steps an operator ran by hand, two of them
outside migkit - stopping the source's writes and pointing the
application at the target - with nothing timing the outage and nothing
undoing a step that failed half way.

A hop that turns `cutover:` on gets the whole path, run back to back
with a clock on it:

    options:
      cutover:
        budget: 30            # seconds of write outage; past it, undone
        start_below: 1        # seconds the stream may be behind at the start
        freeze: auto          # auto | server | database | operator
        pooler: {kind: pgbouncer, host: ..., port: 6432, user: ...,
                 password: ..., databases: [app]}
        flip: <command>       # run at the flip; none = the operator flips
        flip_undo: <command>  # run if the path is undone after the flip ran
        keys: exact           # exact | apart:<margin>
        jobs: move            # move | leave
        no_return: later      # later | at_flip
        restore_points: [source, target]   # through each side's provider

1. preflight, nothing written: the stream's own position reachable and
   the stream less than `start_below` behind it - or, where the stream
   keeps none migkit can read, a mark on the source to drain by
   (`drainmark`); nothing prepared or long open on either side; the
   schema the same on both; nothing a load set aside left off; who can
   write through the freeze named; the jobs on each side; the counters;
   the stream back possible; the outage the last run of this hop
   measured against the budget; approvals signed
2. prepared, before the clock: a restore point of each side the hop
   names taken through its provider's API and waited for
   (`restore_points`, `managed`) - a way back the provider keeps; the
   stream back made and proven where it can wait idle (`reverse:
   at_cutover`), so the flip only starts it; the mark's table made, and a
   first mark proven to arrive
3. jobs the source runs switched off there (a frozen MySQL server refuses
   the statement afterwards)
4. the pooler paused, where the hop names one: writes wait instead of
   failing - the clock starts
5. the source frozen, the way chosen from what it allows (`freeze.choose`,
   one hold per engine, `freeze.Hold`), and read back (`freeze.proof`) -
   the clock starts here without a pooler
6. the drain: a position taken *after* the freeze, reached by the hop's
   own stream - or a mark written after the freeze and found on the
   target - and a stream that keeps positions of its own (a pipeline's
   offsets, a leg run from here that ends at a position) waited for by
   them (`Leg.settle`)
7. the final check of what changed, fenced, and the source's schema
   unchanged since the preflight
8. the target's counters past every key the source handed out
9. the stream torn down, and where the hop asks the stream back started
   and proven to reach the old source
10. the jobs switched on on the target
11. the flip: the target given back its writes, the flip command run, the
    pooler repointed and resumed - the clock stops

Every step is written down before it acts. A failure anywhere, or the
clock passing the budget, undoes every step from the record, the last
first, and the source is where it was: writable, with nothing written
to it that it did not have. A run killed half way is undone by the next
one, which then stops and says so - never left half flipped. A run
holds the cutover's lease while it acts (`_held`), so the next one is
refused while it runs: a record of a run under way was read as a run
that had stopped, and undone in flight. After the
flip the old source stays frozen, so an application still pointed at it
fails rather than writing where nobody reads; `rollback --apply` gives it
back, and so does the point of no return.

The point of no return is a step of its own, behind an approval of its
own (`approvals.steps: [point-of-no-return]`): the same command run again
once a cutover has flipped - or at the flip, where the hop says
`no_return: at_flip`. It ends the forward stream for good, gives the old
source back its writes, drops the stream back, and takes away what the
cutover left on either side (`no_return`); after it, going back is
refused.

A hop without `cutover:` gets the same preflight, the steps in migkit's
words to do by hand, and the proofs around them: the stream torn down
only once nothing is writing to the source, nothing is prepared there,
and the stream has passed where the source stands. Where one of those
fails the stream is left up; `MIGKIT_CUTOVER=unproven` tears it down
anyway and records that it did.
"""
import json
import os
import secrets
import subprocess
import threading
import time

from . import failpoint
from .hopkeys import opt

#: in the order they run; each is a failpoint `cutover.<phase>`
PHASES = ("prepare", "jobs_off", "pause", "freeze", "drain", "verify",
          "counters", "stream", "reverse", "jobs_on", "flip")
#: the point of no return's steps, in order (`no_return`)
NO_RETURN = ("tail_off", "thaw", "back_off", "left", "origin")

DEFAULTS = {"budget": 30, "start_below": 1, "freeze": "auto",
            "keys": "exact", "jobs": "move", "flip": "", "flip_undo": "",
            "pooler": None, "no_return": "later", "restore_points": []}
#: the sides a restore point is taken of before the clock
RESTORE_SIDES = ("source", "target")

FREEZES = ("auto", "server", "database", "operator")
POOLERS = ("pgbouncer", "pgcat")
NO_RETURNS = ("later", "at_flip")
#: the approval step the point of no return waits for
NO_RETURN_STEP = "point-of-no-return"


def settings(hop):
    """The hop's `cutover:` settings, or None where it does not turn the
    cutover on."""
    raw = (hop.options or {}).get("cutover")
    if raw is None or raw is False:
        return None
    if raw is True:
        raw = {}
    if not isinstance(raw, dict):
        raise SystemExit("cutover: is `true` or the path's settings"
                         f" ({', '.join(DEFAULTS)}), not {raw!r}")
    unknown = sorted(set(raw) - set(DEFAULTS))
    if unknown:
        raise SystemExit(f"cutover: {', '.join(unknown)} - not a setting of"
                         f" the cutover; it has {', '.join(DEFAULTS)}")
    got = {**DEFAULTS, **raw}
    try:
        got["budget"] = float(got["budget"])
        got["start_below"] = float(got["start_below"])
    except (TypeError, ValueError):
        raise SystemExit("cutover: budget and start_below are seconds")
    if got["budget"] <= 0:
        raise SystemExit("cutover: budget is the seconds the write outage"
                         " may last, more than 0")
    if str(got["freeze"]) not in FREEZES:
        raise SystemExit(f"cutover: freeze {got['freeze']} - one of"
                         f" {', '.join(FREEZES)}")
    margin(got)
    if str(got["jobs"]) not in ("move", "leave"):
        raise SystemExit("cutover: jobs is move or leave")
    if str(got["no_return"]) not in NO_RETURNS:
        raise SystemExit("cutover: no_return is later (its own run of the"
                         " command, once the flip is done) or at_flip")
    points = got["restore_points"]
    points = [points] if isinstance(points, str) else list(points or [])
    if any(str(p) not in RESTORE_SIDES for p in points):
        raise SystemExit("cutover: restore_points lists the sides a restore"
                         " point is taken of through their provider's API"
                         f" before the clock: {', '.join(RESTORE_SIDES)}")
    got["restore_points"] = [str(p) for p in points]
    pooler = got["pooler"]
    if pooler is not None:
        if not isinstance(pooler, dict) or \
                str(pooler.get("kind", "")).lower() not in POOLERS:
            raise SystemExit("cutover: pooler is {kind: pgbouncer or pgcat,"
                             " host, port, user, password, databases}")
    return got


def margin(conf):
    """The gap `keys: apart:<n>` asks between the source's last key and
    the target's first, 0 for `exact`."""
    keys = str(conf.get("keys") or "exact")
    if keys == "exact":
        return 0
    if keys.startswith("apart:") and keys[6:].isdigit() and int(keys[6:]):
        return int(keys[6:])
    raise SystemExit(f"cutover: keys {keys} - exact, or apart:<a whole"
                     " number above 0>")


def _engine(hop):
    from .engines import ALIASES
    return ALIASES.get(hop.engine, hop.engine)


def _reverse(hop):
    from .hopkeys import reverse
    return reverse(hop) == "at_cutover"


# --- the stream, as the cutover sees it -----------------------------------

class Leg:
    """What carries the hop's changes now, and what the cutover does with
    it. Built by the command (`cli._stream_leg`), which knows the paths.

    kind         "native" (the servers' own replication), "tail" (migkit's
                 own), "follow" (the change leg run from here), "pipeline"
                 (migkit's streaming pipeline), or "none", with `why`
    fence        the engine whose `src_lsn`/`fence_wait` prove the drain,
                 or None where the stream keeps no position migkit can
                 hold it to - drained by a mark then (`drainmark`)
    seen(d, token)
                 where the stream keeps a mark that is not the target's
                 own table (a pipeline's topic), or None
    settle(d, timeout)
                 the stream's own positions waited for after the drain -
                 a pipeline's offsets applied, a leg run from here ended
                 at a position named after the freeze - True, False past
                 `timeout`, or None
    down(d)      tear the stream down; returns source statements left for
                 when the source is written again
    up(d)        put it back, from where the target stands
    after_flip(d)
                 what the stream leaves once the flip is done - a
                 pipeline's stack, a leg's slot - taken away; returns
                 source statements left for when the source is written
                 again
    end(d)       the stream ended for good, at the point of no return
    origin(d)    migkit's own batch table taken off the target where the
                 hop asks (`origin_table: drop`), once no tail needs it
    back_up(d), back_down(d), back_fence
                 the stream back (`reverse: at_cutover`) and the engine
                 that proves it reached the old source, or None
    back_arm(d), back_fire(d), back_proof(d)
                 the stream back made ahead and left idle, started, and
                 read back as ready ("" or why not); None where the
                 engine starts it only whole
    after_thaw   the stream is put back only once the source is writable
                 (it starts from the target's own position, and setting it
                 up writes to the source)
    manual(go)   the teardown a hop without `cutover:` has always had
    """

    def __init__(self, kind, fence=None, down=None, up=None, back_up=None,
                 back_down=None, back_fence=None, after_thaw=False,
                 manual=None, why="", seen=None, settle=None,
                 after_flip=None, end=None, origin=None, back_arm=None,
                 back_fire=None, back_proof=None):
        self.kind, self.fence = kind, fence
        self.down, self.up = down, up
        self.back_up, self.back_down = back_up, back_down
        self.back_fence = back_fence
        self.after_thaw = after_thaw
        self.manual = manual
        self.why = why
        self.seen, self.settle = seen, settle
        self.after_flip, self.end, self.origin = after_flip, end, origin
        self.back_arm, self.back_fire = back_arm, back_fire
        self.back_proof = back_proof


# --- the record -------------------------------------------------------------

def _path(hop):
    return hop.report_dir() / "cutover.json"


def record(hop):
    """The cutover under way, or None. One that cannot be read whole
    stops, naming it: read as none, a half-done cutover is not undone
    (`durable`)."""
    from . import durable
    return durable.read_state(_path(hop), "journal")


def _save(hop, rec):
    from . import durable
    durable.write_json(_path(hop), rec, indent=1)


def _lease_path(hop):
    return hop.report_dir() / "cutover-lease.json"


def _held(hop, say):
    """The cutover's lease, taken for a run that acts (audit ST8): kept
    in the hop's run state where it has one, so a run on another machine
    of the hop is refused as well. Its own, not the hop's: a hop with a
    standby has its change stream's machine hold that one while the
    cutover drains it."""
    from . import lease
    from .state import run_state
    got = lease.Lease(_lease_path(hop), "cutover",
                      remote=run_state(hop)).acquire()
    if got.took_over:
        was = got.took_over
        say(f"[yellow]the cutover run of {was.get('host')} process"
            f" {was.get('pid')} stopped without letting go of its lease"
            "[/yellow]")
    return got


def running(hop):
    """The lease record of a cutover run that is acting now, or None."""
    from . import lease
    from .state import run_state
    return lease.holder(_lease_path(hop), run_state(hop))


def _audit(hop, rec, entry):
    """A step written down - and, as it is, a span of the run's trace for
    a phase, and the hop's receivers told (`notify.cutover`)."""
    from . import audit, notify, traces
    audit.append(hop.report_dir() / "changelog.jsonl",
                 {"op": "cutover", "run": rec.get("id"),
                  "approved_by": rec.get("approvers") or [], **entry})
    phase = entry.get("phase")
    if phase:
        at = (rec.get("phases") or {}).get(phase) or {}
        traces.past(f"cutover {phase}", at.get("began"), at.get("ended"),
                    phase=phase, seconds=entry.get("seconds"),
                    run=rec.get("id"))
    notify.cutover(hop, rec, entry)


def _runs_path(hop):
    return hop.report_dir() / "cutover-runs.jsonl"


#: what the runs' log keeps beside the cutovers themselves: what ended
#: one after its flip
AFTER = ("no-return", "rolled back")


def last_run(hop, after=False):
    """The last cutover of this hop that ended, flipped or undone, with
    the seconds each phase took - what the next one is predicted from;
    with `after`, what ended one after its flip as well (`AFTER`)."""
    try:
        lines = _runs_path(hop).read_text().splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            got = json.loads(line)
        except ValueError:
            continue
        if after or got.get("state") not in AFTER:
            return got
    return None


def past_flip(hop):
    """The cutover that flipped last, where nothing has ended it since -
    no point of no return, no going back - or None."""
    last = last_run(hop, after=True)
    return last if last and last.get("state") == "flipped" else None


def _ended(hop, state, run, **more):
    """A cutover's flip ended by `state` (`AFTER`), kept in the runs'
    log."""
    from . import durable
    durable.append_line(_runs_path(hop), json.dumps(
        {"state": state, "run": (run or {}).get("id"),
         "ended": time.time(), **more}, default=str))


def _close(hop, rec, how):
    rec["state"] = how
    rec["ended"] = time.time()
    clock = rec.get("clock") or {}
    if clock.get("began"):
        rec["outage"] = round((clock.get("stopped") or rec["ended"])
                              - clock["began"], 3)
    rec["took"] = {p: round(v["ended"] - v["began"], 3)
                   for p, v in (rec.get("phases") or {}).items()
                   if v.get("ended") and v.get("began")}
    # on the disk before the record goes: a flip the runs' log lost with
    # the power, its record gone, would be cut over again
    from . import durable
    durable.append_line(_runs_path(hop), json.dumps(rec, default=str))
    _path(hop).unlink(missing_ok=True)
    durable.sync_dir(_path(hop).parent)


# --- running a command at the flip --------------------------------------------

def _run_command(hop, command, timeout, say):
    """The hop's flip command, through the shell, bounded by what is left
    of the budget. Its last line is said without the hop's passwords."""
    from .util import without_secret
    env = {**os.environ, "MIGKIT_HOP": hop.name}
    wait = max(1.0, timeout)
    try:
        got = subprocess.run(["/bin/sh", "-c", command], capture_output=True,
                             text=True, timeout=wait, env=env)
    except subprocess.TimeoutExpired:
        return False, f"still running after {wait:.0f}s"
    out = (got.stdout + got.stderr).strip()
    for secret in (hop.source.password, hop.target.password):
        out = without_secret(out, secret)
    if out:
        say("    " + out.splitlines()[-1][:300])
    return got.returncode == 0, f"exited {got.returncode}"


# --- the pooler ---------------------------------------------------------------

def _pooler_conn(conf):
    import psycopg2

    from .config import secret
    p = conf["pooler"]
    # a reference (env:NAME) went to the pooler as its own text (K7)
    conn = psycopg2.connect(host=p.get("host", "127.0.0.1"),
                            port=int(p.get("port", 6432)),
                            user=p.get("user"),
                            password=secret(p.get("password"),
                                            "cutover.pooler.password"),
                            dbname="pgbouncer", connect_timeout=5)
    conn.autocommit = True
    return conn


def _pooler_dbs(conf, dbs):
    return list((conf["pooler"] or {}).get("databases") or dbs)


def _pooler(conf, command, dbs, timeout):
    """`command` (PAUSE, RESUME, RECONNECT) for each of the pooler's
    databases, each bounded by `timeout`: PAUSE returns only once every
    transaction on its server connections has ended, and one long one
    would hold the cutover past its budget. A PAUSE that did not return is
    left to the undo's RESUME on a console of its own, never issued beside
    it on the same one."""
    conn = _pooler_conn(conf)
    try:
        for name in _pooler_dbs(conf, dbs):
            done = {}

            def ask(name=name):
                try:
                    conn.cursor().execute(f"{command} {name}")
                    done["ok"] = True
                except Exception as e:  # noqa: BLE001 - said below
                    done["error"] = str(e).strip()
            t = threading.Thread(target=ask, daemon=True)
            t.start()
            t.join(max(0.1, timeout))
            if t.is_alive():
                return False, (f"{command} {name} did not return in"
                               f" {timeout:.1f}s")
            error = done.get("error", "")
            # asked to resume what is not paused, or pause what is: the
            # state asked for is the state it is in
            if error and "not paused" not in error \
                    and "already" not in error:
                return False, f"{command} {name}: {error}"
        return True, ""
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - a paused console is let go
            pass


def pooler_ready(conf):
    """"" where the pooler's console answers, or why not."""
    try:
        conn = _pooler_conn(conf)
        conn.cursor().execute("show version")
        conn.close()
        return ""
    except Exception as e:  # noqa: BLE001 - said as the reason
        text = str(e).strip()
        return text.splitlines()[0] if text else repr(e)


# --- preflight ------------------------------------------------------------------

def preflight(hop, eng, dbs, leg, conf, chose=None):
    """[(level, what)] - "fail" stops the cutover before anything is
    written; "warn" is said and does not. Reads only: the lag measure is
    a wait for the stream to pass a position, nothing more. `chose`, a
    dict, is given the ways chosen: the drain's and the freeze's.

    Without `cutover:` most of these only inform, since the operator runs
    the steps: what stops the stream's teardown there is what would lose
    rows - a prepared transaction, a stream that has not caught up (said
    by `_manual` right before it tears down)."""
    from . import freeze
    out = []
    gate = "fail" if conf else "warn"

    def add(level, what):
        out.append((level, what))

    budget = conf["budget"] if conf else None
    from .hopkeys import topology
    if topology(hop) == "two_way" or (hop.options or {}).get("two_way"):
        add("fail", "the hop streams both ways (two_way), and a"
                    " cutover freezes one side: there is no one side to"
                    " move from")
        return out
    if leg.kind not in ("native", "tail") and not (
            conf and (leg.kind in ("pipeline", "none")
                      or (leg.kind == "follow" and leg.fence is not None))):
        add(gate, leg.why + (" - the cutover path drains the change leg"
                             " run from here only where it runs until"
                             " asked (follow_until: stopped)"
                             if conf else " - nothing holds it to a"
                             " position, so the drain is not proven"))
        return out
    add("warn" if leg.kind == "none" else "ok", "the stream: " + {
        "native": "the servers' own replication",
        "tail": "migkit's change tail",
        "follow": "the change leg run from here, ended at a position named"
                  " after the freeze",
        "pipeline": "migkit's streaming pipeline, drained by a mark and"
                    " by its own offsets"}.get(
        leg.kind, leg.why + " - drained by a mark on the source, and left"
                            " to whoever runs it"))
    probes = _probes(hop, leg, dbs, conf)
    drain = drain_way(hop, eng, leg, conf, probes)
    marked = drain.rung is not None and drain.rung.name == "mark"
    how = conf["freeze"] if conf else "operator"
    chosen = None
    choices = [drain]
    if conf and how != "operator":
        got = freeze.choose(hop, eng, dbs, how,
                            (freeze.OURS,) if marked else ())
        choices.append(got)
        if got.rung is None:
            kind = freeze.side_engine(hop, eng, "src")[0]
            add("fail", f"the {hop.engine} source is not frozen by migkit"
                        " here: " + (got.reason if kind in freeze.FREEZES
                                     else freeze.not_held(kind))
                + " - `freeze: operator` has the operator stop its writes"
                  " and migkit prove they stopped")
        else:
            chosen = got.rung.name
            add("ok", f"the freeze: {got.rung.said} ({got.reason})")
        try:
            offer, _ = freeze.offer(hop, eng, dbs, how,
                                    (freeze.OURS,) if marked else ())
        except Exception:  # noqa: BLE001 - nothing offered where unread
            offer = None
        if offer is not None:
            names = ", ".join(c.name for c in offer.changes)
            add("warn", f"offered: {offer.rung.said} - it needs "
                        + "; ".join(c.said for c in offer.changes)
                        + " on the source, made and taken off by migkit"
                          " behind the hop's approvals; answer it ahead"
                          f" (source_changes: {{{names}: yes | no | dba}})"
                        + (f"; without it: {offer.instead.said}"
                           if offer.instead and offer.instead.rung else ""))
    if conf:
        from . import managed
        try:
            src = managed.side(hop, eng, "source")
            dst = managed.side(hop, eng, "target")
            add("ok", "the pair: " + managed.pair_said(src.service,
                                                       dst.service))
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said
            add("warn", "which service each side is could not be read"
                        f" ({str(e).strip()[:120]})")
        for which in conf.get("restore_points") or ():
            why = managed.restore_ready(hop, eng, which)
            add("fail" if why else "ok",
                f"a restore point of the {which} before the clock: "
                + (why or "its provider's API reaches it"))
    if chose is not None:
        chose.update(drain=drain.rung.name if drain.rung else None,
                     freeze=chosen, choices=choices)
    for d in dbs:
        if marked:
            add("ok", f"{d}: the drain: a mark written on the source after"
                      f" the freeze and found on the target - {probes[d][3]}")
        else:
            out += _position_rows(hop, leg, d, conf, gate, probes[d])
        for side, name in (("src", "source"), ("dst", "target")):
            try:
                left = freeze.open_work(hop, eng, d, side,
                                        budget if side == "src" else None)
            except Exception as e:  # noqa: BLE001 - said as the reason
                left = [f"open transactions could not be read ({e})"]
            prepared = [x for x in left if x.startswith("prepared")]
            if prepared:
                add("fail", f"{d} {name}: " + "; ".join(prepared[:3])
                    + " - a prepared transaction outlives every session"
                      " and commits whenever it is told, after any"
                      " position a drain waits for")
            long = [x for x in left if not x.startswith("prepared")]
            if long and side == "src":
                add(gate, f"{d} source: " + "; ".join(long[:3])
                    + (f" - open longer than the {budget:g}s budget"
                       if conf else " - still being written"))
        if chosen:
            for line in freeze.cannot(hop, eng, d, chosen):
                add("fail", f"{d}: {line}")
            try:
                for line in freeze.bypass(hop, eng, d, chosen):
                    add("fail" if "per-database" in line
                        and freeze.SCOPE[chosen] != "server" else "warn",
                        f"{d}: {line}")
            except Exception as e:  # noqa: BLE001
                add("warn", f"{d}: who can write through the freeze could"
                            f" not be read ({e})")
        out += _shape_rows(hop, eng, d, gate)
        out += _set_aside_rows(hop, d, gate)
        # the target's hold read back, or the writes that are not migkit's
        # named - drift, at the gate's level
        out += freeze.kept_rows(hop, eng, d, gate)
        out += _job_rows(hop, eng, d, conf)
        out += _counter_rows(hop, eng, d, conf, gate)
        out += _slot_rows(hop, eng, d)
        if _reverse(hop):
            out += _reverse_rows(hop, eng, d, gate)
    if conf:
        if conf.get("pooler"):
            why = pooler_ready(conf)
            add("fail" if why else "ok",
                f"the pooler's console: {why}" if why else
                "the pooler answers: writes wait there, not fail")
            out += _pooler_sign_in_rows(hop, eng, dbs)
        if conf.get("flip"):
            add("ok", f"the flip runs: {conf['flip']}")
        else:
            add("warn", "no flip command: the application is pointed at"
                        " the target by hand while the clock runs")
        last = last_run(hop)
        from . import rehearsal
        rec = rehearsal.of(hop)
        theirs = rehearsal.last_cutover(rec)
        if last and last.get("state") == "flipped" \
                and last.get("outage") is not None:
            add("fail" if last["outage"] > conf["budget"] else "ok",
                f"the last cutover of this hop took {last['outage']:.2f}s"
                f" of outage; the budget is {conf['budget']:g}s")
        elif theirs and theirs.get("state") == "flipped" \
                and theirs.get("outage") is not None:
            add("fail" if theirs["outage"] > conf["budget"] else "ok",
                f"the rehearsal {rec['name']}'s last cutover took"
                f" {theirs['outage']:.2f}s of outage; the budget is"
                f" {conf['budget']:g}s")
        else:
            add("warn", "no cutover of this hop has been measured to the"
                        f" end: the {conf['budget']:g}s budget is the only"
                        " bound - rehearse on a copy first (goal:"
                        " rehearse)")
    return out


def _pooler_sign_in_rows(hop, eng, dbs):
    """Whether the pooler can sign the application in on the target once
    it is pointed there: it signs in with what the source's password hash
    gave it, and a hash the target salted anew refuses that - measured,
    PgBouncer 1.25 in front of PostgreSQL 16: `password authentication
    failed` for the first write after the repoint, the same password on
    both sides."""
    from . import freeze
    kind, src = freeze.side_engine(hop, eng, "src")
    if kind != "postgres":
        return []
    q = ("select rolname||chr(31)||coalesce(rolpassword, '') from pg_authid"
         " where rolcanlogin")
    try:
        a = dict(line.split("\x1f", 1) for line in
                 src._psql("src", dbs[0], q).splitlines() if line)
        b = dict(line.split("\x1f", 1) for line in
                 src._psql("dst", dbs[0], q).splitlines() if line)
        roles = freeze.app_roles(hop, eng, dbs[0], "src")
    except Exception as e:  # noqa: BLE001 - said as the reason
        return [("warn", f"the accounts' password hashes could not be"
                         f" compared ({str(e).strip()[:120]})")]
    differ = [r for r in roles if a.get(r) and b.get(r) != a.get(r)]
    if differ:
        return [("fail", "through the pooler these accounts cannot sign in"
                         " on the target: their password hash is not the"
                         f" source's ({', '.join(differ[:5])}) - carry them"
                         f" with `migkit users {hop.name}`")]
    return [("ok", "the application's accounts sign in on the target as on"
                   " the source (the same password hashes)")]


def _probes(hop, leg, dbs, conf):
    """{db: (position, reached, seconds, why)}: the source's position as
    the stream's own fence reads it, and whether the stream passed it
    within `start_below` - True, False, or None where no stream of the
    hop's reads it; `why` where there is no position. Reads only: the
    lag measure is a wait, nothing more."""
    from . import freeze
    wait = conf["start_below"] if conf else 1.0
    out = {}
    for d in dbs:
        if leg.fence is None:
            out[d] = (None, None, 0.0, "the stream keeps no position migkit"
                                       " can hold it to")
            continue
        try:
            pos = leg.fence.src_lsn(d)
        except Exception as e:  # noqa: BLE001 - said as the reason
            out[d] = (None, None, 0.0, "the source's position could not be"
                                       f" read ({e})")
            continue
        if pos is None:
            out[d] = (None, None, 0.0, "the source gives no position this"
                      " stream can be held to"
                      + (" (GTID is off)" if freeze.side_engine(
                          hop, leg.fence, "src")[0] == "mysql" else ""))
            continue
        began = time.monotonic()
        got = leg.fence.fence_wait(d, pos, timeout=max(wait, 0.5))
        out[d] = (pos, got, time.monotonic() - began,
                  "no stream of this hop's reads the source" if got is None
                  else "")
    return out


def drain_way(hop, eng, leg, conf, probes):
    """The `decide.Choice` of how the drain is proved: by the stream's own
    position where every database has one it can be held to, by a mark
    written on the source otherwise - only in a cutover migkit runs, with
    a freeze of its own, on a pair a mark can be written and read on
    (`drainmark`)."""
    from . import decide, drainmark
    none = [p[3] for p in probes.values() if p[0] is None or p[1] is None]
    facts = decide.Facts({"positions": not none, "why none": (none or [""])[0],
                          "cutover": conf is not None,
                          "operator": bool(conf)
                          and conf["freeze"] == "operator"},
                         mark=lambda: drainmark.able(hop, eng))
    return decide.climb("the drain", _DRAINS, facts)


def _drain_rungs():
    from .decide import Need, Rung
    return (
        Rung("position", "the stream's own position, taken after the freeze"
             " and waited for", needs=(Need(
                 "a position the stream can be held to",
                 lambda f: f["positions"], lambda f: f["why none"]),),
             because="the stream keeps a position of its own"),
        Rung("mark", "a mark written on the source after the freeze and"
             " found on the target",
             needs=(Need("a cutover migkit runs", lambda f: f["cutover"],
                         "without cutover: migkit writes nothing to the"
                         " source"),
                    Need("a freeze of migkit's own",
                         lambda f: not f["operator"],
                         "with freeze: operator migkit writes nothing to the"
                         " source"),
                    Need("a pair a mark is written and read on",
                         lambda f: f["mark"] == "", lambda f: f["mark"])),
             because="the stream keeps no position migkit can hold it to"))


_DRAINS = _drain_rungs()


def _position_rows(hop, leg, d, conf, gate, probe):
    """Whether the stream can be held to a position at all, and how far
    behind it is now (`_probes`)."""
    pos, got, took, why = probe
    wait = conf["start_below"] if conf else 1.0
    if pos is None:
        return [(gate, f"{d}: {why}, so nothing proves the drain")]
    if got is None:
        return [(gate, f"{d}: no stream of this hop's reads the source, so"
                       " nothing proves the drain")]
    if not got:
        return [(gate, f"{d}: the stream is more than {wait:g}s behind the"
                       " source" + (" (start_below)" if conf else ""))]
    return [("ok", f"{d}: the stream reached where the source stood in"
                   f" {took:.2f}s")]


def _shape_rows(hop, eng, d, gate):
    from . import drift
    if eng.maps_columns():
        return []
    watch = drift.reader(eng)
    dst_reader = getattr(eng, "dst_engine", None) or eng
    if watch is None or not hasattr(dst_reader, "column_catalog"):
        return []
    src = drift.shape(watch, "src", d)
    dst = drift.shape(dst_reader, "dst", d)
    if src is None or dst is None:
        return [("warn", f"{d}: the schema could not be read on both sides")]
    if getattr(eng, "src_engine", None) is not None:
        # across engines the types are named differently: tables only
        missing = sorted(set(src) - set(dst))
        return [(gate, f"{d}: not on the target: {', '.join(missing[:5])}")
                ] if missing else [("ok", f"{d}: every table is on the"
                                          " target")]
    said = drift.changes(dst, src)
    if said:
        return [(gate, f"{d}: the schema differs - " + "; ".join(said[:3])
                 + (" ..." if len(said) > 3 else ""))]
    return [("ok", f"{d}: the schema is the same on both sides")]


def _set_aside_rows(hop, d, gate):
    from .setaside import SetAside
    out = []
    for kind, what in (("dropped-triggers", "triggers"),
                       ("dropped-indexes", "indexes")):
        names = sorted({n for _, _, defs in SetAside(hop, d, kind).records()
                        if defs for n in defs})
        if names:
            out.append((gate, f"{d}: {what} a load took off the target are"
                              f" not back ({', '.join(names[:4])}) - one"
                              " left off after the flip changes nothing and"
                              " says nothing"))
    return out


def _job_rows(hop, eng, d, conf):
    from . import jobs
    out = []
    src = jobs.inventory(hop, eng, d, "src")
    dst = jobs.inventory(hop, eng, d, "dst")
    on_src = [n for n, on in (src or []) if on]
    on_dst = [n for n, on in (dst or []) if on]
    moving = conf and conf["jobs"] == "move"
    if on_src:
        out.append(("ok" if moving else "warn",
                    f"{d}: jobs running on the source:"
                    f" {', '.join(on_src[:5])}"
                    + (" - switched off there before the freeze and on on"
                       " the target at the flip" if moving else
                       " - they move to the target by hand")))
    if on_dst:
        out.append(("warn", f"{d}: jobs already running on the target:"
                            f" {', '.join(on_dst[:5])} - they write there"
                            " before the flip"))
    alone = jobs.left_alone(hop, eng, d, "src")
    if alone:
        out.append(("ok", f"{d}: jobs left as they are on both sides:"
                          f" {', '.join(alone[:5])} - they write only"
                          " tables the hop does not take"))
    return out


def _counter_rows(hop, eng, d, conf, gate):
    try:
        plan = eng.repair_plan(d, "sequences")
    except Exception as e:  # noqa: BLE001 - said as the reason
        return [("warn", f"{d}: the counters could not be read ({e})")]
    out = []
    for a in plan:
        if "REFUSED" in (a.note or ""):
            out.append((gate, f"{d}: counters: {a.note} - a key the target"
                              " handed out that the source never did"))
        elif a.statements:
            out.append(("ok" if conf else "warn",
                        f"{d}: {len(a.statements)} counters on the target"
                        " are behind the source"
                        + (" - raised after the drain" if conf else
                           f" - `migkit sync {hop.name} --go --kind"
                           " sequences --tag pre-cutover` after the"
                           " drain")))
    return out


def _slot_rows(hop, eng, d):
    """Other consumers reading the source: after the flip they read a
    server that no longer changes."""
    if _engine(hop) != "postgres" or not hasattr(eng, "own_slots"):
        return []
    mine = set(eng.own_slots(d))
    try:
        got = eng._psql("src", d, "select slot_name from pg_replication_slots"
                                  f" where database = '{d}'"
                                  " and slot_name not like 'migkit\\_delta\\_%'"
                                  " and slot_name not like 'pg\\_%\\_sync\\_%'")
    except Exception:  # noqa: BLE001
        return []
    others = [s for s in got.split() if s not in mine]
    if not others:
        return []
    return [("warn", f"{d}: other consumers read the source"
                     f" ({', '.join(others[:4])}): after the flip they read"
                     " a server that no longer changes - make their slots"
                     " on the target before its writes resume")]


def _reverse_rows(hop, eng, d, gate):
    kind = _engine(hop)
    tdb = hop.target_db(d)
    out = []
    try:
        if kind == "postgres":
            level = eng._psql("dst", d, "show wal_level").strip()
            if level != "logical":
                out.append((gate, f"{tdb}: the stream back needs"
                                  f" wal_level logical on the target, it"
                                  f" is {level}"))
            else:
                out.append(("ok", f"{tdb}: the target can carry a stream"
                                  " back (wal_level logical)"))
        elif kind == "mysql":
            s, t = eng._brands()

            def major(b):
                return tuple(int(x) for x in str(b.version).split("-")[0]
                             .split(".")[:2])
            if s.name == t.name and s.version and t.version \
                    and major(t) > major(s):
                out.append((gate, f"the target ({t.version}) is newer than"
                                  f" the source ({s.version}), and a"
                                  " replica of a newer server is not"
                                  " supported: the stream back cannot run"))
            got = eng._q("dst", "select @@log_bin, @@gtid_mode")
            if not got or int(got[0][0]) != 1 or str(got[0][1]) != "ON":
                out.append((gate, f"{tdb}: the stream back needs the"
                                  " target's binary log and GTIDs on"))
            else:
                out.append(("ok", f"{tdb}: the target can carry a stream"
                                  " back (binary log, GTIDs)"))
    except Exception as e:  # noqa: BLE001
        out.append(("warn", f"{tdb}: whether the stream back can run could"
                            f" not be read ({e})"))
    return out


# --- the plan in words --------------------------------------------------------

def plan(hop, dbs, conf, drain="position"):
    """What the path does, step by step and side by side, as the dry run
    and the approvers see it; for a hop without `cutover:`, what the
    operator does by hand and what migkit proves around it. `drain` is
    how the drain is proved (`drain_way`)."""
    from .hopkeys import CUTOVER
    back = _reverse(hop)
    name = hop.name
    if not conf:
        # the steps and what turning it on adds, as `hopkeys.CUTOVER`
        # writes them for every place that says them
        return ([
            "without `cutover:` in the hop's options migkit writes nothing"
            " to the source (no goal turns it on); by hand, in this"
            " order:"]
            + [f"  {i}. " + s.format(
                hop=name, back=" and starts the stream back" if back else "")
               for i, s in enumerate(CUTOVER["by hand"], 1)]
            + ["with `cutover:` migkit does all of it, timed, and undoes it"
               " on any failure - it adds:"]
            + [f"  - {a}" for a in CUTOVER["adds"]])
    how = conf["freeze"]
    marked = drain == "mark"
    steps = [f"the cutover, within {conf['budget']:g}s of write outage"
             " (undone past it):"]
    points = conf.get("restore_points") or []
    if back or marked or points:
        steps.append("  before the clock: "
                     + "; ".join(w for w in (
                         f"a restore point of the {' and the '.join(points)}"
                         " taken through its provider's API and waited for"
                         if points else "",
                         "the stream back to the old source made and proven"
                         " idle where its engine lets it wait, so the flip"
                         " only starts it" if back else "",
                         "a table of migkit's made on the source and a"
                         " first mark proven to reach the target"
                         if marked else "") if w))
    if conf["jobs"] == "move":
        steps.append("  source: the jobs it runs switched off (recorded)")
    if conf.get("pooler"):
        steps.append(f"  pooler: PAUSE {', '.join(_pooler_dbs(conf, dbs))}"
                     " - the clock starts")
    if how == "operator":
        steps.append("  source: nothing written - migkit waits for the"
                     " operator's stop and proves it")
    else:
        steps.append("  source: frozen "
                     + ("for the whole server" if how == "server" else
                        "per database" if how == "database" else
                        "as the server allows")
                     + " - the application's grants revoked (recorded),"
                       " its sessions ended; read back"
                     + ("" if conf.get("pooler") else " - the clock starts"))
    steps += [
        "  source: a mark written after the freeze; waited for until the"
        " target holds it, and the stream's own offsets until they are"
        " applied" if marked else
        "  source: its position taken after the freeze; the hop's own"
        " stream waited for until it passes it",
        "  both: the rows changed since the last verified pass compared,"
        " fenced; the source's schema compared with the preflight's",
        "  target: counters past the source's"
        + (f", {margin(conf)} apart" if margin(conf) else ""),
        "  target: the stream torn down"
        + ("; the stream back to the old source started, and proven to"
           " reach it" if back else ""),
    ]
    if conf["jobs"] == "move":
        steps.append("  target: the jobs the source ran switched on")
    steps.append("  target: its writes given back"
                 + (f"; then `{conf['flip']}`" if conf.get("flip") else
                    "; then the operator points the application")
                 + ("; the pooler repointed and resumed"
                    if conf.get("pooler") else "") + " - the clock stops")
    steps.append("  after: the old source stays frozen; `migkit rollback"
                 f" {name} --db DB --apply` gives it back")
    steps.append("  the point of no return, approved on its own ("
                 + ("at the flip" if conf.get("no_return") == "at_flip" else
                    f"`migkit move {name} --mode cdc --drop --go` once the"
                    " flip is done")
                 + "): the forward stream ended for good, the old source"
                   " given back its writes, the stream back dropped, what"
                   " the cutover left taken away - going back is refused"
                   " after it")
    return steps


# --- the path -----------------------------------------------------------------

class _Undo(Exception):
    """The path goes back: `why` is said and recorded."""

    def __init__(self, why):
        super().__init__(why)
        self.why = why


def run(hop, eng, dbs, go, leg, say, approve):
    """The cutover: the phased path for a hop with `cutover:`, the proved
    manual one without - and, once a cutover of it has flipped, its point
    of no return (`no_return`). `approve(detail, step)` asks for the hop's
    approvals of this plan and returns who approved. A run that acts
    holds the cutover's lease from its first read of the record to its
    end (`_held`)."""
    if not go:
        return _run(hop, eng, dbs, go, leg, say, approve)
    held = _held(hop, say)
    try:
        return _run(hop, eng, dbs, go, leg, say, approve)
    finally:
        held.release()


def _run(hop, eng, dbs, go, leg, say, approve):
    conf = settings(hop)
    rec = record(hop)
    if rec and rec.get("state") == "flipped":
        # stopped between the flip and its record: it had cut over
        _flipped(hop, rec, say)
        return rec
    if rec and rec.get("state") == "no-return":
        # stopped half way through the point of no return: it goes on
        return no_return(hop, eng, dbs, go, leg, say, approve, rec=rec)
    if conf and past_flip(hop):
        return no_return(hop, eng, dbs, go, leg, say, approve)
    ended = last_run(hop, after=True)
    if conf and ended and ended.get("state") == "no-return":
        say(f"{hop.name} is cut over and past its point of no return"
            f" ({time.strftime('%F %T', time.localtime(ended.get('ended') or 0))}):"
            " there is nothing left to cut over")
        return None
    if rec and rec.get("state") in ("running", "undoing"):
        if not go:
            now = running(hop)
            if now:
                say(f"a cutover of {hop.name} is running"
                    f" ({now.get('host')}, process {now.get('pid')}), at"
                    f" {_where(rec)}")
                return None
            say(f"[yellow]a cutover of {hop.name} stopped at {_where(rec)}"
                " and was not undone; --go undoes it[/yellow]")
            return None
        say(f"[yellow]a cutover of {hop.name} stopped at {_where(rec)}"
            " without finishing - undoing everything it did[/yellow]")
        began = time.strftime("%F %T", time.localtime(rec.get("began", 0)))
        _undo(hop, eng, rec, leg, rec.get("settings") or conf or DEFAULTS,
              say, f"the run of {began} stopped at {_where(rec)}")
        raise SystemExit("the interrupted cutover was undone: the source is"
                         " writable and has what it had. Nothing was cut"
                         f" over; run `migkit move {hop.name} --mode cdc"
                         " --drop --go` again to cut over.")
    from .movers import DRIVEN
    from .wording import without_programs
    chose = {}
    rows = [(lvl, without_programs(what, DRIVEN))
            for lvl, what in preflight(hop, eng, dbs, leg, conf, chose)]
    _say_rows(rows, say)
    for line in plan(hop, dbs, conf, chose.get("drain") or "position"):
        say(line)
    failed = [w for lvl, w in rows if lvl == "fail"]
    if not go:
        if not conf and leg.manual is not None:
            leg.manual(False)
        say("\ndry-run, add --go to cut over"
            + (f" ({len(failed)} of the checks above stop it)"
               if failed else ""))
        return None
    unproven = not conf and os.environ.get("MIGKIT_CUTOVER") == "unproven"
    if failed and not unproven:
        raise SystemExit(
            "the cutover did not start, nothing was written: "
            + "; ".join(failed[:4])
            + ("" if conf else
               ". To tear the stream down anyway, as an abandoned"
               " migration rather than a cutover: MIGKIT_CUTOVER=unproven"))
    detail = {"budget": conf["budget"] if conf else None,
              "freeze": conf["freeze"] if conf else "by hand",
              "reverse": _reverse(hop), "flip": (conf or {}).get("flip"),
              "dbs": list(dbs)}
    if conf and chose.get("drain") == "mark":
        # a mark is written on the source: signed for as part of the plan
        detail["drain"] = "mark"
    if conf and conf["no_return"] == "at_flip":
        detail["no_return"] = "at_flip"
    approvers = approve(detail)
    if conf and conf["no_return"] == "at_flip":
        # both signed before the clock starts: a flip that waited for its
        # point of no return's approval would hold the old source frozen
        # for as long as nobody signed
        no_return_by = approve({**detail, "step": NO_RETURN_STEP},
                               NO_RETURN_STEP)
    if not conf:
        return _manual(hop, eng, dbs, leg, say, approvers, failed, unproven)
    from . import decide
    for got in chose.get("choices") or ():
        # the ways taken, and why, in the run's decision record
        decide.record(hop, got, say)
    got = _phased(hop, eng, dbs, leg, conf, say, approvers,
                  drain=chose.get("drain"), way=chose.get("freeze"))
    if conf["no_return"] == "at_flip":
        return no_return(hop, eng, dbs, True, leg, say, approve,
                         approvers=no_return_by)
    return got


def _say_rows(rows, say):
    mark = {"ok": "[green]ok  [/green]", "warn": "[yellow]warn[/yellow]",
            "fail": "[red]fail[/red]"}
    for lvl, what in rows:
        say(f"  {mark[lvl]} {what}")


def _step(name):
    """A step of the point of no return as the record and the receivers
    name it."""
    return f"point of no return: {name}"


def _where(rec):
    order = (PHASES if rec.get("state") != "no-return"
             else [_step(n) for n in NO_RETURN])
    began = [p for p in order if (rec.get("phases") or {}).get(p)]
    return began[-1] if began else "the start"


def _manual(hop, eng, dbs, leg, say, approvers, failed, unproven):
    """The hop tears its stream down itself; migkit proves around it.
    `unproven`: MIGKIT_CUTOVER=unproven, which tears it down whatever a
    proof says, and records what the proofs said."""
    from . import audit
    unproven = list(failed) if unproven else None
    for d in dbs:
        got = pos = None
        if leg.fence is not None:
            busy = _quiet(hop, eng, d, 1.5)
            if busy and unproven is not None:
                unproven.append(f"{d} still being written - {busy}")
            elif busy:
                raise SystemExit(f"the stream was left up: {d} is still"
                                 f" being written - {busy}. Stop the"
                                 " application's writes (step 2) and run"
                                 " this again")
            pos = leg.fence.src_lsn(d)
            got = leg.fence.fence_wait(d, pos, timeout=float(
                opt(hop, "fence_timeout"))) if pos is not None else None
            if got is False and unproven is not None:
                unproven.append(f"{d}: the stream short of {pos}")
            elif got is False:
                raise SystemExit(f"the stream was left up: it has not"
                                 f" carried everything the source has ({d}"
                                 f" at {pos}). Tearing it down now would"
                                 " lose that; run this again once it has"
                                 " caught up")
        say(f"  {d}: " + ("the stream passed where the source stands"
                          f" ({pos})" if got else
                          "nothing holds the stream to a position" if got
                          is None else "the stream did NOT reach where the"
                          f" source stands ({pos})"))
        audit.append(hop.report_dir() / "changelog.jsonl",
                     {"op": "cutover", "db": d, "by": "hand",
                      "drained": got, "position": pos,
                      "unproven": unproven or None,
                      "approved_by": approvers or []})
    if unproven:
        say("[yellow]torn down unproven, as MIGKIT_CUTOVER=unproven asks:"
            f" {'; '.join(unproven[:3])}[/yellow]")
    return leg.manual(True)


def _quiet(hop, eng, d, window):
    """"" where nothing writes to the source over `window` seconds, or
    what does: a transaction holding writes when it is looked at, and the
    server's own count of rows written in the database moving between the
    first look and the last. The count is what catches an application
    writing short transactions between two looks."""
    from . import freeze
    end = time.monotonic() + window
    seen = []
    first = freeze.writes_seen(hop, eng, d)
    while True:
        try:
            left = freeze.open_work(hop, eng, d, "src", None)
        except Exception:  # noqa: BLE001 - not readable is not proof
            left = []
        seen += [x for x in left if x not in seen]
        if time.monotonic() >= end:
            break
        time.sleep(0.25)
    last = freeze.writes_seen(hop, eng, d)
    if first is not None and last is not None and last > first:
        seen.insert(0, f"{last - first} rows written in {window:g}s")
    return "; ".join(seen[:3])


def _phased(hop, eng, dbs, leg, conf, say, approvers, drain=None,
            way=None):
    """The path, phase after phase. `drain` and `way` are the preflight's
    choices (`drain_way`, `freeze.choose`), made again here where it was
    not asked."""
    from . import drift, freeze, jobs
    rec = {"id": secrets.token_hex(6), "began": time.time(), "dbs": dbs,
           "settings": conf, "approvers": approvers, "phases": {},
           "state": "running", "clock": {}, "left_on_source": {},
           "counters": {}, "stream": {"down": [], "back": []}, "flip": {}}
    _save(hop, rec)
    _audit(hop, rec, {"event": "began", "budget": conf["budget"],
                      "dbs": dbs})
    budget = conf["budget"]
    from . import decide
    if drain is None:
        got = drain_way(hop, eng, leg, conf, _probes(hop, leg, dbs, conf))
        decide.record(hop, got, say)
        drain = got.rung.name if got.rung else "position"
    how = conf["freeze"]
    if how != "operator":
        if way is None:
            got = freeze.choose(hop, eng, dbs, how,
                                (freeze.OURS,) if drain == "mark" else ())
            decide.record(hop, got, say)
            way = got.rung.name if got.rung else how
        how = freeze.SCOPE.get(way, how)
    rec.update(freeze=how, freeze_way=way, drain=drain)

    def left():
        clock = rec["clock"]
        if not clock.get("began"):
            return budget
        return budget - ((clock.get("stopped") or time.time())
                         - clock["began"])

    def phase(name):
        rec["phases"][name] = {"began": time.time()}
        _save(hop, rec)
        failpoint.hit(f"cutover.{name}")

    def done(name, **said):
        rec["phases"][name].update({"ended": time.time(), **said})
        _save(hop, rec)
        _audit(hop, rec, {"phase": name, "event": "done",
                          "seconds": round(rec["phases"][name]["ended"]
                                           - rec["phases"][name]["began"],
                                           3), **said})
        if left() < 0:
            raise _Undo(f"the {budget:g}s budget ran out after {name}")

    watch = drift.reader(eng)
    before = {d: drift.shape(watch, "src", d) if watch else None
              for d in dbs}
    try:
        points = conf.get("restore_points") or []
        if drain == "mark" or points or (leg.back_up is not None
                                         and leg.back_arm is not None):
            # before the clock: what the flip would otherwise make inside
            # the outage, made and proven now, and undone as any step is
            phase("prepare")
            if points:
                # a way back the provider keeps, taken while the source
                # still writes: kept whatever the path does after, as a
                # restore point is - nothing to undo
                from . import managed
                rec["restore_points"] = managed.restore_points(
                    hop, eng, points, lambda m: say(f"  {m}"))
                _save(hop, rec)
            if drain == "mark":
                _mark_ready(hop, eng, dbs, leg, rec, say)
            if leg.back_up is not None:
                rec["back"] = _arm_back(hop, dbs, leg, rec, say)
            done("prepare", back=rec.get("back"), drain=drain,
                 restore_points=rec.get("restore_points"))
        elif leg.back_up is not None:
            rec["back"] = "at_flip"
        if conf["jobs"] == "move":
            phase("jobs_off")
            n = 0
            for d in dbs:
                running = [nm for nm, on in (jobs.inventory(
                    hop, eng, d, "src") or []) if on]
                n += jobs.switch(hop, eng, d, "src", running, False, say)
            done("jobs_off", switched=n)
        if conf.get("pooler"):
            phase("pause")
            rec["clock"]["began"] = time.time()
            _save(hop, rec)
            ok, why = _pooler(conf, "PAUSE", dbs, left())
            if not ok:
                raise _Undo(f"the pooler did not pause: {why}")
            say("  pooler: paused - writes wait there")
            done("pause")
        phase("freeze")
        if not rec["clock"].get("began"):
            rec["clock"]["began"] = time.time()
            _save(hop, rec)
        if how == "operator":
            _operator_stop(hop, eng, dbs, left, say)
        else:
            freeze.freeze_source(hop, eng, dbs,
                                 lambda m: say(f"  {m}"), way)
            for d in dbs:
                held = freeze.proof(hop, eng, d, way)
                bad = [w for ok, w in held if not ok]
                if bad:
                    raise _Undo(f"{d}: the freeze did not hold - "
                                + "; ".join(bad[:3]))
                say(f"  {d}: the freeze holds - "
                    + "; ".join(w for _, w in held))
        done("freeze", how=how)
        phase("drain")
        at = {}
        for i, d in enumerate(dbs):
            if drain == "mark":
                at[d] = _drain_by_mark(hop, eng, d, i, leg, rec, left, say)
            else:
                pos = _bounded(leg.fence.src_lsn, left(), d)
                if pos is None or pos is False:
                    raise _Undo(f"{d}: the source gave no position to drain"
                                " to" + (" within the budget" if pos is False
                                         else ""))
                at[d] = pos
                got = _bounded(leg.fence.fence_wait, left(), d, pos,
                               timeout=max(0.1, left()))
                if not got:
                    raise _Undo(f"{d}: the stream did not pass {pos}, where"
                                " the source stood after the freeze"
                                + (", within the budget" if got is False
                                   else " - it has no position"))
                say(f"  {d}: the stream passed {pos}, taken after the"
                    " freeze")
            if leg.settle is not None:
                got = _bounded(leg.settle, left(), d, max(0.1, left()))
                if not got:
                    raise _Undo(f"{d}: the stream's own positions did not"
                                " settle after the freeze"
                                + (" within the budget" if got is False
                                   else ""))
                say(f"  {d}: the stream's own positions settled")
        done("drain", positions=at)
        phase("verify")
        for d in dbs:
            _final_check(hop, eng, d, before.get(d), left, say)
        done("verify")
        phase("counters")
        for d in dbs:
            _counters(hop, eng, d, conf, rec, say)
        done("counters")
        phase("stream")
        for d in dbs:
            rec["stream"]["down"].append(d)
            _save(hop, rec)
            leftover = leg.down(d) or []
            if leftover:
                rec["left_on_source"][d] = leftover
                _save(hop, rec)
        if leg.back_up is not None:
            for d in dbs:
                if rec.get("back") == "armed":
                    # made and proven before the clock: only started now
                    leg.back_fire(d)
                    continue
                rec["stream"]["back"].append(d)
                _save(hop, rec)
                leg.back_up(d)
        done("stream")
        if leg.back_up is not None:
            phase("reverse")
            rev = leg.back_fence
            for d in dbs:
                tdb = hop.target_db(d)
                pos = _bounded(rev.src_lsn, left(), tdb)
                got = _bounded(rev.fence_wait, left(), tdb, pos,
                               timeout=max(0.1, left())) \
                    if pos else pos
                if not got:
                    raise _Undo(f"{tdb}: the stream back did not reach the"
                                " old source"
                                + (" within the budget" if got is False
                                   else " - nothing proves it"))
                say(f"  {tdb}: the stream back reached the old source"
                    f" ({pos})")
            done("reverse")
        if conf["jobs"] == "move":
            phase("jobs_on")
            n = 0
            moved = [e for e in jobs.recorded(hop) if e["side"] == "src"]
            for d in dbs:
                names = [e["name"] for e in moved if e["db"] == d]
                n += jobs.switch(hop, eng, d, "dst", names, True, say)
            done("jobs_on", switched=n)
        phase("flip")
        if (hop.options or {}).get("protect_target"):
            rec["flip"]["thawed"] = True
            _save(hop, rec)
            for d in dbs:
                freeze.thaw(hop, eng, d, lambda m: say(f"  {m}"))
        if conf.get("flip"):
            rec["flip"]["ran"] = True
            _save(hop, rec)
            ok, why = _run_command(hop, conf["flip"], left(), say)
            if not ok:
                raise _Undo(f"the flip command failed: {why}")
        if conf.get("pooler"):
            for command in ("RECONNECT", "RESUME"):
                ok, why = _pooler(conf, command, dbs, max(1.0, left()))
                if not ok:
                    raise _Undo(f"the pooler did not {command.lower()}:"
                                f" {why}")
        rec["clock"]["stopped"] = time.time()
        rec["phases"]["flip"]["ended"] = time.time()
        rec["state"] = "flipped"
        _save(hop, rec)
    except _Undo as e:
        return _undone(hop, eng, rec, leg, conf, say, e.why)
    except KeyboardInterrupt:
        _undone(hop, eng, rec, leg, conf, say, "interrupted")
        raise
    except (Exception, SystemExit) as e:  # noqa: BLE001 - any failure goes back
        text = str(e).strip()
        return _undone(hop, eng, rec, leg, conf, say,
                       text.splitlines()[0] if text else type(e).__name__)
    _after_flip(hop, dbs, leg, rec, say)
    return _flipped(hop, rec, say)


def _back_rungs():
    from .decide import Need, Rung
    return (
        Rung("armed", "the stream back made and proven before the freeze,"
             " started at the flip", needs=(Need(
                 "a stream back its engine lets wait idle",
                 lambda f: f["arms"], "this engine starts its stream back"
                 " only whole"),),
             because="made outside the outage, the flip only starts it"),
        Rung("at_flip", "the stream back made at the flip",
             because="made and started at the flip"))


#: how the stream back is made (`reverse: at_cutover`): ahead and idle,
#: or at the flip - ranked by what the flip measured each to take, where
#: both were timed on this hop (`decide.HopCosts`, "cutover-back/")
_BACKS = _back_rungs()


def _arm_back(hop, dbs, leg, rec, say):
    """The stream back made before the clock and left idle, then proven
    ready (`Leg.back_proof`), so the flip only starts it - or, where its
    engine starts it only whole, or it is not ready, made at the flip as
    ever: a way that fails is undone and the next taken (`decide.attempt`).
    Each database is recorded before it is made, so the undo drops it."""
    from . import decide
    made = []

    def run(choice):
        if choice.rung.name != "armed":
            return choice.rung.name
        for d in dbs:
            rec["stream"]["back"].append(d)
            _save(hop, rec)
            made.append(d)
            leg.back_arm(d)
            why = leg.back_proof(d)
            if why:
                raise decide.Blocked(f"{hop.target_db(d)}: the stream back"
                                     f" made ahead is not ready - {why}")
        say("  the stream back to the old source made and proven ready;"
            " the flip only starts it")
        return "armed"

    def undo(rung, error):
        for d in reversed(made):
            leg.back_down(d)
            rec["stream"]["back"].remove(d)
            _save(hop, rec)
        made.clear()
    choice, got = decide.attempt(
        "the stream back", _BACKS, run,
        decide.Facts({"arms": leg.back_arm is not None}),
        costs=decide.HopCosts(hop, "cutover-back/"), undo=undo,
        log=lambda m: say(f"  [yellow]{m}[/yellow]"))
    decide.record(hop, choice, say)
    return got


def _mark_ready(hop, eng, dbs, leg, rec, say):
    """The mark's table made on the source (`drainmark`), before the
    clock, and a first mark proven to reach the target - and the stream's
    own positions to settle behind it - within the hop's `fence_timeout`:
    a stream that does not carry a table made now is found here, with
    nothing frozen yet."""
    from . import drainmark
    wait = float(opt(hop, "fence_timeout"))
    for i, d in enumerate(dbs):
        rec.setdefault("mark", {})[d] = {"made": True}
        _save(hop, rec)
        drainmark.make(hop, eng, d)
        token = f"{rec['id']}-{i}-0"
        began = time.monotonic()
        drainmark.put(hop, eng, d, token)
        if not drainmark.wait(hop, eng, d, token, wait, leg.seen):
            raise _Undo(f"{d}: a mark written on the source did not reach"
                        f" the target in {wait:g}s - the stream does not"
                        " carry a table made now; nothing was frozen")
        if leg.settle is not None and not leg.settle(d, wait):
            raise _Undo(f"{d}: the stream's own positions did not settle in"
                        f" {wait:g}s behind a mark; nothing was frozen")
        say(f"  {d}: a mark written on the source reached the target in"
            f" {time.monotonic() - began:.2f}s")


def _drain_by_mark(hop, eng, d, i, leg, rec, left, say):
    """The drain of one database by a mark written after the freeze: the
    applier that keeps the source's order has applied everything before
    it once the target holds it."""
    from . import drainmark
    token = f"{rec['id']}-{i}-1"
    if _bounded(drainmark.put, left(), hop, eng, d, token) is False:
        raise _Undo(f"{d}: the mark could not be written on the source"
                    " within the budget")
    got = _bounded(drainmark.wait, left(), hop, eng, d, token,
                   max(0.1, left()), leg.seen)
    if not got:
        raise _Undo(f"{d}: the mark written after the freeze did not reach"
                    " the target" + (" within the budget" if got is False
                                     else ""))
    say(f"  {d}: the mark written after the freeze reached the target")
    return f"mark {token}"


def _after_flip(hop, dbs, leg, rec, say):
    """Once the clock has stopped: the time the stream back's way took,
    kept for the next cutover to choose by (`_BACKS`); and what the
    stream leaves behind taken away (`Leg.after_flip`) - a failure there
    is said and kept for the point of no return, never undone: the flip
    stands."""
    from . import decide
    took = sum(v["ended"] - v["began"] for p, v in rec["phases"].items()
               if p in ("stream", "reverse") and v.get("ended"))
    if rec.get("back") and took:
        decide.HopCosts(hop, "cutover-back/").saw(rec["back"], 1, took)
    if leg.after_flip is None:
        return
    for d in dbs:
        try:
            left = leg.after_flip(d) or []
        except Exception as e:  # noqa: BLE001 - said, the flip stands
            say(f"  [yellow]{d}: what the stream left could not be taken"
                f" away: {str(e).strip()[:200]}[/yellow]")
            continue
        if left:
            rec["left_on_source"].setdefault(d, []).extend(left)
            _save(hop, rec)


# --- the point of no return ---------------------------------------------------
# After the flip the old source stays frozen and, where the hop asks, the
# stream back keeps it level with the new primary: going back stays
# possible (`rollback --apply`). The point of no return ends that, as a
# step of its own behind an approval of its own, and gives the old source
# back as it was:
#   tail_off   the forward stream ended for good - a tail paused at the
#              flip is stopped (it carries nothing since: the source is
#              frozen)
#   thaw       the old source given back its writes, exactly what the
#              freeze took
#   back_off   the stream back dropped: from here nothing the target
#              takes reaches the old source - the point itself
#   left       what the cutover left on the old source run or taken away
#              (the statements its frozen source refused, the mark's
#              table), now that it is written again
#   origin     migkit's own batch table taken off the target where the
#              hop asks (`origin_table: drop`), once no tail runs
# Each is recorded before it acts. Up to the stream back's drop every step
# is undone on a failure - the old source frozen again - and going back is
# still possible; a drop that failed half way leaves the old source frozen
# again and the rest to the next run, which goes on from the record; a
# failure after it is said and kept, since the step that cannot be undone
# has run. After it, going back is refused.

def no_return(hop, eng, dbs, go, leg, say, approve, rec=None,
              approvers=None):
    """The point of no return of the cutover that flipped last (`rec` is
    the record of one stopped half way, which this one goes on from).
    `approvers`, where the flip's own run already asked for them
    (`no_return: at_flip`)."""
    from . import freeze
    flipped = (rec or {}).get("flipped") or past_flip(hop)
    if not flipped:
        raise SystemExit(f"{hop.name} has no cutover past its flip to take"
                         " past the point of no return")
    back = _reverse(hop) and leg.back_down is not None
    when = time.strftime("%F %T", time.localtime(flipped.get("began") or 0))
    say(f"the cutover of {when} flipped"
        + (f" with {flipped['outage']:.2f}s of write outage"
           if flipped.get("outage") is not None else "")
        + "; its point of no return:")
    for line in _no_return_plan(hop, dbs, leg, flipped, back):
        say(f"  {line}")
    if not go:
        say("\ndry-run, add --go to take the cutover past its point of no"
            " return")
        return None
    if approvers is None and rec is None:
        approvers = approve({"step": NO_RETURN_STEP,
                             "run": flipped.get("id"), "dbs": list(dbs),
                             "stream back": back}, NO_RETURN_STEP)
    if rec is None:
        rec = {"id": secrets.token_hex(6), "began": time.time(),
               "state": "no-return", "dbs": list(dbs), "phases": {},
               "approvers": approvers or [], "flipped": flipped,
               "back_off": []}
        _save(hop, rec)
    else:
        say(f"[yellow]the point of no return stopped at {_where(rec)};"
            " going on from there[/yellow]")
    way = flipped.get("freeze_way") or flipped.get("freeze")

    def phase(name):
        rec["phases"].setdefault(_step(name), {"began": time.time()})
        _save(hop, rec)
        failpoint.hit(f"no_return.{name}")

    def done(name, **said):
        at = rec["phases"][_step(name)]
        at.update({"ended": time.time(), **said})
        _save(hop, rec)
        _audit(hop, rec, {"phase": _step(name), "event": "done",
                          "seconds": round(at["ended"] - at["began"], 3),
                          **said})

    def s(m):
        say(f"  {m}")
    try:
        phase("tail_off")
        if leg.end is not None:
            for d in dbs:
                leg.end(d)
        done("tail_off")
        phase("thaw")
        if freeze.source_frozen(hop, dbs):
            freeze.thaw_source(hop, eng, dbs, s)
        done("thaw")
        phase("back_off")
        if back:
            for d in dbs:
                if d in rec["back_off"]:
                    continue
                leg.back_down(d)
                rec["back_off"].append(d)
                _save(hop, rec)
        done("back_off", dropped=list(rec["back_off"]))
    except (Exception, SystemExit, KeyboardInterrupt) as e:
        text = (str(e).strip().splitlines() or [type(e).__name__])[0]
        _no_return_back(hop, eng, dbs, rec, way, say, text)
        raise
    kept = []
    phase("left")
    for d, stmts in (flipped.get("left_on_source") or {}).items():
        for x in stmts:
            try:
                eng.apply_replication_stmt("src", d, x)
            except Exception as e:  # noqa: BLE001 - said, kept
                kept.append(f"{d}: {x} - {str(e).strip()[:120]}")
    for d in (flipped.get("mark") or {}):
        try:
            from . import drainmark
            drainmark.drop(hop, eng, d)
        except Exception as e:  # noqa: BLE001 - said, kept
            kept.append(f"{d}: the mark's table - {str(e).strip()[:120]}")
    done("left", kept=kept or None)
    phase("origin")
    if leg.origin is not None:
        for d in dbs:
            try:
                leg.origin(d)
            except Exception as e:  # noqa: BLE001 - said, kept
                kept.append(f"{d}: migkit's batch table - {e}")
    done("origin")
    for k in kept:
        say(f"  [yellow]not taken away: {k}[/yellow]")
    _ended(hop, "no-return", flipped, approved_by=rec.get("approvers"),
           kept=kept or None, at=rec["began"])
    _path(hop).unlink(missing_ok=True)
    say("[green]past the point of no return[/green]: the old source is"
        " given back as it was" + (", the stream back dropped" if back
                                   else "")
        + f"; `migkit rollback {hop.name}` is refused from here")
    return rec


def _no_return_plan(hop, dbs, leg, flipped, back):
    from .hopkeys import origin_dropped
    out = []
    if leg.end is not None:
        out.append("the forward stream: ended for good - it carries nothing"
                   " since the flip")
    out.append("old source: its writes given back - exactly what the freeze"
               " took")
    out.append("the stream back: dropped - from here nothing the target"
               " takes reaches the old source, and going back is refused"
               if back else "no stream back runs: going back after the flip"
                            " already loses what the target took")
    left = sum(len(v) for v in (flipped.get("left_on_source") or {})
               .values())
    marks = len(flipped.get("mark") or {})
    if left or marks:
        out.append("old source: " + ", ".join(w for w in (
            f"{left} statements its frozen state refused run" if left
            else "", "the mark's table taken away" if marks else "")
            if w))
    if leg.origin is not None and origin_dropped(hop):
        out.append("target: migkit's own batch table taken away"
                   " (origin_table: drop)")
    return out


def _no_return_back(hop, eng, dbs, rec, way, say, why):
    """The point of no return stopped before or at the stream back's drop:
    the old source frozen again as the cutover had it, so going back stays
    possible - the whole of it undone where no stream back was dropped
    yet, the rest left to the next run where one was."""
    from . import freeze
    say(f"[red]the point of no return stopped: {why}[/red]")
    if _step("thaw") in rec["phases"] and way and way != "operator":
        # a hold taken again grows its records and takes only what is
        # given back already: safe on a thaw that stopped half way
        try:
            freeze.freeze_source(hop, eng, dbs, lambda m: say(f"  {m}"), way)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said
            say(f"  [red]the old source could not be frozen again:"
                f" {str(e).strip()[:200]}[/red]")
    if rec.get("back_off"):
        rec["why"] = why
        _save(hop, rec)
        raise SystemExit(
            f"the point of no return stopped at the stream back's drop"
            f" ({why}): {', '.join(rec['back_off'])} dropped already; the"
            f" old source is frozen again. `migkit move {hop.name} --mode"
            " cdc --drop --go` goes on from there")
    _path(hop).unlink(missing_ok=True)
    raise SystemExit(f"the point of no return was undone ({why}): the old"
                     " source is frozen as the cutover left it, and going"
                     " back is still possible")


def _bounded(fn, within, *args, **kw):
    """`fn(*args, **kw)`, given no longer than `within` seconds (and a
    moment): a server that stops answering holds a call for as long as
    its connection's own timeouts and retries allow, which a clock with a
    budget cannot. What does not return in time is False - it only reads,
    and is let go."""
    got = {}

    def work():
        try:
            got["value"] = fn(*args, **kw)
        except BaseException as e:  # noqa: BLE001 - handed back below
            got["error"] = e
    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(max(0.1, within) + 0.5)
    if t.is_alive():
        return False
    if "error" in got:
        raise got["error"]
    return got["value"]


def _operator_stop(hop, eng, dbs, left, say):
    """freeze: operator - migkit writes nothing and waits, within the
    budget, until nothing writes to the source."""
    for d in dbs:
        while True:
            busy = _quiet(hop, eng, d, 1.0)
            if not busy:
                say(f"  {d}: nothing writes to the source, nothing is"
                    " prepared there")
                break
            if left() <= 0:
                raise _Undo(f"{d}: the source was still being written when"
                            f" the budget ran out - {busy}")


def _final_check(hop, eng, d, before, left, say):
    """The rows changed since the last verified pass where a pass has run
    (`watch --verify --delta`), every table otherwise; and the source's
    schema against the preflight's. In a thread, bounded by the budget:
    what is still reading when it runs out is let go, it only reads."""
    from . import drift
    result = {}

    def work():
        try:
            if _has_delta(hop, eng, d):
                got = eng.delta_verify(d)
                result["how"] = ("the rows changed since the last verified"
                                 " pass")
            else:
                got = eng.check_data(d)
                result["how"] = "every table"
            # anything but "same" by the one verdict stops it - a flip on
            # a check that compared nothing (every answer skipped, or no
            # answer at all) is the one mistake here with no undo
            from . import verdict
            # every field of each answer: the check it is, the rows it
            # counted - what holds a hop that keeps some changes off the
            # target to them (`verdict.hold`); without them such a hop's
            # kept rows read as a difference and it never cut over
            result["decision"] = verdict.decide(
                [dict(vars(r)) for r in got], hop)
            result["skipped"] = [r.scope for r in got if r.status == "skip"]
        except Exception as e:  # noqa: BLE001 - a check that fails is a stop
            result["error"] = str(e).strip()[:300]
    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(max(0.1, left()))
    if t.is_alive():
        raise _Undo(f"{d}: the final check was still reading when the"
                    " budget ran out")
    if "error" in result:
        raise _Undo(f"{d}: the final check failed - {result['error']}")
    decision = result["decision"]
    if decision.status != "same":
        raise _Undo(f"{d}: the final check did not find the two the same"
                    f" ({decision.word}) - {decision.why}")
    watch = drift.reader(eng)
    if watch is not None and before is not None:
        moved = drift.changes(before, drift.shape(watch, "src", d))
        if moved:
            raise _Undo(f"{d}: the source's schema changed during the"
                        f" cutover - {'; '.join(moved[:3])}")
    say(f"  {d}: the final check - {result['how']} - found the two the"
        " same" + (f"; not compared: {', '.join(result['skipped'][:4])}"
                   if result.get("skipped") else ""))


def _has_delta(hop, eng, d):
    """Whether a verified pass of `watch --verify --delta` stands to start
    from: only then does the delta check compare what changed, rather
    than record a starting point and compare nothing. A position taken
    under another scope than the hop's now, or taken again after the
    scope changed and no whole check since, does not stand: the tables
    the scope took in were never compared whole (`Engine.delta_covers`),
    and every table is compared instead."""
    if not hasattr(eng, "delta_verify"):
        return False
    covers = getattr(eng, "delta_covers", None)
    if callable(covers):
        try:
            if not covers(d):
                return False
        except (Exception, SystemExit):  # noqa: BLE001 - not known (a
            # record cut short too): every table, then
            return False
    if hasattr(eng, "_delta_slot"):
        try:
            return eng._psql("src", d, "select count(*) from"
                                       " pg_replication_slots where"
                                       f" slot_name = '{eng._delta_slot(d)}'"
                             ).strip() == "1"
        except Exception:  # noqa: BLE001
            return False
    where = hop.report_dir(d)
    return any((where / f).exists() for f in (
        "delta-pos.json", "delta-token.json", "pair-delta-token.json"))


def _counters(hop, eng, d, conf, rec, say):
    """The target's counters past the source's, with the undo written
    first; `keys: apart` adds its margin (`SEQUENCE_MARGIN`, which both
    counters' plans read), for this thread's plan only."""
    from .engines.postgres import SEQUENCE_MARGIN
    gap = margin(conf)
    token = SEQUENCE_MARGIN.set(gap) if gap else None
    try:
        plan = eng.repair_plan(d, "sequences")
    finally:
        if token is not None:
            SEQUENCE_MARGIN.reset(token)
    for a in plan:
        if "REFUSED" in (a.note or ""):
            raise _Undo(f"{d}: counters - {a.note}")
    # the restore point `rollback --state pre-cutover` reads, as the
    # runbook's `sync --go --kind sequences --tag pre-cutover` keeps one
    if hasattr(eng, "snapshot_state"):
        from .state import get_store
        point = get_store(hop).new_point(d, "pre-cutover")
        eng.snapshot_state(d, point.dir, "sequences")
        point.set_meta(op="cutover", db=d)
        point.commit(time.strftime("%F %T"))
    n = 0
    for a in plan:
        if not a.statements:
            continue
        rec["counters"].setdefault(d, []).extend(a.undo)
        _save(hop, rec)
        eng.apply(d, a)
        n += len(a.statements)
    say(f"  {d}: {n} counters on the target raised past the source's"
        + (f", {gap} apart" if gap else ""))


def _flipped(hop, rec, say):
    from . import jobs
    jobs.forget(hop)
    _close(hop, rec, "flipped")
    out = rec.get("outage") or 0.0
    budget = (rec.get("settings") or {}).get("budget")
    _audit(hop, rec, {"event": "flipped", "outage": out,
                      "took": rec.get("took"),
                      "left_on_source": rec.get("left_on_source") or None})
    say(f"[green]cut over: {out:.2f}s of write outage[/green] ("
        + ", ".join(f"{p} {s:.2f}s" for p, s in rec["took"].items()) + ")"
        + (f" - past the {budget:g}s budget, which the flip itself took"
           if budget and out > budget else ""))
    for d, stmts in (rec.get("left_on_source") or {}).items():
        say(f"  {d}: left on the old source until it is written again"
            " (going back, or the point of no return, runs them):"
            f" {'; '.join(stmts)}")
    say("  the old source stays frozen, so nothing is written to it by"
        f" mistake; `migkit rollback {hop.name} --db DB --apply` gives it"
        " back, and so does the point of no return: `migkit move"
        f" {hop.name} --mode cdc --drop --go` again, approved on its own")
    return rec


def _undone(hop, eng, rec, leg, conf, say, why):
    # a server's own words are kept, the program that carried them is not
    from .movers import DRIVEN
    from .wording import without_programs
    why = without_programs(why, DRIVEN)
    say(f"[red]the cutover goes back: {why}[/red]")
    rec["why"] = why
    _undo(hop, eng, rec, leg, conf, say, why)
    stop = (f"the cutover was undone ({why}); the source is writable and"
            " has what it had"
            + (f" - {rec['outage']:.2f}s of write outage"
               if rec.get("outage") is not None else ""))
    # the receivers were told of the undo as it was recorded
    from . import notify
    notify.already(stop)
    raise SystemExit(stop)


def _undo(hop, eng, rec, leg, conf, say, why):
    """Every recorded step put back, the last first. A step that fails is
    said and the rest still run - the source is given back its writes
    whatever else could not be put back - and what failed is kept for the
    next run to try again."""
    from . import drainmark, freeze, jobs
    rec["state"] = "undoing"
    _save(hop, rec)
    ph = rec.get("phases") or {}
    dbs = rec.get("dbs") or []
    errors = []

    def s(m):
        say(f"  {m}")

    def step(what, fn, *args):
        from .movers import DRIVEN
        from .wording import without_programs
        try:
            fn(*args)
        except BaseException as e:  # noqa: BLE001 - said, the rest go on
            text = without_programs(str(e).strip(), DRIVEN)[:200]
            errors.append(f"{what}: {text}")
            say(f"  [red]{what} could not be undone: {text}[/red]")

    flip = rec.get("flip") or {}
    if flip.get("ran"):
        if (conf or {}).get("flip_undo"):
            def unflip():
                ok, said = _run_command(hop, conf["flip_undo"], 60, say)
                if not ok:
                    raise RuntimeError(said)
                s("the flip undone")
            step("the flip", unflip)
        else:
            s("[yellow]the flip command ran and the hop gives no"
              " flip_undo: point the application back at the source by"
              " hand[/yellow]")
    if flip.get("thawed"):
        for d in dbs:
            step(f"{d}: the target's hold", freeze.freeze, hop, eng, d, s)
    if "jobs_on" in ph:
        step("the target's jobs", jobs.undo, hop, eng, s, "dst")
    stream = rec.get("stream") or {}
    if leg is not None and leg.back_down is not None:
        for d in stream.get("back", []):
            step(f"{d}: the stream back", leg.back_down, d)
    restore = list(stream.get("down", []))
    if leg is not None and restore and not leg.after_thaw:
        # put back before the source takes writes again: its new slot
        # starts from now, and a write before it would not be carried
        for d in restore:
            step(f"{d}: the stream", leg.up, d)
        restore = []
    for d, stmts in (rec.get("counters") or {}).items():
        if stmts:
            from .engines.base import RepairAction
            step(f"{d}: the target's counters", eng.apply, d,
                 RepairAction(d, "sequences", stmts, [], "cutover undone"))
    if "freeze" in ph:
        step("the source's freeze", freeze.thaw_source, hop, eng, dbs, s)
    if leg is not None:
        for d in restore:
            step(f"{d}: the stream", leg.up, d)
    if "jobs_off" in ph:
        step("the source's jobs", jobs.undo, hop, eng, s, "src")
    if "pause" in ph:
        def resume():
            ok, said = _pooler(conf, "RESUME", dbs, 30)
            if not ok:
                raise RuntimeError(said)
            s("pooler: resumed")
        step("the pooler", resume)
    for d in rec.get("mark") or {}:
        # made first, before the clock: taken away last, from a source
        # that takes writes again
        step(f"{d}: the mark's table", drainmark.drop, hop, eng, d)
    clock = rec.setdefault("clock", {})
    if clock.get("began") and not clock.get("stopped"):
        clock["stopped"] = time.time()
    if errors:
        rec["undo_error"] = "; ".join(errors)
        _save(hop, rec)
        raise SystemExit("the cutover could not be fully undone: "
                         + "; ".join(errors) + ". What is left is recorded;"
                         f" `migkit move {hop.name} --mode cdc --drop --go`"
                         " tries it again")
    _close(hop, rec, "undone")
    _audit(hop, rec, {"event": "undone", "why": why,
                      "outage": rec.get("outage")})


# --- going back -----------------------------------------------------------------

def back_plan(hop, eng, db, say):
    """Where the hop streamed back (`reverse: at_cutover`) or cut over
    (`cutover:`), going back puts the old source's counters past every key
    the new primary handed out: the stream back carries rows, not
    counters, and the first insert on the old source took an id the new
    primary had already used. Read here, from the new primary as it
    stands - before `rollback` puts the target's own counters back to its
    restore point, which would hide the keys handed out since - and
    applied by `back_apply`. None, and nothing written to the source,
    where the hop has neither option; that is said. Refused past the
    point of no return (`no_return`): the stream back is gone and the
    old source has its writes, so nothing carries the target back."""
    from .engines import get_engine
    rec = record(hop)
    ended = last_run(hop, after=True)
    if (rec and rec.get("state") == "no-return") or (
            ended and ended.get("state") == "no-return"):
        who = ", ".join((rec or ended or {}).get("approvers")
                        or (ended or {}).get("approved_by") or []) or "the hop"
        raise SystemExit(
            f"{hop.name} is past the point of no return (approved by {who}):"
            " the stream back is dropped and the old source takes its own"
            " writes, so nothing the target took since is on it. Nothing"
            " was done")
    if not (_reverse(hop) or settings(hop)):
        say("  the old source's counters are left as they are: the hop has"
            " neither a stream back (`reverse: at_cutover`) nor `cutover:`,"
            " so migkit carried nothing the target took after the flip to"
            " the source - rows written on the target since are there"
            " only, and migkit writes nothing to the source")
        return None
    back = hop.reversed()
    rev = get_engine(back)
    tdb = hop.target_db(db)
    plan = rev.repair_plan(tdb, "sequences")
    for a in plan:
        say(f"  old source: {a.note}")
    for x in [x for a in plan for x in a.statements][:10]:
        say(f"    {x}")
    return {"rev": rev, "tdb": tdb, "plan": plan}


def back_apply(hop, eng, db, back, apply, say):
    """`back_plan`'s counters put on the old source (only raised), once the
    stream back has carried everything the target has - the target's own
    counters put back just before are among it, and on MySQL they are a
    statement the stream carries, which applied after the raise lowered
    the old source's counter again (measured on 8.4: the next key there
    was 256 where the new primary's counter stood at 5,000). Then the
    source's freeze, which the cutover left on it, given back, and the
    statements the cutover left for a writable source run."""
    from . import audit, freeze
    rev, tdb, plan = back["rev"], back["tdb"], back["plan"]
    stmts = [x for a in plan for x in a.statements]
    frozen = freeze.source_frozen(hop, hop.databases or [db])
    if not apply:
        if frozen:
            say("  the old source is still frozen by the cutover; --apply"
                " gives it back")
        return stmts
    pos = rev.src_lsn(tdb) if hasattr(rev, "src_lsn") else None
    got = rev.fence_wait(tdb, pos, timeout=float(
        opt(hop, "fence_timeout"))) \
        if pos is not None and hasattr(rev, "fence_wait") else None
    say("  the stream back " + (
        f"has carried everything the target has ({pos})" if got else
        "is not running - the counters are raised from what the target"
        " held" if got is None else
        f"has NOT reached where the target stands ({pos}): stop the"
        " application on the target first, or what it writes now misses"
        " the source"))
    # a MySQL server kept read-only refuses even migkit's statement:
    # given back first there, raised first everywhere else
    first = (frozen and freeze.side_engine(hop, eng, "src")[0] == "mysql"
             and freeze.server_state(hop))
    if first:
        freeze.thaw_source(hop, eng, hop.databases or [db],
                           lambda m: say(f"  {m}"))
    for a in plan:
        if a.statements:
            rev.apply(tdb, a)
    audit.append(hop.report_dir() / "changelog.jsonl",
                 {"op": "rollback-counters", "db": db, "side": "source",
                  "n": len(stmts), "undo": [u for a in plan for u in a.undo],
                  "stream_back": got})
    if stmts:
        say(f"  old source: {len(stmts)} counters raised past the new"
            " primary's")
    if frozen and not first:
        freeze.thaw_source(hop, eng, hop.databases or [db],
                           lambda m: say(f"  {m}"))
    for d, left in _left_on_source(hop).items():
        for x in left:
            try:
                eng.apply_replication_stmt("src", d, x)
            except Exception as e:  # noqa: BLE001 - said, not fatal
                say(f"  [yellow]{d}: {x} - {e}[/yellow]")
    flipped = past_flip(hop)
    if flipped:
        from . import drainmark
        for d in flipped.get("mark") or {}:
            try:
                drainmark.drop(hop, eng, d)
            except Exception as e:  # noqa: BLE001 - said, not fatal
                say(f"  [yellow]{d}: the mark's table - {e}[/yellow]")
        # the flip ended by going back: a later run of the cutover
        # command is a cutover again, not this one's point of no return
        _ended(hop, "rolled back", flipped)
    return stmts


def _left_on_source(hop):
    """What the last flipped cutover could not run on the frozen source."""
    run = last_run(hop)
    if not run or run.get("state") != "flipped":
        return {}
    return run.get("left_on_source") or {}


def summary(hop, eng, dbs):
    """(level, what) for `assess` and `doctor`: what the cutover will do
    on each side, or that the operator does it by hand."""
    from . import freeze
    conf = settings(hop)
    back = _reverse(hop)
    if not conf:
        return ("warn", "no `cutover:` in the hop's options (no goal turns"
                        " it on): migkit writes nothing to the source at"
                        " cutover - the operator"
                        " stops its writes and its jobs, and raises the"
                        " counters, by hand, and migkit proves around it;"
                        f" `migkit move {hop.name} --mode cdc --drop` lists"
                        " the steps")
    how = conf["freeze"]
    if how != "operator":
        got = freeze.choose(hop, eng, dbs, how)
        if got.rung is None:
            kind = freeze.side_engine(hop, eng, "src")[0]
            return ("fail", f"cutover: the {hop.engine} source is not frozen"
                            " by migkit here - "
                            + (got.reason if kind in freeze.FREEZES
                               else freeze.not_held(kind))
                            + "; `freeze: operator`")
        how = f"{got.rung.said} ({got.reason})"
    return ("pass", f"cutover within {conf['budget']:g}s: the source frozen"
                    f" {how if how != 'operator' else 'by the operator, proved by migkit'};"
                    + (" a stream back to the old source" if back else
                       " no stream back - going back after the flip loses"
                       " what the target took")
                    + "; the flip "
                    + (f"runs `{conf['flip']}`" if conf.get("flip")
                       else "is the operator's")
                    + ("; the point of no return at the flip, approved on"
                       " its own" if conf["no_return"] == "at_flip" else
                       "; the point of no return a run of its own, approved"
                       " on its own"))


def said_by_doctor(hop):
    """Lines for `doctor`: a cutover stopped half way, a source the
    cutover left frozen."""
    from . import freeze
    out = []
    rec = record(hop)
    now = running(hop) if rec and rec.get("state") in (
        "running", "undoing", "no-return") else None
    if now:
        out.append(f"a cutover is running ({now.get('host')}, process"
                   f" {now.get('pid')}), at {_where(rec)}")
        rec = None
    if rec and rec.get("state") in ("running", "undoing"):
        out.append(f"a cutover stopped at {_where(rec)}"
                   + (f" and could not be undone ({rec.get('undo_error')})"
                      if rec.get("undo_error") else "")
                   + f": `migkit move {hop.name} --mode cdc --drop --go`"
                     " undoes it")
    if rec and rec.get("state") == "no-return":
        out.append(f"the point of no return stopped at {_where(rec)}"
                   f": `migkit move {hop.name} --mode cdc --drop --go` goes"
                   " on from there")
    if freeze.source_frozen(hop, hop.databases or []):
        out.append("the source is kept frozen by the cutover (the old"
                   f" primary): `migkit rollback {hop.name} --db DB"
                   " --apply` gives it back, and so does the point of no"
                   f" return (`migkit move {hop.name} --mode cdc --drop"
                   " --go` again)")
    return out
