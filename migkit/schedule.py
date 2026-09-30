"""A hop that runs on a schedule of its own.

    options:
      schedule:
        run: check                   # the command it runs (check, move ...)
        args: ["--only", "data"]     # and what it is given
        cron: "0 */6 * * *"
        timezone: Asia/Bangkok       # UTC where not said
        catchup: 1800                # a slot missed by more than this is
                                     # skipped, not run late (seconds)
        max_duration: 14400          # the run, retries included, ends here
        retry_window: 7200           # a failed run tried again until then
        pause_after_failures: 3      # slots in a row that failed

Nothing needs a new command. Whatever already fires on a timer - cron, a
systemd timer, a Kubernetes CronJob - runs the hop's command every minute
with `MIGKIT_SCHEDULED=1`, and migkit decides whether a slot is due
(`gate`, asked once for every command before it runs - a command another
invokes is not asked again): not due, it says why and ends at once, 0.
`report --serve`, which runs anyway, fires them itself (`loop`), each hop
on its own. Retries are bounded by time, as DTS bounds them, not by a
count that means something else on a restart; a slot missed while nothing
ran is skipped past `catchup` rather than every missed one run at once
after an outage; a slot is not started while the last is still running;
and after `pause_after_failures` slots in a row fail the schedule stops
until someone looks (`MIGKIT_SCHEDULE_RESUME=1`).

A slot is held by a lease (`migkit.lease`), renewed while its run goes on:
it was a process number in a file, and in a container every run is
process 1 - after one was killed, every later pod read "still going" and
none ran again, and two pods firing in one minute both ran the slot. A
run counts as done where it did its work - a check that found a
difference did (`verdict.ran`); one that could not compare, or a guard
that stopped it, failed and is tried again.
"""
import datetime
import json
import os
import socket
import subprocess
import sys
import time

STATE = "schedule.json"
#: the lease a slot's run holds while it goes on
LEASE = "schedule-lease.json"
#: the environment a slot's run is started with: when its slot ends
#: (`max_duration` from its start), in seconds since the epoch
SLOT_ENDS = "MIGKIT_SLOT_ENDS"


class NotDue(SystemExit):
    """A scheduled command whose slot is not due: said, and ended 0."""

    def __init__(self, why):
        super().__init__(0)
        self.why = why


def flag(name, default=False, environ=None):
    """A yes/no environment variable: 1/true/yes/on, 0/false/no/off, or
    unset for `default`; anything else is a mistake said by its name -
    `MIGKIT_SCHEDULED=false` was read as on."""
    from .verdict import Usage
    e = os.environ if environ is None else environ
    got = str(e.get(name, "") or "").strip().lower()
    if not got:
        return default
    if got in ("1", "true", "yes", "on"):
        return True
    if got in ("0", "false", "no", "off"):
        return False
    raise Usage(f"{name}={e.get(name)}: one of 1, true, yes, on - or 0,"
                " false, no, off")


def slot_end():
    """When the slot this process runs for ends, or None where no schedule
    started it. A follow started by a slot ends once caught up, and at
    the latest here, its position saved (`tailctl.until`)."""
    got = os.environ.get(SLOT_ENDS, "")
    try:
        return float(got) if got else None
    except ValueError:
        return None


def spec_of(hop):
    got = (hop.options or {}).get("schedule") or {}
    return got if isinstance(got, dict) and got.get("cron") else None


def _slot(spec, now):
    """The latest slot at or before `now` (epoch seconds)."""
    from croniter import croniter
    zone = spec.get("timezone") or "UTC"
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(zone)
    except Exception:  # noqa: BLE001
        raise SystemExit(f"schedule.timezone: {zone} is not a time zone")
    at = datetime.datetime.fromtimestamp(now, tz)
    return int(croniter(spec["cron"], at).get_prev(datetime.datetime)
               .timestamp()) if not croniter.match(spec["cron"], at) \
        else int(at.replace(second=0, microsecond=0).timestamp())


def _remote(hop):
    try:
        from .state import run_state
        return run_state(hop)
    except Exception:  # noqa: BLE001 - kept on this machine then
        return None


def _state(path, remote=None):
    """The schedule's record: the run state's where the hop keeps one, so
    two machines firing the same hop read one record."""
    try:
        text = remote.get(path) if remote is not None else None
        return json.loads(text if text is not None else path.read_text())
    except (OSError, ValueError, TypeError):
        return {}


def _save(path, state, remote=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(state))
    os.replace(tmp, path)
    if remote is not None:
        remote.put(path, json.dumps(state))


def decide(spec, state, now):
    """(due slot or None, why not). Whether the last slot's run still
    goes on is the lease's to say (`take`), not a process number's."""
    slot = _slot(spec, now)
    if (state.get("failures", 0) >= int(spec.get("pause_after_failures", 3))
            and not flag("MIGKIT_SCHEDULE_RESUME")):
        return None, (f"paused after {state['failures']} slots in a row"
                      " failed - MIGKIT_SCHEDULE_RESUME=1 goes on")
    if state.get("last_slot", 0) >= slot:
        return None, "this slot has run"
    if now - slot > int(spec.get("catchup", 1800)):
        return None, "the slot was missed by more than its catch-up window"
    return slot, ""


class Slot:
    """A due slot, held until its run ends (`finished`)."""

    def __init__(self, hop, lease, slot):
        self.hop, self.lease, self.slot = hop, lease, slot

    def finished(self, ok):
        """The run's end, recorded - failures in a row counted - and the
        slot let go."""
        try:
            remote = _remote(self.hop)
            path = self.hop.report_dir() / STATE
            state = _state(path, remote)
            state.pop("running", None)
            state["failures"] = 0 if ok else state.get("failures", 0) + 1
            state["last_result"] = "ok" if ok else "failed"
            state["ended"] = time.time()
            _save(path, state, remote)
        finally:
            self.lease.release()


def take(hop, spec, log, now=None):
    """The due slot of `hop`, held (`Slot`), or None - said why - where
    none is due or the last one's run still holds it."""
    from .lease import Held, Lease
    remote = _remote(hop)
    lease = Lease(hop.report_dir() / LEASE, "scheduled run", remote)
    try:
        lease.acquire()
    except Held:
        log(f"{hop.name}: not due - the last slot's run is still going")
        return None
    try:
        path = hop.report_dir() / STATE
        state = _state(path, remote)
        now = time.time() if now is None else now
        slot, why = decide(spec, state, now)
        if slot is None:
            log(f"{hop.name}: not due - {why}")
            lease.release()
            return None
        state.update(last_slot=slot, started=now, host=socket.gethostname(),
                     pid=os.getpid())
        state.pop("running", None)
        _save(path, state, remote)
    except BaseException:
        lease.release()
        raise
    return Slot(hop, lease, slot)


def gate(hop, command, log):
    """For a command run with `MIGKIT_SCHEDULED=1`: its slot, held, where
    it should run now as its schedule's; `NotDue`, said why, where it
    should not. None for a command run by hand, which is never held
    back."""
    if not flag("MIGKIT_SCHEDULED"):
        return None
    spec = spec_of(hop)
    if spec is None:
        log(f"{hop.name}: not due - the hop has no schedule")
        raise NotDue("no schedule")
    if spec.get("run", "check") != command:
        log(f"{hop.name}: not due - its schedule runs"
            f" {spec.get('run', 'check')}, not {command}")
        raise NotDue("another command")
    got = take(hop, spec, log)
    if got is None:
        raise NotDue("not due")
    os.environ[SLOT_ENDS] = str(time.time()
                                + int(spec.get("max_duration", 14400)))
    return got


def run(hop, spec, argv=None, sleeper=time.sleep):
    """One slot's run, as the scheduler starts it: the command, tried again
    after a failure until `retry_window`, ended at `max_duration`. Returns
    whether it did its work (`verdict.ran`): a check that found a
    difference did, and is not run again for it."""
    from .verdict import ran
    began = time.time()
    deadline = began + int(spec.get("max_duration", 14400))
    retry_until = began + int(spec.get("retry_window", 0))
    cmd = argv or [sys.executable, "-c", "from migkit.cli import main;"
                   " main()", str(spec.get("run", "check")), hop.name,
                   *[str(a) for a in spec.get("args") or []]]
    wait = 30.0
    while True:
        left = deadline - time.time()
        if left <= 0:
            return False
        proc = subprocess.Popen(cmd, env={**os.environ,
                                          "MIGKIT_SCHEDULED": "0",
                                          SLOT_ENDS: str(deadline)})
        try:
            ok = ran(proc.wait(timeout=left))
        except subprocess.TimeoutExpired:
            # ended at a point it keeps: the copy and the check save what
            # they have done as they go
            proc.send_signal(2)
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
            return False
        if ok or time.time() + wait > min(retry_until, deadline):
            return ok
        sleeper(wait)
        wait = min(wait * 2, 900.0)


def _one(name, hop, spec, log, runner=None):
    """One hop's due slot, run to its end; what stops it is said and told
    to the hop's receivers, never the scheduler's end."""
    got = None
    try:
        got = take(hop, spec, lambda m: None)
        if got is None:
            return
        log(f"{name}: slot"
            f" {time.strftime('%F %T', time.localtime(got.slot))}"
            f" - running {spec.get('run', 'check')}")
        ok = (runner or run)(hop, spec)
        slot, got = got, None
        slot.finished(ok)
    except BaseException as e:  # noqa: BLE001 - one hop, not the loop
        why = (str(e).strip().splitlines() or [type(e).__name__])[0][:300]
        log(f"{name}: the scheduled run stopped - {why}")
        try:
            from . import notify
            notify.failed(hop, f"scheduled {spec.get('run', 'check')}",
                          why, log)
        except Exception:  # noqa: BLE001 - said above
            pass
        if got is not None:
            got.finished(False)


def loop(load_hops, stop, log=print, every=30.0, runner=None):
    """`report --serve`'s scheduler: each hop with a schedule run when a
    slot is due, until `stop` is set - every hop on its own thread, so one
    slot's hours do not make another hop miss its own, and one hop's
    mistake (a cron that does not parse, a secret not there, a
    configuration mid-edit) is said and told while the rest go on. It had
    one try around nothing: the first such mistake ended the scheduler
    while the dashboard went on serving as if it ran."""
    import threading
    running = {}
    while not stop.wait(every):
        try:
            hops = load_hops()
        except BaseException as e:  # noqa: BLE001 - the next turn reads again
            log("the scheduler could not read the hops - "
                + (str(e).strip().splitlines() or [type(e).__name__])[0])
            continue
        for name, hop in hops.items():
            if running.get(name) is not None and running[name].is_alive():
                continue
            try:
                spec = spec_of(hop)
                if spec is None:
                    continue
                _slot(spec, time.time())
            except BaseException as e:  # noqa: BLE001 - said, others go on
                why = (str(e).strip().splitlines()
                       or [type(e).__name__])[0][:300]
                log(f"{name}: its schedule cannot be read - {why}")
                continue
            t = threading.Thread(target=_one, args=(name, hop, spec, log,
                                                    runner),
                                 daemon=True, name=f"migkit-schedule-{name}")
            running[name] = t
            t.start()
