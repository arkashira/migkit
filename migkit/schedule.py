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
(`gate`): not due, it says so and ends at once. `report --serve`, which
runs anyway, fires them itself (`loop`). Retries are bounded by time, as
DTS bounds them, not by a count that means something else on a restart; a
slot missed while nothing ran is skipped past `catchup` rather than every
missed one run at once after an outage; a slot is not started while the
last is still running; and after `pause_after_failures` slots in a row
fail the schedule stops until someone looks (`MIGKIT_SCHEDULE_RESUME=1`).
"""
import datetime
import json
import os
import subprocess
import sys
import time

STATE = "schedule.json"


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


def _state(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError, TypeError):
        return False


def decide(spec, state, now):
    """(due slot or None, why not)."""
    slot = _slot(spec, now)
    if state.get("running") and _alive(state["running"]):
        return None, "the last slot's run is still going"
    if (state.get("failures", 0) >= int(spec.get("pause_after_failures", 3))
            and not os.environ.get("MIGKIT_SCHEDULE_RESUME")):
        return None, (f"paused after {state['failures']} slots in a row"
                      " failed - MIGKIT_SCHEDULE_RESUME=1 goes on")
    if state.get("last_slot", 0) >= slot:
        return None, "this slot has run"
    if now - slot > int(spec.get("catchup", 1800)):
        return None, "the slot was missed by more than its catch-up window"
    return slot, ""


def gate(hop, command, log):
    """For a command run with `MIGKIT_SCHEDULED=1`: True where the command
    should run now, as its schedule's; raises SystemExit(0), saying why,
    where it should not. False for a command run by hand, which is never
    held back."""
    if os.environ.get("MIGKIT_SCHEDULED", "") in ("", "0"):
        return False
    spec = spec_of(hop)
    if spec is None or spec.get("run", "check") != command:
        raise SystemExit(0)
    path = hop.report_dir() / STATE
    state = _state(path)
    slot, why = decide(spec, state, time.time())
    if slot is None:
        log(f"{hop.name}: not due - {why}")
        raise SystemExit(0)
    state.update(last_slot=slot, running=os.getpid(), started=time.time())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state))
    return True


def finished(hop, ok):
    """The scheduled run's end, recorded: failures in a row counted."""
    path = hop.report_dir() / STATE
    state = _state(path)
    state.pop("running", None)
    state["failures"] = 0 if ok else state.get("failures", 0) + 1
    state["last_result"] = "ok" if ok else "failed"
    path.write_text(json.dumps(state))


def run(hop, spec, argv=None, sleeper=time.sleep):
    """One slot's run, as the scheduler starts it: the command, tried again
    after a failure until `retry_window`, ended at `max_duration`. Returns
    whether it succeeded."""
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
                                          "MIGKIT_SCHEDULED": "0"})
        try:
            ok = proc.wait(timeout=left) == 0
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


def loop(load_hops, stop, log=print, every=30.0):
    """`report --serve`'s scheduler: each hop with a schedule run when a
    slot is due, one hop at a time, until `stop` is set."""
    while not stop.wait(every):
        for name, hop in load_hops().items():
            spec = spec_of(hop)
            if spec is None:
                continue
            path = hop.report_dir() / STATE
            state = _state(path)
            slot, _ = decide(spec, state, time.time())
            if slot is None:
                continue
            state.update(last_slot=slot, running=os.getpid(),
                         started=time.time())
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(state))
            log(f"{name}: slot {time.strftime('%F %T', time.localtime(slot))}"
                f" - running {spec.get('run', 'check')}")
            finished(hop, run(hop, spec))
