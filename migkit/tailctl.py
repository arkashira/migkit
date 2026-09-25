"""A running change tail as the rest of migkit sees it.

The tail runs in its own `migkit move --mode cdc` process, often for days,
and it is a second writer on the target. A repair that writes rows beside it
races it: whichever lands last survives, and nothing says which. So the tail
says it is running, and it can be asked to pause, which it acknowledges
only once it has applied what it was holding and saved its position.
After the pause it resumes from that position, and the changes it replays
land on top of the repair by key. That is the order that converges.

Files in the database's report directory, nothing else:
* `tail.pid`     this host and pid, while the tail runs
* `tail.pause`   written by whoever wants it paused, removed to resume
* `tail.paused`  written by the tail once it is holding still
* `tail.beat`    written by the tail every time round its loop
* `tail.stopped` what stopped it, when that was not a person
"""
import json
import os
import socket
import time

PID, PAUSE, PAUSED = "tail.pid", "tail.pause", "tail.paused"
BEAT, STOPPED = "tail.beat", "tail.stopped"


class Running:
    """Marks the tail as running for as long as the block lasts."""

    def __init__(self, where):
        self.where = where

    def __enter__(self):
        self.where.mkdir(parents=True, exist_ok=True)
        # what stopped the last one, for whoever was told it had stopped
        self.was_stopped = _read(self.where / STOPPED)
        (self.where / STOPPED).unlink(missing_ok=True)
        (self.where / PID).write_text(f"{socket.gethostname()} {os.getpid()}")
        return self

    def __exit__(self, kind=None, exc=None, tb=None):
        for name in (PID, PAUSED, BEAT):
            (self.where / name).unlink(missing_ok=True)
        if kind is not None and not issubclass(kind, KeyboardInterrupt):
            # ctrl-c and a service manager's stop are a person's; anything
            # else stopped a tail somebody expects to be running
            first = (str(exc).strip().splitlines() or [""])[0][:300]
            (self.where / STOPPED).write_text(json.dumps(
                {"at": time.time(), "kind": kind.__name__,
                 "why": first if kind is SystemExit and first
                 else f"{kind.__name__}: {first}" if first
                 else kind.__name__}))
        return False


def _read(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def beat(where, caught_up_at, changes, room=None):
    """Written every time round the tail's loop: when it last reached the
    end of the source's log, how many changes it has applied, and how much
    longer the source keeps what it has not read (`stream_room`). Written
    whole or not at all, so a reader never sees half of it."""
    tmp = where / (BEAT + ".tmp")
    tmp.write_text(json.dumps({"at": time.time(),
                               "caught_up_at": caught_up_at,
                               "changes": changes, "room": room}))
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
           "changes": None, "room": None}
    b = _read(where / BEAT) if marked else None
    if b:
        now = time.time()
        out["beat_age"] = max(now - b["at"], 0)
        out["behind"] = max(now - b["caught_up_at"], 0)
        out["changes"] = b.get("changes")
        out["room"] = b.get("room")
    return out


def alive(where):
    """True while a tail is marked running here and its process exists.

    A tail on another host cannot be asked about its process, so it counts
    as running: guessing that a writer has stopped is the mistake that
    costs rows, guessing that it runs costs a refusal.
    """
    try:
        host, pid = (where / PID).read_text().split()
    except (OSError, ValueError):
        return False
    return process_alive(host, pid)


def process_alive(host, pid):
    """True while that process may still be running: this host's is asked,
    another host's is assumed to be."""
    if host != socket.gethostname():
        return True
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except ValueError:
        return False


def hold_if_asked(where, log=None):
    """Called by the tail between batches: while a pause is asked for, say
    so and wait. Returns True if it paused."""
    if not (where / PAUSE).exists():
        return False
    (where / PAUSED).write_text(str(time.time()))
    if log:
        log("paused for a repair; holding the position already saved")
    while (where / PAUSE).exists():
        time.sleep(0.5)
    (where / PAUSED).unlink(missing_ok=True)
    if log:
        log("resumed")
    return True


def pause(where, timeout):
    """Ask the tail here to pause; True once it has, False if it did not
    within `timeout` seconds (and the request is withdrawn)."""
    (where / PAUSE).write_text(str(time.time()))
    end = time.time() + timeout
    while time.time() < end:
        if (where / PAUSED).exists():
            return True
        if not alive(where):
            # it stopped rather than paused: nothing is writing now
            return True
        time.sleep(0.5)
    resume(where)
    return False


def resume(where):
    (where / PAUSE).unlink(missing_ok=True)
