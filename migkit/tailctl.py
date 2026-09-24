"""A running change tail as the rest of migkit sees it.

The tail runs in its own `migkit move --mode cdc` process, often for days,
and it is a second writer on the target. A repair that writes rows beside it
races it: whichever lands last survives, and nothing says which. So the tail
says it is running, and it can be asked to pause, which it acknowledges
only once it has applied what it was holding and saved its position.
After the pause it resumes from that position, and the changes it replays
land on top of the repair by key. That is the order that converges.

Files in the database's report directory, nothing else:
* `tail.pid`    this host and pid, while the tail runs
* `tail.pause`  written by whoever wants it paused, removed to resume
* `tail.paused` written by the tail once it is holding still
"""
import os
import socket
import time

PID, PAUSE, PAUSED = "tail.pid", "tail.pause", "tail.paused"


class Running:
    """Marks the tail as running for as long as the block lasts."""

    def __init__(self, where):
        self.where = where

    def __enter__(self):
        self.where.mkdir(parents=True, exist_ok=True)
        (self.where / PID).write_text(f"{socket.gethostname()} {os.getpid()}")
        return self

    def __exit__(self, *exc):
        for name in (PID, PAUSED):
            (self.where / name).unlink(missing_ok=True)
        return False


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
