"""Failure on purpose, at the places a move keeps what it has done.

A move is safe to run again only if it is safe to stop anywhere. Every
place migkit writes down how far it got is named here, and an environment
variable stops the move at one of them - the Nth time it is reached - the
way a crash, a kill or a lost machine would:

    MIGKIT_FAILPOINT=range.committed:3:exit

`exit` ends the process at once, nothing flushed or cleaned up (as kill -9
does); `raise` raises; `hang` stops for good. Several are separated by
commas. Counted in each process: a range copied in a process of its own
counts its own. Only the tests set it.
"""
import os
import threading
import time

_counts = {}
_lock = threading.Lock()


def hit(name):
    """A place a move keeps what it has done has been reached."""
    spec = os.environ.get("MIGKIT_FAILPOINT")
    if not spec:
        return
    for one in spec.split(","):
        parts = one.strip().split(":")
        if parts[0] != name:
            continue
        at = int(parts[1]) if len(parts) > 1 and parts[1] else 1
        what = parts[2] if len(parts) > 2 else "exit"
        with _lock:
            n = _counts[name] = _counts.get(name, 0) + 1
        if n != at:
            continue
        if what == "raise":
            raise RuntimeError(f"failpoint {name} reached ({n})")
        if what == "hang":
            while True:
                time.sleep(3600)
        os._exit(137)
