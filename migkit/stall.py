"""A copy whose link holds and passes nothing is given up on.

A link can stay open and carry nothing: a path whose MTU drops the large
packets, a tunnel that went quiet, a NAT that forgot the connection.
TCP's keepalives are small and pass, so nothing ever errors - measured
through a proxy that holds a connection and forwards nothing, the table
copier waited on it for good. So a copy that has moved nothing for
`MIGKIT_STALL_SECONDS` (300 unless set) is ended, and says so; the range
it was in is copied again by the next run, as after any other stop.

The same limit holds every program migkit runs that says how far it has
got (`runner.run`'s `stall`): its progress reader or the status it serves
feeds `moved()`, and a program that has said nothing new for that long is
ended as a copy is.
"""
import threading
import time


def seconds():
    from . import envkeys
    return float(envkeys.number("MIGKIT_STALL_SECONDS"))


class Stalled(RuntimeError):
    """Nothing moved for the stall limit, and the copy was ended: a way
    that could not run, which the next run - or the next way - goes on
    from."""


class Guard:
    """Watches one copy: `moved()` each time data passes; `end` - the
    processes to kill, or a callable that breaks the connections - is used
    when nothing has passed for `limit` seconds (`seconds()` unless
    given)."""

    def __init__(self, what, end=(), limit=None):
        self.what, self.end = what, end
        self.limit = float(limit) if limit else seconds()
        self.at = time.monotonic()
        self.fired = False
        self._done = threading.Event()
        threading.Thread(target=self._watch, daemon=True).start()

    def moved(self):
        self.at = time.monotonic()

    def _watch(self):
        while not self._done.wait(min(5.0, self.limit / 4)):
            if time.monotonic() - self.at < self.limit:
                continue
            self.fired = True
            if callable(self.end):
                try:
                    self.end()
                except Exception:  # noqa: BLE001 - it is going anyway
                    pass
            else:
                for proc in self.end:
                    try:
                        proc.kill()
                    except Exception:  # noqa: BLE001
                        pass
            return

    def stop(self):
        self._done.set()
        if self.fired:
            raise Stalled(
                f"{self.what}: nothing moved for {self.limit:.0f}s - the"
                " link held and passed nothing (a path that drops large"
                " packets, a quiet tunnel, a forgotten NAT entry), so the"
                " copy was ended; the next run copies it again")


class Watch:
    """What a caller hands `runner.run` as its `stall` where progress is
    seen off the program's output too - a status port read on a thread of
    the caller's: `moved()` from anywhere, before or while the program
    runs. The runner arms it once the program has started."""

    def __init__(self, limit=None):
        self.limit = float(limit) if limit else seconds()
        self._guard = None

    def moved(self):
        guard = self._guard
        if guard is not None:
            guard.moved()

    def arm(self, what, end):
        self._guard = Guard(what, end, self.limit)
        return self._guard

    def disarm(self):
        """Raises `Stalled` where the limit was reached."""
        guard, self._guard = self._guard, None
        if guard is not None:
            guard.stop()
