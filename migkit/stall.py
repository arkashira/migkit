"""A copy whose link holds and passes nothing is given up on.

A link can stay open and carry nothing: a path whose MTU drops the large
packets, a tunnel that went quiet, a NAT that forgot the connection.
TCP's keepalives are small and pass, so nothing ever errors - measured
through a proxy that holds a connection and forwards nothing, the table
copier waited on it for good. So a copy that has moved nothing for
`MIGKIT_STALL_SECONDS` (300 unless set) is ended, and says so; the range
it was in is copied again by the next run, as after any other stop.
"""
import os
import threading
import time


def seconds():
    try:
        return max(1.0, float(os.environ.get("MIGKIT_STALL_SECONDS", "300")))
    except ValueError:
        return 300.0


class Guard:
    """Watches one copy: `moved()` each time data passes; `end` - the
    processes to kill, or a callable that breaks the connections - is used
    when nothing has passed for `seconds()`."""

    def __init__(self, what, end=()):
        self.what, self.end = what, end
        self.limit = seconds()
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
            raise RuntimeError(
                f"{self.what}: nothing moved for {self.limit:.0f}s - the"
                " link held and passed nothing (a path that drops large"
                " packets, a quiet tunnel, a forgotten NAT entry), so the"
                " copy was ended; the next run copies it again")
