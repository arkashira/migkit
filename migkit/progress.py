import sys
import time

from rich.console import Console

from .util import human_int, human_secs

console = Console(highlight=False)


class Reporter:
    def __init__(self, label, total_units=0):
        self.label = label
        self.total = total_units
        self.done = 0
        self.rows = 0
        self.t0 = time.monotonic()
        self.last = 0.0

    def step(self, units=1, rows=0, note=""):
        self.done += units
        self.rows += rows
        now = time.monotonic()
        if now - self.last < 2 and self.done < self.total:
            return
        self.last = now
        el = now - self.t0
        rate = self.rows / el if el > 0 else 0
        eta = ""
        if self.total and self.done:
            eta = f" eta {human_secs(el / self.done * (self.total - self.done))}"
        pct = f"{self.done * 100 // self.total}%" if self.total else str(self.done)
        msg = (f"  {self.label}: {pct} ({self.done}/{self.total or '?'})"
               f" {human_int(int(rate))} rows/s{eta}")
        if note:
            msg += f" {note}"
        console.print(msg)
        sys.stdout.flush()

    def finish(self, status, detail=""):
        el = time.monotonic() - self.t0
        color = {"ok": "green", "diff": "yellow", "error": "red"}.get(status, "white")
        console.print(f"[{color}]{self.label}: {status.upper()}[/{color}]"
                      f" {detail} ({human_secs(el)})")
