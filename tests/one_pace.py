"""The pace as it was before each resource had a limit of its own (master
at 93aa147, `sizing.Pace`), kept verbatim so the one that replaced it is
held to never being slower on the same runs."""
import math
import threading
import time


class OnePace:
    """How many of a move's workers run at once, found while it runs.

    Each finished unit of work reports its weight (rows) and its time; a
    window of them gives a rate. The number is moved one way at a time and
    the rate after compared with the rate before:
    * a step up that paid (5% or more) is kept and followed by another -
      twice as far while it pays 25% or more, never as far as a number that
      has already not paid;
    * a step up that did not pay is taken back to where it was, and that
      number remembered as not worth it, so the next look above goes only
      half the way there;
    * now and then a step down: kept when the rate holds within 5% - as
      fast with fewer is better for both servers - and taken back if not;
    * a server under strain (the `stress` callable's answer) cuts the
      number by 30% at once and holds it two windows.
    Never below one, never past `most`. What it did is in `history`."""

    KEEP, DOUBLE, CUT = 0.05, 0.25, 0.7
    WINDOW, LEAST_DONE, HOLD, LOOK_EVERY, FORGET = 3.0, 4, 2, 8, 32

    def __init__(self, start, most, stress=None, clock=time.monotonic):
        self.most = max(1, int(most))
        self.limit = max(1, min(int(start), self.most))
        self.stress = stress
        self._clock = clock
        self._lock = threading.Lock()
        self._weight, self._n, self._times = 0.0, 0, []
        self._began = clock()
        # the rate at the current number, and where the last move started
        self._ref, self._back, self._move = None, None, None
        self._hold, self._windows, self._growing = 0, 0, True
        self._not_above = self.most + 1
        self.history = [(self.limit, None, "start")]

    def done(self, weight=1, seconds=None):
        with self._lock:
            self._weight += max(0.0, float(weight or 0))
            self._n += 1
            if seconds is not None:
                self._times.append(float(seconds))
                del self._times[:-8]
            now = self._clock()
            took = now - self._began
            typical = (sorted(self._times)[len(self._times) // 2]
                       if self._times else 0.0)
            # while it is still growing, shorter windows: a short copy has
            # few of them, and the first steps up are the ones that pay
            least, window = ((2, self.WINDOW / 3) if self._growing
                             else (self.LEAST_DONE, self.WINDOW))
            if self._n < least or took < max(window, 2 * typical):
                return
            rate = self._weight / took if took > 0 else 0.0
            self._weight, self._n, self._began = 0.0, 0, now
            self._decide(rate)

    def _go(self, n, why, rate, move):
        n = max(1, min(self.most, int(n)))
        if n == self.limit:
            self._move = None
            return
        self._back, self._move = (self.limit, self._ref), move
        self.limit = n
        self.history.append((n, round(rate, 1), why))

    def _decide(self, rate):
        self._windows += 1
        if self._windows % self.FORGET == 0:
            # what was not worth it a while ago may be now
            self._not_above = self.most + 1
        why = ""
        if self.stress is not None:
            try:
                why = self.stress() or ""
            except Exception:  # noqa: BLE001 - a probe never stops a move
                why = ""
        if why:
            self._go(math.floor(self.limit * self.CUT), why, rate, None)
            self._hold, self._ref, self._growing = self.HOLD, None, False
            return
        if self._hold:
            self._hold -= 1
            self._ref = rate if self._ref is None else (self._ref + rate) / 2
            return
        if self._move == "up":
            gain = rate / self._back[1] - 1 if self._back[1] else 1.0
            if gain >= self.KEEP:
                self._ref = rate
                far = (self.limit if self._growing and gain >= self.DOUBLE
                       else max(1, round(self.limit * 0.1)))
                self._go(min(self.limit + far, self._not_above - 1),
                         f"{gain:+.0%} faster, up again", rate, "up")
                return
            self._not_above = min(self._not_above, self.limit)
            self._growing = False
            back, ref = self._back
            self._go(back, f"{gain:+.0%} at {self.limit}, not worth it",
                     rate, None)
            self._ref, self._hold = ref, 1
            return
        if self._move == "down":
            loss = 1 - rate / self._back[1] if self._back[1] else 0.0
            if loss <= self.KEEP:
                self._ref, self._move = rate, None
                return
            back, ref = self._back
            self._go(back, f"{-loss:+.0%} at {self.limit}, back up", rate,
                     None)
            self._ref, self._hold = ref, 1
            return
        self._ref = rate if self._ref is None else (self._ref + rate) / 2
        if len(self.history) == 1 and self.limit < self.most:
            self._go(self.limit + 1, "a step up to measure", rate, "up")
            return
        if self._windows % self.LOOK_EVERY == 0 \
                and self.limit + 1 < self._not_above \
                and self.limit < self.most:
            # half the way to what did not pay, and never more than a
            # quarter again at once: a look is not a leap on a source that
            # serves an application
            room = min(self._not_above - 1, self.most) - self.limit
            self._go(self.limit + max(1, min(room // 2,
                                             round(self.limit * 0.25))),
                     "a look above", rate, "up")
        elif self._windows % self.LOOK_EVERY == self.LOOK_EVERY // 2 \
                and self.limit > 1:
            self._go(self.limit - 1, "a look below", rate, "down")

    def line(self):
        """One line for the log: where it went, if anywhere."""
        seen = [n for n, _, _ in self.history]
        if len(set(seen)) < 2:
            return ""
        return (f"at a time: started at {seen[0]}, ran between {min(seen)}"
                f" and {max(seen)}, ended at {self.limit}")
