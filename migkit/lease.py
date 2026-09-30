"""A write operation's hold on a hop, as a lease with a heartbeat
(backlog 30).

It was a file holding a process number. Another process on the same
machine could ask whether that process was alive; a process on another
machine could not, and a holder that died left the file saying it ran.
Now the holder renews the lease while it runs. A lease that has not been
renewed for its whole term is taken over, with a line saying from whom:
the holder is gone, wherever it ran.

Every read and write of the lease happens under an exclusive lock on a
file beside it, so two processes deciding at once cannot both win. With
the s3 state backend the lease is kept in the bucket instead, written
only over the version that was read, which does the same across machines
- and so is any store machines share (`shared`), through the same two
calls.

`MIGKIT_LEASE_SECONDS` sets the term (60 by default); the heartbeat
renews it every third of that.

A lease the machines of one hop take over from each other (`fenced`,
backlog W1) carries a number, one higher each time it is taken and kept
when it is let go, so every holder there ever was has a number above the
last one's. The number is what the target holds a writer to
(`fencing`): the lease says who may write, and the number lets the
target refuse one that no longer may. Such a holder also knows how long
ago it last renewed (`fresh`): past two thirds of the term it cannot be
sure it still holds, and writes nothing until a renewal says it does.
"""
import fcntl
import json
import os
import socket
import threading
import time


def term():
    try:
        return max(float(os.environ.get("MIGKIT_LEASE_SECONDS", "60")), 1.0)
    except ValueError:
        raise SystemExit("MIGKIT_LEASE_SECONDS is not a number of seconds")


class Held(SystemExit):
    pass


class Lost(SystemExit):
    """This holder's lease was taken by another, or could not be renewed
    within its term: it holds nothing, and writes nothing more."""


class _FileRecord:
    """The lease as a file, changed under a lock on the file beside it."""

    def __init__(self, path):
        self.path = path
        self.guard = path.with_name(path.name + ".guard")

    def change(self, decide):
        """`decide(record)` -> the record to write, None to remove it, or
        KEEP to leave it; returns what it decided."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.guard, os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            try:
                have = json.loads(self.path.read_text())
            except (OSError, ValueError):
                have = None
            new = decide(have)
            if new is None:
                self.path.unlink(missing_ok=True)
            elif new is not KEEP:
                tmp = self.path.with_name(self.path.name + ".tmp")
                tmp.write_text(json.dumps(new))
                os.replace(tmp, self.path)
            return new
        finally:
            os.close(fd)


class _BucketRecord:
    """The lease in the run state's bucket, or any store machines share,
    written only over the version that was read. A write that loses reads
    again and decides again."""

    RELEASED = {"holder": None, "expires": 0}

    def __init__(self, remote, path):
        self.remote, self.path = remote, path

    def change(self, decide):
        from .state import Taken
        for _ in range(50):
            have, version = self.remote.read_record(self.path)
            if have == self.RELEASED:
                have = None
            new = decide(have)
            if new is KEEP:
                return new
            try:
                self.remote.write_record(self.path, new or self.RELEASED,
                                         version)
                return new
            except Taken:
                time.sleep(0.05)
        raise SystemExit("the hop's lease kept changing under migkit while it"
                         " tried to write it; another machine is taking it"
                         " over and back - stop the other runs")


#: `decide` leaves the record as it is
KEEP = object()


def holder(path, remote=None):
    """The record of whoever holds the lease at `path` now - renewed
    within its term and, on this machine, its process still there - or
    None. Reads only: nothing is taken or renewed."""
    if remote is not None:
        have, _ = remote.read_record(path)
        if have == _BucketRecord.RELEASED:
            have = None
    else:
        try:
            have = json.loads(path.read_text())
        except (OSError, ValueError):
            have = None
    if not isinstance(have, dict) or not have.get("holder") \
            or have.get("expires", 0) <= time.time() or Lease._dead(have):
        return None
    return have


class Lease:
    def __init__(self, path, what="a write operation", remote=None,
                 fenced=False, term=None):
        self.path = path
        self.record = (_BucketRecord(remote, path) if remote
                       else _FileRecord(path))
        self.what = what
        self.me = f"{socket.gethostname()}:{os.getpid()}:{id(self)}"
        self.took_over = None
        #: the number this holding carries, where the lease is `fenced`
        self.fenced, self.fence = fenced, None
        self._term = term
        #: when it was last renewed, on this machine's steady clock
        self.renewed = None
        #: what the record held when this holder found it was not its any
        #: more
        self.lost = None
        self._stop = threading.Event()
        self._beat = None
        self._renewing = threading.Lock()

    def term(self):
        return float(self._term) if self._term else term()

    def acquire(self):
        def decide(have):
            now = time.time()
            self.took_over = None
            if have and have.get("holder") and have.get("holder") != self.me:
                if have.get("expires", 0) > now and not self._dead(have):
                    raise Held(
                        f"another migkit {have.get('what', 'operation')} is"
                        f" running on this hop ({have.get('host')}, process"
                        f" {have.get('pid')}, since"
                        f" {time.strftime('%H:%M:%S', time.localtime(have.get('since', now)))});"
                        " its lease is renewed while it runs, so wait for it"
                        f" - or, if it is gone, for the lease to lapse"
                        f" ({int(have['expires'] - now)}s)")
                self.took_over = have
            got = {"holder": self.me, "host": socket.gethostname(),
                   "pid": os.getpid(), "what": self.what,
                   "since": now, "expires": now + self.term()}
            if self.fenced:
                # one above every number this lease was ever taken with,
                # kept when it was let go
                self.fence = int((have or {}).get("fence") or 0) + 1
                got["fence"] = self.fence
            return got
        began = time.monotonic()
        self.record.change(decide)
        self.renewed = began
        self._beat = threading.Thread(target=self._renew, daemon=True)
        self._beat.start()
        return self

    @staticmethod
    def _dead(have):
        """A holder on this machine whose process is gone does not need
        its term waited out."""
        if have.get("host") != socket.gethostname():
            return False
        try:
            os.kill(int(have.get("pid")), 0)
            return False
        except ProcessLookupError:
            return True
        except (PermissionError, TypeError, ValueError):
            return False

    def _renew_once(self):
        """One renewal: True where this holder still holds and its term is
        moved on, False where another took it (`lost` says who). The
        clock is read before the write, so `renewed` is never later than
        the term the store now holds began."""
        lost = []

        def decide(have):
            if not have or have.get("holder") != self.me:
                # taken over while this one was not heard from: it no
                # longer holds anything, and must not write it back
                lost.append(have)
                return KEEP
            return {**have, "expires": time.time() + self.term()}
        with self._renewing:
            if self.lost is not None:
                return False
            began = time.monotonic()
            self.record.change(decide)
            if lost:
                self.lost = lost[0] or {}
                return False
            self.renewed = began
            return True

    def _renew(self):
        while not self._stop.wait(self.term() / 3):
            try:
                if not self._renew_once():
                    return
            except Exception:  # noqa: BLE001 - the next beat tries again
                continue

    def fresh(self):
        """Whether this holder may write now: not taken over, and renewed
        within two thirds of its term - the other third is what a writer
        that checked just before it paused has, before the term it
        renewed ends and another may take it."""
        return (self.lost is None and self.renewed is not None
                and time.monotonic() - self.renewed < self.term() * 2 / 3)

    def check(self):
        """Nothing, where this holder may write; a renewal first where it
        has not been renewed lately; `Lost` where it does not hold, or
        cannot say that it does."""
        if self.fresh():
            return
        try:
            held = self._renew_once()
        except Exception as e:  # noqa: BLE001 - said as the reason
            raise Lost(f"the {self.what} could not renew its lease within"
                       f" its term ({type(e).__name__}: {e}); it writes"
                       " nothing more until it holds it again") from None
        if not held:
            by = self.lost or {}
            raise Lost(f"the {self.what} was taken over by"
                       f" {by.get('host') or 'another machine'}"
                       + (f" process {by.get('pid')}" if by.get("pid")
                          else "")
                       + " while this machine was not heard from; it writes"
                         " nothing more")

    def bump(self, above):
        """The number this holding carries, raised past `above` - the
        target holds a higher one than the lease remembered (the lease's
        record was lost, or kept elsewhere once): only while this holder
        holds it."""
        if not self.fenced:
            return self.fence

        def decide(have):
            if not have or have.get("holder") != self.me:
                return KEEP
            return {**have, "fence": int(above) + 1}
        got = self.record.change(decide)
        if got is KEEP:
            self.lost = self.lost or {}
            raise Lost(f"the {self.what} was taken over while its number"
                       " was being raised; it writes nothing more")
        self.fence = int(above) + 1
        return self.fence

    def release(self):
        self._stop.set()
        if self._beat:
            self._beat.join(timeout=5)

        def decide(have):
            if not have or have.get("holder") != self.me:
                return KEEP
            if self.fenced:
                # let go, its number kept for the next holder to go above
                return {"holder": None, "expires": 0,
                        "fence": have.get("fence") or self.fence}
            return None
        self.record.change(decide)

    # the callers held the old lock file and removed it when done
    unlink = release
