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

A holder measures its own heartbeat (`Jitter`): how much later than its
third of the term each renewal came, and how long the write took. What
the worst of the recent ones shows rides in its record (`jitter`), and a
taker waits that long past the term (`lapse`) before it calls the lease
lapsed - a holder whose renewals run late is not taken over while it is
only late, and one whose heartbeat is steady is taken over at its term.
The allowance is bounded by the holder's own term, so a record that
claims more can not keep the hop from being taken.
"""
import collections
import fcntl
import json
import os
import socket
import threading
import time


def term():
    from . import envkeys
    return float(envkeys.number("MIGKIT_LEASE_SECONDS"))


def lapse(have):
    """The moment (epoch seconds) from which the lease `have` may be taken
    over: its expiry and the jitter its holder measured of its own
    renewals, no more than one term of it."""
    expires = float(have.get("expires") or 0)
    if not expires:
        return 0.0
    try:
        late = float(have.get("jitter") or 0)
    except (TypeError, ValueError):
        late = 0.0
    term = max(expires - float(have.get("renewed") or expires), 0.0)
    return expires + (min(late, term) if late > 0 else 0.0)


class Jitter:
    """How late a holder's renewals ran: for each one, the seconds past
    the interval it was due and the seconds its write took, the recent ones
    kept. `allowance` is the worst of them - what a taker should add to
    the term before it takes the lease from a holder that is only late."""

    #: the renewals whose timing is kept
    KEPT = 32

    def __init__(self):
        self._late = collections.deque(maxlen=self.KEPT)
        self._lock = threading.Lock()

    def beat(self, gap, due, wrote):
        """One renewal that came `gap` seconds after the one before, due
        every `due` seconds, its write taking `wrote`."""
        with self._lock:
            self._late.append(max(float(gap) - float(due), 0.0)
                              + max(float(wrote), 0.0))

    def allowance(self):
        with self._lock:
            return round(max(self._late, default=0.0), 3)


class Held(SystemExit):
    pass


class Lost(SystemExit):
    """This holder's lease was taken by another, or could not be renewed
    within its term: it holds nothing, and writes nothing more."""


class _FileRecord:
    """The lease as a file, changed under a lock on the file beside it -
    written whole and on the disk (`durable`): a holder a power loss
    rolled back to "nobody" would be two writers once it came back."""

    def __init__(self, path, legacy=None):
        self.path = path
        self.guard = path.with_name(path.name + ".guard")
        #: text -> a record, for a file an earlier version wrote in a
        #: shape of its own (`tailctl`'s "host pid")
        self.legacy = legacy

    def read(self):
        try:
            text = self.path.read_text()
        except OSError:
            return None
        try:
            return json.loads(text)
        except ValueError:
            # a torn lease names nobody: taken, and the holder that wrote
            # it finds it is not its own at its next renewal
            return self.legacy(self.path, text) if self.legacy else None

    def change(self, decide):
        """`decide(record)` -> the record to write, None to remove it, or
        KEEP to leave it; returns what it decided."""
        from . import durable
        if not self.path.parent.exists():
            durable.private_dir(self.path.parent)
        fd = os.open(self.guard, os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            new = decide(self.read())
            if new is None:
                self.path.unlink(missing_ok=True)
                durable.sync_dir(self.path.parent)
            elif new is not KEEP:
                durable.write_json(self.path, new)
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

#: every lease this process holds now, from `acquire` to `release`: what
#: `check_held` asks
_HOLDING = set()
_HOLDING_LOCK = threading.Lock()


def check_held():
    """Nothing, where this process still holds every lease it took; `Lost`
    where one was taken over or could not be renewed within its term.
    Asked by the write loops between ranges and batches (audit ST11): the
    heartbeat that renews a lease cannot stop the work, and a run that
    went on writing under a lease another took was two writers."""
    with _HOLDING_LOCK:
        held = list(_HOLDING)
    for lease in held:
        lease.check()


def taken_from():
    """The movers (`fleet.mover_of`) whose leases the ones this process
    holds now were taken over from: whose work this process goes on with,
    from a record that can trail what they wrote."""
    from .fleet import mover_of
    with _HOLDING_LOCK:
        held = list(_HOLDING)
    return {m for m in (mover_of(lease.took_over) for lease in held) if m}

def _pid_alive(pid):
    """Whether a process of this machine's is there: True, False, or None
    where there is no process number to ask. One that is there under
    another user answers "not permitted" - there, then."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return None
    if pid <= 0:
        # 0 and below name process groups, not a process
        return None
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OverflowError:
        return None


def liveness(have, now=None):
    """Whether whoever a lease record names may still be running - the one
    answer migkit gives to "is it still running?" (audit 2026-10-01,
    ST10), for a hop's lease, a tail's (`tailctl.alive`) and the loads
    that set something aside (`setaside`).

    A record that names nobody is not. On this machine its process is
    asked, and that answer stands whatever the term says: a process that
    is there may still write, and one that is gone never will. On another
    machine the term decides - renewed by the holder's heartbeat, so a
    holder that stopped, or a pod that was replaced, lapses once its
    term runs out, instead of counting as running for good."""
    if not isinstance(have, dict) or not have.get("holder"):
        return False
    if have.get("host") == socket.gethostname():
        got = _pid_alive(have.get("pid"))
        if got is not None:
            return got
    return lapse(have) > (time.time() if now is None else now)


def holder(path, remote=None, legacy=None):
    """The record of whoever holds the lease at `path` now - renewed
    within its term and, on this machine, its process still there - or
    None. Reads only: nothing is taken or renewed."""
    if remote is not None:
        have, _ = remote.read_record(path)
        if have == _BucketRecord.RELEASED:
            have = None
    else:
        have = _FileRecord(path, legacy).read()
    if not liveness(have) or lapse(have) <= time.time():
        return None
    return have


class Lease:
    def __init__(self, path, what="a write operation", remote=None,
                 fenced=False, term=None, legacy=None):
        self.path = path
        self.record = (_BucketRecord(remote, path) if remote
                       else _FileRecord(path, legacy))
        self.what = what
        self.me = f"{socket.gethostname()}:{os.getpid()}:{id(self)}"
        self.took_over = None
        #: where it was taken over: the seconds from the last renewal the
        #: one before wrote to this holder's taking it - how long the hop
        #: went with no holder heard from, measured, not the term assumed
        self.unheld = None
        #: the number this holding carries, where the lease is `fenced`
        self.fenced, self.fence = fenced, None
        self._term = term
        #: when it was last renewed, on this machine's steady clock
        self.renewed = None
        #: what the record held when this holder found it was not its any
        #: more
        self.lost = None
        #: how late this holder's renewals run (`Jitter`), written to its
        #: record so a taker waits for it
        self.jitter = Jitter()
        self._stop = threading.Event()
        self._beat = None
        self._renewing = threading.Lock()

    def term(self):
        return float(self._term) if self._term else term()

    def acquire(self):
        def decide(have):
            now = time.time()
            self.took_over = self.unheld = None
            if have and have.get("holder") and have.get("holder") != self.me:
                if lapse(have) > now and not self._dead(have):
                    raise Held(
                        f"another migkit {have.get('what', 'operation')} is"
                        f" running on this hop ({have.get('host')}, process"
                        f" {have.get('pid')}, since"
                        f" {time.strftime('%H:%M:%S', time.localtime(have.get('since', now)))});"
                        " its lease is renewed while it runs, so wait for it"
                        f" - or, if it is gone, for the lease to lapse"
                        f" ({int(lapse(have) - now)}s)")
                self.took_over = have
                heard = have.get("renewed") or have.get("since")
                self.unheld = max(0.0, now - float(heard)) if heard else None
            got = {"holder": self.me, "host": socket.gethostname(),
                   "pid": os.getpid(), "what": self.what,
                   "since": now, "renewed": now, "expires": now + self.term()}
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
        with _HOLDING_LOCK:
            _HOLDING.add(self)
        return self

    @staticmethod
    def _dead(have):
        """A holder on this machine whose process is gone does not need
        its term waited out."""
        return have.get("host") == socket.gethostname() and \
            _pid_alive(have.get("pid")) is False

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
            now = time.time()
            return {**have, "renewed": now, "expires": now + self.term(),
                    "jitter": self.jitter.allowance()}
        with self._renewing:
            if self.lost is not None:
                return False
            began = time.monotonic()
            self.record.change(decide)
            if lost:
                self.lost = lost[0] or {}
                return False
            if self.renewed is not None:
                self.jitter.beat(began - self.renewed, self.term() / 3,
                                 time.monotonic() - began)
            self.renewed = began
            return True

    def _renew(self):
        while not self._stop.wait(self.term() / 3):
            try:
                if not self._renew_once():
                    return
            except Exception:  # noqa: BLE001 - the next beat tries again
                # not renewed: `fresh` goes false within the term, and the
                # write loops' `check` then raises `Lost` between batches
                # (ST11) - the operation stops writing rather than going
                # on under a lease another may have taken
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
        with _HOLDING_LOCK:
            _HOLDING.discard(self)
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
