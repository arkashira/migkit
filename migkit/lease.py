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
only over the version that was read, which does the same across machines.

`MIGKIT_LEASE_SECONDS` sets the term (60 by default); the heartbeat
renews it every third of that.
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
    """The lease in the run state's bucket, written only over the version
    that was read. A write that loses reads again and decides again."""

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


class Lease:
    def __init__(self, path, what="a write operation", remote=None):
        self.path = path
        self.record = (_BucketRecord(remote, path) if remote
                       else _FileRecord(path))
        self.what = what
        self.me = f"{socket.gethostname()}:{os.getpid()}:{id(self)}"
        self.took_over = None
        self._stop = threading.Event()
        self._beat = None

    def acquire(self):
        def decide(have):
            now = time.time()
            self.took_over = None
            if have and have.get("holder") != self.me:
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
            return {"holder": self.me, "host": socket.gethostname(),
                    "pid": os.getpid(), "what": self.what,
                    "since": now, "expires": now + term()}
        self.record.change(decide)
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

    def _renew(self):
        lost = []

        def decide(have):
            if not have or have.get("holder") != self.me:
                # taken over while this one was not heard from: it no
                # longer holds anything, and must not write it back
                lost.append(have)
                return KEEP
            return {**have, "expires": time.time() + term()}
        while not self._stop.wait(term() / 3):
            try:
                self.record.change(decide)
            except Exception:  # noqa: BLE001 - the next beat tries again
                continue
            if lost:
                return

    def release(self):
        self._stop.set()
        if self._beat:
            self._beat.join(timeout=5)
        self.record.change(lambda have: None if have and have.get("holder")
                           == self.me else KEEP)

    # the callers held the old lock file and removed it when done
    unlink = release
