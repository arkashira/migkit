"""Each attempt at a unit of work, the server session that is exactly its
own, and a wait the server stopped answering told apart from a slow answer
(backlog P0-CONNECTION-PROGRESS).

Measured live: a verifier read waited in its TLS poll for about 48 minutes
while its server listed the session asleep. Closing that one session made
the reader's connection fail, and its retry read the same unit again and
went on. Keepalives passed all along; only the read deadline, an hour,
would have ended it. So:

* every attempt is written down (`Ledger`): when it began, its server
  session, the operation, the unit's position, when its query was sent and
  its answer read, its last useful progress and why it was tried again -
  never a credential or a statement's text;
* while an attempt waits past what its operation measured here
  (`decide.session_patience`), the server's own view of the session is
  read (`Engine.session_view`) and the wait is named (`classify`);
* what is done about it is decided (`decide.session_action`): a session
  idle while migkit waits, seen so twice and owned by this attempt alone,
  is closed - that session only (`Engine.close_session`) - and the
  worker's retry takes the unit from its position; a unit closed once that
  made no progress after it is not closed again, but said;
* the keepalives and read deadline are read back off the socket
  (`transport`), not assumed from what was asked.
"""
import collections
import sys
import threading
import time
from dataclasses import asdict, dataclass, field

COMPUTING = "computing"
TRANSFER = "transfer"
CLIENT_STALL = "client stall"
BETWEEN = "between reads"
IDLE = "idle"
LOCKED = "locked"
GONE = "gone"
UNAVAILABLE = "unavailable"

SAID = {
    COMPUTING: "the server is still working on it",
    TRANSFER: "the server is sending the answer",
    CLIENT_STALL: "the server is sending and migkit is not reading",
    BETWEEN: "migkit is working through what it read",
    IDLE: "the server has finished and sits idle while migkit waits",
    LOCKED: "the server waits on a lock",
    GONE: "the server no longer has the session",
    UNAVAILABLE: "the server's view of its sessions cannot be read",
}

#: the waits kept per operation: what its patience is measured from
DURATIONS_KEPT = 256


@dataclass(frozen=True)
class View:
    """One session as its server lists it, in words every engine maps its
    own to: `state` idle, running, sending or locked; `seconds` in it as
    the server counts; `error` why it could not be read."""
    session: object
    owner: str = ""
    state: str = ""
    seconds: float = 0.0
    present: bool = True
    error: str = ""


@dataclass
class Attempt:
    side: str
    operation: str
    unit: str = ""
    position: str = ""
    session: object = None
    reads: bool = True
    started: float = 0.0
    sent: float = 0.0
    answered: float = 0.0
    progress: float = 0.0
    stage: str = "waiting"          #: waiting, reading, done
    retries: int = 0
    reason: str = ""
    kind: str = ""
    idle_reads: int = 0
    idle_seconds: float = 0.0
    closes: int = 0
    unprogressed: int = 0
    patience: float = 0.0
    told: str = ""
    looked: float = 0.0
    #: when it ended, on the monitor's clock
    ended: float = 0.0
    #: the wall time at the clock's zero, so a later run reads its times
    epoch: float = 0.0
    transport: dict = field(default_factory=dict)
    abandon: object = field(default=None, repr=False)

    @property
    def key(self):
        return f"{self.side}|{self.operation}|{self.unit}|{self.position}"

    def record(self):
        got = asdict(self)
        got.pop("abandon", None)
        got["session"] = None if self.session is None else str(self.session)
        return got


def classify(view, attempt):
    """What a wait is, from the server's view of its session and where the
    client stands. An idle server alone is no failure: it is one only while
    the client waits for an answer."""
    if view is None or view.error:
        return UNAVAILABLE
    if not view.present:
        return GONE
    if view.state == "locked":
        return LOCKED
    if view.state == "sending":
        return CLIENT_STALL if attempt.stage == "reading" else TRANSFER
    if view.state == "idle":
        return IDLE if attempt.stage == "waiting" else BETWEEN
    return COMPUTING


def transport(sock, deadline=None, asked=None):
    """The keepalive and read deadline a socket holds, read back from it:
    {"keepalive": bool, "idle"/"interval"/"count": seconds or probes,
    "read_deadline": seconds, "error": why a setting failed}. `asked` is
    `util.keepalive`'s answer: a setting it could not make is said, not
    swallowed."""
    import socket as _s
    got = {"read_deadline": deadline}
    if asked and asked.get("error"):
        got["error"] = asked["error"]
    if sock is None:
        got.setdefault("error", "no socket to read")
        return got
    try:
        got["keepalive"] = bool(sock.getsockopt(_s.SOL_SOCKET,
                                                _s.SO_KEEPALIVE))
        for key, names in (("idle", ("TCP_KEEPIDLE", "TCP_KEEPALIVE")),
                           ("interval", ("TCP_KEEPINTVL",)),
                           ("count", ("TCP_KEEPCNT",))):
            opt = next((getattr(_s, n) for n in names if hasattr(_s, n)),
                       None)
            if opt is not None:
                got[key] = sock.getsockopt(_s.IPPROTO_TCP, opt)
    except OSError as e:
        got["error"] = f"keepalive unreadable: {e.strerror or e}"
    return got


class Ledger:
    """The attempts of one run's units and the waits each operation took,
    kept in one file (`durable`) so a restart reads what was measured and
    which unit was closed under whom. None as the path keeps it in memory."""

    def __init__(self, path=None):
        self.path = path
        self._lock = threading.Lock()
        self.attempts = {}
        self.durations = {}
        if path:
            from .durable import read_state
            got = read_state(path, "cache")
            if isinstance(got, dict):
                self.attempts = dict(got.get("attempts") or {})
                self.durations = {k: list(v) for k, v in
                                  (got.get("durations") or {}).items()}

    def waits(self, operation):
        with self._lock:
            return list(self.durations.get(operation, ()))

    def measured(self, operation, seconds):
        with self._lock:
            kept = collections.deque(self.durations.get(operation, ()),
                                     maxlen=DURATIONS_KEPT)
            kept.append(round(float(seconds), 3))
            self.durations[operation] = list(kept)

    def earlier(self, key):
        with self._lock:
            return dict(self.attempts.get(key) or {})

    def units(self, kind="move", operation=None, now=None):
        """The units the attempts worked, as the estimate reads them
        (`util.Unit`, `util.forecast`): a unit every attempt of which ended
        well is done, timed from its first start to its last end; one whose
        last attempt failed and none runs is owed again; one still in
        flight is running, with the seconds since any of its attempts last
        made useful progress (`idle`), which says a stall. `operation`
        narrows to one; an attempt kept before the wall time was is not
        timed."""
        from .util import Unit
        now = time.time() if now is None else float(now)
        by = collections.defaultdict(list)
        with self._lock:
            for r in self.attempts.values():
                if r.get("unit") and r.get("epoch") and (
                        not operation or r.get("operation") == operation):
                    by[r["unit"]].append(dict(r))
        out = []
        for name, rs in sorted(by.items()):
            at = [float(r["epoch"]) for r in rs]
            tries = sum(int(r.get("retries") or 0) for r in rs) + len(rs)
            began = min(e + float(r.get("started") or 0.0)
                        for e, r in zip(at, rs))
            live = [(e, r) for e, r in zip(at, rs)
                    if r.get("stage") != "done"]
            if live:
                moved = max(e + float(r.get("progress") or r.get("sent")
                                      or r.get("started") or 0.0)
                            for e, r in live)
                out.append(Unit(name, kind, state="running", attempts=tries,
                                idle=max(0.0, now - moved)))
            elif any(r.get("reason") for r in rs):
                out.append(Unit(name, kind, state="failed", attempts=tries))
            else:
                end = max(e + float(r.get("ended") or 0.0)
                          for e, r in zip(at, rs))
                out.append(Unit(name, kind, state="done", attempts=tries,
                                seconds=max(0.0, end - began) or None,
                                ended=end))
        return out

    def put(self, attempt):
        """Keep `attempt`; written to the file where it is a unit's."""
        with self._lock:
            self.attempts[attempt.key] = attempt.record()
        if attempt.unit:
            self.flush()

    def flush(self):
        if not self.path:
            return
        import json
        from .durable import write_atomic
        with self._lock:
            text = json.dumps({"attempts": self.attempts,
                               "durations": self.durations},
                              sort_keys=True, default=str)
        write_atomic(self.path, text)


class Monitor:
    """The attempts in flight on one engine, and the one thread that reads
    the server's view of a session whose wait passed its patience. `clock`
    and `say` are given in tests; `check(now)` is one pass of the thread."""

    def __init__(self, engine, ledger=None, clock=time.monotonic, say=None,
                 thread=True, wall=time.time):
        self.engine, self.clock, self.wall = engine, clock, wall
        self.ledger = ledger or Ledger()
        self.say = say or (lambda line: print(line, file=sys.stderr))
        self._lock = threading.Lock()
        self._flight = {}
        self._wake = threading.Event()
        self._thread = None
        self._threaded = thread
        self.closed = []

    def begin(self, side, operation, unit="", position="", session=None,
              reads=True, transport=None, abandon=None, predicted=None):
        from .decide import session_patience
        now = self.clock()
        a = Attempt(side, operation, unit, position, session, reads, now,
                    now, transport=dict(transport or {}), abandon=abandon,
                    epoch=self.wall() - now)
        before = self.ledger.earlier(a.key)
        if before:
            a.retries = int(before.get("retries", 0)) + 1
            a.reason = before.get("reason", "")
            a.closes = int(before.get("closes", 0))
            a.unprogressed = int(before.get("unprogressed", 0))
        a.patience, _ = session_patience(
            self.ledger.waits(operation), predicted,
            (transport or {}).get("read_deadline"))
        a.patience = a.patience or 0.0
        self.ledger.put(a)
        with self._lock:
            self._flight[id(a)] = a
        if a.patience and self._threaded and self._thread is None:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        self._wake.set()
        return a

    def answered(self, a):
        a.answered = a.progress = self.clock()
        a.stage = "reading"

    def progressed(self, a, position=None):
        a.progress = self.clock()
        if position is not None:
            a.position = str(position)
        a.unprogressed = 0
        self.ledger.put(a)

    def done(self, a, ok, reason=""):
        with self._lock:
            self._flight.pop(id(a), None)
        closed, a.stage = a.stage == "closed", "done"
        a.ended = self.clock()
        if ok:
            self.ledger.measured(a.operation, self.clock() - a.sent)
            a.unprogressed, a.reason = 0, ""
        elif not closed:
            # a session closed here fails its read: the close is the reason
            a.reason = reason or a.reason
        self.ledger.put(a)

    def _run(self):
        while True:
            with self._lock:
                flying = [a for a in self._flight.values() if a.patience]
            now = self.clock()
            due = [self._due(a) - now for a in flying
                   if a.stage not in ("done", "closed")]
            self._wake.clear()
            self._wake.wait(max(min(due), 0.05) if due else None)
            self.check()

    def check(self, now=None):
        """Read the view of every session whose wait passed its patience
        and act on what `decide.session_action` chooses."""
        now = self.clock() if now is None else now
        with self._lock:
            flying = list(self._flight.values())
        for a in flying:
            if not a.patience or a.stage in ("done", "closed") \
                    or now < self._due(a):
                continue
            a.looked = now
            self._look(a, now)

    @staticmethod
    def _due(a):
        """When the session is next read: once the wait passes its
        patience, then a quarter of it after each read."""
        return max(a.sent + a.patience,
                   a.looked + a.patience / 4 if a.looked else 0)

    def _look(self, a, now):
        from .decide import session_action
        try:
            view = self.engine.session_view(a.side, a.session)
        except Exception as e:  # noqa: BLE001 - said as unavailable
            view = View(a.session, error=str(e).splitlines()[0][:120]
                        if str(e) else type(e).__name__)
        kind = classify(view, a)
        if kind == IDLE and view.seconds >= a.idle_seconds:
            a.idle_reads += 1
        else:
            a.idle_reads = 0
        a.kind, a.idle_seconds = kind, float(getattr(view, "seconds", 0))
        owned = bool(view and not view.error and view.present
                     and self.engine.owns_session(a.side, view))
        got = session_action({
            "kind": kind, "idle_seconds": a.idle_seconds,
            "patience": a.patience, "idle_reads": a.idle_reads,
            "reads": a.reads, "owned": owned,
            "unprogressed": a.unprogressed,
            "can_abandon": callable(a.abandon)})
        waited = now - a.sent
        if got.path == "close" and self.engine.close_session(
                a.side, a.session, view):
            a.closes += 1
            a.unprogressed += 1
            a.stage = "closed"
            a.reason = "the server's session sat idle while migkit waited"
            self.closed.append((a.side, a.session))
            self._tell(a, (
                f"the {self._side(a)}'s session {a.session} sat idle"
                f" {a.idle_seconds:.0f}s while migkit waited {waited:.0f}s"
                f" for its answer to {a.operation} (the longest wait"
                f" measured here allows {a.patience:.0f}s): the answer was"
                " lost between the server and migkit, and the wait would"
                f" have lasted to the read deadline"
                f"{self._deadline(a)}. Closed that session only; the unit"
                " is read again from its last recorded position"), now)
        elif got.path == "abandon":
            a.closes += 1
            a.unprogressed += 1
            a.stage = "closed"
            a.reason = "the server no longer had the session"
            try:
                a.abandon()
            except Exception:  # noqa: BLE001 - it is ending anyway
                pass
            self._tell(a, (
                f"the {self._side(a)} no longer has session {a.session}"
                f" while migkit waited {waited:.0f}s for its answer to"
                f" {a.operation}: nothing can answer it. Ended migkit's own"
                " end of that connection; the unit is read again from its"
                " last recorded position"), now)
        elif kind in (GONE, UNAVAILABLE, CLIENT_STALL) \
                or (kind == IDLE and a.idle_reads >= 2):
            # one idle read is not yet a fact: said once it is read again
            self._tell(a, (
                f"{a.operation} on the {self._side(a)} has waited"
                f" {waited:.0f}s, past the {a.patience:.0f}s measured:"
                f" {SAID[kind]}. Not closed: {got.reason}. The wait ends"
                f" at the read deadline{self._deadline(a)}; "
                + self._next(kind)), now)
        self.ledger.put(a)

    @staticmethod
    def _side(a):
        return "source" if a.side == "src" else "target"

    @staticmethod
    def _deadline(a):
        d = (a.transport or {}).get("read_deadline")
        return f" ({d:.0f}s, read off the connection)" if d else ""

    @staticmethod
    def _next(kind):
        if kind == UNAVAILABLE:
            return ("grant the account the right to see its own server's"
                    " sessions, so a lost answer is told from a slow one")
        if kind == CLIENT_STALL:
            return ("the machine running migkit is not reading: check its"
                    " CPU and memory")
        return ("check the network path between migkit and the server"
                " (a tunnel's idle timeout, a path that drops large"
                " packets)")

    def _tell(self, a, line, now):
        if a.told == a.kind + str(a.closes):
            return
        a.told = a.kind + str(a.closes)
        self.say(line)
