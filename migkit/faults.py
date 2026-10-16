"""What can go wrong in a migration, how each is told by its effect, and
what is done about it (backlog P0-RESILIENCE-ALL).

The catalogue (`CATALOGUE`) is data: for every class of failure, how it
is detected - the engine's own code first (a MySQL error number, a
PostgreSQL SQLSTATE), then what the failure did (`effect`: was anything
sent, does the work write, did the server roll the statement back), and
the error's words only last; whether it clears by itself; what must hold
before the work is tried again; how far it is contained; the recovery
actions it admits; and the test that proves it, or `UNPROVED`.

The choice among those actions is not made here: `decide.recovery` climbs
proceed, reconcile, pause, throttle, retry, replan, isolate, probe and
stop over the facts of the failure and its state. `retried` is the one
loop that carries a choice out - `util.with_retry` and `util.run`, and so
every engine's query, connect and program run, go through it:

* tries are bounded, and owned: an operation a run starts has an identity
  (`Owned`), kept in the attempts ledger (`attempts.Ledger.own`), so a
  restart knows which work it began and never saw end;
* an outcome that is not known - a write whose connection was lost after
  it was sent - is reconciled (committed, still running, or nothing) by
  the caller's `check` before anything is repeated; with no way to check
  it is stopped, never repeated on a guess;
* a side that keeps failing opens its breaker (`Breaker`): the others
  wait out its cooldown and one probe goes, so many workers on one sick
  server do not become a storm;
* the operator's log failing never changes what is done (`_say`).
"""
import hashlib
import re
import threading
import time
from dataclasses import dataclass

#: the actions `decide.recovery` chooses among, in the order it climbs them
ACTIONS = ("reconcile", "proceed", "pause", "throttle", "retry", "replan",
           "isolate", "probe", "stop")


@dataclass(frozen=True)
class Fault:
    """One class of failure."""
    name: str
    said: str
    #: clears by itself given time (a dropped link), or not (a password)
    transient: bool
    #: what must hold before the same work is tried again
    retry_after: str
    #: how far the failure is held: the attempt, the unit, the side, the run
    contains: str
    #: the actions `decide.recovery` may take for it, its first choice first
    admits: tuple
    #: MySQL error numbers and PostgreSQL SQLSTATEs (or their class, "08")
    codes: tuple = ()
    #: words of the error, read only where no code says it
    words: tuple = ()
    #: the facts (engine readings) that detect it without any error
    facts: tuple = ()
    #: words that say this class whatever other class's words they hold,
    #: read before any `words`
    phrases: tuple = ()
    #: the server rolled back what failed, so a write may be run again
    rolled_back: bool = False
    #: the test proving it, or "UNPROVED: why"
    proof: str = ""


def _f(name, said, transient, retry_after, contains, admits, **kw):
    return Fault(name, said, transient, retry_after, contains, admits, **kw)


_T = "tests/test_a_failure_is_told_by_what_it_did.py::"

CATALOGUE = {f.name: f for f in (
    _f("connectivity", "the server could not be reached", True,
       "a new connection, after a backoff", "the side",
       ("retry", "pause", "stop"),
       codes=(2002, 2003, 2005, 2006, 2013, 2055, "08"),
       words=("wrong version number", "lost connection", "connection reset",
              "connection refused", "could not connect", "can't connect",
              "server closed the connection", "broken pipe", "gone away",
              "eof occurred", "timeout expired", "connection timed out",
              "temporary failure", "no route to host", "connection aborted",
              "reset by peer", "timed out", "operationalerror",
              "dbprocess is dead", "not connected"),
       proof=_T + "test_a_dropped_connection_is_retried_and_then_proceeds"),
    _f("auth", "the server refused the account", False,
       "never: the credentials must be corrected", "the side",
       ("stop",), codes=(1045, 1698, "28P01", "28000"),
       words=("password authentication failed", "access denied for user"),
       proof=_T + "test_a_refused_account_stops_at_once"),
    _f("permission", "the account lacks a privilege the way needs", False,
       "never on this way: another way that does not need it", "the way",
       ("replan", "stop"),
       codes=(1142, 1143, 1227, 1044, "42501"),
       words=("permission denied", "command denied"),
       proof=_T + "test_a_missing_privilege_replans_and_never_retries"),
    _f("expired_credential", "the credential has expired", False,
       "a renewed credential", "the side",
       ("pause", "stop"), codes=(1820, 1862),
       words=("password has expired", "expiredtoken", "token has expired",
              "security token included in the request is expired"),
       proof=_T + "test_an_expired_credential_waits_only_where_it_renews"),
    _f("quota", "the server or its service limit throttled the work", True,
       "a longer backoff with less work at once", "the side",
       ("throttle", "pause", "stop"),
       codes=(1040, 1203, 1226, "53300"),
       words=("too many connections", "throttl", "rate exceeded",
              "slowdown", "requestlimitexceeded", "too many requests"),
       proof=_T + "test_throttling_backs_off_longer_than_a_drop"),
    _f("cpu_pressure", "the server's processors are saturated", True,
       "the load eased", "the side", ("throttle", "pause"),
       facts=("source_cpu_busy", "target_cpu_busy"),
       proof=_T + "test_pressure_read_as_a_fact_throttles"),
    _f("memory_pressure", "a process ran out of memory", True,
       "smaller units and less work at once", "the unit",
       ("throttle", "stop"), codes=(1037, 1038, 1041, "53200"),
       words=("out of memory", "memoryerror", "cannot allocate memory",
              "invalid memory alloc request size", "resource exhausted"),
       proof=_T + "test_pressure_read_as_a_fact_throttles"),
    _f("disk_pressure", "a disk is full", False,
       "space freed on the full disk", "the side", ("pause", "stop"),
       codes=(1021, 1114, 28, "53100"),
       words=("no space left on device", "disk full", "could not extend file"),
       facts=("free_bytes",),
       proof=_T + "test_a_full_disk_stops_and_is_never_retried_blind"),
    _f("lock", "a lock wait timed out or a deadlock was broken", True,
       "the server rolled the statement back", "the attempt",
       ("retry", "pause", "stop"), codes=(1205, 1213, "40P01", "55P03"),
       words=("deadlock", "lock wait timeout", "could not obtain lock",
              "lock request time out"),
       rolled_back=True,
       proof=_T + "test_a_deadlock_victim_is_rerun_only_where_it_was_whole"),
    _f("packet", "a statement was larger than the server takes in one",
       False, "never the same size: a smaller batch, pieces or a stream",
       "the unit", ("replan", "stop"), codes=(1153, 1301),
       words=("max_allowed_packet", "packet too large",
              "packet bigger than", "total message length",
              "request entity too large", "exceeds the maximum allowed size",
              "exceeds the maximum size", "exceeds the maximum length"),
       proof="tests/test_a_copier_reconciles_and_retunes.py::"
             "test_a_packet_too_large_is_retuned_once_and_completes"),
    _f("statement_timeout", "a statement ran past the server's time limit",
       False, "never the same size: a shorter unit", "the unit",
       ("replan", "stop"), codes=(3024, "57014"),
       words=("statement timeout", "max_execution_time",
              "query execution was interrupted"),
       # read before the connection's words, which a statement's own
       # time limit shares ("timeout expired")
       phrases=("canceling statement due to statement timeout",
                "execution timeout expired", "query timeout expired",
                "query timed out", "statement timed out"),
       proof="tests/test_a_copier_reconciles_and_retunes.py::"
             "test_a_statement_timeout_is_a_sizing_cause"),
    _f("orphaned", "work migkit owns is still running after its attempt",
       False, "the owned work ended or cancelled", "the unit",
       ("reconcile", "isolate", "stop"), facts=("owned_running",),
       proof=_T + "test_unknown_outcome_is_reconciled_before_repeat"),
    _f("duplicate", "an event arrived twice or out of order", False,
       "the unit reconciled against the target", "the unit",
       ("isolate", "stop"), codes=(1062, 1022, "23505"),
       words=("duplicate key", "duplicate entry", "out of order"),
       facts=("position_regressed",),
       proof=_T + "test_a_duplicate_isolates_the_unit"),
    _f("uncertain_commit", "a write's connection was lost after it was sent",
       False, "its outcome reconciled", "the unit",
       ("reconcile", "stop"), codes=("08007", "40003"),
       words=("transaction resolution unknown",),
       proof=_T + "test_unknown_outcome_is_reconciled_before_repeat"),
    _f("interruption", "the server or migkit was shut down mid-work", True,
       "the server back, and a write's outcome reconciled", "the side",
       ("retry", "reconcile", "stop"),
       codes=(1053, 1927, "57P01", "57P02", "57P03"),
       words=("terminating connection due to administrator command",
              "server shutdown in progress", "the database system is"
              " starting up", "the database system is shutting down"),
       proof=_T + "test_a_restart_during_cleanup_reconciles_then_resumes"),
    _f("failover", "the endpoint now reaches a read-only server", False,
       "the writer endpoint read again", "the side",
       ("replan", "stop"), codes=(1290, 1792, 1836, "25006"),
       words=("read-only transaction", "--read-only option",
              "read only transaction"),
       proof=_T + "test_a_failover_replans_and_never_retries_the_reader"),
    _f("stale_checkpoint", "a kept position is older than what the server"
       " still holds", False, "the unit copied again from a fresh position",
       "the unit", ("replan", "stop"), facts=("checkpoint_behind_retention",),
       proof=_T + "test_lost_history_replans"),
    _f("inconsistent_view", "a read could not hold one consistent view", True,
       "a fresh snapshot", "the attempt", ("retry", "stop"),
       codes=(1412, "40001", "72000"),
       words=("could not serialize access", "snapshot too old",
              "table definition has changed"),
       rolled_back=True,
       proof=_T + "test_a_read_retries_a_broken_snapshot"),
    _f("lost_history", "the change history needed was removed or lags past"
       " what is kept", False, "a reseed of what the history no longer has",
       "the hop", ("replan", "stop"), codes=(1236,),
       words=("has already been removed", "purged binary logs",
              "could not find first log file", "requested wal segment"),
       facts=("lag_seconds",),
       proof=_T + "test_lost_history_replans"),
    _f("schema_change", "a table or column changed under the work", False,
       "the plan read again from the schema now there", "the unit",
       ("replan", "stop"),
       codes=(1146, 1054, "42P01", "42703", "0A000"),
       words=("doesn't exist", "does not exist", "unknown column"),
       proof=_T + "test_a_schema_change_replans_the_unit"),
    _f("lossy_conversion", "a value would not be kept whole on the target",
       False, "never: a mapping that keeps it", "the hop", ("stop",),
       codes=(1264, 1406, 1265, "22001", "22003"),
       words=("data too long", "out of range", "data truncated",
              "right truncation"),
       proof=_T + "test_a_lossy_value_stops_and_is_never_set_aside"),
    _f("malformed_data", "a source value is not valid for its type", False,
       "the value corrected at the source or mapped", "the unit",
       ("isolate", "stop"),
       codes=(1366, 1292, 1300, "22P02", "22021", "22007", "22008"),
       words=("incorrect string value", "invalid byte sequence",
              "invalid input syntax", "incorrect datetime value"),
       proof=_T + "test_a_duplicate_isolates_the_unit"),
    _f("corrupted_evidence", "a file migkit kept is torn or does not match"
       " its digest", False, "the evidence read again from the servers",
       "the evidence", ("probe", "stop"),
       words=("digest mismatch", "checksum mismatch"),
       proof=_T + "test_corrupted_evidence_is_probed_never_trusted"),
    _f("dependency", "a program or service the way needs failed", False,
       "another way, or the dependency back", "the way",
       ("replan", "stop"),
       words=("no such file or directory", "command not found",
              "service unavailable", "bad gateway"),
       proof=_T + "test_a_missing_program_replans"),
    _f("external_impact", "the work slows what else runs on a server", True,
       "the workload's own latency back under its limit", "the side",
       ("throttle", "pause"), facts=("workload_slowed",),
       proof=_T + "test_pressure_read_as_a_fact_throttles"),
    _f("unknown", "a failure no class here describes", False,
       "never on a guess", "the unit", ("stop",),
       proof=_T + "test_an_unknown_failure_stops_conservatively"),
)}

#: how each pressure fact reads as present
_PRESSED = {
    "source_cpu_busy": lambda v: bool(v), "target_cpu_busy": lambda v: bool(v),
    "workload_slowed": lambda v: bool(v),
    "checkpoint_behind_retention": lambda v: bool(v),
    "owned_running": lambda v: bool(v),
    "position_regressed": lambda v: bool(v),
}

#: errors raised while connecting, before anything was sent
CONNECTING_CODES = (2002, 2003, 2005, 1040, 1045, 1698, 1203, "08001",
                    "08004", "08006", "53300", "57P03", "28P01", "28000")


def code_of(err):
    """The engine's own code for `err`: a MySQL error number or a
    PostgreSQL SQLSTATE, read off the exception, never its words."""
    for attr in ("sqlstate", "pgcode"):
        got = getattr(err, attr, None)
        if isinstance(got, str) and len(got) == 5:
            return got
    diag = getattr(err, "diag", None)
    got = getattr(diag, "sqlstate", None)
    if isinstance(got, str) and len(got) == 5:
        return got
    args = getattr(err, "args", ()) or ()
    if isinstance(err, OSError) and err.errno:
        return err.errno
    if args and type(args[0]) is int and args[0] >= 1000:
        return args[0]
    m = re.match(r"\s*\((\d{3,5}),", str(err)) or re.search(
        r"\bERROR (\d{3,5}) \(", str(err))
    return int(m.group(1)) if m else None


def _code_class(code):
    """The class whose code is exactly `code`, else whose SQLSTATE class
    ("08") it is in."""
    if code is None:
        return None
    for exact in (True, False):
        for f in CATALOGUE.values():
            for c in f.codes:
                if c == code if exact else (
                        isinstance(c, str) and isinstance(code, str)
                        and len(c) == 2 and code.startswith(c)):
                    return f
    return None


#: a timeout said with nothing else: the server's time limit on a
#: statement while one runs (`statement_timeout`, a size answers it), the
#: connection's while one is opened (`connectivity`) - told apart only by
#: the phase it was met in (`classify(during=)`)
BARE_TIMEOUT = ("timed out", "timeout")


def _bare_timeout(text):
    """Whether `text` says a timeout and nothing else of the connection:
    no word of a link lost, refused or being opened."""
    from .util import CONNECTING
    if not any(w in text for w in BARE_TIMEOUT):
        return False
    rest = text
    for w in sorted(BARE_TIMEOUT, key=len, reverse=True):
        rest = rest.replace(w, " ")
    # the link named in the timeout's own words ("connection timed out")
    # is the link's, whatever phase it was met in
    link = [w for w in CATALOGUE["connectivity"].words
            if w not in BARE_TIMEOUT and w != "operationalerror"] + [
                "connection", "network", "socket"]
    return not any(w in rest for w in link) and not any(
        p in text for p in CONNECTING)


def _words_class(err):
    e = str(err).lower()
    for f in CATALOGUE.values():
        if any(w in e for w in f.phrases):
            return f
    for f in CATALOGUE.values():
        if any(w in e for w in f.words):
            return f
    return None


@dataclass(frozen=True)
class Told:
    """A failure as classified: its class, its code, the phase it failed
    in (connecting, or after the work was sent) and whether what it did is
    known."""
    fault: Fault
    code: object = None
    phase: str = "sent"
    uncertain: bool = False
    by: str = "words"


def classify(err, writes=False, atomic=False, during=None):
    """`Told` for `err`. By the engine's code first, then by what it did:
    a failure after a write was sent, of a kind the server does not roll
    back whole (or of a write that is not one transaction, `atomic`),
    leaves its outcome unknown - an `uncertain_commit` whatever the
    words; a lost connection while connecting sent nothing. A bare
    timeout (`BARE_TIMEOUT`) is the phase's: met `during` a "statement"
    it is the statement's time limit, during a "connect" the link's."""
    from .util import CONNECTING
    code = code_of(err)
    fault, by = _code_class(code), "code"
    if isinstance(err, FileNotFoundError) and not isinstance(code, str):
        fault, by = CATALOGUE["dependency"], "effect"
    if fault is None:
        fault, by = _words_class(err), "words"
    if fault is None and isinstance(err, MemoryError):
        fault, by = CATALOGUE["memory_pressure"], "effect"
    fault = fault or CATALOGUE["unknown"]
    text = str(err).lower()
    bare = by == "words" and fault.name in ("connectivity", "unknown") \
        and _bare_timeout(text)
    if bare and during == "statement":
        fault, by = CATALOGUE["statement_timeout"], "phase"
    elif bare and during == "connect":
        fault, by = CATALOGUE["connectivity"], "phase"
    connecting = code in CONNECTING_CODES or any(
        p in text for p in CONNECTING) or (bare and during == "connect")
    phase = "connecting" if connecting else "sent"
    uncertain = False
    if writes and phase == "sent" and fault.transient:
        whole = fault.rolled_back and atomic
        if not whole:
            uncertain, by = True, "effect"
            fault = CATALOGUE["uncertain_commit"]
    elif writes and fault.name == "uncertain_commit":
        uncertain = True
    return Told(fault, code, phase, uncertain, by)


def from_facts(facts):
    """The classes the readings in `facts` detect with no error raised:
    pressure, a stale checkpoint, lag, a workload slowed."""
    out = []
    for f in CATALOGUE.values():
        for k in f.facts:
            if k not in facts:
                continue
            v = facts[k]
            hit = _PRESSED[k](v) if k in _PRESSED else _threshold(k, v, facts)
            if hit and f not in out:
                out.append(f)
    return out


def _threshold(key, value, facts):
    if key == "free_bytes":
        need = facts.get("bytes_needed")
        return need is not None and value is not None and value < need
    if key == "lag_seconds":
        kept = facts.get("history_kept_seconds")
        return kept is not None and value is not None and value >= kept
    return False


class Breaker:
    """One breaker per side: closed while the side answers; open once it
    failed more times in a row than one operation may try, so it is the
    side and not the operation; while open the side's callers wait its
    cooldown and one goes as the probe (half open). The cooldown is how
    long the side last took to come back, where that was measured, else
    the backoff of its failures past the opening, never above `ceiling`."""

    def __init__(self):
        self._lock = threading.Lock()
        self.sides = {}

    def _side(self, side):
        return self.sides.setdefault(side, {
            "failures": 0, "opened": 0.0, "probing": False,
            "recovered_in": None, "opens": 0})

    def failed(self, side, tries, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            s = self._side(side)
            s["failures"] += 1
            if s["probing"]:
                s["opened"] = now
            s["probing"] = False
            if s["failures"] > tries and not s["opened"]:
                s["opened"], s["opens"] = now, s["opens"] + 1

    def ok(self, side, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            s = self._side(side)
            if s["opened"]:
                s["recovered_in"] = now - s["opened"]
            s.update(failures=0, opened=0.0, probing=False)

    def cooldown(self, side, backoff, ceiling):
        with self._lock:
            s = self._side(side)
            measured = s["recovered_in"]
            past = max(0, s["failures"] - 1)
        got = measured if measured is not None else backoff(past)
        return max(0.0, min(got, ceiling))

    def admit(self, side, cooldown, now=None):
        """"closed", "probe" (this caller is the one probe) or "open"."""
        now = time.monotonic() if now is None else now
        with self._lock:
            s = self._side(side)
            if not s["opened"]:
                return "closed"
            if now - s["opened"] >= cooldown and not s["probing"]:
                s["probing"] = True
                return "probe"
            return "open"


BREAKER = Breaker()


def reset():
    """Every side's breaker closed: a new run, or a test."""
    BREAKER.sides.clear()


@dataclass(frozen=True)
class Owned:
    """An operation's identity: the side, the operation, its unit and
    position - the same work has the same key in every run, so a restart
    finds what it began."""
    side: str
    operation: str
    unit: str = ""
    position: str = ""

    @property
    def key(self):
        raw = f"{self.side}|{self.operation}|{self.unit}|{self.position}"
        return "op:" + hashlib.sha256(raw.encode()).hexdigest()[:16]


#: what a reconcile `check` may answer
COMMITTED, RUNNING, NOTHING = "committed", "running", "nothing"


def _say(log, text):
    """Tell the operator; a log that fails is not a failure of the work."""
    if not log:
        return
    try:
        log(text)
    except Exception:  # noqa: BLE001 - telemetry loss never changes the move
        pass


def _check(check):
    """(state, value) from a reconcile `check`: a state, or (state, value);
    a check that fails answers None - not known."""
    try:
        got = check()
    except Exception:  # noqa: BLE001 - an unanswered check is "not known"
        return None, None
    if isinstance(got, tuple):
        return (got + (None,))[:2]
    return got, None


def _recovered(hurt, outcome, tries=None):
    """Tell telemetry how a failure that was met ended, by its class and
    the action chosen; nothing where no failure was met."""
    if hurt is not None:
        from . import telemetry
        name, path, made, error = hurt
        telemetry.recovered(name, path, tries or made, outcome, error)


def retried(fn, tries=4, side="", operation="", writes=False, atomic=False,
            check=None, renew=None, log=None, label="", backoff=None,
            wait=None, ledger=None, owned=None, breaker=None):
    """fn() run under the recovery `decide.recovery` chooses, try by try.

    `writes`: fn changes something; `atomic`: it is one transaction, so a
    failure the server rolls back left nothing. `check`: answers whether
    an uncertain write committed (`COMMITTED`, `RUNNING`, `NOTHING`, or
    (state, value) - the value returned for a committed one); `renew`
    renews an expired credential. `backoff(i)` seconds before try i+1;
    `wait(s)` sleeps (both read at call time so a run or a test may set
    them). `ledger` (`attempts.Ledger`) and `owned` keep the operation's
    identity, tries and outcome across restarts: an operation a restart
    found begun and never ended is reconciled before it is run again.
    Every try counts against `tries` - a wait, a probe and a reconcile
    too - so no failure makes more than `tries` of anything."""
    from . import decide, failpoint
    backoff = backoff or (lambda i: 0.8 * (2 ** i))
    wait = wait or time.sleep
    breaker = breaker or BREAKER
    side = side or label or "op"
    name = label or operation or "op"
    ceiling = backoff(max(0, tries - 1))
    keep = ledger is not None and owned is not None
    rec = ledger.owned(owned.key) if keep else {}
    spent = int(rec.get("tries") or 0)
    reconciled = None
    # a record that ended committed is read again too, where a way to ask
    # exists: the same work in a later run is counted, not written twice
    if keep and writes and (rec.get("state") in ("began", "unknown", RUNNING)
                            or (rec.get("state") == COMMITTED
                                and check is not None)):
        state, value = _reconcile_restart(check, ledger, owned, log, name)
        if state == COMMITTED:
            return value
        reconciled = state
    last, told, hurt = None, None, None
    for i in range(tries):
        if reconciled == RUNNING:
            state, value = _check(check)
            reconciled = state
            if keep:
                ledger.own(owned.key, state=state or "unknown")
            if state == COMMITTED:
                breaker.ok(side)
                return value
            if state == RUNNING:
                _say(log, f"{name}: the earlier attempt is still running;"
                          " waiting for it, not starting another")
                wait(backoff(i))
                continue
            if state != NOTHING:
                break
        if i and told is not None and breaker.admit(
                side, breaker.cooldown(side, backoff, ceiling)) == "open":
            # the side, not the operation, keeps failing: the recovery is
            # asked again with the breaker open, and the path it takes now
            # is kept with its facts
            facts = _facts(told, check, renew, reconciled, tries - i, "open")
            got = decide.recovery(facts)
            _decided(ledger, owned, got, facts)
            if got.path == "pause":
                _say(log, f"{name}: {side} keeps failing; waiting its"
                          " cooldown before one probe")
                wait(breaker.cooldown(side, backoff, ceiling))
                continue
            if got.path == "throttle":
                _say(log, f"{name}: {side} keeps failing; backing off"
                          " longer with less at once")
                wait(2 * breaker.cooldown(side, backoff, ceiling))
                continue
            # replan, isolate, probe or stop: not tried again on a side
            # whose breaker is open - the caller takes the path named
            try:
                last.recovery = got.path
            except (AttributeError, TypeError):
                pass
            _say(log, f"{name}: {side} keeps failing; {got.path}")
            _recovered(hurt, "failed")
            raise last
        if keep:
            ledger.own(owned.key, state="began", tries=spent + i + 1,
                       side=owned.side, operation=owned.operation,
                       unit=owned.unit, position=owned.position)
        try:
            failpoint.hit("fault.try")
            out = fn()
        except Exception as e:  # noqa: BLE001 - classified by code and effect
            last, told = e, classify(e, writes, atomic)
            if told.fault.transient or told.uncertain:
                breaker.failed(side, tries)
            if keep:
                ledger.own(owned.key, fault=told.fault.name,
                           state="unknown" if told.uncertain else "failed")
            facts = _facts(told, check, renew, reconciled, tries - i - 1,
                           "closed")
            got = decide.recovery(facts)
            _decided(ledger, owned, got, facts)
            try:
                e.fault, e.recovery = told.fault.name, got.path
            except (AttributeError, TypeError):
                pass
            hurt = (told.fault.name, got.path, i + 1, e)
            if got.path == "reconcile":
                state, value = _check(check)
                reconciled = state
                _say(log, f"{name}: outcome not known after"
                          f" {told.fault.said}; reconciled:"
                          f" {state or 'not known'}")
                if keep:
                    ledger.own(owned.key, state=state or "unknown",
                               reconciled=state or "unknown")
                if state == COMMITTED:
                    breaker.ok(side)
                    _recovered(hurt, "recovered")
                    return value
                if state == NOTHING and i < tries - 1:
                    # nothing of it landed: asked again, now that the
                    # effect is known - a size the server refused goes to
                    # the caller's retune, never again as it was
                    facts = _facts(told, check, renew, NOTHING,
                                   tries - i - 1, "closed")
                    again = decide.recovery(facts)
                    _decided(ledger, owned, again, facts)
                    if again.path != "replan":
                        wait(backoff(i) * (2 if again.path == "throttle"
                                           else 1))
                        continue
                    try:
                        e.recovery = again.path
                    except (AttributeError, TypeError):
                        pass
                    _recovered(hurt, "failed")
                    raise
                if state == RUNNING and i < tries - 1:
                    wait(backoff(i))
                    continue
                _recovered(hurt, "failed")
                raise
            if got.path == "pause" and told.fault.name == "expired_credential":
                renew()
                continue
            if got.path in ("retry", "throttle", "pause") and i < tries - 1:
                delay = backoff(i) * (2 if got.path == "throttle" else 1)
                tail = str(e).splitlines()[-1][:60] if str(e) else ""
                _say(log, f"{name}: {told.fault.said}, {got.path}"
                          f" {i + 1}/{tries - 1} in {delay:.1f}s ({tail})")
                wait(delay)
                reconciled = None
                continue
            _recovered(hurt, "failed")
            raise
        breaker.ok(side)
        if keep:
            ledger.own(owned.key, state="done")
        _recovered(hurt, "recovered", i + 1)
        return out
    if last is not None:
        _recovered(hurt, "failed")
        raise last
    raise RuntimeError(f"{name}: the outcome could not be settled within"
                       f" {tries} tries, so the work is not run again")


def _facts(told, check, renew, reconciled, left, breaker):
    f = told.fault
    return {"class": f.name, "admits": f.admits, "transient": f.transient,
            "uncertain": told.uncertain, "can_check": check is not None,
            "renewable": renew is not None, "reconciled": reconciled,
            "tries_left": left, "breaker": breaker, "phase": told.phase}


def _decided(ledger, owned, got, facts):
    """The recovery chosen after a failure kept in the operation's record
    with the facts it was chosen on, so the run's evidence names the path
    and why - every one, in order."""
    if ledger is None or owned is None:
        return
    rec = ledger.owned(owned.key) or {}
    kept = list(rec.get("decided") or ())
    kept.append({"path": got.path, "class": facts["class"],
                 "breaker": facts["breaker"],
                 "tries_left": facts["tries_left"],
                 "reconciled": facts["reconciled"],
                 "uncertain": facts["uncertain"]})
    ledger.own(owned.key, decided=kept)


def _reconcile_restart(check, ledger, owned, log, name):
    """A restart found `owned` begun and never ended: what it did is
    asked before anything repeats it; with no way to ask, it stops."""
    state, value = _check(check) if check else (None, None)
    ledger.own(owned.key, state=state or "unknown")
    _say(log, f"{name}: begun before a restart; reconciled:"
              f" {state or 'not known'}")
    if state in (COMMITTED, NOTHING, RUNNING):
        return state, value
    raise RuntimeError(
        f"{name}: work begun before a restart has an outcome that cannot"
        " be read, so it is not run again; check the target for this unit,"
        " then run again")


def landed(want, held, before=None, idempotent=False, value=None):
    """A reconcile `check` for `retried` from two readings of one unit:
    what the target was to hold (`want()`) and what it holds (`held()`) -
    a row count, a position or a marker. Equal: the write landed
    (`COMMITTED`, with `value(want)` or the reading). Where the write
    replaces its span whole in one transaction (`idempotent`), or the
    target holds what it held before it (`before`), nothing of it is
    there and it may run again (`NOTHING`); anything else is not known,
    and the write stops rather than doubling what it may have left."""
    def check():
        w, h = want(), held()
        if w == h:
            return COMMITTED, (value(w) if value else w)
        if idempotent or (before is not None and h == before):
            return NOTHING, None
        return None, None
    return check


class Injected(Exception):
    """A failure of a class raised on purpose (`failpoint`): the engine
    code the class is detected by, as a driver would carry it."""

    def __init__(self, name):
        f = CATALOGUE[name]
        code = next(iter(f.codes), None)
        if isinstance(code, int):
            super().__init__(code, f.said)
        else:
            super().__init__(f.words[0] if f.words and code is None
                             else f.said)
        if isinstance(code, str) and len(code) == 5:
            self.sqlstate = code
        self.fault = name


def assess(facts, tries_left=1, renewable=False):
    """[(Fault, `decide.Choice`)] for what the readings in `facts` say with
    no error raised - asked before a step and between its units, so
    pressure throttles and a stale checkpoint replans before a failure
    would have said so."""
    from . import decide
    return [(f, decide.recovery({
        "class": f.name, "admits": f.admits, "transient": f.transient,
        "uncertain": False, "can_check": False, "renewable": renewable,
        "reconciled": None, "tries_left": tries_left, "breaker": "closed"}))
        for f in from_facts(facts)]
