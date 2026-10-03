"""Every program migkit runs is started, watched, stopped and said one way.

Measured before there was one place (the audit of 2026-10-01): 62 places
started a program each its own way - 15 with a time limit, 2 in a process
group of their own, 7 listed where the next run could look for them - so a
stopped move left programs writing, a program that hung was waited on for
good, and one failure was said as many ways as there were places. Here:

* one environment (`environment`): migkit's program paths first, the
  switches that keep a program from calling its vendor, and whatever the
  caller hands it - a secret goes in the environment or a file of this
  user's (`util.PrivateFile`, an environment file for a container), never
  on a command line every process listing on the machine can read
* a session of its own (`start`): stopping it stops every process it
  started too - the group is asked to end, then killed (`Proc.stop`); on
  Linux a program started from migkit's main thread is also told to end
  when migkit dies, `kill -9` and the out-of-memory killer included
* SIGTERM taken as ctrl-c while a program runs, so `kill` and a service
  manager's stop end it the way ctrl-c does
* a line in the run's list of programs while it runs (`listing`), by its
  process number and the moment it started, so the next run finds a writer
  still running - and never mistakes a stranger that took its number for
  it (`still_running`)
* its output read as it comes, a line at a time, on threads of their own
  so a full pipe never holds a program up: a progress reader turns lines
  into migkit's words, and only the last of the rest is kept, for a
  failure's words
* a time limit where the caller has one, and a stall limit fed by the
  progress readers (`stall.seconds()`, `MIGKIT_STALL_SECONDS`): a program
  that says nothing new for that long is ended and said
* its failure in migkit's words (`classify`): the database's own where it
  said them, the program's name taken out, never its command line - and of
  one of three kinds, which decide whether another way is tried:
  `Refused` stops every way (the sign-in or a privilege refused, the
  server's disk full, a read-only target: every way would meet it),
  `Blocked` is this way only (not installed here, an option this build
  does not take), `Transient` may go again (a connection dropped, a limit
  reached)
* a container named after the run and started with `--init`
  (`container`), removed when it ends however it ends, and before the
  next run where a run that is gone left one behind
"""
import itertools
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager

from . import decide, stall
from .util import PrivateFile, quiet_argv, tool_env


# ---- what a failure is ------------------------------------------------------

class Failed(RuntimeError):
    """The program failed, in migkit's words: a way that could not run,
    which the decision layer may switch from (`decide.operational`)."""


class Transient(Failed):
    """The connection dropped, timed out, or the program was ended for
    passing nothing: this way again may carry it."""


class Blocked(decide.Blocked):
    """This way cannot run here - not installed, or its build does not
    take what it is given - and nothing was started: another way may."""


class Refused(SystemExit):
    """No way can run: the database refused the sign-in or a privilege it
    needs, its disk is full, or the target takes no writes. Every other
    way would meet the same refusal, so the move stops here, in the
    database's words."""


#: what the database said that no way gets past, by SQLSTATE or by the
#: words each server says it in: (pattern, what it means), the first that
#: matches - MySQL says a privilege refused on a database with the words
#: it says a refused sign-in with, and more after them
_REFUSED = (
    (re.compile(r"\[?\b42501\b|permission denied for "
                r"|command denied to user|Access denied; you need"
                r"|Access denied for user \S+ to database"
                r"|not authorized on \S+ to execute|\bNOPERM\b"
                r"|must be (?:superuser|owner of)", re.I),
     "the database refused a privilege this needs"),
    (re.compile(r"\[?\b(28P01|28000)\b|password authentication failed"
                r"|Access denied for user|Login failed for user"
                r"|Authentication failed|\bWRONGPASS\b|\bNOAUTH\b"
                r"|no pg_hba\.conf entry", re.I),
     "the database refused the sign-in"),
    (re.compile(r"\[?\b53100\b|could not extend file|The table '[^']*' is"
                r" full|\bER_RECORD_FILE_FULL\b", re.I),
     "the database ran out of disk space"),
    (re.compile(r"\[?\b25006\b|read-only transaction"
                r"|running with the --(?:super-)?read-only option"
                r"|\bREADONLY\b You can't write", re.I),
     "the target takes no writes"),
)


def _transient(text):
    from .util import is_transient
    return is_transient(text)


def names():
    """The names of every program migkit runs, which no message keeps."""
    from . import tools
    return tools.names()


def words(text):
    """`text` with no program migkit runs named in it."""
    from .wording import without_programs
    return without_programs(str(text or ""), names())


def said(text, secrets=()):
    """What a program's output says, for a person: the database's own words
    where it said any, the tail of the output otherwise - its secrets
    hidden and no program named."""
    from .wording import database_words
    text = hide(text or "", secrets)
    got = database_words(text) or text.strip()[-500:]
    return words(got)


def classify(rc, text, what="", secrets=()):
    """The exception a program that ended `rc` after saying `text` is
    raised as (`Refused`, `Blocked`, `Transient` or `Failed`), in migkit's
    words: `what` - the step, in migkit's words - then what it said. A
    number below zero is the signal that ended it, said as such."""
    body = said(text, secrets)
    lead = f"{what}: " if what else ""
    for rx, why in _REFUSED:
        if rx.search(text or ""):
            return Refused(f"{lead}{body} ({why}: no other way would get"
                           " past it, so nothing more is tried)")
    if rc == 127 or rc == "missing":
        return Blocked(f"{lead}this way needs a component this machine does"
                       " not have - migkit doctor --install puts it in"
                       " place")
    if isinstance(rc, int) and rc < 0:
        try:
            sig = signal.Signals(-rc).name
        except ValueError:
            sig = f"signal {-rc}"
        return Failed(f"{lead}ended by {sig} from outside migkit - the"
                      " machine's memory running out is the usual one"
                      + (f"; before it: {body}" if body else ""))
    if _transient(text):
        return Transient(f"{lead}{body}")
    return Failed(f"{lead}{body}" if body else
                  f"{lead}it stopped (exit {rc}) without saying why")


# ---- secrets ------------------------------------------------------------------

#: environment variables whose values are secrets
SECRET_ENV = re.compile(r"PASS|PWD|SECRET|TOKEN|URI|PGPASSFILE|_KEY\b", re.I)


def secrets_of(env):
    """The values of `env` that are secrets, by the names they go under;
    an address carrying its password counts."""
    out = []
    for k, v in (env or {}).items():
        if v and SECRET_ENV.search(str(k)):
            out.append(str(v))
    return out


def hide(text, secrets=()):
    """`text` with every secret replaced by `***`: the ones named, the
    run's (`recording`), the environment's, and every secret found by its
    shape (`redact.Redactor`)."""
    from .redact import Redactor
    got = [s for s in list(secrets or ()) + list(_RUN["secrets"]) if s]
    return Redactor(secrets=got)(str(text))


# ---- the run's record and list ------------------------------------------------

_RUN = {"debug": None, "secrets": (), "listing": None}
_LOCK = threading.Lock()


@contextmanager
def recording(debug_log, secrets=()):
    """Every command line started inside goes to `debug_log` (a
    `wording.DebugLog`), with `secrets` hidden - the run's record of what
    it ran, never the screen."""
    before = dict(_RUN)
    _RUN.update(debug=debug_log, secrets=tuple(s for s in secrets if s))
    try:
        yield
    finally:
        _RUN.update(debug=before["debug"], secrets=before["secrets"])


def debug(argv, env=None, secrets=()):
    """One command line into the run's record, secrets hidden."""
    log = _RUN["debug"]
    if log is not None:
        log.command([str(a) for a in argv],
                    list(secrets) + secrets_of(env) + list(_RUN["secrets"]))


_LISTINGS = []


@contextmanager
def listing(path):
    """Every program started inside is listed in `path` while it runs -
    from every thread: tables copied side by side each enter it, and the
    first to finish does not take it from the others."""
    with _LOCK:
        _LISTINGS.append(path)
        _RUN["listing"] = path
    try:
        yield
    finally:
        with _LOCK:
            _LISTINGS.remove(path)
            _RUN["listing"] = _LISTINGS[-1] if _LISTINGS else None


def _read(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _write(path, got):
    from .durable import write_json
    if got:
        write_json(path, got)
    else:
        path.unlink(missing_ok=True)


def _created(pid):
    """When process `pid` started, as the system counts it; None where it
    is gone or cannot be asked."""
    try:
        import psutil
        return round(psutil.Process(int(pid)).create_time(), 2)
    except Exception:  # noqa: BLE001 - gone, or not ours to ask
        return None


def _list(proc):
    path = _RUN["listing"]
    if path is None:
        return None
    rec = {"host": socket.gethostname(), "program": proc.program,
           "since": time.time(), "created": _created(proc.pid)}
    if proc.container:
        rec["container"] = proc.container
    with _LOCK:
        got = _read(path)
        got[str(proc.pid)] = rec
        path.parent.mkdir(parents=True, exist_ok=True)
        _write(path, got)
    return path


def _unlist(path, pid):
    if path is None:
        return
    with _LOCK:
        got = _read(path)
        got.pop(str(pid), None)
        _write(path, got)


def still_running(path, log=None):
    """[(pid, program, since)] of the programs a run that is gone listed
    in `path` that still run, by the moment each started - a number taken
    by another program since is not the one listed. A container such a run
    left is removed here, said: the run that named it is gone, and it would
    write beside the next one. Another machine's listing counts: its
    processes cannot be asked about from here."""
    alive, got = [], _read(path)
    for pid, rec in list(got.items()):
        if rec.get("host") != socket.gethostname():
            alive.append((pid, rec.get("program"), rec.get("since")))
            continue
        if _same(pid, rec):
            alive.append((pid, rec.get("program"), rec.get("since")))
            continue
        name = rec.get("container")
        if name:
            # its client is gone, and it is not: the run that named it
            # cannot stop it any more
            _remove_container(name)
            if log:
                log("a container a stopped run left behind was removed"
                    f" ({name})")
        got.pop(pid)
    _write(path, got)
    return alive


def _same(pid, rec):
    """Whether process `pid` is still the one `rec` lists: its start time
    where the listing kept one, its program's name for a listing an older
    migkit wrote."""
    now = _created(pid)
    if now is None:
        return False
    if rec.get("created") is not None:
        return abs(now - float(rec["created"])) < 1.0
    try:
        import psutil
        return psutil.Process(int(pid)).name() == rec.get("program")
    except Exception:  # noqa: BLE001 - gone between the two questions
        return False


# ---- SIGTERM as ctrl-c while programs run -------------------------------------

_TERM = {"depth": 0, "before": None}


def _as_interrupt(signum, frame):
    raise KeyboardInterrupt


@contextmanager
def stopping():
    """While a program runs, `kill` (SIGTERM) is ctrl-c: the `finally`
    that stops what migkit started runs. Only the main thread can take a
    signal; elsewhere the main thread's handler is the one that counts."""
    main = threading.current_thread() is threading.main_thread()
    if main:
        if _TERM["depth"] == 0:
            try:
                _TERM["before"] = signal.signal(signal.SIGTERM,
                                                _as_interrupt)
            except ValueError:
                main = False
        if main:
            _TERM["depth"] += 1
    try:
        yield
    finally:
        if main:
            _TERM["depth"] -= 1
            if _TERM["depth"] == 0 and _TERM["before"] is not None:
                signal.signal(signal.SIGTERM, _TERM["before"])
                _TERM["before"] = None


# ---- starting and stopping ----------------------------------------------------

def _die_with_parent():
    """Linux only: the program is ended when the thread that started it
    goes, `kill -9` of migkit included (`PR_SET_PDEATHSIG`)."""
    if _PRCTL is not None:
        _PRCTL(1, signal.SIGTERM)


def _prctl():
    if not sys.platform.startswith("linux"):
        return None
    try:
        import ctypes
        return ctypes.CDLL(None, use_errno=True).prctl
    except (OSError, AttributeError):
        return None


_PRCTL = _prctl()


def environment(extra=None):
    """The environment every program starts with: migkit's program paths
    first, the switches that keep programs from calling their vendors, and
    `extra` over them."""
    return tool_env({k: str(v) for k, v in (extra or {}).items()
                     if v is not None})


class Proc:
    """A program started (`start`), stopped with everything it started."""

    def __init__(self, popen, argv, program, container=None, listed=None):
        self.popen, self.argv, self.program = popen, argv, program
        self.container, self.listed = container, listed
        #: what goes when it is done: a container's environment file
        self.closing = None
        self.pid = popen.pid

    def poll(self):
        return self.popen.poll()

    def wait(self, timeout=None):
        return self.popen.wait(timeout)

    @property
    def returncode(self):
        return self.popen.returncode

    def stop(self, grace=15):
        """Ask the program's whole group to end, then make it: SIGTERM,
        `grace` seconds, SIGKILL. A container is removed as well - its
        client's end does not end it."""
        if self.container:
            _remove_container(self.container)
        _signal_group(self.popen, signal.SIGTERM)
        try:
            self.popen.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            _signal_group(self.popen, signal.SIGKILL)
            try:
                self.popen.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                pass
        # what the program started and left behind in its group
        _signal_group(self.popen, signal.SIGKILL)

    def done(self):
        _unlist(self.listed, self.pid)
        self.listed = None
        if self.closing is not None:
            self.closing.close()
            self.closing = None

    def __enter__(self):
        return self

    def __exit__(self, kind, exc, tb):
        try:
            if kind is not None and self.popen.poll() is None:
                self.stop()
            elif kind is not None and self.container:
                _remove_container(self.container)
        finally:
            self.done()
        return False


def _signal_group(popen, sig):
    try:
        if os.name == "nt":
            if popen.poll() is None:
                popen.kill()
            return
        os.killpg(popen.pid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        if popen.poll() is None:
            try:
                popen.send_signal(sig)
            except OSError:
                pass


def start(argv, env=None, *, stdin=None, stdout=subprocess.PIPE,
          stderr=subprocess.STDOUT, cwd=None, text=True, container=None,
          secrets=(), detach=False, check_flags=True):
    """`argv` started in a session of its own with `environment(env)`, its
    command line in the run's record (secrets hidden) and listed while it
    runs: a `Proc`. A program this machine does not have is `Blocked`; one
    handed an option its build does not take is `Blocked` before it
    starts (`flags_refused`). `detach` leaves it to run past the thread
    that started it."""
    argv = quiet_argv([str(a) for a in argv])
    if check_flags:
        gone = flags_refused(argv)
        if gone:
            raise Blocked(gone)
    debug(argv, env, secrets)
    kw = {}
    if os.name == "nt":
        kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kw["start_new_session"] = True
        if (not detach and _PRCTL is not None
                and threading.current_thread() is threading.main_thread()):
            kw["preexec_fn"] = _die_with_parent
    try:
        popen = subprocess.Popen(
            argv, env=environment(env), stdin=stdin, stdout=stdout,
            stderr=stderr, cwd=cwd, text=text,
            **({"errors": "replace"} if text else {}), **kw)
    except FileNotFoundError:
        raise classify("missing", "") from None
    except PermissionError as e:
        raise Blocked(f"this way's component could not be started here"
                      f" ({e.strerror or type(e).__name__}) - migkit"
                      " doctor --install puts it in place") from None
    proc = Proc(popen, argv, os.path.basename(argv[0]), container)
    proc.listed = _list(proc)
    return proc


def flags_refused(argv):
    """Why the build about to run cannot take `argv`, or "": a long option
    a wrapped program (one migkit was measured with) is handed that its
    installed build does not list - asked of the build about to run, on
    the command line about to start, rather than a rehearsal of it. The
    program would stop at option parsing after migkit had begun."""
    from . import tools
    from .toolversion import options
    program = os.path.basename(str(argv[0])) if argv else ""
    facts = tools.PROGRAM.get(program)
    if facts is None or not facts.measured:
        return ""
    sub = _subcommand(argv)
    # the build that will run: the one the command line names where it
    # names a file, the one on migkit's path otherwise
    have = options(program, *sub, path=str(argv[0]) if os.sep in str(argv[0])
                   else None)
    if not have:
        return ""
    flags = {str(a).split("=", 1)[0] for a in argv[1:]
             if str(a).startswith("--")}
    gone = sorted(flags - have)
    if not gone:
        return ""
    from .toolversion import version
    return (f"the installed {facts.role} ({version(program) or 'unknown'})"
            f" does not take {', '.join(gone)}, which this way passes it,"
            " so it would stop at option parsing. Nothing has been started"
            " by it - migkit doctor --install puts a build that does in"
            " place")


_SUBCOMMAND = re.compile(r"[a-z][a-z0-9-]*")


def _subcommand(argv):
    """The subcommand words of one command line: the words after the
    program and before its first option (`copy table-data`, `stream
    sentinel set apply`), none where the first is an option or an
    address."""
    out = []
    for a in list(argv)[1:5]:
        if not _SUBCOMMAND.fullmatch(str(a)):
            break
        out.append(str(a))
    return tuple(out)


# ---- running to the end -------------------------------------------------------

#: lines of a program's output kept for a failure's words
TAIL = 60


class _Reader:
    """One stream of a program's output, read on a thread of its own: each
    line handed to `each`, and the whole kept or only its last `TAIL`
    lines."""

    def __init__(self, stream, each, whole):
        self.stream, self.each, self.whole = stream, each, whole
        self.lines = []
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            for line in self.stream:
                if self.each is not None:
                    try:
                        self.each(line)
                    except Exception:  # noqa: BLE001 - a reader's fault
                        pass
                self.lines.append(line)
                if not self.whole and len(self.lines) > 2 * TAIL:
                    del self.lines[:-TAIL]
        except (OSError, ValueError):
            pass

    def text(self):
        self.thread.join()
        return "".join(self.lines)


def run(argv, env=None, *, input=None, timeout=None, stall=None,
        progress=None, log=None, check=True, retries=0, cwd=None,
        merge=False, whole=None, what="", secrets=(), container=None,
        check_flags=True):
    """`argv` run to its end: `subprocess.CompletedProcess` with what it
    printed. Raised as `classify` says where it fails and `check`.

    * `progress(line)` reads its output as it comes and answers migkit's
      line or None; `log` is told each. A reader with a `failure()` says
      the words of a failure itself
    * `merge`: one stream (stdout and stderr together), which a program
      writing much to both cannot fill; `whole` keeps every line (the
      default without a progress reader: a caller reading its answer),
      else only the last few for a failure's words
    * `timeout` seconds at most; `stall` - seconds, True for the stall
      limit, or a `stall.Watch` fed by the caller - ends it once nothing
      has moved for that long: a progress reader's line is movement, and
      a caller watching something else feeds its `Watch`
    * `retries`: a failure while connecting is tried again, that many
      times - only for a program whose run writes nothing, or whose
      writes are replaced by key
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            got = _once(argv, env, input, timeout, stall, progress, log,
                        check, cwd, merge, whole, what, secrets, container,
                        check_flags)
        except Transient:
            if attempt > retries:
                raise
        else:
            # a caller that reads the return code itself is retried the
            # same where the failure was the connection's
            if not (got.returncode and attempt <= retries and _transient(
                    (got.stderr or "") + (got.stdout or ""))):
                return got
        time.sleep(0.8 * (2 ** (attempt - 1)))


def _watch(limit):
    """The stall watch a `stall` argument asks for, or None."""
    if not limit:
        return None
    if isinstance(limit, stall.Watch):
        return limit
    return stall.Watch(None if limit is True else limit)


def _ended(watch, ended):
    """Takes the watch off; where it ended the program, says so."""
    if watch is None:
        return
    try:
        watch.disarm()
    except stall.Stalled as e:
        ended["stalled"] = str(e)


def _once(argv, env, input, timeout, stall_limit, progress, log, check, cwd,
          merge, whole, what, secrets, container, check_flags):
    whole = (progress is None) if whole is None else whole
    watch = _watch(stall_limit)

    def each(line):
        got = progress(line) if progress is not None else None
        if got is not None and watch is not None:
            watch.moved()
        if got and log:
            log(got)
    proc = start(argv, env, stdin=subprocess.PIPE if input is not None
                 else subprocess.DEVNULL,
                 stderr=subprocess.STDOUT if merge else subprocess.PIPE,
                 cwd=cwd, container=container, secrets=secrets,
                 check_flags=check_flags)
    ended = {}
    with stopping(), proc:
        out = _Reader(proc.popen.stdout, each, whole)
        err = (None if merge else
               _Reader(proc.popen.stderr, each if progress else None,
                       whole))
        if input is not None:
            threading.Thread(target=_feed, args=(proc.popen, input),
                             daemon=True).start()
        if watch is not None:
            watch.arm(what or proc.program, proc.stop)
        try:
            _wait(proc, timeout, ended)
        finally:
            _ended(watch, ended)
        stdout = out.text()
        stderr = stdout if merge else err.text()
    text = stdout if merge else (stderr + stdout)
    _raise_if_ended(ended, what, timeout, text, secrets)
    rc = proc.returncode
    if rc and check:
        explain = getattr(progress, "failure", None)
        own = explain() if explain else None
        got = classify(rc, (own + "\n" if own else "") + text, what,
                       secrets)
        if own and type(got) is Failed:
            # a reader that parsed the program's log knows its error
            # messages, and which of its lines are tolerated
            raise Failed(f"{what}: {words(own)}" if what else words(own))
        raise got
    return subprocess.CompletedProcess(proc.argv, rc, stdout,
                                       "" if merge else stderr)


def _raise_if_ended(ended, what, timeout, text, secrets):
    if "stalled" in ended:
        raise stall.Stalled(ended["stalled"])
    if ended.get("timeout"):
        tail = said(text, secrets)
        raise Transient(f"{what or 'it'}: did not finish in {timeout:.0f}s"
                        " and was ended" + (f"; before it: {tail}" if tail
                                            else ""))


def _feed(popen, data):
    try:
        popen.stdin.write(data)
        popen.stdin.close()
    except (OSError, ValueError):
        pass


def _wait(proc, timeout, ended):
    """Wait for `proc`; past `timeout` seconds it is stopped."""
    limit = None if not timeout else time.monotonic() + float(timeout)
    while True:
        try:
            proc.popen.wait(timeout=0.5 if limit is None else max(
                0.05, min(0.5, limit - time.monotonic())))
            return
        except subprocess.TimeoutExpired:
            if limit is not None and time.monotonic() >= limit:
                ended["timeout"] = True
                proc.stop()
                return


# ---- two programs, one feeding the other --------------------------------------

def pipe(first, second, env=None, *, progress=None, log=None, what="",
         secrets=(), timeout=None, stall=None):
    """`first`'s output into `second`'s input, both started and stopped
    together, the stream between them never passing through migkit;
    `second`'s messages read as they come by `progress`, `first`'s kept as
    they come too - each on a thread of its own, so neither program fills
    a pipe while the other is read. (first's messages, second's)."""
    watch = _watch(stall)

    def each(line):
        got = progress(line) if progress is not None else None
        if got is not None and watch is not None:
            watch.moved()
        if got and log:
            log(got)
    ended = {}
    a = start(first, env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
              stderr=subprocess.PIPE, secrets=secrets)
    with stopping(), a:
        b = start(second, env, stdin=a.popen.stdout,
                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                  secrets=secrets)
        a.popen.stdout.close()
        with b:
            ra = _Reader(a.popen.stderr, None, False)
            rb = _Reader(b.popen.stderr, each, False)
            if watch is not None:
                watch.arm(what or b.program, lambda: (a.stop(), b.stop()))
            try:
                _wait(b, timeout, ended)
                _wait(a, 30, ended)
            finally:
                _ended(watch, ended)
            ea, eb = ra.text(), rb.text()
    _raise_if_ended(ended, what, timeout, ea + eb, secrets)
    if a.returncode or b.returncode:
        raise classify(a.returncode or b.returncode, ea + eb, what, secrets)
    return ea, eb


# ---- containers -------------------------------------------------------------------

_SEQ = itertools.count(1)


def container_name():
    """A name of the run's own for its next container: `migkit-<run>-<n>`."""
    from . import session
    run_id = re.sub(r"[^A-Za-z0-9_.-]", "-", session.current())
    return f"migkit-{run_id}-{next(_SEQ)}"


def _remove_container(name):
    try:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True, timeout=60, env=environment())
    except (OSError, subprocess.TimeoutExpired):
        pass


def _container_argv(image, args, stack, secrets, passthrough, mounts,
                    network, name, init):
    """The `docker run` command line of a container of the run's own name;
    its secrets in an environment file of this user's, held open on
    `stack` for as long as the caller needs it."""
    argv = ["docker", "run", "--rm", "--name", name]
    if init:
        argv.append("--init")
    if network:
        argv += ["--network", network]
    for src, dst in mounts:
        argv += ["-v", f"{src}:{dst}:ro"]
    if secrets:
        argv += ["--env-file", stack.enter_context(PrivateFile(
            "".join(f"{k}={v}\n" for k, v in secrets.items()), ".env"))]
    for k in passthrough or {}:
        argv += ["-e", k]
    return argv + [image, *[str(a) for a in args]]


def container(image, args, env=None, *, secrets=None, passthrough=None,
              mounts=(), network="host", name=None, progress=None,
              log=None, check=True, timeout=None, stall=None, what="",
              merge=True, whole=None, init=True):
    """`args` run in `image`, as `run` runs a program, in a container of
    the run's own name: removed when it ends however it ends - its
    client's end does not end it - and listed while it runs, so a run
    after a crash finds and removes it (`still_running`).

    `secrets` {NAME: value} reach it through an environment file of this
    user's, read by the docker client and gone when it ends; `passthrough`
    {NAME: value} - a value of more than one line, which that file cannot
    hold - through the client's own environment, named with `-e NAME`.
    Neither is ever on a command line."""
    from contextlib import ExitStack
    name = name or container_name()
    hidden = [*(secrets or {}).values(), *(passthrough or {}).values()]
    with ExitStack() as stack:
        argv = _container_argv(image, args, stack, secrets, passthrough,
                               mounts, network, name, init)
        try:
            return run(argv, {**(env or {}), **(passthrough or {})},
                       progress=progress, log=log, check=check,
                       timeout=timeout, stall=stall, what=what, merge=merge,
                       whole=whole, secrets=hidden, container=name,
                       check_flags=False)
        finally:
            _remove_container(name)


def start_container(image, args, env=None, *, secrets=None,
                    passthrough=None, mounts=(), network="host", name=None,
                    init=True, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT):
    """`container`'s container started and handed back (`Proc`), for a
    program that runs until migkit ends it: `Proc.stop` removes it, and
    its environment file goes when the `Proc` is done."""
    from contextlib import ExitStack
    name = name or container_name()
    stack = ExitStack()
    try:
        argv = _container_argv(image, args, stack, secrets, passthrough,
                               mounts, network, name, init)
        proc = start(argv, {**(env or {}), **(passthrough or {})},
                     stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                     container=name, check_flags=False,
                     secrets=[*(secrets or {}).values(),
                              *(passthrough or {}).values()])
    except BaseException:
        stack.close()
        raise
    proc.closing = stack
    return proc


# ---- a script on another machine ----------------------------------------------

def locked_script(script, lock):
    """`script` (a POSIX shell script) wrapped to run on another machine
    under `lock` - a lock file with a pidfile beside it: a second run of
    it waits for none and starts nothing where the first still runs, and
    what the first left running is ended before the second begins. Said on
    its standard error as `migkit-lock: <why>` and exit 75."""
    return (
        f'L={_shq(lock)}; P="$L.pid"\n'
        'exec 9>>"$L"\n'
        'if command -v flock >/dev/null 2>&1; then\n'
        '  flock -n 9 || { echo "migkit-lock: another run holds it" >&2;'
        ' exit 75; }\n'
        'fi\n'
        'if [ -s "$P" ] && kill -0 "$(cat "$P")" 2>/dev/null; then\n'
        '  echo "migkit-lock: an earlier run is still going" >&2; exit 75\n'
        'fi\n'
        'echo $$ > "$P"\n'
        'trap \'rm -f "$P"\' EXIT\n'
        + script)


def _shq(text):
    return "'" + str(text).replace("'", "'\\''") + "'"
