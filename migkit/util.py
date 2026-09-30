import shutil
import subprocess
import sys
import sysconfig
import time

from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent


def _own_script_dirs():
    """Where console scripts installed alongside migkit live - a venv's bin,
    a container's /usr/local/bin, wherever. sys.executable is NOT resolved:
    in a venv it is a symlink to the base interpreter, and following it would
    point at the wrong bin directory."""
    dirs = [sysconfig.get_path("scripts"), str(Path(sys.executable).parent)]
    seen, out = set(), []
    for d in dirs:
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    return out


# migkit's own environment first, then a source checkout's venv (developing in
# place), then the paths Homebrew keeps off the default PATH.
TOOL_PATHS = _own_script_dirs() + [
    str(_BASE / ".venv" / "bin"),
    "/opt/homebrew/opt/libpq/bin", "/opt/homebrew/opt/mysql-client/bin",
    "/opt/homebrew/bin", "/usr/local/bin"]


#: what keeps a program migkit runs from calling its vendor while it runs:
#: the operator never agreed to a connection that leaves the machine with
#: the versions of their servers in it. Measured on 2026-09-28 through a
#: logging proxy and an empty home: the schema differ opened
#: `vercheck.ariga.io:443` on `version` and on `schema diff`, the other
#: differ (4.33) opened `config.liquibase.com:443` on `diff`; with these
#: set, neither opened anything. The online MongoDB sync is told in its
#: own configuration file (`disableTelemetry`), which is where it reads it
NO_CALL_HOME_ENV = {"ATLAS_NO_UPDATE_NOTIFIER": "true",
                    "ATLAS_NO_ANON_TELEMETRY": "true",
                    "ATLAS_NO_UPGRADE_SUGGESTIONS": "true",
                    "LIQUIBASE_ANALYTICS_ENABLED": "false",
                    "DO_NOT_TRACK": "1"}

#: the same for programs that read it from their command line only, by
#: the start of the program's name: every Percona tool checks for updates
#: by default, sending the versions of the operating system, Perl, MySQL
#: and its driver to `v.percona.com` once a day, and prints what it hears
#: on standard output - ahead of the statements a repair reads from it
NO_CALL_HOME_ARGS = {"pt-": ("--no-version-check",)}


def quiet_argv(cmd):
    """`cmd` with the switches that keep its program from calling home,
    right after the program's name; unchanged for a program that has
    none, or a shell string."""
    if isinstance(cmd, str) or not cmd:
        return cmd
    name = str(cmd[0]).rsplit("/", 1)[-1]
    for start, switches in NO_CALL_HOME_ARGS.items():
        if name.startswith(start):
            add = [s for s in switches if s not in cmd]
            return [cmd[0], *add, *cmd[1:]]
    return cmd


def tool_env(extra=None):
    import os
    env = dict(os.environ)
    env["PATH"] = ":".join(TOOL_PATHS) + ":" + env.get("PATH", "")
    env.update(NO_CALL_HOME_ENV)
    if extra:
        env.update(extra)
    return env


def which(name):
    return shutil.which(name, path=tool_env()["PATH"])


# transient failures that a retry will usually clear: TLS handshake races,
# dropped/reset sockets, pooler hiccups, cross-region blips. NOT auth/syntax/
# constraint errors (those are permanent and must surface immediately).
TRANSIENT = (
    "wrong version number", "lost connection", "connection reset",
    "connection refused", "could not connect", "can't connect",
    "server closed the connection", "broken pipe", "gone away",
    "eof occurred", "timeout expired", "connection timed out",
    "temporary failure", "too many connections", "operationalerror",
    "no route to host", "connection aborted", "reset by peer",
    # a driver's own wait for the server running out: measured, the
    # MongoDB driver's `...:27017: timed out` was read as permanent
    "timed out",
)

#: of those, the ones raised while connecting, before anything was sent:
#: the only failures a statement that writes may be run again after. A
#: connection lost during a write may have applied it - measured, a MySQL
#: DDL script resent after `Lost connection` re-applied its first half
#: (DDL commits as it goes) and failed on `Duplicate column`, hiding the
#: cause (audit 2026-10-01, W11)
CONNECTING = (
    "wrong version number", "connection refused", "could not connect",
    "can't connect", "timeout expired", "too many connections",
    "temporary failure", "no route to host",
    "reading initial communication packet", "reading authorization packet",
    "the database system is starting up",
)


def is_transient(err):
    e = str(err).lower()
    return any(p in e for p in TRANSIENT)


def retryable(err, writes=False):
    """Whether a failure may be tried again: a transient one, and where
    what failed writes, only one while connecting."""
    if not is_transient(err):
        return False
    e = str(err).lower()
    return not writes or any(p in e for p in CONNECTING)


def keepalive(sock, idle=60, interval=10, count=5):
    """Make a dead peer show up as an error instead of an endless wait.

    A long scan over a cross-cloud link can sit on a socket for many minutes.
    If the far end or the tunnel goes away in the meantime nothing is ever
    sent, so the OS never notices and the process waits for good. Probing
    while idle turns that into a normal connection error the retry can handle.
    """
    if sock is None:
        return
    import socket as _s
    try:
        sock.setsockopt(_s.SOL_SOCKET, _s.SO_KEEPALIVE, 1)
        for name, val in (("TCP_KEEPIDLE", idle), ("TCP_KEEPALIVE", idle),
                          ("TCP_KEEPINTVL", interval), ("TCP_KEEPCNT", count)):
            opt = getattr(_s, name, None)
            if opt is not None:
                sock.setsockopt(_s.IPPROTO_TCP, opt, val)
    except OSError:
        pass


def _backoff(attempt, base=0.8):
    """Seconds before the next try after `attempt` (from 0) failed."""
    return base * (2 ** attempt)


def with_retry(fn, tries=4, base=0.8, label="", log=None, writes=False):
    """Run fn(), retrying transient connection failures with exponential
    backoff. Permanent errors (auth, syntax, constraint) raise at once.
    `writes`: fn changes something, so only a failure while connecting is
    tried again (`retryable`). The one retry policy: a program `run`
    starts is retried by it too."""
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 - classify by message
            last = e
            if i == tries - 1 or not retryable(e, writes):
                raise
            delay = _backoff(i, base)
            if log:
                log(f"{label or 'op'}: transient error, retry"
                    f" {i + 1}/{tries - 1} in {delay:.1f}s"
                    f" ({str(e).splitlines()[-1][:60]})")
            time.sleep(delay)
    raise last


def without_secret(text, secret):
    """`text` with `secret` hidden, and unchanged when there is none.

    The guard is the whole point. `str.replace("", "****")` inserts the
    mask between every character, so a hop authenticating by `trust` or
    `.pgpass` - an empty password, and the safer arrangement - turned the
    plan it was about to run into `****c****r****e****a****t****e****`.
    The more careful the operator's setup, the less readable migkit made
    the output. Hidden by `redact.Redactor`: in each form it reaches text
    in, and with every secret found by its shape besides.
    """
    from .redact import Redactor
    return Redactor(secrets=(secret,), mask="****")(text)


class CommandFailed(RuntimeError):
    """A program `run` started that failed. Its message is the program's
    own error with every secret hidden; the command line is kept on the
    exception (`argv`) for the debug log and never put in the message -
    it carried hosts, users and the SQL, and a traceback printed it."""

    def __init__(self, returncode, stderr, argv):
        from .redact import Redactor
        said = Redactor()(str(stderr or "").strip())
        super().__init__(f"the command failed (exit {returncode})"
                         + (f"\n{said}" if said else ""))
        self.returncode, self.stderr, self.argv = returncode, stderr, argv


def run(cmd, env=None, input=None, timeout=None, check=True, retries=3,
        writes=False):
    """Run a command, retrying only transient connection failures with
    backoff - the policy and the waits `with_retry` has (`writes`: only a
    failure while connecting). A command that runs out of `timeout` is
    not run again. check=False still returns the (failed) process for
    callers that read returncode themselves (e.g. atlas diff = non-zero on
    diff); a transient failure is retried regardless of check."""
    cmd = quiet_argv(cmd)
    for i in range(retries + 1):
        p = subprocess.run(
            cmd, shell=isinstance(cmd, str), capture_output=True, text=True,
            env=tool_env(env), input=input, timeout=timeout,
        )
        if not p.returncode or i == retries \
                or not retryable(p.stderr, writes):
            break
        time.sleep(_backoff(i))
    if p.returncode and check:
        raise CommandFailed(p.returncode, p.stderr, cmd)
    return p


def human_int(n):
    return f"{n:,}"


def human_secs(s):
    """`wording._duration`, the one way a duration is said: five
    formatters said 59.6 s as `1m00s`, `59s`, `60 seconds`, `60s` and
    `60 seconds` (audit 2026-10-01, DU10)."""
    from .wording import _duration
    return _duration(s)


def stamp(when=None):
    """A time as every record says it: UTC, ISO 8601, with its zone -
    `2026-10-01T09:30:00+00:00`. Records said `at` seven ways, almost all
    in the machine's local time with no zone (audit 2026-10-01, DU21), so
    two machines' records of one run did not sort. `when` is seconds
    since the epoch, now where not given."""
    from datetime import datetime, timezone
    at = datetime.now(timezone.utc) if when is None else \
        datetime.fromtimestamp(float(when), timezone.utc)
    return at.isoformat(timespec="seconds")


class Timer:
    def __init__(self):
        self.t0 = time.monotonic()

    def elapsed(self):
        return time.monotonic() - self.t0

    def eta(self, done, total):
        if not done or not total:
            return "?"
        return human_secs(self.elapsed() / done * (total - done))


class PrivateFile:
    """A file readable by this user only, for as long as a program needs
    it, then gone: where a secret goes instead of a command line, which
    every process listing on the machine can read."""

    def __init__(self, text, suffix=""):
        self.text, self.suffix, self.path = text, suffix, None

    def __enter__(self):
        import os
        import tempfile
        fd, self.path = tempfile.mkstemp(prefix="migkit-", suffix=self.suffix)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(self.text)
        return self.path

    def __exit__(self, *exc):
        import os
        try:
            os.unlink(self.path)
        except OSError:
            pass
        return False


def diff_run_config(url1, table1, url2, table2):
    """The comparison program's run, as its own configuration file reads it
    (`--conf`), so neither address - passwords and all - is on its command
    line."""
    import json
    return ("[run.default]\n"
            f"1 = {{database = {json.dumps(url1)}, table = {json.dumps(table1)}}}\n"
            f"2 = {{database = {json.dumps(url2)}, table = {json.dumps(table2)}}}\n")


# ---- one table name, split and quoted one way -------------------------------

#: each engine's quotes for a name, where they are not `"`
QUOTES = {"mysql": ("`", "`"), "tidb": ("`", "`"), "clickhouse": ("`", "`"),
          "mssql": ("[", "]"), "ase": ("[", "]")}

#: engines whose tables sit straight in a database: a name has no schema
#: part, and a dot in it is the name's own (a MongoDB collection
#: `system.profile`, a topic `orders.v1`)
NO_SCHEMA = ("mysql", "tidb", "clickhouse", "mongodb", "cosmosdb", "redis",
             "kafka", "kinesis", "pubsub", "dynamodb", "cassandra",
             "opensearch")


def _canonical(engine):
    from .engines import ALIASES
    return ALIASES.get(engine, engine)


def _parts(engine, name):
    """The parts of a name, a quoted one unquoted whole - a dot inside
    quotes is the name's own - and, where the engine has schemas, the
    unquoted text split at its dots."""
    opens = {'"': '"'}
    q = QUOTES.get(engine)
    if q:
        opens[q[0]] = q[1]
    flat = engine in NO_SCHEMA
    parts, cur, quoted = [], "", False
    s, i = str(name), 0
    while i < len(s):
        ch = s[i]
        if ch in opens and not cur and not quoted:
            close, j, got = opens[ch], i + 1, ""
            while j < len(s):
                if s[j] == close:
                    if s[j + 1:j + 2] == close:
                        got += close
                        j += 2
                        continue
                    break
                got += s[j]
                j += 1
            if j < len(s):
                cur, quoted, i = got, True, j + 1
                continue
            # never closed: not a quoted name after all
        if ch == "." and (quoted or not flat):
            parts.append(cur)
            cur, quoted = "", False
            i += 1
            continue
        cur += ch
        i += 1
    parts.append(cur)
    return parts


def split_table(engine, name, default=""):
    """(schema, table) of a table's name, plain (`public.orders`) or
    quoted (`public."v1.orders"`, `` `a.b` ``, `[dbo].[x]`), one way for
    every caller. Five ways split it before (audit 2026-10-01, DU14): at
    the first dot, at the last, at every one, the last part only -
    measured with `public."v1.orders"`, the copy left out a table the
    check compared, and a digest named `"public"."v1"."orders"`.

    A dot inside quotes is the name's; unquoted, the first dot ends the
    schema, since a table's name may hold dots and a schema's rarely
    does. An engine with no schemas (`NO_SCHEMA`) keeps an unquoted dot
    as the name's own, and its first part is the database a quoted name
    carries. A name with no schema has `default`."""
    parts = _parts(_canonical(engine), name)
    if len(parts) == 1:
        return default, parts[0]
    return parts[0], ".".join(parts[1:])


def quote_name(engine, name):
    """One name quoted as `engine` quotes it, its quote doubled inside."""
    left, right = QUOTES.get(_canonical(engine), ('"', '"'))
    return left + str(name).replace(right, right + right) + right


def qualify(engine, schema, table):
    """A table's name as the engine's SQL reads it: `"public"."v1.orders"`,
    `` `orders` ``, `[dbo].[orders]` - the schema left out where there
    is none."""
    return ".".join(quote_name(engine, p) for p in (schema, table) if p)


# ---- what a deep check read, and what it could not --------------------------

class Probes:
    """What a check planned to read and what it could not, turned into the
    result's status and words (audit 2026-10-01, LG2).

    A deep check reads one column, one shard, one statistic at a time, and
    a read that failed used to be dropped with a `continue` while the
    result still said `ok`: MongoDB's duplicate-`_id` probe failing read
    "sharded: no dup _id", and MySQL's character-set check over the first
    30 columns said ok over a table of 45. Here each planned read is
    `done` or `failed`; `verdict` makes an `ok` over a part a `warn`
    ("compared 12 of 30 columns; not read: a.b (timeout)") and over
    nothing a `skip`, and gives any other status the same words."""

    def __init__(self, what="columns", planned=()):
        self.what = what
        self.planned = []
        self.read = []
        self.missed = {}
        self.plan(*planned)

    def plan(self, *names):
        for n in names:
            if str(n) not in self.planned:
                self.planned.append(str(n))
        return self

    def done(self, name):
        name = str(name)
        self.plan(name)
        self.missed.pop(name, None)
        if name not in self.read:
            self.read.append(name)

    def failed(self, name, why):
        name = str(name)
        self.plan(name)
        text = str(why).strip().splitlines()
        self.missed[name] = (text[-1] if text
                             else type(why).__name__)[:120]
        if name in self.read:
            self.read.remove(name)

    def reading(self, name):
        """`with probes.reading("a.b"):` - done when the block ends, failed
        with what it raised, which goes no further."""
        return _Reading(self, name)

    def unread(self):
        """{name: why} of every planned read that did not happen: failed,
        or never reached."""
        out = dict(self.missed)
        for n in self.planned:
            if n not in self.read and n not in out:
                out[n] = "not reached"
        return out

    def words(self):
        """"compared 12 of 30 columns; not read: a.b (timeout)", or ""
        where everything planned was read."""
        unread = self.unread()
        if not unread:
            return ""
        shown = [f"{n} ({w})" for n, w in list(unread.items())[:5]]
        more = len(unread) - len(shown)
        return (f"compared {len(self.read)} of {len(self.planned)}"
                f" {self.what}; not read: {', '.join(shown)}"
                + (f" and {more} more" if more > 0 else ""))

    def verdict(self, status, detail=""):
        """(status, detail) with what was not read counted: an `ok` over a
        part is `warn`, over nothing `skip`; any other status stands, its
        detail given the same words."""
        said = self.words()
        if not said:
            return status, detail
        if status == "ok":
            status = "warn" if self.read else "skip"
        return status, "; ".join(p for p in (detail, said) if p)


class _Reading:
    def __init__(self, probes, name):
        self.probes, self.name = probes, name

    def __enter__(self):
        self.probes.plan(self.name)
        return self

    def __exit__(self, kind, err, tb):
        if kind is None:
            self.probes.done(self.name)
            return False
        if not issubclass(kind, Exception):
            return False
        self.probes.failed(self.name, err)
        return True
