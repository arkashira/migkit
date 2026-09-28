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
)


def is_transient(err):
    e = str(err).lower()
    return any(p in e for p in TRANSIENT)


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


def with_retry(fn, tries=4, base=0.8, label="", log=None):
    """Run fn(), retrying transient connection failures with exponential
    backoff. Permanent errors (auth, syntax, constraint) raise at once."""
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 - classify by message
            last = e
            if i == tries - 1 or not is_transient(e):
                raise
            delay = base * (2 ** i)
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
    the output.
    """
    if not secret:
        return text
    return str(text).replace(str(secret), "****")


def run(cmd, env=None, input=None, timeout=None, check=True, retries=3):
    """Run a command, retrying only transient connection failures with
    backoff. check=False still returns the (failed) process for callers
    that read returncode themselves (e.g. atlas diff = non-zero on diff);
    a transient failure is retried regardless of check."""
    attempt = 0
    cmd = quiet_argv(cmd)
    while True:
        attempt += 1
        p = subprocess.run(
            cmd, shell=isinstance(cmd, str), capture_output=True, text=True,
            env=tool_env(env), input=input, timeout=timeout,
        )
        if p.returncode == 0:
            return p
        if attempt <= retries and is_transient(p.stderr):
            time.sleep(0.8 * (2 ** (attempt - 1)))
            continue
        if check:
            raise RuntimeError(
                f"command failed rc={p.returncode}: "
                f"{cmd if isinstance(cmd, str) else ' '.join(map(str, cmd))}"
                f"\n{p.stderr.strip()}")
        return p


def human_int(n):
    return f"{n:,}"


def human_secs(s):
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{s % 3600 // 60:02d}m"


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
