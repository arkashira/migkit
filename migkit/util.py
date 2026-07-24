import shutil
import subprocess
import time

from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent
TOOL_PATHS = [str(_BASE / ".venv-tools" / "bin"), str(_BASE / ".venv" / "bin"),
              "/opt/homebrew/opt/libpq/bin", "/opt/homebrew/opt/mysql-client/bin",
              "/opt/homebrew/bin", "/usr/local/bin"]


def tool_env(extra=None):
    import os
    env = dict(os.environ)
    env["PATH"] = ":".join(TOOL_PATHS) + ":" + env.get("PATH", "")
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


def run(cmd, env=None, input=None, timeout=None, check=True, retries=3):
    """Run a command, retrying only transient connection failures with
    backoff. check=False still returns the (failed) process for callers
    that read returncode themselves (e.g. atlas diff = non-zero on diff);
    a transient failure is retried regardless of check."""
    attempt = 0
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
