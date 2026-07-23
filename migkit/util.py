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


def run(cmd, env=None, input=None, timeout=None, check=True):
    p = subprocess.run(
        cmd, shell=isinstance(cmd, str), capture_output=True, text=True,
        env=tool_env(env), input=input, timeout=timeout,
    )
    if check and p.returncode != 0:
        raise RuntimeError(f"command failed rc={p.returncode}: "
                           f"{cmd if isinstance(cmd, str) else ' '.join(cmd)}\n{p.stderr.strip()}")
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
