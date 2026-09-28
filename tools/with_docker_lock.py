"""Run a command while holding the machine's one docker-test lock.

Several checkouts of migkit test against the same docker daemon with the
same container names and ports; two of them at once collide and, on a
small machine, exhaust its memory. `python tools/with_docker_lock.py
<command...>` waits for the lock at /tmp/migkit-docker.lock, checks the
memory has room, runs the command, and lets the next one in.
"""
import fcntl
import os
import subprocess
import sys
import time

LOCK = "/tmp/migkit-docker.lock"


def _free_percent():
    try:
        out = subprocess.run(["memory_pressure"], capture_output=True,
                             text=True, timeout=20).stdout
        for line in out.splitlines():
            if "free percentage" in line:
                return int(line.rsplit(":", 1)[1].strip().rstrip("%"))
    except Exception:  # noqa: BLE001 - not macOS, or no reading
        return None
    return None


def main(argv):
    if not argv:
        raise SystemExit("usage: with_docker_lock.py <command> [args...]")
    with open(LOCK, "a+") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        waited = 0
        while (_free_percent() or 100) < 12 and waited < 600:
            time.sleep(15)
            waited += 15
        return subprocess.call(argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
