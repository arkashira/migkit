"""Run a command while holding the machine's one docker-test lock.

Several checkouts of migkit test against the same docker daemon with the
same container names and ports; two of them at once collide and, on a
small machine, exhaust its memory. `python tools/with_docker_lock.py
<command...>` waits for the lock at /tmp/migkit-docker.lock, checks the
memory has room, runs the command, and lets the next one in.

`--vm migkit` runs the command against a second colima profile of its
own (aarch64, 6 GB, Rosetta on, for Oracle Free and the x86-only
engines): the profile is started when the command needs it and memory
allows, the command sees it through `DOCKER_CONTEXT=colima-migkit`, and
the profile is stopped afterwards. The default profile, and whatever
another session runs on it, is never touched.
"""
import fcntl
import os
import subprocess
import sys
import time

LOCK = "/tmp/migkit-docker.lock"
#: the profile for the engines the default one cannot hold
VM = {"migkit": ["--arch", "aarch64", "--cpu", "4", "--memory", "6",
                 "--disk", "40", "--vm-type", "vz", "--vz-rosetta"]}
#: free memory a start needs, in percent
ROOM = {None: 12, "migkit": 30}


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


def _wait_for_room(need, most=900):
    waited = 0
    while (_free_percent() or 100) < need and waited < most:
        time.sleep(15)
        waited += 15
    if (_free_percent() or 100) < need:
        raise SystemExit(f"memory stayed under {need}% free for {most}s;"
                         " not starting")


def _context():
    got = subprocess.run(["docker", "context", "show"], capture_output=True,
                         text=True)
    return got.stdout.strip() if got.returncode == 0 else None


def _restore(context):
    """Starting or stopping a colima profile switches the machine's
    current docker context (to the new profile, then to a `default`
    with no daemon behind it) - which every other session on the
    machine then talks to. Measured: the other sessions' docker calls
    failed until it was switched back. Put back what was current."""
    if context and _context() != context:
        subprocess.run(["docker", "context", "use", context],
                       capture_output=True)


def _running(profile):
    got = subprocess.run(["colima", "status", "-p", profile],
                         capture_output=True, text=True)
    return got.returncode == 0


def main(argv):
    vm = None
    if argv[:1] == ["--vm"]:
        vm, argv = argv[1], argv[2:]
        if vm not in VM:
            raise SystemExit(f"--vm {vm}: not one of {', '.join(VM)}")
    if not argv:
        raise SystemExit("usage: with_docker_lock.py [--vm migkit]"
                         " <command> [args...]")
    with open(LOCK, "a+") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        _wait_for_room(ROOM[vm])
        env = dict(os.environ)
        started = False
        before = _context()
        if before in (None, "default") or str(before).startswith("colima-"):
            # what another start or stop left behind, not what this
            # machine runs on
            before = "colima"
        if not vm and before != "colima" and before is not None \
                and "DOCKER_CONTEXT" not in env:
            # a context another start left behind is not this machine's
            # default daemon; the tests talk to colima's
            env["DOCKER_CONTEXT"] = "colima"
        if vm:
            if not _running(vm):
                try:
                    subprocess.run(["colima", "start", "-p", vm] + VM[vm],
                                   check=True)
                finally:
                    _restore(before)
                started = True
            env["DOCKER_CONTEXT"] = f"colima-{vm}"
        try:
            return subprocess.call(argv, env=env)
        finally:
            if started:
                subprocess.run(["colima", "stop", "-p", vm])
                _restore(before)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
