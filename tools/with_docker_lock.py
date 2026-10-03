"""Run a command while holding its share of the machine's docker.

Several checkouts of migkit test against the same docker daemons; two runs
of files that fix the same host port or container name collide, and on a
small machine too many at once exhaust its memory. `python
tools/with_docker_lock.py <command...>` gives the command a lane of its
own and waits only for what it would actually share:

* a lane (`MIGKIT_TEST_LANE`, 0-9): the shared servers of `tests/servers.py`
  take their ports and container names from it, so two commands in two
  lanes never meet. The default profile has lanes 1-4, the migkit VM 5-9;
  lane 0 is for files that name the PostgreSQL pair's fixed port or name,
  which only lane 0 has.
* the host ports and container names the command's test files fix
  themselves (`tests/servers.py:scan`): a file another command is running,
  or one fixing a port it fixes, waits for it; any other runs beside it.
* memory: a start waits while the machine has too little free.

A command whose test files cannot be read from its arguments (a directory,
a script) takes the whole of docker, as every command did before: it waits
for the rest to end and holds the others off until it ends
(`exclusive()`, which `tools/final_suite.py` takes for the final run).

`--vm migkit` runs the command against a second colima profile of its
own (aarch64, 6 GB, Rosetta on, for Oracle Free and the x86-only
engines): the profile is started when the first command needs it and
memory allows, the command sees it through `DOCKER_CONTEXT=colima-migkit`,
and it is stopped when the last command using it ends. The default
profile, and whatever another session runs on it, is never touched.
"""
import contextlib
import fcntl
import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

LOCK = "/tmp/migkit-docker.lock"
#: a waiting whole-docker command holds this, so new lanes wait behind it
#: instead of starving it
INTENT = "/tmp/migkit-docker.intent.lock"
LOCKS = Path("/tmp/migkit-docker.d")
#: the profile for the engines the default one cannot hold
VM = {"migkit": ["--arch", "aarch64", "--cpu", "4", "--memory", "6",
                 "--disk", "40", "--vm-type", "vz", "--vz-rosetta"]}
#: free memory a start needs, in percent
ROOM = {None: 12, "migkit": 30}
#: the lanes each daemon hands out, in the order tried (lane 0 last: the
#: pinned files wait for it)
LANES = {None: [1, 2, 3, 4, 0], "migkit": [5, 6, 7, 8, 9]}
#: more test files than this in one command take the whole of docker
MOST_FILES = 60


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


def home_context():
    """The context this machine's own daemon answers on: `colima` where
    colima runs it, None (docker's own default) elsewhere."""
    before = _context()
    if before in (None, "default") or str(before).startswith("colima-"):
        # what another start or stop left behind, not what this machine
        # runs on
        return "colima" if _has_colima() else None
    return before


def _has_colima():
    try:
        return subprocess.run(["colima", "version"], capture_output=True,
                              timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


# ---- locks ------------------------------------------------------------------

@contextlib.contextmanager
def _flock(path, mode, block=True):
    """`mode` fcntl.LOCK_SH or LOCK_EX; yields whether it was taken."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fh = open(path, "a+")
    try:
        try:
            fcntl.flock(fh, mode | (0 if block else fcntl.LOCK_NB))
            took = True
        except BlockingIOError:
            took = False
        yield took
    finally:
        fh.close()


@contextlib.contextmanager
def exclusive():
    """The whole of docker: every lane ended, none started until this
    ends."""
    with _flock(INTENT, fcntl.LOCK_EX), _flock(LOCK, fcntl.LOCK_EX):
        yield


@contextlib.contextmanager
def shared(lanes, keys):
    """A lane from `lanes` (the first free, waiting for one) and every
    resource in `keys`, taken in one order so no two commands wait on
    each other. Yields the lane."""
    with _flock(INTENT, fcntl.LOCK_SH):
        pass    # wait behind a whole-docker command that is waiting
    with contextlib.ExitStack() as stack:
        stack.enter_context(_flock(LOCK, fcntl.LOCK_SH))
        lane = None
        while lane is None:
            for n in lanes:
                cm = _flock(LOCKS / f"lane-{n}.lock", fcntl.LOCK_EX,
                            block=False)
                if cm.__enter__():
                    stack.callback(cm.__exit__, None, None, None)
                    lane = n
                    break
                cm.__exit__(None, None, None)
            else:
                time.sleep(2)
        for key in sorted(keys):
            digest = hashlib.sha1(key.encode()).hexdigest()[:16]
            stack.enter_context(_flock(LOCKS / f"res-{digest}.lock",
                                       fcntl.LOCK_EX))
        yield lane


# ---- the VM profile ---------------------------------------------------------

def _vm_paths(vm):
    return (LOCKS / f"vm-{vm}.lock", LOCKS / f"vm-{vm}.users",
            LOCKS / f"vm-{vm}.started")


@contextlib.contextmanager
def vm_profile(vm):
    """The colima profile `vm`, running for as long as the block runs:
    started by the first user that needs it, stopped by the last one
    out - and only if one of them started it."""
    state, users, started = _vm_paths(vm)
    before = home_context()
    with contextlib.ExitStack() as stack:
        with _flock(state, fcntl.LOCK_EX):
            if not _running(vm):
                _wait_for_room(ROOM[vm])
                try:
                    subprocess.run(["colima", "start", "-p", vm] + VM[vm],
                                   check=True)
                finally:
                    _restore(before)
                started.parent.mkdir(parents=True, exist_ok=True)
                started.write_text(str(os.getpid()))
            stack.enter_context(_flock(users, fcntl.LOCK_SH))
        try:
            yield f"colima-{vm}"
        finally:
            stack.close()
            with _flock(state, fcntl.LOCK_EX), \
                    _flock(users, fcntl.LOCK_EX, block=False) as last:
                if last and started.exists():
                    subprocess.run(["colima", "stop", "-p", vm])
                    started.unlink(missing_ok=True)
                    _restore(before)


# ---- what a command's tests hold --------------------------------------------

def test_files(argv, cwd=None):
    """The test files `argv` runs (paths and node ids), or None where it
    cannot be told (a directory, no test file named, too many)."""
    cwd = Path(cwd or os.getcwd())
    found = []
    for a in argv:
        path = a.split("::", 1)[0]
        if not path.endswith(".py") and "/" not in path:
            continue
        p = Path(path) if Path(path).is_absolute() else cwd / path
        if p.is_dir():
            return None
        if p.is_file() and p.name.startswith("test_") and p.suffix == ".py":
            found.append(p)
    if not found or len(found) > MOST_FILES:
        return None
    return found


def holds(files):
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    try:
        from tests import servers
    finally:
        sys.path.pop(0)
    keys = set()
    for f in files:
        keys |= servers.scan(f).keys()
    return keys


def main(argv):
    vm = None
    if argv[:1] == ["--vm"]:
        vm, argv = argv[1], argv[2:]
        if vm not in VM:
            raise SystemExit(f"--vm {vm}: not one of {', '.join(VM)}")
    if not argv:
        raise SystemExit("usage: with_docker_lock.py [--vm migkit]"
                         " <command> [args...]")
    env = dict(os.environ)
    files = test_files(argv)
    with contextlib.ExitStack() as stack:
        if files is None:
            stack.enter_context(exclusive())
            lane = 0
        else:
            keys = holds(files)
            lanes = [0] if "lane0" in keys else LANES[vm]
            lane = stack.enter_context(shared(lanes, keys - {"lane0"}))
        env["MIGKIT_TEST_LANE"] = str(lane)
        _wait_for_room(ROOM[None])
        if vm:
            env["DOCKER_CONTEXT"] = stack.enter_context(vm_profile(vm))
        elif "DOCKER_CONTEXT" not in env:
            home = home_context()
            if home and home != _context():
                # a context another start left behind is not this
                # machine's default daemon; the tests talk to colima's
                env["DOCKER_CONTEXT"] = home
        return subprocess.call(argv, env=env)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
