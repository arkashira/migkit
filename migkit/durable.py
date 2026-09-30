"""State that survives a crash and a power loss, written one way.

What migkit keeps between runs - where a copy or a tail stands, the undo
of a change it made, the state of work in progress - used to be written
some twenty ways, with three fsyncs in the whole tree and none of a
directory (audit 2026-10-01, ST1). A process killed mid-write left the
old file; a machine that lost power could leave an empty one, or a newer
file beside an older one it was written after, and each reader met a
torn file its own way: a traceback, a silent start over, `{}` (ST9).

So there is one writer and one reader:

* `write_atomic` - the whole file or none of it, and once it returns the
  file is on the disk: a temporary file of its own (`O_EXCL`, the file's
  mode from the first byte, never chmodded after), fsynced, renamed over
  the old one, and the directory fsynced so the rename is kept too.
  Measured in the audit on an Apple SSD, per write: in place 0.24 ms,
  rename alone 0.50 ms, this 0.39 ms, with `F_FULLFSYNC` (`full=True`,
  macOS's flush through the drive's own cache) 3.9 ms - less than the
  tail already spent a batch.
* `append_line` - one line to a journal, a torn last line (the one a
  crash cut) taken off first, so the next line never glues onto it.
* `read_state(path, kind)` - one policy for a torn file per kind of
  file: a position, a journal or the state of work in progress stops,
  naming the file, since a guess there skips or repeats changes, or
  undoes nothing; a cache reads as absent, since it can be asked again.
* `private_dir` - a directory only this user can read: what migkit keeps
  holds values and secrets.
"""
import errno
import json
import os
import secrets
from pathlib import Path

#: what a torn or unreadable file of each kind is read as: "stop" raises
#: `Torn`, naming the file; "absent" reads as a file that is not there
KINDS = {
    # where a copy, tail or stream stands: read wrong, changes are skipped
    # or applied twice
    "position": "stop",
    # what undoing a change needs: read as empty, the undo undoes nothing
    "journal": "stop",
    # the state of work in progress - table states, freeze and cutover
    # records, the rung a table climbed: read as empty, work is redone
    # or left half done
    "state": "stop",
    # what can be found again by asking: read as not there
    "cache": "absent",
}

#: what each kind holds and what removing it does, for the message
_HOLDS = {
    "position": ("where a copy or a follow of changes stands",
                 "removing it starts that copy or follow over from the"
                 " beginning"),
    "journal": ("what undoing a change needs",
                "the lines it still holds are what can be undone by hand"),
    "state": ("the state of work in progress",
              "removing it starts that work over"),
}


class Torn(SystemExit):
    """A file migkit keeps that cannot be read whole, of a kind where
    going on without it would be a guess."""


def _kind(kind):
    if kind not in KINDS:
        raise ValueError(f"no torn-file policy for kind {kind!r}")
    return KINDS[kind]


def _torn(path, kind, why):
    holds, then = _HOLDS[kind]
    return Torn(f"{path} cannot be read whole ({why}). It holds {holds},"
                " and going on without it would be a guess, so migkit"
                f" stops here; {then}")


def private_dir(path):
    """`path` as a directory only this user can read, made where missing
    (its parents too, each 0700). An existing one of migkit's that an
    earlier version left readable by every local user is closed."""
    path = Path(path)
    missing = []
    at = path
    while not at.exists():
        missing.append(at)
        if at.parent == at:
            break
        at = at.parent
    for d in reversed(missing):
        try:
            os.mkdir(d, 0o700)
        except FileExistsError:
            pass
    st = path.stat()
    if st.st_mode & 0o077 and st.st_uid == os.getuid():
        os.chmod(path, 0o700)
    return path


def _sync(fd, full):
    if full:
        import fcntl
        if hasattr(fcntl, "F_FULLFSYNC"):
            try:
                fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
                return
            except OSError:
                pass
    os.fsync(fd)


def sync_dir(path):
    """The directory's own entries on the disk: without it a rename or a
    new file can be lost with the power while the data it names is
    kept."""
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError as e:
        # a file system that cannot sync a directory keeps nothing more by
        # being asked twice
        if e.errno not in (errno.EINVAL, errno.ENOTSUP, errno.EBADF):
            raise
    finally:
        os.close(fd)


def write_atomic(path, data, mode=0o600, full=False):
    """Replace `path` with `data` (text or bytes), whole or not at all,
    and on the disk once this returns. `mode` is the file's from its
    first byte; the directory is made private where missing. `full`
    flushes through the drive's own cache where the system has a way
    (macOS), for the few files a power loss must never roll back."""
    path = Path(path)
    if isinstance(data, str):
        data = data.encode()
    if not path.parent.exists():
        private_dir(path.parent)
    tmp = path.parent / f".{path.name}.{secrets.token_hex(6)}.tmp"
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view):]
        _sync(fd, full)
        os.close(fd)
        fd = None
        os.replace(tmp, path)
    except BaseException:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    sync_dir(path.parent)
    return path


def write_json(path, value, mode=0o600, full=False, indent=None):
    """`value` as JSON, through `write_atomic`."""
    return write_atomic(path, json.dumps(value, indent=indent, default=str),
                        mode=mode, full=full)


def append_line(path, line, mode=0o600, sync=True):
    """Append one line to a journal; returns how many bytes of a torn
    last line were taken off first (0 almost always).

    A line is written with its newline in one write, so a last line
    without one is a write a crash cut: nothing read it as a line, and
    the next line appended after it would be glued on and unreadable
    too. Held under an exclusive lock, the one every writer of the file
    takes."""
    import fcntl
    path = Path(path)
    if not path.parent.exists():
        private_dir(path.parent)
    text = str(line).rstrip("\n")
    if "\n" in text:
        raise ValueError("one line at a time")
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT | os.O_APPEND, mode)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        dropped = _cut_torn_tail(fd)
        data = (text + "\n").encode()
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view):]
        if sync:
            os.fsync(fd)
        return dropped
    finally:
        os.close(fd)


def _cut_torn_tail(fd):
    size = os.lseek(fd, 0, os.SEEK_END)
    if not size:
        return 0
    if os.pread(fd, 1, size - 1) == b"\n":
        return 0
    at, step = size, 4096
    while at > 0:
        start = max(0, at - step)
        chunk = os.pread(fd, at - start, start)
        nl = chunk.rfind(b"\n")
        if nl >= 0:
            keep = start + nl + 1
            os.ftruncate(fd, keep)
            return size - keep
        at = start
    os.ftruncate(fd, 0)
    return size


def read_state(path, kind):
    """The JSON value `path` holds; None where there is no file yet.

    A file that is empty, cut or not JSON is read by `kind`'s policy
    (`KINDS`): a position, a journal or state stops with `Torn`, naming
    the file; a cache reads as None."""
    policy = _kind(kind)
    path = Path(path)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as e:
        if policy == "absent":
            return None
        raise _torn(path, kind, e.strerror or str(e)) from None
    try:
        if not raw.strip():
            raise ValueError("it is empty")
        return json.loads(raw)
    except ValueError as e:
        if policy == "absent":
            return None
        why = (f"{e.msg} at byte {e.pos}" if isinstance(e, json.JSONDecodeError)
               else str(e))
        raise _torn(path, kind, why) from None


def read_lines(path, kind):
    """The JSON lines a journal holds, [] where there is no file yet.

    A last line without its newline is a write a crash cut - it never
    finished, so nothing acted on it - and is left out. A line that does
    not read anywhere else is a break, read by `kind`'s policy: stops
    for a journal, left out for a cache."""
    policy = _kind(kind)
    path = Path(path)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return []
    lines = raw.split(b"\n")
    # the part after the last newline: "" when the file ends whole
    lines = lines[:-1]
    out = []
    for n, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            if policy == "absent":
                continue
            raise _torn(path, kind, f"line {n} is not a record") from None
    return out
