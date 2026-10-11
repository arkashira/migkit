"""The run's own record of what migkit did, written so a changed record
shows.

Every entry says what was done, when, by whom and from which machine, and
carries the hash of the entry before it; its own hash is of its canonical
text. An entry edited, taken out or moved breaks the chain from there, and
`verify` says where. An entry is written whole or not at all under a lock,
since tables copied side by side - and machines sharing a run - write to
the same record. Kept beside the run's reports and nowhere in the
databases: migkit writes no bookkeeping into a target.

Two holes are closed (audit 2026-10-01, ST16, SEC15):

* **A torn last line** - a write a crash cut - was glued to the next
  entry, and the record read as broken for good: a crash could not be
  told from tampering, and nothing after it verified. The next append
  now takes the torn bytes off first and writes, chained, what it took
  off (`{"op": "torn", "bytes", "sha256"}`), and `verify` says a torn
  last line apart from a break (`torn`).
* **The opening stretch**: entries without a hash were let through
  before the chain began, and the first hashed entry's `prev` was never
  held to anything - strip the hashes of the first entries, edit them,
  and the record still read whole. The first hashed entry now has
  `prev` "" and, where lines stand before it, commits to their SHA-256
  (`older`); a record that begins with a rotated part's head
  (`runlog.rotate`) has nothing before it, and its head is checked
  against the part before it there (`runlog.verify_parts`).

The chain alone shows a changed entry, not a record rewritten whole from
its start by someone who can write the file. A checkpoint - the head and
the number of entries it stands at - is kept where they cannot, in an
object-locked bucket (`anchor`), and `anchor.verify` holds the record
against those copies.
"""
import getpass
import hashlib
import json
import os
import socket
import threading
import time

_LOCK = threading.Lock()


def _canonical(entry):
    return json.dumps(entry, sort_keys=True, default=str,
                      separators=(",", ":"))


def _last_line(fd, size):
    """The last whole line of the file open at `fd`, read back from its
    end however long it is - reading the last 64 KB only lost the chain
    after an entry longer than that."""
    at, step, tail = size, 65536, b""
    while at > 0:
        start = max(0, at - step)
        tail = os.pread(fd, at - start, start) + tail
        at = start
        body = tail.rstrip(b"\n")
        nl = body.rfind(b"\n")
        if nl >= 0 or at == 0:
            return body[nl + 1:]
    return b""


def _chain_state(fd):
    """(the hash of the last entry or "", the bytes before the first
    hashed entry where the chain has not begun yet, else None) of a record
    whose last line is whole."""
    size = os.lseek(fd, 0, os.SEEK_END)
    if not size:
        return "", b""
    last = _last_line(fd, size)
    try:
        got = json.loads(last)
    except ValueError:
        got = None
    if isinstance(got, dict) and got.get("hash"):
        return got["hash"], None
    # no hash on the last line: an earlier version's record, or one whose
    # chain has not begun - read whole, it is small until it has
    raw = os.pread(fd, size, 0)
    for line in reversed(raw.splitlines()):
        try:
            got = json.loads(line)
        except ValueError:
            continue
        if isinstance(got, dict) and got.get("hash"):
            return got["hash"], None
    return "", raw


def append(path, entry):
    """`entry` added to the record at `path`, chained to the one before,
    and stamped with the run that wrote it (`session`), so one run's
    entries can be told from the hop's whole record. Its hash - the head
    of the chain now - is returned, for an anchor kept elsewhere to hold
    the record to (`export`)."""
    import fcntl

    from . import durable, session
    if not path.parent.exists():
        durable.private_dir(path.parent)
    try:
        who = getpass.getuser()
    except Exception:  # noqa: BLE001 - no name to give
        who = ""
    with _LOCK:
        fd = os.open(str(path), os.O_RDWR | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            size = os.lseek(fd, 0, os.SEEK_END)
            torn = b""
            if size and os.pread(fd, 1, size - 1) != b"\n":
                raw = os.pread(fd, size, 0)
                keep = raw.rfind(b"\n") + 1
                torn = raw[keep:]
                os.ftruncate(fd, keep)
            prev, older = _chain_state(fd)
            lines = []
            if torn:
                lines.append({"op": "torn", "bytes": len(torn),
                              "sha256": hashlib.sha256(torn).hexdigest()})
            lines.append(entry)
            # a run started from the web view: the person who started it,
            # beside the machine's user it runs as
            actor = os.environ.get("MIGKIT_ACTOR", "").strip()
            out = []
            for one in lines:
                body = {"at": time.strftime("%F %T"), "who": who,
                        "host": socket.gethostname(),
                        "session": session.current(),
                        **({"by": actor} if actor else {}), **one,
                        "prev": prev}
                if not prev and older:
                    # the first hashed entry binds what stands before it
                    body["older"] = hashlib.sha256(older).hexdigest()
                body["hash"] = prev = hashlib.sha256(
                    _canonical(body).encode()).hexdigest()
                older = None
                out.append(json.dumps(body, default=str) + "\n")
            data = "".join(out).encode()
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view):]
            os.fsync(fd)
            return prev
        finally:
            os.close(fd)


def head(path):
    """The hash of the record's last entry, "" where it has none."""
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return ""
    try:
        return _chain_state(fd)[0]
    finally:
        os.close(fd)


def hashes(path):
    """The hash of each whole line of the record, in order - None for a
    line that has none or is not a record - so a checkpoint that names
    entry N (`anchor`) is held against the entry now at N. A last line
    without its newline is a write a crash cut, and is not counted, as in
    `verify`."""
    try:
        raw = path.read_bytes()
    except OSError:
        return []
    if raw and not raw.endswith(b"\n"):
        raw = raw[:raw.rfind(b"\n") + 1]
    out = []
    for line in (raw.split(b"\n")[:-1] if raw else []):
        try:
            got = json.loads(line)
        except ValueError:
            got = None
        h = got.get("hash") if isinstance(got, dict) else None
        out.append(h if isinstance(h, str) and h else None)
    return out


def verify(path, torn=None):
    """(entries, chained, broken) of the record: how many entries, how many
    of them are chained (a record from before the chain has some that are
    not), and "" or where the chain breaks and how. A last line without
    its newline is a write a crash cut: not a break - `torn`, a list,
    is given its length."""
    try:
        raw = path.read_bytes()
    except OSError:
        return 0, 0, ""
    if raw and not raw.endswith(b"\n"):
        cut = raw.rfind(b"\n") + 1
        if torn is not None:
            torn.append(len(raw) - cut)
        raw = raw[:cut]
    lines = raw.split(b"\n")[:-1] if raw else []
    prev, chained, before, at = None, 0, 0, 0
    for n, line in enumerate(lines, 1):
        at += len(line) + 1
        try:
            entry = json.loads(line)
        except ValueError:
            return len(lines), chained, f"entry {n} is not a record"
        if not isinstance(entry, dict):
            return len(lines), chained, f"entry {n} is not a record"
        if "hash" not in entry:
            if prev is not None:
                return len(lines), chained, (
                    f"entry {n} carries no hash after the chain began -"
                    " written by hand, or its hash taken off")
            before = at
            continue
        body = {k: v for k, v in entry.items() if k != "hash"}
        if hashlib.sha256(_canonical(body).encode()).hexdigest() \
                != entry["hash"]:
            return len(lines), chained, f"entry {n} was changed"
        if prev is None:
            # the chain's first entry: it begins it, and binds what stands
            # before it
            if entry.get("prev"):
                return len(lines), chained, (
                    f"entry {n} begins the chain and names an entry before"
                    " it that is not there - the ones before were taken"
                    " out")
            if n > 1:
                bound = entry.get("older")
                if not bound or bound != hashlib.sha256(
                        raw[:before]).hexdigest():
                    return len(lines), chained, (
                        f"entries 1-{n - 1} stand before the chain and"
                        f" entry {n} does not bind them - they were"
                        " changed, or had their hashes taken off")
        elif entry.get("prev") != prev:
            return len(lines), chained, (
                f"entry {n} does not follow entry {n - 1} - one was taken"
                " out or moved")
        prev = entry["hash"]
        chained += 1
    return len(lines), chained, ""
