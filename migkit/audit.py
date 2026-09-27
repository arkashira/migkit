"""The run's own record of what migkit did, written so a changed record
shows.

Every entry says what was done, when, by whom and from which machine, and
carries the hash of the entry before it; its own hash is of its canonical
text. An entry edited, taken out or moved breaks the chain from there, and
`verify` says where. An entry is written whole or not at all under a lock,
since tables copied side by side - and machines sharing a run - write to
the same record. Kept beside the run's reports and nowhere in the
databases: migkit writes no bookkeeping into a target.
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


def _last_hash(path):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 65536))
            tail = f.read().splitlines()
    except OSError:
        return ""
    for line in reversed(tail):
        try:
            return json.loads(line).get("hash", "") or ""
        except ValueError:
            continue
    return ""


def append(path, entry):
    """`entry` added to the record at `path`, chained to the one before."""
    import fcntl
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK, open(path, "a+b") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            try:
                who = getpass.getuser()
            except Exception:  # noqa: BLE001 - no name to give
                who = ""
            body = {"at": time.strftime("%F %T"), "who": who,
                    "host": socket.gethostname(), **entry,
                    "prev": _last_hash(path)}
            body["hash"] = hashlib.sha256(
                _canonical(body).encode()).hexdigest()
            f.write((json.dumps(body, default=str) + "\n").encode())
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def verify(path):
    """(entries, chained, broken) of the record: how many entries, how many
    of them are chained (a record from before the chain has some that are
    not), and "" or where the chain breaks and how."""
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return 0, 0, ""
    prev, chained = None, 0
    for n, line in enumerate(lines, 1):
        try:
            entry = json.loads(line)
        except ValueError:
            return len(lines), chained, f"entry {n} is not a record"
        if "hash" not in entry:
            if prev is not None:
                return len(lines), chained, (
                    f"entry {n} carries no hash after the chain began -"
                    " written by hand, or its hash taken off")
            continue
        body = {k: v for k, v in entry.items() if k != "hash"}
        if hashlib.sha256(_canonical(body).encode()).hexdigest() \
                != entry["hash"]:
            return len(lines), chained, f"entry {n} was changed"
        if prev is not None and entry.get("prev") != prev:
            return len(lines), chained, (
                f"entry {n} does not follow entry {n - 1} - one was taken"
                " out or moved")
        prev = entry["hash"]
        chained += 1
    return len(lines), chained, ""
