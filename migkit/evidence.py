"""The files migkit writes that hold the application's values, kept
encrypted where the hop asks (backlog R17b):

    options:
      at_rest:
        recipients:                 # who can read them: SSH or age keys
          - ssh-ed25519 AAAA... ann@example.com
          - age1...

A drilldown's keys (`data-*`), a repair's undo statements (`undo*`) and a
two-way tail's conflicts (`conflicts.jsonl`) are written through the hop's
report directory as age files to those recipients, and read back with the
operator's own key (`MIGKIT_IDENTITY`: a private SSH key or an age identity
file, `~/.ssh/id_ed25519` by default). Nothing else changes: every place
that reads or writes one goes on reading and writing a path; the path
itself encrypts (`EvidencePath`). A file written before the hop asked
reads as it was. What a program migkit drives writes on its own (a dump's
files) is not covered: keep those on an encrypted disk.
"""
import base64
import fnmatch
import os
import pathlib

#: the names of the files that hold values
HOLDS_VALUES = ("data-*", "undo*", "*.undo.sql", "conflicts.jsonl")

AGE = b"age-encryption.org/v1"
#: one line of a file written a line at a time, encrypted on its own
LINE = "age:"

#: {report root: recipients}, for the paths under it
_ROOTS = {}


def recipients_of(hop):
    got = ((hop.options or {}).get("at_rest") or {})
    got = got.get("recipients") if isinstance(got, dict) else None
    return [str(r).strip() for r in (got or []) if str(r).strip()]


def _recipient(text):
    import pyrage
    if text.startswith("age1"):
        return pyrage.x25519.Recipient.from_str(text)
    return pyrage.ssh.Recipient.from_str(text)


def _identity():
    import pyrage
    path = os.environ.get("MIGKIT_IDENTITY") or os.path.expanduser(
        "~/.ssh/id_ed25519")
    try:
        raw = pathlib.Path(path).read_bytes()
    except OSError:
        raise SystemExit(
            f"this file is encrypted for the hop's recipients, and no key to"
            f" read it is at {path}: set MIGKIT_IDENTITY to the private SSH"
            " key or age identity it was encrypted to") from None
    text = raw.decode("utf-8", "replace")
    if "AGE-SECRET-KEY-" in text:
        line = next(x for x in text.splitlines()
                    if x.startswith("AGE-SECRET-KEY-"))
        return pyrage.x25519.Identity.from_str(line.strip())
    return pyrage.ssh.Identity.from_buffer(raw)


def _seal(data, recipients):
    import pyrage
    return pyrage.encrypt(data, [_recipient(r) for r in recipients])


def _open(data):
    import pyrage
    try:
        return pyrage.decrypt(data, [_identity()])
    except pyrage.DecryptError:
        raise SystemExit(
            "this file is encrypted to the hop's recipients, and the key in"
            " MIGKIT_IDENTITY is not one of them") from None


class EvidencePath(type(pathlib.Path())):
    """A path under a hop's reports that encrypts the files holding values
    as they are written and decrypts them as they are read."""

    def _sealing(self):
        me = str(self)
        for root, recipients in _ROOTS.items():
            if not me.startswith(root + os.sep):
                continue
            inside = pathlib.PurePath(me[len(root) + 1:]).parts
            if any(fnmatch.fnmatch(self.name, p) for p in HOLDS_VALUES) \
                    or "undo" in inside[:-1]:
                return recipients
        return None

    def write_bytes(self, data):
        who = self._sealing()
        return super().write_bytes(_seal(bytes(data), who) if who
                                   else data)

    def write_text(self, data, encoding=None, errors=None, newline=None):
        who = self._sealing()
        if not who:
            return super().write_text(data, encoding=encoding,
                                      errors=errors, newline=newline)
        super().write_bytes(_seal(data.encode(encoding or "utf-8"), who))
        return len(data)

    def read_bytes(self):
        raw = super().read_bytes()
        if raw.startswith(AGE):
            return _open(raw)
        return raw

    def read_text(self, encoding=None, errors=None):
        raw = super().read_bytes()
        if raw.startswith(AGE):
            return _open(raw).decode(encoding or "utf-8",
                                     errors or "strict")
        if raw.startswith(LINE.encode()) or b"\n" + LINE.encode() in raw:
            return "".join(_opened_line(x) for x in
                           raw.decode().splitlines(keepends=True))
        return raw.decode(encoding or "utf-8", errors or "strict")

    def open(self, mode="r", *args, **kwargs):
        who = self._sealing()
        if who and "a" in mode and "b" not in mode:
            return _LineSealer(super().open(mode, *args, **kwargs), who)
        if "r" in mode and "b" not in mode and self.exists():
            import io
            return io.StringIO(self.read_text())
        return super().open(mode, *args, **kwargs)


def _opened_line(line):
    if not line.startswith(LINE):
        return line
    return _open(base64.b64decode(line[len(LINE):].strip())).decode()


class _LineSealer:
    """A file opened to be added to a line at a time, each line sealed on
    its own - a record that grows cannot be one age file."""

    def __init__(self, handle, recipients):
        self._h, self._who, self._part = handle, recipients, ""

    def write(self, text):
        self._part += text
        while "\n" in self._part:
            line, self._part = self._part.split("\n", 1)
            self._h.write(LINE + base64.b64encode(_seal(
                (line + "\n").encode(), self._who)).decode() + "\n")
        return len(text)

    def close(self):
        if self._part:
            self.write("\n")
        self._h.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def report_path(hop, path):
    """The hop's report directory as a path that encrypts, where the hop
    names recipients; the plain path where it does not."""
    who = recipients_of(hop)
    if not who:
        return path
    _ROOTS[str(pathlib.Path(path))] = who
    return EvidencePath(str(path))
