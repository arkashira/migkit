"""The files migkit writes that hold the application's values, kept
encrypted where the hop asks (backlog R17b):

    options:
      at_rest:
        recipients:                 # who can read them: SSH or age keys
          - ssh-ed25519 AAAA... ann@example.com
          - age1...

A drilldown's keys (`data-*`), a repair's undo statements (`undo*`), a
two-way tail's conflicts (`conflicts.jsonl`) and the rows it parked
(`conflicts-parked.json`), and the evidence export
(`export.FILES`: a finding's words can quote a key) are written through
the hop's report directory as age files to those recipients, and read
back with the operator's own key (`MIGKIT_IDENTITY`: a private SSH key or
an age identity file, `~/.ssh/id_ed25519` by default). Nothing else
changes: every place that reads or writes one goes on reading and writing
a path; the path itself encrypts (`EvidencePath`). A file written before
the hop asked reads as it was. What a program migkit drives writes on its
own (a dump's files) is not covered: keep those on an encrypted disk.
"""
import base64
import fnmatch
import os
import pathlib

#: the names of the files that hold values
HOLDS_VALUES = ("data-*", "undo*", "*.undo.sql", "conflicts.jsonl",
                "conflicts-parked*", "evidence.json", "evidence.html",
                "evidence.md", "tables.csv")

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
            return opened_lines(raw)
        return raw.decode(encoding or "utf-8", errors or "strict")

    def open(self, mode="r", *args, **kwargs):
        """The file, opened: sealed in every mode that writes where it
        holds values - a line at a time where it is added to, whole where
        it is written - and refused in a mode that cannot be (a binary
        append, a file read and written at once): a plain handle there
        wrote values in the clear under a hop that asked for them sealed
        (audit SEC9)."""
        import io
        who = self._sealing()
        if who and any(m in mode for m in "wax+"):
            if "+" in mode or ("a" in mode and "b" in mode):
                raise SystemExit(
                    f"{self} holds values the hop keeps encrypted"
                    f" (at_rest), and a file opened '{mode}' cannot be"
                    " kept so")
            if "a" in mode:
                return _LineSealer(super().open(mode, *args, **kwargs), who)
            return _WholeSealer(self, "b" in mode)
        if "r" in mode and "+" not in mode and self.exists():
            if "b" in mode:
                return io.BytesIO(self.read_bytes())
            return io.StringIO(self.read_text())
        return super().open(mode, *args, **kwargs)


def opened_lines(raw):
    """A file added to a line at a time, each line opened. A last line
    without its newline is a write a crash cut: left out where it cannot
    be opened, as a torn line of a plain file is (`durable.read_lines`)."""
    lines = raw.decode().splitlines(keepends=True)
    out = []
    for i, x in enumerate(lines):
        try:
            out.append(_opened_line(x))
        except (ValueError, SystemExit) as e:
            if i == len(lines) - 1 and not x.endswith("\n"):
                break
            if isinstance(e, SystemExit):
                raise
            raise SystemExit(f"a line of an encrypted file cannot be"
                             f" opened ({e})") from None
    return "".join(out)


def _opened_line(line):
    if not line.startswith(LINE):
        return line
    return _open(base64.b64decode(line[len(LINE):].strip())).decode()


def seal_line(text, recipients):
    """One line of a file added to a line at a time, sealed on its own
    (`LINE`), with its newline."""
    return LINE + base64.b64encode(_seal(
        (text + "\n").encode(), recipients)).decode() + "\n"


class _WholeSealer:
    """A file written whole, sealed as one age file when it is closed."""

    def __init__(self, path, binary):
        import io
        self._path, self._binary = path, binary
        self._buf = io.BytesIO() if binary else io.StringIO()

    def write(self, data):
        return self._buf.write(data)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        pass

    def close(self):
        if self._buf is None:
            return
        data, self._buf = self._buf.getvalue(), None
        if self._binary:
            self._path.write_bytes(data)
        else:
            self._path.write_text(data)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class _LineSealer:
    """A file opened to be added to a line at a time, each line sealed on
    its own - a record that grows cannot be one age file."""

    def __init__(self, handle, recipients):
        self._h, self._who, self._part = handle, recipients, ""

    def write(self, text):
        self._part += text
        while "\n" in self._part:
            line, self._part = self._part.split("\n", 1)
            self._h.write(seal_line(line, self._who))
        return len(text)

    def flush(self):
        """What was written so far handed to the system - a part of a line
        sealed on its own, which reads back joined to the rest (`LINE`):
        a repair's record is flushed before its first statement lands
        (`repairlog.Record.ready`), and a sealer without a flush stopped
        every repair of a hop that keeps its evidence encrypted (audit
        ST13)."""
        if self._part:
            self._h.write(LINE + base64.b64encode(_seal(
                self._part.encode(), self._who)).decode() + "\n")
            self._part = ""
        self._h.flush()

    def fileno(self):
        return self._h.fileno()

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
