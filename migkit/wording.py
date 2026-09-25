"""What the operator reads while migkit works, in migkit's own words.

migkit drives other programs underneath, and each of them prints in its own
voice: command lines, progress formats, warnings. None of that is the
operator's business. They asked migkit to move a database; which program
read which table is migkit's decision, and a line like
`$ pg_restore -h ... -d appdb ...` hands them a puzzle they did not ask for
and cannot act on.

So there is one vocabulary here, and every path speaks it:

* **phases**, each with a fixed wording (`PHASES`)
* **progress** within a phase: table, rows, bytes, rate, time left
* **the command line itself** goes to the run's local debug log, with every
  secret removed, for whoever has to debug migkit - never to the screen

A phase nobody has taught this module raises rather than printing a raw
key, the same rule every dispatch in migkit follows.

It replaces `progress.Reporter`, from the first commit, which formatted
progress too and had no caller: one way to say how far a run has got.
"""
import re
import time
from pathlib import Path

#: How each phase of a run is said. Keys are what the code passes; values
#: are what the operator reads. No wording names a program.
PHASES = {
    "dump": "reading the source into a local copy",
    "create": "creating the tables the target does not have yet",
    "finish-created": "adding the keys, indexes and constraints of the"
                      " tables it created",
    "sequences": "setting the target's sequences from the source's",
    "empty": "emptying the target's tables",
    "load": "loading the local copy into the target",
    "stream-copy": "copying tables straight from source to target",
    "copy-table": "copying table by table",
    "indexes-off": "dropping secondary indexes for the load",
    "indexes-on": "rebuilding the secondary indexes",
    "statistics": "refreshing the target's statistics",
    "follow": "applying the changes made since the copy",
    "follow-stop": "stopping the change stream at its end position",
    "stream": "running the change stream",
    "verify": "comparing source and target",
}


def phase(key, **facts):
    """The line for entering a phase, with the facts that shape it.

    `phase("load", workers=4, tables=12)` reads "loading the local copy into
    the target: 12 tables, 4 at a time". Facts with no wording are refused:
    a fact the operator is shown has to have been decided on, not leaked.
    """
    if key not in PHASES:
        raise ValueError(f"no wording for phase {key!r}")
    parts = []
    for name, value in facts.items():
        if value in (None, "", 0, [], ()):
            continue
        say = _FACTS.get(name)
        if say is None:
            raise ValueError(f"no wording for fact {name!r}")
        parts.append(say(value))
    return PHASES[key] + (": " + ", ".join(parts) if parts else "")


def _n(v):
    return f"{v:,}" if isinstance(v, int) else str(v)


_FACTS = {
    "tables": lambda v: f"{_n(v)} tables",
    "workers": lambda v: f"{_n(v)} at a time",
    "left_out": lambda v: f"{_n(v)} tables left out as the hop asks",
    "routed": lambda v: f"{_n(v)} tables copied table by table, as the"
                        " plan says",
    "row_filters": lambda v: f"{_n(v)} tables read through the hop's row"
                             " filter",
    "rows": lambda v: f"{_n(v)} rows",
    "size": lambda v: human_bytes(v),
    "table": lambda v: str(v),
}


def human_bytes(n):
    """`12.4 GB`, not `13314398618`."""
    n = float(n)
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return (f"{int(n)} {unit}" if unit == "bytes"
                    else f"{n:.1f} {unit}")
        n /= 1024


def progress(table, done, total=None, started=None, now=None, unit="rows"):
    """One progress line: how far, how fast, how long left.

    The rate and the time left are only said when they can be computed -
    a guess presented as a number is worse than no number.
    """
    now = time.monotonic() if now is None else now
    line = f"{table}: {_n(done)}"
    if total:
        pct = min(100.0, 100.0 * done / total)
        line += f" of {_n(total)} {unit} ({pct:.0f}%)"
    else:
        line += f" {unit}"
    if started is not None and now > started and done:
        rate = done / (now - started)
        line += f", {_n(int(rate))} {unit}/s"
        if total and total > done and rate > 0:
            line += f", about {_duration((total - done) / rate)} left"
    return line


def _duration(seconds):
    s = int(round(seconds))
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


#: `scheme://user:password@host` in any argument
_URI_PASSWORD = re.compile(r"(://[^:/@\s]+:)[^@\s]*@")


def redact(argv, secrets=()):
    """`argv` as one line with every secret replaced by `***`.

    Two ways a secret reaches a command line: a URI with the password in
    it, and a value passed on its own. The URI form is found by shape; the
    rest by value, so a password that happens to appear inside another
    argument is caught too.
    """
    line = " ".join(str(a) for a in argv)
    line = _URI_PASSWORD.sub(r"\1***@", line)
    for s in secrets:
        if s:
            line = line.replace(str(s), "***")
    return line


class DebugLog:
    """The run's own record of what was executed, for debugging migkit.

    Kept in the hop's report directory, next to the other artefacts of the
    run, and never printed. Every line goes through `redact` first: this
    file is what someone attaches to a bug report.
    """

    def __init__(self, path):
        self.path = Path(path)

    def command(self, argv, secrets=()):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y-%m-%dT%H:%M:%S")
        with self.path.open("a") as fh:
            fh.write(f"{stamp} $ {redact(argv, secrets)}\n")


class Step(str):
    """One line of a plan: what the operator reads, with the command that
    carries it out kept beside it.

    The plan a dry run prints and the command `--go` runs used to be built
    in two places, so they could say different things. A step is built once:
    its text is the phase in migkit's words, its `argv` is what runs, and
    only the text is ever shown.
    """

    def __new__(cls, text, argv=None):
        step = super().__new__(cls, text)
        step.argv = [str(a) for a in argv] if argv else None
        return step

    @property
    def command(self):
        """The command line, for tests and the debug log, never the screen."""
        return " ".join(self.argv) if self.argv else ""


#: what a program prefixes its own messages with: `pg_dump: error: ...`
_PREFIX = r"(?im)^[ \t]*{name}(?:\[\d+\])?:[ \t]*"


def without_programs(text, names):
    """A program's message with the program taken out of it.

    What a failed copy printed is often the only useful diagnosis - the
    database's own error (`password authentication failed`, `connection
    refused`) is in there, and that is the database's vocabulary, which the
    operator's DBA reads. The program's name around it is not.
    """
    for name in sorted({str(n) for n in names if n}, key=len, reverse=True):
        text = re.sub(_PREFIX.format(name=re.escape(name)), "", text)
        text = re.sub(rf"(?i)(?<![\w/-]){re.escape(name)}(?![\w-])",
                      "the copy", text)
    return text.strip()


#: A server's own error line inside a program's log: PostgreSQL's
#: `ERROR:  <message>` with its `DETAIL`, `HINT` and `CONTEXT`, as the
#: copy programs pass them on - with the side (`[TARGET 114]`) and the
#: SQLSTATE (`[53100]`) where the program adds them. A program's own lines
#: (`ERROR  pgsql.c:3317 ...`, no colon) are not the server's words.
_SERVER_SAID = re.compile(
    r"(?:\[(?P<side>SOURCE|TARGET)\b[^\]]*\]\s*)?(?:\[(?P<state>[0-9A-Z]{5})\]"
    r"\s*)?\b(?P<level>ERROR|FATAL|PANIC|DETAIL|HINT|CONTEXT):\s+(?P<msg>.+)$",
    re.M)

#: libpq's words for a server it could not reach
_UNREACHABLE = re.compile(r'connection to server at "(?P<host>[^"]+)",'
                          r" port (?P<port>\d+) failed: (?P<why>.+)$", re.M)

#: SQLSTATEs whose meaning an operator should not have to look up
_STATES = {"53100": "ran out of disk space",
           "53200": "ran out of memory",
           "53300": "has no connection slots left"}


def database_words(text):
    """What the database itself said, out of a program's log, or ''.

    A failed copy's log ends in the program's own bookkeeping. Measured, a
    target whose disk filled: the last 500 characters were four lines of
    `Sub-process exited with code 12` and the like, and the line that said
    `could not extend file ...: No space left on device`, with its SQLSTATE
    and the database's own hint, was cut off above them."""
    found = list(_SERVER_SAID.finditer(text or ""))
    first = next((m for m in found
                  if m.group("level") in ("ERROR", "FATAL", "PANIC")), None)
    if first is None:
        reach = _UNREACHABLE.search(text or "")
        if reach:
            # measured: a target whose disk filled stopped altogether (a
            # PANIC on the log it could not write), and the next copy's
            # log ended in its own bookkeeping, not in this line
            return (f"the database at {reach.group('host')}:"
                    f"{reach.group('port')} could not be reached:"
                    f" {reach.group('why').strip()}")
        if "no space left on device" in (text or "").lower():
            return ("a disk ran out of space - the database's, or this"
                    " machine's where the local copy is written")
        return ""
    side = {"TARGET": "the target", "SOURCE": "the source"}.get(
        first.group("side") or "", "the database")
    state = first.group("state") or ""
    said = first.group("msg").strip()
    if state:
        said += f" (SQLSTATE {state})"
    extra = [f"{m.group('level').lower()}: {m.group('msg').strip()}"
             for m in found[found.index(first) + 1:]
             if m.group("level") in ("DETAIL", "HINT", "CONTEXT")][:3]
    lead = f"{side} {_STATES[state]}: " if state in _STATES else ""
    return (lead + f"{side} said: {said}"
            + ("; " + "; ".join(extra) if extra else ""))
