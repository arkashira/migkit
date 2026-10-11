"""Whether the client tools on this machine can talk to that server.

`doctor` answers whether a program is installed. That is a different question
from whether it works against the database you are about to point it at, and
the gap between the two is not hypothetical.

Measured on the machine this was written on: `pg_dump` is 18.6 and the servers
are PostgreSQL 16. `mysqldump` is 9.7.1 and the servers are MySQL 8. Both are
client tools running ahead of their targets, which is the normal state of a
laptop with Homebrew on it and the reason this check exists.

Two measurements, both real, both the same shape:

- migkit already carries a workaround in `pgdump_move` for it: "newer pg_dump
  emits SETs older servers reject", swallowing a `pg_restore` exit code
  because the rows did land.
- `pgcopydb` 0.18, built against PostgreSQL 18, emits `SET transaction_timeout
  = 0` on the target. PostgreSQL 16 does not know that parameter - it arrived
  in 17 - so the statement fails, the transaction aborts, and every later
  statement in it is refused. A whole-database clone moved zero rows and zero
  large objects while reporting each rejection separately. That is why migkit
  does not wrap pgcopydb.

The rule here is one-sided, like the lock report: a client **older** than the
server is fine and common, a client **newer** is the direction that breaks, so
only that direction is reported. Being told to check a pairing that turns out
to work costs a minute; the opposite costs a migration that looks finished.

This does not try to predict which statement will fail. Which parameters a
given client emits, and which a given server rejects, is a matrix nobody
should be encoding from memory - so the finding names the pairing and leaves
the judgement to the operator, who can test it in a minute.

What a build is, is asked here once per build (`probe`, `options`): the
file on the path, resolved, and the moment it was last written - a program
upgraded under a long-running process is a new build and is asked again,
the same one is never asked twice. How each program is asked, and where its
version is in the answer, is the registry's (`tools.PROGRAM`). Measured
before: `assess` on a PostgreSQL hop started each client's `--version`
twice, and the version was read four ways.
"""
import functools
import os
import re


def _registry():
    from . import tools
    return tools.PROGRAM


def _patterns():
    return {n: (p.client_of, p.version_pattern)
            for n, p in _registry().items() if p.client_of}


#: tool -> (engine, regex capturing the version), for the client tools of an
#: engine, as the registry has them: the PostgreSQL and MySQL families each
#: print their own way -
#:   pg_dump (PostgreSQL) 18.6
#:   mysqldump  Ver 9.7.1 for macos26.4 on arm64 (Homebrew)
PATTERNS = _patterns()


def major(text, tool):
    """The client's major version from its `--version` output, or None.

    None is returned rather than a guess: a version string this does not
    recognise is a reason to say nothing, not a reason to invent a number and
    compare it.
    """
    pat = PATTERNS.get(tool)
    if not pat or not text:
        return None
    m = re.search(pat[1], str(text))
    return int(m.group(1).split(".")[0]) if m else None


def server_version(text):
    """The server's version from what the engine reported, as numbers -
    `8.4.2` of `8.4.2` and of `11.8.2-MariaDB-ubu2404` alike, (8, 4, 2) -
    or () where it starts with none: the one reading of a version every
    other is held to (audit 2026-10-01, DU23)."""
    m = re.match(r"\s*(\d+(?:\.\d+)*)", str(text or ""))
    return tuple(int(p) for p in m.group(1).split(".")) if m else ()


def server_major(text):
    """The server's major version from what the engine reported."""
    got = server_version(text)
    return got[0] if got else None


def skew(client, server):
    """'' when the pairing is fine, else why it is worth checking.

    Only a client ahead of its server is reported. A client behind the server
    misses newer features and says so plainly when it does; a client ahead
    emits syntax and settings the server has never heard of, and the server
    rejects the statement rather than the connection.
    """
    if client is None or server is None:
        return ""
    if client <= server:
        return ""
    return (f"client is {client}, server is {server} -"
            " a newer client emits settings the older server rejects, and the"
            " failure lands mid-transaction rather than at connect")


def report(tools, server, engine):
    """[(level, tool, detail)] for the client tools of one engine.

    `tools` is {tool: version_text_or_None}; a tool that is not installed is
    not this check's finding and is left to `doctor`.
    """
    smaj = server_major(server)
    out = []
    for tool, text in sorted(tools.items()):
        if PATTERNS.get(tool, ("",))[0] != engine:
            continue
        if text is None:
            continue
        cmaj = major(text, tool)
        if cmaj is None:
            out.append(("warn", tool,
                        f"version not recognised from {str(text)[:60]!r} -"
                        " unknown, not clean"))
            continue
        why = skew(cmaj, smaj)
        if why:
            out.append(("warn", tool, why))
        else:
            out.append(("pass", tool, f"client {cmaj} against server {smaj}"))
    return out


# ---- asking a build ------------------------------------------------------------

def _build(program):
    """(the file that runs as `program` here, resolved, and when it was last
    written), or None where it is not installed."""
    from .util import which
    path = which(program)
    if not path:
        return None
    real = os.path.realpath(path)
    try:
        return real, os.stat(real).st_mtime_ns
    except OSError:
        return None


def probe(program):
    """What the installed build of `program` says when asked its version,
    or None where it is not installed, is asked nothing (its install proves
    it), or says nothing."""
    got = _build(program)
    facts = _registry().get(program)
    args = tuple(facts.version_args) if facts else ("--version",)
    if got is None or not args:
        return None
    return _said(got[0], got[1], args)


@functools.lru_cache(maxsize=None)
def _said(path, stamp, args):
    from . import runner
    try:
        p = runner.run([path, *args], check=False, timeout=60,
                       check_flags=False, what="the version")
    except Exception:  # noqa: BLE001 - a build that cannot be asked
        return None
    return ((p.stdout or "") + (p.stderr or "")) or None


def version(program, text=None):
    """The installed build's version as it prints it (`18.6`, `1.0.5`), or
    None: read from `text` where given, else from asking it."""
    text = probe(program) if text is None else text
    if not text:
        return None
    facts = _registry().get(program)
    pat = facts.version_pattern if facts else r"(\d+\.\d+(?:\.\d+)?)"
    m = re.search(pat, text)
    return m.group(1) if m else None


def older(have, lowest):
    """Whether version `have` comes before `lowest`, part by part as
    numbers; False where either is not a version."""
    def parts(v):
        return [int(x) for x in re.findall(r"\d+", str(v or ""))]
    a, b = parts(have), parts(lowest)
    return bool(a and b) and a[:len(b)] < b


def options(program, *sub, path=None):
    """The long options this installed build of `program` really has - of
    its subcommand `sub` where it has subcommands.

    Read from the option column of its own `--help`, not from the whole
    text: mydumper 1.0.5 names `--overwrite-tables` inside the description
    of `--overwrite-unsafe`, and the binary answers `Unknown option
    --overwrite-tables`. Measured, the column holds 136 options for mydumper
    and 99 for myloader, and that one is in neither.

    A program with subcommands keeps its options there: pgcopydb 0.18's own
    `--help` lists none, and `copy table-data --help` lists 11. Asked
    without the subcommand, every option the copy was passed looked absent
    and nothing could be checked. `path`: the build at that file, where it
    is not the one on migkit's path.
    """
    from .util import which
    path = path or which(program) or program
    try:
        stamp = os.stat(path).st_mtime_ns
    except OSError:
        stamp = None
    return options_of(path, stamp, tuple(sub))


@functools.lru_cache(maxsize=None)
def options_of(path, stamp, sub=()):
    """The option column of `path`'s `--help` (of its subcommand `sub`),
    asked once per build."""
    from . import runner
    try:
        p = runner.run([path, *sub, "--help"], check=False, timeout=60,
                       check_flags=False, what="the options")
    except Exception:  # noqa: BLE001 - a build that cannot be asked
        return frozenset()
    return option_column((p.stdout or "") + (p.stderr or ""))


def option_column(text):
    """The long options in the option column of a `--help` text."""
    # the option column: the flag, then the column gap, the end of the
    # line, or its `=VALUE`. Measured, the MongoDB tools spell theirs in
    # camelCase with `=<value>` (`--bypassDocumentValidation`), and the old
    # lower-case, gap-only pattern found 6 of mongodump's 37 flags. The
    # online sync names its value after one space (`--config value`), and
    # 5 of its 15 were found until that shape was read too; every other
    # program's count stayed the same. The MySQL copy program lists an
    # option beside its alias (`--py, --python`, `--nw, --no-wizard`):
    # 81 of its 91 were found until both halves were read, and the others'
    # counts stayed the same (136, 99, 73, 51, 37, 53, 34). pgcopydb's
    # column is one space short for its longest flag (`--large-objects-jobs
    # Number of ...`, `--summary List ...`), read as the flag and its
    # capitalised description; that added those two and nothing to any
    # other program
    name = r"--[a-zA-Z0-9][a-zA-Z0-9-]*"
    return frozenset(re.findall(
        rf"(?m)^\s+(?:-\w,\s+|{name},\s+)?({name})"
        r"(?=\s{2,}|$|=|\[=|\s[A-Za-z_<][\w<>.-]*\s{2,}|\s[A-Z][a-z]+\s)",
        text)
        + re.findall(rf"(?m)^\s+({name})(?=,\s+--)", text))
