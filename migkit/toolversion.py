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
"""
import re

# tool -> (engine, regex capturing the version). Two formats, because the
# PostgreSQL and MySQL families each print their own way:
#   pg_dump (PostgreSQL) 18.6
#   mysqldump  Ver 9.7.1 for macos26.4 on arm64 (Homebrew)
PATTERNS = {
    "pg_dump": ("postgres", r"\(PostgreSQL\)\s+(\d+)"),
    "pg_restore": ("postgres", r"\(PostgreSQL\)\s+(\d+)"),
    "psql": ("postgres", r"\(PostgreSQL\)\s+(\d+)"),
    "mysqldump": ("mysql", r"Ver\s+(\d+)"),
    "mysql": ("mysql", r"Ver\s+(\d+)"),
}


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
    return int(m.group(1)) if m else None


def server_major(text):
    """The server's major version from what the engine reported."""
    if not text:
        return None
    m = re.match(r"\s*(\d+)", str(text))
    return int(m.group(1)) if m else None


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
