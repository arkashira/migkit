"""The way back, handed over at the same moment as the way forward.

migkit writes a repair script and says what it will lock. It said nothing at
all about undoing it. That asymmetry matters most exactly where the script is
used: against a target that is serving an application, inside a cutover window,
by someone who will be reading it under time pressure. If the repair turns out
to be wrong, or the cutover is abandoned, the operator is composing the inverse
DDL by hand at the worst possible moment.

The inverse is not something migkit has to infer. The repair is a diff between
two live schemas; running that same diff the other way round, from the same
two snapshots, is the undo of exactly those statements. So the revert is
generated at the same instant as the fix, from the same pair of databases,
before either has moved.

What it cannot do is restore data. A `DROP COLUMN` in the forward script gives
a revert that recreates the column, empty; a type change that truncated values
gives a revert that widens the column around values already cut short. Saying
"here is your rollback" without saying that would be worse than offering no
rollback at all, so every irreversible statement in the forward script is named
in the revert's header. The classification errs towards calling a statement
irreversible: a warning that turns out to be unnecessary costs a re-read, and
the opposite costs data.

sqitch is where the shape of this came from - it refuses to accept a change at
all unless a revert script exists alongside the deploy script.
"""
import re

from .locks import split

# Statements whose effect no DDL can undo, because what they removed was data
# and not structure. Deliberately broad: the cost of a false entry here is one
# extra line of prose, and the cost of a missing one is silence about lost
# rows.
_IRREVERSIBLE = [
    (re.compile(r"^drop\s+(table|materialized\s+view)\b"),
     "the rows are gone; the revert recreates it empty"),
    (re.compile(r"^drop\s+(database|schema)\b"),
     "everything it contained is gone; the revert recreates it empty"),
    (re.compile(r"^truncate\b"),
     "the rows are gone; the revert cannot bring them back"),
    (re.compile(r"^alter\s+table\b.*\bdrop\s+column\b"),
     "the column's values are gone; the revert recreates the column empty"),
    (re.compile(r"^alter\s+table\b.*\b(alter|modify|change)\s+column\b"
                r".*\btype\b|^alter\s+table\b.*\bmodify\s+column\b"),
     "a narrowing type change truncates values as it runs, and widening the"
     " column afterwards does not restore them"),
    (re.compile(r"^drop\s+"),
     "the object is gone; whatever it held is not restored by recreating it"),
]

_WS = re.compile(r"\s+")


def irreversible(forward_sql):
    """[(statement, why)] for the forward statements a revert cannot undo."""
    out = []
    for stmt in split(forward_sql):
        s = _WS.sub(" ", stmt.strip().rstrip(";")).lower()
        for pat, why in _IRREVERSIBLE:
            if pat.match(s):
                out.append((_WS.sub(" ", stmt.strip())[:160], why))
                break
    return out


def header(forward_sql, forward_name):
    """The comment block that goes on top of a revert script."""
    lost = irreversible(forward_sql)
    lines = [
        f"-- Undo for {forward_name}.",
        "--",
        "-- Generated from the same two live schemas as the forward script,",
        "-- at the same moment, by diffing them in the opposite direction.",
        "-- It is therefore exact for the structure - and only the structure.",
        "--",
    ]
    if not lost:
        lines += [
            "-- Nothing in the forward script destroys data, so applying this",
            "-- returns the target to where it started.",
            "--",
        ]
    else:
        lines += [
            f"-- {len(lost)} statement(s) in the forward script cannot be",
            "-- undone by any DDL, because what they removed was data:",
            "--",
        ]
        for stmt, why in lost:
            lines.append(f"--   {stmt}")
            lines.append(f"--     -> {why}")
        lines += [
            "--",
            "-- Restoring those needs a backup, not this file. Take one before",
            "-- you apply the forward script, not after.",
            "--",
        ]
    lines.append("-- Read it before running it: a revert is still DDL against"
                 " a live target.")
    lines.append("")
    return "\n".join(lines)


def script(forward_sql, reverse_sql, forward_name):
    """The full revert file, or '' when there is nothing to undo."""
    if not (reverse_sql or "").strip():
        return ""
    return header(forward_sql, forward_name) + reverse_sql.rstrip() + "\n"


def summary(forward_sql, reverse_sql):
    """One clause for the check's detail line, or '' if there is no revert."""
    if not (reverse_sql or "").strip():
        return ""
    n = len(split(reverse_sql))
    lost = len(irreversible(forward_sql))
    if not lost:
        return f"a {n}-statement undo was written alongside it"
    return (f"a {n}-statement undo was written alongside it, but {lost}"
            f" of the forward statements destroy data and no undo restores"
            f" that - take a backup first")
