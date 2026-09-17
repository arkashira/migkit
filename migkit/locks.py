"""What the repair DDL will lock, said before anyone applies it.

migkit hands the operator a `structural-fix.sql` and expects them to run it
against the target - a target that, on the run this was written for, was
serving an application at the time. The check throttles itself while
*reading*; the DDL it emits for *writing* said nothing at all about what it
would block. That asymmetry is the gap this closes.

Classification is static: the statement's shape decides its lock, from
PostgreSQL's documented behaviour. That is enough to answer "will this stop
my application", and it needs no connection, so it works while reviewing a
file. It is not the same as measuring: the definitive answer comes from
executing each statement against a throwaway copy and reading `pg_locks`,
which is what `results lockinfo` does. Where the two could disagree - an
`ADD COLUMN` whose default is volatile, a `SET NOT NULL` already backed by a
validated CHECK - this errs towards naming the heavier lock, because a
warning that turns out to be unnecessary costs a re-read and the opposite
costs an outage.
"""
import re

# PostgreSQL lock modes, least to most disruptive. The names are the ones
# pg_locks reports, so a static verdict and a measured one are comparable.
MODES = [
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
]
SEVERITY = {m: i for i, m in enumerate(MODES)}
# At and above this, concurrent statements start being blocked.
BLOCKS_WRITES = SEVERITY["ShareLock"]
BLOCKS_EVERYTHING = SEVERITY["AccessExclusiveLock"]

_WS = re.compile(r"\s+")


def _norm(stmt):
    return _WS.sub(" ", stmt.strip().rstrip(";")).lower()


# (matcher, lock mode, what it means, a safer way to write it or None)
_RULES = [
    (re.compile(r"^create\s+(unique\s+)?index\s+concurrently\b"),
     "ShareUpdateExclusiveLock",
     "builds without blocking reads or writes", None),
    (re.compile(r"^create\s+(unique\s+)?index\b"),
     "ShareLock",
     "blocks writes to the table for the whole build",
     "CREATE INDEX CONCURRENTLY (cannot run inside a transaction, and a"
     " failure leaves an invalid index to drop)"),
    (re.compile(r"^alter\s+table\b.*\bvalidate\s+constraint\b"),
     "ShareUpdateExclusiveLock",
     "scans the table without blocking reads or writes", None),
    (re.compile(r"^alter\s+table\b.*\badd\s+constraint\b.*\bnot\s+valid\b"),
     "AccessExclusiveLock",
     "brief exclusive lock, no table scan", None),
    (re.compile(r"^alter\s+table\b.*\badd\s+constraint\b.*\bforeign\s+key\b"),
     "AccessExclusiveLock",
     "exclusive lock on both tables while every row is checked",
     "ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT separately"),
    (re.compile(r"^alter\s+table\b.*\badd\s+constraint\b.*\bcheck\b"),
     "AccessExclusiveLock",
     "exclusive lock while every existing row is checked",
     "ADD CONSTRAINT ... NOT VALID, then VALIDATE CONSTRAINT separately"),
    (re.compile(r"^alter\s+table\b.*\balter\s+column\b.*\btype\b"),
     "AccessExclusiveLock",
     "rewrites the whole table under an exclusive lock", None),
    (re.compile(r"^alter\s+table\b.*\bset\s+not\s+null\b"),
     "AccessExclusiveLock",
     "exclusive lock while every row is checked for nulls",
     "add a validated CHECK (col IS NOT NULL) first; on PostgreSQL 12+ the"
     " SET NOT NULL then skips the scan"),
    (re.compile(r"^alter\s+table\b.*\badd\s+column\b.*\bdefault\b"),
     "AccessExclusiveLock",
     "brief exclusive lock; a volatile default rewrites the table", None),
    (re.compile(r"^alter\s+table\b.*\badd\s+column\b"),
     "AccessExclusiveLock",
     "brief exclusive lock, no rewrite", None),
    (re.compile(r"^(drop|truncate)\b"),
     "AccessExclusiveLock", "removes or empties the object", None),
    (re.compile(r"^alter\s+table\b"),
     "AccessExclusiveLock", "exclusive lock for the duration", None),
    # Measured: COMMENT ON takes ShareUpdateExclusiveLock, not the trivial
    # lock its harmlessness suggests. It still blocks nothing an application
    # does, but it conflicts with VACUUM and with other schema changes.
    (re.compile(r"^comment\s+on\b"),
     "ShareUpdateExclusiveLock",
     "no lock on reads or writes; conflicts with VACUUM and other DDL", None),
    (re.compile(r"^(create|grant|revoke|alter\s+sequence|select\s+setval)\b"),
     "RowExclusiveLock", "no lock on existing table data", None),
]


def classify(statement):
    """(mode, meaning, safer_alternative) for one statement."""
    s = _norm(statement)
    for pat, mode, meaning, safer in _RULES:
        if pat.match(s):
            return mode, meaning, safer
    # Unrecognised DDL is reported as the worst case rather than as harmless:
    # an unclassified statement is a reason to look, not a reason to relax.
    return ("AccessExclusiveLock",
            "not recognised - assume it blocks until checked", None)


def split(sql):
    """Statements from a script, ignoring comments and blank lines.

    Deliberately simple: the differ emits one statement per logical block and
    no dollar-quoted bodies. A script that needs real parsing should be read
    with a real parser, not by this.
    """
    out, buf = [], []
    for line in sql.splitlines():
        if not line.strip() or line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            out.append("\n".join(buf))
            buf = []
    if buf:
        out.append("\n".join(buf))
    return out


def report(sql):
    """(text, counts) describing what a repair script will lock."""
    rows = []
    counts = {"total": 0, "blocks_writes": 0, "blocks_everything": 0}
    for stmt in split(sql):
        mode, meaning, safer = classify(stmt)
        sev = SEVERITY[mode]
        counts["total"] += 1
        if sev >= BLOCKS_WRITES:
            counts["blocks_writes"] += 1
        if sev >= BLOCKS_EVERYTHING:
            counts["blocks_everything"] += 1
        rows.append((mode, meaning, safer, _norm(stmt)[:160]))
    lines = [
        "What this script locks, by statement.",
        "",
        "Static classification from each statement's shape, erring towards the",
        "heavier lock where it is ambiguous. Run it against a copy first if a",
        "definitive answer matters.",
        "",
    ]
    for mode, meaning, safer, stmt in rows:
        flag = "!!" if SEVERITY[mode] >= BLOCKS_EVERYTHING else (
            " !" if SEVERITY[mode] >= BLOCKS_WRITES else "  ")
        lines.append(f"{flag} {mode}: {meaning}")
        lines.append(f"     {stmt}")
        if safer:
            lines.append(f"     safer: {safer}")
        lines.append("")
    lines.append(f"{counts['total']} statements,"
                 f" {counts['blocks_writes']} block writes,"
                 f" {counts['blocks_everything']} block reads and writes.")
    return "\n".join(lines) + "\n", counts


def summary(counts):
    if not counts["total"]:
        return ""
    if not counts["blocks_writes"]:
        return f"{counts['total']} statements, none block concurrent access"
    return (f"{counts['blocks_everything']} of {counts['total']} statements"
            f" block reads and writes,"
            f" {counts['blocks_writes']} block writes")
