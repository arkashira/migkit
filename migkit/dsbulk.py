"""A Cassandra or ScyllaDB table copied by the bulk loader (backlog
F7.dsbulk), beside migkit's own copier by token ranges.

The loader reads the table out in as many token-range splits as migkit's
sizing gives, each cell with its time to live and write time
(`schema.preserveTtl`, `schema.preserveTimestamp`), into files of a
directory of this user's only, and loads them back so each cell carries
the time left to it and its own write time - the rule migkit's copier
keeps. Both runs keep the loader's checkpoint, so a copy stopped part way
reads out and loads only what it had not (`log.checkpoint.replayStrategy
resume`); the directory is removed once the table is proved.

What migkit adds around it: the table is made on the target by the
source's own definition first (`CassandraEngine._made_like`) and emptied
unless a stopped load goes on; the passwords are in a configuration file
readable by this user only, never on its command line; no error is
skipped (`log.maxErrors 0`); the rows the load wrote must be the rows the
read-out read, none failed, and the loader's own count of the target
(`count`, `stats.modes global`) must agree before the table is called
copied - after which the move compares it with the source as it compares
every bulk copy. Which of the two ways carries a table is climbed per
table on the seconds a row each has measured here (`CassandraEngine.
TABLE_WAYS`).

What it cannot carry is a need its rung lacks: a counter (added to, never
written, as the copier refuses it too), and a column of many cells - a
collection or a user type not frozen - whose cells have no single time to
keep; TLS is not yet given to it (its key stores are Java's), so a hop
with TLS takes the copier.
"""
import json
import os
import re
import shutil
import subprocess

from . import tooluse
from .tooluse import TOPPED, UNUSED, USED

PROGRAM = "dsbulk"
#: splits a worker reads, as migkit's copier cuts its token ranges
SPLITS_PER_WORKER = 8


def unfit(eng, db, table):
    """Why the bulk loader cannot carry this table as migkit's copier
    does, or None."""
    for ep in (eng.hop.source, eng.hop.target):
        if ep.cassandra_tls():
            return ("the hop reaches a side over TLS, which the bulk loader"
                    " is not yet given")
    cols = eng._columns("src", db, table)
    if any(str(c.type) == "counter" for c in cols):
        return "the table holds counters, which are added to, never written"
    many = [c.column_name for c in cols
            if c.kind in ("regular", "static") and not eng._one_cell(c.type)]
    if many:
        return (f"{', '.join(sorted(many)[:3])} hold many cells each (a"
                " collection or a type not frozen), whose times the bulk"
                " loader cannot keep")
    return None


def _conf(ep):
    """The loader's configuration for one side, as HOCON: where it is and
    how it signs in."""
    lines = ["datastax-java-driver {",
             "  basic.contact-points = ["
             + json.dumps(f"{ep.host}:{int(ep.port or 9042)}") + "]"]
    if ep.user:
        lines += ["  advanced.auth-provider {",
                  "    class = PlainTextAuthProvider",
                  f"    username = {json.dumps(ep.user)}",
                  f"    password = {json.dumps(ep.password or '')}", "  }"]
    lines.append("}")
    return "\n".join(lines) + "\n"


def args(command, keyspace, table, workdir, workers, resume=None):
    """The loader's command line for one operation - `unload`, `load` or
    `count` - of one table: every value migkit decides, no address and no
    password (those are in the configuration file, `-f`)."""
    out = [PROGRAM, command, "-k", keyspace, "-t", table,
           "--log.directory", os.path.join(workdir, "logs"),
           "--log.maxErrors", "0",
           "--datastax-java-driver.basic.request.consistency", "LOCAL_QUORUM",
           "--executor.maxInFlight", str(64 * max(1, int(workers))),
           "--monitoring.reportRate", "5 seconds"]
    if command in ("unload", "load"):
        out += ["-c", "json", "--connector.json.url",
                os.path.join(workdir, "rows"),
                "--schema.preserveTimestamp", "true",
                "--schema.preserveTtl", "true",
                "--log.checkpoint.enabled", "true"]
    if command == "unload":
        out += ["--schema.splits",
                str(SPLITS_PER_WORKER * max(1, int(workers))),
                "--connector.json.maxConcurrentFiles", str(max(1, int(workers)))]
    if command == "count":
        out += ["--stats.modes", "global",
                "--schema.splits",
                str(SPLITS_PER_WORKER * max(1, int(workers)))]
    if resume:
        out += ["--log.checkpoint.file", resume,
                "--log.checkpoint.replayStrategy", "resume"]
    return out


_SUMMARY = re.compile(r"^\s*total\s*\|\s*failed\b", re.I)


def summary(text):
    """(total, failed) from the loader's closing table - `total | failed |
    rows/s | ...` and its line of numbers - or None where it printed
    none."""
    lines = (text or "").splitlines()
    for i, line in enumerate(lines):
        if _SUMMARY.match(line) and i + 1 < len(lines):
            cells = [c.strip().replace(",", "") for c in
                     lines[i + 1].split("|")]
            try:
                return int(cells[0]), int(cells[1])
            except (ValueError, IndexError):
                return None
    return None


def counted(text):
    """The rows a `count` in global mode found: the number it prints on
    a line of its own."""
    for line in reversed((text or "").splitlines()):
        got = line.strip().replace(",", "")
        if got.isdigit():
            return int(got)
    return None


def _checkpoint(workdir, command):
    """The checkpoint file an earlier run of `command` left, or None: the
    loader writes one into each operation's own directory under the log
    directory, named by the operation."""
    logs = os.path.join(workdir, "logs")
    if not os.path.isdir(logs):
        return None
    found = sorted((os.path.join(logs, d, "checkpoint.csv")
                    for d in os.listdir(logs)
                    if d.upper().startswith(command.upper())
                    and os.path.exists(os.path.join(logs, d,
                                                    "checkpoint.csv"))),
                   key=os.path.getmtime)
    return found[-1] if found else None


def workdir(eng, db, table):
    """The table's directory under the move's reports, this user's only:
    the rows read out wait there until they are loaded and the table is
    proved, so a stopped copy goes on from them."""
    path = eng.hop.report_dir(db) / "bulk-loader" / table
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    os.chmod(path, 0o700)
    return str(path)


def _run(argv, conf_text, log):
    from . import movers
    from .util import PrivateFile, tool_env
    with PrivateFile(conf_text, ".conf") as conf:
        cmd = [argv[0], argv[1], "-f", conf] + argv[2:]
        movers._debug(cmd)
        with movers._spawned(subprocess.Popen(
                cmd, env=tool_env(), text=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT), cmd[0]) as p:
            out = p.communicate()[0]
    if p.returncode:
        from .wording import without_programs
        raise RuntimeError(without_programs(out[-500:], movers.DRIVEN))
    return out


def copy_table(eng, db, table, workers, log):
    """One table out of the source and into the target by the bulk loader,
    proved by its own counts. Returns the rows copied; raises where it
    fails, leaving the rows read out for the next run to go on from."""
    ks_src, ks_dst = eng._ks("src", db), eng._ks("dst", db)
    work = workdir(eng, db, table)
    done_unload = os.path.exists(os.path.join(work, "unloaded.json"))
    if not done_unload:
        out = _run(args("unload", ks_src, table, work, workers,
                        _checkpoint(work, "unload")),
                   _conf(eng.hop.source), log)
        got = summary(out)
        if got is None or got[1]:
            raise RuntimeError(f"{table}: the read-out ended without its"
                               " count of rows, or with rows it could not"
                               " read")
        with open(os.path.join(work, "unloaded.json"), "w") as f:
            json.dump({"rows": got[0]}, f)
    read = json.load(open(os.path.join(work, "unloaded.json")))["rows"]
    resume = _checkpoint(work, "load")
    if resume is None:
        eng._session("dst").execute(
            f"truncate {eng._qualified('dst', db, table)}")
    out = _run(args("load", ks_dst, table, work, workers, resume),
               _conf(eng.hop.target), log)
    got = summary(out)
    if got is None or got[1]:
        raise RuntimeError(f"{table}: the load ended without its count of"
                           " rows, or with rows the target refused")
    held = counted(_run(args("count", ks_dst, table, work, workers),
                        _conf(eng.hop.target), log))
    if held != read:
        raise RuntimeError(f"{table}: {read:,} rows were read out and the"
                           f" target holds {held if held is not None else 'an unknown number of'}"
                           " after the load")
    shutil.rmtree(work, ignore_errors=True)
    return read


# ---- the account of what is used (backlog 0f) ----------------------------

_BOTH = ("-k", "-t", "-c", "-f", "--connector.json.url",
         "--connector.json.compression", "--connector.json.maxConcurrentFiles",
         "--connector.json.mode", "--schema.preserveTimestamp",
         "--schema.preserveTtl", "--schema.query", "--schema.mapping",
         "--schema.splits", "--schema.allowMissingFields",
         "--schema.allowExtraFields", "--schema.nullToUnset",
         "--log.directory", "--log.maxErrors", "--log.checkpoint.enabled",
         "--log.checkpoint.file", "--log.checkpoint.replayStrategy",
         "--executor.maxPerSecond", "--executor.maxInFlight",
         "--executor.continuousPaging.enabled",
         "--datastax-java-driver.basic.request.consistency",
         "--monitoring.reportRate",
         "--engine.dryRun", "--codec.timestamp", "--codec.binary",
         "--codec.nullStrings", "--stats.modes", "--stats.numPartitions",
         "-h", "-port", "-u", "-p")
#: the pinned release's documented settings (1.11), per operation
SURFACE = {c: frozenset(_BOTH) for c in ("unload", "load", "count")}


def _account():
    o = {}

    def put(commands, option, state, why):
        for c in commands:
            o[(c, option)] = (state, why)
    every = ("unload", "load", "count")
    rows = ("unload", "load")
    put(every, "-k", USED, "the keyspace, the target's under the hop's"
        " database map")
    put(every, "-t", USED, "one table at a time, the way climbed for it")
    put(every, "-f", USED, "the configuration file of this user's only:"
        " where each side is and how it signs in")
    for f in ("-h", "-port", "-u", "-p"):
        put(every, f, TOPPED, "given in the configuration file instead: a"
            " password on a command line is one every process can list")
    put(rows, "-c", USED, "json: a null and an empty text stay apart, and"
        " every value comes back as it went")
    put(("count",), "-c", UNUSED, "a count reads no files")
    put(rows, "--connector.json.url", USED, "the table's directory of this"
        " user's only, under the move's reports")
    put(("count",), "--connector.json.url", UNUSED, "a count reads no"
        " files")
    put(every, "--connector.json.compression", UNUSED, "not yet: the files"
        " are this machine's and short-lived, and packing them costs the"
        " processor the copy runs on - not measured here")
    put(("unload",), "--connector.json.maxConcurrentFiles", USED, "one file"
        " a worker, as many as migkit's sizing gives")
    put(("load", "count"), "--connector.json.maxConcurrentFiles", UNUSED,
        "the load reads what the read-out wrote, a file a worker already")
    put(every, "--connector.json.mode", UNUSED, "its default, a row a line,"
        " which the load reads back")
    put(rows, "--schema.preserveTimestamp", USED, "each cell keeps its own"
        " write time, so a later write on either side still wins")
    put(rows, "--schema.preserveTtl", USED, "each cell keeps the time left"
        " to it, so it expires on the target when it would on the source")
    put(("count",), "--schema.preserveTimestamp", UNUSED, "a count keeps"
        " nothing")
    put(("count",), "--schema.preserveTtl", UNUSED, "a count keeps nothing")
    put(every, "--schema.query", UNUSED, "the whole table by its name; a"
        " table migkit leaves out is never named")
    put(every, "--schema.mapping", UNUSED, "the target's table is made by"
        " the source's own definition, column for column")
    put(("unload", "count"), "--schema.splits", USED, "eight token ranges a"
        " worker, as migkit's own copier cuts them")
    put(("load",), "--schema.splits", UNUSED, "a load is split by its files")
    put(every, "--schema.allowMissingFields", UNUSED, "off: a row short of"
        " a column is a failure, never a row loaded short")
    put(every, "--schema.allowExtraFields", UNUSED, "off: a row with a"
        " column the target lacks is a failure")
    put(every, "--schema.nullToUnset", UNUSED, "its default, on: a null"
        " is left unset rather than written as a tombstone, as migkit's"
        " copier leaves it")
    put(every, "--log.directory", USED, "the table's own directory, where"
        " the checkpoint a stopped copy goes on from is kept")
    put(every, "--log.maxErrors", USED, "zero: a row it could not carry"
        " stops the copy, never skipped unsaid")
    put(rows, "--log.checkpoint.enabled", USED, "on: a stopped read-out or"
        " load goes on from where it stopped")
    put(rows, "--log.checkpoint.file", USED, "the checkpoint the stopped"
        " run left, found in its operation's directory")
    put(rows, "--log.checkpoint.replayStrategy", USED, "resume: only what"
        " the stopped run had not finished")
    put(("count",), "--log.checkpoint.enabled", UNUSED, "a count is read"
        " again whole")
    put(("count",), "--log.checkpoint.file", UNUSED, "a count is read"
        " again whole")
    put(("count",), "--log.checkpoint.replayStrategy", UNUSED, "a count is"
        " read again whole")
    put(every, "--executor.maxPerSecond", UNUSED, "migkit's sizing reads"
        " both clusters' strain before the copy and sets how much is in"
        " flight; a fixed rate cannot follow it")
    put(every, "--executor.maxInFlight", USED, "from the workers migkit's"
        " sizing gives, never left at its default")
    put(every, "--executor.continuousPaging.enabled", UNUSED, "only"
        " DataStax Enterprise pages continuously; Cassandra and ScyllaDB"
        " page as usual")
    put(every, "--datastax-java-driver.basic.request.consistency", USED,
        "LOCAL_QUORUM: a row read from the one replica that missed a"
        " write would be copied short")
    put(every, "--monitoring.reportRate", USED, "five seconds between its"
        " progress lines")
    put(every, "--engine.dryRun", UNUSED, "migkit's own dry run says what"
        " would run")
    for f in ("--codec.timestamp", "--codec.binary", "--codec.nullStrings"):
        put(every, f, UNUSED, "its defaults read back exactly what they"
            " wrote, which is all a copy between two clusters needs")
    put(("count",), "--stats.modes", USED, "global: the target's rows after"
        " the load, held to the rows the read-out read")
    put(rows, "--stats.modes", UNUSED, "a read-out and a load count their"
        " own rows in their closing line")
    put(every, "--stats.numPartitions", UNUSED, "partitions are not"
        " counted: the move compares every row after")
    return o


tooluse.register(PROGRAM, options=_account(), declared=SURFACE)
