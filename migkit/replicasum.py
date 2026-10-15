"""A MySQL target that is a native replica of the source, proved chunk by
chunk through the replication stream, and every chunk proved again by
migkit's own digest (backlog DEC.ptcs).

The checksum program runs against the source: each chunk's checksum is
written into a table there as a statement, so the replica runs the same
statement over its own rows and the two answers sit side by side in the
replica's copy of that table. Its chunks are its own (`--chunk-time`
sizes each to a target runtime), it pauses where the replica falls behind
(`--max-lag`, from migkit's throttle: the replica's lag before the check
plus the rise the throttle allows it, `throttle.replica_rise`) and where
the source is busy (`--max-load`, the throttle's share of the session
limit).

Never alone: each chunk it wrote is read back from the source's copy of
the table, its boundaries turned into the same range, and migkit's own
sum digest taken of that range on both servers, the replica's after it
has applied what the source had executed (`fence_wait`) and under the
same lag brake (`throttle.Throttle`). Both agreeing is the proof; either
disagreeing is a difference; a chunk that could not be read both ways is
not proved, never ok (`pair`).

Only where the target's rows arrive through the replication stream: a
target migkit writes itself (its own copy or tail, a renamed database, a
mapping) is not this rung's. The program's name is migkit's business; an
operator reads "replica checksum through the replication stream".
"""
import math
import re
import time
from dataclasses import dataclass

from . import decide, tooluse
from .tooluse import TOPPED, UNUSED

PROGRAM = "pt-table-checksum"
SAID = "replica checksum through the replication stream"
#: the program's own table, and migkit's own beside the hop's data where
#: that one cannot be written
THEIRS = ("percona", "checksums")
OWN_TABLE = "migkit_checksums"
#: index column types whose text survives the program's boundary column
#: and is compared by the server as the column's own value
EXACT_TYPES = {"tinyint", "smallint", "mediumint", "int", "integer",
               "bigint", "decimal", "date", "datetime", "timestamp",
               "time", "year", "char", "varchar"}
#: a chunk migkit's digest finds differing on a source being written is
#: read again, at most this many times, before it is a difference
GENERATIONS = 3


@dataclass
class Chunk:
    db: str
    table: str
    chunk: int
    index: str
    lower: object
    upper: object
    #: the program's two answers on the replica: (source, replica)
    counts: tuple
    crcs: tuple

    @property
    def program_agrees(self):
        if self.crcs[1] is None or self.counts[1] is None:
            return None
        return (self.counts[0] == self.counts[1]
                and self.crcs[0] == self.crcs[1])


def deserialize(text):
    """The program's boundary list as it serialises it: commas joined,
    a comma inside a value escaped by a backslash, NULL as `\\N`. None for
    no boundary."""
    if text is None:
        return None
    out = []
    for part in re.split(r"(?<!\\),", str(text)):
        if part == "\\N":
            out.append(None)
        else:
            out.append(part.replace("\\,", ",").replace("\\\\N", "\\N"))
    return out


def chunk_where(cols, lower, upper):
    """(where, params) of the rows a chunk holds, or None where its
    boundaries cannot be read back as the same rows: the program's ranges
    are lexicographic over the index's columns, inclusive at both ends; the
    first and last chunks hold what lies below the first boundary and
    above the last."""
    names = [c for c, _ in cols]
    if not names or any(t not in EXACT_TYPES for _, t in cols):
        return None
    lo, hi = deserialize(lower), deserialize(upper)
    for b in (lo, hi):
        # a value ending in a backslash may have swallowed the separator
        if b is not None and (len(b) != len(names) or None in b
                              or any(v.endswith("\\") for v in b)):
            return None
    row = "(" + ", ".join(f"`{n.replace('`', '``')}`" for n in names) + ")"
    marks = "(" + ", ".join(["%s"] * len(names)) + ")"
    if lo is not None and hi is not None:
        return f"{row} >= {marks} and {row} <= {marks}", (*lo, *hi)
    if hi is not None:
        return f"{row} < {marks}", tuple(hi)
    if lo is not None:
        return f"{row} > {marks}", tuple(lo)
    return None


def max_lag(base_lag, hop=None):
    """Seconds of replica lag the program waits below: where the replica
    stood before the check, and the rise migkit's throttle allows a read
    to cause on it."""
    from . import throttle
    base = float(base_lag or 0.0)
    return max(1, math.ceil(base + throttle.replica_rise(hop)))


def max_load(max_connections):
    """`--max-load` as migkit's throttle reads pressure: running threads
    at its share of the session limit."""
    from . import throttle
    n = max(1, int(throttle.BUSY_RATIO * int(max_connections)))
    return f"Threads_running={n}"


def argv(src_dsn, db, table, replicate, lag, load):
    """The program's command line for one table: the checksum statements
    written into `replicate`, replicas found by nobody (migkit reads the
    replica itself), lag checked on the target named in the DSN."""
    return [PROGRAM, "--replicate", replicate,
            "--recursion-method", "none",
            "--max-lag", str(lag), "--max-load", load,
            "--check-replica-lag", src_dsn[1],
            "--databases", db, "--tables", f"{db}.{table}",
            "--no-replicate-check", "--fail-on-stopped-replication",
            src_dsn[0]]


def pair(chunks, own):
    """Each chunk with migkit's own digests of it: (verdict, lines).

    `own` is {chunk number: ((rows, digest) source, (rows, digest)
    replica)}, or None for a chunk migkit could not read the same way.
    "diff" where either reading disagrees, "ok" only where both agree on
    every chunk, "warn" where a chunk was read one way only."""
    diff, open_ = [], []
    for c in chunks:
        mine = own.get(c.chunk)
        theirs = c.program_agrees
        ours = None if mine is None else (mine[0] == mine[1])
        where = f"{c.table} chunk {c.chunk}"
        if theirs is False and ours is False:
            diff.append(f"{where}: both readings differ")
        elif theirs is False:
            diff.append(f"{where}: the stream's checksum differs where"
                        " migkit's digest agrees")
        elif ours is False:
            diff.append(f"{where}: migkit's digest differs where the"
                        " stream's checksum agrees")
        elif theirs is None or ours is None:
            open_.append(f"{where}: read "
                         + ("by migkit's digest only" if theirs is None
                            else "by the stream's checksum only"))
    if diff:
        return "diff", diff + open_
    if open_ or not chunks:
        return "warn", open_ or ["no chunk came back from the replica"]
    return "ok", [f"{len(chunks)} chunks equal by both readings"]


# --- the rung --------------------------------------------------------------

def _need(what, key, why):
    return decide.Need(what, lambda f: bool(f.get(key)), why)


RUNGS = (
    decide.Rung(
        "replica checksum", SAID + ", every chunk held to migkit's own"
        " digest",
        gives=frozenset({"checksum", "consistent as of", "online"}),
        needs=(
            _need("a native replica", "native replica",
                  "{rung} needs the target to replicate from this source by"
                  " the server's own replication, and it does not"),
            decide.Need("a target migkit does not write",
                        lambda f: not f.get("migkit writes"),
                        lambda f: "{0} is not for a target migkit writes"
                        " itself: {1}".format(SAID, f.get("migkit writes"))),
            _need("the program installed", "installed",
                  "{rung} is not installed here (migkit doctor --install)"),
            _need("its session may write statements", "statement binlog",
                  "the source's account may not set its session's binlog"
                  " format, which {rung} writes its checksums in"),
            _need("a checksum table it may write", "replicate",
                  "{rung} needs a table on the source it may write: neither"
                  " the usual one nor migkit's own beside the data"),
        ),
        prove=lambda f: f.get("replica checksum failed"),
        because="the replica computes each chunk at the same point of the"
                " stream as the source, so a source still written is"
                " proved without being stopped",
        footprint=1, external=True,
        leaves="a checksum table on the source, replicated to the target"),
    decide.Rung(
        "own digest", "migkit's own digest of every table on both servers",
        gives=frozenset({"checksum", "byte exact"}),
        because="migkit's own digest, the proof everywhere else"),
)


def facts(eng, db):
    """The facts the rung reads, each asked of a server only when a need
    gets to it."""
    from .util import which

    def native():
        uuid = str(eng._q("src", "select @@server_uuid")[0][0]).lower()
        for r in eng._q_named("dst", "show replica status"):
            same = str(eng._repl_field(r, ("Source_UUID", "Master_UUID"))
                       or "").lower() == uuid
            sql = str(eng._repl_field(r, eng._REPL_FIELDS["sql"])).lower()
            if same and sql == "yes":
                return True
        return False

    def writes():
        why = eng.native_replica_unsafe() or ""
        if eng.maps_columns():
            why = why or "the hop maps or renames what it carries"
        return why

    def binlog():
        got = " ".join(str(r[0]).lower() for r in eng._q(
            "src", "show grants for current_user()"))
        return ("all privileges" in got or "super" in got
                or "session_variables_admin" in got
                or "system_variables_admin" in got)

    def replicate():
        got = eng._q("src", "select count(*) from information_schema.tables"
                            " where table_schema = %s and table_name = %s",
                     THEIRS)[0][0]
        grants = " ".join(str(r[0]).lower() for r in eng._q(
            "src", "show grants for current_user()"))
        if got and ("`percona`" in grants or "on *.*" in grants):
            return ".".join(THEIRS)
        return f"{db}.{OWN_TABLE}" if ("create" in grants
                                       or "all privileges" in grants) \
            else ""

    return decide.Facts({"installed": bool(which(PROGRAM))},
                        **{"native replica": native, "migkit writes": writes,
                           "statement binlog": binlog,
                           "replicate": replicate})


def _index_columns(eng, db, t, index):
    return [(r[0], str(r[1]).lower()) for r in eng._q(
        "src", "select s.column_name, c.data_type from"
               " information_schema.statistics s join"
               " information_schema.columns c on c.table_schema ="
               " s.table_schema and c.table_name = s.table_name and"
               " c.column_name = s.column_name where s.table_schema = %s"
               " and s.table_name = %s and s.index_name = %s order by"
               " s.seq_in_index", (db, t, index))]


def _own(eng, db, c, gate):
    """migkit's digest of one chunk on both servers, the replica's past
    the source's position and under the lag brake; None where the chunk
    is not read back as the same rows."""
    if not c.index:
        where, params = "", ()
    else:
        got = chunk_where(_index_columns(eng, db, c.table, c.index),
                          c.lower, c.upper)
        if got is None:
            return None
        where, params = got
    pair_ = None
    for _ in range(GENERATIONS):
        with gate.unit():
            src = tuple(eng._q("src", eng._range_digest_sql(
                "src", db, c.table, where), params or None)[0])
            at = str(eng._q("src", "select @@global.gtid_executed")[0][0]
                     or "")
            if at and eng.fence_wait(db, at, timeout=60) is False:
                return None
            dst = tuple(eng._q("dst", eng._range_digest_sql(
                "dst", db, c.table, where), params or None)[0])
        pair_ = (src, dst)
        if src == dst:
            break
    return pair_


def _chunks(eng, replicate, db, t):
    sch, tbl = replicate.split(".", 1)
    rows = eng._q("dst", f"select chunk, chunk_index, lower_boundary,"
                         f" upper_boundary, master_cnt, this_cnt,"
                         f" master_crc, this_crc from `{sch}`.`{tbl}`"
                         " where db = %s and tbl = %s order by chunk",
                  (db, t))
    return [Chunk(db, t, int(r[0]), r[1], r[2], r[3], (r[4], r[5]),
                  (r[6], r[7])) for r in rows]


def check(eng, db, log=print):
    """The deep check's `Result`, or None where the rung does not hold
    (said in the decision record, as every choice is)."""
    from .engines.base import Result
    from .movers import client_defaults, client_dsn
    from .throttle import Throttle
    from .util import quiet_argv, run
    f = facts(eng, db)
    costs = decide.HopCosts(eng.hop, "replica checksum/")
    got = decide.climb(f"{db}: verify", RUNGS, f, costs=costs)
    if got.path != "replica checksum":
        return None
    health = eng._health("dst")
    lag = max_lag(getattr(health, "lag_seconds", None), eng.hop)
    limit = eng._q("src", "select @@max_connections")[0][0]
    gate = Throttle(1, probe=lambda: eng._health("dst"))
    verdicts, lines, rows, t0 = [], [], 0, time.monotonic()
    s, tg = eng.hop.source, eng.hop.target
    for t in eng._tables("src", db):
        if eng.hop.excluded(db, t):
            continue
        with client_defaults(s, tg) as (fs, ft):
            cmd = argv((client_dsn(s, fs), client_dsn(tg, ft)), db, t,
                       f["replicate"], lag, max_load(limit))
            p = run(quiet_argv(cmd), check=False, timeout=None)
        chunks = _chunks(eng, f["replicate"], db, t)
        if p.returncode not in (0, 16) and not chunks:
            f["replica checksum failed"] = f"{t} could not be checksummed"
            return Result("deep", f"{db} replica checksum", "warn",
                          f"{SAID} did not run on {t}; migkit's own digest"
                          " stays the proof", "",
                          "migkit check reads every table with its own"
                          " digest")
        own = {c.chunk: _own(eng, db, c, gate) for c in chunks}
        verdict, said = pair(chunks, {k: v for k, v in own.items()
                                      if v is not None})
        verdicts.append(verdict)
        lines += said
        rows += sum(int(c.counts[0] or 0) for c in chunks)
    costs.saw("replica checksum", max(rows, 1), time.monotonic() - t0)
    status = ("diff" if "diff" in verdicts else
              "warn" if "warn" in verdicts or not verdicts else "ok")
    throttled = gate.line()
    return Result("deep", f"{db} replica checksum", status,
                  f"{SAID}, paired with migkit's own digest: "
                  + "; ".join(lines[:10])
                  + (f" ({throttled})" if throttled else ""))


# --- the program's account (backlog 0f) ------------------------------------

#: each option the program documents, with the token whose presence in
#: this file means it is used
_FEATURES = {o: o for o in (
    "--replicate", "--recursion-method", "--max-lag", "--max-load",
    "--check-replica-lag", "--databases", "--tables",
    "--no-replicate-check", "--fail-on-stopped-replication")}
_UNUSED = {
    "--chunk-time": "its own sizing is asked for, left at its default",
    "--chunk-size": "see --chunk-time",
    "--chunk-size-limit": "see --chunk-time",
    "--chunk-index": "its own choice of index is read back per chunk",
    "--chunk-index-columns": "see --chunk-index",
    "--where": "a hop's row filter is not this rung's, which needs whole"
               " tables replicated",
    "--columns": "every column is checksummed, as migkit's digest reads",
    "--ignore-columns": "see --columns",
    "--function": "its default hash; migkit's own digest is the second",
    "--float-precision": "float index columns are never read back as"
                         " chunks",
    "--trim": "trailing blanks are data the proof must hold",
    "--replicate-check-only": "migkit reads the replica's answers itself",
    "--replicate-check-retries": "see --replicate-check-only",
    "--replicate-database": "statements name their database in full",
    "--resume": "each run checksums its tables from the start again",
    "--empty-replicate-table": "left at its default, old answers cleared",
    "--truncate-replicate-table": "see --empty-replicate-table",
    "--create-replicate-table": "left at its default, made where absent",
    "--binary-index": "binary index columns are never read back as"
                      " chunks",
    "--explain": "the statements are run, not shown",
    "--progress": "its progress is not the operator's",
    "--quiet": "its output is not read at all",
    "--pause-file": "the throttle's brake pauses it instead",
    "--run-time": "a proof is complete or it is not one",
    "--retries": "left at its default for a transient error",
    "--check-interval": "left at its default of one second",
    "--recurse": "replicas are found by nobody; see --recursion-method",
    "--replica-user": "the replica is read by migkit's own connection",
    "--replica-password": "see --replica-user",
    "--channel": "the replica's one channel is the source's",
    "--check-binlog-format": "left at its default, on: a mismatch stops it",
    "--check-plan": "left at its default, on: a bad plan is skipped",
    "--check-replica-tables": "left at its default, on: a table missing on"
                              " the replica stops it",
    "--check-replication-filters": "left at its default, on: a filtered"
                                   " replica drops its statements",
    "--skip-check-replica-lag": "only one replica is checked for lag",
    "--replica-skip-tolerance": "left at its default for a table checked"
                                " whole",
    "--separator": "left at its default inside its own checksum",
    "--plugin": "no code of another runs inside a check",
    "--disable-qrt-plugin": "the server's plugins are left as they are",
    "--pid": "the run's own runner tracks the process",
    "--buffer-stdout": "its output is not read at all",
    "--version-check": "turned off, it would call home (quiet_argv)",
    "--engines": "every table the hop takes is checksummed",
    "--ignore-engines": "see --engines",
    "--databases-regex": "the hop names its databases one by one",
    "--ignore-databases": "see --databases-regex",
    "--ignore-databases-regex": "see --databases-regex",
    "--tables-regex": "the hop's scope names each table one by one",
    "--ignore-tables": "see --tables-regex",
    "--ignore-tables-regex": "see --tables-regex",
    "--charset": "the servers' own character sets are kept",
    "--set-vars": "its session settings are its own defaults",
    "--defaults-file": "each endpoint's file is given in its address",
    "--ask-pass": "nobody is there to type a password",
    "--password": "the password is in a private file, never an argument",
    "--host": "the address is given as one connection string",
    "--port": "see --host", "--user": "see --host", "--socket": "see --host",
    "--mysql_ssl": "the address carries the side's own encryption",
    "--config": "no configuration file of its own is read",
    "--check-slave-lag": "deprecated; see --check-replica-lag",
    "--skip-check-slave-lag": "see --check-slave-lag",
    "--slave-user": "see --check-slave-lag",
    "--slave-password": "see --check-slave-lag",
    "--check-slave-tables": "see --check-slave-lag",
}
_TOPPED = {
    "--max-lag": "the replica's own lag before the check plus the rise"
                 " migkit's throttle allows, not a fixed second",
    "--max-load": "the throttle's share of the source's session limit",
}

tooluse.register(
    PROGRAM, release="3.7.1",
    declared={"": frozenset({*_FEATURES, *_UNUSED})},
    reads=(("replicasum.py::argv",), {("", o): t for o, t in
                                     _FEATURES.items()}),
    options={**{("", o): (UNUSED, why) for o, why in _UNUSED.items()},
             **{("", o): (TOPPED, how) for o, how in _TOPPED.items()}})
