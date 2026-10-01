"""Every fact a choice may read, in one place (backlog P0-SMART, the owner's
note B of 2026-09-30: "every factor").

A rung's needs, its proof and its cost read the world through facts
(`decide.Facts`): a server's version, a grant, a setting, a program
installed, a table's shape, the link's round trip, the hop's limits,
what was measured before. Nothing knew the whole list, so nobody could
say what a decision stood on, nor what it did not know that would have
changed it. This is the list, as `hopkeys` is for a hop's options:
`tests/test_every_fact_is_declared.py` reads the code and fails on a fact
a need, a proof or a probe reads that is not declared here.

Each fact says what it is, its group, and how it comes to be known:
`given` (the hop's configuration or the caller), `read` (asked of a
server, a cloud or this machine), `measured` (timed here) or `predicted`
(the resource model or the knowledge base). A name ending in `*` is a
family (`change clone_plugin`, `rtt:src`).

`account` answers, for one decision, which facts were known, measured,
predicted and unknown - `doctor` and the decision record say it.
"""
from dataclasses import dataclass

GIVEN, READ, MEASURED, PREDICTED = "given", "read", "measured", "predicted"


@dataclass(frozen=True)
class Fact:
    what: str
    group: str
    how: str = READ


FACTS = {}


def declare(name, what, group, how=READ):
    """One fact more, for a module whose rungs read one of their own."""
    FACTS[name] = Fact(what, group, how)


for _name, _what, _group, _how in (
    # --- each side --------------------------------------------------------
    ("engine", "the engine of the hop, or of the side asked", "side",
     GIVEN),
    ("pair", "the engines of the source and the target", "side", GIVEN),
    ("version", "the server's version", "side", READ),
    ("src_version", "the source's version", "side", READ),
    ("dst_version", "the target's version", "side", READ),
    ("edition", "the server's edition", "side", READ),
    ("service", "the managed service a side runs as, if any", "side", READ),
    ("managed_src", "the managed service the source runs as", "side", READ),
    ("source_cloud", "the cloud the source runs in", "side", READ),
    ("src_system", "the source's system identifier", "side", READ),
    ("platform", "the source's operating system", "side", READ),
    ("settings", "a server's settings", "side", READ),
    ("wal_level", "PostgreSQL's wal_level on the source", "side", READ),
    ("log_bin", "whether MySQL writes its binary log", "side", READ),
    ("gtid_mode", "MySQL's GTID mode", "side", READ),
    ("enhanced_binlog", "Aurora MySQL's enhanced binary log", "side",
     READ),
    ("grants", "the privileges migkit's user holds on a side", "side",
     READ),
    ("src_backup_admin", "whether the source's user may take a backup",
     "side", READ),
    ("dst_clone_admin", "whether the target's user may clone", "side",
     READ),
    ("load", "the share of a server busy now", "side", MEASURED),
    ("replica_lag", "a replica's lag behind its primary, seconds", "side",
     READ),
    ("free_connections", "connections a server has free", "side", READ),
    ("free_memory", "memory a server has free", "side", READ),
    ("free_disk", "disk a server has free", "side", READ),
    ("cpus", "a server's CPUs", "side", READ),
    ("replica", "whether a side is a replica", "side", READ),
    ("tablespaces", "the source's tablespaces outside the data"
     " directory", "side", READ),
    ("unlogged", "the source's unlogged tables", "side", READ),
    ("not_innodb", "the source's tables not in InnoDB", "side", READ),
    ("src_subscriptions", "subscriptions on the source", "side", READ),
    ("dst_tables", "tables the target holds already", "side", READ),
    ("target_exists", "whether the target server exists yet", "side",
     READ),
    ("target_signs_in", "whether the target takes migkit's sign-in",
     "side", READ),
    ("our_seed", "whether the target was seeded by this hop", "side",
     READ),
    ("covers", "whether a seed covers every database the hop moves",
     "side", READ),
    ("listening", "whether the redo reader's listener answers", "side",
     READ),
    ("redo here", "whether the redo files are readable from here", "side",
     READ),
    ("truncatable", "whether the target's table may be emptied", "side",
     READ),
    ("streaming", "whether the target takes a streaming load", "side",
     READ),
    ("sdk", "the cloud's own library, where a cloud's copy is used", "side",
     READ),
    ("source", "the source side's own description", "side", GIVEN),
    ("options", "the hop's options", "side", GIVEN),
    ("stageable", "whether rows can be staged on the target", "side",
     READ),
    ("there", "whether a server-side object exists", "side", READ),
    ("reach", "whether the target reaches the source itself", "link",
     READ),
    # --- the link and this machine -----------------------------------------
    ("rtt", "the link's round trip, seconds", "link", MEASURED),
    ("rtt_ms", "the link's round trip, milliseconds", "link", MEASURED),
    ("rtt:*", "a side's round trip, seconds", "link", MEASURED),
    ("bandwidth", "what the link carries, bytes a second", "link",
     MEASURED),
    ("loss", "the share of packets sent again", "link", MEASURED),
    ("egress_price", "what a gigabyte crossing the link costs", "link",
     GIVEN),
    ("machine_cpus", "this machine's CPUs", "machine", READ),
    ("machine_memory", "this machine's free memory", "machine", READ),
    ("machine_disk", "this machine's free disk", "machine", READ),
    ("machine_network", "this machine's network, bytes a second",
     "machine", READ),
    ("programs", "the programs installed here", "machine", READ),
    ("program", "a program installed here", "machine", READ),
    ("available", "the ways that can run here, and why not the others",
     "machine", READ),
    ("client", "the client program's version", "machine", READ),
    ("driver", "the driver a side is written through", "machine", READ),
    ("terms", "the terms a tool's licence allows", "machine", GIVEN),
    # --- each table ---------------------------------------------------------
    ("bytes", "a table's bytes", "table", READ),
    ("rows", "a table's rows", "table", READ),
    ("widest", "the widest row, bytes", "table", READ),
    ("p99_row", "the 99th percentile row, bytes", "table", READ),
    ("lob_share", "the share of a table's bytes in large values", "table",
     READ),
    ("past a piece", "whether a batch holds a value past a piece"
     " (`pieces.PIECE`), which is written a piece a statement", "table",
     READ),
    ("key", "whether a table has a key, and its columns", "table", READ),
    ("keyed", "tables with an auto-increment key", "table", READ),
    ("keyless", "whether a table lacks a key", "table", READ),
    ("types", "a table's column types", "table", READ),
    ("binary", "why a table's types have no binary form, or nothing",
     "table", READ),
    ("kind", "what kind of object or value is read", "table", READ),
    ("partitions", "a table's partitions", "table", READ),
    ("index_bytes", "a table's indexes, bytes", "table", READ),
    ("fks", "a table's foreign keys", "table", READ),
    ("triggers", "a table's triggers", "table", READ),
    ("big", "whether a table is too large for one range", "table", READ),
    ("from", "where a table's rows are read from", "table", READ),
    ("same key", "whether both sides order the key alike", "table", READ),
    ("same engine", "whether both sides run the same engine", "table",
     READ),
    ("key pair", "the key columns on both sides", "table", READ),
    ("group column", "a column rows can be grouped by", "table", READ),
    ("wide sums", "whether the servers sum wide values exactly", "table",
     READ),
    ("sql", "the statement a way would run", "table", GIVEN),
    ("file", "the file a routine is read from", "table", GIVEN),
    ("one", "one routine's own facts", "table", READ),
    ("emulations", "the emulations a converted routine needs", "table",
     READ),
    ("expected", "the rows a proof expects", "table", MEASURED),
    ("disagreed", "whether two readings disagreed", "table", MEASURED),
    ("two readings", "whether a table is read a second way", "table",
     GIVEN),
    ("dump", "a dump of the table on disk", "table", READ),
    ("loads", "a load's own facts", "table", READ),
    ("loads left", "loads still to run", "table", READ),
    ("unfit", "why a bulk program cannot carry a table", "table", READ),
    ("unproved", "why a bulk load's result is not proved", "table", READ),
    ("loader", "whether the bulk loader is installed here", "machine",
     READ),
    ("export", "why the source cannot be exported, or nothing", "side",
     READ),
    ("import", "why the target cannot take an import, or nothing", "side",
     READ),
    ("mirror_unfit", "why the hop cannot be mirrored, or nothing", "side",
     READ),
    ("shake_filters", "why the keyspace copy cannot say the exclusions,"
     " or nothing", "side", READ),
    ("rdb_file", "the source's backup file the hop names", "side", GIVEN),
    ("aof_file", "the source's append-only file the hop names", "side",
     GIVEN),
    ("replica_refused", "why the source will not hand a replica its"
     " snapshot, or nothing", "side", READ),
    ("lanes", "whether a batch goes in lanes side by side, or how many"
     " connections share a snapshot", "table", READ),
    ("tables", "how many tables a pass reads", "table", READ),
    ("tags", "the tags of migkit's the source keeps on a table", "table",
     READ),
    ("refused", "why a table refused a way before, or nothing", "table",
     MEASURED),
    ("abs", "whether the target takes an absolute expiry", "side", READ),
    ("pick", "each column's rendering in the server's SQL, or nothing",
     "table", READ),
    ("proved", "whether the server rendered a table's first rows as"
     " migkit does", "table", MEASURED),
    ("seed", "the copy of the pages the hop allows, and why not", "side",
     READ),
    ("versions", "the source's and the target's versions", "side", READ),
    ("written", "whether the source was written while a table was read",
     "table", MEASURED),
    ("same_shape", "whether a table has the shape it had", "table", READ),
    ("lobs", "whether a table holds a large object, JSON or XML, which a"
     " bulk read carries none of", "table", READ),
    ("odd", "the columns of a batch whose types a bulk load carries none"
     " of", "table", READ),
    ("sessions", "whether migkit's user may read the list of sessions a"
     " system's parallel sessions need", "side", READ),
    ("filterable", "whether a service filters every field of a key",
     "table", READ),
    # --- the changes --------------------------------------------------------
    ("publication_risk", "whether the source's publication would lose a"
     " table's changes", "change", READ),
    ("at", "where a table left the tail", "change", READ),
    ("rereads", "whether the source's log is read again from a position",
     "change", READ),
    ("digest", "what a table's target held when it left the tail",
     "change", READ),
    ("lost", "whether the source's log no longer holds a position",
     "change", READ),
    ("change rate", "changes arriving on the source a second", "change",
     MEASURED),
    ("tail", "the change stream following the source", "change", READ),
    ("repl", "the servers' own replication", "change", READ),
    ("ready", "whether the source is ready to be followed", "change",
     READ),
    ("broker", "the stream a pair delivers changes to", "change", GIVEN),
    ("drop", "whether the stream is torn down", "change", GIVEN),
    ("change *", "where a change on the source stands", "change", READ),
    ("answer *", "the operator's answer to a change on the source",
     "change", GIVEN),
    # --- the hop's limits ---------------------------------------------------
    ("deadline", "seconds the move must be done in", "limits", GIVEN),
    ("source_load", "the share of the source the move may take", "limits",
     GIVEN),
    ("money", "the money the move may spend", "limits", GIVEN),
    ("bytes_billed", "the bytes a warehouse may bill", "limits", GIVEN),
    ("outage", "the write outage the cutover may take, seconds", "limits",
     GIVEN),
    ("target_memory", "the target's memory the move may take", "limits",
     GIVEN),
    ("target_connections", "the target's connections the move may take",
     "limits", GIVEN),
    ("mapping", "the hop's mapping of tables and columns", "limits",
     GIVEN),
    # --- history ------------------------------------------------------------
    ("costs", "what each way cost here before", "history", MEASURED),
    ("measured", "rates measured on this path", "history", MEASURED),
    ("failures", "ways that failed here, by engine, version and service",
     "history", MEASURED),
    ("kept", "the way a restart stands on", "history", GIVEN),
    ("why not", "why a way failed here before", "history", MEASURED),
    ("own replication failed", "why the servers' own replication failed",
     "history", MEASURED),
    ("sketch failed", "why the sketch of differences did not add up",
     "history", MEASURED),
    ("groups failed", "why the grouped narrowing failed", "history",
     MEASURED),
    ("bisection failed", "why the bisection failed", "history", MEASURED),
    # --- what a need reads through ------------------------------------------
    ("hop", "the hop", "context", GIVEN),
    ("db", "the database", "context", GIVEN),
    ("go", "whether the run writes", "context", GIVEN),
    ("src", "the source side's engine", "context", GIVEN),
    ("dst", "the target side's engine", "context", GIVEN),
    ("reader", "the second reader", "context", GIVEN),
    ("row hash reader", "the servers' own row hash reader", "context",
     GIVEN),
    ("bisection reader", "the bisection's reader", "context", GIVEN),
    ("localizer", "the localizer chosen for the table", "context", GIVEN),
    # --- the cutover: a side's writes held, its drain, its stream back -------
    ("others", "the server's databases, topics or indices the hop does not"
     " move", "side", READ),
    ("alter system", "whether migkit's account may change the server's"
     " settings (ALTER SYSTEM)", "side", READ),
    ("read only switch", "whether migkit's account may switch the server"
     " read-only", "side", READ),
    ("actions", "the server-wide actions migkit's account holds", "side",
     READ),
    ("acl", "whether the server keeps accounts of its own", "side", READ),
    ("alter", "whether migkit's account may alter the database", "side",
     READ),
    ("alone in its group", "whether the database stands outside any"
     " availability group", "side", READ),
    ("control", "whether migkit's account may deny rights and give them"
     " back", "side", READ),
    ("kill", "whether migkit's account may end other sessions", "side",
     READ),
    ("authorizer", "whether the cluster checks access", "side", READ),
    ("authorize", "whether migkit's role may grant and revoke on the"
     " keyspace", "side", READ),
    ("cache", "the seconds a cluster's permission cache holds a grant",
     "side", READ),
    ("stream back", "whether the hop starts a stream back at cutover",
     "context", GIVEN),
    ("budget", "the cutover's budget for the write outage, in seconds",
     "limits", GIVEN),
    ("cutover", "whether the hop turns cutover: on", "context", GIVEN),
    ("operator", "whether the source is frozen by its operator", "context",
     GIVEN),
    ("positions", "whether the stream keeps a position for every database",
     "side", READ),
    ("why none", "why the stream keeps no position", "side", READ),
    ("mark", "why a mark cannot be written and read on the pair, or"
     " nothing", "side", READ),
    ("arms", "whether the stream back's engine lets it wait idle", "side",
     READ),
    # --- a table's repair (`repairways`) and how rows are written ----------
    ("named", "the rows the last check named that differ", "table", READ),
    ("int key", "whether a table's key is one integer column on both"
     " sides", "table", READ),
    ("per row", "why the hop decides a table's rows one by one, or"
     " nothing", "context", GIVEN),
    ("filter", "whether the hop filters a table's rows", "context", GIVEN),
    ("replace", "whether the hop lets a copy replace what a table holds",
     "context", GIVEN),
    ("range", "why a span of a table's key cannot be copied again the"
     " move's way, or nothing", "table", READ),
    ("spans", "the spans of a table's key that differ enough to copy"
     " again", "table", READ),
    ("spans why", "why no span of a table's key is copied again, or"
     " nothing", "table", READ),
    ("writers", "what else writes the target, or nothing", "side", READ),
    ("copier", "whether the move copies the table", "table", READ),
    ("copier why", "why the move cannot copy the table, or nothing",
     "table", READ),
    ("quiet", "how the target's user may keep its triggers and foreign"
     " keys quiet while rows are written", "side", READ),
    ("write refused", "why rows may not be written into the target's"
     " tables at all, or nothing", "side", READ),
    # --- the scenarios of a move (`tableengine`, `consolidate`) -------------
    ("there", "whether the target has the table already", "table", READ),
    ("rule", "how the hop keeps the keys of its sources apart", "context",
     GIVEN),
    ("clash", "whether two sources hold one key of a table", "table",
     READ),
    ("integer", "whether a table's key is one whole-number column",
     "table", READ),
):
    declare(_name, _what, _group, _how)

#: the factors a move's plan weighs, each a fact a decision is unknown
#: without - what `account` names as unknown where it was not there
FACTORS = ("engine", "pair", "version", "service", "load", "replica_lag",
           "free_connections", "free_memory", "free_disk", "rtt",
           "bandwidth", "loss", "egress_price", "machine_cpus",
           "machine_memory", "bytes", "rows", "widest", "lob_share", "key",
           "types", "partitions", "index_bytes", "change rate", "deadline",
           "source_load", "money", "outage", "costs", "failures")


def declared(name):
    """Whether `name` is in the catalogue, itself or as a family's."""
    if name in FACTS:
        return True
    for k in FACTS:
        if k.endswith("*") and str(name).startswith(k[:-1]):
            return True
    return False


def fact(name):
    got = FACTS.get(name)
    if got is not None:
        return got
    for k, f in FACTS.items():
        if k.endswith("*") and str(name).startswith(k[:-1]):
            return f
    return None


def undeclared(names):
    """The names of `names` the catalogue does not declare."""
    return sorted({str(n) for n in names if not declared(n)})


def account(known=(), measured=(), predicted=(), factors=FACTORS):
    """{known, measured, predicted, unknown: [fact]} for one decision:
    what it read as given or asked, what it stood on as measured, what it
    stood on as predicted - and the plan's factors it had none of, each
    one that would change it once known."""
    known = set(map(str, known))
    measured = set(map(str, measured))
    predicted = set(map(str, predicted)) - measured
    have = known | measured | predicted
    return {"known": sorted(known - measured - predicted),
            "measured": sorted(measured), "predicted": sorted(predicted),
            "unknown": [f for f in factors if f not in have]}


def said(acc):
    """One line of an account."""
    parts = []
    for k in ("known", "measured", "predicted", "unknown"):
        got = acc.get(k) or []
        if got:
            parts.append(f"{k} {len(got)}"
                         + (f" ({', '.join(got[:8])}"
                            + (" ..." if len(got) > 8 else "") + ")"
                            if k != "known" else ""))
    return "; ".join(parts)
