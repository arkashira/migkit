"""Proof that a table has not moved since it was last proved equal.

Verifying a migration is not done once. During a CDC window it is done again
and again, and every run re-hashes every table from the beginning - including
the ones nobody has written to since the last time they came out equal. On an
estate where a handful of large tables are quiet and a couple of hundred small
ones are busy, that is the difference between hours and seconds.

The idea is cheap. The danger is not.

A marker that says "unchanged" when the table did change turns a skipped scan
into a table that was never verified but is reported as equal. That is a false
negative, and a verifier that produces one has failed at its only job. So the
rule here is stricter than anywhere else in migkit:

**Any doubt at all means re-verify.** The marker is compared for exact
equality, not for "has it grown" - a counter that went *down* (a statistics
reset, a crash) is as much a reason to look again as one that went up. An
engine that cannot produce a trustworthy marker returns `None`, which never
skips anything. A marker is never carried across a change of server version.

What the marker is made of was measured rather than assumed, on PostgreSQL 16:

    change      n_tup_ins/upd/del   relfilenode   size
    UPDATE      moves               same          same
    DELETE      moves               same          same
    TRUNCATE    **does not move**   changes       8192 -> 0
    stats reset drops to 0/0/0      same          same

TRUNCATE is the one that matters: it is invisible to the tuple counters, so a
marker built only from them would skip an emptied table and call it equal.
`relfilenode` catches it, and is in the marker for that reason.

One more limit, deliberately: **the final proof never skips.** The saving is
for the interim runs during the window, where the question is "has anything
drifted since the last look". When the source is frozen and the answer is
going into a cutover decision, every table is read.
"""

# PostgreSQL only publishes its per-table statistics through a collector that,
# before version 15, sent them over UDP and could silently drop a message
# under load. A dropped UPDATE is exactly the false negative this module
# exists to avoid, so the marker is refused on older servers rather than
# trusted. Shared-memory statistics landed in 15.
MIN_PG_MAJOR = 15

class Counters:
    """Where one kind of server keeps its modification counters, and what
    has to hold before they are believed.

    A table is skipped on a measured fact that holds now - the counters are
    the ones the last proof saw (same server start, not reset, not
    rewritten), the server lets them be read, the source is not held for a
    cutover - and never because of what the server is called. The pieces
    are named, so a table that is read again can be told why.
    """
    kind = ""
    family = ""
    pieces = ()
    #: the pieces that are counters: down is a reset, up is a write
    counted = ()
    meaning = {}
    #: why this kind's counters are not read yet; "" where they are
    owed = ""

    def refusal(self, facts, role="src"):
        """Why the counters of a server that answered `facts` are not to be
        believed, or ''. Asked before a marker is built."""
        return self.owed

    def dead(self, marker):
        """Why a marker is the same string forever (a server that keeps no
        such counters), or ''."""
        return ""

    def parts(self, marker):
        got = str(marker).split("|")
        return dict(zip(self.pieces, got)) if len(got) == len(self.pieces) \
            else {}

    def explain(self, before, now):
        """What moved between two markers, in words."""
        a, b = self.parts(before), self.parts(now)
        if not a or not b:
            return ["the marker is not one this kind of server writes"]
        said = []
        for piece in self.pieces:
            if a[piece] == b[piece]:
                continue
            text = self.meaning.get(piece, f"{piece} changed")
            if piece in self.counted:
                try:
                    went = "down" if float(b[piece]) < float(a[piece]) \
                        else "up"
                except ValueError:
                    went = "up"
                text = text[went]
            said.append(f"{text} ({a[piece]} -> {b[piece]})")
        return said

    def evidence(self, marker):
        """What the marker says, for the line that skips a table."""
        return ""


class PostgresCounters(Counters):
    kind = family = "postgres"
    pieces = ("inserts", "updates", "deletes", "file", "size", "columns")
    counted = ("inserts", "updates", "deletes")
    meaning = {
        "inserts": {"up": "rows were inserted",
                    "down": "the statistics were reset"},
        "updates": {"up": "rows were updated",
                    "down": "the statistics were reset"},
        "deletes": {"up": "rows were deleted",
                    "down": "the statistics were reset"},
        "file": "the table was emptied or rewritten",
        "size": "the table's size changed",
        "columns": "a column was added, dropped or changed"}

    def refusal(self, facts, role="src"):
        if not usable_postgres(facts.get("server_version")):
            return (f"statistics before PostgreSQL {MIN_PG_MAJOR} can lose"
                    " a message")
        return ""

    def dead(self, marker):
        if self.parts(marker).get("file") == "0":
            return ("the server keeps no file identity per table, so an"
                    " emptied table would look the same")
        return ""


class SqlServerCounters(Counters):
    """`sys.dm_db_index_usage_stats` counts the statements that wrote an
    index (`user_updates`) and is cleared when the server starts or the
    database closes, so it is only read beside the server's start time. The
    table's row count and pages (`sys.dm_db_partition_stats`) catch what it
    does not count, a TRUNCATE; the statistics' modification counter
    (`sys.dm_db_stats_properties`), which the server keeps through a
    restart, catches a clearing of the usage counters that no restart
    explains.

    A table with no usage row has no marker (no counter is not zero writes),
    and neither has a database that closes when idle.

    Not caught: a row changed in place, in a column no statistics lead
    with, while the database was taken offline and online with no restart.
    Not measured on a server.
    """
    kind = family = "mssql"
    pieces = ("started", "created", "updates", "last_write", "rows",
              "pages", "columns", "shape", "altered", "stat_changes",
              "stat_at", "scope")
    counted = ("updates", "rows", "pages", "stat_changes")
    meaning = {
        "started": "the server restarted, so its counters began again",
        "created": "the database was created again",
        "updates": {"up": "the table was written to",
                    "down": "the usage counters were cleared"},
        "last_write": "a write was counted after the proof",
        "rows": {"up": "rows were added", "down": "rows were removed"},
        "pages": {"up": "the table grew", "down": "the table shrank"},
        "columns": "a column was added or dropped",
        "shape": "a column was changed",
        "altered": "the table was altered",
        "stat_changes": {"up": "rows were modified",
                         "down": "the statistics were rebuilt"},
        "stat_at": "the statistics were rebuilt",
        "scope": "the hop's scope changed"}

    def refusal(self, facts, role="src"):
        if facts.get("started") in (None, "", "NULL"):
            return "the server does not say when it started"
        if str(facts.get("auto_close")) != "0":
            return "the database closes when idle, which clears its counters"
        if str(facts.get("state")) != "0":
            return "the database is not online"
        if role == "src" and str(facts.get("read_only")) != "0":
            return ("the source is held read-only: the proof that goes into"
                    " a cutover reads every table")
        return ""

    def dead(self, marker):
        if self.parts(marker).get("started") in ("", "NULL"):
            return "the server does not say when it started"
        return ""

    def evidence(self, marker):
        p = self.parts(marker)
        return (f"server up since {p['started']}, {p['updates']} writes"
                f" counted, the last {p['last_write']}, {p['rows']} rows")


class MySqlCounters(Counters):
    """What `performance_schema.table_io_waits_summary_by_table` counts per
    table - rows inserted, updated, deleted since the server started - with
    the table's creation time and InnoDB table id, both new when a table is
    emptied or rebuilt (a TRUNCATE moves no row counter the marker could
    see), and its columns. The server's start (the Unix second it began,
    from its own uptime) is in it, so counters that began again after a
    restart are told from counters that did not move. A table with no
    counter row, a server whose instrument is off, or a source held
    read-only has no marker.

    Not measured on a server: the statements are written from the
    server's documentation (stub tests only)."""
    kind = family = "mysql"
    pieces = ("started", "inserts", "updates", "deletes", "created",
              "table_id", "columns")
    counted = ("inserts", "updates", "deletes")
    meaning = {
        "started": "the server restarted, so its counters began again",
        "inserts": {"up": "rows were inserted",
                    "down": "the counters were cleared"},
        "updates": {"up": "rows were updated",
                    "down": "the counters were cleared"},
        "deletes": {"up": "rows were deleted",
                    "down": "the counters were cleared"},
        "created": "the table was emptied, rebuilt or created again",
        "table_id": "the table was emptied or rebuilt",
        "columns": "a column was added, dropped or changed"}
    #: the server's start as the Unix second, and whether the table
    #: instrument is on (read before any marker)
    FACTS_SQL = (
        "select unix_timestamp(now()) - cast(g.VARIABLE_VALUE as signed),"
        " (select count(*) from performance_schema.setup_instruments where"
        " NAME = 'wait/io/table/sql/handler' and ENABLED = 'YES'),"
        " @@global.read_only from performance_schema.global_status g where"
        " g.VARIABLE_NAME = 'Uptime'")
    #: one row a table: schema, table, the pieces after `started`
    ROWS_SQL = (
        "select t.TABLE_SCHEMA, t.TABLE_NAME, i.COUNT_INSERT, i.COUNT_UPDATE,"
        " i.COUNT_DELETE, t.CREATE_TIME, n.TABLE_ID, (select"
        " concat(count(*), ':', md5(group_concat(c.ORDINAL_POSITION, ':',"
        " c.COLUMN_NAME, ':', c.COLUMN_TYPE, ':', c.IS_NULLABLE, ':',"
        " coalesce(c.COLLATION_NAME, '') order by c.ORDINAL_POSITION)))"
        " from information_schema.COLUMNS c where c.TABLE_SCHEMA ="
        " t.TABLE_SCHEMA and c.TABLE_NAME = t.TABLE_NAME) from"
        " information_schema.TABLES t left join"
        " performance_schema.table_io_waits_summary_by_table i on"
        " i.OBJECT_TYPE = 'TABLE' and i.OBJECT_SCHEMA = t.TABLE_SCHEMA and"
        " i.OBJECT_NAME = t.TABLE_NAME left join"
        " information_schema.INNODB_TABLES n on n.NAME = concat("
        "t.TABLE_SCHEMA, '/', t.TABLE_NAME) where t.TABLE_TYPE ="
        " 'BASE TABLE' and t.TABLE_SCHEMA not in ('mysql', 'sys',"
        " 'information_schema', 'performance_schema')")

    def refusal(self, facts, role="src"):
        if facts.get("started") in (None, "", "NULL"):
            return "the server does not say when it started"
        if str(facts.get("instrument")) in ("0", "None", ""):
            return ("the server does not count rows per table"
                    " (performance_schema's table instrument is off)")
        if role == "src" and str(facts.get("read_only")) not in ("0", "OFF"):
            return ("the source is held read-only: the proof that goes into"
                    " a cutover reads every table")
        return ""

    def dead(self, marker):
        p = self.parts(marker)
        if p.get("inserts") in ("", "NULL", "None"):
            return "the table has no row counter, and no counter is not zero"
        if p.get("table_id") in ("", "NULL", "None"):
            return ("the table keeps no identity, so an emptied table would"
                    " look the same")
        return ""

    def evidence(self, marker):
        p = self.parts(marker)
        return (f"server up since {p['started']}, {p['inserts']} inserts,"
                f" {p['updates']} updates, {p['deletes']} deletes counted")

    def modified_sql(self, schema, table):
        """The rows InnoDB counted modified since it last recalculated the
        table's statistics: a floor under what moved since the histogram,
        not a count of it, as a recalculation clears it."""
        name = f"{schema}/{table}".replace("\\", "\\\\").replace("'", "''")
        return ("select MODIFIED_COUNTER from"
                f" information_schema.INNODB_TABLESTATS where NAME = '{name}'")


class Unread(Counters):
    """A kind whose counters are not read yet: every table is read."""

    def __init__(self, kind, family, owed):
        self.kind, self.family, self.owed = kind, family, owed


#: the counters of each kind of server; a kind not here has none read, and
#: a table of it is read every time
COUNTERS = {c.kind: c for c in (
    PostgresCounters(),
    SqlServerCounters(),
    # Measured on CockroachDB v23.2.5, which speaks the PostgreSQL protocol:
    # `pg_stat_all_tables` exists and is empty and `pg_class.relfilenode` is
    # 0 for every relation, so a marker built there is the same string
    # forever and every table would skip its scan on every run
    Unread("cockroachdb", "postgres",
           "pg_stat_all_tables is empty and relfilenode is 0 on every"
           " table, and its own views count reads (index usage) and row"
           " estimates, not writes: no counter of rows modified is exposed"
           " to build a marker on, so a table's statistics are judged by"
           " the rows they counted against the rows now"
           " (`ranges.cut_with`)"),
    # a coordinator's statistics describe shell tables whose rows live on
    # the workers
    Unread("citus", "postgres",
           "the coordinator's statistics describe shell tables, not the"
           " rows on the workers"),
    MySqlCounters(),
)}

#: the kinds whose counters are declared and neither read nor proved
UNPROVED = {k: c.owed for k, c in COUNTERS.items() if c.owed}


def usable_brand(brand):
    """Whether a marker from this software may be trusted at all: the
    counters of its kind are read (`COUNTERS`). An unidentified server is
    refused for the same reason a kind not listed is: "is this safe to
    skip" cannot be answered yes when "what counters does it keep" is not
    known."""
    entry = COUNTERS.get(getattr(brand, "name", None))
    return (entry is not None and not entry.owed
            and entry.family == getattr(brand, "family", None))


def server_major(text):
    """Major version from what the engine reported, or None - read the one
    way every version is (`toolversion.server_major`)."""
    from .toolversion import server_major as major
    return major(text)


def usable_postgres(server_version):
    """Whether this server's statistics can be trusted for a marker."""
    maj = server_major(server_version)
    return maj is not None and maj >= MIN_PG_MAJOR


def marker(parts):
    """One comparable string from the pieces an engine collected.

    `None` anywhere in the pieces makes the whole marker `None`: a marker with
    a hole in it is not a marker, and treating a missing piece as an empty one
    would let two different tables compare equal.
    """
    if parts is None:
        return None
    parts = list(parts)
    if not parts or any(p is None for p in parts):
        return None
    return "|".join(str(p).replace("|", "/") for p in parts)


def unchanged(before, after):
    """True only when both markers exist and are exactly equal.

    Exact equality, not ordering: a counter that went down means the server
    lost its statistics, which is a reason to read the table again rather than
    a reason to relax.
    """
    if not before or not after:
        return False
    return str(before) == str(after)


def skippable(stored, src_now, dst_now):
    """True when both sides prove they have not moved since `stored`.

    `stored` is what was recorded when the table last came out equal, as
    `{"src": marker, "dst": marker}`. Both sides have to match: a target that
    someone wrote to is exactly the case the boundary check exists for, and
    skipping it because the source is quiet would hide it.
    """
    if not isinstance(stored, dict):
        return False
    return (unchanged(stored.get("src"), src_now)
            and unchanged(stored.get("dst"), dst_now))


class Judgement:
    """Whether a table may be skipped, and why: the evidence when it may,
    what moved or what cannot be relied on when it may not."""

    def __init__(self, skip, why):
        self.skip, self.why = skip, why

    def __bool__(self):
        return self.skip


def judge(kind, stored, src_now, dst_now):
    """`skippable`, for a kind of server in `COUNTERS`, with the reason.

    Every refusal is a sentence, so a table that is read again says why: no
    earlier proof, a marker that cannot be built, counters that are dead on
    this server, or the pieces that moved since the proof (a restart, a
    reset, a write).
    """
    entry = COUNTERS.get(kind)
    if entry is None or entry.owed:
        return Judgement(False, (entry.owed if entry else "")
                         or f"no counters are read for {kind}")
    if not isinstance(stored, dict) or not stored:
        return Judgement(False, "no earlier proof of this table")
    said = []
    for side, now in (("src", src_now), ("dst", dst_now)):
        before = stored.get(side)
        where = "source" if side == "src" else "target"
        if not now:
            return Judgement(False, f"the {where}'s counters cannot be read"
                                    " for this table")
        dead = entry.dead(now)
        if dead:
            return Judgement(False, f"the {where}: {dead}")
        if not before:
            return Judgement(False, "no earlier proof of this table")
        if not unchanged(before, now):
            moved = "; ".join(entry.explain(before, now))
            return Judgement(False, f"the {where} moved since the proof:"
                                    f" {moved}")
        said.append(f"{where} {entry.evidence(now)}".strip())
    return Judgement(True, "; ".join(said))


def record(src_now, dst_now):
    """What to store alongside a table that just came out equal, or None."""
    if not src_now or not dst_now:
        return None
    return {"src": src_now, "dst": dst_now}


class Proof:
    """Which tables were proved equal, and the marker they carried then.

    Kept beside the reports rather than in the checkpoint: a checkpoint holds
    partial work inside one run and is cleared the moment a table finishes,
    which is exactly when this needs to start remembering.

    A table is dropped from the store the moment it comes out different. A
    marker recorded against a table that does not match is not weaker
    evidence - it is evidence of the wrong thing, and keeping it would let the
    next run skip a table that is known to be wrong.
    """

    def __init__(self, path):
        import json
        import pathlib as _p
        self.path = _p.Path(path)
        self.data = {}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text())
                if isinstance(loaded, dict):
                    self.data = loaded
            except Exception:
                # an unreadable store proves nothing, and guessing at its
                # contents would be guessing about what was verified
                self.data = {}

    def get(self, table):
        v = self.data.get(str(table))
        return v if isinstance(v, dict) else None

    def set(self, table, src_now, dst_now):
        rec = record(src_now, dst_now)
        if rec:
            self.data[str(table)] = rec
        else:
            self.drop(table)

    def drop(self, table):
        self.data.pop(str(table), None)

    def save(self):
        import json
        import os
        import tempfile
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(self.data, f, indent=1, sort_keys=True)
            os.replace(tmp, self.path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
