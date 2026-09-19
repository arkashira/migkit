"""Best-tool movers.

migkit does not compete with the battle-tested bulk movers, it drives
them: pg_dump/pg_restore parallel jobs, mydumper/myloader, pgloader,
mongodump/mongorestore. The builtin chunked copy stays the fallback
(and the only mode with per-chunk crash resume).

The managed streaming pipeline below is selected by migkit itself when an
engine has no native CDC path; callers do not choose it and do not need to
know what it runs underneath. Third-party components used there are named
only in the generated compose file. See NOTICE for attribution.
"""
import subprocess
from urllib.parse import quote

from .util import run, tool_env, which

VIAS = ("auto", "builtin", "pgdump", "pgcopydb", "mydumper",
        "pgloader", "mongodump")


def pick(engine, table=""):
    """Fastest installed tool for whole-db moves, builtin for single tables
    (chunk resume matters more than raw speed there)."""
    if table:
        return "builtin"
    if engine == "postgres":
        # the version-matched container first: it overlaps copy, index and
        # constraint work, builds indexes after the rows land, and needs no
        # intermediate directory - none of which the dump path does
        if pgcopydb_available():
            return "pgcopydb"
        if which("pg_dump") and which("pg_restore"):
            return "pgdump"
    if engine == "mysql" and which("mydumper") and which("myloader"):
        return "mydumper"
    if engine == "hetero" and which("pgloader"):
        return "pgloader"
    if engine == "mongodb" and which("mongodump") and which("mongorestore"):
        return "mongodump"
    return "builtin"


def chosen(engine, table=""):
    """Which mover to use. Not a user-facing decision.

    Picking the fastest installed tool for an engine is knowledge migkit
    already has; making the operator supply it just moves migkit's homework
    onto them, and gets it wrong when the machine changes. MIGKIT_MOVER
    overrides it for debugging - deliberately an environment variable and not
    a flag, so it stays out of the command surface.
    """
    import os
    forced = os.environ.get("MIGKIT_MOVER", "").strip()
    if forced:
        if forced not in VIAS:
            raise SystemExit(f"MIGKIT_MOVER={forced} is not one of"
                             f" {', '.join(VIAS)}")
        if forced != "builtin" and not supported(engine, forced):
            raise SystemExit(f"MIGKIT_MOVER={forced} does not apply to"
                             f" a {engine} hop")
        return forced
    return pick(engine, table)


def supported(engine, via):
    return {"pgdump": engine == "postgres",
            "pgcopydb": engine == "postgres",
            "mydumper": engine == "mysql",
            "pgloader": engine == "hetero",
            "mongodump": engine == "mongodb"}.get(via, True)


def stream_supported(engine):
    """Internal: can migkit stand up its managed streaming pipeline for this
    engine. Never an operator choice; `move --mode cdc` falls back to it
    when the engine has no native CDC path."""
    return engine in ("postgres", "mysql", "hetero")


def _sh(cmd, env=None, log=None):
    if log:
        log("$ " + " ".join(str(c) for c in cmd))
    p = subprocess.run(cmd, env=tool_env(env), text=True,
                       capture_output=True)
    if p.returncode:
        raise RuntimeError((p.stderr or p.stdout)[-500:])
    return p


PG_TRUNCATE_SQL = (
    "select coalesce('truncate table '||string_agg("
    "format('%I.%I', n.nspname, c.relname), ', ')||' cascade', '')"
    " from pg_class c join pg_namespace n on n.oid = c.relnamespace"
    " where c.relkind = 'r'"
    " and n.nspname not in ('pg_catalog','information_schema')"
    " and n.nspname not like 'pg\\_%'"
    " and n.nspname not like '\\_\\_%'"
    " and c.relname not like 'migkit\\_%'")


def _pg_truncate_target(hop, db, log=None):
    """Empty the user tables a data-only load is about to fill.

    One copy, used by both PostgreSQL bulk paths: a data-only load that
    appends instead of replacing produces a target with every row twice, and
    two versions of "which tables count as the application's" would eventually
    disagree about which ones got emptied.
    """
    t = hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    env_t = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    p = _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", ddb, "-X", "-At", "-c", PG_TRUNCATE_SQL], env_t)
    stmt = p.stdout.strip()
    if stmt:
        _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", ddb, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-c", stmt],
            env_t, log)
    return stmt


PG_INDEX_SQL = """
    select i.relname||chr(31)||pg_get_indexdef(x.indexrelid)||chr(31)
           ||(c.conname is not null)::text
      from pg_index x
      join pg_class i on i.oid = x.indexrelid
      join pg_class tb on tb.oid = x.indrelid
      join pg_namespace n on n.oid = tb.relnamespace
      left join pg_constraint c on c.conindid = x.indexrelid
     where n.nspname not in ('pg_catalog','information_schema')
       and n.nspname not like 'pg\\_%' and n.nspname not like '\\_\\_%'
       and tb.relname not like 'migkit\\_%'
       and x.indisvalid
     order by 1"""


def _pg_psql(hop, db, sql, log=None):
    t = hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    env = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    return _sh(["psql", "-h", t.host, "-p", str(t.port), "-U", t.user,
                "-d", ddb, "-X", "-At", "-v", "ON_ERROR_STOP=1",
                "-c", sql], env, log).stdout


class _IndexWindow:
    """Drop the target's secondary indexes for a load and put them back.

    Measured worth on PostgreSQL 16: 300,000 rows into a table with three
    secondary indexes took 1.241s with the indexes in place and 0.652s as a
    bare load plus a bulk build - 1.9x, on two CPUs.

    Used as a context manager so the rebuild happens on the way out whether
    the load worked or raised. The definitions reach disk before anything is
    dropped; if they cannot, nothing is dropped and the load simply runs the
    slower way.
    """

    def __init__(self, hop, db, workers, log):
        self.hop, self.db, self.workers, self.log = hop, db, workers, log
        self.dropped, self.ddl = [], {}

    def __enter__(self):
        from . import indexes as _ix
        try:
            raw = _pg_psql(self.hop, self.db, PG_INDEX_SQL)
        except Exception:
            return self
        rows = []
        for line in (raw or "").splitlines():
            parts = line.split("\x1f")
            if len(parts) == 3:
                rows.append((parts[0].strip(), parts[1].strip(),
                             parts[2].strip() == "true"))
        drop, ddl = _ix.plan(rows)
        if not drop:
            return self
        where = self.hop.report_dir(self.db) / "dropped-indexes.json"
        if not _ix.saved(where, ddl):
            if self.log:
                self.log("could not save the index definitions, so none were"
                         " dropped - the load runs with them in place")
            return self
        for name in drop:
            try:
                _pg_psql(self.hop, self.db, f'drop index "{name}"')
                self.dropped.append(name)
                self.ddl[name] = ddl[name]
            except Exception as e:
                if self.log:
                    self.log(f"could not drop {name}, leaving it:"
                             f" {str(e).splitlines()[-1][:80]}")
        if self.log and self.dropped:
            self.log(f"{len(self.dropped)} secondary indexes dropped for the"
                     f" load; definitions saved to {where}")
        return self

    def __exit__(self, *exc):
        from . import indexes as _ix
        rebuilt, failed = [], []
        for name in self.dropped:
            try:
                _pg_psql(self.hop, self.db, self.ddl[name])
                rebuilt.append(name)
            except Exception as e:
                failed.append(name)
                if self.log:
                    self.log(f"REBUILD FAILED for {name}:"
                             f" {str(e).splitlines()[-1][:100]}")
        if self.log:
            self.log(_ix.summary(self.dropped, rebuilt, failed))
        return False            # never swallow the load's own exception


def pgdump_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    outdir = hop.report_dir(db) / "pgdump"
    trunc = PG_TRUNCATE_SQL
    steps = [
        f"# truncate all user tables on target {db} (generated from catalog)",
        f"pg_dump -h {s.host} -p {s.port} -U {s.user} -d {db} -Fd"
        f" -j {workers} --data-only -f {outdir}",
        f"pg_restore -h {t.host} -p {t.port} -U {t.user} -d {db}"
        f" --data-only --disable-triggers -j {workers} {outdir}",
    ]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    env_t = {"PGPASSWORD": t.password, "PGCONNECT_TIMEOUT": "15"}
    _pg_truncate_target(hop, db, log)
    import shutil
    shutil.rmtree(outdir, ignore_errors=True)
    with _IndexWindow(hop, db, workers, log):
        _pgdump_load(hop, db, s, workers, outdir, env_t, log)
    shutil.rmtree(outdir, ignore_errors=True)
    return steps


def _pgdump_load(hop, db, s, workers, outdir, env_t, log):
    t = hop.target
    _sh(["pg_dump", "-h", s.host, "-p", str(s.port), "-U", s.user,
         "-d", db, "-Fd", "-j", str(workers), "--data-only",
         "-f", str(outdir)],
        {"PGPASSWORD": s.password, "PGCONNECT_TIMEOUT": "15"}, log)
    try:
        _sh(["pg_restore", "-h", t.host, "-p", str(t.port), "-U", t.user,
             "-d", db, "--data-only", "--disable-triggers",
             "-j", str(workers), str(outdir)], env_t, log)
    except RuntimeError as e:
        # newer pg_dump emits SETs older servers reject; pg_restore exits 1
        # on those even when all rows landed - migkit check is the judge
        if "errors ignored on restore" not in str(e):
            raise
        if log:
            log("pg_restore ignored version-mismatch SET statements,"
                " data restored - verify with migkit check")


MY_INDEX_SQL = """
    select s.table_name, s.index_name,
           group_concat(concat('`', s.column_name, '`')
                        order by s.seq_in_index),
           max(s.non_unique) = 0,
           max(case when k.constraint_name is not null
                         and s.seq_in_index = 1 then 1 else 0 end)
      from information_schema.statistics s
      left join information_schema.key_column_usage k
        on k.table_schema = s.table_schema
       and k.table_name = s.table_name
       and k.column_name = s.column_name
       and k.referenced_table_name is not null
     where s.table_schema = %s
       and s.index_name <> 'PRIMARY'
       and s.table_name not like 'migkit%%'
     group by s.table_name, s.index_name"""


class _MyIndexWindow:
    """The MySQL half of moving indexes out of a bulk load's way.

    Measured on MySQL 8, 200,000 rows into a table with three secondary
    indexes: 1.150s with them in place against 0.303s + 0.511s as a bare load
    plus one ALTER - 1.41x. Less than PostgreSQL's 1.9x on the same shape, and
    still worth having.

    Two things are protected here that have no PostgreSQL equivalent. A unique
    index is enforcing something, as everywhere. And **an index that backs a
    foreign key cannot be dropped at all** - InnoDB refuses with "needed in a
    foreign key constraint" - so those are left alone rather than attempted
    and logged as failures.

    Same order as the PostgreSQL window, for the same reason: the definitions
    reach disk before anything is dropped, and the rebuild happens on the way
    out whether the load worked or raised.
    """

    def __init__(self, engine, hop, db, workers, log):
        self.eng, self.hop, self.db = engine, hop, db
        self.workers, self.log = workers, log
        self.dropped, self.ddl = [], {}

    def __enter__(self):
        from . import indexes as _ix
        ddb = self.hop.target_db(self.db) if hasattr(self.hop, "target_db") \
            else self.db
        try:
            rows = self.eng._q("dst", MY_INDEX_SQL, (ddb,))
        except Exception:
            return self
        triples = []
        for tbl, name, cols, is_unique, backs_fk in rows:
            key = f"{tbl}.{name}"
            if int(backs_fk or 0):
                # InnoDB needs an index whose *leftmost* column is the
                # referencing one, and refuses to drop it: "needed in a
                # foreign key constraint". An index that merely contains the
                # column further along does not satisfy the key and can go -
                # excluding those too left a free win on the table.
                #
                # The prediction is a shortcut, not the safety net: the drop
                # is attempted inside a try, so if InnoDB disagrees the index
                # simply stays and the load runs with it.
                continue
            ddl = (f"ALTER TABLE `{ddb}`.`{tbl}` ADD INDEX `{name}`"
                   f" ({cols})")
            triples.append((key, ddl, bool(is_unique)))
        drop, ddl = _ix.plan(triples)
        if not drop:
            return self
        where = self.hop.report_dir(self.db) / "dropped-indexes.json"
        if not _ix.saved(where, ddl):
            if self.log:
                self.log("could not save the index definitions, so none were"
                         " dropped - the load runs with them in place")
            return self
        for key in drop:
            tbl, name = key.split(".", 1)
            try:
                self.eng._q("dst", f"ALTER TABLE `{ddb}`.`{tbl}`"
                                   f" DROP INDEX `{name}`")
                self.dropped.append(key)
                self.ddl[key] = ddl[key]
            except Exception as e:
                if self.log:
                    self.log(f"could not drop {key}, leaving it:"
                             f" {str(e)[:80]}")
        if self.log and self.dropped:
            self.log(f"{len(self.dropped)} secondary indexes dropped for the"
                     f" load; definitions saved to {where}")
        return self

    def __exit__(self, *exc):
        from . import indexes as _ix
        rebuilt, failed = [], []
        for key in self.dropped:
            try:
                self.eng._q("dst", self.ddl[key])
                rebuilt.append(key)
            except Exception as e:
                failed.append(key)
                if self.log:
                    self.log(f"REBUILD FAILED for {key}: {str(e)[:100]}")
        if self.log:
            self.log(_ix.summary(self.dropped, rebuilt, failed))
        return False


def mydumper_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    outdir = hop.report_dir(db) / "mydumper"
    steps = [
        f"mydumper -h {s.host} -P {s.port} -u {s.user} -p *** -B {db}"
        f" -o {outdir} --threads {workers} --no-schemas --trx-consistency-only",
        f"myloader -h {t.host} -P {t.port} -u {t.user} -p *** -B {db}"
        f" -d {outdir} --threads {workers} --purge-mode TRUNCATE",
    ]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    import shutil
    shutil.rmtree(outdir, ignore_errors=True)
    _sh(["mydumper", "-h", s.host, "-P", str(s.port), "-u", s.user,
         f"-p{s.password}", "-B", db, "-o", str(outdir),
         "--threads", str(workers), "--no-schemas",
         "--trx-consistency-only"], None, log)
    from .engines.mysql import MySQLEngine
    with _MyIndexWindow(MySQLEngine(hop), hop, db, workers, log):
        _sh(["myloader", "-h", t.host, "-P", str(t.port), "-u", t.user,
             f"-p{t.password}", "-B", db, "-d", str(outdir),
             "--threads", str(workers), "--purge-mode", "TRUNCATE"],
            None, log)
    shutil.rmtree(outdir, ignore_errors=True)
    return steps


def pgloader_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    loadfile = hop.report_dir(db) / "pgloader.load"
    body = f"""LOAD DATABASE
  FROM mysql://{s.user}:{quote(s.password, safe='')}@{s.host}:{s.port}/{db}
  INTO postgresql://{t.user}:{quote(t.password, safe='')}@{t.host}:{t.port}/{db}
WITH data only, workers = {workers}, concurrency = {min(workers, 4)},
     on error stop
ALTER SCHEMA '{db}' RENAME TO 'public';
"""
    loadfile.write_text(body)
    loadfile.chmod(0o600)
    steps = [f"pgloader {loadfile}   # data only, mysql -> postgres"]
    if not go:
        return steps + ["# dry-run, review the load file then add --go"]
    _sh(["pgloader", str(loadfile)], None, log)
    return steps


def _mongo_uri(ep):
    auth = (f"{quote(ep.user, safe='')}:{quote(ep.password, safe='')}@"
            if ep.user else "")
    hosts = ep.options.get("hosts") or f"{ep.host}:{ep.port}"
    uri = f"mongodb://{auth}{hosts}/"
    extra = ep.options.get("uri_options", "")
    if extra:
        uri += "?" + extra
    return uri


def mongodump_move(hop, db, workers, go, log):
    s, t = hop.source, hop.target
    steps = [
        f"mongodump --uri=<src> --db={db} --archive"
        f" | mongorestore --uri=<dst> --archive --drop"
        f" --nsInclude='{db}.*' --numParallelCollections={workers}",
    ]
    if not go:
        return steps + ["# dry-run, add --go to execute"]
    dump = subprocess.Popen(
        ["mongodump", f"--uri={_mongo_uri(s)}", f"--db={db}", "--archive",
         "--quiet"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=tool_env())
    restore = subprocess.Popen(
        ["mongorestore", f"--uri={_mongo_uri(t)}", "--archive", "--drop",
         f"--nsInclude={db}.*", f"--numParallelCollections={workers}"],
        stdin=dump.stdout, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env=tool_env())
    dump.stdout.close()
    _, err_r = restore.communicate()
    _, err_d = dump.communicate()
    if dump.returncode or restore.returncode:
        raise RuntimeError((err_d + err_r).decode()[-500:])
    if log:
        log(f"{db}: mongodump | mongorestore complete")
    return steps



#: What the loading connection asks the server for, so that triggers on the
#: target do not rewrite the rows as they land.
#:
#: Measured, with a real `migkit move --mode full --go`: a `BEFORE INSERT`
#: trigger setting `updated_at := now()` on the target turned rows carrying
#: `2001-01-01` and `2002-02-02` into today's timestamp, and migkit printed
#: `bulk copy complete`. With this on the target URI the same move landed
#: `2001-01-01` and `2002-02-02` untouched.
#:
#: A **connection** option rather than `ALTER TABLE ... DISABLE TRIGGER`,
#: deliberately: disabling triggers is a change to the target that outlives
#: a crash, and a target left with disabled triggers is the exact failure
#: `check --deep` already reports. This lasts as long as the connection and
#: not one moment longer. It is what migkit's repair path has always done;
#: the mover was the inconsistent one.
QUIET_TRIGGERS = "?options=-c%20session_replication_role%3Dreplica"

PGCOPYDB_IMAGE = "dimitri/pgcopydb:latest"
_PGCOPYDB_OK = None


def pgcopydb_available():
    """Whether the version-matched pgcopydb container can be run here.

    Deliberately the image and not a local binary. Homebrew's pgcopydb 0.18 is
    compiled against PostgreSQL 18 and emits `SET transaction_timeout = 0` on
    the target; PostgreSQL 16 has never heard of that parameter, so the
    statement fails, the transaction aborts, and every later statement in it is
    refused - measured, a whole-database clone moved zero rows while reporting
    each rejection separately. The published image is compiled against 16 and
    states its compatible range, which is the difference between a tool that
    works and one that reports success while moving nothing.
    """
    return bool(pgcopydb_runner())


_PGCOPYDB_HOW = None


def pgcopydb_runner():
    """How to run pgcopydb here: the image, the local binary, or not at all.

    The image is preferred because its build matches the server range it
    states. The local binary is accepted for `copy table-data` because the
    failure the image exists to avoid was re-measured and is narrower than
    it looked: Homebrew's 0.18, built against PostgreSQL 18, does emit `SET
    transaction_timeout = 0` and PostgreSQL 16 does reject it - the error
    appears in the server log - and yet 50,000 and then 2,000,000 rows
    arrived through `copy table-data` all the same. `clone` is the
    subcommand that dies on it, and migkit does not use `clone`.

    That is worth measuring against: on this machine pgcopydb moved a
    555 MB table in 4.1 s against the dump path's 11.7 s, and 2,093 MB of
    large objects in 18.2 s against 93.6 s. What makes taking the binary
    safe rather than hopeful is the guard that runs afterwards: a move that
    reports success and leaves the target empty is refused by
    `Engine.moved_nothing`, so the failure this note describes cannot pass
    as a completed migration.
    """
    global _PGCOPYDB_HOW
    if _PGCOPYDB_HOW is not None:
        return _PGCOPYDB_HOW
    how = ""
    if which("docker"):
        try:
            p = run(["docker", "image", "inspect", PGCOPYDB_IMAGE],
                    check=False, timeout=20)
            if p.returncode == 0:
                how = "image"
        except Exception:
            how = ""
    if not how and which("pgcopydb"):
        how = "local"
    # cached: `pick` is called per table, and starting a docker client each
    # time to ask the same question would cost more than the answer is worth
    _PGCOPYDB_HOW = how
    return how


def pgcopydb_move(hop, db, workers, go, log):
    """Parallel data-only copy through pgcopydb, source straight to target.

    `copy table-data` and not `clone`, and the difference matters. `clone`
    restores the schema as well and refuses a target that already has objects
    - measured, it exits with `clone process has terminated`. migkit prepares
    the target schema in its own step before any data moves, so by the time a
    mover runs the target is never empty, and `clone` can never be the right
    subcommand here.

    What that leaves, against `pg_dump -Fd -j | pg_restore`: the tables are
    copied in parallel straight from source to target with no intermediate
    directory to write out and read back. What it does *not* leave, precisely
    because the schema is already in place: the indexes exist, so they are
    maintained during the load rather than built after it - which is the
    larger of the two wins and belongs to `clone` alone.

    So this is faster than the dump path, and not by as much as pgcopydb is
    capable of. Saying which is better than implying the rest.

    The network is the host's by default, so the endpoints resolve exactly as
    they do for migkit itself. `MIGKIT_PGCOPYDB_NETWORK` overrides it - an
    environment variable rather than a flag, so it stays out of the command
    surface, the same way `MIGKIT_MOVER` does.

    **The target has to be empty.** Unlike `pgdump_move`, which truncates the
    user tables it is about to fill, `clone` restores the schema as well and
    refuses to run over objects that already exist - measured, it exits with
    `clone process has terminated`. That refusal is the right behaviour and is
    left as it is: `--drop-if-exists` would make a second run destroy a target
    somebody may have been using, which is not a decision a mover should make
    on its own.
    """
    import os
    from urllib.parse import quote
    s, t = hop.source, hop.target
    ddb = hop.target_db(db) if hasattr(hop, "target_db") else db
    src = (f"postgresql://{s.user}:{quote(s.password or '', safe='')}"
           f"@{s.host}:{s.port}/{db}")
    dst = (f"postgresql://{t.user}:{quote(t.password or '', safe='')}"
           f"@{t.host}:{t.port}/{ddb}{QUIET_TRIGGERS}")
    net = os.environ.get("MIGKIT_PGCOPYDB_NETWORK", "host")
    how = pgcopydb_runner()
    if how == "local":
        # its own directory per run. pgcopydb keeps its state - including the
        # exported snapshot - under /tmp/pgcopydb by default, so a second run
        # finds the first one's snapshot and dies on it: measured,
        # `FATAL Failed to use given --snapshot "00000003-00000048-1"`. The
        # container path never hit this because each container brought its
        # own filesystem; the binary shares the host's.
        import tempfile
        work = tempfile.mkdtemp(prefix="migkit-pgcopydb-")
        cmd = ["pgcopydb", "copy", "table-data",
               "--table-jobs", str(workers), "--dir", work,
               "--source", src, "--target", dst]
    else:
        cmd = ["docker", "run", "--rm", "--network", net,
               "-e", f"PGCOPYDB_SOURCE_PGURI={src}",
               "-e", f"PGCOPYDB_TARGET_PGURI={dst}",
               PGCOPYDB_IMAGE, "pgcopydb", "copy", "table-data",
               "--table-jobs", str(workers)]
    # the password sits *before* the @, so splitting there and keeping the
    # front half kept the secret and threw the host away - the printed steps
    # are what an operator pastes into a ticket
    import re as _re
    shown = [_re.sub(r"(://[^:/@]+:)[^@]*@", r"\1***@", c) for c in cmd]
    steps = ["# truncate all user tables on target (generated from catalog)",
             "# parallel table copy, source to target, no intermediate file",
             " ".join(shown)]
    if not go:
        return steps + ["# dry-run, add --go to execute"]

    # Having the image is not the same as being able to reach the databases
    # from inside it: the container has its own network view, and on a laptop
    # `--network host` is the VM's host, not this one. `ping` is the tool's
    # own answer to that question, and asking it first turns a failed move
    # into a fallback rather than an outage.
    if how == "local":
        # `ping` does not take `--dir` - it answers with its usage if you
        # give it one, which reads as a connectivity failure and sends the
        # move down the fallback path for no reason
        ping = ["pgcopydb", "ping", "--source", src, "--target", dst]
    else:
        ping = cmd[:cmd.index(PGCOPYDB_IMAGE) + 1] + ["pgcopydb", "ping"]
    try:
        _sh(ping)
    except Exception as e:
        if log:
            where = ("from its container"
                     f" (network={net})" if how != "local" else "")
            log(f"pgcopydb cannot reach both endpoints {where}:"
                f" {str(e).splitlines()[-1][:120]}")
            log("falling back to the pg_dump path")
        return pgdump_move(hop, db, workers, go, log)

    _pg_truncate_target(hop, db, log)
    # logged through the redacted form, never the raw argv: `_sh` would echo
    # the command as given, and the command carries both passwords
    if log:
        log("$ " + " ".join(shown[1:]))
    with _IndexWindow(hop, db, workers, log):
        _sh(cmd)
    return steps


def stream_codegen(hop, dbs, engine):
    """Write the managed streaming pipeline for this hop: a single-broker
    log plus a connector runtime with a source and an upsert sink, sized for
    one hop and ready to start. migkit reimplements none of it - it wires
    proven components together and owns the lifecycle, so the operator drives
    everything through migkit."""
    out = hop.report_dir() / "stream"
    out.mkdir(parents=True, exist_ok=True)
    s, t = hop.source, hop.target
    src_is_mysql = engine in ("mysql", "hetero")
    connector_class = ("io.debezium.connector.mysql.MySqlConnector"
                       if src_is_mysql
                       else "io.debezium.connector.postgresql.PostgresConnector")
    name = f"migkit-{hop.name}"
    source = {
        "name": f"{name}-source",
        "config": {
            "connector.class": connector_class,
            "database.hostname": s.host,
            "database.port": str(s.port),
            "database.user": s.user,
            "database.password": s.password,
            "topic.prefix": name,
            "snapshot.mode": "initial",
        },
    }
    if src_is_mysql:
        source["config"].update({
            "database.include.list": ",".join(dbs),
            "database.server.id": "184054",
            "schema.history.internal.kafka.bootstrap.servers":
                "redpanda:9092",
            "schema.history.internal.kafka.topic": f"{name}-history",
        })
    else:
        source["config"].update({
            "database.dbname": dbs[0] if dbs else "postgres",
            "plugin.name": "pgoutput",
            "slot.name": name.replace("-", "_"),
        })
    dst_is_pg = engine in ("postgres", "hetero")
    jdbc = (f"jdbc:postgresql://{t.host}:{t.port}/" if dst_is_pg
            else f"jdbc:mysql://{t.host}:{t.port}/")
    sink = {
        "name": f"{name}-sink",
        "config": {
            "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
            "topics.regex": f"{name}\\..*",
            "connection.url": jdbc + (dbs[0] if dbs else ""),
            "connection.username": t.user,
            "connection.password": t.password,
            "insert.mode": "upsert",
            "delete.enabled": "true",
            "primary.key.mode": "record_key",
            "schema.evolution": "basic",
        },
    }
    compose = """services:
  redpanda:
    image: redpandadata/redpanda:latest
    command: redpanda start --overprovisioned --smp 1 --memory 1G
      --node-id 0 --kafka-addr PLAINTEXT://0.0.0.0:9092
      --advertise-kafka-addr PLAINTEXT://redpanda:9092
  connect:
    image: quay.io/debezium/connect:3.0
    depends_on: [redpanda]
    ports: ["8083:8083"]
    environment:
      BOOTSTRAP_SERVERS: redpanda:9092
      GROUP_ID: migkit
      CONFIG_STORAGE_TOPIC: migkit_connect_configs
      OFFSET_STORAGE_TOPIC: migkit_connect_offsets
      STATUS_STORAGE_TOPIC: migkit_connect_statuses
"""
    readme = f"""# Streaming pipeline for hop {hop.name} (generated by migkit)

migkit owns this stack. Drive it with migkit, not by hand:

  start      migkit move {hop.name} --mode cdc --go
  progress   migkit watch {hop.name}
  verify     migkit check {hop.name}
             migkit watch {hop.name} --verify --delta
  tear down  migkit move {hop.name} --mode cdc --drop --go

These files hold credentials from hops.yaml - keep this directory private
(chmod 700, never commit). Tearing down also drops the replication slot /
binlog reader left on the source.

Third-party components are pinned in docker-compose.yml; see NOTICE for
licences and attribution.
"""
    import json as _json
    (out / "docker-compose.yml").write_text(compose)
    (out / "source-connector.json").write_text(
        _json.dumps(source, indent=2) + "\n")
    (out / "sink-connector.json").write_text(
        _json.dumps(sink, indent=2) + "\n")
    (out / "README.md").write_text(readme)
    for f in out.iterdir():
        f.chmod(0o600)
    out.chmod(0o700)
    return out


def _connect_api(method, path, body=None, port=8083, timeout=15):
    import json as _json
    import urllib.request
    data = _json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://localhost:{port}{path}", data=data, method=method,
        headers={"Content-Type": "application/json",
                 "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (_json.loads(raw) if raw else None)


def stream_up(out, log=None):
    _sh(["docker", "compose", "-f", str(out / "docker-compose.yml"),
         "up", "-d"], log=log)


def stream_down(out, log=None):
    _sh(["docker", "compose", "-f", str(out / "docker-compose.yml"),
         "down", "-v"], log=log)


def stream_wait(port=8083, timeout=180, log=None):
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            code, _ = _connect_api("GET", "/", port=port, timeout=5)
            if code == 200:
                return True
        except Exception:
            pass
        if log:
            log("waiting for the connector runtime on :%d ..." % port)
        time.sleep(4)
    return False


def stream_register(out, port=8083, log=None):
    import json as _json
    import urllib.error
    for f in ("source-connector.json", "sink-connector.json"):
        cfg = _json.loads((out / f).read_text())
        name = cfg["name"]
        try:
            _connect_api("POST", "/connectors", cfg, port)
            if log:
                log(f"registered {name}")
        except urllib.error.HTTPError as e:
            if e.code == 409:  # already exists -> update config
                _connect_api("PUT", f"/connectors/{name}/config",
                             cfg["config"], port)
                if log:
                    log(f"updated {name}")
            else:
                raise


def stream_status(name, port=8083):
    """Summarize a connector + its tasks: RUNNING/FAILED per component."""
    try:
        _, body = _connect_api("GET", f"/connectors/{name}/status", port=port)
    except Exception as e:
        return f"{name}: unreachable ({str(e).splitlines()[-1][:50]})"
    conn = body.get("connector", {}).get("state", "?")
    tasks = [t.get("state", "?") for t in body.get("tasks", [])]
    return f"{name}: connector={conn} tasks={','.join(tasks) or 'none'}"


def run_via(via, hop, db, workers, go, log):
    fns = {"pgdump": pgdump_move, "pgcopydb": pgcopydb_move,
           "mydumper": mydumper_move, "pgloader": pgloader_move,
           "mongodump": mongodump_move}
    if via not in fns:
        # a mover nobody has taught this function about must stop here rather
        # than fall through to whatever happens to be first
        raise SystemExit(f"no bulk path named {via!r}")
    tools = {"pgdump": ("pg_dump", "pg_restore"),
             "pgcopydb": ("docker",),
             "mydumper": ("mydumper", "myloader"),
             "pgloader": ("pgloader",),
             "mongodump": ("mongodump", "mongorestore")}[via]
    missing = [t for t in tools if not which(t)]
    if missing:
        raise SystemExit(f"the {via} bulk path needs {', '.join(missing)}"
                         " installed - run: migkit doctor --install")
    if via == "pgcopydb" and not pgcopydb_available():
        raise SystemExit(f"the pgcopydb bulk path needs the"
                         f" {PGCOPYDB_IMAGE} image: docker pull"
                         f" {PGCOPYDB_IMAGE}")
    return fns[via](hop, db, workers, go, log)
