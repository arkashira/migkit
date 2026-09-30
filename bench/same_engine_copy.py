"""The same-engine table copier's ways, timed against each other on 1 GB
and 5 GB tables (backlog R20 items 1, 4 and 5).

    python tools/with_docker_lock.py .venv/bin/python bench/same_engine_copy.py postgres
    python tools/with_docker_lock.py .venv/bin/python bench/same_engine_copy.py mysql

`postgres`: two PostgreSQL 16 servers; a table of `(bigint key, int, text,
numeric, timestamptz, bytea)` made by `generate_series` - 8,000,000 rows
(about 1 GB) and 40,000,000 (about 5 GB). Each timed into an empty target
by the pipe (COPY text through migkit, tallied), the pass-through, the
pass-through with its commits unawaited, and the streaming copy of the
whole table (pgcopydb's `copy table-data` under the table's snapshot),
with 2 and 4 workers; the whole table in one transaction with `FREEZE`
and without; and the choice the table's own trials make with every way
there. Then two tables of 3,000,000 rows with two secondary indexes each,
copied one after the other: their builds inline with the server's own
settings, inline with the session settings `_pg_build_settings` gives,
and beside the next table's copy.

`mysql`: two MySQL 8.4 servers (`local_infile` on); a table of `(bigint
key, int, varchar, decimal, datetime(6), varbinary, bigint unique)` of the
same two sizes. Timed: the table copier's rows, the pass-through, the
pass-through with the target's binlog left off (`target_has_replicas:
false`), the parallel dump and load of the table (mydumper/myloader), and
for the 1 GB table the whole table in one transaction with
`unique_checks = 0` and with them kept, 2 workers, and the measured choice.
The 5 GB table's target keeps no binlog: the machine's disk holds the
table on both sides and not a log of it as well.

Every run is held equal afterwards (a sum of row hashes on both sides) and
prints its wall time, MB/s, rows/s and this process's CPU seconds against
its programs'. Containers `migkit-test-pass-*` on 127.0.0.1:16180-16186,
removed after. `SIZES` (rows, comma-separated) and `INDEX_ROWS` change
the sizes.
"""
import contextlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
# the copier's own files, in a directory of the run's own
os.environ.setdefault("MIGKIT_REPORTS", tempfile.mkdtemp(prefix="bench-"))

from migkit import cli, movers, ranges, sizing  # noqa: E402
from migkit.cli import _Checkpoint  # noqa: E402
from migkit.config import Endpoint, Hop  # noqa: E402

SIZES = [int(x) for x in os.environ.get("SIZES",
                                        "8000000,40000000").split(",")]


def say(*a):
    print(" ".join(str(x) for x in a), flush=True)


def sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


@contextlib.contextmanager
def patched(obj, name, value):
    before = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, before)


def safely(label, fn, *a, **k):
    try:
        return fn(*a, **k)
    except BaseException as e:  # noqa: BLE001 - the next run goes on
        say(f"  {label}: FAILED {type(e).__name__}: {str(e)[:300]}")


def timed_move(eng, move, workers, ctx, one_range, same,
               keep=("did not", "timed on its own", "whole by",
                     "copying it again", "as rows", "could not",
                     "other rows")):
    """One copy into an empty target, timed and held equal."""
    tmp = Path(tempfile.mkdtemp(prefix="bench-"))
    said = []
    with contextlib.ExitStack() as stack:
        for c in ctx:
            stack.enter_context(c)
        stack.enter_context(patched(ranges, "active",
                                    ranges.Slots(workers)))
        stack.enter_context(patched(type(eng), "STREAM_FROM", 1))
        if one_range:
            stack.enter_context(patched(ranges, "step",
                                        lambda rows, chunk, w: 10 ** 12))
        t0, c0 = time.time(), os.times()
        move(eng, _Checkpoint(tmp / "move.json"), said.append)
        el, c1 = time.time() - t0, os.times()
    ok, rows = same()
    cpu = (c1.user - c0.user) + (c1.system - c0.system)
    kids = (c1.children_user - c0.children_user
            + c1.children_system - c0.children_system)
    shutil.rmtree(tmp, ignore_errors=True)
    return el, ok, rows, cpu, kids, [m for m in said
                                     if any(w in m for w in keep)]


def line(label, workers, size, el, ok, rows, cpu, kids, extra=""):
    say(f"  {label:<46} workers {workers}: {el:7.1f}s"
        f" {size / 2**20 / el:6.1f} MB/s {rows / el / 1000:7.0f}k rows/s"
        f" migkit cpu {cpu:6.1f}s programs {kids:6.1f}s{extra}"
        f" {'equal' if ok else 'DIFFERENT'}")


# --- PostgreSQL -------------------------------------------------------------

PG = {16180: "migkit-test-pass-src", 16181: "migkit-test-pass-dst"}
PG_SRC, PG_DST = 16180, 16181


def pg(p):
    import psycopg
    return psycopg.connect(host="127.0.0.1", port=p, user="postgres",
                           password="test", dbname="postgres",
                           autocommit=True)


def pg_up():
    for p, n in PG.items():
        sh("docker", "rm", "-f", "-v", n)
        got = sh("docker", "run", "-d", "--name", n, "--shm-size=512m",
                 "-e", "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{p}:5432",
                 "postgres:16", "-c", "shared_buffers=256MB",
                 "-c", "max_wal_size=2GB", "-c", "checkpoint_timeout=30min",
                 "-c", "wal_level=logical")
        assert got.returncode == 0, got.stderr
    for p in PG:
        for _ in range(90):
            try:
                pg(p).close()
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(1)


PG_DDL = ("create table {t} (id bigint primary key, a int, b text,"
          " c numeric(14,2), d timestamptz, e bytea)")


def pg_seed(rows, t="big"):
    began = time.time()
    with pg(PG_SRC) as c:
        c.execute(f"drop table if exists {t}")
        c.execute(PG_DDL.format(t=t))
        c.execute(f"insert into {t} select g, g % 1000, 'row-' || g ||"
                  " repeat('x', 40), g / 7.0, timestamptz"
                  " '2026-01-01 00:00:00+00' - g * interval '1 second',"
                  " decode(md5(g::text), 'hex')"
                  f" from generate_series(1, {rows}) g")
        c.execute(f"vacuum analyze {t}")
        size = c.execute(f"select pg_table_size('{t}')").fetchone()[0]
    with pg(PG_DST) as c:
        c.execute(f"drop table if exists {t}")
        c.execute(PG_DDL.format(t=t))
    say(f"seed {t}: {rows:,} rows, {size / 2**20:,.0f} MB in"
        f" {time.time() - began:.0f}s")
    return size


def pg_reset(*tables):
    with pg(PG_DST) as c:
        for t in tables:
            c.execute(f"truncate {t}")
        c.execute("checkpoint")
    with pg(PG_SRC) as c:
        c.execute("checkpoint")


def pg_same(t="big"):
    q = (f"select count(*), sum(('x'||substr(md5({t}::text),1,16))::bit(64)"
         f"::bigint::numeric) from {t}")
    with pg(PG_SRC) as a, pg(PG_DST) as b:
        x, y = a.execute(q).fetchone(), b.execute(q).fetchone()
    return x == y, x[0]


def pg_engine(workers):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(Hop(
        name="bench-pass", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=PG_SRC, user="postgres",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=PG_DST, user="postgres",
                        password="test"),
        databases=["postgres"], workers=workers))


def pg_ways(*names, table=("ranges",)):
    """Only these ways a range goes, only these ways a table goes."""
    from migkit.engines.postgres import PostgresEngine
    return (patched(PostgresEngine, "COPY_WAYS", tuple(
        r for r in PostgresEngine.COPY_WAYS if r.name in names)),
        patched(PostgresEngine, "TABLE_WAYS", tuple(
            r for r in PostgresEngine.TABLE_WAYS if r.name in table)))


def pg_run(label, size, workers, *ctx, one_range=False):
    pg_reset("big")
    got = timed_move(pg_engine(workers), lambda e, ck, log: e.move_table(
        "postgres", "public", "big", 500_000, ck, log), workers, ctx,
        one_range, pg_same)
    line(label, workers, size, *got[:5])
    for m in got[5]:
        say("     ", m[:300])


def pg_indexes(rows):
    for t in ("ix1", "ix2"):
        pg_seed(rows, t)
        with pg(PG_DST) as c:
            c.execute(f"create index {t}_b on {t} (b)")
            c.execute(f"create index {t}_ad on {t} (a, d)")
    for label, beside, settings in (
            ("builds inline, the server's settings", False, False),
            ("builds inline, the session settings", False, True),
            ("builds beside the next copy, session settings", True, True)):
        pg_reset("ix1", "ix2")
        tmp = Path(tempfile.mkdtemp(prefix="bench-"))
        eng = pg_engine(2)
        with contextlib.ExitStack() as st:
            st.enter_context(patched(sizing, "estimate",
                                     lambda *a, **k: sizing.Plan(1, 1, {})))
            st.enter_context(patched(cli, "_changelog",
                                     lambda *a, **k: None))
            st.enter_context(patched(cli.console, "print",
                                     lambda *a, **k: None))
            if not beside:
                st.enter_context(patched(movers, "builds_beside",
                                         lambda log=None:
                                         contextlib.nullcontext()))
            if not settings:
                st.enter_context(patched(movers, "_pg_build_settings",
                                         lambda *a: ""))
            t0 = time.time()
            cli._copy_tables(eng.hop, eng, "postgres",
                             [("public", "ix1"), ("public", "ix2")],
                             500_000, _Checkpoint(tmp / "move.json"))
            el = time.time() - t0
        shutil.rmtree(tmp, ignore_errors=True)
        with pg(PG_DST) as c:
            n = c.execute("select count(*) from pg_indexes where tablename"
                          " in ('ix1', 'ix2')").fetchone()[0]
        ok = pg_same("ix1")[0] and pg_same("ix2")[0]
        say(f"  {label:<48}: {el:7.1f}s, {n} indexes on the target,"
            f" {'equal' if ok else 'DIFFERENT'}")


def postgres():
    from migkit.engines.postgres import PostgresEngine
    pg_up()
    try:
        with pg(PG_DST) as c:
            say("target:", c.execute(
                "select string_agg(name || '=' || setting, ' ') from"
                " pg_settings where name in ('shared_buffers',"
                " 'max_wal_size', 'wal_writer_delay', 'maintenance_work_mem',"
                " 'max_parallel_maintenance_workers',"
                " 'max_worker_processes')").fetchone()[0])
        for rows in SIZES:
            size = pg_seed(rows)
            say(f"table of {size / 2**20:,.0f} MB, {rows:,} rows:")
            for w in ((2, 4) if rows <= 10_000_000 else (4,)):
                safely("pipe", pg_run, "pipe (text through migkit, tallied)",
                       size, w, *pg_ways("pipe"))
                safely("pass", pg_run, "pass-through", size, w,
                       *pg_ways("pass-through"))
                safely("unawaited", pg_run, "pass-through unawaited", size,
                       w, *pg_ways("pass-through unawaited"))
                safely("streaming", pg_run, "streaming copy of the table",
                       size, w, *pg_ways("pass-through",
                                         table=("streaming",)))
            safely("freeze", pg_run, "whole table in one, FREEZE", size, 1,
                   *pg_ways("pass-through"), one_range=True)
            safely("no freeze", pg_run, "whole table in one, no FREEZE",
                   size, 1, *pg_ways("pass-through"),
                   patched(PostgresEngine, "_truncatable",
                           lambda *a: False), one_range=True)
            safely("choice", pg_run, "the table's own choice, every way",
                   size, 4)
        with pg(PG_SRC) as c:
            c.execute("drop table big")
        with pg(PG_DST) as c:
            c.execute("drop table big")
        safely("indexes", pg_indexes,
               int(os.environ.get("INDEX_ROWS", "3000000")))
    finally:
        for n in PG.values():
            sh("docker", "rm", "-f", "-v", n)


# --- MySQL ------------------------------------------------------------------

MY = {16185: "migkit-test-pass-mysrc", 16186: "migkit-test-pass-mydst"}
MY_SRC, MY_DST = 16185, 16186


def my(p, sql):
    import pymysql
    c = pymysql.connect(host="127.0.0.1", port=p, user="root",
                        password="test", database="shop", autocommit=True,
                        read_timeout=7200)
    try:
        with c.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        c.close()


def my_up(binlog):
    for p, n in MY.items():
        sh("docker", "rm", "-f", "-v", n)
        got = sh("docker", "run", "-d", "--name", n, "-e",
                 "MYSQL_ROOT_PASSWORD=test", "-e", "MYSQL_DATABASE=shop",
                 "-p", f"127.0.0.1:{p}:3306", "mysql:8.4",
                 "--local-infile=1", "--innodb-buffer-pool-size=512M",
                 "--innodb-redo-log-capacity=1073741824",
                 "--max-allowed-packet=256M",
                 *(["--disable-log-bin"] if p == MY_SRC or not binlog
                   else []))
        assert got.returncode == 0, got.stderr
    for p in MY:
        for _ in range(180):
            try:
                my(p, "select 1")
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(1)


MY_DDL = ("create table big (id bigint primary key, a int, b varchar(64),"
          " c decimal(14,2), d datetime(6), e varbinary(16), u bigint,"
          " unique key big_u (u))")


def my_seed(rows):
    began = time.time()
    my(MY_SRC, "drop table if exists big")
    my(MY_SRC, MY_DDL)
    my(MY_SRC, "set session cte_max_recursion_depth = 100000; insert into"
               " big with recursive g(n) as (select 1 union all select n + 1"
               " from g where n < 10000) select n, n % 1000, concat('row-',"
               " n, repeat('x', 40)), n / 7, timestamp('2026-01-01') -"
               " interval n second, unhex(md5(n)), n * 7 from g")
    have = 10000
    while have < rows:
        add = min(have, rows - have)
        my(MY_SRC, f"insert into big select id + {have}, (id + {have}) %"
                   f" 1000, concat('row-', id + {have}, repeat('x', 40)),"
                   f" (id + {have}) / 7, timestamp('2026-01-01') - interval"
                   f" (id + {have}) second, unhex(md5(id + {have})),"
                   f" (id + {have}) * 7 from big where id <= {add}")
        have += add
    my(MY_SRC, "analyze table big")
    size = int(my(MY_SRC, "select data_length + index_length from"
                          " information_schema.tables where table_schema ="
                          " 'shop' and table_name = 'big'")[0][0])
    my(MY_DST, "drop table if exists big")
    my(MY_DST, MY_DDL)
    say(f"seed: {rows:,} rows, {size / 2**20:,.0f} MB in"
        f" {time.time() - began:.0f}s")
    return size


def my_binlog():
    got = my(MY_DST, "show binary log status")
    return (got[0][0], int(got[0][1])) if got else ("", 0)


def my_same():
    sql = ("select count(*), sum(cast(conv(substring(md5(concat_ws('|', id,"
           " a, b, c, d, hex(e), u)), 1, 15), 16, 10) as unsigned)) from big")
    a, b = my(MY_SRC, sql), my(MY_DST, sql)
    return a == b, int(a[0][0])


def my_engine(workers, **options):
    from migkit.engines.mysql import MySQLEngine

    def ep(p):
        return Endpoint(host="127.0.0.1", port=p, user="root",
                        password="test")
    return MySQLEngine(Hop(name="bench-pass-my", engine="mysql",
                           source=ep(MY_SRC), target=ep(MY_DST),
                           databases=["shop"], workers=workers,
                           options=options))


def my_only(*names):
    from migkit.engines.mysql import MySQLEngine
    return patched(MySQLEngine, "TABLE_WAYS", tuple(
        r for r in MySQLEngine.TABLE_WAYS if r.name in names))


def my_run(label, size, workers, *ctx, one_range=False, **options):
    my(MY_DST, "truncate table big")
    if my_binlog()[0]:
        my(MY_DST, "flush binary logs")
        my(MY_DST, f"purge binary logs to '{my_binlog()[0]}'")
    before = my_binlog()
    got = timed_move(my_engine(workers, **options),
                     lambda e, ck, log: e.move_table("shop", "", "big",
                                                     500_000, ck, log),
                     workers, ctx, one_range, my_same)
    after = my_binlog()
    logged = after[1] - before[1] if after[0] == before[0] else after[1]
    line(label, workers, size, *got[:5],
         extra=f" binlog {logged / 2**20:,.0f} MB")
    for m in got[5]:
        say("     ", m[:300])


def mysql():
    from migkit.engines import my_pass
    try:
        for rows in SIZES:
            small = rows <= 10_000_000
            my_up(binlog=small)
            size = my_seed(rows)
            say(f"table of {size / 2**20:,.0f} MB, {rows:,} rows:")
            safely("rows", my_run, "the table copier's rows", size, 4,
                   my_only("rows"))
            safely("pass", my_run, "pass-through", size, 4,
                   my_only("pass-through"))
            safely("dump", my_run, "dump and load of the table", size, 4,
                   my_only("dump-load"))
            if not small:
                continue
            safely("no binlog", my_run, "pass-through, binlog off", size, 4,
                   my_only("pass-through"), target_has_replicas=False)
            safely("unique 0", my_run,
                   "whole table in one, unique_checks = 0", size, 1,
                   my_only("pass-through"), one_range=True)
            safely("unique 1", my_run,
                   "whole table in one, unique checks kept", size, 1,
                   my_only("pass-through"),
                   patched(my_pass.TableCopy, "_unique_safe",
                           lambda self: False), one_range=True)
            safely("rows 2", my_run, "the table copier's rows", size, 2,
                   my_only("rows"))
            safely("pass 2", my_run, "pass-through", size, 2,
                   my_only("pass-through"))
            safely("choice", my_run, "the table's own choice, every way",
                   size, 4)
    finally:
        for n in MY.values():
            sh("docker", "rm", "-f", "-v", n)


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "postgres"
    if which not in ("postgres", "mysql"):
        raise SystemExit("usage: same_engine_copy.py postgres|mysql")
    postgres() if which == "postgres" else mysql()
