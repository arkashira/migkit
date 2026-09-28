"""How fast migkit's own change paths go, split into their parts: the
recipes R-B and R-C of `docs/research/fast-python-hot-paths-2026-09-28.md`.

    python tools/with_docker_lock.py .venv/bin/python bench/tail_rates.py mysql
    python tools/with_docker_lock.py .venv/bin/python bench/tail_rates.py postgres

`mysql`: a MySQL 8.4 source and a PostgreSQL 16 target, 320,000 changes
queued (200,000 inserts, an update of every other row, a delete of every
tenth, a thousand rows a transaction). Timed: the binlog decoded alone;
the recorded batches applied alone, with the tail's hold on the garbage
collector and without; the whole tail with its reader in a thread, in a
process of its own, and on the way it measures faster. Wall and CPU
seconds each - where decode and apply add up to the whole and the CPU is
one core, the tail is serial on the interpreter's lock. `BENCH_PROFILE=1`
prints where the apply's own time goes.

`postgres`: a PostgreSQL 16 source with a `test_decoding` slot and the
same 320,000 changes, into PostgreSQL 16. Timed: a hundred client program
starts (what the tail paid a query); the server's decoding alone, from
one connection; the parse of the decoded text alone; the whole tail.

Containers `migkit-test-spd-*` on 127.0.0.1:16100-16102, removed after.
Numbers print as JSON; nothing is written anywhere else.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

ROWS = 200_000
TXN = 1000
DB = "appdb"
MY, MY_PORT = "migkit-test-spd-my", 16100
PG, PG_PORT = "migkit-test-spd-pg", 16101
PGS, PGS_PORT = "migkit-test-spd-pgsrc", 16102


def _sh(argv, **kw):
    return subprocess.run(argv, capture_output=True, text=True, **kw)


def _wait(port, ask, timeout=240):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0 and ask():
                return
        time.sleep(2)
    raise SystemExit(f"nothing answered on {port}")


def _my(sql):
    got = _sh(["docker", "exec", "-i", MY, "mysql", "-uroot", "-ptest",
               "-h127.0.0.1", "--protocol=tcp", "-N", "-B"], input=sql)
    if got.returncode:
        raise SystemExit(got.stderr[-400:])
    return got.stdout.strip()


def _pg(name, sql, db="postgres"):
    got = _sh(["docker", "exec", "-i", "-e", "PGPASSWORD=test", name, "psql",
               "-U", "postgres", "-h", "127.0.0.1", "-d", db, "-v",
               "ON_ERROR_STOP=1", "-At"], input=sql)
    if got.returncode:
        raise SystemExit(got.stderr[-400:])
    return got.stdout.strip()


def _start_pg(name, port):
    _sh(["docker", "rm", "-f", "-v", name])
    _sh(["docker", "run", "-d", "--name", name, "-e",
         "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432",
         "postgres:16", "-c", "wal_level=logical"], check=True)
    _wait(port, lambda: _sh(["docker", "exec", "-e", "PGPASSWORD=test",
                             name, "psql", "-U", "postgres", "-h",
                             "127.0.0.1", "-c", "select 1"]
                            ).returncode == 0)
    _pg(name, f"create database {DB}")
    _pg(name, "create table public.t (id bigint primary key, v int,"
              " s varchar(64), amount numeric(12,2), ts timestamp(6))", DB)


def _start_my():
    _sh(["docker", "rm", "-f", "-v", MY])
    _sh(["docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
         "-p", f"127.0.0.1:{MY_PORT}:3306", "mysql:8.4", "--server-id=7",
         "--binlog-row-metadata=FULL"], check=True)
    _wait(MY_PORT, lambda: _sh(["docker", "exec", MY, "mysql", "-uroot",
                                "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                "-e", "select 1"]).returncode == 0)
    _my(f"create database {DB}; create table {DB}.t (id bigint primary key,"
        " v int, s varchar(64), amount decimal(12,2), ts datetime(6))")


def _workload(begin, commit):
    """The 320,000 changes, a thousand rows a transaction, as statements
    with the table left as `{t}`."""
    out = []
    for lo in range(1, ROWS + 1, TXN):
        vals = ", ".join(
            f"({i}, {i % 997}, 'row-{i:08d}-abcdefghijklmnopqrstuvwxyz',"
            f" {i / 100:.2f}, '2026-01-01 00:00:00.{i % 1000000:06d}')"
            for i in range(lo, lo + TXN))
        out.append(f"{begin} insert into {{t}} values {vals}; {commit}")
    for lo in range(1, ROWS + 1, 2 * TXN):
        out.append(f"{begin} update {{t}} set v = v + 1, s = concat(s, 'x')"
                   f" where id % 2 = 0 and id between {lo} and"
                   f" {lo + 2 * TXN - 1}; {commit}")
    for lo in range(1, ROWS + 1, 10 * TXN):
        out.append(f"{begin} delete from {{t}} where id % 10 = 0 and id"
                   f" between {lo} and {lo + 10 * TXN - 1}; {commit}")
    return out


EXPECTED = ROWS + ROWS // 2 + ROWS // 10


def _hop(src_engine, src_port, dst_port):
    from migkit.config import Endpoint, Hop
    return Hop(name=f"spd-{src_engine}", engine="hetero",
               source=Endpoint(host="127.0.0.1", port=src_port,
                               user="root" if src_engine == "mysql"
                               else "postgres", password="test"),
               target=Endpoint(host="127.0.0.1", port=dst_port,
                               user="postgres", password="test"),
               databases=[DB], workers=4,
               options={"source_engine": src_engine,
                        "target_engine": "postgres"})


def _cpu():
    import resource
    me = resource.getrusage(resource.RUSAGE_SELF)
    kids = resource.getrusage(resource.RUSAGE_CHILDREN)
    return (me.ru_utime + me.ru_stime, kids.ru_utime + kids.ru_stime)


def _timed(fn):
    t0, (c0, k0) = time.perf_counter(), _cpu()
    got = fn()
    t1, (c1, k1) = time.perf_counter(), _cpu()
    return got, {"wall": round(t1 - t0, 2), "cpu": round(c1 - c0, 2),
                 "child_cpu": round(k1 - k0, 2)}


def _tail(eng, start, seen_at_end, reader=None):
    """The tail from `start` until `seen_at_end` changes are applied, and
    the seconds from its first read to its last save."""
    work = Path(tempfile.mkdtemp())
    token_path = work / "tail-token.json"
    token_path.write_text(json.dumps({"token": start}))
    marks = {}

    def log(line):
        if line.startswith("changes are read"):
            marks["read"] = line
        if line.endswith(" changes") and line.split()[0].isdigit():
            marks.setdefault("first", time.perf_counter())
            if int(line.split()[0]) >= seen_at_end and "end" not in marks:
                marks["end"] = time.perf_counter()
                raise KeyboardInterrupt
    if reader is not None:
        eng._change_reader = reader
    eng.tail_apply(DB, True, token_path, log)
    return marks


def _target_state(port):
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                            password="test", dbname=DB)
    try:
        with conn.cursor() as cur:
            cur.execute("select count(*), coalesce(sum(v), 0),"
                        " md5(string_agg(s, ',' order by id)) from t")
            return list(cur.fetchone())
    finally:
        conn.close()


def _empty_target(port):
    _pg(PG, "truncate public.t", DB)


def mysql():
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.mysql import MySQLEngine
    _start_my()
    _start_pg(PG, PG_PORT)
    out = {"changes": EXPECTED, "python": sys.version.split()[0],
           "host_cpus": os.cpu_count()}
    hop = _hop("mysql", MY_PORT, PG_PORT)
    start = MySQLEngine(hop).change_point("src", DB)
    _my(f"use {DB};\n" + "\n".join(
        sql.format(t="t") for sql in _workload("begin;", "commit;")))

    # R-A: the binlog decoded alone, and the batches kept for the next part
    def decode():
        src, token, batches, n = MySQLEngine(hop), dict(start), [], 0
        while True:
            got, token = src.neutral_changes("src", DB, token, limit=16000)
            batches.append(got)
            n += len(got)
            if len(got) < 16000:
                return batches, n
    (batches, n), out["decode_only"] = _timed(decode)
    out["decode_only"]["changes"] = n
    flat = [c for b in batches for c in b]

    # the batches applied alone, from the recording
    _empty_target(PG_PORT)
    eng = HeteroEngine(hop)

    def replay(side, db, token=None, limit=1000):
        at = int(token or 0)
        got = flat[at:at + limit]
        return got, at + len(got)
    eng.src_engine.neutral_changes = replay
    eng.src_engine.READS_AHEAD = False
    marks, out["apply_only"] = _timed(lambda: _tail(eng, 0, n))
    out["apply_only"]["first_to_last"] = round(marks["end"] - marks["first"],
                                               2)
    applied = _target_state(PG_PORT)
    if os.environ.get("BENCH_PROFILE"):
        # where the apply's own time goes, printed apart from the numbers
        import cProfile
        import pstats
        _empty_target(PG_PORT)
        eng = HeteroEngine(hop)
        eng.src_engine.neutral_changes = replay
        eng.src_engine.READS_AHEAD = False
        prof = cProfile.Profile()
        prof.runcall(_tail, eng, 0, n)
        pstats.Stats(prof, stream=sys.stderr).sort_stats(
            "tottime").print_stats(30)
    out["target"] = applied
    import migkit.engines.hetero as hetero
    if hasattr(hetero, "_batch_gc"):
        # the same, the collector left as it was
        _empty_target(PG_PORT)
        eng = HeteroEngine(hop)
        eng.src_engine.neutral_changes = replay
        eng.src_engine.READS_AHEAD = False
        kept = hetero._batch_gc
        hetero._batch_gc = lambda: (lambda: None)
        try:
            marks, out["apply_only_collector_as_was"] = _timed(
                lambda: _tail(eng, 0, n))
        finally:
            hetero._batch_gc = kept
        out["apply_only_collector_as_was"]["first_to_last"] = round(
            marks["end"] - marks["first"], 2)

    # the whole tail, its reader beside it in a thread (today's)
    def whole(reader=None):
        _empty_target(PG_PORT)
        eng = HeteroEngine(hop)
        marks, took = _timed(lambda: _tail(eng, start, n, reader))
        took["first_to_last"] = round(marks["end"] - marks["first"], 2)
        took["same_target"] = _target_state(PG_PORT) == applied
        if "read" in marks:
            took["read"] = marks["read"]
        return took
    if hasattr(hetero, "_ReadProcess"):
        out["tail_thread"] = whole(
            lambda db, token_path, log: hetero._ReadAhead(
                HeteroEngine(hop).src_engine, db))
        out["tail_process"] = whole(
            lambda db, token_path, log: hetero._ReadProcess(
                HeteroEngine(hop).src_engine, db))
        out["tail_measured"] = whole()
    else:
        out["tail_thread"] = whole()
    return out


def postgres():
    import psycopg2

    from migkit import pgslot
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.postgres import PostgresEngine
    _start_pg(PGS, PGS_PORT)
    _start_pg(PG, PG_PORT)
    out = {"changes": EXPECTED, "python": sys.version.split()[0]}
    hop = _hop("postgres", PGS_PORT, PG_PORT)
    src = PostgresEngine(hop)
    start = src.change_point("src", DB)
    _pg(PGS, "\n".join(sql.format(t="public.t")
                       for sql in _workload("begin;", "commit;")), DB)
    name = src.slot_name()

    _, out["client_starts_100"] = _timed(
        lambda: [src._psql("src", DB, "select 1") for _ in range(100)])
    conn = psycopg2.connect(host="127.0.0.1", port=PGS_PORT, user="postgres",
                            password="test", dbname=DB,
                            options="-c TimeZone=UTC -c DateStyle=ISO")
    conn.autocommit = True
    cur = conn.cursor()

    def server():
        cur.execute("select count(*) from pg_logical_slot_peek_changes("
                    "%s, null, null)", (name,))
        return cur.fetchone()[0]
    rows, out["server_decode_only"] = _timed(server)
    out["server_decode_only"]["rows"] = rows

    def fetch():
        cur.execute("select lsn::text, data from pg_logical_slot_peek_changes("
                    "%s, null, null)", (name,))
        return cur.fetchall()
    got, out["server_decode_and_fetch"] = _timed(fetch)
    conn.close()

    def parse():
        n = 0
        for _, data in got:
            p = pgslot.parse_line(data)
            if p:
                pgslot.change(p, ["id"])
                n += 1
        return n
    n, out["parse_only"] = _timed(parse)
    out["parse_only"]["changes"] = n

    _empty_target(PG_PORT)
    eng = HeteroEngine(hop)
    marks, out["tail"] = _timed(lambda: _tail(eng, start, n))
    out["tail"]["first_to_last"] = round(marks["end"] - marks["first"], 2)
    out["target"] = _target_state(PG_PORT)
    out["source"] = _pg(PGS, "select count(*), coalesce(sum(v), 0),"
                             " md5(string_agg(s, ',' order by id)) from t",
                        DB).split("|")
    return out


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "mysql"
    os.environ.setdefault("MIGKIT_REPORTS", tempfile.mkdtemp())
    try:
        got = mysql() if which == "mysql" else postgres()
        print(json.dumps(got, indent=1, default=str))
    finally:
        for name in (MY, PG, PGS):
            _sh(["docker", "rm", "-f", "-v", name])


if __name__ == "__main__":
    threading.current_thread().name = "bench"
    main()
