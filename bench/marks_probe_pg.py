"""What a PostgreSQL server shows migkit's own reader of the marks a
two-way tail can leave (backlog R3, the mark rungs), and what each costs.

NOT RUN YET (see WIP.md). Starts a disposable postgres:<version> on
127.0.0.1:15960, then measures:

* whether `test_decoding` takes `include-origin` and `only-local`, and
  what a transaction carrying a replication origin, or a transactional
  logical message, looks like in `pg_logical_slot_peek_changes`
* whether `pg_replication_origin_session_setup` may be called inside a
  transaction block, whether a second session may take the same origin,
  and what `pg_replication_origin_progress` answers after `xact_setup`
* which of the origin and message functions a plain user may call, and
  whether GRANT EXECUTE opens them (16+)
* the applier's cost a transaction with each mark (none, the table row,
  the origin, the origin with the session set up once, the message) and
  the reader's cost with and without `only-local`

    .venv/bin/python bench/marks_probe_pg.py 16 1000
    .venv/bin/python bench/marks_probe_pg.py 14 1000
"""
import json
import subprocess
import sys
import time

import psycopg2

VER = sys.argv[1] if len(sys.argv) > 1 else "16"
NAME, PORT = "migkit-test-rung-pg", 15960
N = int(sys.argv[2]) if len(sys.argv) > 2 else 1000


def sh(*a):
    return subprocess.run(a, capture_output=True, text=True)


def conn(user="postgres", pw="test", auto=False):
    c = psycopg2.connect(host="127.0.0.1", port=PORT, user=user, password=pw,
                         dbname="postgres")
    c.autocommit = auto
    return c


def first_line(e):
    return str(e).strip().splitlines()[0]


def main():
    sh("docker", "rm", "-f", "-v", NAME)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    f"postgres:{VER}", "-c", "wal_level=logical",
                    "-c", "max_replication_slots=20"],
                   check=True, capture_output=True)
    for _ in range(60):
        try:
            conn().close()
            break
        except Exception:
            time.sleep(1)
    time.sleep(2)
    try:
        probe()
    finally:
        sh("docker", "rm", "-f", "-v", NAME)


def probe():
    a = conn(auto=True)
    cur = a.cursor()
    cur.execute("select version()")
    print("VERSION", cur.fetchone()[0])
    cur.execute("create table t (id int primary key, v text)")
    cur.execute("create table migkit_origin (origin text primary key,"
                " n bigint not null default 0, seen text)")
    cur.execute("select pg_create_logical_replication_slot('probe',"
                " 'test_decoding')")
    cur.execute("select pg_replication_origin_create('migkit_h1')")

    def peek(*opts, limit=100000):
        cur.execute("select lsn::text, data from pg_logical_slot_peek_changes"
                    "(%s, NULL, %s" + "".join(", %s" for _ in opts) + ")",
                    ("probe", limit) + opts)
        return cur.fetchall()

    def advance():
        cur.execute("select pg_current_wal_lsn()")
        up = cur.fetchone()[0]
        cur.execute("select pg_replication_slot_advance('probe', %s)", (up,))

    for opt in ("include-origin", "only-local", "include-xids",
                "skip-empty-xacts"):
        try:
            peek(opt, "true", limit=1)
            print("OPTION", opt, "accepted")
        except Exception as e:
            print("OPTION", opt, "refused:", first_line(e))

    w = conn()
    wc = w.cursor()
    try:
        wc.execute("select pg_replication_origin_session_setup('migkit_h1')")
        wc.execute("select pg_replication_origin_xact_setup('0/2A', now())")
        wc.execute("insert into t values (1, 'origin')")
        w.commit()
        print("ORIGIN in-txn session_setup + xact_setup: ok")
    except Exception as e:
        print("ORIGIN in-txn:", first_line(e))
        w.rollback()
    w2 = conn()
    w2c = w2.cursor()
    try:
        w2c.execute("select pg_replication_origin_session_setup('migkit_h1')")
        print("ORIGIN second session on the same origin: ok")
    except Exception as e:
        print("ORIGIN second session on the same origin:", first_line(e))
        w2.rollback()
    w2.close()
    cur.execute("select pg_replication_origin_progress('migkit_h1', true)")
    print("PROGRESS", cur.fetchone()[0])
    wc.execute("select pg_replication_origin_session_reset()")
    w.commit()
    wc.execute("insert into t values (2, 'plain')")
    w.commit()
    wc.execute("select pg_logical_emit_message(true, 'migkit', 'hello')")
    wc.execute("insert into t values (3, 'msg')")
    w.commit()
    # a probe: the origin and a message, no row
    wc.execute("select pg_replication_origin_session_setup('migkit_h1')")
    wc.execute("select pg_replication_origin_xact_setup('0/2B', now())")
    wc.execute("select pg_logical_emit_message(true, 'migkit', 'probe')")
    w.commit()
    wc.execute("select pg_replication_origin_session_reset()")
    w.commit()
    print("--- peek default")
    for lsn, d in peek():
        print("  ", lsn, d[:100])
    print("--- peek only-local")
    for lsn, d in peek("only-local", "true"):
        print("  ", lsn, d[:100])
    print("--- peek only-local + skip-empty-xacts")
    for lsn, d in peek("only-local", "true", "skip-empty-xacts", "true"):
        print("  ", lsn, d[:100])
    try:
        cur.execute(
            "select count(*) from pg_logical_slot_peek_changes('probe', NULL,"
            " NULL, variadic (select case when exists (select 1 from"
            " pg_replication_origin where roname like 'migkit\\_%') then"
            " array['only-local','true'] else '{}'::text[] end))")
        print("VARIADIC subquery ok, rows", cur.fetchone()[0])
    except Exception as e:
        print("VARIADIC subquery:", first_line(e))
    advance()

    cur.execute("create user u1 password 'u1'")
    cur.execute("grant all on t, migkit_origin to u1")
    calls = {
        "pg_replication_origin_create(text)":
            "select pg_replication_origin_create('migkit_u1')",
        "pg_replication_origin_session_setup(text)":
            "select pg_replication_origin_session_setup('migkit_h1')",
        "pg_replication_origin_xact_setup(pg_lsn, timestamptz)":
            "select pg_replication_origin_session_setup('migkit_h1');"
            " select pg_replication_origin_xact_setup('0/1', now())",
        "pg_replication_origin_progress(text, boolean)":
            "select pg_replication_origin_progress('migkit_h1', true)",
        "pg_logical_emit_message(boolean, text, text)":
            "select pg_logical_emit_message(true, 'migkit', 'x')",
    }
    for fn, sql in calls.items():
        for granted in (False, True):
            if granted:
                try:
                    cur.execute(f"grant execute on function {fn} to u1")
                except Exception as e:
                    print("  grant itself:", first_line(e))
                    continue
            u = conn("u1", "u1")
            uc = u.cursor()
            try:
                uc.execute(sql)
                u.commit()
                print("GRANT" if granted else "PLAIN", fn, "ok")
            except Exception as e:
                print("GRANT" if granted else "PLAIN", fn, "->", first_line(e))
                u.rollback()
            u.close()
    u = conn("u1", "u1")
    uc = u.cursor()
    try:
        uc.execute("select roname from pg_replication_origin")
        print("plain user reads pg_replication_origin:", uc.fetchall())
    except Exception as e:
        print("plain user reads pg_replication_origin:", first_line(e))
    u.close()

    def run(label, mark):
        wc.execute("truncate t")
        w.commit()
        advance()
        t0 = time.perf_counter()
        for i in range(N):
            mark(i)
            wc.execute("insert into t values (%s, 'x')", (i,))
            w.commit()
        dt = time.perf_counter() - t0
        t1 = time.perf_counter()
        rows = peek()
        r_plain = time.perf_counter() - t1
        t1 = time.perf_counter()
        rows_local = peek("only-local", "true", "skip-empty-xacts", "true")
        r_local = time.perf_counter() - t1
        print(f"COST {label:8s} apply {dt * 1e6 / N:7.0f} us/txn"
              f" ({N / dt:6.0f} txn/s)  reader plain"
              f" {r_plain * 1e6 / N:6.0f} us/txn ({len(rows)} lines)"
              f"  only-local {r_local * 1e6 / N:6.0f} us/txn"
              f" ({len(rows_local)} lines)")
        advance()

    def lsn(i):
        return f"{i >> 32:X}/{i & 0xffffffff:X}"

    def table(i):
        wc.execute("insert into migkit_origin (origin, n, seen) values"
                   " ('h1', 1, %s) on conflict (origin) do update set n ="
                   " migkit_origin.n + 1, seen = coalesce(excluded.seen,"
                   " migkit_origin.seen)",
                   (json.dumps({"token": "0/1234ABCD", "batch": i}),))

    def origin(i):
        # a batch's connection is its own, so the origin is taken once a
        # batch: released and taken again here, in one round trip
        wc.execute(("select pg_replication_origin_session_reset();"
                    if i else "")
                   + "select pg_replication_origin_session_setup"
                   "('migkit_h1'); select pg_replication_origin_xact_setup"
                   "(%s::pg_lsn, now())", (lsn(i),))

    def origin_once(i):
        if i == 0:
            wc.execute("select pg_replication_origin_session_setup"
                       "('migkit_h1')")
        wc.execute("select pg_replication_origin_xact_setup(%s::pg_lsn,"
                   " now())", (lsn(i),))

    def message(i):
        wc.execute("select pg_logical_emit_message(true, 'migkit', %s)",
                   (json.dumps({"batch": i}),))

    run("off", lambda i: None)
    run("table", table)
    run("origin", origin)
    wc.execute("select pg_replication_origin_session_reset()")
    w.commit()
    run("origin1", origin_once)
    wc.execute("select pg_replication_origin_session_reset()")
    w.commit()
    run("message", message)
    run("off2", lambda i: None)
    cur.execute("select pg_replication_origin_progress('migkit_h1', true)")
    print("PROGRESS after the origin runs", cur.fetchone()[0])
    w.close()
    pgoutput(cur)
    a.close()


def pgoutput(cur):
    """What the binary protocol sends of an origin and a message, read
    through the SQL functions: the Origin message after BEGIN, the
    Message, and what `origin` none/any leave (16+)."""
    cur.execute("create table po (id int primary key, v text)")
    cur.execute("create publication p for table po")
    cur.execute("select pg_create_logical_replication_slot('po',"
                " 'pgoutput')")
    w = conn()
    wc = w.cursor()
    wc.execute("insert into po values (1, 'plain')")
    w.commit()
    wc.execute("select pg_replication_origin_session_setup('migkit_h1');"
               " select pg_replication_origin_xact_setup('0/2A', now())")
    wc.execute("insert into po values (2, 'origin')")
    w.commit()
    wc.execute("select pg_replication_origin_session_reset()")
    w.commit()
    wc.execute("select pg_logical_emit_message(true, 'migkit', 'hello');"
               " insert into po values (3, 'msg')")
    w.commit()
    w.close()
    for opts in ([], ["origin", "none"], ["origin", "any"]):
        try:
            cur.execute("select data from pg_logical_slot_peek_binary_changes"
                        "('po', NULL, NULL, 'proto_version', '1',"
                        " 'publication_names', 'p', 'messages', 'true'"
                        + "".join(f", '{o}'" for o in opts) + ")")
            got = [bytes(d) for (d,) in cur.fetchall()]
            print("PGOUTPUT", opts or "default",
                  [(d[:1].decode(), d[1:48]) for d in got
                   if d[:1] in (b"O", b"M", b"I")])
        except Exception as e:
            print("PGOUTPUT", opts, "->", first_line(e))


if __name__ == "__main__":
    main()
