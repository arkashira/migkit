"""The proof kept up by a running tail, on servers (backlog R20.8): after a
long random workload the leaves the tail kept equal a fresh one-scan tree
on both sides, PostgreSQL to PostgreSQL and MySQL to MySQL; the check then
proves every table from the kept numbers, reading again only the leaves
the last changes touched; and a row changed on the target behind the
tail's back is found by the cold read and named by the next check.

One server at a time, each hop two databases on it, removed after:
PostgreSQL 16 `migkit-test-proof-kpg` on 127.0.0.1:16175 (a
`test_decoding` slot, REPLICA IDENTITY FULL for whole before images) and
MySQL 8.4 `migkit-test-proof-kmy` on 16173 (GTIDs on, FULL row images).
"""
import ctypes
import json
import random
import socket
import subprocess
import threading
import time

import pytest

from migkit import kept, leaves, tailctl
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

ROWS = 20_000
WRITES = 4_000
MY, MY_PORT = "migkit-test-proof-kmy", 16173
PG, PG_PORT = "migkit-test-proof-kpg", 16175


def _tail(pair, db, token_path, lines):
    """The tail in a thread, until `stop()`."""
    done = threading.Event()

    def run():
        try:
            pair.tail_apply(db, True, token_path, lines.append)
        except BaseException as e:  # noqa: BLE001 - KeyboardInterrupt
            lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    def stop():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident),
            ctypes.py_object(KeyboardInterrupt))
        done.wait(timeout=120)
    return stop, done


def _caught_up(pair, db, token_path, end, done, timeout=240):
    began = time.time()
    while time.time() - began < timeout and not done.is_set():
        try:
            at = json.loads(token_path.read_text()).get("token")
        except (OSError, ValueError):
            at = None
        if at is not None and pair.src_engine.position_reached(at, end):
            return True
        time.sleep(0.2)
    return False


def _kept_equals_fresh(pair, db, where):
    """With the tail holding: every kept leaf not marked equals a fresh
    scan of its side. Returns (leaves compared, marked)."""
    assert tailctl.pause(where, 120)
    try:
        st = kept.Store(where / kept.FILE)
        n = marked = 0
        for name, d in st.tables().items():
            t = kept.Table(name, d)
            a, b = t.sides(pair, db)
            ta, tb = leaves.scan(a, t.spec), leaves.scan(b, t.spec)
            got = st.leaves(name)
            z = (0, 0, 0)
            for x in set(got) | set(ta.leaves) | set(tb.leaves):
                leaf = got.get(x)
                if leaf is not None and leaf[kept.STALE]:
                    marked += 1
                    continue
                assert (tuple(leaf[0:3]) if leaf else z) == \
                    ta.leaves.get(x, z), ("source", name, x)
                assert (tuple(leaf[3:6]) if leaf else z) == \
                    tb.leaves.get(x, z), ("target", name, x)
                n += 1
        return n, marked
    finally:
        tailctl.resume(where)


def _data(pair, db):
    got = [r for r in pair.check_data(db) if r.check == "data"]
    return {r.scope.split(".", 1)[1]: r for r in got}


def _scenario(pair, db, where, writer, end_of_log, corrupt, table,
              monkeypatch, native):
    token_path = where / "tail-token.json"
    assert pair.tail_start(db, token_path)
    lines = []
    stop, done = _tail(pair, db, token_path, lines)
    try:
        # the leaves are seeded at the first position the tail saves; no
        # write while they are read, so no change falls between a mark
        # before the read and one after it (a leaf such a change touches
        # is marked to read again, which is not what is measured here)
        writer(1)
        began = time.time()
        while not (where / kept.FILE).exists() or not kept.Store(
                where / kept.FILE).tables():
            assert time.time() - began < 120 and not done.is_set(), \
                lines[-5:]
            time.sleep(0.3)
        writer(WRITES)
        assert _caught_up(pair, db, token_path, end_of_log(), done), \
            lines[-5:]
        compared, marked = _kept_equals_fresh(pair, db, where)
        assert compared >= 20 and marked == 0, (compared, marked)
        # the check proves the table from the kept numbers
        writer(50)
        assert _caught_up(pair, db, token_path, end_of_log(), done)
        r = _data(pair, db)[table]
        assert r.status == "ok", r.detail
        assert "leaves proved by the numbers kept as the data moved" in \
            r.detail, r.detail
        assert "touched since the last pass" in r.detail, r.detail
        # nothing touched since: every leaf proved without a read
        r = _data(pair, db)[table]
        assert r.status == "ok" and "none read again" in r.detail, r.detail
        # the hop's own engine's check stands on the same kept leaves
        got = [r for r in native.check_data(db) if r.check == "data"]
        assert [r.status for r in got] == ["ok"], [r.detail for r in got]
        assert "proved by the numbers kept as the data moved" in \
            got[0].detail, got[0].detail
        # and the row counts merged into it, from the same numbers
        got = native.check_data(db, with_counts=True)
        assert [(r.check, r.status) for r in got[:2]] == [
            ("counts", "ok"), ("data", "ok")], [r.__dict__ for r in got]
        assert "from the kept leaves, no scan" in got[0].detail, got[0].detail
        # a row changed on the target behind the tail's back: the kept
        # numbers still agree, so the check proves the table - until the
        # cold read finds it
        key = corrupt()
        r = _data(pair, db)[table]
        assert r.status == "ok", r.detail
        monkeypatch.setenv("MIGKIT_PROOF_COLD_SECONDS", "0")
        monkeypatch.setenv("MIGKIT_PROOF_COLD_LEAVES", "100000")
        writer(1)
        assert _caught_up(pair, db, token_path, end_of_log(), done)
        began = time.time()
        while not any("written there by something else" in l
                      for l in lines):
            assert time.time() - began < 120, lines[-5:]
            writer(1)
            time.sleep(0.5)
        monkeypatch.setenv("MIGKIT_PROOF_COLD_SECONDS", "3600")
        r = _data(pair, db)[table]
        assert r.status == "diff", r.detail
        assert f"1 with different values ({key})" in r.detail, r.detail
        assert "kept leaves that differ" in r.detail, r.detail
    finally:
        stop()
    return lines


def _pg_sql(sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-i", "-e", "PGPASSWORD=test",
                          PG, "psql", "-U", "postgres", "-h", "127.0.0.1",
                          "-d", db, "-v", "ON_ERROR_STOP=1", "-At"],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture
def pg_server():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-p",
                    f"127.0.0.1:{PG_PORT}:5432", "-e",
                    "POSTGRES_PASSWORD=test", "postgres:16", "-c",
                    "wal_level=logical"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test",
                                 PG, "psql", "-U", "postgres", "-h",
                                 "127.0.0.1", "-c", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", PG_PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("PostgreSQL never answered")
        yield PG_PORT
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


def _pg(where):
    """(the PostgreSQL hop's engine, its pair): `keptsrc` to `keptdst`."""
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                  password="test")
    hop = Hop(name="kept_pg", engine="postgres", source=ep, target=ep,
              databases=["keptsrc"], db_map={"keptsrc": "keptdst"},
              options={"keep_proof": True})
    where.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: where
    eng = PostgresEngine(hop)
    return eng, eng._as_pair()


def test_postgres_kept_leaves_equal_a_fresh_tree(pg_server, tmp_path,
                                                 monkeypatch):
    for db in ("keptsrc", "keptdst"):
        _pg_sql(f"create database {db}")
        _pg_sql(f"""
            create table public.big (id bigint primary key, n int,
              payload text, amount numeric(12,2), at timestamp(6));
            alter table public.big replica identity full;
            insert into public.big select g, 0, md5(g::text), g / 7.0,
              timestamp '2024-01-01' + g * interval '1.5 s'
              from generate_series(1, {ROWS}) g""", db)
    native, pair = _pg(tmp_path / "pg")
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=PG_PORT,
                            user="postgres", password="test",
                            dbname="keptsrc")
    conn.autocommit = True
    rnd, fresh = random.Random(5), [ROWS]

    def writer(n):
        with conn.cursor() as cur:
            for _ in range(n):
                r = rnd.random()
                if r < 0.7:
                    cur.execute("update public.big set n = n + 1, payload ="
                                " md5(random()::text), amount = amount + 1,"
                                " at = clock_timestamp() where id = %s",
                                (rnd.randint(1, ROWS),))
                elif r < 0.8:
                    cur.execute("update public.big set id = %s where id ="
                                " %s", (fresh[0] + 1, rnd.randint(1, ROWS)))
                    fresh[0] += 1
                elif r < 0.9:
                    cur.execute("delete from public.big where id = %s",
                                (rnd.randint(1, ROWS),))
                else:
                    fresh[0] += 1
                    cur.execute("insert into public.big values (%s, 0,"
                                " 'new', null, null)", (fresh[0],))

    def end():
        return _pg_sql("select pg_current_wal_lsn()")

    def corrupt():
        key = _pg_sql("select min(id) from public.big", "keptdst")
        _pg_sql("update public.big set payload = 'behind the tail' where"
                f" id = {key}", "keptdst")
        return key
    try:
        _scenario(pair, "keptsrc", tmp_path / "pg", writer, end, corrupt,
                  "big", monkeypatch, native)
    finally:
        conn.close()


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-p",
                    f"127.0.0.1:{MY_PORT}:3306", "-e",
                    "MYSQL_ROOT_PASSWORD=test", "mysql:8.4",
                    "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                    "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield MY_PORT
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def test_mysql_kept_leaves_equal_a_fresh_tree(mysql_server, tmp_path,
                                              monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    rows = f"""
        set session cte_max_recursion_depth = 100000;
        create table big (id bigint primary key, n int, payload
          varchar(40), amount decimal(12,2), at datetime(6));
        insert into big with recursive g(x) as (select 1 union all
          select x + 1 from g where x < {ROWS})
          select x, 0, md5(x), x / 7.0,
                 timestamp('2024-01-01') + interval x * 1500 microsecond
          from g;"""
    _my("drop database if exists shop; drop database if exists shop_t;"
        " create database shop; create database shop_t;"
        f" use shop; {rows} use shop_t; {rows}")
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    hop = Hop(name="kept_my", engine="mysql", source=ep, target=ep,
              databases=["shop"], db_map={"shop": "shop_t"},
              options={"keep_proof": True})
    where = tmp_path / "my"
    where.mkdir()
    hop.report_dir = lambda db=None: where
    native = MySQLEngine(hop)
    pair = native._as_pair()
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                           password="test", database="shop",
                           autocommit=True)
    rnd, fresh = random.Random(6), [ROWS]

    def writer(n):
        with conn.cursor() as cur:
            for _ in range(n):
                r = rnd.random()
                if r < 0.7:
                    cur.execute("update big set n = n + 1, payload ="
                                " md5(rand()), amount = amount + 1,"
                                " at = now(6) where id = %s",
                                (rnd.randint(1, ROWS),))
                elif r < 0.8:
                    cur.execute("update big set id = %s where id = %s",
                                (fresh[0] + 1, rnd.randint(1, ROWS)))
                    fresh[0] += 1
                elif r < 0.9:
                    cur.execute("delete from big where id = %s",
                                (rnd.randint(1, ROWS),))
                else:
                    fresh[0] += 1
                    cur.execute("insert into big values (%s, 0, 'new',"
                                " null, null)", (fresh[0],))

    def end():
        return pair.src_engine.log_position("src", "shop")

    def corrupt():
        key = _my("select min(id) from shop_t.big")
        _my(f"update shop_t.big set payload = 'behind the tail' where id ="
            f" {key}")
        return key
    try:
        _scenario(pair, "shop", where, writer, end, corrupt, "big",
                  monkeypatch, native)
    finally:
        conn.close()
