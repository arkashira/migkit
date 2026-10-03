"""The fence a check and a cutover wait on, on PostgreSQL (research:
cutover, section 4).

It passed with a row still to come in two ways, each measured on
PostgreSQL 16 before the fix:
* its position was `pg_current_wal_lsn()`, the WAL *written* so far. A
  commit acknowledged under `synchronous_commit = off` sits past that
  until the WAL writer's next round - by up to 2,264 bytes for 4.7s at
  `wal_writer_delay = 10s`, and past it every time even at the default.
  The insert position is where every acknowledged commit is.
* it waited for the smallest confirmed position of the database's
  *active* slots. With the hop's own slot inactive for the moment and
  another consumer's active and caught up, it passed on the other
  consumer's position, which says nothing about this target.
"""
import subprocess
import time

import psycopg2

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.fence", "postgres")]

pytestmark = needs_docker

SRC = "migkit-test-pg-src"


def _hop(pg_pair, tmp_path, **options):
    ep = {"user": "postgres", "password": "test", "host": "127.0.0.1"}
    hop = Hop(name="fence-own", engine="postgres",
              source=Endpoint(port=pg_pair["src"], **ep),
              target=Endpoint(port=pg_pair["dst"], **ep),
              databases=["postgres"], options=options)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _conn(port):
    conn = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                            password="test", dbname="postgres")
    conn.autocommit = True
    return conn


def _one(conn, sql, args=None):
    cur = conn.cursor()
    cur.execute(sql, args)
    return cur.fetchone()[0]


def test_a_position_at_a_page_start_is_where_a_record_can_end():
    """The insert position just past a page's header is the page's start
    for a consumer: nothing ends inside the header."""
    from migkit.engines.postgres import PostgresEngine
    at = PostgresEngine._past_page_header
    # 24 bytes into a page, 40 into a segment's first page
    assert at("0/3002018 8192 16777216") == "0/3002000"
    assert at("1/1000028 8192 16777216") == "1/1000000"
    # anywhere else is left where it is - a record can end there
    assert at("0/3002A48 8192 16777216") == "0/3002A48"
    assert at("0/3002000 8192 16777216") == "0/3002000"
    # 40 bytes into a page that does not start a segment is a record's end
    assert at("0/3002028 8192 16777216") == "0/3002028"


def test_a_commit_acknowledged_without_waiting_is_inside_the_fence(
        pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(pg_pair, tmp_path))
    admin = _conn(pg_pair["src"])
    admin.cursor().execute("alter system set wal_writer_delay = '10s'")
    admin.cursor().execute("select pg_reload_conf()")
    try:
        writer = _conn(pg_pair["src"])
        writer.cursor().execute("set synchronous_commit = off")
        writer.cursor().execute("create table public.fenced (id int primary"
                                " key)")
        # the WAL writer woken once, and into its ten-second round
        writer.cursor().execute("insert into public.fenced values (0)")
        time.sleep(1)
        trailed = 0
        for i in range(1, 6):
            writer.cursor().execute("insert into public.fenced values (%s)",
                                    (i,))
            # where the commit ends: the insert position right after it,
            # less a page header it may point past
            committed = _one(
                writer, "select (pg_current_wal_insert_lsn() - case"
                        " when (pg_current_wal_insert_lsn() - '0/0')"
                        " % 16777216 = 40 then 40 when"
                        " (pg_current_wal_insert_lsn() - '0/0') % 8192 = 24"
                        " then 24 else 0 end)::text")
            # the written position, as the fence took it before
            trailed += _one(admin, "select pg_current_wal_lsn() <"
                                   " %s::pg_lsn", (committed,))
            got = eng.src_lsn("postgres")
            assert eng.position_reached(got, committed), (got, committed)
            time.sleep(0.3)
        # the written position trailed a commit at least once: the case the
        # fence has to cover was there to be covered
        assert trailed >= 1, trailed
    finally:
        admin.cursor().execute("alter system reset wal_writer_delay")
        admin.cursor().execute("select pg_reload_conf()")


def _slot(name, plugin="pgoutput"):
    psql(55432, f"select pg_create_logical_replication_slot('{name}',"
                f" '{plugin}')")


def _drop_slots(*names):
    subprocess.run(["docker", "exec", SRC, "pkill", "-f", "pg_recvlogical"],
                   capture_output=True)
    for name in names:
        for _ in range(20):
            got = psql(55432, "select count(*) from pg_replication_slots"
                              f" where slot_name = '{name}' and active")
            if got.stdout.strip() == "0":
                break
            time.sleep(0.5)
        psql(55432, "select pg_drop_replication_slot(slot_name) from"
                    f" pg_replication_slots where slot_name = '{name}'")


def _another_consumer(name):
    """A consumer that is not the hop's, reading the source and confirming
    what it read every second."""
    return subprocess.Popen(
        ["docker", "exec", SRC, "pg_recvlogical", "-U", "postgres", "-d",
         "postgres", "-S", name, "--start", "-f", "/dev/null", "-F", "0",
         "-s", "1"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _caught_up(name, lsn, wait=30):
    end = time.time() + wait
    while time.time() < end:
        got = psql(55432, "select active and confirmed_flush_lsn >="
                          f" '{lsn}'::pg_lsn from pg_replication_slots"
                          f" where slot_name = '{name}'").stdout.strip()
        if got == "t":
            return True
        time.sleep(0.5)
    return False


def test_the_fence_waits_for_the_hops_own_slot_not_another_consumers(
        pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(pg_pair, tmp_path))
    # the slot the hop's subscription reads: named after the subscription,
    # `migkit_` and the hop's name
    own = "migkit_fence_own"
    psql(55432, "create table public.f (id int primary key)")
    _slot(own)
    _slot("other_consumer", "test_decoding")
    reader = _another_consumer("other_consumer")
    try:
        psql(55432, "insert into public.f values (1)")
        lsn = eng.src_lsn("postgres")
        assert _caught_up("other_consumer", lsn), "the other consumer never" \
                                                  " got there"
        assert psql(55432, "select active from pg_replication_slots where"
                           f" slot_name = '{own}'").stdout.strip() == "f"
        # the only active slot has passed it; the hop's own has not
        assert eng.fence_wait("postgres", lsn, timeout=4) is False
        # and once the hop's own slot is there, it passes
        psql(55432, f"select pg_replication_slot_advance('{own}',"
                    f" '{lsn}')")
        assert eng.fence_wait("postgres", lsn, timeout=4) is True
    finally:
        reader.kill()
        _drop_slots(own, "other_consumer")


def test_no_slot_of_the_hops_is_no_fence_whoever_else_reads(pg_pair,
                                                           tmp_path):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(pg_pair, tmp_path))
    psql(55432, "create table public.f (id int primary key)")
    _slot("other_consumer", "test_decoding")
    reader = _another_consumer("other_consumer")
    try:
        psql(55432, "insert into public.f values (1)")
        lsn = eng.src_lsn("postgres")
        assert _caught_up("other_consumer", lsn)
        assert eng.fence_wait("postgres", lsn, timeout=4) is None
        # a pipeline migkit did not set up is fenced when the hop names it
        named = PostgresEngine(_hop(pg_pair, tmp_path,
                                    slot="other_consumer"))
        assert named.fence_wait("postgres", lsn, timeout=10) is True
    finally:
        reader.kill()
        _drop_slots("other_consumer")


def test_the_seed_slot_is_waited_on_with_no_subscription_on_it(pg_pair,
                                                               tmp_path):
    """A physical seed makes its slot before the copy, and nothing reads it
    until the stream that goes on from the copy starts. The fence waits on
    it by name all the same: behind, it holds; caught up, it passes -
    where before it found no slot of the hop's and could not fence."""
    from migkit import physical
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(pg_pair, tmp_path))
    seed = physical.seed_slot(eng, "postgres")
    assert seed in eng.own_slots("postgres")
    psql(55432, "create table if not exists public.seeded (id int primary"
                " key)")
    _slot(seed)
    try:
        psql(55432, "insert into public.seeded values (1)")
        lsn = eng.src_lsn("postgres")
        # no subscription on the target reads it
        assert psql(55433, "select count(*) from pg_subscription"
                    ).stdout.strip() == "0"
        assert eng.fence_wait("postgres", lsn, timeout=4) is False
        psql(55432, f"select pg_replication_slot_advance('{seed}',"
                    f" '{lsn}')")
        assert eng.fence_wait("postgres", lsn, timeout=4) is True
    finally:
        _drop_slots(seed)
        psql(55432, "drop table if exists public.seeded")
