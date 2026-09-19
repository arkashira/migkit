"""Does the static lock classification match what PostgreSQL actually takes?

Each statement runs inside a transaction against a throwaway database,
`pg_locks` is read for our own backend, and the transaction is rolled back.
Without this the lock table in `migkit/locks.py` would be a plausible-looking
invention, and operators would be making cutover decisions on it.
"""
import socket
import subprocess
import time

import pytest

from migkit.locks import MODES, SEVERITY, classify

NAME = "migkit-test-locks"
PORT = 15484


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def conn():
    import psycopg2
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(PORT)
    last = None
    for _ in range(30):
        try:
            c = psycopg2.connect(host="127.0.0.1", port=PORT, user="postgres",
                                 password="test", dbname="postgres",
                                 connect_timeout=5)
            break
        except Exception as e:      # the port opens before the server does
            last = e
            time.sleep(2)
    else:
        pytest.fail(f"postgres never accepted a connection: {last}")
    c.autocommit = True
    cur = c.cursor()
    cur.execute("""
        drop table if exists child, parent cascade;
        create table parent (id int primary key, n int, s text);
        create table child (id int primary key, parent_id int, v int);
        insert into parent select g, g, 'x'||g from generate_series(1,200) g;
        insert into child select g, g, g from generate_series(1,200) g;
    """)
    yield c
    c.close()
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


LOCKS_SQL = """
select max(case l.mode when 'AccessShareLock' then 0
                       when 'RowShareLock' then 1
                       when 'RowExclusiveLock' then 2
                       when 'ShareUpdateExclusiveLock' then 3
                       when 'ShareLock' then 4
                       when 'ShareRowExclusiveLock' then 5
                       when 'ExclusiveLock' then 6
                       when 'AccessExclusiveLock' then 7 end)
from pg_locks l
join pg_class c on c.oid = l.relation
join pg_namespace n on n.oid = c.relnamespace
where l.pid = pg_backend_pid() and l.locktype = 'relation'
  and n.nspname = 'public'
  -- only the tables that existed before the statement. A CREATE INDEX also
  -- takes AccessExclusiveLock on the index it is building, and measuring that
  -- would answer a question nobody is asking: no other session can see the
  -- new index yet. What blocks an application is the lock on the live table.
  and c.relname in ('parent', 'child')
"""


def _measure(conn, stmt):
    """Highest lock mode the statement takes on a user table."""
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute(stmt)
        cur.execute(LOCKS_SQL)
        got = cur.fetchone()[0]
    finally:
        conn.rollback()
        conn.autocommit = True
    return MODES[got] if got is not None else None


# Statements that can be measured this way. CREATE INDEX CONCURRENTLY is
# excluded on purpose: PostgreSQL refuses to run it inside a transaction, so
# there is no way to hold its lock open and read it - which is itself the
# reason it is the safe one.
CASES = [
    "create index idx_parent_n on parent (n)",
    "alter table parent add column extra int",
    "alter table parent add column extra2 int default 0",
    "alter table parent add constraint ck_n check (n > 0)",
    "alter table parent add constraint ck_n2 check (n > 0) not valid",
    "alter table child add constraint fk_p foreign key (parent_id)"
    " references parent (id)",
    "alter table parent alter column n type bigint",
    "alter table parent alter column s set not null",
    "grant select on parent to postgres",
    "comment on table parent is 'x'",
]


@pytest.mark.parametrize("stmt", CASES)
def test_static_verdict_is_not_lighter_than_the_real_lock(conn, stmt):
    """The one-sided guarantee that matters.

    Being heavier than reality is a false alarm and costs a re-read. Being
    lighter means telling someone a statement is safe when it will stop their
    application, so that direction must never happen.
    """
    measured = _measure(conn, stmt)
    if measured is None:
        pytest.skip("no relation lock recorded for this statement")
    predicted = classify(stmt)[0]
    assert SEVERITY[predicted] >= SEVERITY[measured], (
        f"predicted {predicted} but PostgreSQL took {measured}: {stmt}")


def test_the_index_build_really_does_block_writes(conn):
    """Spot-check the exact value, not just the direction, on the case the
    advice is most often about."""
    assert _measure(conn, "create index idx_parent_n on parent (n)") == \
        "ShareLock"


def test_validate_constraint_really_is_the_cheap_half(conn):
    cur = conn.cursor()
    cur.execute("alter table parent add constraint ck_v check (n > 0)"
                " not valid")
    try:
        assert _measure(conn, "alter table parent validate constraint ck_v") \
            == "ShareUpdateExclusiveLock"
    finally:
        cur.execute("alter table parent drop constraint ck_v")
