"""The PostgreSQL stream publishes exactly the hop's tables.

The publication was `FOR ALL TABLES`. That needs a superuser; it streams
the tables the hop excludes - the target's own - into the target; and it
publishes tables the target does not have, another tool's bookkeeping
among them, over which `CREATE SUBSCRIPTION` will not start. A list of the
hop's tables needs only their owner. A table made later is carried by no
publication on its own - a subscription takes on a new published table
only at `REFRESH PUBLICATION`, once the target has it - so a later run
sets the list again and refreshes: the same step either way.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

NET = "migkit-test-out-net"
SRC, DST = "migkit-test-out-pgs", "migkit-test-out-pgd"
SRC_PORT, DST_PORT = 16094, 16095


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def sql(name, text, user="postgres", ok=True):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", user, "-d", "postgres", "-At", "-v",
                          "ON_ERROR_STOP=1", "-c", text],
                         capture_output=True, text=True)
    if ok:
        assert got.returncode == 0, got.stderr
    return got


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    try:
        for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name,
                            "--network", NET, "-e", "POSTGRES_PASSWORD=test",
                            "-p", f"127.0.0.1:{port}:5432", "postgres:16",
                            "-c", "wal_level=logical"], check=True,
                           capture_output=True)
        for name in (SRC, DST):
            for _ in range(60):
                if sql(name, "select 1", ok=False).returncode == 0:
                    break
                time.sleep(1)
        # the application's owner: it may replicate, and is no superuser
        sql(SRC, "create role app login replication password 'app';"
                 " grant create on database postgres to app;"
                 " grant create on schema public to app")
        sql(SRC, "set role app; create table public.orders (id int primary"
                 " key, v text); create table public.audit (id int primary"
                 " key, v text); insert into public.orders values (1, 'a'),"
                 " (2, 'b'); insert into public.audit values (1, 'src')")
        # another tool's bookkeeping, which the target does not have
        sql(SRC, "create schema pgcopydb; create table pgcopydb.sentinel"
                 " (id int primary key); insert into pgcopydb.sentinel"
                 " values (1)")
        sql(DST, "create table public.orders (id int primary key, v text);"
                 " create table public.audit (id int primary key, v text);"
                 " insert into public.audit values (100, 'the target owns"
                 " this')")
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _eng():
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="pub", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="app",
                              password="app"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              user="postgres", password="test"),
              databases=["postgres"], exclude=["audit"])
    return PostgresEngine(hop)


def _run(eng, plan):
    for stmt in plan["src"]:
        eng.apply_replication_stmt("src", "postgres", stmt)
    for stmt in plan["dst"]:
        # the target reaches the source by its name on their network
        eng.apply_replication_stmt(
            "dst", "postgres",
            stmt.replace(f"host=127.0.0.1 port={SRC_PORT}",
                         f"host={SRC} port=5432"))


def _until(check, secs=60):
    end = time.time() + secs
    while time.time() < end:
        if check():
            return True
        time.sleep(0.5)
    return False


@pytest.mark.docker
def test_the_publication_lists_the_hops_tables_and_needs_no_superuser(pair):
    eng = _eng()
    plan = eng.replicate_sql("postgres", copy_data=True)
    stmt = plan["src"][0]
    assert "for all tables" not in stmt.lower(), stmt
    assert 'for table "public"."orders"' in stmt, stmt
    assert "audit" not in stmt and "pgcopydb" not in stmt, stmt
    # a first run has nothing to refresh
    assert len(plan["dst"]) == 1, plan["dst"]
    # what it was needs a superuser the application's owner is not
    got = sql(SRC, "create publication everything for all tables",
              user="app", ok=False)
    assert got.returncode != 0 and "superuser" in got.stderr, got.stderr
    name = eng._repl_name()
    try:
        _run(eng, plan)
        assert _until(lambda: sql(DST, "select count(*) from public.orders"
                                  ).stdout.strip() == "2")
        sql(SRC, "insert into public.orders values (3, 'c');"
                 " insert into public.audit values (2, 'src')")
        assert _until(lambda: sql(DST, "select count(*) from public.orders"
                                  ).stdout.strip() == "3")
        # the table the hop excludes is the target's, left as it was
        assert sql(DST, "select id, v from public.audit order by 1"
                   ).stdout.strip() == "100|the target owns this"

        # a table made later, on both sides: the next run takes it on
        sql(SRC, "set role app; create table public.later (id int primary"
                 " key); insert into public.later values (7), (8)")
        sql(DST, "create table public.later (id int primary key)")
        again = eng.replicate_sql("postgres", copy_data=True)
        assert '"public"."later"' in again["src"][0], again["src"]
        assert any("refresh publication" in s for s in again["dst"]), \
            again["dst"]
        _run(eng, again)
        assert _until(lambda: sql(DST, "select count(*) from public.later"
                                  ).stdout.strip() == "2")
        sql(SRC, "insert into public.later values (9)")
        assert _until(lambda: sql(DST, "select count(*) from public.later"
                                  ).stdout.strip() == "3")
        # still exactly one publication, of the hop's tables only
        got = sql(SRC, "select schemaname||'.'||tablename from"
                       f" pg_publication_tables where pubname = '{name}'"
                       " order by 1").stdout.split()
        assert got == ["public.later", "public.orders"], got
    finally:
        sql(DST, f"drop subscription if exists {name}", ok=False)
        sql(SRC, f"drop publication if exists {name}", ok=False)
