"""A real PostgreSQL server at wal_level=replica, in the sandbox (backlog
LT.R9): no logical slot can be made, so the follow climbs to the server's
own WAL reading functions (`PostgresEngine.FOLLOW_WAYS`, "wal inspect":
`pg_get_wal_records_info` from 15 on). Rows inserted, updated and deleted
during the follow arrive; a table out of scope is never read; a
transaction still open holds the position until it commits and then
arrives whole; a follow stopped and started again from its saved position
in a new process applies nothing it applied before."""
import os
import time
from types import SimpleNamespace

import pytest

from migkit import decide
from migkit.engines.postgres import PostgresEngine

from . import servers

psycopg2 = pytest.importorskip("psycopg2")
pytestmark = pytest.mark.skipif(not servers.docker_up(),
                                reason="no docker daemon")

IMAGES = ("postgres:16", "postgres:17")


@pytest.fixture(scope="module", params=IMAGES)
def source(request):
    with servers.private(
            __name__, "src-" + request.param.split(":")[1],
            request.param, "-c", "wal_level=replica",
            "-c", "wal_keep_size=256MB", ports=(5432,),
            env={"POSTGRES_PASSWORD": servers.PASSWORD}) as (_, mapped):
        dsn = dict(host=servers.HOST, port=mapped[5432], user="postgres",
                   password=servers.PASSWORD, dbname="postgres")
        for _ in range(120):
            try:
                conn = psycopg2.connect(**dsn, connect_timeout=2)
                break
            except psycopg2.OperationalError:
                time.sleep(0.5)
        else:
            pytest.fail("the sandbox server did not take connections")
        conn.autocommit = True
        with conn.cursor() as c:
            c.execute("create table items (id int primary key, v text)")
            c.execute("create table other (id int primary key, v text)")
            c.execute("insert into items select g, 'v' || g from"
                      " generate_series(1, 3) g")
        yield SimpleNamespace(conn=conn, dsn=dsn, image=request.param)
        conn.close()


def engine(src, kept):
    """The PostgreSQL engine as a follow in a process of its own: nothing
    held in memory, only what it saved beside its position."""
    e = PostgresEngine.__new__(PostgresEngine)
    e.hop = SimpleNamespace(name="", options={},
                            report_dir=lambda db="": kept)
    e.in_scope = lambda db, t: t == "public.items"
    e.neutral_key = lambda side, db, t: ["id"]
    e._split = lambda t: tuple(t.split("."))
    conn = psycopg2.connect(**src.dsn)
    conn.autocommit = True

    def ask(side, db, sql, args=None):
        with conn.cursor() as c:
            c.execute(sql, args)
            return c.fetchall() if c.description else []
    e._wal_ask = ask
    return e


def run(sql, src):
    with src.conn.cursor() as c:
        c.execute(sql)


def rows(src):
    with src.conn.cursor() as c:
        c.execute("select id, v from items order by id")
        return dict(c.fetchall())


def apply(target, changes, seen):
    for ch in changes:
        k = ch["key"]["id"]
        seen.append((ch["op"], k))
        if ch["op"] == "delete":
            target.pop(k, None)
        else:
            target[k] = ch["values"]["v"]


@pytest.fixture(autouse=True)
def _no_costs_kept(monkeypatch):
    monkeypatch.setattr(decide, "HopCosts", lambda hop, prefix="":
                        SimpleNamespace(saw=lambda *a: None))


def test_changes_arrive_and_a_resumed_follow_applies_nothing_twice(
        source, tmp_path):
    e = engine(source, tmp_path)
    facts = e._follow_facts("src", "postgres")
    assert facts["wal_level"] == "replica"
    way = PostgresEngine.follow_choice(facts)
    assert way.path == "wal inspect", way.reason
    assert facts["wal functions"] in ("installed", "creatable")

    target = rows(source)
    out, token = e._wal_changes("src", "postgres", None, 1000, way)
    assert out == [] and token

    run("insert into items values (4, 'v4')", source)
    run("update items set v = 'two' where id = 2", source)
    run("delete from items where id = 3", source)
    run("insert into other values (1, 'never read')", source)
    seen = []
    out, token = e._wal_changes("src", "postgres", token, 1000, way)
    apply(target, out, seen)
    assert target == rows(source) == {1: "v1", 2: "two", 4: "v4"}
    assert ("delete", 3) in seen and ("insert", 4) in seen
    assert all(ch["table"] == "public.items" for ch in out
               if "table" in ch)

    # a transaction still open holds the position: none of it arrives
    other = psycopg2.connect(**source.dsn)
    with other.cursor() as c:
        c.execute("insert into items values (5, 'v5')")
        c.execute("update items set v = 'one' where id = 1")
        c.execute("select pg_current_wal_insert_lsn()")
    run("checkpoint", source)
    out, held = e._wal_changes("src", "postgres", token, 1000, way)
    apply(target, out, seen)
    assert 5 not in target and target[1] == "v1"

    # the follow stops here; a new one starts from the saved position
    del e
    other.commit()
    other.close()
    before = len(seen)
    e2 = engine(source, tmp_path)
    out, token = e2._wal_changes("src", "postgres", held, 1000,
                                 PostgresEngine.follow_choice(
                                     e2._follow_facts("src", "postgres")))
    apply(target, out, seen)
    assert target == rows(source)
    assert target[5] == "v5" and target[1] == "one"
    again = seen[before:]
    # nothing applied before the stop arrives again as new: no second
    # insert of 4, no second delete of 3
    assert ("insert", 4) not in again and ("delete", 3) not in again

    # and a second start from the same position, nothing written since,
    # converges on the same rows without one row arriving as new
    e3 = engine(source, tmp_path)
    out, last = e3._wal_changes("src", "postgres", token, 1000,
                                PostgresEngine.follow_choice(
                                    e3._follow_facts("src", "postgres")))
    assert [c for c in out if c["op"] != "update"] == []
    apply(target, out, seen)
    assert target == rows(source)


def test_the_records_are_read_by_the_functions_of_the_servers_version(
        source):
    with source.conn.cursor() as c:
        c.execute("show server_version_num")
        version = int(c.fetchone()[0])
        c.execute("select proname from pg_proc where proname like"
                  " 'pg_get_wal_%' order by 1")
        have = {r[0] for r in c.fetchall()}
    assert version >= 150000
    assert "pg_get_wal_records_info" in have
    assert ("pg_get_wal_block_info" in have) == (version >= 160000)
    assert os.environ.get("MIGKIT_TEST_LANE", "0").isdigit()
