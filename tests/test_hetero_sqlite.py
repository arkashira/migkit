"""A hop whose source is a file and whose target is a server.

SQLite has no port, no user and no server process, so nothing about it looks
like the other side of this pair. It joins anyway, because what the comparison
needs from an engine is three answers and a rendering - not a connection that
resembles anyone else's.

This is the pairing that shows the registry doing its job: neither half of it
was written with the other in mind.
"""
import socket
import sqlite3
import subprocess
import time

import pytest

PG = "migkit-test-hsq-pg"
PG_PORT = 15477

ROWS = [(1, "café", 1e20), (2, "", 0.000001), (3, None, None)]


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


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def pg_sql(sql, db="cx"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    path = tmp_path_factory.mktemp("hsq") / "a.db"
    conn = sqlite3.connect(path)
    conn.execute("create table items (id integer primary key, name text,"
                 " ratio real)")
    conn.executemany("insert into items values (?,?,?)", ROWS)
    conn.commit()
    assert conn.execute("select count(*) from items").fetchone()[0] == 3
    conn.close()

    subprocess.run(["docker", "rm", "-f", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(PG_PORT)
    for _ in range(40):
        if pg_sql("select 1", "postgres").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never answered")
    assert pg_sql("create database cx", "postgres").returncode == 0
    assert pg_sql("create table items (id bigint primary key, name text,"
                  " ratio double precision)").returncode == 0
    for id_, name, ratio in ROWS:
        lit = ("null" if name is None else "'" + name + "'")
        rat = "null" if ratio is None else repr(ratio)
        assert pg_sql(f"insert into items values ({id_},{lit},{rat})"
                      ).returncode == 0
    assert pg_sql("select count(*) from items").stdout.strip() == "3"

    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="sq", engine="hetero",
              source=Endpoint(host=str(path), port=0, user="", password=""),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              db_map={"main": "cx"},
              options={"source_engine": "sqlite",
                       "target_engine": "postgres"})
    eng = HeteroEngine(hop)
    yield eng, path
    subprocess.run(["docker", "rm", "-f", PG], capture_output=True)


def _one(eng):
    got = [r for r in eng.check_data("main") if r.scope.endswith(".items")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def test_a_file_and_a_server_hold_the_same_rows(engine):
    eng, _ = engine
    assert eng.databases() == ["main"]
    r = _one(eng)
    assert r.status == "ok", r.detail
    assert "sqlite/postgres" in r.detail
    assert "rows 3" in r.detail


def test_a_float_in_the_band_the_engines_render_differently_still_matches(
        engine):
    """0.000001 is the value PostgreSQL prints as `1e-06` and SQLite prints
    as `1e-06` too, while MySQL prints `0.000001`. A comparison built on the
    engines' own text would have been wrong here for one pair and right for
    another, which is the worst way to be wrong."""
    eng, path = engine
    conn = sqlite3.connect(path)
    conn.execute("update items set ratio = 0.000002 where id = 2")
    conn.commit()
    conn.close()
    try:
        r = _one(eng)
        assert r.status == "diff", r.detail
        assert "rows 3 match but the contents do not" in r.detail
    finally:
        conn = sqlite3.connect(path)
        conn.execute("update items set ratio = 0.000001 where id = 2")
        conn.commit()
        conn.close()
    assert _one(eng).status == "ok"


def test_rows_move_from_the_file_into_the_server(engine):
    """This pair refused to move at all until the read/write contract existed.
    A row is deleted from the target and carried back across, and the digest
    - not the absence of an error - is what says it arrived."""
    eng, _ = engine

    class _Checkpoint(dict):
        def save(self):
            pass

    assert pg_sql("delete from items where id = 2").returncode == 0
    assert pg_sql("select count(*) from items").stdout.strip() == "2"
    assert _one(eng).status == "diff"

    assert ("", "items") in eng.list_move_tables("main"), \
        eng.list_move_tables("main")
    eng.move_table("main", "", "items", 10, _Checkpoint(), lambda m: None)
    assert pg_sql("select count(*) from items").stdout.strip() == "3"
    assert _one(eng).status == "ok"


def test_the_target_ddl_is_written_from_a_file_schema_too(engine):
    """`convert_ddl` used to read `show create table` out of MySQL and
    transpile it, so the source had to be MySQL. It now asks whichever engine
    is on the left, and SQLite - whose declared type is a hint rather than a
    guarantee - is as far from that as the sources get."""
    eng, _ = engine
    got = eng.convert_ddl("main")
    one = [s for s in got if "items" in s]
    assert len(one) == 1, got
    assert one[0].startswith('create table "public"."items" ('), one[0]
    assert '"ratio" double precision' in one[0], one[0]
    assert '"name" text' in one[0], one[0]
    assert 'primary key ("id")' in one[0], one[0]


def test_the_report_does_not_claim_a_local_file_crossed_a_network(engine):
    """SQLite is folded in this process too, but its data was already here -
    it is a file on this disk. Saying its rows crossed the network, as the
    line for MongoDB correctly does, would be wrong about the one fact an
    operator reads this clause for."""
    eng, _ = engine
    detail = _one(eng).detail
    assert "sqlite was folded in this process" in detail, detail
    assert "crossed the network" not in detail, detail
