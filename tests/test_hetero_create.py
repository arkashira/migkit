"""Building the table the rows are about to land in.

A move used to stop when the target table was missing. Creating it is the
other half of what a migration tool is for - but the half that is easy to get
dangerously wrong, so the rules are narrow: never touch a table that already
exists, never guess a column type, and widen rather than narrow when the class
does not carry the source's own numbers.
"""
import socket
import subprocess
import time

import pytest

PG, MY = "migkit-test-cr-pg", "migkit-test-cr-my"
PG_PORT, MY_PORT = 15467, 13367


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


def my_sql(sql, db=None):
    cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
           "--default-character-set=utf8mb4", "-N", "-B"]
    if db:
        cmd += ["-D", db]
    return subprocess.run(cmd + ["-e", sql], capture_output=True, text=True)


class _Checkpoint(dict):
    def save(self):
        pass


@pytest.fixture(scope="module")
def engine():
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    assert _wait(PG_PORT) and _wait(MY_PORT)
    for _ in range(40):
        if pg_sql("select 1", "postgres").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never answered")
    for _ in range(60):
        if my_sql("select 1").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never answered")
    assert pg_sql("create database cx", "postgres").returncode == 0
    assert my_sql("create database cx").returncode == 0
    assert pg_sql(
        "create table wide (id bigint primary key, name varchar(50),"
        " amount numeric(12,4), ratio double precision, flag boolean,"
        " made timestamp(6), blob_col bytea, doc jsonb);"
        " insert into wide values"
        " (1,'café',-0.05,cast(1.0 as double precision)/7,true,"
        "  '2026-01-01 00:00:00.123456','\\x00FF41','{\"b\":1,\"a\":2}'),"
        " (2,'',0.0001,0.000001,false,'2026-09-19 13:45:06','\\x',"
        "  '[]')").returncode == 0
    assert pg_sql("select count(*) from wide").stdout.strip() == "2"

    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="cr", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=MY_PORT,
                              user="root", password="test"),
              db_map={"cx": "cx"},
              options={"source_engine": "postgres",
                       "target_engine": "mysql"})
    yield HeteroEngine(hop)
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def test_a_missing_target_is_built_and_the_rows_verify(engine):
    """The whole point: nothing on the MySQL side but an empty database, and
    afterwards the digest agrees with PostgreSQL's."""
    assert my_sql("select count(*) from information_schema.tables"
                  " where table_schema='cx' and table_name='wide'"
                  ).stdout.strip() == "0"
    lines = []
    engine.move_table("cx", "public", "wide", 10, _Checkpoint(), lines.append)
    assert any("created it - create table" in m for m in lines), lines
    assert my_sql("select count(*) from wide", "cx").stdout.strip() == "2"

    got = [r for r in engine.check_data("cx") if r.scope.endswith(".wide")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    assert got[0].status == "ok", got[0].detail


def test_the_source_numbers_travel_into_the_created_types(engine):
    """`varchar(50)` and `numeric(12,4)` carry a length and a precision that
    the neutral class does not. A target built from the class alone would be
    a `varchar(1024)` and a `decimal(65,10)` - wider, so nothing truncates,
    and a constraint the application relied on would be gone."""
    got = dict(l.split("\t") for l in my_sql(
        "select column_name, column_type from information_schema.columns"
        " where table_schema='cx' and table_name='wide'",
        "cx").stdout.strip().splitlines())
    assert got["name"] == "varchar(50)", got
    assert got["amount"] == "decimal(12,4)", got
    assert got["id"] == "bigint", got
    assert got["flag"] == "tinyint(1)", got
    assert got["made"].startswith("datetime(6)"), got


def test_the_primary_key_comes_across(engine):
    """Without it the write cannot replace by key, so a restart would
    duplicate instead of converge."""
    keys = my_sql("select column_name from information_schema"
                  ".key_column_usage where table_schema='cx'"
                  " and table_name='wide' and constraint_name='PRIMARY'",
                  "cx").stdout.strip()
    assert keys == "id", keys


def test_an_existing_table_is_never_redefined(engine):
    """The one thing worse than a missing target is a target that used to
    hold something else."""
    assert my_sql("create table taken (id bigint primary key, note text);"
                  " insert into taken values (1,'do not lose me')",
                  "cx").returncode == 0
    assert pg_sql("create table taken (id bigint primary key,"
                  " other text)").returncode == 0
    try:
        # the table exists on both sides, so the mover pairs them and never
        # reaches the create path - the direct call is what proves the guard
        with pytest.raises(SystemExit) as e:
            engine.dst_engine.neutral_create(
                "dst", "cx", "taken", [("id", "integer", ())], ["id"])
        assert "already exists" in str(e.value)
        assert my_sql("select note from taken where id=1",
                      "cx").stdout.strip() == "do not lose me"
    finally:
        my_sql("drop table taken", "cx")
        pg_sql("drop table taken")


def test_a_type_with_no_neutral_class_stops_the_build(engine):
    """Guessing a column type is how a migration arrives complete and wrong.
    The refusal names the column and the type so the operator can create the
    table themselves."""
    assert pg_sql("create table odd (id bigint primary key,"
                  " v point)").returncode == 0
    try:
        with pytest.raises(SystemExit) as e:
            engine.move_table("cx", "public", "odd", 10, _Checkpoint(),
                              lambda m: None)
        assert "no neutral class for v (point)" in str(e.value)
        assert my_sql("select count(*) from information_schema.tables"
                      " where table_schema='cx' and table_name='odd'"
                      ).stdout.strip() == "0"
    finally:
        pg_sql("drop table odd")


def test_a_source_without_a_key_is_built_without_one_and_says_so(engine):
    assert pg_sql("create table nokey (a int, b text)").returncode == 0
    assert pg_sql("insert into nokey values (1,'x'),(1,'x')").returncode == 0
    try:
        lines = []
        engine.move_table("cx", "public", "nokey", 10, _Checkpoint(),
                          lines.append)
        assert any("created without a primary key" in m for m in lines), lines
        assert my_sql("select count(*) from nokey",
                      "cx").stdout.strip() == "2"
        got = [r for r in engine.check_data("cx")
               if r.scope.endswith(".nokey")]
        assert got[0].status == "ok", got[0].detail
    finally:
        pg_sql("drop table nokey")
        my_sql("drop table nokey", "cx")


def test_binary_arrives_as_bytes_not_as_the_text_of_a_python_object(engine):
    """psycopg2 hands a `bytea` column back as a memoryview and pymysql has
    no escape rule for one. Measured before the fix: MySQL stored
    `<memory at 0x10ad03dc0>` - 23 bytes of an object address where the
    source held 3 bytes of data. No error, the right row count, and the
    content replaced. This is the failure the digest exists to catch, so it
    is checked here as bytes rather than only through the digest."""
    src = pg_sql("select upper(encode(blob_col,'hex')) from wide"
                 " order by id").stdout.strip().splitlines()
    dst = my_sql("select hex(blob_col) from wide order by id",
                 "cx").stdout.strip().splitlines()
    assert src == dst, (src, dst)
    assert src[0] == "00FF41", src
    assert "6D656D6F7279" not in dst[0], dst      # hex of "memory"


def test_convert_schema_prints_exactly_what_the_mover_would_run(engine):
    """One source of truth. When the printed DDL and the executed DDL were
    written separately, the one an operator reviewed was not the one that
    ran - `convert_ddl` was a sqlglot transpile plus ten regular expressions
    while `move` used the type mapping."""
    assert my_sql("drop table if exists wide", "cx").returncode == 0
    printed = [s for s in engine.convert_ddl("cx")
               if '`wide`' in s or '"wide"' in s]
    assert len(printed) == 1, printed

    lines = []
    engine.move_table("cx", "public", "wide", 10, _Checkpoint(), lines.append)
    ran = [m.split("created it - ", 1)[1] for m in lines
           if "created it - " in m]
    assert len(ran) == 1, lines
    assert printed[0] == ran[0] + ";", (printed[0], ran[0])


def test_convert_schema_covers_every_source_table_not_just_one_pair(engine):
    """It used to read `show create table` from MySQL and transpile it, so
    the source had to be MySQL. It now asks whichever engine is on the left."""
    assert pg_sql("create table extra_one (id bigint primary key,"
                  " v varchar(10))").returncode == 0
    try:
        got = engine.convert_ddl("cx")
        assert any("extra_one" in s for s in got), got
        one = [s for s in got if "extra_one" in s][0]
        assert one.startswith("create table `cx`.`extra_one` ("), one
        assert "`v` varchar(10)" in one, one
        assert "primary key (`id`)" in one, one
        assert one.endswith(";"), one
    finally:
        pg_sql("drop table extra_one")
        my_sql("drop table if exists extra_one", "cx")
