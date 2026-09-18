"""Changes read out of one engine's log and applied to another.

This is the second half of the shape AWS DMS has: a full load that copies what
is there, then a tail that carries what happens next. The tail is where the
two engines stop resembling each other most - a binlog row event, a logical
slot message and a change stream document have nothing in common but meaning -
so what crosses is a record with four fields and no dialect in it.

The key is carried apart from the values because the payload is not always
enough to address the row: an UPDATE that moved the primary key has one key in
the before image and another in the after, and taking it from the values would
write a second row instead of moving the first. That case is tested.
"""
import socket
import subprocess
import time

import pytest

MY, PG = "migkit-test-cdc-my", "migkit-test-cdc-pg"
MY_PORT, PG_PORT = 13365, 15465


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


def my_sql(sql, db=None):
    cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
           "--default-character-set=utf8mb4", "-N", "-B"]
    if db:
        cmd += ["-D", db]
    return subprocess.run(cmd + ["-e", sql], capture_output=True, text=True)


def pg_sql(sql, db="cx"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


@pytest.fixture(scope="module")
def pair():
    for n in (MY, PG):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8", "--log-bin=binlog", "--server-id=1",
                    "--binlog-format=ROW", "--binlog-row-image=FULL",
                    "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(MY_PORT) and _wait(PG_PORT)
    for _ in range(60):
        if my_sql("select 1").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never answered")
    for _ in range(40):
        if pg_sql("select 1", "postgres").returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never answered")
    # the binlog has to be on, or every assertion below is about nothing
    assert my_sql("select @@log_bin").stdout.strip() == "1"
    assert my_sql("select @@binlog_format").stdout.strip() == "ROW"

    assert my_sql("create database cx").returncode == 0
    assert my_sql("create table t (id bigint primary key, name varchar(50),"
                  " amount decimal(12,4), blob_col varbinary(20))",
                  "cx").returncode == 0
    assert pg_sql("create database cx", "postgres").returncode == 0
    assert pg_sql("create table t (id bigint primary key, name varchar(50),"
                  " amount numeric(12,4), blob_col bytea)").returncode == 0

    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    my_hop = Hop(name="c", engine="mysql",
                 source=Endpoint(host="127.0.0.1", port=MY_PORT,
                                 user="root", password="test"),
                 target=Endpoint(host="127.0.0.1", port=MY_PORT,
                                 user="root", password="test"),
                 db_map={"cx": "cx"})
    pg_hop = Hop(name="c", engine="postgres",
                 source=Endpoint(host="127.0.0.1", port=PG_PORT,
                                 user="postgres", password="test"),
                 target=Endpoint(host="127.0.0.1", port=PG_PORT,
                                 user="postgres", password="test"),
                 db_map={"cx": "cx"})
    yield MySQLEngine(my_hop), PostgresEngine(pg_hop)
    for n in (MY, PG):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _drain(my, token=None):
    return my.neutral_changes("src", "cx", token, limit=500)


def test_the_three_operations_arrive_as_neutral_records(pair):
    my, _ = pair
    _, token = _drain(my)
    assert my_sql("insert into t values (1,'café',1.5,x'00FF41'),"
                  " (2,'two',2.5,x'01')", "cx").returncode == 0
    assert my_sql("update t set name='changed' where id=1",
                  "cx").returncode == 0
    assert my_sql("delete from t where id=2", "cx").returncode == 0
    changes, token = _drain(my, token)
    ops = [c["op"] for c in changes]
    assert ops == ["insert", "insert", "update", "delete"], changes
    assert changes[0]["table"] == "t"
    assert changes[0]["key"] == {"id": 1}
    assert changes[0]["values"]["name"] == "café"
    assert changes[3]["key"] == {"id": 2}
    assert changes[3]["values"] == {}


def test_the_changes_apply_onto_a_different_engine(pair):
    """The payoff: MySQL's binlog, PostgreSQL's table, and the row counts
    and contents line up afterwards."""
    my, pg = pair
    assert pg_sql("delete from t").returncode == 0
    assert my_sql("truncate table t", "cx").returncode == 0
    _, token = _drain(my)

    assert my_sql("insert into t values (10,'ten',10.25,x'AB'),"
                  " (11,'eleven',11.5,x'CD')", "cx").returncode == 0
    assert my_sql("update t set amount=99.75 where id=10",
                  "cx").returncode == 0
    assert my_sql("delete from t where id=11", "cx").returncode == 0
    changes, token = _drain(my, token)
    assert pg.neutral_apply("dst", "cx", changes) == len(changes)

    assert pg_sql("select count(*) from t").stdout.strip() == "1"
    got = pg_sql("select id, name, amount::text,"
                 " upper(encode(blob_col,'hex')) from t").stdout.strip()
    assert got == "10|ten|99.7500|AB", got


def test_an_update_that_moves_the_key_moves_the_row(pair):
    """The reason `key` is not read out of `values`. Taking the key from the
    after image would insert a second row and leave the first one behind."""
    my, pg = pair
    assert pg_sql("delete from t").returncode == 0
    assert my_sql("truncate table t", "cx").returncode == 0
    _, token = _drain(my)

    assert my_sql("insert into t values (20,'before',1,x'00')",
                  "cx").returncode == 0
    changes, token = _drain(my, token)
    pg.neutral_apply("dst", "cx", changes)
    assert pg_sql("select count(*) from t").stdout.strip() == "1"

    assert my_sql("update t set id=21, name='after' where id=20",
                  "cx").returncode == 0
    changes, token = _drain(my, token)
    assert len(changes) == 1 and changes[0]["op"] == "update", changes
    assert changes[0]["key"] == {"id": 20}, changes[0]
    assert changes[0]["values"]["id"] == 21, changes[0]
    pg.neutral_apply("dst", "cx", changes)

    rows = pg_sql("select id, name from t order by id").stdout.strip()
    assert rows == "21|after", rows


def test_replaying_the_same_changes_is_a_no_op(pair):
    """A tail that is restarted replays what it had already applied. If that
    duplicated rows, every restart would corrupt the target a little more."""
    my, pg = pair
    assert pg_sql("delete from t").returncode == 0
    assert my_sql("truncate table t", "cx").returncode == 0
    _, token = _drain(my)
    assert my_sql("insert into t values (30,'x',1,x'00');"
                  " update t set name='y' where id=30;"
                  " delete from t where id=30;"
                  " insert into t values (31,'keep',2,x'11')",
                  "cx").returncode == 0
    changes, token = _drain(my, token)
    pg.neutral_apply("dst", "cx", changes)
    first = pg_sql("select id, name from t order by id").stdout.strip()
    pg.neutral_apply("dst", "cx", changes)
    assert pg_sql("select id, name from t order by id").stdout.strip() \
        == first
    assert first == "31|keep", first


def test_the_token_resumes_rather_than_starting_over(pair):
    my, _ = pair
    assert my_sql("truncate table t", "cx").returncode == 0
    _, token = _drain(my)
    assert my_sql("insert into t values (40,'a',1,x'00')",
                  "cx").returncode == 0
    first, token = _drain(my, token)
    assert len(first) == 1, first

    again, token2 = _drain(my, token)
    assert again == [], again          # nothing new since
    assert token2["log_file"] == token["log_file"]

    assert my_sql("insert into t values (41,'b',1,x'00')",
                  "cx").returncode == 0
    third, _ = _drain(my, token2)
    assert len(third) == 1 and third[0]["key"] == {"id": 41}, third


def test_a_keyless_table_stops_the_tail_and_names_it(pair):
    """A change to a table with no key cannot be addressed on the target.
    Applying it by matching every column would hit every duplicate of the
    row, so the tail refuses rather than guessing."""
    my, _ = pair
    _, token = _drain(my)
    assert my_sql("create table nokey (a int)", "cx").returncode == 0
    try:
        assert my_sql("insert into nokey values (1)", "cx").returncode == 0
        with pytest.raises(SystemExit) as e:
            _drain(my, token)
        assert "no primary key on nokey" in str(e.value)
    finally:
        my_sql("drop table nokey", "cx")


def test_an_engine_with_no_change_log_says_so(pair):
    """SQLite has none at all. Polling the table and calling the difference a
    change would miss a row that was inserted and deleted between two looks,
    and report that as nothing having happened."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.sqlite import SQLiteEngine
    ep = Endpoint(host="/tmp/does-not-matter.db", port=0, user="",
                  password="")
    eng = SQLiteEngine(Hop(name="s", engine="sqlite", source=ep, target=ep))
    with pytest.raises(NotImplementedError) as e:
        eng.neutral_changes("src", "main")
    assert "change log" in str(e.value)
    assert "not clean" in str(e.value)
