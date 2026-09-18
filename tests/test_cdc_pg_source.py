"""PostgreSQL as the change source, MySQL as the target.

The direction the other CDC test does not cover, and the one where the safety
question is sharpest: a logical slot can be read two ways, and only one of them
survives a crash between reading and applying.

`pg_logical_slot_get_changes` advances the slot as it reads. Read, then die
before applying, and the changes are gone from the server with nothing left to
replay. `peek` leaves them where they are. So migkit peeks, and the slot is
advanced only when the caller hands back the token from last time - which is
how it says "everything up to here is applied".
"""
import socket
import subprocess
import time

import pytest

PG, MY = "migkit-test-pgcdc-pg", "migkit-test-pgcdc-my"
PG_PORT, MY_PORT = 15457, 13357


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


def pg_sql(sql, db="cx", container=PG):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", container, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


def my_sql(sql, db=None):
    cmd = ["docker", "exec", MY, "mysql", "-uroot", "-ptest",
           "--default-character-set=utf8mb4", "-N", "-B"]
    if db:
        cmd += ["-D", db]
    return subprocess.run(cmd + ["-e", sql], capture_output=True, text=True)


def _hop(name, engine, port, user, password):
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="127.0.0.1", port=port, user=user, password=password)
    return Hop(name=name, engine=engine, source=ep, target=ep,
               db_map={"cx": "cx"})


@pytest.fixture(scope="module")
def pair():
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16", "-c", "wal_level=logical"],
                   check=True, capture_output=True)
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
    assert pg_sql("show wal_level", "postgres").stdout.strip() == "logical"
    assert pg_sql("create database cx", "postgres").returncode == 0
    assert pg_sql(
        "create table t (id bigint primary key, name varchar(50),"
        " amount numeric(12,4), b bytea, flag boolean)").returncode == 0
    assert my_sql("create database cx").returncode == 0
    assert my_sql("create table t (id bigint primary key, name varchar(50),"
                  " amount decimal(12,4), b varbinary(20),"
                  " flag tinyint(1))", "cx").returncode == 0

    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    yield (PostgresEngine(_hop("pgcdc", "postgres", PG_PORT,
                               "postgres", "test")),
           MySQLEngine(_hop("pgcdc", "mysql", MY_PORT, "root", "test")))
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def test_the_slot_is_created_before_anything_is_read(pair):
    """A slot that does not exist yet holds no changes. A tail that made one
    on its first call would silently start from "now" and skip everything
    written between the full load and that moment."""
    pg, _ = pair
    name = pg.slot_name()
    assert name == "migkit_pgcdc", name
    assert pg_sql("select count(*) from pg_replication_slots"
                  f" where slot_name='{name}'").stdout.strip() == "0"
    assert pg._slot_ready("src", "cx") == name
    got = pg_sql("select plugin, slot_type from pg_replication_slots"
                 f" where slot_name='{name}'").stdout.strip()
    assert got == "test_decoding|logical", got


def test_peeking_twice_returns_the_same_changes(pair):
    """The property the whole design rests on. If reading consumed, a crash
    between reading and applying would lose the changes with nothing left to
    replay."""
    pg, _ = pair
    pg.neutral_changes("src", "cx")          # drain whatever is pending
    first_token = pg.neutral_changes("src", "cx")[1]
    assert pg_sql("insert into t values (1,'one',1.5,'\\x00FF41',true)"
                  ).returncode == 0
    a, token_a = pg.neutral_changes("src", "cx", first_token)
    b, token_b = pg.neutral_changes("src", "cx")   # no token: nothing moves
    assert len(a) == 1, a
    assert [c["key"] for c in a] == [c["key"] for c in b], (a, b)
    assert token_a == token_b


def test_handing_the_token_back_is_what_advances_the_slot(pair):
    """Only evidence of a successful apply moves the slot, so the act of
    looking never throws anything away."""
    pg, _ = pair
    _, token = pg.neutral_changes("src", "cx")
    assert pg_sql("insert into t values (2,'two',2.5,'\\x01',false)"
                  ).returncode == 0
    changes, token = pg.neutral_changes("src", "cx", token)
    assert len(changes) == 1, changes
    again, token = pg.neutral_changes("src", "cx", token)
    assert again == [], again


def test_the_changes_apply_onto_mysql_and_the_digest_agrees(pair):
    """The payoff, and checked by comparing rather than by the absence of an
    error."""
    from migkit import canon
    pg, my = pair
    assert pg_sql("delete from t").returncode == 0
    assert my_sql("truncate table t", "cx").returncode == 0
    _, token = pg.neutral_changes("src", "cx")

    assert pg_sql(
        "insert into t values (10,'ca''fé : [x]',10.25,'\\xAB',true),"
        " (11,'eleven',11.5,'\\xCD',false)").returncode == 0
    assert pg_sql("update t set amount=99.75 where id=10").returncode == 0
    assert pg_sql("delete from t where id=11").returncode == 0
    changes, token = pg.neutral_changes("src", "cx", token)
    assert [c["op"] for c in changes] == ["insert", "insert", "update",
                                          "delete"], changes
    assert my.neutral_apply("dst", "cx", changes) == len(changes)

    assert my_sql("select count(*) from t", "cx").stdout.strip() == "1"
    got = my_sql("select id, name, amount, hex(b), flag from t",
                 "cx").stdout.strip()
    assert got == "10\tca'fé : [x]\t99.7500\tAB\t1", got

    src_cols = sorted((n, canon.comparable("postgres", t)[0])
                      for n, t in pg.neutral_columns("src", "cx", "public.t"))
    dst_cols = sorted((n, canon.comparable("mysql", t)[0])
                      for n, t in my.neutral_columns("dst", "cx", "t"))
    assert pg.neutral_digest("src", "cx", "public.t", src_cols) \
        == my.neutral_digest("dst", "cx", "t", dst_cols)


def test_an_update_that_moves_the_key_moves_the_row(pair):
    pg, my = pair
    assert pg_sql("delete from t").returncode == 0
    assert my_sql("truncate table t", "cx").returncode == 0
    _, token = pg.neutral_changes("src", "cx")
    assert pg_sql("insert into t values (20,'before',1,null,true)"
                  ).returncode == 0
    changes, token = pg.neutral_changes("src", "cx", token)
    my.neutral_apply("dst", "cx", changes)

    assert pg_sql("update t set id=21, name='after' where id=20"
                  ).returncode == 0
    changes, token = pg.neutral_changes("src", "cx", token)
    assert changes[0]["key"] == {"id": 20}, changes[0]
    assert changes[0]["values"]["id"] == 21, changes[0]
    my.neutral_apply("dst", "cx", changes)
    assert my_sql("select id, name from t order by id",
                  "cx").stdout.strip() == "21\tafter"


def test_a_keyless_table_stops_the_tail_and_names_it(pair):
    pg, _ = pair
    _, token = pg.neutral_changes("src", "cx")
    assert pg_sql("create table nokey (a int)").returncode == 0
    try:
        assert pg_sql("insert into nokey values (1)").returncode == 0
        with pytest.raises(SystemExit) as e:
            pg.neutral_changes("src", "cx", token)
        assert "no primary key on public.nokey" in str(e.value)
        assert "still in the slot" in str(e.value)
        assert "pg_replication_slot_advance" in str(e.value)
        # and it stays there: peeking never consumes, so the next call
        # stops in the same place until someone acts on it
        with pytest.raises(SystemExit):
            pg.neutral_changes("src", "cx")
    finally:
        pg_sql("drop table nokey")
        # the way out the message names, which is the only way past a
        # change that cannot be applied
        assert pg_sql("select pg_replication_slot_advance"
                      f"('{pg.slot_name()}', pg_current_wal_lsn())"
                      ).returncode == 0
    assert pg.neutral_changes("src", "cx")[0] == []


def test_a_slot_made_with_another_plugin_is_refused(pair):
    """Reading one plugin's output as another's does not fail, it
    mis-parses - which is the kind of wrong that reaches the target."""
    pg, _ = pair
    name = pg.slot_name()
    assert pg_sql(f"select pg_drop_replication_slot('{name}')").returncode \
        == 0
    assert pg_sql("select pg_create_logical_replication_slot"
                  f"('{name}', 'pgoutput')").returncode == 0
    try:
        with pytest.raises(SystemExit) as e:
            pg.neutral_changes("src", "cx")
        assert "made with the pgoutput plugin" in str(e.value)
        assert "mis-parses" in str(e.value)
    finally:
        pg_sql(f"select pg_drop_replication_slot('{name}')")


def test_a_server_without_logical_wal_is_refused_with_the_setting(pair):
    """Nothing client-side can make the WAL carry row images it was never
    told to carry, so the message is the setting rather than a workaround."""
    other = "migkit-test-pgcdc-min"
    subprocess.run(["docker", "rm", "-f", other], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", other, "-e",
                    "POSTGRES_PASSWORD=test", "-p", "15456:5432",
                    "postgres:16"], check=True, capture_output=True)
    try:
        assert _wait(15456)
        for _ in range(40):
            if pg_sql("select 1", "postgres", other).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("the second postgres never answered")
        assert pg_sql("show wal_level", "postgres",
                      other).stdout.strip() == "replica"
        assert pg_sql("create database cx", "postgres", other).returncode == 0

        from migkit.engines.postgres import PostgresEngine
        eng = PostgresEngine(_hop("pgcdc", "postgres", 15456,
                                  "postgres", "test"))
        with pytest.raises(SystemExit) as e:
            eng.neutral_changes("src", "cx")
        assert "wal_level is replica" in str(e.value)
        assert "alter system set wal_level" in str(e.value)
    finally:
        subprocess.run(["docker", "rm", "-f", other], capture_output=True)
