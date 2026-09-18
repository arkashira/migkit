"""The change tail, for a pair it was never written for.

`hetero.tail_apply` used to read a MySQL binlog and write PostgreSQL INSERT
statements by hand, which is one pair and no others. It now asks the source
engine for change records and hands them to the target's applier, so the
pairing is configuration - and the direction that used to be impossible is the
one tested here.
"""
import json
import socket
import subprocess
import threading
import time

import pytest

PG, MY = "migkit-test-tail-pg", "migkit-test-tail-my"
PG_PORT, MY_PORT = 15453, 13353


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


def _hop(src, dst):
    from migkit.config import Endpoint, Hop
    pg = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                  password="test")
    my = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    return Hop(name="tail", engine="hetero",
               source=pg if src == "postgres" else my,
               target=my if dst == "mysql" else pg,
               db_map={"cx": "cx"},
               options={"source_engine": src, "target_engine": dst})


@pytest.fixture(scope="module")
def engine():
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
    assert pg_sql("create database cx", "postgres").returncode == 0
    assert pg_sql("create table t (id bigint primary key, name varchar(50),"
                  " amount numeric(12,4), b bytea)").returncode == 0
    assert my_sql("create database cx").returncode == 0
    assert my_sql("create table t (id bigint primary key, name varchar(50),"
                  " amount decimal(12,4), b varbinary(20))",
                  "cx").returncode == 0

    from migkit.engines.hetero import HeteroEngine
    yield HeteroEngine(_hop("postgres", "mysql"))
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _tail_briefly(eng, token_path, go=True, seconds=6):
    """Run the tail in a thread and interrupt it the way a person would."""
    lines = []
    done = threading.Event()

    def run():
        try:
            eng.tail_apply("cx", go, token_path, lines.append)
        except BaseException as e:            # KeyboardInterrupt lands here
            lines.append(f"exit: {type(e).__name__}")
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(seconds)
    # the tail stops on KeyboardInterrupt; there is no other way in, so the
    # test raises it inside the thread rather than pretending
    import ctypes
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(thread.ident),
        ctypes.py_object(KeyboardInterrupt))
    done.wait(timeout=20)
    return lines


def test_the_pair_that_had_no_tail_now_has_one(engine, tmp_path):
    """postgres -> mysql. The old implementation raised for anything that
    was not mysql -> postgres."""
    assert engine.src_name == "postgres" and engine.dst_name == "mysql"
    assert pg_sql("delete from t").returncode == 0
    assert my_sql("truncate table t", "cx").returncode == 0
    # take a token first, the way a migration does before its full load
    engine.src_engine.neutral_changes("src", "cx")

    token_path = tmp_path / "tail-token.json"
    assert pg_sql("insert into t values (1,'ca''fé : [x]',1.5,'\\x00FF41'),"
                  " (2,'two',2.5,'\\x01')").returncode == 0
    assert pg_sql("update t set amount=9.75 where id=1").returncode == 0
    assert pg_sql("delete from t where id=2").returncode == 0

    lines = _tail_briefly(engine, token_path)
    assert any("tailing postgres -> mysql" in m for m in lines), lines
    assert my_sql("select count(*) from t", "cx").stdout.strip() == "1"
    got = my_sql("select id, name, amount, hex(b) from t",
                 "cx").stdout.strip()
    assert got == "1\tca'fé : [x]\t9.7500\t00FF41", got


def test_the_token_is_written_only_after_the_changes_are_applied(engine,
                                                                 tmp_path):
    """Saving it first would skip on a crash, and a skipped change is a row
    that silently never arrives. Saving it second replays, which the
    appliers are idempotent for."""
    token_path = tmp_path / "tail-token.json"
    assert not token_path.exists()
    assert pg_sql("insert into t values (5,'five',5.5,'\\x05')"
                  ).returncode == 0
    _tail_briefly(engine, token_path)
    assert token_path.exists()
    saved = json.loads(token_path.read_text())["token"]
    assert saved, saved
    assert my_sql("select name from t where id=5",
                  "cx").stdout.strip() == "five"


def test_a_resumed_tail_does_not_replay_from_the_beginning(engine, tmp_path):
    token_path = tmp_path / "tail-token.json"
    assert pg_sql("insert into t values (6,'six',6.5,'\\x06')"
                  ).returncode == 0
    _tail_briefly(engine, token_path)
    first = json.loads(token_path.read_text())["token"]

    lines = _tail_briefly(engine, token_path)
    assert any("resuming from" in m for m in lines), lines
    assert json.loads(token_path.read_text())["token"] == first
    assert my_sql("select count(*) from t where id=6",
                  "cx").stdout.strip() == "1"


def test_without_go_nothing_is_applied(engine, tmp_path):
    token_path = tmp_path / "dry-token.json"
    assert pg_sql("insert into t values (7,'seven',7.5,'\\x07')"
                  ).returncode == 0
    try:
        lines = _tail_briefly(engine, token_path, go=False)
        assert any("count-only" in m for m in lines), lines
        assert my_sql("select count(*) from t where id=7",
                      "cx").stdout.strip() == "0"
        assert not token_path.exists()
    finally:
        _tail_briefly(engine, token_path)


def test_a_source_with_no_change_log_says_so_rather_than_failing_oddly(
        tmp_path):
    """SQLite has none. The message points at the full load, which is the
    thing that does work."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    ep = Endpoint(host=str(tmp_path / "a.db"), port=0, user="", password="")
    hop = Hop(name="tail", engine="hetero", source=ep,
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              db_map={"main": "cx"},
              options={"source_engine": "sqlite",
                       "target_engine": "postgres"})
    eng = HeteroEngine(hop)
    with pytest.raises(SystemExit) as e:
        eng.tail_apply("main", True, tmp_path / "tok.json", print)
    assert "sqlite has no change log" in str(e.value)
    assert "migkit move" in str(e.value)
