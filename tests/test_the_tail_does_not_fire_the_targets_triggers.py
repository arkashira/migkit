"""Changes carried by the tail land as the source made them, not as the
target's triggers would rewrite them.

Measured before, applying changes and rows from another engine: on MySQL a
`BEFORE INSERT` trigger appending `!` and a `BEFORE UPDATE` one appending `?`
turned `b` into `b!?`, and an `AFTER INSERT` one wrote audit rows the source
never had; on PostgreSQL the same trigger turned `b` into `b!!` and wrote
four audit rows. The servers' own replication applies without firing them -
what the source's triggers wrote is in the log with the rest - and so does
the tail now: PostgreSQL's connections write as a replica, MySQL's triggers
are off while the tail runs and back when it stops.

A tail runs for days, and a process killed that long is not unusual. What it
took off is named by `check` until a later load puts it back, and a service
manager's SIGTERM stops it the way ctrl-c does.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time

import pytest

PG, MY = "migkit-test-trigtail-pg", "migkit-test-trigtail-my"
PG_PORT, MY_PORT = 15750, 15751


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


def pg(sql, db="cx"):
    got = subprocess.run(
        ["docker", "exec", "-i", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _wait(port):
    end = time.time() + 180
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"nothing answered on {port}")


@pytest.fixture(scope="module")
def servers():
    for n in (PG, MY):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                        "postgres:16", "-c", "wal_level=logical"],
                       check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                        "mysql:8.4", "--binlog-row-metadata=FULL"],
                       check=True, capture_output=True)
        _wait(PG_PORT)
        _wait(MY_PORT)
        for probe in (lambda: pg("select 1", "postgres"),
                      lambda: my("select 1")):
            for _ in range(90):
                try:
                    probe()
                    break
                except AssertionError:
                    time.sleep(2)
            else:
                pytest.fail("a server never answered")
        yield
    finally:
        for n in (PG, MY):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _hop(name, src, dst, tmp_path):
    from migkit.config import Endpoint, Hop
    ends = {"postgres": Endpoint(host="127.0.0.1", port=PG_PORT,
                                 user="postgres", password="test"),
            "mysql": Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test")}
    hop = Hop(name=name, engine="hetero", source=ends[src], target=ends[dst],
              databases=["cx"], db_map={"cx": "cx"},
              options={"source_engine": src, "target_engine": dst})
    hop.report_dir = lambda db=None: tmp_path
    return hop


MY_TARGET = """
drop database if exists cx; create database cx;
create table cx.t (id bigint primary key, name varchar(50));
create table cx.audit (n int auto_increment primary key, id bigint);
create trigger cx.t_bi before insert on cx.t for each row
  set new.name = concat(new.name, '!');
create trigger cx.t_bu before update on cx.t for each row
  set new.name = concat(new.name, '?');
create trigger cx.t_ai after insert on cx.t for each row
  insert into cx.audit (id) values (new.id);
"""

PG_TARGET = """
create table t (id bigint primary key, name varchar(50));
create table audit (n serial primary key, id bigint);
create function bump() returns trigger language plpgsql as $$
begin new.name := new.name || '!'; insert into audit (id) values (new.id);
return new; end $$;
create trigger t_b before insert or update on t for each row
  execute function bump();
"""

MY_TRIGGERS = ("select group_concat(trigger_name order by trigger_name)"
               " from information_schema.triggers where trigger_schema='cx'")


def _fresh_pg():
    # a tail's slot holds its database
    pg("select pg_drop_replication_slot(slot_name)"
       " from pg_replication_slots", "postgres")
    pg("drop database if exists cx", "postgres")


@pytest.fixture
def pg_to_my(servers, tmp_path):
    _fresh_pg()
    pg("create database cx", "postgres")
    pg("create table t (id bigint primary key, name varchar(50))")
    my(MY_TARGET)
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(_hop("tg1", "postgres", "mysql", tmp_path))


@pytest.fixture
def my_to_pg(servers, tmp_path):
    my("drop database if exists cx; create database cx;"
       " create table cx.t (id bigint primary key, name varchar(50))")
    _fresh_pg()
    pg("create database cx", "postgres")
    pg(PG_TARGET)
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(_hop("tg2", "mysql", "postgres", tmp_path))


def _tail(eng, tmp_path, change, seconds=6):
    """The tail in a thread; `change` runs on the source once it is
    applying, and the tail is stopped the way a person stops it."""
    import ctypes
    lines, done = [], threading.Event()

    def run():
        try:
            eng.tail_apply("cx", True, tmp_path / "tok.json", lines.append)
        except BaseException as e:
            lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(3)
    during = change()
    time.sleep(seconds)
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    assert done.wait(timeout=30), lines
    return lines, during


def test_mysql_target_rows_are_the_sources(pg_to_my, tmp_path):
    from migkit.engines.mysql import MySQLEngine

    def change():
        pg("insert into t values (1, 'a'), (2, 'b');"
           " update t set name = 'c' where id = 2")
        time.sleep(3)
        # while the tail runs they are off, and the check says why
        return MySQLEngine(pg_to_my.hop).set_aside("cx"), my(MY_TRIGGERS)

    lines, (held, off) = _tail(pg_to_my, tmp_path, change)
    assert my("select group_concat(concat(id, '=', name) order by id)"
              " from cx.t") == "1=a,2=c", lines
    assert my("select count(*) from cx.audit") == "0", lines
    assert off == "NULL", off
    assert held.status == "ok" and "go back when it stops" in held.detail, \
        held.detail
    # and back once it stopped, with nothing left on record
    assert my(MY_TRIGGERS) == "t_ai,t_bi,t_bu", lines
    assert not list(tmp_path.glob("dropped-triggers.*")), lines
    assert not [r for r in pg_to_my.check_deep("cx")
                if r.scope.endswith("triggers set aside")]


def test_postgres_target_rows_are_the_sources(my_to_pg, tmp_path):
    def change():
        my("insert into cx.t values (1, 'a'), (2, 'b');"
           " update cx.t set name = 'c' where id = 2")

    lines, _ = _tail(my_to_pg, tmp_path, change)
    assert pg("select string_agg(id || '=' || name, ',' order by id)"
              " from t") == "1=a,2=c", lines
    assert pg("select count(*) from audit") == "0", lines
    # nothing about the table itself was changed to get there
    assert pg("select tgenabled from pg_trigger where tgname = 't_b'") == "O"


def test_a_user_who_cannot_write_as_a_replica_is_stopped_first(my_to_pg,
                                                               tmp_path):
    pg("create role loader login password 'pw';"
       " grant all on all tables in schema public to loader;"
       " grant all on all sequences in schema public to loader")
    try:
        my_to_pg.hop.target.user, my_to_pg.hop.target.password = \
            "loader", "pw"
        from migkit.engines.hetero import HeteroEngine
        eng = HeteroEngine(my_to_pg.hop)

        def change():
            my("insert into cx.t values (1, 'a')")
        lines, _ = _tail(eng, tmp_path, change, seconds=3)
        said = " ".join(lines)
        assert "public.t have triggers on the target" in said, lines
        assert "Nothing has been written" in said, lines
        assert pg("select count(*) from t") == "0"
    finally:
        pg("drop owned by loader; drop role loader")


PROGRAM = """
import sys, pathlib
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine
tmp = pathlib.Path(sys.argv[1])
hop = Hop(name="tg1", engine="hetero",
          source=Endpoint(host="127.0.0.1", port={pg}, user="postgres",
                          password="test"),
          target=Endpoint(host="127.0.0.1", port={my}, user="root",
                          password="test"),
          databases=["cx"], db_map={{"cx": "cx"}},
          options={{"source_engine": "postgres", "target_engine": "mysql"}})
hop.report_dir = lambda db=None: tmp
HeteroEngine(hop).tail_apply("cx", True, tmp / "tok.json",
                             lambda m: print(m, flush=True))
print("returned", flush=True)
""".format(pg=PG_PORT, my=MY_PORT)


def _tail_process(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", PROGRAM, str(tmp_path)],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, cwd=os.getcwd())
    end = time.time() + 60
    while time.time() < end and not list(
            tmp_path.glob("dropped-triggers.*.json")):
        assert proc.poll() is None, proc.stdout.read()
        time.sleep(0.5)
    assert my(MY_TRIGGERS) == "NULL"
    return proc


def test_a_service_stop_puts_them_back(pg_to_my, tmp_path):
    proc = _tail_process(tmp_path)
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == 0 and "returned" in out, out
    assert my(MY_TRIGGERS) == "t_ai,t_bi,t_bu", out
    assert not list(tmp_path.glob("dropped-triggers.*")), out


def test_what_a_killed_tail_held_is_named_and_put_back(pg_to_my, tmp_path):
    proc = _tail_process(tmp_path)
    proc.kill()
    proc.communicate(timeout=30)
    assert my(MY_TRIGGERS) == "NULL"
    from migkit.engines.mysql import MySQLEngine
    got = [r for r in pg_to_my.check_deep("cx")
           if r.scope.endswith("triggers set aside")]
    assert [r.status for r in got] == ["diff"], [r.__dict__ for r in got]
    assert "3 triggers a load took off the target and never put back:" \
        " t_ai, t_bi, t_bu" in got[0].detail, got[0].detail
    # the next load into the database puts them back, whatever it writes
    from migkit import movers
    said = []
    with movers._MyTriggerWindow(pg_to_my.hop, "cx", said.append,
                                 {"audit"}):
        assert my(MY_TRIGGERS) == "NULL"
    assert my(MY_TRIGGERS) == "t_ai,t_bi,t_bu", said
    assert "3 triggers an earlier load took off" in " ".join(said), said
    assert not list(tmp_path.glob("dropped-triggers.*")), said
    assert MySQLEngine(pg_to_my.hop).set_aside("cx") is None


def test_a_repair_between_engines_lands_the_sources_rows(pg_to_my,
                                                         tmp_path):
    pg("insert into t values (1, 'a'), (2, 'b')")
    my("insert into cx.t values (1, 'z'); delete from cx.audit")
    pg_to_my.check_data("cx")
    for action in pg_to_my.repair_plan("cx", "rows"):
        pg_to_my.apply("cx", action)
    assert my("select group_concat(concat(id, '=', name) order by id)"
              " from cx.t") == "1=a,2=b"
    assert my("select count(*) from cx.audit") == "0"
    assert my(MY_TRIGGERS) == "t_ai,t_bi,t_bu"
    assert json.dumps(sorted(p.name for p in tmp_path.glob(
        "dropped-triggers.*"))) == "[]"
