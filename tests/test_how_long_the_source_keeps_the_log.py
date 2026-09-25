"""How much longer the source keeps what a change tail has not read.

A tail that falls behind for long enough loses its place: the source drops
the log it was reading (a purged binlog, an oplog that wrapped, a slot that
lost its WAL) and the tail stops. Now each engine is asked how much room is
left (`stream_room`), the tail asks once a minute, and `/metrics` carries it
for an alert to fire before the place is gone.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _up(name, port, image, *args, env=()):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, *sum(
        (["-e", e] for e in env), []), "-p", f"{port}:{args[0]}", image,
        *args[1:]], check=True, capture_output=True)


def _down(name):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def test_postgres_says_what_the_slot_can_still_take(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=pg_pair["src"], user="postgres",
                  password="test")
    eng = PostgresEngine(Hop(name="room", engine="postgres", source=ep,
                             target=ep, databases=["postgres"]))
    token = eng.change_point("src", "postgres")
    try:
        # nothing caps it: the slot keeps everything, and says how much
        got = eng.stream_room("src", "postgres", token)
        assert set(got) == {"held_bytes"}, got
        psql(pg_pair["src"], "alter system set max_slot_wal_keep_size ="
                             " '32MB'")
        psql(pg_pair["src"], "select pg_reload_conf()")
        time.sleep(1)
        got = eng.stream_room("src", "postgres", token)
        assert 0 < got["bytes"] <= 48 * 2 ** 20, got
        # written past it, then a checkpoint: the slot lost its WAL
        psql(pg_pair["src"], "create table if not exists room_fill"
                             " (id int, pad text)")
        psql(pg_pair["src"], "insert into room_fill select g,"
                             " repeat('x', 200) from"
                             " generate_series(1, 400000) g")
        psql(pg_pair["src"], "checkpoint")
        status = psql(pg_pair["src"], "select wal_status from"
                                      " pg_replication_slots where slot_name"
                                      f" = '{eng.slot_name()}'"
                      ).stdout.strip()
        assert status in ("lost", "unreserved"), status
        got = eng.stream_room("src", "postgres", token)
        assert got["bytes"] == 0, (status, got)
    finally:
        psql(pg_pair["src"], "alter system reset max_slot_wal_keep_size")
        psql(pg_pair["src"], "select pg_reload_conf()")
        psql(pg_pair["src"], "select pg_drop_replication_slot"
                             f"('{eng.slot_name()}')")
        psql(pg_pair["src"], "drop table if exists room_fill")


MY, MY_PORT = "migkit-test-room-my", 15783


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def test_mysql_says_when_the_tails_binlog_may_be_purged():
    from migkit.engines.mysql import MySQLEngine
    _up(MY, MY_PORT, "mysql:8.4", "3306", env=["MYSQL_ROOT_PASSWORD=test"])
    try:
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                      password="test")
        eng = MySQLEngine(Hop(name="room", engine="mysql", source=ep,
                              target=ep, databases=["cx"]))
        _my("set global binlog_expire_logs_seconds = 3600")
        token = eng.change_point("src", "cx")
        # the file still being written: the whole expiry is left
        assert eng.stream_room("src", "cx", token) == {"seconds": 3600}
        time.sleep(3)
        _my("flush binary logs")
        closed = time.time()
        got = eng.stream_room("src", "cx", token)["seconds"]
        # from when the file stopped being written, not from now
        assert 3600 - (time.time() - closed) - 3 <= got <= 3600, got
        # nothing purges
        _my("set global binlog_expire_logs_auto_purge = off")
        assert eng.stream_room("src", "cx", token) is None
        _my("set global binlog_expire_logs_auto_purge = on")
        # the file is gone
        _my("flush binary logs")
        newest = _my("show binary logs").splitlines()[-1].split()[0]
        _my(f"purge binary logs to '{newest}'")
        assert eng.stream_room("src", "cx", token) == {"seconds": 0}
    finally:
        _down(MY)


MG, MG_PORT = "migkit-test-room-mg", 15784


def test_mongodb_says_how_much_oplog_is_older_than_the_resume_point():
    from migkit.engines.mongodb import MongoEngine
    _up(MG, MG_PORT, "mongo:7", "27017", "--replSet", "rs0",
        "--bind_ip_all")
    try:
        for _ in range(60):
            got = subprocess.run(
                ["docker", "exec", MG, "mongosh", "--quiet", "--eval",
                 'rs.initiate({_id:"rs0",members:[{_id:0,'
                 'host:"127.0.0.1:27017"}]}).ok'],
                capture_output=True, text=True)
            if got.returncode == 0:
                break
            time.sleep(2)
        # the member's own name is 127.0.0.1:27017, inside the container
        ep = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="",
                      options={"uri_options": "directConnection=true"})
        eng = MongoEngine(Hop(name="room", engine="mongodb", source=ep,
                              target=ep, databases=["app"]))
        for _ in range(30):
            try:
                first = eng.change_point("src", "app")
                break
            except Exception:
                time.sleep(2)
        took = eng.stream_room("src", "app", first)["seconds"]
        time.sleep(3)
        subprocess.run(["docker", "exec", MG, "mongosh", "--quiet", "app",
                        "--eval", "db.t.insertOne({x: 1})"],
                       capture_output=True)
        later = eng.change_point("src", "app")
        grew = eng.stream_room("src", "app", later)["seconds"]
        assert took >= 0 and grew - took >= 3, (took, grew)
        # a token of another kind is no answer, not zero
        assert eng.stream_room("src", "app", "00ff") is None
        assert eng.stream_room("src", "app", None) is None
    finally:
        _down(MG)


def test_the_room_reaches_the_metrics(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import tailctl, ui
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    hop = Hop(name="h", engine="mysql",
              source=Endpoint(host="x", port=0, user="", password=""),
              target=Endpoint(host="x", port=0, user="", password=""))
    where = tmp_path / "h" / "cx"
    where.mkdir(parents=True)
    with tailctl.Running(where):
        tailctl.beat(where, time.time(), 3, {"seconds": 1200})
        text = ui.prometheus({"h": hop})
    assert ('migkit_tail_retention_margin_seconds{hop="h",engine="mysql",'
            'db="cx"} 1200') in text, text
