"""A row repair does not race the stream that writes to the same target.

The rule "no data repair while CDC runs" lived in the runbook. In the code a
repair wrote rows straight onto a target that a tail or a subscription was
also writing to, and whichever write landed last survived.

* migkit's own tail is paused for the repair - DMS's shape: pause, repair,
  resume. It acknowledges only once what it was holding is applied and its
  position saved, and afterwards replays from there on top of the repair.
* replication migkit does not drive (a subscription on the target) is
  named, and the repair refuses before writing anything.
"""
import ctypes
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

MG, MG_PORT = "migkit-test-rstream-mg", 15698


def mongosh(script, db="cx"):
    return subprocess.run(["docker", "exec", MG, "mongosh", "--quiet", db,
                           "--eval", script], capture_output=True, text=True)


@pytest.fixture(scope="module")
def mongo():
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MG, "-p",
                    f"{MG_PORT}:27017", "mongo:7", "--replSet", "rs0",
                    "--bind_ip_all"], check=True, capture_output=True)
    try:
        for _ in range(60):
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", MG_PORT)) == 0:
                    break
            time.sleep(1)
        for _ in range(40):
            if mongosh("db.runCommand({ping:1}).ok", "admin").returncode == 0:
                break
            time.sleep(2)
        mongosh('rs.initiate({_id:"rs0",members:'
                '[{_id:0,host:"127.0.0.1:27017"}]})', "admin")
        for _ in range(30):
            if "PRIMARY" in mongosh(
                    "rs.status().myState === 1 ? 'PRIMARY' : 'no'",
                    "admin").stdout:
                break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)


def _mongo_engine(tmp_path):
    from migkit.engines.mongodb import MongoEngine
    ep = Endpoint(host="127.0.0.1", port=MG_PORT, user="", password="",
                  options={"uri_options": "directConnection=true"})
    hop = Hop(name="rs", engine="mongodb", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"},
              options={"repair_window": 30, "fence_timeout": 10})
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


def _tail_in_thread(eng, path, said):
    done = threading.Event()

    def run():
        try:
            eng.tail_apply("cx", True, path, said.append)
        except BaseException:
            pass
        finally:
            done.set()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    def stop():
        if not done.is_set():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            done.wait(20)
    return stop


@pytest.mark.usefixtures("mongo")
def test_the_tail_is_paused_for_the_repair_and_carries_on(tmp_path,
                                                          monkeypatch):
    from migkit import cli, tailctl
    eng = _mongo_engine(tmp_path)
    mongosh("db.getSiblingDB('cx').dropDatabase();"
            " db.getSiblingDB('cy').dropDatabase();"
            " db.getSiblingDB('cx').t.insertMany([{_id: 1, v: 'a'},"
            " {_id: 2, v: 'b'}]);"
            " db.getSiblingDB('cy').t.insertMany([{_id: 1, v: 'a'},"
            " {_id: 2, v: 'b'}]);")
    path = tmp_path / "tail-token.json"
    tail_said = []
    stop = _tail_in_thread(eng, path, tail_said)
    try:
        time.sleep(4)
        assert tailctl.alive(tmp_path)
        # wrong on the target alone: a real difference, not one in flight
        mongosh("db.t.updateOne({_id: 2}, {$set: {v: 'wrong'}})", "cy")
        got = [r for r in eng.check_data("cx") if r.check == "data"]
        assert got[0].status == "diff", got[0].detail
        shown = []
        monkeypatch.setattr(cli.console, "print",
                            lambda *a, **k: shown.append(" ".join(map(str, a))))
        cli._repair_one(eng.hop, eng, "cx", "rows", True)
        text = " | ".join(shown)
        assert "asking it to pause" in text, text
        assert "paused for a repair" in " | ".join(tail_said), tail_said
        assert mongosh("db.t.findOne({_id: 2}).v", "cy").stdout.strip() \
            == "b"
        assert not (tmp_path / tailctl.PAUSE).exists()
        # and it is carrying changes again
        mongosh("db.t.insertOne({_id: 3, v: 'after'})")
        for _ in range(20):
            if mongosh("(db.t.findOne({_id: 3}) || {}).v", "cy"
                       ).stdout.strip() == "after":
                break
            time.sleep(1)
        else:
            pytest.fail("the tail did not resume")
    finally:
        stop()


def test_replication_it_does_not_drive_stops_the_repair(pg_pair, tmp_path,
                                                         monkeypatch):
    from migkit import cli
    from migkit.engines.base import RepairAction
    from migkit.engines.postgres import PostgresEngine
    src_ip = subprocess.run(
        ["docker", "inspect", "-f",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "migkit-test-pg-src"], capture_output=True, text=True).stdout.strip()
    psql(pg_pair["src"], "create table public.t (id int primary key, v int);"
                         " create publication rs_pub for table public.t")
    psql(pg_pair["dst"], "create table public.t (id int primary key, v int)")
    got = psql(pg_pair["dst"],
               "create subscription rs_sub connection"
               f" 'host={src_ip} port=5432 dbname=postgres user=postgres"
               " password=test' publication rs_pub with (copy_data = false)")
    assert got.returncode == 0, got.stderr
    try:
        hop = Hop(name="rsp", engine="postgres",
                  source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                                  user="postgres", password="test"),
                  target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                                  user="postgres", password="test"),
                  databases=["postgres"])
        hop.report_dir = lambda db=None: tmp_path
        eng = PostgresEngine(hop)
        assert ("subscription rs_sub", False) in eng.stream_writers(
            "postgres")
        applied = []
        monkeypatch.setattr(eng, "repair_plan", lambda db, kind: [
            RepairAction("postgres.public.t", "rows", ["copy 1 rows"], [],
                         "")])
        monkeypatch.setattr(eng, "apply", lambda db, a: applied.append(a))
        with pytest.raises(SystemExit) as e:
            cli._repair_one(hop, eng, "postgres", "rows", True)
        assert "subscription rs_sub is writing to this target" in \
            str(e.value), e.value
        assert applied == []
    finally:
        psql(pg_pair["dst"], "drop subscription if exists rs_sub")
        psql(pg_pair["src"], "drop publication if exists rs_pub")


def test_migkits_own_subscription_is_paused_and_resumed(pg_pair, tmp_path,
                                                        monkeypatch):
    """The subscription `migkit move --mode cdc` set up for this hop: let
    catch up, disabled for the repair, enabled again - and what the source
    wrote meanwhile arrives on top of it."""
    from migkit import cli
    from migkit.engines.postgres import PostgresEngine
    src_ip = subprocess.run(
        ["docker", "inspect", "-f",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "migkit-test-pg-src"], capture_output=True, text=True).stdout.strip()
    hop = Hop(name="rsown", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], options={"repair_window": 60})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    name = eng._repl_name()
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.t (id int primary key, v int)")
    psql(pg_pair["src"], "insert into public.t select g, g from"
                         f" generate_series(1, 5) g; create publication {name}"
                         " for table public.t")
    got = psql(pg_pair["dst"],
               f"create subscription {name} connection"
               f" 'host={src_ip} port=5432 dbname=postgres user=postgres"
               f" password=test' publication {name}")
    assert got.returncode == 0, got.stderr
    try:
        for _ in range(30):
            if psql(pg_pair["dst"], "select count(*) from public.t"
                    ).stdout.strip() == "5":
                break
            time.sleep(1)
        assert (f"subscription {name}", True) in eng.stream_writers(
            "postgres")
        # a row lost on the target alone
        psql(pg_pair["dst"], "delete from public.t where id = 3")
        eng.check_data("postgres")
        real_apply = eng.apply

        def apply_while_the_source_writes(db, action):
            # the subscription is off while this runs
            assert psql(pg_pair["dst"], "select subenabled from"
                        f" pg_subscription where subname = '{name}'"
                        ).stdout.strip() == "f"
            psql(pg_pair["src"], "insert into public.t values (6, 6);"
                                 " update public.t set v = 30 where id = 3")
            return real_apply(db, action)

        monkeypatch.setattr(eng, "apply", apply_while_the_source_writes)
        cli._repair_one(hop, eng, "postgres", "rows", True)
        assert psql(pg_pair["dst"], "select subenabled from pg_subscription"
                    f" where subname = '{name}'").stdout.strip() == "t"
        want = "1:1,2:2,3:30,4:4,5:5,6:6"
        for _ in range(30):
            got = psql(pg_pair["dst"], "select string_agg(id||':'||v, ','"
                       " order by id) from public.t").stdout.strip()
            if got == want:
                break
            time.sleep(1)
        assert got == want, got
    finally:
        psql(pg_pair["dst"], f"drop subscription if exists {name}")
        psql(pg_pair["src"], f"drop publication if exists {name}")
