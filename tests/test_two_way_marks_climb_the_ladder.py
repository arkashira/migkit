"""Each side of a two-way pair marks migkit's own writes on the highest
rung it allows and proves, and keeps it (backlog R3, the rungs).

PostgreSQL 16 and MySQL 8.4 with GTIDs on and each statement's text
logged with its rows - so every rung there is allowed: a replication
origin, a logical message and the table on PostgreSQL; a tagged GTID, a
comment on each statement and the table on MySQL. For each: the reader
of that side leaves out what the rung marked and carries what the
application wrote between two such transactions; a batch the target
committed before the tail saved its position is not applied again on
any rung that says which batch it last committed; counters add on both
sides with each side on its highest rung; the tail stops where no rung
proves itself; and the teardown takes away what the rung left.
"""
import json
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import _wait_tcp, needs_docker

pytestmark = needs_docker

MY, MY_PORT = "migkit-test-rung-my", 15962
PG, PG_PORT = "migkit-test-rung-pg", 15965
ORIGINS = ("select pg_replication_origin_drop(roname) from"
           " pg_replication_origin where roname like 'migkit\\_twoway\\_%'")


def psql(port, sql, db="postgres"):
    return subprocess.run(
        ["docker", "exec", "-e", "PGPASSWORD=test", PG, "psql", "-U",
         "postgres", "-d", db, "-At", "-c", sql],
        capture_output=True, text=True)


@pytest.fixture(scope="module")
def pg():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PG_PORT}:5432",
                    "postgres:16", "-c", "wal_level=logical"], check=True,
                   capture_output=True)
    try:
        assert _wait_tcp("127.0.0.1", PG_PORT)
        for _ in range(60):
            if psql(PG_PORT, "select 1").returncode == 0:
                break
            time.sleep(1)
        yield {"dst": PG_PORT}
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


def my(sql, db="app"):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B", db], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8.4", "--binlog-row-metadata=FULL",
                    "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                    "--binlog-rows-query-log-events=ON"], check=True,
                   capture_output=True)
    try:
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture
def sides(mysql, pg):
    my("drop database if exists app; create database app", db="")
    my("create table acct (id int primary key, balance int, note"
       " varchar(20))")
    port = pg["dst"]
    psql(port, "select pg_drop_replication_slot(slot_name) from"
               " pg_replication_slots where slot_name like 'migkit_%'")
    psql(port, ORIGINS)
    psql(port, "drop database if exists app")
    assert psql(port, "create database app").returncode == 0
    assert psql(port, "create table public.acct (id int primary key,"
                      " balance int, note varchar(20)); alter table"
                      " public.acct replica identity full",
                db="app").returncode == 0
    for i in range(1, 4):
        my(f"insert into acct values ({i}, 100, 'n{i}')")
        psql(port, f"insert into public.acct values ({i}, 100, 'n{i}')",
             db="app")
    yield port
    psql(port, "select pg_drop_replication_slot(slot_name) from"
               " pg_replication_slots where slot_name like 'migkit_%'")
    psql(port, ORIGINS)


def _eps(pg_port, pg_user="postgres", pg_password="test"):
    return (Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                     password="test"),
            Endpoint(host="127.0.0.1", port=pg_port, user=pg_user,
                     password=pg_password))


def _hops(tmp_path, pg_port, pg_user="postgres", pg_password="test",
          **two_way):
    from migkit.engines.hetero import HeteroEngine
    mysql_ep, pg_ep = _eps(pg_port, pg_user, pg_password)
    out = []
    for i, (name, src, dst, se, de) in enumerate((
            ("m2p", mysql_ep, pg_ep, "mysql", "postgres"),
            ("p2m", pg_ep, mysql_ep, "postgres", "mysql"))):
        hop = Hop(name=name, engine="hetero", source=src, target=dst,
                  databases=["app"],
                  options={"source_engine": se, "target_engine": de,
                           "two_way": dict(two_way), "server_id": 4600 + i})
        (tmp_path / name).mkdir(exist_ok=True)
        hop.report_dir = lambda db=None, n=name: tmp_path / n
        out.append(HeteroEngine(hop))
    return out


def _run(eng, tmp_path):
    said, ended = [], {}

    def run():
        try:
            eng.tail_apply("app", True,
                           tmp_path / eng.hop.name / "tail-token.json",
                           said.append)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, said, ended


def _stop(*threads):
    import ctypes
    for thread in threads:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
    for thread in threads:
        thread.join(timeout=30)


def _rows(pg_port):
    a = my("select id, balance, note from acct order by id")
    b = psql(pg_port, "select id || chr(9) || balance || chr(9) || note"
                      " from public.acct order by id",
             db="app").stdout.strip()
    return a, b


def _wait(pg_port, want, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        got = _rows(pg_port)
        if got == (want, want):
            return got
        time.sleep(1)
    return _rows(pg_port)


def _rung(tmp_path, name):
    return json.loads((tmp_path / name / "tail-token.json").read_text()
                      ).get("rung")


def _pg_tables(pg_port):
    return psql(pg_port, "select count(*) from pg_tables where tablename ="
                         " 'migkit_origin'", db="app").stdout.strip()


def test_counters_add_on_both_sides_each_on_its_highest_rung(sides, tmp_path):
    from migkit import marks
    pg_port = sides
    m2p, p2m = _hops(tmp_path, pg_port, on_conflict="error",
                     delta=["balance"])
    for eng in (m2p, p2m):
        eng.tail_start("app", tmp_path / eng.hop.name / "tail-token.json")
    my("update acct set balance = balance + 10 where id = 1")
    psql(pg_port, "update public.acct set balance = balance + 5 where"
                  " id = 1", db="app")
    t1, said1, end1 = _run(m2p, tmp_path)
    t2, said2, end2 = _run(p2m, tmp_path)
    try:
        want = "1\t115\tn1\n2\t100\tn2\n3\t100\tn3"
        got = _wait(pg_port, want)
    finally:
        _stop(t1, t2)
    assert got == (want, want), (got, said1[-3:], said2[-3:], end1, end2)
    # each side on the top of what it allows for a hop that counts
    for eng, name in ((m2p, "m2p"), (p2m, "p2m")):
        dst = eng.dst_engine
        top = marks.choose_rung(dst, dict(dst.mark_facts("dst", "app"),
                                          exact=True))[0][0]
        assert _rung(tmp_path, name) == top.name, (name, said1, said2)
        assert any("came back from its log as migkit's" in m
                   for m in (said1 if name == "m2p" else said2))
        # doctor names it and what it leaves
        line = marks.said(dst, tmp_path / name / "tail-token.json", "app")
        assert top.words in line, line
    # neither of the exact rungs above the table leaves a table
    if _rung(tmp_path, "m2p") == "origin":
        assert _pg_tables(pg_port) == "0"
    if _rung(tmp_path, "p2m") == "gtid_tag":
        assert my("select count(*) from information_schema.tables where"
                  " table_name = 'migkit_origin'") == "0"


@pytest.mark.parametrize("name,rung", [("m2p", "origin"), ("m2p", "table"),
                                       ("p2m", "gtid_tag"), ("p2m", "table")])
def test_a_batch_committed_before_its_position_was_saved_is_added_once(
        sides, tmp_path, monkeypatch, name, rung):
    pg_port = sides
    m2p, p2m = _hops(tmp_path, pg_port, on_conflict="error",
                     delta=["balance"])
    eng = m2p if name == "m2p" else p2m
    token = tmp_path / name / "tail-token.json"
    eng.tail_start("app", token)
    # the tail stands on this rung: its token keeps it
    token.write_text(json.dumps(dict(json.loads(token.read_text()),
                                     rung=rung)))
    if name == "m2p":
        my("update acct set balance = balance + 10 where id = 3")
    else:
        psql(pg_port, "update public.acct set balance = balance + 10 where"
                      " id = 3", db="app")

    def target():
        rows = _rows(pg_port)[1 if name == "m2p" else 0]
        return rows.splitlines()[2]
    # the batch committed on the target, then the tail stopped before its
    # position was saved here - as a kill between the two would
    monkeypatch.setenv("MIGKIT_FAILPOINT", "tail.applied:1:raise")
    from migkit import failpoint
    failpoint._counts.clear()
    said = []
    with pytest.raises(RuntimeError, match="failpoint tail.applied"):
        eng.tail_apply("app", True, token, said.append)
    assert target() == "3\t110\tn3", said
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    t1, said1, end1 = _run(eng, tmp_path)
    try:
        time.sleep(8)
    finally:
        _stop(t1)
    assert target() == "3\t110\tn3", (said1, end1)
    assert any("had committed batch" in m for m in said1), said1
    assert _rung(tmp_path, name) == rung
    # the teardown takes away what the rung left on the target
    steps = eng.two_way_teardown("app", token, True)
    if rung == "table":
        assert steps and "drop table" in steps[0]
        if name == "m2p":
            assert _pg_tables(pg_port) == "0"
        else:
            assert my("select count(*) from information_schema.tables"
                      " where table_name = 'migkit_origin'") == "0"
    elif rung == "origin":
        assert psql(pg_port, "select count(*) from pg_replication_origin"
                             " where roname like 'migkit\\_twoway\\_%'"
                    ).stdout.strip() == "0"
    else:
        assert steps == []


def _pg_side(pg_port, name):
    from migkit.engines.postgres import PostgresEngine
    ep = _eps(pg_port)[1]
    hop = Hop(name=name, engine="postgres", source=ep, target=ep,
              databases=["app"], options={"two_way": {"on_conflict":
                                                      "error"}})
    return PostgresEngine(hop)


def _my_side(name, server_id):
    from migkit.engines.mysql import MySQLEngine
    ep = _eps(0)[0]
    hop = Hop(name=name, engine="mysql", source=ep, target=ep,
              databases=["app"], options={"two_way": {"on_conflict":
                                                      "error"},
                                          "server_id": server_id})
    return MySQLEngine(hop)


def _mine(eng, rung, row, note):
    from migkit import canon
    eng._mark_rung = rung
    eng.neutral_apply("dst", "app", [canon.change(
        "update", "acct", {"id": row}, {"id": row, "balance": 100,
                                        "note": note})])


def test_postgresql_leaves_out_every_rungs_mark_and_nothing_of_the_apps(
        sides):
    pg_port = sides
    applier, reader = _pg_side(pg_port, "pgapply"), _pg_side(pg_port,
                                                             "pgread")
    for rung in ("origin", "message", "table"):
        assert applier.mark_prove("dst", "app", rung) is None, rung
    token = reader.change_point("src", "app")
    for k, rung in enumerate(("origin", "message", "table")):
        _mine(applier, rung, 1, f"mine {rung}")
        psql(pg_port, f"update public.acct set note = 'app {k}' where"
                      " id = 2", db="app")
        _mine(applier, rung, 1, f"mine again {rung}")
    # the application's own message under another prefix is its own
    psql(pg_port, "begin; select pg_logical_emit_message(true,"
                  " 'migkit_app', 'x'); update public.acct set note ="
                  " 'app msg' where id = 3; commit", db="app")
    got, _ = reader.neutral_changes("src", "app", token)
    assert [(c["key"]["id"], c["values"]["note"]) for c in got] == [
        (2, "app 0"), (2, "app 1"), (2, "app 2"), (3, "app msg")], got


def test_mysql_leaves_out_every_rungs_mark_and_nothing_of_the_apps(sides):
    import pymysql
    applier, reader = _my_side("myapply", 4701), _my_side("myread", 4702)
    for rung in ("gtid_tag", "comment", "table"):
        assert applier.mark_prove("dst", "app", rung) is None, rung
    token = reader.change_point("src", "app")
    app = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                          password="test", database="app")
    try:
        for k, rung in enumerate(("gtid_tag", "comment", "table")):
            _mine(applier, rung, 1, f"mine {rung}")
            with app.cursor() as cur:
                # a comment of the application's own is not migkit's
                cur.execute(f"/* app {k} */ update acct set note = 'app {k}'"
                            " where id = 2")
            app.commit()
            _mine(applier, rung, 1, f"mine again {rung}")
    finally:
        app.close()
    got, _ = reader.neutral_changes("src", "app", token)
    assert [(c["key"]["id"], c["values"]["note"]) for c in got] == [
        (2, "app 0"), (2, "app 1"), (2, "app 2")], got
    assert {m["kind"] for m in reader._marks_seen} == {"gtid_tag", "comment",
                                                       "table"}


def test_the_tail_stops_before_applying_where_no_rung_proves_itself(
        sides, tmp_path):
    pg_port = sides
    psql(pg_port, "drop role if exists plain; create role plain login"
                  " password 'plain'; grant create, usage on schema public"
                  " to plain; grant all on public.acct to plain", db="app")
    m2p, _ = _hops(tmp_path, pg_port, pg_user="plain", pg_password="plain",
                   on_conflict="error")
    token = tmp_path / "m2p" / "tail-token.json"
    m2p.tail_start("app", token)
    my("update acct set note = 'never' where id = 2")
    said = []
    with pytest.raises(SystemExit, match="no way of marking") as e:
        m2p.tail_apply("app", True, token, said.append)
    assert "cannot be read here" in str(e.value), str(e.value)
    assert _rows(pg_port)[1].splitlines()[1] == "2\t100\tn2"


def test_another_origin_applying_into_the_side_stops_the_reader(sides):
    pg_port = sides
    reader = _pg_side(pg_port, "pgread")
    token = reader.change_point("src", "app")
    # a subscription's own origin is named pg_<oid>, a name only the
    # server may give; another applier's, as a replication tool names it
    got = psql(pg_port, "select pg_replication_origin_create('leg_77001'),"
                        " pg_replication_origin_create('migkit_twoway_other')")
    assert got.returncode == 0, got.stderr
    try:
        with pytest.raises(SystemExit, match="leg_77001 applies into it"):
            reader.neutral_changes("src", "app", token)
        facts = reader.mark_facts("dst", "app")
        assert facts["foreign_origins"] == ["leg_77001"]
        # and without migkit's own there, nothing is left out by origin
        psql(pg_port, ORIGINS)
        psql(pg_port, "update public.acct set note = 'applied' where id = 2",
             db="app")
        got, _ = reader.neutral_changes("src", "app", token)
        assert [c["values"]["note"] for c in got] == ["applied"], got
    finally:
        psql(pg_port, "select pg_replication_origin_drop('leg_77001')")
