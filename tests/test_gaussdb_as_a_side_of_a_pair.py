"""GaussDB as a side of a pair (backlog P0-CLOUDS), against openGauss
(`opengauss/opengauss`, x86-64: run through `tools/with_docker_lock.py
--vm migkit`), with its password kept as MD5 - its own SHA-256 method is
one a PostgreSQL client does not speak - and its log carrying rows.

A database copied to another and compared, its digest worked out by the
server where its first rows are proved to render as here - and the same
number as folded here; a changed row a difference. Its changes followed
through a logical slot of migkit's own, decoded by openGauss's own JSON
plugin, to a fence. Huawei Cloud's GaussDB is the same engine behind its
console; what its console holds (the parameter changes, its roles'
limits) is proved only on a real instance.
"""
import ctypes
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

PROVES = [
    ("move.table-copy", "gaussdb"),
    ("schema.create", "gaussdb"),
    ("tail.follows", "gaussdb"),
    ("verify.fence", "gaussdb"),
    ("cutover.drain", "gaussdb"),
]

pytestmark = needs_docker

NAME, PORT = "migkit-test-gaussdb", 16454
IMAGE = "opengauss/opengauss:5.0.0"
SANDBOX_PASS = "Migkit@1234"
DATA = "/var/lib/opengauss/data"


def _omm(cmd):
    return subprocess.run(["docker", "exec", "-u", "omm", NAME, "bash",
                           "-lc", cmd], capture_output=True, text=True)


def _gsql(sql, db="postgres"):
    got = _omm(f"gsql -d {db} -p 5432 -t -A -c \"{sql}\"")
    assert got.returncode == 0, got.stderr
    return got.stdout


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "--platform",
                    "linux/amd64", "--privileged", "-e",
                    f"GS_PASSWORD={SANDBOX_PASS}", "-p",
                    f"127.0.0.1:{PORT}:5432", IMAGE], check=True,
                   capture_output=True)
    try:
        end = time.time() + 300
        while time.time() < end:
            if _omm("gsql -d postgres -p 5432 -c 'select 1'").returncode == 0:
                break
            time.sleep(3)
        got = _omm(f"gs_guc set -D {DATA} -c 'wal_level=logical'"
                   " -c 'password_encryption_type=1'"
                   " -h 'host all all 0.0.0.0/0 md5'"
                   f" && gs_ctl restart -D {DATA}")
        assert got.returncode == 0, got.stderr
        end = time.time() + 120
        while time.time() < end:
            if _omm("gsql -d postgres -p 5432 -c 'select 1'").returncode == 0:
                break
            time.sleep(2)
        _gsql(f"create user migkit with sysadmin replication password"
              f" '{SANDBOX_PASS}'")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME],
                       capture_output=True)


@pytest.fixture(autouse=True)
def _reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


@pytest.fixture
def shop(server):
    for db in ("shop", "shop_copy"):
        _omm(f"gsql -d postgres -p 5432 -c 'drop database if exists {db}'")
        _gsql(f"create database {db} owner migkit")
    _gsql("create table public.items (id int primary key, name varchar(40),"
          " price numeric(10,2), at timestamp, raw bytea, small tinyint)",
          "shop")
    _gsql("alter table public.items owner to migkit", "shop")
    rows = ", ".join(f"({i}, 'n{i}', {i}.25, '2026-01-0{i % 9 + 1}"
                     f" 10:00:00.123456', '\\\\x0{i % 10}ff', {i % 250})"
                     for i in range(1, 31))
    _gsql(f"insert into public.items values {rows}", "shop")
    for s in _gsql("select slot_name from pg_replication_slots").split():
        _gsql(f"select pg_drop_replication_slot('{s}')")


def _engine(tmp_path):
    from migkit.engines.gaussdb import GaussEngine

    def ep():
        return Endpoint(host="127.0.0.1", port=PORT, user="migkit",
                        password=SANDBOX_PASS, options={"sslmode": "disable"})
    hop = Hop(name="gauss", engine="gaussdb", source=ep(), target=ep(),
              databases=["shop"], db_map={"shop": "shop_copy"})
    hop.report_dir = lambda db=None: tmp_path
    return GaussEngine(hop)


def _move(eng, tmp_path):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    eng._as_pair().create_missing("shop", lambda m: None)
    for sch, t in eng.list_move_tables("shop"):
        eng.move_table("shop", sch, t, 10, ck, lambda m: None)


def _data(eng):
    return {r.scope: r.status for r in eng.check_data("shop")
            if r.check == "data"}


def test_it_says_it_is_gaussdb_and_its_tables_are_copied_and_compared(
        shop, tmp_path):
    eng = _engine(tmp_path)
    ident = eng.server_identity("src", "shop")
    assert "gauss" in ident["version"].lower()
    _move(eng, tmp_path)
    got = _data(eng)
    assert got and set(got.values()) == {"ok"}, got
    _gsql("update public.items set name = 'changed' where id = 7",
          "shop_copy")
    got = _data(_engine(tmp_path))
    assert "diff" in got.values(), got


def test_the_server_digest_is_proved_and_is_the_folded_number(shop,
                                                              tmp_path):
    from migkit import canon
    eng = _engine(tmp_path)
    cols = [("id", "integer"), ("name", "text"), ("price", "decimal"),
            ("raw", "bytes")]
    server = eng.neutral_digest("src", "shop", "public.items", cols)
    assert eng.folded_last is False, "the server's rendering was not proved"
    here = canon.fold_batches(cols, eng.neutral_batches(
        "src", "shop", "public.items", cols, 1000))
    assert server == here and server[0] == 30
    _gsql("update public.items set price = 99.99 where id = 3", "shop")
    assert eng.neutral_digest("src", "shop", "public.items", cols) != server


def test_its_changes_are_followed_through_a_slot_of_migkits_own(shop,
                                                                tmp_path):
    eng = _engine(tmp_path)
    token = tmp_path / "tail-token.json"
    eng.tail_start("shop", token)
    assert eng._slot("shop") in _gsql("select slot_name from"
                                      " pg_replication_slots")
    _move(eng, tmp_path)
    ended = {}

    def go():
        try:
            eng.tail_apply("shop", True, token, lambda m: None)
        except BaseException as e:
            ended["e"] = e
    thread = threading.Thread(target=go, daemon=True)
    thread.start()
    try:
        _gsql("insert into public.items (id, name) values (100, 'it''s')",
              "shop")
        _gsql("update public.items set price = 1.01, raw = null where"
              " id = 2", "shop")
        _gsql("delete from public.items where id = 3", "shop")
        at = eng.src_lsn("shop")
        assert at is not None
        assert eng.fence_wait("shop", at, timeout=120) is True
        got = _data(_engine(tmp_path))
        assert set(got.values()) == {"ok"}, got
        held = eng.stream_room("src", "shop", {"lsn": "0/0"})
        assert held is not None and held["held_bytes"] >= 0
    finally:
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    assert not ended or isinstance(ended["e"], KeyboardInterrupt), ended
