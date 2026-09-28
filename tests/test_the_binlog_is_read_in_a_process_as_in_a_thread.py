"""The binlog read in a process of its own gives what the thread gives.

The same reader and library in both (`hetero._ReadProcess`,
`hetero._ReadAhead`), so the question is only whether anything is lost,
changed or moved on the way across: every kind of column MySQL 8.4 writes,
a transaction larger than a batch (read on from inside it), a key moved and
rows removed, read as the same batches to the same positions. Then the
tail itself on it: every change landed, a process stopped between reads
started again from the position asked, and a batch the target committed
before its position was saved not applied again.
"""
import json
import socket
import subprocess
import threading
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = pytest.mark.docker

MY, MY_PORT = "migkit-test-spd-my", 16100
PG, PG_PORT = "migkit-test-spd-pg", 16101


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def my(sql, db="appdb"):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B",
                          "--default-character-set=utf8mb4"]
                         + ([db] if db else []), input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def pg(sql, db="appdb"):
    got = subprocess.run(["docker", "exec", "-i", "-e", "PGPASSWORD=test", PG,
                          "psql", "-U", "postgres", "-d", db, "-At", "-v",
                          "ON_ERROR_STOP=1"], input=sql, capture_output=True,
                         text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(name, port, argv, ready):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-p",
                    f"127.0.0.1:{port}:{argv[0]}"] + argv[1:], check=True,
                   capture_output=True)
    end = time.time() + 240
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0 and subprocess.run(
                    ["docker", "exec", name] + ready,
                    capture_output=True).returncode == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module")
def servers():
    try:
        _up(MY, MY_PORT, ["3306", "-e", "MYSQL_ROOT_PASSWORD=test",
                          "mysql:8.4", "--server-id=7",
                          "--binlog-row-metadata=FULL"],
            ["mysql", "-uroot", "-ptest", "-h127.0.0.1", "--protocol=tcp",
             "-e", "select 1"])
        _up(PG, PG_PORT, ["5432", "-e", "POSTGRES_PASSWORD=test",
                          "postgres:16", "-c", "wal_level=logical"],
            ["psql", "-U", "postgres", "-h", "127.0.0.1", "-c", "select 1"])
        yield
    finally:
        for name in (MY, PG):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop(**options):
    return Hop(name="spd", engine="hetero",
               source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                               password="test"),
               target=Endpoint(host="127.0.0.1", port=PG_PORT,
                               user="postgres", password="test"),
               databases=["appdb"], workers=4,
               options={"source_engine": "mysql",
                        "target_engine": "postgres", **options})


KINDS = """
create table k (
  id bigint primary key, u bigint unsigned, i int, ti tinyint,
  si smallint, mi mediumint, d decimal(30,10), f float, g double,
  s varchar(64), c char(8), tx text, b varbinary(32), bl blob, j json,
  dt datetime(6), ts timestamp(6), da date, t time(6), y year,
  e enum('a','b','c'), st set('x','y','z'), bt bit(10)
) default charset utf8mb4;
"""

ROWS = [
    "(1, 18446744073709551615, -2147483648, -128, -32768, -8388608,"
    " 12345678901234567890.0123456789, 1.5, -2.2250738585072014e-308,"
    " 'café \u2028 two\\nlines', 'ch', repeat('t', 3000),"
    " x'00ff41', x'deadbeef', '{\"b\": [1, 2.50, null], \"a\": \"é\"}',"
    " '2026-01-02 03:04:05.000006', '2026-01-02 03:04:05.123456',"
    " '1000-01-01', '-838:59:59.000000', 1901, 'b', 'x,z', b'1010101010')",
    "(2, 0, 0, 0, 0, 0, 0.0000000001, 0, 0, '', '', '', x'', x'', 'null',"
    " '1970-01-01 00:00:01', '1970-01-01 00:00:01', '9999-12-31',"
    " '00:00:00', 2155, 'a', '', b'0')",
    "(3, null, null, null, null, null, null, null, null, null, null, null,"
    " null, null, null, null, null, null, null, null, null, null, null)",
]


def _load():
    my("drop database if exists appdb; create database appdb", db="")
    my(KINDS)
    pg("drop database if exists appdb", db="postgres")
    pg("create database appdb", db="postgres")


def _work():
    """Every kind of value, a transaction larger than a batch, a key moved
    and rows removed."""
    my("insert into k values " + ", ".join(ROWS))
    many = ", ".join(f"({i}, {i}, {i % 7}, 1, 2, 3, {i}.5, 0.25, {i}e-3,"
                     f" 'row {i}', 'c', 'x', x'01', x'02', '[{i}]',"
                     " '2026-01-01 00:00:00', '2026-01-01 00:00:00',"
                     " '2026-01-01', '12:00:00', 2026, 'c', 'y', b'1')"
                     for i in range(10, 2510))
    my(f"begin; insert into k values {many}; commit;")
    my("update k set id = id + 100000, s = 'moved' where id in (1, 11);"
       " update k set j = json_set(j, '$.a', 'changed') where id = 100001;"
       " delete from k where id % 5 = 0 and id < 1000")


def _window(reader, start, limit=333):
    token, out = start, []
    try:
        while True:
            changes, token = reader.next(token, limit)
            out.append((repr(changes), json.dumps(token, sort_keys=True)))
            if len(changes) < limit:
                return out
    finally:
        reader.close()


def test_the_process_reads_the_window_the_thread_reads(servers):
    from migkit.engines.hetero import HeteroEngine, _ReadAhead, _ReadProcess
    _load()
    eng = HeteroEngine(_hop())
    start = eng.src_engine.change_point("src", "appdb")
    _work()
    thread = _window(_ReadAhead(eng.src_engine, "appdb"), start)
    process = _window(_ReadProcess(HeteroEngine(_hop()).src_engine, "appdb"),
                      start)
    # 2,503 inserts, 3 updates, 198 deletes
    assert sum(r.count("'op': ") for r, _ in thread) == 2704
    assert len(thread) >= 5, [t for _, t in thread]
    # read on from inside the large transaction, by a count of its rows
    assert any("skip_rows" in t for _, t in thread), [t for _, t in thread]
    assert process == thread


def test_a_process_stopped_between_reads_is_started_again_where_asked(
        servers):
    from migkit.engines.hetero import HeteroEngine, _ReadProcess
    _load()
    eng = HeteroEngine(_hop())
    start = eng.src_engine.change_point("src", "appdb")
    _work()
    a, t1 = eng.src_engine.neutral_changes("src", "appdb", start, limit=400)
    b, t2 = eng.src_engine.neutral_changes("src", "appdb", t1, limit=400)
    eng.src_engine.release_changes()
    reader = _ReadProcess(HeteroEngine(_hop()).src_engine, "appdb")
    try:
        got, at = reader.next(start, 400)
        assert (repr(got), at) == (repr(a), t1)
        # stopped with the read ahead in it: asked again from there
        first = reader.worker.proc.pid
        reader.worker.proc.kill()
        got, at = reader.next(t1, 400)
        assert (repr(got), at) == (repr(b), t2)
        assert reader.worker.proc.pid != first
        # back to a position saved earlier, as after a lost connection:
        # what was read ahead is not what is asked for
        got, at = reader.next(start, 400)
        assert (repr(got), at) == (repr(a), t1)
    finally:
        reader.close()


def _tail_until(eng, token_path, done, seconds=90):
    said, ended = [], {}

    def run():
        try:
            eng.tail_apply("appdb", True, token_path, said.append)
        except BaseException as e:  # noqa: BLE001 - KeyboardInterrupt here
            ended["e"] = e
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    end = time.time() + seconds
    while time.time() < end and not done() and thread.is_alive():
        time.sleep(0.5)
    if thread.is_alive():
        import ctypes
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
        thread.join(timeout=30)
    return said, ended


def _in_a_process(monkeypatch):
    from migkit.engines import hetero

    def reader(self, db, token_path, log):
        return hetero._ReadProcess(self.src_engine, db)
    monkeypatch.setattr(hetero.HeteroEngine, "_change_reader", reader)


def test_the_tail_on_a_process_lands_every_change(servers, tmp_path,
                                                  monkeypatch):
    from migkit import canon
    from migkit.engines.hetero import HeteroEngine
    _load()
    my("create table w (id bigint primary key, v int, s varchar(20),"
       " amount decimal(12,2), at datetime(6))")
    pg("create table public.w (id bigint primary key, v int,"
       " s varchar(20), amount numeric(12,2), at timestamp(6))")
    eng = HeteroEngine(_hop())
    token_path = tmp_path / "tail-token.json"
    eng.tail_start("appdb", token_path)
    my("insert into w values " + ", ".join(
        f"({i}, {i % 13}, 's{i}', {i}.25, '2026-01-01 00:00:{i % 60:02d}')"
        for i in range(1, 6001)))
    my("update w set v = v + 1 where id % 2 = 0;"
       " delete from w where id % 10 = 0")
    _in_a_process(monkeypatch)

    def landed():
        return pg("select count(*) || ',' || coalesce(sum(v), 0) from"
                  " public.w") == my("select concat(count(*), ',',"
                                     " coalesce(sum(v), 0)) from w")
    said, ended = _tail_until(eng, token_path, landed)
    assert landed(), (said[-5:], ended)
    cols = [(n, canon.comparable("mysql", t)[0])
            for n, t in eng.src_engine.neutral_columns("src", "appdb", "w")]
    assert eng.src_engine.neutral_digest("src", "appdb", "w", cols) == \
        eng.dst_engine.neutral_digest("dst", "appdb", "public.w", cols)


def test_a_batch_committed_before_its_position_was_saved_is_not_added_again(
        servers, tmp_path, monkeypatch):
    """A counter's batch is one transaction with its number on the target;
    read in a process, the tail going on after a stop still finds it there
    and goes on after it."""
    from migkit import failpoint
    from migkit.engines.hetero import HeteroEngine
    _load()
    my("create table acct (id int primary key, balance int)")
    pg("create table public.acct (id int primary key, balance int)")
    my("insert into acct values (1, 100)")
    pg("insert into public.acct values (1, 100)")
    hop = _hop(two_way={"on_conflict": "error", "delta": ["balance"]},
               server_id=4600)
    eng = HeteroEngine(hop)
    token = tmp_path / "tail-token.json"
    eng.tail_start("appdb", token)
    my("update acct set balance = balance + 10 where id = 1")
    _in_a_process(monkeypatch)
    monkeypatch.setenv("MIGKIT_FAILPOINT", "tail.applied:1:raise")
    failpoint._counts.clear()
    with pytest.raises(RuntimeError, match="failpoint tail.applied"):
        eng.tail_apply("appdb", True, token, [].append)
    assert pg("select balance from public.acct") == "110"
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    said, ended = _tail_until(HeteroEngine(hop), token,
                              lambda: False, seconds=8)
    assert pg("select balance from public.acct") == "110", (said, ended)
    assert any("had committed batch" in m for m in said), said
