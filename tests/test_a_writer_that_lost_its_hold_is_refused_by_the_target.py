"""A writer that lost its hold on a hop, a table or a range is refused by
the target itself (backlog W1, `fencing`).

The target keeps the highest number any holder raised a row of
`migkit_origin` to; every transaction a holder writes reads the row under
a shared lock last before its commit and is held to its own number. Here,
on a PostgreSQL and a MySQL target in containers of their own: a write
under a number another holder has since raised past is refused and none
of it stays - through the copier's writes and the tail's batches alike;
a raise waits for a writer's transaction that holds the row, so a batch
in flight commits before it; and the proof each target is put to before
a standby is run over it says "no" to a target whose shared lock holds
nothing back.
"""
import socket
import subprocess
import threading
import time

import pytest

from migkit import fencing
from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("operations.fencing", ("postgres", "mysql"))]

pytestmark = [pytest.mark.docker]

PG, PG_PORT = "migkit-test-w1-fence-pg", 16420
MY, MY_PORT = "migkit-test-w1-fence-my", 16422


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _psql(sql):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", PG,
                          "psql", "-U", "postgres", "-d", "app", "-At",
                          "-c", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B", "app"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def targets():
    if not _docker():
        pytest.skip("docker not available")
    for name in (PG, MY):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-e",
                    "POSTGRES_PASSWORD=test", "-e", "POSTGRES_DB=app", "-p",
                    f"127.0.0.1:{PG_PORT}:5432", "postgres:16"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-e", "MYSQL_DATABASE=app",
                    "-p", f"127.0.0.1:{MY_PORT}:3306", "mysql:8.4"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            pg = subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                                 "postgres", "-h", "127.0.0.1"],
                                capture_output=True).returncode == 0
            my = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as a, socket.socket() as b:
                a.settimeout(1)
                b.settimeout(1)
                up = (a.connect_ex(("127.0.0.1", PG_PORT)) == 0
                      and b.connect_ex(("127.0.0.1", MY_PORT)) == 0)
            if pg and my and up:
                break
            time.sleep(2)
        else:
            pytest.fail("the targets never answered")
        yield
    finally:
        for name in (PG, MY):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _engine(tmp_path, kind):
    from migkit.engines import engine_named
    port, user = (PG_PORT, "postgres") if kind == "postgres" else (MY_PORT,
                                                                  "root")
    ep = Endpoint(host="127.0.0.1", port=port, user=user, password="test")
    hop = Hop(name=f"fence{kind[:2]}", engine=kind, source=ep, target=ep,
              databases=["app"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return engine_named(kind, hop)


def _table(kind):
    if kind == "postgres":
        _psql("drop table if exists public.t, public.migkit_origin;"
              " create table public.t (id int primary key, v text)")
        return "public.t"
    _my("drop table if exists t, migkit_origin;"
        " create table t (id int primary key, v text)")
    return "t"


def _count(kind, where=""):
    sql = "select count(*) from t" + (f" where {where}" if where else "")
    return int(_psql(sql) if kind == "postgres" else _my(sql))


COLS = [("id", "integer"), ("v", "text")]
KINDS = ("postgres", "mysql")


@pytest.mark.parametrize("kind", KINDS)
def test_the_target_proves_it_holds_a_writer_to_its_number(targets,
                                                           tmp_path, kind):
    _table(kind)
    assert fencing.prove(_engine(tmp_path, kind), "dst", "app") is None


@pytest.mark.parametrize("kind", KINDS)
def test_a_shared_lock_that_holds_nothing_back_is_not_trusted(
        targets, tmp_path, kind, monkeypatch):
    _table(kind)
    # the shared lock taken away: a server that speaks the wire and keeps
    # the lock as a no-op
    plain = dict(fencing.SQL[kind],
                 share="select n, seen from {t} where origin = %s")
    monkeypatch.setitem(fencing.SQL, kind, plain)
    why = fencing.prove(_engine(tmp_path, kind), "dst", "app")
    assert why and "did not keep a newer number out" in why, why


@pytest.mark.parametrize("kind", KINDS)
def test_a_write_under_a_number_raised_past_is_refused(targets, tmp_path,
                                                       kind):
    table = _table(kind)
    eng = _engine(tmp_path, kind)
    old = fencing.Token(f"{eng.hop.name}#r:t:0", 1, "machine-a", "dir:x",
                        lazy=True)
    new = fencing.Token(old.row, 2, "machine-b", "dir:x", lazy=True)
    fencing.arm(eng.hop, old)
    try:
        eng.neutral_write("dst", "app", table, COLS,
                          [[i, f"a{i}"] for i in range(1, 11)])
        assert _count(kind) == 10
        # another machine claims the range and raises its number
        assert fencing.raise_once(eng, "dst", "app", new) == 2
        with pytest.raises(fencing.Fenced) as e:
            eng.neutral_write("dst", "app", table, COLS,
                              [[i, f"a{i}"] for i in range(11, 21)])
        assert "raised the target's number to 2" in str(e.value)
        # nothing of the refused write stays
        assert _count(kind) == 10
        with pytest.raises(fencing.Fenced):
            eng.neutral_empty("dst", "app", table)
        assert _count(kind) == 10
    finally:
        fencing.disarm(eng.hop)
    fencing.arm(eng.hop, new)
    try:
        eng.neutral_write("dst", "app", table, COLS,
                          [[i, f"b{i}"] for i in range(11, 21)])
        assert _count(kind) == 20
    finally:
        fencing.disarm(eng.hop)
    # and with nothing armed, writes are as they always were
    eng.neutral_write("dst", "app", table, COLS, [[21, "free"]])
    assert _count(kind) == 21


@pytest.mark.parametrize("kind", KINDS)
def test_a_batch_under_an_old_number_is_refused_and_rolled_back(
        targets, tmp_path, kind):
    table = _table(kind)
    eng = _engine(tmp_path, kind)
    row = f"{eng.hop.name}#fence"
    old = fencing.Token(row, 5, "machine-a", "dir:x")
    assert fencing.raise_to(eng, "dst", "app", old)[0] == 5
    changes = [{"op": "update", "table": table, "key": {"id": i},
                "values": {"id": i, "v": f"c{i}"}} for i in range(1, 6)]
    fencing.arm(eng.hop, old)
    try:
        assert eng.neutral_apply("dst", "app", changes) == 5
        assert _count(kind) == 5
        new = fencing.Token(row, 6, "machine-b", "dir:x")
        assert fencing.raise_once(eng, "dst", "app", new) == 6
        more = [{"op": "update", "table": table, "key": {"id": i},
                 "values": {"id": i, "v": f"late{i}"}} for i in range(1, 9)]
        with pytest.raises(fencing.Fenced):
            eng.neutral_apply("dst", "app", more)
        # the batch was one transaction: none of it landed
        assert _count(kind) == 5
        assert _count(kind, "v like 'late%'") == 0
    finally:
        fencing.disarm(eng.hop)


@pytest.mark.parametrize("kind", KINDS)
def test_a_raise_waits_for_the_writer_holding_the_row(targets, tmp_path,
                                                      kind):
    table = _table(kind)
    eng = _engine(tmp_path, kind)
    row = f"{eng.hop.name}#fence"
    holder = fencing.Token(row, 1, "machine-a", "dir:x")
    fencing.raise_once(eng, "dst", "app", holder)
    held, committed = threading.Event(), {}

    def writer():
        conn, _ = eng.own_table("dst", "app")
        try:
            cur = conn.cursor()
            ins = (f"insert into {table} (id, v) values (%s, %s)")
            cur.execute(ins, (100, "in flight"))
            fencing.hold(eng, "dst", "app", cur)
            held.set()
            time.sleep(2.0)
            conn.commit()
            committed["at"] = time.monotonic()
        finally:
            conn.close()
    fencing.arm(eng.hop, holder)
    try:
        t = threading.Thread(target=writer)
        t.start()
        assert held.wait(30)
        began = time.monotonic()
        n, seen = fencing.raise_to(eng, "dst", "app",
                                   fencing.Token(row, 2, "machine-b",
                                                 "dir:x"))
        done = time.monotonic()
        t.join()
    finally:
        fencing.disarm(eng.hop)
    assert n == 2 and seen["by"] == "machine-b"
    # the raise passed only once the writer holding the row committed
    assert done >= committed["at"], (began, committed["at"], done)
    assert done - began >= 1.5
    assert _count(kind, "id = 100") == 1


@pytest.mark.parametrize("kind", KINDS)
def test_a_holder_of_another_store_is_refused(targets, tmp_path, kind):
    _table(kind)
    eng = _engine(tmp_path, kind)
    row = f"{eng.hop.name}#fence"
    fencing.raise_once(eng, "dst", "app",
                       fencing.Token(row, 3, "machine-a", "bucket:one"))
    # the same number from a machine whose lease is kept elsewhere
    other = fencing.Token(row, 3, "machine-z", "directory:/elsewhere")
    with pytest.raises(fencing.Fenced) as e:
        fencing.raise_once(eng, "dst", "app", other)
    assert "machine-a" in str(e.value)
