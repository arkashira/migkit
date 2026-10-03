"""A cross-engine tail stopped anywhere applies each change once
(audit 2026-10-01, ST1, ST2), between two PostgreSQL 16 servers.

The target counts every row the tail writes into it: a trigger enabled
ALWAYS, so it fires under the replica role the tail applies with,
inserts a row of `applies` for each one. A batch applied twice is a
count above the source's changes; a batch lost is a row missing.

* Stopped dead after its part of the tail's own state was set down and
  before the target committed: the batch is taken back out
  (`tailtables.Store.align`), read again and applied once; what the tail
  accounts as read is the source's changes, not twice them.
* Stopped dead after the target committed and before the position was
  saved: the target's mark of the batch says it was committed, and the
  tail goes on after it.
* The position and the tail's state put back as they were four batches
  earlier - a power loss rolling the disk back, a copy another machine
  kept: the target's mark is held to this run's (`run`), the tail goes
  on after the last batch it committed, nothing applied again.
"""
import os
import shutil
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from migkit import tailctl, tailtables
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = "migkit-test-audb-pg-src", "migkit-test-audb-pg-dst"
SRC_PORT, DST_PORT = 16490, 16491
DB = "postgres"


def _psql(name, sql):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", DB, "-At", "-c",
                          sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{port}:5432", "postgres:16", "-c",
                        "wal_level=logical"], check=True,
                       capture_output=True)
    try:
        for name in (SRC, DST):
            for _ in range(60):
                if subprocess.run(["docker", "exec", name, "pg_isready",
                                   "-U", "postgres"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(1)
        time.sleep(2)
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _tables():
    for name in (SRC, DST):
        _psql(name, "drop table if exists public.t, public.applies,"
                    " public.migkit_origin cascade;"
                    " create table public.t (id int primary key, n int)")
    _psql(DST, textwrap.dedent("""
        create table public.applies (id int, at timestamptz default now());
        create function public.count_apply() returns trigger
          language plpgsql as $$
          begin insert into public.applies (id) values (new.id);
          return new; end $$;
        create trigger t_count after insert or update on public.t
          for each row execute function public.count_apply();
        alter table public.t enable always trigger t_count;
    """))


_TAIL = textwrap.dedent("""
    import sys
    from pathlib import Path
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    where, name = Path(sys.argv[1]), sys.argv[2]
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="127.0.0.1", port=%d, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=%d, user="postgres",
                              password="test"),
              databases=["postgres"], workers=1,
              options={"follow_until": "caught_up"})
    hop.report_dir = lambda db=None: where
    pair = PostgresEngine(hop)._as_pair()
    pair.tail_apply("postgres", True, where / "tail-token.json", print)
""" % (SRC_PORT, DST_PORT))


def _run(where, name, failpoint=None):
    env = dict(os.environ)
    env.pop("MIGKIT_FAILPOINT", None)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    if failpoint:
        env["MIGKIT_FAILPOINT"] = failpoint
    return subprocess.run([sys.executable, "-c", _TAIL, str(where), name],
                          env=env, capture_output=True, text=True,
                          timeout=300)


def _drop_slot(name):
    _psql(SRC, "select pg_drop_replication_slot(slot_name) from"
               " pg_replication_slots where slot_name like 'migkit_%'")


def _counts():
    return (int(_psql(SRC, "select count(*) from public.t")),
            int(_psql(DST, "select count(*) from public.t")),
            int(_psql(DST, "select count(*) from public.applies")))


@pytest.mark.parametrize("stop", ["tail.set_down", "tail.applied"])
def test_stopped_dead_inside_a_batch_each_change_lands_once(servers,
                                                            tmp_path, stop):
    _tables()
    where = tmp_path / "r"
    where.mkdir()
    name = stop.replace(".", "_")
    try:
        # the position taken before anything is written
        got = _run(where, name)
        assert got.returncode == 0, got.stdout + got.stderr
        _psql(SRC, "insert into public.t select g, 0 from"
                   " generate_series(1, 50) g")
        got = _run(where, name, f"{stop}:1:exit")
        assert got.returncode == 137, got.stdout + got.stderr
        landed = int(_psql(DST, "select count(*) from public.applies"))
        # set down only: nothing on the target yet; applied: all of it
        assert landed == (0 if stop == "tail.set_down" else 50), landed
        got = _run(where, name)
        said = got.stdout
        assert got.returncode == 0, said + got.stderr
        if stop == "tail.applied":
            assert "had committed batch" in said, said
        else:
            assert "taken back out" in said, said
        assert _counts() == (50, 50, 50)
        # what the tail read is the source's changes, counted once
        assert tailtables.accounts(where)["public.t"]["read"] == 50
    finally:
        _drop_slot(name)


def test_a_position_put_back_batches_behind_applies_nothing_twice(
        servers, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    _tables()
    where = tmp_path / "r"
    where.mkdir()
    hop = Hop(name="rolled", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              user="postgres", password="test"),
              databases=[DB], workers=1)
    hop.report_dir = lambda db=None: where
    pair = PostgresEngine(hop)._as_pair()
    path = where / tailctl.TOKEN
    lines, done = [], threading.Event()

    def run():
        try:
            pair.tail_apply(DB, True, path, lines.append)
        finally:
            done.set()
    try:
        threading.Thread(target=run, daemon=True).start()
        deadline = time.time() + 60
        while not tailctl.alive(where) and time.time() < deadline:
            time.sleep(0.2)
        kept = None
        for b in range(5):
            _psql(SRC, f"insert into public.t select g, {b} from"
                       f" generate_series({b * 10 + 1}, {b * 10 + 10}) g")
            want = (b + 1) * 10
            while int(_psql(DST, "select count(*) from public.t")) < want:
                assert time.time() < deadline + 60, lines[-5:]
                time.sleep(0.2)
            # held still between batches, its files copied as they are
            assert tailctl.pause(where, 60)
            try:
                if b == 0:
                    kept = tmp_path / "kept"
                    shutil.copytree(where, kept)
            finally:
                tailctl.resume(where)
        tailctl.stop(where)
        assert done.wait(60), lines[-5:]
        assert _counts() == (50, 50, 50)
        # the disk rolled back four batches: this process's hold on the
        # store let go first, then its files as they were
        tailtables.Store.forget(where)
        for n in (tailctl.TOKEN, tailtables.STORE, tailtables.STORE + "-wal",
                  tailtables.STORE + "-shm"):
            (where / n).unlink(missing_ok=True)
            if (kept / n).exists() and not n.endswith("-shm"):
                shutil.copy(kept / n, where / n)
        hop.options = {"follow_until": "caught_up"}
        lines.clear()
        pair = PostgresEngine(hop)._as_pair()
        pair.tail_apply(DB, True, path, lines.append)
        said = " | ".join(lines)
        assert "batches past it" in said, said
        # the target's own count: nothing applied a second time
        assert _counts() == (50, 50, 50)
    finally:
        tailctl.stop(where)
        _drop_slot("rolled")
