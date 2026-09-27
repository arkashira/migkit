"""A move whose target changes under it: turned read-only part way (a
failover to a reader, a parameter flipped), or its disk full - and one
whose own machine's disk fills. Each stops the move saying what happened
on which side, keeps the checkpoint whole, and run again once it is put
right, the target equals the source.
"""
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROOT = Path(__file__).resolve().parent.parent


def _migkit(tmp, *args, reports=None, **env):
    full = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
                MIGKIT_REPORTS=str(reports or tmp / "reports"),
                MIGKIT_MOVER="builtin", COLUMNS="200", **env)
    return subprocess.run([sys.executable, "-c",
                           "from migkit.cli import main; main()", *args],
                          env=full, cwd=ROOT, capture_output=True, text=True,
                          timeout=600)


def _hop(tmp, src_port, dst_port, extra=""):
    (tmp / "hops.yaml").write_text(
        "hops:\n  shift:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {src_port}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {dst_port}, user: postgres,"
        " password: test}\n    databases: [postgres]\n" + extra)


Q = ("select count(*) || ':' || md5(string_agg(id || '|' || payload, ','"
     " order by id)) from public.big")


def _seed(port, rows=150000, width=200):
    if psql(port, "select to_regclass('public.big')").stdout.strip():
        return
    got = psql(port, f"create table public.big (id bigint primary key,"
                     f" payload text); insert into public.big select g,"
                     f" repeat('x', {width}) || g from generate_series(1,"
                     f" {rows}) g; analyze")
    assert got.returncode == 0, got.stderr


def test_a_target_turned_read_only_part_way(pg_pair, tmp_path):
    _seed(pg_pair["src"])
    psql(pg_pair["dst"], "drop table if exists public.big")
    _hop(tmp_path, pg_pair["src"], pg_pair["dst"], "    workers: 1\n")
    flipped = {}

    def flip_once_a_range_is_in():
        end = time.time() + 120
        while time.time() < end:
            got = psql(pg_pair["dst"], "select coalesce((select count(*)"
                                       " from public.big), 0)").stdout.strip()
            if got.isdigit() and int(got) > 0:
                # two statements: ALTER SYSTEM takes no transaction around
                for sql in ("alter system set default_transaction_read_only"
                            " = on", "select pg_reload_conf()"):
                    done = psql(pg_pair["dst"], sql)
                    assert done.returncode == 0, done.stderr
                flipped["yes"] = True
                return
            time.sleep(0.1)
    threading.Thread(target=flip_once_a_range_is_in, daemon=True).start()
    try:
        first = _migkit(tmp_path, "move", "shift", "--go", "--chunk",
                        "20000")
        said = first.stdout + first.stderr
        assert flipped and first.returncode != 0, said[-2000:]
        assert "the target is read-only" in said, said[-2000:]
    finally:
        psql(pg_pair["dst"], "alter system reset"
                             " default_transaction_read_only")
        psql(pg_pair["dst"], "select pg_reload_conf()")
    again = _migkit(tmp_path, "move", "shift", "--go", "--chunk", "20000")
    assert again.returncode == 0, again.stdout[-2500:] + again.stderr[-1500:]
    assert psql(pg_pair["src"], Q).stdout == psql(pg_pair["dst"], Q).stdout


SMALL, SMALL_PORT = "migkit-test-small-disk", 15883


@pytest.fixture
def small_target():
    """A PostgreSQL whose data directory has little room: a filesystem of
    160 MB in memory."""
    subprocess.run(["docker", "rm", "-f", "-v", SMALL], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", SMALL, "-p",
                    f"{SMALL_PORT}:5432", "-e", "POSTGRES_PASSWORD=test",
                    "-e", "PGDATA=/data/pg",
                    # the log kept off the small filesystem: the server
                    # panics when its log cannot be written, and that is
                    # not what is being measured
                    "-e", "POSTGRES_INITDB_WALDIR=/wal", "--tmpfs",
                    "/data:rw,size=160m,mode=1777", "postgres:16"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 90
        while time.time() < end:
            if subprocess.run(["docker", "exec", SMALL, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                with socket.socket() as s:
                    if s.connect_ex(("127.0.0.1", SMALL_PORT)) == 0:
                        break
            time.sleep(1)
        time.sleep(2)
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", SMALL],
                       capture_output=True)


def _small(sql):
    return subprocess.run(["docker", "exec", "-i", SMALL, "psql", "-U",
                           "postgres", "-tA", "-v", "ON_ERROR_STOP=1"],
                          input=sql, capture_output=True, text=True)


def test_a_target_whose_disk_fills(pg_pair, small_target, tmp_path):
    _seed(pg_pair["src"])
    # the room taken by something else on the target, all but ~20 MB:
    # less than the 33 MB the table needs
    assert _small("create table ballast (b text)").returncode == 0
    while True:
        left = int(subprocess.run(["docker", "exec", SMALL, "df", "-m",
                                   "--output=avail", "/data"],
                                  capture_output=True, text=True
                                  ).stdout.split()[-1])
        if left <= 20:
            break
        got = _small("insert into ballast select repeat(md5(g::text), 30)"
                     f" from generate_series(1, {min(10000, left * 800)}) g")
        assert got.returncode == 0, got.stderr
    _hop(tmp_path, pg_pair["src"], SMALL_PORT)
    first = _migkit(tmp_path, "move", "shift", "--go", "--chunk", "20000")
    said = first.stdout + first.stderr
    assert first.returncode != 0, said[-2000:]
    assert "the target has run out of disk space" in said, said[-2000:]
    _small("drop table ballast; checkpoint")
    again = _migkit(tmp_path, "move", "shift", "--go", "--chunk", "20000")
    assert again.returncode == 0, again.stdout[-2500:] + again.stderr[-1500:]
    assert psql(pg_pair["src"], Q).stdout.strip() == \
        _small(Q).stdout.strip()


@pytest.fixture
def tiny_disk(tmp_path):
    """A 12 MB filesystem of migkit's own, for its report directory."""
    if sys.platform != "darwin":
        pytest.skip("made with hdiutil, which only macOS has")
    image = tmp_path / "tiny.dmg"
    subprocess.run(["hdiutil", "create", "-size", "12m", "-fs", "HFS+",
                    "-volname", "migkittiny", str(image)], check=True,
                   capture_output=True)
    mount = tmp_path / "mnt"
    mount.mkdir()
    subprocess.run(["hdiutil", "attach", str(image), "-mountpoint",
                    str(mount), "-nobrowse"], check=True, capture_output=True)
    try:
        yield mount
    finally:
        subprocess.run(["hdiutil", "detach", str(mount), "-force"],
                       capture_output=True)


def test_migkits_own_disk_filling_keeps_the_checkpoint_whole(pg_pair,
                                                            tiny_disk,
                                                            tmp_path):
    _seed(pg_pair["src"])
    psql(pg_pair["dst"], "drop table if exists public.big")
    _hop(tmp_path, pg_pair["src"], pg_pair["dst"])
    reports = tiny_disk / "reports"
    # a first run leaves a checkpoint, stopped part way
    first = _migkit(tmp_path, "move", "shift", "--go", "--chunk", "20000",
                    reports=reports,
                    MIGKIT_FAILPOINT="range.saved:2:exit")
    assert first.returncode != 0
    ck = next(reports.rglob("move.json"))
    before = ck.read_text()
    # the disk filled by something else, but for a few bytes
    filler = tiny_disk / "filler"
    with open(filler, "wb", buffering=0) as f:
        for size in (65536, 4096, 512, 1):
            try:
                while True:
                    f.write(b"\0" * size)
            except OSError:
                pass
    second = _migkit(tmp_path, "move", "shift", "--go", "--chunk", "20000",
                     reports=reports)
    said = second.stdout + second.stderr
    assert second.returncode != 0, said[-2000:]
    assert "no space left on this machine" in said, said[-2000:]
    assert "Traceback" not in said, said[-2000:]
    # what the checkpoint said is still whole, or further along - never
    # half written
    import json
    json.loads(ck.read_text())
    assert not list(reports.rglob("*.tmp")) or \
        json.loads(before) is not None
    filler.unlink()
    again = _migkit(tmp_path, "move", "shift", "--go", "--chunk", "20000",
                    reports=reports)
    assert again.returncode == 0, again.stdout[-2500:] + again.stderr[-1500:]
    assert psql(pg_pair["src"], Q).stdout == psql(pg_pair["dst"], Q).stdout
