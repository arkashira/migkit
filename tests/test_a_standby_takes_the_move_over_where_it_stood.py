"""A standby takes a hop's move over where it stood, and the machine that
lost it writes nothing more (backlog W1, `ha`).

Two machines run the same `migkit move --mode full+cdc --go` of a hop that
says `standby: true`, each its own process with its own report directory,
agreeing through a directory both see. One copies and follows the source;
the other stands by. Then the one moving it is

* killed as kill -9 would: the standby takes it over within the term and
  goes on from the last position committed - every row arrives, and the
  target's own count of the batches it committed (`migkit_origin`) rose
  by exactly as many as the batches were numbered: none applied twice;
* frozen (SIGSTOP) past its term and woken again: the standby took over
  while it slept, and the one that woke stands by instead of writing.

After either, the ledger of holdings and the target's own marks prove one
writer at a time (`ha.one_writer`). A PostgreSQL pair in containers of
its own.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

pytestmark = [pytest.mark.docker]

ROOT = Path(__file__).resolve().parent.parent
SRC, SRC_PORT = "migkit-test-w1-ha-src", 16427
DST, DST_PORT = "migkit-test-w1-ha-dst", 16428
TERM = "4"


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def psql(name, sql):
    got = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                          "psql", "-U", "postgres", "-d", "app", "-At",
                          "-c", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    for name in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-e", "POSTGRES_DB=app",
                        "-p", f"127.0.0.1:{port}:5432", "postgres:16", "-c",
                        "wal_level=logical"], check=True,
                       capture_output=True)
    try:
        end = time.time() + 120
        while time.time() < end:
            ready = all(subprocess.run(
                ["docker", "exec", n, "pg_isready", "-U", "postgres", "-h",
                 "127.0.0.1"], capture_output=True).returncode == 0
                for n in (SRC, DST))
            with socket.socket() as a, socket.socket() as b:
                a.settimeout(1)
                b.settimeout(1)
                up = (a.connect_ex(("127.0.0.1", SRC_PORT)) == 0
                      and b.connect_ex(("127.0.0.1", DST_PORT)) == 0)
            if ready and up:
                break
            time.sleep(2)
        else:
            pytest.fail("the pair never answered")
        time.sleep(2)
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _hop_file(tmp, name):
    (tmp / "hops.yaml").write_text(
        f"hops:\n  {name}:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: postgres,"
        " password: test}\n"
        "    databases: [app]\n"
        "    options:\n"
        "      standby: true\n"
        f"      state: {{shared: {tmp / 'shared'}}}\n")


class Machine:
    """One machine: a `migkit move` process of its own, what it says kept
    as it says it."""

    def __init__(self, tmp, name, hop):
        env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
                   MIGKIT_REPORTS=str(tmp / name),
                   MIGKIT_STANDBY_SECONDS=TERM, MIGKIT_MOVER="builtin",
                   PYTHONUNBUFFERED="1", COLUMNS="200")
        env.pop("MIGKIT_FAILPOINT", None)
        self.lines = []
        self.proc = subprocess.Popen(
            [sys.executable, "-c", "from migkit.cli import main; main()",
             "move", hop, "--mode", "full+cdc", "--db", "app", "--go"],
            env=env, cwd=ROOT, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True)
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.proc.stdout:
            self.lines.append(line.rstrip())

    def said(self, words):
        return any(words in line for line in self.lines)

    def text(self):
        return "\n".join(self.lines[-80:])

    def stop(self):
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(30)


def _until(what, timeout, *machines):
    end = time.time() + timeout
    while time.time() < end:
        if what():
            return
        time.sleep(0.5)
    pytest.fail("never happened; said:\n" + "\n----\n".join(
        m.text() for m in machines))


SUM = ("select count(*) || ':' || coalesce(md5(string_agg(id || '|' || n,"
       " ',' order by id)), '') from {t}")


def _mark(hop):
    got = psql(DST, "select n || chr(31) || coalesce(seen, '') from"
                    f" migkit_origin where origin = '{hop}'")
    n, seen = got.split("\x1f")
    return int(n), json.loads(seen)


def _insert(table, first, last, each=10):
    for lo in range(first, last + 1, each):
        hi = min(lo + each - 1, last)
        psql(SRC, f"insert into {table} select g, g from"
                  f" generate_series({lo}, {hi}) g")
        time.sleep(0.05)


def _prove(tmp, name):
    from migkit import ha, shared
    from migkit.config import Endpoint, Hop
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              user="postgres", password="test"),
              databases=["app"],
              options={"standby": True,
                       "state": {"shared": str(tmp / "shared")}})
    root = tmp / "reader"

    def report_dir(db=None):
        d = root / (db or "")
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    shared.forget()
    try:
        return ha.one_writer(hop, "app")
    finally:
        shared.forget()


def _fresh(table):
    for name in (SRC, DST):
        psql(name, f"drop table if exists {table}")
    psql(SRC, f"create table {table} (id int primary key, n int);"
              f" insert into {table} select g, g from"
              " generate_series(1, 500) g")


def _drop_slots():
    psql(SRC, "select pg_drop_replication_slot(slot_name) from"
              " pg_replication_slots where not active")


def test_a_machine_killed_is_taken_over_from_the_last_batch(pair,
                                                           tmp_path):
    hop, table = "hakill", "tk"
    _fresh(table)
    _hop_file(tmp_path, hop)
    a = Machine(tmp_path, "a", hop)
    b = None
    try:
        _until(lambda: a.said("tailing") and psql(
            DST, f"select to_regclass('{table}') is not null") == "t"
            and psql(DST, f"select count(*) from {table}") == "500", 240, a)
        _until(lambda: a.said("batches are numbered on the target"), 60, a)
        b = Machine(tmp_path, "b", hop)
        _until(lambda: b.said("standing by"), 120, a, b)
        # a first batch, whose mark the count is taken from: the proof of
        # the numbered rung left one of its own that is no batch
        _insert(table, 501, 510)
        _until(lambda: psql(DST, f"select count(*) from {table}") == "510",
               60, a, b)
        n0, seen0 = _mark(hop)
        assert int(seen0["batch"]) > 0, seen0
        _insert(table, 511, 700)
        _until(lambda: psql(DST, f"select count(*) from {table}") == "700",
               60, a, b)
        # killed as kill -9 kills, while the source goes on changing
        a.proc.send_signal(signal.SIGKILL)
        a.proc.wait(30)
        _insert(table, 701, 1000)
        _until(lambda: b.said(f"took {hop} over"), 120, a, b)
        _until(lambda: psql(SRC, SUM.format(t=table))
               == psql(DST, SUM.format(t=table)), 120, a, b)
        # settled: the last batch is marked before it is compared
        time.sleep(3)
        n1, seen1 = _mark(hop)
        assert psql(SRC, SUM.format(t=table)) == psql(DST,
                                                      SUM.format(t=table))
        # each batch the target committed carried its own number once
        assert n1 - n0 == int(seen1["batch"]) - int(seen0["batch"]), (
            n0, seen0, n1, seen1, b.text())
        # and the last one was committed under the standby's number
        got = _prove(tmp_path, hop)
        assert got["status"] == "one writer at a time", got
        fences = [e["fence"] for e in got["epochs"]]
        assert len(fences) == 2 and fences[0] < fences[1], got
        assert seen1.get("fence") == fences[1], (seen1, fences)
    finally:
        a.stop()
        if b is not None:
            b.stop()
        time.sleep(1)
        _drop_slots()


def test_a_machine_frozen_past_its_term_stands_by_when_it_wakes(pair,
                                                               tmp_path):
    hop, table = "hafreeze", "tf"
    _fresh(table)
    _hop_file(tmp_path, hop)
    a = Machine(tmp_path, "a", hop)
    b = None
    try:
        _until(lambda: a.said("tailing") and psql(
            DST, f"select to_regclass('{table}') is not null") == "t"
            and psql(DST, f"select count(*) from {table}") == "500", 240, a)
        b = Machine(tmp_path, "b", hop)
        _until(lambda: b.said("standing by"), 120, a, b)
        _insert(table, 501, 600)
        _until(lambda: psql(DST, f"select count(*) from {table}") == "600",
               60, a, b)
        time.sleep(2)
        # frozen between batches, holding nothing on the target
        a.proc.send_signal(signal.SIGSTOP)
        _until(lambda: b.said(f"took {hop} over"), 120, a, b)
        _insert(table, 601, 800)
        _until(lambda: psql(DST, f"select count(*) from {table}") == "800",
               60, a, b)
        a.proc.send_signal(signal.SIGCONT)
        # woken: it finds it lost the hop and stands by, writing nothing
        _until(lambda: a.said(f"no longer holds {hop}"), 60, a, b)
        _insert(table, 801, 900)
        _until(lambda: psql(SRC, SUM.format(t=table))
               == psql(DST, SUM.format(t=table)), 120, a, b)
        assert a.proc.poll() is None, a.text()
        got = _prove(tmp_path, hop)
        assert got["status"] == "one writer at a time", got
        assert len(got["epochs"]) == 2, got
    finally:
        if a.proc.poll() is None:
            a.proc.send_signal(signal.SIGCONT)
        a.stop()
        if b is not None:
            b.stop()
        time.sleep(1)
        _drop_slots()
