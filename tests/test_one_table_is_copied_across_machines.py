"""One table copied by two machines at once, its ranges claimed from one
record in a directory both see - and a machine that dies mid-range has
its claim taken again once its term lapses (backlog W1, `claims`).

Two `migkit move --mode full --go` processes of a hop that says
`share_tables: ranges`, each with its own report directory. The first is
ended as kill -9 would end it the second time one of its ranges is
written, before the range is marked done; the second copies the rest,
takes the lapsed claim again under a higher number and finishes the
table. The target then holds the source's rows exactly, both machines are
named in the record as having copied ranges, and the lapsed claim is
kept there with the machine that held it. A PostgreSQL pair in
containers of its own.
"""
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

pytestmark = [pytest.mark.docker]

ROOT = Path(__file__).resolve().parent.parent
SRC, SRC_PORT = "migkit-test-w1-share-src", 16421
DST, DST_PORT = "migkit-test-w1-share-dst", 16423


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
                        "-p", f"127.0.0.1:{port}:5432", "postgres:16"],
                       check=True, capture_output=True)
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


SUM = ("select count(*) || ':' || coalesce(md5(string_agg(id || '|' ||"
       " payload, ',' order by id)), '') from {t}")


class Machine:
    def __init__(self, tmp, name, fail=None):
        env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
                   MIGKIT_REPORTS=str(tmp / name), MIGKIT_MOVER="builtin",
                   MIGKIT_STANDBY_SECONDS="4", PYTHONUNBUFFERED="1",
                   COLUMNS="200")
        env.pop("MIGKIT_FAILPOINT", None)
        if fail:
            env["MIGKIT_FAILPOINT"] = fail
        self.lines = []
        self.proc = subprocess.Popen(
            [sys.executable, "-c", "from migkit.cli import main; main()",
             "move", "share", "--mode", "full", "--go"], env=env, cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.proc.stdout:
            self.lines.append(line.rstrip())

    def said(self, words):
        return any(words in line for line in self.lines)

    def text(self):
        return "\n".join(self.lines[-80:])


def _until(what, timeout, *machines):
    end = time.time() + timeout
    while time.time() < end:
        if what():
            return
        time.sleep(0.5)
    pytest.fail("never happened; said:\n" + "\n----\n".join(
        m.text() for m in machines))


def test_a_table_is_shared_and_a_dead_machines_range_taken_again(pair,
                                                                 tmp_path):
    for name in (SRC, DST):
        psql(name, "drop table if exists big, small")
    psql(SRC, "create table big (id bigint primary key, payload text);"
              " insert into big select g, 'row-' || g from"
              " generate_series(1, 300000) g;"
              " create table small (id int primary key, payload text);"
              " insert into small select g, 'v' || g from"
              " generate_series(1, 100) g; analyze")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  share:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: postgres,"
        " password: test}\n"
        "    databases: [app]\n"
        "    options:\n"
        "      share_tables: ranges\n"
        f"      state: {{shared: {tmp_path / 'shared'}}}\n")
    # ended as kill -9 ends it, the second time one of its ranges is
    # written and before it is marked done
    a = Machine(tmp_path, "a", fail="range.committed:2:exit")
    _until(lambda: a.said("shared by every machine of the hop")
           or a.proc.poll() is not None, 240, a)
    b = Machine(tmp_path, "b")
    try:
        a.proc.wait(600)
        assert a.proc.returncode == 137, a.text()
        b.proc.wait(900)
        assert b.proc.returncode == 0, b.text()
    finally:
        for m in (a, b):
            if m.proc.poll() is None:
                m.proc.kill()
    for t in ("big", "small"):
        assert psql(SRC, SUM.format(t=t)) == psql(DST, SUM.format(t=t)), t
    # the record: who copied which range, and the claim that lapsed
    from migkit import claims, shared
    from migkit.config import Endpoint, Hop
    hop = Hop(name="share", engine="postgres", source=Endpoint(),
              target=Endpoint(), databases=["app"],
              options={"share_tables": "ranges",
                       "state": {"shared": str(tmp_path / "shared")}})
    root = tmp_path / "reader"

    def report_dir(db=None):
        d = root / (db or "")
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    store = shared.Directory(hop, tmp_path / "shared")
    rec = claims.Table(store, claims.path(hop, "app", "app.big"), "reader",
                       4).read()
    assert rec and rec["way"] == "ranges" and claims.left(rec) == 0, rec
    by = {st["pid"] for st in rec["state"].values()}
    assert {a.proc.pid, b.proc.pid} <= by, (by, a.proc.pid, b.proc.pid)
    assert any(str(a.proc.pid) in str(x.get("by")) for x in rec["lapsed"]), \
        rec["lapsed"]
    # every claim of a range had a number of its own, higher each time
    numbers = [st["number"] for st in rec["state"].values()]
    assert len(set(numbers)) == len(numbers)
    assert rec["finished"]["pid"] == b.proc.pid
    rows = claims.evidence(hop, "app", store)
    assert any(f"process {b.proc.pid}" in r["by"] for r in rows), rows
