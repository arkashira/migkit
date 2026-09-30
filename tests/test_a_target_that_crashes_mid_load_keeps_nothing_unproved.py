"""A load whose commits do not wait for the target's disk
(`synchronous_commit = off`, on its own sessions only) is safe because a
range is recorded only once the target says its log is on disk past the
range's commit (`pg_pass.TableCopy.durable`).

The target is killed the moment a range's commit has returned and before
its server has flushed it - `wal_writer_delay` is set long here so the
moment is not a race. After the restart that range's rows are gone
although its commit returned; it was never recorded, and neither was
anything the target did not keep; the check says the target differs; and
the next run copies what is missing and the check says equal.
"""
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import _wait_tcp, needs_docker

pytestmark = needs_docker

NAMES = {"src": "migkit-test-pass-crash-src",
         "dst": "migkit-test-pass-crash-dst"}
PORTS = {"src": 16182, "dst": 16183}


def _sql(side, sql):
    return subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test",
                           NAMES[side], "psql", "-U", "postgres", "-At",
                           "-c", sql], capture_output=True, text=True)


def _ready(side):
    end = time.time() + 90
    while time.time() < end:
        if _sql(side, "select 1").stdout.strip() == "1":
            return
        time.sleep(1)
    raise AssertionError(f"{side} did not come back")


@pytest.fixture(scope="module")
def pair():
    for side, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p",
                        f"127.0.0.1:{PORTS[side]}:5432", "postgres:16",
                        "-c", "wal_writer_delay=10000"],
                       check=True, capture_output=True)
    for side in NAMES:
        assert _wait_tcp("127.0.0.1", PORTS[side])
        _ready(side)
    yield PORTS
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def test_a_range_the_target_lost_was_never_recorded(pair, tmp_path,
                                                    monkeypatch):
    from migkit import ranges
    from migkit.cli import _Checkpoint
    from migkit.engines import pg_pass
    from migkit.engines.postgres import PostgresEngine
    shape = "(id bigint primary key, payload text, n numeric)"
    got = _sql("src", f"create table public.big {shape}; insert into"
                      " public.big select g, repeat('p', 60) || g, g / 3.0"
                      " from generate_series(1, 60000) g; analyze")
    assert got.returncode == 0, got.stderr
    assert _sql("dst", f"create table public.big {shape}").returncode == 0
    hop = Hop(name="crash", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    monkeypatch.setattr(ranges, "active", ranges.Slots(1))
    # one worker would halve the table; six ranges of it
    monkeypatch.setattr(ranges, "step", lambda rows, chunk, workers: 10_000)
    monkeypatch.setattr(PostgresEngine, "COPY_WAYS", tuple(
        r for r in PostgresEngine.COPY_WAYS
        if r.name == "pass-through unawaited"))
    real, copied = pg_pass.TableCopy.copy, []
    recorded, finished = [], ranges.finished

    def record(st, after, save):
        finished(st, after, save)
        recorded.append(after)
    monkeypatch.setattr(ranges, "finished", record)

    def copy(self, export, where, *a, **k):
        if len(copied) == 2:
            # the two before it proved and flushed first, so nothing of
            # theirs flushes this one's commit on its way
            end = time.time() + 60
            while len(recorded) < 2 and time.time() < end:
                time.sleep(0.05)
        rows, lsn = real(self, export, where, *a, **k)
        copied.append((where, rows))
        if len(copied) == 3:
            # its commit has returned; the server has not flushed it
            subprocess.run(["docker", "kill", "-s", "KILL", NAMES["dst"]],
                           check=True, capture_output=True)
        return rows, lsn
    monkeypatch.setattr(pg_pass.TableCopy, "copy", copy)
    eng = PostgresEngine(hop)
    with pytest.raises(Exception):
        eng.move_table("postgres", "public", "big", 500_000,
                       _Checkpoint(tmp_path / "move.json"), [].append)
    subprocess.run(["docker", "start", NAMES["dst"]], check=True,
                   capture_output=True)
    _ready("dst")
    st = _Checkpoint(tmp_path / "move.json")["public.big"]
    assert not st.get("done")
    assert len(copied) == 3 and len(st["ranges_done"]) == 2, (copied, st)
    bounds = dict((a, u) for a, u in st["ranges"])

    def count(side, where):
        return int(_sql(side, "select count(*) from public.big where"
                              f" {where}").stdout)
    # every range recorded is on the target's disk, whole
    for after in st["ranges_done"]:
        where = f'"id" > {after} and "id" <= {bounds[after]}'
        assert count("dst", where) == count("src", where), after
    # the range the kill caught had committed, and the target lost it
    where, rows = copied[2]
    assert rows == count("src", where) > 0
    assert count("dst", where) == 0, where
    lost = int(where.split(">")[1].split()[0])
    assert lost not in st["ranges_done"], st
    # the check says the target differs; the next run makes it equal
    monkeypatch.setattr(pg_pass.TableCopy, "copy", real)
    eng = PostgresEngine(hop)

    def data():
        return [(r.status, r.detail) for r in eng.check_data("postgres")
                if r.check == "data"]
    said = data()
    assert [s for s, _ in said] == ["diff"], said
    assert "public.big" in said[0][1], said
    eng.move_table("postgres", "public", "big", 500_000,
                   _Checkpoint(tmp_path / "move.json"), [].append)
    st = _Checkpoint(tmp_path / "move.json")["public.big"]
    assert st["done"]
    said = data()
    assert [s for s, _ in said] == ["ok"], said
