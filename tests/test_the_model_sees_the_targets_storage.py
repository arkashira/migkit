"""The target's storage held to a few megabytes a second (`--device-write-
bps` on its disk): the probes before the move write into a transaction
that is never committed, so nothing they write is flushed and the limit is
not in what they measure - and the move sees it while it runs: its
sessions waiting on storage, the target's own limit cut and named as what
binds, and the time left worked out from the recent rate.

Measured (PostgreSQL 16 pair, 1,000,000 rows, the target's disk at
6 MB/s), 2026-09-30: the probes named the target's writes, at 3,911 rows a
second where the move made 23,506; halfway the move said 25.8s were left
and took 21.2s (22% off); and no session was seen waiting on storage while
it ran. Changed since, not yet run: the upserts measured last on a stream
of their own, so what they leave for the disk is not waited on by the
writes measured before them, and the server's own writers counted among
what waits on storage (`PostgresEngine.WRITERS`) - a session whose commit
does not wait for the disk leaves that wait to them. The test prints what
the target's sessions waited on.
"""
import json
import os
import subprocess
import sys
import threading

import pytest

from tests import cap_pairs
from tests.cap_pairs import PG_DST, PG_SRC, pg

pytestmark = pytest.mark.docker

ROWS = 1_000_000
#: how far the time left said halfway may be from the time the copy then
#: took, as a share of the latter
LIVE_ERROR = 1.0
RESULTS = {}


def _disks():
    """The docker machine's whole disks, as a container sees them."""
    got = subprocess.run(["docker", "run", "--rm", "alpine:3.20", "cat",
                          "/proc/partitions"], capture_output=True, text=True)
    names = [line.split()[-1] for line in got.stdout.splitlines()[2:]
             if line.split()]
    return [f"/dev/{n}" for n in names
            if n.startswith(("vd", "sd", "xvd")) and not n[-1].isdigit()]


@pytest.fixture(scope="module")
def pair():
    args = []
    for dev in _disks():
        args += ["--device-write-bps", f"{dev}:6mb"]
    assert args, "no disk to hold to a rate"
    yield from cap_pairs.pg_pair(dst_args=args)


def test_the_targets_storage_is_seen_while_the_move_runs(pair, tmp_path):
    from migkit import model as M
    pg(PG_SRC, "create table t (id bigint primary key, a text, b int);"
               " insert into t select g, md5(g::text) || repeat('x', 60),"
               f" g % 1000 from generate_series(1, {ROWS}) g; analyze t")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  cap:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pair['dst']},"
        " user: postgres, password: test}\n    databases: [postgres]\n")
    env = dict(os.environ, MIGKIT_CONF=str(tmp_path / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp_path / "reports"),
               MIGKIT_MOVER="builtin", MIGKIT_PROBE_FROM="0", COLUMNS="200")
    where = tmp_path / "reports" / "cap" / "postgres"
    waits, stop = {}, threading.Event()

    def sample():
        # what the target's sessions wait on while it is written
        while not stop.wait(0.5):
            got = subprocess.run(
                ["docker", "exec", PG_DST, "psql", "-U", "postgres", "-At",
                 "-c", "select backend_type || ':' || coalesce("
                       "wait_event_type, 'CPU') from pg_stat_activity"
                       " where state is distinct from 'idle'"],
                capture_output=True, text=True).stdout.split()
            for w in got:
                waits[w] = waits.get(w, 0) + 1
    watcher = threading.Thread(target=sample, daemon=True)
    watcher.start()
    code, out, samples, _ = cap_pairs.watched(
        [sys.executable, "-c", "from migkit.cli import main; main()", "move",
         "cap", "--go"], env, where / "progress.json")
    stop.set()
    watcher.join(5)
    print("\nwhat the target's sessions waited on:",
          sorted(waits.items(), key=lambda kv: -kv[1]))
    assert code == 0, out[-4000:]
    q = "select count(*), sum(hashtext(a || b::text)) from t"
    assert pg(PG_SRC, q) == pg(PG_DST, q)
    m = M.load(where)
    done = json.loads((where / "progress.json").read_text())
    live = [p for _, p in samples
            if p.get("binding_truth") == "measured while it runs"]
    eta = cap_pairs.live_eta(samples)
    n = done["started_at"]
    line = (f"storage at 6 MB/s: the probes named {m.binding().name},"
            f" predicted {m.rows_rate(n):,.0f} rows/s; measured"
            f" {done['rate']:,.0f}; while it ran: "
            + (", ".join(sorted({p['held_by'] for p in live}))
               or "nothing held it")
            + (f"; halfway it said {eta[0]:.1f}s left and took"
               f" {eta[1]:.1f}s ({eta[2]:.0%} off)" if eta else ""))
    RESULTS["storage"] = line
    print("\n" + line + "\n" + "\n".join(
        l for l in out.splitlines() if "at a time" in l or "binds" in l))
    assert any(p["binding"] == "the target's writes"
               and "waiting on storage" in p["held_by"] for p in live), line
    assert "held back by the target's writes" in out
    assert eta is not None and eta[2] <= LIVE_ERROR, line
