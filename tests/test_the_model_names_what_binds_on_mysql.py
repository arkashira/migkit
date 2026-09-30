"""The resource model on a MySQL pair: the target's CPU throttled and then
the source's (`docker update --cpus`), and the model built before the move
names the one that binds, its predicted rate held to the move's.

Measured (MySQL 8.4 pair, 600,000 rows), 2026-09-30: with the target's CPU
at 0.3 the target was named; with the source's, the target was named too.
The probes run as threads of this process, where the driver decodes each
row under one interpreter lock, and the copier's workers are processes of
their own: a probe held by this process's one core is told apart since
(`model.ONE_CORE`), not yet run. Each run prints its line (`RESULTS`).
"""
import json
import os
import subprocess
import sys

import pytest

from tests import cap_pairs
from tests.cap_pairs import MY_DST, MY_SRC, my

pytestmark = pytest.mark.docker

ROWS = 600_000
#: how far the predicted rate may be from the measured one, as a share of
#: the measured
ERROR = 1.0
RESULTS = {}


@pytest.fixture(scope="module")
def pair():
    yield from cap_pairs.my_pair()


@pytest.fixture(scope="module")
def data(pair):
    my(MY_SRC, "create database cap; use cap; create table t (id bigint"
               " primary key, a varchar(100), b int, c datetime);"
               " set session cte_max_recursion_depth = 10000000;"
               " insert into t with recursive g(n) as (select 1 union all"
               f" select n + 1 from g where n < {ROWS}) select n,"
               " concat(md5(n), repeat('x', 60)), n % 1000, now() from g;"
               " analyze table t;")
    my(MY_DST, "create database cap")
    return pair


def _unthrottle():
    for name in (MY_SRC, MY_DST):
        subprocess.run(["docker", "update", "--cpus", "0", name],
                       capture_output=True)


def _move(pair, tmp):
    from migkit import model as M
    my(MY_DST, "drop table if exists cap.t")
    subprocess.run(["rm", "-rf", str(tmp / "reports")])
    (tmp / "hops.yaml").write_text(
        "hops:\n  cap:\n    engine: mysql\n"
        f"    source: {{host: 127.0.0.1, port: {pair['src']}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pair['dst']}, user: root,"
        " password: test}\n    databases: [cap]\n")
    env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp / "reports"), MIGKIT_MOVER="builtin",
               MIGKIT_PROBE_FROM="0", COLUMNS="200")
    where = tmp / "reports" / "cap" / "cap"
    code, out, samples, _ = cap_pairs.watched(
        [sys.executable, "-c", "from migkit.cli import main; main()", "move",
         "cap", "--go"], env, where / "progress.json")
    assert code == 0, out[-4000:]
    m = M.load(where)
    done = json.loads((where / "progress.json").read_text())
    assert done["done"] and done["rows_done"] == ROWS, done
    q = "select count(*), sum(crc32(concat(id, a, b))) from cap.t"
    assert my(MY_SRC, q) == my(MY_DST, q)
    return m, done, out, cap_pairs.live_eta(samples)


def _held(name, m, done, out, live):
    n = done["started_at"]
    predicted, measured = m.rows_rate(n), done["rate"]
    copy = next(p for p in m.planned if p.name == "copy")
    err = abs(predicted - measured) / measured
    line = (f"{name}: binds {m.binding().name}"
            f" (one stream held by {m.per_stream_binding().name}),"
            f" {n} at a time; predicted {predicted:,.0f} rows/s, measured"
            f" {measured:,.0f} ({err:.0%} off); copy planned"
            f" {copy.p50:.1f}s, took {done['seconds']:.1f}s"
            + (f"; halfway it said {live[0]:.1f}s left and took"
               f" {live[1]:.1f}s ({live[2]:.0%} off)" if live else ""))
    print("\n" + line + "\n" + "\n".join(
        l for l in out.splitlines() if l.strip().startswith(
            ("resources:", "binds:", "each stream", "unbound", "memory:",
             "at a time"))))
    RESULTS[name] = line
    return err


def test_nothing_throttled(data, tmp_path):
    m, done, out, live = _move(data, tmp_path)
    assert _held("nothing throttled", m, done, out, live) <= ERROR


def test_the_targets_cpu_binds(data, tmp_path):
    subprocess.run(["docker", "update", "--cpus", "0.3", MY_DST],
                   check=True, capture_output=True)
    try:
        m, done, out, live = _move(data, tmp_path)
    finally:
        _unthrottle()
    err = _held("the target's CPU at 0.3", m, done, out, live)
    assert m.binding().name == "target", m.lines()
    assert err <= ERROR


def test_the_sources_cpu_binds(data, tmp_path):
    subprocess.run(["docker", "update", "--cpus", "0.3", MY_SRC],
                   check=True, capture_output=True)
    try:
        m, done, out, live = _move(data, tmp_path)
    finally:
        _unthrottle()
    err = _held("the source's CPU at 0.3", m, done, out, live)
    assert m.binding().name == "source", m.lines()
    assert err <= ERROR
