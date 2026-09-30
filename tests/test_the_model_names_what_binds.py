"""The resource model held to real servers: each resource throttled in turn
- the target's CPU and the source's (`docker update --cpus`), the link from
the source as a whole (a token bucket on its interface) and the link to the
target a connection at a time (Toxiproxy) - and the model built before the
move names the one that binds, and the rate it predicts is held to the rate
the move then measures.

Measured (PostgreSQL 16 pair on a docker machine of 2 CPUs, 1,500,000
rows, ~200 MB as carried), 2026-09-30: with nothing throttled and with the
target's CPU at 0.3 the model named what the test holds it to, within
`ERROR`; behind the token bucket the source read 7.1 MB/s one stream and
four, and was named where the link was (`model.LINKED` since); through
Toxiproxy the target wrote 2.9 MB/s a stream and read back 2.8, and the
read-back was named where the link was (the same, not yet run). Each run
prints its line (`RESULTS`).
"""
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

from tests.cap_pairs import live_eta, watched
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROOT = Path(__file__).resolve().parent.parent
SRC, DST = "migkit-test-pg-src", "migkit-test-pg-dst"
TC = "migkit-test-cap-tc"
PROXY, API, LISTEN = "migkit-test-cap-toxi", 16222, 16223
ROWS = 1_500_000
#: how far the rate the model predicts may be from the rate the move
#: measured, as a share of the measured one: probes of seconds against a
#: move of a minute on a machine both servers share
ERROR = 1.0


@pytest.fixture(scope="module")
def data(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop schema if exists cap cascade; create schema cap")
    got = psql(pg_pair["src"],
               "create table cap.t (id bigint primary key, a text, b int,"
               " c timestamptz); insert into cap.t select g, md5(g::text) ||"
               f" repeat('x', 60), g % 1000, now() from generate_series(1,"
               f" {ROWS}) g; analyze cap.t")
    assert got.returncode == 0, got.stderr
    yield pg_pair
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop schema if exists cap cascade")
    _unthrottle()


def _unthrottle():
    for name in (SRC, DST):
        subprocess.run(["docker", "update", "--cpus", "0", name],
                       capture_output=True)
    subprocess.run(["docker", "exec", TC, "tc", "qdisc", "del", "dev",
                    "eth0", "root"], capture_output=True)
    for name in (TC, PROXY):
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def _move(pg_pair, tmp, target_port=None):
    """The move, from an empty target table, its progress read while it
    runs: (the model it planned with, what it measured, what it printed,
    the time left it said halfway against the time it took)."""
    from migkit import model as M
    psql(pg_pair["dst"], "drop table if exists cap.t")
    subprocess.run(["rm", "-rf", str(tmp / "reports")])
    (tmp / "hops.yaml").write_text(
        "hops:\n  cap:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port:"
        f" {target_port or pg_pair['dst']},"
        " user: postgres, password: test}\n    databases: [postgres]\n")
    env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp / "reports"), MIGKIT_MOVER="builtin",
               MIGKIT_PROBE_FROM="0", COLUMNS="200")
    where = tmp / "reports" / "cap" / "postgres"
    code, out, samples, _ = watched(
        [sys.executable, "-c", "from migkit.cli import main; main()", "move",
         "cap", "--go"], env, where / "progress.json")
    assert code == 0, out[-4000:]
    m = M.load(where)
    done = json.loads((where / "progress.json").read_text())
    assert done["done"] and done["rows_done"] == ROWS, done
    q = "select count(*), sum(hashtext(a || b::text)) from cap.t"
    assert psql(pg_pair["src"], q).stdout == psql(pg_pair["dst"], q).stdout
    return m, done, out, live_eta(samples)


def _held(name, m, done, out, live):
    """The prediction against the measurement, said and returned."""
    n = done["started_at"]
    predicted = m.rows_rate(n)
    measured = done["rate"]
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


RESULTS = {}


def test_nothing_throttled(data, tmp_path):
    m, done, out, live = _move(data, tmp_path)
    assert _held("nothing throttled", m, done, out, live) <= ERROR


def test_the_targets_cpu_binds(data, tmp_path):
    subprocess.run(["docker", "update", "--cpus", "0.3", DST], check=True,
                   capture_output=True)
    try:
        m, done, out, live = _move(data, tmp_path)
    finally:
        _unthrottle()
    err = _held("the target's CPU at 0.3", m, done, out, live)
    assert m.binding().name == "target", m.lines()
    assert "binds: the target's writes" in out
    assert err <= ERROR


def test_the_sources_cpu_binds(data, tmp_path):
    subprocess.run(["docker", "update", "--cpus", "0.3", SRC], check=True,
                   capture_output=True)
    try:
        m, done, out, live = _move(data, tmp_path)
    finally:
        _unthrottle()
    err = _held("the source's CPU at 0.3", m, done, out, live)
    assert m.binding().name == "source", m.lines()
    assert "binds: the source's reads" in out
    assert err <= ERROR


def test_the_link_from_the_source_binds(data, tmp_path):
    """A token bucket on the source's interface: 64 Mbit/s for every
    connection together - a link no number of workers widens."""
    subprocess.run(["docker", "rm", "-f", TC], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", TC, "--net",
                    f"container:{SRC}", "--cap-add", "NET_ADMIN",
                    "alpine:3.20", "sh", "-c",
                    "apk add --no-cache iproute2-tc >/dev/null && tc qdisc"
                    " replace dev eth0 root tbf rate 64mbit burst 256kb"
                    " latency 200ms && sleep 3600"], check=True,
                   capture_output=True)
    try:
        for _ in range(60):
            got = subprocess.run(["docker", "exec", TC, "tc", "qdisc",
                                  "show", "dev", "eth0"],
                                 capture_output=True, text=True).stdout
            if "tbf" in got:
                break
            time.sleep(1)
        else:
            pytest.fail("the token bucket never came up")
        m, done, out, live = _move(data, tmp_path)
    finally:
        _unthrottle()
    err = _held("the source's link at 64 Mbit/s", m, done, out, live)
    assert m.binding().name == "link:src", m.lines()
    assert not m.binding().at_least
    assert err <= ERROR


def _api(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{API}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        text = r.read().decode()
        return json.loads(text) if text else None


def test_a_link_held_a_connection_at_a_time_takes_more_workers(data,
                                                              tmp_path):
    """Toxiproxy between migkit and the target, each connection held to
    3 MB/s: one stream is the link's, and the workers are as many as it
    takes to fill what binds behind it (Little's law)."""
    subprocess.run(["docker", "rm", "-f", PROXY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PROXY, "-p",
                    f"127.0.0.1:{API}:8474", "-p",
                    f"127.0.0.1:{LISTEN}:{LISTEN}",
                    "ghcr.io/shopify/toxiproxy:2.12.0"],
                   check=True, capture_output=True)
    try:
        ip = subprocess.run(["docker", "inspect", "-f",
                             "{{range .NetworkSettings.Networks}}"
                             "{{.IPAddress}}{{end}}", DST],
                            capture_output=True, text=True).stdout.strip()
        for _ in range(30):
            try:
                _api("POST", "/proxies", {"name": "target",
                                          "listen": f"0.0.0.0:{LISTEN}",
                                          "upstream": f"{ip}:5432"})
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(1)
        for stream in ("upstream", "downstream"):
            _api("POST", "/proxies/target/toxics",
                 {"name": f"slow_{stream}", "type": "bandwidth",
                  "stream": stream, "attributes": {"rate": 3000}})
        m, done, out, live = _move(data, tmp_path, target_port=LISTEN)
    finally:
        _unthrottle()
    err = _held("the target's link at 3 MB/s a connection", m, done,
                out, live)
    assert m.per_stream_binding().name == "link:dst", m.lines()
    assert m.stages["link:dst"].at_least
    assert m.little >= 3 and "each stream: held to" in out
    assert err <= ERROR
