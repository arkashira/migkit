"""The link to the target cut, reset or holding still in the middle of a
move: the move stops and says so, and run again the target equals the
source.

Measured before: a link that stayed open and forwarded nothing held the
table copier for good - no error, no timeout, the move never ending.
Nothing moved for `MIGKIT_STALL_SECONDS` now ends the copy (`migkit.stall`).
The faults are made by a proxy between migkit and the target (Toxiproxy,
driven over its HTTP API).
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROOT = Path(__file__).resolve().parent.parent
PROXY, API, LISTEN = "migkit-test-toxi", 15881, 15882


def _api(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{API}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        text = r.read().decode()
        return json.loads(text) if text else None


@pytest.fixture(scope="module")
def proxy(pg_pair):
    subprocess.run(["docker", "rm", "-f", PROXY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PROXY, "-p",
                    f"{API}:8474", "-p", f"{LISTEN}:{LISTEN}",
                    "ghcr.io/shopify/toxiproxy:2.12.0"],
                   check=True, capture_output=True)
    try:
        ip = subprocess.run(["docker", "inspect", "-f",
                             "{{range .NetworkSettings.Networks}}"
                             "{{.IPAddress}}{{end}}",
                             "migkit-test-pg-dst"],
                            capture_output=True, text=True).stdout.strip()
        for _ in range(30):
            try:
                _api("POST", "/proxies", {"name": "target",
                                          "listen": f"0.0.0.0:{LISTEN}",
                                          "upstream": f"{ip}:5432"})
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(1)
        else:
            pytest.fail("the proxy never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", PROXY], capture_output=True)


def _toxic(name, kind, stream="upstream", **attrs):
    _api("POST", "/proxies/target/toxics",
         {"name": name, "type": kind, "stream": stream, "attributes": attrs})


def _clear():
    for t in _api("GET", "/proxies/target/toxics") or []:
        _api("DELETE", f"/proxies/target/toxics/{t['name']}")


def _migkit(tmp, *args, **env):
    full = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
                MIGKIT_REPORTS=str(tmp / "reports"), MIGKIT_MOVER="builtin",
                COLUMNS="200", **env)
    try:
        return subprocess.run([sys.executable, "-c",
                               "from migkit.cli import main; main()", *args],
                              env=full, cwd=ROOT, capture_output=True,
                              text=True, timeout=float(os.environ.get(
                                  "LINK_TEST_TIMEOUT", "600")))
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else e.stdout
        pytest.fail(f"migkit {' '.join(args)} never ended:\n{out}")


Q = ("select count(*) || ':' || md5(string_agg(id || '|' || payload, ','"
     " order by id)) from public.big")


def _prepare(pg_pair, tmp, extra=""):
    if not psql(pg_pair["src"], "select to_regclass('public.big')"
                ).stdout.strip():
        got = psql(pg_pair["src"], "create table public.big (id bigint"
                                   " primary key, payload text); insert into"
                                   " public.big select g, repeat('x', 200) ||"
                                   " g from generate_series(1, 150000) g;"
                                   " analyze")
        assert got.returncode == 0, got.stderr
    psql(pg_pair["dst"], "drop table if exists public.big")
    subprocess.run(["rm", "-rf", str(tmp / "reports")])
    (tmp / "hops.yaml").write_text(
        "hops:\n  link:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {LISTEN}, user: postgres,"
        " password: test}\n    databases: [postgres]\n" + extra)


def _equal(pg_pair):
    return psql(pg_pair["src"], Q).stdout == psql(pg_pair["dst"], Q).stdout


@pytest.mark.parametrize("fault", ["cut", "reset"])
def test_a_link_cut_or_reset_mid_copy_is_survived(pg_pair, proxy, tmp_path,
                                                  fault):
    _prepare(pg_pair, tmp_path)
    _clear()
    # the table made and a range copied first, through the clean link
    made = _migkit(tmp_path, "move", "link", "--go", "--chunk", "20000",
                   MIGKIT_FAILPOINT="range.committed:1:exit")
    assert made.returncode != 0
    if fault == "cut":
        # every connection closed once 3 MB have gone towards the target
        _toxic("cut", "limit_data", bytes=3_000_000)
    else:
        _toxic("reset", "reset_peer", timeout=1500)
    first = _migkit(tmp_path, "move", "link", "--go", "--chunk", "20000")
    assert first.returncode != 0, first.stdout[-1500:]
    _clear()
    again = _migkit(tmp_path, "move", "link", "--go", "--chunk", "20000")
    assert again.returncode == 0, again.stdout[-2500:] + again.stderr[-1500:]
    assert _equal(pg_pair)


def test_a_link_that_holds_and_passes_nothing_is_given_up_on(pg_pair, proxy,
                                                            tmp_path):
    # one range at a time: the one held is the one in flight, and no other
    # starts a connection that the held link would refuse first
    _prepare(pg_pair, tmp_path, "    workers: 1\n")
    _clear()
    held = {}

    def hold_once_the_copy_runs():
        end = time.time() + 120
        while time.time() < end:
            got = subprocess.run(
                ["docker", "exec", "migkit-test-pg-dst", "psql", "-U",
                 "postgres", "-tAc", "select coalesce(max(tuples_processed),"
                 " 0) from pg_stat_progress_copy"],
                capture_output=True, text=True).stdout.strip()
            if int(got or 0) > 0:
                # data dropped both ways from now on, the connections open
                _toxic("hold_up", "timeout", timeout=0)
                _toxic("hold_down", "timeout", stream="downstream", timeout=0)
                held["at"] = time.time()
                return
            time.sleep(0.05)
    threading.Thread(target=hold_once_the_copy_runs, daemon=True).start()
    began = time.time()
    # ranges large enough to be caught while one is being copied
    first = _migkit(tmp_path, "move", "link", "--go", "--chunk", "150000",
                    MIGKIT_STALL_SECONDS="6")
    took = time.time() - began
    assert held, "the copy finished before the link could be held"
    assert first.returncode != 0
    assert "nothing moved for 6s" in first.stdout + first.stderr, \
        first.stdout[-2000:] + first.stderr[-1000:]
    assert took < 120, took
    _clear()
    again = _migkit(tmp_path, "move", "link", "--go", "--chunk", "20000")
    assert again.returncode == 0, again.stdout[-2500:] + again.stderr[-1500:]
    assert _equal(pg_pair)


def test_a_target_that_refuses_the_copy_ends_it_at_once(pg_pair, proxy,
                                                       tmp_path):
    """The target refusing the copy - here the table is not there - left
    the source's side of the pipe running with nowhere to write, and the
    move waited on it for good."""
    _prepare(pg_pair, tmp_path)
    _clear()
    began = time.time()
    got = _migkit(tmp_path, "move", "link", "--go", "--table", "public.big",
                  "--chunk", "20000")
    assert got.returncode != 0 and time.time() - began < 60
    assert 'relation "public.big" does not exist' in got.stdout + got.stderr
