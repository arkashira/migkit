"""`doctor` tells a link that connects and never carries a large reply
apart from one that works.

Measured before (the incident this comes from, and here again): through a
path that drops packets larger than 1,200 bytes, the server's port
answered, signing in worked, `select 1` came back in milliseconds - and a
reply of 6.4 KB never arrived. Every check said the hop was fine, and the
first copy hung. The path is made by dropping the server's outgoing TCP
packets larger than 1,200 bytes in its own network namespace.
"""
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

ROOT = Path(__file__).resolve().parent.parent
PG, PORT = "migkit-test-mtu-pg", 15889


@pytest.fixture(scope="module")
def narrow_path():
    subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-p",
                    f"{PORT}:5432", "-e", "POSTGRES_PASSWORD=test",
                    "postgres:16"], check=True, capture_output=True)
    try:
        end = time.time() + 90
        while time.time() < end:
            if subprocess.run(["docker", "exec", PG, "pg_isready", "-U",
                               "postgres"], capture_output=True
                              ).returncode == 0:
                with socket.socket() as s:
                    if s.connect_ex(("127.0.0.1", PORT)) == 0:
                        break
            time.sleep(1)
        time.sleep(2)
        got = subprocess.run(
            ["docker", "run", "--rm", "--cap-add", "NET_ADMIN", "--net",
             f"container:{PG}", "alpine:3.20", "sh", "-c",
             "apk add -q --no-cache iptables >/dev/null 2>&1 && iptables -A"
             " OUTPUT -p tcp --sport 5432 -m length --length 1201:65535"
             " -j DROP"], capture_output=True, text=True)
        if got.returncode != 0:
            pytest.skip("the packet filter could not be set up here:"
                        f" {got.stderr[-200:]}")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", PG], capture_output=True)


def _eng(port):
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=port, user="postgres",
                  password="test")
    return PostgresEngine(Hop(name="l", engine="postgres", source=ep,
                              target=ep, databases=["postgres"]))


def test_a_clear_link_carries_every_size(pg_pair):
    got = _eng(pg_pair["dst"]).link_probe("dst", "postgres")
    assert "stalled" not in got, got
    assert got["replies"] == got["requests"] == 1 << 20, got
    assert got["rtt_ms"] < 1000 and got["mb_s"] > 0, got


def test_a_path_that_drops_large_packets_is_named(narrow_path):
    got = _eng(PORT).link_probe("dst", "postgres", wait=4)
    assert got.get("stalled", (None,))[0] == "replies", got
    # small replies came back; the first that fills a packet did not
    assert got["replies"] == 512 and got["stalled"][1] <= 2048, got


def test_doctor_says_so(narrow_path, tmp_path):
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  narrow:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {PORT}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {PORT}, user: postgres,"
        " password: test}\n    databases: [postgres]\n")
    env = dict(os.environ, MIGKIT_CONF=str(tmp_path / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp_path / "reports"), COLUMNS="300")
    got = subprocess.run([sys.executable, "-c",
                          "from migkit.cli import main; main()", "doctor"],
                         env=env, cwd=ROOT, capture_output=True, text=True,
                         timeout=300)
    said = got.stdout + got.stderr
    assert "link: STALLED - replies from the target of" in said, said[-2500:]
    assert "MTU" in said
