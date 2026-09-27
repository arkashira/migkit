"""The source failing over in the middle of a move: its standby promoted
and the address the hop uses sent to it, as a managed service's endpoint
is. The move stops on the connections it lost, and run again it reads the
new primary and ends with the target equal to it.
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
STANDBY, PROXY, API, LISTEN = ("migkit-test-standby", "migkit-test-toxi-src",
                               15884, 15885)


def _ip(name):
    return subprocess.run(["docker", "inspect", "-f",
                           "{{range .NetworkSettings.Networks}}"
                           "{{.IPAddress}}{{end}}", name],
                          capture_output=True, text=True).stdout.strip()


def _api(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{API}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        text = r.read().decode()
        return json.loads(text) if text else None


def _standby_sql(sql):
    return subprocess.run(["docker", "exec", "-i", STANDBY, "psql", "-U",
                           "postgres", "-tA"], input=sql,
                          capture_output=True, text=True)


@pytest.fixture
def standby(pg_pair):
    got = psql(pg_pair["src"], """
        drop table if exists public.big;
        create table public.big (id bigint primary key, payload text);
        insert into public.big select g, repeat('x', 200) || g
          from generate_series(1, 150000) g;
        analyze""")
    assert got.returncode == 0, got.stderr
    # the primary takes a copy for its standby from any address
    subprocess.run(["docker", "exec", "migkit-test-pg-src", "sh", "-c",
                    "echo 'host replication all all scram-sha-256' >>"
                    " /var/lib/postgresql/data/pg_hba.conf"], check=True)
    psql(pg_pair["src"], "select pg_reload_conf()")
    primary = _ip("migkit-test-pg-src")
    for name in (STANDBY, PROXY):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--name", STANDBY, "-e",
         "PGPASSWORD=test", "--entrypoint", "sh", "postgres:16", "-c",
         f"mkdir -p /var/lib/postgresql/data && chown postgres"
         f" /var/lib/postgresql/data && chmod 700 /var/lib/postgresql/data"
         f" && su postgres -c 'pg_basebackup -h {primary} -U postgres -D"
         f" /var/lib/postgresql/data -R -X stream --checkpoint=fast'"
         f" && exec su postgres -c"
         f" 'postgres -D /var/lib/postgresql/data'"],
        check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PROXY, "-p",
                    f"{API}:8474", "-p", f"{LISTEN}:{LISTEN}",
                    "ghcr.io/shopify/toxiproxy:2.12.0"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 120
        while time.time() < end:
            ok = _standby_sql("select pg_is_in_recovery()").stdout.strip()
            if ok == "t":
                break
            time.sleep(1)
        else:
            logs = subprocess.run(["docker", "logs", STANDBY],
                                  capture_output=True, text=True)
            state = subprocess.run(["docker", "ps", "-a", "--filter",
                                    f"name={STANDBY}", "--format",
                                    "{{.Status}}"], capture_output=True,
                                   text=True).stdout
            pytest.fail(f"the standby never came up ({state.strip()}):\n"
                        + (logs.stdout + logs.stderr)[-1500:]
                        + _standby_sql("select 1").stderr)
        for _ in range(30):
            try:
                _api("POST", "/proxies", {"name": "source",
                                          "listen": f"0.0.0.0:{LISTEN}",
                                          "upstream": f"{primary}:5432"})
                break
            except Exception:  # noqa: BLE001 - not up yet
                time.sleep(1)
        # the standby has every row before the move starts
        end = time.time() + 60
        while time.time() < end:
            if _standby_sql("select count(*) from public.big"
                            ).stdout.strip() == "150000":
                break
            time.sleep(1)
        yield
    finally:
        for name in (STANDBY, PROXY):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


def _migkit(tmp, *args):
    env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp / "reports"), MIGKIT_MOVER="builtin",
               COLUMNS="200")
    return subprocess.run([sys.executable, "-c",
                           "from migkit.cli import main; main()", *args],
                          env=env, cwd=ROOT, capture_output=True, text=True,
                          timeout=600)


Q = ("select count(*) || ':' || md5(string_agg(id || '|' || payload, ','"
     " order by id)) from public.big")


def test_a_move_whose_source_fails_over_ends_equal_to_the_new_primary(
        pg_pair, standby, tmp_path):
    psql(pg_pair["dst"], "drop table if exists public.big")
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  over:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {LISTEN}, user: postgres,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n    workers: 1\n")
    failed_over = {}

    def fail_over_once_a_range_is_in():
        end = time.time() + 120
        while time.time() < end:
            got = psql(pg_pair["dst"], "select coalesce((select count(*)"
                                       " from public.big), 0)").stdout.strip()
            if got.isdigit() and int(got) > 0:
                # the endpoint sent to the standby - the proxy drops what
                # it had open - and the standby promoted
                _api("POST", "/proxies/source",
                     {"upstream": f"{_ip(STANDBY)}:5432"})
                _standby_sql("select pg_promote()")
                failed_over["at"] = time.time()
                return
            time.sleep(0.05)
    threading.Thread(target=fail_over_once_a_range_is_in,
                     daemon=True).start()
    first = _migkit(tmp_path, "move", "over", "--go", "--chunk", "20000")
    assert failed_over, "the move ended before the source could fail over"
    # the new primary goes on taking writes the old one never saw
    end = time.time() + 30
    while _standby_sql("select pg_is_in_recovery()").stdout.strip() != "f":
        assert time.time() < end, "the standby was never promoted"
        time.sleep(0.5)
    got = _standby_sql("insert into public.big values (150001, 'after the"
                       " failover'); update public.big set payload ="
                       " 'changed' where id = 7")
    assert got.returncode == 0, got.stderr
    again = _migkit(tmp_path, "move", "over", "--go", "--chunk", "20000")
    said = first.stdout + first.stderr + again.stdout + again.stderr
    assert again.returncode == 0, said[-3000:]
    assert _standby_sql(Q).stdout.strip() == \
        psql(pg_pair["dst"], Q).stdout.strip(), said[-3000:]
