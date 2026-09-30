"""The proof's sandbox started by migkit itself (`proof_sandbox: docker`):
servers of the engines and versions the hop's own run, on 127.0.0.1,
with passwords of their own, removed when the proof is done.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MY, PG = "migkit-test-code-my", "migkit-test-code-pg"
MY_PORT, PG_PORT = 16080, 16081
AUTO, AUTO_PORTS = "migkit-test-code-auto", [16084, 16085]


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    for n in (MY, PG, f"{AUTO}-src", f"{AUTO}-dst"):
        _sh("docker", "rm", "-f", "-v", n)
    _sh("docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
        "-p", f"127.0.0.1:{MY_PORT}:3306", "mysql:8.4",
        "--innodb-buffer-pool-size=16M", "--performance-schema=OFF")
    _sh("docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
        "-p", f"127.0.0.1:{PG_PORT}:5432", "postgres:16")
    try:
        for _ in range(90):
            if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
                   "-h127.0.0.1", "--protocol=tcp", "-e",
                   "create database if not exists st").returncode == 0:
                break
            time.sleep(2)
        for _ in range(60):
            if _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
                   "create database st").returncode == 0:
                break
            time.sleep(1)
        for port in (MY_PORT, PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        import pymysql
        conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                               password="test", database="st",
                               autocommit=True)
        with conn.cursor() as cur:
            cur.execute("create table counters (id int primary key, n int"
                        " not null)")
            cur.execute("insert into counters values (1, 0), (2, 5)")
            cur.execute("create procedure bump(in which int) begin update"
                        " counters set n = n + 1 where id = which; end")
        conn.close()
        made = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres",
                   "-d", "st", "-v", "ON_ERROR_STOP=1", input="""
            create table counters (id integer primary key, n integer
              not null);
            insert into counters values (1, 0), (2, 5);
            create procedure bump(which integer) language sql as
              $$ update counters set n = n + 2 where id = which $$;
            """)
        assert made.returncode == 0, made.stderr
        yield
    finally:
        for n in (MY, PG, f"{AUTO}-src", f"{AUTO}-dst"):
            _sh("docker", "rm", "-f", "-v", n)


def test_the_sandbox_is_started_used_and_removed(servers, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="st", engine="hetero",
              options={"source_engine": "mysql", "target_engine": "postgres",
                       "proof_sandbox": {"docker": {"name": AUTO,
                                                    "ports": AUTO_PORTS}}},
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["st"])
    hop.report_dir = lambda db=None: tmp_path
    got = HeteroEngine(hop).prove_converted("st")
    record = json.loads((tmp_path / "code-proof.json").read_text())
    assert record["sandbox"] == "used", record
    bump = {v["routine"]: v for v in record["verdicts"]}["procedure bump"]
    # the target's adds two where the source's adds one: run, and told
    assert bump["status"] == "diff", bump
    assert "rows changed in counters" in bump["reason"], bump
    assert got.status == "diff", got.__dict__
    # gone once the proof is done
    left = _sh("docker", "ps", "-a", "--format", "{{.Names}}").stdout.split()
    assert not [n for n in left if n.startswith(AUTO)], left
