"""PostgreSQL's stored code held to MySQL's versions of it, the other way
round from the rest of R11's tests: the proof is one machine for both
directions - PL/pgSQL run in the source's sandbox, the MySQL versions run
(and probed for coverage) in the target's.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

PG, MY = "migkit-test-code-pg", "migkit-test-code-my"
SB_PG, SB_MY = "migkit-test-code-sbpg", "migkit-test-code-sbmy"
PG_PORT, MY_PORT, SB_PG_PORT, SB_MY_PORT = 16081, 16080, 16083, 16082
MY_SMALL = ["--innodb-buffer-pool-size=16M", "--performance-schema=OFF",
            "--log-bin-trust-function-creators=1"]


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


def _pg(sql, db="ledger"):
    got = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d", db,
              "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(*statements, db="ledger"):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                           password="test", database=db, autocommit=True)
    try:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
            return cur.fetchall()
    finally:
        conn.close()


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    from migkit.storedcode.sandbox import PG_DOCKERFILE, pg_tag
    tag = pg_tag(16)
    if _sh("docker", "image", "inspect", tag).returncode != 0:
        assert _sh("docker", "build", "-t", tag, "-", timeout=900,
                   input=PG_DOCKERFILE.format(v=16)).returncode == 0
    for n, port, image in ((PG, PG_PORT, "postgres:16"),
                           (SB_PG, SB_PG_PORT, tag)):
        _sh("docker", "rm", "-f", "-v", n)
        _sh("docker", "run", "-d", "--name", n, "-e",
            "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432", image)
    for n, port in ((MY, MY_PORT), (SB_MY, SB_MY_PORT)):
        _sh("docker", "rm", "-f", "-v", n)
        _sh("docker", "run", "-d", "--name", n, "-e",
            "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{port}:3306",
            "mysql:8.4", *MY_SMALL)
    try:
        for n in (PG, SB_PG):
            for _ in range(60):
                if _sh("docker", "exec", n, "psql", "-U", "postgres", "-c",
                       "select 1").returncode == 0:
                    break
                time.sleep(1)
        for n in (MY, SB_MY):
            for _ in range(90):
                if _sh("docker", "exec", n, "mysql", "-uroot", "-ptest",
                       "-h127.0.0.1", "--protocol=tcp", "-e",
                       "select 1").returncode == 0:
                    break
                time.sleep(2)
        for port in (PG_PORT, MY_PORT, SB_PG_PORT, SB_MY_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _pg("create database ledger", db="postgres")
        _pg("""
            create table accounts (id integer primary key, owner varchar(20)
              not null, balance numeric(10,2) not null default 0);
            insert into accounts values (1, 'ann', 100.00), (2, 'Bob', 0),
              (3, 'cy', 250.50);
            create function fee(amount numeric) returns numeric
              language plpgsql as $$
            begin
              if amount > 100 then
                return round(amount * 0.01, 2);
              end if;
              return 1.00;
            end $$;
            create function accounts_bu() returns trigger language plpgsql
              as $$ begin new.owner := upper(new.owner); return new; end $$;
            create trigger accounts_bu before update on accounts
              for each row execute function accounts_bu();
            """)
        _my("create database ledger", db=None)
        _my("create table accounts (id int primary key, owner varchar(20)"
            " not null, balance decimal(10,2) not null default 0)",
            "insert into accounts values (1, 'ann', 100.00), (2, 'Bob', 0),"
            " (3, 'cy', 250.50)",
            "create function fee(amount decimal(12,4)) returns"
            " decimal(12,4) deterministic begin if amount > 100 then return"
            " round(amount * 0.01, 2); end if; return 1.00; end",
            # lowers what the source's trigger raises: found by what it
            # writes
            "create trigger accounts_bu before update on accounts for each"
            " row set new.owner = lower(new.owner)")
        yield
    finally:
        for n in (PG, MY, SB_PG, SB_MY):
            _sh("docker", "rm", "-f", "-v", n)


def _engine(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="pgmy", engine="hetero",
              options={"source_engine": "postgres", "target_engine": "mysql",
                       "proof_sandbox": {
                           "source": {"host": "127.0.0.1",
                                      "port": SB_PG_PORT,
                                      "user": "postgres",
                                      "password": "test"},
                           "target": {"host": "127.0.0.1",
                                      "port": SB_MY_PORT, "user": "root",
                                      "password": "test"}}},
              source=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              databases=["ledger"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_postgresql_code_held_to_mysqls_versions(servers, tmp_path):
    got = _engine(tmp_path).prove_converted("ledger")
    by = {v["routine"]: v for v in json.loads(
        (tmp_path / "code-proof.json").read_text())["verdicts"]}
    assert got.status == "diff", got.__dict__
    assert by["trigger accounts_bu"]["status"] == "diff", by
    assert "rows changed in public.accounts" in \
        by["trigger accounts_bu"]["reason"], by
    # the function answers alike, and each side's coverage is measured:
    # the source's by its profiler, the target's by probes in its copy
    fee = by["function fee"]
    assert fee["status"] == "ok", fee
    assert "source: " in fee["coverage"] and "target: " in \
        fee["coverage"], fee
    # the hop's own servers were only read
    assert _pg("select string_agg(owner, ',' order by id) from accounts") \
        == "ann,Bob,cy"
    assert _my("select group_concat(owner order by id) from accounts") == \
        (("ann,Bob,cy",),)
