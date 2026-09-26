"""A connection pooler between migkit and PostgreSQL (problems F4).

Measured against PgBouncer 1.25 in transaction pooling, in front of
PostgreSQL 16:
* a setting given as the connection opens is refused -
  `FATAL: unsupported startup parameter in options: statement_timeout` -
  which is how the row repair opens its connection; it read as the
  database refusing something
* a subscription made through it copied the table and followed the next
  insert, so a change stream through a pooler is not refused

So `assess` names the pooler, how it pools and what that means, and the
refusal is said as what it is. PgBouncer is known by its console, where it
lets its administrators in, and otherwise by how it refuses a database it
does not serve.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

NET = "migkit-test-pooler-net"
PG, BOUNCER = "migkit-test-pooler-pg", "migkit-test-pooler-bouncer"
PG_PORT, BOUNCER_PORT = 15851, 15850


def _up(port):
    end = time.time() + 90
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(1)
    return False


def _ready():
    import psycopg2
    end = time.time() + 90
    while time.time() < end:
        try:
            psycopg2.connect(host="127.0.0.1", port=BOUNCER_PORT,
                             user="postgres", password="test",
                             dbname="postgres", connect_timeout=3).close()
            return True
        except Exception:
            time.sleep(1)
    return False


@pytest.fixture(scope="module")
def pooled():
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    for name in (PG, BOUNCER):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "--network", NET,
                    "-p", f"{PG_PORT}:5432", "-e", "POSTGRES_PASSWORD=test",
                    "postgres:16"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", BOUNCER, "--network",
                    NET, "-p", f"{BOUNCER_PORT}:5432", "-e",
                    "DB_USER=postgres", "-e", "DB_PASSWORD=test", "-e",
                    f"DB_HOST={PG}", "-e", "DB_NAME=postgres", "-e",
                    "POOL_MODE=transaction", "-e",
                    "AUTH_TYPE=scram-sha-256", "edoburu/pgbouncer:latest"],
                   check=True, capture_output=True)
    try:
        assert _up(PG_PORT) and _up(BOUNCER_PORT)
        assert _ready(), "the pooler never let a client through"
        subprocess.run(["docker", "exec", PG, "psql", "-U", "postgres", "-c",
                        "create role app login password 'CHANGE_ME-app'"],
                       check=True, capture_output=True)
        yield
    finally:
        for name in (BOUNCER, PG):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _engine(src_port=BOUNCER_PORT, user="postgres", password="test"):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(Hop(
        name="pooled", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=src_port, user=user,
                        password=password),
        target=Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                        password="test"),
        databases=["postgres"]))


def test_its_administrator_is_told_how_it_pools(pooled):
    import re
    eng = _engine()
    got = eng.pooler("src")
    assert re.fullmatch(r"\d+\.\d+\.\d+", got.pop("version") or ""), got
    assert got == {"name": "PgBouncer", "mode": "transaction"}
    # the database itself is not a pooler
    assert eng.pooler("dst") is None
    rows = eng._pooler_items()
    assert [(r["level"], r["item"]) for r in rows] == [
        ("warn", "the source is reached through a connection pooler")], rows
    assert re.match(r"PgBouncer \d+\.\d+\.\d+, pooling by transaction: a"
                    " session setting", rows[0]["detail"]), rows


def test_a_user_outside_its_console_still_finds_it(pooled):
    eng = _engine(user="app", password="CHANGE_ME-app")
    assert eng.pooler("src") == {"name": "a connection pooler",
                                 "mode": None, "version": None}
    # a wrong password is a wrong password, not a pooler
    assert _engine(user="app", password="CHANGE_ME-wrong").pooler("src") is None
    # the database itself, asked by the same user, is not one
    assert _engine(src_port=PG_PORT, user="app",
                   password="CHANGE_ME-app").pooler("src") is None


def test_an_rds_proxy_is_known_by_its_address():
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="shop.proxy-abc123example.eu-west-1.rds.amazonaws.com",
                  port=3306, user="admin", password="CHANGE_ME")
    eng = MySQLEngine(Hop(name="p", engine="mysql", source=ep,
                          target=Endpoint(host="10.0.0.2", port=3306,
                                          user="admin", password="CHANGE_ME"),
                          databases=["shop"]))
    assert eng.pooler("src")["name"] == "RDS Proxy"
    assert eng.pooler("dst") is None
    rows = eng._pooler_items()
    assert [r["detail"] for r in rows] == [
        "RDS Proxy, sharing server sessions between transactions, and keeping"
        " one to a client that changes its session: every connection migkit"
        " opens there changes its session, and holds a server session the"
        " application could have had for as long as it runs. Give the hop"
        " the database's own address for the move and the repair; the"
        " pooler's is for the application"], rows


def test_a_refused_setting_is_said_as_the_pooler(pooled, tmp_path):
    eng = _engine()
    eng.hop.report_dir = lambda db=None: tmp_path
    with pytest.raises(SystemExit) as e:
        eng._psql_run("src", "postgres", "select 1;")
    said = str(e.value)
    assert said.startswith("the source: a connection pooler stands between"
                           " migkit and the database, and refused the"
                           " setting statement_timeout"), said
    # through the pooler's port the words were the database's
    from migkit.wording import database_words
    assert database_words(
        'psql: error: connection to server at "127.0.0.1", port 15850'
        " failed: FATAL:  unsupported startup parameter in options:"
        " statement_timeout").startswith("a connection pooler stands")
