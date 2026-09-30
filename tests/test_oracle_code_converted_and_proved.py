"""Oracle's PL/SQL converted by the open PL/SQL converter (driven as a
program) and proved against Oracle itself (backlog R11, as far as the
sandbox allows).

Oracle Free runs natively on arm64 from 23.5, capped at 2 GB and 2 CPUs;
two of them and two PostgreSQL servers run in the machine's second VM
(`with_docker_lock.py --vm migkit`). migkit's own rules do not convert
PL/SQL; the converter built by `doctor --install` does, and its version
is held to Oracle's answers like any candidate's, and Oracle's own
block coverage of the source's routine is part of its evidence
(`test_oracle_code_coverage_and_counters` holds it on its own).
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

OR, SB_OR = "migkit-test-code-or", "migkit-test-code-sbor"
PG, SB_PG = "migkit-test-code-pg", "migkit-test-code-sbpg"
OR_PORT, SB_OR_PORT, PG_PORT, SB_PG_PORT = 16084, 16085, 16081, 16083
PASSWORD = "CHANGE_ME_test1"
IMAGE = "gvenzl/oracle-free:slim-faststart"


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


def _or(*statements, port=OR_PORT, user="system", password=PASSWORD):
    import oracledb
    conn = oracledb.connect(user=user, password=password,
                            dsn=f"127.0.0.1:{port}/FREEPDB1")
    try:
        cur = conn.cursor()
        out = None
        for s in statements:
            cur.execute(s)
            out = cur.fetchall() if cur.description else out
        conn.commit()
        return out
    finally:
        conn.close()


def _pg(sql, name=PG, db="app"):
    got = _sh("docker", "exec", "-i", name, "psql", "-U", "postgres", "-d",
              db, "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    from migkit import tools
    if tools.built_program("ora2pg") is None and \
            not tools.install_from_source("ora2pg"):
        pytest.skip("the PL/SQL converter could not be built here")
    for n, port in ((OR, OR_PORT), (SB_OR, SB_OR_PORT)):
        _sh("docker", "rm", "-f", "-v", n)
        _sh("docker", "run", "-d", "--name", n, "-e",
            f"ORACLE_PASSWORD={PASSWORD}", "-p", f"127.0.0.1:{port}:1521",
            IMAGE)
    from migkit.storedcode.sandbox import PG_DOCKERFILE, pg_tag
    tag = pg_tag(16)
    if _sh("docker", "image", "inspect", tag).returncode != 0:
        assert _sh("docker", "build", "-t", tag, "-", timeout=900,
                   input=PG_DOCKERFILE.format(v=16)).returncode == 0
    # the target runs the emulation of Oracle's functions, as its sandbox
    for n, port, image in ((PG, PG_PORT, tag), (SB_PG, SB_PG_PORT, tag)):
        _sh("docker", "rm", "-f", "-v", n)
        _sh("docker", "run", "-d", "--name", n, "-e",
            "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432", image)
    try:
        for port in (OR_PORT, SB_OR_PORT):
            for _ in range(150):
                try:
                    _or("select 1 from dual", port=port)
                    break
                except Exception:
                    time.sleep(2)
        for n in (PG, SB_PG):
            for _ in range(60):
                if _sh("docker", "exec", n, "psql", "-U", "postgres", "-c",
                       "select 1").returncode == 0:
                    break
                time.sleep(1)
        for port in (OR_PORT, SB_OR_PORT, PG_PORT, SB_PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _or("create user app identified by \"CHANGE_ME_app1\" quota"
            " unlimited on users",
            "grant create session, create table, create procedure,"
            " create trigger to app")
        _or("create table app.items (id number(10) primary key, qty"
            " number(10) not null, price number(8,2), name varchar2(20))",
            "insert into app.items values (1, 10, 0.25, 'bolt')",
            "insert into app.items values (2, 0, 0.10, 'nut')",
            "insert into app.items values (3, 5, 12.50, 'Gear')",
            # Oracle counts a negative start from the end; PostgreSQL's
            # substring does not - only the target's emulation answers so
            "create view app.tails as select id, substr(name, -2) as tail"
            " from app.items",
            "create function app.worth(p_id number) return number is"
            " v number; begin select qty * price into v from app.items"
            " where id = p_id; return v; exception when no_data_found then"
            " return -1; end;",
            "create procedure app.bump(p_id in number, p_by in number,"
            " p_left out number) is begin update app.items set qty = qty +"
            " p_by where id = p_id; select qty into p_left from app.items"
            " where id = p_id; end;")
        _pg("create database app", db="postgres")
        _pg("""
            create extension orafce;
            create table items (id numeric(10) primary key, qty numeric(10)
              not null, price numeric(8,2), name varchar(20));
            insert into items values (1, 10, 0.25, 'bolt'),
              (2, 0, 0.10, 'nut'), (3, 5, 12.50, 'Gear');
            """)
        yield
    finally:
        for n in (OR, SB_OR, PG, SB_PG):
            _sh("docker", "rm", "-f", "-v", n)


def _engine(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="or", engine="hetero",
              options={"source_engine": "oracle", "target_engine": "postgres",
                       "proof_sandbox": {
                           "source": {"host": "127.0.0.1",
                                      "port": SB_OR_PORT, "user": "system",
                                      "password": PASSWORD},
                           "target": {"host": "127.0.0.1",
                                      "port": SB_PG_PORT,
                                      "user": "postgres",
                                      "password": "test"}},
                       "code_dir": str(tmp_path / "none")},
              source=Endpoint(host="127.0.0.1", port=OR_PORT, user="system",
                              password=PASSWORD),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def test_plsql_converted_by_a_program_and_proved_on_oracle(servers,
                                                           tmp_path):
    eng = _engine(tmp_path)
    eng.converted_code("app")
    conv = json.loads((tmp_path / "code-conversion.json").read_text())
    # the named sandbox was used (else this says why not)
    assert conv["sandbox"] == "used", conv["sandbox"]
    record = {r["routine"]: r for r in conv["routines"]}
    # each version the converters made was run against Oracle's own: it
    # is proved, with the source's coverage counted by Oracle itself, or
    # it failed by something a call did - never taken on trust
    for name in ("function worth", "procedure bump"):
        got = record[name]
        said = " ".join(got.get("reasons") or [])
        proved = got["class"] in ("proved", "proved with caveats") and \
            got["rung"] in ("plsql-converter", "dialect-converter") and \
            "source: " in got["evidence"]["coverage"]
        failed_by_running = got["class"] == "not converted" and \
            "the PL/SQL converter: " in said and any(
                w in said for w in ("called with", "rows changed",
                                    "refuses it", "static check"))
        assert proved or failed_by_running, got
    # the view: the translator's substring means another thing, and the
    # target's emulation of Oracle's substr is what proves
    assert record["view tails"]["rung"] == "emulate", record["view tails"]
    assert any("the query translator" in f and "differ" in f
               for f in record["view tails"]["fell"]), record["view tails"]
    # Oracle was only read
    assert _or("select sum(qty) from app.items") == [(15,)]
