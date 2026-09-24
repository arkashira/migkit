"""`check` on a cross-engine hop reads the data a second way, by itself.

The independent reader was built and measured, and nothing ran it. The
planner's first rule now does: a cross-engine pair gets a second reading
whenever the reader is installed and speaks both engines, because every
value is converted on the way and migkit's own reading was the only one
looking.

It reads aggregates, not the reader's row hash, because that was measured
too: on equal data the hash called a row holding the double `-1e-07`
different, since MySQL and PostgreSQL spell it differently before hashing.
"""
import os
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

MY, PG = "migkit-test-sr2-my", "migkit-test-sr2-pg"
MY_PORT, PG_PORT = 15692, 15693
READER = os.environ.get("MIGKIT_SECOND_READER_PYTHON",
                        "/tmp/dvt-venv/bin/python")


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def _hop():
    return Hop(name="sr2", engine="hetero",
               source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                               password="test"),
               target=Endpoint(host="127.0.0.1", port=PG_PORT,
                               user="postgres", password="test"),
               databases=["app"],
               options={"source_engine": "mysql",
                        "target_engine": "postgres"})


def test_the_planner_adds_it_only_where_it_can_run(monkeypatch):
    from migkit import second_reader
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(second_reader, "interpreter", lambda: "/x/python")
    assert HeteroEngine(_hop()).planned_checks() == ("second",)
    monkeypatch.setattr(second_reader, "interpreter", lambda: None)
    assert HeteroEngine(_hop()).planned_checks() == ()
    # a pair the reader cannot read gets nothing, installed or not
    monkeypatch.setattr(second_reader, "interpreter", lambda: "/x/python")
    hop = _hop()
    hop.options = {"source_engine": "mysql", "target_engine": "sqlite"}
    assert HeteroEngine(hop).planned_checks() == ()


def _sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


@pytest.fixture(scope="module")
def pair():
    _sh("docker", "rm", "-f", "-v", MY, PG)
    _sh("docker", "run", "-d", "--name", MY, "-e", "MYSQL_ROOT_PASSWORD=test",
        "-p", f"{MY_PORT}:3306", "mysql:8")
    _sh("docker", "run", "-d", "--name", PG, "-e", "POSTGRES_PASSWORD=test",
        "-p", f"{PG_PORT}:5432", "postgres:16")
    try:
        for port in (MY_PORT, PG_PORT):
            for _ in range(120):
                with socket.socket() as s:
                    s.settimeout(2)
                    if s.connect_ex(("127.0.0.1", port)) == 0:
                        break
                time.sleep(1)
        for _ in range(90):
            if _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest",
                   "-h127.0.0.1", "--protocol=tcp", "-e",
                   "select 1").returncode == 0:
                break
            time.sleep(2)
        for _ in range(40):
            if _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
                   "select 1").returncode == 0:
                break
            time.sleep(1)
        rows = "(1, 10, 1.5, 'abc'), (2, -3, 0.0001, 'x'), (3, 7, 2.25, '')"
        assert _sh("docker", "exec", MY, "mysql", "-uroot", "-ptest", "-e",
                   "create database app; create table app.t (id int primary"
                   " key, n bigint, d decimal(12,4), s varchar(20));"
                   f" insert into app.t values {rows}").returncode == 0
        _sh("docker", "exec", PG, "psql", "-U", "postgres", "-c",
            "create database app")
        assert _sh("docker", "exec", PG, "psql", "-U", "postgres", "-d",
                   "app", "-c", "create table t (id int primary key, n bigint,"
                   " d numeric(12,4), s varchar(20));"
                   f" insert into t values {rows}").returncode == 0
        yield
    finally:
        _sh("docker", "rm", "-f", "-v", MY, PG)


def _check(tmp_path, monkeypatch):
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    monkeypatch.setenv("MIGKIT_SECOND_READER_PYTHON", READER)
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  sr2:\n    engine: hetero\n"
        f"    source: {{host: 127.0.0.1, port: {MY_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {PG_PORT}, user: postgres,"
        " password: test}\n"
        "    databases: [app]\n"
        "    options: {source_engine: mysql, target_engine: postgres}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    got = CliRunner().invoke(cli.main, ["check", "sr2"])
    return " ".join(got.output.split())


@pytest.mark.usefixtures("pair")
@pytest.mark.docker
@pytest.mark.skipif(not _docker(), reason="docker not available")
@pytest.mark.skipif(not os.path.exists(READER),
                    reason="the second reader is not installed")
def test_an_ordinary_check_reads_it_twice(tmp_path, monkeypatch):
    said = _check(tmp_path, monkeypatch)
    assert "measures agree, read a second way" in said, said
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    assert not [t for t in TOOLS if t in said.lower()], said


@pytest.mark.usefixtures("pair")
@pytest.mark.docker
@pytest.mark.skipif(not _docker(), reason="docker not available")
@pytest.mark.skipif(not os.path.exists(READER),
                    reason="the second reader is not installed")
def test_a_changed_value_is_seen_the_second_way_too(tmp_path, monkeypatch):
    _sh("docker", "exec", PG, "psql", "-U", "postgres", "-d", "app", "-c",
        "update t set n = 8 where id = 3")
    try:
        said = _check(tmp_path, monkeypatch)
        assert "differ when read a second way" in said, said
    finally:
        _sh("docker", "exec", PG, "psql", "-U", "postgres", "-d", "app",
            "-c", "update t set n = 7 where id = 3")
