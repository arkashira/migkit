"""A table that left a running tail and comes back gets only its changes
since it left, read again from where it left (backlog L6,
`tailtables._add_ways`), where that is exact: the source reads its log
again from an older position (a binlog), the log still holds it, the
table's shape is as it was, and its target holds what it held when it
left. Where the target was written meanwhile, it is copied instead.

MySQL to MySQL, one server, two databases (`db_map`); the hop's `exclude`
changed in its file while the tail runs.
"""
import json
import socket
import subprocess
import threading
import time

import pytest

from migkit import tailctl
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MY, PORT = "migkit-test-livetail-readd-my", 16261


def _my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p",
                    f"127.0.0.1:{PORT}:3306", "mysql:8.4", "--server-id=7",
                    "--binlog-row-metadata=FULL"],
                   check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1", "--protocol=tcp",
                                 "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _rows(db, table):
    return _my(f"select count(*), coalesce(sum(crc32(concat_ws('|', id,"
               f" v))), 0) from {db}.{table}")


def _pair(where, name):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name=name, engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"}, workers=2)
    where.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: where
    return MySQLEngine(hop)._as_pair()


def _state(where, table):
    try:
        return json.loads((where / "tail-tables.json").read_text())[
            "tables"].get(table) or {}
    except (OSError, ValueError):
        return {}


def _wait(cond, what, lines, done, seconds=120):
    end = time.time() + seconds
    while time.time() < end:
        if cond():
            return
        if done.is_set():
            pytest.fail(f"the tail ended before {what}: {lines}")
        time.sleep(0.3)
    pytest.fail(f"never {what}: {lines[-20:]}")


@pytest.mark.usefixtures("mysql_server")
@pytest.mark.parametrize("touched", [False, True])
def test_a_table_back_in_scope_reads_its_changes_since_it_left(
        tmp_path, monkeypatch, touched):
    import migkit.config
    name = f"readd{int(touched)}"
    conf = tmp_path / "hops.yaml"
    conf.write_text(f"hops:\n  {name}:\n    exclude: []\n")
    monkeypatch.setattr(migkit.config, "CONF", conf)
    _my("drop database if exists cx; drop database if exists cy;"
        " create database cx; create database cy;"
        " create table cx.a (id int primary key, v varchar(20));"
        " create table cx.b (id int primary key, v varchar(20));"
        " create table cy.a like cx.a; create table cy.b like cx.b;"
        " insert into cx.b values (1, 'one'), (2, 'two');"
        " insert into cy.b values (1, 'one'), (2, 'two')")
    where = tmp_path / "r"
    pair = _pair(where, name)
    lines, done = [], threading.Event()

    def run():
        try:
            pair.tail_apply("cx", True, where / "tail-token.json",
                            lines.append)
        except BaseException as e:  # noqa: BLE001 - said in the lines
            lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    threading.Thread(target=run, daemon=True).start()
    try:
        _wait(lambda: (where / tailctl.PID).exists(), "running", lines,
              done)
        conf.write_text(f"hops:\n  {name}:\n    exclude: [cx.b]\n")
        _wait(lambda: _state(where, "b").get("state") == "removed",
              "b removed", lines, done)
        _wait(lambda: _state(where, "b").get("digest"), "b digested",
              lines, done)
        _my("update cx.b set v = 'changed' where id = 1;"
            " insert into cx.b values (3, 'three');"
            " insert into cx.a values (1, 'a')")
        if touched:
            # its target written while it was out of the tail
            _my("insert into cy.b values (77, 'stray')")
        conf.write_text(f"hops:\n  {name}:\n    exclude: []\n")
        _wait(lambda: _state(where, "b").get("state") == "live",
              "b back", lines, done)
        _wait(lambda: _rows("cx", "b") == _rows("cy", "b"), "b level",
              lines, done)
        _wait(lambda: _rows("cx", "a") == _rows("cy", "a"), "a level",
              lines, done)
        said = " | ".join(lines)
        if touched:
            assert "its target changed while it was out" in said, said
            assert _my("select count(*) from cy.b where id = 77") == "0"
        else:
            assert "read again from where it left" in said, said
            assert "copying it instead" not in said, said
    finally:
        tailctl.stop(where)
        done.wait(60)
    assert not any(line.startswith("exit:") for line in lines), lines
