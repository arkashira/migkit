"""The row-by-row text repair, on MySQL as on PostgreSQL (backlog 20).

MySQL's mojibake check has always ended by asking for this repair - only
the values that re-encode to valid UTF-8, never the whole column, because
the blanket conversion breaks the text that was never broken - and only
PostgreSQL had it. The decision of which values to touch is shared
(`_mojibake_updates`); MySQL supplies the rows, read on the target.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = pytest.mark.docker

MY, PORT = "migkit-test-mymoji", 15773


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B",
                          "--default-character-set=utf8mb4"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        with socket.socket() as s:
            s.settimeout(5)
            assert s.connect_ex(("127.0.0.1", PORT)) == 0
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture
def eng(server, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    my("drop database if exists cy; create database cy"
       " character set utf8mb4;"
       " create table cy.people (id int primary key, name text);"
       " insert into cy.people values (1, 'cafÃ©'), (2, 'café'),"
       " (3, 'plain'), (4, 'MÃ¼ller')")
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="tx", engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def test_only_the_double_encoded_values_are_repaired(eng, monkeypatch):
    monkeypatch.setenv("MIGKIT_REPAIR_TEXT", "1")
    action = eng._mojibake_repair("cx")
    assert action is not None and action.kind == "text"
    assert len(action.statements) == 2, action.statements
    eng.apply("cx", action)
    assert my("select group_concat(concat(id, '=', name) order by id)"
              " from cy.people") == "1=café,2=café,3=plain,4=Müller"
    # and the undo puts back exactly what was there
    from migkit.engines.base import RepairAction
    eng.apply("cx", RepairAction("cx", "text", action.undo, [], ""))
    assert my("select name from cy.people where id = 1") == "cafÃ©"


def test_without_the_switch_it_only_describes_itself(eng, monkeypatch):
    monkeypatch.delenv("MIGKIT_REPAIR_TEXT", raising=False)
    action = eng._mojibake_repair("cx")
    assert action is not None and action.statements == [], action
    assert "2 double-encoded values" in action.note, action.note
    eng.apply("cx", action)
    assert my("select name from cy.people where id = 1") == "cafÃ©"
    # and it is part of the row repair's plan
    kinds = [a.kind for a in eng.repair_plan("cx", "rows")]
    assert "text" in kinds, kinds
