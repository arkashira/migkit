"""A MySQL source at binlog_row_image MINIMAL or NOBLOB, with JSON diffs
in its log, is followed, not refused (LT.R7).

An UPDATE's after image holds only what changed: the tail writes those
columns, names the rest as left to the target (`canon.UNCHANGED`), and a
JSON column the log carries as a diff is read where the source holds it.
A DELETE's image holds the key.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

MY, MY_PORT = "migkit-test-lt-r7-my", 16580


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
                    f"127.0.0.1:{MY_PORT}:3306", "mysql:8.4",
                    "--server-id=7", "--binlog-row-metadata=FULL",
                    "--binlog-row-image=MINIMAL",
                    "--binlog-row-value-options=PARTIAL_JSON"],
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
                if ok and s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                    break
            time.sleep(2)
        else:
            pytest.fail("MySQL never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _engine():
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    return MySQLEngine(Hop(name="ltr7", engine="mysql", source=ep,
                           target=ep, databases=["appdb"]))


@needs_docker
def test_a_minimal_image_is_followed(mysql_server):
    _my("create database appdb; create table appdb.t (id int primary key,"
        " a varchar(20), b text, j json)")
    assert _my("select @@binlog_row_image") == "MINIMAL"
    token = _engine().change_point("src", "appdb")
    _my("""insert into appdb.t values (1, 'x', 'long text', '{"p": 1, "q": 2}');
update appdb.t set a = 'y' where id = 1;
update appdb.t set j = json_set(j, '$.q', 3) where id = 1""")
    changes, token = _engine().neutral_changes("src", "appdb", token)
    assert [(c["op"], c["key"]["id"]) for c in changes] == [
        ("insert", 1), ("update", 1), ("update", 1)], changes
    ins, name, js = changes
    # an insert is whole
    assert ins["values"]["b"] == "long text"
    # only the changed column is written; the rest is left to the target
    assert name["values"] == {"id": 1, "a": "y"}, name
    assert sorted(name["unchanged"]) == ["b", "j"], name
    # a JSON diff: the column as the source holds it, not a NULL
    assert js["values"]["j"] in ('{"p": 1, "q": 3}', {"p": 1, "q": 3}), js
    assert "a" not in js["values"] and "b" not in js["values"], js
    assert "unchanged" not in js or "j" not in js["unchanged"], js
    # the row gone from the source before the diff is read: nothing is
    # written for the column, and the delete that follows removes the row
    _my("update appdb.t set j = json_set(j, '$.p', 9) where id = 1;"
        " update appdb.t set id = 5 where id = 1;"
        " delete from appdb.t where id = 5")
    changes, _ = _engine().neutral_changes("src", "appdb", token)
    gone_j, move, gone = changes
    assert "j" not in gone_j["values"] and "j" in gone_j["unchanged"], gone_j
    # a moved key finds the old row and carries the new one
    assert move["key"] == {"id": 1} and move["values"]["id"] == 5, move
    # a delete carries its key only
    assert gone["key"] == {"id": 5} and not gone.get("before"), gone
