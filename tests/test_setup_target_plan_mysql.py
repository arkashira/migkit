"""The MySQL target's preparation plan (backlog 21).

It loaded the whole schema before the data, so every secondary index was
kept up row by row through the load, and it created the database as
`utf8mb4` whatever the source used. The database now comes in the source's
own character set and collation, read from the source, and the plan says
what the move already does in the order that measures best - the tables
the target lacks, indexes built once after the load, triggers off - as
migkit's commands rather than a program's.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

MY, PORT = "migkit-test-mysetup", 15774


def _hop(port, tmp_path):
    ep = Endpoint(host="127.0.0.1", port=port, user="root", password="test")
    hop = Hop(name="prep", engine="mysql", source=ep, target=ep,
              databases=["legacy"], db_map={"legacy": "fresh"})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _plan(port, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    return MySQLEngine(_hop(port, tmp_path)).setup_target_plan("legacy")


def test_a_source_it_cannot_read_is_said_not_guessed(tmp_path):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    plan = _plan(1, tmp_path)
    assert "could not be read" in plan[0], plan
    assert "utf8mb4" not in plan[0], plan
    text = " ".join(plan).lower()
    assert not [t for t in TOOLS if t in text], plan
    assert "migkit move prep --go" in text, plan


@pytest.mark.docker
def test_the_database_comes_in_the_sources_character_set(tmp_path):
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "create database if not exists legacy"
                                     " character set latin1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        with socket.socket() as s:
            s.settimeout(5)
            assert s.connect_ex(("127.0.0.1", PORT)) == 0
        plan = _plan(PORT, tmp_path)
        assert plan[0].startswith("create database `fresh` character set"
                                  " latin1 collate latin1_swedish_ci"), plan
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
