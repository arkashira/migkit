"""The MySQL bulk path says which table it is on, in migkit's words.

The dump and the load were run blind: one phase line each and nothing until
they finished, however many tables there were. Both programs can log one
JSON object per event - measured on the installed build, the dump emits
`dump_table_progress` with `db`, `table` and `tables_total`, and the load
`restore_data_progress` per chunk and a closing `restore_completed` with
its own error count. Those are read by field and said as migkit's lines.

A failure used to be the raw tail of whatever the program printed; with a
machine log that tail is JSON, so the failure is now the program's own
error messages, without its name.
"""
import shutil
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

MY, PORT = "migkit-test-mysp", 15689


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.docker,
    pytest.mark.skipif(not _docker(), reason="docker not available"),
    pytest.mark.skipif(not (shutil.which("mydumper")
                            and shutil.which("myloader")),
                       reason="the MySQL dump programs are not installed"),
]


def my(sql):
    got = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                          "-h127.0.0.1", "--protocol=tcp", "-N", "-B", "-e",
                          sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        for _ in range(90):
            try:
                my("select 1")
                break
            except AssertionError:
                time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _hop(tmp_path):
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="sp", engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"})
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _seed(target_tables=("a", "b")):
    my("drop database if exists cx; drop database if exists cy;"
       " create database cx; create database cy;"
       " create table cx.a (id int primary key, v text);"
       " create table cx.b (id int primary key, v text);"
       " insert into cx.a values (1, 'x'), (2, 'y');"
       " insert into cx.b values (1, 'z');")
    for t in target_tables:
        my(f"create table cy.{t} (id int primary key, v text)")


@pytest.mark.usefixtures("server")
def test_each_table_is_named_as_it_is_read_and_loaded(tmp_path):
    from migkit import movers
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    _seed()
    said = []
    movers.mydumper_move(_hop(tmp_path), "cx", 2, True, said.append)
    text = " | ".join(str(s) for s in said)
    assert "cx.a: reading" in text and "cx.b: reading" in text, text
    assert "of 2 tables" in text, text
    assert "cy.a: loading" in text and "cy.b: loading" in text, text
    assert my("select count(*) from cy.a") == "2"
    assert not [t for t in TOOLS if t in text.lower()], text


@pytest.mark.usefixtures("server")
def test_a_failed_load_is_its_message_not_a_machine_log(tmp_path):
    """The target has no table `b`, so the data-only load cannot land."""
    from migkit import movers
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    _seed(target_tables=("a",))
    with pytest.raises((RuntimeError, SystemExit)) as e:
        movers.mydumper_move(_hop(tmp_path), "cx", 2, True, lambda m: None)
    said = str(e.value)
    assert '{"schema_version"' not in said, said
    assert not [t for t in TOOLS if t in said.lower()], said


def test_a_load_that_counts_errors_is_not_complete(tmp_path, monkeypatch):
    """The load closes with its own error count. An exit code of 0 beside
    a count above zero is still a load with errors in it."""
    import contextlib
    import json

    from migkit import movers

    def fake_sh(argv, env=None, log=None, progress=None):
        if progress is not None and argv[0] == "myloader":
            progress(json.dumps({"event": "restore_completed",
                                 "errors": "2", "tables": "2"}))
    monkeypatch.setattr(movers, "_sh", fake_sh)
    monkeypatch.setattr(movers, "_my_truncate_target", lambda *a, **k: None)
    monkeypatch.setattr(movers, "_MyIndexWindow",
                        lambda *a, **k: contextlib.nullcontext())
    with pytest.raises(RuntimeError) as e:
        movers.mydumper_move(_hop(tmp_path), "cx", 2, True, lambda m: None)
    assert "2 errors" in str(e.value), e.value


@pytest.mark.usefixtures("server")
def test_mysql_reads_the_planners_facts_in_one_query(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    _seed()
    my("create table cx.bare (v int); analyze table cx.a")
    got = MySQLEngine(_hop(tmp_path)).table_facts("src", "cx")
    assert got["a"]["key"] is True and got["bare"]["key"] is False, got
    assert got["a"]["rows"] == 2, got
