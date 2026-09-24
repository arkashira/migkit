"""A binlog written compressed stops the change tail instead of being
skipped.

Measured before, MySQL 8.4 with `binlog_transaction_compression = ON` and
MariaDB 11 with `log_bin_compress = ON`: an insert, an update and a
delete went into the binlog compressed, and the reader returned no change
and did not move its position. The tail would have called itself caught up
for as long as it ran. The reader cannot open a compressed transaction, so
the tail now stops on one, says which setting writes it and what to do,
and `assess` fails the setting before anything starts.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = pytest.mark.docker

MY, MY_PORT = "migkit-test-zc-my", 15706
MARIA, MARIA_PORT = "migkit-test-zc-maria", 15707


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def _sql(name, client, sql):
    got = subprocess.run(["docker", "exec", "-i", name, client, "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _server(name, port, image, client, *args):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    env = "MARIADB_ROOT_PASSWORD" if "mariadb" in image else \
        "MYSQL_ROOT_PASSWORD"
    subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                    f"{env}=test", "-p", f"{port}:3306", image, *args],
                   check=True, capture_output=True)
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, client, "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp",
                             "-e", "select 1"],
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(2)
    else:
        pytest.fail(f"{name} never answered")
    _sql(name, client, "create database appdb;"
                       " create table appdb.t (id int primary key, v text)")


@pytest.fixture(scope="module")
def mysql():
    try:
        _server(MY, MY_PORT, "mysql:8.4", "mysql", "--server-id=7",
                "--binlog-row-metadata=FULL")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


@pytest.fixture(scope="module")
def mariadb():
    try:
        _server(MARIA, MARIA_PORT, "mariadb:11", "mariadb", "--server-id=7",
                "--log-bin=mysql-bin", "--binlog-format=ROW",
                "--binlog-row-metadata=FULL", "--log-bin-compress=ON",
                "--log-bin-compress-min-len=10")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MARIA],
                       capture_output=True)


def _eng(port):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=port, user="root", password="test")
    return MySQLEngine(Hop(name="zc", engine="mysql", source=ep, target=ep,
                           databases=["appdb"]))


def _changes(eng, token):
    got, _ = eng.neutral_changes("src", "appdb", token)
    return [(c["op"], c["key"]["id"]) for c in got]


def test_the_reader_really_skips_a_compressed_transaction(mysql):
    """What the stop rests on. If the reader learns to open them, this
    fails, and the stop can give way to reading them."""
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (DeleteRowsEvent,
                                              UpdateRowsEvent,
                                              WriteRowsEvent)
    eng = _eng(MY_PORT)
    token = eng.change_point("src", "appdb")
    _sql(MY, "mysql", "set session binlog_transaction_compression = ON;"
                      " insert into appdb.t values (1, 'a');"
                      " update appdb.t set v = 'b' where id = 1;"
                      " delete from appdb.t where id = 1")
    stream = BinLogStreamReader(
        connection_settings={"host": "127.0.0.1", "port": MY_PORT,
                             "user": "root", "passwd": "test"},
        server_id=4380, blocking=False, resume_stream=True,
        log_file=token["log_file"], log_pos=token["log_pos"],
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent])
    try:
        assert list(stream) == []
    finally:
        stream.close()


def test_a_compressed_transaction_stops_the_tail_where_it_is(mysql):
    eng = _eng(MY_PORT)
    token = eng.change_point("src", "appdb")
    _sql(MY, "mysql", "insert into appdb.t values (10, 'plain')")
    # an application session turning it on for itself, the server's
    # own setting still off: what assess cannot see
    _sql(MY, "mysql", "set session binlog_transaction_compression = ON;"
                      " insert into appdb.t values (11, 'packed')")
    with pytest.raises(SystemExit) as e:
        _changes(eng, token)
    said = str(e.value)
    assert "binlog_transaction_compression" in said, said
    assert f"position {token['log_pos']}" in said, said
    assert "move again with --mode full+cdc" in said, said
    # what is written uncompressed is read as before
    token = eng.change_point("src", "appdb")
    _sql(MY, "mysql", "insert into appdb.t values (12, 'plain')")
    assert _changes(eng, token) == [("insert", 12)]


def test_assess_fails_the_setting_on_mysql(mysql):
    eng = _eng(MY_PORT)
    item = "binlog_transaction_compression=OFF on source"
    got = [i for i in eng.assess() if i["item"].startswith(item)]
    assert [i["level"] for i in got] == ["pass"], got
    _sql(MY, "mysql", "set global binlog_transaction_compression = ON")
    try:
        got = [i for i in eng.assess() if i["item"].startswith(item)]
    finally:
        _sql(MY, "mysql", "set global binlog_transaction_compression = OFF")
    assert [i["level"] for i in got] == ["fail"], got
    assert "set global binlog_transaction_compression = OFF" in \
        got[0]["detail"], got


def test_mariadb_compressed_rows_stop_the_tail_and_fail_assess(mariadb):
    eng = _eng(MARIA_PORT)
    token = eng.change_point("src", "appdb")
    _sql(MARIA, "mariadb", "insert into appdb.t values"
                           " (1, repeat('a', 200));"
                           " delete from appdb.t where id = 1")
    with pytest.raises(SystemExit) as e:
        _changes(eng, token)
    assert "log_bin_compress" in str(e.value), e.value
    got = [i for i in eng.assess()
           if i["item"].startswith("log_bin_compress=OFF")]
    assert [i["level"] for i in got] == ["fail"], got
    assert "set global log_bin_compress = OFF" in got[0]["detail"], got


def test_a_compressed_transaction_stops_the_delta_where_it_is(mysql,
                                                              tmp_path):
    """The delta loop read the same binlog with the same reader, and said
    `0 changes since last verified position` over a compressed insert -
    then moved its position past it."""
    eng = _eng(MY_PORT)
    eng.hop.report_dir = lambda db=None: tmp_path
    eng.delta_verify("appdb")
    state = tmp_path / "delta-pos.json"
    before = state.read_text()
    _sql(MY, "mysql", "set session binlog_transaction_compression = ON;"
                      " insert into appdb.t values (21, 'packed')")
    got = eng.delta_verify("appdb")
    assert [r.status for r in got] == ["error"], [r.__dict__ for r in got]
    assert "binlog_transaction_compression" in got[0].detail, got[0].detail
    assert "was verified" in got[0].detail, got[0].detail
    assert state.read_text() == before
