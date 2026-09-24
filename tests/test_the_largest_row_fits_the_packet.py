"""`assess` holds the source's largest row against the target's
`max_allowed_packet`, and says the value to set.

Measured on MySQL 8.4 before this was written: a row whose value was
8 MiB, loaded into a target whose packet was 4 MiB, stopped the copy with
`Lost connection` and no word of the packet. An 8 MiB value of zero bytes
stopped a load that a 9 MiB packet carried when the value was letters: a
loader escapes binary, and a zero byte comes out twice as long. The value
`assess` gives is sized for that.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = pytest.mark.docker

SRC, DST = "migkit-test-pkt-src", "migkit-test-pkt-dst"
SRC_PORT, DST_PORT = 15702, 15703
MIB = 1024 * 1024


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


if not _docker():
    pytest.skip("docker not available", allow_module_level=True)


def _my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B", "--max-allowed-packet=64M"],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(name, port):
    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name,
                    "-e", "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                    "mysql:8.4", f"--server-id={port}"],
                   check=True, capture_output=True)
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                             "-ptest", "-h127.0.0.1", "--protocol=tcp",
                             "-e", "select 1"],
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module")
def servers():
    try:
        _up(SRC, SRC_PORT)
        _up(DST, DST_PORT)
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def seeded(servers):
    schema = ("drop database if exists appdb; create database appdb;"
              " create table appdb.t (id int primary key, b longblob);"
              " create table appdb.small (id int primary key, v text);"
              " create table appdb.audit (id int primary key, b longblob);")
    _my(SRC, schema + " insert into appdb.small values (1, 'a');"
                      " insert into appdb.t values"
                      " (1, repeat('x', 8 * 1024 * 1024)), (2, 'tiny');"
                      " insert into appdb.audit values"
                      " (1, repeat('z', 9 * 1024 * 1024));")
    _my(DST, schema)
    yield
    _my(DST, "set global max_allowed_packet = 67108864")


def _hop(tmp_path, exclude=(), **options):
    hop = Hop(name="pkt", engine="mysql", exclude=list(exclude),
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              databases=["appdb"], options=options)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _packet(eng):
    got = [i for i in eng._packet_items() if i["scope"] == "appdb"]
    assert len(got) >= 1, got
    return got


def test_a_row_the_packet_cannot_carry_fails_with_the_value(seeded,
                                                            tmp_path):
    from migkit.engines.mysql import MySQLEngine
    _my(DST, f"set global max_allowed_packet = {4 * MIB}")
    got = _packet(MySQLEngine(_hop(tmp_path, exclude=["appdb.audit"])))
    fail = [i for i in got if i["level"] == "fail"]
    assert fail, got
    assert "t 8,388,608 bytes" in fail[0]["detail"], fail
    assert f"max_allowed_packet = {18 * MIB}" in fail[0]["detail"], fail
    # the excluded table holds the larger row, and is not what it names
    assert "audit" not in " ".join(i["detail"] for i in got), got


def test_a_packet_that_fits_letters_is_warned_about_binary(seeded, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    _my(DST, f"set global max_allowed_packet = {9 * MIB}")
    got = _packet(MySQLEngine(_hop(tmp_path, exclude=["appdb.audit"])))
    assert [i["level"] for i in got] == ["warn"], got
    assert "can double once escaped" in got[0]["detail"], got
    assert f"max_allowed_packet = {18 * MIB}" in got[0]["detail"], got


def test_a_large_enough_packet_passes_and_small_files_are_not_read(
        seeded, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop(tmp_path, exclude=["appdb.audit"]))
    got = _packet(eng)
    assert [i["level"] for i in got] == ["pass"], got
    kinds = {t: k for t, k, _ in eng._row_sizes(
        "appdb", (64 * MIB - eng.PACKET_MARGIN) / 2, time.monotonic() + 30)}
    # a table whose file is far under the packet cannot hold a row over it
    assert kinds == {"t": "bounded", "small": "bounded"}, kinds


def test_a_compressed_table_is_read_not_bounded_by_its_file(seeded,
                                                           tmp_path):
    """The file of a compressed table is far smaller than its values:
    measured, 73,728 bytes on disk for an 8 MiB value."""
    from migkit.engines.mysql import MySQLEngine
    _my(SRC, "create table appdb.packed (id int primary key, b longblob)"
             " row_format = compressed;"
             " insert into appdb.packed values"
             " (1, repeat('x', 8 * 1024 * 1024))")
    _my(DST, "create table appdb.packed (id int primary key, b longblob);"
             f" set global max_allowed_packet = {4 * MIB}")
    eng = MySQLEngine(_hop(tmp_path, exclude=["appdb.audit", "appdb.t"]))
    assert eng._file_sizes("appdb")["packed"] < 1 * MIB
    got = _packet(eng)
    assert [i["level"] for i in got] == ["fail"], got
    assert "packed 8,388,608 bytes" in got[0]["detail"], got


def test_what_the_time_did_not_reach_is_unknown_not_clean(seeded, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    _my(DST, f"set global max_allowed_packet = {4 * MIB}")
    got = _packet(MySQLEngine(_hop(tmp_path, lob_scan_seconds=0,
                                   exclude=["appdb.audit"])))
    assert [i["level"] for i in got] == ["warn"], got
    assert "not measured within lob_scan_seconds" in got[0]["detail"], got
    assert "unknown, not clean" in got[0]["detail"], got


def test_the_value_it_gives_carries_a_binary_row(seeded, tmp_path,
                                                 monkeypatch):
    """The reason for twice the row: measured through the move itself."""
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli, movers
    from migkit.engines.mysql import MySQLEngine
    if not (movers.which("mydumper") and movers.which("myloader")):
        pytest.skip("the MySQL bulk copy is not installed here")
    _my(SRC, "update appdb.t set b = repeat(char(0), 8 * 1024 * 1024)"
             " where id = 1")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  pkt:\n    engine: mysql\n"
        f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: root,"
        " password: test}\n"
        "    databases: [appdb]\n    exclude: [appdb.audit]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")

    def move():
        return CliRunner().invoke(cli.main, ["move", "pkt", "--mode", "full",
                                             "--go"])

    _my(DST, f"set global max_allowed_packet = {9 * MIB}")
    stopped = move()
    assert stopped.exit_code != 0
    # the move says why, not only that the connection went
    said = " ".join((stopped.output + str(stopped.exception or "")).split())
    assert "The likely cause, in appdb: t 8,388,608 bytes" in said, said
    assert f"max_allowed_packet = {18 * MIB}" in said, said
    detail = _packet(MySQLEngine(_hop(tmp_path, exclude=["appdb.audit"])))
    want = int(detail[0]["detail"].split("max_allowed_packet = ")[1]
               .split()[0])
    _my(DST, f"set global max_allowed_packet = {want}")
    got = move()
    assert got.exit_code == 0, got.output
    assert _my(DST, "select length(b), b = repeat(char(0), 8 * 1024 * 1024)"
                    " from appdb.t where id = 1") == f"{8 * MIB}\t1"


def test_the_value_is_twice_the_row_and_the_rest_in_whole_mib(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop(tmp_path))
    assert eng._packet_to_set(8 * MIB) == 18 * MIB
    assert eng._packet_to_set(8 * MIB + 1) == 19 * MIB
    assert eng._packet_to_set(900 * MIB) == 1024 ** 3
