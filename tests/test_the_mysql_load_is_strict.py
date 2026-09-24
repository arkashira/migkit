"""The MySQL bulk load stops on a value the target cannot hold, instead
of changing it.

Left to itself the dump takes the source's sql_mode without its strict
part, writes that at the head of every data file, and the load runs under
it. Measured on 8.4, onto a target column of the wrong type, length or
kind: `'z'` landed as `0`, `'12345678901'` as `'12345'`, and `'2026-13-45'`
as `0000-00-00`. The move said complete. The load now runs strict, so the
first such row stops it with the server's own words. What a lax source
legitimately holds still lands exactly as it is: zero dates, a zero day
inside a date, and a key of 0.
"""
import shutil
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

MY, PORT = "migkit-test-mystrict", 15711


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
                    "mysql:8.4"], check=True, capture_output=True)
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


def _hop(tmp_path, **mapping):
    ep = Endpoint(host="127.0.0.1", port=PORT, user="root", password="test")
    hop = Hop(name="st", engine="mysql", source=ep, target=ep,
              databases=["cx"], db_map={"cx": "cy"}, mapping=mapping)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _fresh():
    my("drop database if exists cx; drop database if exists cy;"
       " create database cx; create database cy")


@pytest.mark.usefixtures("server")
@pytest.mark.parametrize("target_type,value", [
    ("int", "z"),                    # was loaded as 0
    ("varchar(5)", "12345678901"),   # was loaded as 12345
    ("date", "2026-13-45"),          # was loaded as 0000-00-00
])
def test_a_value_the_target_cannot_hold_stops_the_load(tmp_path,
                                                       target_type, value):
    from migkit import movers
    _fresh()
    my("create table cx.t (id int primary key, v varchar(20));"
       f" insert into cx.t values (1, '{value}');"
       f" create table cy.t (id int primary key, v {target_type})")
    with pytest.raises(RuntimeError) as e:
        movers.mydumper_move(_hop(tmp_path), "cx", 2, True, lambda m: None)
    assert "ERROR 1366" in str(e.value) or "ERROR 1406" in str(e.value) \
        or "ERROR 1292" in str(e.value), e.value
    assert my("select count(*) from cy.t") == "0"


@pytest.mark.usefixtures("server")
def test_what_a_lax_source_holds_lands_as_it_is(tmp_path):
    from migkit import movers
    _fresh()
    my("set session sql_mode = 'NO_AUTO_VALUE_ON_ZERO';"
       " create table cx.t (id int auto_increment primary key, d date,"
       "  ts datetime, v varchar(10));"
       " insert into cx.t values (0, '0000-00-00', '2026-00-15 00:00:00',"
       "  'zero'), (5, '2026-01-02', '0000-00-00 00:00:00', 'x');"
       " create table cy.t (id int auto_increment primary key, d date,"
       "  ts datetime, v varchar(10))")
    movers.mydumper_move(_hop(tmp_path), "cx", 2, True, lambda m: None)
    want = my("select id, d, ts, v from cx.t order by id")
    assert my("select id, d, ts, v from cy.t order by id") == want
    assert want.startswith("0\t0000-00-00\t2026-00-15 00:00:00\tzero"), want


def test_the_dump_is_read_strict_and_still_filtered(tmp_path):
    from migkit import movers
    got = movers.mydumper_session(_hop(tmp_path, where={"t": "id > 1"}),
                                  "cx")
    assert got.startswith("[mydumper_session_variables]\nsql_mode ="
                          " 'NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES'\n"), got
    assert "[`cx`.`t`]\nwhere = id > 1" in got, got
    steps = movers.mydumper_move(_hop(tmp_path), "cx", 2, False, None)
    argv = next(s.argv for s in steps if getattr(s, "argv", None))
    assert "--defaults-file" in argv, argv


@pytest.mark.usefixtures("server")
def test_the_table_copier_is_strict_on_a_lax_target(tmp_path, monkeypatch):
    """A managed 5.7's default mode is lax; the copier writes under its
    own. Measured before: `'12345678901'` written into a varchar(5) as
    `'12345'`, and the move complete."""
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    _fresh()
    default = my("select @@global.sql_mode")
    my("create table cx.t (id int primary key, v varchar(20));"
       " insert into cx.t values (1, '12345678901');"
       " create table cy.t (id int primary key, v varchar(5));"
       " set global sql_mode = 'NO_ENGINE_SUBSTITUTION'")
    try:
        conf = tmp_path / "hops.yaml"
        conf.write_text(
            "hops:\n  st:\n    engine: mysql\n"
            f"    source: {{host: 127.0.0.1, port: {PORT}, user: root,"
            " password: test}\n"
            f"    target: {{host: 127.0.0.1, port: {PORT}, user: root,"
            " password: test}\n"
            "    databases: [cx]\n    db_map: {cx: cy}\n")
        monkeypatch.setattr(cfg, "CONF", str(conf))
        monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
        monkeypatch.setenv("MIGKIT_MOVER", "builtin")
        got = CliRunner().invoke(cli.main, ["move", "st", "--go"])
    finally:
        my(f"set global sql_mode = '{default}'")
    said = got.output + str(got.exception or "")
    assert got.exit_code != 0, said
    assert "ERROR 1406: Data too long for column 'v'" in said, said
    # said, not a traceback
    assert isinstance(got.exception, SystemExit), repr(got.exception)
    assert my("select count(*) from cy.t") == "0"
