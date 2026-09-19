"""The replication statements, run against the server they were written for.

migkit generated one plan for everything in the MySQL family. Measured, that
plan is not a near-miss on MariaDB - it is a syntax error, and the two dialects
reject each other's form with the same code:

    MariaDB 11.8   change replication source to ...   ERROR 1064
    MySQL 8        change master to ... MASTER_USE_GTID   ERROR 1064

Underneath it was a quieter mistake. MySQL has a `gtid_mode` variable; MariaDB
has none at all, and `show variables like 'gtid_mode'` returns **zero rows**
there rather than a row saying OFF. The plan read that empty result as falsy
and decided GTID was off, on a server where GTID is always available once the
binary log is on.

So the tests here do not read the generated text and call it correct. They run
it.
"""
import socket
import subprocess
import time

import pytest

MY, MARIA = "migkit-test-repl-my", "migkit-test-repl-maria"
MY_PORT, MARIA_PORT = 13347, 13345


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def run_sql(container, client, sql):
    return subprocess.run(
        ["docker", "exec", container, client, "-uroot", "-ptest",
         "--default-character-set=utf8mb4", "-e", sql],
        capture_output=True, text=True)


def _engine(port):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=port, user="root", password="test")
    return MySQLEngine(Hop(name="repl", engine="mysql", source=ep, target=ep,
                           db_map={"cx": "cx"}))


@pytest.fixture(scope="module")
def servers():
    for n in (MY, MARIA):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8", "--log-bin=binlog", "--server-id=1"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MARIA, "-e",
                    "MARIADB_ROOT_PASSWORD=test", "-p",
                    f"{MARIA_PORT}:3306", "mariadb:11",
                    "--log-bin=binlog", "--server-id=1"],
                   check=True, capture_output=True)
    # from here on the containers exist, so every exit has to go through
    # the teardown - including the `pytest.fail` below, which used to leave
    # a MySQL and a MariaDB server running for the rest of the day
    try:
        assert _wait(MY_PORT) and _wait(MARIA_PORT)
        for container, client in ((MY, "mysql"), (MARIA, "mariadb")):
            for _ in range(60):
                if run_sql(container, client, "select 1").returncode == 0:
                    break
                time.sleep(2)
            else:
                pytest.fail(f"{container} never answered")
        yield
    finally:
        for n in (MY, MARIA):
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def test_gtid_mode_is_a_variable_on_one_and_absent_on_the_other(servers):
    """The measurement the silent half of the bug comes from. An empty result
    is not the same answer as OFF, and reading it as falsy made them one."""
    got_my = run_sql(MY, "mysql", "show variables like 'gtid_mode'").stdout
    got_maria = run_sql(MARIA, "mariadb",
                        "show variables like 'gtid_mode'").stdout
    assert "gtid_mode" in got_my and "OFF" in got_my, got_my
    assert "gtid_mode" not in got_maria, got_maria
    # and MariaDB's own names are there instead
    names = run_sql(MARIA, "mariadb",
                    "select @@gtid_binlog_pos, @@gtid_current_pos")
    assert names.returncode == 0, names.stderr


def test_each_dialect_is_a_syntax_error_on_the_other_server(servers):
    """Not a behaviour difference to work around - a hard parse failure, in
    both directions."""
    mysql_form = ("change replication source to SOURCE_HOST='x',"
                  " SOURCE_PORT=1, SOURCE_USER='u', SOURCE_PASSWORD='p',"
                  " GET_SOURCE_PUBLIC_KEY=1, SOURCE_AUTO_POSITION=1")
    maria_form = ("change master to MASTER_HOST='127.0.0.1', MASTER_PORT=1,"
                  " MASTER_USER='u', MASTER_PASSWORD='p',"
                  " MASTER_USE_GTID=current_pos")
    on_maria = run_sql(MARIA, "mariadb", mysql_form)
    assert on_maria.returncode != 0 and "1064" in on_maria.stderr, on_maria
    on_mysql = run_sql(MY, "mysql", maria_form)
    assert on_mysql.returncode != 0 and "1064" in on_mysql.stderr, on_mysql


def test_the_plan_mariadb_gets_actually_runs_on_mariadb(servers):
    """The whole point: the statements are executed, not read."""
    eng = _engine(MARIA_PORT)
    assert eng._brands()[0].name == "mariadb"
    plan = eng.replicate_sql("cx")
    assert "written for mariadb" in plan["note"], plan["note"]
    assert any("change master to" in c for c in plan["dst"]), plan["dst"]
    assert not any("GET_SOURCE_PUBLIC_KEY" in c for c in plan["dst"])

    try:
        for statement in plan["src"] + plan["dst"]:
            got = run_sql(MARIA, "mariadb", statement)
            assert "1064" not in got.stderr, (statement, got.stderr)
        status = run_sql(MARIA, "mariadb", plan["status"])
        assert "1064" not in status.stderr, status.stderr
    finally:
        for statement in plan["drop_dst"] + plan["drop_src"]:
            run_sql(MARIA, "mariadb", statement)


def test_the_plan_mysql_gets_actually_runs_on_mysql(servers):
    eng = _engine(MY_PORT)
    assert eng._brands()[0].name == "mysql"
    plan = eng.replicate_sql("cx")
    assert "written for mysql" in plan["note"], plan["note"]
    assert any("change replication source to" in c for c in plan["dst"])

    try:
        for statement in plan["src"] + plan["dst"]:
            got = run_sql(MY, "mysql", statement)
            assert "1064" not in got.stderr, (statement, got.stderr)
        status = run_sql(MY, "mysql", plan["status"])
        assert "1064" not in status.stderr, status.stderr
    finally:
        for statement in plan["drop_dst"] + plan["drop_src"]:
            run_sql(MY, "mysql", statement)


def test_mariadb_is_not_reported_as_having_gtid_switched_off(servers):
    """The silent half. The old plan said `gtid OFF` because a query for a
    variable that does not exist came back empty."""
    eng = _engine(MARIA_PORT)
    on, note = eng._gtid_state("mariadb")
    assert on is True
    assert "no gtid_mode to switch" in note, note
    assert "gtid OFF" not in eng.replicate_sql("cx")["note"]


def test_a_brand_with_no_known_gtid_equivalent_says_it_is_guessing(servers):
    """If some other MySQL-protocol server also lacks `gtid_mode`, migkit
    falls back to file coordinates - and says that the fallback is the safe
    direction rather than a measured fact."""
    eng = _engine(MARIA_PORT)
    on, note = eng._gtid_state("some-other-fork")
    assert on is False
    assert "does not know this brand's equivalent" in note, note
    assert "may not be the true one" in note, note
