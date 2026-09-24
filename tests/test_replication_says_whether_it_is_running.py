""""Replication started" is not the same as "replication is running".

`migkit move HOP --mode cdc --go` prints the statements it runs and then
one line read back from the target. That line was fetched with
`eng._psql(...)`, a method only the PostgreSQL engine has, so the MySQL
path - which is the one `--mode cdc` takes for every MySQL hop, because
MySQL has no `tail_apply` - crashed:

    File "migkit/cli.py", line 925, in _replicate
      console.print("  " + eng._psql("dst", d, sql["status"]))
    AttributeError: 'MySQLEngine' object has no attribute '_psql'

Reproduced against two real MySQL 8.4 containers. The crash lands *after*
both sides' statements have run, so replication is configured and the
changelog entry that records it never gets written.

Fixing the crash is the small half. The half worth the file is what the
line has to say, because the two engines fail in opposite directions:

* PostgreSQL's `CREATE SUBSCRIPTION` dials the source while it runs, so a
  target with no route to the source fails loudly - after 134 seconds,
  measured, and `connect_timeout` in the subscription's own conninfo does
  not shorten it (the same conninfo through plain psql gave up in exactly
  the 10 seconds it was given).
* MySQL's `START REPLICA` returns OK and lets the IO thread fail behind
  it. Measured on 8.4 with the replica pointed at an address it cannot
  reach: the statement succeeded, and `SHOW REPLICA STATUS` said
  `Replica_IO_Running: Connecting` with the timeout in `Last_IO_Error`.

Three states were measured on live pairs and all three are pinned below:

    cannot reach source   io Connecting, sql Yes, Last_IO_Error ..., NOT
    applier dead          io Yes, sql No, Last_SQL_Error ..., NOT
    healthy               io Yes, sql Yes, lag 0s        (and no NOT)

The healthy row is the one that keeps the rest honest: a check that always
says "NOT replicating" would pass the first two tests and be useless.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

# Recorded from `SHOW REPLICA STATUS` on MySQL 8.4, trimmed to the columns
# this reads. The full row is fifty-odd columns wide.
UNREACHABLE = {
    "Source_Host": "10.0.0.5", "Replica_IO_Running": "Connecting",
    "Replica_SQL_Running": "Yes", "Seconds_Behind_Source": 0,
    "Last_IO_Error": "Error connecting to source 'migkit_repl@10.0.0.5:3306'."
                     " This was attempt 1/10, with a delay of 60 seconds"
                     " between attempts. Message: Can't connect to MySQL"
                     " server",
    "Last_SQL_Error": "",
}
APPLIER_DEAD = {
    "Source_Host": "10.0.0.5", "Replica_IO_Running": "Yes",
    "Replica_SQL_Running": "No", "Seconds_Behind_Source": None,
    "Last_IO_Error": "",
    "Last_SQL_Error": "Coordinator stopped because there were error(s) in"
                      " the worker(s). The most recent failure being: Worker"
                      " 1 failed executing transaction",
}
HEALTHY = {
    "Source_Host": "10.0.0.5", "Replica_IO_Running": "Yes",
    "Replica_SQL_Running": "Yes", "Seconds_Behind_Source": 0,
    "Last_IO_Error": "", "Last_SQL_Error": "",
}
# MariaDB answers the same facts under the pre-8.0 names.
MARIA_HEALTHY = {
    "Master_Host": "10.0.0.5", "Slave_IO_Running": "Yes",
    "Slave_SQL_Running": "Yes", "Seconds_Behind_Master": 3,
    "Last_IO_Error": "", "Last_SQL_Error": "",
}


def _mysql(rows, tmp_path=None):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="s", port=3306, user="u", password="p"),
              target=Endpoint(host="t", port=3306, user="u", password="p"),
              databases=["appdb"])
    if tmp_path is not None:
        hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    eng._q_named = lambda side, sql: rows
    return eng


def test_a_replica_that_cannot_reach_the_source_is_not_called_started():
    got = _mysql([UNREACHABLE]).replication_status("appdb", "show replica status")
    assert "NOT replicating" in got, got
    assert "io Connecting" in got, got
    assert "Last_IO_Error" in got and "Can't connect" in got, got


def test_a_dead_applier_is_not_called_started():
    """The other half of the pair: connected, and applying nothing."""
    got = _mysql([APPLIER_DEAD]).replication_status("appdb", "show replica status")
    assert "NOT replicating" in got, got
    assert "io Yes" in got and "sql No" in got, got
    assert "Last_SQL_Error" in got, got


def test_a_working_replica_is_not_cried_over():
    """Measured on a real pair whose rows genuinely arrived. Without this
    the two tests above would pass on a function that always says no."""
    got = _mysql([HEALTHY]).replication_status("appdb", "show replica status")
    assert "NOT replicating" not in got, got
    assert "io Yes" in got and "sql Yes" in got and "lag 0s" in got, got


def test_mariadbs_column_names_are_read_too():
    """`Slave_IO_Running` against `Replica_IO_Running` is not a dialect
    detail here - reading the wrong one returns None, which is not "Yes",
    which would report every MariaDB replica as broken."""
    got = _mysql([MARIA_HEALTHY]).replication_status("appdb", "show slave status")
    assert "NOT replicating" not in got, got
    assert "io Yes" in got and "sql Yes" in got and "lag 3s" in got, got


def test_no_replica_at_all_is_not_silence():
    """`SHOW REPLICA STATUS` answers zero rows when nothing was configured.
    An empty string printed after the statements ran would read as success.
    """
    got = _mysql([]).replication_status("appdb", "show replica status")
    assert "nothing is replicating" in got, got


def test_an_unknown_lag_is_not_reported_as_zero():
    """`Seconds_Behind_Source` is NULL while the applier is down. Zero is
    the one number it must not be turned into."""
    got = _mysql([APPLIER_DEAD]).replication_status("appdb", "show replica status")
    assert "lag unknown" in got, got
    assert "lag 0" not in got, got


def test_the_cli_asks_the_engine_rather_than_a_postgres_only_method():
    """The bug itself. `_psql` exists on PostgresEngine and nowhere else,
    so calling it from the shared path crashed every MySQL cdc run."""
    import pathlib
    cli = (pathlib.Path(__file__).resolve().parents[1] / "migkit" / "cli.py")
    text = cli.read_text()
    assert 'eng.replication_status(d, sql["status"])' in text
    assert 'eng._psql("dst", d, sql["status"])' not in text


def test_every_engine_that_starts_replication_can_report_on_it():
    """The contract, checked against the registry rather than a list kept
    by hand: an engine that emits `replicate_sql` and inherits the base
    `replication_status` would raise at the moment it is needed - after the
    statements have already run."""
    from migkit.engines import NAMES, _class_for
    from migkit.engines.base import Engine
    offered = [n for n in NAMES
               if _class_for(n) and hasattr(_class_for(n), "replicate_sql")]
    assert set(offered) == {"postgres", "mysql"}, offered
    for n in offered:
        cls = _class_for(n)
        assert cls.replication_status is not Engine.replication_status, n


def test_the_base_refuses_rather_than_returning_nothing():
    """An engine that grows `replicate_sql` later must fail loudly here,
    not print an empty line over a replica nobody looked at."""
    from migkit.engines.base import Engine

    class Half(Engine):
        pass

    hop = Hop(name="h", engine="x",
              source=Endpoint(host="s", port=1, user="u", password="p"),
              target=Endpoint(host="t", port=1, user="u", password="p"),
              databases=["d"])
    with pytest.raises(NotImplementedError) as e:
        Half(hop).replication_status("d", "select 1")
    assert "Half" in str(e.value), str(e.value)


def test_named_rows_pair_values_with_the_columns_they_came_from():
    """`_q` returns tuples, and the codebase already says so in
    `_health`: "Replication lag needs the column names, which `_q` does not
    return". Reading a fifty-column row by index is the guess this avoids.
    """
    from migkit.engines.mysql import MySQLEngine

    class Cur:
        description = (("Replica_IO_Running",), ("Last_IO_Error",))

        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [("Connecting", "boom")]

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class Conn:
        def cursor(self):
            return Cur()

        def close(self):
            self.closed = True

    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="s", port=3306, user="u", password="p"),
              target=Endpoint(host="t", port=3306, user="u", password="p"),
              databases=["appdb"])
    eng = MySQLEngine(hop)
    eng._conn = lambda side: Conn()
    assert eng._q_named("dst", "show replica status") == [
        {"Replica_IO_Running": "Connecting", "Last_IO_Error": "boom"}]


PORT = 13431
NAME = "migkit-test-replstatus"


@pytest.fixture(scope="module")
def lone_mysql():
    """One server, told to replicate from an address nothing answers on.
    That is all it takes to show `START REPLICA` reporting success over a
    replica that will never receive a byte."""
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{PORT}:3306",
                    "mysql:8", "--server-id=7", "--log-bin=mysql-bin",
                    "--gtid-mode=ON", "--enforce-gtid-consistency=ON"],
                   check=True, capture_output=True)
    end = time.time() + 300
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", PORT)) == 0:
                break
        time.sleep(2)
    for _ in range(90):
        if subprocess.run(["docker", "exec", NAME, "mysql", "-uroot",
                           "-ptest", "-h127.0.0.1",
                           "--protocol=tcp", "-e", "select 1"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(2)
    else:
        subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
        pytest.fail("mysql never answered")
    yield PORT
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


@needs_docker
def test_start_replica_reports_success_over_a_replica_that_cannot_connect(
        lone_mysql):
    """The measurement the whole file rests on, taken again here so it is
    not just a comment. `start replica` returns OK; the truth is in the
    status row, and migkit's line has to carry it."""
    from migkit.engines.mysql import MySQLEngine
    started = subprocess.run(
        ["docker", "exec", NAME, "mysql", "-uroot", "-ptest", "-e",
         "change replication source to SOURCE_HOST='127.0.0.1',"
         " SOURCE_PORT=1, SOURCE_USER='migkit_repl',"
         " SOURCE_PASSWORD='CHANGE_ME', SOURCE_AUTO_POSITION=1;"
         " start replica;"], capture_output=True, text=True)
    assert started.returncode == 0, started.stderr
    time.sleep(3)
    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=1, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=lone_mysql, user="root",
                              password="test"),
              databases=["mysql"])
    got = MySQLEngine(hop).replication_status("mysql", "show replica status")
    assert "NOT replicating" in got, got
    assert "io Connecting" in got or "io No" in got, got
