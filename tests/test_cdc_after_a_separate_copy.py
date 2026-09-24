"""`move --mode cdc` after an earlier `move --mode full` starts where that
copy left the source, not from whenever it is run.

A full copy on its own recorded nothing about where the source's log was.
A stream started later began at the log's end, and what the source changed
between the copy and that moment was carried by nothing. On MySQL with
GTID on it was worse: auto-positioning asked the source for everything,
because the target's own GTID set does not hold the source's.

Now every full copy records when it ran and - where asking costs the
source nothing - where the log was before it. A copy that was a snapshot
at a position of its own records that position. `--mode cdc` then:
* starts a replica exactly there. Measured on 8.4 with GTID on: an insert
  and an update made after the dump arrived, and the replica kept running
* starts a change tail from the position taken before the copy, and
  replays by key
* says plainly, on PostgreSQL, that a subscription made now carries only
  what changes from now on
"""
import json
import socket
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import movers
from migkit.config import Endpoint, Hop

NET = "migkit-test-cdcsep-net"
SRC, DST = "migkit-test-cdcsep-src", "migkit-test-cdcsep-dst"
SRC_PORT, DST_PORT = 15719, 15720


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


def my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    if not _docker():
        pytest.skip("docker not available")
    if not (movers.which("mydumper") and movers.which("myloader")):
        pytest.skip("the MySQL dump programs are not installed")
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    for name, port, sid in ((SRC, SRC_PORT, 21), (DST, DST_PORT, 22)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "--network",
                        NET, "-e", "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"{port}:3306", "mysql:8.4", f"--server-id={sid}",
                        "--gtid-mode=ON", "--enforce-gtid-consistency=ON"],
                       check=True, capture_output=True)
    try:
        for name, port in ((SRC, SRC_PORT), (DST, DST_PORT)):
            end = time.time() + 180
            while time.time() < end:
                ok = subprocess.run(["docker", "exec", name, "mysql",
                                     "-uroot", "-ptest", "-h127.0.0.1",
                                     "--protocol=tcp", "-e", "select 1"],
                                    capture_output=True).returncode == 0
                with socket.socket() as s:
                    s.settimeout(2)
                    if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                        break
                time.sleep(2)
            else:
                pytest.fail(f"{name} never answered")
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _hop_file(tmp_path, monkeypatch):
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  sep:\n    engine: mysql\n"
        f"    source: {{host: 127.0.0.1, port: {SRC_PORT}, user: root,"
        " password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {DST_PORT}, user: root,"
        " password: test}\n"
        "    databases: [appdb]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path / "reports" / "sep" / "appdb"


def test_a_replica_after_a_separate_copy_starts_where_it_ended(
        pair, tmp_path, monkeypatch):
    from migkit import cli
    from migkit.config import get_hop
    from migkit.engines.mysql import MySQLEngine
    my(SRC, "drop database if exists appdb; create database appdb;"
            " create table appdb.t (id int primary key, v int);"
            " insert into appdb.t values (1, 1), (2, 2), (3, 3)")
    my(DST, "stop replica; reset replica all; drop database if exists appdb")
    where = _hop_file(tmp_path, monkeypatch)
    got = CliRunner().invoke(cli.main, ["move", "sep", "--go"])
    assert got.exit_code == 0, got.output
    rec = json.loads((where / "copy-position.json").read_text())
    assert rec["exact"]["log_file"] and rec["exact"]["log_pos"], rec
    # the source moves on before any stream is started
    my(SRC, "insert into appdb.t values (4, 4);"
            " update appdb.t set v = 10 where id = 1")
    plan = MySQLEngine(get_hop("sep")).replicate_sql("appdb", False, "pw",
                                                     copied=rec)
    stmt = plan["dst"][0]
    assert f"SOURCE_LOG_FILE = '{rec['exact']['log_file']}'" in stmt, stmt
    assert f"SOURCE_LOG_POS = {rec['exact']['log_pos']}" in stmt, stmt
    assert "SOURCE_AUTO_POSITION = 0" in stmt, stmt
    # run it as written, reaching the source by its name on the network
    my(SRC, "create user if not exists 'migkit_repl'@'%' identified by 'pw';"
            " grant replication slave on *.* to 'migkit_repl'@'%'")
    my(DST, stmt.replace("SOURCE_HOST = '127.0.0.1'", f"SOURCE_HOST = '{SRC}'")
            .replace(f"SOURCE_PORT = {SRC_PORT}", "SOURCE_PORT = 3306")
            + " start replica;")
    for _ in range(30):
        if my(DST, "select group_concat(concat(id, ':', v) order by id)"
                   " from appdb.t") == "1:10,2:2,3:3,4:4":
            break
        time.sleep(1)
    status = subprocess.run(["docker", "exec", DST, "mysql", "-uroot",
                             "-ptest", "-e", "show replica status\\G"],
                            capture_output=True, text=True).stdout
    assert "Replica_SQL_Running: Yes" in status, status
    assert my(DST, "select group_concat(concat(id, ':', v) order by id)"
                   " from appdb.t") == "1:10,2:2,3:3,4:4"


def test_a_copy_with_no_position_of_its_own_is_said(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="n", engine="mysql",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"), databases=["appdb"])
    eng = MySQLEngine(hop)
    eng._binlog_position = lambda side: ("binlog.000009", 44)
    eng._gtid_state = lambda brand: (False, "gtid OFF")
    eng._brands = lambda: [type("B", (), {"name": "mysql"})()]
    plan = eng.replicate_sql("appdb", False, "pw",
                             copied={"at": "2026-09-24 10:00:00",
                                     "before": None})
    assert "was not taken at a position of its own" in plan["note"], plan
    assert "--mode full+cdc" in plan["note"], plan


def test_postgres_says_a_new_subscription_misses_the_gap():
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="n", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"), databases=["appdb"])
    plan = PostgresEngine(hop).replicate_sql(
        "appdb", False, copied={"at": "2026-09-24 10:00:00", "before": None})
    assert "carried by nothing" in plan["note"], plan
    assert "note" not in PostgresEngine(hop).replicate_sql("appdb", False)


def test_a_tail_starts_from_the_position_before_the_copy(tmp_path,
                                                         monkeypatch):
    from migkit import cli
    hop = Hop(name="t", engine="mongodb",
              source=Endpoint(host="10.0.0.1", port=1, user="",
                              password=""),
              target=Endpoint(host="10.0.0.2", port=2, user="",
                              password=""), databases=["app"])
    hop.report_dir = lambda db=None: tmp_path
    (tmp_path / "copy-position.json").write_text(json.dumps(
        {"at": "2026-09-24 10:00:00", "before": "8264F0AA"}))
    seen = {}

    class Eng:
        def tail_seed(self, db, path, point):
            seen["seed"] = point
            path.write_text("{}")

        def tail_apply(self, db, go, path, log):
            seen["apply"] = path.exists()

    monkeypatch.setattr(cli, "_tail_token", lambda h, d: tmp_path / "tok")
    cli._tail(hop, Eng(), "app", True)
    assert seen == {"seed": "8264F0AA", "apply": True}, seen
    # used once: a second stream would replay what the first applied
    assert not (tmp_path / "copy-position.json").exists()
    assert (tmp_path / "copy-position.used.json").exists()
