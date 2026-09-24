"""A native MySQL replica writes only what the hop covers.

The replica migkit set up had no filters, so it applied everything the
source's log carried. Measured on 8.4:
* a write into a database the hop does not name arrived on the target
* a row for a table the hop excludes (one the target owns) was applied too,
  and stopped the replica on a key the target already had

Now the plan limits the replica to the hop's databases, under the names
the hop maps them to, and leaves out the tables it excludes. With the
filters, neither write arrived, and neither did a DROP of the excluded
table. The filters do not survive a restart of the target (measured: the
replica came back by itself with none), so the plan names the lines the
target's configuration needs, and the replica's status says when they are
missing.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

NET = "migkit-test-replscope-net"
SRC, DST = "migkit-test-replscope-src", "migkit-test-replscope-dst"
SRC_PORT, DST_PORT = 15723, 15724


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


def _wait(name, port):
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
def pair():
    if not _docker():
        pytest.skip("docker not available")
    subprocess.run(["docker", "network", "create", NET], capture_output=True)
    for name, port, sid in ((SRC, SRC_PORT, 41), (DST, DST_PORT, 42)):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "--network",
                        NET, "-e", "MYSQL_ROOT_PASSWORD=test", "-p",
                        f"{port}:3306", "mysql:8.4", f"--server-id={sid}"],
                       check=True, capture_output=True)
    try:
        _wait(SRC, SRC_PORT)
        _wait(DST, DST_PORT)
        yield
    finally:
        for name in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
        subprocess.run(["docker", "network", "rm", NET], capture_output=True)


def _eng(target_host="127.0.0.1"):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="scope", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host=target_host, port=DST_PORT, user="root",
                              password="test"),
              databases=["cx"], db_map={"cx": "cy"}, exclude=["cx.audit"])
    return MySQLEngine(hop)


def test_the_replica_writes_only_what_the_hop_covers(pair):
    my(SRC, "create database cx; create database other;"
            " create table cx.t (id int primary key);"
            " create table cx.audit (id int primary key, v varchar(20));"
            " create table other.x (id int primary key)")
    my(DST, "create database cy; create database other;"
            " create table cy.t (id int primary key);"
            " create table cy.audit (id int primary key, v varchar(20));"
            " create table other.x (id int primary key);"
            " insert into cy.audit values (1, 'the target owns this')")
    eng = _eng()
    plan = eng.replicate_sql("cx", False, "pw")
    stmts = plan["dst"]
    filt = [x for x in stmts if x.startswith("change replication filter")]
    assert filt, stmts
    assert "REPLICATE_REWRITE_DB = ((cx, cy))" in filt[0], filt
    assert "REPLICATE_WILD_DO_TABLE = ('cy.%')" in filt[0], filt
    assert "REPLICATE_IGNORE_TABLE = (cy.audit)" in filt[0], filt
    assert "replicate-ignore-table = cy.audit" in plan["note"], plan["note"]
    my(SRC, "create user 'migkit_repl'@'%' identified by 'pw';"
            " grant replication slave on *.* to 'migkit_repl'@'%'")
    for stmt in stmts:
        my(DST, stmt.replace("SOURCE_HOST = '127.0.0.1'",
                             f"SOURCE_HOST = '{SRC}'")
           .replace(f"SOURCE_PORT = {SRC_PORT}", "SOURCE_PORT = 3306"))
    my(SRC, "insert into cx.t values (1); insert into other.x values (9);"
            " insert into cx.audit values (1, 'the source copy');"
            " drop table cx.audit")
    for _ in range(20):
        if my(DST, "select count(*) from cy.t") == "1":
            break
        time.sleep(1)
    time.sleep(2)
    assert my(DST, "select count(*) from cy.t") == "1"
    assert my(DST, "select count(*) from other.x") == "0"
    assert my(DST, "select v from cy.audit") == "the target owns this"
    said = eng.replication_status("cx", plan["status"])
    assert "sql Yes" in said and "NOT" not in said, said


def test_a_restart_that_drops_the_filters_is_said(pair):
    subprocess.run(["docker", "restart", DST], check=True,
                   capture_output=True)
    _wait(DST, DST_PORT)
    said = _eng().replication_status("cx", "show replica status")
    assert "NOT limited to this hop" in said, said


def test_a_managed_target_is_told_the_parameters(pair):
    plan = _eng("db.example.rds.amazonaws.com").replicate_sql("cx", False,
                                                              "pw")
    assert not [x for x in plan["dst"]
                if x.startswith("change replication filter")], plan["dst"]
    assert "parameter group" in plan["note"], plan["note"]
    assert "replicate-wild-do-table = cy.%" in plan["note"], plan["note"]


def test_mariadb_is_given_its_own_form():
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["cx"], db_map={"cx": "cy"})
    stmts, conf = MySQLEngine(hop)._replica_filter_sql("mariadb")
    assert stmts == ["set global replicate_rewrite_db = 'cx->cy';",
                     "set global replicate_wild_do_table = 'cy.%';"], stmts


def test_a_hop_of_two_databases_sets_up_one_replica(tmp_path, monkeypatch):
    """The plan ran per database: the second one re-issued the replica's
    source while it was running, and gave the replication user a new
    password the running replica did not have."""
    import migkit.config as cfg
    from click.testing import CliRunner

    from migkit import cli
    from migkit.engines.mysql import MySQLEngine
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  two:\n    engine: mysql\n"
        "    source: {host: 10.0.0.1, port: 1, user: u, password: CHANGE_ME}\n"
        "    target: {host: 10.0.0.2, port: 2, user: u, password: CHANGE_ME}\n"
        "    databases: [a, b]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    ran = []
    monkeypatch.setattr(MySQLEngine, "_binlog_position",
                        lambda self, side: ("binlog.000001", 4))
    monkeypatch.setattr(MySQLEngine, "_gtid_state",
                        lambda self, brand: (False, "gtid OFF"))
    monkeypatch.setattr(MySQLEngine, "_brands",
                        lambda self: [type("B", (), {"name": "mysql"})()])
    monkeypatch.setattr(MySQLEngine, "apply_replication_stmt",
                        lambda self, side, d, stmt: ran.append((side, stmt)))
    monkeypatch.setattr(MySQLEngine, "replication_status",
                        lambda self, d, sql: "io Yes, sql Yes")
    got = CliRunner().invoke(cli.main, ["move", "two", "--mode", "cdc",
                                        "--go"])
    assert got.exit_code == 0, got.output + str(got.exception)
    starts = [s for side, s in ran if s.startswith("start replica")]
    assert len(starts) == 1, ran
    secrets = {s.split("identified by '")[1].split("'")[0]
               for side, s in ran if "identified by" in s}
    assert len(secrets) == 1, secrets
    filt = [s for side, s in ran if s.startswith("change replication filter")]
    assert "'a.%', 'b.%'" in filt[0], filt
    assert "carried by the replica set up for a" in got.output, got.output
