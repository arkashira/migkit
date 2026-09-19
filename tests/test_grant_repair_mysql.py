"""Replaying grants the mover did not carry, on a real MySQL pair.

The statement applied is the one the source server printed, with only the
database name swapped. Reassembling a GRANT from the canonical comparison
form would mean re-deriving the quoting - and the quoting is exactly what
differs between servers, which is why the canonical form exists at all.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-grantfix-src", "migkit-test-grantfix-dst"
SRC_PORT, DST_PORT = 13471, 13472


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


def _mysql(name, sql, db="mysql"):
    r = subprocess.run(["docker", "exec", name, "mysql", "-uroot", "-ptest",
                        "-N", db, "-e", sql], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        for _ in range(90):
            r = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                "-ptest", "-e", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="g", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              db_map={"shop": "shop"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _fresh(src_grants="", dst_grants=""):
    for n, g in ((SRC, src_grants), (DST, dst_grants)):
        _mysql(n, "drop database if exists shop; create database shop;"
                  " drop user if exists 'app'@'%';"
                  " create user 'app'@'%' identified by 'x';")
        _mysql(n, "create table t (id int primary key);", db="shop")
        if g:
            _mysql(n, g)
    assert _mysql(SRC, "select count(*) from information_schema.tables"
                       " where table_schema='shop'") == "1"


def _privs(name):
    rows = _mysql(name, "show grants for 'app'@'%'")
    return sorted(r for r in rows.splitlines() if "shop" in r)


def _plan(tmp_path):
    acts = [a for a in _engine(tmp_path).repair_plan("shop", "all")
            if a.kind == "grants"]
    return acts[0] if acts else None


def test_a_missing_grant_is_replayed_and_undone(pair, tmp_path):
    _fresh(src_grants="grant select, insert on shop.* to 'app'@'%';")
    assert _privs(SRC) and not _privs(DST), (_privs(SRC), _privs(DST))

    eng = _engine(tmp_path)
    act = _plan(tmp_path)
    assert act is not None, "no grant repair was planned"
    assert any(s.upper().startswith("GRANT") for s in act.statements), \
        act.statements
    eng.apply("shop", act)
    assert _privs(DST), "the grant was not applied"
    assert "SELECT" in _privs(DST)[0] and "INSERT" in _privs(DST)[0]

    for s in act.undo:
        _mysql(DST, s)
    assert not _privs(DST), _privs(DST)


def test_matching_grants_plan_nothing(pair, tmp_path):
    g = "grant select on shop.* to 'app'@'%';"
    _fresh(src_grants=g, dst_grants=g)
    assert _privs(DST), "the seed granted nothing on the target"
    assert _plan(tmp_path) is None


def test_a_user_missing_on_the_target_is_left_to_the_users_command(pair,
                                                                  tmp_path):
    """Granting to an account that does not exist would just fail, and the
    users check already reports it."""
    _fresh(src_grants="grant select on shop.* to 'app'@'%';")
    _mysql(DST, "drop user 'app'@'%'")
    act = _plan(tmp_path)
    assert act is None, act.statements if act else None


def test_the_statement_carries_the_targets_database_name(pair, tmp_path):
    """A hop may map the database to a different name, and replaying the
    source's own text unchanged would grant on a database that is not there."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    _mysql(SRC, "drop database if exists shopsrc; create database shopsrc;"
                " drop user if exists 'app'@'%';"
                " create user 'app'@'%' identified by 'x';"
                " grant select on shopsrc.* to 'app'@'%';")
    _mysql(SRC, "create table t (id int primary key);", db="shopsrc")
    _mysql(DST, "drop database if exists shopdst; create database shopdst;"
                " drop user if exists 'app'@'%';"
                " create user 'app'@'%' identified by 'x';")
    _mysql(DST, "create table t (id int primary key);", db="shopdst")
    hop = Hop(name="g", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              db_map={"shopsrc": "shopdst"})
    hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    acts = [a for a in eng.repair_plan("shopsrc", "all") if a.kind == "grants"]
    assert acts, "no grant repair for the mapped database"
    stmt = acts[0].statements[0]
    assert "shopdst" in stmt and "shopsrc" not in stmt, stmt
    eng.apply("shopsrc", acts[0])
    assert any("shopdst" in r for r in _mysql(DST, "show grants for 'app'@'%'")
               .splitlines())


def test_an_unknown_repair_kind_refuses_rather_than_guessing(pair, tmp_path):
    """The row branch used to be the fallthrough, so a kind it had never been
    taught was read as a table name."""
    from migkit.engines.base import RepairAction
    eng = _engine(tmp_path)
    with pytest.raises(RuntimeError, match="no way to apply"):
        eng.apply("shop", RepairAction("shop", "invented", ["SELECT 1;"]))


def test_a_privilege_set_holds_statements_not_characters(pair, tmp_path):
    """For one commit the comprehension indexed the statement text again and
    every user's privilege set came out as {'G'} - which compares equal
    between any two servers that grant anything at all."""
    _fresh(src_grants="grant select, insert on shop.* to 'app'@'%';")
    src, err = _engine(tmp_path)._grants_for_db("src", "shop")
    assert not err, err
    assert "app@%" in src, src
    privs = src["app@%"]
    assert all(len(p) > 5 for p in privs), privs
    assert any("SELECT" in p and "INSERT" in p for p in privs), privs


def test_a_user_with_nothing_on_the_target_is_not_called_missing(pair,
                                                                 tmp_path):
    """It exists. It simply lost every grant it had on this database, which is
    the worst case, not an absent account - and it used to be reported as
    'does not exist on target', pointing at the wrong fix."""
    _fresh(src_grants="grant select, insert on shop.* to 'app'@'%';")
    assert _mysql(DST, "select count(*) from mysql.user where user='app'") \
        == "1", "the target account is missing, so this tests nothing"
    results = _engine(tmp_path)._check_grants("shop")
    assert len(results) == 1
    r = results[0]
    assert r.status == "diff", r.detail
    assert "do not exist on target" not in r.detail, r.detail
    assert "missing on target" in r.detail, r.detail
    assert "app@%" in r.detail
