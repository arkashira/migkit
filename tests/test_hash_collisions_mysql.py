"""Two different tables must never check out as identical.

This is the test that was missing. Before the row encoding became injective,
migkit compared a source holding ('x#y','z') and (NULL,'q') against a target
holding ('x','y#z') and ('~null~','q') - every row different - and reported
`rows 2==2, checksum 810ced44==810ced44`. Every assertion here is the
end-to-end verdict, not the expression, because the expression being right is
only interesting if the answer is.

A false positive costs a re-read. This direction costs a migration that was
signed off as verified.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-collide-src", "migkit-test-collide-dst"
SRC_PORT, DST_PORT = 13481, 13482


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


def _mysql(name, sql, db="shop"):
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
                                "-ptest", "-h127.0.0.1",
                                "--protocol=tcp", "-e", "select 1"],
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
    hop = Hop(name="c", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              db_map={"shop": "shop"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _load(src_rows, dst_rows, cols="id int primary key,"
                                   " a varchar(60), b varchar(60)"):
    for n, rows in ((SRC, src_rows), (DST, dst_rows)):
        _mysql(n, "drop database if exists shop; create database shop",
               db="mysql")
        _mysql(n, f"create table t ({cols}); insert into t values {rows}")
        # a seed that silently failed would make every verdict below
        # meaningless, and "identical" is exactly what an empty pair looks like
        assert int(_mysql(n, "select count(*) from t")) > 0


def _verdict(tmp_path):
    rs = _engine(tmp_path).check_data("shop")
    hits = [r for r in rs if r.scope.endswith(".t")]
    assert hits, [r.scope for r in rs]
    return hits[0]


def test_a_separator_inside_a_value_does_not_read_as_identical(pair, tmp_path):
    """('x#y','z') against ('x','y#z'). The exact pair that used to pass."""
    _load("(1,'x#y','z')", "(1,'x','y#z')")
    r = _verdict(tmp_path)
    assert r.status == "diff", r.detail


def test_a_null_is_not_equal_to_the_text_that_used_to_stand_for_it(pair,
                                                                  tmp_path):
    _load("(1,NULL,'q')", "(1,'~null~','q')")
    r = _verdict(tmp_path)
    assert r.status == "diff", r.detail


def test_the_new_separator_inside_a_value_is_handled_too(pair, tmp_path):
    """Changing `#` for another character would only move the collision."""
    _load("(1,'x|y','z')", "(1,'x','y|z')")
    r = _verdict(tmp_path)
    assert r.status == "diff", r.detail


def test_a_value_shaped_like_the_encoding_is_handled(pair, tmp_path):
    """A value that is itself a well-formed encoding of something else - the
    case escaping schemes get wrong and length prefixes do not."""
    _load("(1,'1:x|1:y','z')", "(1,'1:x','1:y|z')")
    r = _verdict(tmp_path)
    assert r.status == "diff", r.detail


def test_an_empty_string_is_not_a_null(pair, tmp_path):
    _load("(1,'','q')", "(1,NULL,'q')")
    r = _verdict(tmp_path)
    assert r.status == "diff", r.detail


def test_genuinely_identical_tables_still_check_out(pair, tmp_path):
    """The fix must not have bought correctness by calling everything
    different - including for values full of separators and nulls."""
    rows = "(1,'x#y','z'),(2,NULL,'q'),(3,'','~null~'),(4,'a|b','c:d')"
    _load(rows, rows)
    r = _verdict(tmp_path)
    assert r.status == "ok", r.detail


def test_a_tab_in_a_primary_key_does_not_corrupt_the_drilldown(pair,
                                                               tmp_path):
    """The drilldown builds a key from the primary-key columns and takes it
    apart again to write a WHERE clause. It used to join on a tab and split on
    a tab, so a tab inside a key value shifted every field along by one."""
    from migkit import rowtext
    cols = "k varchar(40), j varchar(40), v varchar(40), primary key (k, j)"
    _load("('a\tb','c','v1'),('a','b\tc','v2')",
          "('a\tb','c','CHANGED'),('a','b\tc','v2')", cols=cols)
    r = _verdict(tmp_path)
    assert r.status == "diff", r.detail

    # the round trip the drilldown depends on, over keys the old split broke
    eng = _engine(tmp_path)
    keyexpr = eng._key_expr("shop", "t")
    assert keyexpr, "composite primary key was not detected"
    # through the driver, not the CLI: `mysql -N` prints a real tab as the
    # two characters \t, which would make this fail for a reason that has
    # nothing to do with migkit
    rows = eng._q("src", f"select {keyexpr} from `shop`.`t` order by k, j")
    encoded = [r[0] for r in rows]
    assert len(encoded) == 2, encoded
    assert any("\t" in e for e in encoded), encoded   # the tab survived
    assert sorted(rowtext.parse(e) for e in encoded) == \
        sorted([["a", "b\tc"], ["a\tb", "c"]]), encoded
