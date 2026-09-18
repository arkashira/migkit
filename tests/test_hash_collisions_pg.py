"""The same guarantee as `test_hash_collisions_mysql.py`, on PostgreSQL.

PostgreSQL reached it by a different route and was measured to already have
it for the row hash: `ROW(a, b)::text` renders ('x#y','z') as `(x#y,z)` and
('x','y#z') as `(x,y#z)`, quoting embedded separators rather than losing
them. That is worth asserting rather than trusting, because the whole point of
the guarantee is that it must hold on every engine.

The key hash did not have it - it joined with `chr(2)` and stood in for NULL
with `chr(1)`, and `coalesce(chr(1), chr(1)) = coalesce(null, chr(1))` was
measured true. Unlikely in a primary key, and exactly as wrong.
"""
import socket
import subprocess
import time

import pytest

NAME = "migkit-test-collide-pg"
PORT = 15473


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


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _sql(db, sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", NAME,
                        "psql", "-U", "postgres", "-d", db, "-At",
                        "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pg():
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    assert _wait(PORT)
    for _ in range(45):
        r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", NAME,
                            "psql", "-U", "postgres", "-c", "select 1"],
                           capture_output=True)
        if r.returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("postgres never accepted a connection")
    yield
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"srcdb": "dstdb"})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _load(src_rows, dst_rows, cols="id int primary key, a text, b text"):
    for db, rows in (("srcdb", src_rows), ("dstdb", dst_rows)):
        _sql("postgres", f"drop database if exists {db}")
        _sql("postgres", f"create database {db}")
        _sql(db, f"create table t ({cols}); insert into t values {rows}")
        assert int(_sql(db, "select count(*) from t")) > 0


def _line(tmp_path):
    rc, out = _engine(tmp_path)._data_fast_native("srcdb")
    hit = [l for l in out.splitlines() if l.startswith("public.t:")]
    assert hit, out
    return rc, hit[0]


def test_a_separator_inside_a_value_does_not_read_as_identical(pg, tmp_path):
    _load("(1,'x#y','z')", "(1,'x','y#z')")
    rc, line = _line(tmp_path)
    assert rc == 1 and ": DIFF" in line, line


def test_the_encodings_own_separator_is_handled(pg, tmp_path):
    _load("(1,'x|y','z')", "(1,'x','y|z')")
    rc, line = _line(tmp_path)
    assert rc == 1 and ": DIFF" in line, line


def test_a_null_is_not_the_text_that_stands_for_it(pg, tmp_path):
    _load("(1,NULL,'q')", "(1,'~null~','q')")
    rc, line = _line(tmp_path)
    assert rc == 1 and ": DIFF" in line, line


def test_an_empty_string_is_not_a_null(pg, tmp_path):
    _load("(1,'','q')", "(1,NULL,'q')")
    rc, line = _line(tmp_path)
    assert rc == 1 and ": DIFF" in line, line


def test_a_control_character_in_a_key_is_not_read_as_a_null(pg, tmp_path):
    """The key hash used chr(1) to stand in for NULL, so a key holding chr(1)
    hashed as if it were missing."""
    cols = "k text, j text, v text, primary key (k, j)"
    _load("(chr(1),'x','v1'),('a','y','v2')",
          "(chr(1),'x','CHANGED'),('a','y','v2')", cols=cols)
    rc, line = _line(tmp_path)
    assert rc == 1 and ": DIFF" in line, line
    # and the shape is named, which is what the key hash exists for
    assert "kind=values-changed" in line, line


def test_genuinely_identical_tables_still_check_out(pg, tmp_path):
    rows = ("(1,'x#y','z'),(2,NULL,'q'),(3,'','~null~'),"
            "(4,'a|b','c:d'),(5,chr(1),chr(2))")
    _load(rows, rows)
    rc, line = _line(tmp_path)
    assert rc == 0 and ": OK" in line, line


def test_the_key_expression_no_longer_leans_on_control_characters(pg,
                                                                  tmp_path):
    _load("(1,'a','b')", "(1,'a','b')")
    expr = _engine(tmp_path)._key_hash_expr("src", "srcdb", "public.t")
    assert expr, "no key expression was produced for a table with a key"
    assert "chr(1)" not in expr and "chr(2)" not in expr, expr
    assert "length(" in expr, expr
