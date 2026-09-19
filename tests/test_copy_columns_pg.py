"""The builtin copy must land values in the columns they came from.

COPY maps by position unless the columns are named, and position is not
something the two sides agree on. A source whose history includes a DROP
COLUMN stores its columns in a different physical order from a target created
fresh out of the same logical schema - migkit has already measured that exact
pairing happening, when the row hash was made order-independent.

Before the column list, that pairing produced a target where every value was
in the wrong column, with no error from anything.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-ccol-src", "migkit-test-ccol-dst"
SRC_PORT, DST_PORT = 15417, 15418


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


def _sql(name, db, sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                        "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n,
                        "-e", "POSTGRES_PASSWORD=test", "-p", f"{p}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        for _ in range(45):
            r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", n,
                                "psql", "-U", "postgres", "-c", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
        _sql(n, "postgres", "create database app")
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT,
                              user="postgres", password="test"),
              db_map={"app": "app"})
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng._report = lambda db=None: tmp_path
    return eng


def _skewed():
    """The shape that occurs in practice: the source carries the scars of a
    DROP COLUMN, the target was built from today's schema."""
    for n in (SRC, DST):
        _sql(n, "app", "drop table if exists t")
    _sql(SRC, "app", "create table t (id int primary key, junk text,"
                     " a text, b text);"
                     " alter table t drop column junk;"
                     " alter table t add column c text;"
                     " insert into t (id, a, b, c) values"
                     " (1,'AAA','BBB','CCC'),(2,'aa','bb','cc')")
    _sql(DST, "app", "create table t (id int primary key, c text,"
                     " a text, b text)")
    src_order = _sql(SRC, "app", "select string_agg(attname, ',' order by"
                                 " attnum) from pg_attribute where attrelid"
                                 " = 't'::regclass and attnum > 0"
                                 " and not attisdropped")
    dst_order = _sql(DST, "app", "select string_agg(attname, ',' order by"
                                 " attnum) from pg_attribute where attrelid"
                                 " = 't'::regclass and attnum > 0"
                                 " and not attisdropped")
    assert src_order != dst_order, f"the premise failed: both {src_order}"
    assert _sql(SRC, "app", "select count(*) from t") == "2"


def test_values_land_in_the_columns_they_came_from(pair, tmp_path):
    _skewed()
    eng = _engine(tmp_path)
    cols = eng._copy_cols("app", "public", "t")
    assert cols == ["id", "a", "b", "c"], cols
    eng._copy_pipe("app", eng._copy_select('"public"."t"', cols),
                   '"public"."t"', 'truncate "public"."t"', columns=cols)
    got = _sql(DST, "app", "select a||'/'||b||'/'||c from t where id = 1")
    assert got == "AAA/BBB/CCC", got


def test_without_the_column_list_the_values_move_across(pair, tmp_path):
    """The bug this closes, reproduced deliberately so the fix is not
    mistaken for a tidy-up."""
    _skewed()
    eng = _engine(tmp_path)
    eng._copy_pipe("app", 'select * from "public"."t"', '"public"."t"',
                   'truncate "public"."t"')
    got = _sql(DST, "app", "select a||'/'||b||'/'||c from t where id = 1")
    assert got != "AAA/BBB/CCC", "positional COPY no longer swaps values"
    assert got == "BBB/CCC/AAA", got


def test_the_move_path_uses_the_named_columns(pair, tmp_path):
    """End to end through `move_table`, which is what a no-PK or single-table
    move actually calls."""
    _skewed()
    eng = _engine(tmp_path)

    class _Ck(dict):
        """What `move_table` needs from a checkpoint: a place to keep
        per-table state and something to call when it changes."""
        def save(self):
            pass

    eng.move_table("app", "public", "t", 1000, _Ck(), lambda *_: None)
    rows = _sql(DST, "app", "select id||':'||a||'/'||b||'/'||c"
                            " from t order by id")
    assert rows.splitlines() == ["1:AAA/BBB/CCC", "2:aa/bb/cc"], rows


def test_generated_columns_are_left_out(pair, tmp_path):
    """A generated column cannot be written to, so naming it in the COPY
    would make the whole load fail."""
    for n in (SRC, DST):
        _sql(n, "app", "drop table if exists g")
        _sql(n, "app", "create table g (id int primary key, a int,"
                       " dbl int generated always as (a * 2) stored)")
    _sql(SRC, "app", "insert into g (id, a) values (1, 5)")
    eng = _engine(tmp_path)
    cols = eng._copy_cols("app", "public", "g")
    assert cols == ["id", "a"], cols
    eng._copy_pipe("app", eng._copy_select('"public"."g"', cols),
                   '"public"."g"', 'truncate "public"."g"', columns=cols)
    assert _sql(DST, "app", "select a||'/'||dbl from g") == "5/10"
