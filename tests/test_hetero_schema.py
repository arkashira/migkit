"""The cross-engine hop that never compared its columns.

`HeteroEngine` is the one whose whole job is a pair of *different* engines,
and it declared `checks = ("counts", "data")` - no schema check at all. The
row comparison does notice a column that exists on one side only, and it
reports it as a footnote on a green verdict. Measured, a SQLite source with
a `secret` column the PostgreSQL target does not have:

    OK  main.items
        rows 2 and every compared column equal across sqlite/postgres
        (digest 1475545921195705015)
        columns only on the source, not compared: secret

Every word of that is true - every *compared* column was equal - and a
migration that dropped a whole column passed verification. Nobody reading a
green line has a reason to read the tail of it.

Two engines never spell a type the same way, so the comparison is by column
name and by the **class** each side's declared type renders to: that class
is the only thing both sides can be held to, and it is already what decides
whether a column's values can be compared at all. `INTEGER` against `text`
is a finding; `INTEGER` against `bigint` is not.

The classification is computed once, in `_classify_columns`, and read twice
- the row comparison turns it into a footnote and this turns it into a
verdict. Deciding it in two places is how the footnote and the verdict come
to disagree.
"""
import os
import pathlib
import socket
import sqlite3
import subprocess
import tempfile
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

PORT, NAME = 15609, "migkit-test-hetero-schema"


def _pg(sql, db="postgres"):
    return subprocess.run(["docker", "exec", NAME, "psql", "-U", "postgres",
                           "-d", db, "-At", "-c", sql],
                          capture_output=True, text=True)


@pytest.fixture(scope="module")
def server():
    subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NAME, "-e",
                    "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432",
                    "postgres:16"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    break
            time.sleep(1)
        for _ in range(90):
            if subprocess.run(["docker", "exec", NAME, "pg_isready", "-U",
                               "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail("postgres never answered")
        assert _pg("create database cx").returncode == 0
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NAME], capture_output=True)


def _engine(src_ddl, src_rows, dst_ddl, dst_rows):
    from migkit.engines.hetero import HeteroEngine
    d = tempfile.mkdtemp()
    path = os.path.join(d, "s.db")
    c = sqlite3.connect(path)
    c.execute(src_ddl)
    if src_rows:
        c.executemany(
            f"insert into items values ({','.join('?' * len(src_rows[0]))})",
            src_rows)
    c.commit()
    c.close()
    assert _pg("drop table if exists items", "cx").returncode == 0
    assert _pg(dst_ddl, "cx").returncode == 0
    for row in dst_rows:
        vals = ", ".join("null" if v is None else
                         (repr(v) if not isinstance(v, str)
                          else "'" + v.replace("'", "''") + "'")
                         for v in row)
        assert _pg(f"insert into items values ({vals})", "cx").returncode == 0
    hop = Hop(name="sq", engine="hetero",
              source=Endpoint(host=path, port=0, user="", password=""),
              target=Endpoint(host="127.0.0.1", port=PORT, user="postgres",
                              password="test"),
              db_map={"main": "cx"},
              options={"source_engine": "sqlite",
                       "target_engine": "postgres"})
    hop.report_dir = lambda db=None, _p=pathlib.Path(d): _p
    return HeteroEngine(hop)


def _items(res):
    got = [r for r in res if r.scope.endswith(".items")]
    assert len(got) == 1, [(r.scope, r.status, r.detail) for r in got]
    return got[0]


def test_the_check_is_offered_at_all():
    """It was not. `check --only schema` on a cross-engine hop had nothing
    to run, and a full check skipped the schema in silence."""
    from migkit.engines.hetero import HeteroEngine
    assert "schema" in HeteroEngine.checks


def test_the_classification_has_one_definition():
    """The footnote and the verdict read the same answer. Two of them is
    how they come to disagree."""
    import ast
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "engines" / "base.py").read_text()
    tree = ast.parse(src)
    defs = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
            and n.name == "_classify_columns"]
    assert len(defs) == 1
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
              and n.name == "_comparable_columns")
    called = [n.func.attr for n in ast.walk(fn) if isinstance(n, ast.Call)
              and isinstance(n.func, ast.Attribute)]
    assert "_classify_columns" in called, "the row path derives it again"


@needs_docker
def test_a_column_the_target_does_not_have_is_a_difference(server):
    """The whole point. Before this, the same pair reported OK."""
    eng = _engine("create table items (id integer primary key, name text,"
                  " secret text)", [(1, "a", "p1"), (2, "b", "p2")],
                  "create table items (id bigint primary key, name text)",
                  [(1, "a"), (2, "b")])
    got = _items(eng.check_schema("main"))
    assert got.status == "diff", got.detail
    assert "secret" in got.detail, got.detail
    assert "not carried" in got.detail, got.detail


@needs_docker
def test_and_the_row_check_still_calls_that_pair_ok(server):
    """Not changed, and worth pinning: the sentence is true, and on its own
    it is what let a dropped column through. The schema line is what makes
    the report as a whole honest."""
    eng = _engine("create table items (id integer primary key, name text,"
                  " secret text)", [(1, "a", "p1"), (2, "b", "p2")],
                  "create table items (id bigint primary key, name text)",
                  [(1, "a"), (2, "b")])
    got = _items(eng.check_data("main"))
    assert got.status == "ok", got.detail
    assert "every compared column equal" in got.detail, got.detail
    assert "columns only on the source" in got.detail, got.detail


@needs_docker
def test_two_sides_that_match_pass(server):
    """SQLite numbers an `integer primary key` itself, so the target's key
    is numbered too (`test_tables_built_across_engines_keep_their_rules`
    for what is said when it is not)."""
    eng = _engine("create table items (id integer primary key, name text)",
                  [(1, "a"), (2, "b")],
                  "create table items (id bigint generated by default as"
                  " identity primary key, name text)",
                  [(1, "a"), (2, "b")])
    got = _items(eng.check_schema("main"))
    assert got.status == "ok", got.detail
    assert "2 columns" in got.detail, got.detail


@needs_docker
def test_types_that_are_spelled_differently_are_not_a_difference(server):
    """`INTEGER` against `bigint` is what a cross-engine hop looks like
    when it is right. Reporting it would make the check useless."""
    eng = _engine("create table items (id integer primary key, n integer)",
                  [(1, 5)],
                  "create table items (id bigint generated by default as"
                  " identity primary key, n bigint)",
                  [(1, 5)])
    assert _items(eng.check_schema("main")).status == "ok"


@needs_docker
def test_a_key_the_target_does_not_number_is_named(server):
    """SQLite gives an `integer primary key` its next number when an insert
    leaves it out; a target key that does not refuses that insert after
    cutover."""
    eng = _engine("create table items (id integer primary key, n integer)",
                  [(1, 5)],
                  "create table items (id bigint primary key, n bigint)",
                  [(1, 5)])
    got = _items(eng.check_schema("main"))
    assert got.status == "diff", got.detail
    assert "id is numbered by the source and not by the target" in \
        got.detail, got.detail


@needs_docker
def test_a_number_that_became_text_is_a_difference(server):
    eng = _engine("create table items (id integer primary key, amount"
                  " integer)", [(1, 5)],
                  "create table items (id bigint primary key, amount text)",
                  [(1, "5")])
    got = _items(eng.check_schema("main"))
    assert got.status == "diff", got.detail
    assert "amount" in got.detail, got.detail
    assert "not the same kind of value" in got.detail, got.detail


@needs_docker
def test_a_table_on_one_side_only_is_named(server):
    eng = _engine("create table items (id integer primary key)", [],
                  "create table items (id bigint primary key)", [])
    c = sqlite3.connect(eng.hop.source.host)
    c.execute("create table orphan (id integer primary key)")
    c.commit()
    c.close()
    got = [r for r in eng.check_schema("main") if r.scope.endswith(".orphan")]
    assert got and got[0].status == "diff", got
    assert "not on the target" in got[0].detail, got[0].detail
    assert "none of its rows were compared" in got[0].detail, got[0].detail


# ---- the verdict shapes, without two servers ----

def _verdict(**kw):
    from migkit.engines.hetero import HeteroEngine
    got = {"pairs": [], "unreadable": [], "only_src": [], "only_dst": [],
           "src_types": {}, "dst_types": {}}
    got.update(kw)
    return HeteroEngine._schema_result(HeteroEngine, "db.t", got)


def test_columns_only_on_the_target_are_reported_too():
    got = _verdict(pairs=[("id", int, int)], only_dst=["added"])
    assert got.status == "diff", got.detail
    assert "only the target has" in got.detail and "added" in got.detail


def test_a_column_neither_side_can_render_is_a_warning_not_a_pass():
    """"We did not look" must not read as "they match" - and it is not a
    difference either, because nothing was compared to differ."""
    got = _verdict(pairs=[("id", int, int)],
                   unreadable=[("blob", "no canonical rendering")])
    assert got.status == "warn", got.detail
    assert got.status != "ok"
    assert "blob" in got.detail, got.detail
    assert "does not look at those" in got.detail, got.detail


def test_an_unreadable_column_rides_along_with_a_real_difference():
    got = _verdict(pairs=[("id", int, int)], only_src=["gone"],
                   unreadable=[("blob", "why")])
    assert got.status == "diff", got.detail
    assert "gone" in got.detail and "blob" in got.detail, got.detail


def test_the_verdicts_name_no_tool():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    for kw in ({"only_src": ["a"]}, {"unreadable": [("b", "why")]},
               {"pairs": [("id", int, int)]}):
        got = _verdict(**kw)
        text = f"{got.detail} {got.fix_hint}".lower()
        assert not [t for t in TOOLS if t in text], text
