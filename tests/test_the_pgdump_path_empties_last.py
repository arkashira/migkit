"""The pg_dump path emptied the target before it had anything to load.

The dump is a directory on disk, so nothing forces the order - and the
order was: empty the target, then dump the source, then restore. Measured,
with a source that could not be reached:

    move failed: ... Is the server running on that host and accepting
    TCP/IP connections?
    target orders: 2 rows before, 0 after

A move that failed, and still deleted what it had come to replace.

Two more things in the same function, found while fixing it:

* A hop that excludes tables, on a source whose table list could not be
  read, went ahead with a note in the plan. The emptying keeps excluded
  tables, so a dump that was not told to skip them loads the source's rows
  on top of the target's own. The MySQL path already stopped here; both now
  raise the same refusal.
* `pg_restore -d` was the source's name for the database, while the
  emptying and the index window used `target_db()`. With a `db_map`, the
  right database was emptied and another one was loaded.
"""
import ast
import pathlib

import pytest

from migkit import movers
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _hop(pg_pair, tmp_path, src_port=None, **kw):
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="127.0.0.1",
                              port=src_port or pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2, **kw)
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    return hop


def _seed(pg_pair, db="postgres"):
    for port in (pg_pair["src"], pg_pair["dst"]):
        assert psql(port, "create table orders (id int primary key, v text)",
                    db).returncode == 0
    psql(pg_pair["src"], "insert into orders values (1,'s'),(2,'s'),(3,'s')")
    assert psql(pg_pair["dst"], "insert into orders values (1,'live'),"
                "(2,'live')", db).returncode == 0


def _rows(pg_pair, db="postgres"):
    got = psql(pg_pair["dst"], "select string_agg(id||v, ',' order by id)"
               " from orders", db)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def test_an_unreachable_source_leaves_the_target_as_it_was(pg_pair,
                                                           tmp_path):
    _seed(pg_pair)
    with pytest.raises(RuntimeError):
        movers.pgdump_move(_hop(pg_pair, tmp_path, src_port=1), "postgres",
                           2, True, None)
    assert _rows(pg_pair) == "1live,2live", "the target was emptied anyway"


def test_a_move_that_works_still_replaces_the_rows(pg_pair, tmp_path):
    """Moving the emptying must not turn the load into an append."""
    _seed(pg_pair)
    movers.pgdump_move(_hop(pg_pair, tmp_path), "postgres", 2, True, None)
    assert _rows(pg_pair) == "1s,2s,3s"


def test_an_exclusion_it_cannot_resolve_stops_before_anything(pg_pair,
                                                               tmp_path):
    _seed(pg_pair)
    with pytest.raises(SystemExit) as e:
        movers.pgdump_move(_hop(pg_pair, tmp_path, src_port=1,
                                exclude=["audit_log"]),
                           "postgres", 2, True, None)
    said = str(e.value)
    assert "cannot be told to skip them" in said, said
    assert "Nothing has been changed on the target" in said, said
    assert _rows(pg_pair) == "1live,2live"


def test_the_restore_goes_to_the_targets_name_for_the_database(pg_pair,
                                                               tmp_path):
    psql(pg_pair["dst"], "drop database if exists mapped")
    assert psql(pg_pair["dst"], "create database mapped").returncode == 0
    try:
        _seed(pg_pair)
        assert psql(pg_pair["dst"], "create table orders (id int primary key,"
                    " v text)", "mapped").returncode == 0
        movers.pgdump_move(_hop(pg_pair, tmp_path,
                                db_map={"postgres": "mapped"}),
                           "postgres", 2, True, None)
        assert _rows(pg_pair, "mapped") == "1s,2s,3s"
        assert _rows(pg_pair) == "1live,2live", \
            "the source's name on the target was loaded instead"
    finally:
        psql(pg_pair["dst"], "drop database if exists mapped")


def test_the_plan_is_in_the_order_the_run_is(tmp_path):
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"], db_map={"appdb": "appdb_new"})
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    steps = movers.pgdump_move(hop, "appdb", 2, False, None)

    def runs(program):
        return next(i for i, s in enumerate(steps)
                    if getattr(s, "argv", None) and s.argv[0] == program)
    dump, load = runs("pg_dump"), runs("pg_restore")
    empty = next(i for i, s in enumerate(steps) if "empty" in s)
    assert dump < empty < load, steps
    assert " -d appdb_new " in steps[load].command, steps[load].command
    assert "CHANGE_ME" not in "\n".join(steps)
    assert "CHANGE_ME" not in "\n".join(s.command for s in steps
                                        if getattr(s, "argv", None))


def test_both_bulk_paths_stop_the_same_way():
    """One refusal, raised from both, so the two cannot drift apart."""
    src = (pathlib.Path(__file__).resolve().parents[1] / "migkit" /
           "movers.py").read_text()
    callers = set()
    for fn in ast.walk(ast.parse(src)):
        if isinstance(fn, ast.FunctionDef):
            for node in ast.walk(fn):
                if (isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id == "_unresolved_exclusion"):
                    callers.add(fn.name)
    # every bulk path that reads the source's table list: the streaming
    # copier used to go ahead with only a note in its plan, and the MongoDB
    # path, which now reads the collection list for the exclude list, stops
    # at the same point
    assert callers == {"pgdump_move", "mydumper_move", "pgcopydb_move",
                       "mongodump_move"}, callers


def test_the_refusal_names_no_tool():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    said = str(movers._unresolved_exclusion("appdb", "refused")).lower()
    assert not [t for t in TOOLS if t in said], said
    assert "pgdump" not in said and "pg_dump" not in said, said
