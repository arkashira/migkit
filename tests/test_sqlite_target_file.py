"""Moving into a SQLite target whose file is not there yet.

`assess` has always said a target file is created by the first write. It was
not true for a move: the mover starts by listing what the target already
holds, that read opens the path read-only, and a path with nothing at it
cannot be opened that way. What an operator got was

    OperationalError: unable to open database file

with no path in it and no hint that the fix was to create an empty file by
hand first.

Two changes. `prepare_target` on the engine contract makes whatever the target
needs before a first write - nothing at all for PostgreSQL, MySQL and MongoDB,
because `create database` there is a decision with an owner and an encoding
behind it, and an empty file for SQLite, where the database *is* the file and
the mover creates the tables in it anyway. And the read-only failure now says
which path and which side, and whether the file is missing or unreadable.

What did not change: a missing *source* is still an error. Two files that are
both absent must not compare equal.
"""
import os
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


class _Checkpoint(dict):
    def save(self):
        pass


def _hetero(pg_port, lite_path, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="h", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=pg_port,
                              user="postgres", password="test"),
              target=Endpoint(host=str(lite_path), port=0, user="",
                              password=""),
              databases=["postgres"],
              options={"source_engine": "postgres",
                       "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _sqlite(src, dst, tmp_path):
    from migkit.engines.sqlite import SQLiteEngine

    def ep(path):
        return Endpoint(host=str(path), port=0, user="", password="")
    hop = Hop(name="s", engine="sqlite", source=ep(src), target=ep(dst))
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


def _seed_pg(port):
    psql(port, "create table t (id bigint primary key, v text);"
               " insert into t values (1,'one'),(2,'two');")


def test_a_move_into_a_file_that_does_not_exist_makes_it(pg_pair, tmp_path):
    _seed_pg(pg_pair["src"])
    lite = tmp_path / "fresh.db"
    assert not lite.exists()
    said = []
    eng = _hetero(pg_pair["src"], lite, tmp_path)
    eng.move_table("postgres", "public", "t", 100, _Checkpoint(), said.append)

    assert lite.exists()
    assert any("created the empty target database file" in m for m in said), \
        said
    got = [r for r in eng.check_data("postgres") if r.scope.endswith(".t")]
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]


def test_a_file_that_is_already_there_is_left_alone(pg_pair, tmp_path):
    """Nothing that exists is touched by the preparation - the note is only
    printed when something was actually made."""
    _seed_pg(pg_pair["src"])
    lite = tmp_path / "already.db"
    conn = sqlite3.connect(lite)
    conn.execute("create table keep_me (a text)")
    conn.execute("insert into keep_me values ('still here')")
    conn.commit()
    conn.close()
    before = os.path.getmtime(lite)

    eng = _hetero(pg_pair["src"], lite, tmp_path)
    assert eng.dst_engine.prepare_target("postgres") is None
    assert os.path.getmtime(lite) == before

    eng.move_table("postgres", "public", "t", 100, _Checkpoint(),
                   lambda m: None)
    conn = sqlite3.connect(lite)
    try:
        assert conn.execute("select a from keep_me").fetchall() == \
            [("still here",)]
    finally:
        conn.close()


def test_a_directory_that_is_not_there_is_refused(pg_pair, tmp_path):
    """Usually a typo, and making the tree would turn it into a migration
    into somewhere nobody meant."""
    _seed_pg(pg_pair["src"])
    lite = tmp_path / "nope" / "x.db"
    eng = _hetero(pg_pair["src"], lite, tmp_path)
    with pytest.raises(SystemExit) as caught:
        eng.move_table("postgres", "public", "t", 100, _Checkpoint(),
                       lambda m: None)
    said = str(caught.value)
    assert str(tmp_path / "nope") in said, said
    assert "make the directory first" in said, said
    assert not lite.exists()


def test_the_other_engines_make_nothing(tmp_path):
    """A database on a server is the operator's to create, so the answer
    there is no answer - not an empty database conjured on their behalf."""
    from migkit.engines.base import Engine
    from migkit.engines.mongodb import MongoEngine
    from migkit.engines.mysql import MySQLEngine
    from migkit.engines.postgres import PostgresEngine
    from migkit.engines.sqlite import SQLiteEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    for cls in (PostgresEngine, MySQLEngine, MongoEngine):
        hop = Hop(name="x", engine="x", source=ep, target=ep,
                  db_map={"d": "d"})
        assert cls(hop).prepare_target("d") is None, cls
        assert cls.prepare_target is Engine.prepare_target, cls
    assert SQLiteEngine.prepare_target is not Engine.prepare_target


def test_a_missing_source_is_reported_with_the_path_and_the_side(tmp_path):
    """And as a result rather than a traceback: one unreadable file must not
    take the whole run down."""
    dst = tmp_path / "there.db"
    sqlite3.connect(dst).close()
    eng = _sqlite(tmp_path / "gone.db", dst, tmp_path)
    for got in (eng.check_counts("main"), eng.check_data("main")):
        assert [r.status for r in got] == ["error"], [r.detail for r in got]
        assert str(tmp_path / "gone.db") in got[0].detail, got[0].detail
        assert "source database file does not exist" in got[0].detail


def test_two_files_that_are_both_missing_do_not_compare_equal(tmp_path):
    """The shape this project keeps finding: nothing read on either side
    reported as the two sides agreeing."""
    eng = _sqlite(tmp_path / "a.db", tmp_path / "b.db", tmp_path)
    for got in (eng.check_counts("main"), eng.check_data("main")):
        assert not any(r.status == "ok" for r in got), [
            (r.status, r.detail) for r in got]
        assert got[0].status == "error", got[0].detail


def test_an_unreadable_file_is_told_apart_from_a_missing_one(tmp_path):
    if os.geteuid() == 0:
        pytest.skip("root reads everything")
    path = tmp_path / "locked.db"
    sqlite3.connect(path).close()
    os.chmod(path, 0o000)
    try:
        eng = _sqlite(path, path, tmp_path)
        got = eng.check_counts("main")
        assert got[0].status == "error", got[0].detail
        assert "cannot be read" in got[0].detail, got[0].detail
        assert "does not exist" not in got[0].detail, got[0].detail
    finally:
        os.chmod(path, 0o600)
