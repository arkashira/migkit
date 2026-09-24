"""A primary key column holding NULL, and what it did to the report.

Every engine that reads its own key reads it from a real constraint - a
primary key, or MongoDB's `_id` - and a primary key cannot be NULL. That is
true of PostgreSQL and MySQL, and it is **not** true of SQLite. Measured:

    create table a (id text primary key, v text)
    insert into a (id, v) values (NULL, 'x')      accepted, id is null

    create table b (id integer primary key, v text)
    insert into b (id, v) values (NULL, 'x')      a rowid is assigned

    create table c (id text primary key not null, v text)
    insert into c (id, v) values (NULL, 'x')      NOT NULL constraint failed

A non-INTEGER `PRIMARY KEY` in SQLite takes NULLs unless it also says NOT
NULL, so any migkit hop on such a table can meet one.

Two things happened when it did, both in code shared by every engine:

**The data check crashed.** `"/".join(key)` on a key holding `None`:

    TypeError: sequence item 0: expected str instance, NoneType found

- raised from `_drill_rows`, which took the whole check down. The row
counts had already spotted the difference correctly (`src=3 dst=2`); the
drilldown turned a finding into a traceback. The same line existed twice,
in `_drill_rows` and in `_rows_plan`, so fixing one left the other.

**And the repair would have done nothing, quietly.** The key comes back out
of the drilldown as text and through `canon.from_text`, which answers
`None` for `None` - measured - and that goes into the predicate as
`where id = NULL`, which is never true. The repair would have reported the
row carried and changed nothing, which is the single failure this path
exists to prevent. It refuses now, and says why.
"""
import os
import pathlib
import sqlite3
import tempfile

import pytest

from migkit.config import Endpoint, Hop

DDL = "create table t (id text primary key, v text)"


def _pair(rows_src, rows_dst, ddl=DDL):
    d = tempfile.mkdtemp()
    paths = []
    for name, rows in (("s.db", rows_src), ("t.db", rows_dst)):
        p = os.path.join(d, name)
        c = sqlite3.connect(p)
        c.execute(ddl)
        c.executemany("insert into t (id, v) values (?, ?)", rows)
        c.commit()
        c.close()
        paths.append(p)
    return d, paths[0], paths[1]


def _engine(d, src, dst):
    from migkit.engines.sqlite import SQLiteEngine
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host="", port=0, user="", password="",
                              options={"path": src}),
              target=Endpoint(host="", port=0, user="", password="",
                              options={"path": dst}),
              databases=["main"])
    hop.report_dir = lambda db=None, _p=pathlib.Path(d): _p
    return SQLiteEngine(hop)


def test_sqlite_really_does_accept_a_null_primary_key():
    """The premise. If a future SQLite refuses it, this file is moot and
    should say so by failing."""
    d = tempfile.mkdtemp()
    c = sqlite3.connect(os.path.join(d, "x.db"))
    c.execute("create table a (id text primary key, v text)")
    c.execute("insert into a (id, v) values (NULL, 'x')")
    c.commit()
    assert c.execute("select id is null from a").fetchone()[0] == 1

    c.execute("create table b (id integer primary key, v text)")
    c.execute("insert into b (id, v) values (NULL, 'x')")
    assert c.execute("select id is null from b").fetchone()[0] == 0, \
        "an INTEGER primary key gets a rowid instead, so it is safe"

    c.execute("create table c (id text primary key not null, v text)")
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("insert into c (id, v) values (NULL, 'x')")
    c.close()


def test_the_other_engines_take_their_key_from_a_constraint():
    """Why this is not spread to them: a primary key cannot hold NULL, and
    that is where each of them looks. Asserted from the source so a future
    engine that reads a key out of config instead is noticed."""
    import inspect
    from migkit.engines import _class_for
    for name, must in (("postgres", "indisprimary"),
                       ("mysql", "PRIMARY"),
                       ("mongodb", "_id"),
                       ("sqlite", "table_info")):
        cls = _class_for(name)
        src = inspect.getsource(cls.neutral_key)
        # MySQL's reads the key through the one query its own paths use
        src += getattr(cls, "PK_SQL", "")
        assert must in src, (name, src)


def test_a_key_holding_null_renders_instead_of_crashing():
    from migkit.engines.base import Engine
    assert Engine._key_text(("a", "b")) == "a/b"
    assert Engine._key_text((None,)) == "NULL"
    assert Engine._key_text(("a", None)) == "a/NULL"
    assert Engine._key_text((1, None)) == "1/NULL"


def test_the_renderer_has_one_definition():
    """The same `"/".join(key)` existed in two places, so fixing the
    drilldown left the repair plan still crashing."""
    import pathlib as _p
    src = (_p.Path(__file__).resolve().parents[1] / "migkit" / "engines" /
           "base.py").read_text()
    assert '"/".join(k)' not in src, "a second raw key join is back"
    assert src.count("def _key_text") == 1


def test_the_data_check_survives_it_and_says_what_it_means():
    d, src, dst = _pair([("a", "1"), (None, "ghost"), ("b", "2")],
                        [("a", "1"), ("b", "2")])
    got = [r for r in _engine(d, src, dst).check_data("main")
           if r.scope.endswith(".t")]
    assert len(got) == 1, got
    assert got[0].status == "diff", got[0].detail
    assert "1 missing on the target (NULL)" in got[0].detail, got[0].detail
    assert "cannot be addressed on the other side" in got[0].detail
    assert "will not repair them" in got[0].detail, got[0].detail


def test_the_counts_check_had_already_been_right():
    """The drilldown turned a correct finding into a traceback; the counts
    were never wrong, which is what made the crash a regression rather than
    a missing feature."""
    d, src, dst = _pair([("a", "1"), (None, "ghost"), ("b", "2")],
                        [("a", "1"), ("b", "2")])
    got = _engine(d, src, dst).check_counts("main")
    assert any(r.status == "diff" and "3" in r.detail and "2" in r.detail
               for r in got), [r.detail for r in got]


def test_the_repair_refuses_rather_than_reporting_a_row_it_did_not_move():
    d, src, dst = _pair([("a", "1"), (None, "ghost"), ("b", "2")],
                        [("a", "1"), ("b", "2")])
    eng = _engine(d, src, dst)
    eng.check_data("main")
    plan = eng.repair_plan("main", "rows")
    assert plan, "nothing to repair, so the refusal cannot be reached"
    with pytest.raises(SystemExit) as e:
        for action in plan:
            eng.apply("main", action)
    said = str(e.value)
    assert "NULL in the key" in said, said
    assert "report success and change nothing" in said, said
    assert sqlite3.connect(dst).execute(
        "select count(*) from t").fetchone()[0] == 2


def test_a_repair_with_no_null_key_still_runs():
    """The refusal must be reachable only by the thing it is about - every
    hop in use has keys that are filled."""
    d, src, dst = _pair([("a", "1"), ("b", "2"), ("c", "3")],
                        [("a", "1"), ("b", "2")])
    eng = _engine(d, src, dst)
    eng.check_data("main")
    for action in eng.repair_plan("main", "rows"):
        eng.apply("main", action)
    assert sqlite3.connect(dst).execute(
        "select count(*) from t").fetchone()[0] == 3


def test_canon_really_does_hand_back_none():
    """The measured reason the repair could not have worked: the key text
    goes through this on its way into the predicate."""
    from migkit import canon
    assert canon.from_text(str, None) is None
    assert canon.from_text(int, None) is None


def test_the_warning_names_no_tool():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    d, src, dst = _pair([("a", "1"), (None, "ghost")], [("a", "1")])
    for r in _engine(d, src, dst).check_data("main"):
        text = f"{r.detail} {r.fix_hint}".lower()
        assert not [t for t in TOOLS if t in text], text
