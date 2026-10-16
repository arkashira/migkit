"""The SQL engines' schema check goes through the one neutral comparer
(`verifiers.compare_schema`): each engine reads its catalogue into the
neutral shape and nothing in its own file compares two schemas, so a
difference is worded alike for every pair (backlog AUD-H, SC1)."""
import inspect
import os
import pathlib
import sqlite3
import tempfile

import pytest

from migkit import engines, verifiers
from migkit.config import Endpoint, Hop
from migkit.engines.sqlite import SQLiteEngine

MOVED = ["ase", "cockroachdb", "db2", "db2i", "db2zos", "dsql", "duckdb",
         "gaussdb", "oceanbase", "oceanbase-oracle", "oracle", "polardbx",
         "dcdb", "sqlite", "teradata", "tidb"]


def test_no_listed_engine_keeps_a_compare_of_its_own():
    assert not set(MOVED) & set(verifiers.engines_with_own_compare())


@pytest.mark.parametrize("name", MOVED)
def test_the_engine_hands_its_pair_to_the_comparer_and_compares_nothing(name):
    src = inspect.getsource(engines._class_for(name).check_schema)
    assert "compare_schema" in src
    assert "_as_pair().check_schema" not in src
    assert "difflib" not in src


@pytest.mark.parametrize("name", MOVED)
def test_the_comparer_is_asked_of_the_pair_and_its_answer_is_kept(
        name, monkeypatch):
    cls = engines._class_for(name)
    eng = cls.__new__(cls)
    pair = object()
    asked = []
    eng.__dict__["_as_pair"] = lambda: pair
    eng.__dict__["check_objects"] = lambda db: "objects"
    eng.__dict__["_structural"] = lambda db: "structural"
    monkeypatch.setattr(verifiers, "compare_schema",
                        lambda e, db, max_age=0.0: asked.append((e, db))
                        or ["compared"])
    got = eng.check_schema("appdb")
    assert asked == [(pair, "appdb")]
    assert got[0] == "compared"
    assert len(got) in (1, 2)


def _engine(src_sql, dst_sql, **options):
    d = tempfile.mkdtemp()
    paths = []
    for name, script in (("s.db", src_sql), ("t.db", dst_sql)):
        p = os.path.join(d, name)
        c = sqlite3.connect(p)
        c.executescript(script)
        c.commit()
        c.close()
        paths.append(p)
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host="", port=0, user="", password="",
                              options={"path": paths[0]}),
              target=Endpoint(host="", port=0, user="", password="",
                              options={"path": paths[1]}),
              databases=["main"], **options)
    hop.report_dir = lambda db=None, _p=pathlib.Path(d): _p
    return SQLiteEngine(hop)


BASE = """
create table t (id integer primary key, n integer not null default 0,
                label text);
create index t_label on t (label);
create view v as select id, label from t;
create trigger t_ins after insert on t begin
  update t set n = n + 1 where id = new.id; end;
"""


def _statuses(eng):
    return {r.scope: (r.status, r.detail) for r in eng.check_schema("main")}


def test_the_same_schema_on_both_sides_is_not_a_difference():
    got = _statuses(_engine(BASE, BASE))
    assert [s for s, _ in got.values()] == ["ok"], got


def test_a_column_type_that_differs_is_found_by_its_table():
    other = BASE.replace("label text", "label varchar(20)")
    got = _statuses(_engine(BASE, other))
    assert got["main.t"][0] == "diff" and "label" in got["main.t"][1], got


def test_an_index_missing_on_the_target_is_found():
    other = BASE.replace("create index t_label on t (label);", "")
    got = _statuses(_engine(BASE, other))
    assert got["main.t"][0] == "diff" and "index" in got["main.t"][1], got


def test_a_trigger_body_that_differs_is_found():
    other = BASE.replace("n = n + 1", "n = n + 2")
    got = _statuses(_engine(BASE, other))
    assert got["main.t"][0] == "diff", got


def test_a_view_that_differs_or_is_missing_is_found():
    other = BASE.replace("select id, label from t", "select id, label from t"
                         " where id > 0")
    assert _statuses(_engine(BASE, other))["main.v"][0] == "diff"
    gone = BASE.replace("create view v as select id, label from t;", "")
    assert _statuses(_engine(BASE, gone))["main.v"][0] == "diff"


def test_a_nullability_and_default_that_differ_are_found():
    other = BASE.replace("n integer not null default 0", "n integer")
    got = _statuses(_engine(BASE, other))
    assert got["main.t"][0] == "diff", got


def test_the_shape_leaves_out_what_the_hop_does_not_take():
    eng = _engine(BASE, BASE, exclude=["t"])
    cat = eng.shape_catalog("src", "main")
    assert "t" not in cat.tables
    assert cat.indexes == {} and "t" not in cat.others
