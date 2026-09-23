"""`exclude` means the same thing on every engine.

The hop's exclude list is what protects a target-owned table: migkit
neither verifies nor repairs it. PostgreSQL, MySQL and MongoDB read it when
they list tables; SQLite, SQL Server, Redis, Kafka and the generic engine
did not read it at all.

Measured on SQLite before the fix, with `exclude: [audit]` and a row only
the target has in `audit`:

    counts  main        diff  audit src=1 dst=2
    data    main.audit  diff  ... 1 only on the target (7)
    repair  main.audit  delete 1 rows the source does not have: 7

and after `apply`, the target-owned row was gone.
"""
import sqlite3

import pytest

from migkit.config import Endpoint, Hop


def _pair(tmp_path, exclude=()):
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    s = sqlite3.connect(src)
    s.executescript("""
        create table orders (id integer primary key, v text);
        insert into orders values (1, 'a'), (2, 'b');
        create table audit (id integer primary key, x text);
        insert into audit values (1, 'x');
    """)
    s.commit()
    s.close()
    d = sqlite3.connect(dst)
    d.executescript("""
        create table orders (id integer primary key, v text);
        insert into orders values (1, 'a');
        create table audit (id integer primary key, x text, owner text);
        create index audit_owner on audit (owner);
        insert into audit values (1, 'x', null), (7, 'target-owned', 'app');
    """)
    d.commit()
    d.close()
    report = tmp_path / "report"
    report.mkdir()
    from migkit.engines.sqlite import SQLiteEngine
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              db_map={"main": "main"}, exclude=list(exclude))
    hop.report_dir = lambda db=None: report
    return SQLiteEngine(hop), dst


def _said(results):
    return " | ".join(f"{r.scope} {r.status} {r.detail}" for r in results)


def test_the_check_leaves_an_excluded_table_alone(tmp_path):
    eng, _ = _pair(tmp_path, ["audit"])
    counts = eng.check_counts("main")
    data = eng.check_data("main")
    said = _said(counts + data)
    assert "audit" not in said, said
    # and the check did run: the table it may look at is still judged
    assert "orders" in said and any(r.status == "diff" for r in counts), said


def test_the_schema_check_leaves_its_indexes_alone_too(tmp_path):
    eng, _ = _pair(tmp_path, ["audit"])
    got = eng.check_schema("main")
    assert [r.status for r in got] == ["ok"], _said(got)


def test_repair_does_not_touch_a_target_owned_row(tmp_path):
    eng, dst = _pair(tmp_path, ["audit"])
    eng.check_data("main")
    plan = eng.repair_plan("main", "rows")
    assert plan and all("audit" not in a.scope for a in plan), \
        [a.scope for a in plan]
    for action in plan:
        eng.apply("main", action)
    con = sqlite3.connect(dst)
    try:
        assert con.execute("select id from audit order by id").fetchall() \
            == [(1,), (7,)]
        assert con.execute("select id from orders order by id").fetchall() \
            == [(1,), (2,)]
    finally:
        con.close()


def test_an_excluded_table_still_counts_as_there(tmp_path):
    """Leaving it alone is not pretending it is absent: building a table
    by that name on the target is still refused."""
    eng, _ = _pair(tmp_path, ["audit"])
    with pytest.raises(SystemExit):
        eng.neutral_create("dst", "main", "audit",
                           [("id", "integer", ())], ["id"])


def test_a_copy_between_two_files_leaves_it_alone(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    eng, _ = _pair(tmp_path, ["audit"])
    hop = eng.hop
    hop.engine = "hetero"
    hop.options = {"source_engine": "sqlite", "target_engine": "sqlite"}
    tables = [t for _, t in HeteroEngine(hop).list_move_tables("main")]
    assert tables == ["orders"], tables


def test_without_an_exclude_list_nothing_changes(tmp_path):
    eng, _ = _pair(tmp_path)
    said = _said(eng.check_counts("main") + eng.check_data("main"))
    assert "audit" in said, said
    assert [r.status for r in eng.check_schema("main")] == ["diff"]
