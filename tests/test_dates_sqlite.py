"""Dates and times on their way into SQLite, which has neither type.

Three separate things were measured before any of this was written.

**A `time` could not cross at all.** The driver refuses it outright:

    sqlite3.ProgrammingError: Error binding parameter 1:
                              type 'datetime.time' is not supported

**A `datetime` and a `date` crossed on adapters Python has deprecated.** They
work today and print a DeprecationWarning; 3.12 deprecated them and says they
will go. What they produced is `2024-01-02 03:04:05.000006` and `2024-01-02` -
which is exactly the canonical text the other engines print, so writing it
explicitly changes nothing that is already stored and removes the dependency.

**A column declared `datetime` was not compared.** SQLite has no date type and
the declared name is a hint rather than a promise, so migkit left those columns
unmapped - and an unmapped column is one the comparison skips. They are `text`
now: what is stored is compared as what it is. A column holding an epoch
integer against a PostgreSQL timestamp reads as a difference, which is what it
is.
"""
import datetime
import sqlite3
import warnings

import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROWS = (
    (1, "2024-01-02 03:04:05.000006", "2024-01-02", "03:04:05.000006"),
    (2, "1999-12-31 23:59:59.999999", "1999-12-31", "23:59:59.999999"),
)


class _Checkpoint(dict):
    def save(self):
        pass


def _engine(pg_port, lite_path, tmp_path):
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


def _seed_pg(port):
    values = ", ".join(f"({i}, '{ts}', '{d}', '{tm}')"
                       for i, ts, d, tm in ROWS)
    psql(port, "create table t (id bigint primary key, ts timestamp,"
               f" d date, tm time); insert into t values {values};")


def _rows(lite_path):
    conn = sqlite3.connect(lite_path)
    try:
        return conn.execute("select id, ts, d, tm from t order by id"
                            ).fetchall()
    finally:
        conn.close()


def test_the_driver_really_does_refuse_a_time(tmp_path):
    """Pinned because the conversion exists for it."""
    conn = sqlite3.connect(tmp_path / "probe.db")
    conn.execute("create table t (v text)")
    with pytest.raises(sqlite3.ProgrammingError) as caught:
        conn.execute("insert into t values (?)",
                     (datetime.time(3, 4, 5, 6),))
    assert "not supported" in str(caught.value), caught.value
    conn.close()


def test_all_three_cross_and_the_two_sides_compare_equal(pg_pair, tmp_path):
    _seed_pg(pg_pair["src"])
    lite = tmp_path / "t.db"
    sqlite3.connect(lite).close()
    eng = _engine(pg_pair["src"], lite, tmp_path)
    eng.move_table("postgres", "public", "t", 100, _Checkpoint(),
                   lambda m: None)

    assert _rows(lite) == [tuple(r) for r in ROWS], _rows(lite)
    got = [r for r in eng.check_data("postgres") if r.scope.endswith(".t")]
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]


def test_nothing_leans_on_an_adapter_python_is_removing(pg_pair, tmp_path):
    """The same move with DeprecationWarning turned into an error. It used to
    raise here, on the `datetime` and the `date`."""
    _seed_pg(pg_pair["src"])
    lite = tmp_path / "t.db"
    sqlite3.connect(lite).close()
    eng = _engine(pg_pair["src"], lite, tmp_path)
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        eng.move_table("postgres", "public", "t", 100, _Checkpoint(),
                       lambda m: None)
    assert _rows(lite) == [tuple(r) for r in ROWS], _rows(lite)


def test_the_text_written_is_the_text_the_old_adapters_produced():
    """So a file written by an earlier migkit still compares equal to one
    written by this one."""
    when = datetime.datetime(2024, 1, 2, 3, 4, 5, 6)
    from migkit.engines.sqlite import SQLiteEngine
    assert SQLiteEngine._bind(when) == "2024-01-02 03:04:05.000006"
    assert SQLiteEngine._bind(when.date()) == "2024-01-02"
    assert SQLiteEngine._bind(when.time()) == "03:04:05.000006"


def test_a_column_someone_declared_datetime_is_compared(pg_pair, tmp_path):
    """The table is not one migkit made - it uses the declared names SQLite
    allows - and those columns used to be dropped from the comparison."""
    _seed_pg(pg_pair["src"])
    lite = tmp_path / "t.db"
    conn = sqlite3.connect(lite)
    conn.execute("create table t (id integer primary key, ts datetime,"
                 " d date, tm time)")
    conn.executemany("insert into t values (?,?,?,?)", ROWS)
    conn.commit()
    conn.close()

    eng = _engine(pg_pair["src"], lite, tmp_path)
    got = [r for r in eng.check_data("postgres") if r.scope.endswith(".t")]
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert "not compared" not in got[0].detail, got[0].detail

    conn = sqlite3.connect(lite)
    conn.execute("update t set ts = '2024-01-02 03:04:05.999999'"
                 " where id = 1")
    conn.commit()
    conn.close()
    got = [r for r in eng.check_data("postgres") if r.scope.endswith(".t")]
    assert got[0].status == "diff", got[0].detail
    assert "1 with different values (1)" in got[0].detail, got[0].detail


def test_the_in_process_rendering_matches_what_postgres_prints(pg_pair):
    """One query returns the values and the server's own text for them."""
    import psycopg2
    _seed_pg(pg_pair["src"])
    conn = psycopg2.connect(host="127.0.0.1", port=pg_pair["src"],
                            user="postgres", password="test",
                            dbname="postgres")
    try:
        cur = conn.cursor()
        cur.execute(
            f"select ts, {canon.expr('postgres', 'ts', 'timestamp')},"
            f" d, {canon.expr('postgres', 'd', 'date')},"
            f" tm, {canon.expr('postgres', 'tm', 'time')} from t order by id")
        rows = cur.fetchall()
    finally:
        conn.close()
    assert len(rows) == len(ROWS)
    for ts, ts_text, d, d_text, tm, tm_text in rows:
        assert canon.render_value("timestamp", ts) == ts_text
        assert canon.render_value("date", d) == d_text
        assert canon.render_value("time", tm) == tm_text


def test_sqlite_date_names_map_to_what_sqlite_actually_stores():
    for declared in ("date", "datetime", "timestamp", "time"):
        assert canon.type_class("sqlite", declared) == "text", declared
    # and the engines that do have the types keep them
    assert canon.type_class("postgres", "date") == "date"
    assert canon.type_class("mysql", "datetime") == "timestamp"


def test_a_sqlite_column_holding_an_epoch_number_reads_as_a_difference(
        pg_pair, tmp_path):
    """The consequence of comparing what is stored: a column declared
    `datetime` that holds seconds does not match a timestamp, and says so
    rather than being left out."""
    _seed_pg(pg_pair["src"])
    lite = tmp_path / "t.db"
    conn = sqlite3.connect(lite)
    conn.execute("create table t (id integer primary key, ts datetime,"
                 " d date, tm time)")
    conn.executemany("insert into t values (?,?,?,?)",
                     [(1, 1704164645, ROWS[0][2], ROWS[0][3]),
                      (2, ROWS[1][1], ROWS[1][2], ROWS[1][3])])
    conn.commit()
    conn.close()
    eng = _engine(pg_pair["src"], lite, tmp_path)
    got = [r for r in eng.check_data("postgres") if r.scope.endswith(".t")]
    assert got[0].status == "diff", got[0].detail
    assert "1 with different values (1)" in got[0].detail, got[0].detail
