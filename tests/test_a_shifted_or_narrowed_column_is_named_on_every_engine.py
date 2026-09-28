"""A column shifted by a timezone and a column narrower on the target are
named by the deep check in the same words on PostgreSQL and MySQL (#22).

Both engines carried the two checks, each with its own copy of the
reasoning, and the copies had drifted: PostgreSQL's narrowing check did not
know that `numeric(12,2)` into `numeric(12,4)` loses the values of 10^8 and
up, and neither knew that a length nobody set (`varchar` on PostgreSQL) is
wider than any. The reasoning is now `verdict.narrowing` and
`verdict.uniform_shift`, over `canon.capacity`, and both engines call it.

A shift is a conversion applied to the whole column - a mover's session
in a zone other than UTC - so it is named apart from rows changed one by
one: one delta on every row, or two an hour apart for a zone with daylight
saving. Rows each changed by their own amount are not called a shift; the
row comparison names them.

Run one engine at a time (`-k postgres`, `-k mysql`): one pair of
containers at once.
"""
import pytest

from tests import mysql_pair
from tests.conftest import psql, verdict
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

pytestmark = pytest.mark.docker


@pytest.fixture
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


#: six instants a day apart, and the same eight hours later
UNIFORM = [(i, f"2026-01-0{i} 07:00:00", f"2026-01-0{i} 15:00:00")
           for i in range(1, 7)]
#: eight instants a month apart across the change to summer time, and the
#: wall clock New York showed at each: five hours back in winter, four in
#: summer (2026-03-08 to 2026-11-01)
ZONE = [(1, "2026-01-10 12:00:00", "2026-01-10 07:00:00"),
        (2, "2026-02-09 12:00:00", "2026-02-09 07:00:00"),
        (3, "2026-03-11 12:00:00", "2026-03-11 08:00:00"),
        (4, "2026-04-10 12:00:00", "2026-04-10 08:00:00"),
        (5, "2026-05-10 12:00:00", "2026-05-10 08:00:00"),
        (6, "2026-06-09 12:00:00", "2026-06-09 08:00:00"),
        (7, "2026-11-20 12:00:00", "2026-11-20 07:00:00"),
        (8, "2026-12-20 12:00:00", "2026-12-20 07:00:00")]
#: rows changed each by its own number of seconds
APART = [(i, "2026-01-01 00:00:00", f"2026-01-01 00:{i:02d}:{i * 7 % 60:02d}")
         for i in range(1, 7)]


def _seed(run, side, types, same=False):
    """The same three tables of instants and one of columns on either
    engine, the target's shifted, narrowed or changed row by row - or, with
    `same`, as the source's."""
    at = 1 if side == "src" or same else 2
    for table, rows in (("ev", UNIFORM), ("zone_ev", ZONE), ("apart", APART)):
        values = ", ".join(f"({r[0]}, '{r[at]}')" for r in rows)
        run(side, f"create table {table} (id int primary key,"
                  f" at {types['ts']}); insert into {table} values {values};")
    run(side, "create table nr (id int primary key,"
              + types["src_cols" if side == "src" or same else "dst_cols"]
              + ");")


PG = {"ts": "timestamp",
      "src_cols": " name varchar(100), price numeric(12,4), big bigint,"
                  " note text, wide numeric(12,2)",
      "dst_cols": " name varchar(50), price numeric(12,2), big integer,"
                  " note varchar(200), wide numeric(12,4)"}
MY = {"ts": "datetime",
      "src_cols": " name varchar(100), price decimal(12,4), big bigint,"
                  " note text, wide decimal(12,2), u int unsigned",
      "dst_cols": " name varchar(50), price decimal(12,2), big int,"
                  " note varchar(200), wide decimal(12,4), u int"}


def _pg(side, sql):
    got = psql(55432 if side == "src" else 55433, sql)
    assert got.returncode == 0, got.stderr


def _pg_engine():
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(Hop(
        name="chk", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=55432, user="postgres",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=55433, user="postgres",
                        password="test"),
        databases=["postgres"]))


def _my(side, sql):
    my(side, "use tz; " + sql)


def _both(deep, db):
    return verdict(deep, f"{db} timeshift"), verdict(deep, f"{db} narrowing")


def _assert_the_findings(deep, db, prefix):
    shift, narrow = _both(deep, db)
    assert shift.status == "diff", shift
    assert shift.category == "value.timezone"
    assert (f"{prefix}ev.at: every row shifted 28800s (~8.0h)"
            in shift.detail), shift.detail
    assert (f"{prefix}zone_ev.at: every row shifted -18000s or -14400s"
            " (~-5.0h/-4.0h, a zone with daylight saving)"
            in shift.detail), shift.detail
    # changed row by row: not a shift, whatever its size
    assert f"{prefix}apart.at" not in shift.detail, shift.detail
    assert narrow.status == "diff", narrow
    assert narrow.category == "value.narrowing"
    return shift, narrow


def test_postgres_names_the_shift_and_the_narrowing(pg_pair, reports):
    for side in ("src", "dst"):
        _seed(_pg, side, PG)
    eng = _pg_engine()
    deep = eng.check_deep("postgres")
    _, narrow = _assert_the_findings(deep, "postgres", "public.")
    for line in ("public.nr.name: character varying(100) -> character"
                 " varying(50) (longer values are cut)",
                 "public.nr.price: numeric(12,4) -> numeric(12,2)"
                 " (scale 4 -> 2: rounds)",
                 "public.nr.big: bigint -> integer (overflow)",
                 "public.nr.note: text -> character varying(200)"
                 " (longer values are cut)",
                 # what the check did not see before: a wider scale in as
                 # many digits leaves fewer for the whole part
                 "public.nr.wide: numeric(12,2) -> numeric(12,4)"
                 " (integer digits 10 -> 8: overflow)"):
        assert line in narrow.detail, narrow.detail
    # the rows changed one by one are the row comparison's to name
    data = [r for r in eng.check_data("postgres")
            if r.status == "diff" and "apart" in f"{r.scope} {r.detail}"]
    assert data, eng.check_data("postgres")


def test_mysql_names_the_shift_and_the_narrowing(my_pair, reports):
    for side in ("src", "dst"):
        my(side, "drop database if exists tz; create database tz;")
        _seed(_my, side, MY)
    eng = mysql_pair.engine("tz")
    deep = eng.check_deep("tz")
    _, narrow = _assert_the_findings(deep, "tz", "")
    for line in ("nr.name: varchar(100) -> varchar(50) (longer values are"
                 " cut)",
                 "nr.price: decimal(12,4) -> decimal(12,2) (scale 4 -> 2:"
                 " rounds)",
                 "nr.big: bigint -> int (overflow)",
                 "nr.note: text -> varchar(200) (longer values are cut)",
                 "nr.wide: decimal(12,2) -> decimal(12,4) (integer digits"
                 " 10 -> 8: overflow)",
                 "nr.u: int unsigned -> int (overflow)"):
        assert line in narrow.detail, narrow.detail
    data = [r for r in eng.check_data("tz")
            if r.status == "diff" and "apart" in f"{r.scope} {r.detail}"]
    assert data, eng.check_data("tz")


def test_postgres_the_same_columns_are_not_named(pg_pair, reports):
    for side in ("src", "dst"):
        _seed(_pg, side, PG, same=True)
    shift, narrow = _both(_pg_engine().check_deep("postgres"), "postgres")
    assert shift.status == "ok", shift
    assert narrow.status == "ok", narrow


def test_mysql_the_same_columns_are_not_named(my_pair, reports):
    for side in ("src", "dst"):
        my(side, "drop database if exists tz; create database tz;")
        _seed(_my, side, MY, same=True)
    shift, narrow = _both(mysql_pair.engine("tz").check_deep("tz"), "tz")
    assert shift.status == "ok", shift
    assert narrow.status == "ok", narrow
