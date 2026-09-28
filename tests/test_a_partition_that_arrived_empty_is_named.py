"""Every partition of a partitioned table is held to its own count and
digest, and one the target has empty, lacks or has extra is named.

Measured on a DTS leg: the current month's partitions arrived with 0 rows
while the parents' totals looked plausible. Before this, the deep check on
both engines compared the partition key and the set of bounds, and the
rows in the target's catch-all - nothing about the rows of each partition.
Measured on the pairs below with that check:

* PostgreSQL, a partition present on both sides and empty on the target:
  `partitions OK 1 partitioned tables, schemes and bounds match, default
  empty`. (The counts named `public.events_2026_08 src=30 dst=0`, as a
  table among tables.)
* MySQL, the same: `partitions OK`, and the counts said only
  `events src=80 dst=50` - MySQL's partitions are not tables, and nothing
  else looked inside one.
* MySQL, the August partition missing on the target and its rows in the
  MAXVALUE catch-all: counts and data both OK (the same rows are all
  there); only the deep check's missing bound and stranded rows said it.
* MySQL, four hash partitions on the source and three on the target:
  counts, data and partitions all OK - a hash partition has no bound to
  find missing.
* A partition only the target has: the deep check OK on both engines;
  PostgreSQL's counts named it as an extra table, MySQL said nothing.
* A partition whose rows changed, counts equal: the deep check OK on
  both; the data check named the table.

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
    return tmp_path / "reports"


# -- PostgreSQL ------------------------------------------------------------

PG_PARENT = ("create table events (id int, at date, v text,"
             " primary key (id, at)) partition by range (at);")
PG_JULY = ("create table events_2026_07 partition of events"
           " for values from ('2026-07-01') to ('2026-08-01');")
PG_AUGUST = ("create table events_2026_08 partition of events"
             " for values from ('2026-08-01') to ('2026-09-01');")
PG_SEPT = ("create table events_2026_09 partition of events"
           " for values from ('2026-09-01') to ('2026-10-01');")
PG_DEFAULT = "create table events_def partition of events default;"
PG_JULY_ROWS = ("insert into events select g, date '2026-07-01' + g % 31,"
                " 'v' || g from generate_series(1, 50) g;")
PG_AUGUST_ROWS = ("insert into events select 100 + g, date '2026-08-01'"
                  " + g % 31, 'v' || g from generate_series(1, 30) g;")


def _pg(pg_pair, side, sql):
    got = psql(pg_pair[side], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


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


def _pg_partitions():
    return verdict(_pg_engine().check_deep("postgres"),
                   "postgres partitions")


def _pg_source(pg_pair):
    _pg(pg_pair, "src", PG_PARENT + PG_JULY + PG_AUGUST + PG_DEFAULT
        + PG_JULY_ROWS + PG_AUGUST_ROWS)


def test_postgres_a_partition_empty_on_the_target(pg_pair, reports):
    _pg_source(pg_pair)
    _pg(pg_pair, "dst", PG_PARENT + PG_JULY + PG_AUGUST + PG_DEFAULT
        + PG_JULY_ROWS)
    got = _pg_partitions()
    assert got.status == "diff", got
    assert ("public.events partition public.events_2026_08: empty on"
            " target, the source's holds 30 rows") in got.detail, got.detail
    # the partition that did arrive is not named
    assert "events_2026_07" not in got.detail, got.detail
    # every partition, with its rows and digest per side, beside the finding
    listing = (reports / "chk" / "postgres" / "deep-partitions.diff"
               ).read_text()
    assert got.report.endswith("deep-partitions.diff"), got.report
    assert "src public.events public.events_2026_08 rows=30" in listing
    assert "dst public.events public.events_2026_08 rows=0" in listing


def test_postgres_rows_in_the_catch_all_of_a_target_that_lacks_one(
        pg_pair, reports):
    _pg_source(pg_pair)
    _pg(pg_pair, "dst", PG_PARENT + PG_JULY + PG_DEFAULT + PG_JULY_ROWS
        + PG_AUGUST_ROWS)
    got = _pg_partitions()
    assert got.status == "diff", got
    assert ("public.events partition public.events_2026_08: missing on"
            " target, the source's holds 30 rows") in got.detail, got.detail
    assert ("public.events partition public.events_def: 30 rows stranded in"
            " the catch-all on target (src=0 dst=30)") in got.detail, \
        got.detail


def test_postgres_a_partition_only_the_target_has(pg_pair, reports):
    _pg_source(pg_pair)
    _pg(pg_pair, "dst", PG_PARENT + PG_JULY + PG_AUGUST + PG_SEPT
        + PG_DEFAULT + PG_JULY_ROWS + PG_AUGUST_ROWS)
    got = _pg_partitions()
    assert got.status == "diff", got
    assert ("public.events partition public.events_2026_09: extra on target"
            " (empty)") in got.detail, got.detail


def test_postgres_a_partition_whose_rows_changed(pg_pair, reports):
    _pg_source(pg_pair)
    _pg(pg_pair, "dst", PG_PARENT + PG_JULY + PG_AUGUST + PG_DEFAULT
        + PG_JULY_ROWS + PG_AUGUST_ROWS
        + " update events set v = 'changed' where id = 105;")
    got = _pg_partitions()
    assert got.status == "diff", got
    assert ("public.events partition public.events_2026_08: 30 rows both"
            " sides, content differs") in got.detail, got.detail
    assert "events_2026_07" not in got.detail, got.detail


def test_postgres_the_same_partitions_on_both_sides(pg_pair, reports):
    _pg_source(pg_pair)
    _pg(pg_pair, "dst", PG_PARENT + PG_JULY + PG_AUGUST + PG_DEFAULT
        + PG_JULY_ROWS + PG_AUGUST_ROWS)
    got = _pg_partitions()
    assert got.status == "ok", got
    assert "1 partitioned tables, 3 partitions" in got.detail, got.detail
    assert "compared by count only" not in got.detail, got.detail
    assert not (reports / "chk" / "postgres" / "deep-partitions.diff"
                ).exists()


def test_postgres_a_sub_partitioned_leaf_is_compared(pg_pair, reports):
    """A partition that is itself partitioned holds no rows of its own:
    its leaves are what is compared."""
    ddl = ("create table m (id int, at date, k int, primary key (id, at, k))"
           " partition by range (at);"
           " create table m_2026 partition of m for values from"
           " ('2026-01-01') to ('2027-01-01') partition by list (k);"
           " create table m_2026_a partition of m_2026 for values in (1);"
           " create table m_2026_b partition of m_2026 for values in (2);")
    _pg(pg_pair, "src", ddl + " insert into m select g, '2026-03-01',"
        " 1 + g % 2 from generate_series(1, 20) g;")
    _pg(pg_pair, "dst", ddl + " insert into m select g, '2026-03-01',"
        " 1 from generate_series(1, 20) g where g % 2 = 0;")
    got = _pg_partitions()
    assert got.status == "diff", got
    assert ("public.m partition public.m_2026_b: empty on target, the"
            " source's holds 10 rows") in got.detail, got.detail


# -- MySQL -----------------------------------------------------------------

MY_TABLE = ("create table events (id int, at date, v varchar(20),"
            " primary key (id, at)) partition by range (to_days(at)) (")
MY_JULY = "partition p202607 values less than (to_days('2026-08-01'))"
MY_AUGUST = "partition p202608 values less than (to_days('2026-09-01'))"
MY_SEPT = "partition p202609 values less than (to_days('2026-10-01'))"
MY_MAX = "partition pmax values less than maxvalue"
MY_JULY_ROWS = ("insert into events with recursive g(n) as (select 1"
                " union all select n + 1 from g where n < 50) select n,"
                " date '2026-07-01' + interval (n % 31) day, concat('v', n)"
                " from g;")
MY_AUGUST_ROWS = ("insert into events with recursive g(n) as (select 1"
                  " union all select n + 1 from g where n < 30) select"
                  " 100 + n, date '2026-08-01' + interval (n % 31) day,"
                  " concat('v', n) from g;")


def _my_db(side, parts, rows=""):
    my(side, "drop database if exists pt; create database pt; use pt;"
       + MY_TABLE + ", ".join(parts) + ");" + rows)


def _my_partitions():
    return verdict(mysql_pair.engine("pt").check_deep("pt"),
                   "pt partitions")


def _my_source():
    _my_db("src", [MY_JULY, MY_AUGUST, MY_MAX],
           MY_JULY_ROWS + MY_AUGUST_ROWS)


def test_mysql_a_partition_empty_on_the_target(my_pair, reports):
    _my_source()
    _my_db("dst", [MY_JULY, MY_AUGUST, MY_MAX], MY_JULY_ROWS)
    got = _my_partitions()
    assert got.status == "diff", got
    assert ("events partition p202608: empty on target, the source's holds"
            " 30 rows") in got.detail, got.detail
    assert "p202607" not in got.detail, got.detail
    listing = (reports / "chk" / "pt" / "deep-partitions.diff").read_text()
    assert "src events p202608 rows=30" in listing, listing
    assert "dst events p202608 rows=0" in listing, listing


def test_mysql_rows_in_maxvalue_that_counts_and_data_call_equal(
        my_pair, reports):
    _my_source()
    _my_db("dst", [MY_JULY, MY_MAX], MY_JULY_ROWS + MY_AUGUST_ROWS)
    eng = mysql_pair.engine("pt")
    # the same rows are all there: what is wrong is where they are
    assert verdict(eng.check_counts("pt"), "pt").status == "ok"
    got = _my_partitions()
    assert got.status == "diff", got
    assert ("events partition p202608: missing on target, the source's"
            " holds 30 rows") in got.detail, got.detail
    assert ("events partition pmax: 30 rows stranded in the catch-all on"
            " target (src=0 dst=30)") in got.detail, got.detail


def test_mysql_a_partition_only_the_target_has(my_pair, reports):
    _my_source()
    _my_db("dst", [MY_JULY, MY_AUGUST, MY_SEPT, MY_MAX],
           MY_JULY_ROWS + MY_AUGUST_ROWS)
    got = _my_partitions()
    assert got.status == "diff", got
    assert "events partition p202609: extra on target (empty)" in \
        got.detail, got.detail


def test_mysql_a_partition_whose_rows_changed(my_pair, reports):
    _my_source()
    _my_db("dst", [MY_JULY, MY_AUGUST, MY_MAX],
           MY_JULY_ROWS + MY_AUGUST_ROWS
           + " update events set v = 'changed' where id = 105;")
    got = _my_partitions()
    assert got.status == "diff", got
    assert ("events partition p202608: 30 rows both sides, content"
            " differs") in got.detail, got.detail


def test_mysql_hash_partitions_of_another_number(my_pair, reports):
    table = ("create table h (id int primary key, v int)"
             " partition by hash (id) partitions {};"
             " insert into h with recursive g(n) as (select 1 union all"
             " select n + 1 from g where n < 40) select n, n from g;")
    for side, n in (("src", 4), ("dst", 3)):
        my(side, "drop database if exists pt; create database pt; use pt;"
           + table.format(n))
    got = _my_partitions()
    assert got.status == "diff", got
    assert ("h partition p3: missing on target, the source's holds 10"
            " rows") in got.detail, got.detail


def test_mysql_the_same_partitions_on_both_sides(my_pair, reports):
    _my_source()
    _my_db("dst", [MY_JULY, MY_AUGUST, MY_MAX],
           MY_JULY_ROWS + MY_AUGUST_ROWS)
    got = _my_partitions()
    assert got.status == "ok", got
    assert "1 partitioned tables, 3 partitions" in got.detail, got.detail
    assert "compared by count only" not in got.detail, got.detail
