"""Two columns that compare equal and do not mean the same thing.

A checksum reads the values as they now stand and will agree that
`2020-11-01 01:05:00` equals `2020-11-01 01:05:00`. What it cannot say is
that the source column recorded an *instant* and the target column records
digits off a wall clock, because that is a fact about the type rather than
about any row. The conversion happens on the way across, once, and every
check afterwards agrees with the result.

Measured on live servers before this was written:

    postgres  insert '2020-11-01 01:05:00+04'
                timestamp    -> 2020-11-01 01:05:00
                timestamptz  -> 2020-10-31 21:05:00+00

Four hours, discarded from a value that named its own offset. And across a
DST boundary `2026-11-01 01:30:00-04` and `...-05` are two different
instants - `05:30:00+00` and `06:30:00+00` - that a wall-clock column both
records as `01:30:00`.

The trap that makes this worth a check rather than a footnote is that
**`timestamp` means opposite things in the two engines**, also measured:

    mysql  written at time_zone '+00:00', read back at '+07:00'
             datetime   2026-07-01 12:00:00   (unchanged - a wall clock)
             timestamp  2026-07-01 19:00:00   (converted - an instant)

PostgreSQL's `timestamp` is the wall clock; MySQL's is the instant. A MySQL
`timestamp` landing in a PostgreSQL `timestamp` looks like the identity
mapping and is the silent conversion above.
"""
import socket
import subprocess
import time

import pytest

from migkit import canon
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _engine(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="temporal", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _both(pg_pair, src_sql, dst_sql):
    for port, sql in ((pg_pair["src"], src_sql), (pg_pair["dst"], dst_sql)):
        got = psql(port, "drop table if exists public.events;" + sql)
        assert got.returncode == 0, got.stderr


def test_an_instant_landing_in_a_wall_clock_is_named(pg_pair, tmp_path):
    _both(pg_pair,
          "create table public.events (id int primary key,"
          " at timestamptz, note text);",
          "create table public.events (id int primary key,"
          " at timestamp, note text);")
    got = _engine(pg_pair, tmp_path)._temporal_meaning("postgres")
    assert got.status == "diff", got.detail
    assert "public.events.at" in got.detail, got.detail
    assert "instant" in got.detail and "wall clock" in got.detail, got.detail
    assert "offset is dropped" in got.detail, got.detail


def test_the_other_direction_is_a_finding_too(pg_pair, tmp_path):
    """A wall clock read as an instant invents an offset instead of losing
    one, which is the same accident facing the other way."""
    _both(pg_pair,
          "create table public.events (id int primary key, at timestamp);",
          "create table public.events (id int primary key, at timestamptz);")
    got = _engine(pg_pair, tmp_path)._temporal_meaning("postgres")
    assert got.status == "diff", got.detail
    assert "at timestamp without time zone (wall clock) ->" in got.detail, \
        got.detail
    assert "timestamp with time zone (instant)" in got.detail, got.detail


def test_matching_columns_are_counted_not_merely_passed(pg_pair, tmp_path):
    _both(pg_pair,
          "create table public.events (id int primary key, at timestamptz,"
          " day date, note text);",
          "create table public.events (id int primary key, at timestamptz,"
          " day date, note text);")
    got = _engine(pg_pair, tmp_path)._temporal_meaning("postgres")
    assert got.status == "ok", got.detail
    assert "2 temporal columns" in got.detail, got.detail


def test_a_table_with_no_temporal_column_is_not_a_finding(pg_pair, tmp_path):
    _both(pg_pair,
          "create table public.events (id int primary key, note text);",
          "create table public.events (id int primary key, note text);")
    got = _engine(pg_pair, tmp_path)._temporal_meaning("postgres")
    assert got.status == "skip", got.detail
    assert "no temporal columns" in got.detail


def test_the_full_deep_report_carries_it(pg_pair, tmp_path):
    _both(pg_pair,
          "create table public.events (id int primary key, at timestamptz);",
          "create table public.events (id int primary key, at timestamp);")
    got = [r for r in _engine(pg_pair, tmp_path).check_deep("postgres")
           if r.scope.endswith("temporal meaning")]
    assert len(got) == 1, [r.scope for r in got]
    assert got[0].status == "diff", got[0].detail


def test_the_server_really_does_what_the_check_is_warning_about(pg_pair):
    """The check is only worth having if the loss is real. This is the
    measurement, run against the server rather than quoted from a docstring.
    """
    port = pg_pair["src"]
    got = psql(port, "drop table if exists public.proof;"
                     " create table public.proof (a timestamp, b timestamptz);"
                     " insert into public.proof values"
                     " ('2020-11-01 01:05:00+04','2020-11-01 01:05:00+04');")
    assert got.returncode == 0, got.stderr
    read = psql(port, "set timezone='UTC';"
                      " select a::text||' / '||b::text from public.proof;")
    assert read.stdout.strip().splitlines()[-1] == \
        "2020-11-01 01:05:00 / 2020-10-31 21:05:00+00", read.stdout

    # and the ambiguous hour: two instants, one set of digits
    ambiguous = psql(port, "set timezone='UTC'; select"
                           " ('2026-11-01 01:30:00-04'::timestamptz)::text"
                           " ||' / '||"
                           " ('2026-11-01 01:30:00-05'::timestamptz)::text")
    assert ambiguous.stdout.strip().splitlines()[-1] == \
        "2026-11-01 05:30:00+00 / 2026-11-01 06:30:00+00", ambiguous.stdout
    flattened = psql(port, "select"
                           " ('2026-11-01 01:30:00-04'::timestamp)::text"
                           " ||' / '||"
                           " ('2026-11-01 01:30:00-05'::timestamp)::text")
    assert flattened.stdout.strip() == \
        "2026-11-01 01:30:00 / 2026-11-01 01:30:00", flattened.stdout


def test_the_type_table_needs_no_server():
    """Pinned because the two engines use the same word for opposite things
    and a plausible-looking edit would swap them."""
    assert canon.time_meaning("postgres", "timestamp with time zone") == \
        canon.INSTANT
    assert canon.time_meaning("postgres", "timestamp(6) with time zone") == \
        canon.INSTANT
    assert canon.time_meaning("postgres", "timestamp without time zone") == \
        canon.WALL
    assert canon.time_meaning("postgres", "timestamp") == canon.WALL

    # the reversal, which is the whole reason for the table
    assert canon.time_meaning("mysql", "timestamp") == canon.INSTANT
    assert canon.time_meaning("mysql", "datetime(6)") == canon.WALL

    for not_temporal in ("varchar(50)", "text", "int"):
        assert canon.time_meaning("mysql", not_temporal) is None, not_temporal
    # an engine whose temporal types were never measured says nothing
    assert canon.time_meaning("sqlite", "datetime") is None


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="t", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    bad = eng._temporal_meaning_result(
        "x", [("t", "at", "timestamptz", canon.INSTANT, "timestamp",
               canon.WALL)], 0, 3)
    assert bad.status == "diff" and "t.at" in bad.detail

    fine = eng._temporal_meaning_result("x", [], 0, 3)
    assert fine.status == "ok" and "3 temporal columns" in fine.detail

    # columns on an unmeasured engine are not agreement
    silent = eng._temporal_meaning_result("x", [], 4, 1)
    assert silent.status == "skip", silent.detail
    assert "has not measured" in silent.detail, silent.detail

    nothing = eng._temporal_meaning_result("x", [], 0, 0)
    assert nothing.status == "skip" and "no temporal columns" in nothing.detail


# ---- mysql, where the same word means the other thing -------------------

MY = "migkit-test-temporal-my"
MY_PORT = 13403


@pytest.fixture(scope="module")
def mysql_one():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                    "mysql:8"], check=True, capture_output=True)
    end = time.time() + 180
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", MY_PORT)) == 0:
                break
        time.sleep(1)
    for _ in range(60):
        if subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                           "-e", "select 1"],
                          capture_output=True).returncode == 0:
            break
        time.sleep(2)
    else:
        pytest.fail("mysql never answered")
    yield MY_PORT
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _my(sql):
    return subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                           "-N", "-B", "-e", sql], capture_output=True,
                          text=True)


def test_mysql_datetime_against_timestamp_is_the_same_finding(mysql_one,
                                                               tmp_path):
    from migkit.engines.mysql import MySQLEngine
    for name, typ in (("tsrc", "timestamp"), ("tdst", "datetime")):
        got = _my(f"drop database if exists {name}; create database {name};"
                  f" create table {name}.events (id int primary key,"
                  f" at {typ});")
        assert got.returncode == 0, got.stderr

    hop = Hop(name="temporal", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              databases=["tsrc"], db_map={"tsrc": "tdst"})
    hop.report_dir = lambda db=None: tmp_path
    got = MySQLEngine(hop)._temporal_meaning("tsrc")
    assert got.status == "diff", got.detail
    assert "events.at" in got.detail, got.detail
    assert "timestamp (instant)" in got.detail, got.detail
    assert "datetime (wall clock)" in got.detail, got.detail


def test_the_mysql_server_really_converts_one_and_not_the_other(mysql_one):
    """The measurement behind the table, against the server."""
    got = _my("drop database if exists tproof; create database tproof;"
              " create table tproof.t (dt datetime, ts timestamp);"
              " set time_zone='+00:00';"
              " insert into tproof.t values"
              " ('2026-07-01 12:00:00','2026-07-01 12:00:00');")
    assert got.returncode == 0, got.stderr
    at_utc = _my("set time_zone='+00:00';"
                 " select concat_ws(' / ', dt, ts) from tproof.t;")
    at_bkk = _my("set time_zone='+07:00';"
                 " select concat_ws(' / ', dt, ts) from tproof.t;")
    assert at_utc.stdout.strip() == \
        "2026-07-01 12:00:00 / 2026-07-01 12:00:00", at_utc.stdout
    assert at_bkk.stdout.strip() == \
        "2026-07-01 12:00:00 / 2026-07-01 19:00:00", at_bkk.stdout
