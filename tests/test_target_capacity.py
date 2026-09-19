"""Rows the target has no room for, counted before anything moves.

A narrower column on the target is not worth an argument until a row
actually exceeds it - and then it is what stops the load halfway through,
with part of the table already on the other side and no obvious way back.

So this check does not report that `varchar(255)` became `varchar(50)`. It
reports that **three rows are longer than fifty characters and the longest
is 120**, which is a decision somebody can make before starting rather than
an error somebody reads at 2am.

The wider version of the same problem, measured on live servers: MySQL
stores `0000-00-00` quite happily with `sql_mode` relaxed, and PostgreSQL
answers `select '0000-00-00'::date` with `ERROR: date/time field value out
of range`. No checksum can warn about that, because by the time a checksum
runs the load has already stopped.

The one thing this must not do is compare limits measured in different
units. MySQL's TEXT family is limited in **bytes** and `varchar(n)` in
**characters**, so a utf8mb4 string of 20,000 characters can overflow a
65,535-byte TEXT. Those pairs are counted and said out loud rather than
quietly passed.
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
    hop = Hop(name="cap", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=2)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _both(pg_pair, src_sql, dst_sql):
    for port, sql in ((pg_pair["src"], src_sql), (pg_pair["dst"], dst_sql)):
        got = psql(port, "drop table if exists public.people;" + sql)
        assert got.returncode == 0, got.stderr


def test_rows_too_long_for_the_target_are_counted_and_measured(pg_pair,
                                                                tmp_path):
    _both(pg_pair,
          "create table public.people (id int primary key, note varchar(255));"
          " insert into public.people values (1, repeat('x', 120)),"
          " (2, repeat('y', 80)), (3, 'short');",
          "create table public.people (id int primary key, note varchar(50));")
    got = _engine(pg_pair, tmp_path)._capacity_gaps("postgres")
    assert got.status == "diff", got.detail
    assert "public.people.note 2 rows" in got.detail, got.detail
    assert "largest 120" in got.detail, got.detail
    assert "against the target's 50" in got.detail, got.detail
    assert "halfway through" in got.fix_hint or "widen" in got.fix_hint


def test_a_narrower_column_nothing_exceeds_is_not_an_alarm(pg_pair,
                                                            tmp_path):
    """Reporting the narrowing itself would cry wolf on every rebuilt
    schema, and the report nobody reads is the one that misses the real
    finding."""
    _both(pg_pair,
          "create table public.people (id int primary key, note varchar(255));"
          " insert into public.people values (1, 'short'), (2, 'also short');",
          "create table public.people (id int primary key, note varchar(50));")
    got = _engine(pg_pair, tmp_path)._capacity_gaps("postgres")
    assert got.status == "ok", got.detail
    assert "1 columns are narrower" in got.detail, got.detail
    assert "no row exceeds any of them yet" in got.detail, got.detail


def test_unlimited_text_landing_in_a_varchar_is_caught(pg_pair, tmp_path):
    """`text` has no limit to compare, which is exactly why it must not be
    read as "nothing to worry about" - it is the widest source there is."""
    _both(pg_pair,
          "create table public.people (id int primary key, note text);"
          " insert into public.people values (1, repeat('x', 300));",
          "create table public.people (id int primary key, note varchar(50));")
    got = _engine(pg_pair, tmp_path)._capacity_gaps("postgres")
    assert got.status == "diff", got.detail
    assert "largest 300" in got.detail, got.detail


def test_a_number_too_big_for_the_target_is_caught(pg_pair, tmp_path):
    _both(pg_pair,
          "create table public.people (id int primary key, n bigint);"
          " insert into public.people values (1, 5000000000), (2, 7);",
          "create table public.people (id int primary key, n integer);")
    got = _engine(pg_pair, tmp_path)._capacity_gaps("postgres")
    assert got.status == "diff", got.detail
    assert "public.people.n 1 rows" in got.detail, got.detail
    assert "5000000000" in got.detail, got.detail
    # and the server agrees the value really does not fit
    refused = psql(pg_pair["dst"], "select 5000000000::integer")
    assert refused.returncode != 0
    assert "out of range" in refused.stderr, refused.stderr


def test_a_decimal_past_the_target_precision_is_caught(pg_pair, tmp_path):
    _both(pg_pair,
          "create table public.people (id int primary key, amt numeric(20,4));"
          " insert into public.people values (1, 123456789.5), (2, 1.25);",
          "create table public.people (id int primary key,"
          " amt numeric(10,2));")
    got = _engine(pg_pair, tmp_path)._capacity_gaps("postgres")
    assert got.status == "diff", got.detail
    assert "public.people.amt 1 rows" in got.detail, got.detail
    refused = psql(pg_pair["dst"], "select 123456789.5::numeric(10,2)")
    assert refused.returncode != 0, refused.stdout


def test_a_widening_target_is_not_a_finding(pg_pair, tmp_path):
    _both(pg_pair,
          "create table public.people (id int primary key, note varchar(50));"
          " insert into public.people values (1, repeat('x', 40));",
          "create table public.people (id int primary key, note text);")
    got = _engine(pg_pair, tmp_path)._capacity_gaps("postgres")
    assert got.status == "ok", got.detail
    assert "no column on the target is narrower" in got.detail, got.detail


def test_the_full_deep_report_carries_it(pg_pair, tmp_path):
    _both(pg_pair,
          "create table public.people (id int primary key, note varchar(255));"
          " insert into public.people values (1, repeat('x', 120));",
          "create table public.people (id int primary key, note varchar(50));")
    got = [r for r in _engine(pg_pair, tmp_path).check_deep("postgres")
           if r.scope.endswith("target capacity")]
    assert len(got) == 1, [r.scope for r in got]
    assert got[0].status == "diff", got[0].detail


def test_the_capacity_table_needs_no_server():
    assert canon.capacity("postgres", "character varying(50)") == ("chars", 50)
    assert canon.capacity("postgres", "text") == ("chars", None)
    assert canon.capacity("postgres", "integer")[0] == "int"
    assert canon.capacity("postgres", "numeric(10,2)") == ("numeric", 10, 2)
    assert canon.capacity("mysql", "int unsigned") == ("int", 0, 4294967295)
    assert canon.capacity("mysql", "text") == ("bytes", 65535)
    assert canon.capacity("mysql", "varchar(80)") == ("chars", 80)
    # a type nobody measured says nothing rather than guessing a size
    assert canon.capacity("postgres", "jsonb") is None

    wider = canon.capacity("postgres", "character varying(255)")
    narrow = canon.capacity("postgres", "character varying(50)")
    assert canon.narrower(wider, narrow) is True
    assert canon.narrower(narrow, wider) is False
    # unlimited into a limit is the widest source there is
    assert canon.narrower(("chars", None), narrow) is True
    assert canon.narrower(narrow, ("chars", None)) is False
    # characters and bytes are not compared at all
    assert canon.narrower(("chars", 1000), ("bytes", 255)) is False
    assert canon.narrower(canon.capacity("mysql", "mediumtext"),
                          canon.capacity("mysql", "text")) is True
    # and an unmeasured type cannot make a claim either way
    assert canon.narrower(None, narrow) is False

    with pytest.raises(ValueError):
        canon.narrower(("nonsense", 1), ("nonsense", 2))


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    hit = eng._capacity_result(
        "x", [("t", "note", "varchar(255)", "varchar(50)", 3, "120",
               ("chars", 50))], 1, 0)
    assert hit.status == "diff" and "3 rows, largest 120" in hit.detail

    safe = eng._capacity_result("x", [], 2, 0)
    assert safe.status == "ok" and "2 columns are narrower" in safe.detail

    none = eng._capacity_result("x", [], 0, 0)
    assert none.status == "ok"
    assert "no column on the target is narrower" in none.detail

    # pairs measured in different units are said out loud, not passed
    mixed = eng._capacity_result("x", [], 1, 2)
    assert mixed.status == "skip", mixed.detail
    assert "characters against bytes" in mixed.detail, mixed.detail

    with pytest.raises(ValueError):
        eng._capacity_probe("c", ("unheard-of", 1))


def test_an_engine_that_cannot_count_says_so(tmp_path):
    from migkit.engines.base import Engine
    hop = Hop(name="c", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = Engine(hop)
    assert eng._scalar("src", "x", "select 1") is None
    got = eng._capacity_gaps("x")
    assert got.status == "skip", got.detail
    assert "cannot be asked to count" in got.detail


# ---- mysql, so this is not a postgres-only capability ------------------

MY = "migkit-test-cap-my"
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


def test_mysql_counts_the_same_overflow(mysql_one, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    sql = ("drop database if exists csrc; drop database if exists cdst;"
           " create database csrc; create database cdst;"
           " create table csrc.people (id int primary key, note varchar(255),"
           " n bigint);"
           " create table cdst.people (id int primary key, note varchar(50),"
           " n int);"
           " insert into csrc.people values (1, repeat('x',120), 5000000000),"
           " (2, 'short', 7);")
    got = subprocess.run(["docker", "exec", MY, "mysql", "-uroot", "-ptest",
                          "-e", sql], capture_output=True, text=True)
    assert got.returncode == 0, got.stderr

    hop = Hop(name="cap", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=mysql_one, user="root",
                              password="test"),
              databases=["csrc"], db_map={"csrc": "cdst"})
    hop.report_dir = lambda db=None: tmp_path
    res = MySQLEngine(hop)._capacity_gaps("csrc")
    assert res.status == "diff", res.detail
    assert "people.note 1 rows" in res.detail, res.detail
    assert "people.n 1 rows" in res.detail, res.detail
    assert "largest 120" in res.detail, res.detail
