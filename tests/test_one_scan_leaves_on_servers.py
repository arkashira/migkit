"""The leaves and the sketch computed by PostgreSQL and MySQL are the numbers
this process computes over the same rows - the rendering, the salt, the
60-bit slice and the exact sums all the same - so a tree read from a
server, one read into this process and one kept up from a change stream
can be compared leaf for leaf (backlog R20.8-9, F1).

And a table past the row walk's reach, across two engines, has its
differing rows named from the leaves: an integer key, a composite text key
the old halving refused, and a table with no key.

One pair of servers, removed after: MySQL 8.4 `migkit-test-proof-my` on
127.0.0.1:16172 and PostgreSQL 16 `migkit-test-proof-pg` on 16174; a
same-engine hop is two databases on one of them.
"""
import socket
import subprocess
import time

import pytest

from migkit import leaves
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

MY, MY_PORT = "migkit-test-proof-my", 16172
PG, PG_PORT = "migkit-test-proof-pg", 16174


def my(sql, db=""):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"] + ([db] if db else []),
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def pg(sql, db="postgres"):
    got = subprocess.run(["docker", "exec", "-i", "-e", "PGPASSWORD=test",
                          PG, "psql", "-U", "postgres", "-h", "127.0.0.1",
                          "-d", db, "-v", "ON_ERROR_STOP=1", "-At"],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _up(name, port, ask):
    end = time.time() + 180
    while time.time() < end:
        ok = subprocess.run(["docker", "exec", name] + ask,
                            capture_output=True).returncode == 0
        with socket.socket() as s:
            s.settimeout(2)
            if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(2)
    pytest.fail(f"{name} never answered")


@pytest.fixture(scope="module")
def servers():
    for name in (MY, PG):
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", MY, "-p",
                    f"127.0.0.1:{MY_PORT}:3306", "-e",
                    "MYSQL_ROOT_PASSWORD=test", "mysql:8.4",
                    "--gtid-mode=ON", "--enforce-gtid-consistency=ON"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", PG, "-p",
                    f"127.0.0.1:{PG_PORT}:5432", "-e",
                    "POSTGRES_PASSWORD=test", "postgres:16"],
                   check=True, capture_output=True)
    try:
        _up(MY, MY_PORT, ["mysql", "-uroot", "-ptest", "-h127.0.0.1",
                          "--protocol=tcp", "-e", "select 1"])
        _up(PG, PG_PORT, ["env", "PGPASSWORD=test", "psql", "-U",
                          "postgres", "-h", "127.0.0.1", "-c", "select 1"])
        yield
    finally:
        for name in (MY, PG):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


class InProcess:
    """An engine read into this process whatever it could compute in SQL:
    what the server's numbers are held to."""
    SQL_DIALECT = None

    def __init__(self, eng):
        self._eng = eng

    def __getattr__(self, name):
        return getattr(self._eng, name)


def _pg_pair(where):
    """A PostgreSQL hop from `proofsrc` to `proofdst` on one server, as
    the pair machinery sees it."""
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=PG_PORT, user="postgres",
                  password="test")
    hop = Hop(name="proof_pg", engine="postgres", source=ep, target=ep,
              databases=["proofsrc"], db_map={"proofsrc": "proofdst"})
    hop.report_dir = lambda db=None: where
    return PostgresEngine(hop)._as_pair()


def _pg_dbs(src_sql, dst_sql=None):
    for db in ("proofsrc", "proofdst"):
        pg(f"drop database if exists {db}")
        pg(f"create database {db}")
    pg(src_sql, "proofsrc")
    pg(src_sql if dst_sql is None else dst_sql, "proofdst")


def _my_pair(where):
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    hop = Hop(name="proof_my", engine="mysql", source=ep, target=ep,
              databases=["shop"], db_map={"shop": "shop_t"})
    hop.report_dir = lambda db=None: where
    return MySQLEngine(hop)._as_pair()


PG_MIX = """
drop table if exists public.mix, public.bag;
create table public.mix (id bigint primary key, t text, n numeric(12,3),
  b boolean, ts timestamp, d date, f float8, bt bytea, nul text);
insert into public.mix
  select g, 'row ' || g || case when g % 7 = 0 then E'\\n|:' else '' end,
         g * 1.5, g % 2 = 0, timestamp '2024-01-01' + g * interval '37 min',
         date '2020-01-01' + g, g / 7.0, decode(md5(g::text), 'hex'), case when g % 3 = 0 then null
         else 'v' end
    from generate_series(1, 3000) g;
create table public.bag (a int, b text);
insert into public.bag select g % 50, 'x' || (g % 50)
  from generate_series(1, 2000) g;
"""

MY_MIX = """
drop database if exists shop; drop database if exists shop_t;
create database shop; create database shop_t;
use shop;
create table mix (id bigint primary key, t varchar(100), n decimal(12,3),
  b tinyint(1), ts datetime(6), d date, f double, bt varbinary(40),
  nul varchar(10));
set session cte_max_recursion_depth = 100000;
insert into mix with recursive g(n) as (select 1 union all select n + 1
  from g where n < 3000)
  select n, concat('row ', n, if(n % 7 = 0, '\\n|:', '')), n * 1.5,
         n % 2 = 0, timestamp('2024-01-01') + interval n * 37 minute,
         date('2020-01-01') + interval n day, n / 7.0, unhex(md5(n)),
         if(n % 3 = 0, null, 'v') from g;
create table bag (a int, b varchar(10));
insert into bag with recursive g(n) as (select 1 union all select n + 1
  from g where n < 2000) select n % 50, concat('x', n % 50) from g;
create table shop_t.mix like shop.mix;
insert into shop_t.mix select * from shop.mix;
create table shop_t.bag like shop.bag;
insert into shop_t.bag select * from shop.bag;
"""


def _held_to_this_process(pair, db, table, n):
    """The server's leaves and sketch equal the ones this process computes
    over the same rows, for each way of making leaves."""
    sc, dc, _ = pair._column_plan(table, table, db)
    a, _ = leaves.sides(pair, db, table, table, sc, dc, (None, None))
    here = leaves.Side(InProcess(a.eng), a.side, a.db, a.table, a.cols,
                       a.key, a.where)
    assert a.sql and not here.sql
    kinds = ("hash", "range") if a.key else ("row",)
    for kind in kinds:
        spec = leaves.plan(n, kind, 1, n, salt="c0ffee")
        server, local = leaves.scan(a, spec), leaves.scan(here, spec)
        assert server.root[0] == n
        assert server.leaves == local.leaves, kind
        w = leaves.sketch_width(40)
        assert leaves.sketch(a, spec, w).cells == \
            leaves.sketch(here, spec, w).cells, kind
    return sc


def test_postgres_computes_the_numbers_this_process_does(servers, tmp_path):
    _pg_dbs(PG_MIX)
    pair = _pg_pair(tmp_path)
    sc = _held_to_this_process(pair, "proofsrc", "public.mix", 3000)
    # every kind of value is in the row: a column left out would pass.
    # JSON within one engine is compared in the server's own text, which
    # this process does not write (`kept.OPAQUE`); across two, it is folded
    # here on both sides (the composite-key test below)
    assert {c for _, c in sc} >= {"integer", "text", "decimal", "boolean",
                                  "timestamp", "date", "float", "bytes"}, sc
    _held_to_this_process(pair, "proofsrc", "public.bag", 2000)


def test_mysql_computes_the_numbers_this_process_does(servers, tmp_path):
    my(MY_MIX)
    pair = _my_pair(tmp_path)
    _held_to_this_process(pair, "shop", "mix", 3000)
    _held_to_this_process(pair, "shop", "bag", 2000)


def _hetero(tmp_path, **options):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="proof_x", engine="hetero",
              options={"source_engine": "mysql",
                       "target_engine": "postgres", **options},
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["cx"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _twin(mysql_sql, pg_sql):
    my("drop database if exists cx; create database cx;")
    my(mysql_sql, "cx")
    pg("drop database if exists cx")
    pg("create database cx")
    pg(pg_sql, "cx")


def _data(eng):
    got = [r for r in eng.check_data("cx") if r.check == "data"]
    assert len(got) == 1, [r.__dict__ for r in got]
    return got[0]


def test_a_text_composite_key_past_the_walk_is_localized_across_engines(
        servers, tmp_path):
    _twin("set session cte_max_recursion_depth = 100000;"
          " create table t (region varchar(8), code varchar(20), v int,"
          " doc json, primary key (region, code));"
          " insert into t with recursive g(n) as (select 1 union all"
          " select n + 1 from g where n < 30000)"
          " select concat('r', n % 13), concat('Code-', n), n,"
          " json_object('n', n, 'x', 1.5) from g",
          "create table t (region varchar(8), code varchar(20), v int,"
          " doc jsonb, primary key (region, code));"
          " insert into t select 'r' || (g % 13), 'Code-' || g, g,"
          " jsonb_build_object('n', g, 'x', 1.5)"
          " from generate_series(1, 30000) g;"
          " update t set v = -1 where code = 'Code-29999';"
          " delete from t where code = 'Code-777';"
          " insert into t values ('r0', 'code-777', 7, null)")
    r = _data(_hetero(tmp_path))
    assert r.status == "diff", r.detail
    assert "1 missing on the target (r10/Code-777)" in r.detail, r.detail
    assert "1 with different values (r8/Code-29999)" in r.detail, r.detail
    assert "1 only on the target (r0/code-777)" in r.detail, r.detail
    assert "leaves that differ" in r.detail, r.detail
    assert "adds up to the whole difference" in r.detail, r.detail
    assert "differs in v" in r.detail, r.detail


def test_a_table_with_no_key_is_localized_by_value(servers, tmp_path):
    _twin("create table t (a int, b varchar(10));"
          " insert into t values (1, 'a'), (1, 'a'), (5, 'e')",
          "create table t (a int, b varchar(10));"
          " insert into t values (2, 'b'), (2, 'b'), (5, 'e')")
    r = _data(_hetero(tmp_path))
    assert r.status == "diff", r.detail
    assert "2 rows on the source that the target does not hold" in \
        r.detail, r.detail
    assert "2 rows on the target that the source does not hold" in \
        r.detail, r.detail
    assert "adds up to the whole difference" in r.detail, r.detail


def test_the_sketch_names_the_rows_on_both_servers(servers, tmp_path):
    _twin("set session cte_max_recursion_depth = 100000;"
          " create table t (id int primary key, v varchar(20));"
          " insert into t with recursive g(n) as (select 1 union all"
          " select n + 1 from g where n < 30000) select n, concat('v', n)"
          " from g",
          "create table t (id bigint primary key, v varchar(20));"
          " insert into t select g, 'v' || g from generate_series(1, 30000)"
          " g; update t set v = 'changed' where id = 29000;"
          " delete from t where id = 12345; insert into t values (30001, 'x')")
    eng = _hetero(tmp_path)
    sc, dc, _ = eng._column_plan("t", "public.t", "cx")
    a, b = leaves.sides(eng, "cx", "t", "public.t", sc, dc, (None, None))
    spec = leaves.plan(30000, "hash", salt="5a17")
    got, why = leaves.by_sketch(a, b, spec, expected=3)
    assert why is None and got.complete
    assert (got.missing, got.changed, got.extra) == (
        [("12345",)], [("29000",)], [("30001",)]), got
    # and the choice: few differences expected (an earlier run named
    # three) and a slow link - the sketch, with no leaf read
    eng._write_drill("cx", "t", missing=['["12345"]'], changed=[],
                     extra=['["30001"]'])
    import migkit.leaves as lv
    real = lv.rtt_ms
    lv.rtt_ms = lambda *a, **k: 80.0
    try:
        r = _data(eng)
    finally:
        lv.rtt_ms = real
    assert "found from a sketch of the differences (3 rows decoded)" in \
        r.detail, r.detail
    assert "1 with different values (29000)" in r.detail, r.detail


def test_the_bisecting_reader_localizes_where_it_measured_cheaper(
        servers, tmp_path):
    """One engine on both sides: the leaves read back first, then - never
    timed - the bisecting reader, whose rows are each read back and held
    to the two digests; and it is not asked a second time as the second
    reading."""
    from migkit.util import which
    if not which("reladiff"):
        pytest.skip("the bisecting reader is not installed here")
    make = ("create table public.w (id bigint primary key, v int, s text);"
            " insert into public.w select g, g % 100, md5(g::text)"
            " from generate_series(1, 30000) g;")
    _pg_dbs(make, make + " update public.w set v = -1 where id in"
                         " (7, 29000); delete from public.w where id ="
                         " 15000;")
    pair = _pg_pair(tmp_path)

    def data():
        got = [r for r in pair.check_data("proofsrc") if r.check == "data"
               and r.scope.endswith(".w")]
        assert len(got) == 1 and got[0].status == "diff", got
        return got[0].detail
    first = data()
    assert "leaves that differ" in first, first
    assert "read a second way by bisection: 1 rows only on the source," \
        " 0 only on the target, 2 changed" in first, first
    second = data()
    assert "found by bisecting the key range (3 rows named" in second, \
        second
    assert "adds up to the whole difference" in second, second
    assert "1 missing on the target (15000)" in second, second
    assert "2 with different values (7, 29000)" in second, second
    assert "read a second way by bisection" not in second, second
