"""The conversion's breadth on servers, MySQL into PostgreSQL (backlog
CV.P5, CV.P10, CV.P11, CV.P15, CV.P17, CV.P19, CV.P20, CV.P21, W3).

One hop and its proof, every new part turned on: the application's code
read for its SQL (`application_sql`), the source's record of its
statements replayed (`workload_replay`), the hop restructuring two
tables (a column renamed, one left out), MySQL's own test cases for the
functions the routines call (`builtin_suites`), and the source's answers
kept for after the cutover (`code_reference_days`). Each routine and
statement differs from the source only where a new part looks:

* `bonus` parts at its second statement and shows it only at the end -
  the reports of its state say where (P5);
* `flag` answers apart at x = 123457 only, which no pool, constant or
  draw reaches and the solver does (P20); `twice` is equal for every
  input and is said so;
* `tag` calls LEFT, which MySQL's own test of a negative length shows
  answering otherwise on PostgreSQL (P17);
* the application's `status = 'open'` finds MySQL's 'OPEN' rows too
  (its collation folds case) and PostgreSQL's does not (P10, P11);
* reading the column the hop leaves out is refused by name (P21).

The servers are the shared `mysql` (the source, and the source's
sandbox) and `pg` (the target, and its sandbox) pairs; the index advice
needs a target with the hypothetical-index extension, a container of the
module's own.
"""
import json
import time

import pytest

from migkit.config import Hop
from tests import servers
from tests.conftest import needs_docker
from tests.waits import until

my = servers.fixture("mysql")
pg = servers.fixture("pg")

SOURCE = [
    "create table orders (id int primary key, status varchar(10) not null,"
    " total decimal(10,2), note varchar(40))",
    "insert into orders values (1, 'open', 10.50, null), (2, 'OPEN', 20.00,"
    " 'rush'), (3, 'closed', 5.25, 'x'), (4, 'open', 7.00, 'rush')",
    "create table items (id int primary key, order_id int, sku varchar(12),"
    " legacy_code varchar(8))",
    "insert into items values (1, 1, 'A-1', 'L1'), (2, 2, 'B-2', 'L2'),"
    " (3, 4, 'C-3', null)",
    """create function bonus(n int) returns int deterministic no sql
begin
  declare t int default 0;
  set t = n * 2;
  set t = t + 1;
  set t = t * 3;
  return t;
end""",
    "create function twice(x int) returns int deterministic no sql"
    " return x * 2",
    "create function flag(x int) returns int deterministic no sql return 0",
    "create function tag(s varchar(20)) returns varchar(20) deterministic"
    " no sql return left(s, 3)",
    "create table sales (id int, made date, amount decimal(10,2), primary"
    " key (id, made)) partition by range (year(made)) (partition p2024"
    " values less than (2025), partition p2025 values less than (2026),"
    " partition pmax values less than maxvalue)",
    "insert into sales values (1, '2024-03-01', 1.00), (2, '2025-12-31',"
    " 2.00), (3, '2026-01-01', 3.00), (4, '2024-12-31', 4.00)",
]

TARGET = """
create table orders (id integer primary key, status varchar(10) not null,
                     amount numeric(10,2), note varchar(40));
insert into orders values (1, 'open', 10.50, null), (2, 'OPEN', 20.00, 'rush'),
  (3, 'closed', 5.25, 'x'), (4, 'open', 7.00, 'rush');
create table items (id integer primary key, order_id integer,
                    sku varchar(12));
insert into items values (1, 1, 'A-1'), (2, 2, 'B-2'), (3, 4, 'C-3');
-- a step of 2 where the source's is 1: apart from the second statement
create function bonus(n integer) returns integer language plpgsql as $$
declare t integer := 0;
begin
  t := n * 2;
  t := t + 2;
  t := t * 3;
  return t;
end $$;
create function twice(x integer) returns integer language sql
  as $$ select x * 2 $$;
create function flag(x integer) returns integer language sql
  as $$ select case when x * 3 = 370371 then 1 else 0 end $$;
create function tag(s varchar) returns varchar language sql
  as $$ select left(s, 3) $$;
-- not partitioned: the source's partitioning is advised for it
create table sales (id integer, made date, amount numeric(10,2),
                    primary key (id, made));
insert into sales values (1, '2024-03-01', 1.00), (2, '2025-12-31', 2.00),
  (3, '2026-01-01', 3.00), (4, '2024-12-31', 4.00);
"""

APP = '''def open_orders(cur):
    cur.execute("SELECT id, total FROM orders WHERE status = 'open'")


def by_id(cur, oid):
    cur.execute("SELECT id, status, total FROM orders WHERE id = %s", (oid,))


def mark(cur, oid):
    cur.execute("UPDATE orders SET note = 'seen' WHERE id = %s", (oid,))


def legacy(cur, oid):
    cur.execute(f"SELECT * FROM orders WHERE id = {oid}")


def codes(cur):
    cur.execute("SELECT legacy_code FROM items WHERE id = %s", (1,))


def edge(cur):
    cur.execute("SELECT LEFT('abc', -1) AS x")
'''

SUITE = ("--echo # LEFT with a length below zero\n"
         "SELECT LEFT('abc', -1);\n"
         "SELECT LEFT('abc', 2), LEFT(NULL, 1);\n")


def _hop(my, pg, tmp, **options):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="breadth", engine="hetero", options={
        "source_engine": "mysql", "target_engine": "postgres",
        "proof_sandbox": {
            "source": {"host": my.dst.host, "port": my.dst.port,
                       "user": my.dst.user, "password": my.dst.password},
            "target": {"host": pg.dst.host, "port": pg.dst.port,
                       "user": pg.dst.user, "password": pg.dst.password}},
        **options},
        source=my.src.endpoint(), target=pg.src.endpoint(),
        databases=[my.name],
        mapping={"columns": {"orders": {"rename": {"total": "amount"}},
                             "items": {"drop": ["legacy_code"]}}})
    hop.report_dir = lambda db=None: tmp
    return HeteroEngine(hop)


@pytest.fixture(scope="module")
def proved(my, pg, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("breadth")
    db = my.name
    my.src.sql("set global log_bin_trust_function_creators = 1")
    for s in SOURCE:
        my.src.sql(s, db)
    pg.src.sql(TARGET, db)
    # the application's statements, as the source's record keeps them
    for _ in range(4):
        my.src.sql("SELECT id, total FROM orders WHERE status = 'open'", db)
    for _ in range(3):
        my.src.sql("SELECT id, status FROM orders WHERE id = 2", db)
    for _ in range(2):
        my.src.sql("UPDATE orders SET note = note WHERE id = 3", db)
    (tmp / "app").mkdir()
    (tmp / "app" / "orders.py").write_text(APP)
    (tmp / "suite").mkdir()
    (tmp / "suite" / "left.test").write_text(SUITE)
    (tmp / "reports").mkdir()
    eng = _hop(my, pg, tmp / "reports",
               application_sql=str(tmp / "app"), workload_replay=True,
               builtin_suites=str(tmp / "suite"), code_reference_days=30,
               index_advice=True)
    schema = []
    line = eng.prove_converted(db, schema)
    verdicts = {v["routine"]: v for v in json.loads(
        (tmp / "reports" / "code-proof.json").read_text())["verdicts"]}
    return SimpleSpace(eng=eng, db=db, line=line, verdicts=verdicts,
                       schema={r.scope: r for r in schema},
                       reports=tmp / "reports")


class SimpleSpace:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _app(proved):
    return {v["origin"].split(" ")[0]: v for v in json.loads(
        (proved.reports / "application-sql.json").read_text())}


@needs_docker
def test_where_two_versions_part_is_said(my, pg, proved):
    v = proved.verdicts["function bonus"]
    assert v["status"] == "diff", v
    # the end says 3 against 6; the states say where it began
    assert "they first part after the source's statement at line 4" in \
        v["reason"], v["reason"]
    assert "t holds 1 on the source and 2 on the target" in v["reason"], v
    # the copies that reported are gone from both sandboxes
    assert my.dst.sql("select count(*) from information_schema.routines"
                      " where routine_name like 'migkit_trace%'") == [(0,)]
    assert pg.dst.sql("select count(*) from pg_proc where proname like"
                      " 'migkit_trace%'", proved.db) == [(0,)]


@needs_docker
def test_the_solver_finds_what_no_draw_reaches_and_says_what_holds(proved):
    v = proved.verdicts["function flag"]
    assert v["status"] == "diff", v
    assert "x=123457" in v["reason"], v["reason"]
    v = proved.verdicts["function twice"]
    assert v["status"] in ("ok", "warn"), v
    assert "the solver finds no input they answer apart on" in \
        v["coverage"], v


@needs_docker
def test_mysqls_own_test_of_left_is_held_against_the_target(proved):
    line = proved.schema[f"{proved.db} built-in functions"]
    assert line.status == "warn" and "left.test:2" in line.detail, line
    kept = json.loads((proved.reports / "builtin-semantics.json")
                      .read_text())
    apart = kept["functions"]["left"]["apart"]
    assert [c["case"] for c in apart] == ["LEFT('abc', -1)"], apart
    assert apart[0]["source answers"] == "" and \
        apart[0]["target answers"] == "ab", apart
    # LEFT('abc', 2) and LEFT(NULL, 1) answered alike
    assert kept["functions"]["left"]["cases"] == 3
    v = proved.verdicts["function tag"]
    assert "calls left, which the target answers otherwise at" \
        " LEFT('abc', -1) (the engine's own test left.test:2)" in \
        v["coverage"], v


@needs_docker
def test_the_applications_code_is_read_carried_and_held(proved):
    line = proved.schema[f"{proved.db} application statements"]
    assert line.status == "diff", line
    got = _app(proved)
    assert got["orders.py:2"]["status"] == "diff", got["orders.py:2"]
    assert "answered otherwise on the target" in \
        got["orders.py:2"]["reason"]
    # the column the hop renames is read by its new name, and proved
    assert got["orders.py:6"]["status"] == "ok", got["orders.py:6"]
    assert "amount" in got["orders.py:6"]["target"]
    assert got["orders.py:10"]["status"] == "ok", got["orders.py:10"]
    assert got["orders.py:14"]["status"] == "not proved" and \
        "as it runs" in got["orders.py:14"]["reason"]
    assert got["orders.py:18"]["status"] == "not converted" and \
        "items.legacy_code, which the hop leaves out" in \
        got["orders.py:18"]["reason"], got["orders.py:18"]
    # a call MySQL's own test shows answering otherwise is refused, with
    # where that was written down
    assert got["orders.py:22"]["status"] == "not converted" and \
        "left.test:2" in got["orders.py:22"]["reason"], got["orders.py:22"]
    sql = (proved.reports / "application-sql.sql").read_text()
    assert "-- orders.py:6: ok" in sql and \
        "SELECT id, status, amount FROM orders WHERE id = %s;" in sql, sql
    # the application was read, never written
    assert "SELECT id, total FROM orders" in (
        proved.reports.parent / "app" / "orders.py").read_text()


@needs_docker
def test_the_sources_record_is_replayed_weighted_and_kept_masked(proved):
    line = proved.schema[f"{proved.db} workload replayed"]
    assert line.status == "diff", line
    assert "answered otherwise on the target" in line.detail, line.detail
    assert "runs on record" in line.detail, line.detail
    kept = (proved.reports / "workload-corpus.json").read_text()
    assert "'open'" not in kept and "status" in kept, kept
    statements = json.loads(kept)["statements"]
    assert any(s["runs"] >= 4 and s["kind"] == "read" for s in statements)
    assert any(s["kind"] == "write" for s in statements)


@needs_docker
def test_the_sources_answers_hold_the_target_after_the_cutover(pg, proved):
    from migkit.storedcode import reference
    kept = json.loads((proved.reports / reference.FILE).read_text())
    assert {"twice", "flag", "tag"} <= set(kept["routines"]), kept
    runs = proved.reports / "cutover-runs.jsonl"
    pair, db = proved.eng, proved.db
    try:
        runs.write_text(json.dumps({"state": "flipped",
                                    "ended": time.time()}) + "\n")
        got = reference.check(pair, db)
        # bonus never answered as the source did; twice does
        assert got.status == "diff" and "bonus(" in got.detail and \
            "twice(" not in got.detail, got.detail
        pg.src.sql("create or replace function twice(x integer) returns"
                   " integer language sql as $$ select x * 3 $$", db)
        got = reference.check(pair, db)
        assert "twice(" in got.detail and "where the source answered" in \
            got.detail, got.detail
        runs.write_text(json.dumps({"state": "flipped", "ended":
                                    time.time() - 40 * 86400}) + "\n")
        assert reference.check(pair, db).status == "skip"
    finally:
        runs.unlink()
        pg.src.sql("create or replace function twice(x integer) returns"
                   " integer language sql as $$ select x * 2 $$", db)


@needs_docker
def test_the_sources_partitioning_is_advised_with_its_routing_proved(
        proved):
    line = proved.schema[f"{proved.db} partitions advised"]
    assert line.status == "warn", line
    assert "sales: partitioned on the source by RANGE" in line.detail and \
        "puts every fixture row where the source keeps it" in line.detail, \
        line.detail
    made = (proved.reports / "partitions-for-the-target.sql").read_text()
    assert 'partition by range ((EXTRACT(YEAR FROM CAST("made" AS' \
        ' DATE))))' in made and '"sales_partitioned_p2025"' in made, made


@needs_docker
def test_a_partitioning_that_routes_otherwise_is_found(my, pg, proved):
    from migkit.storedcode import partitions
    from migkit.storedcode.prove import Proof
    proof = Proof(proved.eng, proved.db)
    proof.open(True)
    try:
        proof.begin([])
        scheme = partitions.schemes(proved.eng.src_engine, "src",
                                    proved.db)["sales"]
        # the second bound a year late: 2025's rows land in p2024
        wrong = dict(scheme, p={"p2024": "2026", "p2025": "2027",
                                "pmax": "MAXVALUE"})
        made, _ = partitions.advised("sales", wrong, "sales", "mysql",
                                     name="migkit_part_sales")
        got = partitions.routes(proof, "sales", made, "migkit_part_sales")
        assert any(there == "migkit_part_sales_p2025" and
                   here == "migkit_part_sales_p2024"
                   for _, there, here in got), got
        # and the right one routes every row as the source does
        made, _ = partitions.advised("sales", scheme, "sales", "mysql",
                                     name="migkit_part_sales")
        assert partitions.routes(proof, "sales", made,
                                 "migkit_part_sales") == []
    finally:
        proof.end()
        proof.close()
    # nothing of it stayed on the sandbox target
    assert pg.dst.sql("select count(*) from pg_class where relname like"
                      " 'migkit_part%'", proved.db) == [(0,)]


HYPO = ("FROM postgres:16\nRUN apt-get update && apt-get install -y"
        " --no-install-recommends postgresql-16-hypopg && rm -rf"
        " /var/lib/apt/lists/*\n")


@needs_docker
def test_an_index_is_advised_only_where_the_targets_planner_wants_it(
        my, tmp_path):
    from migkit import workload
    from migkit.config import Endpoint
    from migkit.engines.hetero import HeteroEngine
    db = my.name
    my.src.sql("create table events (id int primary key, kind varchar(10),"
               " at datetime)", db)
    my.src.sql("set session cte_max_recursion_depth = 1000000; insert into"
               " events with recursive s(g) as (select 1 union all select"
               " g + 1 from s where g < 200000) select g, concat('k', g %"
               " 500), '2026-01-01' from s; analyze table events", db)
    for _ in range(5):
        my.src.sql("SELECT count(*) FROM events WHERE kind = 'k7'", db)
    tag = servers.image("pg-hypopg", HYPO)
    # the target must run the extension already: a server of its own
    with servers.private(__name__, "hypopg", tag, ports=(5432,),
                         env={"POSTGRES_PASSWORD": "test"}) as (_, ports):
        import psycopg2
        target = Endpoint(host=servers.HOST, port=ports[5432],
                          user="postgres", password="test")

        def sql(text, dbname="postgres"):
            c = psycopg2.connect(host=target.host, port=target.port,
                                 user="postgres", password="test",
                                 dbname=dbname, connect_timeout=5)
            c.autocommit = True
            try:
                with c.cursor() as cur:
                    cur.execute(text)
            finally:
                c.close()
        until(lambda: sql("select 1") or True, 120,
              "the hypothetical-index target")
        sql(f'create database "{db}"')
        sql("create extension hypopg; create table events (id integer"
            " primary key, kind varchar(10), at timestamp); insert into"
            " events select g, 'k' || (g % 500), '2026-01-01' from"
            " generate_series(1, 200000) g; analyze events", db)
        hop = Hop(name="advice", engine="hetero", options={
            "source_engine": "mysql", "target_engine": "postgres",
            "index_advice": True},
            source=my.src.endpoint(), target=target, databases=[db])
        hop.report_dir = lambda d=None: tmp_path
        got = workload.compare(HeteroEngine(hop), db)
        said = f"{got.detail} {got.fix_hint}"
        assert "events (kind): the target's planner estimates" in said, said
        made = (tmp_path / "for-the-target.sql").read_text()
        assert 'create index "events_kind_idx" on "events" ("kind")' in \
            made, made
        # measured, never made: the target holds no such index
        c = psycopg2.connect(host=target.host, port=target.port,
                             user="postgres", password="test", dbname=db)
        try:
            with c.cursor() as cur:
                cur.execute("select count(*) from pg_indexes where"
                            " tablename = 'events'")
                assert cur.fetchone()[0] == 1
        finally:
            c.close()
