"""The PostgreSQL engine reads a side's catalogue in one statement, and
answers every reader of a table's shape from it (audit 2026-10-01, SC3,
SC4, SC15, DU5, DU18, DU19, DU26).

Columns, keys, rules and indexes were a statement a table each - about
16,000 for a check of 2,000 tables. "A user table" was written seven ways,
so the counts and the copy between engines compared different sets. Row
estimates came from three places with three "unknown"s, one of them 0. A
key's order was decided three ways. The "unchanged" marker ignored the
table's shape, so a column dropped on the target read as unchanged. And
drift compared a column's name and a type without its length.
"""
import pytest

from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

pair = servers.fixture("pg")

SHAPES = """
create table public.shaped (
  b int not null,
  a text collate "C" default 'x',
  n numeric(12,2) check (n >= 0),
  v varchar(50),
  g int generated always as (b * 2) stored,
  i int generated always as identity,
  primary key (b, a));
create index shaped_v on public.shaped (v) where v is not null;
create table public.parent (id int primary key);
create table public.child (id int primary key,
  p int references public.parent (id) on delete cascade);
create function public.stamp() returns trigger language plpgsql as
  $$ begin new.id := new.id; return new; end $$;
create trigger child_stamp before insert on public.child
  for each row execute function public.stamp();
create table public.parts (id int, at date) partition by range (at);
create table public.parts_a partition of public.parts
  for values from ('2020-01-01') to ('2021-01-01');
create table public.parts_b partition of public.parts
  for values from ('2021-01-01') to ('2022-01-01');
create schema __tool;
create table __tool.state (id int);
create table public.migkit_origin (origin text primary key);
create table public.migkit_audit (id int);
"""


@pytest.fixture
def db(pair):
    """The module's database, made again with the shapes on both sides."""
    for server in (pair.src, pair.dst):
        server.sql(f'drop database if exists "{pair.name}" with (force)')
        server.sql(f'create database "{pair.name}"')
        server.sql(SHAPES, pair.name)
    return pair.name


@pytest.fixture
def run(pair, db):
    """`run(side, sql)`: SQL on one side's database, from outside migkit."""
    return lambda side, sql: getattr(pair, side).sql(sql, db)


@pytest.fixture
def eng(pair, db, tmp_path):
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="catalogue", engine="postgres",
              source=pair.src.endpoint(), target=pair.dst.endpoint(),
              databases=[db])
    hop.report_dir = lambda d=None: tmp_path
    got = PostgresEngine(hop)
    yield got
    got.release_kept()


def test_the_catalogue_holds_the_whole_definition(eng, db):
    cat = eng.catalog("src", db)
    cols = {c.name: c for c in cat.columns["public.shaped"]}
    assert cols["v"].declared == "character varying(50)"
    assert cols["n"].declared == "numeric(12,2)"
    assert cols["b"].nullable is False and cols["v"].nullable is True
    assert cols["a"].default == "'x'::text" and cols["a"].collation == "C"
    assert cols["g"].extra == "generated always as ((b * 2)) stored"
    assert cols["g"].default is None
    assert cols["i"].extra == "identity always"
    # the key in key order, not the columns' order
    assert cat.keys["public.shaped"] == ("b", "a")
    assert eng.neutral_key("src", db, "public.shaped") == ["b", "a"]
    assert eng._pk_cols_of(db, "public.shaped") == ["b", "a"]
    ix = cat.indexes["public.shaped"]
    assert [(i.name, i.columns) for i in ix] == [("shaped_v", ("v",))]
    assert "WHERE (v IS NOT NULL)" in ix[0].definition
    fk = cat.fks["public.child"][0]
    assert (fk.columns, fk.ref_table, fk.ref_columns, fk.on_delete) == (
        ("p",), "public.parent", ("id",), "c")
    others = cat.others["public.shaped"]
    assert any(o.startswith("check shaped_n_check CHECK ((n >= (0)")
               for o in others), others
    assert any(o.startswith("trigger child_stamp ")
               for o in cat.others["public.child"])
    # the rules a copy builds a table by, from the same reading
    rules = eng.neutral_column_rules("src", db, "public.shaped")
    assert "g" not in rules and rules["i"]["identity"] is True
    assert eng._write_rules("src", db, "public.shaped") == {
        "always": {"i"}, "generated": {"g"}}


def test_one_statement_answers_every_table(eng, db, run, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    for n in range(20):
        run("src", f"create table public.t{n} (id int primary key, v text)")
    asked = []
    real = PostgresEngine._psql

    def counted(self, side, d, sql, *a, **k):
        asked.append(sql)
        return real(self, side, d, sql, *a, **k)
    monkeypatch.setattr(PostgresEngine, "_psql", counted)
    for n in range(20):
        t = f"public.t{n}"
        assert eng.neutral_columns("src", db, t) == [
            ("id", "integer"), ("v", "text")]
        assert eng.neutral_key("src", db, t) == ["id"]
        eng.neutral_column_rules("src", db, t)
        eng.neutral_indexes("src", db, t)
        eng.column_facts("src", db, t)
    # the catalogue once, and the server's version once
    assert len(asked) <= 2, asked


def test_a_change_migkit_makes_is_read_again_at_once(eng, db, run):
    assert eng.neutral_columns("src", db, "public.parent") == [
        ("id", "integer")]
    assert eng.neutral_columns("dst", db, "public.parent") == [
        ("id", "integer")]
    eng.execute_ddl("dst", db, "alter table public.parent add column w int")
    assert eng.neutral_columns("dst", db, "public.parent") == [
        ("id", "integer"), ("w", "integer")]
    # a table made since the reading is found by reading again
    run("src", "create table public.later (id int)")
    assert eng.neutral_columns("src", db, "public.later") == [
        ("id", "integer")]


def test_drift_sees_what_a_type_string_did_not(eng, db, run):
    from migkit import drift
    before = drift.shape(eng, "src", db)
    same = drift.changes(before, drift.shape(eng, "src", db))
    assert same == [], same
    for ddl in ("alter table public.shaped alter column v type"
                " varchar(100)",
                "alter table public.shaped alter column n set default 1",
                "alter table public.parent add constraint positive"
                " check (id > 0)",
                "create or replace function public.stamp() returns trigger"
                " language plpgsql as $$ begin return new; end $$"):
        run("src", ddl)
    got = drift.changes(before, drift.shape(eng, "src", db))
    said = " ".join(got)
    assert "public.shaped" in said and "public.parent" in said, got
    assert "public.child" in said, got
    # the resume's and the cutover's reading, one statement: with lengths
    cols = dict(eng.column_catalog("src", db)["public.shaped"])
    assert cols["v"] == "character varying(100)", cols


def test_one_rule_for_what_a_table_of_the_application_is(pair, eng, db):
    # another session's temporary table is no table of the application's
    import psycopg2
    held = psycopg2.connect(host=pair.src.host, port=pair.src.port,
                            user=pair.src.user, password=pair.src.password,
                            dbname=db)
    held.autocommit = True
    try:
        held.cursor().execute("create temp table theirs (id int)")
        logical = eng.user_tables("src", db)
        leaves = eng.user_tables("src", db, "leaves")
        every = eng.user_tables("src", db, "all", own=True)
    finally:
        held.close()
    assert "public.parts" in logical and "public.parts_a" not in logical
    assert "public.parts_a" in leaves and "public.parts" not in leaves
    assert {"public.parts", "public.parts_a", "__tool.state",
            "public.migkit_origin"} <= set(every)
    for got in (logical, leaves, every):
        assert not any(t.startswith("pg_temp") for t in got), got
    # migkit's own by its exact name; an application's `migkit_audit` is
    # the application's
    assert "public.migkit_origin" not in logical
    assert "public.migkit_audit" in logical
    assert "__tool.state" not in logical
    # a copy to another engine takes the partitioned table once
    assert "public.parts_b" not in eng.neutral_tables("src", db)
    assert "public.parts" in eng.neutral_tables("src", db)
    # the counts and the fast checksum read every table holding rows
    held_rows = eng._psql("src", db, eng.USER_TABLES).splitlines()
    assert "public.parts_a" in held_rows and "public.parts" not in held_rows
    assert "public.migkit_audit" in held_rows
    assert "public.migkit_origin" not in held_rows


def test_a_count_the_statistics_do_not_know_is_unknown_not_zero(eng, db,
                                                                 run):
    run("src", "insert into public.parent select g from"
               " generate_series(1, 1000) g")
    # the statistics lost - a reset, a fail-over - and never analysed:
    # unknown, where it was read as 0
    run("src", "select pg_stat_reset()")
    rows = eng.row_estimate("src", db)
    assert rows["public.parent"] is None, rows
    assert eng.table_facts("src", db)["public.parent"]["rows"] is None
    run("src", "analyze public.parent")
    assert eng.row_estimate("src", db)["public.parent"] == 1000
    run("src", "insert into public.parts values (1, '2020-06-01'),"
               " (2, '2021-06-01'); analyze public.parts_a;"
               " analyze public.parts_b")
    assert eng.row_estimate("src", db)["public.parts"] == 2
    # a big table whose count is not known is checked restartably
    run("src", "create table public.unknown (id bigint primary key)")
    assert "public.unknown" in eng._chunkable(db)
    run("src", "analyze public.unknown")
    assert "public.unknown" not in eng._chunkable(db)


def test_the_marker_moves_with_the_tables_shape(eng, db, run):
    """A column dropped on the target rewrites nothing, and the check said
    UNCHANGED over a table that had lost a column's data."""
    import time
    first = eng._change_markers("dst", db)
    time.sleep(1.2)
    assert eng._change_markers("dst", db)["public.shaped"] \
        == first["public.shaped"]
    run("dst", "alter table public.shaped drop column v")
    assert eng._change_markers("dst", db)["public.shaped"] \
        != first["public.shaped"]
    renamed = eng._change_markers("dst", db)["public.parent"]
    run("dst", "alter table public.parent rename column id to pid")
    assert eng._change_markers("dst", db)["public.parent"] != renamed
    # one statement a side for every table
    asked = []
    real = eng._psql
    eng._psql = lambda *a, **k: asked.append(a) or real(*a, **k)
    eng._change_markers("dst", db)
    assert len(asked) == 1, asked
