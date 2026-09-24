"""Between two servers of the same engine, a type with no rendering shared
across engines is compared by that engine's own text of it.

An interval, a range, a text search vector, a bit string: none has a
canonical rendering, so the pair machinery left them out of the comparison
with a footnote. The pair machinery now also compares same-engine tables -
a column mapping on a PostgreSQL or MySQL hop, SQLite's own - so a changed
value in such a column read "every compared column equal". One engine on
both sides and one declared type give the same text for the same value,
which is what is compared now. Across engines they are still left out,
and said.
"""
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

DDL = ("create table public.odd (id int primary key, span interval,"
       " during int4range, words tsvector, flags bit(4))")
ROWS = ("insert into public.odd values"
        " (1, '1 day 2 hours', '[1,5)', to_tsvector('simple', 'a b'),"
        " B'1010'), (2, '3 minutes', '[2,3)', to_tsvector('simple', 'c'),"
        " B'0001')")


def _pair(pg_pair, tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="own", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)._as_pair()


def test_they_are_compared_and_a_difference_is_one(pg_pair, tmp_path):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, DDL + "; " + ROWS)
    pair = _pair(pg_pair, tmp_path)
    got = [r for r in pair.check_data("postgres", "odd") if r.check == "data"]
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert "not compared" not in got[0].detail, got[0].detail
    schema = [r for r in pair.check_schema("postgres", only=["public.odd"])]
    assert [r.status for r in schema] == ["ok"], [r.detail for r in schema]
    psql(pg_pair["dst"], "update public.odd set span = '1 day 3 hours'"
                         " where id = 1")
    got = [r for r in pair.check_data("postgres", "odd") if r.check == "data"]
    assert [r.status for r in got] == ["diff"], [r.detail for r in got]
    assert "1 with different values" in got[0].detail, got[0].detail


def test_a_table_built_on_the_same_engine_keeps_those_types(pg_pair,
                                                            tmp_path):
    psql(pg_pair["src"], DDL + "; " + ROWS)
    pair = _pair(pg_pair, tmp_path)
    said = []
    pair.create_missing("postgres", said.append)
    got = psql(pg_pair["dst"], "select string_agg(data_type, ',' order by"
                               " column_name) from information_schema.columns"
                               " where table_name = 'odd'").stdout.strip()
    # by column name: during, flags, id, span, words - every one as the
    # source wrote it, the key included
    assert got == "int4range,bit,integer,interval,tsvector", (got, said)


def test_a_table_built_on_the_same_engine_keeps_its_exact_types(pg_pair,
                                                                tmp_path):
    """Through the classes, a `timestamptz` was built as `timestamp(6)`,
    its offset gone, and an `int` as `bigint`."""
    psql(pg_pair["src"], "create table public.exact (id int primary key,"
                         " at timestamptz, v varchar(20), n numeric(8,3))")
    _pair(pg_pair, tmp_path).create_missing("postgres", lambda m: None)
    got = psql(pg_pair["dst"], "select string_agg(column_name || ' '"
                               " || data_type, ', ' order by column_name)"
                               " from information_schema.columns"
                               " where table_name = 'exact'").stdout.strip()
    assert got == ("at timestamp with time zone, id integer, n numeric,"
                   " v character varying"), got
    got = psql(pg_pair["dst"], "select character_maximum_length from"
                               " information_schema.columns where"
                               " table_name = 'exact' and column_name = 'v'"
                               ).stdout.strip()
    assert got == "20", got
