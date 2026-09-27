"""DuckDB as one side of a pair: a PostgreSQL table moved into a DuckDB
file and back, read back as it goes and checked, every type of the
table's landing as the value it was.

Asked by the owner: Parquet is supported, so is DuckDB? Before this, only
through the `generic` engine, which compares and cannot move, and nothing
had been run against DuckDB at all.
"""
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

TABLE = """
    create table public.items (id bigint primary key, name text,
        price numeric(12, 2), ratio double precision, active boolean,
        raw bytea, day date, at timestamp, zoned timestamptz, doc jsonb,
        ref uuid);
    insert into public.items select g, 'item-' || g || ' ‘café’',
        g / 7.0, g / 3.0, g % 2 = 0, decode(md5(g::text), 'hex'),
        date '2024-01-01' + g, timestamp '2024-01-01 00:00:00.123456'
        + g * interval '1 minute', timestamptz '2024-01-01 00:00:00+07'
        + g * interval '1 hour', jsonb_build_object('n', g, 'tags',
        jsonb_build_array('a', g)), md5(g::text)::uuid
      from generate_series(1, 3000) g;
    insert into public.items (id) values (0);"""


def _hop(tmp_path, src, dst, **kw):
    hop = Hop(name="dk", engine="hetero", source=src, target=dst, **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _pg(port):
    return Endpoint(host="127.0.0.1", port=port, user="postgres",
                    password="test")


def test_postgres_into_duckdb_and_back(pg_pair, tmp_path):
    import duckdb

    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    got = psql(pg_pair["src"], TABLE)
    assert got.returncode == 0, got.stderr
    path = tmp_path / "app.duckdb"
    into = HeteroEngine(_hop(tmp_path, _pg(pg_pair["src"]),
                             Endpoint(host=str(path)),
                             databases=["postgres"],
                             db_map={"postgres": "main"},
                             options={"source_engine": "postgres",
                                      "target_engine": "duckdb"}))
    into.move_table("postgres", "public", "items", 500_000,
                    _Checkpoint(tmp_path / "in.json"), [].append)
    con = duckdb.connect(str(path))
    got = con.execute("select count(*), count(raw), sum(price)::varchar"
                      " from main.items").fetchone()
    con.close()
    want = psql(pg_pair["src"], "select sum(price) from public.items"
                ).stdout.strip()
    assert got == (3001, 3000, want), (got, want)
    got = [(r.status, r.detail) for r in into.check_data("postgres")
           if r.check == "data"]
    assert [s for s, _ in got] == ["ok"], got

    # a value changed in the file is found
    con = duckdb.connect(str(path))
    con.execute("update main.items set ratio = ratio + 1e-9 where id = 7")
    con.close()
    got = [r.status for r in into.check_data("postgres")
           if r.check == "data"]
    assert got == ["diff"], got
    con = duckdb.connect(str(path))
    con.execute("update main.items set ratio = 7 / 3.0 where id = 7")
    con.close()

    # and out of the file into another PostgreSQL
    back = HeteroEngine(_hop(tmp_path, Endpoint(host=str(path)),
                             _pg(pg_pair["dst"]), databases=["main"],
                             db_map={"main": "postgres"},
                             options={"source_engine": "duckdb",
                                      "target_engine": "postgres"}))
    back.move_table("main", "", "items", 500_000,
                    _Checkpoint(tmp_path / "out.json"), [].append)
    got = [(r.status, r.detail) for r in back.check_data("main")
           if r.check == "data"]
    assert [s for s, _ in got] == ["ok"], got
    q = ("select count(*), md5(string_agg(concat_ws('|', id, name, price,"
         " active, encode(raw, 'hex'), day, at, doc::text, ref), ','"
         " order by id)) from public.items")
    assert psql(pg_pair["src"], q).stdout == psql(pg_pair["dst"], q).stdout
