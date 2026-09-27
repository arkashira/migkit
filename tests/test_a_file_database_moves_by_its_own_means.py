"""SQLite and DuckDB move file to file by their own means: the source
attached to the target read-only and each table copied by one INSERT ...
SELECT inside the database, with no row through Python.

Measured (500,000 rows, one table, the whole move with its read-back):
SQLite to SQLite through the table copier 10.3s, by SQLite itself 1.1s;
DuckDB to DuckDB 115s (37.8s once the copier wrote through Arrow), by
DuckDB itself 4.5s. A table the hop excludes is left alone, a table with a
row filter or
mapped columns still goes table by table, a table already on the target
is emptied and filled again, and a table the target lacks arrives with the
source's keys, indexes and triggers.
"""
import sqlite3

import duckdb
import pytest


def _run(*argv):
    from click.testing import CliRunner

    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join((got.output + str(got.exception or "")).split())


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    with sqlite3.connect(src) as con:
        con.executescript(
            "create table orders (id integer primary key autoincrement,"
            " item text not null, qty int check (qty > 0));"
            " create index by_item on orders(item);"
            " create trigger stamp after insert on orders begin select 1;"
            " end;"
            " with recursive g(value) as (select 1 union all select"
            " value + 1 from g where value < 5000) insert into orders"
            " (item, qty) select 'item-' || value, value % 9 + 1 from g;"
            " create table kept (id int primary key, v text);"
            " insert into kept values (1, 'src'), (2, 'src');"
            " create table notes (id int primary key, v text);"
            " insert into notes values (1, 'a'), (2, 'b'), (3, 'c');"
            " create table audit (id int, v text);"
            " insert into audit values (1, 'source');")
    with sqlite3.connect(dst) as con:
        # the target's own: one table it owns, one it already has made
        con.executescript(
            "create table audit (id int, v text);"
            " insert into audit values (9, 'target');"
            " create table kept (id int primary key, v text);"
            " insert into kept values (7, 'old');")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {src}, user: x, password: x}}\n"
        f"    target: {{host: {dst}, user: x, password: x}}\n"
        "    databases: [main]\n    exclude: [audit]\n"
        "    mapping:\n      where:\n        notes: \"id < 3\"\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    return src, dst


def test_sqlite_tables_are_copied_inside_sqlite(lite):
    from migkit import movers
    src, dst = lite
    assert movers.pick("sqlite") == "native"
    got, said = _run("move", "lite", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    assert "orders: copied" in said and "notes: copied" not in said, said
    with sqlite3.connect(dst) as con:
        assert con.execute("select count(*), sum(qty) from orders"
                           ).fetchone() == (5000, sum(v % 9 + 1 for v in
                                                      range(1, 5001)))
        made = {r[0] for r in con.execute(
            "select name from sqlite_master where tbl_name = 'orders'")}
        assert {"orders", "by_item", "stamp"} <= made, made
        # its counter came with the rows
        assert con.execute("select seq from sqlite_sequence where name ="
                           " 'orders'").fetchone() == (5000,)
        # a table the target had: emptied and filled, its rows the source's
        assert con.execute("select id, v from kept order by id"
                           ).fetchall() == [(1, "src"), (2, "src")]
        # excluded: untouched; filtered: only what the filter selects
        assert con.execute("select * from audit").fetchall() == \
            [(9, "target")]
        assert con.execute("select id from notes order by id").fetchall() \
            == [(1,), (2,)]
    got, said = _run("check", "lite")
    assert got.exit_code == 0, said


def test_a_move_run_again_ends_the_same(lite):
    src, dst = lite
    for _ in range(2):
        got, said = _run("move", "lite", "--mode", "full", "--go")
        assert got.exit_code == 0, said
    with sqlite3.connect(dst) as con:
        assert con.execute("select count(*) from orders").fetchone() == \
            (5000,)


def test_the_source_file_is_not_written(lite):
    src, _ = lite
    before = src.read_bytes()
    got, said = _run("move", "lite", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    assert src.read_bytes() == before


def test_duckdb_tables_are_copied_inside_duckdb(tmp_path, monkeypatch):
    import migkit.config as cfg
    src, dst = tmp_path / "a.duckdb", tmp_path / "b.duckdb"
    with duckdb.connect(str(src)) as c:
        c.execute("create sequence ids start 1; create table events (id"
                  " bigint primary key default nextval('ids'), happened"
                  " timestamptz, doc json, amount decimal(12, 3));"
                  " insert into events (happened, doc, amount) select"
                  " timestamptz '2024-01-01 00:00:00+00' + to_minutes(range),"
                  " json_object('n', range), range / 8 from range(20000);"
                  " create table skip (id int)")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  duck:\n    engine: duckdb\n"
        f"    source: {{host: {src}}}\n    target: {{host: {dst}}}\n"
        "    databases: [main]\n    exclude: [skip]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    got, said = _run("move", "duck", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    assert "events: copied" in said, said
    with duckdb.connect(str(dst), read_only=True) as c:
        assert c.execute("select count(*), sum(amount)::varchar from events"
                         ).fetchone() == (20000, "24998750.000")
        assert c.execute("select count(*) from duckdb_constraints() where"
                         " table_name = 'events' and constraint_type ="
                         " 'PRIMARY KEY'").fetchone() == (1,)
        assert not c.execute("select * from duckdb_tables() where"
                             " table_name = 'skip'").fetchall()
    got, said = _run("check", "duck")
    assert got.exit_code == 0, said


def test_the_table_copier_builds_duckdb_columns_as_declared(tmp_path,
                                                            monkeypatch):
    """Measured before: DuckDB to DuckDB through the table copier built
    every column VARCHAR, and the read back stopped the copy on the first
    double and timestamp. The same engine on both sides takes the source's
    own type as it is written."""
    import migkit.config as cfg
    src, dst = tmp_path / "a.duckdb", tmp_path / "b.duckdb"
    with duckdb.connect(str(src)) as c:
        c.execute("create table t (id bigint primary key, n double, ts"
                  " timestamp, z timestamptz, m decimal(12, 3), j json);"
                  " insert into t select range, range / 7, timestamp"
                  " '2024-01-01' + to_seconds(range), timestamptz"
                  " '2024-01-01 00:00:00+00' + to_seconds(range), range / 8,"
                  " json_object('a', range) from range(50)")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  duck:\n    engine: duckdb\n"
        f"    source: {{host: {src}}}\n    target: {{host: {dst}}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    got, said = _run("move", "duck", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    assert "emptied -1" not in said, said
    with duckdb.connect(str(dst), read_only=True) as c:
        assert [r[1] for r in c.execute("describe t").fetchall()] == [
            "BIGINT", "JSON", "DECIMAL(12,3)", "DOUBLE", "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE"]
    got, said = _run("check", "duck")
    assert got.exit_code == 0, said


def test_the_table_copier_writes_sqlite_one_range_at_a_time(
        tmp_path, monkeypatch):
    """Measured before: SQLite to SQLite through the table copier split a
    large table into ranges written side by side, and the seventh of eight
    stopped on `database is locked` - a file takes one writer."""
    import migkit.config as cfg
    from migkit import ranges
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    with sqlite3.connect(src) as con:
        con.executescript(
            "create table t (id integer primary key, v text);"
            " with recursive g(x) as (select 1 union all select x + 1 from"
            " g where x < 20000) insert into t select x, 'v' || x from g;")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {src}}}\n    target: {{host: {dst}}}\n"
        "    databases: [main]\n    workers: 4\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.setenv("MIGKIT_MOVER", "builtin")
    monkeypatch.setattr(ranges, "LEAST", 1000)
    got, said = _run("move", "lite", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    # the lock is met only when ranges are long enough to outlast the
    # driver's wait, so what is held here is the cause: no ranges side by
    # side at all
    assert "locked" not in said and " ranges)" not in said, said
    with sqlite3.connect(dst) as con:
        assert con.execute("select count(*) from t").fetchone() == (20000,)


def _parquet_table(root, table, rows, key=("id",)):
    """A Parquet table as migkit writes one, made through the engine."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.parquet import ParquetEngine
    hop = Hop(name="mk", engine="parquet", source=Endpoint(host=str(root)),
              target=Endpoint(host=str(root)), databases=["lake"])
    eng = ParquetEngine(hop)
    cols = [("id", "integer", ()), ("name", "text", ()),
            ("amount", "decimal", (12, 2))]
    eng.neutral_create("dst", "lake", table, cols, list(key))
    for i in range(0, len(rows), 1000):
        eng.neutral_write("dst", "lake", table,
                          [("id", "integer"), ("name", "text"),
                           ("amount", "decimal")], rows[i:i + 1000])


def test_parquet_part_files_are_copied_as_they_are(tmp_path, monkeypatch):
    from decimal import Decimal

    import migkit.config as cfg
    src, dst = tmp_path / "src", tmp_path / "dst"
    _parquet_table(src, "orders", [(i, f"n{i}", Decimal(i) / 4)
                                   for i in range(5000)])
    _parquet_table(src, "left", [(1, "x", Decimal(1))])
    # a table already on the target, with a part the source never had
    _parquet_table(dst, "orders", [(99999, "stale", Decimal(0))])
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lake:\n    engine: parquet\n"
        f"    source: {{host: {src}}}\n    target: {{host: {dst}}}\n"
        "    databases: [lake]\n    exclude: [left]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    monkeypatch.delenv("MIGKIT_MOVER", raising=False)
    got, said = _run("move", "lake", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    assert "orders: 5 part files copied" in said, said
    parts = sorted(p.name for p in (src / "lake" / "orders").iterdir())
    assert sorted(p.name for p in (dst / "lake" / "orders").iterdir()) \
        == parts
    for name in parts:
        assert (dst / "lake" / "orders" / name).read_bytes() == \
            (src / "lake" / "orders" / name).read_bytes()
    assert not (dst / "lake" / "left").exists()
    got, said = _run("check", "lake")
    assert got.exit_code == 0, said


def test_a_parquet_snapshot_keeps_the_target_files(tmp_path):
    from decimal import Decimal

    from migkit.config import Endpoint, Hop
    from migkit.engines.parquet import ParquetEngine
    dst = tmp_path / "dst"
    _parquet_table(dst, "orders", [(i, "n", Decimal(i)) for i in range(10)])
    hop = Hop(name="lake", engine="parquet",
              source=Endpoint(host=str(tmp_path / "src")),
              target=Endpoint(host=str(dst)), databases=["lake"])
    point = tmp_path / "point"
    point.mkdir()
    ParquetEngine(hop).snapshot_state("lake", point)
    kept = sorted(p.name for p in (point / "dst-files" / "orders").iterdir())
    assert kept == sorted(p.name for p in (dst / "lake" / "orders").iterdir())
    import json
    listed = json.loads((point / "dst-files.json").read_text())
    assert set(listed["orders"]) == set(kept)
