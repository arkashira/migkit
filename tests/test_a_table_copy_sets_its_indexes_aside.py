"""A table copied from its start goes in with its secondary indexes set
aside and built once after - in the PostgreSQL copier, the MySQL one and
the copier every pair shares.

Measured before: the table copiers wrote every row through every index.
A million rows into a PostgreSQL table with three secondary indexes: 7.46s
with them in place, 4.42s as a bare copy and a build; 6.52s and 3.82s in
four ranges. MySQL, the same shape: 11.62s and 10.45s, 8.17s and 6.57s in
four, with the table's indexes built in one statement (one each had made
it slower than leaving them).
"""
import json
import socket
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

INDEXES = ("create index big_a on public.big (a);"
           " create index big_bc on public.big (b, c);"
           " create unique index big_u on public.big (u)")


def _defs(port):
    return psql(port, "select indexname || ' ' || indexdef from pg_indexes"
                      " where tablename = 'big' order by 1").stdout


def _seed(pg_pair, rows=60_000):
    got = psql(pg_pair["src"], f"""
        create table public.big (id bigint primary key, a text, b int,
                                 c numeric, u text);
        insert into public.big select g, md5(g::text), g % 97, g / 3.0,
               'u-' || g from generate_series(1, {rows}) g;
        analyze public.big""")
    assert got.returncode == 0, got.stderr
    got = psql(pg_pair["dst"], "create table public.big (id bigint primary"
                               " key, a text, b int, c numeric, u text);"
                               + INDEXES)
    assert got.returncode == 0, got.stderr


@pytest.fixture
def engine(pg_pair, tmp_path, monkeypatch):
    from migkit import ranges
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="ixw", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=4)
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    monkeypatch.setattr(ranges, "active", ranges.Slots(4))
    monkeypatch.setattr(PostgresEngine, "INDEX_WINDOW_FROM", 50_000)
    return PostgresEngine(hop)


def _same(pg_pair):
    q = ("select count(*), md5(string_agg(id || a || b || c || u, ','"
         " order by id)) from public.big")
    return psql(pg_pair["src"], q).stdout == psql(pg_pair["dst"], q).stdout


def test_a_large_table_is_copied_with_its_indexes_set_aside(
        engine, pg_pair, tmp_path, monkeypatch):
    from migkit.cli import _Checkpoint
    _seed(pg_pair)
    before = _defs(pg_pair["dst"])
    seen = []
    real = engine._copy_checked

    def watching(*a, **k):
        # what the target has while a range is written
        seen.append(psql(pg_pair["dst"], "select string_agg(indexname, ','"
                                         " order by indexname) from"
                                         " pg_indexes where tablename ="
                                         " 'big'").stdout.strip())
        return real(*a, **k)
    monkeypatch.setattr(engine, "_copy_checked", watching)
    said = []
    engine.move_table("postgres", "public", "big", 500_000,
                      _Checkpoint(tmp_path / "m.json"), said.append)
    # the unique index and the key stayed while the rows went in
    assert seen and set(seen) == {"big_pkey,big_u"}, seen
    assert "2 secondary indexes dropped for the load;" in " ".join(said)
    assert "2 indexes dropped for the load and rebuilt after" in said, said
    assert _defs(pg_pair["dst"]) == before
    assert _same(pg_pair)
    assert not list(tmp_path.glob("dropped-indexes.*.json"))


def test_a_small_table_or_one_part_copied_keeps_its_indexes(
        engine, pg_pair, tmp_path):
    from migkit.cli import _Checkpoint
    _seed(pg_pair, rows=20_000)
    said = []
    engine.move_table("postgres", "public", "big", 500_000,
                      _Checkpoint(tmp_path / "m.json"), said.append)
    assert not any("dropped for the load" in m for m in said), said
    # a table part of which an earlier run copied: those ranges were
    # written with the indexes in place and are read back by them
    _seed_more = psql(pg_pair["src"], "insert into public.big select g,"
                                      " md5(g::text), g % 97, g / 3.0, 'u-'"
                                      " || g from generate_series(20001,"
                                      " 60000) g; analyze public.big")
    assert _seed_more.returncode == 0
    ck = _Checkpoint(tmp_path / "m.json")
    ck["public.big"].pop("done")
    ck.save()
    said = []
    engine.move_table("postgres", "public", "big", 500_000,
                      _Checkpoint(tmp_path / "m.json"), said.append)
    assert not any("dropped for the load" in m for m in said), said
    assert _same(pg_pair)


def test_a_copy_that_fails_puts_the_indexes_back_on_its_way_out(
        engine, pg_pair, tmp_path, monkeypatch):
    from migkit.cli import _Checkpoint
    _seed(pg_pair)
    before = _defs(pg_pair["dst"])
    real, calls = engine._copy_checked, []

    def dies_on_the_third(*a, **k):
        calls.append(1)
        if len(calls) == 3:
            raise RuntimeError("the connection went away")
        return real(*a, **k)
    monkeypatch.setattr(engine, "_copy_checked", dies_on_the_third)
    said = []
    with pytest.raises(RuntimeError):
        engine.move_table("postgres", "public", "big", 500_000,
                          _Checkpoint(tmp_path / "m.json"), said.append)
    assert "2 indexes dropped for the load and rebuilt after" in said, said
    assert _defs(pg_pair["dst"]) == before


def test_indexes_a_killed_copy_set_aside_are_built_by_the_next(
        engine, pg_pair, tmp_path, monkeypatch):
    """A process killed with the indexes off leaves only its record. The
    next move builds what it lists and the target lacks - even where every
    table is done and nothing is copied."""
    from migkit import cli
    from migkit.cli import _Checkpoint
    monkeypatch.setattr(cli, "_changelog", lambda *a, **k: None)
    _seed(pg_pair)
    before = _defs(pg_pair["dst"])
    engine.move_table("postgres", "public", "big", 500_000,
                      _Checkpoint(tmp_path / "move.json"), [].append)
    definition = psql(pg_pair["dst"], "select indexdef from pg_indexes"
                                      " where indexname = 'big_a'"
                                      ).stdout.strip()
    psql(pg_pair["dst"], "drop index public.big_a")
    # the record a process that is gone had written
    (tmp_path / f"dropped-indexes.{socket.gethostname()}.999999.1.json"
     ).write_text(json.dumps({"public.big_a": definition}))
    ck = _Checkpoint(tmp_path / "move.json")
    printed = []
    monkeypatch.setattr(cli.console, "print",
                        lambda m="", *a, **k: printed.append(str(m)))
    cli._copy_tables(engine.hop, engine, "postgres", [("public", "big")],
                     500_000, ck)
    assert any("1 indexes dropped for the load and rebuilt after" in m
               for m in printed), printed
    assert _defs(pg_pair["dst"]) == before
    assert not list(tmp_path.glob("dropped-indexes.*.json"))


def test_the_shared_copier_sets_the_target_indexes_aside(pg_pair, tmp_path,
                                                        monkeypatch):
    from migkit.cli import _Checkpoint
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.postgres import PostgresEngine
    path = tmp_path / "a.db"
    conn = sqlite3.connect(path)
    conn.execute("create table big (id integer primary key, a text, b int,"
                 " c real, u text)")
    conn.executemany("insert into big values (?, ?, ?, ?, ?)",
                     [(g, f"a{g}", g % 97, g / 4, f"u-{g}")
                      for g in range(1, 30_001)])
    conn.commit()
    conn.close()
    got = psql(pg_pair["dst"], "create table public.big (id bigint primary"
                               " key, a text, b int, c double precision,"
                               " u text);" + INDEXES)
    assert got.returncode == 0, got.stderr
    before = _defs(pg_pair["dst"])
    hop = Hop(name="sqx", engine="hetero",
              source=Endpoint(host=str(path), port=0, user="", password=""),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              db_map={"main": "postgres"},
              options={"source_engine": "sqlite",
                       "target_engine": "postgres"})
    hop.report_dir = lambda db=None: tmp_path
    eng = HeteroEngine(hop)
    monkeypatch.setattr(PostgresEngine, "INDEX_WINDOW_FROM", 10_000)
    monkeypatch.setattr(eng.src_engine, "_rows_of",
                        lambda side, db, table: 30_000)
    said = []
    eng.move_table("main", "", "big", 500_000,
                   _Checkpoint(tmp_path / "m.json"), said.append)
    assert "2 indexes dropped for the load and rebuilt after" in said, said
    assert _defs(pg_pair["dst"]) == before
    assert psql(pg_pair["dst"], "select count(*), sum(b) from public.big"
                ).stdout.strip() == \
        f"30000|{sum(g % 97 for g in range(1, 30_001))}"
    assert [r.status for r in eng.check_data("main")
            if r.check == "data"] == ["ok"]
