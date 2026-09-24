"""Rows next to each other in a batch, of one table and one set of columns,
are written by one statement.

Measured on PostgreSQL 16, 20,000 upserts in one transaction: one statement
a row took 3.9s, and 1,000 rows to a statement took 0.06s. Across a network
every one of those statements is a round trip as well.
"""
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _lite(tmp_path):
    from migkit.engines.sqlite import SQLiteEngine
    path = tmp_path / "t.db"
    con = sqlite3.connect(path)
    con.executescript("create table a (id integer primary key, v text);"
                      " create table b (id integer primary key, v text);")
    con.close()
    ep = Endpoint(host=str(path), port=0, user="", password="")
    hop = Hop(name="r", engine="sqlite", source=ep, target=ep,
              databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


def _ins(table, i, **values):
    return {"op": "insert", "table": table, "key": {"id": i},
            "values": {"id": i, **values}}


def _del(table, i):
    return {"op": "delete", "table": table, "key": {"id": i}}


def test_only_rows_next_to_each_other_are_joined(tmp_path, monkeypatch):
    eng = _lite(tmp_path)
    runs = []
    real = type(eng)._apply_run

    def spy(self, side, db, shape, rows):
        runs.append((shape[0], shape[1], [k["id"] for k, _ in rows]))
        return real(self, side, db, shape, rows)
    monkeypatch.setattr(type(eng), "_apply_run", spy)
    eng.neutral_apply("dst", "main", [
        _ins("a", 1, v="x"), _ins("a", 2, v="y"),
        _ins("b", 1, v="z"),
        # the same table again after another: its own run, after it
        _ins("a", 3, v="w"),
        _del("a", 4), _del("a", 5),
        # fewer columns: another statement
        _ins("a", 6),
    ])
    assert runs == [("a", "upsert", [1, 2]), ("b", "upsert", [1]),
                    ("a", "upsert", [3]), ("a", "delete", [4, 5]),
                    ("a", "upsert", [6])], runs


def test_one_row_twice_is_two_statements(tmp_path, monkeypatch):
    """Two keys a batch keeps apart that the target reads as one row - the
    same number carried as two types - are not put in one statement, which
    PostgreSQL refuses (`cannot affect row a second time`)."""
    eng = _lite(tmp_path)
    runs = []
    real = type(eng)._apply_run

    def spy(self, side, db, shape, rows):
        runs.append(len(rows))
        return real(self, side, db, shape, rows)
    monkeypatch.setattr(type(eng), "_apply_run", spy)
    eng.neutral_apply("dst", "main", [
        {"op": "insert", "table": "a", "key": {"id": 1},
         "values": {"id": 1, "v": "p"}},
        {"op": "insert", "table": "a", "key": {"id": Decimal(1)},
         "values": {"id": Decimal(1), "v": "q"}}])
    assert runs == [1, 1], runs


MIXED = """
drop table if exists public.w, public.pair, public.gen, public.ident;
create table public.w (id int primary key, n numeric(12,2), at timestamptz,
                       raw bytea, doc jsonb, note text);
create table public.pair (a int, b text, v int, primary key (a, b));
create table public.gen (id int primary key, price int, qty int,
                         total int generated always as (price * qty) stored);
create table public.ident (id int generated always as identity primary key,
                           v text);
"""


def _pg(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    ep = Endpoint(host="127.0.0.1", port=pg_pair["dst"], user="postgres",
                  password="test")
    return PostgresEngine(Hop(name="r", engine="postgres", source=ep,
                              target=ep, databases=["postgres"]))


def _batch(n):
    at = datetime(2026, 9, 25, 1, 2, 3, tzinfo=timezone.utc)
    changes = [{"op": "insert", "table": "public.w", "key": {"id": i},
                "values": {"id": i, "n": Decimal(i) / 4, "at": at,
                           "raw": bytes([i % 256, 0]),
                           "doc": {"i": i} if i % 3 else None,
                           "note": None if i % 5 == 0 else f"r{i}"}}
               for i in range(1, n + 1)]
    changes += [{"op": "update", "table": "public.w", "key": {"id": i},
                 "values": {"note": "changed"}} for i in range(1, n, 7)]
    changes += [{"op": "delete", "table": "public.w", "key": {"id": i}}
                for i in range(2, n, 11)]
    changes += [{"op": "insert", "table": "public.pair",
                 "key": {"a": i % 3, "b": f"k{i}"},
                 "values": {"a": i % 3, "b": f"k{i}", "v": i}}
                for i in range(40)]
    changes += [{"op": "delete", "table": "public.pair",
                 "key": {"a": i % 3, "b": f"k{i}"}} for i in range(0, 40, 4)]
    changes += [{"op": "insert", "table": "public.gen", "key": {"id": i},
                 "values": {"id": i, "price": i, "qty": 2, "total": -1}}
                for i in range(1, 6)]
    changes += [{"op": "insert", "table": "public.ident", "key": {"id": i},
                 "values": {"id": i, "v": f"v{i}"}} for i in range(1, 6)]
    return changes


def _one(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _one_a_row(monkeypatch):
    """Every run written a row at a time, by the same statements."""
    from migkit.engines.postgres import PostgresEngine
    many = PostgresEngine.__dict__["_apply_upserts"]
    gone = PostgresEngine.__dict__["_apply_deletes"]
    monkeypatch.setattr(
        PostgresEngine, "_apply_run",
        lambda self, side, db, shape, rows: [
            gone(self, side, db, shape[0], [k]) if shape[1] == "delete"
            else many(self, side, db, shape[0], [(k, v)])
            for k, v in rows])


@needs_docker
def test_postgres_runs_land_what_one_a_row_landed(pg_pair, monkeypatch):
    """The same batch applied a row a statement and a run a statement
    leaves the same tables, across the page size, composite keys, a
    generated column and an identity column."""
    eng = _pg(pg_pair)
    port = pg_pair["dst"]
    got = {}
    for how in ("one a row", "runs"):
        _one(port, MIXED)
        if how == "one a row":
            _one_a_row(monkeypatch)
        batch = _batch(2500)
        try:
            eng.neutral_apply("dst", "postgres", batch)
            # a replay changes nothing
            eng.neutral_apply("dst", "postgres", batch)
        finally:
            monkeypatch.undo()
        got[how] = [_one(port, f"select md5(string_agg(t::text, '|' order"
                               f" by t::text)), count(*) from {t} t")
                    for t in ("public.w", "public.pair", "public.gen",
                              "public.ident")]
    assert got["runs"] == got["one a row"], got
    assert got["runs"][0].endswith("|2272"), got
    assert got["runs"][1].endswith("|30"), got
    assert _one(port, "select count(*) from public.w where note ="
                      " 'changed'") == "325"
    assert _one(port, "select total from public.gen where id = 5") == "10"
    _one(port, "drop table public.w, public.pair, public.gen, public.ident")


class _Counted:
    """The connection a batch writes on, counting the statements sent -
    the round trips, which are what a run saves."""

    def __init__(self, conn, sent):
        self._conn, self._sent = conn, sent

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def cursor(self, *a, **k):
        real, sent = self._conn.cursor(*a, **k), self._sent

        class Cursor:
            def __getattr__(self, name):
                return getattr(real, name)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                real.close()
                return False

            def execute(self, *a, **k):
                sent.append(1)
                return real.execute(*a, **k)
        return Cursor()


@needs_docker
def test_postgres_sends_a_statement_a_run_not_a_row(pg_pair, monkeypatch):
    """Counted rather than timed: 5,000 rows are five statements of a
    thousand, and one a row was 5,000. (The time measured was 0.06s
    against 3.9s for 20,000; a timing under a loaded machine is not a
    test.)"""
    from migkit.engines.postgres import PostgresEngine
    eng = _pg(pg_pair)
    port = pg_pair["dst"]
    _one(port, "drop table if exists public.s;"
               " create table public.s (id int primary key, v text)")
    sent = {"runs": [], "one a row": []}
    real = PostgresEngine._open_writer
    try:
        for how, base in (("runs", 0), ("one a row", 100000)):
            monkeypatch.setattr(PostgresEngine, "_open_writer",
                                lambda self, side, db, how=how:
                                _Counted(real(self, side, db), sent[how]))
            if how == "one a row":
                _one_a_row(monkeypatch)
            batch = [{"op": "insert", "table": "public.s",
                      "key": {"id": base + i},
                      "values": {"id": base + i, "v": "x" * 40}}
                     for i in range(5000)]
            eng.neutral_apply("dst", "postgres", batch)
            monkeypatch.undo()
        assert _one(port, "select count(*) from public.s") == "10000"
        assert len(sent["runs"]) == 5, len(sent["runs"])
        assert len(sent["one a row"]) == 5000, len(sent["one a row"])
    finally:
        _one(port, "drop table if exists public.s")


MY, MY_PORT = "migkit-test-runs-my", 15780


def my(sql):
    got = subprocess.run(["docker", "exec", "-i", MY, "mysql", "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


@pytest.fixture(scope="module")
def mysql_server():
    subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)
    try:
        subprocess.run(["docker", "run", "-d", "--name", MY, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{MY_PORT}:3306",
                        "mysql:8.4"], check=True, capture_output=True)
        for _ in range(90):
            if subprocess.run(["docker", "exec", MY, "mysql", "-uroot",
                               "-ptest", "-h127.0.0.1", "--protocol=tcp",
                               "-e", "select 1"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail("mysql never answered")
        yield
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", MY], capture_output=True)


def _my():
    from migkit.engines.mysql import MySQLEngine
    ep = Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                  password="test")
    return MySQLEngine(Hop(name="r", engine="mysql", source=ep, target=ep,
                           databases=["cx"]))


@needs_docker
def test_mysql_runs_land_the_batch(mysql_server):
    my("drop database if exists cx; create database cx;"
       " create table cx.w (id int primary key, n decimal(12,2),"
       "  raw varbinary(8), note text);"
       " create table cx.pair (a int, b varchar(10), v int,"
       "  primary key (a, b));"
       " create table cx.gen (id int primary key, price int, qty int,"
       "  total int generated always as (price * qty) stored)")
    changes = [{"op": "insert", "table": "w", "key": {"id": i},
                "values": {"id": i, "n": Decimal(i) / 4,
                           "raw": bytes([i % 256, 0]),
                           "note": None if i % 5 == 0 else f"r{i}"}}
               for i in range(1, 2501)]
    changes += [{"op": "update", "table": "w", "key": {"id": i},
                 "values": {"note": "changed"}} for i in range(1, 2500, 7)]
    changes += [{"op": "delete", "table": "w", "key": {"id": i}}
                for i in range(2, 2500, 11)]
    changes += [{"op": "insert", "table": "pair",
                 "key": {"a": i % 3, "b": f"k{i}"},
                 "values": {"a": i % 3, "b": f"k{i}", "v": i}}
                for i in range(40)]
    changes += [{"op": "delete", "table": "pair",
                 "key": {"a": i % 3, "b": f"k{i}"}} for i in range(0, 40, 4)]
    changes += [{"op": "insert", "table": "gen", "key": {"id": i},
                 "values": {"id": i, "price": i, "qty": 2, "total": -1}}
                for i in range(1, 6)]
    eng = _my()
    eng.neutral_apply("dst", "cx", changes)
    eng.neutral_apply("dst", "cx", changes)
    assert my("select count(*), sum(note = 'changed'), sum(note is null),"
              " sum(n) from cx.w") == "2272\t325\t390\t710284.00"
    assert my("select count(*), sum(v) from cx.pair") == "30\t600"
    assert my("select group_concat(total order by id) from cx.gen") == \
        "2,4,6,8,10"
    assert my("select hex(raw) from cx.w where id = 257") == "0100"


@needs_docker
def test_mysql_a_row_of_nothing_but_its_key_replays(mysql_server):
    """A table whose columns are all its key: replaying the insert was
    error 1062, and a tail that replays after a failure stopped there on
    every attempt. It is left as it is now."""
    my("drop database if exists cx; create database cx;"
       " create table cx.link (a int, b int, primary key (a, b))")
    batch = [{"op": "insert", "table": "link", "key": {"a": 1, "b": i},
              "values": {"a": 1, "b": i}} for i in range(3)]
    eng = _my()
    eng.neutral_apply("dst", "cx", batch)
    eng.neutral_apply("dst", "cx", batch)
    assert my("select count(*) from cx.link") == "3"


@needs_docker
def test_the_reader_asks_a_tables_key_once_a_batch(mysql_server,
                                                   monkeypatch):
    """It asked for every event, on a connection of its own: 472 one-row
    transactions a second left the tail 38 seconds behind."""
    from migkit.engines.mysql import MySQLEngine
    my("set global binlog_row_metadata = 'FULL';"
       " drop database if exists cx; create database cx;"
       " create table cx.t (id int primary key, v int)")
    eng = _my()
    token = eng.change_point("src", "cx")
    my("".join(f"insert into cx.t values ({i}, {i});\n" for i in range(50)))
    asked = []
    real = MySQLEngine._pk_cols
    monkeypatch.setattr(MySQLEngine, "_pk_cols",
                        lambda self, db, t: asked.append(t)
                        or real(self, db, t))
    changes, _ = eng.neutral_changes("src", "cx", token)
    assert [c["key"]["id"] for c in changes] == list(range(50)), changes
    assert asked == ["t"], asked
