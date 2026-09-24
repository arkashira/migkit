"""A DDL on the source in the middle of a move is said, not missed.

The rows copied before it and after it belong to two different tables,
and a move that says "complete" over that is believed. Where migkit reads
no change stream it compares the source's catalogue before and after;
simulated here the way it happens - the application alters a table while
the copy runs.
"""
import sqlite3

import pytest


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    src = tmp_path / "a.db"
    con = sqlite3.connect(src)
    con.executescript("create table orders (id integer primary key, v text);"
                      " insert into orders values (1, 'a'), (2, 'b');"
                      " create table people (id integer primary key);"
                      " insert into people values (1);")
    con.commit()
    con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {src}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return src


def _move():
    from click.testing import CliRunner

    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "lite", "--mode", "full",
                                        "--go"])
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_a_quiet_move_says_complete(lite):
    got, said = _move()
    assert got.exit_code == 0, said
    assert "move complete" in said, said


def test_a_column_added_mid_move_stops_the_complete(lite, monkeypatch):
    from migkit.engines.base import NeutralCopier
    real = NeutralCopier.move_table
    done = []

    def copy_then_alter(self, db, sch, tbl, chunk, ck, log):
        real(self, db, sch, tbl, chunk, ck, log)
        if not done:
            done.append(tbl)
            con = sqlite3.connect(lite)
            con.execute("alter table people add column name text")
            con.commit()
            con.close()

    monkeypatch.setattr(NeutralCopier, "move_table", copy_then_alter)
    got, said = _move()
    assert got.exit_code != 0, said
    assert "schema changed while it was being moved" in said, said
    assert "people: column name added" in said, said
    assert "move complete" not in said, said


def test_the_shape_reader_says_what_changed():
    from migkit import drift
    before = {"a": [("id", "int"), ("v", "text")], "gone": [("id", "int")]}
    after = {"a": [("id", "bigint"), ("w", "text")], "new": [("id", "int")]}
    got = drift.changes(before, after)
    assert "gone: dropped" in got and "new: created" in got, got
    assert ("a: column w added, column v dropped, column id int -> bigint"
            in got), got
    assert drift.changes(None, after) == []


def test_a_table_the_hop_excludes_is_not_watched(lite, monkeypatch, tmp_path):
    """Altering a table the move leaves alone does not stop the move."""
    import migkit.config as cfg
    conf = tmp_path / "hops.yaml"
    conf.write_text(conf.read_text() + "    exclude: [people]\n")
    from migkit.engines.base import NeutralCopier
    real = NeutralCopier.move_table

    def copy_then_alter(self, db, sch, tbl, chunk, ck, log):
        real(self, db, sch, tbl, chunk, ck, log)
        con = sqlite3.connect(lite)
        con.execute("alter table people add column n%d text" % len(tbl))
        con.commit()
        con.close()

    monkeypatch.setattr(NeutralCopier, "move_table", copy_then_alter)
    got, said = _move()
    assert got.exit_code == 0, said
    assert "move complete" in said, said


@pytest.mark.docker
def test_postgres_reads_its_whole_column_catalogue_in_one_query(pg_pair):
    """What the move compares before and after itself, without a query per
    table on a schema of thousands."""
    from migkit import drift
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    from tests.conftest import psql
    psql(pg_pair["src"], "create table public.a (id int, v text)")
    hop = Hop(name="d", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    eng = PostgresEngine(hop)
    before = drift.shape(eng, "src", "postgres")
    assert before["public.a"] == [("id", "integer"), ("v", "text")], before
    psql(pg_pair["src"], "alter table public.a add column w int")
    got = drift.changes(before, drift.shape(eng, "src", "postgres"))
    assert got == ["public.a: column w added"], got


@pytest.mark.docker
def test_mysql_reads_its_whole_column_catalogue_in_one_query():
    import socket
    import subprocess
    import time

    from migkit import drift
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    name, port = "migkit-test-ddl-my", 15667

    def my(sql):
        got = subprocess.run(["docker", "exec", "-i", name, "mysql",
                              "-uroot", "-ptest", "-N", "-B"], input=sql,
                             capture_output=True, text=True)
        assert got.returncode == 0, got.stderr

    subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                    "MYSQL_ROOT_PASSWORD=test", "-p", f"{port}:3306",
                    "mysql:8"], check=True, capture_output=True)
    try:
        end = time.time() + 180
        while time.time() < end:
            ok = subprocess.run(["docker", "exec", name, "mysql", "-uroot",
                                 "-ptest", "-h127.0.0.1",
                                 "--protocol=tcp", "-e", "select 1"],
                                capture_output=True).returncode == 0
            with socket.socket() as s:
                s.settimeout(2)
                if ok and s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(2)
        my("create database appdb; create table appdb.a (id int,"
           " v varchar(10));")
        ep = Endpoint(host="127.0.0.1", port=port, user="root",
                      password="test")
        eng = MySQLEngine(Hop(name="d", engine="mysql", source=ep,
                              target=ep, databases=["appdb"]))
        before = drift.shape(eng, "src", "appdb")
        assert before["a"] == [("id", "int"), ("v", "varchar(10)")], before
        my("alter table appdb.a modify v varchar(20);")
        got = drift.changes(before, drift.shape(eng, "src", "appdb"))
        assert got == ["a: column v varchar(10) -> varchar(20)"], got
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def test_a_table_by_table_move_leaves_statistics_behind_it(lite, tmp_path):
    """The table copier left the planner with nothing to go on; only the
    bulk path used to refresh statistics after a load."""
    got, said = _move()
    assert got.exit_code == 0, said
    assert "refreshed the target's statistics" in said, said
    con = sqlite3.connect(tmp_path / "b.db")
    try:
        stats = con.execute("select count(*) from sqlite_stat1").fetchone()[0]
    finally:
        con.close()
    assert stats > 0
