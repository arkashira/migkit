"""An answer a DDL on the source overtook is taken back, not reported.

A check reads the source table by table. An `ALTER` that lands between
the schema pass and the data pass leaves the schema verdict describing a
table that is no longer there, and a data verdict read across it compares
two shapes as one. migration-verifier fails the whole run for this reason.
Here the answers about the tables the DDL touched, and about the database
as a whole, stop being verdicts; the rest keep theirs. A resumed run
reuses nothing the source has changed under since.
"""
import json
import sqlite3

import pytest
from click.testing import CliRunner

from migkit import cli, verdict
from tests.conftest import needs_docker, psql


@pytest.fixture
def lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.executescript(
            "create table orders (id integer primary key, v text);"
            " insert into orders values (1, 'a'), (2, 'b');"
            " create table people (id integer primary key);"
            " insert into people values (1);")
        con.commit()
        con.close()
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _check(*extra):
    got = CliRunner().invoke(cli.main, ["check", "lite", *extra])
    return got, " ".join((got.output + str(got.exception or "")).split())


def _alter(where, sql):
    con = sqlite3.connect(where / "a.db")
    con.execute(sql)
    con.commit()
    con.close()


def _records(where):
    return json.loads((where / "reports" / "lite" / "summary.json"
                       ).read_text())


def test_a_quiet_check_is_still_all_green(lite):
    got, said = _check()
    assert got.exit_code == 0, said
    assert "all green" in said, said
    env = json.loads((lite / "reports" / "lite" / "verdict.json").read_text())
    assert env["status"] == "same" and "stale" not in env, env


def test_a_column_added_mid_check_takes_the_answers_back(lite, monkeypatch):
    from migkit.engines.sqlite import SQLiteEngine
    real = SQLiteEngine.check_counts

    def count_then_alter(self, db):
        got = real(self, db)
        _alter(lite, "alter table people add column name text")
        return got

    monkeypatch.setattr(SQLiteEngine, "check_counts", count_then_alter)
    got, said = _check()
    assert got.exit_code != 0, said
    assert "all green" not in said, said
    assert "overtaken by a schema change on the source" in said, said
    by = {(r["check"], r["scope"]): r for r in _records(lite)}
    # the schema and count answers were about the database as a whole
    for key in (("schema", "main"), ("counts", "main")):
        assert by[key]["status"] == "skip", by[key]
        assert "people: column name added" in by[key]["stale"], by[key]
        assert "It read: ok" in by[key]["detail"], by[key]
    # the table the DDL did not touch keeps its verdict
    assert by[("data", "main.orders")]["status"] == "ok", by
    env = json.loads((lite / "reports" / "lite" / "verdict.json").read_text())
    assert env["status"] == "incomplete", env
    assert env["has_differences"] is False, env
    assert "main" in env["stale"], env
    assert any(f.get("scope") == "main" and f["check"] == "schema"
               for f in env["findings"]), env["findings"]


def test_a_resumed_run_reuses_nothing_the_source_changed_under(lite):
    got, said = _check()
    assert got.exit_code == 0, said
    # the application adds a column the target does not have
    _alter(lite, "alter table orders add column note text default 'x'")
    got, said = _check("--resume")
    assert "schema changed since the run being resumed" in said, said
    assert "orders: column note added" in said, said
    by = {(r["check"], r["scope"]): r for r in _records(lite)}
    assert by[("schema", "main")]["status"] == "diff", by[("schema", "main")]
    assert got.exit_code != 0, said


def test_a_resumed_run_with_nothing_changed_still_reuses(lite):
    got, said = _check()
    assert got.exit_code == 0, said
    got, said = _check("--resume")
    assert got.exit_code == 0, said
    assert "resume: skipped, was ok" in json.dumps(_records(lite)), said
    assert "schema changed since" not in said, said


def test_which_answers_are_taken_back():
    records = [
        {"check": "data", "scope": "postgres", "status": "ok", "detail": "3"},
        {"check": "data", "scope": "postgres public.t", "status": "diff",
         "detail": "1 row"},
        {"check": "data", "scope": "appdb.orders", "status": "ok"},
        {"check": "data", "scope": "appdb.x_orders", "status": "ok"},
        {"check": "data", "scope": "postgres public.other", "status": "ok"},
        {"check": "data", "scope": "postgres public.t2", "status": "error"},
        {"check": "params", "scope": "postgres", "status": "ok"},
        {"check": "schema", "scope": "postgres", "status": "skip"},
        {"check": "schema", "scope": "postgres (structural)",
         "status": "ok"},
    ]
    verdict.mark_stale(records, "postgres", {"public.t", "orders",
                                             "public.t2"}, "t: changed")
    got = [(r["scope"], r["status"], bool(r.get("stale"))) for r in records]
    assert got == [
        ("postgres", "skip", True),            # the whole database
        ("postgres public.t", "skip", True),   # a diff read across it too
        ("appdb.orders", "skip", True),
        ("appdb.x_orders", "ok", False),       # a name that only ends alike
        ("postgres public.other", "ok", False),
        ("postgres public.t2", "error", False),  # an error stays an error
        ("postgres", "ok", False),             # server settings
        ("postgres", "skip", False),           # nothing to take back
        ("postgres (structural)", "skip", True),  # the database's shape
    ], got


@needs_docker
def test_postgres_whole_database_answer_is_taken_back(pg_pair, tmp_path,
                                                      monkeypatch):
    """PostgreSQL answers a clean database in one record for the whole
    database - the one a DDL on any of its tables overtakes."""
    import migkit.config as cfg
    from migkit.engines.postgres import PostgresEngine
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.a (id int primary key, v text);"
                   " insert into public.a values (1, 'x')")
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  pgs:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg_pair['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg_pair['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    real = PostgresEngine.check_data

    def data_then_alter(self, db, *a, **kw):
        got = real(self, db, *a, **kw)
        psql(pg_pair["src"], "alter table public.a add column w int")
        return got

    monkeypatch.setattr(PostgresEngine, "check_data", data_then_alter)
    got = CliRunner().invoke(cli.main, ["check", "pgs", "--only",
                                        "schema,counts,data"])
    said = " ".join((got.output + str(got.exception or "")).split())
    assert got.exit_code != 0, said
    by = {(r["check"], r["scope"]): r for r in json.loads(
        (tmp_path / "reports" / "pgs" / "summary.json").read_text())}
    assert by[("data", "postgres")]["status"] == "skip", by
    assert "public.a: column w added" in by[("data", "postgres")]["stale"]
    # every schema answer was about a shape the source no longer has
    schema = [r for (c, _), r in by.items() if c == "schema"]
    assert schema and all(r["status"] == "skip" for r in schema), schema
    assert "all green" not in said, said
