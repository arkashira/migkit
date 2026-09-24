"""`assess` says the value to set, not only the verdict.

Google's DMS test job suggests the parameter values instead of failing on
them; AWS lists one named check per risk. migkit's checks said ok or not.
They now carry the setting that would make them pass, computed from what
the server reports - `max_replication_slots` from the slots already used
plus one per database in scope, the binlog settings the change tail needs
spelled as the statement to run. And every engine says how many tables are
in scope and which names differ only by case.
"""
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker


def _lite(tmp_path, tables):
    from migkit.engines.sqlite import SQLiteEngine
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        for t in tables:
            con.execute(f'create table "{t}" (id integer primary key)')
        con.commit()
        con.close()
    hop = Hop(name="lite", engine="sqlite",
              source=Endpoint(host=str(tmp_path / "a.db"), port=0, user="",
                              password=""),
              target=Endpoint(host=str(tmp_path / "b.db"), port=0, user="",
                              password=""), databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


def _said(items):
    return {i["item"]: i for i in items}


def test_names_that_differ_only_by_case_fail(tmp_path, monkeypatch):
    # SQLite folds case itself, so the source here is one that does not
    eng = _lite(tmp_path, ["people"])
    monkeypatch.setattr(eng, "neutral_tables",
                        lambda side, db: ["Orders", "orders", "people"])
    got = _said(eng.assess())
    clash = got["table names that differ only by case"]
    assert clash["level"] == "fail" and "Orders / orders" in \
        clash["detail"], clash
    assert got["tables in scope"]["detail"].startswith("3"), got


def test_many_tables_say_how_to_split(tmp_path, monkeypatch):
    from migkit.engines.base import Engine
    monkeypatch.setattr(Engine, "MANY_TABLES", 2)
    got = _said(_lite(tmp_path, ["a", "b", "c"]).assess())
    assert got["tables in scope"]["level"] == "warn"
    assert "split the hop" in got["tables in scope"]["detail"]


@needs_docker
def test_postgres_says_how_many_slots_to_set(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="slots", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    eng = PostgresEngine(hop)
    got = {i["item"]: i for i in eng._stream_capacity()}
    slots = got["max_replication_slots leaves room for this hop's streams"]
    assert slots["level"] == "pass", slots
    assert "1 needed" in slots["detail"], slots
    # a server with no room is told the number, not only "no"
    monkeypatch_slots = eng._psql
    eng._psql = lambda side, db, sql: "3|3|0|10"
    try:
        got = {i["item"]: i for i in eng._stream_capacity()}
    finally:
        eng._psql = monkeypatch_slots
    slots = got["max_replication_slots leaves room for this hop's streams"]
    assert slots["level"] == "fail", slots
    assert "set max_replication_slots = 4" in slots["detail"], slots
