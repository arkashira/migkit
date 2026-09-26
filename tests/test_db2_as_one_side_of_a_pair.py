"""Db2 as one side of a pair (backlog 34), held to what does not need a
server: IBM's Db2 image runs on x86 only, and this machine is arm64.

The values the driver hands back digest as PostgreSQL digests the same
rows in the server, a `CHAR` padded to its length and bytes from a
`FOR BIT DATA` column included. Parameters are the driver's `?`, and
names meet across the capitals Db2 folds them to.
"""
import datetime
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop


def _db2():
    from migkit.engines.db2 import Db2Engine
    return Db2Engine(Hop(
        name="db2", engine="hetero",
        options={"source_engine": "db2", "target_engine": "postgres"},
        source=Endpoint(host="10.0.0.6", port=50000, user="db2inst1",
                        password="CHANGE_ME", options={"database": "SAMPLE"}),
        target=Endpoint(host="127.0.0.1", port=5432, user="postgres",
                        password="test"), databases=["app"]))


def test_marks_and_names():
    from migkit.engines.dbapi import MARK
    eng = _db2()
    assert eng._sql(f"select 1 from t where a = {MARK} and b = {MARK}",
                    True) == "select 1 from t where a = ? and b = ?"
    assert eng._qualified("src", "app", "orders") == '"APP"."ORDERS"'
    assert eng._said("ORDERS") == "orders"


@pytest.mark.parametrize("declared,cls", [
    ("DECIMAL(10,2)", "decimal"), ("CHARACTER(5)", "text"),
    ("VARBINARY(8)", "bytes"), ("TIMESTAMP(6)", "timestamp"),
    ("BIGINT", "integer"), ("XML", None)])
def test_types(declared, cls):
    from migkit import canon
    assert canon.type_class("db2", declared) == cls


def test_its_values_digest_as_postgresql_digests_them(pg_pair, monkeypatch):
    from tests.conftest import psql

    from migkit import canon
    from migkit.engines.db2 import Db2Engine
    from migkit.engines.dbapi import DbapiRows
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["dst"]
    psql(port, "drop database if exists db2d")
    assert psql(port, "create database db2d").returncode == 0
    assert psql(port, """
        create table t (id bigint primary key, code char(5),
          amount numeric(10,2), at timestamp(6), raw bytea);
        insert into t values (1, 'ab', 12.50, '2024-02-29 13:14:15.5',
          '\\x00ff'), (2, null, null, null, null)""",
                db="db2d").returncode == 0
    declared = {"id": "BIGINT", "code": "CHARACTER(5)",
                "amount": "DECIMAL(10,2)", "at": "TIMESTAMP(6)",
                "raw": "VARBINARY(8)"}
    columns = [(n, canon.comparable("db2", t)[0])
               for n, t in sorted(declared.items())]
    rows = [{"amount": Decimal("12.50"), "at": datetime.datetime(
                2024, 2, 29, 13, 14, 15, 500000), "code": "ab   ", "id": 1,
             "raw": b"\x00\xff"},
            {"amount": None, "at": None, "code": None, "id": 2,
             "raw": None}]

    def batches(self, side, db, table, cols, size=1000, where=None):
        yield [[r[n] for n, _ in cols] for r in rows]
    monkeypatch.setattr(DbapiRows, "neutral_batches", batches)
    monkeypatch.setattr(Db2Engine, "neutral_columns",
                        lambda self, side, db, table: list(declared.items()))
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"), databases=["db2d"])
    theirs = PostgresEngine(hop).neutral_digest("src", "db2d", "public.t",
                                                columns)
    assert _db2().neutral_digest("src", "app", "t", columns) == theirs
    rows[0]["code"] = "ac   "
    assert _db2().neutral_digest("src", "app", "t", columns) != theirs
