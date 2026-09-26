"""SQL Server as one side of a pair of engines (backlog 39, T-SQL).

No SQL Server runs on this machine's architecture, so what is held here is
everything that does not need one: that its values render to the same
text and the same digest PostgreSQL computes in-server for the same data,
that a T-SQL function's body reaches another engine without its `@`
parameters and comes back with them, and that the resume condition it
reads by - T-SQL has no row comparison - is the one the other engines
use.
"""
import datetime
import uuid
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop


def _hop(src="mssql", dst="postgres", port=1433, dst_port=5432):
    return Hop(name="ms", engine="hetero",
               options={"source_engine": src, "target_engine": dst},
               source=Endpoint(host="127.0.0.1", port=port, user="sa",
                               password="CHANGE_ME"),
               target=Endpoint(host="127.0.0.1", port=dst_port,
                               user="postgres", password="test"),
               databases=["cx"])


def _catalog(monkeypatch, answers):
    """SQL Server's catalog as canned rows, by a phrase of each query."""
    from migkit.engines.mssql import MSSQLEngine

    def rows(self, side, db, sql, args=None):
        for phrase, got in answers.items():
            if phrase in sql:
                return got(args) if callable(got) else got
        raise AssertionError(f"unexpected query: {sql}")
    monkeypatch.setattr(MSSQLEngine, "_rows", rows)


def test_a_function_and_a_view_reach_postgresql(monkeypatch, tmp_path):
    from migkit.engines.hetero import HeteroEngine
    _catalog(monkeypatch, {
        "from sys.tables t": [["dbo.orders"]],
        "from sys.views v": [
            ["dbo.big", "CREATE VIEW dbo.big AS SELECT id, amount"
                        " FROM dbo.orders WHERE amount > 10"]],
        "from sys.objects o": [
            [11, "add_tax", "FN", "CREATE FUNCTION dbo.add_tax"
             "(@x decimal(10,2)) RETURNS decimal(10,2) AS BEGIN"
             " RETURN @x * 1.07 END"],
            [12, "steps", "FN", "CREATE FUNCTION dbo.steps(@x int)"
             " RETURNS int AS BEGIN DECLARE @y int; SET @y = @x + 1;"
             " RETURN @y END"],
            [13, "ping", "P ", "CREATE PROCEDURE dbo.ping AS SELECT 1"]],
        "from sys.parameters p": lambda args: {
            11: [["", "decimal(10,2)", 0], ["@x", "decimal(10,2)", 1]],
            12: [["", "int", 0], ["@x", "int", 1]],
            13: []}[args[0]],
    })
    eng = HeteroEngine(_hop())
    got = {n: sql for n, sql, _ in eng.converted_code("cx")}
    # the schema of a table the move carries is not one the target has
    assert got["big"] == ('create view big as SELECT id, amount FROM orders'
                          ' WHERE amount > 10;'), got["big"]
    assert got["add_tax"] == (
        'create function "add_tax"("x" numeric(10,2)) returns numeric(10,2)'
        ' language sql as $$ select CAST(CAST("x" AS DECIMAL(10, 2)) * 1.07'
        ' AS DECIMAL(10, 2)) $$;'), got["add_tax"]
    assert got["steps"].startswith("-- function steps not converted: its"
                                   " body is statements"), got["steps"]
    assert got["ping"].startswith("-- procedure ping not converted"), \
        got["ping"]


def test_a_function_reaches_sql_server_with_its_parameters(monkeypatch):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop())
    got = eng.neutral_function_sql(
        "add_tax", [("x", "decimal(10,2)")], "decimal(10,2)",
        "CAST(x AS DECIMAL(10, 2)) * 1.07")
    assert got == ("create function [add_tax](@x decimal(10,2)) returns"
                   " decimal(10,2) as begin return CAST(@x AS NUMERIC(10, 2))"
                   " * 1.07 end"), got


def test_the_resume_condition_is_spelled_out(monkeypatch):
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop())
    cond, args = eng._after(["a", "b", "c"], (1, "x", 2))
    assert eng._sql(cond, True) == (
        "(([a] > %s) or ([a] = %s and [b] > %s)"
        " or ([a] = %s and [b] = %s and [c] > %s))"), cond
    assert args == [1, 1, "x", 1, "x", 2], args


def test_its_values_digest_as_postgresql_digests_them(pg_pair, monkeypatch):
    """The driver's Python values against the same rows in PostgreSQL:
    the digest SQL Server's side computes here has to be the number
    PostgreSQL computes in-server, or every table would read as
    different."""
    from tests.conftest import psql

    from migkit import canon
    from migkit.engines.mssql import MSSQLEngine
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["dst"]
    psql(port, "drop database if exists msd")
    assert psql(port, "create database msd").returncode == 0
    made = psql(port, """
        create table t (id bigint primary key, amount numeric(10,2),
          flag boolean, u uuid, b bytea, s varchar(20), d date,
          ts timestamp(6), tm time(6), f double precision);
        insert into t values
          (1, 12.50, true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
           '\\xdeadbeef', 'Céline', '2024-02-29',
           '2024-02-29 23:59:59.123456', '10:11:12.5', 0.1),
          (2, null, false, null, null, '', null, null, null, 1e20),
          (3, -0.01, null, null, '\\x', 'a ', '1999-12-31',
           '2000-01-01 00:00:00', '00:00:00', -2.5)""", db="msd")
    assert made.returncode == 0, made.stderr
    hop = Hop(name="pgside", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"), databases=["msd"])
    declared = {"id": "bigint", "amount": "decimal(10,2)", "flag": "bit",
                "u": "uniqueidentifier", "b": "varbinary(max)",
                "s": "nvarchar(20)", "d": "date", "ts": "datetime2(6)",
                "tm": "time(6)", "f": "float"}
    columns = [(n, canon.comparable("mssql", t)[0])
               for n, t in sorted(declared.items())]
    assert all(c for _, c in columns), columns
    # what pymssql hands back for those rows, column order as above
    rows = {
        1: {"amount": Decimal("12.50"), "b": b"\xde\xad\xbe\xef",
            "d": datetime.date(2024, 2, 29), "f": 0.1, "flag": True,
            "id": 1, "s": "Céline",
            "tm": datetime.time(10, 11, 12, 500000),
            "ts": datetime.datetime(2024, 2, 29, 23, 59, 59, 123456),
            "u": uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")},
        2: {"amount": None, "b": None, "d": None, "f": 1e20,
            "flag": False, "id": 2, "s": "", "tm": None, "ts": None,
            "u": None},
        3: {"amount": Decimal("-0.01"), "b": b"",
            "d": datetime.date(1999, 12, 31), "f": -2.5, "flag": None,
            "id": 3, "s": "a ", "tm": datetime.time(0, 0),
            "ts": datetime.datetime(2000, 1, 1), "u": None}}

    def batches(self, side, db, table, cols, size=1000, where=None):
        yield [[r[n] for n, _ in cols] for r in rows.values()]
    monkeypatch.setattr(MSSQLEngine, "neutral_batches", batches)
    ours = MSSQLEngine(_hop()).neutral_digest("src", "cx", "dbo.t", columns)
    theirs = PostgresEngine(hop).neutral_digest("src", "msd", "public.t",
                                                columns)
    assert ours == theirs, (ours, theirs)
    # and a single changed value is a different number
    rows[3]["s"] = "a"
    assert MSSQLEngine(_hop()).neutral_digest(
        "src", "cx", "dbo.t", columns) != theirs


@pytest.mark.parametrize("declared,cls", [
    ("datetimeoffset(7)", None), ("timestamp", None),
    ("nvarchar(max)", "text"), ("bit", "boolean"), ("money", "decimal")])
def test_what_has_no_honest_rendering_stays_unmapped(declared, cls):
    from migkit import canon
    assert canon.type_class("mssql", declared) == cls
