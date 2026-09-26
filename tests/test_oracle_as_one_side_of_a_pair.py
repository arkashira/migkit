"""Oracle as one side of a pair (backlog 11), held to everything that does
not need a server.

Oracle Free needs more than this machine's container VM has: measured, it
filled the VM's disk and took 2.5 GB of its 3.8 GB of memory before it
was stopped. What is held here is what migkit decides without one:
* the values the driver hands back digest as PostgreSQL digests the same
  rows in the server: a `CHAR` padded, a `DATE` holding a time, a
  `NUMBER(10,2)` that kept no trailing zero
* names meet across the case Oracle folds them to
* parameters are marked the way the driver reads them
* a PL/SQL function of one `RETURN` is carried, and anything longer is
  named
"""
import datetime
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop


def _hop():
    return Hop(name="ora", engine="hetero",
               options={"source_engine": "oracle",
                        "target_engine": "postgres"},
               source=Endpoint(host="10.0.0.5", port=1521, user="app",
                               password="CHANGE_ME",
                               options={"service": "FREEPDB1"}),
               target=Endpoint(host="127.0.0.1", port=5432,
                               user="postgres", password="test"),
               databases=["app"])


def _oracle():
    from migkit.engines.oracle import OracleEngine
    return OracleEngine(_hop())


def test_names_meet_across_the_case_oracle_folds_to():
    eng = _oracle()
    assert eng._q("orders") == '"ORDERS"'
    assert eng._q("MixedCase") == '"MixedCase"'
    assert eng._said("ORDERS") == "orders"
    assert eng._said("MixedCase") == "MixedCase"
    assert eng._qualified("src", "app", "orders") == '"APP"."ORDERS"'


def test_parameters_are_marked_as_each_driver_reads_them():
    from migkit.engines.dbapi import MARK
    from migkit.engines.mssql import MSSQLEngine
    sql = f"select a from t where a = {MARK} and b like 'x%' and c = {MARK}"
    assert _oracle()._sql(sql, True) == \
        "select a from t where a = :1 and b like 'x%' and c = :2"
    # a driver that reads % as its own gets every other one doubled
    assert MSSQLEngine(_hop())._sql(sql, True) == \
        "select a from t where a = %s and b like 'x%%' and c = %s"
    assert MSSQLEngine(_hop())._sql("select 'x%'", False) == "select 'x%'"


@pytest.mark.parametrize("declared,cls", [
    ("NUMBER(10,2)", "decimal"), ("NUMBER", "decimal"),
    ("TIMESTAMP(6)", "timestamp"), ("DATE", "timestamp"),
    ("timestamp with time zone", None), ("VARCHAR2(20)", "text"),
    ("CHAR(5)", "text"), ("BLOB", "bytes"), ("BINARY_DOUBLE", "float"),
    ("INTERVAL DAY(2) TO SECOND(6)", None)])
def test_types(declared, cls):
    from migkit import canon
    assert canon.type_class("oracle", declared) == cls


def test_its_values_digest_as_postgresql_digests_them(pg_pair, monkeypatch):
    from tests.conftest import psql

    from migkit import canon
    from migkit.engines.dbapi import DbapiRows
    from migkit.engines.oracle import OracleEngine
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["dst"]
    psql(port, "drop database if exists orad")
    assert psql(port, "create database orad").returncode == 0
    made = psql(port, """
        create table t (id bigint primary key, code char(5),
          amount numeric(10,2), at timestamp(0), name varchar(20),
          raw bytea, ratio double precision);
        insert into t values
          (1, 'ab', 12.50, '2024-02-29 13:14:15', 'Céline', '\\x00ff', 0.1),
          (2, null, null, null, null, null, null),
          (3, 'abcde', -0.01, '1999-12-31 00:00:00', ' x ', '\\x', 2.5)""",
                db="orad")
    assert made.returncode == 0, made.stderr
    declared = {"id": "NUMBER(19,0)", "code": "CHAR(5)",
                "amount": "NUMBER(10,2)", "at": "DATE",
                "name": "VARCHAR2(20)", "raw": "BLOB",
                "ratio": "BINARY_DOUBLE"}
    columns = [(n, canon.comparable("oracle", t)[0])
               for n, t in sorted(declared.items())]
    assert all(c for _, c in columns), columns
    # what python-oracledb hands back: CHAR padded to its length, a DATE
    # as a datetime, NUMBER as int or Decimal
    rows = [
        {"amount": Decimal("12.5"), "at": datetime.datetime(
            2024, 2, 29, 13, 14, 15), "code": "ab   ", "id": 1,
         "name": "Céline", "ratio": 0.1, "raw": b"\x00\xff"},
        {"amount": None, "at": None, "code": None, "id": 2, "name": None,
         "ratio": None, "raw": None},
        {"amount": Decimal("-0.01"), "at": datetime.datetime(1999, 12, 31),
         "code": "abcde", "id": 3, "name": " x ", "ratio": 2.5,
         "raw": b""}]

    def batches(self, side, db, table, cols, size=1000, where=None):
        yield [[r[n] for n, _ in cols] for r in rows]
    monkeypatch.setattr(DbapiRows, "neutral_batches", batches)
    monkeypatch.setattr(OracleEngine, "neutral_columns",
                        lambda self, side, db, table: list(declared.items()))
    hop = Hop(name="pg", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=port, user="postgres",
                              password="test"), databases=["orad"])
    theirs = PostgresEngine(hop).neutral_digest("src", "orad", "public.t",
                                                columns)
    # NUMBER keeps no trailing zeros, and 12.5 is read back at the scale
    # the column declares: the text numeric(10,2) renders on the other side
    assert _oracle().neutral_digest("src", "app", "t", columns) == theirs
    rows[2]["name"] = "x"
    assert _oracle().neutral_digest("src", "app", "t", columns) != theirs


def test_a_plsql_function_of_one_return_is_carried(monkeypatch):
    from migkit.engines.oracle import OracleEngine
    source = [("ADD_TAX", "FUNCTION", "FUNCTION add_tax(x NUMBER)\n"),
              ("ADD_TAX", "FUNCTION", "RETURN NUMBER IS\n"),
              ("ADD_TAX", "FUNCTION", "BEGIN\n  RETURN ROUND(x * 1.07, 2);\n"),
              ("ADD_TAX", "FUNCTION", "END add_tax;\n"),
              ("STEPS", "FUNCTION", "FUNCTION steps(x NUMBER) RETURN NUMBER"
                                    " IS y NUMBER; BEGIN y := x + 1;"
                                    " RETURN y; END;"),
              ("PING", "PROCEDURE", "PROCEDURE ping IS BEGIN NULL; END;")]
    args = {"ADD_TAX": [(None, "NUMBER", 0), ("X", "NUMBER", 1)],
            "STEPS": [(None, "NUMBER", 0), ("X", "NUMBER", 1)],
            "PING": []}

    def rows(self, side, db, sql, a=None):
        if "from all_source" in sql:
            return source
        if "from all_arguments" in sql:
            return args[a[1]]
        raise AssertionError(sql)
    monkeypatch.setattr(OracleEngine, "_rows", rows)
    got = {n: (p, r, one) for n, p, r, one in
           _oracle().neutral_functions("src", "app")}
    assert got["add_tax"] == ([("x", "NUMBER")], "NUMBER",
                              "ROUND(x * 1.07, 2)"), got["add_tax"]
    assert got["steps"][2] is None and got["ping"][2] is None, got
    made = _oracle().neutral_function_sql(
        "add_tax", [("x", "NUMBER(10,2)")], "NUMBER(10,2)",
        'ROUND("x" * 1.07, 2)')
    assert made == ('create function "ADD_TAX"("x" NUMBER) return NUMBER is'
                    ' begin return ROUND("x" * 1.07, 2); end;'), made
