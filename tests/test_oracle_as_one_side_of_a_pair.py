"""Oracle as one side of a pair (backlog 11), held to everything that does
not need a server; what does is held against Oracle Free in
`test_oracle_is_a_whole_side.py` and `test_oracle_pairs_with_postgresql.py`.
What is held here is what migkit decides without one:
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
    ("timestamp with time zone", "timestamp"),
    ("timestamp with local time zone", "timestamp"), ("VARCHAR2(20)", "text"),
    ("CHAR(5)", "text"), ("BLOB", "bytes"), ("BINARY_DOUBLE", "float"),
    ("INTERVAL DAY(2) TO SECOND(6)", None)])
def test_types(declared, cls):
    from migkit import canon
    assert canon.type_class("oracle", declared) == cls


def test_its_values_digest_as_postgresql_digests_them(pg_pair, monkeypatch):
    from tests.conftest import psql

    from migkit import canon
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

    class Cursor:
        arraysize = prefetchrows = 0

        def __init__(self):
            self.left = [[[r[n] for n, _ in columns] for r in rows]]

        def fetchmany(self, size):
            return self.left.pop() if self.left else []

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass
    # folded here, over the values the driver hands back: the server's
    # own sum is held to the same rendering against a server
    monkeypatch.setattr(OracleEngine, "_digest_sql",
                        lambda self, *a, **k: None)
    monkeypatch.setattr(OracleEngine, "_connect", lambda self, s, d: Conn())
    monkeypatch.setattr(OracleEngine, "_run", lambda self, cur, sql, a=None:
                        None)
    monkeypatch.setattr(OracleEngine, "_read_plan",
                        lambda self, side, db, table, cols: (
                            "*", lambda raw: [list(r) for r in raw]))
    monkeypatch.setattr(OracleEngine, "neutral_key",
                        lambda self, side, db, table: ["id"])
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


def test_rows_read_back_by_key_are_at_their_declared_scale(monkeypatch):
    """A batch written to Oracle is read back by its keys and held to what
    was read from the source. NUMBER keeps no trailing zeros: read by key
    without its declared scale, 0.10 came back 0.1 and every move into
    Oracle of such a value stopped as a difference."""
    from migkit import canon
    from migkit.engines.dbapi import DbapiRows
    from migkit.engines.oracle import OracleEngine
    declared = {"id": "NUMBER(10,0)", "price": "NUMBER(8,2)",
                "code": "CHAR(3)"}
    columns = [(n, canon.comparable("oracle", t)[0])
               for n, t in declared.items()]
    monkeypatch.setattr(OracleEngine, "neutral_columns",
                        lambda self, side, db, table: list(declared.items()))
    monkeypatch.setattr(DbapiRows, "_rows", lambda self, side, db, sql,
                        args=None: [(Decimal("2"), Decimal("0.1"), "ab ")])
    monkeypatch.setattr(DbapiRows, "_wide_times",
                        lambda self, side, db, table: {})
    monkeypatch.setattr(DbapiRows, "_whole_times",
                        lambda self, side, db, table, names, rows: rows)
    got = _oracle().neutral_rows_by_key("dst", "app", "items", columns,
                                        ["id"], [(2,)])
    (row,) = got.values()
    assert [canon.render_value(c, v) for (_, c), v in zip(columns, row)] \
        == ["2", "0.10", "ab"], row


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


# ---- the digest, in the server and here ------------------------------------

def _facts_of(monkeypatch, facts, charset="AL32UTF8", limit=4000):
    from migkit.engines.oracle import OracleEngine
    monkeypatch.setattr(OracleEngine, "_facts",
                        lambda self, side, db, table: facts)
    monkeypatch.setattr(OracleEngine, "_server_facts",
                        lambda self, side, db: (charset, limit))


def test_a_table_is_summed_in_the_server_where_every_column_renders(
        monkeypatch):
    from migkit import canon
    _facts_of(monkeypatch, [
        {"name": "id", "kind": "NUMBER", "length": 22, "precision": 10,
         "scale": 0, "chars": 0, "virtual": False},
        {"name": "amount", "kind": "NUMBER", "length": 22, "precision": 12,
         "scale": 2, "chars": 0, "virtual": False},
        {"name": "at", "kind": "TIMESTAMP WITH TIME ZONE", "length": 13,
         "precision": None, "scale": 9, "chars": 0, "virtual": False}])
    eng = _oracle()
    cols = [("amount", "decimal"), ("at", "timestamp"), ("id", "decimal")]
    sql, _ = eng._digest_sql("src", "app", "t", cols)
    # the digest every engine sums: md5, its first fifteen hex digits
    assert "standard_hash(" in sql and "'MD5')" in sql, sql
    assert f"1, {canon.DIGEST_HEX})" in sql
    assert "to_char(\"AMOUNT\", 'FM99999999990.00')" in sql, sql
    assert "sys_extract_utc(\"AT\")" in sql and "'FF9'" in sql, sql
    assert "checksum" not in sql.lower()
    salted, _ = eng._digest_sql("src", "app", "t", cols,
                                salt="0f1e2d3c4b5a6978")
    assert "'SHA256')" in salted and "'0f1e2d3c4b5a6978' ||" in salted
    assert "1, 16)" in salted, salted
    # a column with no exact rendering here, a character set that is not
    # UTF-8, a row wider than a hash's input: folded here instead
    assert eng._digest_sql("src", "app", "t",
                           cols + [("f", "float")]) is None
    _facts_of(monkeypatch, [
        {"name": "id", "kind": "NUMBER", "length": 22, "precision": 10,
         "scale": 0, "chars": 0, "virtual": False}], charset="WE8ISO8859P1")
    assert eng._digest_sql("src", "app", "t", [("id", "decimal")]) is None
    _facts_of(monkeypatch, [
        {"name": "s", "kind": "VARCHAR2", "length": 4000, "precision": None,
         "scale": None, "chars": 1000, "virtual": False}])
    assert eng._digest_sql("src", "app", "t", [("s", "text")]) is None


def test_oracle_to_oracle_is_summed_salted_by_sha256(monkeypatch):
    """Both sides Oracle: the pair asks each for its salted SHA-256 sum
    behind one salt; against PostgreSQL, the digest every engine sums.
    Two copies of a row add; a salt changes every total."""
    import hashlib

    from migkit import canon, rowtext
    from migkit.engines.hetero import HeteroEngine
    from migkit.engines.oracle import OracleEngine
    from migkit.engines.postgres import PostgresEngine
    rows = [[1, "x"], [1, "x"], [2, "y"]]
    classes = ["decimal", "text"]
    n, total = OracleEngine._sha_fold(classes, rows, "aa")
    want = sum(int(hashlib.sha256(("aa" + rowtext.encode(
        [canon.render_value(c, v) for c, v in zip(classes, r)])).encode()
                                  ).hexdigest()[:16], 16) for r in rows)
    assert (n, total) == (3, want)
    assert OracleEngine._sha_fold(classes, rows, "bb")[1] != total
    one = OracleEngine._sha_fold(classes, rows[:1], "aa")[1]
    assert OracleEngine._sha_fold(classes, rows[:2], "aa")[1] == 2 * one
    asked = []
    monkeypatch.setattr(OracleEngine, "same_engine_digest",
                        lambda self, side, db, t, c, where=None, salt=None:
                        asked.append(("sha", side, salt)) or (1, "7"))
    monkeypatch.setattr(OracleEngine, "neutral_digest",
                        lambda self, side, db, t, c, where=None:
                        asked.append(("md5", side)) or (1, "7"))
    monkeypatch.setattr(PostgresEngine, "neutral_digest",
                        lambda self, side, db, t, c, where=None:
                        asked.append(("md5", side)) or (1, "7"))
    both = _hop()
    both.options = {"source_engine": "oracle", "target_engine": "oracle"}
    pair = HeteroEngine(both)
    pair._digests("app", "t", [], "t", [], (None, None))
    assert [a[:2] for a in asked] == [("sha", "src"), ("sha", "dst")]
    assert asked[0][2] and asked[0][2] == asked[1][2], asked
    asked.clear()
    HeteroEngine(_hop())._digests("app", "t", [], "t", [], (None, None))
    assert asked == [("md5", "src"), ("md5", "dst")], asked
