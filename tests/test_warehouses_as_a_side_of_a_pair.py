"""Redshift, Snowflake and BigQuery as one side of a pair (backlog 33).

No warehouse runs here - each needs an account this machine has none of -
so what is pinned is what migkit would say to one: the tables it would
create, how it reads a page of rows, how Snowflake's catalogue is read
back, and the Parquet BigQuery's stage is loaded from.
"""
import datetime
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("schema.create", ("redshift", "snowflake"))]

COLUMNS = [("id", "integer", (), {"null": False}),
           ("amount", "decimal", (12, 2)), ("name", "text", (40,)),
           ("raw", "bytes", ()), ("at", "timestamp", (6,)),
           ("doc", "json", ())]


def _engine(name):
    from migkit.engines import engine_named
    return engine_named(name, Hop(
        name="w", engine=name, source=Endpoint(),
        target=Endpoint(options={"database": "analytics"}),
        databases=["sales"]))


@pytest.mark.parametrize("name, ddl", [
    ("redshift", 'create table "sales"."orders" ("id" bigint not null,'
                 ' "amount" numeric(12,2), "name" varchar(40),'
                 ' "raw" varbyte(1024000), "at" timestamp, "doc" super,'
                 ' primary key ("id"))'),
    ("snowflake", 'create table "SALES"."ORDERS" ("ID" NUMBER(38,0) not'
                  ' null, "AMOUNT" NUMBER(12,2), "NAME" VARCHAR(40),'
                  ' "RAW" BINARY, "AT" TIMESTAMP_NTZ(6), "DOC" VARIANT,'
                  ' primary key ("ID"))'),
    ("bigquery", "create table `sales`.`orders` (`id` INT64 not null,"
                 " `amount` BIGNUMERIC(12,2), `name` STRING(40),"
                 " `raw` BYTES, `at` DATETIME, `doc` JSON,"
                 " primary key (`id`) not enforced)"),
])
def test_the_table_each_would_create(name, ddl):
    assert _engine(name).neutral_create_sql(
        "dst", "sales", "orders", COLUMNS, key=("id",)) == ddl


@pytest.mark.parametrize("name", ["redshift", "snowflake", "bigquery"])
def test_a_page_is_read_with_limit(name):
    eng = _engine(name)
    eng.neutral_key = lambda side, db, table: ["id"]
    # Snowflake reads a timestamp's nine digits as its text, so it asks
    # which columns are timestamps first
    eng.neutral_columns = lambda side, db, table: [("id", "INTEGER"),
                                                   ("name", "TEXT")]
    sql, args, key, names = eng._read_query(
        "dst", "sales", "orders", [("id", "integer"), ("name", "text")],
        (41,), 500, None)
    assert sql.endswith(" limit 500") and "fetch first" not in sql, sql
    assert args == [41] and key == ["id"]


def test_snowflake_reads_its_catalogue_in_small_letters():
    eng = _engine("snowflake")
    asked = []

    def rows(side, db, sql, args=None):
        asked.append(args)
        return [["ID", "NUMBER(38,0)"], ["AMOUNT", "NUMBER(12,2)"],
                ["Mixed", "TEXT(16777216)"]]
    eng._rows = rows
    assert eng.neutral_columns("dst", "sales", "orders") == [
        ("id", "INTEGER"), ("amount", "NUMBER(12,2)"),
        ("Mixed", "TEXT(16777216)")]
    # the schema and table as Snowflake keeps them
    assert asked == [("SALES", "ORDERS")]
    from migkit import canon
    assert [canon.type_class("snowflake", t) for t in
            ("INTEGER", "NUMBER(12,2)", "TEXT(16777216)",
             "TIMESTAMP_NTZ(9)")] == ["integer", "decimal", "text",
                                      "timestamp"]


def test_bigquerys_stage_holds_each_value_as_its_column_takes_it():
    """The Parquet a BigQuery stage is loaded from: an instant given to a
    DATETIME as UTC reads it (a DATETIME refuses an offset), to a TIMESTAMP
    as the instant it is; bytes as bytes; a decimal at NUMERIC's nine
    places; a field that is not there as NULL."""
    from google.cloud.bigquery import SchemaField

    from migkit import canon
    eng = _engine("bigquery")
    at = datetime.datetime(2024, 2, 29, 7, 5, tzinfo=datetime.timezone(
        datetime.timedelta(hours=7)))
    fields = [SchemaField("id", "INT64"), SchemaField("amount", "NUMERIC"),
              SchemaField("raw", "BYTES"), SchemaField("at", "DATETIME"),
              SchemaField("seen", "TIMESTAMP"), SchemaField("note", "STRING")]
    names = [f.name for f in fields]
    rows = [[1, Decimal("1.50"), b"\x00\xff", at, at, None],
            [2, None, None, None, None, "b"]]
    body = eng._arrow(fields, names, rows)
    got = body.to_pylist()
    assert got[0] == {
        "id": 1, "amount": Decimal("1.500000000"), "raw": b"\x00\xff",
        "at": datetime.datetime(2024, 2, 29, 0, 5),
        "seen": datetime.datetime(2024, 2, 29, 0, 5,
                                  tzinfo=datetime.timezone.utc),
        "note": None}
    assert got[1]["note"] == "b" and got[1]["amount"] is None
    assert str(body.schema.field("at").type) == "timestamp[us]"
    assert str(body.schema.field("seen").type) == "timestamp[us, tz=UTC]"
    # a row a document store left a field out of is written with it NULL
    written = []
    eng.neutral_key = lambda side, db, table: ["id"]
    eng._load_batch = lambda side, db, table, columns, rows, key: \
        written.append(rows)
    eng.neutral_write("dst", "sales", "orders", [("id", "integer"),
                                                 ("note", "text")],
                      [[3, canon.ABSENT]])
    assert written == [[[3, None]]]
