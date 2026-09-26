"""Redshift, Snowflake and BigQuery as one side of a pair (backlog 33).

No warehouse runs here - each needs an account this machine has none of -
so what is pinned is what migkit would say to one: the tables it would
create, how it reads a page of rows, how Snowflake's catalogue is read
back, and the rows BigQuery's load job would be given.
"""
import base64
import datetime
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop

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


def test_bigquery_is_given_a_load_job_after_the_keys_go():
    from migkit import canon
    eng = _engine("bigquery")
    eng.neutral_key = lambda side, db, table: ["id"]
    ran, loaded = [], []

    class Cursor:
        def execute(self, sql, args=None):
            ran.append((sql, args))

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    class Job:
        def result(self):
            return None

    class Client:
        def load_table_from_json(self, records, ref, job_config):
            loaded.append((ref, records, job_config.write_disposition))
            return Job()
    eng._connect = lambda side, db: Conn()
    eng._client = lambda side: Client()
    at = datetime.datetime(2024, 2, 29, 7, 5, tzinfo=datetime.timezone(
        datetime.timedelta(hours=7)))
    n = eng.neutral_write("dst", "sales", "orders",
                          [("id", "integer"), ("amount", "decimal"),
                           ("raw", "bytes"), ("at", "timestamp"),
                           ("note", "text")],
                          [[1, Decimal("1.50"), b"\x00\xff", at,
                            canon.ABSENT],
                           [2, None, None, None, "b"]])
    assert n == 2
    assert ran == [("delete from `sales`.`orders` where (`id` = %s) or"
                    " (`id` = %s)", (1, 2))]
    assert loaded == [("analytics.sales.orders", [
        {"id": 1, "amount": "1.50",
         "raw": base64.b64encode(b"\x00\xff").decode(),
         "at": "2024-02-29T00:05:00"},
        {"id": 2, "amount": None, "raw": None, "at": None, "note": "b"}],
        "WRITE_APPEND")]
