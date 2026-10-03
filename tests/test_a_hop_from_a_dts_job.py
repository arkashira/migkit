"""A hop that checks what a cloud's DTS job takes.

A DTS job is told, object by object, what to move: databases and their
new names, schemas, tables and their new names, a row filter per table,
the columns of a table. The check that follows must compare exactly that
- a log table the job was told to leave behind is neither moved nor
called missing. The sample files under `tests/samples` were written for
these tests (no real system's).
"""
import json
from pathlib import Path

import pytest

from migkit import hopkeys, imports
from migkit.config import build_hop

SAMPLES = Path(__file__).resolve().parent / "samples"


def _hop(got, target=None):
    raw = dict(got.raw)
    if target:
        raw["target"] = target
    assert not [p for p in hopkeys.check({"hops": {got.name: raw}})
                if p.level == "error"], raw
    return build_hop(got.name, raw)


def test_a_migration_job_as_the_cli_prints_it():
    got = imports.read(SAMPLES / "tencent-dts-migration.json")
    assert got.name == "orders-mysql-to-cloud"
    assert got.raw["goal"] == "verify"
    assert got.raw["source"] == ("mysql://dts_reader@10.0.0.21"
                                 "/billing,logistics")
    # the target is an instance the job reaches by the cloud's network
    assert "target" not in got.raw
    assert got.raw["db_map"] == {"logistics": "logistics_new"}
    assert got.raw["include"] == ["billing.*", "logistics.orders",
                                  "logistics.parcels"]
    assert got.raw["mapping"] == {
        "tables": {"logistics.parcels": "parcel"},
        "where": {"logistics.parcels": "id > 10"}}
    said = " | ".join(got.notes)
    for words in ("cdb-example1", "IsMigrateAccount false",
                  "IsDstReadOnly false", "reads 10% of the rows",
                  "covers only billing", "without encrypting"):
        assert words in said, (words, said)
    hop = _hop(got, "mysql://app@10.0.0.22")
    assert hop.databases == ["billing", "logistics"]
    assert hop.target_db("logistics") == "logistics_new"
    assert not hop.excluded("billing", "invoices")
    assert not hop.excluded("logistics", "orders")
    # the table the job was not told to take is the check's to leave
    assert hop.excluded("logistics", "event_log")
    assert hop.target_table_in("logistics", "parcels") == "parcel"
    assert hop.row_filter("logistics", "parcels") == "id > 10"


def _sync(**over):
    job = {"JobId": "sync-example1", "JobName": "cart-sync",
           "SrcDatabaseType": "postgresql", "DstDatabaseType": "postgresql",
           "SrcAccessType": "vpncloud",
           "SrcInfo": {"Ip": "10.0.0.31", "Port": 5432, "User": "rep"},
           "DstAccessType": "cdb", "DstInfo": {"InstanceId": "pg-example"},
           "Options": {"InitType": "Full",
                       "OpTypes": ["Insert", "Update", "Delete", "DDL"],
                       "ConflictHandleType": "ReportError"},
           "Objects": {"Mode": "Partial", "Databases": [
               {"DbName": "cart_src", "NewDbName": "cart",
                "DbMode": "Partial", "SchemaName": "public",
                "TableMode": "Partial", "Tables": [
                    {"TableName": "cart"},
                    {"TableName": "cart_item", "ColumnMode": "Partial",
                     "Columns": [{"ColumnName": "id"},
                                 {"ColumnName": "qty",
                                  "NewColumnName": "quantity"}]}]},
               {"DbName": "cart_src", "SchemaName": "audit",
                "DbMode": "Partial", "TableMode": "All"}]}}
    job.update(over)
    return {"Response": {"TotalCount": 1, "JobList": [job]}}


def test_a_sync_job_down_to_its_columns():
    got = imports.from_tencent_dts(_sync())
    assert got.raw["source"] == "postgres://rep@10.0.0.31/cart_src"
    assert got.raw["db_map"] == {"cart_src": "cart"}
    assert got.raw["include"] == ["cart_src.public.cart",
                                  "cart_src.public.cart_item",
                                  "cart_src.audit.*"]
    assert got.raw["mapping"] == {"columns": {
        "cart_src.public.cart_item": {"keep": ["id", "qty"],
                                      "rename": {"qty": "quantity"}}}}
    hop = _hop(got, "postgres://app@10.0.0.32")
    assert not hop.excluded("cart_src", "audit", "anything")
    assert hop.excluded("cart_src", "public", "cart_log")
    assert hop.column_rules("cart_src", "public", "cart_item")["keep"] == \
        ["id", "qty"]


def test_what_a_sync_job_does_not_carry_is_said():
    got = imports.from_tencent_dts(_sync(Options={
        "InitType": "None", "OpTypes": ["Insert", "Update"],
        "ConflictHandleType": "Ignore",
        "DealOfExistSameTable": "ExecuteAfterIgnore"}))
    said = " | ".join(got.notes)
    for words in ("does not carry deletes", "changes only",
                  "ConflictHandleType Ignore", "ExecuteAfterIgnore"):
        assert words in said, (words, said)


def test_a_job_over_the_whole_instance_takes_everything():
    job = {"MigrateOption": {"DatabaseTable": {"ObjectMode": "all"}},
           "SrcInfo": {"DatabaseType": "postgresql",
                       "Info": [{"Host": "10.0.0.5", "User": "a"}]},
           "DstInfo": {"DatabaseType": "postgresql",
                       "Info": [{"Host": "10.0.0.6", "User": "b"}]}}
    got = imports.from_tencent_dts(job)
    assert "include" not in got.raw and "databases" not in got.raw
    assert "whole instance" in " | ".join(got.notes)


def test_whole_databases_are_said_by_the_databases_alone():
    job = {"MigrateOption": {"DatabaseTable": {
        "ObjectMode": "partial", "Databases": [
            {"DbName": "a", "DBMode": "all"},
            {"DbName": "b", "DBMode": "all"}]}},
        "SrcInfo": {"DatabaseType": "mysql",
                    "Info": [{"Host": "10.0.0.5", "User": "u"}]}}
    got = imports.from_tencent_dts(job)
    assert got.raw["source"] == "mysql://u@10.0.0.5/a,b"
    assert "include" not in got.raw


def test_the_objects_a_data_check_compares():
    objects = {"ObjectMode": "partial", "ObjectItems": [
        {"DbName": "shop", "DbMode": "partial", "TableMode": "partial",
         "Tables": [{"TableName": "orders", "FilterCondition": "id < 100"}]}]}
    got = imports.from_tencent_dts(objects)
    assert got.raw["include"] == ["shop.orders"]
    assert got.raw["mapping"] == {"where": {"shop.orders": "id < 100"}}


def test_a_file_of_several_jobs_asks_for_one():
    many = _sync()
    many["Response"]["JobList"] *= 2
    with pytest.raises(SystemExit, match="give it one"):
        imports.from_tencent_dts(many)


def test_an_engine_migkit_has_none_for_is_refused():
    with pytest.raises(SystemExit, match="no engine for"):
        imports.from_tencent_dts(_sync(SrcDatabaseType="cosmos-of-mars"))


def test_a_password_in_the_file_is_never_copied():
    job = _sync()
    job["Response"]["JobList"][0]["SrcInfo"]["Password"] = "CHANGE_ME"
    got = imports.from_tencent_dts(job)
    assert "CHANGE_ME" not in json.dumps(got.raw)


def test_an_alibaba_job_as_its_detail_prints_it():
    got = imports.read(SAMPLES / "alibaba-dts-job.json")
    assert got.name == "shop-to-polardb"
    assert got.raw["goal"] == "verify"
    assert got.raw["source"] == "mysql://dts@10.0.0.41/shop,crm"
    assert got.raw["db_map"] == {"shop": "shop_new"}
    assert got.raw["include"] == ["shop.orders", "shop.users", "crm.*"]
    m = got.raw["mapping"]
    assert m["tables"] == {"shop.users": "members"}
    assert m["where"] == {"shop.orders": "id>10"}
    assert m["columns"] == {"shop.users": {"keep": ["id", "mail"],
                                           "rename": {"mail": "email"}}}
    said = " | ".join(got.notes)
    assert "pc-example" in said and "does not carry deletes" in said
    hop = _hop(got, "mysql://app@10.0.0.42")
    assert hop.excluded("shop", "sessions") and not hop.excluded("crm", "x")


def test_an_alibaba_job_whose_source_has_schemas():
    job = {"DtsJobName": "pg-one", "SourceEndpointEngineName": "POSTGRESQL",
           "SourceEndpointIP": "10.0.0.51", "SourceEndpointUserName": "dts",
           "SourceEndpointDatabaseName": "appdb",
           "DestinationEndpointEngineName": "POSTGRESQL",
           "DestinationEndpointDatabaseName": "appdb2",
           "DbList": {"public": {"name": "public", "all": False, "Table": {
               "orders": {"name": "orders", "all": True}}},
               "audit": {"name": "audit", "all": True}}}
    got = imports.from_alibaba_dts(job)
    assert got.raw["source"] == "postgres://dts@10.0.0.51/appdb"
    assert got.raw["db_map"] == {"appdb": "appdb2"}
    assert got.raw["include"] == ["appdb.public.orders", "appdb.audit.*"]
