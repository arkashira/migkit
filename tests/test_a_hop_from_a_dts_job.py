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


def test_a_migration_job_of_the_older_api():
    """The 2018 API keeps a job's objects in `DatabaseInfo`, a JSON
    string, and says a whole instance with `MigrateObject` 1."""
    job = {"Response": {"JobList": [{
        "JobId": "dts-old1", "JobName": "legacy", "SrcDatabaseType": "mysql",
        "SrcInfo": {"Ip": "10.0.0.71", "Port": 3306, "User": "u"},
        "DstDatabaseType": "mysql", "DstInfo": {"InstanceId": "cdb-x"},
        "MigrateOption": {"MigrateType": 3, "MigrateObject": 2},
        "DatabaseInfo": json.dumps([{"Database": "a",
                                     "Table": ["t1", "t2"]},
                                    {"Database": "b"}])}]}}
    got = imports.from_tencent_dts(job)
    assert got.raw["source"] == "mysql://u@10.0.0.71/a,b"
    assert got.raw["include"] == ["a.t1", "a.t2", "b.*"]
    whole = json.loads(json.dumps(job))
    whole["Response"]["JobList"][0]["MigrateOption"]["MigrateObject"] = 1
    assert "include" not in imports.from_tencent_dts(whole).raw


def test_a_subscription_names_what_it_takes_and_asks_for_a_target():
    sub = {"Response": {
        "SubscribeId": "subs-1", "SubscribeName": "orders-feed",
        "Product": "mysql", "AccessType": "cdb", "SubscribeMode": "dml",
        "Endpoints": [{"HostName": "10.0.0.81", "Port": 3306, "User": "s"}],
        "SubscribeObjects": [{"ObjectType": "table", "Database": "shop",
                              "Tables": ["orders"]}]}}
    assert imports._is_tencent_dts(sub)
    got = imports.from_tencent_dts(sub)
    assert got.raw["source"] == "mysql://s@10.0.0.81/shop"
    assert got.raw["include"] == ["shop.orders"]
    assert "target" not in got.raw
    assert "a subscription carries the changes to a topic" in \
        " | ".join(got.notes)


def test_a_data_check_task_and_what_it_concluded():
    task = {"Response": {"Items": [{
        "JobId": "dts-1", "CompareTaskId": "dts-1-cmp-1", "TaskName": "c1",
        "Method": "sampleDataCheck", "Options": {"SampleRate": 20},
        "Conclusion": "same",
        "Config": {"ObjectMode": "partial", "ObjectItems": [
            {"DbName": "shop", "TableMode": "partial",
             "Tables": [{"TableName": "orders"}]}]}}]}}
    got = imports.from_tencent_dts(task)
    assert got.raw["include"] == ["shop.orders"]
    said = " | ".join(got.notes)
    assert "reads 20% of the rows" in said and "concluded same" in said


def test_a_list_of_migration_jobs_says_which_detail_to_save():
    jobs = {"Response": {"JobList": [{"JobId": "dts-abc", "JobName": "x",
                                      "SrcInfo": {"DatabaseType": "mysql"}}]}}
    with pytest.raises(SystemExit, match="DescribeMigrationDetail --JobId"
                                         " dts-abc"):
        imports.from_tencent_dts(jobs)


def test_the_changes_a_sync_job_carries_become_the_hops_contract():
    got = imports.from_tencent_dts(_sync(
        Options={"OpTypes": ["Insert", "Update", "DDL"]},
        Objects={"Mode": "Partial",
                 "Databases": [{"DbName": "a", "DbMode": "All"}],
                 "DatabasesOpFilter": [{"DbName": "a", "Tables": [
                     {"TableName": "t", "OpFilter": {"OpTypes":
                                                     ["Insert"]}}]}]}))
    assert got.raw["mapping"]["ops"] == {"*": ["insert", "update"],
                                         "a.*.t": ["insert"]}
    hop = _hop(got, "postgres://app@10.0.0.32")
    assert hop.carries("a", "public", "t") == {"insert"}
    assert hop.carries("a", "public", "other") == {"insert", "update"}


def test_an_alibaba_job_of_the_older_api_with_its_left_out_tables():
    job = {"MigrationJobId": "dtsx", "MigrationJobName": "old-one",
           "SourceEndpoint": {"EngineName": "MySQL", "IP": "10.0.0.61",
                              "Port": "3306", "UserName": "u"},
           "DestinationEndpoint": {"EngineName": "MySQL",
                                   "InstanceId": "rm-example"},
           "MigrationObject": json.dumps([
               {"DBName": "shop", "NewDBName": "shop2", "TableIncludes": [
                   {"TableName": "orders", "FilterCondition": "id>1",
                    "ColumnIncludes": [{"ColumnName": "id"},
                                       {"ColumnName": "st",
                                        "NewColumnName": "status"}]}]},
               {"DBName": "crm", "AllTable": True,
                "TableExcludes": [{"TableName": "audit_log"}]}])}
    assert imports._is_alibaba_dts(job)
    got = imports.from_alibaba_dts(job)
    assert got.raw["include"] == ["shop.orders", "crm.*"]
    assert got.raw["exclude"] == ["crm.audit_log"]
    assert got.raw["db_map"] == {"shop": "shop2"}
    assert got.raw["mapping"]["columns"] == {
        "shop.orders": {"keep": ["id", "st"], "rename": {"st": "status"}}}
    hop = _hop(got, "mysql://app@10.0.0.62")
    assert hop.excluded("crm", "audit_log") and not hop.excluded("crm", "x")


def test_the_dml_an_alibaba_job_carries_becomes_the_contract():
    got = imports.read(SAMPLES / "alibaba-dts-job.json")
    assert got.raw["mapping"]["ops"] == {"shop.orders": ["insert",
                                                         "update"]}


def test_the_objects_a_job_names_one_by_one_and_what_it_does_not_carry():
    job = {"MigrateOption": {"DatabaseTable": {
        "ObjectMode": "partial", "AdvancedObjects": ["function"],
        "Databases": [
            {"DbName": "a", "DBMode": "partial", "TableMode": "all",
             "ViewMode": "partial", "Views": [{"ViewName": "v1"}],
             "FunctionMode": "partial", "Functions": ["f1"]},
            {"DbName": "b", "DBMode": "all", "ViewMode": "all"}]}},
        "SrcInfo": {"DatabaseType": "mysql",
                    "Info": [{"Host": "10.0.0.5", "User": "u",
                              "Port": 3306}]}}
    got = imports.from_tencent_dts(job)
    assert got.raw["objects"] == {"views": ["a.v1", "b.*"],
                                  "functions": ["a.f1", "b.*"]}
    # a kind the job does not take at all is a finding, not left out
    said = " | ".join(got.notes)
    assert "carries no events, procedures, triggers" in said
    hop = _hop(got, "mysql://app@10.0.0.6")
    assert not hop.takes_object("views", "a", "v2")
    assert hop.takes_object("procedures", "a", "p1")
    assert hop.takes_object("triggers", "a", "t1")
