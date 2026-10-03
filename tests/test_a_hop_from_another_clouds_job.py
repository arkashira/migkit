"""A hop that checks what Huawei Cloud's, Google Cloud's or Azure's
migration service took.

Each service is told, object by object, what to move - databases and
the names they land under, schemas, tables and theirs, a row filter, a
table's columns - and the check that follows must compare exactly that,
read from the service's own answer: DRS's job detail with its objects
beside it, a Database Migration Service job with its connection
profiles, a Datastream stream, an Azure project task or database
migration. None of these files may be taken for another tool's: each
sample is read by its own reader and no other. The sample files under
`tests/samples` and the dicts below were written for these tests (no
real system's), in the shapes the services' API references give.
"""
import json
from pathlib import Path

import pytest

from migkit import hopkeys, imports
from migkit.config import build_hop

SAMPLES = Path(__file__).resolve().parent / "samples"
GCP = "projects/example-project/locations/us-central1"


def _hop(got, target=None):
    raw = dict(got.raw)
    if target:
        raw["target"] = target
    assert not [p for p in hopkeys.check({"hops": {got.name: raw}})
                if p.level == "error"], raw
    return build_hop(got.name, raw)


def _said(got):
    return " | ".join(got.notes)


# ---- Huawei Cloud DRS ----------------------------------------------------

def _drs_v3():
    """The v3 batch detail: one job, its objects as `object_infos`, the
    answer's row filters beside it - one of them another job's."""
    return {"count": 1, "results": [{
        "id": "example-drs-job-0002", "name": "legacy-move",
        "status": "FULL_TRANSFER_STARTED", "task_type": "FULL_TRANS",
        "db_use_type": "migration",
        "source_endpoint": {"ip": "10.0.0.93", "db_type": "mysql",
                            "db_port": 3306, "ssl_link": True,
                            "db_user": "root"},
        "target_endpoint": {"ip": "10.0.0.94", "db_type": "mysql",
                            "db_port": 3306, "ssl_link": False,
                            "inst_id": "example-in02", "db_user": "root"},
        "is_target_readonly": True, "migrate_user": False,
        "object_infos": [
            {"id": "shop", "type": "database", "name": "shop",
             "select": "partial"},
            {"id": "shop-*-*-orders", "type": "table", "name": "orders",
             "select": "true", "parent_id": "shop"},
            {"id": "shop-*-*-items", "type": "table", "name": "items",
             "alias_name": "lines", "select": "true", "parent_id": "shop"},
            {"id": "crm", "type": "database", "name": "crm",
             "select": "true"},
            {"id": "crm-*-*-audit", "type": "table", "name": "audit",
             "select": "false", "parent_id": "crm"},
            {"id": "logs", "type": "database", "name": "logs",
             "select": "false"}]}],
        "data_transformation": {"total_count": 2, "filter_conditions": [
            {"data_transformation_object_infos": [
                {"id": "f1", "db_name": "shop", "table_name": "orders",
                 "data_transformation_type": "contentConditionalFilter"}],
             "transformation_info": {
                 "value": "id > 5",
                 "transformation_type": "contentConditionalFilter"}},
            {"data_transformation_object_infos": [
                {"id": "f2", "db_name": "other", "table_name": "t",
                 "data_transformation_type": "contentConditionalFilter"}],
             "transformation_info": {
                 "value": "id > 6",
                 "transformation_type": "contentConditionalFilter"}}]}}


def _drs_pg(**over):
    """The v5 batch APIs' `jobs`: a PostgreSQL job whose databases hold
    schemas."""
    job = {"base_info": {"name": "pg-sync",
                         "engine_type": "postgresql-to-postgresql",
                         "job_type": "sync", "task_type": "INCR_TRANS"},
           "source_endpoint": [{
               "db_type": "postgresql", "endpoint_role": "so",
               "endpoint": {"ip": "10.0.0.95:5433", "db_user": "rep",
                            "db_password": "CHANGE_ME"},
               "ssl": {"ssl_link": True}}],
           "target_endpoint": [{
               "db_type": "postgresql", "endpoint_role": "ta",
               "endpoint": {"ip": "10.0.0.96", "db_port": "5432",
                            "db_user": "app"}}],
           "db_object": {"object_scope": "table", "object_info": {
               "appdb": {"name": "appdb", "all": False, "schemas": {
                   "public": {"name": "public", "all": False, "tables": {
                       "orders": {"name": "orders", "type": "table"},
                       "items": {"name": "lines", "type": "table"}}},
                   "audit": {"name": "audit", "all": True}}}}}}
    job.update(over)
    return {"jobs": [job]}


def test_a_drs_job_with_its_objects_beside_it():
    got = imports.read(SAMPLES / "huawei-drs-job.json")
    assert got.name == "shop-to-cloud"
    assert got.raw["goal"] == "verify"
    assert got.raw["source"] == "mysql://drs_reader@10.0.0.91/billing,shop"
    # the target is an instance the job reaches by the cloud's network
    assert "target" not in got.raw
    assert got.raw["db_map"] == {"shop": "shop_new"}
    assert got.raw["include"] == ["billing.*", "shop.orders", "shop.users"]
    assert got.raw["mapping"] == {
        "tables": {"shop.users": "members"},
        "columns": {"shop.users": {"drop": ["secret"],
                                   "rename": {"mail": "email"}}},
        "where": {"shop.orders": "id > 100"},
        "ops": {"*": ["insert", "update"]}}
    # a view the job names by name; procedures it says nothing of
    assert got.raw["objects"] == {"views": ["billing.*", "shop.v_sales"]}
    said = _said(got)
    for words in ("example-rds-in01", "is_target_readonly false",
                  "does not carry deletes", "conflict_policy ignore",
                  "ddl_trans false", "does not move accounts"):
        assert words in said, (words, said)
    hop = _hop(got, "mysql://app@10.0.0.92")
    assert hop.databases == ["billing", "shop"]
    assert hop.target_db("shop") == "shop_new"
    assert not hop.excluded("billing", "invoices")
    assert hop.excluded("shop", "sessions")
    assert hop.target_table_in("shop", "users") == "members"
    assert hop.row_filter("shop", "orders") == "id > 100"
    assert hop.column_rules("shop", "users")["drop"] == ["secret"]
    assert hop.carries("shop", "orders") == {"insert", "update"}
    assert not hop.takes_object("views", "shop", "v_other")
    assert hop.takes_object("views", "billing", "v_any")


def test_a_drs_batch_detail_of_the_older_api_with_its_row_filters():
    got = imports.from_huawei_drs(_drs_v3())
    assert got.name == "legacy-move"
    assert got.raw["source"] == ("mysql://root@10.0.0.93/shop,crm"
                                 "?ssl-mode=REQUIRED")
    assert got.raw["target"] == "mysql://root@10.0.0.94/shop,crm"
    assert got.raw["include"] == ["shop.orders", "shop.items", "crm.*"]
    assert got.raw["exclude"] == ["crm.audit"]
    # the filter of a database the job does not take is another job's
    assert got.raw["mapping"] == {"tables": {"shop.items": "lines"},
                                  "where": {"shop.orders": "id > 5"}}
    said = _said(got)
    assert "moves the data once" in said and "does not move accounts" \
        in said, said
    hop = _hop(got)
    assert hop.excluded("crm", "audit") and not hop.excluded("crm", "x")
    assert hop.excluded("shop", "sessions")


def test_a_drs_job_whose_databases_hold_schemas():
    got = imports.from_huawei_drs(_drs_pg())
    assert got.raw["source"] == ("postgres://rep@10.0.0.95:5433/appdb"
                                 "?sslmode=require")
    assert got.raw["target"] == "postgres://app@10.0.0.96/appdb"
    assert got.raw["include"] == ["appdb.public.orders",
                                  "appdb.public.items", "appdb.audit.*"]
    assert got.raw["mapping"] == {"tables": {
        "appdb.public.items": "public.lines"}}
    assert "carries changes only" in _said(got)
    hop = _hop(got)
    assert hop.excluded("appdb", "public", "other")
    assert not hop.excluded("appdb", "audit", "anything")


def test_the_objects_alone_take_their_databases_and_ask_for_the_sides():
    got = imports.from_huawei_drs({
        "object_scope": "database",
        "object_info": {"a": {"name": "a2", "all": True}, "b": {}}})
    assert got.raw["databases"] == ["a", "b"]
    assert got.raw["db_map"] == {"a": "a2"}
    assert "include" not in got.raw
    said = _said(got)
    assert "names no source" in said and "names no target" in said


def test_a_drs_detail_without_its_objects_says_where_they_are():
    job = _drs_pg()["jobs"][0]
    del job["db_object"]
    with pytest.raises(SystemExit, match="db-objects"):
        imports.from_huawei_drs({"job": job})


def test_a_file_of_several_drs_jobs_asks_for_one():
    many = _drs_pg()
    many["jobs"] *= 2
    with pytest.raises(SystemExit, match="give it one"):
        imports.from_huawei_drs(many)


def test_a_drs_engine_migkit_has_none_for_is_refused():
    job = _drs_pg()
    job["jobs"][0]["source_endpoint"][0]["db_type"] = "gaussdbv5"
    with pytest.raises(SystemExit, match="no engine for"):
        imports.from_huawei_drs(job)


# ---- Google Cloud Database Migration Service ------------------------------

def _gdms_mysql():
    """A JSON list of the resources: a one-time job of some tables, and
    one of the two profiles it names."""
    return [{"name": f"{GCP}/migrationJobs/shop-mysql",
             "type": "ONE_TIME",
             "source": f"{GCP}/connectionProfiles/shop-src",
             "destination": f"{GCP}/connectionProfiles/shop-dst",
             "sourceDatabase": {"provider": "RDS", "engine": "MYSQL"},
             "objectsConfig": {"sourceObjectsConfig": {
                 "objectsSelectionType": "SPECIFIED_OBJECTS",
                 "objectConfigs": [
                     {"objectIdentifier": {"database": "shop",
                                           "table": "orders",
                                           "type": "TABLE"}},
                     {"objectIdentifier": {"database": "crm",
                                           "type": "DATABASE"}}]}}},
            {"name": f"{GCP}/connectionProfiles/shop-src",
             "mysql": {"host": "db.example.com", "port": 3306,
                       "username": "dms", "password": "CHANGE_ME",
                       "ssl": {"type": "SERVER_ONLY"}}}]


def _gdms_sqlserver():
    return {"name": f"{GCP}/migrationJobs/sales-mssql",
            "displayName": "sales-mssql", "type": "CONTINUOUS",
            "source": f"{GCP}/connectionProfiles/sales-src",
            "destination": f"{GCP}/connectionProfiles/sales-dst",
            "sourceDatabase": {"engine": "SQLSERVER"},
            "sqlserverHomogeneousMigrationJobConfig": {
                "backupFilePattern": ".*",
                "databaseBackups": [{"database": "sales"},
                                    {"database": "hr"}]}}


def _gdms_oracle():
    """A heterogeneous job: what it takes is its conversion workspace's."""
    return {"migrationJobs": [{
        "name": f"{GCP}/migrationJobs/ora-to-pg", "type": "CONTINUOUS",
        "source": f"{GCP}/connectionProfiles/ora",
        "destination": f"{GCP}/connectionProfiles/pg",
        "sourceDatabase": {"engine": "ORACLE"},
        "destinationDatabase": {"provider": "CLOUDSQL",
                                "engine": "POSTGRESQL"},
        "conversionWorkspace": {
            "name": f"{GCP}/conversionWorkspaces/ws"},
        "filter": "schema = \"SALES\""}],
        "connectionProfiles": [
            {"name": f"{GCP}/connectionProfiles/ora",
             "oracle": {"host": "10.0.0.71", "port": 1521,
                        "username": "reader",
                        "databaseService": "ORCLPDB"}},
            {"name": f"{GCP}/connectionProfiles/pg",
             "postgresql": {"host": "10.0.0.72", "port": 5432,
                            "username": "app"}}]}


def test_a_migration_job_with_its_connection_profiles():
    got = imports.read(SAMPLES / "gcp-dms-job.json")
    assert got.name == "orders-pg"
    assert got.raw["goal"] == "verify"
    assert got.raw["source"] == ("postgres://dms_reader@10.0.0.51"
                                 "/orders,billing?sslmode=require")
    # a Cloud SQL destination read at its address, on the engine's port
    assert got.raw["target"] == "postgres://10.0.0.52/orders,billing"
    # whole databases: the databases say it
    assert "include" not in got.raw
    said = _said(got)
    for words in ("users and roles", "materialized view", "unlogged",
                  "sequence", "no primary key"):
        assert words in said, (words, said)
    hop = _hop(got)
    assert hop.databases == ["orders", "billing"]


def test_a_job_of_some_tables_and_a_profile_the_file_does_not_hold():
    got = imports.from_google_dms(_gdms_mysql())
    assert got.name == "shop-mysql"
    assert got.raw["source"] == ("mysql://dms@db.example.com/shop,crm"
                                 "?ssl-mode=REQUIRED")
    assert "target" not in got.raw
    assert got.raw["include"] == ["shop.orders", "crm.*"]
    assert "CHANGE_ME" not in json.dumps(got.raw)
    said = _said(got)
    for words in ("no connection profile shop-dst", "moves the data once",
                  "mysql system database", "certificate"):
        assert words in said, (words, said)
    hop = _hop(got, "mysql://app@10.0.0.53")
    assert hop.excluded("shop", "sessions") and not hop.excluded("crm", "x")


def test_a_sql_server_job_takes_the_databases_it_has_backups_of():
    got = imports.from_google_dms(_gdms_sqlserver())
    assert got.raw["databases"] == ["sales", "hr"]
    assert "include" not in got.raw
    assert "no connection profile sales-src" in _said(got)


def test_a_heterogeneous_job_says_what_its_workspace_decides():
    got = imports.from_google_dms(_gdms_oracle())
    assert got.raw["engine"] == "hetero"
    assert got.raw["options"] == {"source_engine": "oracle",
                                  "target_engine": "postgres"}
    assert got.raw["source"] == "oracle://reader@10.0.0.71:1521/ORCLPDB"
    assert got.raw["target"] == "postgres://app@10.0.0.72"
    assert "conversion workspace" in _said(got)
    assert "include" not in got.raw


def test_a_file_of_several_migration_jobs_asks_for_one():
    job = _gdms_sqlserver()
    with pytest.raises(SystemExit, match="give it one"):
        imports.from_google_dms({"migrationJobs": [job, job]})


# ---- Google Cloud Datastream ----------------------------------------------

def _stream():
    return {"streams": [{
        "name": f"{GCP}/streams/orders-cdc", "displayName": "orders-cdc",
        "sourceConfig": {
            "sourceConnectionProfile": f"{GCP}/connectionProfiles/orders",
            "mysqlSourceConfig": {
                "includeObjects": {"mysqlDatabases": [
                    {"database": "shop", "mysqlTables": [
                        {"table": "orders"},
                        {"table": "customers", "mysqlColumns": [
                            {"column": "id"}, {"column": "name"},
                            {"column": "ssn"}]}]},
                    {"database": "crm"}]},
                "excludeObjects": {"mysqlDatabases": [
                    {"database": "shop", "mysqlTables": [
                        {"table": "customers",
                         "mysqlColumns": [{"column": "ssn"}]}]},
                    {"database": "crm",
                     "mysqlTables": [{"table": "audit"}]}]}}},
        "destinationConfig": {
            "destinationConnectionProfile": f"{GCP}/connectionProfiles/bq",
            "bigqueryDestinationConfig": {
                "singleTargetDataset": {"datasetId": "example:orders"},
                "appendOnly": {}}},
        "backfillAll": {"mysqlExcludedObjects": {"mysqlDatabases": [
            {"database": "crm", "mysqlTables": [{"table": "events"}]}]}}}],
        "connectionProfiles": [
            {"name": f"{GCP}/connectionProfiles/orders",
             "mysqlProfile": {"hostname": "10.0.0.81", "port": 3306,
                              "username": "ds_reader",
                              "password": "CHANGE_ME"}},
            {"name": f"{GCP}/connectionProfiles/bq",
             "bigqueryProfile": {}}]}


def _pg_stream():
    return [{"name": f"{GCP}/streams/pg-cdc",
             "sourceConfig": {
                 "sourceConnectionProfile": f"{GCP}/connectionProfiles/pg",
                 "postgresqlSourceConfig": {
                     "includeObjects": {"postgresqlSchemas": [
                         {"schema": "public", "postgresqlTables": [
                             {"table": "orders"}]}]},
                     "replicationSlot": "example_slot",
                     "publication": "example_pub"}},
             "destinationConfig": {
                 "gcsDestinationConfig": {"path": "/cdc"}},
             "backfillNone": {}},
            {"name": f"{GCP}/connectionProfiles/pg",
             "postgresqlProfile": {"hostname": "10.0.0.82", "port": 5432,
                                   "username": "ds", "database": "appdb"}}]


def test_a_stream_and_what_it_includes_and_leaves_out():
    got = imports.from_datastream(_stream())
    assert got.name == "orders-cdc"
    assert got.raw["goal"] == "verify"
    assert got.raw["source"] == "mysql://ds_reader@10.0.0.81/shop,crm"
    assert "target" not in got.raw
    assert got.raw["include"] == ["shop.orders", "shop.customers", "crm.*"]
    assert got.raw["exclude"] == ["crm.audit"]
    # a table's columns left out keep the table and drop the columns
    assert got.raw["mapping"] == {"columns": {
        "shop.customers": {"keep": ["id", "name"]}}}
    assert "CHANGE_ME" not in json.dumps(got.raw)
    said = _said(got)
    for words in ("BigQuery", "appendOnly", "crm.events"):
        assert words in said, (words, said)
    hop = _hop(got, "mysql://app@10.0.0.83")
    assert hop.excluded("crm", "audit") and not hop.excluded("crm", "x")
    assert hop.column_rules("shop", "customers")["keep"] == ["id", "name"]


def test_a_postgres_stream_names_its_tables_from_the_profiles_database():
    got = imports.from_datastream(_pg_stream())
    assert got.raw["source"] == "postgres://ds@10.0.0.82/appdb"
    assert got.raw["include"] == ["appdb.public.orders"]
    said = _said(got)
    assert "changes only" in said and "Cloud Storage" in said, said


# ---- Azure Database Migration Service ------------------------------------

def _sql_db():
    return {"name": "sales_new",
            "type": "Microsoft.DataMigration/databaseMigrations",
            "properties": {
                "kind": "SqlDb", "migrationStatus": "InProgress",
                "scope": "/subscriptions/00000000-0000-0000-0000-000000000000"
                         "/resourceGroups/example-rg/providers/Microsoft.Sql"
                         "/servers/example-sql",
                "sourceDatabaseName": "sales",
                "sourceSqlConnection": {
                    "dataSource": "tcp:10.0.0.71,1433",
                    "authentication": "SqlAuthentication",
                    "userName": "migrator", "password": "CHANGE_ME",
                    "encryptConnection": True,
                    "trustServerCertificate": True},
                "targetSqlConnection": {
                    "dataSource": "example-sql.database.example.com",
                    "authentication": "SqlAuthentication",
                    "userName": "admin", "encryptConnection": True},
                "tableList": ["[dbo].[Orders]", "[Sales].[Order Lines]"],
                "offlineConfiguration": {"offline": True}}}


def _sql_mi():
    return {"name": "db1",
            "type": "Microsoft.DataMigration/databaseMigrations",
            "properties": {
                "kind": "SqlMi", "sourceDatabaseName": "sourcename",
                "scope": "/subscriptions/00000000-0000-0000-0000-000000000000"
                         "/resourceGroups/example-rg/providers/Microsoft.Sql"
                         "/managedInstances/example-mi",
                "sourceSqlConnection": {"dataSource": "10.0.0.73",
                                        "userName": "migrator"},
                "backupConfiguration": {"sourceLocation": {
                    "fileStorageType": "FileShare"}},
                "offlineConfiguration": {"offline": False}}}


def _mongo_task():
    return {"value": [{
        "name": "mongo-move",
        "type": "Microsoft.DataMigration/services/projects/tasks",
        "properties": {"taskType": "Migrate.MongoDb", "input": {
            "source": {"type": "MongoDbConnectionInfo",
                       "connectionString": "mongodb://reader:CHANGE_ME@"
                                           "10.0.0.75:27017/?replicaSet=rs0"},
            "target": {"type": "MongoDbConnectionInfo",
                       "connectionString":
                           "mongodb+srv://example.mongo.example.com"},
            "databases": {"shop": {"collections": {
                "orders": {"canDelete": True}}},
                "crm": {"collections": {}}},
            "replication": "OneTime"}}}]}


def _cosmos():
    return {"name": "migrationRequest",
            "type": "Microsoft.DataMigration/databaseMigrations",
            "properties": {
                "kind": "MongoToCosmosDbMongo",
                "collectionList": [{"sourceDatabase": "shop",
                                    "sourceCollection": "orders",
                                    "targetDatabase": "shop2",
                                    "targetCollection": "orders_v2"}],
                "sourceMongoConnection": {"host": "10.0.0.76",
                                          "port": 27017},
                "targetMongoConnection": {
                    "host": "example.mongocluster.example.com",
                    "port": 10255}}}


def _pg_task():
    return {"properties": {
        "taskType": "Migrate.PostgreSql.AzureDbForPostgreSql.SyncV2",
        "input": {
            "sourceConnectionInfo": {
                "type": "PostgreSqlConnectionInfo", "serverName": "10.0.0.77",
                "port": 5432, "databaseName": "postgres", "userName": "rep",
                "encryptConnection": True},
            "targetConnectionInfo": {
                "type": "PostgreSqlConnectionInfo",
                "serverName": "pg.example.com", "port": 5432,
                "userName": "app", "encryptConnection": True},
            "selectedDatabases": [{
                "name": "dvd", "targetDatabaseName": "dvd",
                "selectedTables": [{"name": "public.actor"},
                                   {"name": "\"Sales\".\"Order\""}]}]}}}


def test_an_azure_task_as_the_api_answers_it():
    got = imports.read(SAMPLES / "azure-dms-task.json")
    assert got.name == "shop-offline"
    assert got.raw["goal"] == "verify"
    assert got.raw["source"] == ("mysql://dms@10.0.0.61/shop,crm"
                                 "?ssl-mode=REQUIRED")
    assert got.raw["target"] == ("mysql://admin@shop.mysql.example.com"
                                 "/shop_new,crm?ssl-mode=REQUIRED")
    assert got.raw["include"] == ["shop.orders", "shop.users", "crm.*"]
    assert got.raw["mapping"] == {"tables": {"shop.users": "members"}}
    assert "moves the data once" in _said(got)
    hop = _hop(got)
    assert hop.databases == ["shop", "crm"]
    assert hop.target_db("shop") == "shop_new"
    assert hop.excluded("shop", "sessions") and not hop.excluded("crm", "x")
    assert hop.target_table_in("shop", "users") == "members"


def test_a_sql_database_migration_of_the_newer_service():
    got = imports.from_azure_dms(_sql_db())
    assert got.name == "sales-new"
    assert got.raw["source"] == ("mssql://migrator@10.0.0.71/sales"
                                 "?encrypt=require")
    # the resource's own name is the target database's
    assert got.raw["target"] == ("mssql://admin@example-sql.database."
                                 "example.com/sales_new?encrypt=require")
    assert got.raw["include"] == ["sales.dbo.Orders",
                                  "sales.Sales.Order Lines"]
    assert "CHANGE_ME" not in json.dumps(got.raw)
    said = _said(got)
    assert "moves the data once" in said and "trustServerCertificate" \
        in said, said
    hop = _hop(got)
    assert hop.excluded("sales", "dbo", "Customers")
    assert not hop.excluded("sales", "dbo", "Orders")


def test_a_managed_instance_migration_leaves_its_target_to_be_asked():
    got = imports.from_azure_dms(_sql_mi())
    assert got.raw["source"] == "mssql://migrator@10.0.0.73/sourcename"
    assert "target" not in got.raw
    assert got.raw["db_map"] == {"sourcename": "db1"}
    said = _said(got)
    assert "example-mi" in said and "logins" in said, said
    assert "moves the data once" not in said


def test_a_mongodb_task_and_a_cosmos_migration():
    got = imports.from_azure_dms(_mongo_task())
    assert got.name == "mongo-move"
    assert got.raw["source"] == ("mongodb://reader@10.0.0.75:27017"
                                 "/shop,crm?replicaSet=rs0")
    assert got.raw["target"] == ("mongodb+srv://example.mongo.example.com"
                                 "/shop,crm")
    assert got.raw["include"] == ["shop.orders", "crm.*"]
    assert "CHANGE_ME" not in json.dumps(got.raw)
    said = _said(got)
    assert "password was left out" in said and "moves the data once" \
        in said, said
    cosmos = imports.from_azure_dms(_cosmos())
    assert cosmos.raw["source"] == "mongodb://10.0.0.76/shop"
    assert cosmos.raw["target"] == ("mongodb://example.mongocluster."
                                    "example.com:10255/shop2")
    assert cosmos.raw["include"] == ["shop.orders"]
    assert cosmos.raw["mapping"] == {"tables": {"shop.orders":
                                                "orders_v2"}}


def test_a_postgres_sync_task_takes_its_selected_tables():
    got = imports.from_azure_dms(_pg_task())
    assert got.raw["source"] == ("postgres://rep@10.0.0.77/dvd"
                                 "?sslmode=require")
    assert got.raw["include"] == ["dvd.public.actor", "dvd.Sales.Order"]
    said = _said(got)
    assert "sequence" in said and "moves the data once" not in said


def test_an_azure_task_that_migrates_nothing_is_refused():
    task = {"name": "probe",
            "type": "Microsoft.DataMigration/services/projects/tasks",
            "properties": {"taskType": "ConnectToSource.SqlServer",
                           "input": {}}}
    assert imports._is_azure_dms(task)
    with pytest.raises(SystemExit, match="does not migrate"):
        imports.from_azure_dms(task)


# ---- each reader knows its own files, and only them ---------------------

DETECTORS = {"tencent": imports._is_tencent_dts,
             "alibaba": imports._is_alibaba_dts,
             "huawei": imports._is_huawei_drs,
             "google": imports._is_google_dms,
             "datastream": imports._is_datastream,
             "azure": imports._is_azure_dms,
             "dms": imports._is_dms,
             "connector": imports._is_connector}
READERS = {"tencent": imports.from_tencent_dts,
           "alibaba": imports.from_alibaba_dts,
           "huawei": imports.from_huawei_drs,
           "google": imports.from_google_dms,
           "datastream": imports.from_datastream,
           "azure": imports.from_azure_dms,
           "dms": imports.from_dms,
           "connector": imports.from_connector}
#: the JSON samples and the reader that is each one's own
OWN = {"tencent-dts-migration.json": "tencent",
       "alibaba-dts-job.json": "alibaba",
       "dms-task.json": "dms",
       "connector-postgres.json": "connector",
       "huawei-drs-job.json": "huawei",
       "gcp-dms-job.json": "google",
       "azure-dms-task.json": "azure"}


def test_the_samples_named_here_are_there():
    assert not [s for s in OWN if not (SAMPLES / s).is_file()]


@pytest.mark.parametrize("sample", sorted(p.name for p in
                                          SAMPLES.glob("*.json")))
def test_each_sample_is_known_by_its_own_reader_alone(sample):
    """Every JSON sample - these and any added later - is taken by one
    reader, its own, and `read` gives what that reader gives."""
    data = json.loads((SAMPLES / sample).read_text())
    seen = {k for k, said in DETECTORS.items() if said(data)}
    assert len(seen) == 1, (sample, seen)
    own = seen.pop()
    assert OWN.get(sample, own) == own, (sample, own)
    got = imports.read(SAMPLES / sample)
    want = READERS[own](data, imports._name(Path(sample).stem))
    assert (got.name, got.raw, got.notes) == (want.name, want.raw,
                                              want.notes)


INLINE = [("huawei", _drs_v3), ("huawei", _drs_pg),
          ("google", _gdms_mysql), ("google", _gdms_sqlserver),
          ("google", _gdms_oracle), ("datastream", _stream),
          ("datastream", _pg_stream), ("azure", _sql_db),
          ("azure", _sql_mi), ("azure", _mongo_task), ("azure", _cosmos),
          ("azure", _pg_task)]


@pytest.mark.parametrize("own,make", INLINE,
                         ids=[m.__name__ for _, m in INLINE])
def test_each_shape_of_theirs_is_known_by_its_own_reader_alone(own, make):
    assert {k for k, seen in DETECTORS.items() if seen(make())} == {own}


def test_the_files_that_are_not_json_are_still_read_as_they_were():
    props = SAMPLES / "connector-mysql.properties"
    assert imports.read(props).raw == imports.from_connector(
        imports._properties(props.read_text()), "connector-mysql").raw
    load = SAMPLES / "sakila.load"
    assert imports.read(load).raw == imports.from_load_file(
        load.read_text(), "sakila").raw
