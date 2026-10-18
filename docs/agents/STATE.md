# migkit state

Written by `tools/gen_codemap.py` from `capabilities.matrix()`, `decide.coverage()`, `decide.still_lacking()`, `capabilities.depth()` and `docs/backlog-index.md`; the pre-commit hook keeps it current. What this machine last proved (the full suite, unpushed work) is in `.codemap/LOCAL.md`.

## Capabilities by engine

16 capabilities (`capabilities.CAPABILITIES`) and 12 shapes (`decide.coverage()`); a row lists what is not yet there and counts the depth items owed, as each engine says it still lacks them (`decide.still_lacking()`).

| engine | capabilities | shapes | depth owed | not yet |
|---|---|---|---|---|
| analyticdb-mysql | 16/16 | 12/12 | 75 | - |
| analyticdb-postgres | 13/16 | 12/12 | 75 | params, statistics, snapshot |
| ase | 15/16 | 11/12 | 68 | stream, partitioned |
| bigquery | 16/16 | 12/12 | 50 | - |
| bigtable | 16/16 | 11/12 | 42 | large values |
| cassandra | 16/16 | 12/12 | 30 | - |
| clickhouse | 16/16 | 12/12 | 46 | - |
| cockroachdb | 16/16 | 12/12 | 52 | - |
| copybook | 16/16 | 12/12 | 31 | - |
| cosmosdb | 16/16 | 12/12 | 22 | - |
| couchbase | 16/16 | 12/12 | 50 | - |
| databricks | 16/16 | 7/12 | 76 | keyless, no unique index, wide key, large values, partitioned |
| datastore | 16/16 | 12/12 | 37 | - |
| db2 | 16/16 | 12/12 | 68 | - |
| db2i | 16/16 | 12/12 | 71 | - |
| db2zos | 16/16 | 12/12 | 61 | - |
| dcdb | 15/16 | 12/12 | 66 | bulk-move |
| delta | 9/16 | 11/12 | 49 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| dsql | 16/16 | 12/12 | 29 | - |
| duckdb | 16/16 | 12/12 | 46 | - |
| dynamodb | 16/16 | 12/12 | 9 | - |
| exasol | 16/16 | 7/12 | 76 | keyless, no unique index, wide key, large values, partitioned |
| firestore | 16/16 | 12/12 | 44 | - |
| gaussdb | 16/16 | 12/12 | 69 | - |
| geminidb-cassandra | 16/16 | 12/12 | 48 | - |
| geminidb-mongo | 16/16 | 12/12 | 51 | - |
| geminidb-redis | 16/16 | 12/12 | 37 | - |
| generic | 16/16 | 12/12 | 59 | - |
| hetero | 16/16 | 10/12 | 46 | partitioned, missing grant |
| iceberg | 9/16 | 11/12 | 50 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| informix | 16/16 | 7/12 | 76 | keyless, no unique index, wide key, large values, partitioned |
| kafka | 16/16 | 12/12 | 12 | - |
| keyspaces | 16/16 | 12/12 | 51 | - |
| kinesis | 16/16 | 12/12 | 4 | - |
| lindorm | 12/16 | 10/12 | 57 | stream, fence, confirm, delta, large values, partitioned |
| mongodb | 16/16 | 11/12 | 12 | missing grant |
| mssql | 16/16 | 12/12 | 35 | - |
| mysql | 16/16 | 12/12 | 1 | - |
| neo4j | 13/16 | 10/12 | 56 | bulk-move, confirm, snapshot, large values, partitioned |
| neptune | 16/16 | 11/12 | 45 | large values |
| netezza | 16/16 | 7/12 | 76 | keyless, no unique index, wide key, large values, partitioned |
| oceanbase | 15/16 | 12/12 | 68 | bulk-move |
| oceanbase-oracle | 11/16 | 12/12 | 71 | bulk-move, stream, fence, confirm, delta |
| opensearch | 16/16 | 12/12 | 27 | - |
| oracle | 16/16 | 12/12 | 41 | - |
| parquet | 16/16 | 12/12 | 32 | - |
| polardbx | 15/16 | 12/12 | 69 | bulk-move |
| postgres | 16/16 | 12/12 | 0 | - |
| progress | 16/16 | 7/12 | 76 | keyless, no unique index, wide key, large values, partitioned |
| pubsub | 16/16 | 12/12 | 4 | - |
| qdrant | 14/16 | 10/12 | 36 | bulk-move, snapshot, large values, partitioned |
| redis | 16/16 | 12/12 | 11 | - |
| redshift | 16/16 | 12/12 | 52 | - |
| saas | 16/16 | 12/12 | 22 | - |
| saphana | 16/16 | 7/12 | 76 | keyless, no unique index, wide key, large values, partitioned |
| sapodp | 16/16 | 12/12 | 22 | - |
| singlestore | 16/16 | 12/12 | 75 | - |
| snowflake | 16/16 | 12/12 | 58 | - |
| spanner | 16/16 | 12/12 | 60 | - |
| sqlite | 16/16 | 12/12 | 43 | - |
| synapse | 16/16 | 12/12 | 73 | - |
| teradata | 16/16 | 12/12 | 32 | - |
| tidb | 16/16 | 12/12 | 17 | - |
| timestream | 16/16 | 11/12 | 38 | large values |
| vertica | 16/16 | 7/12 | 76 | keyless, no unique index, wide key, large values, partitioned |
| yugabytedb | 13/16 | 12/12 | 75 | params, statistics, snapshot |

## Depth by engine

90 items in 10 areas (`capabilities.depth()`, backlog P0-PARITY), each read off the code an engine runs; a way counts once a test proves it on that engine (`PROVES` in the test module). Cells: built, unproved 1392, n/a 1508, not yet 1814, test owed 230, yes 996. A cell below is the ways proved of the items that apply.

| engine | scope | verify | tail | assess | schema | move | cutover | security | managed | operations | all |
|---|---|---|---|---|---|---|---|---|---|---|---|
| analyticdb-mysql | 0/10 | 0/40 | 0/7 | 0/7 | 0/6 | 0/7 | 0/8 | 0/1 | 1/1 | 0/1 | 1/88 |
| analyticdb-postgres | 0/10 | 0/40 | 0/7 | 0/7 | 0/6 | 0/7 | 0/8 | 0/1 | 1/1 | 0/1 | 1/88 |
| ase | 0/10 | 0/35 | 0/7 | 0/7 | 1/6 | 3/8 | 1/7 | 0/1 | 0/1 | 0/1 | 5/83 |
| bigquery | 5/9 | 1/29 | 0/7 | 1/6 | 1/6 | 1/7 | 1/5 | 1/1 | 1/1 | 0/1 | 12/72 |
| bigtable | 1/9 | 0/18 | 1/6 | 0/6 | 1/3 | 2/6 | 0/3 | 1/1 | 1/1 | 0/1 | 7/54 |
| cassandra | 6/9 | 15/24 | 1/6 | 0/7 | 1/3 | 2/7 | 5/5 | 1/1 | 1/1 | 1/1 | 33/64 |
| clickhouse | 7/9 | 6/30 | 1/7 | 1/7 | 1/5 | 5/8 | 4/6 | 1/1 | 1/1 | 1/1 | 28/75 |
| cockroachdb | 5/10 | 5/35 | 3/7 | 1/7 | 3/6 | 1/7 | 3/7 | 1/1 | 1/1 | 0/1 | 23/82 |
| copybook | 0/7 | 0/16 | - | 0/4 | 1/2 | 2/4 | 0/1 | 1/1 | - | 0/1 | 4/36 |
| cosmosdb | 6/9 | 14/21 | 4/6 | 2/6 | 1/3 | 3/6 | 1/3 | 1/1 | 1/1 | 0/1 | 33/57 |
| couchbase | 0/9 | 1/23 | 1/6 | 1/7 | 1/4 | 1/7 | 2/6 | 1/1 | 0/1 | 0/1 | 8/65 |
| databricks | 0/10 | 0/39 | 0/7 | 0/7 | 0/6 | 0/7 | 2/6 | 0/1 | 0/1 | 0/1 | 2/85 |
| datastore | 0/8 | 0/17 | - | 0/6 | 0/2 | 2/4 | 0/2 | 1/1 | 1/1 | 0/1 | 4/42 |
| db2 | 1/10 | 0/37 | 0/7 | 0/7 | 1/6 | 3/8 | 1/7 | 0/1 | 1/1 | 0/1 | 7/85 |
| db2i | 0/10 | 1/37 | 0/7 | 0/7 | 0/6 | 0/8 | 0/6 | 1/1 | 0/1 | 0/1 | 2/84 |
| db2zos | 0/9 | 0/34 | - | 1/7 | 0/6 | 0/7 | 0/4 | 0/1 | 0/1 | 0/1 | 1/70 |
| dcdb | 0/10 | 2/35 | 1/7 | 0/7 | 1/6 | 0/8 | 3/7 | 1/1 | 1/1 | 0/1 | 9/83 |
| delta | 0/9 | 0/24 | 0/7 | 0/5 | 1/2 | 2/7 | 0/3 | 1/1 | 0/1 | 0/1 | 4/60 |
| dsql | 5/10 | 21/32 | 1/7 | 3/7 | 5/6 | 3/6 | 2/5 | 1/1 | 1/1 | 0/1 | 42/76 |
| duckdb | 1/9 | 2/24 | - | 0/7 | 1/6 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 11/57 |
| dynamodb | 6/8 | 12/14 | 4/6 | 5/6 | 2/2 | 4/5 | 4/4 | 1/1 | 1/1 | 0/1 | 39/48 |
| exasol | 0/10 | 0/39 | 0/7 | 0/7 | 0/6 | 0/7 | 2/6 | 0/1 | 0/1 | 0/1 | 2/85 |
| firestore | 0/9 | 2/21 | 1/6 | 0/6 | 0/2 | 2/5 | 2/3 | 1/1 | 1/1 | 0/1 | 9/55 |
| gaussdb | 0/10 | 3/39 | 1/7 | 0/7 | 1/6 | 2/8 | 3/7 | 1/1 | 1/1 | 0/1 | 12/87 |
| geminidb-cassandra | 0/9 | 1/22 | 0/1 | 0/7 | 0/4 | 0/5 | 0/5 | 1/1 | 1/1 | 0/1 | 3/56 |
| geminidb-mongo | 0/9 | 1/24 | 0/7 | 0/7 | 0/3 | 0/6 | 0/5 | 1/1 | 1/1 | 0/1 | 3/64 |
| geminidb-redis | 0/8 | 1/12 | 0/7 | 0/7 | 0/1 | 1/4 | 0/5 | 1/1 | 1/1 | 0/1 | 4/47 |
| generic | 9/10 | 10/39 | 1/7 | 1/7 | 0/6 | 3/7 | 0/4 | 0/1 | 0/1 | 0/1 | 24/83 |
| hetero | 4/10 | 6/40 | 1/2 | 2/5 | - | 6/9 | 2/5 | - | - | - | 21/71 |
| iceberg | 0/9 | 0/24 | 0/7 | 0/5 | 1/3 | 2/7 | 0/3 | 1/1 | 0/1 | 0/1 | 4/61 |
| informix | 0/10 | 0/39 | 0/7 | 0/7 | 0/6 | 0/7 | 2/6 | 0/1 | 0/1 | 0/1 | 2/85 |
| kafka | 5/9 | 15/17 | 5/6 | 5/7 | 0/1 | 5/7 | 5/5 | 1/1 | 1/1 | 1/1 | 43/55 |
| keyspaces | 0/9 | 0/24 | 1/6 | 0/7 | 1/3 | 1/6 | 0/3 | 1/1 | 1/1 | 0/1 | 5/61 |
| kinesis | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| lindorm | 0/9 | 2/27 | 0/7 | 0/7 | 0/5 | 0/7 | 1/5 | 1/1 | 1/1 | 0/1 | 5/70 |
| mongodb | 8/9 | 21/24 | 4/7 | 6/7 | 1/3 | 4/7 | 4/5 | 1/1 | 1/1 | 1/1 | 51/65 |
| mssql | 9/10 | 21/35 | 3/7 | 0/7 | 4/6 | 4/8 | 5/7 | 0/1 | 1/1 | 0/1 | 47/83 |
| mysql | 10/10 | 32/32 | 7/7 | 7/7 | 6/6 | 7/9 | 8/8 | 1/1 | 1/1 | 1/1 | 80/82 |
| neo4j | 0/9 | 1/25 | 1/6 | 0/7 | 0/4 | 0/8 | 0/4 | 0/1 | 0/1 | 0/1 | 2/66 |
| neptune | 0/8 | 0/20 | 1/6 | 0/7 | 0/3 | 2/5 | 0/3 | 1/1 | 1/1 | 0/1 | 5/55 |
| netezza | 0/10 | 0/39 | 0/7 | 0/7 | 0/6 | 0/7 | 2/6 | 0/1 | 0/1 | 0/1 | 2/85 |
| oceanbase | 0/10 | 1/36 | 0/7 | 0/7 | 1/6 | 1/8 | 0/7 | 1/1 | 1/1 | 0/1 | 5/84 |
| oceanbase-oracle | 0/10 | 1/38 | 0/7 | 0/7 | 0/6 | 0/8 | 2/7 | 1/1 | 1/1 | 0/1 | 5/86 |
| opensearch | 7/9 | 17/25 | 1/7 | 2/7 | 2/4 | 6/7 | 4/6 | 1/1 | 1/1 | 0/1 | 41/68 |
| oracle | 9/10 | 14/37 | 3/7 | 1/7 | 4/6 | 5/8 | 4/7 | 0/1 | 1/1 | 1/1 | 42/85 |
| parquet | 0/7 | 0/16 | - | 0/4 | 1/2 | 3/6 | 1/1 | 1/1 | 0/1 | 0/1 | 6/39 |
| polardbx | 0/10 | 1/36 | 0/7 | 0/7 | 0/6 | 0/8 | 1/7 | 1/1 | 1/1 | 0/1 | 4/84 |
| postgres | 10/10 | 38/38 | 7/7 | 7/7 | 6/6 | 9/9 | 8/8 | 1/1 | 1/1 | 1/1 | 88/88 |
| progress | 0/10 | 0/39 | 0/7 | 0/7 | 0/6 | 0/7 | 2/6 | 0/1 | 0/1 | 0/1 | 2/85 |
| pubsub | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| qdrant | 0/7 | 1/15 | - | 0/7 | 1/2 | 1/7 | 0/2 | 0/1 | 0/1 | 0/1 | 3/43 |
| redis | 5/8 | 12/12 | 2/7 | 7/7 | 0/1 | 4/5 | 5/6 | 1/1 | 1/1 | 1/1 | 38/49 |
| redshift | 5/10 | 0/29 | 0/5 | 3/7 | 1/5 | 1/3 | 2/6 | 1/1 | 1/1 | 0/1 | 14/68 |
| saas | 0/6 | 0/3 | 0/5 | 1/5 | - | 0/1 | 0/1 | 0/1 | 0/1 | - | 1/23 |
| saphana | 0/10 | 0/39 | 0/7 | 0/7 | 0/6 | 0/7 | 2/6 | 0/1 | 0/1 | 0/1 | 2/85 |
| sapodp | 0/6 | 0/3 | 0/5 | 0/5 | 0/1 | 0/1 | 1/1 | 1/1 | 0/1 | - | 2/24 |
| singlestore | 0/10 | 0/40 | 0/7 | 0/7 | 0/6 | 0/7 | 0/8 | 0/1 | 1/1 | 0/1 | 1/88 |
| snowflake | 4/10 | 0/33 | 0/7 | 3/7 | 1/6 | 0/5 | 2/7 | 1/1 | 0/1 | 0/1 | 11/78 |
| spanner | 0/10 | 2/31 | 1/7 | 0/6 | 1/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 11/73 |
| sqlite | 2/9 | 4/28 | - | 2/7 | 2/5 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 17/60 |
| synapse | 0/10 | 1/37 | 0/5 | 0/7 | 0/6 | 0/6 | 2/7 | 0/1 | 1/1 | 0/1 | 4/81 |
| teradata | 6/8 | 12/30 | - | 2/7 | 3/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 30/64 |
| tidb | 9/10 | 25/32 | 4/7 | 5/7 | 5/5 | 5/8 | 4/6 | 1/1 | 0/1 | 0/1 | 58/78 |
| timestream | 0/7 | 0/19 | - | 0/6 | 0/1 | 1/5 | 1/3 | 1/1 | 1/1 | 0/1 | 4/44 |
| vertica | 0/10 | 0/39 | 0/7 | 0/7 | 0/6 | 0/7 | 2/6 | 0/1 | 0/1 | 0/1 | 2/85 |
| yugabytedb | 0/10 | 0/40 | 0/7 | 0/7 | 0/6 | 0/7 | 0/8 | 0/1 | 1/1 | 0/1 | 1/88 |

Built, unproved - a way the code has on an engine no sandbox here can run; the engine's module says which server it was not run on (`UNPROVED_ON`):

- analyticdb-mysql: built; not run on a real AnalyticDB for MySQL or AnalyticDB for PostgreSQL instance (67 cells)
- analyticdb-postgres: built; not run on a real AnalyticDB for MySQL or AnalyticDB for PostgreSQL instance (59 cells)
- ase: built; not run on a real SAP ASE server (15 cells)
- bigquery: built; not run on a real Redshift cluster, Snowflake account or BigQuery project (50 cells)
- cockroachdb: built; not run on a real CockroachDB cluster, only on its single-node container and on fakes: the accounts, the cluster settings compared, the export and import through a stage, the snapshot's mark and large values read in pieces (46 cells)
- cosmosdb: built; not run on a real Cosmos DB account, only on its emulator and on fakes: the bulk writes and the snapshot's continuation per feed range (12 cells)
- couchbase: built; not run on a real Couchbase cluster, only on its community container and on fakes: the accounts, the bucket settings compared, the statistics refreshed, the bulk importer and the snapshot's mark (17 cells)
- databricks: built; not run on a real Netezza, SAP HANA, Informix, OpenEdge, Vertica, Exasol or Databricks server (23 cells)
- db2: built; not run on a real Db2 LUW server (51 cells)
- db2i: built; not run on a real IBM i (54 cells)
- db2zos: built; not run on a real Db2 for z/OS subsystem (45 cells)
- dcdb: built; not run on a real PolarDB-X or TDSQL instance (56 cells)
- dsql: built; not run on a real Aurora DSQL cluster, only on a PostgreSQL server and on fakes: the statistics refreshed, the snapshot's mark and large values read in pieces (21 cells)
- exasol: built; not run on a real Netezza, SAP HANA, Informix, OpenEdge, Vertica, Exasol or Databricks server (23 cells)
- gaussdb: built; not run on a real GaussDB instance (36 cells)
- geminidb-cassandra: built; not run on a real GeminiDB instance (46 cells)
- geminidb-mongo: built; not run on a real GeminiDB instance (59 cells)
- geminidb-redis: built; not run on a real GeminiDB instance (32 cells)
- informix: built; not run on a real Netezza, SAP HANA, Informix, OpenEdge, Vertica, Exasol or Databricks server (23 cells)
- kafka: built; not run on a real cluster of several brokers (7 cells)
- keyspaces: built; not run on a real Amazon Keyspaces table (46 cells)
- kinesis: built; not run on a real Amazon Kinesis Data Stream (1 cells)
- lindorm: built; not run on a real Lindorm instance (14 cells)
- mongodb: built; not run on a real MongoDB replica set, only on fakes; a cell whose lane test is not written yet is owed that test (12 cells)
- mssql: built; not run on a real SQL Server, only on its edge edition in the sandbox (stopped Agent, CLR off, a thinner catalogue); a cell whose lane test is not written yet is owed that test (30 cells)
- neptune: built; not run on a real Amazon Neptune cluster (20 cells)
- netezza: built; not run on a real Netezza, SAP HANA, Informix, OpenEdge, Vertica, Exasol or Databricks server (23 cells)
- oceanbase: built; not run on a real OceanBase tenant (64 cells)
- oceanbase-oracle: built; not run on a real OceanBase tenant (22 cells)
- oracle: built; not run on a real Oracle server, only on fakes and stand-ins; a cell whose lane test is not written yet is owed that test (39 cells)
- polardbx: built; not run on a real PolarDB-X or TDSQL instance (44 cells)
- progress: built; not run on a real Netezza, SAP HANA, Informix, OpenEdge, Vertica, Exasol or Databricks server (23 cells)
- pubsub: built; not run on a real Google Cloud Pub/Sub topic (1 cells)
- redshift: built; not run on a real Redshift cluster, Snowflake account or BigQuery project (29 cells)
- saas: built; not run on a real SaaS production service (4 cells)
- saphana: built; not run on a real Netezza, SAP HANA, Informix, OpenEdge, Vertica, Exasol or Databricks server (23 cells)
- sapodp: built; not run on a real SAP Gateway system (5 cells)
- singlestore: built; not run on a real SingleStore workspace (67 cells)
- snowflake: built; not run on a real Redshift cluster, Snowflake account or BigQuery project (45 cells)
- synapse: built; not run on a real Synapse dedicated SQL pool (35 cells)
- tidb: built; not run on a real TiDB cluster, only on its single-node container and on fakes: the accounts, the bulk load, the snapshot's mark, TiCDC's open-protocol and Avro messages, binary keys on the canal-json way only, and the service details unverified (IMPORT INTO wildcard, --ignore-fields, Avro watermark framing, DSQL ANALYZE) (9 cells)
- timestream: built; not run on a real Amazon Timestream database (11 cells)
- vertica: built; not run on a real Netezza, SAP HANA, Informix, OpenEdge, Vertica, Exasol or Databricks server (23 cells)
- yugabytedb: built; not run on a real YugabyteDB cluster (60 cells)
- alibaba: built; not run against a real Alibaba Cloud account (free.provider-metric)
- alibaba-analyticdb-mysql: built; not run on a real AnalyticDB for MySQL cluster (managed.readiness, managed.brand-api)
- alibaba-analyticdb-postgres: built; not run on a real AnalyticDB for PostgreSQL instance (managed.readiness, managed.brand-api)
- aws: built; not run against a real AWS account (managed.sign-in, managed.forwarder, managed.readiness, managed.brand-api, managed.retention-api, managed.egress-gate, free.provider-metric)
- azure: built; not run against a real Azure account (managed.sign-in, managed.forwarder, managed.readiness, managed.egress-gate, free.provider-metric)
- azure-synapse: built; not run on a real Synapse dedicated SQL pool (managed.readiness, managed.brand-api)
- documentdb: built; not run against a real DocumentDB cluster (brand.told, preflight.change-streams, preflight.retention, tail.ddl-snapshot)
- ebs: built on fakes; not run against a real EBS snapshot (seed.blocks, seed.changed-list, seed.fast-forward)
- gcp: built; not run against a real Google Cloud account (managed.sign-in, managed.forwarder, managed.readiness, managed.egress-gate, free.provider-metric)
- huawei: built; not run against a real Huawei Cloud account (free.provider-metric)
- ibm: built; not run against a real IBM Cloud account (free.provider-metric)
- lvm: built on fakes; not run against a real LVM thin volume (seed.blocks, seed.changed-list, seed.fast-forward)
- msk: built; not run against a real MSK cluster (brand.told, sign-in.iam, sign-in.mechanisms)
- oracle: built; not run against a real Oracle Cloud account (free.provider-metric)
- redis-enterprise: built; not run against a real Redis Enterprise (brand.told)
- singlestore-helios: built; not run on a real SingleStore Helios workspace (managed.readiness, managed.brand-api)
- tencent: built; not run against a real Tencent Cloud account (free.provider-metric)
- windows: built; not run on Windows (bundle.install, bundle.source-build)
- yugabyte-aeon: built; not run on a real YugabyteDB Aeon cluster (managed.readiness, managed.brand-api)
- zfs: built on fakes; not run against a real ZFS dataset (seed.blocks, seed.changed-list, seed.fast-forward)

Per item: the engines in each state - "test owed" is a way in the code whose test on that engine is not written (`UNPROVED`); "built, unproved" is one whose engine module declares it was not run on a real server.

| item | yes | test owed | built, unproved | not yet | n/a |
|---|---|---|---|---|---|
| scope.listings | bigquery bigtable cassandra clickhouse cockroachdb cosmosdb dsql dynamodb generic kafka mongodb mssql mysql opensearch oracle postgres redis redshift snowflake teradata tidb | datastore firestore neo4j qdrant spanner | the rest (26) | copybook couchbase db2i db2zos delta duckdb hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.deep | cassandra clickhouse cockroachdb cosmosdb db2 dynamodb generic kafka mongodb mssql mysql opensearch oracle postgres redis teradata tidb | bigtable datastore duckdb firestore hetero qdrant spanner | the rest (29) | copybook couchbase delta iceberg neo4j neptune parquet sqlite | kinesis pubsub saas sapodp timestream |
| scope.copy | bigquery cassandra clickhouse cockroachdb cosmosdb dsql dynamodb generic kafka mongodb mssql mysql opensearch oracle postgres redis redshift snowflake teradata tidb | bigtable datastore firestore neo4j qdrant spanner | the rest (26) | copybook couchbase db2i db2zos delta duckdb hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.repair | cassandra clickhouse cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis redshift snowflake sqlite teradata tidb | bigtable datastore firestore neo4j qdrant spanner | the rest (31) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| scope.counters | mssql mysql oracle postgres tidb | duckdb | analyticdb-mysql analyticdb-postgres ase cockroachdb db2 db2i db2zos dcdb dsql gaussdb oceanbase oceanbase-oracle polardbx redshift singlestore snowflake yugabytedb | databricks exasol generic hetero informix netezza progress saphana spanner sqlite synapse vertica | the rest (31) |
| scope.objects | generic kafka mongodb mssql mysql oracle postgres tidb | cassandra clickhouse duckdb opensearch sqlite teradata | the rest (28) | ase bigtable couchbase datastore delta firestore hetero iceberg lindorm neo4j polardbx spanner timestream | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub qdrant redis saas sapodp |
| scope.ops | bigquery cassandra clickhouse cockroachdb cosmosdb dsql dynamodb generic hetero mongodb mssql mysql opensearch oracle postgres redis redshift snowflake tidb | bigtable delta firestore iceberg neo4j spanner | the rest (28) | ase kafka lindorm oceanbase-oracle | copybook datastore db2zos duckdb parquet qdrant sqlite teradata timestream |
| scope.row-filter | bigquery clickhouse cockroachdb dsql duckdb generic hetero mongodb mssql mysql opensearch oracle postgres redshift sqlite teradata tidb | bigtable cassandra copybook datastore delta dynamodb firestore iceberg neo4j parquet qdrant spanner | the rest (26) | analyticdb-mysql analyticdb-postgres geminidb-redis kafka redis saas sapodp singlestore synapse | kinesis pubsub |
| scope.transforms | generic mysql postgres | - | databricks exasol informix mssql netezza progress saphana vertica | the rest (53) | kinesis pubsub |
| scope.names | bigquery cassandra clickhouse cosmosdb dsql dynamodb generic hetero mongodb mssql mysql opensearch oracle postgres teradata tidb | bigtable copybook datastore delta duckdb firestore iceberg neo4j parquet qdrant spanner sqlite | the rest (31) | geminidb-redis kafka redis saas sapodp | kinesis pubsub |
| verify.value.one-plan | mssql mysql postgres | - | analyticdb-mysql dcdb oceanbase polardbx singlestore tidb | the rest (49) | geminidb-redis kinesis neo4j pubsub qdrant redis saas sapodp |
| verify.access.object-ownership | dsql mysql postgres tidb | clickhouse teradata | analyticdb-mysql analyticdb-postgres cockroachdb db2 db2i db2zos dcdb mssql oceanbase oracle redshift singlestore snowflake synapse yugabytedb | ase databricks exasol gaussdb generic hetero informix lindorm neo4j netezza oceanbase-oracle polardbx progress saphana spanner vertica | the rest (29) |
| verify.access.sequence-grants | postgres | - | analyticdb-postgres cockroachdb dsql mssql oracle snowflake yugabytedb | analyticdb-mysql databricks db2 db2i db2zos dcdb exasol gaussdb generic hetero informix netezza oceanbase oceanbase-oracle polardbx progress saphana singlestore spanner synapse vertica | the rest (37) |
| verify.access.table-grants | cassandra kafka mysql opensearch postgres redis tidb | clickhouse teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb dcdb dsql geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mongodb mssql oceanbase oracle redshift singlestore snowflake synapse yugabytedb | the rest (20) | bigtable copybook cosmosdb datastore delta duckdb dynamodb firestore iceberg kinesis neptune parquet pubsub qdrant saas sapodp sqlite timestream |
| verify.identity.sequence-collision | mssql mysql oracle postgres tidb | - | analyticdb-mysql analyticdb-postgres cockroachdb db2 db2i db2zos dcdb dsql oceanbase singlestore yugabytedb | ase databricks duckdb exasol gaussdb generic hetero informix netezza oceanbase-oracle polardbx progress redshift saphana snowflake spanner sqlite synapse vertica | the rest (31) |
| verify.identity.sequence-parity | mssql mysql oracle postgres tidb | - | analyticdb-mysql analyticdb-postgres cockroachdb db2 db2i db2zos dcdb dsql oceanbase oceanbase-oracle singlestore snowflake yugabytedb | ase databricks duckdb exasol gaussdb generic hetero informix netezza polardbx progress redshift saphana spanner sqlite synapse vertica | the rest (31) |
| verify.movement.target-ahead | cassandra dsql kafka mongodb mssql mysql postgres redis tidb | clickhouse opensearch teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase oracle redshift singlestore snowflake synapse yugabytedb | the rest (31) | kinesis pubsub saas sapodp |
| verify.structure.collection-options | cosmosdb dynamodb kafka mongodb mysql opensearch postgres | cassandra clickhouse | analyticdb-mysql bigquery db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces oceanbase oracle redshift singlestore snowflake synapse tidb | the rest (33) | copybook geminidb-redis kinesis parquet pubsub redis saas sapodp |
| verify.structure.columns | cassandra cockroachdb dsql mssql mysql opensearch postgres teradata tidb | clickhouse | analyticdb-mysql analyticdb-postgres bigquery db2 db2i db2zos dcdb geminidb-cassandra keyspaces oceanbase oracle redshift singlestore snowflake synapse yugabytedb | the rest (21) | bigtable cosmosdb couchbase datastore dynamodb firestore geminidb-mongo geminidb-redis kafka kinesis mongodb neo4j neptune pubsub qdrant redis saas sapodp timestream |
| verify.structure.deferrable | dsql postgres | - | oracle | analyticdb-mysql analyticdb-postgres databricks exasol gaussdb generic hetero informix netezza oceanbase-oracle progress saphana singlestore spanner sqlite synapse vertica yugabytedb | the rest (45) |
| verify.structure.extensions | postgres | - | - | analyticdb-mysql analyticdb-postgres databricks exasol gaussdb generic hetero informix neo4j netezza progress saphana singlestore synapse vertica yugabytedb | the rest (49) |
| verify.structure.foreign-keys | dsql generic mssql mysql postgres tidb | teradata | analyticdb-mysql bigquery cockroachdb databricks db2 db2i db2zos dcdb exasol informix netezza oceanbase oracle progress redshift saphana singlestore snowflake synapse vertica | analyticdb-postgres ase duckdb gaussdb hetero neptune oceanbase-oracle polardbx spanner sqlite yugabytedb | the rest (28) |
| verify.structure.indexes | cosmosdb dsql dynamodb firestore gaussdb generic lindorm mongodb postgres qdrant teradata tidb | cassandra clickhouse datastore spanner | the rest (19) | analyticdb-mysql ase bigtable couchbase dcdb duckdb hetero neo4j neptune oceanbase oceanbase-oracle polardbx singlestore snowflake sqlite synapse | copybook delta geminidb-redis iceberg kafka kinesis mysql opensearch parquet pubsub redis redshift saas sapodp timestream |
| verify.structure.keys | cockroachdb dsql generic mssql mysql postgres redis teradata tidb | clickhouse | analyticdb-mysql analyticdb-postgres bigquery databricks db2 db2i db2zos dcdb exasol geminidb-redis informix netezza oceanbase oracle progress redshift saphana singlestore snowflake synapse vertica yugabytedb | ase copybook duckdb gaussdb hetero iceberg oceanbase-oracle polardbx sqlite | the rest (25) |
| verify.structure.materialized-views | postgres | cassandra clickhouse | bigquery cockroachdb db2 db2i db2zos geminidb-cassandra keyspaces oracle redshift snowflake synapse | analyticdb-mysql analyticdb-postgres ase bigtable cosmosdb databricks exasol gaussdb generic hetero iceberg informix lindorm netezza oceanbase oceanbase-oracle polardbx progress saphana singlestore vertica yugabytedb | the rest (30) |
| verify.structure.objects | cockroachdb duckdb generic kafka mongodb mssql mysql oracle postgres tidb | cassandra clickhouse opensearch teradata | the rest (22) | analyticdb-mysql ase bigtable couchbase datastore dcdb delta firestore gaussdb hetero iceberg lindorm neo4j oceanbase polardbx singlestore spanner sqlite timestream | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub qdrant redis saas sapodp |
| verify.structure.partitions | kafka mysql postgres tidb | clickhouse teradata | analyticdb-mysql bigquery db2 db2i db2zos dcdb gaussdb mssql oceanbase oceanbase-oracle oracle singlestore | analyticdb-postgres ase databricks delta exasol generic hetero iceberg informix lindorm netezza parquet polardbx progress saphana synapse timestream vertica yugabytedb | the rest (29) |
| verify.structure.row-security | mssql opensearch postgres | clickhouse | bigquery db2 db2i db2zos oracle redshift snowflake synapse | analyticdb-mysql analyticdb-postgres ase cockroachdb databricks delta exasol firestore gaussdb generic hetero iceberg informix netezza oceanbase-oracle progress saphana singlestore teradata vertica yugabytedb | the rest (33) |
| verify.structure.schema-diff | dsql mysql postgres | cassandra | analyticdb-postgres bigquery cockroachdb db2 db2i db2zos gaussdb geminidb-cassandra keyspaces yugabytedb | the rest (35) | cosmosdb couchbase datastore dynamodb firestore geminidb-mongo geminidb-redis kafka kinesis mongodb neo4j neptune pubsub qdrant redis saas sapodp |
| verify.structure.sharding | cosmosdb mongodb | clickhouse opensearch teradata | db2 db2i db2zos geminidb-mongo redshift | analyticdb-mysql analyticdb-postgres databricks dcdb exasol gaussdb generic hetero informix neo4j netezza oceanbase oceanbase-oracle polardbx progress qdrant saphana singlestore synapse timestream vertica yugabytedb | the rest (34) |
| verify.structure.triggers | mssql mysql postgres | cassandra teradata | analyticdb-mysql cockroachdb cosmosdb db2 db2i db2zos dcdb geminidb-cassandra keyspaces oceanbase oracle singlestore | analyticdb-postgres ase couchbase databricks exasol gaussdb generic hetero informix neo4j netezza oceanbase-oracle polardbx progress saphana sqlite synapse vertica yugabytedb | the rest (30) |
| verify.structure.unvalidated-constraints | dsql mysql oracle postgres tidb | - | analyticdb-mysql cockroachdb db2 db2i db2zos dcdb mssql oceanbase singlestore | analyticdb-postgres ase databricks exasol gaussdb generic hetero informix neo4j netezza oceanbase-oracle polardbx progress saphana spanner sqlite synapse teradata vertica yugabytedb | the rest (32) |
| verify.value.charset | cassandra mssql mysql oracle postgres teradata tidb | clickhouse | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb dsql geminidb-cassandra geminidb-mongo keyspaces mongodb oceanbase singlestore yugabytedb | the rest (31) | cosmosdb dynamodb kafka kinesis neo4j opensearch pubsub qdrant redis saas sapodp |
| verify.value.collation | dsql mongodb mysql opensearch postgres teradata tidb | - | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-mongo mssql oceanbase oracle singlestore yugabytedb | ase databricks delta duckdb exasol gaussdb generic hetero informix netezza oceanbase-oracle polardbx progress redshift saphana snowflake sqlite synapse vertica | the rest (26) |
| verify.value.generated | cosmosdb dsql mysql opensearch postgres teradata tidb | clickhouse | analyticdb-mysql analyticdb-postgres cockroachdb db2 db2i db2zos dcdb mssql oceanbase oracle singlestore yugabytedb | ase databricks delta duckdb exasol gaussdb generic hetero informix lindorm netezza oceanbase-oracle polardbx progress redshift saphana snowflake spanner sqlite synapse vertica | the rest (25) |
| verify.value.narrowing | cassandra cockroachdb dsql mongodb mysql opensearch postgres teradata tidb | clickhouse | analyticdb-mysql analyticdb-postgres bigquery db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle singlestore yugabytedb | the rest (32) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.null-empty | cassandra cosmosdb dsql dynamodb generic kafka mongodb mysql opensearch postgres tidb | clickhouse teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb databricks db2 db2i db2zos dcdb exasol geminidb-cassandra geminidb-mongo informix keyspaces mssql netezza oceanbase oracle progress saphana singlestore vertica yugabytedb | the rest (24) | geminidb-redis kinesis pubsub redis saas sapodp |
| verify.value.precision | cassandra dsql mongodb mysql opensearch postgres tidb | clickhouse teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle singlestore yugabytedb | the rest (32) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.rendering | dsql mongodb mssql mysql postgres | clickhouse | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-mongo oceanbase oracle singlestore yugabytedb | the rest (35) | bigtable cassandra cosmosdb dynamodb geminidb-redis kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.timezone | cassandra dsql mongodb mysql postgres teradata tidb | clickhouse opensearch | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle singlestore yugabytedb | the rest (31) | bigtable cosmosdb dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.type-drift | cosmosdb dynamodb mongodb opensearch | - | geminidb-mongo kafka | analyticdb-mysql analyticdb-postgres bigtable couchbase databricks datastore exasol firestore geminidb-redis generic hetero informix neo4j neptune netezza progress qdrant saphana singlestore sqlite synapse timestream vertica yugabytedb | the rest (36) |
| verify.delta | cassandra clickhouse cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | bigtable firestore neo4j spanner | the rest (27) | delta iceberg lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| verify.fence | the rest (22) | bigtable neo4j | analyticdb-mysql analyticdb-postgres ase bigquery databricks db2 db2i exasol geminidb-mongo informix keyspaces neptune netezza oceanbase polardbx progress saphana singlestore snowflake vertica yugabytedb | delta iceberg lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| verify.confirm | cassandra cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | bigtable clickhouse firestore spanner | the rest (26) | delta iceberg lindorm neo4j oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| verify.users | cassandra clickhouse db2i dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis hetero kafka lindorm mongodb mssql mysql neo4j oceanbase oceanbase-oracle opensearch oracle polardbx postgres redis spanner synapse teradata | - | analyticdb-mysql analyticdb-postgres ase bigquery cockroachdb couchbase db2 db2zos redshift singlestore snowflake tidb yugabytedb | - | the rest (28) |
| verify.repair | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | bigtable cassandra clickhouse datastore firestore neo4j opensearch qdrant spanner | the rest (32) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.repair-undo | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | bigtable cassandra clickhouse datastore firestore neo4j opensearch qdrant spanner | the rest (25) | ase copybook databricks delta duckdb exasol iceberg informix netezza parquet progress saphana vertica | kinesis pubsub saas sapodp |
| verify.rows-apart | cassandra clickhouse cosmosdb dsql dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | - | analyticdb-postgres cockroachdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces yugabytedb | the rest (42) | kinesis pubsub |
| verify.tables | cassandra clickhouse cosmosdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis | - | geminidb-cassandra geminidb-mongo geminidb-redis keyspaces redshift snowflake synapse | the rest (45) | kinesis pubsub |
| verify.table | bigquery cassandra clickhouse cosmosdb dsql duckdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite teradata tidb | bigtable copybook datastore delta firestore hetero iceberg neo4j parquet qdrant spanner | the rest (27) | databricks exasol generic informix netezza progress saphana vertica | kinesis pubsub |
| tail.follows | the rest (26) | - | analyticdb-mysql analyticdb-postgres bigquery databricks db2 db2i exasol geminidb-mongo geminidb-redis informix netezza oceanbase polardbx progress saphana singlestore snowflake vertica yugabytedb | ase delta iceberg lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| tail.table-states | cockroachdb cosmosdb dynamodb kafka mongodb mysql postgres redis tidb | bigtable firestore hetero neo4j spanner | the rest (23) | ase cassandra clickhouse databricks delta exasol generic iceberg informix lindorm netezza oceanbase-oracle opensearch progress redshift saphana synapse vertica | copybook datastore db2zos duckdb kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.added | cockroachdb kafka mssql mysql oracle postgres tidb | spanner | analyticdb-mysql bigquery geminidb-mongo mongodb oceanbase polardbx singlestore snowflake | the rest (37) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.readded | cosmosdb dynamodb kafka mssql mysql oracle postgres tidb | - | analyticdb-mysql bigquery geminidb-mongo mongodb oceanbase polardbx singlestore snowflake | the rest (37) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.ddl | cosmosdb dynamodb kafka mongodb mysql postgres | - | analyticdb-mysql geminidb-mongo oceanbase oracle polardbx singlestore | the rest (41) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.renames | mongodb mysql postgres | - | analyticdb-mysql bigquery geminidb-mongo mssql oceanbase oracle polardbx singlestore snowflake | the rest (31) | bigtable cassandra copybook cosmosdb couchbase datastore db2zos duckdb dynamodb firestore geminidb-cassandra hetero kafka keyspaces kinesis neo4j neptune parquet pubsub qdrant sqlite teradata timestream |
| tail.quiet | mysql postgres | bigtable clickhouse dynamodb firestore neo4j opensearch redis spanner | the rest (31) | ase cassandra delta generic iceberg keyspaces lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| assess.versions | dsql kafka mongodb mysql opensearch postgres redis redshift snowflake sqlite teradata tidb | cassandra clickhouse | analyticdb-postgres cockroachdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oracle synapse yugabytedb | the rest (26) | bigquery bigtable copybook cosmosdb datastore delta dynamodb firestore hetero iceberg kinesis parquet pubsub spanner timestream |
| assess.brands | kafka mongodb mysql opensearch postgres redis redshift snowflake tidb | cassandra clickhouse teradata | analyticdb-mysql analyticdb-postgres cockroachdb dcdb dsql gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase oracle polardbx singlestore synapse yugabytedb | the rest (30) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.engine-checks | bigquery cockroachdb cosmosdb couchbase db2zos dsql dynamodb generic hetero kafka mongodb mysql oracle postgres redis redshift saas snowflake sqlite teradata tidb | bigtable cassandra clickhouse datastore firestore neo4j opensearch qdrant spanner | the rest (27) | ase copybook db2 delta duckdb iceberg kinesis parquet pubsub | - |
| assess.preflight | cosmosdb dsql dynamodb kafka mongodb mysql postgres redis tidb | cassandra clickhouse opensearch teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase oracle polardbx redshift singlestore snowflake synapse yugabytedb | the rest (27) | kinesis pubsub saas sapodp |
| assess.capacity | clickhouse dynamodb hetero mongodb mysql postgres redis tidb | cassandra opensearch teradata | analyticdb-mysql bigquery cosmosdb dcdb geminidb-cassandra geminidb-mongo geminidb-redis kafka keyspaces mssql oceanbase oracle polardbx redshift singlestore snowflake | the rest (37) | saas sapodp |
| assess.writers | dynamodb mysql postgres redis | clickhouse opensearch teradata | bigquery cosmosdb geminidb-mongo kafka mongodb mssql oracle | the rest (50) | kinesis pubsub |
| assess.handwork | dynamodb kafka mongodb mysql postgres redis | cassandra clickhouse opensearch teradata | analyticdb-postgres bigquery cockroachdb cosmosdb dsql gaussdb geminidb-mongo geminidb-redis mssql oracle redshift snowflake synapse tidb yugabytedb | the rest (37) | copybook kinesis parquet pubsub |
| schema.create | the rest (32) | datastore firestore neo4j | analyticdb-mysql analyticdb-postgres db2i db2zos geminidb-cassandra kafka lindorm neptune oceanbase-oracle polardbx singlestore synapse timestream yugabytedb | databricks exasol generic informix netezza progress saphana vertica | geminidb-mongo geminidb-redis hetero kinesis mongodb pubsub redis saas sapodp |
| schema.indexes | cockroachdb dsql dynamodb mongodb mysql postgres teradata tidb | cassandra clickhouse spanner sqlite | analyticdb-mysql analyticdb-postgres cosmosdb dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle polardbx singlestore yugabytedb | the rest (25) | copybook delta geminidb-redis hetero iceberg kafka kinesis opensearch parquet pubsub redis redshift saas sapodp timestream |
| schema.foreign-keys | dsql mssql mysql oracle postgres tidb | neo4j spanner teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb dcdb gaussdb neptune oceanbase polardbx redshift singlestore snowflake yugabytedb | ase databricks db2 db2i db2zos duckdb exasol generic informix netezza oceanbase-oracle progress saphana sqlite synapse vertica | the rest (28) |
| schema.defaults | cockroachdb dsql mysql opensearch postgres sqlite teradata tidb | clickhouse spanner | analyticdb-mysql analyticdb-postgres bigquery dcdb gaussdb mssql oceanbase oracle polardbx redshift sapodp singlestore snowflake synapse yugabytedb | the rest (21) | bigtable cassandra cosmosdb couchbase datastore dynamodb firestore geminidb-mongo geminidb-redis hetero kafka kinesis mongodb neo4j neptune pubsub qdrant redis saas timestream |
| schema.views | dsql mssql mysql oracle postgres tidb | clickhouse teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb dcdb gaussdb geminidb-mongo mongodb oceanbase polardbx redshift singlestore snowflake synapse yugabytedb | ase bigtable couchbase databricks db2 db2i db2zos duckdb exasol generic iceberg informix lindorm netezza oceanbase-oracle opensearch progress saphana spanner sqlite vertica | the rest (22) |
| schema.routines | mssql mysql oracle postgres | - | analyticdb-mysql bigquery cockroachdb dcdb geminidb-mongo mongodb oceanbase polardbx singlestore | the rest (33) | bigtable copybook datastore delta dynamodb firestore hetero iceberg kafka keyspaces kinesis neptune parquet pubsub qdrant saas sapodp sqlite tidb timestream |
| move.table-copy | the rest (38) | neo4j | analyticdb-mysql analyticdb-postgres bigquery databricks db2i db2zos dcdb exasol geminidb-cassandra geminidb-mongo informix lindorm netezza oceanbase-oracle polardbx progress saphana singlestore snowflake synapse timestream vertica yugabytedb | - | kinesis pubsub saas sapodp |
| move.bulk | bigtable cassandra clickhouse copybook datastore duckdb dynamodb firestore gaussdb kafka mongodb mssql mysql neptune opensearch oracle parquet postgres redis spanner sqlite timestream | hetero | ase bigquery cockroachdb cosmosdb couchbase db2 db2i db2zos geminidb-cassandra geminidb-mongo keyspaces tidb | dcdb delta iceberg neo4j oceanbase oceanbase-oracle polardbx qdrant | the rest (23) |
| move.large-values | the rest (19) | teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb couchbase db2i db2zos dcdb dsql gaussdb geminidb-mongo geminidb-redis oceanbase oceanbase-oracle polardbx singlestore snowflake synapse yugabytedb | bigtable databricks exasol informix lindorm neo4j neptune netezza progress qdrant saphana timestream vertica | cassandra copybook cosmosdb datastore dynamodb firestore geminidb-cassandra keyspaces kinesis pubsub redshift saas sapodp spanner |
| move.partitioned | clickhouse generic kafka mysql postgres tidb | parquet teradata | analyticdb-mysql analyticdb-postgres bigquery db2 db2i db2zos dcdb gaussdb mssql oceanbase oceanbase-oracle oracle polardbx singlestore synapse yugabytedb | ase databricks delta exasol hetero iceberg informix lindorm neo4j netezza progress qdrant saphana vertica | the rest (28) |
| move.spread | hetero postgres | cassandra mysql | mongodb | - | the rest (61) |
| move.sized | ase bigquery clickhouse cosmosdb db2 dsql duckdb dynamodb hetero kafka mongodb mssql mysql opensearch postgres redis sqlite tidb | cassandra teradata | analyticdb-mysql analyticdb-postgres cockroachdb db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle oracle polardbx redshift singlestore snowflake synapse yugabytedb | the rest (23) | kinesis pubsub |
| move.load-window | hetero mysql opensearch oracle postgres | cassandra clickhouse teradata | analyticdb-mysql dcdb geminidb-cassandra geminidb-mongo keyspaces mongodb mssql oceanbase polardbx singlestore | the rest (30) | bigquery bigtable copybook delta dynamodb geminidb-redis iceberg kafka kinesis neptune parquet pubsub redis redshift saas sapodp snowflake timestream |
| move.create-missing | cosmosdb dsql dynamodb hetero mysql opensearch oracle postgres teradata tidb | cassandra clickhouse | analyticdb-mysql analyticdb-postgres bigquery cockroachdb dcdb gaussdb geminidb-cassandra kafka keyspaces mssql oceanbase polardbx redshift singlestore snowflake synapse yugabytedb | the rest (26) | datastore firestore geminidb-mongo geminidb-redis kinesis mongodb neptune pubsub redis saas sapodp |
| move.reshard | postgres | bigtable cassandra clickhouse dynamodb firestore generic hetero mysql neo4j opensearch redis spanner | the rest (32) | ase delta iceberg lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| cutover.freeze | cassandra clickhouse cockroachdb databricks exasol informix kafka mongodb mssql mysql netezza opensearch postgres progress redis redshift saphana snowflake synapse teradata tidb vertica | hetero | analyticdb-mysql analyticdb-postgres ase couchbase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis oceanbase oceanbase-oracle oracle polardbx singlestore yugabytedb | - | the rest (27) |
| cutover.read-only-target | the rest (30) | hetero | analyticdb-mysql analyticdb-postgres db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis oceanbase oceanbase-oracle polardbx singlestore yugabytedb | - | bigtable copybook cosmosdb datastore delta duckdb firestore generic iceberg keyspaces kinesis neo4j neptune parquet pubsub qdrant saas sapodp spanner sqlite timestream |
| cutover.counters | dcdb duckdb gaussdb hetero mssql mysql oceanbase-oracle oracle polardbx postgres spanner sqlite tidb | - | analyticdb-mysql analyticdb-postgres ase cockroachdb db2 db2i db2zos dsql oceanbase redshift singlestore snowflake yugabytedb | - | the rest (40) |
| cutover.rollback | cassandra clickhouse dcdb duckdb dynamodb firestore gaussdb hetero kafka mssql mysql oceanbase-oracle opensearch oracle parquet postgres redis spanner sqlite timestream | bigtable copybook datastore | the rest (23) | analyticdb-postgres delta iceberg neo4j qdrant yugabytedb | databricks exasol generic informix kinesis netezza progress pubsub redshift saas saphana sapodp teradata vertica |
| cutover.drain | the rest (21) | bigtable | analyticdb-mysql bigquery db2 db2i geminidb-cassandra geminidb-mongo geminidb-redis keyspaces neptune oceanbase polardbx saas singlestore snowflake | analyticdb-postgres ase databricks delta exasol generic hetero iceberg informix lindorm neo4j netezza oceanbase-oracle progress redshift saphana synapse vertica yugabytedb | copybook datastore db2zos duckdb kinesis parquet pubsub qdrant sqlite teradata timestream |
| cutover.pause-replication | mysql postgres | - | - | analyticdb-mysql analyticdb-postgres databricks exasol generic informix neo4j netezza progress qdrant redis saphana singlestore synapse vertica yugabytedb | the rest (48) |
| cutover.two-way | cassandra dynamodb kafka mongodb mysql postgres redis teradata | - | geminidb-cassandra geminidb-mongo geminidb-redis keyspaces | the rest (43) | copybook db2zos duckdb hetero kinesis parquet pubsub qdrant saas sapodp sqlite |
| cutover.jobs | mysql postgres | clickhouse | mssql oracle | the rest (31) | bigtable cassandra copybook cosmosdb datastore delta dsql duckdb dynamodb firestore geminidb-cassandra geminidb-mongo geminidb-redis hetero iceberg kafka keyspaces kinesis mongodb neo4j neptune parquet pubsub qdrant redis saas sapodp spanner sqlite tidb |
| security.tls | the rest (44) | - | analyticdb-mysql analyticdb-postgres oracle singlestore yugabytedb | ase databricks db2 db2zos exasol generic informix mssql neo4j netezza progress qdrant saas saphana synapse vertica | hetero |
| managed.catalogued | the rest (41) | - | - | ase couchbase databricks db2i db2zos delta duckdb exasol generic iceberg informix neo4j netezza parquet progress qdrant saas saphana sapodp snowflake sqlite tidb vertica | copybook hetero |
| operations.fencing | cassandra clickhouse kafka mongodb mysql oracle postgres redis | - | analyticdb-mysql analyticdb-postgres cockroachdb dcdb dsql gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase polardbx singlestore tidb yugabytedb | the rest (40) | hetero saas sapodp |

## Source layout

440 upward imports still listed (`docs/agents/LAYERS.md`; the list only shrinks); 1 files no package claims (`docs/agents/INVENTORY.md`).

Gaps per pair (`autonomous.gaps`: a pair lacks what its two engines lack): 4290 ordered pairs of 66 engines; 50 shape lacks, 38 capability lacks, 3112 depth lacks in all; fewest mysql -> postgres (1), most vertica -> saphana (162).

## Gaps by pair

71 pairs the registry carries (every engine of `engines.NAMES` to itself, and every pair a cross-engine way joins, `crossmove._joined`), each with what `autonomous.gaps(src, dst)` says it lacks by kind; 3323 lacks in all (backlog P0-DECISION-ALL.gaps).

| pair | shape | capability | depth | in all |
|---|---|---|---|---|
| analyticdb-mysql -> analyticdb-mysql | 0 | 0 | 75 | 75 |
| analyticdb-postgres -> analyticdb-postgres | 0 | 3 | 75 | 78 |
| ase -> ase | 1 | 1 | 68 | 70 |
| bigquery -> bigquery | 0 | 0 | 50 | 50 |
| bigtable -> bigtable | 1 | 0 | 42 | 43 |
| cassandra -> cassandra | 0 | 0 | 30 | 30 |
| clickhouse -> clickhouse | 0 | 0 | 46 | 46 |
| cockroachdb -> cockroachdb | 0 | 0 | 52 | 52 |
| copybook -> copybook | 0 | 0 | 31 | 31 |
| cosmosdb -> cosmosdb | 0 | 0 | 22 | 22 |
| couchbase -> couchbase | 0 | 0 | 50 | 50 |
| databricks -> databricks | 5 | 0 | 76 | 81 |
| datastore -> datastore | 0 | 0 | 37 | 37 |
| db2 -> db2 | 0 | 0 | 68 | 68 |
| db2i -> db2i | 0 | 0 | 71 | 71 |
| db2zos -> db2zos | 0 | 0 | 61 | 61 |
| dcdb -> dcdb | 0 | 1 | 66 | 67 |
| delta -> delta | 1 | 7 | 49 | 57 |
| dsql -> dsql | 0 | 0 | 29 | 29 |
| duckdb -> duckdb | 0 | 0 | 46 | 46 |
| duckdb -> postgres | 0 | 0 | 46 | 46 |
| dynamodb -> dynamodb | 0 | 0 | 9 | 9 |
| exasol -> exasol | 5 | 0 | 76 | 81 |
| firestore -> firestore | 0 | 0 | 44 | 44 |
| gaussdb -> gaussdb | 0 | 0 | 69 | 69 |
| geminidb-cassandra -> geminidb-cassandra | 0 | 0 | 48 | 48 |
| geminidb-mongo -> geminidb-mongo | 0 | 0 | 51 | 51 |
| geminidb-redis -> geminidb-redis | 0 | 0 | 37 | 37 |
| generic -> generic | 0 | 0 | 59 | 59 |
| hetero -> hetero | 2 | 0 | 46 | 48 |
| iceberg -> iceberg | 1 | 7 | 50 | 58 |
| informix -> informix | 5 | 0 | 76 | 81 |
| kafka -> kafka | 0 | 0 | 12 | 12 |
| keyspaces -> keyspaces | 0 | 0 | 51 | 51 |
| kinesis -> kinesis | 0 | 0 | 4 | 4 |
| lindorm -> lindorm | 2 | 4 | 57 | 63 |
| mongodb -> mongodb | 1 | 0 | 12 | 13 |
| mssql -> mssql | 0 | 0 | 35 | 35 |
| mysql -> mysql | 0 | 0 | 1 | 1 |
| mysql -> postgres | 0 | 0 | 1 | 1 |
| neo4j -> neo4j | 2 | 3 | 56 | 61 |
| neptune -> neptune | 1 | 0 | 45 | 46 |
| netezza -> netezza | 5 | 0 | 76 | 81 |
| oceanbase -> oceanbase | 0 | 1 | 68 | 69 |
| oceanbase-oracle -> oceanbase-oracle | 0 | 5 | 71 | 76 |
| opensearch -> opensearch | 0 | 0 | 27 | 27 |
| oracle -> oracle | 0 | 0 | 41 | 41 |
| parquet -> parquet | 0 | 0 | 32 | 32 |
| parquet -> postgres | 0 | 0 | 32 | 32 |
| polardbx -> polardbx | 0 | 1 | 69 | 70 |
| postgres -> mysql | 0 | 0 | 1 | 1 |
| postgres -> postgres | 0 | 0 | 0 | 0 |
| progress -> progress | 5 | 0 | 76 | 81 |
| pubsub -> pubsub | 0 | 0 | 4 | 4 |
| qdrant -> qdrant | 2 | 2 | 36 | 40 |
| redis -> redis | 0 | 0 | 11 | 11 |
| redshift -> redshift | 0 | 0 | 52 | 52 |
| saas -> saas | 0 | 0 | 22 | 22 |
| saphana -> saphana | 5 | 0 | 76 | 81 |
| sapodp -> sapodp | 0 | 0 | 22 | 22 |
| singlestore -> singlestore | 0 | 0 | 75 | 75 |
| snowflake -> snowflake | 0 | 0 | 58 | 58 |
| spanner -> spanner | 0 | 0 | 60 | 60 |
| sqlite -> postgres | 0 | 0 | 43 | 43 |
| sqlite -> sqlite | 0 | 0 | 43 | 43 |
| synapse -> synapse | 0 | 0 | 73 | 73 |
| teradata -> teradata | 0 | 0 | 32 | 32 |
| tidb -> tidb | 0 | 0 | 17 | 17 |
| timestream -> timestream | 1 | 0 | 38 | 39 |
| vertica -> vertica | 5 | 0 | 76 | 81 |
| yugabytedb -> yugabytedb | 0 | 3 | 75 | 78 |

## Backlog (688 items, `docs/backlog-index.md`)

DONE 517 - REVIEW 52 - PARTLY 81 - OPEN 17 - BLOCKED 12 - NOT PURSUED 9

| area | DONE | REVIEW | PARTLY | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED |
|---|---|---|---|---|---|---|---|
| change follow | 60 | 1 | 7 | 0 | 2 | 1 | 0 |
| conversion | 29 | 2 | 2 | 0 | 0 | 0 | 0 |
| correctness | 5 | 0 | 0 | 0 | 0 | 0 | 0 |
| correctness/operations | 1 | 0 | 1 | 0 | 0 | 0 | 0 |
| correctness/performance | 0 | 0 | 1 | 0 | 0 | 0 | 0 |
| cutover | 22 | 1 | 1 | 0 | 0 | 0 | 0 |
| decision engine | 62 | 37 | 1 | 0 | 0 | 0 | 0 |
| ease of use | 36 | 2 | 6 | 0 | 0 | 1 | 1 |
| engines | 47 | 0 | 13 | 0 | 2 | 3 | 1 |
| maintainability | 10 | 0 | 2 | 0 | 2 | 0 | 0 |
| move | 51 | 2 | 14 | 0 | 2 | 0 | 2 |
| operations/scale | 58 | 1 | 13 | 0 | 5 | 5 | 3 |
| operations/verify | 0 | 0 | 1 | 0 | 0 | 0 | 0 |
| schema | 0 | 1 | 0 | 0 | 0 | 0 | 0 |
| security | 12 | 0 | 6 | 0 | 1 | 1 | 1 |
| tools used whole | 42 | 0 | 5 | 0 | 0 | 0 | 0 |
| two-way | 10 | 1 | 2 | 0 | 0 | 0 | 0 |
| types | 13 | 0 | 1 | 0 | 0 | 0 | 0 |
| verify | 59 | 4 | 5 | 0 | 3 | 1 | 1 |

Not done (162): P0-DE.1, P0-DE.2, P0-DE.4, BAR-1, BAR-3, 0f-DVT, 0f-REST, 0a.measure, 0b.window, 6.mariadb, 6.fkwin, 7.free, A2/D10, C1.managed, P3.dryrun, 26.versions, MSSQL-x86, AUD.release, P0-DEPTH.calibrate, P0-DEPTH.json, P0-DEPTH.rank, P0-AUDIT, AUD-E.pin, AUD-G2, AUD-T2, P0-PARITY.small, P0-PARITY.lv.new, DEC1001.media, DEC1001.gui, P0-SCENARIOS.managed, P0-SCENARIOS.arch, P0-SCENARIOS.charset, P0-SCENARIOS.engine, P0-SCENARIOS.consolidate, S1.e, F3, F7.beat, F7.edges, F7.sec, CV.P9, ER1.acct, ER4, ER5, ER6, W5, LT.R1, LT.R8, LT.C1, LT.P1, LT.P2, W1, W2, W4.b, W8, W9, 28.rates, R5.actions, R5.anchor, 45.run, R3.topo, R17b.hmac, R17b.prog, R8.more, R8.mv, R12.minio, R13-DB2, R13-ASE, R13-GENERIC, R16b, R16c, R17d, R18.6/R20.2, R18.7, R19.1, R19.3, R19.9/R20.3, R20.5, R20.6, R20.7, R20.10, R20.11, R21.8, R21.9, R21.M, DTS.sample, REL, REL.gate, R25.3, P0-SCOPE.open, P0-PARITY.scope, P0-PARITY.verify, P0-PARITY.tail, P0-PARITY.all, P0-MINIMAL-INPUT.ownership, P0-MINIMAL-INPUT.acceptance, P0-AUTO-RESOURCES.proof, P0-SAFETY-INTEGRITY, P0-RESILIENCE-ALL, P0-AUTONOMOUS-MIGRATION, P0-PRODUCT-TELEMETRY, P0-AUTOTUNE-ALL, P1-SOURCE-HIERARCHY.layout, P1-SOURCE-HIERARCHY.engines, P1-SOURCE-HIERARCHY.navigation, P1-SOURCE-HIERARCHY.proof, P0-VALIDATE.unattended.proof, P0-ETA-ALL, P0-CONNECTION-PROGRESS, P0-VERIFY-EXECUTION, P0-MINIMAL-INPUT.writeback, SAML.real-idp, TEST.closed-bundle-arm, P0-DECISION-ALL.gaps, SUPER.store-forward, SUPER.ddl-mutation, SUPER.query-guard, AUTO.target-tune, AUTO.deferred-index, AUTO.proxy-shift, AUTO.reverse-failback, AUTO.schema-drift, AUTO.one-command, REV-D1.limit-gate, REV-D2.strategy-guarantees, REV-D4.billed-bytes, SMART-N1.contract, SMART-N2.fact-validity, SMART-N3.context-learning, SMART-N4.information-budget, SMART-N5.live-replan, SMART-N6.complete-path, SMART-N7.proof-planning, SMART-N8.robust-scheduling, SMART-N9.capability-discovery, SMART-N10.closed-loop-proof, SMART-N11.connection-only, SMART-N12.discovery-closure, SMART-N13.action-compiler, SMART-N14.semantic-negotiation, SMART-N15.causal-diagnosis, SMART-N16.continuous-scope, SMART-N17.uncertain-effects, SMART-N18.application-readiness, SMART-N19.time-aware-control, SMART-N20.work-granularity, SMART-N21.progress-liveness, SMART-N22.resource-ownership, SMART-N23.fleet-coordination, SMART-N24.decision-compute, SMART-N25.operation-experiments, RCH01.provider-path, RCH02.lob-policy, RCH03.transaction-dag, RCH04.apply-contract, RCH05.repair-fence, RCH06.online-confirmation, RCH07.keyless-proof, RCH08.effect-checkpoint, RCH09.schema-epoch, RCH40.end-to-end-bar, FIELD-OPS11.no-pk-ctid-page-slicing, FIELD-OPS12.users-grants-parity-orchestrator

