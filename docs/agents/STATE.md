# migkit state

Written by `tools/gen_codemap.py` from `capabilities.matrix()`, `decide.coverage()`, `decide.still_lacking()`, `capabilities.depth()` and `docs/backlog-index.md`; the pre-commit hook keeps it current. What this machine last proved (the full suite, unpushed work) is in `.codemap/LOCAL.md`.

## Capabilities by engine

16 capabilities (`capabilities.CAPABILITIES`) and 12 shapes (`decide.coverage()`); a row lists what is not yet there and counts the depth items owed, as each engine says it still lacks them (`decide.still_lacking()`).

| engine | capabilities | shapes | depth owed | not yet |
|---|---|---|---|---|
| analyticdb-mysql | 16/16 | 12/12 | 74 | - |
| analyticdb-postgres | 13/16 | 10/12 | 74 | params, statistics, snapshot, large values, partitioned |
| ase | 15/16 | 11/12 | 66 | stream, partitioned |
| bigquery | 16/16 | 12/12 | 48 | - |
| bigtable | 16/16 | 11/12 | 40 | large values |
| cassandra | 16/16 | 12/12 | 28 | - |
| clickhouse | 16/16 | 12/12 | 44 | - |
| cockroachdb | 16/16 | 12/12 | 50 | - |
| copybook | 16/16 | 12/12 | 31 | - |
| cosmosdb | 16/16 | 12/12 | 20 | - |
| couchbase | 16/16 | 12/12 | 48 | - |
| datastore | 16/16 | 11/12 | 37 | large values |
| db2 | 16/16 | 12/12 | 66 | - |
| db2i | 16/16 | 12/12 | 69 | - |
| db2zos | 16/16 | 12/12 | 61 | - |
| dcdb | 15/16 | 12/12 | 64 | bulk-move |
| delta | 9/16 | 11/12 | 47 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| dsql | 16/16 | 12/12 | 27 | - |
| duckdb | 16/16 | 12/12 | 46 | - |
| dynamodb | 16/16 | 12/12 | 7 | - |
| firestore | 16/16 | 11/12 | 42 | large values |
| gaussdb | 16/16 | 12/12 | 67 | - |
| geminidb-cassandra | 16/16 | 12/12 | 48 | - |
| geminidb-mongo | 16/16 | 12/12 | 49 | - |
| geminidb-redis | 16/16 | 12/12 | 35 | - |
| generic | 16/16 | 12/12 | 57 | - |
| hetero | 16/16 | 10/12 | 45 | partitioned, missing grant |
| iceberg | 9/16 | 11/12 | 48 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| kafka | 16/16 | 12/12 | 11 | - |
| keyspaces | 16/16 | 12/12 | 49 | - |
| kinesis | 16/16 | 12/12 | 4 | - |
| lindorm | 12/16 | 10/12 | 55 | stream, fence, confirm, delta, large values, partitioned |
| mongodb | 16/16 | 11/12 | 10 | missing grant |
| mssql | 16/16 | 12/12 | 33 | - |
| mysql | 16/16 | 12/12 | 4 | - |
| neo4j | 12/16 | 7/12 | 54 | bulk-move, confirm, users, snapshot, keyless, no unique index, wide key, large values, partitioned |
| neptune | 16/16 | 11/12 | 43 | large values |
| oceanbase | 15/16 | 12/12 | 66 | bulk-move |
| oceanbase-oracle | 11/16 | 12/12 | 69 | bulk-move, stream, fence, confirm, delta |
| opensearch | 16/16 | 12/12 | 25 | - |
| oracle | 16/16 | 12/12 | 39 | - |
| parquet | 16/16 | 12/12 | 32 | - |
| polardbx | 15/16 | 12/12 | 67 | bulk-move |
| postgres | 16/16 | 12/12 | 2 | - |
| pubsub | 16/16 | 12/12 | 4 | - |
| qdrant | 14/16 | 7/12 | 36 | bulk-move, snapshot, keyless, no unique index, wide key, large values, partitioned |
| redis | 16/16 | 12/12 | 9 | - |
| redshift | 16/16 | 12/12 | 52 | - |
| saas | 16/16 | 12/12 | 22 | - |
| sapodp | 16/16 | 12/12 | 22 | - |
| singlestore | 16/16 | 12/12 | 74 | - |
| snowflake | 16/16 | 12/12 | 56 | - |
| spanner | 16/16 | 11/12 | 58 | large values |
| sqlite | 16/16 | 12/12 | 43 | - |
| synapse | 11/16 | 8/12 | 74 | sequences, params, users, statistics, snapshot, large values, partitioned, read-only target, writes held |
| teradata | 16/16 | 12/12 | 32 | - |
| tidb | 16/16 | 12/12 | 15 | - |
| timestream | 16/16 | 11/12 | 38 | large values |
| yugabytedb | 13/16 | 10/12 | 74 | params, statistics, snapshot, large values, partitioned |

## Depth by engine

88 items in 10 areas (`capabilities.depth()`, backlog P0-PARITY), each read off the code an engine runs; a way counts once a test proves it on that engine (`PROVES` in the test module). Cells: built, unproved 1088, n/a 1436, not yet 1405, test owed 300, yes 963. A cell below is the ways proved of the items that apply.

| engine | scope | verify | tail | assess | schema | move | cutover | security | managed | operations | all |
|---|---|---|---|---|---|---|---|---|---|---|---|
| analyticdb-mysql | 0/10 | 0/40 | 0/6 | 0/7 | 0/6 | 0/6 | 0/8 | 0/1 | 0/1 | 0/1 | 0/86 |
| analyticdb-postgres | 0/10 | 0/40 | 0/6 | 0/7 | 0/6 | 0/6 | 0/8 | 0/1 | 0/1 | 0/1 | 0/86 |
| ase | 0/10 | 0/35 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 0/1 | 0/1 | 5/81 |
| bigquery | 5/9 | 1/29 | 0/6 | 1/6 | 1/6 | 1/6 | 1/5 | 1/1 | 1/1 | 0/1 | 12/70 |
| bigtable | 1/9 | 0/18 | 1/5 | 0/6 | 1/3 | 2/5 | 0/3 | 1/1 | 1/1 | 0/1 | 7/52 |
| cassandra | 6/9 | 15/24 | 1/5 | 0/7 | 1/3 | 2/6 | 5/5 | 1/1 | 1/1 | 1/1 | 33/62 |
| clickhouse | 7/9 | 6/30 | 1/6 | 1/7 | 1/5 | 5/7 | 4/6 | 1/1 | 1/1 | 1/1 | 28/73 |
| cockroachdb | 5/10 | 5/35 | 3/6 | 1/7 | 3/6 | 1/6 | 3/7 | 1/1 | 1/1 | 0/1 | 23/80 |
| copybook | 0/7 | 0/16 | - | 0/4 | 1/2 | 2/4 | 0/1 | 1/1 | - | 0/1 | 4/36 |
| cosmosdb | 6/9 | 14/21 | 4/5 | 2/6 | 1/3 | 3/5 | 1/3 | 1/1 | 1/1 | 0/1 | 33/55 |
| couchbase | 0/9 | 1/23 | 1/5 | 1/7 | 1/4 | 1/6 | 2/6 | 1/1 | 0/1 | 0/1 | 8/63 |
| datastore | 0/8 | 0/17 | - | 0/6 | 0/2 | 2/5 | 0/2 | 1/1 | 1/1 | 0/1 | 4/43 |
| db2 | 1/10 | 0/37 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 1/1 | 0/1 | 7/83 |
| db2i | 0/10 | 1/37 | 0/6 | 0/7 | 0/6 | 0/7 | 0/6 | 1/1 | 0/1 | 0/1 | 2/82 |
| db2zos | 0/9 | 0/34 | - | 1/7 | 0/6 | 0/7 | 0/4 | 0/1 | 0/1 | 0/1 | 1/70 |
| dcdb | 0/10 | 2/35 | 1/6 | 0/7 | 1/6 | 0/7 | 3/7 | 1/1 | 1/1 | 0/1 | 9/81 |
| delta | 0/9 | 0/24 | 0/6 | 0/5 | 1/2 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/58 |
| dsql | 5/10 | 21/32 | 1/6 | 3/7 | 5/6 | 3/5 | 2/5 | 1/1 | 1/1 | 0/1 | 42/74 |
| duckdb | 1/9 | 2/24 | - | 0/7 | 1/6 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 11/57 |
| dynamodb | 6/8 | 12/14 | 4/5 | 5/6 | 2/2 | 4/4 | 4/4 | 1/1 | 1/1 | 0/1 | 39/46 |
| firestore | 0/9 | 2/21 | 1/5 | 0/6 | 0/2 | 2/5 | 2/3 | 1/1 | 1/1 | 0/1 | 9/54 |
| gaussdb | 0/10 | 3/39 | 1/6 | 0/7 | 1/6 | 2/7 | 3/7 | 1/1 | 1/1 | 0/1 | 12/85 |
| geminidb-cassandra | 0/9 | 1/22 | 0/1 | 0/7 | 0/4 | 0/5 | 0/5 | 1/1 | 1/1 | 0/1 | 3/56 |
| geminidb-mongo | 0/9 | 1/24 | 0/6 | 0/7 | 0/3 | 0/5 | 0/5 | 1/1 | 1/1 | 0/1 | 3/62 |
| geminidb-redis | 0/8 | 1/12 | 0/6 | 0/7 | 0/1 | 1/3 | 0/5 | 1/1 | 1/1 | 0/1 | 4/45 |
| generic | 9/10 | 10/39 | 1/6 | 1/7 | 0/6 | 3/6 | 0/4 | 0/1 | 0/1 | 0/1 | 24/81 |
| hetero | 4/10 | 6/40 | 1/2 | 2/5 | - | 6/8 | 2/5 | - | - | - | 21/70 |
| iceberg | 0/9 | 0/24 | 0/6 | 0/5 | 1/3 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/59 |
| kafka | 5/9 | 15/17 | 5/5 | 5/7 | 0/1 | 5/6 | 5/5 | 1/1 | 1/1 | 0/1 | 42/53 |
| keyspaces | 0/9 | 0/24 | 1/5 | 0/7 | 1/3 | 1/5 | 0/3 | 1/1 | 1/1 | 0/1 | 5/59 |
| kinesis | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| lindorm | 0/9 | 2/27 | 0/6 | 0/7 | 0/5 | 0/6 | 1/5 | 1/1 | 1/1 | 0/1 | 5/68 |
| mongodb | 8/9 | 21/24 | 4/6 | 6/7 | 1/3 | 4/6 | 4/5 | 1/1 | 1/1 | 1/1 | 51/63 |
| mssql | 9/10 | 21/35 | 3/6 | 0/7 | 4/6 | 4/7 | 5/7 | 0/1 | 1/1 | 0/1 | 47/81 |
| mysql | 9/10 | 29/32 | 6/6 | 7/7 | 6/6 | 7/8 | 8/8 | 1/1 | 1/1 | 1/1 | 75/80 |
| neo4j | 0/9 | 0/25 | 1/5 | 0/7 | 0/4 | 0/7 | 0/4 | 0/1 | 0/1 | 0/1 | 1/64 |
| neptune | 0/8 | 0/20 | 1/5 | 0/7 | 0/3 | 2/4 | 0/3 | 1/1 | 1/1 | 0/1 | 5/53 |
| oceanbase | 0/10 | 1/36 | 0/6 | 0/7 | 1/6 | 1/7 | 0/7 | 1/1 | 1/1 | 0/1 | 5/82 |
| oceanbase-oracle | 0/10 | 1/38 | 0/6 | 0/7 | 0/6 | 0/7 | 2/7 | 1/1 | 1/1 | 0/1 | 5/84 |
| opensearch | 7/9 | 17/25 | 1/6 | 2/7 | 2/4 | 6/6 | 4/6 | 1/1 | 1/1 | 0/1 | 41/66 |
| oracle | 9/10 | 14/37 | 3/6 | 1/7 | 4/6 | 5/7 | 4/7 | 0/1 | 1/1 | 1/1 | 42/83 |
| parquet | 0/7 | 0/16 | - | 0/4 | 1/2 | 3/6 | 1/1 | 1/1 | 0/1 | 0/1 | 6/39 |
| polardbx | 0/10 | 1/36 | 0/6 | 0/7 | 0/6 | 0/7 | 1/7 | 1/1 | 1/1 | 0/1 | 4/82 |
| postgres | 9/10 | 38/38 | 5/6 | 7/7 | 6/6 | 8/8 | 8/8 | 1/1 | 1/1 | 1/1 | 84/86 |
| pubsub | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| qdrant | 0/7 | 1/15 | - | 0/7 | 1/2 | 1/7 | 0/2 | 0/1 | 0/1 | 0/1 | 3/43 |
| redis | 5/8 | 12/12 | 2/6 | 7/7 | 0/1 | 4/4 | 5/6 | 1/1 | 1/1 | 1/1 | 38/47 |
| redshift | 5/10 | 0/29 | 0/5 | 3/7 | 1/5 | 1/3 | 2/6 | 1/1 | 1/1 | 0/1 | 14/68 |
| saas | 0/6 | 0/3 | 0/5 | 1/5 | - | 0/1 | 0/1 | 0/1 | 0/1 | - | 1/23 |
| sapodp | 0/6 | 0/3 | 0/5 | 0/5 | 0/1 | 0/1 | 1/1 | 1/1 | 0/1 | - | 2/24 |
| singlestore | 0/10 | 0/40 | 0/6 | 0/7 | 0/6 | 0/6 | 0/8 | 0/1 | 0/1 | 0/1 | 0/86 |
| snowflake | 4/10 | 0/33 | 0/6 | 3/7 | 1/6 | 0/4 | 2/7 | 1/1 | 0/1 | 0/1 | 11/76 |
| spanner | 0/10 | 2/31 | 1/6 | 0/6 | 1/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 11/72 |
| sqlite | 2/9 | 4/28 | - | 2/7 | 2/5 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 17/60 |
| synapse | 0/10 | 0/37 | 0/5 | 0/7 | 0/6 | 0/6 | 0/8 | 0/1 | 0/1 | 0/1 | 0/82 |
| teradata | 6/8 | 12/30 | - | 2/7 | 3/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 30/64 |
| tidb | 9/10 | 25/32 | 4/6 | 5/7 | 5/5 | 5/7 | 4/6 | 1/1 | 0/1 | 0/1 | 58/76 |
| timestream | 0/7 | 0/19 | - | 0/6 | 0/1 | 1/5 | 1/3 | 1/1 | 1/1 | 0/1 | 4/44 |
| yugabytedb | 0/10 | 0/40 | 0/6 | 0/7 | 0/6 | 0/6 | 0/8 | 0/1 | 0/1 | 0/1 | 0/86 |

Built, unproved - a way the code has on an engine no sandbox here can run; the engine's module says which server it was not run on (`UNPROVED_ON`):

- analyticdb-mysql: built; not run on a real AnalyticDB for MySQL or AnalyticDB for PostgreSQL instance (65 cells)
- analyticdb-postgres: built; not run on a real AnalyticDB for MySQL or AnalyticDB for PostgreSQL instance (55 cells)
- ase: built; not run on a real SAP ASE server (15 cells)
- bigquery: built; not run on a real Redshift cluster, Snowflake account or BigQuery project (48 cells)
- cockroachdb: built; not run on a real CockroachDB cluster, only on its single-node container and on fakes: the accounts, the cluster settings compared, the export and import through a stage, the snapshot's mark and large values read in pieces (44 cells)
- cosmosdb: built; not run on a real Cosmos DB account, only on its emulator and on fakes: the bulk writes and the snapshot's continuation per feed range (10 cells)
- couchbase: built; not run on a real Couchbase cluster, only on its community container and on fakes: the accounts, the bucket settings compared, the statistics refreshed, the bulk importer and the snapshot's mark (15 cells)
- db2: built; not run on a real Db2 LUW server (49 cells)
- db2i: built; not run on a real IBM i (52 cells)
- db2zos: built; not run on a real Db2 for z/OS subsystem (45 cells)
- dcdb: built; not run on a real PolarDB-X or TDSQL instance (54 cells)
- dsql: built; not run on a real Aurora DSQL cluster, only on a PostgreSQL server and on fakes: the statistics refreshed, the snapshot's mark and large values read in pieces (19 cells)
- gaussdb: built; not run on a real GaussDB instance (34 cells)
- geminidb-cassandra: built; not run on a real GeminiDB instance (46 cells)
- geminidb-mongo: built; not run on a real GeminiDB instance (57 cells)
- geminidb-redis: built; not run on a real GeminiDB instance (30 cells)
- kafka: built; not run on a real broker that takes transactional producers (6 cells)
- keyspaces: built; not run on a real Amazon Keyspaces table (45 cells)
- lindorm: built; not run on a real Lindorm instance (14 cells)
- mssql: built; not run on a real SQL Server, only on its edge edition in the sandbox (stopped Agent, CLR off, a thinner catalogue); a cell whose lane test is not written yet is owed that test (27 cells)
- oceanbase: built; not run on a real OceanBase tenant (62 cells)
- oceanbase-oracle: built; not run on a real OceanBase tenant (22 cells)
- polardbx: built; not run on a real PolarDB-X or TDSQL instance (42 cells)
- redshift: built; not run on a real Redshift cluster, Snowflake account or BigQuery project (29 cells)
- singlestore: built; not run on a real SingleStore workspace (65 cells)
- snowflake: built; not run on a real Redshift cluster, Snowflake account or BigQuery project (43 cells)
- synapse: built; not run on a real Synapse dedicated SQL pool (32 cells)
- tidb: built; not run on a real TiDB cluster, only on its single-node container and on fakes: the accounts, the bulk load, the snapshot's mark and TiCDC's open-protocol and Avro messages (7 cells)
- yugabytedb: built; not run on a real YugabyteDB cluster (56 cells)
- alibaba-analyticdb-mysql: built; not run on a real AnalyticDB for MySQL cluster (managed.readiness, managed.brand-api)
- alibaba-analyticdb-postgres: built; not run on a real AnalyticDB for PostgreSQL instance (managed.readiness, managed.brand-api)
- aws: built; not run against a real AWS account (managed.sign-in, managed.forwarder, managed.readiness, managed.brand-api, managed.retention-api, managed.egress-gate)
- azure: built; not run against a real Azure account (managed.sign-in, managed.forwarder, managed.readiness, managed.egress-gate)
- azure-synapse: built; not run on a real Synapse dedicated SQL pool (managed.readiness, managed.brand-api)
- documentdb: built; not run against a real DocumentDB cluster (brand.told, preflight.change-streams, preflight.retention, tail.ddl-snapshot)
- gcp: built; not run against a real Google Cloud account (managed.sign-in, managed.forwarder, managed.readiness, managed.egress-gate)
- msk: built; not run against a real MSK cluster (brand.told, sign-in.iam, sign-in.mechanisms)
- redis-enterprise: built; not run against a real Redis Enterprise (brand.told)
- singlestore-helios: built; not run on a real SingleStore Helios workspace (managed.readiness, managed.brand-api)
- yugabyte-aeon: built; not run on a real YugabyteDB Aeon cluster (managed.readiness, managed.brand-api)

Per item: the engines in each state - "test owed" is a way in the code whose test on that engine is not written (`UNPROVED`); "built, unproved" is one whose engine module declares it was not run on a real server.

| item | yes | test owed | built, unproved | not yet | n/a |
|---|---|---|---|---|---|
| scope.listings | the rest (21) | datastore firestore neo4j neptune qdrant spanner timestream | analyticdb-mysql analyticdb-postgres ase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx singlestore synapse yugabytedb | copybook couchbase db2i db2zos delta duckdb hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.deep | cassandra clickhouse cockroachdb cosmosdb db2 dynamodb generic kafka mongodb mssql mysql opensearch oracle postgres redis teradata tidb | bigtable datastore duckdb firestore hetero qdrant spanner | the rest (22) | copybook couchbase delta iceberg neo4j neptune parquet sqlite | kinesis pubsub saas sapodp timestream |
| scope.copy | the rest (20) | bigtable datastore firestore neo4j neptune qdrant spanner timestream | analyticdb-mysql analyticdb-postgres ase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx singlestore synapse yugabytedb | copybook couchbase db2i db2zos delta duckdb hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.repair | cassandra clickhouse cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis redshift snowflake sqlite teradata tidb | bigtable datastore firestore neo4j neptune qdrant spanner timestream | the rest (22) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| scope.counters | mssql mysql oracle postgres tidb | duckdb | analyticdb-mysql analyticdb-postgres ase cockroachdb db2 db2i db2zos dcdb dsql gaussdb oceanbase oceanbase-oracle polardbx redshift singlestore snowflake yugabytedb | generic hetero spanner sqlite synapse | the rest (31) |
| scope.objects | generic kafka mongodb mssql mysql oracle postgres tidb | cassandra clickhouse duckdb opensearch sqlite teradata | the rest (21) | ase bigtable couchbase datastore delta firestore hetero iceberg lindorm neo4j polardbx spanner timestream | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub qdrant redis saas sapodp |
| scope.ops | the rest (19) | bigtable delta firestore iceberg kinesis neo4j neptune pubsub saas sapodp spanner | analyticdb-mysql analyticdb-postgres couchbase db2 db2i dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase polardbx singlestore synapse yugabytedb | ase kafka lindorm oceanbase-oracle | copybook datastore db2zos duckdb parquet qdrant sqlite teradata timestream |
| scope.row-filter | the rest (17) | bigtable cassandra copybook datastore delta dynamodb firestore iceberg neo4j neptune parquet qdrant spanner timestream | ase cosmosdb couchbase db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces lindorm oceanbase oceanbase-oracle polardbx snowflake yugabytedb | analyticdb-mysql analyticdb-postgres geminidb-redis kafka redis saas sapodp singlestore synapse | kinesis pubsub |
| scope.transforms | generic | - | - | the rest (56) | kinesis pubsub |
| scope.names | bigquery cassandra clickhouse cosmosdb dsql dynamodb generic hetero mongodb mssql mysql opensearch oracle postgres teradata tidb | bigtable copybook datastore delta duckdb firestore iceberg neo4j neptune parquet qdrant spanner sqlite timestream | the rest (22) | geminidb-redis kafka redis saas sapodp | kinesis pubsub |
| verify.value.one-plan | mssql postgres | mysql | analyticdb-mysql dcdb oceanbase polardbx singlestore tidb | the rest (42) | geminidb-redis kinesis neo4j pubsub qdrant redis saas sapodp |
| verify.access.object-ownership | dsql mysql postgres tidb | clickhouse oracle teradata | analyticdb-mysql analyticdb-postgres cockroachdb db2 db2i db2zos dcdb mssql oceanbase redshift singlestore snowflake synapse yugabytedb | ase gaussdb generic hetero lindorm neo4j oceanbase-oracle polardbx spanner | the rest (29) |
| verify.access.sequence-grants | postgres | oracle | analyticdb-postgres cockroachdb dsql mssql snowflake yugabytedb | analyticdb-mysql db2 db2i db2zos dcdb gaussdb generic hetero oceanbase oceanbase-oracle polardbx singlestore spanner synapse | the rest (37) |
| verify.access.table-grants | cassandra kafka mysql opensearch postgres redis tidb | clickhouse mongodb oracle teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb dcdb dsql geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase redshift singlestore snowflake synapse yugabytedb | ase couchbase db2 db2i db2zos gaussdb generic hetero lindorm neo4j oceanbase-oracle polardbx spanner | the rest (18) |
| verify.identity.sequence-collision | mssql mysql oracle postgres tidb | - | analyticdb-mysql analyticdb-postgres cockroachdb db2 db2i db2zos dcdb dsql oceanbase singlestore yugabytedb | ase duckdb gaussdb generic hetero oceanbase-oracle polardbx redshift snowflake spanner sqlite synapse | the rest (31) |
| verify.identity.sequence-parity | mssql mysql oracle postgres tidb | - | analyticdb-mysql analyticdb-postgres cockroachdb db2 db2i db2zos dcdb dsql oceanbase oceanbase-oracle singlestore snowflake yugabytedb | ase duckdb gaussdb generic hetero polardbx redshift spanner sqlite synapse | the rest (31) |
| verify.movement.target-ahead | cassandra dsql kafka mongodb mssql mysql postgres redis tidb | clickhouse opensearch oracle teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase redshift singlestore snowflake synapse yugabytedb | the rest (24) | kinesis pubsub saas sapodp |
| verify.structure.collection-options | cosmosdb dynamodb kafka mongodb opensearch postgres | cassandra clickhouse mysql oracle | analyticdb-mysql bigquery db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces oceanbase redshift singlestore snowflake synapse tidb | the rest (26) | copybook geminidb-redis kinesis parquet pubsub redis saas sapodp |
| verify.structure.columns | cassandra cockroachdb dsql mssql mysql opensearch postgres teradata tidb | clickhouse oracle | analyticdb-mysql analyticdb-postgres bigquery db2 db2i db2zos dcdb geminidb-cassandra keyspaces oceanbase redshift singlestore snowflake synapse yugabytedb | ase copybook delta duckdb gaussdb generic hetero iceberg lindorm oceanbase-oracle parquet polardbx spanner sqlite | the rest (19) |
| verify.structure.deferrable | dsql postgres | oracle | - | analyticdb-mysql analyticdb-postgres gaussdb generic hetero oceanbase-oracle singlestore spanner sqlite synapse yugabytedb | the rest (45) |
| verify.structure.extensions | postgres | - | - | analyticdb-mysql analyticdb-postgres gaussdb generic hetero neo4j singlestore synapse yugabytedb | the rest (49) |
| verify.structure.foreign-keys | dsql generic mssql mysql postgres tidb | oracle teradata | analyticdb-mysql bigquery cockroachdb db2 db2i db2zos dcdb oceanbase redshift singlestore snowflake synapse | analyticdb-postgres ase duckdb gaussdb hetero neptune oceanbase-oracle polardbx spanner sqlite yugabytedb | the rest (28) |
| verify.structure.indexes | cosmosdb dsql dynamodb firestore gaussdb generic lindorm mongodb postgres qdrant teradata tidb | cassandra clickhouse datastore oracle spanner | analyticdb-postgres bigquery cockroachdb db2 db2i db2zos geminidb-cassandra geminidb-mongo keyspaces mssql yugabytedb | the rest (16) | copybook delta geminidb-redis iceberg kafka kinesis mysql opensearch parquet pubsub redis redshift saas sapodp timestream |
| verify.structure.keys | cockroachdb dsql generic mssql mysql postgres redis teradata tidb | clickhouse oracle | analyticdb-mysql analyticdb-postgres bigquery db2 db2i db2zos dcdb geminidb-redis oceanbase redshift singlestore snowflake synapse yugabytedb | ase copybook duckdb gaussdb hetero iceberg oceanbase-oracle polardbx sqlite | the rest (25) |
| verify.structure.materialized-views | postgres | cassandra clickhouse oracle | bigquery cockroachdb db2 db2i db2zos geminidb-cassandra keyspaces redshift snowflake synapse | analyticdb-mysql analyticdb-postgres ase bigtable cosmosdb gaussdb generic hetero iceberg lindorm oceanbase oceanbase-oracle polardbx singlestore yugabytedb | the rest (30) |
| verify.structure.objects | cockroachdb duckdb generic kafka mongodb mssql mysql oracle postgres tidb | cassandra clickhouse opensearch teradata | analyticdb-postgres bigquery cosmosdb db2 db2i db2zos dsql geminidb-cassandra geminidb-mongo keyspaces oceanbase-oracle redshift snowflake synapse yugabytedb | the rest (19) | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub qdrant redis saas sapodp |
| verify.structure.partitions | kafka mysql postgres tidb | clickhouse oracle teradata | analyticdb-mysql bigquery db2 db2i db2zos dcdb gaussdb mssql oceanbase oceanbase-oracle singlestore | analyticdb-postgres ase delta generic hetero iceberg lindorm parquet polardbx synapse timestream yugabytedb | the rest (29) |
| verify.structure.row-security | mssql opensearch postgres | clickhouse oracle | bigquery db2 db2i db2zos redshift snowflake synapse | analyticdb-mysql analyticdb-postgres ase cockroachdb delta firestore gaussdb generic hetero iceberg oceanbase-oracle singlestore teradata yugabytedb | the rest (33) |
| verify.structure.schema-diff | dsql mysql postgres | cassandra | analyticdb-postgres bigquery cockroachdb db2 db2i db2zos gaussdb geminidb-cassandra keyspaces yugabytedb | the rest (28) | cosmosdb couchbase datastore dynamodb firestore geminidb-mongo geminidb-redis kafka kinesis mongodb neo4j neptune pubsub qdrant redis saas sapodp |
| verify.structure.sharding | cosmosdb mongodb | clickhouse opensearch teradata | db2 db2i db2zos geminidb-mongo redshift | analyticdb-mysql analyticdb-postgres dcdb gaussdb generic hetero neo4j oceanbase oceanbase-oracle polardbx qdrant singlestore synapse timestream yugabytedb | the rest (34) |
| verify.structure.triggers | mssql postgres | cassandra mysql oracle teradata | analyticdb-mysql cockroachdb cosmosdb db2 db2i db2zos dcdb geminidb-cassandra keyspaces oceanbase singlestore | analyticdb-postgres ase couchbase gaussdb generic hetero neo4j oceanbase-oracle polardbx sqlite synapse yugabytedb | the rest (30) |
| verify.structure.unvalidated-constraints | dsql mysql oracle postgres tidb | - | analyticdb-mysql cockroachdb db2 db2i db2zos dcdb mssql oceanbase singlestore | analyticdb-postgres ase gaussdb generic hetero neo4j oceanbase-oracle polardbx spanner sqlite synapse teradata yugabytedb | the rest (32) |
| verify.value.charset | cassandra mssql mysql oracle postgres teradata tidb | clickhouse mongodb | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb dsql geminidb-cassandra geminidb-mongo keyspaces oceanbase singlestore yugabytedb | the rest (24) | cosmosdb dynamodb kafka kinesis neo4j opensearch pubsub qdrant redis saas sapodp |
| verify.value.collation | dsql mongodb mysql opensearch postgres teradata tidb | oracle | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-mongo mssql oceanbase singlestore yugabytedb | ase delta duckdb gaussdb generic hetero oceanbase-oracle polardbx redshift snowflake sqlite synapse | the rest (26) |
| verify.value.generated | cosmosdb dsql mysql opensearch postgres teradata tidb | clickhouse oracle | analyticdb-mysql analyticdb-postgres cockroachdb db2 db2i db2zos dcdb mssql oceanbase singlestore yugabytedb | ase delta duckdb gaussdb generic hetero lindorm oceanbase-oracle polardbx redshift snowflake spanner sqlite synapse | the rest (25) |
| verify.value.narrowing | cassandra cockroachdb dsql mongodb mysql opensearch postgres teradata tidb | clickhouse oracle | analyticdb-mysql analyticdb-postgres bigquery db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase singlestore yugabytedb | the rest (25) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.null-empty | cassandra cosmosdb dsql dynamodb generic kafka mongodb mysql opensearch postgres tidb | clickhouse oracle teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase singlestore yugabytedb | the rest (24) | geminidb-redis kinesis pubsub redis saas sapodp |
| verify.value.precision | cassandra dsql mongodb mysql opensearch postgres tidb | clickhouse oracle teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase singlestore yugabytedb | the rest (25) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.rendering | dsql mongodb mssql mysql postgres | clickhouse oracle | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-mongo oceanbase singlestore yugabytedb | the rest (28) | bigtable cassandra cosmosdb dynamodb geminidb-redis kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.timezone | cassandra dsql mongodb mysql postgres teradata tidb | clickhouse opensearch oracle | analyticdb-mysql analyticdb-postgres bigquery cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase singlestore yugabytedb | the rest (24) | bigtable cosmosdb dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.type-drift | cosmosdb dynamodb mongodb opensearch | - | geminidb-mongo kafka | analyticdb-mysql analyticdb-postgres bigtable couchbase datastore firestore geminidb-redis generic hetero neo4j neptune qdrant singlestore sqlite synapse timestream yugabytedb | the rest (36) |
| verify.delta | cassandra clickhouse cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | bigtable firestore neo4j neptune spanner | the rest (19) | delta iceberg lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| verify.fence | the rest (22) | bigtable neo4j neptune | analyticdb-mysql analyticdb-postgres ase bigquery db2 db2i geminidb-mongo keyspaces oceanbase polardbx singlestore snowflake yugabytedb | delta iceberg lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| verify.confirm | cassandra cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | bigtable clickhouse firestore neptune spanner | the rest (18) | delta iceberg lindorm neo4j oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| verify.users | the rest (23) | - | analyticdb-mysql analyticdb-postgres ase bigquery cockroachdb couchbase db2 db2zos redshift singlestore snowflake tidb yugabytedb | neo4j synapse | bigtable copybook cosmosdb datastore delta dsql duckdb dynamodb firestore generic iceberg keyspaces kinesis neptune parquet pubsub qdrant saas sapodp sqlite timestream |
| verify.repair | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | bigtable cassandra clickhouse datastore firestore neo4j neptune opensearch qdrant spanner timestream | the rest (23) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.repair-undo | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | bigtable cassandra clickhouse datastore firestore neo4j neptune opensearch qdrant spanner timestream | the rest (23) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.rows-apart | cassandra clickhouse cosmosdb dsql dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | - | analyticdb-postgres cockroachdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces yugabytedb | the rest (35) | kinesis pubsub |
| verify.tables | cassandra clickhouse cosmosdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis | - | geminidb-cassandra geminidb-mongo geminidb-redis keyspaces redshift snowflake synapse | the rest (38) | kinesis pubsub |
| verify.table | bigquery cassandra clickhouse cosmosdb dsql duckdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite teradata tidb | bigtable copybook datastore delta firestore hetero iceberg neo4j neptune parquet qdrant saas sapodp spanner timestream | the rest (23) | generic | kinesis pubsub |
| tail.follows | the rest (26) | - | analyticdb-mysql analyticdb-postgres bigquery db2 db2i geminidb-mongo geminidb-redis oceanbase polardbx singlestore snowflake yugabytedb | ase delta iceberg lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant redshift saas sapodp sqlite synapse teradata timestream |
| tail.table-states | cockroachdb cosmosdb dynamodb kafka mongodb mysql postgres redis tidb | bigtable firestore hetero neo4j neptune oracle saas sapodp spanner | the rest (19) | ase cassandra clickhouse delta generic iceberg lindorm oceanbase-oracle opensearch redshift synapse | copybook datastore db2zos duckdb kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.added | cockroachdb kafka mssql mysql oracle postgres tidb | mongodb spanner | analyticdb-mysql bigquery geminidb-mongo oceanbase polardbx singlestore snowflake | the rest (30) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.readded | cosmosdb dynamodb kafka mssql mysql oracle tidb | mongodb | analyticdb-mysql bigquery geminidb-mongo oceanbase polardbx singlestore snowflake | the rest (31) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.ddl | cosmosdb dynamodb kafka mongodb mysql postgres | oracle | analyticdb-mysql geminidb-mongo oceanbase polardbx singlestore | the rest (34) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.renames | mongodb mysql postgres | oracle | analyticdb-mysql bigquery geminidb-mongo mssql oceanbase polardbx singlestore snowflake | the rest (24) | bigtable cassandra copybook cosmosdb couchbase datastore db2zos duckdb dynamodb firestore geminidb-cassandra hetero kafka keyspaces kinesis neo4j neptune parquet pubsub qdrant sqlite teradata timestream |
| assess.versions | dsql kafka mongodb mysql opensearch postgres redis redshift snowflake sqlite teradata tidb | cassandra clickhouse oracle | analyticdb-postgres cockroachdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql synapse yugabytedb | the rest (19) | bigquery bigtable copybook cosmosdb datastore delta dynamodb firestore hetero iceberg kinesis parquet pubsub spanner timestream |
| assess.brands | kafka mongodb mysql opensearch postgres redis redshift snowflake tidb | cassandra clickhouse oracle teradata | analyticdb-mysql analyticdb-postgres cockroachdb dcdb dsql gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase polardbx singlestore synapse yugabytedb | the rest (23) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.engine-checks | the rest (21) | bigtable cassandra clickhouse datastore firestore neo4j neptune opensearch qdrant sapodp spanner timestream | analyticdb-mysql analyticdb-postgres db2i dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm mssql oceanbase oceanbase-oracle polardbx singlestore synapse yugabytedb | ase copybook db2 delta duckdb iceberg kinesis parquet pubsub | - |
| assess.preflight | cosmosdb dsql dynamodb kafka mongodb mysql postgres redis tidb | cassandra clickhouse opensearch oracle teradata | the rest (21) | ase bigtable copybook couchbase datastore delta duckdb firestore generic hetero iceberg lindorm neo4j neptune oceanbase-oracle parquet qdrant spanner sqlite timestream | kinesis pubsub saas sapodp |
| assess.capacity | clickhouse dynamodb hetero mongodb mysql postgres redis tidb | cassandra opensearch oracle teradata | analyticdb-mysql bigquery cosmosdb dcdb geminidb-cassandra geminidb-mongo geminidb-redis kafka keyspaces mssql oceanbase polardbx redshift singlestore snowflake | the rest (30) | saas sapodp |
| assess.writers | dynamodb mysql postgres redis | clickhouse mongodb opensearch oracle teradata | bigquery cosmosdb geminidb-mongo kafka mssql | the rest (43) | kinesis pubsub |
| assess.handwork | dynamodb kafka mongodb mysql postgres redis | cassandra clickhouse opensearch oracle teradata | analyticdb-postgres bigquery cockroachdb cosmosdb dsql gaussdb geminidb-mongo geminidb-redis mssql redshift snowflake synapse tidb yugabytedb | the rest (30) | copybook kinesis parquet pubsub |
| schema.create | the rest (32) | datastore firestore neo4j neptune timestream | analyticdb-mysql analyticdb-postgres db2i db2zos geminidb-cassandra kafka lindorm oceanbase-oracle polardbx singlestore synapse yugabytedb | generic | geminidb-mongo geminidb-redis hetero kinesis mongodb pubsub redis saas sapodp |
| schema.indexes | cockroachdb dsql dynamodb mongodb mysql postgres teradata tidb | cassandra clickhouse oracle spanner sqlite | analyticdb-mysql analyticdb-postgres cosmosdb dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase polardbx singlestore yugabytedb | the rest (18) | copybook delta geminidb-redis hetero iceberg kafka kinesis opensearch parquet pubsub redis redshift saas sapodp timestream |
| schema.foreign-keys | dsql mssql mysql oracle postgres tidb | neo4j neptune spanner teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb dcdb gaussdb oceanbase polardbx redshift singlestore snowflake yugabytedb | ase db2 db2i db2zos duckdb generic oceanbase-oracle sqlite synapse | the rest (28) |
| schema.defaults | cockroachdb dsql mysql opensearch postgres sqlite teradata tidb | clickhouse oracle sapodp spanner | analyticdb-mysql analyticdb-postgres bigquery dcdb gaussdb mssql oceanbase polardbx redshift singlestore snowflake synapse yugabytedb | ase copybook db2 db2i db2zos delta duckdb geminidb-cassandra generic iceberg keyspaces lindorm oceanbase-oracle parquet | the rest (20) |
| schema.views | dsql mssql mysql oracle postgres tidb | clickhouse mongodb teradata | analyticdb-mysql analyticdb-postgres bigquery cockroachdb dcdb gaussdb geminidb-mongo oceanbase polardbx redshift singlestore snowflake synapse yugabytedb | ase bigtable couchbase db2 db2i db2zos duckdb generic iceberg lindorm oceanbase-oracle opensearch spanner sqlite | the rest (22) |
| schema.routines | mssql mysql oracle postgres | mongodb | analyticdb-mysql bigquery cockroachdb dcdb geminidb-mongo oceanbase polardbx singlestore | the rest (26) | bigtable copybook datastore delta dynamodb firestore hetero iceberg kafka keyspaces kinesis neptune parquet pubsub qdrant saas sapodp sqlite tidb timestream |
| move.table-copy | the rest (38) | neo4j timestream | analyticdb-mysql analyticdb-postgres bigquery db2i db2zos dcdb geminidb-cassandra geminidb-mongo lindorm oceanbase-oracle polardbx singlestore snowflake synapse yugabytedb | - | kinesis pubsub saas sapodp |
| move.bulk | the rest (22) | hetero | ase bigquery cockroachdb cosmosdb couchbase db2 db2i db2zos geminidb-cassandra geminidb-mongo keyspaces tidb | dcdb delta iceberg neo4j oceanbase oceanbase-oracle polardbx qdrant | analyticdb-mysql analyticdb-postgres dsql geminidb-redis generic kinesis lindorm pubsub redshift saas sapodp singlestore snowflake synapse teradata yugabytedb |
| move.large-values | the rest (19) | teradata | analyticdb-mysql bigquery cockroachdb couchbase db2i db2zos dcdb dsql gaussdb geminidb-mongo geminidb-redis oceanbase oceanbase-oracle polardbx singlestore snowflake | analyticdb-postgres bigtable datastore firestore lindorm neo4j neptune qdrant spanner synapse timestream yugabytedb | cassandra copybook cosmosdb dynamodb geminidb-cassandra keyspaces kinesis pubsub redshift saas sapodp |
| move.partitioned | clickhouse generic kafka mysql postgres tidb | oracle parquet teradata | analyticdb-mysql bigquery db2 db2i db2zos dcdb gaussdb mssql oceanbase oceanbase-oracle polardbx singlestore | analyticdb-postgres ase delta hetero iceberg lindorm neo4j qdrant synapse yugabytedb | the rest (28) |
| move.spread | hetero postgres | cassandra mongodb mysql | - | - | the rest (54) |
| move.sized | ase bigquery clickhouse cosmosdb db2 dsql duckdb dynamodb hetero kafka mongodb mssql mysql opensearch postgres redis sqlite tidb | cassandra oracle teradata | the rest (20) | bigtable copybook couchbase datastore delta firestore generic iceberg neo4j neptune parquet qdrant saas sapodp spanner timestream | kinesis pubsub |
| move.load-window | hetero mysql opensearch oracle postgres | cassandra clickhouse mongodb teradata | analyticdb-mysql dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase polardbx singlestore | the rest (23) | bigquery bigtable copybook delta dynamodb geminidb-redis iceberg kafka kinesis neptune parquet pubsub redis redshift saas sapodp snowflake timestream |
| move.create-missing | cosmosdb dsql dynamodb hetero mysql opensearch oracle postgres teradata tidb | cassandra clickhouse | analyticdb-mysql analyticdb-postgres bigquery cockroachdb dcdb gaussdb geminidb-cassandra kafka keyspaces mssql oceanbase polardbx redshift singlestore snowflake synapse yugabytedb | the rest (19) | datastore firestore geminidb-mongo geminidb-redis kinesis mongodb neptune pubsub redis saas sapodp |
| cutover.freeze | cassandra clickhouse cockroachdb kafka mongodb mssql mysql opensearch postgres redis redshift snowflake teradata tidb | hetero oracle | analyticdb-mysql analyticdb-postgres ase couchbase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis oceanbase oceanbase-oracle polardbx singlestore yugabytedb | synapse | the rest (27) |
| cutover.read-only-target | the rest (22) | hetero | analyticdb-mysql analyticdb-postgres db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis oceanbase oceanbase-oracle polardbx singlestore yugabytedb | synapse | bigtable copybook cosmosdb datastore delta duckdb firestore generic iceberg keyspaces kinesis neo4j neptune parquet pubsub qdrant saas sapodp spanner sqlite timestream |
| cutover.counters | dcdb duckdb gaussdb hetero mssql mysql oceanbase-oracle oracle polardbx postgres spanner sqlite tidb | - | analyticdb-mysql analyticdb-postgres ase cockroachdb db2 db2i db2zos dsql oceanbase redshift singlestore snowflake yugabytedb | synapse | the rest (32) |
| cutover.rollback | the rest (20) | bigtable copybook datastore mongodb neptune | analyticdb-mysql ase bigquery cockroachdb cosmosdb couchbase db2 db2i db2zos dsql geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase polardbx singlestore snowflake tidb | analyticdb-postgres delta iceberg neo4j qdrant synapse yugabytedb | generic kinesis pubsub redshift saas sapodp teradata |
| cutover.drain | the rest (21) | bigtable neptune saas | analyticdb-mysql bigquery db2 db2i geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase polardbx singlestore snowflake | analyticdb-postgres ase delta generic hetero iceberg lindorm neo4j oceanbase-oracle redshift synapse yugabytedb | copybook datastore db2zos duckdb kinesis parquet pubsub qdrant sqlite teradata timestream |
| cutover.pause-replication | mysql postgres | - | - | analyticdb-mysql analyticdb-postgres generic neo4j qdrant redis singlestore synapse yugabytedb | the rest (48) |
| cutover.two-way | cassandra dynamodb kafka mongodb mysql postgres redis teradata | - | geminidb-cassandra geminidb-mongo geminidb-redis keyspaces | the rest (36) | copybook db2zos duckdb hetero kinesis parquet pubsub qdrant saas sapodp sqlite |
| cutover.jobs | mysql postgres | clickhouse oracle | mssql | analyticdb-mysql analyticdb-postgres ase bigquery cockroachdb couchbase db2 db2i db2zos dcdb gaussdb generic lindorm oceanbase oceanbase-oracle opensearch polardbx redshift singlestore snowflake synapse teradata timestream yugabytedb | the rest (30) |
| security.tls | the rest (44) | oracle | analyticdb-mysql analyticdb-postgres singlestore yugabytedb | ase db2 db2zos generic mssql neo4j qdrant saas synapse | hetero |
| managed.catalogued | the rest (36) | - | - | analyticdb-mysql analyticdb-postgres ase couchbase db2i db2zos delta duckdb generic iceberg neo4j parquet qdrant saas sapodp singlestore snowflake sqlite synapse tidb yugabytedb | copybook hetero |
| operations.fencing | cassandra clickhouse mongodb mysql oracle postgres redis | - | analyticdb-mysql analyticdb-postgres cockroachdb dcdb dsql gaussdb geminidb-cassandra geminidb-mongo geminidb-redis kafka keyspaces oceanbase polardbx singlestore tidb yugabytedb | the rest (33) | hetero saas sapodp |

## Source layout

384 upward imports still listed (`docs/agents/LAYERS.md`; the list only shrinks); 0 files no package claims (`docs/agents/INVENTORY.md`).

Gaps per pair (`autonomous.gaps`: a pair lacks what its two engines lack): 3422 ordered pairs of 59 engines; 32 shape lacks, 44 capability lacks, 2510 depth lacks in all; fewest kinesis -> postgres (6), most yugabytedb -> synapse (162).

## Backlog (506 items, `docs/backlog-index.md`)

DONE 357 - PARTLY 85 - IN PROGRESS 4 - OPEN 32 - BLOCKED 19 - NOT PURSUED 9

| area | DONE | PARTLY | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED |
|---|---|---|---|---|---|---|
| change follow | 45 | 7 | 0 | 6 | 5 | 0 |
| conversion | 29 | 2 | 0 | 0 | 0 | 0 |
| correctness | 3 | 0 | 0 | 0 | 0 | 0 |
| correctness/operations | 1 | 1 | 0 | 0 | 0 | 0 |
| correctness/performance | 0 | 1 | 0 | 0 | 0 | 0 |
| cutover | 18 | 2 | 0 | 0 | 0 | 0 |
| ease of use | 34 | 6 | 0 | 0 | 1 | 1 |
| engines | 32 | 14 | 1 | 4 | 5 | 1 |
| maintainability | 1 | 2 | 0 | 2 | 0 | 0 |
| move | 31 | 15 | 2 | 4 | 0 | 2 |
| operations/scale | 43 | 15 | 0 | 8 | 5 | 3 |
| operations/verify | 0 | 1 | 0 | 0 | 0 | 0 |
| security | 10 | 6 | 0 | 0 | 2 | 1 |
| tools used whole | 40 | 5 | 0 | 2 | 0 | 0 |
| two-way | 6 | 2 | 0 | 1 | 0 | 0 |
| types | 12 | 1 | 1 | 0 | 0 | 0 |
| verify | 52 | 5 | 0 | 5 | 1 | 1 |

Not done (140): P0-DE.1, P0-DE.2, P0-DE.4, BAR-1, BAR-3, 0f-DVT, 0f-REST, 0a.measure, 0b.window, 6.mariadb, 6.fkwin, 7.free, R9.docdb, A2/D10, C1.managed, P3.dryrun, R6.cloud, 26.versions, MSSQL-x86, AUD.release, P0-DEPTH.calibrate, P0-DEPTH.json, P0-DEPTH.triggers, P0-DEPTH.rank, P0-AUDIT, AUD-C, AUD-E.pin, AUD-G2, AUD-H, AUD-T2, AUD-Z, P0-PARITY.small, P0-PARITY.lv.new, P0-CLOUDS.native, P0-CLOUDS.more, DEC1001.media, DEC1001.gui, P0-SCENARIOS.brands, P0-SCENARIOS.managed, P0-SCENARIOS.arch, P0-SCENARIOS.charset, P0-SCENARIOS.engine, P0-SCENARIOS.consolidate, DEC30.sweep, DEC.deps, S1.e, S4, F2.freeze, F3, F7.beat, F7.edges, F7.sec, CV.P9, ER1.acct, ER4, ER5, ER6, W5, LT.R1, LT.R2, LT.R3, LT.R6, LT.R8, LT.R9, LT.C1, LT.C4, LT.C5, LT.C6, LT.P1, LT.P2, LT.P3, W1, W2, W4.a, W4.b, W6, W7, W8, W9, 28.rates, R5.actions, R5.anchor, 45.run, R4.msk, R3.topo, 38.real, R17b.hmac, R17b.prog, DOCS, R8.more, R8.mv, R10.pieces, R12.minio, R13-DB2, R13-ASE, R13-HETERO, R13-GENERIC, R16b, R16c, R17d, R18.6/R20.2, R18.7, R19.1, R19.3, R19.9/R20.3, R20.5, R20.6, R20.7, R20.10, R20.11, R21.8, R21.9, R21.M, DTS.sample, REL, REL.gate, R25.2, R25.3, P0-SCOPE.open, P0-PARITY.scope, P0-PARITY.verify, P0-PARITY.tail, P0-PARITY.all, P0-MINIMAL-INPUT.ownership, P0-MINIMAL-INPUT.acceptance, P0-AUTO-RESOURCES.proof, P0-DECISION-ALL.engines, P0-SAFETY-INTEGRITY, P0-RESILIENCE-ALL, P0-AUTONOMOUS-MIGRATION, P0-PRODUCT-TELEMETRY, P0-AUTOTUNE-ALL, P1-SOURCE-HIERARCHY.layout, P1-SOURCE-HIERARCHY.engines, P1-SOURCE-HIERARCHY.navigation, P1-SOURCE-HIERARCHY.proof, P0-VALIDATE.unattended.proof, P0-ETA-ALL, P0-CONNECTION-PROGRESS, P0-VERIFY-EXECUTION

