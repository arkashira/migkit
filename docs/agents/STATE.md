# migkit state

Written by `tools/gen_codemap.py` from `capabilities.matrix()`, `decide.coverage()`, `decide.still_lacking()`, `capabilities.depth()` and `docs/backlog-index.md`; the pre-commit hook keeps it current. What this machine last proved (the full suite, unpushed work) is in `.codemap/LOCAL.md`.

## Capabilities by engine

16 capabilities (`capabilities.CAPABILITIES`) and 12 shapes (`decide.coverage()`); a row lists what is not yet there and counts the depth items owed, as each engine says it still lacks them (`decide.still_lacking()`).

| engine | capabilities | shapes | depth owed | not yet |
|---|---|---|---|---|
| ase | 15/16 | 11/12 | 66 | stream, partitioned |
| bigquery | 15/16 | 12/12 | 48 | users |
| bigtable | 16/16 | 11/12 | 40 | large values |
| cassandra | 16/16 | 12/12 | 29 | - |
| clickhouse | 16/16 | 12/12 | 45 | - |
| cockroachdb | 12/16 | 11/12 | 50 | params, bulk-move, users, snapshot, large values |
| copybook | 16/16 | 12/12 | 31 | - |
| cosmosdb | 14/16 | 12/12 | 20 | bulk-move, snapshot |
| couchbase | 11/16 | 12/12 | 48 | params, bulk-move, users, statistics, snapshot |
| datastore | 16/16 | 11/12 | 37 | large values |
| db2 | 16/16 | 12/12 | 66 | - |
| db2i | 16/16 | 12/12 | 69 | - |
| db2zos | 16/16 | 12/12 | 61 | - |
| dcdb | 15/16 | 12/12 | 64 | bulk-move |
| delta | 9/16 | 11/12 | 47 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| dsql | 13/16 | 11/12 | 27 | users, statistics, snapshot, large values |
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
| mongodb | 16/16 | 11/12 | 11 | missing grant |
| mssql | 16/16 | 12/12 | 33 | - |
| mysql | 16/16 | 12/12 | 4 | - |
| neo4j | 12/16 | 7/12 | 54 | bulk-move, confirm, users, snapshot, keyless, no unique index, wide key, large values, partitioned |
| neptune | 16/16 | 11/12 | 43 | large values |
| oceanbase | 15/16 | 12/12 | 66 | bulk-move |
| oceanbase-oracle | 11/16 | 12/12 | 69 | bulk-move, stream, fence, confirm, delta |
| opensearch | 16/16 | 12/12 | 25 | - |
| oracle | 16/16 | 12/12 | 40 | - |
| parquet | 16/16 | 12/12 | 32 | - |
| polardbx | 15/16 | 12/12 | 67 | bulk-move |
| postgres | 16/16 | 12/12 | 2 | - |
| pubsub | 16/16 | 12/12 | 4 | - |
| qdrant | 14/16 | 7/12 | 36 | bulk-move, snapshot, keyless, no unique index, wide key, large values, partitioned |
| redis | 16/16 | 12/12 | 10 | - |
| redshift | 6/16 | 12/12 | 52 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| saas | 16/16 | 12/12 | 22 | - |
| sapodp | 16/16 | 12/12 | 22 | - |
| snowflake | 6/16 | 12/12 | 56 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| spanner | 16/16 | 11/12 | 58 | large values |
| sqlite | 16/16 | 12/12 | 43 | - |
| teradata | 16/16 | 12/12 | 32 | - |
| tidb | 13/16 | 12/12 | 15 | bulk-move, users, snapshot |
| timestream | 16/16 | 11/12 | 38 | large values |

## Depth by engine

88 items in 10 areas (`capabilities.depth()`, backlog P0-PARITY), each read off the code an engine runs; a way counts once a test proves it on that engine (`PROVES` in the test module). Cells: built, unproved 590, n/a 1414, not yet 1301, test owed 489, yes 958. A cell below is the ways proved of the items that apply.

| engine | scope | verify | tail | assess | schema | move | cutover | security | managed | operations | all |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ase | 0/10 | 0/35 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 0/1 | 0/1 | 5/81 |
| bigquery | 5/9 | 1/29 | 0/6 | 1/6 | 1/6 | 1/6 | 1/5 | 1/1 | 1/1 | 0/1 | 12/70 |
| bigtable | 1/9 | 0/18 | 1/5 | 0/6 | 1/3 | 2/5 | 0/3 | 1/1 | 1/1 | 0/1 | 7/52 |
| cassandra | 6/9 | 15/24 | 1/5 | 0/7 | 1/3 | 2/6 | 5/5 | 1/1 | 1/1 | 0/1 | 32/62 |
| clickhouse | 7/9 | 6/30 | 1/6 | 1/7 | 1/5 | 5/7 | 4/6 | 1/1 | 1/1 | 0/1 | 27/73 |
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
| dsql | 5/10 | 21/33 | 1/6 | 3/7 | 5/6 | 3/5 | 2/5 | 1/1 | 1/1 | 0/1 | 42/75 |
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
| mongodb | 8/9 | 21/24 | 4/6 | 6/7 | 1/3 | 4/6 | 4/5 | 1/1 | 1/1 | 0/1 | 50/63 |
| mssql | 9/10 | 21/35 | 3/6 | 0/7 | 4/6 | 4/7 | 5/7 | 0/1 | 1/1 | 0/1 | 47/81 |
| mysql | 9/10 | 29/32 | 6/6 | 7/7 | 6/6 | 7/8 | 8/8 | 1/1 | 1/1 | 1/1 | 75/80 |
| neo4j | 0/9 | 0/25 | 1/5 | 0/7 | 0/4 | 0/7 | 0/4 | 0/1 | 0/1 | 0/1 | 1/64 |
| neptune | 0/8 | 0/20 | 1/5 | 0/7 | 0/3 | 2/4 | 0/3 | 1/1 | 1/1 | 0/1 | 5/53 |
| oceanbase | 0/10 | 1/36 | 0/6 | 0/7 | 1/6 | 1/7 | 0/7 | 1/1 | 1/1 | 0/1 | 5/82 |
| oceanbase-oracle | 0/10 | 1/38 | 0/6 | 0/7 | 0/6 | 0/7 | 2/7 | 1/1 | 1/1 | 0/1 | 5/84 |
| opensearch | 7/9 | 17/25 | 1/6 | 2/7 | 2/4 | 6/6 | 4/6 | 1/1 | 1/1 | 0/1 | 41/66 |
| oracle | 9/10 | 14/37 | 3/6 | 1/7 | 4/6 | 5/7 | 4/7 | 0/1 | 1/1 | 0/1 | 41/83 |
| parquet | 0/7 | 0/16 | - | 0/4 | 1/2 | 3/6 | 1/1 | 1/1 | 0/1 | 0/1 | 6/39 |
| polardbx | 0/10 | 1/36 | 0/6 | 0/7 | 0/6 | 0/7 | 1/7 | 1/1 | 1/1 | 0/1 | 4/82 |
| postgres | 9/10 | 38/38 | 5/6 | 7/7 | 6/6 | 8/8 | 8/8 | 1/1 | 1/1 | 1/1 | 84/86 |
| pubsub | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| qdrant | 0/7 | 1/15 | - | 0/7 | 1/2 | 1/7 | 0/2 | 0/1 | 0/1 | 0/1 | 3/43 |
| redis | 5/8 | 12/12 | 2/6 | 7/7 | 0/1 | 4/4 | 5/6 | 1/1 | 1/1 | 0/1 | 37/47 |
| redshift | 5/10 | 0/32 | 0/6 | 3/7 | 1/5 | 1/4 | 2/7 | 1/1 | 1/1 | 0/1 | 14/74 |
| saas | 0/6 | 0/3 | 0/5 | 1/5 | - | 0/1 | 0/1 | 0/1 | 0/1 | - | 1/23 |
| sapodp | 0/6 | 0/3 | 0/5 | 0/5 | 0/1 | 0/1 | 1/1 | 1/1 | 0/1 | - | 2/24 |
| snowflake | 4/10 | 0/33 | 0/6 | 3/7 | 1/6 | 0/5 | 2/7 | 1/1 | 0/1 | 0/1 | 11/77 |
| spanner | 0/10 | 2/31 | 1/6 | 0/6 | 1/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 11/72 |
| sqlite | 2/9 | 4/28 | - | 2/7 | 2/5 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 17/60 |
| teradata | 6/8 | 12/30 | - | 2/7 | 3/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 30/64 |
| tidb | 9/10 | 25/32 | 4/6 | 5/7 | 5/5 | 5/7 | 4/6 | 1/1 | 0/1 | 0/1 | 58/76 |
| timestream | 0/7 | 0/19 | - | 0/6 | 0/1 | 1/5 | 1/3 | 1/1 | 1/1 | 0/1 | 4/44 |

Built, unproved - a way the code has on an engine no sandbox here can run; the engine's module says which server it was not run on (`UNPROVED_ON`):

- ase: built; not run on a real SAP ASE server (15 cells)
- db2: built; not run on a real Db2 LUW server (49 cells)
- db2i: built; not run on a real IBM i (52 cells)
- db2zos: built; not run on a real Db2 for z/OS subsystem (45 cells)
- dcdb: built; not run on a real PolarDB-X or TDSQL instance (54 cells)
- gaussdb: built; not run on a real GaussDB instance (34 cells)
- geminidb-cassandra: built; not run on a real GeminiDB instance (45 cells)
- geminidb-mongo: built; not run on a real GeminiDB instance (56 cells)
- geminidb-redis: built; not run on a real GeminiDB instance (29 cells)
- keyspaces: built; not run on a real Amazon Keyspaces table (44 cells)
- lindorm: built; not run on a real Lindorm instance (14 cells)
- mssql: built; not run on a real SQL Server, only on its edge edition in the sandbox (stopped Agent, CLR off, a thinner catalogue); a cell whose lane test is not written yet is owed that test (27 cells)
- oceanbase: built; not run on a real OceanBase tenant (62 cells)
- oceanbase-oracle: built; not run on a real OceanBase tenant (22 cells)
- polardbx: built; not run on a real PolarDB-X or TDSQL instance (42 cells)
- aws: built; not run against a real AWS account (managed.sign-in, managed.forwarder, managed.readiness, managed.brand-api, managed.retention-api, managed.egress-gate)
- azure: built; not run against a real Azure account (managed.sign-in, managed.forwarder, managed.readiness, managed.egress-gate)
- documentdb: built; not run against a real DocumentDB cluster (brand.told, preflight.change-streams, preflight.retention, tail.ddl-snapshot)
- gcp: built; not run against a real Google Cloud account (managed.sign-in, managed.forwarder, managed.readiness, managed.egress-gate)
- msk: built; not run against a real MSK cluster (brand.told, sign-in.iam, sign-in.mechanisms)
- redis-enterprise: built; not run against a real Redis Enterprise (brand.told)

Per item: the engines in each state - "test owed" is a way in the code whose test on that engine is not written (`UNPROVED`); "built, unproved" is one whose engine module declares it was not run on a real server.

| item | yes | test owed | built, unproved | not yet | n/a |
|---|---|---|---|---|---|
| scope.listings | the rest (21) | datastore firestore neo4j neptune qdrant spanner timestream | ase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx | copybook couchbase db2i db2zos delta duckdb hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.deep | the rest (17) | bigquery bigtable datastore dsql duckdb firestore hetero qdrant redshift snowflake spanner | ase db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx | copybook couchbase delta iceberg neo4j neptune parquet sqlite | kinesis pubsub saas sapodp timestream |
| scope.copy | the rest (20) | bigtable datastore firestore neo4j neptune qdrant spanner timestream | ase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx | copybook couchbase db2i db2zos delta duckdb hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.repair | the rest (19) | bigquery bigtable cockroachdb couchbase datastore dsql firestore neo4j neptune qdrant spanner timestream | db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| scope.counters | mssql mysql oracle postgres tidb | cockroachdb dsql duckdb | ase db2 db2i db2zos dcdb gaussdb oceanbase oceanbase-oracle polardbx | generic hetero redshift snowflake spanner sqlite | the rest (31) |
| scope.objects | generic kafka mongodb mssql mysql oracle postgres tidb | bigquery cassandra clickhouse cockroachdb cosmosdb dsql duckdb opensearch redshift snowflake sqlite teradata | db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces oceanbase oceanbase-oracle | the rest (13) | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub qdrant redis saas sapodp |
| scope.ops | the rest (19) | bigtable couchbase delta firestore iceberg kinesis neo4j neptune pubsub saas sapodp spanner | db2 db2i dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase polardbx | ase kafka lindorm oceanbase-oracle | copybook datastore db2zos duckdb parquet qdrant sqlite teradata timestream |
| scope.row-filter | the rest (17) | bigtable cassandra copybook cosmosdb couchbase datastore delta dynamodb firestore iceberg neo4j neptune parquet qdrant snowflake spanner timestream | ase db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces lindorm oceanbase oceanbase-oracle polardbx | geminidb-redis kafka redis saas sapodp | kinesis pubsub |
| scope.transforms | generic | - | - | the rest (51) | kinesis pubsub |
| scope.names | bigquery cassandra clickhouse cosmosdb dsql dynamodb generic hetero mongodb mssql mysql opensearch oracle postgres teradata tidb | the rest (18) | ase db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces lindorm oceanbase oceanbase-oracle polardbx | geminidb-redis kafka redis saas sapodp | kinesis pubsub |
| verify.value.one-plan | mssql postgres | mysql tidb | dcdb oceanbase polardbx | the rest (39) | geminidb-redis kinesis neo4j pubsub qdrant redis saas sapodp |
| verify.access.object-ownership | dsql mysql postgres tidb | clickhouse cockroachdb oracle redshift snowflake teradata | db2 db2i db2zos dcdb mssql oceanbase | ase gaussdb generic hetero lindorm neo4j oceanbase-oracle polardbx spanner | the rest (29) |
| verify.access.sequence-grants | postgres | cockroachdb dsql oracle snowflake | mssql | db2 db2i db2zos dcdb gaussdb generic hetero oceanbase oceanbase-oracle polardbx spanner | the rest (37) |
| verify.access.table-grants | cassandra kafka mysql opensearch postgres redis tidb | bigquery clickhouse cockroachdb dsql mongodb oracle redshift snowflake teradata | dcdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase | ase couchbase db2 db2i db2zos gaussdb generic hetero lindorm neo4j oceanbase-oracle polardbx spanner | the rest (18) |
| verify.identity.sequence-collision | mssql mysql oracle postgres tidb | cockroachdb dsql | db2 db2i db2zos dcdb oceanbase | ase duckdb gaussdb generic hetero oceanbase-oracle polardbx redshift snowflake spanner sqlite | the rest (31) |
| verify.identity.sequence-parity | mssql mysql oracle postgres tidb | cockroachdb dsql snowflake | db2 db2i db2zos dcdb oceanbase oceanbase-oracle | ase duckdb gaussdb generic hetero polardbx redshift spanner sqlite | the rest (31) |
| verify.movement.target-ahead | cassandra dsql kafka mongodb mssql mysql postgres redis tidb | bigquery clickhouse cockroachdb opensearch oracle redshift snowflake teradata | db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase | the rest (24) | kinesis pubsub saas sapodp |
| verify.structure.collection-options | cosmosdb dynamodb kafka mongodb opensearch postgres | bigquery cassandra clickhouse mysql oracle redshift snowflake tidb | db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces oceanbase | the rest (24) | copybook geminidb-redis kinesis parquet pubsub redis saas sapodp |
| verify.structure.columns | cassandra cockroachdb dsql mssql mysql opensearch postgres teradata tidb | bigquery clickhouse oracle redshift snowflake | db2 db2i db2zos dcdb geminidb-cassandra keyspaces oceanbase | ase copybook delta duckdb gaussdb generic hetero iceberg lindorm oceanbase-oracle parquet polardbx spanner sqlite | the rest (19) |
| verify.structure.deferrable | dsql postgres | oracle | - | gaussdb generic hetero oceanbase-oracle spanner sqlite | the rest (45) |
| verify.structure.extensions | postgres | - | - | gaussdb generic hetero neo4j | the rest (49) |
| verify.structure.foreign-keys | dsql generic mssql mysql postgres tidb | bigquery cockroachdb oracle redshift snowflake teradata | db2 db2i db2zos dcdb oceanbase | ase duckdb gaussdb hetero neptune oceanbase-oracle polardbx spanner sqlite | the rest (28) |
| verify.structure.indexes | cosmosdb dsql dynamodb firestore gaussdb generic lindorm mongodb postgres qdrant teradata tidb | bigquery cassandra clickhouse cockroachdb datastore oracle spanner | db2 db2i db2zos geminidb-cassandra geminidb-mongo keyspaces mssql | ase bigtable couchbase dcdb duckdb hetero neo4j neptune oceanbase oceanbase-oracle polardbx snowflake sqlite | the rest (15) |
| verify.structure.keys | cockroachdb dsql generic mssql mysql postgres redis teradata tidb | bigquery clickhouse oracle redshift snowflake | db2 db2i db2zos dcdb geminidb-redis oceanbase | ase copybook duckdb gaussdb hetero iceberg oceanbase-oracle polardbx sqlite | the rest (25) |
| verify.structure.materialized-views | postgres | bigquery cassandra clickhouse cockroachdb oracle redshift snowflake | db2 db2i db2zos geminidb-cassandra keyspaces | ase bigtable cosmosdb gaussdb generic hetero iceberg lindorm oceanbase oceanbase-oracle polardbx | the rest (30) |
| verify.structure.objects | cockroachdb duckdb generic kafka mongodb mssql mysql oracle postgres tidb | bigquery cassandra clickhouse cosmosdb dsql opensearch redshift snowflake teradata | db2 db2i db2zos geminidb-cassandra geminidb-mongo keyspaces oceanbase-oracle | the rest (17) | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub qdrant redis saas sapodp |
| verify.structure.partitions | kafka mysql postgres tidb | bigquery clickhouse oracle teradata | db2 db2i db2zos dcdb gaussdb mssql oceanbase oceanbase-oracle | ase delta generic hetero iceberg lindorm parquet polardbx timestream | the rest (29) |
| verify.structure.row-security | mssql opensearch postgres | bigquery clickhouse oracle redshift snowflake | db2 db2i db2zos | ase cockroachdb delta firestore gaussdb generic hetero iceberg oceanbase-oracle teradata | the rest (33) |
| verify.structure.schema-diff | dsql mysql postgres | bigquery cassandra cockroachdb | db2 db2i db2zos gaussdb geminidb-cassandra keyspaces | the rest (25) | cosmosdb couchbase datastore dynamodb firestore geminidb-mongo geminidb-redis kafka kinesis mongodb neo4j neptune pubsub qdrant redis saas sapodp |
| verify.structure.sharding | cosmosdb mongodb | clickhouse opensearch redshift teradata | db2 db2i db2zos geminidb-mongo | dcdb gaussdb generic hetero neo4j oceanbase oceanbase-oracle polardbx qdrant timestream | the rest (34) |
| verify.structure.triggers | mssql postgres | cassandra cockroachdb cosmosdb mysql oracle teradata | db2 db2i db2zos dcdb geminidb-cassandra keyspaces oceanbase | ase couchbase gaussdb generic hetero neo4j oceanbase-oracle polardbx sqlite | the rest (30) |
| verify.structure.unvalidated-constraints | dsql mysql oracle postgres tidb | cockroachdb | db2 db2i db2zos dcdb mssql oceanbase | ase gaussdb generic hetero neo4j oceanbase-oracle polardbx spanner sqlite teradata | the rest (32) |
| verify.value.charset | cassandra mssql mysql oracle postgres teradata tidb | bigquery clickhouse cockroachdb dsql mongodb | db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces oceanbase | the rest (23) | cosmosdb dynamodb kafka kinesis neo4j opensearch pubsub qdrant redis saas sapodp |
| verify.value.collation | dsql mongodb mysql opensearch postgres teradata tidb | bigquery cockroachdb oracle | db2 db2i db2zos dcdb geminidb-mongo mssql oceanbase | ase delta duckdb gaussdb generic hetero oceanbase-oracle polardbx redshift snowflake sqlite | the rest (26) |
| verify.value.generated | cosmosdb dsql mysql opensearch postgres teradata tidb | clickhouse cockroachdb oracle | db2 db2i db2zos dcdb mssql oceanbase | ase delta duckdb gaussdb generic hetero lindorm oceanbase-oracle polardbx redshift snowflake spanner sqlite | the rest (25) |
| verify.value.narrowing | cassandra cockroachdb dsql mongodb mysql opensearch postgres teradata tidb | bigquery clickhouse oracle | db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase | the rest (24) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.null-empty | cassandra cosmosdb dsql dynamodb generic kafka mongodb mysql opensearch postgres tidb | bigquery clickhouse cockroachdb oracle teradata | db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase | the rest (23) | geminidb-redis kinesis pubsub redis saas sapodp |
| verify.value.precision | cassandra dsql mongodb mysql opensearch postgres tidb | bigquery clickhouse cockroachdb oracle teradata | db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase | the rest (24) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.rendering | dsql mongodb mssql mysql postgres | bigquery clickhouse cockroachdb oracle | db2 db2i db2zos dcdb geminidb-mongo oceanbase | the rest (27) | bigtable cassandra cosmosdb dynamodb geminidb-redis kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.timezone | cassandra dsql mongodb mysql postgres teradata tidb | bigquery clickhouse cockroachdb opensearch oracle | db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase | the rest (23) | bigtable cosmosdb dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.type-drift | cosmosdb dynamodb mongodb opensearch | kafka | geminidb-mongo | bigtable couchbase datastore firestore geminidb-redis generic hetero neo4j neptune qdrant sqlite timestream | the rest (36) |
| verify.delta | the rest (15) | bigquery bigtable cockroachdb couchbase dsql firestore neo4j neptune spanner | ase db2 db2i dcdb gaussdb geminidb-mongo geminidb-redis keyspaces oceanbase polardbx | delta iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant saas sapodp sqlite teradata timestream |
| verify.fence | the rest (22) | bigquery bigtable neo4j neptune | ase db2 db2i geminidb-mongo keyspaces oceanbase polardbx | delta iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub qdrant saas sapodp sqlite teradata timestream |
| verify.confirm | cassandra cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | bigquery bigtable clickhouse cockroachdb couchbase dsql firestore neptune spanner | ase db2 db2i dcdb gaussdb geminidb-mongo keyspaces oceanbase polardbx | delta iceberg lindorm neo4j oceanbase-oracle redshift snowflake | the rest (15) |
| verify.users | the rest (23) | - | ase db2 db2zos | bigquery cockroachdb couchbase dsql neo4j redshift snowflake tidb | bigtable copybook cosmosdb datastore delta duckdb dynamodb firestore generic iceberg keyspaces kinesis neptune parquet pubsub qdrant saas sapodp sqlite timestream |
| verify.repair | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | the rest (16) | db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.repair-undo | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | the rest (16) | db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.rows-apart | cassandra clickhouse cosmosdb dsql dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | cockroachdb | gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces | the rest (32) | kinesis pubsub |
| verify.tables | cassandra clickhouse cosmosdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis | redshift snowflake | geminidb-cassandra geminidb-mongo geminidb-redis keyspaces | the rest (34) | kinesis pubsub |
| verify.table | bigquery cassandra clickhouse cosmosdb dsql duckdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite teradata tidb | the rest (19) | ase db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx | generic | kinesis pubsub |
| tail.follows | the rest (26) | bigquery | db2 db2i geminidb-mongo geminidb-redis oceanbase polardbx | ase delta iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant saas sapodp sqlite teradata timestream |
| tail.table-states | cockroachdb cosmosdb dynamodb kafka mongodb mysql postgres redis tidb | the rest (12) | db2 db2i dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase polardbx | ase cassandra clickhouse delta generic iceberg lindorm oceanbase-oracle opensearch redshift snowflake | copybook datastore db2zos duckdb kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.added | cockroachdb kafka mssql mysql oracle postgres tidb | bigquery mongodb spanner | geminidb-mongo oceanbase polardbx | the rest (28) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.readded | cosmosdb dynamodb kafka mssql mysql oracle tidb | bigquery mongodb | geminidb-mongo oceanbase polardbx | the rest (29) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.ddl | cosmosdb dynamodb kafka mongodb mysql postgres | oracle | geminidb-mongo oceanbase polardbx | the rest (31) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.renames | mongodb mysql postgres | bigquery oracle | geminidb-mongo mssql oceanbase polardbx | ase clickhouse cockroachdb db2 db2i dcdb delta dsql gaussdb geminidb-redis generic iceberg lindorm oceanbase-oracle opensearch redis redshift saas sapodp snowflake spanner tidb | the rest (23) |
| assess.versions | dsql kafka mongodb mysql opensearch postgres redis redshift snowflake sqlite teradata tidb | cassandra clickhouse cockroachdb oracle | gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql | the rest (17) | bigquery bigtable copybook cosmosdb datastore delta dynamodb firestore hetero iceberg kinesis parquet pubsub spanner timestream |
| assess.brands | kafka mongodb mysql opensearch postgres redis redshift snowflake tidb | cassandra clickhouse cockroachdb dsql oracle teradata | dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase polardbx | the rest (23) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.engine-checks | the rest (21) | bigtable cassandra clickhouse datastore firestore neo4j neptune opensearch qdrant sapodp spanner timestream | db2i dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm mssql oceanbase oceanbase-oracle polardbx | ase copybook db2 delta duckdb iceberg kinesis parquet pubsub | - |
| assess.preflight | cosmosdb dsql dynamodb kafka mongodb mysql postgres redis tidb | bigquery cassandra clickhouse cockroachdb opensearch oracle redshift snowflake teradata | db2 db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase polardbx | the rest (20) | kinesis pubsub saas sapodp |
| assess.capacity | clickhouse dynamodb hetero mongodb mysql postgres redis tidb | bigquery cassandra cosmosdb kafka opensearch oracle redshift snowflake teradata | dcdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase polardbx | the rest (27) | saas sapodp |
| assess.writers | dynamodb mysql postgres redis | bigquery clickhouse cosmosdb kafka mongodb opensearch oracle teradata | geminidb-mongo mssql | the rest (38) | kinesis pubsub |
| assess.handwork | dynamodb kafka mongodb mysql postgres redis | bigquery cassandra clickhouse cockroachdb cosmosdb dsql opensearch oracle redshift snowflake teradata tidb | gaussdb geminidb-mongo geminidb-redis mssql | the rest (28) | copybook kinesis parquet pubsub |
| schema.create | the rest (32) | datastore firestore kafka neo4j neptune timestream | db2i db2zos geminidb-cassandra lindorm oceanbase-oracle polardbx | generic | geminidb-mongo geminidb-redis hetero kinesis mongodb pubsub redis saas sapodp |
| schema.indexes | cockroachdb dsql dynamodb mongodb mysql postgres teradata tidb | cassandra clickhouse cosmosdb oracle spanner sqlite | dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase polardbx | the rest (17) | copybook delta geminidb-redis hetero iceberg kafka kinesis opensearch parquet pubsub redis redshift saas sapodp timestream |
| schema.foreign-keys | dsql mssql mysql oracle postgres tidb | bigquery cockroachdb neo4j neptune redshift snowflake spanner teradata | dcdb gaussdb oceanbase polardbx | ase db2 db2i db2zos duckdb generic oceanbase-oracle sqlite | the rest (28) |
| schema.defaults | cockroachdb dsql mysql opensearch postgres sqlite teradata tidb | bigquery clickhouse oracle redshift sapodp snowflake spanner | dcdb gaussdb mssql oceanbase polardbx | ase copybook db2 db2i db2zos delta duckdb geminidb-cassandra generic iceberg keyspaces lindorm oceanbase-oracle parquet | the rest (20) |
| schema.views | dsql mssql mysql oracle postgres tidb | bigquery clickhouse cockroachdb mongodb redshift snowflake teradata | dcdb gaussdb geminidb-mongo oceanbase polardbx | ase bigtable couchbase db2 db2i db2zos duckdb generic iceberg lindorm oceanbase-oracle opensearch spanner sqlite | the rest (22) |
| schema.routines | mssql mysql oracle postgres | bigquery cockroachdb mongodb | dcdb geminidb-mongo oceanbase polardbx | the rest (23) | bigtable copybook datastore delta dynamodb firestore hetero iceberg kafka keyspaces kinesis neptune parquet pubsub qdrant saas sapodp sqlite tidb timestream |
| move.table-copy | the rest (38) | bigquery neo4j snowflake timestream | db2i db2zos dcdb geminidb-cassandra geminidb-mongo lindorm oceanbase-oracle polardbx | - | kinesis pubsub saas sapodp |
| move.bulk | the rest (22) | bigquery hetero | ase db2 db2i db2zos geminidb-cassandra geminidb-mongo keyspaces | cockroachdb cosmosdb couchbase dcdb delta iceberg neo4j oceanbase oceanbase-oracle polardbx qdrant redshift snowflake tidb | dsql geminidb-redis generic kinesis lindorm pubsub saas sapodp teradata |
| move.large-values | the rest (19) | bigquery couchbase snowflake teradata | db2i db2zos dcdb gaussdb geminidb-mongo geminidb-redis oceanbase oceanbase-oracle polardbx | bigtable cockroachdb datastore dsql firestore lindorm neo4j neptune qdrant spanner timestream | cassandra copybook cosmosdb dynamodb geminidb-cassandra keyspaces kinesis pubsub redshift saas sapodp |
| move.partitioned | clickhouse generic kafka mysql postgres tidb | bigquery oracle parquet teradata | db2 db2i db2zos dcdb gaussdb mssql oceanbase oceanbase-oracle polardbx | ase delta hetero iceberg lindorm neo4j qdrant | the rest (28) |
| move.spread | hetero postgres | cassandra mongodb mysql | - | - | the rest (49) |
| move.sized | the rest (18) | cassandra cockroachdb oracle redshift snowflake teradata | db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle polardbx | bigtable copybook couchbase datastore delta firestore generic iceberg neo4j neptune parquet qdrant saas sapodp spanner timestream | kinesis pubsub |
| move.load-window | hetero mysql opensearch oracle postgres | cassandra clickhouse mongodb teradata | dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase polardbx | the rest (20) | bigquery bigtable copybook delta dynamodb geminidb-redis iceberg kafka kinesis neptune parquet pubsub redis redshift saas sapodp snowflake timestream |
| move.create-missing | cosmosdb dsql dynamodb hetero mysql opensearch oracle postgres teradata tidb | bigquery cassandra clickhouse cockroachdb kafka redshift snowflake | dcdb gaussdb geminidb-cassandra keyspaces mssql oceanbase polardbx | the rest (19) | datastore firestore geminidb-mongo geminidb-redis kinesis mongodb neptune pubsub redis saas sapodp |
| cutover.freeze | cassandra clickhouse cockroachdb kafka mongodb mssql mysql opensearch postgres redis redshift snowflake teradata tidb | couchbase hetero oracle | ase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis oceanbase oceanbase-oracle polardbx | - | the rest (27) |
| cutover.read-only-target | the rest (22) | hetero | db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis oceanbase oceanbase-oracle polardbx | - | bigtable copybook cosmosdb datastore delta duckdb firestore generic iceberg keyspaces kinesis neo4j neptune parquet pubsub qdrant saas sapodp spanner sqlite timestream |
| cutover.counters | dcdb duckdb gaussdb hetero mssql mysql oceanbase-oracle oracle polardbx postgres spanner sqlite tidb | cockroachdb dsql | ase db2 db2i db2zos oceanbase | redshift snowflake | the rest (32) |
| cutover.rollback | the rest (20) | bigquery bigtable copybook datastore mongodb neptune | ase db2 db2i db2zos geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase polardbx | cockroachdb cosmosdb couchbase delta dsql iceberg neo4j qdrant redshift snowflake tidb | generic kinesis pubsub saas sapodp teradata |
| cutover.drain | the rest (21) | bigquery bigtable neptune saas | db2 db2i geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase polardbx | ase delta generic hetero iceberg lindorm neo4j oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb kinesis parquet pubsub qdrant sqlite teradata timestream |
| cutover.pause-replication | mysql postgres | - | - | generic neo4j qdrant redis | the rest (48) |
| cutover.two-way | cassandra dynamodb kafka mongodb mysql postgres redis teradata | - | geminidb-cassandra geminidb-mongo geminidb-redis keyspaces | the rest (31) | copybook db2zos duckdb hetero kinesis parquet pubsub qdrant saas sapodp sqlite |
| cutover.jobs | mysql postgres | clickhouse oracle | mssql | ase bigquery cockroachdb couchbase db2 db2i db2zos dcdb gaussdb generic lindorm oceanbase oceanbase-oracle opensearch polardbx redshift snowflake teradata timestream | the rest (30) |
| security.tls | the rest (44) | oracle | - | ase db2 db2zos generic mssql neo4j qdrant saas | hetero |
| managed.catalogued | the rest (36) | - | - | ase couchbase db2i db2zos delta duckdb generic iceberg neo4j parquet qdrant saas sapodp snowflake sqlite tidb | copybook hetero |
| operations.fencing | mysql postgres | cockroachdb dsql tidb | dcdb gaussdb oceanbase polardbx | the rest (42) | hetero saas sapodp |

## Source layout

381 upward imports still listed (`docs/agents/LAYERS.md`; the list only shrinks); 0 files no package claims (`docs/agents/INVENTORY.md`).

Gaps per pair (`autonomous.gaps`: a pair lacks what its two engines lack): 2862 ordered pairs of 54 engines; 26 shape lacks, 71 capability lacks, 2145 depth lacks in all; fewest kinesis -> postgres (6), most oceanbase-oracle -> db2i (143).

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

