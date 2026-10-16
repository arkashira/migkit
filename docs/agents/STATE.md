# migkit state

Written by `tools/gen_codemap.py` from `capabilities.matrix()`, `decide.coverage()`, `decide.still_lacking()`, `capabilities.depth()` and `docs/backlog-index.md`; the pre-commit hook keeps it current. What this machine last proved (the full suite, unpushed work) is in `.codemap/LOCAL.md`.

## Capabilities by engine

16 capabilities (`capabilities.CAPABILITIES`) and 12 shapes (`decide.coverage()`); a row lists what is not yet there and counts the depth items owed, as each engine says it still lacks them (`decide.still_lacking()`).

| engine | capabilities | shapes | depth owed | not yet |
|---|---|---|---|---|
| ase | 6/16 | 11/12 | 66 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned |
| bigquery | 15/16 | 12/12 | 48 | users |
| bigtable | 16/16 | 11/12 | 40 | large values |
| cassandra | 16/16 | 12/12 | 30 | - |
| clickhouse | 16/16 | 12/12 | 45 | - |
| cockroachdb | 12/16 | 11/12 | 50 | params, bulk-move, users, snapshot, large values |
| copybook | 16/16 | 12/12 | 31 | - |
| cosmosdb | 14/16 | 12/12 | 20 | bulk-move, snapshot |
| couchbase | 11/16 | 12/12 | 48 | params, bulk-move, users, statistics, snapshot |
| datastore | 16/16 | 11/12 | 37 | large values |
| db2 | 7/16 | 12/12 | 66 | params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| db2i | 13/16 | 12/12 | 69 | params, bulk-move, snapshot |
| db2zos | 11/16 | 12/12 | 61 | params, bulk-move, users, statistics, snapshot |
| dcdb | 15/16 | 11/12 | 64 | bulk-move, large values |
| delta | 9/16 | 11/12 | 47 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| dsql | 13/16 | 11/12 | 27 | users, statistics, snapshot, large values |
| duckdb | 16/16 | 12/12 | 46 | - |
| dynamodb | 16/16 | 12/12 | 8 | - |
| firestore | 16/16 | 11/12 | 42 | large values |
| gaussdb | 16/16 | 10/12 | 67 | large values, partitioned |
| geminidb-cassandra | 16/16 | 11/12 | 48 | large values |
| geminidb-mongo | 16/16 | 11/12 | 49 | large values |
| geminidb-redis | 16/16 | 11/12 | 35 | large values |
| generic | 16/16 | 12/12 | 57 | - |
| hetero | 16/16 | 10/12 | 45 | partitioned, missing grant |
| iceberg | 9/16 | 11/12 | 48 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| kafka | 16/16 | 12/12 | 12 | - |
| keyspaces | 16/16 | 11/12 | 49 | large values |
| kinesis | 16/16 | 12/12 | 4 | - |
| lindorm | 12/16 | 10/12 | 55 | stream, fence, confirm, delta, large values, partitioned |
| mongodb | 16/16 | 11/12 | 12 | missing grant |
| mssql | 16/16 | 12/12 | 38 | - |
| mysql | 16/16 | 12/12 | 5 | - |
| neo4j | 10/16 | 5/12 | 54 | params, bulk-move, confirm, users, statistics, snapshot, keyless, no unique index, wide key, large values, partitioned, read-only target, writes held |
| neptune | 16/16 | 11/12 | 43 | large values |
| oceanbase | 15/16 | 11/12 | 66 | bulk-move, large values |
| oceanbase-oracle | 11/16 | 10/12 | 69 | bulk-move, stream, fence, confirm, delta, large values, partitioned |
| opensearch | 16/16 | 12/12 | 25 | - |
| oracle | 16/16 | 12/12 | 40 | - |
| parquet | 16/16 | 12/12 | 32 | - |
| polardbx | 15/16 | 11/12 | 67 | bulk-move, large values |
| postgres | 16/16 | 12/12 | 2 | - |
| pubsub | 16/16 | 12/12 | 4 | - |
| qdrant | 13/16 | 5/12 | 36 | params, bulk-move, snapshot, keyless, no unique index, wide key, large values, partitioned, read-only target, writes held |
| redis | 16/16 | 12/12 | 11 | - |
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

88 items in 10 areas (`capabilities.depth()`, backlog P0-PARITY), each read off the code an engine runs; a way counts once a test proves it on that engine (`PROVES` in the test module). Cells: n/a 1408, not yet 1350, test owed 1047, yes 947. A cell below is the ways proved of the items that apply.

| engine | scope | verify | tail | assess | schema | move | cutover | security | managed | operations | all |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ase | 0/10 | 0/35 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 0/1 | 0/1 | 5/81 |
| bigquery | 5/9 | 1/29 | 0/6 | 1/6 | 1/6 | 1/6 | 1/5 | 1/1 | 1/1 | 0/1 | 12/70 |
| bigtable | 1/9 | 0/18 | 1/5 | 0/6 | 1/3 | 2/5 | 0/3 | 1/1 | 1/1 | 0/1 | 7/52 |
| cassandra | 6/9 | 15/24 | 1/5 | 0/7 | 1/3 | 2/6 | 4/5 | 1/1 | 1/1 | 0/1 | 31/62 |
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
| dynamodb | 6/8 | 12/14 | 4/5 | 5/6 | 2/2 | 4/4 | 3/4 | 1/1 | 1/1 | 0/1 | 38/46 |
| firestore | 0/9 | 2/21 | 1/5 | 0/6 | 0/2 | 2/5 | 2/3 | 1/1 | 1/1 | 0/1 | 9/54 |
| gaussdb | 0/10 | 3/39 | 1/6 | 0/7 | 1/6 | 2/7 | 3/7 | 1/1 | 1/1 | 0/1 | 12/85 |
| geminidb-cassandra | 0/9 | 1/22 | 0/1 | 0/7 | 0/4 | 0/6 | 0/5 | 1/1 | 1/1 | 0/1 | 3/57 |
| geminidb-mongo | 0/9 | 1/24 | 0/6 | 0/7 | 0/3 | 0/5 | 0/5 | 1/1 | 1/1 | 0/1 | 3/62 |
| geminidb-redis | 0/8 | 1/12 | 0/6 | 0/7 | 0/1 | 1/3 | 0/5 | 1/1 | 1/1 | 0/1 | 4/45 |
| generic | 9/10 | 10/39 | 1/6 | 1/7 | 0/6 | 3/6 | 0/4 | 0/1 | 0/1 | 0/1 | 24/81 |
| hetero | 4/10 | 6/40 | 1/2 | 2/5 | - | 6/8 | 2/5 | - | - | - | 21/70 |
| iceberg | 0/9 | 0/24 | 0/6 | 0/5 | 1/3 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/59 |
| kafka | 5/9 | 15/17 | 5/5 | 5/7 | 0/1 | 5/6 | 4/5 | 1/1 | 1/1 | 0/1 | 41/53 |
| keyspaces | 0/9 | 0/24 | 1/5 | 0/7 | 1/3 | 1/6 | 0/3 | 1/1 | 1/1 | 0/1 | 5/60 |
| kinesis | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| lindorm | 0/9 | 2/27 | 0/6 | 0/7 | 0/5 | 0/6 | 1/5 | 1/1 | 1/1 | 0/1 | 5/68 |
| mongodb | 8/9 | 21/24 | 4/6 | 6/7 | 1/3 | 4/6 | 3/5 | 1/1 | 1/1 | 0/1 | 49/63 |
| mssql | 7/10 | 18/35 | 3/6 | 0/7 | 4/6 | 4/7 | 5/7 | 0/1 | 1/1 | 0/1 | 42/81 |
| mysql | 9/10 | 28/32 | 6/6 | 7/7 | 6/6 | 7/8 | 8/8 | 1/1 | 1/1 | 1/1 | 74/80 |
| neo4j | 0/9 | 0/25 | 1/5 | 0/7 | 0/4 | 0/7 | 0/6 | 0/1 | 0/1 | 0/1 | 1/66 |
| neptune | 0/8 | 0/20 | 1/5 | 0/7 | 0/3 | 2/4 | 0/3 | 1/1 | 1/1 | 0/1 | 5/53 |
| oceanbase | 0/10 | 1/36 | 0/6 | 0/7 | 1/6 | 1/7 | 0/7 | 1/1 | 1/1 | 0/1 | 5/82 |
| oceanbase-oracle | 0/10 | 1/38 | 0/6 | 0/7 | 0/6 | 0/7 | 2/7 | 1/1 | 1/1 | 0/1 | 5/84 |
| opensearch | 7/9 | 17/25 | 1/6 | 2/7 | 2/4 | 6/6 | 4/6 | 1/1 | 1/1 | 0/1 | 41/66 |
| oracle | 9/10 | 14/37 | 3/6 | 1/7 | 4/6 | 5/7 | 4/7 | 0/1 | 1/1 | 0/1 | 41/83 |
| parquet | 0/7 | 0/16 | - | 0/4 | 1/2 | 3/6 | 1/1 | 1/1 | 0/1 | 0/1 | 6/39 |
| polardbx | 0/10 | 1/36 | 0/6 | 0/7 | 0/6 | 0/7 | 1/7 | 1/1 | 1/1 | 0/1 | 4/82 |
| postgres | 9/10 | 38/38 | 5/6 | 7/7 | 6/6 | 8/8 | 8/8 | 1/1 | 1/1 | 1/1 | 84/86 |
| pubsub | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| qdrant | 0/7 | 1/15 | - | 0/7 | 1/2 | 1/7 | 0/4 | 0/1 | 0/1 | 0/1 | 3/45 |
| redis | 5/8 | 12/12 | 2/6 | 7/7 | 0/1 | 4/4 | 4/6 | 1/1 | 1/1 | 0/1 | 36/47 |
| redshift | 5/10 | 0/32 | 0/6 | 3/7 | 1/5 | 1/4 | 2/7 | 1/1 | 1/1 | 0/1 | 14/74 |
| saas | 0/6 | 0/3 | 0/5 | 1/5 | - | 0/1 | 0/1 | 0/1 | 0/1 | - | 1/23 |
| sapodp | 0/6 | 0/3 | 0/5 | 0/5 | 0/1 | 0/1 | 1/1 | 1/1 | 0/1 | - | 2/24 |
| snowflake | 4/10 | 0/33 | 0/6 | 3/7 | 1/6 | 0/5 | 2/7 | 1/1 | 0/1 | 0/1 | 11/77 |
| spanner | 0/10 | 2/31 | 1/6 | 0/6 | 1/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 11/72 |
| sqlite | 2/9 | 4/28 | - | 2/7 | 2/5 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 17/60 |
| teradata | 6/8 | 12/30 | - | 2/7 | 3/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 30/64 |
| tidb | 9/10 | 25/32 | 4/6 | 5/7 | 5/5 | 5/7 | 4/6 | 1/1 | 0/1 | 0/1 | 58/76 |
| timestream | 0/7 | 0/19 | - | 0/6 | 0/1 | 1/5 | 1/3 | 1/1 | 1/1 | 0/1 | 4/44 |

Per item: the engines in each state - "test owed" is a way in the code whose test on that engine is not written (`UNPROVED`).

| item | yes | test owed | not yet | n/a |
|---|---|---|---|---|
| scope.listings | the rest (21) | ase datastore db2 dcdb firestore gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm neo4j neptune oceanbase oceanbase-oracle polardbx qdrant spanner timestream | copybook couchbase db2i db2zos delta duckdb hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.deep | cassandra clickhouse cockroachdb cosmosdb db2 dynamodb generic kafka mongodb mssql mysql opensearch oracle postgres redis teradata tidb | the rest (24) | copybook couchbase delta iceberg neo4j neptune parquet sqlite | kinesis pubsub saas sapodp timestream |
| scope.copy | the rest (20) | ase bigtable datastore db2 dcdb firestore gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm neo4j neptune oceanbase oceanbase-oracle polardbx qdrant spanner timestream | copybook couchbase db2i db2zos delta duckdb hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.repair | cassandra clickhouse cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis redshift snowflake sqlite teradata tidb | the rest (25) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| scope.counters | mssql mysql oracle postgres tidb | cockroachdb db2 db2i db2zos dcdb dsql duckdb gaussdb oceanbase oceanbase-oracle polardbx | ase generic hetero redshift snowflake spanner sqlite | the rest (31) |
| scope.objects | generic kafka mongodb mssql mysql oracle postgres tidb | the rest (22) | ase bigtable couchbase datastore delta firestore hetero iceberg lindorm neo4j polardbx spanner timestream | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub qdrant redis saas sapodp |
| scope.ops | bigquery cassandra clickhouse cockroachdb cosmosdb dsql dynamodb generic hetero mongodb mssql mysql opensearch oracle postgres redis redshift snowflake tidb | the rest (21) | ase db2 kafka lindorm oceanbase-oracle | copybook datastore db2zos duckdb parquet qdrant sqlite teradata timestream |
| scope.row-filter | bigquery clickhouse cockroachdb dsql duckdb generic hetero mongodb mysql opensearch oracle postgres redshift sqlite teradata tidb | the rest (31) | geminidb-redis kafka redis saas sapodp | kinesis pubsub |
| scope.transforms | generic | - | the rest (51) | kinesis pubsub |
| scope.names | bigquery cassandra clickhouse cosmosdb dsql dynamodb generic hetero mongodb mysql opensearch oracle postgres teradata tidb | the rest (32) | geminidb-redis kafka redis saas sapodp | kinesis pubsub |
| verify.value.one-plan | mssql postgres | dcdb mysql oceanbase polardbx tidb | the rest (39) | geminidb-redis kinesis neo4j pubsub qdrant redis saas sapodp |
| verify.access.object-ownership | dsql mysql postgres tidb | clickhouse cockroachdb db2 db2i db2zos dcdb mssql oceanbase oracle redshift snowflake teradata | ase gaussdb generic hetero lindorm neo4j oceanbase-oracle polardbx spanner | the rest (29) |
| verify.access.sequence-grants | postgres | cockroachdb dsql mssql oracle snowflake | db2 db2i db2zos dcdb gaussdb generic hetero oceanbase oceanbase-oracle polardbx spanner | the rest (37) |
| verify.access.table-grants | cassandra kafka mysql opensearch postgres redis tidb | bigquery clickhouse cockroachdb dcdb dsql geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mongodb mssql oceanbase oracle redshift snowflake teradata | ase couchbase db2 db2i db2zos gaussdb generic hetero lindorm neo4j oceanbase-oracle polardbx spanner | the rest (18) |
| verify.identity.sequence-collision | mssql mysql oracle postgres tidb | cockroachdb db2 db2i db2zos dcdb dsql oceanbase | ase duckdb gaussdb generic hetero oceanbase-oracle polardbx redshift snowflake spanner sqlite | the rest (31) |
| verify.identity.sequence-parity | mssql mysql oracle postgres tidb | cockroachdb db2 db2i db2zos dcdb dsql oceanbase oceanbase-oracle snowflake | ase duckdb gaussdb generic hetero polardbx redshift spanner sqlite | the rest (31) |
| verify.movement.target-ahead | cassandra dsql kafka mongodb mssql mysql postgres redis tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase opensearch oracle redshift snowflake teradata | the rest (24) | kinesis pubsub saas sapodp |
| verify.structure.collection-options | cosmosdb dynamodb kafka mongodb opensearch postgres | bigquery cassandra clickhouse db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mysql oceanbase oracle redshift snowflake tidb | the rest (24) | copybook geminidb-redis kinesis parquet pubsub redis saas sapodp |
| verify.structure.columns | cassandra cockroachdb dsql mssql mysql opensearch postgres teradata tidb | bigquery clickhouse db2 db2i db2zos dcdb geminidb-cassandra keyspaces oceanbase oracle redshift snowflake | ase copybook delta duckdb gaussdb generic hetero iceberg lindorm oceanbase-oracle parquet polardbx spanner sqlite | the rest (19) |
| verify.structure.deferrable | dsql postgres | oracle | gaussdb generic hetero oceanbase-oracle spanner sqlite | the rest (45) |
| verify.structure.extensions | postgres | - | gaussdb generic hetero neo4j | the rest (49) |
| verify.structure.foreign-keys | dsql generic mssql mysql postgres tidb | bigquery cockroachdb db2 db2i db2zos dcdb oceanbase oracle redshift snowflake teradata | ase duckdb gaussdb hetero neptune oceanbase-oracle polardbx spanner sqlite | the rest (28) |
| verify.structure.indexes | cosmosdb dsql dynamodb firestore gaussdb generic lindorm mongodb postgres qdrant teradata tidb | bigquery cassandra clickhouse cockroachdb datastore db2 db2i db2zos geminidb-cassandra geminidb-mongo keyspaces mssql oracle spanner | ase bigtable couchbase dcdb duckdb hetero neo4j neptune oceanbase oceanbase-oracle polardbx snowflake sqlite | the rest (15) |
| verify.structure.keys | cockroachdb dsql generic mssql mysql postgres redis teradata tidb | bigquery clickhouse db2 db2i db2zos dcdb geminidb-redis oceanbase oracle redshift snowflake | ase copybook duckdb gaussdb hetero iceberg oceanbase-oracle polardbx sqlite | the rest (25) |
| verify.structure.materialized-views | postgres | bigquery cassandra clickhouse cockroachdb db2 db2i db2zos geminidb-cassandra keyspaces oracle redshift snowflake | ase bigtable cosmosdb gaussdb generic hetero iceberg lindorm oceanbase oceanbase-oracle polardbx | the rest (30) |
| verify.structure.objects | cockroachdb duckdb generic kafka mongodb mssql mysql oracle postgres tidb | bigquery cassandra clickhouse cosmosdb db2 db2i db2zos dsql geminidb-cassandra geminidb-mongo keyspaces oceanbase-oracle opensearch redshift snowflake teradata | the rest (17) | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub qdrant redis saas sapodp |
| verify.structure.partitions | kafka mysql postgres tidb | bigquery clickhouse db2 db2i db2zos dcdb mssql oceanbase oracle teradata | ase delta gaussdb generic hetero iceberg lindorm oceanbase-oracle parquet polardbx timestream | the rest (29) |
| verify.structure.row-security | opensearch postgres | bigquery clickhouse db2 db2i db2zos mssql oracle redshift snowflake | ase cockroachdb delta firestore gaussdb generic hetero iceberg oceanbase-oracle teradata | the rest (33) |
| verify.structure.schema-diff | dsql mysql postgres | bigquery cassandra cockroachdb db2 db2i db2zos gaussdb geminidb-cassandra keyspaces | the rest (25) | cosmosdb couchbase datastore dynamodb firestore geminidb-mongo geminidb-redis kafka kinesis mongodb neo4j neptune pubsub qdrant redis saas sapodp |
| verify.structure.sharding | cosmosdb mongodb | clickhouse db2 db2i db2zos geminidb-mongo opensearch redshift teradata | dcdb gaussdb generic hetero neo4j oceanbase oceanbase-oracle polardbx qdrant timestream | the rest (34) |
| verify.structure.triggers | mssql postgres | cassandra cockroachdb cosmosdb db2 db2i db2zos dcdb geminidb-cassandra keyspaces mysql oceanbase oracle teradata | ase couchbase gaussdb generic hetero neo4j oceanbase-oracle polardbx sqlite | the rest (30) |
| verify.structure.unvalidated-constraints | dsql mysql oracle postgres tidb | cockroachdb db2 db2i db2zos dcdb mssql oceanbase | ase gaussdb generic hetero neo4j oceanbase-oracle polardbx spanner sqlite teradata | the rest (32) |
| verify.value.charset | cassandra mysql oracle postgres teradata tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb dsql geminidb-cassandra geminidb-mongo keyspaces mongodb mssql oceanbase | the rest (23) | cosmosdb dynamodb kafka kinesis neo4j opensearch pubsub qdrant redis saas sapodp |
| verify.value.collation | dsql mongodb mysql opensearch postgres teradata tidb | bigquery cockroachdb db2 db2i db2zos dcdb geminidb-mongo mssql oceanbase oracle | ase delta duckdb gaussdb generic hetero oceanbase-oracle polardbx redshift snowflake sqlite | the rest (26) |
| verify.value.generated | cosmosdb dsql mysql opensearch postgres teradata tidb | clickhouse cockroachdb db2 db2i db2zos dcdb mssql oceanbase oracle | ase delta duckdb gaussdb generic hetero lindorm oceanbase-oracle polardbx redshift snowflake spanner sqlite | the rest (25) |
| verify.value.narrowing | cassandra cockroachdb dsql mongodb mysql opensearch postgres teradata tidb | bigquery clickhouse db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle | the rest (24) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.null-empty | cassandra cosmosdb dsql dynamodb generic kafka mongodb mysql opensearch postgres tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle teradata | the rest (23) | geminidb-redis kinesis pubsub redis saas sapodp |
| verify.value.precision | cassandra dsql mongodb mysql opensearch postgres tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle teradata | the rest (24) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.rendering | dsql mongodb postgres | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-mongo mssql mysql oceanbase oracle | the rest (27) | bigtable cassandra cosmosdb dynamodb geminidb-redis kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.timezone | cassandra dsql mongodb mysql postgres teradata tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase opensearch oracle | the rest (23) | bigtable cosmosdb dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.type-drift | cosmosdb dynamodb mongodb opensearch | geminidb-mongo kafka | bigtable couchbase datastore firestore geminidb-redis generic hetero neo4j neptune qdrant sqlite timestream | the rest (36) |
| verify.delta | cassandra clickhouse cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | the rest (17) | ase db2 delta iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant saas sapodp sqlite teradata timestream |
| verify.fence | the rest (22) | bigquery bigtable db2i geminidb-mongo keyspaces neo4j neptune oceanbase polardbx | ase db2 delta iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub qdrant saas sapodp sqlite teradata timestream |
| verify.confirm | cassandra cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | the rest (16) | ase db2 delta iceberg lindorm neo4j oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub qdrant saas sapodp sqlite teradata timestream |
| verify.users | the rest (23) | - | ase bigquery cockroachdb couchbase db2 db2zos dsql neo4j redshift snowflake tidb | bigtable copybook cosmosdb datastore delta duckdb dynamodb firestore generic iceberg keyspaces kinesis neptune parquet pubsub qdrant saas sapodp sqlite timestream |
| verify.repair | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | the rest (29) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.repair-undo | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | the rest (29) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.rows-apart | cassandra clickhouse cosmosdb dsql dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | cockroachdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces | the rest (32) | kinesis pubsub |
| verify.tables | cassandra clickhouse cosmosdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis | geminidb-cassandra geminidb-mongo geminidb-redis keyspaces redshift snowflake | the rest (34) | kinesis pubsub |
| verify.table | bigquery cassandra clickhouse cosmosdb dsql duckdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite teradata tidb | the rest (33) | generic | kinesis pubsub |
| tail.follows | the rest (26) | bigquery db2i geminidb-mongo geminidb-redis oceanbase polardbx | ase db2 delta iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub qdrant saas sapodp sqlite teradata timestream |
| tail.table-states | cockroachdb cosmosdb dynamodb kafka mongodb mysql postgres redis tidb | the rest (22) | ase cassandra clickhouse db2 delta generic iceberg lindorm oceanbase-oracle opensearch redshift snowflake | copybook datastore db2zos duckdb kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.added | cockroachdb kafka mssql mysql oracle postgres tidb | bigquery geminidb-mongo mongodb oceanbase polardbx spanner | the rest (28) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.readded | cosmosdb dynamodb kafka mssql mysql oracle tidb | bigquery geminidb-mongo mongodb oceanbase polardbx | the rest (29) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.ddl | cosmosdb dynamodb kafka mongodb mysql postgres | geminidb-mongo oceanbase oracle polardbx | the rest (31) | copybook datastore db2zos duckdb geminidb-cassandra hetero kinesis parquet pubsub qdrant sqlite teradata timestream |
| tail.renames | mongodb mysql postgres | bigquery geminidb-mongo mssql oceanbase oracle polardbx | ase clickhouse cockroachdb db2 db2i dcdb delta dsql gaussdb geminidb-redis generic iceberg lindorm oceanbase-oracle opensearch redis redshift saas sapodp snowflake spanner tidb | the rest (23) |
| assess.versions | dsql kafka mongodb mysql opensearch postgres redis redshift snowflake sqlite teradata tidb | cassandra clickhouse cockroachdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oracle | the rest (17) | bigquery bigtable copybook cosmosdb datastore delta dynamodb firestore hetero iceberg kinesis parquet pubsub spanner timestream |
| assess.brands | kafka mongodb mysql opensearch postgres redis redshift snowflake tidb | cassandra clickhouse cockroachdb dcdb dsql gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase oracle polardbx teradata | the rest (23) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.engine-checks | bigquery cockroachdb cosmosdb couchbase db2zos dsql dynamodb generic hetero kafka mongodb mysql oracle postgres redis redshift saas snowflake sqlite teradata tidb | the rest (24) | ase copybook db2 delta duckdb iceberg kinesis parquet pubsub | - |
| assess.preflight | cosmosdb dsql dynamodb kafka mongodb mysql postgres redis tidb | the rest (21) | ase bigtable copybook couchbase datastore delta duckdb firestore generic hetero iceberg lindorm neo4j neptune oceanbase-oracle parquet qdrant spanner sqlite timestream | kinesis pubsub saas sapodp |
| assess.capacity | clickhouse dynamodb hetero mongodb mysql postgres redis tidb | bigquery cassandra cosmosdb dcdb geminidb-cassandra geminidb-mongo geminidb-redis kafka keyspaces mssql oceanbase opensearch oracle polardbx redshift snowflake teradata | the rest (27) | saas sapodp |
| assess.writers | dynamodb mysql postgres redis | bigquery clickhouse cosmosdb geminidb-mongo kafka mongodb mssql opensearch oracle teradata | the rest (38) | kinesis pubsub |
| assess.handwork | dynamodb kafka mongodb mysql postgres redis | bigquery cassandra clickhouse cockroachdb cosmosdb dsql gaussdb geminidb-mongo geminidb-redis mssql opensearch oracle redshift snowflake teradata tidb | the rest (28) | copybook kinesis parquet pubsub |
| schema.create | the rest (32) | datastore db2i db2zos firestore geminidb-cassandra kafka lindorm neo4j neptune oceanbase-oracle polardbx timestream | generic | geminidb-mongo geminidb-redis hetero kinesis mongodb pubsub redis saas sapodp |
| schema.indexes | cockroachdb dsql dynamodb mongodb mysql postgres teradata tidb | cassandra clickhouse cosmosdb dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle polardbx spanner sqlite | the rest (17) | copybook delta geminidb-redis hetero iceberg kafka kinesis opensearch parquet pubsub redis redshift saas sapodp timestream |
| schema.foreign-keys | dsql mssql mysql oracle postgres tidb | bigquery cockroachdb dcdb gaussdb neo4j neptune oceanbase polardbx redshift snowflake spanner teradata | ase db2 db2i db2zos duckdb generic oceanbase-oracle sqlite | the rest (28) |
| schema.defaults | cockroachdb dsql mysql opensearch postgres sqlite teradata tidb | bigquery clickhouse dcdb gaussdb mssql oceanbase oracle polardbx redshift sapodp snowflake spanner | ase copybook db2 db2i db2zos delta duckdb geminidb-cassandra generic iceberg keyspaces lindorm oceanbase-oracle parquet | the rest (20) |
| schema.views | dsql mssql mysql oracle postgres tidb | bigquery clickhouse cockroachdb dcdb gaussdb geminidb-mongo mongodb oceanbase polardbx redshift snowflake teradata | ase bigtable couchbase db2 db2i db2zos duckdb generic iceberg lindorm oceanbase-oracle opensearch spanner sqlite | the rest (22) |
| schema.routines | mssql mysql oracle postgres | bigquery cockroachdb dcdb geminidb-mongo mongodb oceanbase polardbx | the rest (23) | bigtable copybook datastore delta dynamodb firestore hetero iceberg kafka keyspaces kinesis neptune parquet pubsub qdrant saas sapodp sqlite tidb timestream |
| move.table-copy | the rest (38) | bigquery db2i db2zos dcdb geminidb-cassandra geminidb-mongo lindorm neo4j oceanbase-oracle polardbx snowflake timestream | - | kinesis pubsub saas sapodp |
| move.bulk | the rest (22) | bigquery geminidb-cassandra geminidb-mongo hetero keyspaces | ase cockroachdb cosmosdb couchbase db2 db2i db2zos dcdb delta iceberg neo4j oceanbase oceanbase-oracle polardbx qdrant redshift snowflake tidb | dsql geminidb-redis generic kinesis lindorm pubsub saas sapodp teradata |
| move.large-values | ase clickhouse db2 delta duckdb generic hetero iceberg kafka mongodb mssql mysql opensearch oracle parquet postgres redis sqlite tidb | bigquery couchbase db2i db2zos snowflake teradata | the rest (20) | cassandra copybook cosmosdb dynamodb kinesis pubsub redshift saas sapodp |
| move.partitioned | clickhouse generic kafka mysql postgres tidb | bigquery db2 db2i db2zos dcdb mssql oceanbase oracle parquet polardbx teradata | ase delta gaussdb hetero iceberg lindorm neo4j oceanbase-oracle qdrant | the rest (28) |
| move.spread | hetero postgres | cassandra mongodb mysql | - | the rest (49) |
| move.sized | the rest (18) | cassandra cockroachdb db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle oracle polardbx redshift snowflake teradata | bigtable copybook couchbase datastore delta firestore generic iceberg neo4j neptune parquet qdrant saas sapodp spanner timestream | kinesis pubsub |
| move.load-window | hetero mysql opensearch oracle postgres | cassandra clickhouse dcdb geminidb-cassandra geminidb-mongo keyspaces mongodb mssql oceanbase polardbx teradata | the rest (20) | bigquery bigtable copybook delta dynamodb geminidb-redis iceberg kafka kinesis neptune parquet pubsub redis redshift saas sapodp snowflake timestream |
| move.create-missing | cosmosdb dsql dynamodb hetero mysql opensearch oracle postgres teradata tidb | bigquery cassandra clickhouse cockroachdb dcdb gaussdb geminidb-cassandra kafka keyspaces mssql oceanbase polardbx redshift snowflake | the rest (19) | datastore firestore geminidb-mongo geminidb-redis kinesis mongodb neptune pubsub redis saas sapodp |
| cutover.freeze | cassandra clickhouse cockroachdb kafka mongodb mssql mysql opensearch postgres redis redshift snowflake teradata tidb | ase couchbase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis hetero oceanbase oceanbase-oracle oracle polardbx | neo4j qdrant | the rest (25) |
| cutover.read-only-target | the rest (22) | db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis hetero oceanbase oceanbase-oracle polardbx | neo4j qdrant | bigtable copybook cosmosdb datastore delta duckdb firestore generic iceberg keyspaces kinesis neptune parquet pubsub saas sapodp spanner sqlite timestream |
| cutover.counters | dcdb duckdb gaussdb hetero mssql mysql oceanbase-oracle oracle polardbx postgres spanner sqlite tidb | cockroachdb db2 db2i db2zos dsql oceanbase | ase redshift snowflake | the rest (32) |
| cutover.rollback | the rest (20) | bigquery bigtable copybook datastore geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm mongodb neptune oceanbase polardbx | ase cockroachdb cosmosdb couchbase db2 db2i db2zos delta dsql iceberg neo4j qdrant redshift snowflake tidb | generic kinesis pubsub saas sapodp teradata |
| cutover.drain | the rest (21) | bigquery bigtable db2i geminidb-cassandra geminidb-mongo geminidb-redis keyspaces neptune oceanbase polardbx saas | ase db2 delta generic hetero iceberg lindorm neo4j oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb kinesis parquet pubsub qdrant sqlite teradata timestream |
| cutover.pause-replication | mysql postgres | - | generic neo4j qdrant redis | the rest (48) |
| cutover.two-way | mysql postgres teradata | - | the rest (40) | copybook db2zos duckdb hetero kinesis parquet pubsub qdrant saas sapodp sqlite |
| cutover.jobs | mysql postgres | clickhouse mssql oracle | ase bigquery cockroachdb couchbase db2 db2i db2zos dcdb gaussdb generic lindorm oceanbase oceanbase-oracle opensearch polardbx redshift snowflake teradata timestream | the rest (30) |
| security.tls | the rest (44) | oracle | ase db2 db2zos generic mssql neo4j qdrant saas | hetero |
| managed.catalogued | the rest (36) | - | ase couchbase db2i db2zos delta duckdb generic iceberg neo4j parquet qdrant saas sapodp snowflake sqlite tidb | copybook hetero |
| operations.fencing | mysql postgres | cockroachdb dcdb dsql gaussdb oceanbase polardbx tidb | the rest (42) | hetero saas sapodp |

## Backlog (506 items, `docs/backlog-index.md`)

DONE 353 - PARTLY 79 - IN PROGRESS 4 - OPEN 41 - BLOCKED 20 - NOT PURSUED 9

| area | DONE | PARTLY | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED |
|---|---|---|---|---|---|---|
| change follow | 45 | 7 | 0 | 6 | 5 | 0 |
| conversion | 28 | 2 | 0 | 1 | 0 | 0 |
| correctness | 3 | 0 | 0 | 0 | 0 | 0 |
| correctness/operations | 1 | 1 | 0 | 0 | 0 | 0 |
| correctness/performance | 0 | 1 | 0 | 0 | 0 | 0 |
| cutover | 18 | 2 | 0 | 0 | 0 | 0 |
| ease of use | 34 | 6 | 0 | 0 | 1 | 1 |
| engines | 32 | 12 | 1 | 5 | 6 | 1 |
| maintainability | 0 | 0 | 0 | 5 | 0 | 0 |
| move | 31 | 15 | 2 | 4 | 0 | 2 |
| operations/scale | 42 | 15 | 0 | 9 | 5 | 3 |
| operations/verify | 0 | 1 | 0 | 0 | 0 | 0 |
| security | 10 | 5 | 0 | 1 | 2 | 1 |
| tools used whole | 40 | 4 | 0 | 3 | 0 | 0 |
| two-way | 6 | 2 | 0 | 1 | 0 | 0 |
| types | 12 | 1 | 1 | 0 | 0 | 0 |
| verify | 51 | 5 | 0 | 6 | 1 | 1 |

Not done (144): P0-DE.1, P0-DE.2, P0-DE.4, BAR-1, BAR-3, 0f-DVT, 0f-REST, 0a.measure, 0b.window, 6.mariadb, 6.fkwin, 7.free, R9.docdb, A2/D10, C1.managed, P3.dryrun, R6.cloud, 26.versions, 26.striim, MSSQL-x86, AUD.release, P0-DEPTH.calibrate, P0-DEPTH.json, P0-DEPTH.triggers, P0-DEPTH.rank, P0-AUDIT, AUD-C, AUD-E.pin, AUD-G2, AUD-H, AUD-T2, AUD-Z, P0-PARITY.small, P0-PARITY.lv.new, P0-CLOUDS.native, P0-CLOUDS.more, DEC1001.media, DEC1001.gui, P0-SCENARIOS.brands, P0-SCENARIOS.managed, P0-SCENARIOS.arch, P0-SCENARIOS.charset, P0-SCENARIOS.engine, P0-SCENARIOS.consolidate, DEC30.sweep, DEC.deps, S1.e, S4, F2.freeze, F3, F7.beat, F7.edges, F7.sec, CV.P9, ER1.acct, ER4, ER5, ER6, W5, LT.R1, LT.R2, LT.R3, LT.R6, LT.R8, LT.R9, LT.C1, LT.C4, LT.C5, LT.C6, LT.P1, LT.P2, LT.P3, W1, W2, W4.a, W4.b, W6, W7, W8, W9, 28.rates, R5.actions, R5.anchor, 45.run, R4.msk, R3.topo, 38.real, R17b.hmac, R17b.prog, DOCS, 43.fix, R8.more, R8.mv, R10.pieces, R12.minio, R13-DB2, R13-ASE, R13-HETERO, R13-GENERIC, R16b, R16c, R17d, R18.6/R20.2, R18.7, R19.1, R19.3, R19.9/R20.3, R20.5, R20.6, R20.7, R20.10, R20.11, R21.8, R21.9, R21.M, DTS.sample, REL, REL.gate, R25.2, R25.3, P0-SCOPE.open, P0-PARITY.scope, P0-PARITY.verify, P0-PARITY.tail, P0-PARITY.all, P0-WORKFLOW-DEPTH.model, P0-MINIMAL-INPUT.ownership, P0-MINIMAL-INPUT.acceptance, P0-AUTO-RESOURCES.proof, P0-DECISION-ALL.engines, P0-SAFETY-INTEGRITY, P0-RESILIENCE-ALL, P0-AUTONOMOUS-MIGRATION, P0-PRODUCT-TELEMETRY, P0-AUTOTUNE-ALL, P1-SOURCE-HIERARCHY.inventory, P1-SOURCE-HIERARCHY.layout, P1-SOURCE-HIERARCHY.engines, P1-SOURCE-HIERARCHY.navigation, P1-SOURCE-HIERARCHY.proof, P0-VALIDATE.unattended.proof, P0-ETA-ALL, P0-CONNECTION-PROGRESS, P0-VERIFY-EXECUTION

