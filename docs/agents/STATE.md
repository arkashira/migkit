# migkit state

Written by `tools/gen_codemap.py` from `capabilities.matrix()`, `decide.coverage()`, `capabilities.depth()` and `docs/backlog-index.md`; the pre-commit hook keeps it current. What this machine last proved (the full suite, unpushed work) is in `.codemap/LOCAL.md`.

## Capabilities by engine

16 capabilities (`capabilities.CAPABILITIES`) and 12 shapes (`decide.coverage()`); a row lists what is not yet there.

| engine | capabilities | shapes | not yet |
|---|---|---|---|
| ase | 6/16 | 11/12 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned |
| bigquery | 15/16 | 12/12 | users |
| bigtable | 16/16 | 11/12 | large values |
| cassandra | 16/16 | 12/12 | - |
| clickhouse | 16/16 | 12/12 | - |
| cockroachdb | 12/16 | 11/12 | params, bulk-move, users, snapshot, large values |
| copybook | 16/16 | 12/12 | - |
| cosmosdb | 14/16 | 12/12 | bulk-move, snapshot |
| couchbase | 11/16 | 12/12 | params, bulk-move, users, statistics, snapshot |
| datastore | 16/16 | 11/12 | large values |
| db2 | 7/16 | 12/12 | params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| db2i | 13/16 | 12/12 | params, bulk-move, snapshot |
| db2zos | 11/16 | 12/12 | params, bulk-move, users, statistics, snapshot |
| dcdb | 15/16 | 11/12 | bulk-move, large values |
| delta | 9/16 | 11/12 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| dsql | 13/16 | 11/12 | users, statistics, snapshot, large values |
| duckdb | 16/16 | 12/12 | - |
| dynamodb | 16/16 | 12/12 | - |
| firestore | 16/16 | 11/12 | large values |
| gaussdb | 16/16 | 10/12 | large values, partitioned |
| geminidb-cassandra | 16/16 | 11/12 | large values |
| geminidb-mongo | 16/16 | 11/12 | large values |
| geminidb-redis | 16/16 | 11/12 | large values |
| generic | 4/16 | 3/12 | sequences, params, bulk-move, table-copy, stream, fence, confirm, delta, users, guard, statistics, snapshot, keyless, no unique index, wide key, too large, large values, partitioned, read-only source, missing grant, slow link |
| hetero | 16/16 | 10/12 | partitioned, missing grant |
| iceberg | 9/16 | 11/12 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| kafka | 16/16 | 12/12 | - |
| keyspaces | 16/16 | 11/12 | large values |
| kinesis | 16/16 | 12/12 | - |
| lindorm | 12/16 | 10/12 | stream, fence, confirm, delta, large values, partitioned |
| mongodb | 16/16 | 11/12 | missing grant |
| mssql | 16/16 | 12/12 | - |
| mysql | 16/16 | 12/12 | - |
| neptune | 16/16 | 11/12 | large values |
| oceanbase | 15/16 | 11/12 | bulk-move, large values |
| oceanbase-oracle | 11/16 | 10/12 | bulk-move, stream, fence, confirm, delta, large values, partitioned |
| opensearch | 16/16 | 12/12 | - |
| oracle | 16/16 | 12/12 | - |
| parquet | 16/16 | 12/12 | - |
| polardbx | 15/16 | 11/12 | bulk-move, large values |
| postgres | 16/16 | 12/12 | - |
| pubsub | 16/16 | 12/12 | - |
| redis | 16/16 | 12/12 | - |
| redshift | 6/16 | 12/12 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| saas | 16/16 | 12/12 | - |
| sapodp | 16/16 | 12/12 | - |
| snowflake | 6/16 | 12/12 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| spanner | 16/16 | 11/12 | large values |
| sqlite | 16/16 | 12/12 | - |
| teradata | 16/16 | 12/12 | - |
| tidb | 13/16 | 12/12 | bulk-move, users, snapshot |
| timestream | 16/16 | 11/12 | large values |

## Depth by engine

87 items in 10 areas (`capabilities.depth()`, backlog P0-PARITY), each read off the code an engine runs; a way counts once a test proves it on that engine (`PROVES` in the test module). Cells: n/a 1340, not yet 1261, test owed 1006, yes 917. A cell below is the ways proved of the items that apply.

| engine | scope | verify | tail | assess | schema | move | cutover | security | managed | operations | all |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ase | 0/10 | 0/34 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 0/1 | 0/1 | 5/80 |
| bigquery | 5/9 | 1/28 | 0/6 | 1/6 | 1/6 | 1/6 | 1/5 | 1/1 | 1/1 | 0/1 | 12/69 |
| bigtable | 1/9 | 0/17 | 1/5 | 0/6 | 1/3 | 2/5 | 0/3 | 1/1 | 1/1 | 0/1 | 7/51 |
| cassandra | 6/9 | 15/23 | 1/5 | 0/7 | 1/3 | 2/6 | 4/5 | 1/1 | 1/1 | 0/1 | 31/61 |
| clickhouse | 7/9 | 6/29 | 1/6 | 1/7 | 1/5 | 5/7 | 4/6 | 1/1 | 1/1 | 0/1 | 27/72 |
| cockroachdb | 5/10 | 5/34 | 3/6 | 1/7 | 3/6 | 1/6 | 3/7 | 1/1 | 1/1 | 0/1 | 23/79 |
| copybook | 0/7 | 0/15 | - | 0/4 | 1/2 | 2/4 | 0/1 | 1/1 | - | 0/1 | 4/35 |
| cosmosdb | 6/9 | 14/20 | 4/5 | 2/6 | 1/3 | 3/5 | 1/3 | 1/1 | 1/1 | 0/1 | 33/54 |
| couchbase | 0/9 | 1/22 | 1/5 | 1/7 | 1/4 | 1/6 | 2/6 | 1/1 | 0/1 | 0/1 | 8/62 |
| datastore | 0/8 | 0/16 | - | 0/6 | 0/2 | 2/5 | 0/2 | 1/1 | 1/1 | 0/1 | 4/42 |
| db2 | 1/10 | 0/36 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 1/1 | 0/1 | 7/82 |
| db2i | 0/10 | 1/36 | 0/6 | 0/7 | 0/6 | 0/7 | 0/6 | 1/1 | 0/1 | 0/1 | 2/81 |
| db2zos | 0/9 | 0/33 | - | 1/7 | 0/6 | 0/7 | 0/4 | 0/1 | 0/1 | 0/1 | 1/69 |
| dcdb | 0/10 | 2/34 | 1/6 | 0/7 | 1/6 | 0/7 | 3/7 | 1/1 | 1/1 | 0/1 | 9/80 |
| delta | 0/9 | 0/23 | 0/6 | 0/5 | 1/2 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/57 |
| dsql | 5/10 | 21/32 | 1/6 | 3/7 | 5/6 | 3/5 | 2/5 | 1/1 | 1/1 | 0/1 | 42/74 |
| duckdb | 1/9 | 2/23 | - | 0/7 | 1/6 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 11/56 |
| dynamodb | 6/8 | 12/13 | 4/5 | 5/6 | 2/2 | 4/4 | 3/4 | 1/1 | 1/1 | 0/1 | 38/45 |
| firestore | 0/9 | 2/20 | 1/5 | 0/6 | 0/2 | 2/5 | 2/3 | 1/1 | 1/1 | 0/1 | 9/53 |
| gaussdb | 0/10 | 3/38 | 1/6 | 0/7 | 1/6 | 2/7 | 3/7 | 1/1 | 1/1 | 0/1 | 12/84 |
| geminidb-cassandra | 0/9 | 1/21 | 0/1 | 0/7 | 0/4 | 0/6 | 0/5 | 1/1 | 1/1 | 0/1 | 3/56 |
| geminidb-mongo | 0/9 | 1/23 | 0/6 | 0/7 | 0/3 | 0/5 | 0/5 | 1/1 | 1/1 | 0/1 | 3/61 |
| geminidb-redis | 0/7 | 1/12 | - | 0/7 | 0/1 | 1/3 | 0/5 | 1/1 | 1/1 | 0/1 | 4/38 |
| generic | 1/10 | 2/39 | 0/6 | 1/7 | 0/6 | 0/7 | 0/6 | 0/1 | 0/1 | 0/1 | 4/84 |
| hetero | 4/10 | 6/39 | 1/2 | 2/5 | - | 6/8 | 2/5 | - | - | - | 21/69 |
| iceberg | 0/9 | 0/23 | 0/6 | 0/5 | 1/3 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/58 |
| kafka | 5/9 | 15/16 | 5/5 | 5/7 | 0/1 | 5/6 | 4/5 | 1/1 | 1/1 | 0/1 | 41/52 |
| keyspaces | 0/9 | 0/23 | 1/5 | 0/7 | 1/3 | 1/6 | 0/3 | 1/1 | 1/1 | 0/1 | 5/59 |
| kinesis | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| lindorm | 0/9 | 2/26 | 0/6 | 0/7 | 0/5 | 0/6 | 1/5 | 1/1 | 1/1 | 0/1 | 5/67 |
| mongodb | 8/9 | 21/23 | 1/6 | 6/7 | 1/3 | 4/6 | 3/5 | 1/1 | 1/1 | 0/1 | 46/62 |
| mssql | 7/10 | 17/34 | 3/6 | 0/7 | 4/6 | 4/7 | 5/7 | 0/1 | 1/1 | 0/1 | 41/80 |
| mysql | 9/10 | 28/31 | 6/6 | 7/7 | 6/6 | 7/8 | 8/8 | 1/1 | 1/1 | 1/1 | 74/79 |
| neptune | 0/8 | 0/19 | 1/5 | 0/7 | 0/3 | 2/4 | 0/3 | 1/1 | 1/1 | 0/1 | 5/52 |
| oceanbase | 0/10 | 1/35 | 0/6 | 0/7 | 1/6 | 1/7 | 0/7 | 1/1 | 1/1 | 0/1 | 5/81 |
| oceanbase-oracle | 0/10 | 1/37 | 0/6 | 0/7 | 0/6 | 0/7 | 2/7 | 1/1 | 1/1 | 0/1 | 5/83 |
| opensearch | 7/9 | 17/24 | 1/6 | 2/7 | 2/4 | 6/6 | 4/6 | 1/1 | 1/1 | 0/1 | 41/65 |
| oracle | 9/10 | 14/36 | 3/6 | 1/7 | 4/6 | 5/7 | 4/7 | 0/1 | 1/1 | 0/1 | 41/82 |
| parquet | 0/7 | 0/15 | - | 0/4 | 1/2 | 3/6 | 1/1 | 1/1 | 0/1 | 0/1 | 6/38 |
| polardbx | 0/10 | 1/35 | 0/6 | 0/7 | 0/6 | 0/7 | 1/7 | 1/1 | 1/1 | 0/1 | 4/81 |
| postgres | 9/10 | 37/37 | 5/6 | 7/7 | 6/6 | 8/8 | 8/8 | 1/1 | 1/1 | 1/1 | 83/85 |
| pubsub | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| redis | 5/8 | 12/12 | 1/6 | 7/7 | 0/1 | 4/4 | 4/6 | 1/1 | 1/1 | 0/1 | 35/47 |
| redshift | 5/10 | 0/31 | 0/6 | 3/7 | 1/5 | 1/4 | 2/7 | 1/1 | 1/1 | 0/1 | 14/73 |
| saas | 0/6 | 0/3 | 0/5 | 1/5 | - | 0/1 | 0/1 | 0/1 | 0/1 | - | 1/23 |
| sapodp | 0/6 | 0/3 | 0/5 | 0/5 | 0/1 | 0/1 | 1/1 | 1/1 | 0/1 | - | 2/24 |
| snowflake | 4/10 | 0/32 | 0/6 | 3/7 | 1/6 | 0/5 | 2/7 | 1/1 | 0/1 | 0/1 | 11/76 |
| spanner | 0/10 | 2/30 | 1/6 | 0/6 | 1/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 11/71 |
| sqlite | 2/9 | 4/27 | - | 2/7 | 2/5 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 17/59 |
| teradata | 6/8 | 12/29 | - | 2/7 | 3/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 30/63 |
| tidb | 9/10 | 25/31 | 4/6 | 5/7 | 5/5 | 5/7 | 4/6 | 1/1 | 0/1 | 0/1 | 58/75 |
| timestream | 0/7 | 0/18 | - | 0/6 | 0/1 | 1/5 | 1/3 | 1/1 | 1/1 | 0/1 | 4/43 |

Per item: the engines in each state - "test owed" is a way in the code whose test on that engine is not written (`UNPROVED`).

| item | yes | test owed | not yet | n/a |
|---|---|---|---|---|
| scope.listings | the rest (20) | ase datastore db2 dcdb firestore gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm neptune oceanbase oceanbase-oracle polardbx spanner timestream | copybook couchbase db2i db2zos delta duckdb generic hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.deep | cassandra clickhouse cockroachdb cosmosdb db2 dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis teradata tidb | the rest (23) | copybook couchbase delta generic iceberg neptune parquet sqlite | kinesis pubsub saas sapodp timestream |
| scope.copy | the rest (19) | ase bigtable datastore db2 dcdb firestore gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm neptune oceanbase oceanbase-oracle polardbx spanner timestream | copybook couchbase db2i db2zos delta duckdb generic hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.repair | cassandra clickhouse cosmosdb dynamodb generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis redshift snowflake sqlite teradata tidb | the rest (23) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| scope.counters | mssql mysql oracle postgres tidb | cockroachdb db2 db2i db2zos dcdb dsql duckdb gaussdb oceanbase oceanbase-oracle polardbx | ase generic hetero redshift snowflake spanner sqlite | the rest (29) |
| scope.objects | kafka mongodb mssql mysql oracle postgres tidb | the rest (22) | ase bigtable couchbase datastore delta firestore generic hetero iceberg lindorm polardbx spanner timestream | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub redis saas sapodp |
| scope.ops | bigquery cassandra clickhouse cockroachdb cosmosdb dsql dynamodb hetero mongodb mssql mysql opensearch oracle postgres redis redshift snowflake tidb | the rest (19) | ase db2 generic kafka lindorm oceanbase-oracle | copybook datastore db2zos duckdb geminidb-redis parquet sqlite teradata timestream |
| scope.row-filter | bigquery clickhouse cockroachdb dsql duckdb hetero mongodb mysql opensearch oracle postgres redshift sqlite teradata tidb | the rest (29) | geminidb-redis generic kafka redis saas sapodp | kinesis pubsub |
| scope.transforms | - | - | the rest (50) | kinesis pubsub |
| scope.names | bigquery cassandra clickhouse cosmosdb dsql dynamodb hetero mongodb mysql opensearch oracle postgres teradata tidb | the rest (30) | geminidb-redis generic kafka redis saas sapodp | kinesis pubsub |
| verify.access.object-ownership | dsql mysql postgres tidb | clickhouse cockroachdb db2 db2i db2zos dcdb mssql oceanbase oracle redshift snowflake teradata | ase gaussdb generic hetero lindorm oceanbase-oracle polardbx spanner | the rest (28) |
| verify.access.sequence-grants | postgres | cockroachdb dsql mssql oracle snowflake | db2 db2i db2zos dcdb gaussdb generic hetero oceanbase oceanbase-oracle polardbx spanner | the rest (35) |
| verify.access.table-grants | cassandra kafka mysql opensearch postgres redis tidb | bigquery clickhouse cockroachdb dcdb dsql geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mongodb mssql oceanbase oracle redshift snowflake teradata | ase couchbase db2 db2i db2zos gaussdb generic hetero lindorm oceanbase-oracle polardbx spanner | the rest (17) |
| verify.identity.sequence-collision | mssql mysql oracle postgres tidb | cockroachdb db2 db2i db2zos dcdb dsql oceanbase | ase duckdb gaussdb generic hetero oceanbase-oracle polardbx redshift snowflake spanner sqlite | the rest (29) |
| verify.identity.sequence-parity | mssql mysql oracle postgres tidb | cockroachdb db2 db2i db2zos dcdb dsql oceanbase oceanbase-oracle snowflake | ase duckdb gaussdb generic hetero polardbx redshift spanner sqlite | the rest (29) |
| verify.movement.target-ahead | cassandra dsql kafka mongodb mssql mysql postgres redis tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces oceanbase opensearch oracle redshift snowflake teradata | the rest (22) | kinesis pubsub saas sapodp |
| verify.structure.collection-options | cosmosdb dynamodb kafka mongodb opensearch postgres | bigquery cassandra clickhouse db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mysql oceanbase oracle redshift snowflake tidb | the rest (22) | copybook geminidb-redis kinesis parquet pubsub redis saas sapodp |
| verify.structure.columns | cassandra cockroachdb dsql mssql mysql opensearch postgres teradata tidb | bigquery clickhouse db2 db2i db2zos dcdb geminidb-cassandra keyspaces oceanbase oracle redshift snowflake | ase copybook delta duckdb gaussdb generic hetero iceberg lindorm oceanbase-oracle parquet polardbx spanner sqlite | the rest (17) |
| verify.structure.deferrable | dsql postgres | oracle | gaussdb generic hetero oceanbase-oracle spanner sqlite | the rest (43) |
| verify.structure.extensions | postgres | - | gaussdb generic hetero | the rest (48) |
| verify.structure.foreign-keys | dsql mssql mysql postgres tidb | bigquery cockroachdb db2 db2i db2zos dcdb oceanbase oracle redshift snowflake teradata | ase duckdb gaussdb generic hetero neptune oceanbase-oracle polardbx spanner sqlite | the rest (26) |
| verify.structure.indexes | cosmosdb dsql dynamodb firestore gaussdb lindorm mongodb postgres teradata tidb | bigquery cassandra clickhouse cockroachdb datastore db2 db2i db2zos geminidb-cassandra geminidb-mongo keyspaces mssql oracle spanner | ase bigtable couchbase dcdb duckdb generic hetero neptune oceanbase oceanbase-oracle polardbx snowflake sqlite | the rest (15) |
| verify.structure.keys | cockroachdb dsql mssql mysql postgres redis teradata tidb | bigquery clickhouse db2 db2i db2zos dcdb geminidb-redis oceanbase oracle redshift snowflake | ase copybook duckdb gaussdb generic hetero iceberg oceanbase-oracle polardbx sqlite | the rest (23) |
| verify.structure.materialized-views | postgres | bigquery cassandra clickhouse cockroachdb db2 db2i db2zos geminidb-cassandra keyspaces oracle redshift snowflake | ase bigtable cosmosdb gaussdb generic hetero iceberg lindorm oceanbase oceanbase-oracle polardbx | the rest (28) |
| verify.structure.objects | cockroachdb duckdb kafka mongodb mssql mysql oracle postgres tidb | bigquery cassandra clickhouse cosmosdb db2 db2i db2zos dsql geminidb-cassandra geminidb-mongo keyspaces oceanbase-oracle opensearch redshift snowflake teradata | the rest (17) | copybook dynamodb geminidb-redis kinesis neptune parquet pubsub redis saas sapodp |
| verify.structure.partitions | kafka mysql postgres tidb | bigquery clickhouse db2 db2i db2zos dcdb mssql oceanbase oracle teradata | ase delta gaussdb generic hetero iceberg lindorm oceanbase-oracle parquet polardbx timestream | the rest (27) |
| verify.structure.row-security | opensearch postgres | bigquery clickhouse db2 db2i db2zos mssql oracle redshift snowflake | ase cockroachdb delta firestore gaussdb generic hetero iceberg oceanbase-oracle teradata | the rest (31) |
| verify.structure.schema-diff | dsql mysql postgres | bigquery cassandra cockroachdb db2 db2i db2zos gaussdb geminidb-cassandra keyspaces | the rest (25) | cosmosdb couchbase datastore dynamodb firestore geminidb-mongo geminidb-redis kafka kinesis mongodb neptune pubsub redis saas sapodp |
| verify.structure.sharding | cosmosdb mongodb | clickhouse db2 db2i db2zos geminidb-mongo opensearch redshift teradata | dcdb gaussdb generic hetero oceanbase oceanbase-oracle polardbx timestream | the rest (34) |
| verify.structure.triggers | mssql postgres | cassandra cockroachdb cosmosdb db2 db2i db2zos dcdb geminidb-cassandra keyspaces mysql oceanbase oracle teradata | ase couchbase gaussdb generic hetero oceanbase-oracle polardbx sqlite | the rest (29) |
| verify.structure.unvalidated-constraints | dsql mysql oracle postgres tidb | cockroachdb db2 db2i db2zos dcdb mssql oceanbase | ase gaussdb generic hetero oceanbase-oracle polardbx spanner sqlite teradata | the rest (31) |
| verify.value.charset | cassandra mysql oracle postgres teradata tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb dsql geminidb-cassandra geminidb-mongo keyspaces mongodb mssql oceanbase | the rest (23) | cosmosdb dynamodb kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.collation | dsql mongodb mysql opensearch postgres teradata tidb | bigquery cockroachdb db2 db2i db2zos dcdb geminidb-mongo mssql oceanbase oracle | ase delta duckdb gaussdb generic hetero oceanbase-oracle polardbx redshift snowflake sqlite | the rest (24) |
| verify.value.generated | cosmosdb dsql mysql opensearch postgres teradata tidb | clickhouse cockroachdb db2 db2i db2zos dcdb mssql oceanbase oracle | ase delta duckdb gaussdb generic hetero lindorm oceanbase-oracle polardbx redshift snowflake spanner sqlite | the rest (23) |
| verify.value.narrowing | cassandra cockroachdb dsql mongodb mysql opensearch postgres teradata tidb | bigquery clickhouse db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle | the rest (22) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.null-empty | cassandra cosmosdb dsql dynamodb kafka mongodb mysql opensearch postgres tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle teradata | the rest (22) | geminidb-redis kinesis pubsub redis saas sapodp |
| verify.value.precision | cassandra dsql mongodb mysql opensearch postgres tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle teradata | the rest (22) | bigtable dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.rendering | dsql mongodb postgres | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-mongo mssql mysql oceanbase oracle | the rest (25) | bigtable cassandra cosmosdb dynamodb geminidb-redis kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.timezone | cassandra dsql mongodb mysql postgres teradata tidb | bigquery clickhouse cockroachdb db2 db2i db2zos dcdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase opensearch oracle | the rest (21) | bigtable cosmosdb dynamodb geminidb-redis kafka kinesis pubsub redis saas sapodp |
| verify.value.type-drift | cosmosdb dynamodb mongodb opensearch | geminidb-mongo kafka | bigtable couchbase datastore firestore geminidb-redis generic hetero neptune sqlite timestream | the rest (36) |
| verify.delta | cassandra clickhouse cosmosdb dynamodb hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | the rest (16) | ase db2 delta generic iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra kinesis parquet pubsub saas sapodp sqlite teradata timestream |
| verify.fence | the rest (21) | bigquery bigtable db2i geminidb-mongo keyspaces neptune oceanbase polardbx | ase db2 delta generic iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub saas sapodp sqlite teradata timestream |
| verify.confirm | cassandra cosmosdb dynamodb hetero kafka mongodb mssql mysql opensearch oracle postgres redis tidb | the rest (16) | ase db2 delta generic iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub saas sapodp sqlite teradata timestream |
| verify.users | the rest (23) | - | ase bigquery cockroachdb couchbase db2 db2zos dsql generic redshift snowflake tidb | bigtable copybook cosmosdb datastore delta duckdb dynamodb firestore iceberg keyspaces kinesis neptune parquet pubsub saas sapodp sqlite timestream |
| verify.repair | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | the rest (27) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.repair-undo | cosmosdb dsql dynamodb generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite teradata tidb | the rest (27) | ase copybook delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.rows-apart | cassandra clickhouse cosmosdb dsql dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | cockroachdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces | the rest (30) | kinesis pubsub |
| verify.tables | cassandra clickhouse cosmosdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis | geminidb-cassandra geminidb-mongo geminidb-redis keyspaces redshift snowflake | the rest (32) | kinesis pubsub |
| verify.table | bigquery cassandra clickhouse cosmosdb dsql duckdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite teradata tidb | the rest (31) | generic | kinesis pubsub |
| tail.follows | the rest (24) | bigquery db2i geminidb-mongo oceanbase polardbx | ase db2 delta generic iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis kinesis parquet pubsub saas sapodp sqlite teradata timestream |
| tail.table-states | cockroachdb cosmosdb dynamodb kafka mysql postgres tidb | the rest (19) | ase cassandra clickhouse db2 delta geminidb-mongo generic iceberg lindorm mongodb oceanbase-oracle opensearch redis redshift snowflake | copybook datastore db2zos duckdb geminidb-redis kinesis parquet pubsub sqlite teradata timestream |
| tail.added | cockroachdb kafka mssql mysql oracle postgres tidb | bigquery oceanbase polardbx spanner | the rest (28) | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis hetero kinesis parquet pubsub sqlite teradata timestream |
| tail.readded | cosmosdb dynamodb kafka mssql mysql oracle tidb | bigquery oceanbase polardbx | the rest (29) | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis hetero kinesis parquet pubsub sqlite teradata timestream |
| tail.ddl | cosmosdb dynamodb kafka mysql postgres | oceanbase oracle polardbx | the rest (31) | copybook datastore db2zos duckdb geminidb-cassandra geminidb-redis hetero kinesis parquet pubsub sqlite teradata timestream |
| tail.renames | mysql postgres | bigquery mssql oceanbase oracle polardbx | the rest (23) | bigtable cassandra copybook cosmosdb couchbase datastore db2zos duckdb dynamodb firestore geminidb-cassandra geminidb-redis hetero kafka keyspaces kinesis neptune parquet pubsub sqlite teradata timestream |
| assess.versions | dsql kafka mongodb mysql opensearch postgres redis redshift snowflake sqlite teradata tidb | cassandra clickhouse cockroachdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oracle | the rest (15) | bigquery bigtable copybook cosmosdb datastore delta dynamodb firestore hetero iceberg kinesis parquet pubsub spanner timestream |
| assess.brands | kafka mongodb mysql opensearch postgres redis redshift snowflake tidb | cassandra clickhouse cockroachdb dcdb dsql gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces mssql oceanbase oracle polardbx teradata | the rest (21) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.engine-checks | bigquery cockroachdb cosmosdb couchbase db2zos dsql dynamodb generic hetero kafka mongodb mysql oracle postgres redis redshift saas snowflake sqlite teradata tidb | the rest (22) | ase copybook db2 delta duckdb iceberg kinesis parquet pubsub | - |
| assess.preflight | cosmosdb dsql dynamodb kafka mongodb mysql postgres redis tidb | the rest (21) | ase bigtable copybook couchbase datastore delta duckdb firestore generic hetero iceberg lindorm neptune oceanbase-oracle parquet spanner sqlite timestream | kinesis pubsub saas sapodp |
| assess.capacity | clickhouse dynamodb hetero mongodb mysql postgres redis tidb | bigquery cassandra cosmosdb dcdb geminidb-cassandra geminidb-mongo geminidb-redis kafka keyspaces mssql oceanbase opensearch oracle polardbx redshift snowflake teradata | the rest (25) | saas sapodp |
| assess.writers | dynamodb mysql postgres redis | bigquery clickhouse cosmosdb geminidb-mongo kafka mongodb mssql opensearch oracle teradata | the rest (36) | kinesis pubsub |
| assess.handwork | dynamodb kafka mongodb mysql postgres redis | bigquery cassandra clickhouse cockroachdb cosmosdb dsql gaussdb geminidb-mongo geminidb-redis mssql opensearch oracle redshift snowflake teradata tidb | the rest (26) | copybook kinesis parquet pubsub |
| schema.create | the rest (31) | datastore db2i db2zos firestore geminidb-cassandra kafka lindorm neptune oceanbase-oracle polardbx timestream | generic | geminidb-mongo geminidb-redis hetero kinesis mongodb pubsub redis saas sapodp |
| schema.indexes | cockroachdb dsql dynamodb mongodb mysql postgres teradata tidb | cassandra clickhouse cosmosdb dcdb gaussdb geminidb-cassandra geminidb-mongo keyspaces mssql oceanbase oracle polardbx spanner sqlite | the rest (15) | copybook delta geminidb-redis hetero iceberg kafka kinesis opensearch parquet pubsub redis redshift saas sapodp timestream |
| schema.foreign-keys | dsql mssql mysql oracle postgres tidb | bigquery cockroachdb dcdb gaussdb neptune oceanbase polardbx redshift snowflake spanner teradata | ase db2 db2i db2zos duckdb generic oceanbase-oracle sqlite | the rest (27) |
| schema.defaults | cockroachdb dsql mysql opensearch postgres sqlite teradata tidb | bigquery clickhouse dcdb gaussdb mssql oceanbase oracle polardbx redshift sapodp snowflake spanner | ase copybook db2 db2i db2zos delta duckdb geminidb-cassandra generic iceberg keyspaces lindorm oceanbase-oracle parquet | the rest (18) |
| schema.views | dsql mssql mysql oracle postgres tidb | bigquery clickhouse cockroachdb dcdb gaussdb geminidb-mongo mongodb oceanbase polardbx redshift snowflake teradata | ase bigtable couchbase db2 db2i db2zos duckdb generic iceberg lindorm oceanbase-oracle opensearch spanner sqlite | the rest (20) |
| schema.routines | mssql mysql oracle postgres | bigquery cockroachdb dcdb geminidb-mongo mongodb oceanbase polardbx | the rest (22) | bigtable copybook datastore delta dynamodb firestore hetero iceberg kafka keyspaces kinesis neptune parquet pubsub saas sapodp sqlite tidb timestream |
| move.table-copy | the rest (36) | bigquery db2i db2zos dcdb geminidb-cassandra geminidb-mongo lindorm oceanbase-oracle polardbx snowflake timestream | generic | kinesis pubsub saas sapodp |
| move.bulk | the rest (22) | bigquery geminidb-cassandra geminidb-mongo hetero keyspaces | ase cockroachdb cosmosdb couchbase db2 db2i db2zos dcdb delta generic iceberg oceanbase oceanbase-oracle polardbx redshift snowflake tidb | dsql geminidb-redis kinesis lindorm pubsub saas sapodp teradata |
| move.large-values | ase clickhouse db2 delta duckdb hetero iceberg kafka mongodb mssql mysql opensearch oracle parquet postgres redis sqlite tidb | bigquery couchbase db2i db2zos snowflake teradata | the rest (19) | cassandra copybook cosmosdb dynamodb kinesis pubsub redshift saas sapodp |
| move.partitioned | clickhouse kafka mysql postgres tidb | bigquery db2 db2i db2zos dcdb mssql oceanbase oracle parquet polardbx teradata | ase delta gaussdb generic hetero iceberg lindorm oceanbase-oracle | the rest (28) |
| move.spread | hetero postgres | cassandra mongodb mysql | - | the rest (47) |
| move.sized | the rest (18) | cassandra cockroachdb db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm oceanbase oceanbase-oracle oracle polardbx redshift snowflake teradata | bigtable copybook couchbase datastore delta firestore generic iceberg neptune parquet saas sapodp spanner timestream | kinesis pubsub |
| move.load-window | hetero mysql opensearch oracle postgres | cassandra clickhouse dcdb geminidb-cassandra geminidb-mongo keyspaces mongodb mssql oceanbase polardbx teradata | the rest (18) | bigquery bigtable copybook delta dynamodb geminidb-redis iceberg kafka kinesis neptune parquet pubsub redis redshift saas sapodp snowflake timestream |
| move.create-missing | cosmosdb dsql dynamodb hetero mysql opensearch oracle postgres teradata tidb | bigquery cassandra clickhouse cockroachdb dcdb gaussdb geminidb-cassandra kafka keyspaces mssql oceanbase polardbx redshift snowflake | the rest (17) | datastore firestore geminidb-mongo geminidb-redis kinesis mongodb neptune pubsub redis saas sapodp |
| cutover.freeze | cassandra clickhouse cockroachdb kafka mongodb mssql mysql opensearch postgres redis redshift snowflake teradata tidb | ase couchbase db2 dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis hetero oceanbase oceanbase-oracle oracle polardbx | - | the rest (25) |
| cutover.read-only-target | the rest (22) | db2i db2zos dcdb gaussdb geminidb-cassandra geminidb-mongo geminidb-redis hetero oceanbase oceanbase-oracle polardbx | - | bigtable copybook cosmosdb datastore delta duckdb firestore generic iceberg keyspaces kinesis neptune parquet pubsub saas sapodp spanner sqlite timestream |
| cutover.counters | dcdb duckdb gaussdb hetero mssql mysql oceanbase-oracle oracle polardbx postgres spanner sqlite tidb | cockroachdb db2 db2i db2zos dsql oceanbase | ase generic redshift snowflake | the rest (29) |
| cutover.rollback | the rest (20) | bigquery bigtable copybook datastore geminidb-cassandra geminidb-mongo geminidb-redis keyspaces lindorm mongodb neptune oceanbase polardbx | ase cockroachdb cosmosdb couchbase db2 db2i db2zos delta dsql generic iceberg redshift snowflake tidb | kinesis pubsub saas sapodp teradata |
| cutover.drain | the rest (21) | bigquery bigtable db2i geminidb-cassandra geminidb-mongo geminidb-redis keyspaces neptune oceanbase polardbx saas | ase db2 delta generic hetero iceberg lindorm oceanbase-oracle redshift snowflake | copybook datastore db2zos duckdb kinesis parquet pubsub sqlite teradata timestream |
| cutover.pause-replication | mysql postgres | - | generic redis | the rest (48) |
| cutover.two-way | mysql postgres teradata | - | the rest (39) | copybook db2zos duckdb hetero kinesis parquet pubsub saas sapodp sqlite |
| cutover.jobs | mysql postgres | clickhouse mssql oracle | ase bigquery cockroachdb couchbase db2 db2i db2zos dcdb gaussdb generic lindorm oceanbase oceanbase-oracle opensearch polardbx redshift snowflake teradata timestream | the rest (28) |
| security.tls | the rest (44) | oracle | ase db2 db2zos generic mssql saas | hetero |
| managed.catalogued | the rest (36) | - | ase couchbase db2i db2zos delta duckdb generic iceberg parquet saas sapodp snowflake sqlite tidb | copybook hetero |
| operations.fencing | mysql postgres | cockroachdb dcdb dsql gaussdb oceanbase polardbx tidb | the rest (40) | hetero saas sapodp |

## Backlog (506 items, `docs/backlog-index.md`)

DONE 372 - PARTLY 43 - IN PROGRESS 4 - OPEN 58 - BLOCKED 20 - NOT PURSUED 9

| area | DONE | PARTLY | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED |
|---|---|---|---|---|---|---|
| change follow | 44 | 5 | 0 | 9 | 5 | 0 |
| conversion | 29 | 1 | 0 | 1 | 0 | 0 |
| correctness | 3 | 0 | 0 | 0 | 0 | 0 |
| correctness/operations | 2 | 0 | 0 | 0 | 0 | 0 |
| correctness/performance | 1 | 0 | 0 | 0 | 0 | 0 |
| cutover | 18 | 1 | 0 | 1 | 0 | 0 |
| ease of use | 36 | 1 | 0 | 3 | 1 | 1 |
| engines | 34 | 5 | 1 | 10 | 6 | 1 |
| maintainability | 0 | 0 | 0 | 5 | 0 | 0 |
| move | 34 | 9 | 2 | 7 | 0 | 2 |
| operations/scale | 51 | 6 | 0 | 9 | 5 | 3 |
| operations/verify | 1 | 0 | 0 | 0 | 0 | 0 |
| security | 10 | 5 | 0 | 1 | 2 | 1 |
| tools used whole | 40 | 4 | 0 | 3 | 0 | 0 |
| two-way | 7 | 1 | 0 | 1 | 0 | 0 |
| types | 12 | 1 | 1 | 0 | 0 | 0 |
| verify | 50 | 4 | 0 | 8 | 1 | 1 |

Not done (125): P0-DE.1, P0-DE.2, P0-DE.4, BAR-1, BAR-3, 0f-DVT, 0f-REST, 0a.measure, 0b.window, 6.mariadb, 6.fkwin, 7.free, R9.docdb, A2/D10, C1.managed, P3.dryrun, R6.cloud, 26.versions, 26.striim, MSSQL-x86, AUD.release, P0-DEPTH.calibrate, P0-DEPTH.json, P0-DEPTH.triggers, P0-DEPTH.rank, P0-AUDIT, AUD-C, AUD-E.pin, AUD-T2, AUD-Z, P0-PARITY.small, P0-PARITY.lv.new, P0-SMART.route, P0-CLOUDS.native, P0-CLOUDS.more, DEC1001.media, DEC1001.gui, P0-SCENARIOS.brands, P0-SCENARIOS.managed, P0-SCENARIOS.arch, P0-SCENARIOS.charset, P0-SCENARIOS.engine, P0-SCENARIOS.consolidate, DEC30.sweep, DEC.deps, DEC.ptcs, S1.e, S4, F2.freeze, F3, F7.beat, F7.edges, F7.sec, ER1.acct, ER4, ER5, ER6, W5, LT.R1, LT.R2, LT.R3, LT.R6, LT.R8, LT.R9, LT.R10, LT.C1, LT.C4, LT.C5, LT.C6, LT.P2, LT.P3, W2, W4.a, W4.b, W6, W7, W8, W9, 28.rates, R5.actions, R5.anchor, 45.run, R4.msk, 38.real, R17b.hmac, R17b.prog, DOCS, 43.fix, R8.more, R8.mv, R10.pieces, R12.minio, R13-DB2, R13-ASE, R13-HETERO, R13-GENERIC, R16b, R16c, R17d, R18.6/R20.2, R18.7, R19.1, R19.3, R19.9/R20.3, R20.10, R20.11, R21.8, R21.9, R21.M, DTS.sample, REL, REL.gate, R25.3, P0-SCOPE.open, P0-PARITY.scope, P0-PARITY.verify, P0-PARITY.tail, P0-PARITY.all, P0-DECISION-ALL.engines, P1-SOURCE-HIERARCHY.inventory, P1-SOURCE-HIERARCHY.layout, P1-SOURCE-HIERARCHY.engines, P1-SOURCE-HIERARCHY.navigation, P1-SOURCE-HIERARCHY.proof, P0-VALIDATE.unattended.proof

