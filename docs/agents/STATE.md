# migkit state

Written by `tools/gen_codemap.py` from `capabilities.matrix()`, `decide.coverage()`, `capabilities.depth()` and `docs/backlog-index.md`; the pre-commit hook keeps it current. What this machine last proved (the full suite, unpushed work) is in `.codemap/LOCAL.md`.

## Capabilities by engine

16 capabilities (`capabilities.CAPABILITIES`) and 11 shapes (`decide.coverage()`); a row lists what is not yet there.

| engine | capabilities | shapes | not yet |
|---|---|---|---|
| ase | 6/16 | 10/11 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned |
| bigquery | 5/16 | 10/11 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned |
| cassandra | 16/16 | 11/11 | - |
| clickhouse | 16/16 | 11/11 | - |
| cockroachdb | 12/16 | 10/11 | params, bulk-move, users, snapshot, large values |
| copybook | 16/16 | 11/11 | - |
| cosmosdb | 14/16 | 11/11 | bulk-move, snapshot |
| couchbase | 11/16 | 10/11 | params, bulk-move, users, statistics, snapshot, large values |
| db2 | 6/16 | 10/11 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned |
| db2i | 12/16 | 10/11 | sequences, params, bulk-move, snapshot, partitioned |
| db2zos | 10/16 | 10/11 | sequences, params, bulk-move, users, statistics, snapshot, partitioned |
| delta | 9/16 | 10/11 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| dsql | 13/16 | 10/11 | users, statistics, snapshot, large values |
| duckdb | 16/16 | 11/11 | - |
| dynamodb | 16/16 | 11/11 | - |
| generic | 4/16 | 2/11 | sequences, params, bulk-move, table-copy, stream, fence, confirm, delta, users, guard, statistics, snapshot, keyless, no unique index, wide key, too large, large values, partitioned, read-only source, missing grant, slow link |
| hetero | 15/16 | 9/11 | users, partitioned, missing grant |
| iceberg | 9/16 | 10/11 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| kafka | 16/16 | 11/11 | - |
| kinesis | 16/16 | 11/11 | - |
| mongodb | 16/16 | 10/11 | missing grant |
| mssql | 16/16 | 11/11 | - |
| mysql | 16/16 | 11/11 | - |
| opensearch | 16/16 | 11/11 | - |
| oracle | 16/16 | 11/11 | - |
| parquet | 16/16 | 10/11 | partitioned |
| postgres | 16/16 | 11/11 | - |
| pubsub | 16/16 | 11/11 | - |
| redis | 16/16 | 11/11 | - |
| redshift | 6/16 | 11/11 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| saas | 16/16 | 11/11 | - |
| sapodp | 16/16 | 11/11 | - |
| snowflake | 6/16 | 11/11 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| sqlite | 16/16 | 11/11 | - |
| teradata | 16/16 | 11/11 | - |
| tidb | 13/16 | 10/11 | bulk-move, users, snapshot, large values |

## Depth by engine

86 items in 10 areas (`capabilities.depth()`, backlog P0-PARITY), each read off the code an engine runs; a way counts once a test proves it on that engine (`PROVES` in the test module). Cells: n/a 916, not yet 981, test owed 386, yes 813. A cell below is the ways proved of the items that apply.

| engine | scope | verify | tail | assess | schema | move | cutover | security | managed | operations | all |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ase | 0/10 | 0/34 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 0/1 | 0/1 | 5/80 |
| bigquery | 1/10 | 0/31 | 0/6 | 1/7 | 1/6 | 1/6 | 1/6 | 1/1 | 1/1 | 0/1 | 7/75 |
| cassandra | 6/9 | 15/23 | 1/5 | 0/7 | 1/3 | 2/5 | 4/5 | 1/1 | 1/1 | 0/1 | 31/60 |
| clickhouse | 7/9 | 6/29 | 1/6 | 1/7 | 1/5 | 5/7 | 4/6 | 1/1 | 1/1 | 0/1 | 27/72 |
| cockroachdb | 5/10 | 5/34 | 3/6 | 1/7 | 3/6 | 1/6 | 3/7 | 1/1 | 1/1 | 0/1 | 23/79 |
| copybook | 0/7 | 0/15 | - | 0/4 | 1/2 | 2/4 | 0/1 | 1/1 | - | 0/1 | 4/35 |
| cosmosdb | 6/9 | 14/20 | 4/5 | 2/6 | 1/3 | 3/5 | 1/3 | 1/1 | 1/1 | 0/1 | 33/54 |
| couchbase | 0/9 | 1/22 | 1/5 | 1/7 | 1/4 | 1/6 | 2/6 | 1/1 | 0/1 | 0/1 | 8/62 |
| db2 | 1/10 | 0/35 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 1/1 | 0/1 | 7/81 |
| db2i | 0/10 | 1/35 | 0/6 | 0/7 | 0/6 | 0/7 | 0/6 | 1/1 | 0/1 | 0/1 | 2/80 |
| db2zos | 0/9 | 0/32 | - | 1/7 | 0/6 | 0/7 | 0/4 | 0/1 | 0/1 | 0/1 | 1/68 |
| delta | 0/9 | 0/23 | 0/6 | 0/5 | 1/2 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/57 |
| dsql | 5/10 | 21/32 | 1/6 | 3/7 | 5/6 | 3/5 | 2/5 | 1/1 | 1/1 | 0/1 | 42/74 |
| duckdb | 1/9 | 2/23 | - | 0/7 | 1/6 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 11/56 |
| dynamodb | 6/8 | 12/13 | 4/5 | 5/6 | 2/2 | 4/4 | 3/4 | 1/1 | 1/1 | 0/1 | 38/45 |
| generic | 1/10 | 2/39 | 0/6 | 1/7 | 0/6 | 0/7 | 0/6 | 0/1 | 0/1 | 0/1 | 4/84 |
| hetero | 4/10 | 5/39 | 1/2 | 2/5 | - | 5/7 | 2/5 | - | - | - | 19/68 |
| iceberg | 0/9 | 0/23 | 0/6 | 0/5 | 1/3 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/58 |
| kafka | 5/9 | 15/16 | 5/5 | 5/7 | 0/1 | 5/6 | 4/5 | 1/1 | 1/1 | 0/1 | 41/52 |
| kinesis | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| mongodb | 8/9 | 21/23 | 1/6 | 6/7 | 1/3 | 4/5 | 3/5 | 1/1 | 1/1 | 0/1 | 46/61 |
| mssql | 7/10 | 17/34 | 3/6 | 0/7 | 4/6 | 4/7 | 5/7 | 0/1 | 1/1 | 0/1 | 41/80 |
| mysql | 9/10 | 28/31 | 6/6 | 7/7 | 6/6 | 7/7 | 8/8 | 1/1 | 1/1 | 1/1 | 74/78 |
| opensearch | 7/9 | 17/24 | 1/6 | 2/7 | 2/4 | 6/6 | 4/6 | 1/1 | 1/1 | 0/1 | 41/65 |
| oracle | 9/10 | 14/36 | 3/6 | 1/7 | 4/6 | 5/7 | 4/7 | 0/1 | 1/1 | 0/1 | 41/82 |
| parquet | 0/7 | 0/15 | - | 0/4 | 1/2 | 3/6 | 1/1 | 1/1 | 0/1 | 0/1 | 6/38 |
| postgres | 9/10 | 37/37 | 5/6 | 7/7 | 6/6 | 7/7 | 8/8 | 1/1 | 1/1 | 1/1 | 82/84 |
| pubsub | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| redis | 5/8 | 12/12 | 1/6 | 7/7 | 0/1 | 4/4 | 4/6 | 1/1 | 1/1 | 0/1 | 35/47 |
| redshift | 5/10 | 0/31 | 0/6 | 3/7 | 1/5 | 1/4 | 2/7 | 1/1 | 1/1 | 0/1 | 14/73 |
| saas | 0/6 | 0/3 | 0/5 | 1/5 | - | 0/1 | 0/1 | 0/1 | 0/1 | - | 1/23 |
| sapodp | 0/6 | 0/3 | 0/5 | 0/5 | 0/1 | 0/1 | 1/1 | 1/1 | 0/1 | - | 2/24 |
| snowflake | 4/10 | 0/32 | 0/6 | 3/7 | 1/6 | 0/5 | 2/7 | 1/1 | 0/1 | 0/1 | 11/76 |
| sqlite | 2/9 | 4/27 | - | 2/7 | 2/5 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 17/59 |
| teradata | 6/8 | 12/29 | - | 2/7 | 3/6 | 2/6 | 3/4 | 1/1 | 1/1 | 0/1 | 30/63 |
| tidb | 9/10 | 25/31 | 4/6 | 5/7 | 5/5 | 4/7 | 4/6 | 1/1 | 0/1 | 0/1 | 57/75 |

Per item: the engines in each state - "test owed" is a way in the code whose test on that engine is not written (`UNPROVED`).

| item | yes | test owed | not yet | n/a |
|---|---|---|---|---|
| scope.listings | the rest (18) | ase | bigquery copybook couchbase db2 db2i db2zos delta duckdb generic hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.deep | the rest (16) | ase db2i db2zos dsql hetero redshift snowflake | bigquery copybook couchbase delta duckdb generic iceberg parquet sqlite | kinesis pubsub saas sapodp |
| scope.copy | the rest (18) | ase | bigquery copybook couchbase db2 db2i db2zos delta duckdb generic hetero iceberg parquet saas sapodp sqlite | kinesis pubsub |
| scope.repair | the rest (19) | cockroachdb couchbase dsql | ase bigquery copybook db2 db2i db2zos delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| scope.counters | mssql mysql oracle postgres tidb | cockroachdb dsql | ase bigquery db2 db2i db2zos duckdb generic hetero redshift snowflake sqlite | the rest (18) |
| scope.objects | kafka mongodb mssql mysql oracle postgres tidb | cassandra clickhouse cockroachdb cosmosdb dsql opensearch redshift snowflake teradata | the rest (12) | copybook dynamodb kinesis parquet pubsub redis saas sapodp |
| scope.ops | the rest (17) | bigquery couchbase db2i delta iceberg kinesis pubsub saas sapodp | ase db2 generic kafka | copybook db2zos duckdb parquet sqlite teradata |
| scope.row-filter | the rest (15) | ase cassandra copybook cosmosdb couchbase db2 db2i db2zos delta dynamodb iceberg mssql parquet snowflake | generic kafka redis saas sapodp | kinesis pubsub |
| scope.transforms | - | - | the rest (34) | kinesis pubsub |
| scope.names | cassandra clickhouse cosmosdb dsql dynamodb hetero mongodb mysql opensearch oracle postgres teradata tidb | the rest (16) | generic kafka redis saas sapodp | kinesis pubsub |
| verify.access.object-ownership | dsql mysql postgres tidb | clickhouse cockroachdb mssql oracle redshift snowflake teradata | ase db2 db2i db2zos generic hetero | the rest (19) |
| verify.access.sequence-grants | postgres | cockroachdb dsql mssql oracle snowflake | db2 db2i db2zos generic hetero | the rest (25) |
| verify.access.table-grants | cassandra kafka mysql opensearch postgres redis tidb | clickhouse cockroachdb dsql mongodb mssql oracle redshift snowflake teradata | ase bigquery couchbase db2 db2i db2zos generic hetero | the rest (12) |
| verify.identity.sequence-collision | mssql mysql oracle postgres tidb | cockroachdb dsql | ase bigquery db2 db2i db2zos duckdb generic hetero redshift snowflake sqlite | the rest (18) |
| verify.identity.sequence-parity | mssql mysql oracle postgres tidb | cockroachdb dsql snowflake | ase bigquery db2 db2i db2zos duckdb generic hetero redshift sqlite | the rest (18) |
| verify.movement.target-ahead | cassandra dsql kafka mongodb mssql mysql postgres redis tidb | clickhouse cockroachdb opensearch oracle redshift snowflake teradata | the rest (16) | kinesis pubsub saas sapodp |
| verify.structure.collection-options | cosmosdb dynamodb kafka mongodb opensearch postgres | cassandra clickhouse mysql oracle redshift snowflake tidb | the rest (16) | copybook kinesis parquet pubsub redis saas sapodp |
| verify.structure.columns | cassandra cockroachdb dsql mssql mysql opensearch postgres teradata tidb | clickhouse oracle redshift snowflake | the rest (13) | cosmosdb couchbase dynamodb kafka kinesis mongodb pubsub redis saas sapodp |
| verify.structure.deferrable | dsql postgres | oracle | generic hetero sqlite | the rest (30) |
| verify.structure.extensions | postgres | - | generic hetero | the rest (33) |
| verify.structure.foreign-keys | dsql mssql mysql postgres tidb | cockroachdb oracle redshift snowflake teradata | ase bigquery db2 db2i db2zos duckdb generic hetero sqlite | the rest (17) |
| verify.structure.indexes | cosmosdb dsql dynamodb mongodb postgres teradata tidb | cassandra clickhouse cockroachdb mssql oracle | ase bigquery couchbase db2 db2i db2zos duckdb generic hetero snowflake sqlite | the rest (13) |
| verify.structure.keys | cockroachdb dsql mssql mysql postgres redis teradata tidb | clickhouse oracle redshift snowflake | ase bigquery copybook db2 db2i db2zos duckdb generic hetero iceberg sqlite | the rest (13) |
| verify.structure.materialized-views | postgres | cassandra clickhouse cockroachdb oracle redshift snowflake | ase bigquery cosmosdb db2 db2i db2zos generic hetero iceberg | the rest (20) |
| verify.structure.objects | cockroachdb duckdb kafka mongodb mssql mysql oracle postgres tidb | cassandra clickhouse cosmosdb dsql opensearch redshift snowflake teradata | the rest (11) | copybook dynamodb kinesis parquet pubsub redis saas sapodp |
| verify.structure.partitions | kafka mysql postgres tidb | clickhouse mssql oracle teradata | ase bigquery db2 db2i db2zos delta generic hetero iceberg parquet | the rest (18) |
| verify.structure.row-security | opensearch postgres | clickhouse mssql oracle redshift snowflake | ase bigquery cockroachdb db2 db2i db2zos delta generic hetero iceberg teradata | the rest (18) |
| verify.structure.schema-diff | dsql mysql postgres | cassandra cockroachdb | the rest (21) | cosmosdb couchbase dynamodb kafka kinesis mongodb pubsub redis saas sapodp |
| verify.structure.sharding | cosmosdb mongodb | clickhouse opensearch redshift teradata | generic hetero | the rest (28) |
| verify.structure.triggers | mssql postgres | cassandra cockroachdb cosmosdb mysql oracle teradata | ase couchbase db2 db2i db2zos generic hetero sqlite | the rest (20) |
| verify.structure.unvalidated-constraints | dsql mysql oracle postgres tidb | cockroachdb mssql | ase db2 db2i db2zos generic hetero sqlite teradata | the rest (21) |
| verify.value.charset | cassandra mysql oracle postgres teradata tidb | clickhouse cockroachdb dsql mongodb mssql | the rest (16) | cosmosdb dynamodb kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.collation | dsql mongodb mysql opensearch postgres teradata tidb | cockroachdb mssql oracle | ase bigquery db2 db2i db2zos delta duckdb generic hetero redshift snowflake sqlite | the rest (14) |
| verify.value.generated | cosmosdb dsql mysql opensearch postgres teradata tidb | clickhouse cockroachdb mssql oracle | ase bigquery db2 db2i db2zos delta duckdb generic hetero redshift snowflake sqlite | the rest (13) |
| verify.value.narrowing | cassandra cockroachdb dsql mongodb mysql opensearch postgres teradata tidb | clickhouse mssql oracle | the rest (17) | dynamodb kafka kinesis pubsub redis saas sapodp |
| verify.value.null-empty | cassandra cosmosdb dsql dynamodb kafka mongodb mysql opensearch postgres tidb | clickhouse cockroachdb mssql oracle teradata | the rest (16) | kinesis pubsub redis saas sapodp |
| verify.value.precision | cassandra dsql mongodb mysql opensearch postgres tidb | clickhouse cockroachdb mssql oracle teradata | the rest (17) | dynamodb kafka kinesis pubsub redis saas sapodp |
| verify.value.rendering | dsql mongodb postgres | clickhouse cockroachdb mssql mysql oracle | the rest (18) | cassandra cosmosdb dynamodb kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.timezone | cassandra dsql mongodb mysql postgres teradata tidb | clickhouse cockroachdb mssql opensearch oracle | the rest (16) | cosmosdb dynamodb kafka kinesis pubsub redis saas sapodp |
| verify.value.type-drift | cosmosdb dynamodb mongodb opensearch | kafka | couchbase generic hetero sqlite | the rest (27) |
| verify.delta | the rest (14) | cockroachdb couchbase db2i dsql | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| verify.fence | the rest (17) | db2i | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| verify.confirm | the rest (13) | clickhouse cockroachdb couchbase db2i dsql | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| verify.users | the rest (12) | - | ase bigquery cockroachdb couchbase db2 db2zos dsql generic hetero redshift snowflake tidb | copybook cosmosdb delta duckdb dynamodb iceberg kinesis parquet pubsub saas sapodp sqlite |
| verify.repair | the rest (15) | cassandra clickhouse cockroachdb couchbase opensearch redshift snowflake | ase bigquery copybook db2 db2i db2zos delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.repair-undo | the rest (15) | cassandra clickhouse cockroachdb couchbase opensearch redshift snowflake | ase bigquery copybook db2 db2i db2zos delta duckdb iceberg parquet | kinesis pubsub saas sapodp |
| verify.rows-apart | cassandra clickhouse cosmosdb dsql dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | cockroachdb | the rest (19) | kinesis pubsub |
| verify.tables | cassandra clickhouse cosmosdb dynamodb kafka mongodb mssql mysql opensearch oracle postgres redis | redshift snowflake | the rest (20) | kinesis pubsub |
| verify.table | the rest (17) | ase bigquery cockroachdb copybook couchbase db2 db2i db2zos delta hetero iceberg parquet redshift saas sapodp snowflake | generic | kinesis pubsub |
| tail.follows | the rest (17) | db2i | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| tail.table-states | cockroachdb cosmosdb dynamodb kafka mysql postgres tidb | couchbase db2i dsql hetero mssql oracle saas sapodp | the rest (13) | copybook db2zos duckdb kinesis parquet pubsub sqlite teradata |
| tail.added | cockroachdb kafka mssql mysql oracle postgres tidb | - | the rest (20) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.readded | cosmosdb dynamodb kafka mssql mysql oracle tidb | - | the rest (20) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.ddl | cosmosdb dynamodb kafka mysql postgres | oracle | the rest (21) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.renames | mysql postgres | mssql oracle | the rest (18) | cassandra copybook cosmosdb couchbase db2zos duckdb dynamodb hetero kafka kinesis parquet pubsub sqlite teradata |
| assess.versions | the rest (12) | cassandra clickhouse cockroachdb mssql oracle | ase bigquery couchbase db2 db2i db2zos duckdb generic saas sapodp | copybook cosmosdb delta dynamodb hetero iceberg kinesis parquet pubsub |
| assess.brands | kafka mongodb mysql opensearch postgres redis redshift snowflake tidb | cassandra clickhouse cockroachdb dsql mssql oracle teradata | the rest (13) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.engine-checks | the rest (21) | cassandra clickhouse db2i mssql opensearch sapodp | ase copybook db2 delta duckdb iceberg kinesis parquet pubsub | - |
| assess.preflight | cosmosdb dsql dynamodb kafka mongodb mysql postgres redis tidb | cassandra clickhouse cockroachdb mssql opensearch oracle redshift snowflake teradata | the rest (14) | kinesis pubsub saas sapodp |
| assess.capacity | clickhouse dynamodb hetero mongodb mysql postgres redis tidb | cassandra cosmosdb kafka mssql opensearch oracle redshift snowflake teradata | the rest (17) | saas sapodp |
| assess.writers | dynamodb mysql postgres redis | clickhouse cosmosdb kafka mongodb mssql opensearch oracle teradata | the rest (22) | kinesis pubsub |
| assess.handwork | dynamodb kafka mongodb mysql postgres redis | cassandra clickhouse cockroachdb cosmosdb dsql mssql opensearch oracle redshift snowflake teradata tidb | the rest (14) | copybook kinesis parquet pubsub |
| schema.create | the rest (25) | db2i db2zos kafka | generic | hetero kinesis mongodb pubsub redis saas sapodp |
| schema.indexes | cockroachdb dsql dynamodb mongodb mysql postgres teradata tidb | cassandra clickhouse cosmosdb mssql oracle sqlite | ase bigquery couchbase db2 db2i db2zos duckdb generic snowflake | the rest (13) |
| schema.foreign-keys | dsql mssql mysql oracle postgres tidb | cockroachdb redshift snowflake teradata | ase bigquery db2 db2i db2zos duckdb generic sqlite | the rest (18) |
| schema.defaults | cockroachdb dsql mysql opensearch postgres sqlite teradata tidb | clickhouse mssql oracle redshift sapodp snowflake | the rest (11) | cassandra cosmosdb couchbase dynamodb hetero kafka kinesis mongodb pubsub redis saas |
| schema.views | dsql mssql mysql oracle postgres tidb | clickhouse cockroachdb mongodb redshift snowflake teradata | ase bigquery couchbase db2 db2i db2zos duckdb generic iceberg opensearch sqlite | the rest (13) |
| schema.routines | mssql mysql oracle postgres | cockroachdb mongodb | the rest (17) | copybook delta dynamodb hetero iceberg kafka kinesis parquet pubsub saas sapodp sqlite tidb |
| move.table-copy | the rest (27) | bigquery db2i db2zos snowflake | generic | kinesis pubsub saas sapodp |
| move.bulk | the rest (15) | hetero | ase bigquery cockroachdb cosmosdb couchbase db2 db2i db2zos delta generic iceberg redshift snowflake tidb | dsql kinesis pubsub saas sapodp teradata |
| move.large-values | the rest (17) | bigquery db2i db2zos snowflake teradata | cockroachdb couchbase dsql generic tidb | cassandra copybook cosmosdb dynamodb kinesis pubsub redshift saas sapodp |
| move.partitioned | clickhouse kafka mysql postgres tidb | mssql oracle teradata | ase bigquery db2 db2i db2zos delta generic hetero iceberg parquet | the rest (18) |
| move.sized | the rest (18) | cassandra cockroachdb db2i db2zos oracle redshift snowflake teradata | copybook couchbase delta generic iceberg parquet saas sapodp | kinesis pubsub |
| move.load-window | hetero mysql opensearch oracle postgres | cassandra clickhouse mongodb mssql teradata | ase cockroachdb cosmosdb couchbase db2 db2i db2zos dsql duckdb generic sqlite tidb | the rest (14) |
| move.create-missing | cosmosdb dsql dynamodb hetero mysql opensearch oracle postgres teradata tidb | cassandra clickhouse cockroachdb kafka mssql redshift snowflake | the rest (13) | kinesis mongodb pubsub redis saas sapodp |
| cutover.freeze | cassandra clickhouse cockroachdb kafka mongodb mssql mysql opensearch postgres redis redshift snowflake teradata tidb | ase couchbase db2 hetero oracle | - | the rest (17) |
| cutover.read-only-target | the rest (21) | db2i db2zos hetero | - | copybook cosmosdb delta duckdb generic iceberg kinesis parquet pubsub saas sapodp sqlite |
| cutover.counters | duckdb hetero mssql mysql oracle postgres sqlite tidb | cockroachdb dsql | ase bigquery db2 db2i db2zos generic redshift snowflake | the rest (18) |
| cutover.rollback | cassandra clickhouse duckdb dynamodb hetero kafka mssql mysql opensearch oracle parquet postgres redis sqlite | copybook mongodb | the rest (15) | kinesis pubsub saas sapodp teradata |
| cutover.drain | the rest (17) | db2i saas | ase bigquery db2 delta generic hetero iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub sqlite teradata |
| cutover.pause-replication | mysql postgres | - | generic redis | the rest (32) |
| cutover.two-way | mysql postgres teradata | - | the rest (23) | copybook db2zos duckdb hetero kinesis parquet pubsub saas sapodp sqlite |
| cutover.jobs | mysql postgres | clickhouse mssql oracle | ase bigquery cockroachdb couchbase db2 db2i db2zos generic opensearch redshift snowflake teradata | the rest (19) |
| security.tls | the rest (28) | oracle | ase db2 db2zos generic mssql saas | hetero |
| managed.catalogued | the rest (20) | - | ase couchbase db2i db2zos delta duckdb generic iceberg parquet saas sapodp snowflake sqlite tidb | copybook hetero |
| operations.fencing | mysql postgres | cockroachdb dsql tidb | the rest (28) | hetero saas sapodp |

## Backlog (496 items, `docs/backlog-index.md`)

DONE 286 - PARTLY 61 - IN PROGRESS 8 - OPEN 112 - BLOCKED 20 - NOT PURSUED 9

| area | DONE | PARTLY | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED |
|---|---|---|---|---|---|---|
| change follow | 41 | 7 | 1 | 9 | 5 | 0 |
| conversion | 29 | 1 | 0 | 1 | 0 | 0 |
| correctness | 0 | 0 | 0 | 1 | 0 | 0 |
| cutover | 14 | 3 | 1 | 2 | 0 | 0 |
| ease of use | 19 | 2 | 1 | 17 | 1 | 1 |
| engines | 27 | 8 | 2 | 13 | 6 | 1 |
| maintainability | 0 | 0 | 0 | 5 | 0 | 0 |
| move | 30 | 11 | 2 | 9 | 0 | 2 |
| operations/scale | 26 | 10 | 0 | 29 | 5 | 3 |
| security | 10 | 5 | 0 | 1 | 2 | 1 |
| tools used whole | 32 | 6 | 0 | 9 | 0 | 0 |
| two-way | 4 | 2 | 0 | 3 | 0 | 0 |
| types | 12 | 1 | 1 | 0 | 0 | 0 |
| verify | 42 | 5 | 0 | 13 | 1 | 1 |

Not done (201): P0-DE.1, P0-DE.2, P0-DE.3, P0-DE.4, BAR-1, BAR-2, BAR-3, 0f-DVT, 0f-REST, 0a.measure, 0b.window, 6.mariadb, 6.fkwin, 7.free, R9.docdb, A2/D10, C1.managed, F1.dual, P3.dryrun, R6.cloud, 26.versions, 26.trino, 26.striim, MSSQL-x86, AUD.release, P0-DEPTH.constants, P0-DEPTH.calibrate, P0-DEPTH.json, P0-DEPTH.triggers, P0-DEPTH.rank, P0-DEPTH.tombstones, P0-AUDIT, AUD-B, AUD-C, AUD-D, AUD-E.pin, AUD-F, AUD-G2, AUD-H, AUD-T2, AUD-Z, P0-PARITY.lv, P0-PARITY.part, P0-PARITY.small, P0-PARITY.lv.new, P0-SMART.route, P0-CLOUDS.native, P0-CLOUDS.more, DEC1001.media, DEC1001.gui, P0-SCENARIOS.brands, P0-SCENARIOS.managed, P0-SCENARIOS.arch, P0-SCENARIOS.charset, P0-SCENARIOS.engine, P0-SCENARIOS.consolidate, DEC30.sweep, DEC.deps, DEC.ptcs, S1.e, S3.link, S4, F2.freeze, F3, F7.beat, F7.edges, F7.sec, ER1.acct, ER4, ER5, ER6, W5, LT.R1, LT.R2, LT.R3, LT.R6, LT.R8, LT.R9, LT.R10, LT.C1, LT.C2, LT.C3, LT.C4, LT.C5, LT.C6, LT.P1, LT.P2, LT.P3, W1, W2, W4.a, W4.b, W6, W7, W8, W9, 28.rates, R5.actions, R5.anchor, 32.rds, 45.run, R4.msk, R4.formats, R3.topo, 38.real, R17b.hmac, R17b.prog, DOCS, 43.fix, R8.more, R8.mv, R10.pieces, R12.minio, R13-DB2, R13-ASE, R13-HETERO, R13-GENERIC, R13.kafka, R13.sqlite, R13.parquet, R15.2nd, R16b, R16c, R17d, R18.6/R20.2, R18.7, R19.1, R19.3, R19.9/R20.3, R20.5, R20.6, R20.7, R20.10, R20.11, R21.7, R21.8, R21.9, R21.M, DTS.sample, REL, REL.gate, R23.7, R23.8, R25.2, R25.3, P0-SCOPE.open, P0-PARITY.scope, P0-PARITY.verify, P0-PARITY.tail, P0-PARITY.all, P0-VALIDATE.decide, P0-VALIDATE.ranges, P0-VALIDATE.state, P0-VALIDATE.results, P0-VALIDATE.config, P0-VALIDATE.depth, P0-DECISION-ALL.coverage, P0-DECISION-ALL.reason, P0-DECISION-ALL.adapt, P0-DECISION-ALL.experience, P0-DECISION-ALL.proof, P0-WORKFLOW-DEPTH.model, P0-WORKFLOW-DEPTH.investigate, P0-WORKFLOW-DEPTH.connect, P0-WORKFLOW-DEPTH.recover, P0-WORKFLOW-DEPTH.operator, P0-WORKFLOW-DEPTH.acceptance, P0-MINIMAL-INPUT.setup, P0-MINIMAL-INPUT.ownership, P0-MINIMAL-INPUT.correctness, P0-MINIMAL-INPUT.idempotency, P0-MINIMAL-INPUT.acceptance, P0-RESULT-SCOPE.scope, P0-RESULT-SCOPE.summary, P0-RESULT-SCOPE.usability, P0-RESULT-SCOPE.commercial, P0-RESULT-SCOPE.unmatched, P0-AUTO-RESOURCES.route, P0-AUTO-RESOURCES.budget, P0-AUTO-RESOURCES.adapt, P0-AUTO-RESOURCES.explain, P0-AUTO-RESOURCES.proof, P0-DECISION-ALL.engines, P0-DECISION-ALL.priority, P0-SAFETY-INTEGRITY, P0-RESILIENCE-ALL, P0-AUTONOMOUS-MIGRATION, P0-AUTOTUNE-ALL, P0-PRODUCT-TELEMETRY, P1-SOURCE-HIERARCHY.inventory, P1-SOURCE-HIERARCHY.layout, P1-SOURCE-HIERARCHY.engines, P1-SOURCE-HIERARCHY.navigation, P1-SOURCE-HIERARCHY.proof, P0-RESULT-SCOPE.contract, P0-RESULT-SCOPE.presentation, P0-RESULT-SCOPE.artifacts, P0-READABILITY-ALL, P0-ARTIFACTS, P0-VALIDATE.no-silent-skip, P0-RESUME-ALL

