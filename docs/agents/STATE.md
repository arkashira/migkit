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
| cockroachdb | 11/16 | 9/11 | sequences, params, bulk-move, users, snapshot, large values, partitioned |
| copybook | 16/16 | 11/11 | - |
| cosmosdb | 14/16 | 10/11 | bulk-move, snapshot, large values |
| couchbase | 11/16 | 10/11 | params, bulk-move, users, statistics, snapshot, large values |
| db2 | 6/16 | 10/11 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned |
| db2i | 12/16 | 10/11 | sequences, params, bulk-move, snapshot, partitioned |
| db2zos | 10/16 | 10/11 | sequences, params, bulk-move, users, statistics, snapshot, partitioned |
| delta | 9/16 | 10/11 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned |
| dsql | 12/16 | 9/11 | sequences, users, statistics, snapshot, large values, partitioned |
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
| redshift | 5/16 | 11/11 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| saas | 16/16 | 11/11 | - |
| sapodp | 16/16 | 11/11 | - |
| snowflake | 5/16 | 11/11 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot |
| sqlite | 16/16 | 11/11 | - |
| teradata | 16/16 | 9/11 | large values, partitioned |
| tidb | 13/16 | 10/11 | bulk-move, users, snapshot, large values |

## Depth by engine

86 items in 10 areas (`capabilities.depth()`, backlog P0-PARITY), each read off the code an engine runs; a way counts once a test proves it on that engine (`PROVES` in the test module). Cells: n/a 898, not yet 1267, test owed 295, yes 636. A cell below is the ways proved of the items that apply.

| engine | scope | verify | tail | assess | schema | move | cutover | security | managed | operations | all |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ase | 0/10 | 0/34 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 0/1 | 0/1 | 5/80 |
| bigquery | 1/10 | 0/31 | 0/6 | 1/7 | 1/6 | 1/6 | 1/6 | 1/1 | 1/1 | 0/1 | 7/75 |
| cassandra | 0/9 | 3/24 | 1/5 | 0/7 | 1/4 | 2/5 | 4/5 | 1/1 | 1/1 | 0/1 | 13/62 |
| clickhouse | 7/9 | 6/29 | 1/6 | 1/7 | 1/5 | 5/7 | 4/6 | 1/1 | 1/1 | 0/1 | 27/72 |
| cockroachdb | 4/10 | 2/35 | 3/6 | 1/7 | 3/6 | 1/7 | 3/7 | 1/1 | 1/1 | 0/1 | 19/81 |
| copybook | 0/7 | 0/15 | - | 0/4 | 1/2 | 2/4 | 0/1 | 1/1 | - | 0/1 | 4/35 |
| cosmosdb | 0/9 | 1/23 | 1/5 | 1/7 | 1/3 | 1/6 | 1/3 | 1/1 | 1/1 | 0/1 | 8/59 |
| couchbase | 0/9 | 1/22 | 1/5 | 1/7 | 1/4 | 1/6 | 2/6 | 1/1 | 0/1 | 0/1 | 8/62 |
| db2 | 1/10 | 0/35 | 0/6 | 0/7 | 1/6 | 3/7 | 1/7 | 0/1 | 1/1 | 0/1 | 7/81 |
| db2i | 0/10 | 1/35 | 0/6 | 0/7 | 0/6 | 0/7 | 0/6 | 1/1 | 0/1 | 0/1 | 2/80 |
| db2zos | 0/9 | 0/32 | - | 1/7 | 0/6 | 0/7 | 0/4 | 0/1 | 0/1 | 0/1 | 1/68 |
| delta | 0/9 | 0/23 | 0/6 | 0/5 | 1/2 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/57 |
| dsql | 3/10 | 1/30 | 1/6 | 1/7 | 3/5 | 0/6 | 1/5 | 1/1 | 1/1 | 0/1 | 12/72 |
| duckdb | 1/9 | 2/23 | - | 0/7 | 1/6 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 11/56 |
| dynamodb | 0/8 | 2/18 | 1/5 | 0/7 | 1/2 | 2/5 | 3/4 | 1/1 | 1/1 | 0/1 | 11/52 |
| generic | 1/10 | 2/39 | 0/6 | 1/7 | 0/6 | 0/7 | 0/6 | 0/1 | 0/1 | 0/1 | 4/84 |
| hetero | 4/10 | 5/39 | 1/2 | 2/5 | - | 5/7 | 2/5 | - | - | - | 19/68 |
| iceberg | 0/9 | 0/23 | 0/6 | 0/5 | 1/3 | 2/6 | 0/3 | 1/1 | 0/1 | 0/1 | 4/58 |
| kafka | 5/9 | 15/16 | 5/5 | 5/7 | 0/1 | 5/6 | 4/5 | 1/1 | 1/1 | 0/1 | 41/52 |
| kinesis | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| mongodb | 8/9 | 21/23 | 1/6 | 4/7 | 0/3 | 4/5 | 3/5 | 1/1 | 1/1 | 0/1 | 43/61 |
| mssql | 7/10 | 17/34 | 3/6 | 0/7 | 4/6 | 4/7 | 5/7 | 0/1 | 1/1 | 0/1 | 41/80 |
| mysql | 9/10 | 28/31 | 6/6 | 7/7 | 6/6 | 7/7 | 8/8 | 1/1 | 1/1 | 1/1 | 74/78 |
| opensearch | 7/9 | 17/24 | 1/6 | 2/7 | 2/4 | 6/6 | 4/6 | 1/1 | 1/1 | 0/1 | 41/65 |
| oracle | 9/10 | 14/36 | 3/6 | 1/7 | 4/6 | 5/7 | 4/7 | 0/1 | 1/1 | 0/1 | 41/82 |
| parquet | 0/7 | 0/15 | - | 0/4 | 1/2 | 3/6 | 1/1 | 1/1 | 0/1 | 0/1 | 6/38 |
| postgres | 9/10 | 37/37 | 5/6 | 7/7 | 6/6 | 7/7 | 8/8 | 1/1 | 1/1 | 1/1 | 82/84 |
| pubsub | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| redis | 5/8 | 12/12 | 1/6 | 7/7 | 0/1 | 4/4 | 4/6 | 1/1 | 1/1 | 0/1 | 35/47 |
| redshift | 1/10 | 0/32 | 0/6 | 1/7 | 1/5 | 1/4 | 1/7 | 1/1 | 1/1 | 0/1 | 7/74 |
| saas | 0/6 | 0/3 | 0/5 | 1/5 | - | 0/1 | 0/1 | 0/1 | 0/1 | - | 1/23 |
| sapodp | 0/6 | 0/3 | 0/5 | 0/5 | 0/1 | 0/1 | 1/1 | 1/1 | 0/1 | - | 2/24 |
| snowflake | 0/10 | 0/33 | 0/6 | 0/7 | 1/6 | 0/5 | 1/7 | 1/1 | 0/1 | 0/1 | 3/77 |
| sqlite | 2/9 | 4/27 | - | 2/7 | 2/5 | 4/6 | 2/2 | 1/1 | 0/1 | 0/1 | 17/59 |
| teradata | 1/8 | 1/30 | - | 1/7 | 1/6 | 1/6 | 2/4 | 1/1 | 0/1 | 0/1 | 8/64 |
| tidb | 4/10 | 2/31 | 3/6 | 3/7 | 5/6 | 2/7 | 4/6 | 1/1 | 0/1 | 0/1 | 24/76 |

Per item: the engines in each state - "test owed" is a way in the code whose test on that engine is not written (`UNPROVED`).

| item | yes | test owed | not yet | n/a |
|---|---|---|---|---|
| scope.listings | clickhouse cockroachdb dsql kafka mongodb mssql mysql opensearch oracle postgres redis tidb | ase | the rest (21) | kinesis pubsub |
| scope.deep | clickhouse cockroachdb db2 kafka mongodb mssql mysql opensearch oracle postgres redis teradata tidb | ase db2i db2zos hetero | the rest (15) | kinesis pubsub saas sapodp |
| scope.copy | clickhouse cockroachdb dsql kafka mongodb mssql mysql opensearch oracle postgres redis tidb | ase | the rest (21) | kinesis pubsub |
| scope.repair | clickhouse generic hetero kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | cockroachdb cosmosdb couchbase dsql tidb | the rest (15) | kinesis pubsub saas sapodp |
| scope.counters | mssql mysql oracle postgres tidb | - | ase bigquery cockroachdb db2 db2i db2zos dsql duckdb generic hetero redshift snowflake sqlite | the rest (18) |
| scope.objects | kafka mongodb mssql mysql oracle postgres | clickhouse opensearch | the rest (20) | copybook dynamodb kinesis parquet pubsub redis saas sapodp |
| scope.ops | clickhouse hetero mongodb mssql mysql opensearch oracle postgres redis | the rest (16) | ase cassandra db2 generic kafka | copybook db2zos duckdb parquet sqlite teradata |
| scope.row-filter | bigquery clickhouse cockroachdb dsql duckdb hetero mongodb mysql opensearch oracle postgres redshift sqlite | the rest (16) | generic kafka redis saas sapodp | kinesis pubsub |
| scope.transforms | - | - | the rest (34) | kinesis pubsub |
| scope.names | clickhouse hetero mongodb mysql opensearch oracle postgres | the rest (22) | generic kafka redis saas sapodp | kinesis pubsub |
| verify.access.object-ownership | mysql postgres | clickhouse mssql oracle | ase cockroachdb db2 db2i db2zos dsql generic hetero redshift snowflake teradata tidb | the rest (19) |
| verify.access.sequence-grants | postgres | mssql oracle | cockroachdb db2 db2i db2zos dsql generic hetero snowflake | the rest (25) |
| verify.access.table-grants | kafka mysql opensearch postgres redis | clickhouse mongodb mssql oracle | the rest (15) | copybook cosmosdb delta duckdb dynamodb iceberg kinesis parquet pubsub saas sapodp sqlite |
| verify.identity.sequence-collision | mssql mysql oracle postgres | - | ase bigquery cockroachdb db2 db2i db2zos dsql duckdb generic hetero redshift snowflake sqlite tidb | the rest (18) |
| verify.identity.sequence-parity | mssql mysql oracle postgres | - | ase bigquery cockroachdb db2 db2i db2zos dsql duckdb generic hetero redshift snowflake sqlite tidb | the rest (18) |
| verify.movement.target-ahead | kafka mongodb mssql mysql postgres redis | clickhouse opensearch oracle | the rest (23) | kinesis pubsub saas sapodp |
| verify.structure.collection-options | kafka mongodb opensearch postgres | clickhouse mysql oracle | the rest (22) | copybook kinesis parquet pubsub redis saas sapodp |
| verify.structure.columns | mssql mysql opensearch postgres | clickhouse oracle | the rest (20) | cosmosdb couchbase dynamodb kafka kinesis mongodb pubsub redis saas sapodp |
| verify.structure.deferrable | postgres | oracle | generic hetero sqlite | the rest (31) |
| verify.structure.extensions | postgres | - | generic hetero | the rest (33) |
| verify.structure.foreign-keys | mssql mysql postgres | oracle | ase bigquery cockroachdb db2 db2i db2zos duckdb generic hetero redshift snowflake sqlite teradata tidb | the rest (18) |
| verify.structure.indexes | mongodb postgres tidb | clickhouse mssql oracle | the rest (17) | copybook delta iceberg kafka kinesis mysql opensearch parquet pubsub redis redshift saas sapodp |
| verify.structure.keys | mssql mysql postgres redis | clickhouse oracle | the rest (17) | cassandra cosmosdb couchbase delta dynamodb kafka kinesis mongodb opensearch parquet pubsub saas sapodp |
| verify.structure.materialized-views | postgres | clickhouse oracle | ase bigquery cassandra cockroachdb cosmosdb db2 db2i db2zos generic hetero iceberg redshift snowflake teradata | the rest (19) |
| verify.structure.objects | cockroachdb duckdb kafka mongodb mssql mysql oracle postgres | clickhouse opensearch | the rest (18) | copybook dynamodb kinesis parquet pubsub redis saas sapodp |
| verify.structure.partitions | kafka mysql postgres | clickhouse mssql oracle tidb | the rest (15) | cassandra copybook cosmosdb couchbase duckdb dynamodb kinesis mongodb opensearch pubsub redis saas sapodp sqlite |
| verify.structure.row-security | opensearch postgres | clickhouse mssql oracle | ase bigquery cockroachdb db2 db2i db2zos delta generic hetero iceberg redshift snowflake teradata | the rest (18) |
| verify.structure.schema-diff | mysql postgres | - | the rest (24) | cosmosdb couchbase dynamodb kafka kinesis mongodb pubsub redis saas sapodp |
| verify.structure.sharding | mongodb | clickhouse opensearch | cosmosdb generic hetero redshift teradata | the rest (28) |
| verify.structure.triggers | mssql postgres | mysql oracle | ase cassandra cockroachdb cosmosdb couchbase db2 db2i db2zos generic hetero sqlite teradata | the rest (20) |
| verify.structure.unvalidated-constraints | mysql oracle postgres | mssql | ase cockroachdb db2 db2i db2zos generic hetero sqlite teradata tidb | the rest (22) |
| verify.value.charset | mysql oracle postgres | clickhouse mongodb mssql | the rest (23) | kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.collation | mongodb mysql opensearch postgres | mssql oracle | the rest (16) | cassandra clickhouse copybook cosmosdb couchbase dynamodb iceberg kafka kinesis parquet pubsub redis saas sapodp |
| verify.value.generated | mysql opensearch postgres | clickhouse mssql oracle | the rest (17) | cassandra copybook couchbase dynamodb iceberg kafka kinesis mongodb parquet pubsub redis saas sapodp |
| verify.value.narrowing | mongodb mysql opensearch postgres | clickhouse mssql oracle | the rest (23) | kafka kinesis pubsub redis saas sapodp |
| verify.value.null-empty | kafka mongodb mysql opensearch postgres | clickhouse mssql oracle | the rest (23) | kinesis pubsub redis saas sapodp |
| verify.value.precision | mongodb mysql opensearch postgres | clickhouse mssql oracle | the rest (23) | kafka kinesis pubsub redis saas sapodp |
| verify.value.rendering | mongodb postgres | clickhouse mssql mysql oracle | the rest (23) | kafka kinesis opensearch pubsub redis saas sapodp |
| verify.value.timezone | mongodb mysql postgres | clickhouse mssql opensearch oracle | the rest (23) | kafka kinesis pubsub redis saas sapodp |
| verify.value.type-drift | mongodb opensearch | kafka | cosmosdb couchbase dynamodb generic hetero sqlite | the rest (27) |
| verify.delta | the rest (12) | cockroachdb cosmosdb couchbase db2i dsql tidb | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| verify.fence | the rest (17) | db2i | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| verify.confirm | hetero kafka mongodb mssql mysql opensearch oracle postgres redis | cassandra clickhouse cockroachdb cosmosdb couchbase db2i dsql dynamodb tidb | ase bigquery db2 delta generic iceberg redshift snowflake | the rest (10) |
| verify.users | the rest (12) | - | ase bigquery cockroachdb couchbase db2 db2zos dsql generic hetero redshift snowflake tidb | copybook cosmosdb delta duckdb dynamodb iceberg kinesis parquet pubsub saas sapodp sqlite |
| verify.repair | generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite | clickhouse cockroachdb cosmosdb couchbase dsql opensearch tidb | the rest (15) | kinesis pubsub saas sapodp |
| verify.repair-undo | generic hetero kafka mongodb mssql mysql oracle postgres redis sqlite | clickhouse cockroachdb cosmosdb couchbase dsql opensearch tidb | the rest (15) | kinesis pubsub saas sapodp |
| verify.rows-apart | clickhouse kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | - | the rest (24) | kinesis pubsub |
| verify.tables | clickhouse kafka mongodb mssql mysql opensearch oracle postgres redis | - | the rest (25) | kinesis pubsub |
| verify.table | clickhouse duckdb kafka mongodb mssql mysql opensearch oracle postgres redis sqlite | the rest (22) | generic | kinesis pubsub |
| tail.follows | the rest (17) | db2i | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| tail.table-states | cockroachdb kafka mysql postgres | cosmosdb couchbase db2i dsql dynamodb hetero mssql oracle saas sapodp tidb | the rest (13) | copybook db2zos duckdb kinesis parquet pubsub sqlite teradata |
| tail.added | cockroachdb kafka mssql mysql oracle postgres tidb | - | the rest (20) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.readded | kafka mssql mysql oracle tidb | - | the rest (22) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.ddl | kafka mysql postgres | oracle | the rest (23) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.renames | mysql postgres | mssql oracle | the rest (18) | cassandra copybook cosmosdb couchbase db2zos duckdb dynamodb hetero kafka kinesis parquet pubsub sqlite teradata |
| assess.versions | kafka mysql opensearch postgres redis sqlite | clickhouse mongodb mssql oracle | the rest (19) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.brands | kafka mongodb mysql opensearch postgres redis tidb | clickhouse mssql oracle | the rest (19) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.engine-checks | the rest (19) | clickhouse db2i mssql opensearch sapodp snowflake | ase cassandra copybook db2 delta duckdb dynamodb iceberg kinesis parquet pubsub | - |
| assess.preflight | kafka mysql postgres redis | clickhouse mssql opensearch oracle tidb | the rest (23) | kinesis pubsub saas sapodp |
| assess.capacity | clickhouse hetero mongodb mysql postgres redis tidb | kafka mssql opensearch oracle | the rest (23) | saas sapodp |
| assess.writers | mysql postgres redis | clickhouse kafka mssql opensearch oracle | the rest (26) | kinesis pubsub |
| assess.handwork | kafka mongodb mysql postgres redis | clickhouse mssql opensearch oracle | the rest (23) | copybook kinesis parquet pubsub |
| schema.create | the rest (25) | db2i db2zos kafka | generic | hetero kinesis mongodb pubsub redis saas sapodp |
| schema.indexes | cockroachdb dsql mysql postgres tidb | clickhouse mssql oracle sqlite teradata | the rest (13) | copybook delta hetero iceberg kafka kinesis opensearch parquet pubsub redis redshift saas sapodp |
| schema.foreign-keys | mssql mysql oracle postgres tidb | - | ase bigquery cockroachdb db2 db2i db2zos duckdb generic redshift snowflake sqlite teradata | the rest (19) |
| schema.defaults | cockroachdb dsql mysql opensearch postgres sqlite tidb | clickhouse mssql oracle sapodp teradata | the rest (14) | cosmosdb couchbase dynamodb hetero kafka kinesis mongodb pubsub redis saas |
| schema.views | mssql mysql oracle postgres tidb | clickhouse | the rest (17) | cassandra copybook cosmosdb delta dynamodb hetero kafka kinesis parquet pubsub redis saas sapodp |
| schema.routines | mssql mysql oracle postgres | tidb | the rest (19) | copybook delta dynamodb hetero iceberg kafka kinesis parquet pubsub saas sapodp sqlite |
| move.table-copy | the rest (26) | bigquery db2i db2zos dsql snowflake | generic | kinesis pubsub saas sapodp |
| move.bulk | the rest (15) | hetero | ase bigquery cockroachdb cosmosdb couchbase db2 db2i db2zos delta generic iceberg redshift snowflake tidb | dsql kinesis pubsub saas sapodp teradata |
| move.large-values | the rest (17) | bigquery db2i db2zos snowflake | cockroachdb cosmosdb couchbase dsql generic teradata tidb | cassandra copybook dynamodb kinesis pubsub redshift saas sapodp |
| move.partitioned | clickhouse kafka mysql postgres | mssql oracle tidb | ase bigquery cockroachdb db2 db2i db2zos delta dsql generic hetero iceberg parquet teradata | the rest (16) |
| move.sized | the rest (14) | cockroachdb db2i db2zos dsql oracle redshift snowflake teradata tidb | cassandra copybook cosmosdb couchbase delta dynamodb generic iceberg parquet saas sapodp | kinesis pubsub |
| move.load-window | hetero mysql opensearch oracle postgres | clickhouse mssql tidb | the rest (15) | bigquery copybook delta iceberg kafka kinesis parquet pubsub redis redshift saas sapodp snowflake |
| move.create-missing | hetero mysql opensearch oracle postgres tidb | clickhouse kafka mssql | the rest (21) | kinesis mongodb pubsub redis saas sapodp |
| cutover.freeze | cassandra clickhouse cockroachdb kafka mongodb mssql mysql opensearch postgres redis tidb | ase couchbase db2 hetero oracle redshift snowflake teradata | - | the rest (17) |
| cutover.read-only-target | the rest (20) | db2i db2zos dsql hetero | - | copybook cosmosdb delta duckdb generic iceberg kinesis parquet pubsub saas sapodp sqlite |
| cutover.counters | duckdb hetero mssql mysql oracle postgres sqlite tidb | - | ase bigquery cockroachdb db2 db2i db2zos dsql generic redshift snowflake | the rest (18) |
| cutover.rollback | cassandra clickhouse duckdb dynamodb hetero kafka mssql mysql opensearch oracle parquet postgres redis sqlite | copybook mongodb | the rest (15) | kinesis pubsub saas sapodp teradata |
| cutover.drain | the rest (17) | db2i saas | ase bigquery db2 delta generic hetero iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub sqlite teradata |
| cutover.pause-replication | mysql postgres | - | generic redis | the rest (32) |
| cutover.two-way | mysql postgres teradata | - | the rest (23) | copybook db2zos duckdb hetero kinesis parquet pubsub saas sapodp sqlite |
| cutover.jobs | mysql postgres | clickhouse mssql oracle | ase bigquery cockroachdb couchbase db2 db2i db2zos generic opensearch redshift snowflake teradata | the rest (19) |
| security.tls | the rest (28) | oracle | ase db2 db2zos generic mssql saas | hetero |
| managed.catalogued | the rest (19) | - | ase couchbase db2i db2zos delta duckdb generic iceberg parquet saas sapodp snowflake sqlite teradata tidb | copybook hetero |
| operations.fencing | mysql postgres | tidb | the rest (30) | hetero saas sapodp |

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

