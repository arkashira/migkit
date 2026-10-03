# migkit state

Written by `tools/gen_codemap.py` from `capabilities.matrix()`, `decide.coverage()`, `capabilities.depth()` and `docs/backlog-index.md`; the pre-commit hook keeps it current. What this machine last proved (the full suite, unpushed work) is in `.codemap/LOCAL.md`.

## Capabilities by engine

16 capabilities (`capabilities.CAPABILITIES`) and 11 shapes (`decide.coverage()`); a row lists what is not yet there.

| engine | capabilities | shapes | not yet |
|---|---|---|---|
| ase | 6/16 | 8/11 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned, read-only target, writes held |
| bigquery | 5/16 | 8/11 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned, read-only target, writes held |
| cassandra | 16/16 | 10/11 | read-only target |
| clickhouse | 16/16 | 9/11 | read-only target, writes held |
| cockroachdb | 11/16 | 7/11 | sequences, params, bulk-move, users, snapshot, large values, partitioned, read-only target, writes held |
| copybook | 16/16 | 10/11 | read-only target |
| cosmosdb | 14/16 | 8/11 | bulk-move, snapshot, large values, read-only target, writes held |
| couchbase | 11/16 | 8/11 | params, bulk-move, users, statistics, snapshot, large values, read-only target, writes held |
| db2 | 6/16 | 8/11 | sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned, read-only target, writes held |
| db2i | 12/16 | 8/11 | sequences, params, bulk-move, snapshot, partitioned, read-only target, writes held |
| db2zos | 10/16 | 8/11 | sequences, params, bulk-move, users, statistics, snapshot, partitioned, read-only target, writes held |
| delta | 9/16 | 8/11 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned, read-only target, writes held |
| dsql | 12/16 | 7/11 | sequences, users, statistics, snapshot, large values, partitioned, read-only target, writes held |
| duckdb | 16/16 | 10/11 | read-only target |
| dynamodb | 16/16 | 10/11 | read-only target |
| generic | 4/16 | 0/11 | sequences, params, bulk-move, table-copy, stream, fence, confirm, delta, users, guard, statistics, snapshot, keyless, no unique index, wide key, too large, large values, partitioned, read-only source, missing grant, read-only target, slow link, writes held |
| hetero | 15/16 | 7/11 | users, partitioned, missing grant, read-only target, writes held |
| iceberg | 9/16 | 8/11 | deep, bulk-move, stream, fence, confirm, delta, snapshot, partitioned, read-only target, writes held |
| kafka | 16/16 | 10/11 | read-only target |
| kinesis | 16/16 | 11/11 | - |
| mongodb | 16/16 | 9/11 | missing grant, read-only target |
| mssql | 16/16 | 9/11 | partitioned, read-only target |
| mysql | 16/16 | 11/11 | - |
| opensearch | 16/16 | 10/11 | read-only target |
| oracle | 16/16 | 8/11 | partitioned, read-only target, writes held |
| parquet | 16/16 | 9/11 | partitioned, read-only target |
| postgres | 16/16 | 11/11 | - |
| pubsub | 16/16 | 11/11 | - |
| redis | 16/16 | 9/11 | partitioned, read-only target |
| redshift | 5/16 | 8/11 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned, read-only target, writes held |
| saas | 16/16 | 10/11 | writes held |
| sapodp | 16/16 | 10/11 | writes held |
| snowflake | 5/16 | 8/11 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned, read-only target, writes held |
| sqlite | 16/16 | 10/11 | read-only target |
| teradata | 16/16 | 7/11 | large values, partitioned, read-only target, writes held |
| tidb | 13/16 | 7/11 | bulk-move, users, snapshot, large values, partitioned, read-only target, writes held |

## Depth by engine

86 items in 10 areas (`capabilities.depth()`, backlog P0-PARITY), each read off the code an engine runs; a way counts once a test proves it on that engine (`PROVES` in the test module). Cells: n/a 869, not yet 1540, test owed 252, yes 435. A cell below is the ways proved of the items that apply.

| engine | scope | verify | tail | assess | schema | move | cutover | security | managed | operations | all |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ase | 0/10 | 0/34 | 0/6 | 0/7 | 1/6 | 3/7 | 0/7 | 0/1 | 0/1 | 0/1 | 4/80 |
| bigquery | 0/10 | 0/31 | 0/6 | 1/7 | 1/6 | 1/6 | 0/7 | 1/1 | 1/1 | 0/1 | 5/76 |
| cassandra | 0/9 | 3/24 | 1/5 | 0/7 | 1/4 | 2/5 | 3/5 | 1/1 | 1/1 | 0/1 | 12/62 |
| clickhouse | 1/9 | 3/30 | 1/6 | 0/7 | 1/5 | 5/7 | 2/6 | 1/1 | 1/1 | 0/1 | 15/73 |
| cockroachdb | 1/10 | 2/35 | 3/6 | 0/7 | 1/6 | 1/7 | 1/7 | 0/1 | 1/1 | 0/1 | 10/81 |
| copybook | 0/7 | 0/15 | - | 0/4 | 1/2 | 2/4 | 0/2 | 0/1 | - | 0/1 | 3/36 |
| cosmosdb | 0/9 | 1/23 | 1/5 | 0/7 | 1/3 | 1/6 | 1/5 | 1/1 | 1/1 | 0/1 | 7/61 |
| couchbase | 0/9 | 1/22 | 1/5 | 0/7 | 1/4 | 1/6 | 1/6 | 1/1 | 0/1 | 0/1 | 6/62 |
| db2 | 1/10 | 0/35 | 0/6 | 0/7 | 1/6 | 3/7 | 0/7 | 0/1 | 1/1 | 0/1 | 6/81 |
| db2i | 0/10 | 1/35 | 0/6 | 0/7 | 0/6 | 0/7 | 0/7 | 1/1 | 0/1 | 0/1 | 2/81 |
| db2zos | 0/9 | 0/32 | - | 1/7 | 0/6 | 0/7 | 0/5 | 0/1 | 0/1 | 0/1 | 1/69 |
| delta | 0/9 | 0/23 | 0/6 | 0/5 | 1/2 | 2/6 | 0/5 | 1/1 | 0/1 | 0/1 | 4/59 |
| dsql | 0/10 | 1/30 | 1/6 | 0/7 | 0/5 | 0/6 | 1/6 | 0/1 | 1/1 | 0/1 | 4/73 |
| duckdb | 0/9 | 1/23 | - | 0/7 | 1/6 | 4/6 | 2/3 | 0/1 | 0/1 | 0/1 | 8/57 |
| dynamodb | 0/8 | 2/18 | 1/5 | 0/7 | 1/2 | 2/5 | 2/4 | 1/1 | 1/1 | 0/1 | 10/52 |
| generic | 1/10 | 2/39 | 0/6 | 1/7 | 0/6 | 0/7 | 0/8 | 0/1 | 0/1 | 0/1 | 4/86 |
| hetero | 4/10 | 5/39 | 1/2 | 1/5 | - | 5/7 | 2/5 | - | - | - | 18/68 |
| iceberg | 0/9 | 0/23 | 0/6 | 0/5 | 1/3 | 2/6 | 0/5 | 1/1 | 0/1 | 0/1 | 4/60 |
| kafka | 0/9 | 6/18 | 1/5 | 2/7 | 0/1 | 5/6 | 2/5 | 1/1 | 1/1 | 0/1 | 18/54 |
| kinesis | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| mongodb | 1/9 | 11/23 | 1/6 | 4/7 | 0/3 | 4/5 | 2/5 | 1/1 | 1/1 | 0/1 | 25/61 |
| mssql | 4/10 | 12/35 | 1/6 | 0/7 | 3/6 | 4/7 | 4/7 | 0/1 | 1/1 | 0/1 | 29/81 |
| mysql | 8/10 | 22/31 | 5/6 | 5/7 | 6/6 | 7/7 | 7/8 | 1/1 | 1/1 | 1/1 | 63/78 |
| opensearch | 0/9 | 3/26 | 1/6 | 2/7 | 1/4 | 3/6 | 3/6 | 1/1 | 1/1 | 0/1 | 15/67 |
| oracle | 3/10 | 8/36 | 1/6 | 1/7 | 3/6 | 4/7 | 3/7 | 0/1 | 1/1 | 0/1 | 24/82 |
| parquet | 0/7 | 0/15 | - | 0/4 | 1/2 | 3/6 | 1/2 | 0/1 | 0/1 | 0/1 | 5/39 |
| postgres | 9/10 | 29/37 | 4/6 | 7/7 | 5/6 | 7/7 | 8/8 | 1/1 | 1/1 | 1/1 | 72/84 |
| pubsub | 0/1 | - | - | 0/2 | - | - | - | 1/1 | 1/1 | 0/1 | 2/6 |
| redis | 1/8 | 7/14 | 1/6 | 4/7 | 0/1 | 4/5 | 2/6 | 1/1 | 1/1 | 0/1 | 21/50 |
| redshift | 0/10 | 0/32 | 0/6 | 1/7 | 1/5 | 1/5 | 0/7 | 1/1 | 1/1 | 0/1 | 5/75 |
| saas | 0/6 | 0/3 | 0/5 | 0/5 | - | 0/1 | 0/2 | 0/1 | 0/1 | - | 0/24 |
| sapodp | 0/6 | 0/3 | 0/5 | 0/5 | 0/1 | 0/1 | 1/2 | 1/1 | 0/1 | - | 2/25 |
| snowflake | 0/10 | 0/33 | 0/6 | 0/7 | 1/6 | 0/6 | 0/7 | 1/1 | 0/1 | 0/1 | 2/78 |
| sqlite | 2/9 | 2/27 | - | 2/7 | 2/5 | 4/6 | 2/3 | 1/1 | 0/1 | 0/1 | 15/60 |
| teradata | 1/8 | 1/30 | - | 0/7 | 1/6 | 1/6 | 1/4 | 1/1 | 0/1 | 0/1 | 6/64 |
| tidb | 0/10 | 1/31 | 1/6 | 0/7 | 0/6 | 2/7 | 2/6 | 0/1 | 0/1 | 0/1 | 6/76 |

Per item: the engines in each state - "test owed" is a way in the code whose test on that engine is not written (`UNPROVED`).

| item | yes | test owed | not yet | n/a |
|---|---|---|---|---|
| scope.listings | mssql mysql postgres | ase cockroachdb dsql oracle tidb | the rest (26) | kinesis pubsub |
| scope.deep | clickhouse cockroachdb db2 mssql mysql oracle postgres teradata | ase db2i db2zos hetero mongodb tidb | the rest (18) | kinesis pubsub saas sapodp |
| scope.copy | postgres | ase cockroachdb dsql mongodb mssql mysql oracle tidb | the rest (25) | kinesis pubsub |
| scope.repair | generic hetero mongodb mssql mysql postgres redis sqlite | cockroachdb cosmosdb couchbase dsql oracle tidb | the rest (18) | kinesis pubsub saas sapodp |
| scope.counters | mssql mysql oracle postgres | tidb | ase bigquery cockroachdb db2 db2i db2zos dsql duckdb generic hetero redshift snowflake sqlite | the rest (18) |
| scope.objects | mysql oracle postgres | - | the rest (25) | copybook dynamodb kinesis parquet pubsub redis saas sapodp |
| scope.ops | hetero mysql postgres | the rest (19) | ase cassandra clickhouse db2 generic kafka mongodb opensearch | copybook db2zos duckdb parquet sqlite teradata |
| scope.row-filter | hetero mysql postgres sqlite | the rest (23) | generic kafka mongodb mssql redis saas sapodp | kinesis pubsub |
| scope.transforms | - | - | the rest (34) | kinesis pubsub |
| scope.names | hetero mysql postgres | the rest (24) | generic kafka mongodb mssql redis saas sapodp | kinesis pubsub |
| verify.access.object-ownership | mysql postgres | - | ase clickhouse cockroachdb db2 db2i db2zos dsql generic hetero mssql oracle redshift snowflake teradata tidb | the rest (19) |
| verify.access.sequence-grants | postgres | - | cockroachdb db2 db2i db2zos dsql generic hetero mssql oracle snowflake | the rest (25) |
| verify.access.table-grants | mysql postgres | - | the rest (22) | copybook cosmosdb delta duckdb dynamodb iceberg kinesis parquet pubsub saas sapodp sqlite |
| verify.identity.sequence-collision | mssql mysql oracle postgres | - | ase bigquery cockroachdb db2 db2i db2zos dsql duckdb generic hetero redshift snowflake sqlite tidb | the rest (18) |
| verify.identity.sequence-parity | mssql mysql oracle postgres | - | ase bigquery cockroachdb db2 db2i db2zos dsql duckdb generic hetero redshift snowflake sqlite tidb | the rest (18) |
| verify.movement.target-ahead | - | mongodb mssql mysql postgres | the rest (28) | kinesis pubsub saas sapodp |
| verify.structure.collection-options | mongodb | - | the rest (28) | copybook kinesis parquet pubsub redis saas sapodp |
| verify.structure.columns | mssql postgres | mysql | the rest (23) | cosmosdb couchbase dynamodb kafka kinesis mongodb pubsub redis saas sapodp |
| verify.structure.deferrable | postgres | - | generic hetero oracle sqlite | the rest (31) |
| verify.structure.extensions | postgres | - | generic hetero | the rest (33) |
| verify.structure.foreign-keys | mssql mysql postgres | - | ase bigquery cockroachdb db2 db2i db2zos duckdb generic hetero oracle redshift snowflake sqlite teradata tidb | the rest (18) |
| verify.structure.indexes | mongodb postgres | tidb | the rest (20) | copybook delta iceberg kafka kinesis mysql opensearch parquet pubsub redis redshift saas sapodp |
| verify.structure.keys | mssql mysql postgres redis | - | the rest (19) | cassandra cosmosdb couchbase delta dynamodb kafka kinesis mongodb opensearch parquet pubsub saas sapodp |
| verify.structure.materialized-views | - | postgres | ase bigquery cassandra clickhouse cockroachdb cosmosdb db2 db2i db2zos generic hetero iceberg mssql oracle redshift snowflake teradata | the rest (18) |
| verify.structure.objects | cockroachdb duckdb mongodb mysql oracle postgres | - | the rest (22) | copybook dynamodb kinesis parquet pubsub redis saas sapodp |
| verify.structure.partitions | mysql postgres | - | the rest (20) | cassandra copybook cosmosdb couchbase duckdb dynamodb kinesis mongodb opensearch pubsub redis saas sapodp sqlite |
| verify.structure.row-security | postgres | - | ase bigquery clickhouse cockroachdb db2 db2i db2zos delta generic hetero iceberg mssql opensearch oracle redshift snowflake teradata | the rest (18) |
| verify.structure.schema-diff | postgres | - | the rest (25) | cosmosdb couchbase dynamodb kafka kinesis mongodb pubsub redis saas sapodp |
| verify.structure.sharding | mongodb | - | clickhouse cosmosdb generic hetero kafka opensearch redshift teradata | the rest (27) |
| verify.structure.triggers | mssql postgres | - | ase cassandra cockroachdb cosmosdb couchbase db2 db2i db2zos generic hetero mysql oracle sqlite teradata | the rest (20) |
| verify.structure.unvalidated-constraints | mysql postgres | - | ase cockroachdb db2 db2i db2zos generic hetero mssql oracle sqlite teradata tidb | the rest (22) |
| verify.value.charset | mysql postgres | - | the rest (30) | kinesis pubsub saas sapodp |
| verify.value.collation | mysql postgres | - | the rest (21) | cassandra copybook cosmosdb couchbase dynamodb iceberg kafka kinesis parquet pubsub redis saas sapodp |
| verify.value.generated | mysql postgres | - | the rest (21) | cassandra copybook couchbase dynamodb iceberg kafka kinesis mongodb parquet pubsub redis saas sapodp |
| verify.value.narrowing | mysql postgres | - | the rest (28) | kafka kinesis pubsub redis saas sapodp |
| verify.value.null-empty | mongodb mysql postgres | - | the rest (28) | kinesis pubsub redis saas sapodp |
| verify.value.precision | mysql postgres | - | the rest (28) | kafka kinesis pubsub redis saas sapodp |
| verify.value.rendering | - | postgres | the rest (29) | kafka kinesis pubsub redis saas sapodp |
| verify.value.timezone | mysql postgres | - | the rest (28) | kafka kinesis pubsub redis saas sapodp |
| verify.value.type-drift | mongodb | - | cosmosdb couchbase dynamodb generic hetero kafka opensearch redis sqlite | the rest (26) |
| verify.delta | the rest (12) | cockroachdb cosmosdb couchbase db2i dsql tidb | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| verify.fence | the rest (17) | db2i | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| verify.confirm | hetero kafka mongodb mssql mysql oracle redis | the rest (11) | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| verify.users | cassandra clickhouse db2i kafka mssql mysql opensearch oracle postgres redis teradata | mongodb | the rest (12) | copybook cosmosdb delta duckdb dynamodb iceberg kinesis parquet pubsub saas sapodp sqlite |
| verify.repair | generic hetero kafka mongodb mysql oracle postgres redis sqlite | cockroachdb cosmosdb couchbase dsql mssql tidb | the rest (17) | kinesis pubsub saas sapodp |
| verify.repair-undo | generic hetero kafka mongodb mysql postgres redis sqlite | cockroachdb cosmosdb couchbase dsql mssql oracle tidb | the rest (17) | kinesis pubsub saas sapodp |
| verify.rows-apart | mssql | mongodb mysql postgres | the rest (30) | kinesis pubsub |
| verify.tables | mssql | postgres | the rest (32) | kinesis pubsub |
| verify.table | - | the rest (30) | generic kafka mssql redis | kinesis pubsub |
| tail.follows | the rest (17) | db2i | ase bigquery db2 delta generic iceberg redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub saas sapodp sqlite teradata |
| tail.table-states | cockroachdb mysql postgres | cosmosdb couchbase db2i dsql dynamodb hetero mssql oracle saas sapodp tidb | the rest (14) | copybook db2zos duckdb kinesis parquet pubsub sqlite teradata |
| tail.added | cockroachdb postgres | mssql mysql tidb | the rest (22) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.readded | mysql | mssql tidb | the rest (24) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.ddl | mysql postgres | - | the rest (25) | copybook db2zos duckdb hetero kinesis parquet pubsub sqlite teradata |
| tail.renames | mysql | postgres | the rest (20) | cassandra copybook cosmosdb couchbase db2zos duckdb dynamodb hetero kafka kinesis parquet pubsub sqlite teradata |
| assess.versions | opensearch postgres redis sqlite | mongodb mysql | the rest (23) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.brands | kafka mongodb mysql opensearch postgres redis | tidb | the rest (22) | copybook delta hetero iceberg kinesis parquet pubsub |
| assess.engine-checks | bigquery db2zos generic hetero kafka mongodb mysql oracle postgres redis redshift sqlite | cockroachdb cosmosdb couchbase db2i dsql saas sapodp snowflake teradata tidb | the rest (14) | - |
| assess.preflight | postgres | - | the rest (31) | kinesis pubsub saas sapodp |
| assess.capacity | mongodb mysql postgres | clickhouse hetero tidb | the rest (28) | saas sapodp |
| assess.writers | mysql postgres redis | - | the rest (31) | kinesis pubsub |
| assess.handwork | mongodb mysql postgres | - | the rest (29) | copybook kinesis parquet pubsub |
| schema.create | the rest (23) | db2i db2zos dsql tidb | generic kafka | hetero kinesis mongodb pubsub redis saas sapodp |
| schema.indexes | mysql postgres | cockroachdb dsql sqlite teradata tidb | the rest (16) | copybook delta hetero iceberg kafka kinesis opensearch parquet pubsub redis redshift saas sapodp |
| schema.foreign-keys | mysql postgres | mssql oracle tidb | ase bigquery cockroachdb db2 db2i db2zos duckdb generic redshift snowflake sqlite teradata | the rest (19) |
| schema.defaults | mysql postgres sqlite | cockroachdb dsql sapodp teradata tidb | the rest (18) | cosmosdb couchbase dynamodb hetero kafka kinesis mongodb pubsub redis saas |
| schema.views | mssql mysql oracle | postgres tidb | the rest (18) | cassandra copybook cosmosdb delta dynamodb hetero kafka kinesis parquet pubsub redis saas sapodp |
| schema.routines | mssql mysql oracle postgres | tidb | the rest (19) | copybook delta dynamodb hetero iceberg kafka kinesis parquet pubsub saas sapodp sqlite |
| move.table-copy | the rest (26) | bigquery db2i db2zos dsql snowflake | generic | kinesis pubsub saas sapodp |
| move.bulk | the rest (15) | hetero | ase bigquery cockroachdb cosmosdb couchbase db2 db2i db2zos delta generic iceberg redshift snowflake tidb | dsql kinesis pubsub saas sapodp teradata |
| move.large-values | the rest (17) | bigquery db2i db2zos snowflake | cockroachdb cosmosdb couchbase dsql generic teradata tidb | cassandra copybook dynamodb kinesis pubsub redshift saas sapodp |
| move.partitioned | clickhouse kafka mysql postgres | - | the rest (19) | cassandra copybook cosmosdb couchbase duckdb dynamodb kinesis mongodb opensearch pubsub saas sapodp sqlite |
| move.sized | the rest (13) | cockroachdb db2i db2zos dsql oracle redshift snowflake teradata tidb | cassandra copybook cosmosdb couchbase delta dynamodb generic iceberg opensearch parquet saas sapodp | kinesis pubsub |
| move.load-window | hetero mysql oracle postgres | tidb | the rest (18) | bigquery copybook delta iceberg kafka kinesis parquet pubsub redis redshift saas sapodp snowflake |
| move.create-missing | hetero mysql postgres tidb | - | the rest (26) | kinesis mongodb pubsub redis saas sapodp |
| cutover.freeze | cassandra kafka mongodb mssql mysql opensearch postgres redis | - | the rest (21) | copybook duckdb dynamodb kinesis parquet pubsub sqlite |
| cutover.read-only-target | mysql postgres | - | the rest (30) | kinesis pubsub saas sapodp |
| cutover.counters | duckdb hetero mssql mysql oracle postgres sqlite tidb | - | ase bigquery cockroachdb db2 db2i db2zos dsql generic redshift snowflake | the rest (18) |
| cutover.rollback | cassandra clickhouse duckdb dynamodb hetero kafka mssql opensearch oracle parquet postgres redis sqlite | copybook mongodb mysql | the rest (15) | kinesis pubsub saas sapodp teradata |
| cutover.drain | the rest (15) | db2i saas | ase bigquery db2 delta generic hetero iceberg kafka redis redshift snowflake | copybook db2zos duckdb kinesis parquet pubsub sqlite teradata |
| cutover.pause-replication | mysql postgres | - | generic redis | the rest (32) |
| cutover.two-way | mysql postgres teradata | - | the rest (23) | copybook db2zos duckdb hetero kinesis parquet pubsub saas sapodp sqlite |
| cutover.jobs | mysql postgres | - | ase bigquery clickhouse cockroachdb couchbase db2 db2i db2zos generic mssql opensearch oracle redshift snowflake teradata | the rest (19) |
| security.tls | the rest (22) | cockroachdb copybook dsql duckdb parquet tidb | ase db2 db2zos generic mssql oracle saas | hetero |
| managed.catalogued | the rest (19) | - | ase couchbase db2i db2zos delta duckdb generic iceberg parquet saas sapodp snowflake sqlite teradata tidb | copybook hetero |
| operations.fencing | mysql postgres | tidb | the rest (30) | hetero saas sapodp |

## Backlog (444 items, `docs/backlog-index.md`)

DONE 261 - PARTLY 49 - IN PROGRESS 18 - OPEN 87 - BLOCKED 20 - NOT PURSUED 9

| area | DONE | PARTLY | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED |
|---|---|---|---|---|---|---|
| change follow | 34 | 3 | 1 | 20 | 5 | 0 |
| conversion | 18 | 3 | 9 | 1 | 0 | 0 |
| cutover | 14 | 1 | 1 | 4 | 0 | 0 |
| ease of use | 19 | 2 | 1 | 3 | 1 | 1 |
| engines | 26 | 6 | 3 | 14 | 6 | 1 |
| move | 29 | 10 | 2 | 11 | 0 | 2 |
| operations/scale | 26 | 9 | 0 | 11 | 5 | 3 |
| security | 10 | 2 | 0 | 4 | 2 | 1 |
| tools used whole | 30 | 6 | 0 | 4 | 0 | 0 |
| two-way | 4 | 2 | 0 | 3 | 0 | 0 |
| types | 11 | 1 | 1 | 1 | 0 | 0 |
| verify | 40 | 4 | 0 | 11 | 1 | 1 |

Not done (174): P0-DE.1, P0-DE.2, P0-DE.3, P0-DE.4, BAR-1, BAR-2, BAR-3, 0f-DVT, 0f-REST, 0a.measure, 0b.window, 6.mariadb, 6.fkwin, 7.free, R9.docdb, A2/D10, B4, C1.managed, F1.dual, P3.resumedump, P3.dryrun, R6.cloud, 26.rest, 26.trino, 26.striim, 26.bytebase, MSSQL-x86, AUD.slot.process, AUD.slot.more, AUD.osc.tail, AUD.atlas, AUD.release, P0-DEPTH.constants, P0-DEPTH.calibrate, P0-DEPTH.json, P0-DEPTH.triggers, P0-DEPTH.rank, P0-DEPTH.tombstones, P0-AUDIT, AUD-B, AUD-C, AUD-D, AUD-E.pin, AUD-F, AUD-G2, AUD-H, AUD-T2, AUD-Z, P0-PARITY.lv, P0-PARITY.part, P0-PARITY.small, P0-PARITY.lv.new, P0-SMART.route, P0-CLOUDS.native, P0-CLOUDS.more, DEC1001.media, DEC1001.gui, P0-SCENARIOS.brands, P0-SCENARIOS.managed, P0-SCENARIOS.arch, P0-SCENARIOS.charset, P0-SCENARIOS.engine, P0-SCENARIOS.consolidate, DEC30.sweep, DEC.terms.odbc, DEC.deps, DEC.ptcs, S1.e, S3.link, S4, F2.freeze, F3, F7.beat, F7.edges, F7.sec, CV.P5, CV.P9, CV.P10, CV.P11, CV.P15, CV.P17, CV.P19, CV.P20, CV.P21, W3, ER1.acct, ER4, ER5, ER6, W5, LT.R1, LT.R2, LT.R3, LT.R6, LT.R7, LT.R8, LT.R9, LT.R10, LT.C1, LT.C2, LT.C3, LT.C4, LT.C5, LT.C6, LT.P1, LT.P2, LT.P3, W1, W2, W4.a, W4.b, W6, W7, W8, W9, 28.rates, R5.actions, R5.anchor, 32.rds, 45.run, R4.msk, R4.formats, R3.topo, 38.real, R17b.hmac, R17b.prog, DOCS, 43.fix, R8.more, R8.mv, R10.pieces, R12.minio, R13-DB2, R13-ASE, R13-HETERO, R13-GENERIC, R13.kafka, R13.redis, R13.sqlite, R13.parquet, R15.2nd, R16b, R16c, R17d, R18.6/R20.2, R18.7, R19.1, R19.3, R19.9/R20.3, R20.5, R20.6, R20.7, R20.10, R20.11, R21.7, R21.8, R21.9, R21.M, DTS.ops, DTS.sample, DTS.gtid, DTS.csfilter, REL, REL.gate, R23.4, R23.7, R23.8, R25.2, R25.3, P0-SCOPE.open, P0-PARITY.scope, P0-PARITY.verify, P0-PARITY.tail, P0-PARITY.all

