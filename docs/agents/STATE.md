# migkit state

Written by `tools/gen_codemap.py` from `capabilities.matrix()`, `decide.coverage()` and `docs/backlog-index.md`; the pre-commit hook keeps it current. What this machine last proved (the full suite, unpushed work) is in `.codemap/LOCAL.md`.

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
| mssql | 16/16 | 10/11 | partitioned |
| mysql | 16/16 | 11/11 | - |
| opensearch | 16/16 | 11/11 | - |
| oracle | 16/16 | 10/11 | partitioned |
| parquet | 16/16 | 10/11 | partitioned |
| postgres | 16/16 | 11/11 | - |
| pubsub | 16/16 | 11/11 | - |
| redis | 16/16 | 10/11 | partitioned |
| redshift | 5/16 | 10/11 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned |
| saas | 16/16 | 11/11 | - |
| sapodp | 16/16 | 11/11 | - |
| snowflake | 5/16 | 10/11 | deep, sequences, params, bulk-move, stream, fence, confirm, delta, users, statistics, snapshot, partitioned |
| sqlite | 16/16 | 11/11 | - |
| teradata | 16/16 | 9/11 | large values, partitioned |
| tidb | 13/16 | 9/11 | bulk-move, users, snapshot, large values, partitioned |

## Backlog (445 items, `docs/backlog-index.md`)

DONE 282 - PARTLY 46 - IN PROGRESS 8 - OPEN 80 - BLOCKED 20 - NOT PURSUED 9

| area | DONE | PARTLY | IN PROGRESS | OPEN | BLOCKED | NOT PURSUED |
|---|---|---|---|---|---|---|
| change follow | 39 | 3 | 1 | 15 | 5 | 0 |
| conversion | 29 | 1 | 0 | 1 | 0 | 0 |
| cutover | 14 | 1 | 1 | 4 | 0 | 0 |
| ease of use | 19 | 2 | 1 | 3 | 1 | 1 |
| engines | 24 | 5 | 3 | 17 | 6 | 1 |
| move | 30 | 10 | 2 | 10 | 0 | 2 |
| operations/scale | 26 | 9 | 0 | 11 | 5 | 3 |
| security | 10 | 2 | 0 | 4 | 2 | 1 |
| tools used whole | 32 | 6 | 0 | 2 | 0 | 0 |
| two-way | 4 | 2 | 0 | 3 | 0 | 0 |
| types | 13 | 1 | 0 | 0 | 0 | 0 |
| verify | 42 | 4 | 0 | 10 | 1 | 1 |

Not done (154): P0-DE.1, P0-DE.2, P0-DE.3, P0-DE.4, BAR-1, BAR-2, BAR-3, 0f-DVT, 0f-REST, 0a.measure, 0b.window, 6.mariadb, 6.fkwin, 7.free, R9.docdb, A2/D10, C1.managed, F1.dual, P3.dryrun, R6.cloud, 26.versions, 26.trino, 26.striim, MSSQL-x86, AUD.slot.more, AUD.release, P0-DEPTH.constants, P0-DEPTH.calibrate, P0-DEPTH.json, P0-DEPTH.triggers, P0-DEPTH.rank, P0-DEPTH.tombstones, P0-AUDIT, AUD-B, AUD-C, AUD-D, AUD-E.pin, AUD-F, AUD-G2, AUD-H, AUD-T2, AUD-Z, P0-PARITY.lv, P0-PARITY.part, P0-PARITY.small, P0-PARITY.lv.new, P0-SMART.route, P0-CLOUDS.native, P0-CLOUDS.more, DEC1001.media, DEC1001.gui, P0-SCENARIOS.brands, P0-SCENARIOS.managed, P0-SCENARIOS.arch, P0-SCENARIOS.charset, P0-SCENARIOS.engine, P0-SCENARIOS.consolidate, DEC30.sweep, DEC.deps, DEC.ptcs, S1.e, S3.link, S4, F2.freeze, F3, F7.beat, F7.edges, F7.sec, ER1.acct, ER4, ER5, ER6, W5, LT.R1, LT.R2, LT.R3, LT.R6, LT.R7, LT.R8, LT.R9, LT.R10, LT.C1, LT.C2, LT.C3, LT.C4, LT.C5, LT.C6, LT.P1, LT.P2, LT.P3, W1, W2, W4.a, W4.b, W7, W8, W9, 28.rates, R5.actions, R5.anchor, 32.rds, 45.run, R4.msk, R4.formats, R3.topo, 38.real, R17b.hmac, R17b.prog, DOCS, 43.fix, R8.more, R8.mv, R10.pieces, R12.minio, R13-DB2, R13-ASE, R13-HETERO, R13-GENERIC, R13.kafka, R13.redis, R13.sqlite, R13.parquet, R15.2nd, R16b, R16c, R17d, R18.6/R20.2, R18.7, R19.1, R19.3, R19.9/R20.3, R20.5, R20.6, R20.7, R20.10, R20.11, R21.7, R21.8, R21.9, R21.M, DTS.sample, REL, REL.gate, R23.7, R23.8, R25.2, R25.3, P0-SCOPE.open, P0-PARITY.scope, P0-PARITY.verify, P0-PARITY.tail, P0-PARITY.matrix, P0-PARITY.all, P0-PARITY.guard

