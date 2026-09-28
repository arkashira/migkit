# MongoDB migration and verification tools: research report for a Python wrapper (as of 2026-09-27)

Web research only. No local files were created or changed. One note: when I fetched `downloads.mongodb.org/tools/db/full.json`, the WebFetch tool saved its response in its own cache (`~/.claude/projects/.../tool-results/webfetch-*.bin`). That is outside the project and nothing was run.

---

## 1. mongosync (Cluster-to-Cluster Sync)

**Version and status.** Latest is 1.22.0, released 2026-09-09. It is built with Go 1.26.7. The macOS build is now arm64 only, and Amazon Linux 2 builds have stopped. Live upgrades to 1.22 are not supported. Starting in 1.22, the source user also needs `find` and `collStats` on `local.oplog.rs`. https://www.mongodb.com/docs/mongosync/current/release-notes/1.22/

**Distribution.** Tarballs for Amazon Linux 2023 (x86_64 and ARM64), RHEL 7/8/9 and Ubuntu 20.04/22.04/24.04. https://www.mongodb.com/docs/mongosync/current/installation/install-on-linux/

**License.** Closed-source and proprietary. MongoDB says it is "free for Atlas and Enterprise Advanced users"; Community Edition users are told to contact their account team. https://www.mongodb.com/products/tools/mongosync
- The download archive says that for non-customers, downloading and using it counts as accepting the **Customer Agreement**. https://www.mongodb.com/download-center/mongosync/releases/archive
- It is not tested with Community Edition. https://www.mongodb.com/docs/mongosync/current/reference/limitations/

**Telemetry.** On by default since 1.4. Turn it off with `--disableTelemetry`. https://mongodb.com/docs/cluster-to-cluster-sync/current/reference/telemetry

**Server versions.**
- Source: 5.0, 6.0, 7.0 or 8.0. Destination: 7.0 or 8.0 only.
- 8.0 source requires an 8.0 destination.
- Minimum patches: 5.0.29, 6.0.24, 7.0.18, 8.0.5.
- Rapid releases (8.1 to 8.3) are not supported.
- https://www.mongodb.com/docs/mongosync/current/reference/supported-server-version/

**How it works.**
- It copies collections, then applies change-stream events (the CEA phase). CEA has two stages: "collection copy drain", then "steady state".
- Writes are combined and reordered, so the destination is only eventually consistent until commit. Insert order does not preserve natural order.
- Destination collections get **new UUIDs**.
- Reads use majority read concern. Writes use `w:majority, j:true`. Read preference must be primary.
- https://www.mongodb.com/docs/mongosync/current/reference/mongosync/mongosync-behavior/

**Metadata it writes.** It stores its state in `__mdb_internal_mongosync` and `__mdb_internal_mongosync_verifier*` on the destination. The process itself is stateless: restart it with the same parameters to resume. If a source outage lasts longer than the oplog window, it cannot resume. (same URL)

**Temporary changes on the destination, restored at commit.**
- Unique indexes are created as non-unique.
- TTL `expireAfterSeconds` is set to MAX_INT.
- Hidden indexes are created visible.
- Capped collections get a 1 PB cap. These need `--enableCappedCollectionHandling` (1.20+).
- (same URL)

**Sharded clusters.**
- Run one mongosync per shard using `--id`, and send identical API calls to every instance. https://www.mongodb.com/docs/mongosync/current/reference/mongosync-binary/
- Connect through mongos.
- The balancer must be stopped on the destination (wait 15 minutes after stopping it). On the source it must be off, or off per collection for filtered sync.
- mongosync pre-splits about 90 chunks per collection. It does not preserve chunk distribution or zones.
- Sharded source to replica-set destination is not supported.
- Running `shardCollection` inside the filter during sync is fatal.
- https://www.mongodb.com/docs/mongosync/current/reference/limitations/

**REST API.** Default port 27182, bound to localhost, no authentication.

| Endpoint | Allowed in state |
|---|---|
| `POST /api/v1/start` | IDLE |
| `GET /progress` | any |
| `POST /pause` | RUNNING |
| `POST /resume` | PAUSED |
| `POST /commit` | RUNNING, and only when `canCommit` is true |
| `POST /reverse` | COMMITTED |

- States: INITIALIZING, IDLE, RUNNING, PAUSED, COMMITTING, COMMITTED, REVERSING. https://www.mongodb.com/docs/mongosync/current/reference/mongosync-states/
- Responses are `{success, error, errorDescription}`. https://www.mongodb.com/docs/mongosync/current/reference/api/start/

**`/start` body.** Required: `source`, `destination`. Options:
- `buildIndexes`: `afterDataCopy` (the default when the source is 6.0+), `beforeDataCopy`, `excludeHashed`, `excludeHashedAfterCopy`, `never`.
- `includeNamespaces` / `excludeNamespaces`: a list of `{database, collections[], collectionsRegex:{pattern, options}}`.
- `reversible`
- `preExistingDestinationData` (preview)
- `skipDiskSpaceCheck`. There is a 1.25× disk-space pre-check that returns `InsufficientDestinationDiskSpace`.
- `detectRandomId` (default true; collections over 20 GiB with random `_id` are copied in natural order)
- `copyInNaturalOrder`
- `sharding.shardingEntries`
- `verification.enabled` (default true)
- https://www.mongodb.com/docs/mongosync/current/reference/api/start/

**`/progress` fields.**
- `state`, `canCommit`, `canWrite`, `info`, `ceaStage`
- `lag.{overallLagSeconds, crudLagSeconds, ddlLagSeconds}`. `lagTimeSeconds` is deprecated since 1.21.
- `collectionCopy.{estimatedTotalBytes, estimatedCopiedBytes}`
- `indexBuilding.*`, `totalEventsApplied`, `estimatedOplogTimeRemaining`, `estimatedSecondsToCEACatchup`, `warnings[]`
- `verification.{source,destination}.{phase, hashedDocumentCount, estimatedDocumentCount, lagTimeSeconds, scannedCollectionCount, totalCollectionCount}`
- https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/api/progress/

**Commit and write blocking.**
- By default both source and destination are write-blocked. With filtering, or with a pre-6.0 source, only the destination is blocked. https://www.mongodb.com/docs/mongosync/current/reference/mongosync/mongosync-behavior/
- `canCommit` is false if mongosync or the verifier lags by more than 30 seconds (1.21+).
- With `afterDataCopy`, commit waits until all indexes are built.
- If mongosync stops before `canWrite:true`, the whole migration must be restarted.
- Writing to the source during commit can lose data.
- https://www.mongodb.com/docs/mongosync/current/reference/api/commit/

**Reverse.** Requires all of the following:
- `reversible:true` set at start
- The destination oplog has not rolled over since `canWrite`
- Same shard count and same major version on both sides
- No filtering, no pre-6.0 source, no legacy unique indexes
- https://www.mongodb.com/docs/mongosync/current/reference/api/reverse/

**Embedded verifier.**
- Added in 1.9. On by default for replica sets since 1.9 and for sharded clusters since 1.10.
- It does "initial hashing", then "stream hashing". Since 1.15 it also checks metadata, indexes and views. Since 1.22 it starts at CEA.
- It **cannot resume**: a pause or restart makes it start over.
- It needs **10 GB RAM + 500 MB per 1M documents**.
- It skips capped collections, TTL collections and collections with a non-default collation.
- It can fail a migration falsely when field order differs between source nodes.
- https://www.mongodb.com/docs/mongosync/current/reference/verification/embedded/

**Logs and roles.**
- Logs are JSON lines (`level`, `mongosyncID`, `componentName`, `time`, `message`) written to `mongosync.log`. Rotate with `SIGUSR1`. https://www.mongodb.com/docs/mongosync/current/reference/logging/
- Other flags: `--loadLevel 1-4`, `--verbosity`, `--metricsLoggingFilepath`. https://www.mongodb.com/docs/mongosync/current/reference/mongosync-binary/
- Roles on both sides: `backup`, `clusterManager`, `clusterMonitor`, `readWriteAnyDatabase`, `restore`. Add `dbAdminAnyDatabase` for reversing. https://www.mongodb.com/docs/mongosync/current/connecting/onprem-to-onprem/

**Hard limits and known failure modes** (all from https://www.mongodb.com/docs/mongosync/current/reference/limitations/):
- Not supported: time-series collections, clustered collections with TTL, `$`-prefixed or duplicate field names, QE/CSFLE, users/roles, Atlas Search indexes, `applyOps`.
- A collection cannot have both a unique and a non-unique index on the same fields.
- Sharded collections are limited to 63 indexes.
- **8.0 below 8.0.20:** a replace that adds `$v` is fatal and forces a restart from scratch.
- `afterDataCopy` can fail to apply a Rename DDL.
- mongosync does not check whether you meet its limitations; breaking them causes undefined behaviour.
- 1.22 fixed a bug where lag grew without bound when capped collections were included. https://www.mongodb.com/docs/mongosync/current/release-notes/1.22/

## 2. mongomirror

- **EOL since 2025-07-31**, replaced by mongosync. No updates, patches or support. https://www.mongodb.com/community/forums/t/mongomirror-upcoming-eol-on-july-31st-2025/314595
- It copied a replica set into Atlas: initial sync, then it tailed the oplog and replayed it on the destination.
- Sources 2.6 to 5.0, targets 6.0 only. Not for 6.0+ to 6.0+.
- No users/roles, TTL or time-series.
- Closed-source MongoDB binary. I did not find an OSS license.
- https://www.mongodb.com/docs/atlas/import/mongomirror/

## 3. mongodb-labs/migration-verifier

**Status and distribution.**
- Latest is v0.2.4 (2026-08-25). Assets: `migration_verifier_vX_{darwin_arm64,linux_amd64,linux_arm64}.tar.gz`. https://api.github.com/repos/mongodb-labs/migration-verifier/releases
- License is **Apache-2.0** per the LICENSE file, although the GitHub API reports NOASSERTION. https://raw.githubusercontent.com/mongodb-labs/migration-verifier/main/LICENSE
- Actively maintained: last push 2026-08-25, 16 open issues. https://api.github.com/repos/mongodb-labs/migration-verifier
- MongoDB's docs say it supports server 4.2+. https://www.mongodb.com/docs/mongosync/current/reference/verification/verifier/

**Partitioning (`_id` scheme, the default).**
1. `$collStats` gives size and count.
2. `numPartitions = ceil(sizeBytes × filteredRatio / partitionSize)`. The default partition size is **400 MiB**.
3. `$sample` of min(4% of documents, 3 × numPartitions).
4. `$bucketAuto` on `_id` sets the boundaries, with MinKey/MaxKey as the outer bounds.
5. Capped collections and collections under 101 documents get one partition. If partitioning with a filter times out, it retries without the filter.
- Source: https://github.com/mongodb-labs/migration-verifier/blob/main/internal/partitions/partitions.go

**`natural` scheme.** Partitions by record ID and fetches the matching destination documents by `_id`. Replica set only. It cannot detect documents that exist only on the destination unless they change. Before 4.2 there is one task per collection. https://raw.githubusercontent.com/mongodb-labs/migration-verifier/main/README.md

**Comparison.**
- Both sides are read with `readConcern majority` and `afterClusterTime`, sorted by `_id`. Documents are compared with `bytes.Equal` on raw BSON first. If that fails, an unordered compare runs; if only field order differs, that is still reported as a mismatch unless `ignoreFieldOrder` is set. https://raw.githubusercontent.com/mongodb-labs/migration-verifier/main/internal/verifier/compare.go
- `--docCompareMethod` options (https://github.com/mongodb-labs/migration-verifier):
  - `binary` (default)
  - `ignoreFieldOrder`
  - `toHashedIndexKey`: compares hash plus length only. Long and Double with the same value compare equal, and there is no field-level detail.

**Generations, rechecks and writesOff.**
- Generation 0 is a full scan.
- Change events come from `--srcChangeReader` / `--dstChangeReader`, which is `changeStream` (default) or `tailOplog` (replica set only). Changed documents and failed comparisons are queued in `recheckQueue_gen{N+1}` for the next generation.
- Recheck tasks are at most 10,000 IDs or 1 MiB.
- https://raw.githubusercontent.com/mongodb-labs/migration-verifier/main/internal/verifier/recheck.go
- On `writesOff`, it waits for the change reader to finish, then sets `lastGeneration`. The loop exits after that generation. https://raw.githubusercontent.com/mongodb-labs/migration-verifier/main/internal/verifier/check.go
- Verification has passed when `phase=="idle"`, `error` is null and `failedTasks==0`. https://raw.githubusercontent.com/mongodb-labs/migration-verifier/main/README.md

**Metadata.** Stored in `__mdb_internal_migration_verifier` on the destination by default (`--metaDBName`, or a separate cluster via `--metaURI`). It holds tasks, the recheck queues, mismatches and resume tokens. Each generation also compares collection specs, indexes, views, shard keys and read-only status. `--indexSpecIgnore` can skip `expireAfterSeconds` and `unique`. https://github.com/mongodb-labs/migration-verifier

**REST API.** Port 27020.
- `POST /api/v1/check` (optional `filter`), `POST /writesOff`
- `GET /progress`: `phase`, `generation`, `generationStats`, `verificationStatus.{failedTasks,…}`, and `srcChangeStats` / `dstChangeStats` with `lagSecs`
- `GET /summary`
- `GET /docMismatches` and `/nsMismatches`, both as **NDJSON**
- https://github.com/mongodb-labs/migration-verifier

**Limits and bugs.**
- DDL makes it crash by default (`--ddlHandling failAll`). `warnMost` still crashes on drop or rename.
- Time-series collections cannot be verified under namespace filtering.
- If the oplog rolls over, the failure is permanent.
- Generations keep growing when the write rate is higher than the recheck rate. It has been tested at about 15,000 writes/s.
- https://github.com/mongodb-labs/migration-verifier
- Open bugs: #310 (a `/progress` call before `/check` causes a nil-pointer panic) and #197 (`--checkOnly` panics). https://api.github.com/repos/mongodb-labs/migration-verifier/issues

## 4. mongodump / mongorestore (Database Tools 100.x)

**Version and license.**
- Latest is 100.19.0 (2026-09-22). It fixes TOOLS-4355: unbounded memory use in mongorestore on untrusted archives. It moves to Go 1.26.7 and Go driver 2.8.2. https://www.mongodb.com/docs/database-tools/release-notes/dbtools-100.19.0-changelog/
- 100.18 added server 9.0 support. https://www.mongodb.com/docs/database-tools/release-notes/database-tools-changelog/
- License: Apache-2.0, based on the mongo-tools repo. The GitHub API reports NOASSERTION, and I did not re-check the license file in this session. https://github.com/mongodb/mongo-tools
- Per-OS/arch artifacts for 100.19 are not verified. The download page renders them dynamically.

**Oplog consistency.**
- `mongodump --oplog` gives a point-in-time dump of a **whole replica set member only**. It cannot be combined with `--db`, `--collection` or `--query`, and it does not work on sharded clusters or mongos.
- It fails if renameCollection, `$out`, mapReduce, or user/role changes happen during the dump.
- Default `--numParallelCollections` is 4. `--viewsAsCollections` dumps views as data.
- On sharded clusters, pause the balancer, DDL and cross-shard transactions first. Do not change FCV between 8.x and 9.0 while a dump is running if there are time-series collections.
- https://www.mongodb.com/docs/database-tools/mongodump/

**mongorestore options** (https://www.mongodb.com/docs/database-tools/mongorestore/):
- `--oplogReplay`: not on sharded clusters, and not together with ns filters or renames. `--oplogLimit ts:ord` sets an end point.
- Namespace filters: `--nsInclude` / `--nsExclude` use `*` wildcards, and exclude wins. `--nsFrom` / `--nsTo` use `*` or `$var$` renames.
- Parallelism: `-j` / `--numParallelCollections` (default 4). `--numInsertionWorkersPerCollection` (default 1). Each collection runs on one insertion worker by default, which is the known bottleneck for single huge collections.
- `--archive` (stdin/stdout streaming) and `--gzip`.
- `--noIndexRestore`
- `--preserveUUID`: requires `--drop`, not on sharded clusters.
- `--bypassDocumentValidation`
- `--convertLegacyIndexes`: removes invalid index options and rewrites legacy key values to 1.
- `--maintainInsertionOrder`: forces `--stopOnError` and one worker.
- `--fixDottedHashIndex`
- Default `writeConcern` is majority.
- By default it continues past duplicate-key and validation errors.
- It restores only into the same major version or FCV as the source. It rejects QE/CSFLE collections.

**Progress output.** Not JSON; everything goes to **stderr**.
- Format: `<ts>\t[####....] ns x/y (pct%)`, a 24-character bar redrawn every 3 seconds.
- Other lines: `done dumping …`, `finished restoring … (N documents, M failures)`, a final `N document(s) restored successfully. M document(s) failed to restore.`, and a `Failed:` prefix on fatal errors.
- Sources: https://pkg.go.dev/github.com/mongodb/mongo-tools/common/progress, https://github.com/mongodb/mongo-tools/blob/master/mongodump/main/mongodump.go, https://jira.mongodb.org/browse/TOOLS-2186

## 5. MongoShake (Alibaba)

**Status and license.**
- Latest is v2.8.8 (2026-04-28). It added filtering by operation type, Prometheus metrics and time-series handling. Shipped as a single `mongo-shake-vX.tgz`. https://api.github.com/repos/alibaba/MongoShake/releases
- License is **GPL-3.0** (copyleft). The repo is active: last push 2026-09-21. https://api.github.com/repos/alibaba/MongoShake

**Architecture.**
- A collector fetches the oplog, and a receiver or tunnel applies it. Tunnels: direct (MongoDB), rpc, tcp, file, kafka, mock.
- Checkpoints go to a "register center", which is the source database by default. https://github.com/alibaba/MongoShake

**Configuration** (https://raw.githubusercontent.com/alibaba/MongoShake/develop/conf/collector.conf):
- `sync_mode`: all, full or incr.
- `incr_sync.mongo_fetch_method`: `oplog` or `change_stream`. Sharded sources use `mongo_s_url`.
- Checkpoint: `checkpoint.storage.url` / `.db=mongoshake` / `.collection=ckpt_default`, plus `checkpoint.start_position`.
- Full-sync parallelism: `full_sync.reader.collection_parallel=6`, `write_document_parallel=8`, `parallel_thread` / `parallel_index=_id` for splitting inside a collection.
- `full_sync.create_index`: none, foreground or background.
- Incremental replay: `incr_sync.shard_key` = id, collection or auto (auto picks based on whether a unique index exists).
- Conflicts: `incr_sync.executor.upsert`, `insert_on_dup_update`, `conflict_write_to`.
- Filters: `filter.namespace.white` / `black`, `filter.ddl_enable=false`.
- Ports: 9100 (incremental), 9101 (full sync), 9102 (Prometheus).

**Limits and failure modes** (https://github.com/alibaba/MongoShake/wiki/FAQ):
- MongoDB 3.0+ only. Transactions are supported since 1.6.0.
- The balancer must be off on sharded sources.
- DDL replay is not idempotent, so it is risky.
- If the oplog purges past the full-sync start, it fails with "load checkpoint queryTs is less than oldTs".
- Oplog cursor timeouts.
- Replaying per document (`id`) requires that there are no unique indexes. https://github.com/alibaba/MongoShake/wiki/MongoShake-Detailed-Documentation
- REST endpoints on port 9100: `/repl` (lsn_ack, lsn_ckpt), `/worker`, `/sentinel`.

**Verification companion.** There is no real "full-check" tool for MongoDB. The bundled `comparison.py` only compares database, collection and document counts and checks that `_id`s exist. NimoFullCheck is for DynamoDB to MongoDB. RedisFullCheck is where the multi-round recheck idea comes from. https://github.com/alibaba/MongoShake/wiki/FAQ, https://www.alibabacloud.com/help/en/mongodb/user-guide/use-nimofullcheck-to-check-data-consistency-after-migration

## 6. Monstache

- **What it does:** a daemon that syncs MongoDB to Elasticsearch in near real time. License **MIT**. https://api.github.com/repos/rwynn/monstache
- **Low maintenance:** the last release is v6.8.0 (2025-08-22), there is nothing in 2026, and there are 271 open issues. Binaries for darwin/linux/windows on arm64 and x86_64. https://api.github.com/repos/rwynn/monstache/releases
- **Compatibility:** v6 targets Elasticsearch 7+. OpenSearch is not documented. https://rwynn.github.io/monstache-site/start/
- **Resume:** progress is stored in `monstache.monstache`. `resume-strategy` 0 uses a timestamp (4.0+), 1 uses a token (3.6+).
- **Other options:**
  - `change-stream-namespaces`
  - `direct-read-namespaces` for a full copy, with `direct-read-split-max` (default 9, -1 turns splitting off) and `direct-read-stateful`
  - `cluster-name` for HA
  - `enable-http-server` on `:8080`
  - https://rwynn.github.io/monstache-site/config/

## 7. mongo-connector (legacy)

- **Unmaintained.** Last PyPI release is 3.1.1 (2018-12-05), for MongoDB 3.4/3.6. https://pypi.org/pypi/mongo-connector/json
- License Apache-2.0. Last push 2024-03-27, 262 open issues; not formally archived. https://api.github.com/repos/yougov/mongo-connector
- Users describe it as "not maintained anymore". https://github.com/yougov/mongo-connector/issues/916

## 8. Fast copy from Python, and server helpers

**pymongo.**
- Latest is 4.18.2, Apache-2.0. Extras: `zstd` (needs `backports.zstd` before Python 3.14) and `snappy` (python-snappy). zlib is built in. Compression is off unless you set `compressors=`. https://pypi.org/pypi/pymongo/json, https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
- `find_raw_batches` / `aggregate_raw_batches` return a `RawBatchCursor` of raw BSON bytes. They do not support auto-encryption. `insert_many` accepts `RawBSONDocument` and `ordered=False`, and the driver splits batches automatically. https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html
- `RawBSONDocument` only decodes lazily and is recommended for moving documents between databases. https://pymongo.readthedocs.io/en/stable/api/bson/raw_bson.html
- `MongoClient.bulk_write` (pymongo 4.9+, **server 8.0+**) writes across namespaces, e.g. `InsertOne(namespace="db.coll", document=…)`. Errors raise `ClientBulkWriteException`, and nothing after the failure runs. https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/bulk-write/
- **I found no published throughput benchmarks** for raw-batch copying. The only related number: python-bsonjs is 3 to 4 times faster than json_util. https://github.com/mongodb-labs/python-bsonjs

**PyMongoArrow.** Version 1.15.0 (2026-07-17), Apache-2.0, wheels for macOS arm64 and manylinux aarch64. It is built for typed, columnar analytics, not for faithful BSON copies. I could not verify its type-fidelity docs (the page returned 404). https://pypi.org/pypi/pymongoarrow/json, https://api.github.com/repos/mongodb-labs/mongo-arrow/releases

**Server helpers.**
- **`$toHashedIndexKey`** returns the 64-bit Long hash that hashed indexes use. Numerically equal Long and Double hash the same, and collation is ignored. https://www.mongodb.com/docs/manual/reference/operator/aggregation/toHashedIndexKey/ It exists since 4.7, with backports to 4.4.10 and 4.2.24. https://jira.mongodb.org/browse/SERVER-49214
- **`$hash` / `$hexHash`** are new in **8.3** (md5, sha256 or xxh64). They only accept a string or BinData, so they cannot hash a whole document. They are also rapid-release only, which mongosync does not support. https://www.mongodb.com/docs/manual/reference/operator/aggregation/hash/
- **`$collStats`** must be the first stage and returns one document per shard. Its `count` comes from metadata. https://www.mongodb.com/docs/manual/reference/operator/aggregation/collStats/ The `collStats` command is deprecated since 6.2. https://www.mongodb.com/docs/manual/reference/command/collStats/
- **`$sample`** uses a pseudo-random cursor only when it is the first stage, N is under 5%, and the collection has more than 100 documents. Otherwise it scans and randomly sorts everything. On sharded clusters each shard samples. https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/
- **`$bucketAuto`** sorts all of its input, spills past 100 MB, and can return fewer buckets than requested. `min` is inclusive and `max` is exclusive, except in the last bucket. https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucketAuto/
- **`splitVector`** is an **internal, undocumented** command; its docs page now redirects to the command index. It runs on the shard's mongod (not mongos), `AllowedOnSecondary::kNever`, and rejects time-series collections. Parameters: `keyPattern`, `min`, `max`, `maxChunkSize` (MB), `maxChunkSizeBytes`, `maxSplitPoints`, `maxChunkObjects`. It needs the `splitVector` action. https://raw.githubusercontent.com/mongodb/mongo/master/src/mongo/db/s/split_vector_command.cpp

## 9. dbHash

**What it hashes.**
- MD5 over the **full raw BSON bytes of every document**, in `_id` index order.
- Capped collections use natural order unless you pass `useIndexScanForCappedCollections`. Clustered collections, or runs with `includeReplicatedRecordIds`, also use natural order.
- **Indexes and collection options are not hashed.**
- It skips `tmp.mr.*`, unreplicated namespaces, and optionally temp collections (`skipTempCollections`). The `collections: []` option narrows the set.
- It takes an **S lock on the database, which blocks writes**. The IS-lock snapshot mode is test-only.
- https://raw.githubusercontent.com/mongodb/mongo/master/src/mongo/db/commands/dbhash.cpp
- It returns `collections{}`, `capped[]`, `uuids{}`, `md5` and `timeMillis`. MongoDB describes it for comparing across mongod instances, "such as across members of replica sets". https://www.mongodb.com/docs/manual/reference/command/dbHash/

**When two clusters' values are comparable.** This is my inference from the source code above:
- Hashes match only if both sides have the same documents with **byte-identical BSON**: same field order and same numeric types.
- The `_id` ordering must also be identical, which means the same collation on the `_id` index.
- It is not usable for capped collections copied by mongosync or mongorestore, because natural order is not preserved.
- It runs on mongod only. For sharded clusters you would hash each shard, and those per-shard values only line up when both clusters have the same chunk placement. mongosync does not keep chunk placement.
- UUIDs differ between clusters, but they are returned separately and are not part of `md5`.
