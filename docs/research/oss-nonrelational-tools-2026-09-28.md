# Open-source non-relational tools, the layer above the engine reports: wrap plans, decision rules, scorecard (2026-09-28)

**Status (written 2026-09-28, stopped on the owner's token limit):**
§1 code inventory complete; §2 wrap plans complete for MongoDB, Redis,
Kafka, Cassandra/Scylla, ClickHouse, OpenSearch, DynamoDB, files/lakes;
§3 gaps settled except the four marked "(verify)"; §4 new engines
complete for Neo4j, Milvus/Qdrant, Iceberg/Delta, the three warehouses,
Trino/Spark, pipelines; §5 table, §6 scorecard and §7 build list
complete. Partial: Redpanda Connect linux-arm64 assets, Qdrant `order_by`
index rule, APOC per-procedure Core/Extended split, `_partition_id`
through `remote()` - each one docker run or one page fetch away.

Public research plus a read of migkit's own code. Nothing was run, installed
or connected. This report does not repeat the six tool reports of
2026-09-27; it cites them by section and adds only what they lacked:

* `mongodb-tools-2026-09-27.md` (mongosync §1, mongomirror §2,
  migration-verifier §3, mongodump §4, MongoShake §5, Monstache §6, pymongo
  and server helpers §8, dbHash §9)
* `redis-tools-2026-09-27.md` (RedisShake §1, RIOT/RIOT-X §2, RDB parsers §3,
  DUMP/RESTORE/MIGRATE §5, notifications and CLIENT TRACKING §6, PSYNC and
  managed services §7, clients §8, redis-full-check §9)
* `kafka-tools-2026-09-27.md` (MM2 §1, Replicator §2, kcat §4, clients §5,
  Redpanda §6, offset-preserving options §7, copier pitfalls §8)
* `cassandra-scylla-tools-2026-09-27.md` (DSBulk §1, CDM §2, ZDM §3,
  scylla-migrator §4, sstables §5, CDC §6, drivers §7, verification models §8)
* `clickhouse-opensearch-tools-2026-09-27.md` (clickhouse-backup §1,
  copier §2, remote() §3, clickhouse-local/chDB §4, BACKUP/RESTORE §5, part
  hashes §6, PeerDB §7, table functions §9, elasticdump §10, Migration
  Assistant §11, remote reindex §12, snapshots §13, CCR §14, clients §17)
* `dynamodb-lakes-pipelines-2026-09-27.md` (export/import §1-2, dynamodump
  §3, streams §4, write patterns §5, rclone §7, s5cmd §8, S3 §9, DuckDB §10,
  pyarrow §11, pyiceberg §12, delta-rs §13, pipelines §15-20, graph/vector)
* the security facts per tool are in `security-oss-tools-2026-09-27.md`
  (cited as SEC §tool) and are not restated here.

Conventions. `file:line` is migkit's code as of this morning. A verdict
in the scorecard is **migkit's standing against the tool** on that axis:
`ahead`, `equal`, `behind`. `[key]` is a source in §8. "(verify)" marks a
fact from a page that did not render in this pass, or that needs a sandbox
run. Wrapping follows the owner's rules of 2026-09-27: pip first, binaries
through `doctor --install`, no program named to the operator, no new mode
or flag, the decision engine (`docs/backlog.md`, "P0: the decision layer",
decided 2026-09-27) choosing the rung from measured facts.

---

## 1. What migkit holds today, engine by engine (read off the code)

| engine | bulk path | change follow | verify | resume / exactness | type and metadata fidelity | topology | snapshot, settings, users | wrapped programs and libraries |
|---|---|---|---|---|---|---|---|---|
| MongoDB | raw-BSON copier, `insert_many(ordered=False)` into an emptied collection, indexes after data (`engines/mongodb.py:95-187`); mongosync and mongodump/mongorestore movers (`movers.py:2472-2560`, `:2340-2400`) | database-wide change stream, `fullDocument=updateLookup`, removed fields as ABSENT (`mongodb.py:558-632`); tail (`:1878`) | per-BSON-type `$bucketAuto` ranges of 200k docs with a coverage check (`:1073-1140`), field types, indexes, mongos (`:1308-1550`), who-wrote (`:963`) | resume by `_id` through `$expr` across types (`:143-159`); delta by resume token, `ChangeStreamHistoryLost` (286) forces a re-baseline (`:1555-1600`); fence on cluster time (`:1058`) | bytes as sent (`RawBSONDocument`, `:128-131`); mongosync measured keeping NumberLong and Decimal128 (`movers.py:2482`) | replica set required for a stream (`:583-589`); sharded detected for handwork (`:1770`) | snapshot (`:1305`); settings (`:812`); users via `users.py:140,231`; leftovers and handwork (`:1703-1800`) | pymongo>=4.6 (`pyproject.toml:41`); mongodump/mongorestore via brew (`tools.py:24-25`); mongosync fetched from the vendor, pinned 1.21.0 (`tools.py:193`), measured 1.21.0 (`movers.py:3548`) |
| Redis/Valkey | SCAN + pipelined DUMP/PTTL, RESTORE REPLACE with the relative TTL, target emptied first, resume by cursor (`engines/redis.py:214-290`); REPLICAOF plan with a replication-only ACL user (`:872-917`), refused where unsafe (`:800-870`) | none of migkit's own; the server's replication where REPLICAOF is allowed (`:872`) | sampled type/value compare with a throttle (`:494-610`), TTL drift >10% or 60 s and lost TTLs (`:709-760`), modules and kinds (`:328-408`) | delta by `CLIENT TRACKING BCAST` over RESP2 (`:1171-1230`); confirm pass (`:1040-1070`); fence (`:1009`) | DUMP payload keeps encodings and stream consumer groups; **TTL relative, no ABSTTL, no IDLETIME/FREQ** (`:263-271`) | single node per endpoint (`:9-31`); cluster refuses REPLICAOF (`:811-815`); **the copier does not walk a cluster's nodes** | shape count only (`:310-326`), no BGSAVE; settings (`:410`); ACL users with hashes (`users.py:168,192`) | redis>=5.0 (`pyproject.toml:42`); hiredis measured and not adopted (backlog R18.2) |
| Kafka | per-partition copy with `read_committed`, `max_in_flight=1`, explicit partition, source timestamp and headers, schema-registry re-registration (`engines/kafka.py:297-378`) | rounds of the same copier (`:439-484`) | topics, partition counts, critical configs (`:755-830`), broker settings (`:649`), content re-hash per partition (`:1141`), SCRAM users and ACLs (`users.py:146,200`) | per-partition `next`; what reached the target after the last checkpoint counted off the target's end (`:320-345`); fence per partition (`:519`); delta by end offsets (`:1418`) | key, value, headers, timestamp carried; **no source-offset header; producer not idempotent** (`:92-99`, `acks=all` only) | brokers listed, SASL PLAIN/SCRAM/AWS_MSK_IAM, SSL (`:20-40`) | ends, group offsets, topic configs (`:566-590`) | kafka-python>=2.2.15 (`pyproject.toml:43`); confluent-kafka planned (R18.7); Avro through `avrostream.py`, shapes through `streamout.py` |
| Cassandra/Scylla | token ranges, `workers x 8` uniform slices of the ring, each cell written `USING TTL ? AND TIMESTAMP ?`, collections untimed, counters refused, target truncated first (`engines/cassandra.py:392-500`) | none (GAPS `capabilities.py:175-183`) | client-side fold, "has no way to hash rows in the server" (`:25`, `:178-186`); replication factor (`:262-290`); settings and table options (`:298-340`) | `RESUMES_BY_KEY = False` (`:27`); no range digest on the server | per-cell TTL and WRITETIME kept; `timestamp` millisecond loss said (`:11-13`) | driver token map available, not used for routing | definitions and counts, no node snapshot (`:349-362`); roles with salted hashes (`users.py:156,210`) | cassandra-driver>=3.29 (`pyproject.toml:36`) |
| ClickHouse | none of its own (GAPS `capabilities.py:112-122`); cross-engine copier deletes keys then inserts (`engines/clickhouse.py:279-320`) | none | per-partition `sum(cityHash64(toString(tuple(...))))` and count on each server, the target grouped by the source's partition key through `partitionId` (`:399-450`); unfinished mutations (`:358-395`) | delta by `system.parts` signatures, only changed partitions (`:545-620`) | wall clock handed over in the server's zone (`:322-332`) | replicated/distributed not handled | FREEZE WITH NAME (`:517-543`); settings, roles, users, grants (`users.py:151,222`) | clickhouse-connect>=0.8 (`pyproject.toml:32`) |
| OpenSearch/ES | sliced scroll, `_bulk` with the same `_id` and `_source`, replicas 0 and refresh off during the load (`engines/opensearch.py:637-700`) | none (GAPS `capabilities.py:166-174`) | mappings, settings, analysis (`:385-450`) | delta by each primary shard's max `_seq_no` plus an id-set diff for deletes (`:524-635`) | `_source` unchanged | any HTTP endpoint, Basic auth, `verify: false` (`:62-90`); **no SigV4** | fs snapshot under `path.repo` (`:473-520`) | urllib only; no elasticsearch-py or opensearch-py |
| DynamoDB | parallel Scan, one segment per worker, BatchWrite 25 at a time, table made with key, billing, GSI/LSI (`engines/dynamodb.py:886-960`) | Streams, parent shard first, a re-created stream refused (`:629-770`) | check_data/deep (`:481-628`) | fence, confirm, delta through the pair (`:774-793`) | items as held, maps/lists/sets via `RawAttr` (`:27-45`) | endpoint, region, DynamoDB Local | `CreateBackup` (`:862-885`); table settings, TTL, PITR (`:795-845`) | boto3 (`pyproject.toml:67`) |
| Parquet | part files copied byte for byte by `pyarrow.fs`, `_table.json` last (`engines/parquet.py:386-427`) | n/a | footer-based checks (`:347`) | table copied whole | Arrow types, unconstrained decimals as text (`:11-15`) | local, S3 (`:110`) | files copied beside the state (`:429-455`) | pyarrow, duckdb (`pyproject.toml:50,54`) |
| DuckDB / SQLite | `ATTACH ... (read_only)` + `INSERT ... SELECT` (`engines/duckdb.py:251-300`; `engines/sqlite.py:1014`) | n/a | own digests (`sqlite.py:58-100`) | one transaction a table | native | file | DuckDB copies itself (`duckdb.py:235`) | duckdb>=1.1 |
| Snowflake / BigQuery / Redshift | none: DB-API delete+insert; BigQuery a DML delete then `load_table_from_json` (`engines/warehouse.py:250-290`, not one transaction, `:203-206`); "loading from staged Parquet ... still to come" (`:11-13`); written without an account (`:14-15`) | none | schema, counts, data by the in-process renderer (`:108-116`) | keys deleted then inserted | as the DB-API driver gives them | n/a | none (GAPS `capabilities.py:184-196`, all `33`) | snowflake-connector-python, google-cloud-bigquery (`pyproject.toml:37-38`) |

Two things the read turned up that the wrap plans below fix first:

* `movers.py:2587` commits mongosync on `lagTimeSeconds <= 1`; that field is
  deprecated since 1.21 in favour of `lag.overallLagSeconds` (mongodb report
  §1, `/progress`). On a 1.22 binary it may be absent and read as 0, which
  commits early.
* `engines/redis.py:271` restores with the relative `PTTL` read a pipeline
  earlier; the key lands short by the pipeline's latency, and an already
  expired key is skipped rather than clamped (better than RedisShake's 1 ms,
  redis report §1), but `ABSTTL` (Redis 5+) with `PEXPIRETIME` (7.0+) would
  make it exact, and `IDLETIME`/`FREQ` are not carried at all.

---

## 2. Wrap plans and decision rules, tool by tool

Each entry: **wrap as** (pip / binary / server feature / not wrapped),
**licence**, **surface migkit drives**, **progress and errors** (mapped to
migkit's own events; the program is never named), **decision rule** (facts
the engine measures before choosing it), **fallback**, **how migkit beats
it** (what migkit adds on top, or does instead).

### 2.1 MongoDB

**mongosync** (already wrapped, `movers.py:2472-2600`).
* Wrap as: vendor binary through `doctor --install` (`tools.py:193-270`),
  pinned 1.21.0; move the pin to 1.22.0 only after the source user has
  `find` and `collStats` on `local.oplog.rs`, which 1.22 needs (mongodb §1).
* Licence: proprietary; free for Atlas and Enterprise Advanced, Customer
  Agreement otherwise (mongodb §1). `doctor` must say so once, as it says
  the download list. Telemetry off (`disableTelemetry: true`, `movers.py:2534`, kept).
* Surface used: `/start` with `preExistingDestinationData`, `includeNamespaces`,
  `excludeNamespaces`; `/progress` bytes; `/commit`. Not used and to take
  in: `buildIndexes` (`afterDataCopy` is already the default on 6.0+; use
  `excludeHashedAfterCopy` on sharded targets), `reversible: true` (the
  rollback path, needs same shard count and major, no filter, no pre-6.0
  source; mongodb §1 "Reverse"), `verification.enabled` (on by default;
  turn it **off** where the machine lacks 10 GB + 500 MB/1M docs, or where
  capped, TTL or non-default-collation collections would be skipped, and
  let migkit verify), `detectRandomId`/`copyInNaturalOrder`, `sharding`
  entries, `skipDiskSpaceCheck` never, `/pause`/`/resume` for the operator's
  hold (`tailctl`), per-shard instances with `--id` for sharded sources,
  `lag.{overallLagSeconds,crudLagSeconds,ddlLagSeconds}`,
  `estimatedSecondsToCEACatchup`, `warnings[]`, `verification.*` phases.
* Progress and errors: `/progress` fields to `wording.progress`; JSON log
  lines from `logPath` (level, componentName, message) to migkit's log;
  `InsufficientDestinationDiskSpace` to the assessment's free-space item;
  `canCommit=false` past 30 s of lag said as "still catching up".
* Decision rule (all measured, none configured): same engine both sides;
  source 5.0.29+/6.0.24+/7.0.18+/8.0.5+, destination 7.0 or 8.0, 8.0
  source needs 8.0 destination, no rapid release; replica set or sharded
  on both (a standalone cannot); no time-series, QE/CSFLE, clustered+TTL,
  `$`-prefixed fields, more than 63 indexes on a sharded collection (all
  read off `listCollections` and `$collStats`); oplog window on the source
  longer than the measured copy time (`stream_room`, `mongodb.py:456`);
  sharded: balancer stopped on both and 15 minutes elapsed; licence
  accepted; write blocking acceptable at commit (or filtered/pre-6.0 so
  only the destination blocks). Ranked against the raw-BSON copier plus
  migkit's change-stream tail by measured bytes/s; the copier wins on
  small databases and on any shape mongosync excludes.
* Fallback: raw-BSON copier + change stream tail (`mongodb.py:95-187`, `:558-632`).
* How migkit beats it: verification that resumes (mongosync's embedded
  verifier restarts from zero on any pause, mongodb §1), covers capped and
  TTL collections, and bisects by `_id` type (`:1073-1140`); a delta that
  survives restarts by resume token (`:1555`); an exact copy (`ReplaceOne`
  by `_id`, `:161`) where mongosync is eventually consistent until commit
  and re-applies after a restart; users and roles carried (`users.py:140`),
  which mongosync never does; the commit gate on `lag.overallLagSeconds`
  and migkit's own fence (`:1058`) rather than the deprecated field.

**mongodump / mongorestore** (already wrapped, `movers.py:2340-2400`).
* Wrap as: brew/apt binary (`tools.py:24-25`); Apache-2.0 (mongo-tools);
  latest 100.19.0 fixes unbounded memory on untrusted archives (mongodb §4).
* Surface used: `--archive` pipe, `--drop`, `--bypassDocumentValidation`,
  `--nsInclude/--nsExclude/--nsFrom/--nsTo`, `--numParallelCollections`,
  password by `--config` file (`movers.py:2402-2430`). To take in:
  `--oplog` + `--oplogReplay [--oplogLimit ts:ord]` for a point-in-time
  copy of a whole replica set (not with `--db`, not sharded; mongodb §4),
  `--gzip` where the link is the limit (measured), `--numInsertionWorkersPerCollection`
  sized by migkit for one huge collection (the documented bottleneck),
  `--noIndexRestore` with migkit building indexes in parallel after the
  data (R19.5), `--preserveUUID` where `--drop` is on and not sharded,
  `--convertLegacyIndexes` on sources below 4.2, `--maintainInsertionOrder`
  never (one worker), progress parsed off stderr (`<ts>\t[####] ns x/y (pct%)`,
  `finished restoring ... (N documents, M failures)`, `Failed:` prefix).
* Decision rule: whole database, no online follow needed (or the follow is
  migkit's own tail from a point taken before the dump), source below 5.0
  (mongosync cannot), licence blocks mongosync, or the measured dump+restore
  rate beats the copier for this document size. Refused: sharded source
  with `--oplog`, QE/CSFLE, major-version or FCV mismatch (restore only
  into the same major/FCV; mongodb §4).
* Fallback: raw-BSON copier.
* Beats it: exact resume (the pipe restarts from zero; the copier from the
  last `_id`), verification as it lands per range, users carried, and a
  restore that continues past duplicate-key errors is caught by the check.

**mongodb-labs/migration-verifier** (new; second reader, backlog R8).
* Wrap as: GitHub release binary (`darwin_arm64`, `linux_amd64`,
  `linux_arm64`; mongodb §3) through `doctor --install`; Apache-2.0.
* Surface: REST on 27020: `POST /api/v1/check` (with `filter`),
  `POST /writesOff`, `GET /progress` (`phase`, `generation`,
  `verificationStatus.failedTasks`, `srcChangeStats.lagSecs`), `/summary`,
  `/docMismatches` and `/nsMismatches` as NDJSON; `--docCompareMethod
  binary|ignoreFieldOrder|toHashedIndexKey`; `--srcChangeReader tailOplog`
  on replica sets; `--metaURI` to keep its metadata off the target;
  `--indexSpecIgnore expireAfterSeconds,unique` during a mongosync run.
* Progress: `/progress` to migkit's verify events; NDJSON mismatches into
  the drilldown files (`evidence.py`, encrypted at rest).
* Decision rule: same-engine pair, server 4.2+, a second independent
  reader asked for by the hop's rules (as DVT is for SQL), oplog window
  long enough, and a DDL freeze in force (`freeze.py`; it crashes on DDL by
  default, mongodb §3). Never on cross-engine pairs. Known bugs #310 and
  #197 avoided by never calling `/progress` before `/check` and never
  `--checkOnly`.
* Beats it: cross-engine verify by `canon` (it is Mongo-to-Mongo only);
  `_id` ranges bracketed by type (`mongodb.py:1073`); its generations model
  is the same idea as migkit's delta-by-token cycle; its 15k writes/s
  tested ceiling is a number migkit must measure against (R14 style).

**MongoShake**: GPL-3.0 (mongodb §5), so never bundled and not worth
driving: what it adds over migkit's stream is oplog tailing on 3.0-3.4
sources, a Kafka tunnel (migkit's `streamout` does that) and a count-only
`comparison.py`. Rung of migkit's own for 3.0-3.4 sources: a tailable
cursor on `local.oplog.rs` decoded to neutral changes; chosen only when
`hello.maxWireVersion` says change streams (3.6+) are absent.

**Monstache, mongo-connector**: Mongo-to-Elasticsearch daemons (MIT;
unmaintained Apache-2.0). Not wrapped: the `hetero` pair mongodb to
opensearch is already migkit's, checked, and resumable.

**pymongo top-ups** (pip, already a dependency): `compressors=zstd,snappy,zlib`
on both clients where the link's measured bandwidth, not the server, is the
limit (off unless set; mongodb §8); `find_raw_batches` for the copier's
read (a batch of bytes instead of one `RawBSONDocument` per document);
`MongoClient.bulk_write` across namespaces on 8.0+ targets for the tail's
batches; `$toHashedIndexKey` (4.4.10+) as a server-side per-document hash
so a range digest is `sum` of hashes computed by the server, no document
leaving it - equal Long and Double hash alike, so it is a screen, and the
raw-bytes compare stays the judge; `dbHash` as a rung **only after a
raw-BSON copy** on a quiesced replica set (field order and numeric types
preserved, so the md5s line up; excluded for capped collections and
differing `_id` collations; it takes an S lock, mongodb §9); `$collStats`
for sizes before ranging. PyMongoArrow stays out (slower, R18).

### 2.2 Redis and Valkey

**RedisShake v4** (new).
* Wrap as: release binary for darwin/linux, amd64/arm64 (redis §1) through
  `doctor --install`; MIT.
* Surface: `shake.toml` written by migkit to a private file: `sync_reader`
  (`cluster`, `prefer_replica`, `try_diskless`, `sync_aof`), `scan_reader`
  (`ksn`, `dbs`, `count`, `skip_unknown_type`), `rdb_reader`, `aof_reader`
  with a timestamp; `redis_writer` with `cluster`, `tls`, `off_reply`;
  `rdb_restore_command_behavior=rewrite`; `target_redis_proto_max_bulk_len`
  (0 to rebuild every key as plain commands for an older target);
  `target_mbbloom_version`; key/db/command filters; the Lua `function` to
  remap databases into a cluster (`allow_db=[0]` or `shake.call` with the
  db rewritten); `pipeline_count_limit`, `target_redis_max_qps` set from
  migkit's throttle (`throttle.py`), never left default; `status_port` for
  the JSON status. `aws_psync` where a support ticket enabled it.
* Progress and errors: `status_port` JSON (`total_entries_count`, `reader`
  RDB bytes and AOF offsets, `consistent`) to migkit's progress; `consistent`
  true over two samples is the "caught up" reading for the fence; a panic
  on topology change or a dropped link (no reconnect, redis §1) is a stop
  migkit restarts from its own position, since RedisShake has none: for
  `sync_reader` that means a full resync, said before it happens.
* Decision rule: `INFO`/`CONFIG GET` show PSYNC allowed (not ElastiCache,
  MemoryDB, Azure, Tencent unless enabled; redis §7), source not managed by
  Sentinel, no failover or resharding expected in the window (the operator
  is asked once through `approvals.py`, not a flag), source CPU headroom
  below what `DUMP` would cost (measured 47% to 91%, redis §1), keyspace
  larger than the copier's measured keys/s can carry in the window, RDB
  version of the source readable by the target (Redis 7.4+ files refused by
  Valkey; Valkey 9 = RDB 80; `rdb-version-check` on Valkey 8.1+), else
  `target_redis_proto_max_bulk_len=0` and the per-type rebuild. A cluster
  source or target: RedisShake with `cluster=true`, because the builtin
  copier reads one node (`redis.py:9-31`). A file: `rdb_reader`.
* Fallback: the builtin SCAN copier plus REPLICAOF where safe (`redis.py:800-917`).
* Beats it: exact TTL (`ABSTTL` from `PEXPIRETIME`, 7.0+), `IDLETIME` and
  `FREQ` carried (`OBJECT IDLETIME|FREQ`, `RESTORE ... IDLETIME s FREQ n`,
  redis §5), verification (RedisShake has none), delta by CLIENT TRACKING
  (`redis.py:1171`), resume by cursor for the scan path, users by ACL
  hashes, a keyspace-notification listener that is per node and told when
  it dropped (RedisShake's `ksn` cannot see FLUSHALL, redis §1).

**redis-full-check** (new; second reader).
* Wrap as: release binary, **linux-amd64 only** (redis §9), so `doctor`
  offers it on x86 Linux and says why not elsewhere; Apache-2.0 since
  1.4.10.
* Surface: `--comparemode 1` (full value; the default 2 is length only),
  `--comparetimes`, `--interval`, `--parallel`, `--qps` from the throttle,
  `--bigkeythreshold`, `--filterlist`, `--result` TSV and the SQLite
  conflict tables read directly.
* Decision rule: same-engine pair, a second reader asked for, x86 Linux
  host, no modules, Redis 2.x-7.x. Its multi-round recheck is the model
  migkit's delta already follows ("a key that differed is asked again",
  `redis.py:1171-1180`).
* Beats it: TTL compared with a tolerance (`redis.py:736-755`; full-check
  does not compare TTL, redis §9), both directions (extra keys on the
  target found, `:1040-1070`), arm64, and no linux-only binary.

**RIOT / RIOT-X**: RIOT archived (Apache-2.0); RIOT-X is BSL, production
use only with Redis products (redis §2), so it is not wrapped and `doctor`
never installs it. Taken from it as a rung of migkit's own: the `--struct`
mode - a per-type rebuild (`HGETALL`/`HSET`, `SMEMBERS`, `ZRANGE WITHSCORES`,
`LRANGE`, `XRANGE`/`XADD` with explicit IDs and `XGROUP CREATE ... ENTRIESREAD`
plus `XCLAIM` for the PEL) chosen when `RESTORE` is refused (payload version,
Redis 8 hash-field-TTL types 24/25 into Valkey's `HASH_2`, module types) or
when the brands differ (`redis.py:_brands`, `:840-870`).

**RDB parsers**: none in Python reads RDB 10+ (redis §3). `librdb`'s
`rdb-cli` (MIT, C, RDB up to 14, builds from source) is the one to fetch
when the source is a file (`--rdb` from `redis-cli`, a `BGSAVE` copied off
a replica, a managed service's export): `rdb-cli file.rdb resp | redis-cli
--pipe` with `-k/-t/-d` filters. HDT3213/rdb (Apache-2.0, Go) has no
linux-arm64 binary, so it is the second choice. Decision: source reachable
only as a file, or a keyspace whose big keys make `DUMP` too heavy.

**Clients**: redis-py stays (in `pyproject.toml:42`; the surrogateescape
packer is migkit's own, `redis.py:20-31`). valkey-glide (Apache-2.0) splits
a non-atomic `ClusterBatch` across slots itself (redis §8), which is the
piece a cluster copier needs; measured before adoption, and Redis 8.0+ is
not on its compatibility list. `MIGRATE` is never a rung: it blocks both
servers and is refused on every managed service (redis §5).

### 2.3 Kafka

**MirrorMaker 2** (new).
* Wrap as: `connect-mirror-maker.sh` from the Kafka tarball, or the
  `apache/kafka` image (amd64 and arm64), Java 17; Apache-2.0 (kafka §1).
  `doctor --install` offers the image, never a JDK by hand.
* Surface: a properties file migkit writes: `IdentityReplicationPolicy`
  (names unchanged; topology must be acyclic), `sync.group.offsets.enabled`,
  `emit.checkpoints.interval.seconds`, `sync.topic.configs.enabled`,
  `sync.topic.acls.enabled`, `refresh.topics.interval.seconds`,
  `replication.factor` from the target's default, `offset.lag.max`,
  `source.consumer.isolation.level=read_committed` (the Java default copies
  aborted records), `exactly.once.source.support=enabled` with
  `dedicated.mode.enable.internal.rest=true` on 3.5+ (KIP-618), 3.8+
  required for checkpoints of earlier-mirrored offsets (KAFKA-15905).
* Progress and errors: the `<src>.checkpoints.internal` topic decoded by
  migkit (group, topic, partition, upstream, downstream) as the translation
  table; `offset-syncs` for lag; JMX only where a JMX exporter is present.
* Decision rule: topics x partitions and the measured msg/s exceed what the
  Python producer sustains (kafka §5: confluent-kafka about a third of Java),
  a container runtime is present, a long-running follow, brokers 2.1+ for
  4.x MM2, target partition counts equal (KIP-382). Off for compacted topics
  where offset gaps matter (re-produce packs them; only byte copiers keep
  them, kafka §8), and where SCRAM users must move (nobody can; said).
* Fallback: the builtin copier and tail (`kafka.py:297-378`, `:439`).
* Beats it: exactness - migkit's copy counts what already reached the
  target after the last checkpoint (`:320-345`) and MM2 is at-least-once
  without EOS; translation - MM2 keeps 64 sparse syncs per partition and
  deliberately picks an earlier offset (KAFKA-12468, KAFKA-14666), migkit
  translates by timestamp and message (`:280-283`); ACLs by full key
  (`users.py:146`) where MM2 syncs TOPIC+LITERAL only and downgrades ALL to
  READ; a **source-offset header** on every copied message (the Redpanda
  migrator's device, kafka §6) makes the translation exact and is the
  decision layer's neutral mark for Kafka (R3) - to build.

**Redpanda Connect `redpanda_migrator`** (new, conditional).
* Licence: source files Apache-2.0, `migrator.go` has no licence check
  (kafka §6); the licensing page names no connector, the component page
  carries no enterprise mark, "Introduced in version 4.67.5" [rp-mig]. The
  release assets page did not render twice, so linux arm64 stays (verify);
  darwin arm64 is confirmed (lakes §15). Resolved as: treated as Apache-2.0,
  proven at first run in docker without a licence key (a blocked enterprise
  connector fails at startup, which the probe catches).
* Wrap as: `rpk connect` or the `redpandadata/connect` image; a YAML migkit
  writes; Prometheus metrics (`redpanda_migrator_cg_offsets_translated_total`,
  `input_redpanda_migrator_lag{topic,partition}`) scraped as progress.
* Decision rule: both clusters have schema registries and IDs must be
  preserved (destination in IMPORT mode, `translate_ids=false`), or the
  target is Redpanda; group offsets translated by its header and timestamp
  (best effort; equal partition counts).
* Beats it: exact counts and resume; migkit's own registry translation
  re-registers (`kafka.py:191`) and gets new IDs - the top-up is IMPORT
  mode to keep IDs where the target registry allows (`/mode/{subject}`),
  chosen when the target registry is empty for those subjects.

**KIP-1279 cluster mirroring**: still under discussion, missed 4.3 and 4.4;
4.4.0 was not released as of 2026-09-21 [kip1279-status]. When it ships, it
is the top rung for Kafka to Kafka (byte-for-byte, offsets kept, source 2.1+),
asynchronous only, no active-active; `assess` should name it as "not yet in
any release". Confluent Replicator, Cluster Linking, Redpanda Shadowing,
AutoMQ, WarpStream Orbit: commercial, named by `assess` as the provider's
own offset-preserving path where the target is that product (kafka §2, §7).

**kcat**: BSD-2, unmaintained since 2022 (kafka §4). Optional second
reader only (`-C -J -e` per partition, counts and content hashes without
Python in the way); the `-J` envelope fields stay (verify). Not a mover.
uReplicator: dormant, no.

**confluent-kafka** (pip, Apache-2.0; R18.7): the exact-batch mechanism
Kafka itself offers - a transactional producer that writes the copied
messages and migkit's checkpoint (to a `migkit.checkpoints` topic, or
`send_offsets_to_transaction` for a consumer group) in one transaction, so
the target answers "which batch did you last commit" without counting its
end offsets, and the assumption behind `kafka.py:320-345` (nobody else
produces to the target partition during the copy) goes away.
`enable.idempotence=true`, `compression.type=zstd|lz4` from the link
measurement, `linger.ms` and `batch.size` from `sizing.py`; partition set
explicitly so the partitioner never matters (kafka §8).

### 2.4 Cassandra and ScyllaDB

**DSBulk** (new).
* Wrap as: release tarball (1.11.2, Apache-2.0, JVM 8+) through
  `doctor --install`; no official image (community images only, none arm64
  stated [dsbulk-docker]); needs a JRE (brew `openjdk`, apt `default-jre`).
* Surface: `unload` piped into `load` (or through files on a spool),
  `schema.splits` sized by migkit (must exceed `engine.maxConcurrentQueries`),
  `schema.preserveTimestamp`/`preserveTtl` (load becomes BATCH per row;
  never with counters or a custom query; collections' writetime only on
  5.0+ sources), `schema.nullToUnset`, `count --stats.modes ranges,partitions`
  (the largest partitions for the planner), `log.checkpoint.file` with
  `replayStrategy=resume` for the resume, `log.maxErrors` from the hop's
  tolerance, `.bad` files with `log.sources=true` into the report directory
  (encrypted), `monitoring.reportRate`, Prometheus, exit codes 0-5
  (cassandra §1). `executor.continuousPaging` only at CL ONE/LOCAL_ONE and
  DSE (warning otherwise).
* Decision rule: table bytes x RF beyond what the Python driver copied per
  second in the last measured run (`planner.record_rate`), a JRE present,
  CL ONE acceptable for the read (else the driver path), no counters.
* Fallback: the token-range copier (`cassandra.py:392-500`).
* Beats it: per-cell TTL and WRITETIME are equal (both do it); migkit adds
  verification as it lands (a range digest read back, once the server-side
  hash question is settled: see §7), token ranges split by
  `system.table_estimates` per node rather than uniform slices (both today
  split blindly), and replica-routed reads through the driver's `token_map`
  (`get_replicas`, cassandra §7). DSBulk's checkpoint file exists even on
  success (cassandra §1) - migkit reads the exit code, never the file.

**Cassandra Data Migrator (CDM)** (new, conditional).
* Wrap as: `quay.io/datastax/cassandra-data-migrator` (6.1.1, Spark 4.2,
  Java 17; Apache-2.0); arm64 not stated (verify: `docker manifest inspect`
  at first `doctor`); `spark-submit --master local[*]` inside the container.
* Surface: `Migrate`, `DiffData` (autocorrect off by default and **kept off
  during dual writes**), `GuardrailCheck` (column size), `numParts`,
  `partition.min/max` for a range, `writetime.min/max` as a filter,
  `trackRun`/`autoRerun`/`rerunMultiplier` for the resume in the target
  keyspace's `cdm_run_*` tables (a footprint, named by `doctor`),
  `ratelimit.origin/target` from the throttle, `explodeMap`, codecs. The
  summary line format and exit codes are undocumented (README silent
  [cdm-readme]; measured in docker before trust); migkit parses the per-key
  `Mismatch row found`, `Missing target row`, `Corrected mismatch` lines.
* Decision rule: a table too large for a client copy where a VM beside the
  cluster can run Spark; DiffData as the second reader for Cassandra pairs
  (R8) with autocorrect never on while writes flow (it resurrects deletes;
  cassandra §2).
* Beats it: CDM writes the **max** writetime/TTL of the row to every cell
  (cassandra §2) - migkit writes each cell its own (`cassandra.py:434-445`);
  counters refused rather than doubled (`:409`); a repair that compares
  writetime so last-write-wins cannot shadow it silently (to build).

**ZDM proxy** (not a mover; named). Apache-2.0, arm64 image (cassandra §3).
`assess` names it as the zero-downtime path for CQL applications when the
source has no usable change log (Cassandra's commitlog CDC needs a JVM on
every node, arrives RF times, and blocks writes when full; cassandra §6),
and migkit supplies what its docs ask for: a reconciliation before and
after phase 4 - `check` and `repair` with LWTs, counters and collection
`+=` singled out as the operations that diverge. Effort S: a sentence and a
checklist item, no driving.

**scylla-migrator** (new, conditional). Apache-2.0, Spark 4.0 jar.
Wrap as CDM is wrapped; savepoint YAML (`skipTokenRanges`) as the resume;
Validator with its tolerances (`ttlToleranceMillis` 60000,
`writetimeToleranceMillis` 1000, `floatingPointTolerance` 0.001) mapped to
migkit's own tolerances; `copyMissingRows` never on. Decision rule: target
is ScyllaDB and the table is large (its connector is shard- and
tablet-aware; the Python `scylla-driver` cannot be installed beside
`cassandra-driver` and publishes no arm64 wheel [scylla-driver-pypi]); or
DynamoDB to Alternator. Beats it: migkit's DynamoDB Streams tail resumes
and refuses a re-created stream (`dynamodb.py:683`) where the migrator's
`streamChanges` loses records past 24 h and must be cleaned by hand;
`preserveTimestamps` cannot be used with collections there, migkit keeps
the timed cells and writes collections untimed (`cassandra.py:420-424`).

**sstableloader / nodetool import / Scylla load-and-stream** (physical
rung, R19.1). Needs a shell on a node (the tunnel module, R17) and
matching sstable formats: Cassandra to Cassandra by `sstableloader -d`
with the throttle flags; Scylla to Scylla by `nodetool refresh -las`;
Cassandra `nb`/`oa` into Scylla undocumented with open crashes (#8583,
#11267, #22707; cassandra §5), so refused until a sandbox proves it.
Decision rule: same product and major, files reachable, 5.0's
`storage_compatibility_mode` known, MV and index sstables left out. Verify
after: the range digest of live rows (tombstones and TTLs land as they
were, which is more than any CQL copy carries).

**CDC**: Cassandra's commitlog CDC is not a rung (JVM agent per node,
duplicates per replica, no backfill, write timeouts when full; Debezium
incubating; cassandra §6): a Cassandra source follows by ZDM or by a stop.
ScyllaDB's CDC log tables are a rung migkit builds itself (R13): per-stream
queries over `cdc$time` windows behind a confidence window, generations
from `system_distributed.cdc_streams_descriptions_v2`, tablets' stream sets
from `system.cdc_streams`, progress per (generation, stream) in the
checkpoint; `scylla-cdc-go` as a compiled sidecar (R19.9) if the Python
reader's decode is the ceiling, since no `scylla-cdc-python` exists.

### 2.5 ClickHouse

**Native `BACKUP` / `RESTORE` SQL** (server feature; the physical rung).
No binary: `BACKUP TABLE|DATABASE ... TO S3(...)|Disk(...)` with
`base_backup` for increments, `ASYNC`, progress from `system.backups`
(`status`, `files_read`, `bytes_read`, `error`), `allow_non_empty_tables`,
`restore_access_entities_with_current_grants` (Cloud 26.4+); unique paths
(`BACKUP_ALREADY_EXISTS`). Decision rule: same major (cross-version
undocumented, clickhouse §5: probe by restoring one small table first),
object storage reachable from both servers, table sizes where a logical
`INSERT SELECT` would exceed the window, MergeTree family. Verify after:
`system.parts` hashes are comparable only after a physical copy
(clickhouse §6) - `hash_of_all_files` per part on both sides, then
`CHECK TABLE` on the target; cheaper than the logical fingerprint.

**clickhouse-backup (Altinity)** (new, conditional). MIT, Go, arm64
linux/darwin, image (clickhouse §1). Adds over the SQL: diff chains,
`--rbac`/`--configs` (2.8.x fixed silent RBAC drops), `watch`, the REST
`server` with `operation_id` and callbacks, resumable uploads (#1569 open),
custom rclone/kopia storage. It needs the data directory (same host or
pod) unless `use_embedded_backup_restore: true`, which is the SQL above.
Decision rule: RBAC or configs must move with the data, or the operator
already runs it (its `server` found on 7171). Otherwise the SQL rung needs
nothing installed. Progress: `/backup/status` and `/metrics` mapped.

**`remote()` / `remoteSecure()` `INSERT ... SELECT`** (server feature;
R13 "still to"). Pull (`INSERT INTO t SELECT ... FROM remoteSecure(...)`)
with a `readonly=1` source user, or push (`INSERT INTO FUNCTION
remoteSecure(...)`) where only the source can reach the target; one
partition per statement (`max_partitions_per_insert_block` 100;
`_partition_id` filter is safe in push mode, through `remote()` (verify) -
a docker test), `max_insert_threads`/`max_threads`/`min_insert_block_size_*`
from `sizing.py`, `connect_timeout_with_failover_ms` raised above the
measured RTT, no retry on a dropped link (clickhouse §3) so each partition
is a unit of resume; `insert_deduplication_token = <hop>:<table>:<partition>:<batch>`
on Replicated* tables (or `non_replicated_deduplication_window > 0`) makes
a retried partition land once - the exact batch; a stable SELECT
(`ORDER BY ALL`, one stream) for the INSERT-SELECT dedup. Partitions whose
fingerprint (`clickhouse.py:399-450`) already matches are skipped (R19.3).
Decision rule: both servers reach each other on 9000/9440 (probe), same
schema or migkit's `neutral_create`, table > the copier's measured rate.
Beats Altinity's script (clickhouse §3): verification by fingerprint per
partition rather than `system.parts` row counts; resume by fingerprint.

**clickhouse-copier**: obsolete (removed in 24.2). Taken from it as a rung:
insert into a temporary table on the target, then `ALTER TABLE ... ATTACH
PARTITION FROM`, so a partition lands whole or not at all.

**clickhouse-local / chDB**: `chdb` (pip, Apache-2.0, arm64) is the
in-process mover and second reader for files: Parquet, `s3()`, `iceberg()`
(v1/v2, writes 25.7+ behind `allow_insert_into_iceberg`), `deltaLake()`
(writes beta 25.10+), `postgresql()`/`mysql()`/`mongodb()` pulls (simple
pushdown only, clickhouse §9). Decision rule: a side is files or a lake
table; or the target is ClickHouse and it can reach the source database,
so the server pulls (no row through migkit) and migkit verifies by `canon`.
`clickhouse local` binary via `curl https://clickhouse.com/ | sh` in
`doctor --install` where chDB's wheel is missing.

**PeerDB**: AGPLv3 now (clickhouse §7); not wrapped; its ctid-range
snapshot under one exported snapshot is R19.1/pgcopydb territory already.
**MaterializedPostgreSQL**: experimental, no DDL; not a rung.
**MaterializedMySQL**: removed.

### 2.6 OpenSearch and Elasticsearch

**Reindex-from-Snapshot (RFS) of the OpenSearch Migration Assistant**
(new, conditional). Apache-2.0. The full assistant needs Kubernetes, but
RFS runs standalone (`./gradlew DocumentsFromSnapshotMigration:run --args=
"--snapshot-name ... --snapshot-local-dir ... --lucene-dir ... --target-host
..."`, `--coordinator-host` optional; exit 0 done, 2 lease handoff, 3 no
work) [rfs-readme]. Wrap as: its container run repeatedly until exit 3,
leases in the coordinator index (a footprint on the target unless a
coordinator is given), `--allowed-doc-exception-types
version_conflict_engine_exception`, `--max-shard-size-bytes`, 2x shard
size of local disk. Decision rule: a snapshot repository exists or can be
taken (fs or S3), source under load (RFS reads the snapshot, not the
cluster), ES 5-8 or OS 1-2 source, `_source` present (else
`enableSourcelessMigrations` best-effort), no zstd codec, and the version
gap refuses remote reindex (ES 7.12+ into OpenSearch). Metadata migration
(`evaluate`, then `migrate`) for templates, component templates, aliases
and the type transforms (`dense_vector` to `knn_vector`, nmslib to faiss);
never security config, ISM, pipelines (clickhouse-opensearch §11).
Beats it: a delta by `_seq_no` (`opensearch.py:524-635`) and a follow (RFS
is backfill only); exactness by `_id` on both.

**`_reindex` from remote** (server feature). `reindex.remote.allowlist`
(needs a restart, probed in `_cluster/settings` and said), no slicing, 100
MB buffer, `version_type: external`, `op_type: create`, `conflicts:
proceed`, `wait_for_completion=false` task polling, `requests_per_second`
from the throttle, same or newer major only. Decision rule: allowlist
already set, `_source` present, the target can reach the source. The
target pulls, no document through migkit; migkit verifies after.

**Snapshot restore across clusters** (physical rung). ES 6.0-7.10 indices
into OpenSearch; 7.12+ rejected; OS 3.0 rejects pre-2.x indices; never to an
older version (clickhouse-opensearch §13). Decision rule: the version
matrix admits it and a repository both clusters reach (fully S3-compatible)
exists; `rename_pattern` for the prefix; verify by `_count` and the
`_source` sample compare.

**Cross-cluster replication plugin** (follow rung, R13 "still to"). OpenSearch
CCR is Apache-2.0; ES CCR is Platinum (closed to new self-managed
customers). Decision rule: `_cat/plugins` shows it on both, follower version
>= leader, security both on or both off, `remote_cluster_client` role on
followers, AWS's same-major rule. Lag from `_plugins/_replication/{index}/_status`;
fence = leader and follower `_seq_no` checkpoints equal per shard; cutover
`_stop` makes the follower writable. Fallback: `_seq_no` polling plus id
diff (the delta's mechanism, applied continuously).

**elasticdump**: Apache-2.0, Node.js; misses the last batch at
`concurrency=2` (#741), heap limits, `--offset` cannot resume (clickhouse-
opensearch §10). Not wrapped as a mover (the sliced scroll is ahead); its
type list is borrowed: templates, component templates, index templates,
aliases, ISM/ILM policies moved by migkit's own REST calls.

**Logstash**, **esrally/opensearch-benchmark**: not wrapped (JVM; corpus
tooling).

**Client top-ups**: `opensearch-py` (Apache-2.0) for `AWSV4SignerAuth`
(migkit's `_call` has Basic auth only, `opensearch.py:75-77`), PIT +
`search_after` so a slice resumes by `_shard_doc` instead of restarting
the scroll, `streaming_bulk`'s retry on 429 with backoff (`_call` raises on
the first `errors: true`, `:678-688`); elasticsearch-py refuses OpenSearch
since 7.14, so the client is chosen by the `/` banner's `distribution`.

### 2.7 DynamoDB

**ExportTableToPointInTime / ImportTable** (service feature; the physical
rung). Full export needs PITR, costs no RCUs, writes `manifest-files.json`
with `itemCount` and `md5Checksum` per file (free verification of the
export); incremental export (15 min to 24 h windows, `NEW_AND_OLD_IMAGES`,
compacted to one final state per item, DynamoDB's own clock) is a change
feed past the 24 h Streams retention. ImportTable creates a **new** table
only, no LSIs, duplicates overwrite silently, a failed import leaves a
partial table, 50k objects / 15 TB (lakes §1-2). Decision rule: target
table absent (or migkit imports into a new name and the cutover renames by
the hop's `db_map`), PITR on, table bytes above the point where the
measured parallel-scan rate under the RCU budget exceeds the export's
latency (no SLA: said), same account or a bucket policy; import only the
full export's files (never full + incremental together, which is where
duplicates come from), then apply the incremental exports or the Streams
tail through migkit's own writer. Verify: `DescribeImport.ImportedItemCount`
against the manifest, then the key digest by parallel scan on both (no
server-side hash exists; consistent reads cost 2x). Cost said before the
move ($0.10/GB + $0.15/GB, us-east-1).

**dynamodump**: MIT pip; sequential scan, drops unprocessed items after 6
retries (lakes §3). Behind the builtin; not wrapped.

**Kinesis Data Streams for DynamoDB** (change rung for long moves): 1-year
retention, duplicates and reordering settled by `ApproximateCreationDateTime`,
same account and region. Decision rule: the copy's measured duration
exceeds the 24 h Streams window. **Global Tables** (2019.11.21, cross-account
since 2026-02): different account and different region only, last writer
wins, 10 TB/day; named by `assess`, never a rung migkit drives.

**Write side** (pip, boto3 already): `TransactWriteItems` with
`ClientRequestToken` (10-minute idempotency, 100 actions, 4 MB) is the
exact batch for the tail - the "transact-write" mark of the decision layer;
`batch_writer(overwrite_by_pkeys=...)` for the bulk; "newer wins"
conditional writes ratcheting on a timestamp attribute for two-way (R3);
`WarmThroughput` suggested by `assess` with its irreversibility said, set
only through `approvals.py`; `max_pool_connections` = workers (aioboto3
adds nothing, lakes §6).

### 2.8 Files and lakes

**rclone** (new, conditional). MIT, arm64 everywhere; `rc` API
(`core/stats`, `job/status`), `--use-json-log`, exit codes 1-10,
`rclone check --download` for byte compare, multipart ETag caveat (lakes
§7). Wrap for the Parquet engine's copies between two stores (S3, GCS,
Azure, SFTP, local) where `pyarrow.fs.copy_files` (`parquet.py:386-427`)
copies one file at a time with no checksum: decision rule: object count or
bytes above the measured single-stream rate, or the two stores differ.
Progress from `core/stats`. Beats it: migkit verifies rows (footer
`num_rows` and the table digest), not only bytes.

**s5cmd**: MIT; official 2.3.0 (2024-12), forks carry the path-traversal
fix, no S3-to-S3 copy above 5 GB (lakes §8). Only where rclone is absent
and both sides are S3. Low.

**S3 native**: `CopyObject` up to 5 GB and an error inside a `200 OK`;
CRC64NVME full-object checksums (default since 2024-12), Batch Operations
"compute checksum" (lakes §9). Rung: after any copy to S3 compare
`ChecksumCRC64NVME` (same algorithm, full object) rather than ETag; read
the 200 body for an embedded error.

**DuckDB** (pip, in deps): `parquet_file_metadata()` `num_rows` for free
counts; `md5_number`/`sha256` for durable digests (`hash()` changes across
versions; lakes §10); `COPY ... (FORMAT parquet, ROW_GROUP_SIZE,
PARTITION_BY, RETURN_STATS)` as the Parquet writer with sizes from
`sizing.py`; Iceberg writes (INSERT 1.4, UPDATE/DELETE 1.4.2, MERGE 1.5.3;
catalog required; merge-on-read) and Delta append-only; the scanners
(`postgres`, `mysql`, `sqlite`) as second readers. 2.0 expected late
October 2026: pin.

**pyarrow** (pip, in deps): `write_dataset(file_visitor=...)` gives each
file's row count; **no row-hash kernel** - `hash32`/`hash64` (PR #45001) is
still open, "awaiting changes", as of September 2026 [arrow-hash-pr], so
row digests stay `canon.fold_rows` or DuckDB `md5_number` over an Arrow
table (zero copy). Settled: the report's "pyarrow row-hash: UNVERIFIED".

**pyiceberg** (pip, Apache-2.0, 0.12.0; arm64). Catalogs REST/SQL/Glue/Hive/
DynamoDB/BigQuery; `append`, `overwrite(overwrite_filter=)`, `delete`,
`dynamic_partition_overwrite`, `upsert` (identifier fields), `add_files`
(registers Parquet without rewriting; `check_duplicate_files`),
**`snapshot_properties={...}` on `append`, `overwrite` and `add_files`**,
read back as `metadata.snapshots[-1].summary[...]` and in
`inspect.snapshots()`; `branch=` on `append` [pyiceberg-api]. Gaps: only
`expire_snapshots` (no compaction, no orphan removal; expiring can delete
files registered by `add_files`; lakes §12).

**delta-rs** (pip `deltalake`, Apache-2.0, 1.6.6; arm64). `write_deltalake(
mode, predicate, schema_mode, target_file_size)`, `optimize.compact`,
`z_order`, `vacuum` (dry run by default), `load_cdf` (Delta as a **source's
change log**: `_change_type`, `_commit_version`, `_commit_timestamp`),
`CommitProperties(app_transactions=[Transaction(app_id, version)])` writes
the protocol's `txn` action and `DeltaTable.transaction_version(app_id)`
reads it back; delta-rs does **not** skip a duplicate version itself
(tracking issue #3821) [deltars-txn] - the caller compares first. S3 needs
a locking provider for concurrent writers (migkit is one writer per table;
set it anyway).

---

## 3. Gaps the reports flagged UNVERIFIED, now settled

| item (report) | finding | source |
|---|---|---|
| `redpanda_migrator` licence tier (kafka §6, lakes §15) | no enterprise mark on the component page, "Introduced in version 4.67.5"; the licensing page names no connector; `migrator.go` has no licence check. Treat as Apache-2.0; prove by a licence-less run in docker. Linux arm64 asset list did not render (verify). | [rp-mig], [rp-lic] |
| KIP-1279 status (kafka §1) | still under discussion; missed 4.3 and 4.4; 4.4.0 unreleased as of 2026-09-21. Not a rung yet. | [kip1279-status] |
| pyarrow row-hash kernel (lakes §11) | `hash32`/`hash64` PR #45001 open, unmerged, September 2026. Use DuckDB `md5_number` or `canon.fold_rows`. | [arrow-hash-pr] |
| Airbyte connector licences (lakes, top) | `source-mongodb-v2` 2.0.7: `license: ELv2`, Java, certified - the same as postgres and mysql. Every database source connector is ELv2 and Java. | [airbyte-mongo] |
| CDM summary line and exit codes (cassandra §2) | README says nothing about either; images on Quay from 6.1.1, arm64 not stated; Java 17, Spark 4.2.0. Measure in docker. | [cdm-readme] |
| DSBulk docker image (cassandra §1) | no DataStax image for DSBulk; community images only, none stating arm64; `datastax/astra-cli` bundles it for Astra. Fetch the tarball and run on a JRE. | [dsbulk-docker] |
| scylla-driver arm64 wheels (cassandra §7) | 3.29.12 publishes x86_64/i686 wheels only; Apache-2.0. Not installable beside cassandra-driver anyway. | [scylla-driver-pypi] |
| RFS outside Kubernetes (clickhouse-opensearch §11) | runs standalone by gradle/jar/container with `--snapshot-local-dir`; exit codes 0/2/3. Testable in docker. | [rfs-readme] |
| `_partition_id` through `remote()` (clickhouse §3) | still (verify): a one-line docker test (`SELECT _partition_id FROM remote(...)`). Push mode needs no answer. | - |
| Neo4j APOC Core vs Extended (lakes, graph) | APOC Core is Apache-2.0 (`neo4j/apoc` LICENSE); the Core manual documents export to CSV, JSON, GraphML and Cypher; the per-procedure split page did not render (verify). | [apoc-lic], [apoc-export] |
| Neo4j `database copy`, CDC edition (lakes, graph) | `neo4j-admin database copy` is an Enterprise command, source offline; Community has `dump`/`load` offline only. CDC is Enterprise/Aura only (5.13+, `txLogEnrichment DIFF|FULL`, `db.cdc.query` needs admin, `db.cdc.current` gives `txCommitTime` from 2026.06). | [neo4j-copy], [neo4j-cdc] |
| Singer/Meltano tap licences (lakes §18) | MeltanoLabs `tap-postgres`: **Elastic License 2.0**; MeltanoLabs `tap-mongodb`: Apache-2.0; singer-io `tap-mongodb`: AGPL-3.0. A licence minefield per tap. | [tap-pg-lic], [tap-mongo-lic], [singer-mongo-lic] |
| Qdrant scroll ordering (lakes, vector) | scroll is "sorted by id" by default with `next_page_offset`; `order_by` by a payload field exists (its index requirement (verify)). | [qdrant-scroll] |
| fakesnow / bigquery-emulator (R13 "emulators with gaps") | fakesnow 0.11.16 (DuckDB-backed; classifier MIT, LICENSE file Apache-2.0; liberal dialect); goccy/bigquery-emulator MIT, v0.8.1, multi-arch image. Redshift has no open emulator. | [fakesnow], [bq-emu] |

---

## 4. Engines and tools not yet covered

### 4.1 Neo4j (backlog R16c, deferred)

* Server: Community GPLv3 (not bundled; driven only), Enterprise commercial.
  Python driver `neo4j` 6.0.2, Apache-2.0 (and Python-2.0), 3.10+ [neo4j-pypi].
  APOC Core Apache-2.0 [apoc-lic].
* Bulk rungs: `neo4j-admin database dump`/`load` (Community: database
  offline; no users or roles; `load --overwrite-destination` from s3/gs/azb
  paths); `neo4j-admin database import` (offline, CSV or Parquet, full
  import into an empty database; incremental import Enterprise; the
  fastest load: "writes CSV data into Neo4j's native file format as fast
  as possible") [neo4j-import]; `database copy` Enterprise only [neo4j-copy];
  APOC `apoc.export.{csv,json,cypher,graphml}` with `stream:true` to the
  client so no file lands on the server (`apoc.export.file.enabled` not
  needed) [apoc-export].
* Logical copy of migkit's own: nodes per label ordered by a business key
  (a constraint's property) or, inside one run, by `elementId()` for the
  resume; relationships per type by their endpoints' business keys; the
  schema (constraints, indexes) carried first; counts per label and type,
  property digests per label, endpoints compared. Change follow: CDC
  Enterprise/Aura only (`db.cdc.query` from a change id, `db.cdc.current`)
  - Community: none, said by `capabilities.GAPS`.
* Verdict: engine effort L; nobody else supports it (R16); wrap the driver
  (pip) and drive `neo4j-admin` where a shell on the host exists.

### 4.2 Milvus, Qdrant, pgvector (short)

* pgvector: done (R16a); `pg_dump` moves it; HNSW rebuilt on restore.
* Qdrant: `qdrant-client` Apache-2.0; scroll by id with `next_page_offset`
  (exact resume by id) [qdrant-scroll]; upsert by id converges; snapshots
  same or next minor, per node, aliases excluded; `qdrant/migration`
  container (Apache-2.0, batch 50, resumable; lakes, vector). R16b plan
  stands: configuration carried, points by id, digest of id + vector bytes
  + payload, recall@k as the check no row compare gives. Effort M; arm64
  image; testable in docker.
* Milvus: `pymilvus` 3.0.2 Apache-2.0 with the `bulk_writer` extra
  (`RemoteBulkWriter` Parquet/JSON to the bucket, `bulk_import` up to 16 GB
  per file, 1024 files, `get_import_progress`; nothing said about duplicate
  primary keys) [pymilvus], [milvus-import]; `milvus-backup` (Apache-2.0,
  segment binlogs, 2.2+ into 2.5+ only, snapshot format same-provider);
  `milvus-cdc` (Apache-2.0, reads the source's etcd and message queue,
  active-standby) [milvus-cdc]; VTS (SeaTunnel, no CDC). Engine plan: read
  by primary key order (`query` with `pk > last`), bulk by BulkWriter +
  import, follow by milvus-cdc only where its footprint (etcd access) is
  allowed, verify by recall@k. Effort L; deferred with R16b.

### 4.3 Iceberg and Delta as targets

Both are file tables with a log, so they are Parquet targets with three
things migkit's Parquet engine lacks: a catalog, atomic commits, and a
place to write the exact batch.

* **Exact batches.** Iceberg: `snapshot_properties={"migkit.hop": id,
  "migkit.batch": n, "migkit.position": token}` on every `append`/
  `overwrite`/`add_files`; on resume read the current snapshot's summary
  (the Kafka Connect sink keeps its offsets the same way, coordinated
  through a control topic because it has many writers [iceberg-kc]; migkit
  is one writer per table, so the summary alone is the ledger). Delta:
  `CommitProperties(app_transactions=[Transaction(app_id=hop, version=n)])`
  and `transaction_version(app_id)` before each write [deltars-txn]. Both
  are the R3 "which batch did you last commit" answer with no table of
  migkit's own.
* **Bulk.** Parquet written by DuckDB `COPY` or pyarrow `write_dataset`
  with `file_visitor` row counts, registered by `add_files` (Iceberg) or
  written by `write_deltalake` (Delta); verify by footer `num_rows` against
  the source count and the table digest through DuckDB (`iceberg_scan`,
  `delta_scan`).
* **Follow.** Delta `load_cdf` as a **source's** change log (needs
  `delta.enableChangeDataFeed=true`); Iceberg incremental reads by
  snapshot id (`scan(snapshot_id=)`, `inspect.entries`) for append-only
  sources.
* **Housekeeping said, not done:** pyiceberg cannot compact or remove
  orphans; delta-rs `vacuum` is a dry run by default; `doctor` names the
  footprint (small files) and the operator's own compaction.
* Effort M each (the engine skeleton is Parquet's); testable in docker with
  a REST catalog (Lakekeeper or the Iceberg REST fixture) and MinIO.

### 4.4 Snowflake, BigQuery, Redshift native load and unload paths

`engines/warehouse.py` writes by DB-API and by BigQuery JSON load jobs and
says the staged-Parquet path is "still to come" (`:11-13`). The native
paths, each with its idempotence device and its server-side digest:

| warehouse | load | unload | exact batch device | server-side digest | test path |
|---|---|---|---|---|---|
| Snowflake | Parquet to a temporary stage by `PUT`, then `COPY INTO <table>` (`write_pandas` does exactly this: `chunk_size`, `compression`, `on_error`, `parallel`, `use_logical_type`) [sf-connector] | `COPY INTO @stage/... FILE_FORMAT=(TYPE=PARQUET)` | **load metadata**: a file already loaded (name + eTag) is skipped for 64 days; `FORCE=TRUE` to override; `LOAD_UNCERTAIN_FILES` past 64 days [sf-loadmeta] - name each batch's file `hop-table-batch-N.parquet` and a retry is a no-op; `COPY_HISTORY` is the ledger | `HASH_AGG(*)` order-independent 64-bit, NULL-aware, "not a cryptographic hash", stable for equal values [sf-hashagg]; for the cross-engine fold use `SUM` of an MD5-derived number over the canonical text | fakesnow (DuckDB-backed; dialect looser than Snowflake) [fakesnow]; real account for anything else |
| BigQuery | `load_table_from_uri`/`load_table_from_file` with `PARQUET`, `WRITE_APPEND`; or the Storage Write API `pending` stream (buffered until commit) / `committed` stream with offsets: "The write operation is only performed if the offset value matches the next append offset"; protobuf or Arrow; Python client supported [bq-writeapi] | `EXPORT DATA OPTIONS(format='PARQUET')` | a client-generated `job_id` per batch: "you must generate a new job ID" to repeat, six-month job history [bq-jobs] - re-submitting a batch's id is refused, so the batch ran once; Write API offsets for the tail | `SUM(FARM_FINGERPRINT(canonical_text))` (never `BIT_XOR`, R18: equal rows cancel) | goccy/bigquery-emulator (MIT, multi-arch; Write API coverage (verify)) [bq-emu] |
| Redshift | `COPY ... FORMAT AS PARQUET` from S3 in the same region, columns positional, `MANIFEST`, `IAM_ROLE`, no `MAXERROR` for columnar, errors in `STL_LOAD_ERRORS`, Spectrum presigned URLs (bucket policy `s3:signatureAge` >= 1 h) [rs-copy] | `UNLOAD ... FORMAT AS PARQUET PARTITION BY ... MAXFILESIZE ... MANIFEST VERBOSE` (row count per file and totals in the manifest: verification for free), `ROWGROUPSIZE`, SSE-KMS [rs-unload] | `STL_LOAD_COMMITS` names every file loaded: ask it before each batch's COPY; or a staging table and `INSERT ... SELECT WHERE NOT EXISTS` on the batch id | `FNV_HASH(value, seed)` chained across columns ("compute the FNV hash of the first column and pass it as a seed to the hash of the second"), BIGINT, `SUM` over rows [rs-fnv] | none open (LocalStack Pro only); **blocked on an account** as R13 says |

Common: stage files written by DuckDB `COPY` with `RETURN_STATS`
(row counts in hand before the load), sized by `sizing.py`; every load's
count compared with the stage's; the digests above as the verify rung
where both sides are warehouses, `canon` elsewhere. `redshift_connector`
(Apache-2.0) is the driver Redshift's IAM sign-in needs; not in
`pyproject.toml` (Redshift goes through psycopg today).

### 4.5 Trino and Spark connectors as movers

* Trino: `trino` client 0.340.0 (2026-09-23), Apache-2.0 [trino-pypi];
  server image `trinodb/trino` amd64 and arm64 [trino-docker]. As a mover
  (`INSERT INTO b.t SELECT * FROM a.t` across catalogs): fault-tolerant
  execution can commit an INSERT **twice** when the TableFinish task is
  retried (issue #31246, since 455, "the same retry re-runs other
  connectors' finish", JDBC connectors included) [trino-dup] - so only with
  `retry-policy=NONE`, and migkit's count and digest after. As a second
  reader (R8) where a Trino already exists: one SQL over any two catalogs,
  `SUM(from_big_endian_64(xxhash64(to_utf8(canonical_text))))` on both
  sides. Never installed by migkit (1 GB JVM image). Effort M, value M.
* Spark: only inside CDM and scylla-migrator (§2.4). `pyspark` as a general
  mover brings a JVM and no exactness; not wrapped.

### 4.6 Benthos / Redpanda Connect / bento, NiFi, Airbyte, Singer, dlt

* **bento** (MIT fork by WarpStream, `ghcr.io/warpstreamlabs/bento`;
  kafka, redis, mongodb, cassandra, dynamodb, elasticsearch, sql incl.
  ClickHouse; no CDC inputs) [bento-readme]. Not a mover (NeutralCopier
  does the same, checked). Candidate for the **relay agent** of R17d: a
  single static binary, YAML-configured, started over SSH beside the
  source, framing batches to migkit - to measure against migkit's own
  Python relay before choosing. Redpanda Connect's CDC inputs are
  enterprise (lakes §15); not used.
* **NiFi**: Apache-2.0, Java 21 service; not embedded.
* **Airbyte**: platform ELv2; CDK MIT; every database source ELv2 and Java
  (docker) [airbyte-mongo]; JSON records, "the final bottleneck" (R18).
  Not wrapped; its STATE/GLOBAL state design is what `checkpoint.py`
  already does per table and per stream.
* **Singer / Meltano**: Meltano MIT; taps ELv2 (MeltanoLabs tap-postgres),
  Apache-2.0 (MeltanoLabs tap-mongodb), AGPL-3.0 (singer-io tap-mongodb,
  pipelinewise). JSON lines per row. Not wrapped.
* **dlt** (Apache-2.0, pip): its `sql_database` pyarrow/connectorx backends
  and its staging loaders for Snowflake/BigQuery/Redshift/Iceberg/Delta are
  the same paths as §4.4, but it normalises schemas and adds `_dlt_*`
  columns and a `_dlt_pipeline_state` table on the target, against
  migkit's rule that the target is untouched. Not wrapped; its staging
  sequence is borrowed.

---

## 5. Prioritised table

Effort: S under a day, M a few days, L a week or more. Docker: testable on
this arm64 docker host without an account.

| # | tool | wrap as | licence | decision rule (measured) | value | effort | docker |
|---|---|---|---|---|---|---|---|
| 1 | mongosync top-up (`lag.overallLagSeconds`, `reversible`, `verification` off when RAM or shapes forbid, `/pause`, per-shard `--id`, 1.22 grants) | vendor binary (have) | proprietary, telemetry off | same engine, 5.0.29+/6.0.24+/7.0.18+/8.0.5+ to 7.0/8.0, replica sets, oplog window > measured copy time, no excluded shapes, licence accepted | fixes an early commit on 1.22; rollback path | S | y (two replica sets) |
| 2 | Redis `ABSTTL` + `PEXPIRETIME`, `IDLETIME`/`FREQ` carried; cluster-aware SCAN | pip (have) | MIT | source 7.0+ for `PEXPIRETIME` (else 5.0+ `ABSTTL` from `PTTL` + now); `cluster_enabled` -> per-node SCAN | exact TTL, LRU/LFU fidelity nobody else keeps | S | y |
| 3 | Kafka source-offset header + transactional checkpoint (confluent-kafka) | pip | Apache-2.0 | brokers 0.11+ for transactions, `IdempotentWrite` and `TransactionalId` ACLs; header only where consumers tolerate one more header | exactly-once copy, exact translation, the R3 mark | M | y |
| 4 | ClickHouse `remote()` pull/push per partition with `insert_deduplication_token`, ATTACH-PARTITION landing | server feature | Apache-2.0 | mutual reach on 9000/9440, RTT < 50 ms else timeouts raised, Replicated* or dedup window set, table > copier's measured rate | the bulk cell (`GAPS 33`), no row through migkit | M | y (`_partition_id` via remote() to test) |
| 5 | ClickHouse `BACKUP`/`RESTORE` to object storage + `system.parts` hash compare | server feature | Apache-2.0 | same major, bucket reachable from both, MergeTree | physical rung, verification nearly free | M | y (MinIO) |
| 6 | RedisShake v4 | binary via doctor | MIT | PSYNC allowed, no Sentinel, no resharding in window, CPU headroom, RDB version readable, cluster on either side | the PSYNC rung and the only cluster-to-cluster path | M | y |
| 7 | pyiceberg + delta-rs targets with the exact batch in snapshot summary / `txn` | pip | Apache-2.0 | target is a lake table; catalog reachable | lake targets with exactness; Delta CDF as a source | M | y (REST catalog + MinIO) |
| 8 | Snowflake `PUT`+`COPY INTO` with file-name idempotence; `HASH_AGG` digest | pip (have) | Apache-2.0 connector | Snowflake target, stage grant, batch > N rows measured | bulk cell for a warehouse | M | partial (fakesnow) |
| 9 | BigQuery Parquet load with client `job_id`; Write API pending streams for the tail | pip (have) | Apache-2.0 | BigQuery target; batch size vs quota | bulk and exact tail | M | partial (bigquery-emulator) |
| 10 | Redshift `COPY` Parquet + `STL_LOAD_COMMITS`; `UNLOAD MANIFEST VERBOSE`; `FNV_HASH` digest | pip (`redshift_connector`) | Apache-2.0 | Redshift side; S3 same region; IAM role | bulk cell | M | n (account) |
| 11 | OpenSearch CCR follow; `_reindex` from remote; snapshot rung; opensearch-py (SigV4, PIT, 429 retry) | pip + server features | Apache-2.0 | plugin on both, follower >= leader; allowlist set; version matrix | the follow, fence, confirm cells (`GAPS 34`) | M | y |
| 12 | RFS worker (Migration Assistant) | container | Apache-2.0 | snapshot repo exists, source loaded, ES 5-8 / OS 1-2, `_source` present | zero-load backfill and ES-to-OS version gaps | M | y |
| 13 | DynamoDB export / import rung + incremental export catch-up | pip (have) | AWS service | PITR on, target absent, bytes above the scan/RCU crossover, cost said | terabyte tables without RCUs | M | n (Local lacks export) |
| 14 | DSBulk | tarball + JRE via doctor | Apache-2.0 | table bytes x RF above the driver's measured rate, CL ONE ok, no counters | Cassandra bulk at JVM speed with per-cell time | M | y |
| 15 | Scylla CDC log reader (own) | pip (have) | Apache-2.0 | source is Scylla with `cdc` on; generations/tablets read | Cassandra-family follow cell | L | y (Scylla image) |
| 16 | migration-verifier | binary via doctor | Apache-2.0 | same-engine pair, 4.2+, DDL frozen, second reader asked | independent verifier | S | y |
| 17 | Cassandra ranges by `table_estimates`, replica routing, server-hash question (§7) | pip (have) | Apache-2.0 | every Cassandra copy | balanced workers, verify as it lands | M | y |
| 18 | CDM / scylla-migrator | container | Apache-2.0 | very large tables with a VM beside the cluster; DiffData as second reader | JVM-speed bulk, DynamoDB to Scylla | M | y (arm64 to verify) |
| 19 | mongodump `--oplog`/`--oplogReplay`, `--gzip`, insertion workers, index build after | binary (have) | Apache-2.0 | whole replica set, no `--db`, not sharded; link-bound | point-in-time dump; the huge-collection bottleneck | S | y |
| 20 | MirrorMaker 2 | container | Apache-2.0 | msg/s above Python's ceiling, container present, long follow | throughput rung | M | y |
| 21 | rclone for cross-store Parquet copies; S3 CRC64NVME verify | binary via doctor | MIT | many objects / bytes, stores differ | faster file copies, checksums | S | y (MinIO) |
| 22 | redis-full-check | binary (x86 Linux only) | Apache-2.0 | second reader on x86 | independent verifier | S | n (amd64 only) |
| 23 | `redpanda_migrator` | binary/container | Apache-2.0 (proven at run) | registries on both sides, IDs to keep | schema-ID preservation | S | y |
| 24 | clickhouse-backup binary | binary via doctor | MIT | RBAC/configs must move; server already present | RBAC with data | S | y |
| 25 | chDB / clickhouse-local | pip | Apache-2.0 | a side is files/lake; server-side pulls | mover and second reader for files | S | y |
| 26 | `rdb-cli` (librdb) | source build via doctor | MIT | source is an RDB file | file sources | S | y |
| 27 | Qdrant engine | pip | Apache-2.0 | R16b | new engine | M | y |
| 28 | Neo4j engine | pip + `neo4j-admin` | driver Apache-2.0, server GPLv3 (driven) | R16c | new engine | L | y |
| 29 | Milvus engine | pip | Apache-2.0 | R16b | new engine | L | y |
| 30 | Trino as second reader | pip | Apache-2.0 | a Trino already reaches both sides; never as a mover with FTE | cross-engine digest in one SQL | S | y (heavy image) |
| 31 | ZDM proxy named in `assess`; kcat probe; elasticdump's type list built in | none | - | see §2 | completeness | S | - |
| - | not wrapped: MongoShake (GPL), RIOT-X (BSL), PeerDB (AGPL), dynamodump, elasticdump, Logstash, Airbyte, Singer taps, dlt, NiFi, Vector, s5cmd (unless rclone absent), clickhouse-copier, MaterializedMySQL | | | | | | |

---

## 6. Scorecard

Verdict = migkit against the tool on that axis. Evidence: the tool's fact
[source] and migkit's file:line. "S" = the security report
(`security-oss-tools-2026-09-27.md`) section for that tool.

| tool | correctness / verification depth | bulk throughput + technique | change-follow latency / apply rate | resume / idempotence | type and metadata fidelity | topology coverage | ops / observability | security (TLS, auth, secrets, ACL carry-over) | licence |
|---|---|---|---|---|---|---|---|---|---|
| mongosync | ahead: its verifier restarts from zero on pause, skips capped/TTL/collation [ms-verify]; migkit ranges per `_id` type with coverage check `mongodb.py:1073-1140`, delta by token `:1555` | behind: multi-collection parallel copy in Go, natural-order copy for random `_id` [ms-start]; migkit raw BSON 200k docs 0.7-1.0 s `mongodb.py:128-131` (unmeasured against it) | equal: CEA stream vs migkit change stream `mongodb.py:558`; migkit's apply rate unmeasured (R14) | ahead: mongosync re-applies after restart, whole migration restarts if stopped before `canWrite` [ms-commit]; migkit `ReplaceOne` by `_id` `:161`, resume `:143-159` | equal: both keep BSON; mongosync rewrites unique/TTL/capped until commit [ms-beh]; migkit builds indexes after `:179-186` | behind: sharded with per-shard instances [ms-bin]; migkit's copier is per replica set, sharded only detected `:1770` | equal: `/progress` lag fields [ms-progress] vs migkit events; migkit reads the deprecated `lagTimeSeconds` `movers.py:2587` | equal: S §mongosync; migkit private config file `movers.py:2545`, telemetry off `:2534` | proprietary; migkit MIT |
| mongodump/mongorestore | ahead: no verification [md-restore]; migkit check + delta | behind on one huge collection (1 insertion worker default) [md-restore]; equal otherwise; migkit `--numParallelCollections` `movers.py:2361-2365` | n/a (`--oplog` point-in-time only) | ahead: pipe restarts from zero; continues past dup-key errors silently [md-restore]; migkit copier resumes by `_id` | equal: BSON archive; `--preserveUUID` optional | behind: `--oplog` not sharded [md-dump]; equal otherwise | ahead: stderr bar only [md-progress]; migkit parses nothing yet, own copier reports | equal: S §mongodump; password by `--config` file `movers.py:2402-2430` | Apache-2.0 |
| migration-verifier | equal: raw-BSON `bytes.Equal`, generations, rechecks [mv-compare]; migkit `check_data` `mongodb.py:863` + delta `:1555`; migkit ahead cross-engine (`canon`) | n/a | equal: change reader lag `srcChangeStats.lagSecs` [mv-api] vs migkit fence `:1058` | equal: resume tokens in metadata; DDL crashes it [mv-limits]; migkit code 286 re-baseline `:1596` | equal: binary compare; `toHashedIndexKey` mode equates Long/Double [mv-compare] | equal: replica set and sharded; `natural` scheme replica set only | equal: REST `/progress`, NDJSON mismatches [mv-api] | equal: S n/a; connection strings on the command line (migkit would pass files) | Apache-2.0 |
| MongoShake | ahead: count-only `comparison.py` [msk-faq] | behind: 6x8 parallel full sync [msk-conf]; unmeasured | equal: oplog/change stream; DDL replay not idempotent [msk-faq] | behind: checkpoint in a register centre [msk-arch]; migkit tail token `mongodb.py:1878` equal in kind | equal | ahead: sharded needs balancer off, mongos url [msk-conf]; migkit same limits | behind: Prometheus 9102 [msk-rel]; migkit `watch_sample` `:1965` | equal: S n/a | GPL-3.0: never bundled |
| RedisShake v4 | ahead: none [rs-mode]; migkit `check_data` `redis.py:494`, TTL drift `:709-755`, delta `:1171` | behind: PSYNC stream in Go [rs-sync]; migkit SCAN+DUMP 200k keys 3.4-3.7 s (R18.2) | behind: AOF forwarded at 100 ms ACKs [rs-sync]; migkit has no own follow (REPLICAOF only `:872`) | ahead: no resume, no reconnect, panic on topology change [rs-mode]; migkit cursor resume `:281-283` | equal-: RESTORE with relative TTL, expired clamped 1 ms, no IDLETIME/FREQ [rs-rdb]; migkit relative TTL too, expired skipped `:269-271`, no IDLETIME/FREQ | behind: standalone, sentinel, cluster, Tair [rs-readme]; migkit single node `:9-31` | equal: `status_port` JSON [rs-status]; migkit progress `:285` | equal: S §redis-shake; TLS on the writer; migkit socket TLS not set (R17a "still to") | MIT |
| redis-full-check | ahead: length-only default, one direction, no TTL [rfc-aliyun]; migkit both directions `:1040-1070`, TTL `:736-755` | n/a | n/a | equal: rounds re-ask conflicts [rfc-aliyun]; migkit "asked again until it matches" `:1171-1180` | equal: modes 1-4 | equal: standalone, cluster, Aliyun/Tencent proxies [rfc-readme] | behind: SQLite conflict tables, metrics file [rfc-readme]; migkit drilldown files (`evidence.py`) equal in kind | equal: S n/a | Apache-2.0 (1.4.10+); amd64 only |
| RIOT-X | ahead: `compare full` type/TTL/value after scan [riot-docs]; migkit continuous delta | equal-: DUMP/RESTORE or `--struct` [riot-docs]; migkit lacks `--struct` | behind: live mode on keyspace notifications (unreliable, its own words) [riot-docs]; migkit none | equal: none documented | ahead: `--struct` loses encodings; migkit DUMP keeps them | equal | behind: Prometheus `/metrics` (third-party claim) [riotx-wiki] | equal: S n/a | BSL: not wrapped |
| MirrorMaker 2 | ahead: no verification; at-least-once without EOS [mm2-eos]; migkit re-hash per partition `kafka.py:1141`, exact count `:320-345` | behind: JVM Connect tasks [mm2-ops]; Python producer a third of Java [kafka-bench] | behind: continuous Connect tasks; migkit 1 s polling rounds `:439-484` | ahead: translation coarse, earlier offsets by design (KAFKA-12468) [mm2-upgrade]; migkit by timestamp and message `:280-283` | equal: key, value, headers, timestamp, partition [mm2-task]; migkit same `:349-354` | equal: any brokers 2.1+ for 4.x [kafka40]; migkit any | behind: JMX `record-rate`, `replication-latency-ms` [mm2-ops]; migkit `tailctl.beat` `:479` | ahead: ACL sync TOPIC+LITERAL only, ALL downgraded [mm2-acl]; migkit ACLs by full key `users.py:146`; S §MM2 | Apache-2.0 |
| redpanda_migrator | ahead: none; offsets "best-effort" [rp-out]; migkit exact count | behind: Go, Connect pipeline | behind: continuous | equal: source-offset header for translation [rp-groups]; migkit has none (to build) | ahead-: schema IDs preserved via IMPORT [rp-out]; migkit re-registers `kafka.py:191` (behind on IDs) | equal | behind: Prometheus per topic/partition lag [rp-in] | equal: ACL rule same as MM2 [rp-out]; migkit ahead by full key | Apache-2.0 (proven at run) |
| kcat | n/a (probe) | n/a | n/a | n/a | equal: `%o %p %T %h` tokens [kcat-readme] | equal | equal | equal: `-X` librdkafka TLS/SASL | BSD-2; unmaintained |
| DSBulk | ahead: no verification; `count` modes only [dsb-count]; migkit compare `cassandra.py:259` (client fold) | behind: `8C` splits, continuous paging on DSE [dsb-schema]; migkit `workers x 8` uniform ranges `:457-461`, unmeasured | n/a | equal: `checkpoint.csv` resume/retry strategies [dsb-log]; migkit `RESUMES_BY_KEY = False` `:27`, whole-table restart (behind) | equal: `preserveTimestamp/preserveTtl` per cell, BATCH per row, not counters [dsb-schema]; migkit per cell `:434-445`, counters refused `:409` | equal: any Cassandra 2.1+, DSE, Astra [dsb-install] | behind: Prometheus, per-execution logs, exit codes 0-5 [dsb-exit]; migkit a log line per table `:497` | equal: S §DSBulk; migkit driver TLS unset (grep: none in `cassandra.py`) | Apache-2.0 |
| CDM | equal: DiffData + AutoCorrect, resurrects deletes if on [cdm-readme]; migkit compare, no repair on Cassandra yet | behind: Spark `numParts` 5000, `ratelimit` 20000 [cdm-props] | n/a | equal: `trackRun`, `autoRerun`, `rerunMultiplier` in target keyspace [cdm-props]; migkit whole-table restart (behind), no footprint (ahead) | ahead: max writetime/TTL per row [cdm-props]; migkit per cell `:434-445`; counter zombies [cdm-props] vs refused `:409` | equal | behind: log lines, TRACE detail; summary and exit codes undocumented [cdm-readme] | equal: S §CDM | Apache-2.0 |
| ZDM proxy | n/a (dual write) | n/a | equal: dual writes at client CL; LWT, counters, `+=` diverge [zdm-feas]; migkit's check is the reconciliation | n/a | n/a | equal: any CQL v3-v5 [zdm-rel] | behind: Prometheus and 3 dashboards [zdm-auto] | equal: S n/a; no Kerberos DSE auth [zdm-feas] | Apache-2.0 |
| scylla-migrator | ahead-: Validator with tolerances, `copyMissingRows` resurrects [sm-config]; migkit no tolerances yet on Cassandra | behind: Spark, shard-aware connector [sm-docs] | behind: DynamoDB `streamChanges` (24 h) [sm-stream]; migkit Streams tail `dynamodb.py:667` ahead on resume, behind on Cassandra (none) | equal: savepoint YAML `skipTokenRanges` [sm-save]; migkit segments per shard (DynamoDB) | equal: `preserveTimestamps` not with collections [sm-config]; migkit collections untimed `cassandra.py:420-424` | ahead: Scylla, Cassandra, DynamoDB, Alternator, Parquet, MySQL [sm-rel] | behind: Spark UI/logs | equal: S n/a | Apache-2.0 |
| sstableloader / load-and-stream | ahead: none; migkit digest after | behind: physical streaming to owners [cas-bulk] | n/a | equal: re-run streams again | ahead-: tombstones and TTLs land as stored (better than any CQL copy) | behind: format matrix nb/oa/me/mt, Scylla undocumented for nb/oa [scy-8583] | behind: `--verbose` progress | equal: SSL flags [cas-bulk]; migkit through ssh (R17c) | Apache-2.0 / AGPL server (Scylla) |
| clickhouse-backup | ahead: `max_broken_part_ratio`, no data verification [chb-readme]; migkit fingerprints `clickhouse.py:399-450` | behind: FREEZE hardlinks + object copy, server-side CopyObject [chb-readme] | n/a (`watch` is periodic backups) | equal: `--resume`, `.resumable` files (#1569 open) [chb-issues]; migkit delta by parts `:545-620` | equal: parts byte for byte; RBAC and configs [chb-readme]; migkit users `users.py:151` | behind: replicated restore, `--drop-replica-if-exists` [chb-rel] | behind: REST `server`, `/metrics`, callbacks [chb-readme] | equal: S §clickhouse-backup | MIT |
| `remote()` INSERT SELECT | equal: Altinity verifies by `system.parts` counts [alt-remote]; migkit fingerprints per partition (ahead) | equal: server to server, `max_insert_threads` [alt-remote]; migkit none yet (`GAPS 33`) | n/a | equal: per-partition re-run, `insert_deduplication_token` [ch-dedup]; migkit none yet | equal: native types | equal: `cluster()` / shards [ch-cluster] | behind: `system.query_log` only | equal: `remoteSecure`, `readonly=1` user [ch-remote] | Apache-2.0 |
| PeerDB | equal: none in-flight | behind: ctid ranges + binary COPY, 150 MB/s [peer-blog] | behind: slot streaming in Go | equal: Temporal catalog | equal | equal | behind: Temporal UI | equal: S n/a | AGPLv3: not wrapped |
| elasticdump | ahead: none; misses last batch at concurrency 2 (#741) [ed-741]; migkit `check_data` `opensearch.py:391`, delta `:588` | ahead: single scroll, Node heap limits (#472) [ed-472]; migkit sliced scroll per worker `:666` | n/a | ahead: `--offset` cannot resume (#624) [ed-624]; migkit resumes per table (slice resume to build) | equal: `_source` and `_id`; types settings/mapping/alias/template [ed-readme] (migkit lacks templates: behind on that) | equal | equal: stdout | equal: S §elasticdump | Apache-2.0 |
| OpenSearch MA / RFS | ahead: backfill only, `CompletedWithErrors` [osma-rel]; migkit delta by seq_no `opensearch.py:524-635` | behind: 590k docs/min per 2 vCPU worker, 5 TiB in 35 min with 200 workers [osma-readme] | behind: Capture/Replay (ids not preserved) [osma-fit]; migkit none (CCR to build) | equal: leases, exit 0/2/3, re-run overwrites by `_id` [rfs-readme] | equal: `_source` needed; type transforms ES->OS [osma-meta] | ahead: Kubernetes required for the assistant [osma-arch]; RFS standalone | behind: Argo workflows, metrics | equal: S §MA | Apache-2.0 |
| `_reindex` from remote | equal: none | equal: server pull, no slicing, 100 MB buffer [es-reindex] | n/a | equal: `op_type: create`, `conflicts: proceed` re-run | ahead: `version_type: external` keeps versions [es-reindex]; migkit does not carry `_version` (behind on that) | ahead: same/newer major only | equal: task API | equal: allowlist + SSL in yml [os-reindex] | Apache-2.0 (OS) |
| OpenSearch CCR | n/a | n/a | behind: shard-level replication [os-ccr]; migkit none | equal | equal | equal: follower >= leader, both secured or both not [os-ccr] | behind: `_status` API | equal: built-in roles [os-ccr-perm] | Apache-2.0 |
| DynamoDB export / import | ahead: import counts duplicates as success, partial table on failure [ddb-import]; migkit key compare `dynamodb.py:487` | behind: service-side, no RCU/WCU [ddb-export] | behind: incremental export 15 min-24 h windows [ddb-export-out]; migkit Streams tail `:667` (ahead on latency, behind past 24 h) | equal: export is a snapshot; import once | equal: DynamoDB JSON | ahead: new table only, no LSI [ddb-import] | equal: `DescribeImport` counts [ddb-import] | equal: S n/a; IAM | AWS service |
| dynamodump | ahead: drops unprocessed after 6 retries [dd-src] | ahead: sequential scan [dd-src]; migkit segments `:906-909` | n/a | ahead | equal | equal | equal | equal | MIT |
| rclone | equal: `check --download` bytes [rc-check]; migkit rows | behind: 4 streams, 8 checkers, server-side S3 copies [rc-docs]; migkit one file at a time `parquet.py:411-416` | n/a | equal: re-run syncs | n/a (files) | ahead: 70+ backends [rc-docs]; migkit local + S3 `parquet.py:110` (behind on that) | behind: `rc` stats, JSON log, exit codes [rc-rc] | equal: S n/a | MIT |
| pyiceberg / delta-rs (targets) | equal: `inspect.files` / footer counts; migkit's digest | equal: `add_files` registers without rewrite [pyi-api] | equal: delta `load_cdf` as a source [dr-cdf] | equal: snapshot summary / `txn` (caller checks) [pyi-api], [deltars-txn] | equal: Arrow/Parquet | equal | n/a | equal | Apache-2.0 |
| Airbyte (DB sources) | ahead: no verification | ahead: JSON records, Java in docker [ab-lic] | equal: CDC via Debezium | equal: STATE messages [ab-proto] | equal | equal | behind: platform UI | equal: S §Airbyte (managed report) | ELv2 connectors |
| Singer / Meltano taps | ahead | ahead: JSON lines | equal (wal2json) | equal: bookmarks | equal | equal | behind | equal | ELv2 / Apache / AGPL per tap |
| dlt | ahead: none | equal: pyarrow backend 20-30x sqlalchemy [dlt-sql]; migkit Arrow planned (R18.6) | equal: incremental cursors | equal: `_dlt_pipeline_state` on target [dlt-state] (a footprint: migkit ahead) | equal: `full_with_precision` [dlt-sql] | equal | equal | equal | Apache-2.0 |
| Trino | equal (second reader) | behind as a mover; double commit under FTE [trino-dup] | n/a | ahead: not exact under retries [trino-dup] | equal | ahead-: connectors for every engine here | behind: web UI, `system.runtime.queries` | equal: TLS/JWT/Kerberos | Apache-2.0 |
| neo4j-admin dump/load/import | equal: none; migkit counts per label (to build) | behind: native-format import [neo4j-import] | behind: CDC Enterprise only [neo4j-cdc] | equal: offline whole-database | equal | ahead: Community offline only [neo4j-copy] | equal | equal: no users/roles in dumps, no TLS [neo4j-dump] | GPLv3 server, Apache-2.0 driver |
| milvus-backup / VTS / milvus-cdc | equal: none | behind: segment binlogs [mb-readme] | behind: milvus-cdc active-standby [milvus-cdc]; migkit none | equal | equal | ahead-: 2.2+ into 2.5+, same-provider storage [mb-readme] | equal | equal | Apache-2.0 |
| qdrant/migration | equal: none; migkit recall@k (to build) | equal: batch 50 [qd-mig] | n/a | equal: resumable [qd-mig] | equal | equal: container only; some targets pre-created | equal | equal | Apache-2.0 |
| Snowflake `COPY INTO` / `write_pandas` | equal: `HASH_AGG` server digest [sf-hashagg]; migkit none yet | behind: PUT + COPY from Parquet [sf-connector]; migkit DB-API inserts `warehouse.py:114` | n/a | ahead-: 64-day load metadata idempotence [sf-loadmeta]; migkit delete+insert | equal: `use_logical_type` | n/a | equal: `COPY_HISTORY` | equal: key-pair/OAuth in connector | Apache-2.0 connector |
| BigQuery load / Write API | equal: `FARM_FINGERPRINT` sums | behind: Parquet load, Write API pending streams [bq-writeapi]; migkit JSON load per batch `warehouse.py:285` | behind: committed streams with offsets [bq-writeapi] | ahead-: `job_id` per batch [bq-jobs], offsets; migkit delete then load, not atomic `warehouse.py:203-206` | equal | n/a | equal: job history six months [bq-jobs] | equal | Apache-2.0 client |
| Redshift `COPY` / `UNLOAD` | equal: `FNV_HASH` chain [rs-fnv]; `MANIFEST VERBOSE` counts [rs-unload] | behind: COPY Parquet via Spectrum [rs-copy]; migkit DB-API | n/a | equal: `STL_LOAD_COMMITS` ledger; COPY has no dedup | ahead-: TIMESTAMPTZ zone dropped on Parquet unload [rs-unload] | n/a | equal: `STL_LOAD_ERRORS` [rs-copy] | equal: IAM_ROLE, SSE-KMS | AWS service |

---

## 7. What migkit must build to be ahead, per engine (the mechanism)

**MongoDB**
1. Commit gate on `lag.overallLagSeconds` and migkit's own fence, never
   `lagTimeSeconds` (`movers.py:2587`).
2. Sharded sources: one mongosync per shard by `--id`, identical API calls
   to each; balancer state checked and said; or the copier per shard through
   `config.shards` with `_id` ranges per chunk.
3. Verification that costs the server, not migkit: `$toHashedIndexKey`
   summed per `_id` range on both sides as the screen, raw-bytes compare on
   the ranges that differ; `dbHash` after a raw copy on a quiesced set.
4. `find_raw_batches` and `compressors` chosen from the link probe;
   `bulk_write` across namespaces on 8.0+.
5. mongodump `--oplog` + `--oplogReplay` as the point-in-time rung; index
   builds after data, in parallel, only where measured faster (R19.5).

**Redis / Valkey**
1. `RESTORE ... ABSTTL <PEXPIRETIME> IDLETIME <OBJECT IDLETIME> FREQ <OBJECT FREQ>`
   per key; hash-field TTLs (`HPEXPIRETIME`, 7.4+) checked on both sides.
2. Cluster keyspaces: SCAN per master node (`CLUSTER SHARDS`), slot-aware
   writes, `RedisCluster` pipelines per node; or RedisShake `cluster=true`
   where PSYNC is allowed.
3. The per-type rebuild rung (RIOT-X's `--struct`, migkit's own) when
   RESTORE is refused or brands differ, streams with consumer groups and
   PEL rebuilt exactly.
4. A follow of migkit's own where REPLICAOF is refused: RedisShake's
   sync_reader wrapped, or keyspace notifications per node with a
   dropped-listener error as the delta already has (`redis.py:1171`).
5. Snapshot by `BGSAVE` to the target's own disk (or `--rdb` from a
   replica) named in the state, not a shape count (`:310`).

**Kafka**
1. A `migkit.source.offset` header on every copied message; group offsets
   translated exactly by header, timestamp as the fallback.
2. Transactional copy with confluent-kafka: messages and the checkpoint in
   one transaction; idempotent producer; zstd/lz4 from the link probe.
3. Schema registry IDs preserved through IMPORT mode when the target's
   subjects are empty; translated (as now) otherwise.
4. MM2 as the throughput rung (container), its checkpoints topic read for
   translation; KIP-1279 named as not yet released.
5. Compacted topics: the gap loss said before the move (only byte copiers
   keep offsets); tombstones as `value=None`; target `delete.retention.ms`
   compared.

**Cassandra / ScyllaDB**
1. Ranges from `system.table_estimates` on every node (node-local, not
   replicated), replica-routed by `token_map.get_replicas`, `BYPASS CACHE`
   on Scylla; the uniform slices (`cassandra.py:457-461`) only where the
   estimates are absent.
2. A range digest without a server hash: read each range once, fold on
   the client while writing (verify-as-it-lands at no extra read), and a
   Merkle-style bisect on mismatch (cassandra §8); on 5.0 compare
   `WRITETIME`/`TTL` per cell within scylla-migrator's tolerances.
3. The follow: Scylla CDC log tables of migkit's own (generations, tablets
   stream sets, confidence window); Cassandra sources by ZDM or a stop,
   said by `capabilities.GAPS`.
4. Resume by token range (`RESUMES_BY_KEY = False` today): the checkpoint
   holds the ranges done, as DSBulk's `checkpoint.csv` and CDM's
   `cdm_run_details` do, without their footprint.
5. DSBulk and CDM wrapped for JVM-speed bulk where measured faster; counter
   tables moved by `c = c + delta` from a read of the source, once, with the
   target's counter proven zero first.

**ClickHouse**
1. `remote()` pull/push per partition with `insert_deduplication_token`
   as the exact batch, `ATTACH PARTITION FROM` a temporary table for atomic
   landing, partitions skipped by matching fingerprint.
2. `BACKUP`/`RESTORE` as the physical rung; `system.parts` hash compare and
   `CHECK TABLE` after it; clickhouse-backup for RBAC/configs.
3. A follow by changed partitions: `system.parts` signatures polled (the
   delta's device, `clickhouse.py:545`) and only changed partitions
   re-copied; fence = replication, mutation and distribution queues empty
   (R13 list).
4. Replicated and Distributed tables: shard-local `INSERT SELECT`
   (`parallel_distributed_insert_select=2`), `clusterAllReplicas` for the
   settings compare.

**OpenSearch / Elasticsearch**
1. CCR as the follow where the plugin exists; `_seq_no` polling plus id
   diff otherwise (continuous form of `opensearch.py:524-635`); fence by
   per-shard checkpoints; `_version` carried with `version_type: external`.
2. PIT + `search_after` per slice so a copy resumes by `_shard_doc`;
   `streaming_bulk`-style 429/503 retry; opensearch-py with SigV4.
3. `_reindex` from remote and snapshot restore as server-side rungs; RFS
   as the zero-load backfill; templates, component templates, aliases,
   ISM/ILM moved by REST (elasticdump's list) and compared.
4. Security roles compared and carried (`_plugins/_security/api`), the
   R13 "still to".

**DynamoDB**
1. Export/import rung with manifest counts as the free verification and
   incremental exports as the catch-up beyond 24 h; Kinesis for long moves.
2. `TransactWriteItems` + `ClientRequestToken` as the tail's exact batch;
   `WarmThroughput` proposed through approvals.
3. Consistent-read parallel scan only for the verify sample; the rest
   eventually consistent, then the Streams replay (AWS's own advice).

**Parquet / lakes / warehouses**
1. Iceberg and Delta targets with the exact batch in the snapshot summary
   or `txn` action; DuckDB as the Parquet writer with `RETURN_STATS`;
   footer counts and CRC64NVME as the free checks; rclone for cross-store
   copies.
2. Snowflake `PUT` + `COPY INTO` with file-name idempotence and `HASH_AGG`;
   BigQuery Parquet load jobs with client job ids and the Write API's
   pending streams; Redshift `COPY`/`UNLOAD MANIFEST VERBOSE` with
   `STL_LOAD_COMMITS` and `FNV_HASH` - each a `native_bulk` on
   `warehouse.py`, chosen when the batch exceeds the measured DB-API rate.

**New engines (deferred with R16)**: Qdrant (M), Neo4j (L), Milvus (L) as
described in §4.1-4.2, each with a check no row compare gives (recall@k,
endpoints compared).

---

## 8. Sources

migkit's own reports: `docs/research/{mongodb,redis,kafka,cassandra-scylla,clickhouse-opensearch,dynamodb-lakes-pipelines,security-oss-tools}-tools-2026-09-27.md` (the six carry the per-tool URLs not repeated here); `docs/backlog.md` "P0: the decision layer" (decided 2026-09-27), R13, R16, R17, R18, R19.

* [ms-beh] https://www.mongodb.com/docs/mongosync/current/reference/mongosync/mongosync-behavior/
* [ms-start] https://www.mongodb.com/docs/mongosync/current/reference/api/start/
* [ms-progress] https://www.mongodb.com/docs/cluster-to-cluster-sync/current/reference/api/progress/
* [ms-commit] https://www.mongodb.com/docs/mongosync/current/reference/api/commit/
* [ms-verify] https://www.mongodb.com/docs/mongosync/current/reference/verification/embedded/
* [ms-bin] https://www.mongodb.com/docs/mongosync/current/reference/mongosync-binary/
* [md-dump] https://www.mongodb.com/docs/database-tools/mongodump/
* [md-restore] https://www.mongodb.com/docs/database-tools/mongorestore/
* [md-progress] https://pkg.go.dev/github.com/mongodb/mongo-tools/common/progress
* [mv-compare] https://raw.githubusercontent.com/mongodb-labs/migration-verifier/main/internal/verifier/compare.go
* [mv-api] https://github.com/mongodb-labs/migration-verifier
* [mv-limits] https://github.com/mongodb-labs/migration-verifier (README, limitations)
* [msk-faq] https://github.com/alibaba/MongoShake/wiki/FAQ
* [msk-conf] https://raw.githubusercontent.com/alibaba/MongoShake/develop/conf/collector.conf
* [msk-arch] https://github.com/alibaba/MongoShake
* [msk-rel] https://api.github.com/repos/alibaba/MongoShake/releases
* [rs-mode] https://tair-opensource.github.io/RedisShake/en/guide/mode.html
* [rs-sync] https://github.com/tair-opensource/RedisShake/blob/v4/internal/reader/sync_standalone_reader.go
* [rs-rdb] https://github.com/tair-opensource/RedisShake/blob/v4/internal/rdb/rdb.go
* [rs-readme] https://github.com/tair-opensource/RedisShake
* [rs-status] https://github.com/tair-opensource/RedisShake/blob/v4/internal/status/status.go
* [rfc-aliyun] https://developer.aliyun.com/article/690463
* [rfc-readme] https://github.com/tair-opensource/RedisFullCheck
* [riot-docs] https://redis.github.io/riot/
* [riotx-wiki] https://deepwiki.com/redis/riotx-dist
* [mm2-task] https://github.com/apache/kafka/blob/trunk/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java
* [mm2-ops] https://kafka.apache.org/41/operations/geo-replication-cross-cluster-data-mirroring/
* [mm2-eos] https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors
* [mm2-upgrade] https://kafka.apache.org/38/getting-started/upgrade/
* [mm2-acl] https://github.com/orgs/strimzi/discussions/5419
* [kafka40] https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/
* [kafka-bench] https://sderosiaux.medium.com/i-benchmarked-java-vs-python-kafka-clients-with-bayesian-optimization-java-was-3x-faster-9d89ca843607
* [kip1279-status] https://getkafkanated.substack.com/p/get-kafka-nated-espresso-august-2026 ; https://cwiki.apache.org/confluence/spaces/KAFKA/pages/429064575/Release+Plan+4.4.0 ; https://acemq.com/blogs/kafka-end-of-life/ ; https://developers.redhat.com/articles/2026/09/22/data-liberation-apache-kafka-native-cluster-mirroring
* [rp-mig] https://docs.redpanda.com/redpanda-connect/components/inputs/redpanda_migrator/
* [rp-in] https://docs.redpanda.com/redpanda-connect/components/inputs/redpanda_migrator/
* [rp-out] https://docs.redpanda.com/redpanda-connect/components/outputs/redpanda_migrator/
* [rp-groups] https://github.com/redpanda-data/connect/blob/main/internal/impl/redpanda/migrator/migrator_groups.go
* [rp-lic] https://docs.redpanda.com/redpanda-connect/get-started/licensing/
* [kcat-readme] https://github.com/edenhill/kcat
* [dsb-schema] https://docs.datastax.com/en/dsbulk/reference/schema-options.html
* [dsb-count] https://docs.datastax.com/en/dsbulk/developing/count-data.html
* [dsb-log] https://docs.datastax.com/en/dsbulk/reference/logging-options.html
* [dsb-exit] https://docs.datastax.com/en/dsbulk/reference/exit-codes.html
* [dsb-install] https://docs.datastax.com/en/dsbulk/overview/install.html
* [dsbulk-docker] https://github.com/datastax/dsbulk ; https://hub.docker.com/r/datastax/astra-cli ; https://hub.docker.com/r/leogloriainfnet/dsbulk
* [cdm-readme] https://raw.githubusercontent.com/datastax/cassandra-data-migrator/main/README.md
* [cdm-props] https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties
* [zdm-feas] https://docs.datastax.com/en/data-migration/feasibility-checklists.html
* [zdm-rel] https://github.com/datastax/zdm-proxy/releases
* [zdm-auto] https://github.com/datastax/zdm-proxy-automation
* [sm-config] https://migrator.docs.scylladb.com/stable/configuration.html
* [sm-docs] https://migrator.docs.scylladb.com/stable/
* [sm-stream] https://migrator.docs.scylladb.com/stable/stream-changes.html
* [sm-save] https://migrator.docs.scylladb.com/stable/resume-interrupted-migration.html
* [sm-rel] https://github.com/scylladb/scylla-migrator/releases
* [cas-bulk] https://cassandra.apache.org/doc/latest/cassandra/managing/operating/bulk_loading.html
* [scy-8583] https://github.com/scylladb/scylladb/issues/8583
* [scylla-driver-pypi] https://pypi.org/pypi/scylla-driver/json
* [chb-readme] https://github.com/Altinity/clickhouse-backup/blob/master/ReadMe.md
* [chb-rel] https://github.com/Altinity/clickhouse-backup/releases
* [chb-issues] https://github.com/Altinity/clickhouse-backup/issues?q=is%3Aissue+is%3Aopen+restore
* [alt-remote] https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-data-migration/remote-table-function/
* [ch-remote] https://clickhouse.com/docs/sql-reference/table-functions/remote
* [ch-dedup] https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries
* [ch-cluster] https://clickhouse.com/docs/sql-reference/table-functions/cluster
* [peer-blog] https://blog.peerdb.io/parallelized-initial-load-for-cdc-based-streaming-from-postgres
* [ed-readme] https://github.com/elasticsearch-dump/elasticsearch-dump/blob/master/README.md
* [ed-741] https://github.com/elasticsearch-dump/elasticsearch-dump/issues/741
* [ed-472] https://github.com/elasticsearch-dump/elasticsearch-dump/issues/472
* [ed-624] https://github.com/taskrabbit/elasticsearch-dump/issues/624
* [rfs-readme] https://raw.githubusercontent.com/opensearch-project/opensearch-migrations/main/DocumentsFromSnapshotMigration/README.md
* [osma-readme] https://github.com/opensearch-project/opensearch-migrations/blob/main/README.md
* [osma-rel] https://github.com/opensearch-project/opensearch-migrations/releases
* [osma-fit] https://docs.opensearch.org/latest/migration-assistant/is-migration-assistant-right-for-you/
* [osma-arch] https://docs.opensearch.org/latest/migration-assistant/architecture/
* [osma-meta] https://docs.aws.amazon.com/solutions/latest/migration-assistant-for-amazon-opensearch-service/migrate-metadata.html
* [es-reindex] https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reindex-indices
* [os-reindex] https://docs.opensearch.org/latest/api-reference/document-apis/reindex/
* [os-ccr] https://docs.opensearch.org/latest/tuning-your-cluster/replication-plugin/getting-started/
* [os-ccr-perm] https://docs.opensearch.org/latest/tuning-your-cluster/replication-plugin/permissions/
* [ddb-export] https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html
* [ddb-export-out] https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html
* [ddb-import] https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.Validation.html
* [dd-src] https://raw.githubusercontent.com/bchew/dynamodump/master/dynamodump/dynamodump.py
* [rc-docs] https://rclone.org/docs/
* [rc-check] https://rclone.org/commands/rclone_check/
* [rc-rc] https://rclone.org/rc/
* [pyi-api] https://py.iceberg.apache.org/api/
* [deltars-txn] https://github.com/delta-io/delta-rs/issues/3821 ; https://delta-io.github.io/delta-rs/api/delta_table/ ; https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-acid-transactions/
* [dr-cdf] https://delta-io.github.io/delta-rs/latest/usage/read-cdf/
* [iceberg-kc] https://raw.githubusercontent.com/apache/iceberg/main/docs/docs/kafka-connect.md
* [arrow-hash-pr] https://github.com/apache/arrow/pull/45001
* [ab-lic] https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-integrations/connectors/source-mongodb-v2/metadata.yaml (`license: ELv2`, 2.0.7)
* [airbyte-mongo] same as [ab-lic]
* [ab-proto] https://docs.airbyte.com/platform/understanding-airbyte/airbyte-protocol
* [tap-pg-lic] https://raw.githubusercontent.com/MeltanoLabs/tap-postgres/main/LICENSE (Elastic License 2.0)
* [tap-mongo-lic] https://raw.githubusercontent.com/MeltanoLabs/tap-mongodb/main/LICENSE (Apache-2.0)
* [singer-mongo-lic] https://raw.githubusercontent.com/singer-io/tap-mongodb/master/LICENSE (AGPL-3.0)
* [dlt-sql] https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/configuration
* [dlt-state] https://dlthub.com/docs/general-usage/state
* [bento-readme] https://raw.githubusercontent.com/warpstreamlabs/bento/main/README.md
* [trino-pypi] https://pypi.org/pypi/trino/json (0.340.0, 2026-09-23, Apache-2.0)
* [trino-docker] https://github.com/trinodb/trino/blob/master/core/docker/Dockerfile ; https://hub.docker.com/r/trinodb/trino
* [trino-dup] https://github.com/trinodb/trino/issues/31246
* [neo4j-pypi] https://pypi.org/pypi/neo4j/json (6.0.2, Apache-2.0)
* [apoc-lic] https://raw.githubusercontent.com/neo4j/apoc/dev/LICENSE (Apache-2.0)
* [apoc-export] https://neo4j.com/docs/apoc/current/export/
* [neo4j-copy] https://neo4j.com/docs/operations-manual/current/backup-restore/copy-database/ ; https://neo4j.com/docs/operations-manual/current/backup-restore/
* [neo4j-dump] https://neo4j.com/docs/operations-manual/current/backup-restore/offline-backup/
* [neo4j-import] https://neo4j.com/docs/operations-manual/current/import/
* [neo4j-cdc] https://neo4j.com/docs/cdc/current/ ; https://neo4j.com/docs/cdc/current/changelog/ ; https://neo4j.com/docs/cdc/current/procedures/
* [pymilvus] https://pypi.org/pypi/pymilvus/json (3.0.2, Apache-2.0, `bulk_writer` extra)
* [milvus-import] https://milvus.io/docs/import-data.md
* [milvus-cdc] https://github.com/zilliztech/milvus-cdc
* [mb-readme] https://github.com/zilliztech/milvus-backup
* [qd-mig] https://github.com/qdrant/migration
* [qdrant-scroll] https://api.qdrant.tech/api-reference/points/scroll-points
* [sf-connector] https://docs.snowflake.com/en/user-guide/python-connector-api
* [sf-loadmeta] https://docs.snowflake.com/en/user-guide/data-load-considerations-load
* [sf-hashagg] https://docs.snowflake.com/en/sql-reference/functions/hash_agg
* [bq-writeapi] https://docs.cloud.google.com/bigquery/docs/write-api
* [bq-jobs] https://docs.cloud.google.com/bigquery/docs/managing-jobs ; https://docs.cloud.google.com/bigquery/docs/running-jobs
* [bq-emu] https://github.com/goccy/bigquery-emulator ; https://github.com/goccy/bigquery-emulator/pkgs/container/bigquery-emulator
* [fakesnow] https://pypi.org/project/fakesnow/ ; https://github.com/tekumara/fakesnow/blob/main/pyproject.toml
* [rs-copy] https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html
* [rs-unload] https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
* [rs-fnv] https://docs.aws.amazon.com/redshift/latest/dg/r_FNV_HASH.html
