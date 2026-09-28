# Cassandra and ScyllaDB migration/verification tools: research report for wrapping from Python (as of 2026-09-27)

All facts below come from public pages I fetched. A tag after a statement marks one of three things:
- **(inference)**: my own reasoning, not stated by the source.
- **(verify)**: not confirmed within this session.
- **(unverified)**: from my background knowledge, not from a fetched page.

I ran out of WebSearch budget partway through, so the later findings come from direct WebFetch of known URLs.

---

## 1. DSBulk (datastax/dsbulk)

**Status and distribution**
- DSBulk still lives at datastax/dsbulk on the `1.x` branch. Its license is Apache-2.0. https://github.com/datastax/dsbulk
- apache/cassandra-analytics is a different project: a Spark Bulk Reader/Writer for Cassandra 4.x/5.0 that needs Apache Cassandra Sidecar. https://github.com/apache/cassandra-analytics
- Recent releases on Maven Central:
  - 1.11.2 (2026-07-02): CVE upgrades to Netty, the Java driver, logback and Jackson.
  - 1.11.1 (2026-03-03): collection-null option, and moved publishing to Central Portal.
  - 1.11.0 (2023-07-13): S3 URL read support and the vector type.
  - Sources: https://repo1.maven.org/maven2/com/datastax/oss/dsbulk-distribution/ and https://github.com/datastax/dsbulk/releases
- Maintenance looks light: CVE fixes and small features only.
- Packaging: zip and tar.gz are the production formats. The executable jar is for trying it out only ("not for production"). The Maven artifact is `dsbulk-distribution`. https://github.com/datastax/dsbulk
- I found no official Docker image in the README (verify).
- Java: the launcher needs Java 8 or later. It fails with "Unable to find java 8 (or later)" when `JAVA_TOOL_OPTIONS` adds an extra line to the `java -version` output. https://github.com/datastax/dsbulk/issues/371
- arm64: it is pure JVM, so it should run anywhere a JDK does (inference).
- Supported databases: Cassandra 2.1+, DSE 5.1/6.8/6.9, HCD and Astra. https://docs.datastax.com/en/dsbulk/overview/install.html

**How it works**
- Subcommands are `load`, `unload`, `count` and `help`. https://docs.datastax.com/en/dsbulk/reference/dsbulk-cmd.html
- `schema.splits` (default `8C`, meaning 8 × cores) splits the token ring into ranges that are read in parallel for unload and count.
  - It must be larger than `engine.maxConcurrentQueries`.
  - It is ignored for load.
  - https://docs.datastax.com/en/dsbulk/reference/schema-options.html
- Each split is read with `token(pk) > ? AND token(pk) <= ?` queries routed to the replica that owns it (unverified).
- Throughput control:
  - `engine.maxConcurrentQueries` defaults to `AUTO` and accepts `NC`. https://github.com/datastax/dsbulk/blob/1.x/manual/settings.md
  - `executor.maxPerSecond`, `executor.maxInFlight` and `executor.maxBytesPerSecond` are **deprecated** and disabled by default. The docs say to use `engine.maxConcurrentQueries` instead. https://docs.datastax.com/en/dsbulk/reference/driver-options.html
- Continuous paging:
  - `executor.continuousPaging.enabled` defaults to true, but it only works at CL `ONE`/`LOCAL_ONE`. https://docs.datastax.com/en/dsbulk/reference/driver-options.html
  - It is a DSE-only protocol feature (unverified).
  - A warning is printed when unloading with CL > ONE. https://github.com/datastax/dsbulk/blob/1.x/changelog/README.md
- Mapping and timestamps:
  - `schema.mapping` accepts indexed (`0=col1`) or named (`fieldA=col1`) forms, plus `*=*` and exclusions like `*=[-c2]`. It also supports `writetime(*)`/`ttl(*)` pseudo-columns and functions.
  - `schema.preserveTimestamp` and `schema.preserveTtl` both default to false.
    - On unload they export `writetime(col)`/`ttl(col)` fields.
    - On load, preserveTimestamp generates **BATCH** queries.
    - Neither works with counter tables or a custom `schema.query`.
  - `schema.nullToUnset` defaults to true and needs protocol v4+.
  - `schema.queryTtl` defaults to -1.
  - Source for this group: https://docs.datastax.com/en/dsbulk/reference/schema-options.html
- `count` modes (`stats.modes`):
  - `global` (default), `ranges` (rows per token range), `hosts` (rows per host) and `partitions` (the top-N biggest partitions; `stats.numPartitions` defaults to 10).
  - Only `global` is allowed together with a custom `-query`.
  - https://docs.datastax.com/en/dsbulk/developing/count-data.html

**Output, progress and exit codes**
- Log directory: `log.directory` defaults to `./logs`, with one subfolder per execution ID such as `LOAD_20220814-221616-336561`. https://docs.datastax.com/en/dsbulk/reference/logging-options.html , https://github.com/datastax/dsbulk/issues/451
- Files written include `operation.log`, `paxos-errors.log` and `.bad` files (`connector.bad`, `mapping.bad`, `load.bad`, `paxos.bad`). https://docs.datastax.com/en/dsbulk/reference/logging-options.html
- A positions file is produced on unload/count (added in 1.10.0). https://github.com/datastax/dsbulk/blob/1.x/changelog/README.md
- Checkpoint/resume (1.10.0+):
  - `log.checkpoint.enabled` defaults to true and writes `checkpoint.csv`.
  - To resume, re-run with `--log.checkpoint.file=<path>`.
  - `log.checkpoint.replayStrategy` is `retry` (default: new + rejected records), `resume` (new only; safest for non-idempotent loads) or `retryAll`.
  - https://docs.datastax.com/en/dsbulk/reference/logging-options.html
- Error limit: `log.maxErrors` defaults to 100 and accepts an absolute number or `N%`. A negative value disables it. https://github.com/datastax/dsbulk/blob/1.x/manual/settings.md
- Progress: `monitoring.reportRate` defaults to 5 s. Prometheus export exists since 1.9.0. https://github.com/datastax/dsbulk/blob/1.x/manual/settings.md , https://github.com/datastax/dsbulk/releases
- Exit codes: 0 OK, 1 COMPLETED_WITH_ERRORS, 2 ABORTED_TOO_MANY_ERRORS, 3 ABORTED_FATAL_ERROR, 4 INTERRUPTED, 5 CRASHED. https://docs.datastax.com/en/dsbulk/reference/exit-codes.html

**Failure modes**
- A checkpoint file and resume hint are written even when the run succeeds, so a wrapper must check the exit code rather than the file's existence. https://github.com/datastax/dsbulk/issues/451
- `.bad` files only contain the source data when `log.sources=true`. https://docs.datastax.com/en/dsbulk/reference/logging-options.html
- Non-frozen collection writetimes cannot be preserved on pre-5.0 sources, because Cassandra <5.0 cannot select WRITETIME on collections. https://issues.apache.org/jira/browse/CASSANDRA-8877 (inference)

---

## 2. Cassandra Data Migrator (datastax/cassandra-data-migrator, CDM)

**Status and distribution**
- License is Apache-2.0. https://github.com/datastax/cassandra-data-migrator
- Very active:
  - 6.1.1 (2026-09-17): Spark 4.2.0 docs and Dockerfile, images now published to Quay.
  - 6.1.0 (2026-09-11).
  - 6.0.0 (2026-06-10): **Java 17 minimum**, Spark 4.1.x.
  - 5.x line: Java 11+, Spark 3.5.8, Scala 2.13.
  - https://github.com/datastax/cassandra-data-migrator/blob/main/RELEASE.md
- It ships as a jar you run with `spark-submit`, usually `--master "local[*]"` on a single VM. It can also run on a Spark cluster. Docker images are on Quay (6.1.1+) and older ones on DockerHub. https://github.com/datastax/cassandra-data-migrator
- arm64 image availability: not stated (verify).

**Jobs (selected with `--class`)**
- `com.datastax.cdm.job.Migrate`
- `com.datastax.cdm.job.DiffData`: validation, with optional AutoCorrect.
- `com.datastax.cdm.job.GuardrailCheck`: flags rows with any field larger than `spark.cdm.feature.guardrail.colSizeInKB`.
- The table is chosen with `spark.cdm.schema.origin.keyspaceTable`.
- Source: https://github.com/datastax/cassandra-data-migrator

**How it works** (all properties from https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties)
- Splitting: `spark.cdm.perfops.numParts` (default 5000) cuts the token range −2^63..2^63−1 into parts that are processed in parallel.
  - The range can be narrowed with `spark.cdm.filter.cassandra.partition.min/max` (inclusive).
  - Other filters: `spark.cdm.filter.cassandra.whereCondition` and `spark.cdm.filter.java.writetime.min/max`.
- Performance settings:
  - `batchSize` 5 (UNLOGGED batch), `fetchSizeInRows` 1000.
  - `spark.cdm.perfops.ratelimit.origin` / `.target` default to 20000. These are concurrent operations per CDM VM, applied per worker on a cluster.
  - Read and write consistency default to LOCAL_QUORUM.
- WRITETIME/TTL handling:
  - `spark.cdm.schema.origin.column.writetime.automatic` and `ttl.automatic` default to true. CDM takes the **max** writetime/TTL across eligible columns and applies it to the **whole row**, not per cell. `.names` restricts which columns count.
  - `spark.cdm.schema.ttlwritetime.calc.useCollections` defaults to false.
  - `spark.cdm.transform.custom.writetime` / `.custom.ttl` set constant values.
  - `spark.cdm.transform.custom.writetime.incrementBy` works around duplicate entries in non-frozen lists on reruns.
- Other transforms:
  - `spark.cdm.feature.explodeMap.*`: turns a map into rows; the key column must be in the target PK.
  - `spark.cdm.feature.constantColumns.names/types/values`.
  - `spark.cdm.feature.extractJson.*`.
  - Codecs such as `INT_STRING`, `TIMESTAMP_STRING_FORMAT` and so on.
  - `spark.cdm.transform.map.remove.null.value`.
- AutoCorrect:
  - `spark.cdm.autocorrect.missing` and `.mismatch` both default to false.
  - `spark.cdm.autocorrect.missing.counter` defaults to false.
  - DiffData never deletes anything; it only inserts or updates. https://github.com/datastax/cassandra-data-migrator
- Track/rerun:
  - `spark.cdm.trackRun=true` records progress in `cdm_run_info` and per-part `cdm_run_details` in the **target** keyspace.
  - `spark.cdm.trackRun.previousRunId=<id>` reprocesses only the token ranges that failed.
  - `spark.cdm.trackRun.autoRerun=true` (5.6.0+) finds the last incomplete run automatically.
  - `spark.cdm.trackRun.rerunMultiplier` (5.7.0+) splits failed ranges further.
  - `spark.cdm.trackRun.runId` sets a custom run ID.
  - Sources: https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties , https://github.com/datastax/cassandra-data-migrator/blob/main/RELEASE.md

**Output**
- Per-key log lines:
  - `ERROR DiffJobSession: Mismatch row found for key: [...] Mismatch: Target Index: ... Origin: ... Target: ...`
  - `Missing target row found for key:`
  - `Corrected mismatch row in target:`
  - `Inserted missing row in target:`
  - The docs say to grep `ERROR`. A log4j2 template splits these into their own file. https://github.com/datastax/migration-docs/blob/main/modules/ROOT/pages/cassandra-data-migrator.adoc
- Since 5.5.1, DiffData detail is logged at TRACE level. https://github.com/datastax/cassandra-data-migrator/blob/main/RELEASE.md
- Final summary counters (Read / Mismatch / Missing / Valid / Skipped): exact wording not confirmed (verify in `JobCounter.java`).
- Exit code semantics are not documented (verify).

**Failure modes**
- Null fields are written as UNSET to avoid tombstones (5.4.0+). As a result, a stale non-null value on the target will not be cleared (inference). https://github.com/datastax/cassandra-data-migrator
- If the origin WRITETIME is lower than the target's, the correction is silently shadowed by last-write-wins. https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties
- Counter "zombie" doubling: re-inserting a counter the target just deleted gives 5323 + 5323 = 10646. Cassandra rejects USING TIMESTAMP/TTL on counters. https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties , https://cassandra.apache.org/doc/latest/cassandra/developing/cql/counter-column.html
- Reruns can duplicate list entries. https://github.com/datastax/cassandra-data-migrator

---

## 3. ZDM Proxy (datastax/zdm-proxy)

**Status and distribution**
- Written in Go, licensed Apache-2.0. https://github.com/datastax/zdm-proxy
- Recent tags: v2.5.1 (2026-09-18), v2.5.0 (2026-07-21), v2.4.0 (2026-01-16). https://github.com/datastax/zdm-proxy/tags
- v2.5.1 moved images to Quay (`quay.io/datastax/zdm-proxy:v2.5.1`) and publishes **linux/amd64 and arm64**. https://github.com/datastax/zdm-proxy/releases
- v2.5.0 added `ZDM_PROXY_MAX_PREPARED_STATEMENT_CACHE_SIZE` (default 10000, LRU).
- v2.4.0 added protocol v5, LZ4/Snappy compression and `ZDM_BLOCKED_PROTOCOL_VERSIONS`.
- ZDM Proxy Automation is Ansible playbooks plus the "ZDM Utility" Go binary, with a Prometheus/Grafana stack and three dashboards. https://github.com/datastax/zdm-proxy-automation

**Configuration**
- Main variables: `ZDM_ORIGIN_CONTACT_POINTS`, `ZDM_TARGET_CONTACT_POINTS`, `ZDM_PRIMARY_CLUSTER` (default ORIGIN), `ZDM_READ_MODE` (default PRIMARY_ONLY), `ZDM_PROXY_LISTEN_PORT` (14002) and `ZDM_LOG_LEVEL`. https://github.com/datastax/zdm-proxy
- Async dual reads are enabled with `read_mode: DUAL_ASYNC_ON_SECONDARY` followed by a rolling restart. https://docs.datastax.com/en/data-migration/enable-async-dual-reads.html

**Behaviour**
- Writes go to both clusters concurrently at the client's CL. https://docs.datastax.com/en/data-migration/components.html
- "If the write fails in either cluster, then ZDM Proxy passes a write failure, originating from the primary cluster, back to the client." The client's retry policy then applies, but only for statements marked idempotent. https://docs.datastax.com/en/data-migration/components.html , https://docs.datastax.com/en/data-migration/feasibility-checklists.html
- Reads go to the primary. Async reads to the secondary are for testing only: their results are discarded and failures are only logged. https://docs.datastax.com/en/data-migration/components.html
- Phases:
  1. Deploy the proxy and connect clients.
  2. Migrate data.
  3. Enable async dual reads.
  4. Route reads to the target.
  5. Connect directly to the target.
  - https://docs.datastax.com/en/data-migration/introduction.html

**Limitations and failure modes** (from https://docs.datastax.com/en/data-migration/feasibility-checklists.html unless noted)
- Operations that can diverge between the two clusters:
  - LWTs, which can succeed with or without applying on each side.
  - Counters.
  - Collection `+=`/`-=`.
  - `now()`/`uuid()`: `replace_cql_functions` only replaces `now()` and is off by default because it costs performance.
  - DataStax recommends a reconciliation pass before and after Phase 4.
- Other limits:
  - Thrift is not supported.
  - DseAuthenticator with Kerberos is not supported.
  - Protocol v5 costs 0–15 % throughput.
  - Every statement must succeed unchanged on both clusters, so schemas must match.
- Enabling async reads can make **writes** on the secondary time out. https://docs.datastax.com/en/data-migration/enable-async-dual-reads.html

---

## 4. scylla-migrator (Spark)

**Status and distribution**
- License Apache-2.0, very active. Tags: v2.1.5 (2026-06-18), v2.1.0 and v2.1.1 (2026-05-01), v2.0.0 (2026-03-20), v1.1.2 (2026-01-30). https://github.com/scylladb/scylla-migrator/tags
- Migrator 2.x needs Spark 4.0.x and Scala 2.13; 1.x and 0.9.x need Spark 3.5.x. https://migrator.docs.scylladb.com/stable/
- The README still says "Java 8+ JDK" for building, which conflicts with Spark 4's Java 17 baseline (verify). https://github.com/scylladb/scylla-migrator
- Output is `scylla-migrator-assembly.jar`. It can be built with Docker, and docker-compose and Ansible/EMR flows exist. https://github.com/scylladb/scylla-migrator , https://migrator.docs.scylladb.com/stable/validate.html
- Classes: `com.scylladb.migrator.Migrator` and `com.scylladb.migrator.Validator`, configured with YAML.

**Sources and targets** (https://migrator.docs.scylladb.com/stable/configuration.html)
- Sources: `cassandra`, `parquet`, `dynamodb`, `alternator` and `dynamodb-s3-export`, plus `mysql` since v2.1.1. https://github.com/scylladb/scylla-migrator/releases
- Targets: `cassandra` (CQL), `dynamodb` and `alternator`. v2.0.0 also added Parquet and S3-export targets. https://github.com/scylladb/scylla-migrator/releases
- `splitCount` should be about 8 × Spark cores; more splits make resume finer-grained. https://github.com/scylladb/scylla-migrator/blob/master/config.yaml.example
- `preserveTimestamps` **cannot be used with tables that have collections**. https://migrator.docs.scylladb.com/stable/configuration.html

**Savepoints**
- The Spark connector records completed token ranges in a Spark accumulator (a custom AccumulatorV2). A scheduled thread on the driver dumps it every `savepoints.intervalSeconds` (example value 300) to `savepoint_<unix-ts>.yaml`.
- That file is a full config with `skipTokenRanges` filled in (DynamoDB uses `skipSegments`; Parquet uses file-level tracking via `enableParquetFileTracking`).
- To resume, start a new run using the latest savepoint as the config.
- The path can be local, `s3a://` or `gs://`.
- MySQL has **no** savepoints; an interrupted run restarts from scratch.
- Sources: https://www.scylladb.com/2019/03/12/deep-dive-into-the-scylla-spark-migrator/ , https://github.com/scylladb/scylla-migrator/blob/master/config.yaml.example , https://migrator.docs.scylladb.com/stable/resume-interrupted-migration.html

**Validator**
- It joins source and target by primary key in Spark and compares columns.
- Keys and defaults (https://github.com/scylladb/scylla-migrator/blob/master/config.yaml.example):
  - `compareTimestamps` (writetime + TTL).
  - `ttlToleranceMillis`: 60000.
  - `writetimeToleranceMillis`: 1000.
  - `failuresToFetch`: 100.
  - `floatingPointTolerance`: 0.001.
  - `timestampMsTolerance`: 0.
  - `copyMissingRows`: false. This copies only missing rows and never overwrites mismatches.
  - `repairWritetimeStrategy`: `source`, `coordinator` or `config`.
  - `hashColumns`: MySQL only.
  - `numericTypePolicy`: `Lenient`, `DetectWiden` or `StrictType`.
- Validating while the tables are taking live writes gives inconsistent results. https://migrator.docs.scylladb.com/stable/configuration.html
- Exit code on differences: not documented (verify).

**DynamoDB streams**
- `streamChanges: true` takes a snapshot and then replicates changes continuously, until stopped with Ctrl-C. `skipInitialSnapshotTransfer` skips the snapshot.
- The snapshot must finish within the stream's **24 h** retention, or changes are lost.
- The stream has to be cleaned up by hand afterwards.
- https://migrator.docs.scylladb.com/stable/stream-changes.html

**Failure modes**
- `copyMissingRows` cannot tell "never migrated" apart from "deleted on target", so it can resurrect deletes.
- With `repairWritetimeStrategy: source`, repair writes can be silently shadowed by target tombstones.
- Both from https://github.com/scylladb/scylla-migrator/blob/master/config.yaml.example
- Savepoint errors with S3 export sources. https://github.com/scylladb/scylla-migrator/issues/247

---

## 5. sstableloader, nodetool import/refresh, Scylla load-and-stream

**Cassandra sstableloader**
- It reads a `keyspace/table` directory, gets the ring from the `-d` hosts, and streams to each replica only the parts it owns.
- Options: `--throttle-mib`, `--inter-dc-throttle-mib`, `--entire-sstable-throttle-mib`, `-cph`, `-i` (ignore nodes), `-k` (target keyspace), `-f` (cassandra.yaml), plus SSL flags.
- Source: https://cassandra.apache.org/doc/latest/cassandra/managing/operating/bulk_loading.html
- Progress output can be turned off with `--no-progress` or made detailed with `--verbose`. https://cassandra.apache.org/doc/latest/cassandra/managing/tools/sstable/sstableloader.html
- Snapshots from an older major version must be upgraded with `upgradesstables` before loading (from older DataStax docs; verify for 5.0).

**nodetool import (4.0+)**
- It replaced `nodetool refresh`, which was deprecated in 4.0. https://github.com/apache/cassandra/blob/cassandra-4.0/NEWS.txt
- It is **local only**: the files must already be on that node's filesystem.
- Options: `-cd/--copy-data`, `-t/--no-tokens` (skip the owned-token check), `-l`, `-r`, `-v`, `-e/--extended-verify`, `-q`, `-ri`, `-niv`. https://cassandra.apache.org/doc/latest/cassandra/managing/tools/nodetool/import.html

**SSTable formats**
- `nb` = 4.0; `oa` = 5.0 BIG; `da` = 5.0 BTI (trie-indexed). BTI is enabled with `sstable: selected_format: bti`. https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html
- 5.0 defaults to `storage_compatibility_mode: CASSANDRA_4`, which keeps writing **nb** (so `upgradesstables` does not produce oa). BTI refuses to start in that mode. https://www.mail-archive.com/user@cassandra.apache.org/msg63981.html
- Streaming new-format sstables to a CASSANDRA_4-mode node is refused. https://issues.apache.org/jira/browse/CASSANDRA-19012
- BTI range-query bug in 5.0.5 and earlier; use 5.0.6+. https://digitalis.io/post/how-to-upgrade-cassandra-from-4-x-to-5-x-without-downtime-3-proven-steps

**ScyllaDB**
- `nodetool refresh <ks> <tbl> --load-and-stream` (`-las`):
  - Takes sstables from any node or topology, placed in `.../<table>-UUID/upload`.
  - Streams each partition to its owners, with no cleanup needed afterwards.
  - Do not load MV or secondary-index sstables; they are rebuilt automatically.
  - https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/refresh.html
- Scylla's own sstableloader works over CQL and is **deprecated** in favour of load-and-stream. It does not support encrypted files. https://docs.scylladb.com/manual/stable/operating-scylla/admin-tools/sstableloader.html
- Formats Scylla writes: `me`/`mt`. `ms`/`mt` are hybrids of `me` with the Cassandra `da` trie index; the trie index is the default since 2026.2. https://docs.scylladb.com/manual/stable/architecture/sstable/ , https://www.scylladb.com/2026/06/30/trie-index-3x-more-throughput/
- Documented as supported: Cassandra 3.x sstables and Apache Cassandra only (not DSE). https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cassandra-to-scylla-migration-process.html
- **Cassandra nb/oa support is not documented.** There is an open request (#8583), a crash loading Cassandra 4.1 sstables (#11267), and a load-and-stream failure with tablets (#22707).
  - https://github.com/scylladb/scylladb/issues/8583
  - https://github.com/scylladb/scylladb/issues/11267
  - https://github.com/scylladb/scylladb/issues/22707
- **Practical conclusion:** for Cassandra 4/5 → Scylla, use CQL-level tools (scylla-migrator, DSBulk). (inference)

---

## 6. CDC

**Cassandra commitlog CDC**
- Tables opt in with `cdc=true`.
- Commitlog segments are hard-linked into `cdc_raw` (default `/var/lib/cassandra/cdc_raw`). Each has a `_cdc.idx` file holding the persisted offset plus a `COMPLETED` marker. The consumer must parse up to that offset and then **delete** the files.
- Parsing uses `CommitLogReader` with a `CommitLogReadHandler`.
- Source: https://cassandra.apache.org/doc/latest/cassandra/managing/operating/cdc.html
- Settings: `cdc_enabled` false; `cdc_total_space` = min(4096 MiB, 1/8 of the drive); `cdc_block_writes` true; `cdc_on_repair_enabled` true; `cdc_free_space_check_interval` 250 ms. https://cassandra.apache.org/doc/latest/cassandra/managing/configuration/cass_yaml_file.html
- Failure mode: with no consumer running, the space fills up and **writes to CDC tables fail with WriteTimeoutException**. https://cassandra.apache.org/doc/latest/cassandra/managing/configuration/cass_yaml_file.html
- Because every replica writes its own commitlog, events arrive RF times (inference).

**Consumers for Cassandra CDC**
- DataStax CDC for Apache Cassandra:
  - A JVM agent on every node sends events to Pulsar. The Pulsar source connector de-duplicates with a cache and **reads the full row back over CQL**.
  - Supports Cassandra 3.11/4.0/4.1 and DSE 6.8.16+.
  - Does not provide before-images, range deletes, or backfill of existing data.
  - Latest v2.3.10 (2026-08-03); Apache-2.0.
  - https://github.com/datastax/cdc-apache-cassandra , https://github.com/datastax/cdc-apache-cassandra/tags
- Debezium Cassandra connector:
  - **Incubating**. Supports Cassandra 3.x/4.x/5.x and DSE.
  - A standalone JVM on each node reads `cdc_raw` and produces to Kafka.
  - It keeps its position as commitlog file + offset.
  - It does not de-duplicate across replicas, events can arrive out of order, and there are no before-images.
  - Not supported: range deletes, static columns, TTL on collections, MVs/secondary indexes, LWT.
  - https://debezium.io/documentation/reference/stable/connectors/cassandra.html

**ScyllaDB CDC**
- Enabled with `WITH cdc = {'enabled':true, 'preimage':false|true|'full', 'postimage':false, 'delta':'full'|'keys', 'ttl':86400}`. https://docs.scylladb.com/manual/stable/features/cdc/cdc-intro.html
- The log table `<table>_scylla_cdc_log` has:
  - Partition key `cdc$stream_id`.
  - Clustering keys `cdc$time` (timeuuid) and `cdc$batch_seq_no`.
  - `cdc$operation`, `cdc$ttl`, and `cdc$deleted_<col>` columns.
  - Truncating the base table does not truncate the log table.
  - https://docs.scylladb.com/manual/stable/features/cdc/cdc-log-table.html
- Log entries go to the same replica set as the base write. https://docs.scylladb.com/manual/stable/features/cdc/cdc-intro.html
- Vnode keyspaces use **generations**, stored in `system_distributed.cdc_generation_timestamps` and `system_distributed.cdc_streams_descriptions_v2`.
  - A new generation starts on node join or `checkAndRepairCdcStreams`.
  - Before switching generations, wait until every node's clock has passed the new generation's timestamp.
  - https://docs.scylladb.com/manual/stable/features/cdc/cdc-stream-changes.html , https://docs.scylladb.com/manual/stable/features/cdc/cdc-querying-streams.html
- **Tablets keyspaces** use per-table stream sets in `system.cdc_timestamps` and `system.cdc_streams`, with `stream_state` 0 = current, 1 = closed, 2 = opened. Stream sets change on tablet split/merge. https://docs.scylladb.com/manual/stable/features/cdc/cdc-stream-changes.html
- Ordering is guaranteed within a stream only. Read by querying each stream separately rather than scanning the table. https://docs.scylladb.com/manual/stable/features/cdc/cdc-streams.html , https://docs.scylladb.com/manual/stable/features/cdc/cdc-querying-streams.html

**ScyllaDB CDC client libraries**
- scylla-cdc-go v1.2.1 (2026-03-06), Apache-2.0, supports tablets. https://pkg.go.dev/github.com/scylladb/scylla-cdc-go
  - Polls each stream over `cdc$time` windows of `QueryTimeWindowSize`, only reading changes older than `ConfidenceWindowSize`.
  - Generation polling runs every 15 s, backing off to 5 min.
  - `TableBackedProgressManager` stores `last_timestamp` per (generation, app, table, stream_id) with a 7-day TTL.
- scylla-cdc-java 1.1.0, Apache-2.0. Progress is kept through the `CDCStateStore` interface, which is **in-memory by default**. The Debezium-compatible Scylla CDC Source Connector is built on it. https://github.com/scylladb/scylla-cdc-java
- **No official scylla-cdc-python exists**: github.com/scylladb/scylla-cdc-python returns 404. A Python tool would have to implement the generation/window reading itself, or shell out to Go/Java.

---

## 7. Python-side building blocks

**Drivers**
- `cassandra-driver`:
  - Now apache/cassandra-python-driver, Apache-2.0.
  - PyPI 3.30.1, Python ≥3.10 (the project says 3.10–3.14).
  - Supports Cassandra 2.1+ and DSE 4.7+.
  - Sources: https://github.com/apache/cassandra-python-driver , https://pypi.org/pypi/cassandra-driver/json
- `scylla-driver`:
  - Fork of the above: PyPI 3.29.12, Python 3.10–3.15, shard-aware and tablet-aware.
  - **It uses the same `cassandra` import namespace, so the two drivers cannot be installed together.**
  - arm64 wheels were not found in the PyPI data (verify).
  - Sources: https://github.com/scylladb/python-driver , https://pypi.org/pypi/scylla-driver/json
- Concurrency: `execute_concurrent(session, stmts_and_params, concurrency=100, raise_on_first_error=True, results_generator=False)` returns `ExecutionResult(success, result_or_exc)` in input order. https://docs.datastax.com/en/developer/python-driver/3.29/api/cassandra/concurrent/index.html

**Ring introspection**
- `cluster.metadata.token_map` exposes `ring`, `token_to_host_owner`, `tokens_to_hosts_by_ks` and `get_replicas(ks, token)`. Token class is `Murmur3Token`. `TableMetadata.export_as_string()` dumps table DDL. https://docs.datastax.com/en/developer/python-driver/3.29/api/cassandra/metadata/index.html
- Murmur3: `MINIMUM = Long.MIN_VALUE`, and `normalize()` maps a MIN hash to MAX. So no key ever has token −2^63, and scanning `(−2^63, 2^63−1]` covers every row. https://github.com/apache/cassandra/blob/cassandra-5.0/src/java/org/apache/cassandra/dht/Murmur3Partitioner.java

**Split planning**
- `system.size_estimates` (primary ranges only) is deprecated in favour of `system.table_estimates`. Its `range_type` is `primary` or `local_primary`, with `mean_partition_size` and `partitions_count`. https://github.com/apache/cassandra/blob/cassandra-4.0/src/java/org/apache/cassandra/db/SystemKeyspace.java
- These tables are node-local and not replicated, so you must query every node. https://issues.apache.org/jira/browse/CASSANDRA-15637

**WRITETIME/TTL**
- Cassandra 5.0 lets `WRITETIME`/`MAXWRITETIME`/`TTL` be applied to non-frozen collections and UDTs, returning a list, and supports element selectors. https://issues.apache.org/jira/browse/CASSANDRA-8877 , https://cassandra.apache.org/doc/latest/cassandra/developing/cql/dml.html
- ScyllaDB rejects them on a whole non-frozen map/set; you must use element access such as `WRITETIME(m[key])` or `WRITETIME(udt.field)`. https://docs.scylladb.com/manual/stable/cql/dml/select.html
- ScyllaDB also has `BYPASS CACHE` and `USING TIMEOUT`, both useful for full scans. https://docs.scylladb.com/manual/stable/cql/dml/select.html

**Write semantics**
- Without `USING TIMESTAMP`, the coordinator's time in microseconds is used. https://cassandra.apache.org/doc/latest/cassandra/developing/cql/dml.html
- Within a batch, operations without their own timestamp all share one. https://cassandra.apache.org/doc/latest/cassandra/developing/cql/dml.html
- Counters reject both USING TIMESTAMP and TTL, so the only way to copy one is `c = c + delta`. https://cassandra.apache.org/doc/latest/cassandra/developing/cql/counter-column.html

---

## 8. Verification models to borrow

- **Cassandra repair:**
  - Builds Merkle trees (a hierarchy of hashes) per token range on each replica, then streams only the ranges that differ.
  - `--validate` compares repaired data across replicas **without streaming**.
  - `--preview` estimates how much would be streamed.
  - `-pr` limits it to primary ranges.
  - https://cassandra.apache.org/doc/latest/cassandra/managing/operating/repair.html
- **ScyllaDB row-level repair:**
  - Hashes each row and uses set reconciliation, reading data only once.
  - In the 99.9 %-in-sync benchmark it was 6.78× faster and moved 4.28 GiB instead of 120.52 GiB.
  - Tablets use `nodetool cluster repair`, and incremental/automatic repair exist.
  - https://www.scylladb.com/2019/08/13/scylla-open-source-3-1-efficiently-maintaining-consistency-with-row-level-repair/ , https://docs.scylladb.com/manual/stable/operating-scylla/procedures/maintenance/repair.html
- **Suggested approach for a Python verifier** (inference):
  - Split the ring into N token sub-ranges.
  - Hash each sub-range on both sides as an ordered fold of (pk, values, optional writetime).
  - Compare the hashes and recursively bisect only the ranges that differ, down to row level.
  - Apply scylla-migrator-style tolerances (floats, writetime ±1 s, TTL ±60 s).
  - Never auto-copy missing rows during dual writes. Both CDM and scylla-migrator warn this resurrects deletes.

---

**Gaps to check before relying on them:**
- CDM's final summary line format and its exit codes.
- scylla-migrator's Java baseline for 2.x, and its exit code on validation failure.
- Official Docker image and arm64 availability for DSBulk and CDM.
- Whether ScyllaDB accepts Cassandra `nb`/`oa` sstables through load-and-stream (test on a sandbox node).
