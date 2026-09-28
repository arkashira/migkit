# ClickHouse and Elasticsearch/OpenSearch migration tools: research report (as of 2026-09-27)

Every fact below is followed by its source. Items marked **UNVERIFIED** are ones I could not confirm with a public source; don't rely on them. The WebSearch budget ran out partway through, so the rest was checked by fetching the primary pages directly.

---

## ClickHouse

### 1. clickhouse-backup (Altinity)
- **License: MIT.** Copyright © 2018-2019 Alexander Akulov and © 2020-2099 Altinity Inc. https://github.com/Altinity/clickhouse-backup/blob/master/LICENSE
- **Version and status:** actively maintained. Latest is v2.8.1 (17 Sep 2026); v2.8.0 was 16 Jul 2026. Altinity's docs-site release notes are stale (they stop at 2.6.33, Aug 2025), so use GitHub. https://github.com/Altinity/clickhouse-backup/releases · https://docs.altinity.com/releasenotes/altinity-backup-release-notes/
- **Distribution:**
  - It is a Go binary, not a PyPI package.
  - Release tarballs: `clickhouse-backup-{linux,darwin}-{amd64,arm64}.tar.gz`, plus `-fips` variants and RPMs (x86_64, aarch64). macOS arm64 and linux arm64 are both included.
  - Docker image: `altinity/clickhouse-backup`.
  - https://github.com/Altinity/clickhouse-backup/releases
- **Algorithm:**
  - `create` runs `ALTER TABLE ... FREEZE`, which hardlinks the existing parts into `shadow/`. This makes a snapshot without copying data.
  - `restore` copies parts into `detached/` and then runs `ALTER TABLE ... ATTACH PART` for each part.
  - It needs local filesystem access to the data, so it must run on the same host or pod as clickhouse-server. Over a remote connection it can only do schema operations.
  - Mainly supports the MergeTree family.
  - https://github.com/Altinity/clickhouse-backup/blob/master/ReadMe.md
- **CLI:**
  - Commands: `create`, `upload`, `download`, `restore`, `create_remote` (create + upload), `restore_remote` (download + restore), `watch`, `server`, `list`, `delete`, `clean`.
  - Flags: `--diff-from`, `--diff-from-remote`, `--partitions`, `--table`, `--rbac`, `--configs`, `--schema`, `--resume`.
  - Incremental backups form a "required backup" chain. `delete` respects the chain unless you pass `--force` or set `rebase_during_delete: true`.
  - https://github.com/Altinity/clickhouse-backup · https://github.com/Altinity/clickhouse-backup/blob/master/ReadMe.md
- **Storage backends:** S3, GCS, Azure Blob, Tencent COS, FTP, SFTP, and a "custom" method (rclone/kopia/restic). Object-storage disks are copied server-side with CopyObject where the backend supports it. https://github.com/Altinity/clickhouse-backup
- **Embedded mode:**
  - `use_embedded_backup_restore: true` switches to native `BACKUP`/`RESTORE` SQL (ClickHouse 23.2+).
  - Related settings: `embedded_backup_disk`, `use_embedded_backup_restore_cluster` (runs `ON CLUSTER`).
  - Throttling is handed to the server via `max_backup_bandwidth` (ClickHouse 25.1+).
  - https://github.com/Altinity/clickhouse-backup/blob/master/ReadMe.md
- **REST API (`server` mode):**
  - Listens on `api.listen`, default port 7171.
  - Endpoints: `POST /backup/{create,create_remote,upload,download,restore,restore_remote,kill,watch,clean}`, `POST /restart`, `GET /backup/{status,actions,list,tables}`, `GET /metrics` (Prometheus).
  - Async operations return an `operation_id`. Poll `/backup/status`, or pass `?callback=` / set `callback_url` to get a POST when the operation finishes.
  - https://github.com/Altinity/clickhouse-backup/blob/master/ReadMe.md
- **Failure modes:**
  - If the process is killed with SIGKILL, frozen `shadow/` directories are left behind; remove them with `clean`.
  - `max_broken_part_ratio=0` (the default) aborts the backup on any broken part.
  - `.resumable` state files are not supported with the `custom` storage method.
  - https://github.com/Altinity/clickhouse-backup/blob/master/ReadMe.md
- **Fixed in 2.8.x:**
  - `restore --rbac` used to silently drop RBAC objects whose storage type (local vs Keeper) didn't exist on the target, while still reporting success (#881).
  - Missing remote objects now fail fast instead of burning the whole retry budget; new `--allow-missing-files` salvage mode.
  - New `--drop-replica-if-exists`.
  - `rebind_replica_path_if_exists` must stay false during a concurrent multi-replica restore, or you risk split-brain.
  - https://github.com/Altinity/clickhouse-backup/releases
- **Open issues:**
  - #1569: `upload --resume` writes an incomplete manifest.
  - #1454: files within a part are transferred sequentially.
  - #1235: Kafka/*Queue engines and refreshable MVs are left as "postponedTables" after restore.
  - https://github.com/Altinity/clickhouse-backup/issues?q=is%3Aissue+is%3Aopen+restore

### 2. clickhouse-copier
- **Removed from the ClickHouse bundle in 24.2 (PR #61058).** It moved to https://github.com/ClickHouse/copier. https://clickhouse.com/docs/whats-new/changelog/24.2-fast-release
- Removal was proposed in #60734 because it is "hard to use, and it is hardly supported." https://github.com/ClickHouse/ClickHouse/issues/60734
- The repo is labelled "obsolete". Its last release is tagged "final", and the README says the tool is no longer supported. https://github.com/ClickHouse/copier
- **What it did:**
  - Tasks were stored in ZooKeeper/Keeper. Many copier workers coordinated there; each picked the "closest" source shard, with concurrency capped by `max_workers`.
  - Supported `where_condition` filters and explicit partition lists, and could re-shard.
  - Read each partition, inserted into temp tables on the target, then ran `ATTACH PARTITION`.
  - It drops and refills destination partitions it considers incomplete.
  - https://github.com/ClickHouse/copier · https://github.com/ClickHouse/ClickHouse/issues/60734
  - Splitting partitions into pieces via `number_of_splits`: **UNVERIFIED**.

### 3. Server-to-server copy with `remote()` / `remoteSecure()`
- **Syntax:** `remote(addresses_expr, db, table, user, password, sharding_key)`.
  - Default ports: 9000 for `remote`, 9440 for `remoteSecure`.
  - Address globs: `{a,b}`, `{N..M}`, `{a|b}` (replicas).
  - A new connection is opened for every query, so it suits one-off migrations.
  - https://clickhouse.com/docs/sql-reference/table-functions/remote
- **Pull or push:** pull with `INSERT INTO t SELECT * FROM remoteSecure(...)`. Push with `INSERT INTO FUNCTION remoteSecure(...)` when the target isn't reachable from the source side. Use a `readonly=1` user on the source. https://clickhouse.com/docs/cloud/migration/clickhouse-to-cloud
- **No automatic retry if the connection drops.** A single copy is fine up to "hundreds of GB." Beyond that, slice by a WHERE condition on the partition key.
  - If ping is above 50 ms, raise `connect_timeout_with_failover_ms` (default 50) and `connect_timeout_with_failover_secure_ms` (default 100).
  - https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-data-migration/remote-table-function/
- **Throughput settings:**
  - `max_insert_threads` only helps if the SELECT side is parallel (`max_threads`).
  - Peak memory ≈ `max_insert_threads × min_insert_block_size_{rows|bytes}`.
  - Altinity's per-partition script uses `max_insert_threads=20`, `max_threads=20`, `min_insert_block_size_bytes=536870912`, `min_insert_block_size_rows=16777216`, `max_insert_block_size=16777216`, `optimize_on_insert=0`.
  - The script lists partitions from `system.parts`, skips ones already on the target, and verifies via `system.parts`. That is the resume pattern.
  - https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-data-migration/remote-table-function/
- **`parallel_distributed_insert_select`:** default **2**.
  - 0 = off; 1 = the SELECT runs on each shard; 2 = both SELECT and INSERT run shard-locally.
  - Applies to `INSERT ... SELECT` between Distributed tables on the same cluster.
  - https://clickhouse.com/docs/reference/settings/session-settings/parallel
- **`max_partitions_per_insert_block`:** default 100; exceeding it throws "Too many partitions for single INSERT block." Copy one partition at a time and insert in partition-key order. https://github.com/ClickHouse/ClickHouse/issues/24006 · https://medium.com/@jgrodman/clickhouse-optimizing-bulk-inserts-for-partitioned-tables-9ea91b3e7c3b
- **`_partition_id`:** the virtual column filters one partition (e.g. `WHERE _partition_id='20190225'`). Whether it is visible *through* `remote()` is **UNVERIFIED**; it is safe in push mode, where the table is local. https://chistadata.com/parts-and-partitions-in-clickhouse-part-i/
- **Deduplication:**
  - `insert_deduplicate` (default 1) applies to Replicated* tables.
  - Plain MergeTree needs `non_replicated_deduplication_window` > 0 (default 0).
  - `insert_deduplication_token` replaces the content hash with a token you supply.
  - https://clickhouse.com/docs/reference/settings/session-settings/insert · https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries
- **INSERT…SELECT retries are deduplicated only if the SELECT is "stable":** literally `ORDER BY ALL` plus a single read stream.
  - Otherwise `deduplicate_insert_select=enable_when_possible` skips dedup with a warning; `force_enable` throws instead.
  - With a token set, a retry after the source changed is silently dropped.
  - https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries
- **`cluster()` vs `clusterAllReplicas()`:** `cluster()` queries one replica per shard; `clusterAllReplicas()` queries every replica. Both need the cluster defined in `remote_servers`. https://clickhouse.com/docs/sql-reference/table-functions/cluster
- **Network compression:** `network_compression_method` (NONE/LZ4/LZ4HC/ZSTD), shown as default **ZSTD**; `network_zstd_compression_level` shown as default 3 (range 1–15). Older releases defaulted to LZ4 (**UNVERIFIED**). https://clickhouse.com/docs/reference/settings/session-settings/network
- **Other migration options (Altinity comparison):**
  - FETCH PARTITION needs the same ZooKeeper (no chroot) and identical schema.
  - freeze + rsync + attach uses little CPU but needs identical schema.
  - Adding a replica needs ZK port 2181 and the replication ports.
  - https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-data-migration/

### 4. clickhouse-local, plus Python-embeddable options
- Runs SQL without a server: `clickhouse local -q ... --structure --input-format --file --output-format`. `--copy` converts between formats (e.g. `< data.json > data.csv`). https://clickhouse.com/docs/operations/utilities/clickhouse-local
- Supports `file()`, `s3()`, `url()`, `remote()`, `mysql()`, so it can act as a server-less mover or converter. https://clickhouse.com/docs/operations/utilities/clickhouse-local
- Install with `curl https://clickhouse.com/ | sh` (single binary). Runs on Linux, macOS including arm64, and Windows via WSL2. Not meant for serving applications. https://clickhouse.com/docs/operations/utilities/clickhouse-local
- **chDB** (PyPI `chdb`): Apache-2.0, in-process ClickHouse for Python 3.9+, macOS/Linux x86_64 and arm64. https://github.com/chdb-io/chdb
- **clickhouse-connect** (PyPI): Apache-2.0, HTTP interface, Python ≥3.10, Arrow/pandas/polars support, experimental chDB backend. https://github.com/ClickHouse/clickhouse-connect

### 5. BACKUP / RESTORE SQL
- **Syntax:** `BACKUP|RESTORE TABLE|DATABASE|ALL ... TO|FROM Disk()|S3()|File()|AzureBlobStorage() [SETTINGS ...] [ASYNC]`.
  - Settings: `base_backup` (incremental), `structure_only`, `password` (zip only), `allow_non_empty_tables`, `compression_method`/`compression_level`.
  - Concurrency is controlled by `allow_concurrent_backups` / `allow_concurrent_restores`.
  - https://clickhouse.com/docs/operations/backup
- **Incremental:** `SETTINGS base_backup = S3(...)`. To restore, name only the incremental backup; the base is pulled in automatically. https://clickhouse.com/docs/operations/backup/s3_endpoint
- **Progress via `system.backups`:**
  - Columns: `id` (settable with `SETTINGS id=`), `name`, `base_backup_name`, `status`, `error`, `start_time`/`end_time`, `num_files`, `total_size`, `num_entries`, `uncompressed_size`, `compressed_size`, `files_read`, `bytes_read`, `ProfileEvents`.
  - Status values: CREATING_BACKUP, BACKUP_CREATED, BACKUP_FAILED, RESTORING, RESTORED, RESTORE_FAILED, BACKUP_CANCELLED, RESTORE_CANCELLED.
  - https://clickhouse.com/docs/operations/system-tables/backups
- **ClickHouse Cloud:** BACKUP/RESTORE works to your own S3, GCS (via `S3()`) or Azure bucket.
  - Each backup needs a unique path, otherwise you get `BACKUP_ALREADY_EXISTS`.
  - Cloud never deletes these backups for you.
  - `restore_access_entities_with_current_grants` needs Cloud 26.4+.
  - https://clickhouse.com/docs/cloud/manage/backups/backup-restore-via-commands
- **Cross-version compatibility** (restoring an older or newer version's backup, OSS to Cloud): **not documented in the pages I fetched**.

### 6. Part-level integrity
- **`system.parts` hash columns:** `hash_of_all_files` (sipHash128 of the compressed files), `hash_of_uncompressed_files` (marks, index, etc.), and `uncompressed_hash_of_compressed_files` (hash of the data as if uncompressed). https://clickhouse.com/docs/operations/system-tables/parts
- **Part name format:** `<partition_id>_<min_block>_<max_block>_<level>_<data_version>`. https://clickhouse.com/docs/operations/system-tables/parts
- **Do identical parts have identical checksums? Only when the same bytes are produced or copied.**
  - ReplicatedMergeTree compares merge results with checksums stored in ZooKeeper. On a mismatch it logs "Data after merge is not byte-identical…" and fetches the part from another replica instead.
  - Listed causes include a newer compression library, a different compression method, and differing part-format settings.
  - https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/MergeTree/MergeFromLogEntryTask.cpp
  - My inference: comparing part checksums is only valid for physical copies (backup, FETCH, ATTACH). A logical `INSERT…SELECT` builds different parts, so compare logical content hashes per partition instead.
- **CHECK TABLE:**
  - Checks file sizes and checksums for MergeTree and Log engines. Scope with `PARTITION` or `PART`, or use `CHECK ALL TABLES`.
  - Returns `part_path`, `is_passed`, `message`.
  - Heavy on IO, and does not check cross-replica consistency.
  - https://clickhouse.com/docs/sql-reference/statements/check-table
- **Partition operations:**
  - `ATTACH PARTITION FROM` and `REPLACE PARTITION` need the same structure, partition key, ORDER BY, primary key and storage policy. They copy data and leave the source intact.
  - `MOVE PARTITION TO TABLE` additionally needs the same engine family (both replicated or both not), and it removes the data from the source.
  - `FETCH PARTITION|PART FROM '<zk path>'` downloads into `detached/` on the local server only; you then run ATTACH.
  - `FREEZE` hardlinks into `shadow/`; `UNFREEZE` removes the frozen copy.
  - https://clickhouse.com/docs/sql-reference/statements/alter/partition

### 7. PeerDB / ClickPipes Postgres
- **License has changed: the repo LICENSE is now AGPLv3.** At the July 2024 acquisition it was ELv2, including the Enterprise edition. https://github.com/PeerDB-io/peerdb/blob/main/LICENSE · https://blog.peerdb.io/clickhouse-acquires-peerdb-for-native-postgres-cdc-integration
- **Status:** active. Latest listed release is v0.37.10 (dated Sep 24; the page doesn't show the year).
  - Maintained sources: Postgres, MySQL, MongoDB, CockroachDB, BigQuery.
  - Maintained destinations: ClickHouse/ClickHouse Cloud and Postgres. Snowflake, BigQuery, Elasticsearch, Kafka, S3 and others are deprecated as destinations.
  - Runs on Temporal with flow workers and a Postgres catalog. Deploy with docker-compose or Helm.
  - https://github.com/PeerDB-io/peerdb · https://github.com/PeerDB-io/peerdb/releases
- **Initial load:** the table is split into CTID ranges read in parallel, using a held snapshot connection for consistency, then it switches to streaming from the logical replication slot. https://blog.peerdb.io/parallelized-initial-load-for-cdc-based-streaming-from-postgres
- **ClickPipes (ClickHouse Cloud only):** built on PeerDB.
  - Defaults: `--initial-load-parallelism` 4, `--snapshot-rows-per-partition` 100000, `--snapshot-parallel-tables` 1, with CTID partitioning.
  - Needs `wal_level=logical`.
  - https://clickhouse.com/docs/integrations/clickpipes/postgres

### 8. MaterializedPostgreSQL / MaterializedMySQL
- **MaterializedPostgreSQL is still experimental.**
  - Enable with `allow_experimental_database_materialized_postgresql=1`; not available on Cloud.
  - Takes a snapshot, then streams changes over pgoutput logical replication.
  - Needs `wal_level=logical`, `max_replication_slots≥2`, and a PK or unique-index replica identity.
  - DDL is not replicated, so schema changes break it. New tables need a manual `ATTACH TABLE`.
  - https://clickhouse.com/docs/engines/database-engines/materialized-postgresql
- **MaterializedMySQL has been removed.** PR #73879 (merged 28 Dec 2024): "The obsolete `MaterializedMySQL` database engine has been removed." Docs were removed in #70798 and the last code remnants in #84516 (Jul 2025). https://github.com/ClickHouse/ClickHouse/pull/73879 · https://github.com/ClickHouse/ClickHouse/issues?q=MaterializedMySQL+remove

### 9. Table functions for server-side pulls
- **`postgresql()`:**
  - Reads with `COPY (SELECT …) TO STDOUT` inside a read-only transaction, committed after each SELECT.
  - Only simple `= != > >= < <= IN` filters are pushed down; LIMIT, joins and aggregates run in ClickHouse.
  - Writes use `COPY … FROM STDIN`. Connection pool size 32.
  - Multi-dimension PostgreSQL arrays must have a consistent dimension.
  - https://clickhouse.com/docs/sql-reference/table-functions/postgresql
- **`mysql()`:**
  - Pushes down only simple comparisons; LIMIT is not pushed.
  - `query('SELECT…')` passes a query through, but is read-only.
  - `external_table_strict_query=1` rejects filters that can't be pushed down.
  - Write options: `replace_query` / `on_duplicate_clause`.
  - https://clickhouse.com/docs/sql-reference/table-functions/mysql
- **`mongodb()`:** needs an explicit `structure`. Accepts host or URI. `oid_columns` defaults to `_id`. https://clickhouse.com/docs/sql-reference/table-functions/mongodb
- **`s3()`:**
  - Globs: `* ** ? {a,b} {N..M}`.
  - Virtual columns: `_path`, `_file`, `_size`, `_time`.
  - `INSERT INTO FUNCTION s3 … PARTITION BY` with `wildcard` or `hive` strategy.
  - Inserts only create new files; format and compression are detected from the extension.
  - https://clickhouse.com/docs/sql-reference/table-functions/s3
- **`iceberg()`** (alias of `icebergS3`; also Azure/HDFS/Local variants):
  - Reads v1/v2, partial v3.
  - Writes since 25.7 behind `allow_insert_into_iceberg`.
  - Time travel via `iceberg_timestamp_ms` or `iceberg_snapshot_id`; equality deletes since 25.8.
  - https://clickhouse.com/docs/sql-reference/table-functions/iceberg
- **`deltaLake()`:** reads everywhere. Writes are a beta, off by default: S3/GCS since 25.10, Azure since 26.9. https://clickhouse.com/docs/sql-reference/table-functions/deltalake

---

## Elasticsearch / OpenSearch

### 10. elasticdump / multielasticdump
- **License:** Apache-2.0. https://github.com/elasticsearch-dump/elasticsearch-dump
- **Version and status:** maintained, mostly by one maintainer. Latest is v6.125.1 (May 2026).
  - OpenSearch supported since 6.76.0; `--searchAfter`/`--pit` since 6.117.0.
  - https://www.npmjs.com/package/elasticdump · https://github.com/elasticsearch-dump/elasticsearch-dump/releases
- **Distribution:** npm package (Node.js) or Docker image `elasticdump/elasticsearch-dump`. No native binaries. https://github.com/elasticsearch-dump/elasticsearch-dump
- **What it moves (`--type`):** index, settings, analyzer, data, mapping, policy, alias, template, component_template, index_template. https://github.com/elasticsearch-dump/elasticsearch-dump/blob/master/README.md
- **Defaults:** `limit 100`, `concurrency 1`, `concurrencyInterval 5000`, `intervalCap 5`, `retryAttempts 0`, `retryDelay 5000`, `timeout null`, `scrollTime 10m`, `scrollRetryDelay 15000`, `size -1`. https://github.com/elasticsearch-dump/elasticsearch-dump/blob/master/bin/elasticdump
- **Algorithm:** reads with scroll (or search_after/PIT) and writes with bulk. Since 6.1.0, records are processed out of order. Supports `--transform`, `--searchBody`, `--fileSize`/`--maxRows` splitting, and `--awsChain`. https://github.com/elasticsearch-dump/elasticsearch-dump/blob/master/README.md
- **S3:** read and write `s3://` URLs; 5.0.0 switched to s3urls (breaking change). **multielasticdump** forks n = number of CPUs; in dump mode the output must be a directory, with data, mapping and analyzer files per index. https://github.com/elasticsearch-dump/elasticsearch-dump
- **Failure modes:**
  - #741: randomly misses the last batch (exactly `--limit` docs) with `concurrency=2`. https://github.com/elasticsearch-dump/elasticsearch-dump/issues/741
  - #559: "scrollId not found" on a 50 GB index. https://github.com/taskrabbit/elasticsearch-dump/issues/559
  - #624: `--offset` can't resume because `from` isn't allowed in a scroll context. https://github.com/taskrabbit/elasticsearch-dump/issues/624
  - #472: Node heap is exhausted at `limit≥6000`. https://github.com/elasticsearch-dump/elasticsearch-dump/issues/472
  - `--ignore-errors` and `--timeout` are documented as acceptable-data-loss options.
  - Always compare the dumped count with the source `_count`.

### 11. OpenSearch Migration Assistant (opensearch-project/opensearch-migrations)
- **License and version:** Apache-2.0. Latest is v3.3.8 (28 Aug 2026); releases are frequent (3.3.6 and 3.3.7 in Aug 2026).
  - Artifacts: `migration-assistant-*.tgz` (Helm), bootstrap scripts, CloudFormation EKS templates, SBOMs. No PyPI package.
  - https://github.com/opensearch-project/opensearch-migrations/releases · https://github.com/opensearch-project/opensearch-migrations/blob/main/README.md
- **Deployment: Kubernetes is required** (EKS recommended, GKE via Terraform, or self-managed). Built on Argo Workflows and Strimzi Kafka. Local Docker is for development only. https://docs.opensearch.org/latest/migration-assistant/is-migration-assistant-right-for-you/ · https://docs.opensearch.org/latest/migration-assistant/architecture/
- **Supported paths:**
  - ES 1–2 → OpenSearch 1/2/3: backfill only.
  - ES 5–7 → OpenSearch 1/2/3.
  - ES 8 → OpenSearch 2/3.
  - OpenSearch 1–2 → OpenSearch 2/3.
  - Solr 6–9 → OpenSearch 3: backfill only.
  - https://docs.opensearch.org/latest/migration-assistant/is-migration-assistant-right-for-you/
- **Reindex-from-Snapshot (RFS) algorithm:**
  - Takes one shard per work item. Downloads the shard blobs from the snapshot, unpacks and de-obfuscates them into a local Lucene directory, and reads documents from the stored `_source`.
  - Bulk-indexes into the target and keeps original `_id`s, so a restarted shard overwrites its earlier partial attempt.
  - https://aws.amazon.com/blogs/big-data/accelerate-your-migration-to-amazon-opensearch-service-with-reindexing-from-snapshot · https://github.com/opensearch-project/opensearch-migrations/blob/main/DocumentsFromSnapshotMigration/README.md
- **Leasing:**
  - Work-item state lives in a special index on a coordinator cluster: the target by default for the CLI, or a dedicated single-node coordinator in the workflow.
  - `--initial-lease-duration` defaults to PT10M. Early checkpoint fires at `max(0.75·lease, lease−4m30s)`: the worker saves a progress cursor and creates `successor_items`.
  - Exit codes: 0 = done, 2 = lease handoff (recoverable), 3 = no work left.
  - Other defaults: `--max-shard-size-bytes` 80 GB, 10 MiB bulk size, `--max-connections` 10. Needs about 2× the shard size in local disk.
  - Because a shard can be processed twice after a handoff, pass `--allowed-doc-exception-types version_conflict_engine_exception`.
  - https://github.com/opensearch-project/opensearch-migrations/blob/main/DocumentsFromSnapshotMigration/README.md
- **Limits:**
  - Source needs the repository-s3 plugin. zstd / zstd_no_dict codecs are not supported.
  - Capture & Replay: auto-generated IDs are not preserved; limited to under 4 TB/day of traffic.
  - https://docs.opensearch.org/latest/migration-assistant/is-migration-assistant-right-for-you/
  - User-supplied snapshots need `include_global_state:true` and `compress:false`. https://repost.aws/articles/ARfWjsD2ZjRYGLgByh84qYzA/complete-guide-migration-assistant-tool-for-aws-opensearch
  - Indices with `_source` disabled: opt in with `enableSourcelessMigrations` for best-effort rebuild from stored fields, doc_values and terms, or use `useRecoverySource`. https://docs.aws.amazon.com/solutions/latest/migration-assistant-for-amazon-opensearch-service/backfill-tuning.html
  - Since 3.3.6: optional failed-document stream to S3 and a `CompletedWithErrors` status. https://github.com/opensearch-project/opensearch-migrations/releases
- **Performance:** about 590k docs/min per worker (2 vCPU / 4 GB). AWS demo: 5 TiB (3.9B docs) in about 35 min with 200 workers. https://github.com/opensearch-project/opensearch-migrations/blob/main/README.md · https://aws.amazon.com/blogs/big-data/accelerate-your-migration-to-amazon-opensearch-service-with-reindexing-from-snapshot
- **Metadata migration:**
  - Migrates index settings, mappings, legacy and composable templates, component templates, and aliases.
  - Does **not** migrate security config, ILM/ISM policies, ingest pipelines, Kibana/Dashboards objects, data streams, or cluster settings.
  - Built-in transforms: multi-type → single type, `string` → text/keyword, `flattened` → `flat_object`, `dense_vector` → `knn_vector`, nmslib → faiss, analyzer fixes.
  - `evaluate` is a dry run; `migrate` applies. An existing target index is fatal unless `--allow-existing-indexes true`.
  - https://docs.aws.amazon.com/solutions/latest/migration-assistant-for-amazon-opensearch-service/migrate-metadata.html
- **Capture Proxy and Traffic Replayer:**
  - The proxy relays traffic to the source and copies raw request/response streams to Kafka.
  - The replayer rebuilds requests from protobuf TrafficStreams and applies transforms (JOLT, JMESPath, JS). It rewrites host and auth headers; reused SigV4 headers break if the content is reformatted.
  - It writes NDJSON tuples (source and target request/response, timings) to `tuples.log`. These contain auth headers.
  - https://github.com/opensearch-project/opensearch-migrations/blob/main/TrafficCapture/trafficReplayer/README.md · https://docs.opensearch.org/3.0/migration-assistant/migration-phases/live-traffic-migration/using-traffic-replayer/
- **CLI:**
  - Legacy commands: `console snapshot create`, `console metadata evaluate|migrate`, `console backfill start|status --deep-check`, `console clusters curl`.
  - Current model: `workflow configure edit / submit / manage / show / log`.
  - https://docs.opensearch.org/latest/migration-assistant/migration-console/migration-console-command-reference/ · https://docs.aws.amazon.com/solutions/latest/migration-assistant-for-amazon-opensearch-service/migrate-metadata.html

### 12. Reindex from remote
- **Allowlist setting:** Elasticsearch uses `reindex.remote.whitelist`. https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reindex-indices
- OpenSearch uses `reindex.remote.allowlist` (error text: "not allowlisted in reindex.remote.allowlist"). SSL for the remote must be set in `opensearch.yml` and needs a restart. https://docs.opensearch.org/latest/api-reference/document-apis/reindex/ · https://forum.opensearch.org/t/not-able-reindex-remotely/18583
- **Limits:**
  - 100 MB on-heap buffer, so shrink batch size for large docs.
  - No manual or automatic slicing for remote reindex.
  - `socket_timeout` and `connect_timeout` default to 30s.
  - No forward compatibility across majors (e.g. 7.x → 6.x fails).
  - https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reindex-indices
- **Controls:**
  - `version_type: external` keeps source versions; `op_type: create`; `conflicts: proceed`.
  - `routing`: `keep`, `discard`, or `=value`.
  - `wait_for_completion=false` returns a task ID for polling; `requests_per_second` throttles.
  - Requires `_source`.
  - https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reindex-indices · https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/mapping-source-field
- **Amazon OpenSearch Service:** remote ES 1.5+ into a 6.7+ domain; VPC endpoint mode needs OpenSearch 1.3+. https://aws.amazon.com/about-aws/whats-new/2023/01/amazon-opensearch-service-remote-reindex-vpc-domains

### 13. Snapshot / restore across versions
- **Elasticsearch rules:**
  - What decides compatibility is the version that *created* each index, not the version that took the snapshot.
  - You can never restore to an older version.
  - Previous-major indices are fully supported. Older ones (back to 5.x) only as read-only "archive indices" or searchable snapshots, with limited querying.
  - 9.x allows archive and searchable indices from N-2.
  - https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore · https://www.elastic.co/docs/deploy-manage/upgrade/deployment-or-cluster/reading-indices-from-older-elasticsearch-versions · https://github.com/elastic/elasticsearch/pull/118941
- **OpenSearch restoring Elasticsearch snapshots:**
  - Compatible with indices created in ES 6.0–7.10. https://opensearch.org/faq/
  - Snapshots from ES 7.12+ fail (`unknown field [uuid]`, repository `min_version 7.12.0`). https://github.com/opensearch-project/OpenSearch/issues/17567
  - Amazon OpenSearch Service rejects snapshots from ES 7.11+. https://docs.aws.amazon.com/opensearch-service/latest/developerguide/snapshot-based-migration.html
  - OpenSearch 2.x rejects 6.x-created indices; OpenSearch 3.0 rejects indices created before 2.x. https://github.com/opensearch-project/OpenSearch/issues/18717
- S3-compatible storage (e.g. MinIO) must be *fully* S3-compatible. https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore/s3-repository
- Searchable snapshots and archive indices need an Enterprise license: **UNVERIFIED**.

### 14. Cross-cluster replication
- **OpenSearch CCR plugin:** Apache-2.0 (opensearch-project repo). https://github.com/opensearch-project/cross-cluster-replication
  - Plugin must be installed on both clusters.
  - Security plugin must be enabled on both or disabled on both.
  - Follower nodes need the `remote_cluster_client` role.
  - Metadata is stored in `.replication-metadata-store`.
  - Built-in roles: `cross_cluster_replication_{leader,follower}_full_access`.
  - https://docs.opensearch.org/latest/tuning-your-cluster/replication-plugin/getting-started/ · https://docs.opensearch.org/latest/tuning-your-cluster/replication-plugin/permissions/
  - Follower version must be ≥ leader version. https://www.instaclustr.com/support/documentation/opensearch/getting-started-with-opensearch/creating-an-opensearch-cluster-with-cross-cluster-replication/
  - On AWS: same major, or last minor → next major; ES 7.10 / OpenSearch 1.1+; cannot replicate between AOS and self-managed clusters. https://docs.aws.amazon.com/opensearch-service/latest/developerguide/replication.html
- **Elasticsearch CCR:**
  - Platinum feature, needed on both clusters (Enterprise includes it). https://www.elastic.co/docs/deploy-manage/tools/cross-cluster-replication
  - Self-managed Platinum is closed to new customers. https://www.elastic.co/subscriptions
  - Needs soft deletes (default for indices created in 7.0+). Retention lease is 12h; a follower that falls further behind gets a fatal exception and must be recreated.
  - Follower must be the same or newer version, including patch level.
  - https://www.elastic.co/docs/deploy-manage/tools/cross-cluster-replication

### 15. Logstash
- **AGPLv3 was added only to Elasticsearch and Kibana** (announced 29 Aug 2024, landing by 8.16). The FAQ says "no other products will be impacted"; Logstash, Beats and the client libraries stay Apache-2.0. Binaries remain under the Elastic License. https://www.elastic.co/pricing/faq/licensing · https://www.businesswire.com/news/home/20240829537786/en/Elastic-Announces-Open-Source-License-for-Elasticsearch-and-Kibana-Source-Code
- **The Logstash repo itself is mixed:** Apache-2.0 outside `x-pack/`, Elastic License inside it; `-oss` artifacts are Apache-2.0. https://github.com/elastic/logstash/blob/main/LICENSE.txt
- **Elasticsearch input plugin:**
  - `search_api: auto` picks `search_after`+PIT for ES 8.0+, otherwise scroll. search_after needs at least one sort field.
  - `slices`: recommended 2–8; more slices than shards hurts.
  - `docinfo` carries `_index`, `_id`, `_routing`.
  - Defaults: `size` 1000, `scroll` 1m, `retries` 0. A partial failure retries the whole query, which can duplicate data.
  - https://www.elastic.co/docs/reference/logstash/plugins/plugins-inputs-elasticsearch

### 16. esrally
- **Version and license:** 2.13.0 (27 Mar 2026), Apache-2.0, Python 3.10–3.13, Linux/macOS only. https://pypi.org/project/esrally/
- **`esrally create-track`:**
  - Args: `--track`, `--target-hosts`, `--indices` or `--data-streams`, `--output-path`, `--client-options`.
  - Produces `track.json`, per-index mapping files, full and `-1k` document corpora (plus bz2), and example operations and challenges.
  - The tutorial assumes ES 7.0+.
  - https://esrally.readthedocs.io/en/stable/adding_tracks.html
- For OpenSearch targets the equivalent is the opensearch-benchmark fork; I could not fetch its details (**UNVERIFIED**).

### 17. Techniques and Python libraries
- **Pagination:**
  - Use PIT + `search_after` instead of scroll for deep paging; the `_shard_doc` tiebreaker is implicit; `keep_alive` is extended on every request.
  - `from`+`size` is capped at `index.max_result_window` (10,000).
  - `search.max_open_scroll_context` defaults to 500.
  - Slicing beyond the shard count is costly; for append-only time-based indices, slice on a timestamp field.
  - https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results
- **Versioning:**
  - `external`: index only if the version is strictly greater. `external_gte`: greater or equal, and documented as "use with care" (it can lose data). Maximum version is about 9.2e18.
  - https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-index
  - `_seq_no`/`_primary_term` uniquely identify a change; `if_seq_no`/`if_primary_term` give optimistic concurrency control. https://www.elastic.co/docs/reference/elasticsearch/rest-apis/optimistic-concurrency-control
- **`_source` disabled** means no reindex, update or update_by_query. Synthetic source (`index.mapping.source.mode: synthetic`) needs a subscription and reorders arrays and fields. https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/mapping-source-field
- **Counting:** `_cat/indices` `docs.count` includes hidden nested docs. Use `_count` / `_cat/count` for true document counts. https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-indices
- **elasticsearch-py:**
  - Apache-2.0, forward-compatible within a major.
  - Since 7.14 it raises `UnsupportedProductError` against non-Elasticsearch servers such as OpenSearch.
  - Since 7.13, `ELASTIC_CLIENT_APIVERSIONING=1` sends `application/vnd.elasticsearch+json;compatible-with=7`.
  - https://github.com/elastic/elasticsearch-py · https://github.com/elastic/elasticsearch-py/blob/7.17/docs/guide/release-notes.asciidoc
- **elasticsearch-py helper defaults:**
  - `streaming_bulk`: `chunk_size=500`, `max_chunk_bytes=100MiB`, `max_retries=0`, `initial_backoff=2`, `max_backoff=600`, `retry_on_status=(429,)`.
  - `parallel_bulk`: `thread_count=4`, `queue_size=4`, and **no retry logic**.
  - `scan`: `scroll="5m"`, `size=1000`, `preserve_order=False`, `clear_scroll=True`.
  - `reindex`: `scan` + `bulk`, with an optional `target_client`, so it can copy across clusters.
  - https://github.com/elastic/elasticsearch-py/blob/main/elasticsearch/helpers/actions.py
- **opensearch-py:**
  - Apache-2.0 fork of elasticsearch-py.
  - Helpers: `bulk`, `streaming_bulk`, `parallel_bulk`, `scan`, `reindex`, plus async variants.
  - SigV4 auth classes: `AWSV4SignerAuth`, `RequestsAWSV4SignerAuth`, `Urllib3AWSV4SignerAuth`, `AWSV4SignerAsyncAuth`.
  - https://github.com/opensearch-project/opensearch-py/blob/main/opensearchpy/helpers/__init__.py
  - The 3.x client supports OpenSearch 1.0–3.x as long as you avoid removed features. https://github.com/opensearch-project/opensearch-py/blob/main/COMPATIBILITY.md
  - Whether OpenSearch accepts ES 8 `compatible-with=8` headers: **UNVERIFIED**.

---

### What this means for the wrapper (my inferences, not from the sources)
- **Clients:** elasticsearch-py's product check means an OpenSearch target should be driven with opensearch-py.
- **Bulk writes:** prefer `streaming_bulk` with retries (in your own thread pool) over `parallel_bulk`, which doesn't retry.
- **ClickHouse logical copies:** verify each partition with logical content hashes. `system.parts` hashes are only comparable after a physical copy (clickhouse-backup, FETCH, ATTACH).
- **Licenses to watch:** PeerDB is now AGPLv3. Elasticsearch/Kibana binaries are Elastic License. Everything else here is MIT or Apache-2.0: clickhouse-backup, elasticdump, Migration Assistant, esrally, OpenSearch CCR, and the Python clients.
