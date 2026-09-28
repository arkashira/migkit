# Tools a Python migration tool could wrap: DynamoDB, files/lakes, pipelines, graph/vector (research as of 2026-09-27)

I hit the session's 200-search limit partway through. After that I used WebFetch only, mostly vendor docs and PyPI JSON. Anything I could not confirm is marked **UNVERIFIED**.

## Things that most affect the design
- **Airbyte connector licences are inconsistent.** Airbyte's licence FAQ says its own connectors are MIT, but `source-postgres` 3.8.5 and `source-mysql` 3.53.5 declare `license: ELv2` in their metadata.yaml. Both are Java, so running them from Python needs Docker. (https://docs.airbyte.com/platform/developer-guides/licenses/license-faq ; https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-integrations/connectors/source-postgres/metadata.yaml ; .../source-mysql/metadata.yaml)
- **Redpanda Connect's CDC inputs are paid.** `postgres_cdc`, `mysql_cdc` and `mongodb_cdc` need an enterprise licence (30-day trial). The MIT fork `bento` shows no CDC inputs.
- **`pipelinewise-tap-postgres` is AGPL-3.0 and archived** (2024-09-23).
- **dynamodump can lose data silently.** After 6 retries it logs unprocessed items and drops them.
- **DynamoDB ImportTable can look successful when it isn't.** Duplicate keys overwrite each other and are not counted as errors. A failed import can leave a partly loaded table behind.
- **S3 CopyObject can fail with HTTP 200.** The error is embedded in a `200 OK` body.
- **DuckDB `hash()` output can change between DuckDB versions.** Use md5 or sha256 for fingerprints.

---

## DynamoDB

### 1. ExportTableToPointInTime (full and incremental)
- **What it does**
  - Needs PITR. Can export any moment inside the PITR window.
  - Asynchronous, uses no RCUs, no effect on the table.
  - Formats: DynamoDB JSON or Ion. Can write to an S3 bucket in another account or Region.
  - No SLA on how long it takes, so don't build fixed-time workflows. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html)
- **Output layout**
  - `prefix/AWSDynamoDB/<ExportId>/` holds `manifest-summary.json`, `manifest-files.json`, `.checksum`/`.md5` files and an empty `_started` file.
  - Full exports put data under `<ExportId>/data/*.json.gz`. Incremental exports share one `AWSDynamoDB/data/` folder.
  - `manifest-files.json` is JSON-lines with `itemCount`, `md5Checksum`, `etag` and `dataFileS3Key` per file. That is enough for built-in verification.
  - The summary has `itemCount`, `billedSizeBytes` and `outputView`; `exportType` appears only on incremental exports.
  - At least one file per partition; empty partitions get empty files. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html)
- **Incremental exports**
  - Window is 15 minutes to 24 hours; start is inclusive, end exclusive.
  - View type `NEW_AND_OLD_IMAGES` (default) or `NEW_IMAGE`. The API text says "NEW_IMAGES" but the CLI enum is `NEW_IMAGE`.
  - Each record has `Metadata.WriteTimestampMicros`, `Keys`, `NewImage` and optionally `OldImage`.
  - The operation is inferred from shape: a delete is keys only, or keys plus old image.
  - Output is compacted to one final state per item. An insert followed by a delete in the same window produces nothing.
  - The timestamp comes from DynamoDB's internal clock, not the application clock. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html ; https://aws.amazon.com/blogs/database/introducing-incremental-export-from-amazon-dynamodb-to-amazon-s3/)
- **Quotas:** 300 concurrent exports or 100 TB in flight. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html)
- **Cost**
  - Full export is billed on table plus LSI size at the export time. Incremental is billed on the change data processed, minimum 10 MB. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html)
  - $0.10/GB in us-east-1. (https://aws.amazon.com/dynamodb/pricing/on-demand/)

### 2. ImportTable from S3
- **What it does**
  - Creates a **new** table only; importing into an existing table is not supported.
  - Formats: CSV, DynamoDB JSON or Ion; GZIP, ZSTD or uncompressed.
  - GSIs can be defined at import. LSIs are not supported.
  - Uses no WCUs. Source bucket can be in another account. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.HowItWorks.html)
- **Quotas**
  - 50 concurrent imports.
  - 15 TB total source size in us-east-1, us-west-2 and eu-west-1; 1 TB elsewhere.
  - 50,000 S3 objects per import. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.Validation.html)
- **Error reporting**
  - Per-item errors go to CloudWatch log group `/aws-dynamodb/imports`, stream `<import-id>/error`.
  - `DescribeImport` returns `ErrorCount`, `ProcessedItemCount`, `ImportedItemCount`, `FailureCode` (for example `ItemValidationError`, `S3NoSuchBucket`) and `FailureMessage`. (same URL)
- **Failure modes**
  - Duplicate keys overwrite in random order, are not counted as errors and are not logged.
  - A malformed object can cause the rest of that object to be skipped.
  - Items that fail validation are skipped, and the job ends FAILED.
  - A table with partial data may be left behind; `ResourceInUseException` if the table already exists. (same URL)
- **Cost:** $0.15/GB of uncompressed source in us-east-1 (third-party and re:Post figure). Failed items are billed too. (https://www.usage.ai/blogs/aws/reserved-instances/dynamodb/import-from-s3/ ; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataImport.HowItWorks.html)

### 3. dynamodump (bchew)
- MIT; PyPI `dynamodump` 1.11.1, released 2025-11-07, Python ≥3.10. Maintenance is slow, about 2 releases a year. (https://pypi.org/pypi/dynamodump/json ; https://github.com/bchew/dynamodump/releases)
- **How it works**
  - Up to `MAX_NUMBER_BACKUP_WORKERS = 25` threads, one table per thread.
  - Each table is scanned **sequentially**; no Segment/TotalSegments.
  - Writes in batches of 25 with `BATCH_WRITE_SLEEP_INTERVAL=0.15`s and linear backoff.
  - After `MAX_RETRY=6`, unprocessed items are **logged and ignored**.
  - Temporarily changes provisioned capacity (`--readCapacity`, `--writeCapacity`, `--skipThroughputUpdate`).
  - Dump layout: `dump/<table>/schema.json` and `data/0001.json`.
  - README says it suits "smaller data volume". (https://raw.githubusercontent.com/bchew/dynamodump/master/dynamodump/dynamodump.py ; https://github.com/bchew/dynamodump)

### 4. Change streams, Global Tables, zero-ETL
- **DynamoDB Streams**
  - 24-hour retention. Each record appears exactly once. Ordering is guaranteed per item (per primary key), not per partition.
  - Shards split. You must process the parent shard before its children; `DescribeStream` has a `ShardFilter` to find child shards.
  - At most 2 readers per shard (1 for global tables).
  - A put or update that changes nothing writes no record.
  - Disabling and re-enabling creates a new stream ARN. `StreamViewType` cannot be changed after creation. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html ; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html)
- **Kinesis Data Streams for DynamoDB**
  - Retention up to 1 year; 5 consumers per shard, or 20 with enhanced fan-out.
  - Duplicates and out-of-order records can occur. Order and dedupe with `ApproximateCreationDateTime` (millisecond or microsecond precision).
  - Same account and Region only; one Kinesis stream per table.
  - Binary values are base64-encoded twice.
  - Billed at 1 change data capture unit per KB of change. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/kds.html ; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/streamsmain.html)
- **Global Tables (version 2019.11.21)**
  - Last writer wins.
  - Multi-account replication since 2026-02-03, MREC only.
  - Every replica must be in a **different account and a different Region**, so it can't do a same-Region cross-account move.
  - Can start from an existing non-empty table.
  - Backfill quota: 10 TB per day per Region. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html ; https://aws.amazon.com/blogs/database/amazon-dynamodb-global-tables-now-support-replication-across-aws-accounts/ ; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html)
- **Zero-ETL and Glue**
  - Targets: Redshift (GA 2024-10), OpenSearch (uses export plus Streams), SageMaker Lakehouse/Iceberg (built on exports, typically 15–30 minutes behind).
  - Glue has three DynamoDB readers: Scan-based, export-based, and a Spark DataFrame connector (2025-11, Glue 5.0+). (https://aws.amazon.com/blogs/database/amazon-dynamodb-zero-etl-integration-with-amazon-sagemaker-lakehouse-part-1/ ; https://aws.amazon.com/about-aws/whats-new/2025/11/glue-dynamodb-connector)

### 5. Write-side patterns
- **BatchWriteItem**
  - Up to 25 puts/deletes and 16 MB per call; 400 KB per item.
  - Not atomic as a whole. Failed items come back in `UnprocessedItems`, which AWS says to retry with exponential backoff.
  - The **whole batch is rejected** if two requests have the same key or a put and delete hit the same item.
  - No conditions allowed. Deleting a nonexistent item still costs 1 WCU.
  - `ReturnConsumedCapacity` accepts INDEXES, TOTAL or NONE.
  - Throttling errors now carry `ThrottlingReasons`. (https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)
- **boto3 `batch_writer`:** buffers and resends unprocessed items automatically. `overwrite_by_pkeys` drops an earlier buffered request with the same key, which avoids the duplicate-key rejection. (https://docs.aws.amazon.com/boto3/latest/guide/dynamodb.html)
- **PartiQL `BatchExecuteStatement`:** up to 25 statements, all reads or all writes. HTTP 200 does not mean every statement succeeded; check each statement's `Error`. (https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html)
- **TransactWriteItems**
  - Up to 100 actions and 4 MB total; no two actions on the same item.
  - `ClientRequestToken` makes the call idempotent for 10 minutes.
  - `TransactionCanceledException` returns `CancellationReasons` such as ConditionalCheckFailed or TransactionConflict. (https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html)
- **"Newer wins" conditional writes**
  - Condition: the incoming timestamp or version must not be older than the stored one (AWS calls this a ratchet).
  - Retries must reuse the first attempt's timestamp.
  - Use tombstones for deletes so a late replay can't recreate the item.
  - Does not work across Global Table Regions, which use last writer wins. (https://aws.amazon.com/blogs/database/timestamp-writes-for-write-hedging-in-amazon-dynamodb/ ; https://pynamodb.readthedocs.io/en/latest/optimistic_locking.html)
- **Parallel Scan**
  - `TotalSegments` 1 to 1,000,000; pages of 1 MB.
  - Segments are assigned by partition-key hash, so skewed keys give uneven segments and more segments don't guarantee more speed.
  - Many workers can use up all table throughput; use `Limit` to cap each worker.
  - `ConsistentRead` costs 2x RCU and still gives no snapshot isolation. AWS suggests pairing a consistent scan with Streams replay. (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html ; https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)
- **Warm throughput (pre-warming the target)**
  - `UpdateTable`/`CreateTable` `WarmThroughput={ReadUnitsPerSecond,WriteUnitsPerSecond}`, also per GSI. Status goes UPDATING, then ACTIVE.
  - Can't be decreased once raised; increases are charged.
  - Capped by the table quota (default 40,000 read and 40,000 write units). (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/warm-throughput.html ; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/update-warm-throughput.html)

### 6. aioboto3 / aiobotocore versus boto3 with threads
- No rigorous DynamoDB benchmark exists. Informal S3 tests show them roughly equal: 10,000 GETs took 27.65 s with aioboto3 and 28.58 s with boto3 in threads. (https://github.com/terricain/aioboto3/issues/359)
- Both default to a 10-connection HTTP pool. Raising `max_pool_connections` probably matters more than the choice of library. (https://github.com/boto/botocore/issues/766)
- aiobotocore 2.25.0 came out 2025-10.

---

## Files and data lakes

### 7. rclone
- **Licence, version, distribution:** MIT; v1.75.1 (Sept 2026). Binaries for Linux arm64 (zip/deb/rpm), macOS arm64 and Windows arm64. (https://rclone.org/downloads/)
- **Defaults:** checkers 8, `--multi-thread-streams` 4, `--multi-thread-cutoff` 256M; `--checksum` compares hash plus size. (https://rclone.org/docs/)
- **Verification**
  - `rclone check` compares size and MD5/SHA1.
  - `--download` compares actual bytes when the two remotes share no hash.
  - `--one-way`; report files `--combined`, `--differ`, `--missing-on-src`, `--error`. (https://rclone.org/commands/rclone_check/)
- **S3 hashes:** multipart ETags are not MD5, so rclone stores `X-Amz-Meta-Md5chksum` itself. My inference: multipart objects written by other tools will lack an MD5 to compare. Same-Region S3→S3 copies are server-side. (https://rclone.org/s3/)
- **Progress and error reporting**
  - `--use-json-log` gives JSON-lines logs.
  - The rc HTTP API (`--rc`/`rcd`, default localhost:5572) has `_async=true`, `job/status`, `job/stop` and `core/stats` (bytes, transfers, errors, eta, speed, per-file progress). (https://rclone.org/rc/)
  - `librclone` is a C shared library with a ctypes Python wrapper calling `RcloneRPC`. (https://github.com/rclone/rclone/tree/master/librclone)
- **Exit codes:** 1 uncategorised, 2 usage, 3 dir not found, 4 file not found, 5 retryable, 6 no-retry, 7 fatal, 8 transfer limit, 9 no files transferred (with `--error-on-no-transfer`), 10 duration limit. Numbers 1 and 2 were swapped in older versions. (https://pkg.go.dev/github.com/rclone/rclone/lib/exitcode ; https://rclone.org/docs/)
- **Python wrapper:** `rclone-python` 0.1.24 (MIT) needs the rclone binary installed; progress via `listener` or `pbar`. (https://github.com/Johannes11833/rclone_python)

### 8. s5cmd
- **Licence and releases:** MIT. The official latest is v2.3.0 (2024-12-16); nothing newer from peak. Versions 2.4.x and 2.5.0 come from an unofficial fork (terryrankine). (https://github.com/peak/s5cmd/releases/tag/v2.3.0 ; https://github.com/terryrankine/s5cmd/releases/tag/v2.4.1)
- **Features**
  - 256 workers by default; `run` batch mode reads a command file or stdin; `--json` output.
  - Retries 10 times for up to a minute.
  - Verifies Content-MD5 and x-amz-content-sha256 on upload; exits 1 on mismatch.
  - Claims 12x faster than aws-cli on uploads and about 4.3 GB/s downloads. (https://github.com/peak/s5cmd)
- **Limits and failure modes**
  - Official README: S3→S3 copy of objects over 5 GB is not supported.
  - Path-traversal issue peak#872: crafted object keys can write outside the target on `cp`/`sync` downloads. Only the fork documents a fix. (https://github.com/terryrankine/s5cmd/releases/tag/v2.4.1)
- **Distribution:** PyPI `s5cmd` 0.3.3 (2025-09-09) wraps the binary, with wheels for manylinux and musllinux aarch64, macOS arm64 and Windows arm64. (https://pypi.org/pypi/s5cmd/json)

### 9. S3 native
- **Copy limits**
  - `CopyObject` is atomic up to 5 GB. Anything larger needs `UploadPartCopy`. Objects can now be up to 50 TB.
  - **An error can come back inside a `200 OK`**, and a dropped connection can cancel the copy while still returning 200. (https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
- **Checksums**
  - Default protections since 2024-12: SDKs send CRCs on upload and S3 stores a whole-object CRC. (https://aws.amazon.com/blogs/aws/introducing-default-data-integrity-protections-for-new-objects-in-amazon-s3/)
  - 10 algorithms now; MD5, SHA-512 and XXHash3/64/128 were added 2026-04-22. CRC64NVME is the default. (https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
  - Full-object checksums on multipart uploads work only with CRC64NVME, CRC32 and CRC32C. The others support composite (per-part) only.
  - Copying a multipart object changes its checksum value even though the data is identical.
  - ETag is not an MD5 for multipart, `UploadPartCopy`, SSE-KMS or SSE-C objects. (https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)
- **Batch Operations "Compute checksum"** (2025-08-18) verifies billions of objects at rest without downloading them and produces an integrity report. (https://aws.amazon.com/about-aws/whats-new/2025/08/amazon-s3-verify-content-stored-datasets/)

### 10. DuckDB
- **Licence and versions:** MIT. Stable 1.5.5 (2026-07-22), LTS 1.4.5; v2.0 expected late October 2026. (https://duckdb.org/2026/07/22/announcing-duckdb-155 ; https://duckdb.org/2026/09/02/try-duckdb-20-alpha)
- **Iceberg**
  - INSERT from 1.4.0; UPDATE/DELETE from 1.4.2; MERGE INTO and ALTER TABLE from 1.5.3.
  - Writes need a catalog; merge-on-read only; UPDATE/DELETE don't work on sorted tables. (https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg ; https://duckdb.org/2026/05/29/new-iceberg-features)
- **Delta:** uses delta-kernel-rs. Writes are append-only INSERT (May 2026); no UPDATE, DELETE or MERGE; time travel by VERSION. (https://duckdb.org/2026/05/07/delta-uc-updates)
- **`COPY … (FORMAT parquet)` options**
  - `ROW_GROUP_SIZE` default 122,880 rows; `ROW_GROUP_SIZE_BYTES`; `FILE_SIZE_BYTES`.
  - `PARTITION_BY`, `PER_THREAD_OUTPUT`, `FILENAME_PATTERN`.
  - `OVERWRITE`, `OVERWRITE_OR_IGNORE`, `APPEND`.
  - Compression defaults to snappy.
  - `RETURN_STATS` and `RETURN_FILES`. (https://duckdb.org/docs/current/sql/statements/copy)
- **Metadata functions:** `parquet_metadata()` (per row-group min, max, null count and sizes), `parquet_file_metadata()` (`num_rows`, `num_row_groups`), `parquet_kv_metadata()`, `parquet_bloom_probe()`. All accept globs. (https://duckdb.org/docs/current/data/parquet/metadata)
- **Hashing:** `hash()` returns UBIGINT and "may change across DuckDB versions". Use `md5`, `md5_number` or `sha256` for durable fingerprints. (https://duckdb.org/docs/current/sql/functions/utility)

### 11. pyarrow
- Apache-2.0; 25.0.1 (2026-08-10); wheels for manylinux/musllinux aarch64 and macOS arm64. (https://pypi.org/pypi/pyarrow/25.0.1/json)
- **`write_dataset` settings**
  - `max_rows_per_file` default 0 (no limit); `min_rows_per_group` 0; `max_rows_per_group` 1,048,576.
  - `max_partitions` and `max_open_files` both 1024.
  - `existing_data_behavior` = `error`, `overwrite_or_ignore` or `delete_matching`.
  - `file_visitor` receives each written file's path and Parquet metadata, so row counts come for free. (https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html)
- **pyarrow.compute row hashing: UNVERIFIED.** I ran out of searches before checking.

### 12. pyiceberg
- Apache-2.0; 0.12.0 (2026-09-01); aarch64 and arm64 wheels; extras for glue, dynamodb, sql-postgres, duckdb and others. (https://pypi.org/pypi/pyiceberg/0.12.0/json ; https://pypi.org/pypi/pyiceberg/json)
- **Catalogs:** REST, SQL, Glue, Hive, DynamoDB, BigQuery, in-memory.
- **Writes:** `append`, `overwrite(overwrite_filter=)`, `delete`, `dynamic_partition_overwrite`, and `upsert` (needs identifier fields; returns rows updated and inserted).
- **`add_files`** registers existing Parquet without rewriting it and defaults to `check_duplicate_files=True`.
- **Inspection and snapshots:** `inspect.snapshots/files/manifests/partitions/entries/history/refs`; `scan(snapshot_id=)`; tags and branches via `manage_snapshots`. (https://py.iceberg.apache.org/api/)
- **Gaps and failure modes**
  - Maintenance covers **only `expire_snapshots`**: no compaction and no orphan-file removal.
  - Expiring snapshots can delete files that were registered with `add_files`.
  - `add_files` needs matching field IDs or a name mapping, and only order-preserving partition transforms.
  - Streaming writes are unpartitioned only.
  - The docs describe no rollback method. (https://raw.githubusercontent.com/apache/iceberg-python/main/mkdocs/docs/api.md)

### 13. delta-rs (`deltalake`)
- Apache-2.0; 1.6.6 (2026-09-24); abi3 wheels for aarch64 and arm64. (https://pypi.org/pypi/deltalake/1.6.6/json)
- **`write_deltalake`**
  - Modes: error (default), append, overwrite, ignore.
  - `predicate` gives replaceWhere-style overwrites; written rows must all match the predicate or the write fails.
  - `schema_mode` = merge or overwrite; `target_file_size`. (https://delta-io.github.io/delta-rs/latest/usage/writing/)
- **Maintenance**
  - `optimize.compact()` targets `delta.targetFileSize` or **100 MB**; parallelism defaults to CPU count; returns metrics.
  - `z_order()` rewrites file order. (https://delta-io.github.io/delta-rs/latest/api/delta_table/delta_table_optimizer/)
  - `vacuum` is a **dry run by default**, with 7-day retention. (https://delta-io.github.io/delta-rs/latest/usage/managing-tables/)
- **Change data feed:** `load_cdf(starting_version, ending_version, …)` needs `delta.enableChangeDataFeed=true` and returns `_change_type`, `_commit_version` and `_commit_timestamp`. (https://delta-io.github.io/delta-rs/latest/usage/read-cdf/)
- **Failure mode on S3:** concurrent writers need `AWS_S3_LOCKING_PROVIDER='dynamodb'` (or `conditional_put` on R2/MinIO), otherwise `AWS_S3_ALLOW_UNSAFE_RENAME` risks lost updates. (https://delta-io.github.io/delta-rs/latest/usage/writing/writing-to-s3-with-locking-provider/)

### 14. Compaction and sizing
- Parquet recommends row groups of 512 MB–1 GB and 8 KB pages. (https://parquet.apache.org/docs/file-format/configurations/)
- Cheap row-count checks without scanning data: sum `num_rows` from footers via DuckDB `parquet_file_metadata` or pyarrow `file_visitor` metadata, and compare with DynamoDB `manifest-files.json` `itemCount` or Iceberg `inspect.files`.

---

## Generic pipelines

### 15. Benthos → Redpanda Connect, and the bento fork
- **Redpanda Connect:** v4.111.0 (2026-09-25); darwin arm64 builds confirmed, linux arm64 not confirmed. (https://github.com/redpanda-data/connect/releases)
- **Licence split:** Apache-2.0 covers "the majority of connectors"; the RCL-licensed parts are enterprise features. (https://github.com/redpanda-data/connect/tree/main/licenses)
- **CDC inputs (all enterprise, all at-least-once)**
  - `postgres_cdc`: Postgres 14+; a failed snapshot needs manual replication-slot deletion. (https://docs.redpanda.com/redpanda-connect/components/inputs/postgres_cdc/)
  - `mysql_cdc`: MySQL 8+; locks tables briefly for a consistent snapshot. (https://docs.redpanda.com/redpanda-connect/components/inputs/mysql_cdc/)
  - `mongodb_cdc`: Mongo 6+ replica set. (https://docs.redpanda.com/redpanda-connect/components/inputs/mongodb_cdc/)
- **`redpanda_migrator`:** moves Kafka topics, ACLs, schemas and consumer-group offsets, since 4.67.5. Its licence tier is **UNVERIFIED**. (https://docs.redpanda.com/redpanda-connect/components/inputs/redpanda_migrator/)
- **bento (WarpStream):** MIT fork taken before the licence change, actively maintained, no CDC inputs seen. (https://github.com/warpstreamlabs/bento)

### 16. Vector
- MPL-2.0; v0.58.0 (Aug 2026); maintained by Datadog; built for logs, metrics and traces. Low relevance for database migration. (https://github.com/vectordotdev/vector ; https://github.com/vectordotdev/vector/releases)

### 17. Airbyte
- **Licences:** platform ELv2, which bans offering it as a managed service. CDK `airbyte-cdk` 7.30.0 is MIT. (https://docs.airbyte.com/platform/developer-guides/licenses/license-faq ; https://pypi.org/pypi/airbyte-cdk/json) Connector licences: see the ELv2 conflict at the top.
- **PyAirbyte:** MIT; 0.71.0; Python 3.10–3.12; connectors run in venvs or Docker (Java connectors need Docker); DuckDB is the default cache. (https://pypi.org/pypi/airbyte/json ; https://github.com/airbytehq/PyAirbyte)
- **Protocol** (v0.19.0, 2025-10-10)
  - Messages: RECORD, STATE, LOG, TRACE, SPEC, CATALOG, CONNECTION_STATUS, CONTROL.
  - State types: STREAM, GLOBAL (used for CDC) or LEGACY. Destinations echo state back in order.
  - Derived from Singer. (https://docs.airbyte.com/platform/understanding-airbyte/airbyte-protocol)
- 2025 performance data: not researched.

### 18. Meltano and Singer
- Meltano MIT, 4.3.0. State backends: system DB, local files, S3, Azure, GCS, Snowflake. Non-transactional backends use locking. `meltano state get/set` moves state between backends. (https://pypi.org/pypi/meltano/json ; https://docs.meltano.com/concepts/state_backends/)
- **Singer:** SCHEMA, RECORD and STATE messages as JSON lines on stdout; `--config`, `--state`, `--catalog`; `key_properties` and `bookmark_properties`. (https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
- **pipelinewise-tap-postgres:** **AGPL-3.0**, archived 2024-09-23, log-based mode relies on wal2json. (https://github.com/transferwise/pipelinewise-tap-postgres ; https://raw.githubusercontent.com/transferwise/pipelinewise-tap-postgres/master/LICENSE)

### 19. dlt
- Apache-2.0; 1.30.0; pure-Python wheel; extras include deltalake, pyiceberg, postgres, duckdb, sql-database and qdrant. (https://pypi.org/pypi/dlt/json)
- **`sql_database` backends**
  - sqlalchemy (default, slowest).
  - pyarrow: claimed 20–30x faster.
  - pandas: loses decimal precision.
  - connectorx: ignores `chunk_size` unless using `arrow_stream`.
  - Use `reflection_level="full_with_precision"` to keep exact types.
  - Incremental loading via `dlt.sources.incremental` (cursor, `end_value`, `row_order`). (https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/configuration)
- **Merge strategies:** delete-insert (default), scd2, upsert, insert-only. Upsert targets include postgres, mssql, and filesystem with delta or iceberg. Hints: `hard_delete`, `dedup_sort`. (https://dlthub.com/docs/general-usage/merge-loading)
- **State:** kept in `_dlt_pipeline_state` at the destination and restored automatically. (https://dlthub.com/docs/general-usage/state)

### 20. Apache NiFi
- Apache-2.0; 2.12.0 (2026-09-13); Java 21; native Python processors in 2.x; provenance and back-pressure. A heavy JVM service, so a poor fit to embed. (https://nifi.apache.org/download/ ; https://github.com/apache/nifi)

### 21. Kafka Connect SMTs (a set worth mirroring as built-in transforms)
- **Built-in transforms:** Cast, DropHeaders, ExtractField, Filter, Flatten, HeaderFrom, HoistField, InsertField, InsertHeader, MaskField, RegexRouter, ReplaceField, SetSchemaMetadata, TimestampConverter, TimestampRouter, ValueToKey.
- **Predicates:** TopicNameMatches, HasHeaderKey, RecordIsTombstone, each with `negate`.
- **Error handling:** `errors.tolerance=all`, `errors.deadletterqueue.topic.name`, `errors.retry.timeout`. The default is fail-fast. (https://kafka.apache.org/41/kafka-connect/user-guide/)

---

## Graph and vector (short notes)

- **pgvector**
  - PostgreSQL licence; 0.8.6.
  - `vector`: 16,000 dimensions stored, 2,000 indexable. `halfvec`: 4,000 indexable. `sparsevec`: 1,000 non-zeros indexable.
  - pg_dump works; indexes are rebuilt on restore (HNSW builds are faster with more `maintenance_work_mem`); run `ALTER EXTENSION vector UPDATE` after upgrading.
  - (https://github.com/pgvector/pgvector ; https://raw.githubusercontent.com/pgvector/pgvector/master/LICENSE)
- **milvus-backup**
  - Apache-2.0; CLI plus REST server on port 8080; copies segment binlogs.
  - Backs up Milvus 2.2+, restores to 2.5+, never to an older version.
  - The snapshot format (default on 3.0.1+) requires backup storage on the same provider. (https://github.com/zilliztech/milvus-backup)
- **VTS (Zilliz)**
  - Apache-2.0, built on SeaTunnel; Docker or jar; Milvus ≥2.3.6.
  - Connectors: Milvus, Elasticsearch, Pinecone, Qdrant, pgvector, Weaviate, S3.
  - **No incremental or CDC sync yet.** (https://github.com/zilliztech/vts)
- **Qdrant**
  - `qdrant/migration`: Apache-2.0, container only, batch size 50, resumable. Some sources need target collections pre-created; Pinecone serverless only. (https://github.com/qdrant/migration)
  - Snapshots restore only to the same or next minor version, are per node, and exclude aliases. Full-storage snapshots are single-node only. (https://qdrant.tech/documentation/snapshots/)
- **Neo4j**
  - Community is GPLv3; Enterprise is commercial; current version 2026.09. (https://github.com/neo4j/neo4j)
  - `neo4j-admin database dump`: Community requires the database offline; users and roles are not included; no TLS. (https://neo4j.com/docs/operations-manual/current/backup-restore/offline-backup/)
  - `load --overwrite-destination` requires the database stopped (or DROP in a cluster) and accepts s3/gs/azb paths. (https://neo4j.com/docs/operations-manual/current/backup-restore/restore-dump/)
  - The documented path into 2025/2026 releases is from 5.26 LTS, with `block` store format recommended and Java 21 required. (https://neo4j.com/docs/upgrade-migration-guide/current/version-2025/upgrade/)
  - APOC exports CSV, JSON, Cypher and GraphML. Which of these sit in Core versus Extended is **UNVERIFIED**. (https://neo4j.com/docs/apoc/current/export/)

## Not verified
- pyarrow row-hash kernel.
- Airbyte 2025 performance figures.
- Whether `redpanda_migrator` needs an enterprise licence.
- Redpanda Connect linux arm64 binaries.
- Whether Vector has a Postgres sink.
- Which delta-rs operations write change-data-feed files.
- Neo4j loading of dumps older than 5.26.
