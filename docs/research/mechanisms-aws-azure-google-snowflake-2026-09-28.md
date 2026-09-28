# The hyperscalers' migration mechanisms, and what migkit has of each (research as of 2026-09-28)

Scope: the internal mechanisms of AWS DMS (classic, Serverless, homogeneous, Schema Conversion, Fleet Advisor, Time Travel), AWS zero-ETL, Aurora clone and RDS snapshot paths, Google DMS and Datastream, BigQuery ingestion, Azure DMS (SQL Server LRS and MI link), the Azure PostgreSQL migration service, Azure MySQL paths, Azure Data Factory copy, Azure PostgreSQL PITR, Snowflake loading and CDC, Redshift COPY. Everything here is from public documentation and vendor blogs; where the docs do not say how something works, that is stated rather than guessed. Each section ends with migkit's status by file and line at HEAD (2026-09-28), and the table at the end ranks what is worth building.

Convention: *problem* is what the mechanism exists to solve; *mechanism* uses the vendor's own terms; *limits* are documented failure modes; *migkit* is where the same idea lives in the code, or that it does not.

---

## 1. AWS DMS (classic replication instance)

### 1.1 Full load: tables, segments, threads

**Problem.** One thread per table makes the load as long as the largest table.

**Mechanism.**
- `MaxFullLoadSubTasks` (default 8, max 49): tables *or table segments* loaded in parallel.
- `ParallelLoadThreads` (default 0, target-dependent max), `ParallelLoadBufferSize` (default 100, max 1000), `ParallelLoadQueuesPerThread`: threads pushing records to one target per sub-task. The two settings are *multiplicative*: total threads is about `ParallelLoadThreads * MaxFullLoadSubTasks`; too high and the task runs out of memory.
- `CommitRate` (default 10,000): records per commit on the target during full load.
- `TransactionConsistencyTimeout` (default 600 s): how long DMS waits for open transactions to close before it takes the full-load start position; it starts anyway when the timeout passes.
- `CreatePkAfterFullLoad`: build the primary key after the rows.
- Segmenting a table is a `table-settings` rule with `parallel-load`, one of: `partitions-auto` (each partition a thread), `subpartitions-auto` (Oracle), `partitions-list` (named partitions/subpartitions), `ranges` (`columns` + `boundaries`: each column-value array is the *upper* boundary of a segment, the top segment is everything above the last boundary, up to 10 columns, indexed columns recommended; DOUBLE/FLOAT/LOB columns cannot bound; **rows with NULL in a boundary column are not replicated**), or `none`. PostgreSQL sources support only `ranges`. MongoDB/DocumentDB add autosegmentation (`number-of-partitions` default 16, `collection-count-from-metadata`, `max-records-skip-per-page` default 10,000, `batch-size`). The generated WHERE for a 3-column boundary is a lexicographic `(COL1 < a) OR (COL1 = a AND COL2 < b) OR ...` and each following segment is `NOT (previous) AND (this)`.
- Table-settings rules refuse `%` wildcards: every segmented table is named by hand.

**Limits.** Boundaries are static and human-chosen; segment skew is the operator's problem. 49 segments in flight at most per task. NULL-keyed rows silently dropped under `ranges`.

**migkit.** Ranges are planned by the tool from the source's row count, split into ranges of *equal rows* (edges by `row_number()`, not equal key spans) and checkpointed per range: `migkit/ranges.py:222` (`plan`), `:322` (`step`), `:330` (`bounds_sql`), driven from `migkit/engines/hetero.py:1205` (`_move_in_ranges`, processes not threads, measured 10.6 s to 7.0 s on 2 CPUs). Keyless tables go by physical spans (`ctid` page ranges on PostgreSQL 14+, `migkit/engines/postgres.py:713`; `migkit/ranges.py:260 spans_to_copy`). Concurrency is not a setting: `migkit/sizing.py:126` (`estimate`, from host CPUs/memory, both servers' free connections and CPUs, the work) and `:204` (`Pace`, climbs while rows/s rise, backs off on `strain`). Per-table path choice with reasons: `migkit/planner.py:55`. Gaps: multi-column and text keys are not range-split (`hetero.py:1213` requires one integer key), partition-aware splitting (DMS `partitions-auto`) is not built, and NULLs in the key are not an issue because the key is the primary key.

### 1.2 LOB modes

**Problem.** Fetching a large value per row is slow; pre-allocating for the largest possible value wastes memory.

**Mechanism.** Task-level (`TargetMetadata`) or per-table (`lob-settings`):
- **Limited LOB mode** (default, `LobMaxSize` in KB, max 102,400): LOBs fetched inline with the row in bulk; memory pre-allocated as `LobMaxSize * CommitRate * LOB columns`; **anything larger is truncated with only a log warning**. Oracle: under 32 KB is optimal because LOBs are fetched as VARCHAR2.
- **Full LOB mode** (`FullLobMode=true`, `LobChunkSize` default/recommended 64 KB): row written with an empty LOB, then each LOB looked up and updated in chunks; never truncates; much slower; needs a primary key; a LOB column on a keyless table is dropped from the migration.
- **Inline LOB mode** (`InlineLobMaxSize`, only with `FullLobMode=true`): LOBs under the size go inline during *full load only*; larger ones by lookup. Per-table `lob-settings` names the same three as `limited` / `unlimited` with `bulk-max-size` 0 (standard) or >0 (combination); "multiply the largest LOB by three" because LOBs are converted to binary.
- Batch apply is allowed only in limited LOB mode. Validation of a truncated column needs `ValidationPartialLobSize` set to the same KB value, or it reports a false mismatch.

**Limits.** Silent truncation is the documented default. S3 and Redshift targets have no full LOB mode.

**migkit.** No truncating mode exists; the table copier reads values whole. What DMS makes the operator guess, migkit measures: every wide column's largest `octet_length` on both sides against the 1 GB field limit and the mover's limit (`migkit/engines/postgres.py:2979 _lob_columns`, `:3014 _lob_sizes`, `:3044 _lob_check`; `migkit/engines/base.py:137 LOB_HEADROOM`), deliberately not filtered by TOAST size because a 1,000,000-byte value compressed to 11,452 bytes. PostgreSQL large objects are carried under the same OIDs in 8 MB pieces and compared piece by piece (`postgres.py:2557`, backlog R10 at `docs/backlog.md:4006`). Still to do (R10): MySQL/Oracle values larger than one read, fetched by `SUBSTRING`/LOB locator in steps; a size-based time estimate.

### 1.3 Table preparation, cached changes, stop points

**Mechanism.**
- `TargetTablePrepMode`: `DO_NOTHING` (target rows and metadata untouched), `DROP_AND_CREATE` (DMS creates a bare table: no secondary indexes, FKs, defaults), `TRUNCATE_BEFORE_LOAD`. Reloading a table applies the mode again; with `DO_NOTHING` the operator truncates by hand. On an S3 target a restart under `DO_NOTHING` writes the full-load files again (duplicates).
- Changes made during the full load are **cached changes**: kept in memory until `MemoryKeepTime`/`MemoryLimitTotal` push them to the instance's disk; a task that pauses before applying them can fill the disk. `StopTaskCachedChangesNotApplied` / `StopTaskCachedChangesApplied` stop the task before or after they are applied (stop reasons `STOPPED_AFTER_FULL_LOAD` / `STOPPED_AFTER_CACHED_EVENTS`) so FKs and indexes can be added at a consistent point.

**migkit.** Target preparation is a step of its own (`migkit setup-target`, schema from the native dump; `advisors.py` tells the DMS operator to use `DO_NOTHING` and never let DMS create tables). The bulk paths truncate what they will fill (`movers.py:619 _truncate_step`, `:635`, `:1914`). Secondary indexes are set aside for the load and rebuilt in parallel afterwards, only where measured faster (PostgreSQL 1.9x, `movers.py:819 _IndexWindow`; MySQL measured slower and left in place, `:1554`). Changes during the copy are taken from a position captured before the copy (`docs/cutover.md` step 4) and interleaved under the invariants of `tests/test_a_copy_and_its_changes_interleave_safely.py`; R19 lever 8 (`docs/backlog.md:4535`) proposes dropping changes to not-yet-copied ranges (DBLog watermark), not built.

### 1.4 Change apply: transactional vs batch optimized

**Problem.** Applying every source statement one at a time cannot keep up.

**Mechanism.**
- **Transactional apply** (default): changes applied in source commit order, transactional integrity preserved; `MinTransactionSize` (default 1,000) and `CommitTimeout` (default 1 s) group small transactions. Slower; long transactions need more memory. Mandatory for S3 targets.
- **Batch optimized apply** (`BatchApplyEnabled=true`; default on Redshift): the *Sorter* keeps events in source commit order; a batch accumulates until `BatchApplyTimeoutMin` (1 s) / `BatchApplyTimeoutMax` (30 s) or `BatchSplitSize` (0 = unbounded) under `BatchApplyMemoryLimit` (500 MB); DMS computes **net changes** per key on the instance, writes them as CSV, loads them into a **net changes table** on the target (`awsdms_changes000000000XXXX`), then runs `DELETE`, `INSERT`, `UPDATE` from that table into the real table. Needs a PK or unique index; tables with both a PK and a unique constraint are refused; with no key, inserts go in bulk and updates/deletes one by one. Referential integrity "almost always" violated mid-batch: turn FKs off. Oracle target column limit: `2 * columns + pk columns <= 999`. A batch that fails falls back to **one-by-one mode**, and the failing transaction is logged to `awsdms_apply_exceptions`.
- `BatchApplyPreserveTransaction` (default true, **Oracle targets only**): a batch holds whole source transactions; false trades integrity for speed. Holding long transactions across batches is a documented OOM cause.
- Published effect: 3 M changes, transactional 3.5 h / batch ~7 min on dms.r5.large (see `docs/research/throughput-published-2026-09-27.md`).

**migkit.** The same net-change collapse, done in the process rather than through a staging table on the target: `migkit/engines/base.py:3919` (`_collapsed`: later values over earlier, partial updates merged so an untouched column is not blanked, a row made and removed in one batch not written, a moved key deleted at the old address and written at the new one in the same transaction). Runs of same-shape rows go as one statement, and past 1,000 rows on PostgreSQL by COPY into a session temp table shaped like the target and one `insert ... select ... on conflict` (`postgres.py:1144 STAGE_FROM`, `:1148 _stage`; measured 5,000 rows 24 ms vs 51 ms, 20,000 in 91 vs 166). Each batch is one transaction, rolled back whole and replayed on failure (`base.py:3583` region). Foreign keys: kept on by default; turned off per session only where every parent is in scope, with an orphan scan in the deep check (R2 item 5, `docs/backlog.md:3594`). Difference from DMS: migkit's fallback on a failed lane is "the whole batch again, in order, in one transaction", not a permanent switch to one-by-one; and there is no `awsdms_changes` table written into the user's database.

### 1.5 Ordering guarantees and parallel apply

**Mechanism.** `ParallelApplyThreads` (0-32), `ParallelApplyBufferSize` (100-1000), `ParallelApplyQueuesPerThread` exist in `TargetMetadata`, but DMS documents them for Kafka, Kinesis, DynamoDB, OpenSearch and Redshift-type targets; for MySQL, PostgreSQL and SQL Server targets CDC is applied by one thread. Ordering: transactional apply replays in source commit order; batch apply keeps order only *between* batches and within a batch only as DELETE-INSERT-UPDATE runs. DMS never applies a partial transaction in transactional mode.

**migkit.** Lanes by dependency: rows joined by a foreign key, a shared unique index or a key are placed in one lane (`base.py:3611 _lanes`, from 2,000 changes, `LANES_FROM`); tables with no dependency are written as one run of deletes and one of upserts per table, "the order DMS's batch apply uses" (`base.py:3663 _net_rows`); measured MySQL to PostgreSQL 320,000 changes 21.8 s to 4.9 s, and 59.6 s to 6.0 s with 10 ms RTT (R2). The exact-batch mark (`twoway.py`, `postgres.py` origin table, R3) forces one transaction per batch where a counter must survive a restart. Still open (R2 items 4 SQL Server, 6 decoding sidecar; R19 lever 7 writer beside the target, lever 10 pipeline mode).

### 1.6 Checkpoints and native start points

**Mechanism.** `RecoveryCheckpoint` (from `DescribeReplicationTasks`, or continuously in `awsdms_txn_state` on the target when `TaskRecoveryTableEnabled`) has the form `checkpoint:V1#34#00000132/0F000E48#0#0#*#0#121` (PostgreSQL) or `checkpoint:V1#27#mysql-bin-changelog.157832:1975:-1:2002:...` (MySQL) and is accepted by `CdcStartPosition`, as are a timestamp and a native LSN/SCN/binlog position. PostgreSQL CDC-only from a position needs a pre-existing slot named in `slotName`.

**migkit.** The tail's position is saved beside the batch it belongs to (`tailctl.py`, `postgres.py:2652 _saved_batch`, `:2672 _committed_ahead`), a native subscription's position lives in a per-hop, per-database origin (`postgres.py:6670 follow_origin`, `:6682 follow_slot`), and the target answers which batch it last committed (R3). Nothing is written into the user's database except the origin-mark table a two-way hop asks for.

### 1.7 Data validation

**Problem.** Rows that arrived wrong, and rows still in flight, look the same to a naive compare.

**Mechanism** (`ValidationSettings`).
- `EnableValidation`; validation starts per table as soon as its full load finishes and continues through CDC.
- `PartitionSize` (default 10,000): rows read per comparison batch on both sides; `ThreadCount` (default 5): each thread takes not-yet-validated partitions. Full load and validation-only use range queries; ongoing replication switches dynamically between range queries and per-record fetches.
- `ValidationMode`: `ROW_LEVEL` compares row by row; `GROUP_LEVEL` (auto from engine 3.5.4 on Oracle/SQL Server to PostgreSQL and like-to-like Oracle/SQL Server) hashes groups; needs `DBMS_CRYPTO` on Oracle, `pgcrypto` on PostgreSQL, a `BIT_XOR` aggregate on PG 12/13, and Secrets Manager credentials.
- Revalidation: a table updated by CDC moves from `Validated` to `Pending revalidation`; `ValidationQueryCdcDelaySeconds` (default 0; 180 on validation-only) delays the first query per change; `RecordFailureDelayInMinutes` / `RecordFailureDelayLimitInMinutes` hold back a failure report for at least the task's CDC latency, so a row still in flight is not reported. CDC validation retries mismatched rows several times before failing them.
- Thresholds: `FailureMaxCount` 10,000 per task, `TableFailureMaxCount` 1,000 per table, `RecordSuspendDelayInMinutes`; past them validation is suspended.
- `HandleCollationDiff` (sort order differs across engines; PostgreSQL key collation not `C` vs Oracle fails validation), `SkipLobColumns`, `ValidationPartialLobSize`, `override-validation-function` rule per column (not on key columns).
- `ValidationOnly=true` (immutable, `TargetTablePrepMode=DO_NOTHING`): full-load flavour compares everything in one pass and stops, never suspends, counts source changes as failures; CDC flavour validates existing rows then follows the source log to re-validate changed keys. It must run in the same direction as the migration: reversed, it sees only what DMS sent.
- Control table `awsdms_control.awsdms_validation_failures_v1` (schema varies: `awsdms_control` MySQL, `public` PostgreSQL, target schema Oracle, `dbo` SQL Server): `TASK_NAME, TABLE_OWNER, TABLE_NAME, FAILURE_TIME, KEY_TYPE ('Row'), KEY (JSON), FAILURE_TYPE (RECORD_DIFF | MISSING_SOURCE | MISSING_TARGET | TABLE_WARNING), DETAILS (JSON of differing columns)`. From 3.6.1, **data resync** on PostgreSQL targets writes `awsdms_validation_failures_v2` with `RESYNC_RESULT`, `RESYNC_TIME`, `RESYNC_ACTION (UPSERT | DELETE)`, `RESYNC_ID`, and re-applies the failed keys from the source.
- Table states: `Pending validation`, `Preparing table`, `Pending records`, `Pending revalidation`, `Validated`, `Mismatched records`, `Suspended records`, `No primary key`, `Table error`, `Error`, `Not enabled`.

**Limits.** PK or unique index required (no CLOB/BLOB keys, VARCHAR keys under 1,024, no NULL key values, Oracle `NOVALIDATE` keys not counted); views not validated; a target written by anything else gives wrong answers; rows changing continuously cannot be validated; stops at 10,000 failed or suspended; not supported for consolidations, masked columns, Aurora Limitless; heavy on Redshift.

**migkit.** Whole-table digests in one server-side parallel aggregate, no partition queries (README: 488 M rows in ~10 min); per-key drilldown and bisection to the row across engines (`hetero.py:462 _bisect`, `canon.py`); partial sums per key range with a fingerprint so a long verify resumes (`checkpoint.py:1-30`); the confirm pass re-reads suspects behind an LSN/GTID/cluster-time fence and reports *in flight* apart from *wrong* (`base.py:3194 fenced_recheck`, `:3233 _resolve_inflight`; backlog P0 item 1 `docs/backlog.md:872`); delta verify re-checks only keys the log names (`postgres.py:6907`, `mysql.py:2074`, `mssql.py:578` via Change Tracking, `mongodb.py:1555`); verify-as-it-lands reads each batch back by key right after it is written (`hetero.py:1328 _write_checked`, `:925 _verify_batch`, `:874 _batch_digest`); keyless tables by whole-row hash buckets and span folds; collation and time-zone fingerprints (`base.py:811`); business-rule aggregates (`rules.py`); repair by key with undo (`sync --kind rows`), which is DMS's resync with a restore point. Nothing is written into the target for it (`audit.py`: "migkit writes no bookkeeping into a target"). Gaps: an operator-supplied per-column normalisation like `override-validation-function` (migkit has `canon` classes but no per-column user expression), and SQL Server has no change stream for the fence yet (P0 item 1, still open).

### 1.8 Premigration assessments

**Mechanism.** A *premigration assessment run* executes *individual assessments* against a task configuration and writes JSON to S3. `DescribeApplicableIndividualAssessments` lists what applies. For all endpoints: `unsupported-data-types-in-source`, `full-lob-not-nullable-at-target`, `table-with-lob-but-without-primary-key-or-unique-constraint`, `table-with-no-primary-key-or-unique-constraint`, `target-table-has-unique-key-or-primary-key-for-cdc`. PostgreSQL adds: DDL event trigger `ENABLE ALWAYS`, PostGIS columns, FKs on target during full load, similarly-named tables, ARRAY without PK, keys with `BatchApplyEnabled`, secondary indexes on target during full load, limited LOB with batch apply, version, `logical_decoding_work_mem`, long transactions, `max_slot_wal_keep_size`, `max_wal_senders`. MySQL: `REPLICATION CLIENT`/`REPLICATION SLAVE`, `server_id`, binlog expiry set, LOBs over `max_allowed_packet`, rows over 65,535 bytes under batch apply. SQL Server: secondary indexes, triggers, computed columns, columnstore, memory-optimised and temporal tables, delayed durability, ADR, >10k tables, special characters, masked columns, encrypted or URL/Azure backups. DMS Serverless (Feb 2025) runs them automatically into a system bucket, with `IncludeOnly`, `Exclude`, `FailOnAssessmentFailure` (default true).

**migkit.** `migkit assess` per engine with the value to set: `postgres.py:5639` (wal_level, slots and senders against use, `max_slot_wal_keep_size`, oldest snapshot age, replica identity per keyless table, target read-only, mover leftovers), `mysql.py:4510` (`binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL`, retention with the RDS procedure, transaction compression, `server_id`, GTID consistency, long transactions), `hetero.py:569` for pairs, plus what DMS does not assess: mover leftovers on the source (`leftovers.py`), collation drift in stored code, standbys on the target (R9, `docs/backlog.md:3979`), the largest value per wide column (1.2). Playbooks for running DMS/DTS with migkit around them: `advisors.py`. Gap: free space (blocked on cloud metrics) and DocumentDB (R9).

### 1.9 Time Travel

**Mechanism.** From 3.4.6 (PostgreSQL source), 3.5.0 (Oracle, SQL Server sources); PostgreSQL and MySQL targets; full-load+CDC and CDC-only tasks. `TTSettings`: `EnableTT`, `TTS3Settings` (bucket, `ServiceAccessRoleArn` named `dms-tt-s3-access-role`, `EncryptionMode` `SSE_KMS` with a customer-managed key only), `TTRecordSettings` (`EnableRawData`, `OperationsToLog` `ALL | INSERT | UPDATE | DELETE`, `MaxRecordSize` KB), retention `ttRetentionPeriod` (hours). It records the captured transactions and DML, and the data, to S3 so a discrepancy can be examined after the fact without re-running with verbose logging. Task must be stopped to turn it on.

**migkit.** Every check writes evidence (`data-evidence.txt`, per-key `data-*` files), every repair its undo, every two-way conflict both versions (`evidence.py`, optionally age-encrypted to recipients), all under a hash-chained audit record (`audit.py`). What migkit does not keep: the raw change stream it applied. Building a "changes applied" journal on the tail (compressed, retention-bounded, off by default) would give Time Travel's capability locally; see the table.

### 1.10 Source-side objects and privileges per engine

**PostgreSQL.** `wal_level=logical` (`rds.logical_replication=1` on RDS/Aurora, reboot); the endpoint user needs `rds_superuser` and `rds_replication` on RDS or superuser/`REPLICATION` self-managed. DMS creates one logical slot per task with `pglogical` if the extension is present, else `test_decoding` (`PluginName` overrides; `slotName` reuses a pre-created slot). To capture DDL it installs an **event trigger** `awsdms_intercept_ddl ON ddl_command_end`, a function of the same name and a table `awsdms_ddl_audit` in `DdlArtifactsSchema` (default `public`); `CaptureDDLs=N` skips them; the trigger must be `ENABLE ALWAYS` if another replication system is also a target. `HeartbeatEnable`/`HeartbeatFrequency` (5 min)/`HeartbeatSchema` write a dummy transaction so an idle slot does not pin WAL. `wal_sender_timeout=0` recommended. Not captured: TRUNCATE, two-phase commits, DDL inside procedure bodies, primary-key definition changes; a keyless table has no before image unless `REPLICA IDENTITY FULL`; `max_slot_wal_keep_size` set and passed fails the task. Read replicas and RDS Proxy are not CDC sources.

**MySQL / MariaDB / Aurora MySQL.** `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_checksum=NONE` (3.4.7 and older), `expire_logs_days >= 1` or `binlog_expire_logs_seconds`, `log_slave_updates=TRUE` on a replica source, `server_id >= 1`; on RDS/Aurora `call mysql.rds_set_configuration('binlog retention hours', 24)` (max 168; default NULL = purge at once). Privileges `REPLICATION CLIENT`, `REPLICATION SLAVE`, `SELECT`. Transaction compression and `PARTIAL_JSON` row values are unsupported.

**Oracle.** `ARCHIVELOG`; supplemental logging at database level (`ADD SUPPLEMENTAL LOG DATA`) plus `PRIMARY KEY` or `ALL` columns per table (`AddSupplementalLogging` endpoint attribute adds it); LogMiner (default, archived and online redo, PDB not supported) needs `EXECUTE ON DBMS_LOGMNR`, `SELECT ON V_$LOGMNR_LOGS`, `V_$LOGMNR_CONTENTS`, `LOGMINING` (12c+); **Binary Reader** (`UseLogminerReader=N`, `UseBfile=Y`) parses raw redo files itself, is recommended over 10 GB/h of redo and required for PDBs and ASM; keep archive logs at least as long as the longest transaction (24 h typical).

**SQL Server.** Full or bulk-logged recovery model with full backups; self-managed sources use **MS-Replication** (a distribution database and publication, sysadmin or a prepared non-sysadmin path) for keyed tables and **MS-CDC** for keyless ones; RDS for SQL Server has no MS-Replication, so MS-CDC for everything; DMS then reads the log with `fn_dblog()`/`fn_dump_dblog()` by LSN. `db_owner`, `VIEW DATABASE STATE`, `VIEW SERVER STATE`, `VIEW DEFINITION`. Without either mechanism only INSERT/DELETE on keyless tables are captured; UPDATE and TRUNCATE are ignored. Log or database backups in progress block a non-sysadmin restart.

**migkit.** PostgreSQL: the tail reads a slot through `test_decoding` parsed properly (`pgslot.py`, three quoting traps documented) or migkit stands up a native publication/subscription with `streaming = parallel|on` chosen by version and `origin = none` for two-way (`postgres.py:7498 replicate_sql`); no event trigger, no audit table, no heartbeat table in the user's database; DDL is seen by comparing catalogue snapshots before each batch and the tail stops rather than applying a row the target cannot hold (`drift.py`, P0 item 5, `docs/backlog.md:1122`). MySQL: `pymysqlreplication` reader with the compressed-transaction payload opened (`binlog_payload.py`, `mysql.py:649`), the purge horizon computed (`mysql.py:1010`), positions by GTID where run (`mysql.py:157`). SQL Server: Change Tracking for delta verify (`mssql.py:578`) and a neutral change reader (`mssql.py:895`); no MS-Replication/MS-CDC/`fn_dblog` path. Oracle: `oracle.py` is 11 KB, no redo reader (R11/R12). Gap that matters: heartbeat on an idle PostgreSQL slot is not implemented (an idle source with a migkit slot pins WAL like anyone else's slot; `assess` warns on `max_slot_wal_keep_size` but does not advance the slot).

### 1.11 S3 and Redshift targets

**Mechanism.** S3: full-load files `LOAD00001.csv|parquet` per table folder, CDC files by timestamp `20230405-094615814.parquet`; `CdcPath` + `PreserveTransactions=true` writes CDC in transaction order to one folder (3.4.2+), mutually exclusive with `DatePartitionEnabled` (`YYYYMMDD` etc., delimiter, `DatePartitionTimezone`); first column `I|U|D` (`IncludeOpForFullLoad` adds `I` on full load); Parquet `parquet_1_0|2_0`, `RowGroupLength` 10,000, `ParquetTimestampInMillisecond`; `cdcMaxBatchInterval`/`cdcMinFileSize`; no batch apply; a restart under `DO_NOTHING` duplicates. Redshift: DMS writes CSV per table to an S3 bucket in the Redshift region (same account, `ServiceAccessRoleArn` with `s3:PutObject/GetObject/ListBucket`), issues `COPY`, and applies CDC as batch-apply net changes through the same staging because "commits can be quite expensive" on a warehouse.

**migkit.** Parquet on disk or S3 as a side of a pair (`engines/parquet.py`); warehouses as a side (`engines/warehouse.py`: Redshift through the PostgreSQL driver, Snowflake connector, BigQuery client), rows written as delete-then-insert per key, BigQuery through a load job with `WRITE_APPEND` after a delete of the batch's keys (`warehouse.py:250`, not one transaction, converges on restart). The staged bulk path ("loading each warehouse from staged Parquet through its own bulk command") is explicitly *still to come* (`warehouse.py:13-14`, backlog item 33 at `docs/backlog.md:2989`). Change streams to Kafka/Kinesis/Pub/Sub in `json | debezium | canal | avro` (`streamout.py`, `avrostream.py`).

---

## 2. AWS DMS Serverless

**Mechanism.** Capacity in **DCU** (1 DCU = 2 GB RAM; steps 1, 2, 4, 8, 16, 32, 64, 128, 192, 256, 384). Min DCU optional: DMS assesses the workload to set it. Scale-up after sustained utilisation over a threshold; scale-down only after 60 consecutive minutes under about 45 % utilisation with no large pending transactions (re:Post), which is why tasks "stick" at high DCU; changing the range needs a stop. Storage auto-grows since April 2025. Phases: Full load, CDC (initial: the cached changes, skipped when `StopTaskCachedChangesNotApplied`), CDC (ongoing). **Enhanced Full Load Performance**: the replication orchestrator reads table metadata, "creates equally weighted segments per table" automatically (auto-segmentation by ROWID for Oracle) and loads them in parallel up to `MaxFullLoadSubTasks` (set 49 and a higher max DCU); today Oracle to Redshift (2-10x) and Oracle to S3 (up to 2x) only. Premigration assessments run automatically (1.8).

**migkit.** Concurrency from the host and both servers, paced live (`sizing.py`), per-table auto-split (1.1) on every SQL engine, not only Oracle; no capacity units because migkit runs where it is started, and scale-out across machines is backlog item 31 (`docs/backlog.md:2662`).

---

## 3. AWS DMS homogeneous data migrations

**Mechanism.** Serverless environment running the engine's own tools. PostgreSQL: full load `pg_dump` to the environment's disk then `pg_restore`; full load + CDC: `pg_dump`/`pg_restore` of *schema only*, then a **publisher/subscriber (native logical replication) with the Initial Data Synchronization option** copying table data source-to-target, then ongoing replication; CDC-only from a native start point (LSN) or "Immediately". MySQL: `mydumper` to disk then `myloader`; then binlog replication from the position taken at the start of the full load (`show master status`); GTIDs are not used, so a source failover is not followed; encrypted tables land unencrypted; selection rules only for full load. MongoDB/DocumentDB: `mongodump`/`mongorestore`, collections in parallel with segments computed at run time, then oplog/change streams; no create/rename/drop collection replication; no time-series collections. No table mapping rules, **no built-in validation**, DDL replicated only for MySQL, no higher-to-lower version, port 8081 reserved.

**migkit.** The same tools driven with their edge cases handled: `movers.py` (`pgdump_move :925`, `pgcopydb_move :2727` with `copy table-data` not `clone` and why, `mydumper_move :2172`, `mongosync_move :2472`, `mongodump_move :2333`, `native_move :244`), tool availability and version pinning (`tools.py`, `toolversion.py`), the loader's refusals parsed rather than tolerated (`movers.py:1034 _RestoreLog`), programs killed with the run (`movers.py:283 _spawned`). Native replication after the copy: `postgres.py:7498` / `mysql.py:5672` (`replicate_sql`, `copy_data` true or false, the note that a subscription made *after* a copy carries nothing from between). Difference: migkit verifies whatever moved the rows; homogeneous DMS has no validation at all.

---

## 4. DMS Schema Conversion and Fleet Advisor

**Schema Conversion.** A **rules-based engine** converts what it can deterministically; from Dec 2024 an opt-in **generative AI** pass (Bedrock-hosted LLMs, cross-region inference) rewrites the SQL elements the rules left as *action items* (procedures, functions), for Oracle/SQL Server/SAP ASE to PostgreSQL; AWS claims "up to 90 %". July 2026: an MCP server and a `dms-schema-conversion` skill let an agent drive projects, metadata browsing, conversion, assessment reports and export; "the agent handles orchestration, the rule-based engine handles known patterns, generative AI handles the edge cases".

**Fleet Advisor.** A Windows **data collector** (.NET, LDAP discovery, RTPS over TLS to AWS) or the ADS Agentless Collector OVA gathers metadata and capacity (schemas, versions, CPU/memory/disk, IOPS, connections) in single-run or 1-60-day monitoring mode, up to 100 databases at once, into an S3 bucket, and produces target recommendations.

**migkit.** Cross-engine DDL and type conversion through the neutral layer (`canon.py:903 ddl_type`, `hetero.py:2044 convert_ddl`, `:2160 converted_code`, `:2315 prove_converted`: a converted view or function is *held to the same inputs and outputs* as the source's, which DMS SC does not do); AI assistance from any provider, off by default, proposals marked as proposals and never a verdict, definitions only and never rows (`assist.py`; backlog item 43). Fleet Advisor's discovery is out of scope (migkit starts from a hop, not a fleet); `doctor` inventories the machine and the hops. Wider stored-code conversion is R11 (`docs/backlog.md:4030`).

---

## 5. AWS zero-ETL (Aurora / RDS to Redshift)

**Mechanism.** No COPY, no DMS: **seeding** from Aurora's storage layer ("fast clones and the decoupled storage" / a snapshot export, 20-25 min or more, reseeds on DDL), then storage-level CDC. Aurora MySQL needs **enhanced binlog** (`aurora_enhanced_binlog=1`, `binlog_backup=0`, `binlog_replication_globaldb=0`, ROW, `FULL` image and metadata, no compression, no `PARTIAL_JSON`); Aurora PostgreSQL needs **enhanced logical replication** (`aurora.enhanced_logical_replication=1`, which always logs all column values, `aurora.logical_replication_backup=0`, `aurora.logical_replication_globaldb=0`). Target: Redshift Serverless or RA3, encrypted, case-sensitive, **read-only** destination database. Every table needs a PK; DDL on a source table triggers a table resync; same region; one integration per source-target pair; data filters per table (MySQL). Published: 1.4 M tx/min (23.8 M rows/min) PostgreSQL, 1 M tx/min MySQL, p50 under 15 s. Cost traps: no concurrency scaling for its writes, Serverless never auto-pauses, enhanced binlog raises storage.

**migkit.** Same shape without the platform lock: bulk to a warehouse side plus a change tail (`warehouse.py` + `streamout.py`/tail), verified. What is missing is the staged bulk load (item 33) and any equivalent of "storage-level" seeding, which only the cloud can do; migkit's answer is R19 lever 1 (below).

---

## 6. Aurora clone / RDS snapshot restore, then logical continuation

**Mechanism.** Aurora **fast database cloning** is copy-on-write on the shared storage volume (`restore-db-cluster-to-point-in-time --restore-type copy-on-write --use-latest-restorable-time`): a clone of any size in about the time to provision an instance; storage is charged only for pages either side changes afterwards. **Continuation**: create the publication and a logical slot on the source *before* cloning (`pg_create_logical_replication_slot(..., 'pgoutput')`), clone, read the clone's own start LSN with `aurora_volume_logical_start_lsn()` (Aurora PG 11.15+/12.10+/13.6+/14.3+/15.2+), create the subscription on the clone with `copy_data = false, create_slot = false, enabled = false` pointing at that slot, advance its origin `pg_replication_origin_advance('pg_<subscription oid>', '<lsn>')` (needs Aurora PG 15+ for the function on Aurora), enable. The RDS-to-Aurora variant restores a snapshot into a temporary RDS instance to read the seed LSN, then does the same. Aurora MySQL: clone, then `mysql.rds_set_external_source(host, port, user, pass, 'mysql-bin-changelog.NNNNNN', pos, 0)` with the position from the restored cluster's "Binlog position from crash recovery" log line; `rds_start_replication_until` bounds it; binlog retention up to 90 days. **Caveat**: with enhanced binlog, a restored or cloned cluster has *no* binlog files despite retention, and enhanced binlog reported wrong coordinates after a snapshot restore until Aurora MySQL 8.4.8 (Sept 2026).

**RDS snapshot copy across accounts/regions.** Unencrypted shared snapshots restore directly; encrypted ones must be copied first with a customer-managed key shared to the account (default RDS key cannot be shared); cross-region copies are queued (20 in flight per destination region for RDS, 5 for Aurora), take "hours" (a 2-6 h range reported for one multi-hundred-GB case), and since Sept 2025 cross-region + cross-account is one step. A restored instance is "available" in minutes but **lazily hydrates** from S3 (`StorageOperationStatus = Initializing`, `StorageOperationPercentProgress`); pre-warm with full scans or `VACUUM ANALYZE`.

**migkit.** Not built. Prerequisites exist: slot and origin naming per hop and database (`postgres.py:6670`, `:6682`), a subscription with `copy_data=false` (`postgres.py:7498`), the "slot before snapshot" rule in the owner's memory and in the note `replicate_sql` prints when a subscription is made after a copy, the position-fenced check (`base.py:3194`). R19 lever 1 (`docs/backlog.md:4475`) is exactly this rung: physical copy (`pg_basebackup` with server-side zstd, MySQL `CLONE INSTANCE` 8.0.17+, XtraBackup, Mongo file snapshot with `--oplog`, or a cloud snapshot/clone) then the tail from a slot made *before* the copy, fast-forwarded to the copy's own end position (control-file checkpoint, `gtid_executed`, oplog last entry), refused rather than guessed where the privilege is missing. The Aurora functions above (`aurora_volume_logical_start_lsn`, `rds_set_external_source`, the "crash recovery" log line) are the cloud-specific inputs that rung needs.

---

## 7. Google Cloud Database Migration Service

### 7.1 PostgreSQL (to Cloud SQL / AlloyDB)

**Mechanism.** **pglogical**, not pg_dump for data. One-time or continuous. The initial snapshot takes "a short (under 10 seconds) lockout on the database tables, one at a time" (one user measured 1-2 minutes of exclusive locks in total at MAXIMUM parallelism). **Data dump parallelism** `MINIMAL | OPTIMAL (default) | MAXIMUM` maps to the number of pglogical **subscriptions per database** (up to 4 at MAXIMUM, 10 slots total), tables distributed across **replication sets** by size to balance them; each subscription copies its tables in parallel with the others, but **one synchronization worker per table**, so a single huge table gets no intra-table parallelism ("current parallelism is table-level due to the limitations of pglogical"). Secondary indexes are built after the data with `pg_restore --jobs` sized to the destination machine; `max_parallel_maintenance_workers` helps. Source flags: `max_replication_slots >= databases * jobs (+ extra for parallelism)`, `max_wal_senders >= that`, `max_worker_processes >= databases`, `wal_level=logical`, and on RDS `rds.logical_replication=1`, `wal_sender_timeout=0`. Promotion disconnects the destination from the source and makes it primary; it can be triggered with non-zero lag ("may affect the accuracy of the data"). Verification: the docs point at the open-source **Data Validation Tool** (DVT) as the precise compare. Reported throughput at MAXIMUM: 150-250 MiB/s.

**Limits.** Generated columns not replicated (PG 12+); DDL only via `pglogical.replicate_ddl_command`; new tables need `pglogical.replication_set_add_table`; keyless tables get the snapshot and INSERTs only; materialized views empty; sequence values may differ; UNLOGGED/TEMP tables and large objects not migrated; no read-replica-in-recovery sources; no AWS SCT extension pack on the source; C-language UDFs not migrated; databases added after the job started are not migrated; no table/schema selection inside a database; no automated backups during migration.

**migkit.** Native logical replication as one path (`postgres.py:7498`), migkit's own tail as another, `pgcopydb` and `pg_dump -j` for bulk, per-table ranges inside one table (which pglogical cannot do), sequences carried (`sync --kind sequences`), large objects carried (1.2), materialized views and extensions compared in `check schema`, DDL detected and the tail stopped (P0 5). DVT-style verification is native (1.7). Gap: none of Google's mechanism is better than what migkit has, except the managed runtime.

### 7.2 MySQL

**Mechanism.** `mysqldump` with a fixed set of flags (`--single-transaction`, `--master-data=1`, `--set-gtid-purged=AUTO`, `--hex-blob`, `--no-autocommit`, `--routines`; user-tunable `add-locks`, `ignore-error`, `max-allowed-packet`) *or* **data dump parallelism** (a high-performance dump needing `local_infile=ON` on the destination and a source account *without* `BACKUP_ADMIN`; `OPTIMAL | MAXIMUM | MINIMUM`), mutually exclusive with dump flags; or bring-your-own dump under 24 h old from Cloud Storage. Then binlog replication (ROW format, `server_id`, GTID `ON` or `OFF`, not `ON_PERMISSIVE`; GTID `ON` required with read replicas or a manual dump). InnoDB only. Cloud SQL replica parallelism via `replica_parallel_workers`.

**migkit.** `mydumper`/`myloader` (`movers.py:2172`), a binlog tail that opens compressed transactions (`binlog_payload.py`), native replica set-up with four appliers where the target has one and `replica_preserve_commit_order` allows (backlog 29, `docs/backlog.md:2473`), MariaDB `MASTER_USE_GTID` handled. Gap: the safe `LOAD DATA LOCAL` path (R19 lever 6) is measured 36 % faster and set aside because `local_infile` lets the server ask the client for any file; the plan is a client that answers only for the chunk migkit prepared.

### 7.3 SQL Server

**Mechanism.** Backup and restore, not log reading: a full `.bak` (plus optional differential) uploaded to a Cloud Storage bucket, then transaction-log backups (`.trn`) polled and restored continuously; a `.trn.final` file ends the incremental phase and moves the job to "Ready to promote"; strict file naming; no simple recovery model; encrypted backups must all share one key per database; Cloud SQL sources have their exports automated.

**migkit.** SQL Server is verify-and-repair only with script hints (`mssql.py:150`, `:571`); no backup-chain driver. See 10 for the Azure twin of this mechanism and the table.

### 7.4 Oracle to PostgreSQL (heterogeneous)

**Mechanism.** A **conversion workspace**: the current built-in deterministic converter (pull schema, edit mappings, "Convert source", draft schema, apply to destination) or the *legacy* **Ora2Pg** workspace (operator-supplied `ora2pg.conf`, connection details ignored, `WHERE` directive ignored, schema applied by hand, no pre-test); data by Google's serverless CDC from an initial snapshot; primary keys required (create one from all columns if none). SQL Server to PostgreSQL and to AlloyDB follow the same shape.

**migkit.** `hetero.py` converts and *proves* converted objects (4); Oracle as an engine is R11/R12.

---

## 8. Google Datastream

**Mechanism.** A **stream** = source + destination; **objects** = tables; **events** carry the full row plus generic metadata (`stream_name`, `read_method` `backfill*|cdc*`, `object`, `uuid`, `read_timestamp`, `source_timestamp`, `sort_keys`) and source metadata (MySQL `log_file`/`log_position` or GTID; Oracle `scn`, `rs_id`, `ssn`; PostgreSQL `lsn`; SQL Server `lsn`). Delivery is **at-least-once** ("no event is missed, but there's a possibility of duplicate events", duplicates within minutes, dedupe by `uuid`); **ordering is not guaranteed**; consumers order by `sort_keys` or, in BigQuery, by the internal **change sequence number (CSN)**. Backfill is **incremental** by default (ranges of rows in batches) or **full dump**; each backfill task is "an unfiltered SELECT query on a table"; `maxConcurrentBackfillTasks` default 15 (1-50) and `maxConcurrentCdcTasks` default 5 (1-50, MySQL and Oracle only; PostgreSQL and SQL Server CDC are single-threaded; too low and log position is lost and the stream fails permanently). Recommended up to 10,000 tables per stream; a stream can be started from a specific GTID set or binlog position, or an Oracle SCN.

**Per source.** MySQL: binlog ROW, GTID recommended for failover. Oracle: LogMiner (archived redo only, single-threaded, higher latency; smaller redo files help) or **binary reader** (Preview: online and archived redo via ASM or directory objects, multithreaded, low impact, limited types, VARCHAR2 over 4000). PostgreSQL: a `pgoutput` publication (filtered publication recommended; `FOR TABLES IN SCHEMA` on 15+) and **one slot per stream**, so one hot table delays every table in the stream; `max_slot_wal_keep_size` recommended. SQL Server: **change tables** (SQL Server's own CDC capture instances: lower throughput, shorter log retention, encrypted tables OK, DDL after the capture instance is ignored, new capture instances not followed) or **transaction logs** (`sp_cdc_change_job @pollinginterval = 86399`, log truncation safeguard via an open transaction; SQL Agent down long enough loses changes permanently); snapshot isolation for consistent backfill.

**BigQuery destination.** Datastream writes through the Storage Write API. **Merge mode** (tables with a PK): BigQuery consolidates by PK under the table's `max_staleness`, set from the stream's staleness limit when the table is created, applied in background merge jobs or at query time; "no historical record"; the staleness limit does *not* change how often merge jobs run; buy a BACKGROUND reservation or pay on-demand. **Append-only mode**: every event as a row with `datastream_metadata.CHANGE_TYPE` `INSERT | UPDATE-INSERT | UPDATE-DELETE | DELETE`, `CHANGE_SEQUENCE_NUMBER`, `SORT_KEYS`; a PK change is two rows; LOB-column tables on Oracle/PostgreSQL/SQL Server may miss intermediate rapid changes. Keyless tables are append-only with `IS_DELETED`. Event size 20 MB max; no adding/removing a PK on a replicated table without Support; FLOAT keys unsupported; names with `. $ / @ +` rewritten; four clustering columns max. Backfill rows all share one timestamp, UUID and CSN.

**migkit.** Exactly-once by construction is the stated bar (`docs/backlog.md:190` region, R3): the batch's mark and number travel with it and the target says which batch it last committed; Datastream's at-least-once plus dedupe-by-uuid is what migkit's design refuses for counters and keyless rows. Streams out in consumer formats with a per-row key partition and a size limit (`streamout.py`). What Datastream has and migkit lacks: Oracle redo readers, SQL Server log/change-table readers (R11/R12), and a BigQuery merge-mode writer (migkit's BigQuery writer deletes keys then loads; `warehouse.py:250`).

---

## 9. BigQuery ingestion

**Load jobs.** Atomic ("either all records get inserted or none"); use `WRITE_TRUNCATE` for idempotent retries; quotas: **1,500 load jobs per table per day (cannot be raised; shared with copy and query jobs that write the table; failed jobs count; tied to the table *name*, so drop-and-recreate does not reset)**, 15 TB per job across files, 10,000 URIs, 10 M files, 6 h execution; parallel across files not within one (100 MB-1 GB files best); partitioned tables get a separate, higher partition-modification limit; DML does not count. **Data Transfer Service** schedules load jobs from Cloud Storage/S3/Azure Blob and runs the **Teradata** (on-prem agent: TPT `tbuild` per partition batch to pipe-delimited files, or JDBC to Avro, uploaded to Cloud Storage, commands over Pub/Sub) and **Redshift** (GKE agent: `UNLOAD` compressed to S3, Storage Transfer Service to Cloud Storage, load; CSV with ASCII 0 fails) migrations; the open-source data-migration tool then compares aggregates source vs target into reporting tables.

**Storage Write API.** Streams: **default** (at-least-once, highest throughput, no stream quota), **committed** (visible on ack; **exactly-once when the client supplies stream offsets**: the API "never writes two messages that have the same offset within a stream"; retry without an offset can duplicate), **pending** (invisible until `FinalizeWriteStream` + `BatchCommitWriteStreams`, atomic batches), **buffered**. **CDC**: `_CHANGE_TYPE` `UPSERT | DELETE` and `_CHANGE_SEQUENCE_NUMBER` (1-4 sections of up to 16 hex chars separated by `/`, custom ordering) on a table with a non-enforced `PRIMARY KEY` and `max_staleness` (0 min to 24 h); **CDC requires the default stream** (no offsets, so no exactly-once through the API itself; dedupe is by PK plus sequence number); the pseudocolumns are not queryable; a query that triggers a runtime merge on a partitioned table scans the whole table; exports skip unapplied changes (use `EXPORT DATA`); `@@max_staleness_override` per query.

**migkit.** BigQuery side exists (`warehouse.py:196`), through load jobs after a key delete, no Storage Write API, no `_CHANGE_TYPE` upserts, no offsets. The exactly-once shape migkit already keeps on the tail (batch number, target-side mark) maps directly onto committed streams with offsets for bulk and onto `_CHANGE_SEQUENCE_NUMBER` for CDC; see the table.

---

## 10. Azure DMS for SQL Server: Log Replay Service and MI link

**LRS (what DMS uses to Managed Instance and SQL on VM).** Backups (`FULL`, `DIFF`, `LOG`, ideally `WITH CHECKSUM`; `TO URL` on 2016+) land in a Blob container, one database per folder at the container root; LRS reads only the **file headers** to build the backup chain (no naming convention), the target's restore service builds a **restore plan** skipping irrelevant files, restores `WITH NORECOVERY`, and in **continuous** mode keeps polling the container and restoring new differentials and logs by LSN; **autocomplete** mode restores through a named last file. Cutover: stop the workload, take and upload the tail-log backup, verify it restored, trigger cutover; the last backup is restored `WITH RECOVERY`; the process is final (no more differentials). Databases are unreadable while restoring. LRS is the only way to apply differentials on Managed Instance. Backup integrity is verified explicitly when `CHECKSUM` is absent. Works from SQL Server 2008 to 2022; direct use via `Start-AzSqlInstanceDatabaseLogReplay` / `az sql midb log-replay start`.

**MI link (distributed availability group).** SQL Server 2016+ (2019 CU15+/2022): a distributed AG between an on-prem AG (a single node without WSFC is enough) and the managed instance, automatic seeding (trace flag 9567 compresses the seed stream), near-real-time replication, read-only testing on the MI before cutover, seconds-to-minutes cutover, and on 2022 fail-back and MI-to-SQL-Server direction; transactional-replication publishers must be reconfigured after.

**migkit.** SQL Server is verify/repair plus Change Tracking; no backup-chain or AG driver. Google DMS (7.3) uses the same backup-chain idea. This is the one engine where migkit's "wrap the native tool" rule points at `BACKUP ... TO URL`/`RESTORE ... WITH NORECOVERY` chains rather than at row copying; see the table.

---

## 11. Azure Database for PostgreSQL migration service (pgcopydb)

**Mechanism.** A hosted **pgcopydb**: offline = `pgcopydb clone` (tables over 20 GB with an int/bigint PK are split and copied in parallel; published offline numbers on D4ds_v4: 1 TB in 7 h); online = `pgcopydb follow` with `pgoutput` (or `test_decoding` below PG 10) from a slot per database (`max_replication_slots > databases`, `max_wal_senders >= slots`). **What the service adds**: a **premigration validation** step (`Validate` / `Validate and migrate`, a rule set for extensions, collations, versions, server parameters, keyless tables) that must reach `Succeeded`; a **migration runtime server** (a separate Flexible Server instance used as a jump host into a private network); a **cutover trigger** (`az postgres flexible-server migration update --cutover`) that applies everything pending and completes, including with non-zero latency ("all the changes made to data in the last 15 minutes are applied to the target"); a **7-day lifetime** per migration and a **72-hour cutover window** after the base copy; guidance that keyless tables get INSERTs only unless `REPLICA IDENTITY FULL`; a warning that the base copy generates target WAL that needs archiving. Sources: on-prem/VM, RDS, Aurora, AlloyDB, Google Cloud SQL, Azure Single Server.

**migkit.** `pgcopydb` is wrapped for the bulk (`movers.py:2727`; the exported snapshot noted at `:2805`), migkit's own or native logical replication for the tail (not `pgcopydb follow`), `assess` for the validation step (1.8), `tunnel.py` for the private-network path (SSH legs, SSM/IAP commands, several connections sized by RTT), the fenced `check` plus `sync --kind sequences` plus `move --mode cdc --drop` for the cutover (`docs/cutover.md`). Missing relative to Azure: nothing mechanical; migkit has no lifetime limits.

---

## 12. Azure MySQL paths

**DMS "Replicate changes" / online.** Streams **binlog row events** and applies them as `BINLOG` statements on 8.0 targets, or translated to INSERT/UPDATE/DELETE on 5.7 (no privilege for `BINLOG`); schema by `MySqlConnector`; ROW format only; binlog retention must outlast the migration; DDL now replicated for selected objects on 5.7/8.0 targets (not `CREATE TABLE ... AS SELECT`); no mixed-case databases; no renames; cutover when `SHOW MASTER STATUS` on the source equals the applied position. The older two-step form took the binlog coordinates from an offline run with "Enable Transactional Consistency".

**Data-in replication.** The Flexible Server as a native replica of an external MySQL (binlog position or GTID; GTID required with HA), seeded by `mydumper`/`myloader`; `mysql` system database, accounts and grants not replicated; HA must be disabled on the target while it is a replica.

**migkit.** Both shapes: migkit's tail (row events applied as statements, with runs and lanes) and the native replica set up by `replicate_sql` (`mysql.py:5672`), plus users and grants carried (`users.py`), which Data-in replication does not.

---

## 13. Azure Data Factory copy activity

**Mechanism.** **DIUs** (4-256, CPU+memory+network of one unit; "Auto" picks by source-sink pair and data pattern) on the Azure IR; on a self-hosted IR scale up or out to 4 nodes and one copy partitions its file set across them. **parallelCopies** is orthogonal to DIUs: file-level for file stores (chunking inside a file is automatic), partition-level for partition-enabled sources (default 4, max = partitions). **Partition options** for SQL sources: `None`, **Physical partitions of table** (one thread per physical partition, "Degree of copy parallelism" default 20, max 50), **Dynamic range** (an int or date/datetime column, optional upper/lower bound used only for the stride, the query must contain `?DfDynamicRangePartitionCondition`; for PostgreSQL `?AdfTabularPartitionName` / `?AdfRangePartitionColumnName`), and the number of generated queries equals the parallel copies used. **Staged copy**: source to a staging Blob/ADLS first (compressed on-prem over 443, no port 1433 to the sink), then to the sink, needed for PolyBase/COPY loads; not between two self-hosted IRs; data consistency verification unsupported through staging. **Fault tolerance**: abort, or skip incompatible rows (type mismatch, PK violation: "copies only the first row" of a duplicate key, the rest skipped), skipped rows redirected to Blob/ADLS with a session log (`rowsCopied`, `rowsSkipped`, `redirectRowPath`); binary files: skip missing or forbidden files. **Data consistency verification** (`validateDataConsistency`): for files, size + `lastModifiedDate` + MD5 (block-level checksums via the Blob/ADLS APIs); for tables, only that rows read equals rows written plus rows skipped; not for FTP/SFTP/HTTP/Snowflake/Office 365/Databricks Delta; only with `PreserveHierarchy`. **Change Data Capture resource**: a continuously running, latency-driven job (billed as 4 v-cores of data flow while running) over native SQL CDC tables or watermark columns, net changes only, one use per source/target mapping, no complex types.

**migkit.** Equal-row ranges instead of stride ranges (1.1), self-sized concurrency (`sizing.py`), the relay beside the source reading compressed inside SSH (`postgres.py:7140 _relay`, measured 7.1 s to 4.3 s over a throttled link; R17d) and multi-connection tunnels (`tunnel.py`) as the "staged copy over a slow link" equivalent without a staging store, verify-as-it-lands by key and digest rather than a row count (`hetero.py:1328`), and *no* skip-incompatible-rows mode by design: a batch that reads back different is rewritten once and then the copy stops and names the key (`hetero.py:1360`). ADF's redirect-and-continue with a log is a legitimate mode for lake targets; see the table for whether to add it as an explicit, off-by-default option.

---

## 14. Azure Database for PostgreSQL Flexible Server PITR

**Mechanism.** Daily **snapshot backups of the data files** plus WAL archived as segments fill; retention 7 (default) to 35 days; ZRS or LRS by region. Restore always creates a **new server** (no in-place): **Latest restore point** (latest snapshot + all WAL), **Custom point in time** (`--restore-time` ISO 8601 UTC: the newest snapshot before the point, then WAL replay to it), **Fast restore** (a full backup only, no WAL replay; also on-demand backups). Restore time "depends on the size of data and the amount of recovery"; a 2 TB PG 16 PITR reported at 7+ hours with no progress visibility. Backups cannot be exported or used outside the service.

**migkit.** `snapshot_state` / restore points and `rollback` are logical (sequences, keys, undo files: `postgres.py:5619`, `mysql.py:1334`, `revert.py`), not storage snapshots. A cloud PITR is a bulk rung for R19 lever 1 only where the *source* is the one restored (a clone to migrate from); as a target-side safety net it is the operator's, and `docs/cutover.md` step 9 keeps migkit's own restore point instead.

---

## 15. Snowflake

**Stages and COPY INTO.** Internal (user `@~`, table `@%t`, named) or external stages (S3/GCS/Azure, storage integrations); `PUT` uploads (client-side encryption, gzip); `COPY INTO <table>` loads files in parallel across the warehouse; **load metadata per table for 64 days** (path, size, last-modified; a matching file is skipped, so a re-run is idempotent; files older than 64 days are "uncertain" and skipped unless `LOAD_UNCERTAIN_FILES=TRUE`; `FORCE=TRUE` reloads and duplicates; dropping or recreating the table clears the metadata). `COPY_HISTORY` records every load. **Snowpipe** (`CREATE PIPE ... AUTO_INGEST`, cloud event notifications or REST `insertFiles`): serverless, per-file, **pipe-level metadata for 14 days by filename** (a modified file with the same name is ignored; `CREATE OR REPLACE PIPE` clears it; mixing COPY and Snowpipe on the same path duplicates; a pipe paused over 14 days is stale). **Snowpipe Streaming**: **Named Channels** with **offset tokens** give ordered, **exactly-once** ingestion per channel (Snowflake's committed token is the source of truth; replay from it on recovery; ordering only within a channel; reopen on schema change), **Elastic Channels** (GA Sept 2026) at-least-once without ordering or tokens; rowsets buffered server-side; "up to 20 GB/s per table, ingest-to-query as low as 5 s"; classic `MAX_CLIENT_LAG` trades latency for partition size. **Streams and tasks**: a stream is a per-table offset exposing `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID` between two transactional points; it is consumed only by a DML that reads it (typically `MERGE ... USING stream`), atomically; an unconsumed stream extends the table's retention up to 14 days and then goes **stale and unrecoverable** (`SHOW STREAMS ... STALE_AFTER`); a task on a cron with `WHEN SYSTEM$STREAM_HAS_DATA` runs the MERGE; DAGs of tasks. **MERGE** is the idempotent upsert primitive. **Openflow connectors** (PostgreSQL/MySQL/SQL Server, NiFi on Kubernetes, replacing the deprecated agent-based connectors): schema introspection, snapshot by `COPY`, then a `pgoutput` slot (created with `failover=true` on PG 17+) streamed through Snowpipe Streaming into **journal tables**, merged into destination tables by a scheduled `MergeSnowflakeJournalTable` (soft deletes via `_SNOWFLAKE_DELETED`, dropped source columns renamed not dropped); journal tables kept forever and re-versioned on schema change; identity key required (PK, unique index with `USING INDEX`, or `REPLICA IDENTITY FULL`) or INSERTs only; 16 MB per value (128 MB with a flag); single-node runtime; changes between dropping a slot and re-snapshotting are absorbed into the snapshot.

**migkit.** Snowflake side through the connector (`warehouse.py:131`), rows as delete-then-insert per key; no stage/`COPY INTO`, no Snowpipe Streaming, no MERGE, no stream/task consumption. The 64-day file metadata and per-channel offset tokens are the two idempotence primitives migkit's exact-batch design would use; the staged Parquet bulk path (item 33) is the missing piece for both Snowflake and Redshift.

---

## 16. Amazon Redshift COPY

**Mechanism.** `COPY ... FROM 's3://prefix' IAM_ROLE ...` reads files in parallel across all **slices** (2 per dc2.large, 16 per dc2.8xlarge, RA3 by node size); file count should be a multiple of slice count, 1 MB-1 GB after compression, roughly equal sizes because Redshift ignores file size when dividing; uncompressed CSV and Parquet/ORC 128 MB+ are split automatically, gzip'd files are not; **one COPY per table** (several concurrent COPYs on one table serialise and then need `VACUUM`); manifests for explicit lists; `SYS_LOAD_HISTORY`/`SYS_LOAD_DETAIL` record files, sizes and rows; `COPY JOB` with S3 event integration auto-loads new files. DMS to Redshift is this COPY behind CSV staging (1.11); zero-ETL bypasses it (5).

**migkit.** Redshift side through the PostgreSQL driver with row statements (`warehouse.py:118`); no S3 staging + COPY; Parquet writer exists (`engines/parquet.py`) so the staged path is a composition, not new ground.

---

## 17. Cross-cutting patterns the clouds share, and migkit's position

1. **Static, human-chosen segmentation** (DMS `boundaries`, ADF stride, pglogical replication sets by size) versus migkit's equal-row ranges from `row_number()` plus live pacing. migkit is ahead; the gap is multi-column/text keys and partition-aware splitting.
2. **Net-change apply through a staging table on the target** (DMS `awsdms_changes`, Snowflake journal tables, BigQuery background merges) versus migkit's in-process collapse and session temp table. Same algorithm, no footprint in the user's database.
3. **Lag-aware validation** (DMS `RecordFailureDelayInMinutes`, `ValidationQueryCdcDelaySeconds`, retries) versus migkit's position fence. migkit's is deterministic where a position exists; DMS's is a timer.
4. **Truncating LOB defaults** (DMS limited mode) versus migkit's measured sizes and no truncation.
5. **At-least-once with dedupe keys** (Datastream uuid, Snowpipe filename, BigQuery default stream + PK) versus migkit's exact batch. Only Snowpipe Streaming Named Channels and Storage Write API committed streams with offsets offer the same; migkit should write to those APIs with its batch number as the token/offset.
6. **Physical seeding** (Aurora clone + `aurora_volume_logical_start_lsn`, RDS snapshot + seed LSN, LRS backup chains, zero-ETL storage seeding) is where the clouds are genuinely faster. migkit's R19 lever 1 is the answer and is unbuilt.
7. **Source footprint**: DMS's event trigger and audit table, heartbeat table, `awsdms_control` schemas; Tencent's `__tencentdb__`; Openflow's journal tables. migkit writes nothing but a slot/publication (and an origin table only for two-way) and counts everyone else's leftovers (`leftovers.py`).
8. **Runtime limits**: Azure's 7-day/72-hour windows, DMS validation's 10,000-failure stop, BigQuery's 1,500 loads/table/day, Snowflake's 14/64-day metadata. Any warehouse path migkit builds must size batches against the 1,500/day quota and use the 64-day metadata for idempotence rather than fight it.

---

## 18. Mechanism table

| Mechanism | Service | What it gives | migkit status | How migkit builds it better | Effort | Testable in docker |
|---|---|---|---|---|---|---|
| Parallel load by ranges/partitions (`MaxFullLoadSubTasks`, `ranges`, `partitions-auto`) | AWS DMS | Intra-table parallelism | Built: equal-row ranges, paced workers (`ranges.py:222`, `hetero.py:1205`, `sizing.py:204`) | Add multi-column/text keys by quantile and native partitions as ranges; keep pacing | M | Yes (PG/MySQL partitioned tables) |
| Auto-segmentation, DCU auto-scaling | DMS Serverless | No operator sizing | Built for concurrency (`sizing.py:126`); no scale-out | Scale-out across machines (backlog 31) with the same pace signal | L | Partly (compose with 2 workers) |
| `CommitRate`, `ParallelLoadThreads` | AWS DMS | Commit granularity | Built as batch/range size (`ranges.py:322`) | Nothing to add | - | - |
| Limited / full / inline LOB modes | AWS DMS | Speed vs truncation trade | Built as measurement, never truncation (`postgres.py:2979-3060`); PG large objects in pieces (`:2557`) | MySQL/Oracle chunked reads (R10); a size-driven time estimate | M | Yes |
| `TargetTablePrepMode`, `CreatePkAfterFullLoad`, cached-changes stop points | AWS DMS | Consistent point for FKs/indexes | Built: setup-target, truncate step, index window where measured faster (`movers.py:819`) | R19 lever 8: drop changes to uncopied ranges (DBLog watermark) | M | Yes (interleave tests exist) |
| Batch optimized apply (net changes table) | AWS DMS / Qlik | High-rate CDC | Built in-process (`base.py:3919`, `:3663`), COPY staging (`postgres.py:1144`) | Already better: no `awsdms_changes` table, whole-batch replay not one-by-one fallback; add SQL Server staging (R2.4) | S-M | Yes |
| `BatchApplyPreserveTransaction`, transactional apply order | AWS DMS | Integrity within batch | Built: batch ends at a source commit, lanes only for independent rows (`base.py:3611`) | Nothing to add | - | - |
| `ParallelApplyThreads` | AWS DMS (streams/warehouses only) | Parallel CDC apply | Built for every SQL target by dependency lanes | Writer beside the target (R19 lever 7), pipeline mode (lever 10) | M | Yes |
| Recovery checkpoint / `CdcStartPosition` | AWS DMS | Resume and second targets | Built (`tailctl.py`, origins `postgres.py:6670`) | Nothing to add | - | - |
| Validation partitions, `ThreadCount`, `GROUP_LEVEL` | AWS DMS | Row-level compare | Built and faster: one server-side digest, resumable partial sums (`checkpoint.py`) | Nothing to add | - | - |
| Revalidation on lag (`RecordFailureDelay*`, `ValidationQueryCdcDelaySeconds`) | AWS DMS | No false positives | Built deterministically (`base.py:3194 fenced_recheck`) | SQL Server fence once its change stream exists (P0 1) | M | No (no arm64 SQL Server) |
| `ValidationOnly` tasks, `awsdms_validation_failures_v1/v2`, data resync | AWS DMS | Verify-only, repair failed keys | Built: `check`, drilldown key files, `sync --kind rows` with undo | Add per-column `override-validation-function` equivalent as a hop mapping expression | S | Yes |
| Premigration assessments (per-engine list) | AWS DMS / Serverless | Stop before failing | Built wider (`postgres.py:5639`, `mysql.py:4510`, `leftovers.py`) | Free space via cloud metrics; DocumentDB (R9) | S | Partly |
| Time Travel (change log to S3) | AWS DMS | Post-hoc forensics | Partly: evidence, undo, conflicts, hash-chained audit (`audit.py`, `evidence.py`) | Optional applied-changes journal on the tail, zstd, retention-bounded, age-encrypted | M | Yes |
| Schema Conversion rules + genAI + agent | AWS DMS SC | Cross-engine DDL/code | Built: `canon`/`hetero` conversion, proved by execution (`hetero.py:2315`); AI any provider (`assist.py`) | Widen stored code (R11); keep proof-by-execution, which SC lacks | L | Yes (PG/MySQL) |
| Fleet Advisor collector | AWS DMS | Fleet discovery | Not built; out of scope (hop-centric) | - | - | - |
| Homogeneous migrations (pg_dump/pg_restore, mydumper, mongodump, native replication) | AWS DMS | Native tools, serverless | Built and verified (`movers.py`) | Nothing to add | - | - |
| Source privileges/objects per engine (slot plugin, DDL trigger, heartbeat, MS-CDC) | AWS DMS | CDC prerequisites | Built for PG/MySQL/Mongo; minimal footprint; DDL by catalogue diff | Slot heartbeat on an idle PG source (advance the slot, no table); SQL Server MS-CDC/`fn_dblog` reader (R12) | S / L | Yes / No |
| S3 target (`CdcPath`, date partitions, Parquet) and Redshift via S3 COPY | AWS DMS | Lake and warehouse targets | Parquet side built; warehouse bulk path *still to come* (`warehouse.py:13`) | Staged Parquet then `COPY`/`COPY INTO`/load job, file counts sized to slices and quotas, verified by digest | M | Partly (MinIO for S3; no Redshift) |
| Zero-ETL storage seeding + enhanced binlog/logical replication | AWS | Seconds of lag, no pipeline | Not built; cloud-only mechanism | R19 lever 1 physical rung is the portable equivalent | L | Partly |
| Aurora clone + `aurora_volume_logical_start_lsn` + origin advance; RDS snapshot + seed LSN; MySQL `rds_set_external_source` from crash-recovery position | AWS | Terabytes in minutes, exact continuation | Not built; prerequisites exist (`postgres.py:6670`, `:7498`) | R19 lever 1: slot before copy, physical copy, origin advanced to the copy's end LSN/GTID, refused without the privilege | L | Yes for `pg_basebackup`/`CLONE INSTANCE`; cloud parts no |
| Cross-account/region snapshot copy, lazy hydration | AWS RDS | Fleet moves | Not built | Document as a rung with pre-warm scans; `StorageOperationStatus` polling | S | No |
| pglogical multiple subscriptions, replication sets by size, `pg_restore --jobs` for indexes | Google DMS | Table-level parallel copy + CDC | Built with intra-table parallelism (which pglogical lacks) | Nothing to add | - | - |
| Promotion with non-zero lag, DVT verify | Google DMS | Cutover | Built with a fence and native verify | Nothing to add | - | - |
| Data dump parallelism (LOAD DATA path) | Google DMS MySQL | Fast MySQL load | Measured 36 % faster, set aside (R19 lever 6) | Client that answers `local_infile` only for migkit's chunk | S | Yes |
| SQL Server backup-chain restore (`.bak` + `.trn`, `.trn.final`) | Google DMS / Azure LRS | Online SQL Server migration without log readers | Not built | Drive `BACKUP TO URL` / `RESTORE WITH NORECOVERY` chains by header LSN, verified after `RECOVERY` | L | No |
| Datastream sort keys / CSN, at-least-once + uuid dedupe | Google | Ordered consumption downstream | Built stronger: exact batch (R3) | Emit `sort_keys`-style metadata in `streamout` messages | S | Yes |
| Datastream concurrency controls (backfill 15, CDC 5) | Google | Source load control | Built as pacing + throttle (`throttle.py`) | Nothing to add | - | - |
| Datastream to BigQuery merge mode, `max_staleness` | Google | CDC into a warehouse | Not built (BigQuery writer deletes then loads) | Write CDC through Storage Write API `_CHANGE_TYPE`/`_CHANGE_SEQUENCE_NUMBER` with the batch number as sequence | M | No (emulator lacks Write API CDC) |
| BigQuery load jobs (atomic, 1,500/table/day, 15 TB) | Google | Bulk | Built (per-batch load jobs, quota-unaware) | Batch-size against the quota; `WRITE_TRUNCATE` per partition for idempotence | S | Partly |
| Storage Write API committed stream + offsets (exactly-once) | Google | Exactly-once bulk streaming | Not built | Offsets = migkit batch numbers; pending streams for atomic table loads | M | No |
| LRS continuous restore, header-built chain | Azure SQL MI | Online migration | Not built (see backup-chain row) | Same row | L | No |
| MI link distributed AG | Azure SQL MI | Near-real-time, fail-back | Not built; native-only | Detect and fence on an AG's `last_hardened_lsn` when present | M | No |
| pgcopydb-based service: validation step, runtime server, cutover trigger, 7-day limit | Azure PostgreSQL | Hosted pgcopydb | Built without limits (`movers.py:2727`, `assess`, `tunnel.py`, `docs/cutover.md`) | Nothing to add | - | - |
| Azure PITR (snapshot + WAL replay, fast restore) | Azure PostgreSQL | Point-in-time copy | Not applicable to migkit's role; source-side rung only | Part of lever 1's cloud inputs | - | - |
| ADF DIUs / parallelCopies / physical and dynamic-range partitions | Azure Data Factory | Copy parallelism | Built better (equal rows, pacing) | Nothing to add | - | - |
| ADF staged copy over slow links | Azure Data Factory | Bandwidth | Built as relay + multi-leg tunnel (`postgres.py:7140`, `tunnel.py`) | Binary COPY on both ends (R19 lever 2) | S | Yes (tc netem) |
| ADF fault tolerance: skip incompatible rows, redirect + session log | Azure Data Factory | Continue past bad rows | Deliberately not built (copy stops and names the key) | Optional, off by default, only for file/lake targets, with the skipped keys as evidence | S | Yes |
| ADF data consistency verification (MD5 files, row-count tables) | Azure Data Factory | Post-copy check | Built stronger (per-batch read-back and digest, `hetero.py:1328`) | Nothing to add | - | - |
| Snowflake stages + `COPY INTO` with 64-day load metadata | Snowflake | Idempotent bulk load | Not built | Staged Parquet + `COPY INTO`, relying on load metadata for re-runs, verified by digest | M | No (no Snowflake in docker) |
| Snowpipe (14-day pipe metadata) | Snowflake | Serverless file ingest | Not built | Not needed for migration; `COPY INTO` suffices | - | - |
| Snowpipe Streaming Named Channels + offset tokens | Snowflake | Exactly-once CDC ingest | Not built | Channel per table, token = migkit batch number; the only exactly-once path Snowflake offers | M | No |
| Streams/tasks + MERGE, Openflow journal tables | Snowflake | CDC apply inside the warehouse | Not built | Apply MERGE from a migkit-written journal table with the batch number; drop the journal after verify (Openflow keeps it forever) | M | No |
| Redshift `COPY` from S3 sized to slices, one COPY per table | Redshift | Bulk | Not built | Same staged path as Snowflake; file count from `STV_SLICES`, one COPY per table | M | No |

Effort: S under a day, M days, L a week or more, on the project's measured pace.

---

## Sources

AWS DMS
- Full-load task settings: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.FullLoad.html
- Table and collection settings rules (parallel-load, lob-settings): https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Tablesettings.html
- LOB support: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.LOBSupport.html
- Target metadata task settings: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.TargetMetadata.html
- Change processing tuning settings: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.ChangeProcessingTuning.html
- Batch apply (re:Post): https://repost.aws/knowledge-center/dms-batch-apply-cdc-replication
- Understand and optimize replication for Redshift with DMS (Sorter, net changes table): https://aws.amazon.com/blogs/database/understand-and-optimize-replication-for-amazon-redshift-with-aws-dms/
- Ongoing replication, checkpoints, native start points: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Task.CDC.html
- Data validation: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html
- Data validation task settings: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.DataValidation.html
- Validation-only tasks blog: https://aws.amazon.com/blogs/database/optimize-data-validation-using-aws-dms-validation-only-tasks/
- Validation troubleshooting (re:Post): https://repost.aws/knowledge-center/dms-task-validation-failed-stuck
- Individual assessments: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.Assessments.html
- Assessments for all endpoint types: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.Assessments.All.html
- PostgreSQL / MySQL / SQL Server assessments: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.PG.html , https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.MySQL.html , https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.AssessmentReport.SqlServer.html
- Serverless premigration assessments: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Serverless.Premigrations.html
- Time Travel task settings: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.TimeTravel.html
- Debug DMS tasks using Time Travel: https://aws.amazon.com/blogs/database/debug-aws-dms-tasks-using-time-travel/
- PostgreSQL source: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html
- PostgreSQLSettings API: https://docs.aws.amazon.com/dms/latest/APIReference/API_PostgreSQLSettings.html
- test_decoding vs pglogical: https://aws.amazon.com/blogs/database/comparison-of-test_decoding-and-pglogical-plugins-in-amazon-aurora-postgresql-for-data-migration-using-aws-dms
- MySQL source: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MySQL.html
- RDS binlog retention: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html
- Oracle source: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.Oracle.html
- SQL Server source CDC: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.SQLServer.CDC.html
- S3 target: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html
- S3Settings API: https://docs.aws.amazon.com/dms/latest/APIReference/API_S3Settings.html
- Redshift target: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Redshift.html
- Troubleshooting (TRUNCATE, DDL artifacts): https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Troubleshooting.html
- Serverless components: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Serverless.Components.html
- Enhanced full load in Serverless: https://aws.amazon.com/blogs/database/enhanced-full-load-performance-in-aws-dms-serverless/
- Serverless scale-down conditions (re:Post): https://repost.aws/questions/QUNLDrXVHqQZGAWa3ucCmV2g/dms-serverless-task-is-not-scaling-down
- DMS pricing (DCU): https://aws.amazon.com/dms/pricing/
- Homogeneous migrations: https://docs.aws.amazon.com/dms/latest/userguide/data-migrations.html , https://docs.aws.amazon.com/dms/latest/userguide/dm-migrating-data-postgresql.html , https://docs.aws.amazon.com/dms/latest/userguide/dm-migrating-data-mysql.html , https://docs.aws.amazon.com/dms/latest/userguide/dm-migrating-data-mongodb.html
- Schema Conversion with generative AI: https://docs.aws.amazon.com/dms/latest/userguide/schema-conversion-convert.databaseobjects.html , https://aws.amazon.com/blogs/aws/aws-data-migration-service-improves-database-schema-conversion-with-generative-ai/
- Schema Conversion AI agents: https://docs.aws.amazon.com/dms/latest/userguide/sc-genai-agents.html , https://aws.amazon.com/blogs/database/accelerate-database-modernization-with-agentic-ai-in-aws-dms-schema-conversion/
- Fleet Advisor: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_FleetAdvisor.html , https://docs.aws.amazon.com/dms/latest/userguide/fa-data-collectors.html

AWS zero-ETL, Aurora, RDS
- Aurora zero-ETL: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/zero-etl.html
- Redshift zero-ETL considerations: https://docs.aws.amazon.com/redshift/latest/mgmt/zero-etl.reqs-lims.html
- Aurora PostgreSQL zero-ETL GA: https://aws.amazon.com/blogs/database/amazon-aurora-postgresql-zero-etl-integration-with-amazon-redshift-is-generally-available/
- Aurora fast cloning: https://aws.amazon.com/blogs/aws/amazon-aurora-fast-database-cloning
- Cross-account Aurora PostgreSQL sync (aurora_volume_logical_start_lsn): https://aws.amazon.com/blogs/database/amazon-aurora-postgresql-cross-account-synchronization-using-logical-replication
- RDS to Aurora with seeded logical replication: https://aws.amazon.com/blogs/database/migrating-amazon-rds-for-postgresql-to-amazon-aurora-using-seeded-logical-replication/
- Aurora major upgrade via logical replication: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.MajorVersionUpgrade.html
- Aurora MySQL binlog replication set-up: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Replication.MySQL.SettingUp.html
- Aurora MySQL enhanced binlog: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Enhanced.binlog.html , https://aws.amazon.com/blogs/database/introducing-amazon-aurora-mysql-enhanced-binary-log-binlog
- Aurora MySQL 8.4.8 release notes: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraMySQLReleaseNotes/AuroraMySQL.Updates.848.html
- Cross-account Aurora MySQL migration with cloning + binlog: https://aws.amazon.com/blogs/database/cross-account-amazon-aurora-mysql-migration-with-aurora-cloning-and-binlog-replication-for-reduced-downtime/
- RDS snapshot copy: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CopySnapshot.html
- RDS snapshot sharing: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ShareSnapshot.html
- Cross-Region + cross-account single-step copy (Sept 2025): https://aws.amazon.com/about-aws/whats-new/2025/09/amazon-rds-cross-region-cross-account-snapshot-copy/
- Snapshot restore and lazy loading: https://aws.amazon.com/blogs/database/amazon-rds-snapshot-restore-and-recovery-demystified/ , https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RestoreFromSnapshot.html
- Redshift COPY from S3: https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html , https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-use-multiple-files.html , https://docs.aws.amazon.com/redshift/latest/dg/t_splitting-data-files.html , https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-single-copy-command.html

Google Cloud
- DMS PostgreSQL known limitations: https://docs.cloud.google.com/database-migration/docs/postgres/known-limitations
- DMS PostgreSQL configure source: https://cloud.google.com/database-migration/docs/postgres/configure-source-database
- DMS PostgreSQL migration types: https://docs.cloud.google.com/database-migration/docs/postgres/migration-types
- DMS PostgreSQL promote: https://docs.cloud.google.com/database-migration/docs/postgres/promote-migration
- DMS PostgreSQL create job (parallelism): https://cloud.google.com/database-migration/docs/postgres/create-migration-job
- Best practices PostgreSQL to Cloud SQL with DMS (multiple subscriptions): https://cloud.google.com/blog/products/databases/best-practices-for-migrating-postgresql-to-cloud-sql-with-dms
- DMS MySQL create job (dump flags, parallelism): https://docs.cloud.google.com/database-migration/docs/mysql/create-migration-job
- DMS MySQL known limitations: https://docs.cloud.google.com/database-migration/docs/mysql/known-limitations
- DMS MySQL mysqldump: https://docs.cloud.google.com/database-migration/docs/mysql/mysql-dump
- DMS SQL Server overview and backups: https://docs.cloud.google.com/database-migration/docs/sqlserver/scenario-overview , https://docs.cloud.google.com/database-migration/docs/sqlserver/export-backup-files
- DMS Oracle to PostgreSQL conversion workspaces: https://docs.cloud.google.com/database-migration/docs/oracle-to-postgresql/create-conversion-workspace , https://docs.cloud.google.com/database-migration/docs/oracle-to-postgresql/legacy-conversion-workspaces
- Verify a migration (DVT): https://docs.cloud.google.com/database-migration/docs/oracle-to-alloydb/verify-migration
- Datastream events and streams: https://docs.cloud.google.com/datastream/docs/events-and-streams
- Datastream BigQuery destination: https://docs.cloud.google.com/datastream/docs/destination-bigquery
- Datastream configure BigQuery destination (write modes, staleness): https://docs.cloud.google.com/datastream/docs/configure-bigquery-destination
- Datastream concurrency controls: https://docs.cloud.google.com/datastream/docs/stream-concurrency-controls
- Datastream best practices: https://docs.cloud.google.com/datastream/docs/best-practices-general
- Datastream backfill: https://docs.cloud.google.com/datastream/docs/manage-backfill-for-the-objects-of-a-stream
- Datastream Oracle / SQL Server / PostgreSQL / MySQL sources: https://docs.cloud.google.com/datastream/docs/sources-oracle , https://docs.cloud.google.com/datastream/docs/sources-sqlserver , https://docs.cloud.google.com/datastream/docs/sources-postgresql , https://docs.cloud.google.com/datastream/docs/sources-mysql
- BigQuery CDC: https://docs.cloud.google.com/bigquery/docs/change-data-capture
- Storage Write API: https://docs.cloud.google.com/bigquery/docs/write-api-grpc , https://docs.cloud.google.com/bigquery/docs/write-api-streaming
- BigQuery batch loading and quotas: https://docs.cloud.google.com/bigquery/docs/batch-loading-data , https://docs.cloud.google.com/bigquery/docs/optimize-load-jobs , https://docs.cloud.google.com/bigquery/quotas
- BigQuery Data Transfer Service: https://docs.cloud.google.com/bigquery/docs/dts-introduction
- Teradata / Redshift migration: https://cloud.google.com/bigquery/docs/migration/teradata , https://docs.cloud.google.com/bigquery/docs/migration/redshift

Azure
- Log Replay Service overview: https://learn.microsoft.com/en-us/azure/azure-sql/managed-instance/log-replay-service-overview?view=azuresql
- Azure DMS architecture: https://techcommunity.microsoft.com/blog/microsoftdatamigration/architecture-of-azure-database-migration-service/4159054
- Azure DMS FAQ: https://learn.microsoft.com/en-us/azure/dms/faq
- Managed Instance link: https://learn.microsoft.com/en-us/azure/azure-sql/managed-instance/managed-instance-link-feature-overview?view=azuresql
- PostgreSQL migration service overview: https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/overview-migration-service-postgresql
- PostgreSQL migration service best practices (pgcopydb, numbers, limits): https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/best-practices-migration-service-postgresql
- PostgreSQL online tutorial (cutover): https://learn.microsoft.com/en-us/azure/postgresql/migrate/migration-service/tutorial-migration-service-iaas-online
- Azure DMS MySQL replicate changes: https://learn.microsoft.com/en-us/azure/dms/concepts-migrate-azure-mysql-replicate-changes
- Azure DMS MySQL online tutorial: https://learn.microsoft.com/en-us/azure/dms/tutorial-mysql-azure-external-to-flex-online-portal
- Data-in replication: https://learn.microsoft.com/en-us/azure/mysql/flexible-server/concepts-data-in-replication
- ADF copy performance features: https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-performance-features
- ADF copy fault tolerance: https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-fault-tolerance
- ADF data consistency verification: https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-data-consistency
- ADF SQL partition options: https://learn.microsoft.com/en-us/fabric/data-factory/connector-azure-sql-database-copy-activity
- ADF CDC resource: https://learn.microsoft.com/en-us/azure/data-factory/concepts-change-data-capture-resource
- Azure PostgreSQL backup and restore: https://learn.microsoft.com/en-us/azure/postgresql/backup-restore/concepts-backup-restore
- Azure PostgreSQL fast restore: https://learn.microsoft.com/en-us/azure/postgresql/backup-restore/how-to-restore-full-backup

Snowflake
- Load metadata (64 days): https://docs.snowflake.com/en/user-guide/data-load-considerations-load
- Snowpipe intro / manage / troubleshoot: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro , https://docs.snowflake.com/en/user-guide/data-load-snowpipe-manage , https://docs.snowflake.com/en/user-guide/data-load-snowpipe-ts
- Snowpipe Streaming overview and Named Channels: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview , https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-channels
- Elastic Channels GA (Sept 2026): https://docs.snowflake.com/en/release-notes/2026/other/2026-09-15-snowpipe-streaming-elastic-channels-ga
- Streams and tasks: https://docs.snowflake.com/en/user-guide/streams-intro , https://docs.snowflake.com/en/user-guide/data-pipelines-intro
- Openflow Connector for PostgreSQL: https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/postgres/about , https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/postgres/maintenance , https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/postgres/failover
- Openflow Connector for MySQL: https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/mysql/about
- Legacy connector concepts (snapshot, journal, merge): https://docs.snowflake.com/en/connectors/db-connector-concepts
