# Mechanisms: the Asian-cloud DTS products and the distributed-SQL migration toolkits (research as of 2026-09-28)

**Status of this pass (written before the owner's token window closed):** every section (1-17) is complete against the sources listed. Partial only where the vendor publishes nothing: Alibaba's hot-merge algorithm and the exact contents of its `dts` schema (1.2, 1.4, marked *inferred*), Tencent's `ConditionCover` operator vocabulary (2.4), Huawei's "patented" comparison algorithm (3), Dumpling's row-estimate SQL (7, not fetched from source), PeerDB's per-partition snapshot detail (12, marked *inferred*). Nothing was left unresearched.

Scope: the internal mechanisms of Alibaba Cloud DTS, Tencent Cloud DTS, Huawei DRS, TiDB DM, TiDB Lightning, sync-diff-inspector, Dumpling, Vitess (VReplication, VDiff v2, MoveTables, Online DDL), PlanetScale imports, CockroachDB MOLT (Fetch, Verify, Replicator), YugabyteDB Voyager, ClickHouse ClickPipes/PeerDB, SingleStore CDC-in, the Spanner migration tool, Neon and Supabase import tooling. Public documentation and source only. For each: the problem, the exact mechanism, the limits and failure modes, and migkit's status with `file:line` in this repository. Statements I could not confirm from a primary page are marked *(inferred)* or *(not documented publicly)*.

migkit line references are to the working tree on 2026-09-28. Backlog references are to `docs/backlog.md`: P0 decision layer (83-135), R2 applier (3594-3661), R3 two-way and the rung ladder decided 2026-09-27 (3663-3802), R19 faster-without-losing-a-row (4463-4563).

---

## 1. Alibaba Cloud DTS

### 1.1 Full migration: three modules, slices, source and sink threads

**Problem.** Copy a live table in parallel without a global lock, and keep the destination's storage close to the source's.

**Mechanism.** Full migration is three modules: a database/table capture module (reads the dblist and both sides' table structures), a table slicing module (divides each table into fixed-size slices by walking the primary key in order - the worked example scans the key until the filtered rows reach 1,000) and a slice scheduling module (several source threads pull one slice each into an in-memory queue inside DTS; several sink threads consume the queue and write). The incremental reader is started *at the beginning* of the full phase; the parsed, reformatted changes are stored locally on the DTS server and applied after the full phase. Slice size and rows-per-batch are tuned from the internal console ("Numen") by support, not by the user. Concurrent writes make the destination 5-10% larger than the source.

**Limits.** No primary key or unique index: slicing degrades, and DTS "may write duplicate records". Slicing raises the source's IOPS and outbound bandwidth. The three-module design is described in a community blog, not a spec.

**migkit.** Same shape, one difference. Key ranges are planned once and kept in the checkpoint entry so a restart copies the same ranges (`migkit/ranges.py:222` `plan`, `:342` `split`); the edges come from `bounds_sql` - a `row_number() over (order by key)` walk that returns every N-th key (`migkit/ranges.py:330`), which is a full index scan on the source before the first byte moves. PostgreSQL 14+ also splits by heap pages from the catalogue with no scan (`migkit/engines/postgres.py:714` `position_spans`, `ctid` ranges from `relpages`/`reltuples`). A table with no key goes as spans in one transaction each, and a stop is settled by the target's count (`migkit/ranges.py:260` `spans_to_copy`) - DTS's "may write duplicates" is exactly the case this refuses. Workers are auto-sized and cut back under source stress (`migkit/throttle.py:70`, `migkit/sizing.py:126`), where DTS's slice threads are a fixed number. Gap: a **statistics-based split** (MySQL `information_schema`/histograms, PostgreSQL `pg_stats.histogram_bounds`) so the edges cost nothing on the source - see MOLT Fetch (10.1) and sync-diff-inspector (6.1).

### 1.2 Incremental: hot-row merge (`trans.hot.merge.enable`) and hotspot reporting

**Problem.** A row updated far more often than the applier can write it makes the whole stream wait on one key. The performance white paper measured the hot-row workload stuck at 1,200 RPS until `trans.hot.merge.enable=true`, after which it reached the instance-class cap.

**Mechanism.** The parameter is off by default and set per instance. *(inferred)* It merges the queued changes to one primary/unique key into the last state before writing. The console shows "Hotspot Table Information" as `db.table:pk,uk:conflict depth`, where conflict depth is the number of incremental changes still to be applied to that key (>= 1), refreshed only after the consumption checkpoint moves. The FAQ's answer to latency above 1,000 ms is: look at the hot data, then turn the merge on. The algorithm itself is not documented.

**migkit.** Built, on by default: a batch is collapsed to one write per key, later values merged over earlier, a delete in between restarting the row, a key change leaving the old address and arriving at the new (`migkit/engines/base.py:3917` `_collapsed`); the collapsed rows go as one run of deletes and one of upserts per table where nothing orders them (`:3656` `_net_rows`), and in parallel lanes by key hash from 2,000 changes (`:3607` `LANES_FROM`, `:3609` `_lanes`). Measured in R2: 320,000 changes 21.8s to 4.9s. Missing: the **hotspot readout** - the tail's `watch_sample` (`migkit/engines/hetero.py:3145`) does not report which keys collapse most and how deep. Cheap to add from `_collapsed`'s counts.

### 1.3 Two-way sync: loop prevention, conflict detection, conflict policies

**Problem.** Two databases each replicating the other must not echo a change back, and a row changed on both must be decided.

**Loop prevention.** While a two-way task runs, DTS creates a database named `dts` in the destination of both the forward and the reverse task; the task account needs read/write on it. It also periodically runs `CREATE DATABASE IF NOT EXISTS test` on the source to advance the binlog offset, and updates `dts_health_check.ha_health_check` as a heartbeat. *(inferred)* The reverse task recognises transactions that touch `dts` as its own and leaves them out - the marker-table design. Initialization rule: only one direction may do schema + full sync; the other is incremental only. DDL: forward direction only; the reverse task filters DDL out; a "switch direction" operation moves the DDL direction, and distributed two-way instances cannot switch.

**Conflict detection (MySQL two-way).** Three kinds: (1) INSERT with a key already present - uniqueness conflict; (2) UPDATE whose row is missing in the destination (converted to INSERT, which may then conflict) or whose new key conflicts; (3) DELETE of a missing row - always ignored, whatever the policy. Precheck requires `binlog_format=row`, `binlog_row_image=full`, keys (or Exactly-Once write), `log_slave_updates` on dual-primary clusters, binlog retention >= 3 days.

**Policies.** Global: `TaskFailed` (stop), `Ignore` (skip the statement, keep the destination row), `Overwrite` (destination row replaced by the source's). Per-table "independent" policies on MySQL/PolarDB-MySQL add `UseMax` and `UseMin` (compare the two records, keep the larger/smaller value); independent policies override the global one; MongoDB supports `Ignore` only; policies act in the incremental phase only, and after a pause/restart the destination is overwritten by default. The docs say plainly that no policy prevents two nodes updating one key at once.

**migkit.** Loop prevention: `migkit_origin`, a row per applying thread written first in every applied transaction, and the reader the other way drops the whole transaction (`migkit/twoway.py:44` `TABLE`, `:115` `thread_origin`; readers in `migkit/engines/mysql.py:702-704`, `migkit/engines/postgres.py:1195-1197`). The table is the bottom rung of the R3 ladder (backlog 3734-3802): replication origin / logical message on PostgreSQL, tagged GTID / `skip_replication` / statement comment on MySQL and MariaDB, each proved by a probe before it is trusted and chosen per side by measurement. DTS has one rung (the `dts` database) on both. Conflicts: `insert_exists`, `update_missing`, `update_origin_differs`, `delete_origin_differs` from the before image (`migkit/twoway.py:217` `resolve`), policies `error`, `apply_remote`, `keep_local`, `last_update_wins` by a named column with rank breaking ties, `source_priority`, and per-column `delta` counters that add instead of deciding (`:49` `POLICIES`, `:354` `_decide`, `:327` `_as_added`); every decision written with both versions to `conflicts.jsonl` (`:383` `_record`). Gaps against DTS: **per-table policy overrides** (migkit's policy is per hop); **UseMax/UseMin** are a special case of `last_update_wins` on a column but not spelled as such; **DELETE of a missing row** is `delete_missing` in PostgreSQL 18's naming and R3 lists it, but `resolve` today only names `delete_origin_differs` - a delete of a missing row falls through as no conflict, which matches DTS's "always ignored" and should be said in the log. Better than DTS: migkit's `error` default and the conflict file mean a conflict is never silently overwritten; DTS overwrites after every restart.

### 1.4 Exactly-Once write (tables without keys)

**Problem.** A keyless table replayed after a retry gets duplicate rows.

**Mechanism.** The task creates a `dts` database/schema in the destination holding "transactional tables" that must not be touched; *(inferred)* the position is committed in the same transaction as the rows, so a replay after a restart knows what landed. Requires GTID mode on a self-managed MySQL source; the full phase does a full scan to build a snapshot and *locks keyless tables* on MySQL/PolarDB-MySQL sources; not available on serverless billing or Limitless PolarDB; the reverse task inherits the setting. Data-clearing DDL (DROP/TRUNCATE) with an incremental-writer restart can still lose data.

**migkit.** For the copy: keyless spans committed one per transaction, the stop settled by the target's count, and a restart that cannot settle empties the table and says so (`migkit/ranges.py:260-297`), no lock on the source. For the tail: `exact` batches carry their batch number in the `migkit_origin` mark and the tail resumes after the batch the target last committed (`migkit/twoway.py:134` `batch_seen`, `:150` `committed_ahead`; `migkit/engines/hetero.py:2672` `_committed_ahead`) - today only for hops with counters or two-way; the R3 rung ladder makes every rung "exact" where it can. Gap: exact batches on a plain one-way tail are not on by default; idempotent upserts cover keyed tables, and a keyless table in the tail is a lane barrier - its exactness after a stop rests on the position being saved after the commit (`failpoint.hit("range.saved")` window in `migkit/ranges.py:242`). Making `exact` the default rung for one-way tails closes it (R19.11).

### 1.5 Data verification: schema, full, incremental; sampling; row count

**Mechanism.** Three types: schema verification, full data verification, incremental data verification. Full has two methods: *full field validation by row sampling* - a sampling percentage from 10 to 100, all fields of the sampled rows compared, charged by data verified - and *row count only* (free). Incremental verification starts after the instance's replication latency first reaches zero, and verifies only rows the incremental task changed, never rows changed by hand on the destination. Rate limits: max rows/s and MB/s read by full verification (0 = unlimited). Rules: a table with neither primary key nor unique index and more than 10,000 rows is skipped; a table with more than 100,000 inconsistent rows stops its verification; sampling plus ETL flags ETL-modified rows as inconsistent; multi-table merge instances cannot be verified; pause/restart restarts full verification; MongoDB's "sampling" is fixed at 100%.

**Limits.** Sampling below 100% is a statistical statement, not a proof; the verify is a separate billed reader with its own connections; the check does not see DDL.

**migkit.** Full verification is a digest per table, both sides, no rows across the link (`migkit/engines/mysql.py:1720` `_checksum`: count, `sum(crc32)`, summed 32-bit md5 prefix; `migkit/engines/postgres.py:6313` `_row_hash_expr`, name-sorted columns, geometry canonicalised), restartable by primary-key range with a fingerprint that discards partials when the plan changes (`migkit/checkpoint.py:51` `Checkpoint`, `:39` `fingerprint`), row counts merged into the same pass (`mysql.py:1599-1690`). Incremental verification is `delta_verify` - only rows changed since the last clean proof (`migkit/engines/hetero.py:1934`, `postgres.py:6907`, `mysql.py:2074`), and `unchanged.py` skips tables whose marker (tuple counters + relfilenode) has not moved except at the final proof. A difference is confirmed against the replication position before it is called one (`migkit/engines/base.py:3223` `_resolve_inflight`, `:3194` `fenced_recheck`; `hetero.py:1902` `fence_wait`). **Sampling is deliberately absent** - nothing in `migkit/engines/base.py` samples rows for a verdict (its only sampling is of text values for encoding diagnostics, `:1050-1138`); the digest is the whole table at less cost than DTS's sample. Keyless tables are digested whole rather than skipped at 10,000 rows. What DTS has that migkit lacks: a **per-verify rate cap in rows/s and MB/s** as a setting - migkit's `Throttle` backs off on measured stress instead (`migkit/throttle.py:70`), which is better on a shared server and worse for an operator who has been told "no more than N MB/s".

### 1.6 ETL and DDL rules

ETL: DSL scripts on migration and sync tasks transform and clean rows only; they cannot create objects (a column added by ETL must be added on the destination by hand or the script does nothing); streaming ETL in DAG mode is incremental-only and same-region. DDL: supported set CREATE/DROP/ALTER/RENAME/TRUNCATE, narrowed per pair (AnalyticDB MySQL: CREATE/ALTER/DROP TABLE only; an unsupported DDL stops the task, it cannot be skipped); RENAME TABLE of a single-table object loses the table (select the whole database); gh-ost/pt-osc on the source during sync are forbidden (use DMS); DDL during schema/full sync is forbidden; DDL is 68 ops/s in the white paper.

**migkit.** Rules and masking are declarative (`migkit/rules.py`, `migkit/masking.py`, row filters and column mappings through `hetero._column_plan` `hetero.py:174`); there is no script hook, by design. DDL is not forwarded by the tail: the source's column shape is read from the catalogue before each batch and saved beside the position, and the tail stops when the target lacks a column the source now has (`migkit/engines/hetero.py:2808` `_shape_gate`, `:2853` `_target_lacks`); an online schema change's working tables are recognised and left out (`hetero.py:3037-3040`, `drift.transient`). Gap: the **two-way DDL direction rule** is not written down for migkit's own tails - with both directions gated, a DDL made on either side stops both tails until the other side is level, which is safe and should be documented as such in `docs/cutover.md`.

---

## 2. Tencent Cloud DTS

### 2.1 `__tencentdb__`: the checksum that travels through the binlog

**Problem.** Compare a chunk on the source and the target at the same logical instant while the target is still replicating, with no coordination between the two readers.

**Mechanism.** DTS creates the system database `__tencentdb__` *in the source* with the task's account and writes a `Checksums` table. Per chunk (a fixed range on the check column, e.g. primary keys 1 to 1,000 of table A) the rows are concatenated in order and a CRC computed with the row count; the value `crc1`/`count1` is written to `__tencentdb__.Checksums` on the source. That write is itself replicated: DTS parses the row-mode binlog, reconstructs the statement that wrote the check values, and replays it on the target, where the same expression is recomputed with the same variables over the same chunk, giving `crc2`, which is compared with `crc1`. This is pt-table-checksum's design: the checksum statement is an ordinary transaction, so the target evaluates it at the same point in its own apply order as the source did. The database uses a single-threaded connection, occupies 0.01-0.1% of the source's size, is not deleted after the task (kept for later diagnosis), must not be dropped during incremental sync, and the account needs `ALL PRIVILEGES ON __tencentdb__.*` plus `RELOAD, LOCK TABLES, REPLICATION CLIENT, REPLICATION SLAVE, SHOW DATABASES, SHOW VIEW, PROCESS` and `SELECT ON mysql.*`. A read-only source skips the check entirely.

**Limits.** Requires `binlog_format=row` and write access on the source - a footprint, and impossible on a read-only or replica source. Checksums are computed by the target's SQL engine, so a heterogeneous target cannot recompute them (Tencent's "independent check" exists for that, 2.2).

**migkit.** Deliberately no write on the source. The same guarantee at the table grain comes from the fence: the source's position is read, the tail is waited to that position, and only then is the target digested (`migkit/engines/hetero.py:1893` `src_lsn`, `:1902` `fence_wait`; `base.py:3194` `fenced_recheck`). PostgreSQL adds a whole-database consistent snapshot on the source (`postgres.py:6448` `_export_snapshot`, `:6524` `_fast_consistent`). What the binlog trick gives that the fence does not: the check runs *on the target's own replica apply thread* with zero coordination and works on a native MySQL replica target without migkit's tail. For a hop whose target is a native replica (`loops_prevented`, `mysql.py:5610` names that topology), a rung that writes the chunk digest into a migkit-owned schema on the source only where the hop allows a footprint would give per-chunk exactness for free. Bottom rung of the verification ladder, never the default.

### 2.2 Independent consistency check: full, sampling, row count; slicing; rechecks

**Mechanism.** Three check types: full (all objects selected), sampling (10-90% of objects), row count only (no primary key needed). The independent check "splits table data into slices, with each data shard serving as the smallest unit for data querying and comparison", converting heterogeneous types to a standard form before comparing. Tables with no primary or unique key and more than 50,000 rows are skipped. Single queries on either side time out at 10 minutes. The check does not detect DDL; re-run it after one. The user sets a number of rechecks and an interval; inconsistent rows are re-validated in the background that many times.

**migkit.** Slicing by key ranges and a hash per slice - built (1.5). Rechecks of the differing rows - built, but fenced to the position rather than timed (`base.py:3223` `_resolve_inflight`; the `settle` option is the timed fallback). Missing: nothing to take except the **10-minute statement cap** as a guard: migkit's `Rate.chunk_rows` sizes a range for a target number of seconds (`migkit/checkpoint.py:193`, `TARGET_SECONDS`), which is the same intent from the other end.

### 2.3 REPLICA IDENTITY FULL on PostgreSQL sources

**Mechanism.** Tencent's precheck for TencentDB for PostgreSQL reports a *warning* (not an error) for a table with no primary key and `REPLICA IDENTITY NOTHING`, and recommends `ALTER TABLE s.t REPLICA IDENTITY FULL`; without it, UPDATE/DELETE on that table are not carried. Alibaba is stricter: it requires FULL on every migrated table before writes and runs the ALTER itself during initialization if the precheck is skipped. (The identity is what logical decoding needs to name the old row; FULL logs every old column, at WAL cost.)

**migkit.** The before image is used, not required: without it the two-way check is "blind" to `update_origin_differs`/`delete_origin_differs` and says so once (`migkit/twoway.py:299-305`), and counters cannot be added and are written as they end (`:327` `_as_added`). migkit changes no server setting (R3 rule). Gap: **assess should name every table in scope whose identity would make the tail blind or refused** - a PostgreSQL table with no key and `NOTHING`/default identity refuses UPDATE/DELETE on the publisher, which stops the application, not migkit. Worth one query in `assess` per side.

### 2.4 Conflict handling: ReportError, Ignore, Cover, ConditionCover

**Mechanism.** Policies are implemented as statement rewrites on the target: `ReportError` - no rewrite, a primary-key INSERT conflict stops the task, UPDATE conflicts are not handled; `Ignore` - INSERT rewritten to `INSERT IGNORE`, UPDATE conflicts still error; `Cover` - INSERT rewritten to `REPLACE INTO`, UPDATE rewritten to `DELETE + REPLACE INTO`. Policies apply to unique keys as well as primary keys; only INSERT/UPDATE conflicts, never DELETE. `ConditionCover` (API `ConflictHandleType`) carries a `ConflictHandleOption` with `ConditionColumn` ("conditionally overwritten column"), `ConditionOperator` ("conditional overwrite operation") and `ConditionOrderInSrcAndDst` ("conditional overwrite priority configuration") - the source row overwrites the target's only when the named column compares as configured *(the exact operator vocabulary is not on the public page)*. Tencent's own guidance: in a multi-node topology a conflict policy alone cannot converge - two-way `Cover` both ways makes the sides *exchange* values - so partition the key space or add versioning. DDL is one-way only in two-way setups.

**migkit.** `last_update_wins` on a named column with ranks breaking ties is `ConditionCover` with operator "greater" (`migkit/twoway.py:354-381`). `REPLACE INTO` is avoided on purpose: MySQL applies with `on duplicate key update` because REPLACE deletes and re-inserts, firing delete triggers and cascades and moving auto-increment (`migkit/engines/mysql.py:293-296`); PostgreSQL uses `on conflict do update` (`postgres.py:1116`). The "exchange, not converge" failure Tencent warns about is the one migkit measured on native MySQL two-way and now says at setup (R3, backlog 3671-3677). Gap: an explicit **operator** on the compared column (`<` for "oldest wins", `=` for "same version only") - small.

### 2.5 Retry window, timed start, resumable export; the full phase

**Mechanism.** Automatic retry: on a temporary interruption DTS retries and resumes within a window the user sets, 5 to 720 minutes. Scheduled execution: the task passes prechecks and waits for the set time; a scheduled time already past becomes "start now". Resumable transfer: the Redis migration doc says the source side supports breakpoint resumption under network jitter; for MySQL the full export's resumability is not stated. Full phase on MySQL: lock-free by default - no FTWRL, only keyless tables locked; 8 threads by default (adjustable); 18-45% of an 8-core source's CPU, 40-60 MB/s extra read, ~8 sessions; the incremental phase is one connection reading the binlog. Conflict rule across phases: a key conflict during full sync errors; a row the full sync wrote that the incremental then meets is overwritten; a post-full conflict errors. DDL is not supported during the full export step. Only InnoDB/MyISAM/TokuDB.

**migkit.** Timed start: `migkit/schedule.py` (172 lines) and the prod deploy window; retry window: the tail's lease and stall handling (`migkit/lease.py`, `migkit/stall.py`) and `tailctl`; every copy resumes by range (`migkit/ranges.py:222`) and every verify by range (`checkpoint.py:88`). Keyless tables are never locked on the source. Gap: **a stated retry budget** ("retry for N minutes then stop and say so") as one hop option, since the pieces exist but the number is not one setting.

---

## 3. Huawei Cloud DRS

**Mechanism.** Real-time migration, synchronization and disaster recovery with the same engine: full phase (6-10 sessions reading concurrently; a keyless table may be locked for 3 s; temporary accounts `drsFull` and `drsIncremental` created on the target and dropped at the end), then incremental by log parsing. Conflict policy at task setup: `Ignore` (skip, keep the target's row - stability first), `Overwrite` (source wins), `Report error` (stop - quality first); Oracle-to-PostgreSQL adds concurrent replay tasks and a replay policy. Extras: many-to-one sync, "synchronization timestamp" and "type stamp" columns (the target row carries when and by which operation it last changed), soft deletion. Comparison: object-level; data-level row comparison (fast, recommended first) and value comparison (primary-key tables only); sampling comparison by ratio; periodic row/object comparison by policy; account comparison; static value comparison (no ongoing writes) versus **dynamic** value comparison (all rows once, then incremental rows compared in real time as they change); row comparison capped at 60 minutes (30 for non-relational sources); results kept 60 days; case-sensitivity and encoding caveats; a "Review" button re-checks inconsistent rows and removes them from the result if now equal; value comparison needs a large-or-higher task spec and a choice of compute resource (DRS's or the database's). Huawei calls the algorithm "patented" and does not publish it.

**Limits.** Duration caps mean a large table's value comparison is simply not finished; the compare needs a spec bump; value compare is keyed tables only.

**migkit.** Dynamic comparison = `delta_verify` after a full digest, in the `watch` loop (`migkit/engines/hetero.py:1934`; `migkit/cli.py` watch); Review = `fenced_recheck` (`base.py:3194`); row-then-value = `check_counts` then `check_data` (`base.py:125`, `hetero.py:1851`, `:1857`); account comparison = `migkit users` (`migkit/users.py`). Not built, deliberately: **stamp columns and soft delete** on the target (a footprint on the application's tables) and **temporary accounts on the target** (migkit runs as the account it is given). The "duration cap that stops a compare" failure mode is what `Checkpoint` exists to avoid (`checkpoint.py:1-25`).

---

## 4. TiDB Data Migration (DM)

### 4.1 Sharding merge: pessimistic and optimistic DDL coordination

**Problem.** Many upstream shards (`db_N.tbl_M`) merge into one downstream table; a DDL on the shards arrives at different times, and DML from shards on the old and new schema interleave.

**Pessimistic (default).** Two-level sharding groups. A DM-worker reaching a DDL on one shard pauses that shard's DDL and DML and reports the DDL to DM-master; the master creates a DDL lock for it, sends the lock back and marks the first worker the *owner*; DML of shards that have not reached the DDL keeps flowing; DML of shards that have reached it is held; when every shard has sent the identical DDL, the master asks the owner to execute it downstream once and tells the others to skip it and continue. Restrictions: the same DDL in the same order on every shard; no partial execution. A lock that will never complete (a shard dropped, a DDL only some shards will run) needs `shard-ddl-lock unlock` by hand - the documented "abnormal scenarios".

**Optimistic.** Each shard's DDL is applied downstream immediately if it is *compatible*; DM-master keeps every shard's schema and computes the joined schema all shards' DML can write to. ADD COLUMN on one shard: the column appears downstream at once; other shards' DML still applies (the column stays empty for their rows). DROP COLUMN: the column is kept downstream until every shard has dropped it. Conflicts: "type 2" DDL (column rename, type change, default change) or two shards adding a same-named column with different types/defaults; under `strict-optimistic-shard-mode: true` the task stops at once, otherwise a differing execution order stops it; the example `Age INT DEFAULT 0` on one shard and `DEFAULT -1` on another needs manual correction. Forbidden: DROP TABLE/DATABASE, TRUNCATE, defaults with `current_timestamp`/`rand()`/`uuid()`, multi-table DDL.

**migkit.** Many-to-one is R3's "topologies" item, not built; nothing coordinates DDL across sources, and the tail stops on any shape change the target lacks (`hetero.py:2808`). The pessimistic lock is the honest model for a merge (one owner executes, others skip), and it needs a coordinator that owns the lock - a `many_to_one` hop with one tail per source and a shared shape file is the migkit shape of it. Effort M-L; docker-testable with two MySQL shards and one target.

### 4.2 Relay log

**Problem.** The upstream purges its binlog after its retention; a long-stopped task cannot resume. Several tasks on one upstream each pull the same binlog.

**Mechanism.** DM-worker writes the upstream binlog to local disk as numbered files under `<upstream UUID>.<serial>` with a `relay.meta` (current file, position, GTID) and a `server-uuid.index` of subdirectories; tasks read the relay instead of the upstream; purge by `purge.interval`/`purge.expires`/`purge.remain-space` or `purge-relay` by hand; in GTID mode a corrupt relay is re-pulled from `relay-binlog-gtid`. Cost: disk I/O and CPU on the worker; "not recommended for latency-sensitive scenarios" (writes optimised since 2.0.7).

**migkit.** No relay; the tail reads the log directly with a reader running beside the applier (`migkit/engines/hetero.py:3196` `_ReadAhead`). On PostgreSQL the slot holds the WAL (`migkit/pgslot.py`); on MySQL nothing holds the binlog, and `assess`/`doctor` measure how long the tail may be stopped from `binlog_expire_logs_seconds` or RDS's retention hours (`migkit/engines/mysql.py:948-949`, `:1013-1028`). The "relay beside the source" in R19.2 is a *row* relay for the copy, not a binlog relay. Gap: **a durable local log buffer** for a MySQL source whose retention is short and whose target may be down long - Voyager's event queue (11.2) and DTS's local store (1.1) are the same thing. Effort M; the failure it prevents (binlog purged under a stopped tail) is testable in docker with `binlog_expire_logs_seconds` set low.

### 4.3 Safe mode

**Problem.** DML apply and checkpoint flush are asynchronous, so after a crash the checkpoint may be behind what was applied, and replaying would duplicate.

**Mechanism.** In safe mode INSERT is rewritten to REPLACE, and UPDATE to DELETE by primary/unique key + REPLACE, so replay overwrites instead of conflicting. Entered automatically on resume: until the binlog position passes the recorded `safemode_exit_point` (`exit_safe_binlog_*` columns) when there is one, otherwise for the first two checkpoint intervals (2 x 30 s = 60 s by default, `safe-mode-duration`). Cost: DELETE + REPLACE churns keys and indexes more than UPDATE, so it is not left on.

**migkit.** Idempotent by construction: every write is an upsert and every delete "whether or not it is there" (`migkit/engines/base.py:3990-3995`, `mysql.py:293-296`, `postgres.py:1116-1117`), so there is no window and no mode; the R3 `exact` mark removes even the replay (`twoway.py:125`). DM's DELETE+REPLACE is what migkit's `on duplicate key update` / `on conflict do update` avoid. Nothing to take.

### 4.4 Checkpoint tables and the schema tracker

**Mechanism.** Checkpoints live *in the downstream* in `dm_meta` (`meta-schema`): `{task}_syncer_checkpoint` with `id` (source-id), `cp_schema`, `cp_table`, `binlog_name`, `binlog_pos`, `binlog_gtid`, `exit_safe_binlog_*`, `table_info`, `is_global`. One global row (resume point) and one row per table (so a re-sync skips events already applied for that table - needed by sharding). Checkpoints are held in memory and flushed every `checkpoint-flush-interval` (30 s). `table_info` is the serialised schema of the table *as of the checkpointed position*, kept by an in-memory schema tracker, so row events between the checkpoint and now are decoded with the schema they were written under; lookup order when the tracker lacks a table: checkpoint `table_info`, then optimistic-DDL metadata, then the downstream table; `binlog-schema update --flush` repairs it. The loader's `{task}_loader_checkpoint` records a byte offset per data file and *the checkpoint update is added to the same transaction as the batch*, so the loader's position is exact. The dump stage has no checkpoint: a restart wipes the dump directory and dumps again. Start precedence: `remove-meta` > existing downstream checkpoint > task `meta` > upstream's latest position.

**migkit.** Positions and shapes are local files beside the run (`migkit/checkpoint.py`, `migkit/state.py`, `tail-shape.json` in `hetero.py:2830`), exact positions in the target through the mark (4.3). Two things worth taking: **(a) decode with the schema as of the position** - migkit's binlog reader takes the table map of each event but the *catalogue* it compares against is the source now (`_shape_gate` reads `drift.shape` live), so events written before a DDL that happened while the tail was stopped are gated (the tail stops, correctly) rather than decoded under their own schema and applied - DM continues where migkit stops; effort M; **(b) the loader's "position in the same transaction as the rows"** is what `exact` does for the tail and what `spans_to_copy`'s count-settle approximates for keyless copies; a mark row per span in the same transaction would make the keyless copy exact without the count heuristic - small, and it is the table rung of the R3 ladder reused.

---

## 5. TiDB Lightning (physical import)

**Problem.** Load terabytes into a distributed store faster than SQL can, with a proof the bytes landed.

**Mechanism.** `backend = "local"`: Lightning reads SQL/CSV in parallel, encodes rows to KV pairs in TiDB's own encoding, sorts them in local RocksDB *engines* (one data engine per ~100 GB of source per table, `table-concurrency` at a time; exactly one index engine per table because index KVs arrive unordered), splits each engine into SST files at `region-split-size`, scatters the regions, and *ingests* the SSTs into TiKV directly. TiKV is put into "import mode" (writes favoured, reads degraded); PD scheduling is paused for the table's regions. Requirements: 32+ cores, 64+ GiB, `sorted-kv-dir` exclusive and larger than the data, TiDB >= 4.0, the target table empty, no DDL/DML on the table during import, not on a production cluster. Post-import: **checksum** - three numbers per table computed from the KV pairs as encoded (count of KV pairs, total length, bitwise XOR of each pair's CRC-64-ECMA) compared against `ADMIN CHECKSUM TABLE` on TiDB (`post-restore.checksum = required|optional|off`); a mismatch aborts; then `ANALYZE`. **Conflicts** (`conflict.strategy`): `""` - no detection, a duplicate key surfaces as a checksum failure; `"error"` - stop and report; `"replace"` - keep the latest row, overwrite the old, record the losers in `lightning_task_info.conflict_error_v3` (`table_name, index_name, key_data, row_data`) / `conflict_view`; `precheck-conflict-before-import = true` scans the sorted KVs for duplicates before ingest (single node only, not with `disk-quota`; recommended above ~1% conflicts / 1,000,000 records); `threshold` caps tolerated conflicts and `max-record-rows` follows it from 8.1. **Checkpoints** (file or MySQL driver; a RAM disk or a separate MySQL is recommended so the target is not stressed) record per-table/engine/chunk status so a restart continues; `--checkpoint-error-destroy` drops the table's data and resets it; `--checkpoint-error-ignore` clears the error "as if nothing happened" and can lose data; `--checkpoint-remove` forgets. Disk quota pauses reading and flushes sorted KVs when the sort dir fills. Published rate 100-500 GiB/h per instance; 10 TiB across 5 instances ~10 h.

**Limits.** Only TiDB; the target is unusable during import; duplicate detection without the precheck costs a second pass; `error-ignore` is a foot-gun.

**migkit.** Physical load is R19.1 (`pg_basebackup`, `CLONE INSTANCE`, XtraBackup, snapshots) - not built. The ideas that carry: **the checksum triple** - migkit's fold is a *sum* of per-row 32-bit hashes with the count beside it, chosen after XOR was measured to cancel duplicate rows (`migkit/checkpoint.py:118-133`, `mysql.py:1713-1718`); Lightning's XOR of CRC-64 has the same weakness (two identical KV pairs cancel) but Lightning's precheck catches duplicates before the XOR would. **Deferred indexes** - built for PostgreSQL (`migkit/movers.py:819` `_IndexWindow`, measured 1.9x), refused for MySQL where deferring measured slower (R19.5). **Duplicate detection before load** - `check_deep`'s hunt for duplicate rows under unique indexes (`migkit/engines/base.py:989-1045`) is Lightning's precheck on the *source*; the `replace` strategy's "keep the latest" is what `_collapsed` does in a batch. **`checkpoint-error-ignore`'s data loss** is the failure `Checkpoint.begin`'s fingerprint refuses (`checkpoint.py:88-102`).

---

## 6. sync-diff-inspector

**Problem.** Find the differing rows between two large tables without moving both tables.

**6.1 Chunking.** Chunks are cut on `index-fields` (else a chosen primary key, unique key or indexed column - `GetSplitFields` order pk, uk, index). Three splitters: **bucket** (TiDB sources only) reads the table's statistics buckets (`SHOW STATS_BUCKETS` via `dbutil.GetBucketsInfo`), merges consecutive buckets until their row count reaches `chunk-size`, and splits an oversized bucket by *random rows* (`splitRangeByRandom`); **random** (`GetRandomValues`: `SELECT cols FROM (SELECT cols, rand() rand_value FROM t WHERE range ORDER BY rand_value LIMIT n) ORDER BY cols`) for MySQL sources without buckets; **limit** for the rest. `CalculateChunkSize(rows) = 50,000`, or `rows/10,000` once the table exceeds 500 million rows (about 10,000 chunks). Chunks are generated asynchronously so comparison starts before splitting ends. Statistics must be fresh (`ANALYZE TABLE` when the server is quiet). Each chunk carries `(table index, first bucket, last bucket, chunk count in bucket, chunk index)`, which gives a global order so the checkpoint (`${output}/checkpoint/sync_diff_checkpoints.pb`, every 10 s) records only the last *contiguous* completed chunk.

**6.2 Checksum.** Per chunk, on both sides:

```
SELECT COUNT(*) as CNT,
       BIT_XOR(CAST(CONV(SUBSTRING(MD5(CONCAT_WS(',', c1, c2, ..., CONCAT(ISNULL(c1), ISNULL(c2), ...))), 1, 16), 16, 10) AS UNSIGNED)
             ^ CAST(CONV(SUBSTRING(MD5(CONCAT_WS(',', ...)), 17, 16), 16, 10) AS UNSIGNED)) as CHECKSUM
FROM t WHERE <chunk range>;
```

NULLs are made distinct from empty by appending `ISNULL()` flags; FLOAT/DOUBLE are rounded to 6/15 significant digits before hashing; JSON compares with collation/charset caveats.

**6.3 Mismatch.** If the checksums differ and the upstream count exceeds `SplitThreshold = 1000`, `BinGenerate` finds a midpoint row with `SELECT cols FROM t WHERE range ORDER BY cols LIMIT 1 OFFSET count/2`, makes two ranges, re-checksums each, and recurses; it stops when a half's count is zero or at or below the threshold, or - by design, "pessimistic" - when *both* halves differ, and then compares rows: both sides streamed `ORDER BY` the key and merge-compared (`compareRows`), producing `REPLACE INTO` for missing/changed and `DELETE` for extra rows into `${output}/fix-on-${instance}/schema:table:range.sql`, annotated with which columns differed.

**Limits.** No online check: "ensure that no data is written into the upstream-downstream checklist" during the run. XOR fold: two identical extra rows cancel. `OFFSET count/2` is a scan. `check-thread-count` (4) bounds parallelism; connections are slightly more.

**migkit.** Chunking is time-sized (`checkpoint.py:172` `Rate`: an EMA of rows/s, `chunk_rows` for a target number of seconds between `MIN_CHUNK` and `MAX_CHUNK`) and key-based (1.1), with **no statistics splitter** - gap, same as 1.1. Checksum: count + `sum(crc32)` + summed md5 prefix on MySQL (`mysql.py:1720`), `md5(ROW(...)::text)` folded as a numeric sum on PostgreSQL (`postgres.py:6313`, `:6828`); NULL/empty ambiguity fixed in `migkit/rowtext.py` (the same problem sync-diff solves with `ISNULL` flags); XOR rejected after measurement. Bisect: `hetero._bisect` halves the *integer key range* arithmetically (no `OFFSET` scan), follows only differing halves, leaf at `BISECT_LEAF = 2000` rows, then walks the leaves row by row across engines (`migkit/engines/hetero.py:460-538`); on one engine the drilldown is per-key hashes (`mysql.py:1959`, `postgres.py:2819`). Gap: **bisect on text or composite keys** - `_bisect` returns None unless the key is a single integer on both sides (`hetero.py:474-482`), for the collation reason; sync-diff's `OFFSET count/2` works for any key at the cost of a scan, and MOLT's stats-based shards (10.1) for any type at no cost - a stats-bucket splitter would give migkit both. Fix SQL: `repair_plan` and `migkit sync --kind rows` (`base.py:1807`, `postgres.py:5126`, `mysql.py:4028`) with the revert generated at the same instant (`migkit/revert.py`), which sync-diff does not do. Online check: migkit's fence is the answer sync-diff refuses to give.

---

## 7. Dumpling

**Mechanism.** `--consistency`: `flush` (`FLUSH TABLES WITH READ LOCK` for the duration of the connection setup - MySQL; not for TiDB), `snapshot` (TiDB: `tidb_snapshot` at a TSO or datetime; `--snapshot`), `lock` (`LOCK TABLES ... READ` on every exported table), `none`, `auto` (flush on MySQL, snapshot on TiDB). Splitting: `-r/--rows` enables in-table concurrency; on TiDB the region boundaries (and `_tidb_rowid`) are used and the `-r` value does not affect the split; on MySQL only when the primary key (or the first column of a composite key) is INT or STRING; the row count is *estimated* (progress logs print "estimate total rows"), integer keys are cut between `MIN` and `MAX` - the known limit is an unevenly distributed key (issue #104), and since PR #305 a split that fails dumps the whole table as one chunk; `-F` caps file size (256 MiB or less for Lightning). The `metadata` file records `Started dump at`, `SHOW MASTER STATUS: Log, Pos` (and `GTID` when present), `Finished dump at`. *(The exact `EXPLAIN`/`information_schema` estimate and the string-key path are in `dumpling/export/dump.go` in the TiDB monorepo; not fetched.)*

**migkit.** PostgreSQL bulk: one exported snapshot shared by every worker (`postgres.py:6448`, `:6481`), the R19 "slot before snapshot" rule (memory note). MySQL bulk: through MySQL Shell's dump/load (`movers.py:499-576`), which takes its own consistent position; migkit's own copier needs no global snapshot because the tail starts from a position taken before the copy and the interleaving is proved (`tests/test_a_copy_and_its_changes_interleave_safely.py`) - the Vitess/DBLog design (8.1), not Dumpling's. Row splitting: 1.1. Nothing to take but the honesty of `auto`: migkit's decision layer (P0) already chooses the consistency mode per engine.

---

## 8. Vitess

### 8.1 VReplication: copy, catchup, fast-forward, replicate

**Problem.** Copy a table while it changes, from a source that keeps a binlog, and end exactly consistent with no long snapshot.

**Mechanism ("Life of a Stream").** One table at a time, in primary-key order. **Copy**: the source selects the next batch of rows with PK greater than the last copied, under a consistent snapshot, for at most `vreplication-copy-phase-max-duration`; the target inserts the batch and updates `_vt.copy_state` (one row per table, `lastpk`) *in the same transaction*, and records the GTID in `_vt.vreplication`. **Catchup**: binlog events are applied, but an event for a row of the table being copied is applied *only if its PK <= lastpk*; rows beyond are ignored because the next copy batch will read their later state. **Fast-forward**: a new snapshot is taken for the next batch; the events between the catchup's stop GTID and the snapshot's GTID are replayed first, then the batch. When every table's `copy_state` row is deleted the stream stays in **replicate** forever. Throttled by the tablet throttler; resumable at a point consistent with the replication position; `--defer-secondary-keys` (default true) drops secondary indexes on the target at copy start and re-adds them per table when its copy completes; `--atomic-copy` (experimental) copies all tables under one snapshot with `foreign_key_checks=off` for foreign-key schemas.

**migkit.** The concurrent copy-and-tail with the same invariants: ranges copied and marked in the checkpoint (`ranges.py:222-257`), the tail seeded from a position taken first (`hetero.py:2884` `tail_seed`), the interleaving proved by test. **The `lastpk` rule is R19.8 and not built**: today every change is applied whether or not its range has been copied (correct - the upsert lands and the later copy overwrites with the same or newer state - but wasted work during a heavy catch-up). Vitess's "copy_state updated in the batch's transaction" is the loader rule of 4.4(b). Effort S-M, docker-testable by counting applied statements during a copy under load.

### 8.2 VDiff v2

**Problem.** Compare source and target of a running workflow at one logical point, restartably, without stopping the workflow for long.

**Mechanism.** Per table: on the target, stop the workflow to freeze it and record its GTID position; on the source, lock the tables, read `GTID_EXECUTED`, `START TRANSACTION WITH CONSISTENT SNAPSHOT`, unlock; on the target, resume replication `UNTIL` that GTID, then take its own consistent snapshot - both sides now at the same logical point. Both run `SELECT <cols> FROM t ORDER BY <pk>`; the target's primary tablet (v2 runs on the target shards in parallel, not in vtctld) merge-sorts the shard streams and compares rows. Resumable: `_vt.vdiff_table` keeps the last PK per table (`resume` continues where PK > last); `--auto-retry` (default true) recovers from crashes, failovers, network loss; `--max-diff-duration` stops and restarts a table's diff so a long repeatable-read snapshot does not hold back purge; `--max-extra-rows-to-compare` (1,000) re-compares rows that arrive in a different order because of collation differences before calling them extra; `--update-table-stats` runs `ANALYZE TABLE` for progress/ETA; `--limit`, `--tables`, `--only-pks`, `--max-report-sample-rows` (10), `--row-diff-column-truncate-at` (128), `--filtered-replication-wait-time` (30 s), `--wait`. Report: `RowsCompared`, `HasMismatch`, per table `ProcessedRows`, `MatchingRows`, `MismatchedRows`, `ExtraRowsSource`, `ExtraRowsTarget`, progress percentage and ETA. Throttled by the tablet throttler.

**migkit.** The same-logical-point trick is `fence_wait` plus PostgreSQL's exported snapshot; on a MySQL target the fenced re-check of suspect keys (`base.py:3164-3222`). Rows are never streamed across the link for the first pass (digest first); the per-key walk only for leaves. Resumable by range with fingerprint (`checkpoint.py`). `--max-diff-duration`'s reason - a snapshot held too long - is the "snapshot age" P0 item in the backlog; today `_fast_consistent` holds one snapshot across the whole database and prints how long it was held (`postgres.py:2893-2900`), but does not *stop and re-snapshot* per table. Gap S: **per-table re-snapshot with a maximum hold**, which the range checkpoint makes free. `--max-extra-rows-to-compare` is the collation problem `_bisect` sidesteps by refusing text keys; a comparison in a shared collation (`hetero._rows_differ` `hetero.py:837`) is the same idea at the leaf.

### 8.3 MoveTables: SwitchTraffic, ReverseTraffic, routing rules, DeniedTables

**Mechanism.** `Create` sets up the workflow; `Status` reports copy percentage then lag; `SwitchTraffic` for replica/rdonly rewrites the *table routing rules* (`customer@replica -> target.customer`) so reads move first; for primaries it (1) adds the tables to the source shard's **DeniedTables** (tablet controls), which refuses writes at the source, (2) waits for the workflow to reach the source's current position (`--max-replication-lag-allowed` must hold before it starts; `--timeout` 30 s bounds the wait), (3) creates the **reverse workflow** (`<name>_reverse`, `--enable-reverse-replication` default true) from target back to source, (4) rewrites routing rules to the target, (5) unblocks. `ReverseTraffic` restores the rules and removes the denied-table entries. After a switch the forward workflow is *frozen*. `Complete` removes the artifacts (and drops the source tables by default); `Cancel` only before any switch. `--dry-run` prints the plan.

**migkit.** `reverse: at_cutover` starts the stream back the moment the forward stream is torn down (`migkit/cli.py:1821-1832`; `docs/cutover.md` steps 10, 12, 13) - the reverse workflow. Writes at the source are stopped by `freeze.py` per role from the catalogue, measured against the ways a role can still write (`migkit/freeze.py:1-33`), which is DeniedTables without a proxy. The wait-to-position is `fence_wait`. Routing rules are outside migkit (step 11; no proxy, by design). Gaps: **`--max-replication-lag-allowed` as a refusal before the flip** - `cutover.md` step 9 waits for the tail; a named threshold that refuses to begin the freeze while lag exceeds it is one option; and a **dry run of the cutover** naming what will be frozen, waited for, verified and started (the planner has `Decision` lines for the move, `migkit/planner.py:27`; not for the cutover).

### 8.4 Online DDL (`vitess` strategy) and the cut-over

**Mechanism.** All online-schema-change tools build a shadow table, fill it, tail changes into it (gh-ost, fb-osc and Vitess from the binlog; pt-osc by triggers) and swap. The swap is the hard part because MySQL will not let a connection rename a table it holds locked. gh-ost: a sentry table blocks the RENAME until the migration is satisfied, then an atomic swap, with documented lock-priority edge cases. Vitess: (1) a 10 s **buffering rule** at VTTablet holds new queries on the table; (2) stall 100 ms for in-flight queries; (3) `RENAME original TO _elsewhere` - the *puncture*, which waits for pending statements to finish; (4) mark `gtid_executed`; (5) consume the remaining binlog up to that mark into the shadow table; (6) `RENAME shadow TO original`; (7) clear the buffer. Failure before (3): nothing done; failure at (3): undo the buffer and retry later; VTTablet crash mid-puncture: the new VTTablet restores the original table. Cut-over threshold 5-30 s (default 10) is both the maximum lag allowed and the lock/buffer timeout; a busy table may fail the cut-over and retry every minute for hours, so a *forced* cut-over exists. Only safe when all traffic goes through Vitess; migrations are revertible and resume after failover (unlike gh-ost/pt-osc); gh-ost and pt-osc strategies removed in v22.

**migkit.** Out of scope as a schema tool, in scope as the cutover shape: freeze (3 above) -> mark the position (4) -> `fence_wait` (5) -> verify -> flip. What Vitess adds that migkit cannot: buffering at a proxy so the application sees a pause rather than an error; migkit's freeze *ends* open sessions on PostgreSQL (`freeze.py:19-24`), which the application sees. Nothing to build; the `cutover.md` "what each step guards against" table already names these.

---

## 9. PlanetScale imports

**Mechanism.** Built on MoveTables. Requirements: binlogs on, `gtid_mode=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`, retention > 2 days, InnoDB only, every table with a unique not-null key; with foreign keys every table is imported (no subset). Steps: connect and validate -> **Copying** (VReplication copy) -> **Running**: *bidirectional* replication is active, the external database stays authoritative and the application can be pointed at PlanetScale to test -> **Switch replica traffic** (reads) -> **Switch primary traffic** (writes; the connection string must already point at PlanetScale) -> **Complete workflow**. Options: **deferred secondary index creation** (checked by default; indexes built after the copy, in one bulk build per table, because maintaining many indexes while inserting is slow) and **Verify data** (VDiff) before switching. VStream is the gRPC transport under it.

**migkit.** Deferred indexes on PostgreSQL (`movers.py:819`), refused on MySQL by measurement (R19.5; PlanetScale's target is MySQL, which is the interesting disagreement: Vitess builds *all* index records in one bulk operation per table after copy, which on InnoDB means a sorted build rather than random inserts - migkit measured "deferring indexes hurting MySQL" for its own load path; worth re-measuring against a bulk `ALTER TABLE ... ADD INDEX` after a key-ordered load). "Running with bidirectional replication before the switch" is R3's two-way through migkit's tails, with the prod side authoritative - the topology `docs/cutover.md` should name as a rehearsal mode.

---

## 10. CockroachDB MOLT

### 10.1 Fetch

**Mechanism.** Export, stage, import. Sharding: **range-based** (default for MySQL and Oracle; INT/FLOAT/UUID primary keys only) or **stats-based** (PostgreSQL 11+: reads `pg_stats` histogram bounds for evenly sized shards, up to 200 per table, any key type). `--table-concurrency` tables at once, `--export-concurrency` shards per table, `--row-batch-size` rows per file. Staging in S3/GCS/Azure, a local file server, or directly in memory (`--direct-copy`). Import: `IMPORT INTO` (fastest, the target table offline, compression supported) or `COPY FROM` (online, slower, `--use-copy`). Consistent point captured at start and printed as `cdc_cursor`: a replication slot LSN (PostgreSQL), `gtid_executed` (MySQL), `CURRENT_SCN` (Oracle) - the handoff to Replicator. Resume: a failed run prints a fetch ID and a continuation token per failed table; `--fetch-id`, `--continuation-token`, `--continuation-file-name part_00000003.csv.gz` restart at table or file grain; `molt fetch tokens list`. Schema: `--table-handling drop-on-target-and-recreate` creates tables with only PRIMARY KEY and NOT NULL; other indexes and constraints are the user's; sequences are not migrated (`setval` by hand). Modes: `data-load`, `export-only`, `import-only`; the in-Fetch replication modes are deprecated in favour of Replicator.

**migkit.** Stats-based sharding is the gap named in 1.1 and 6.1; migkit's `bounds_sql` is exact but scans. Continuation at file grain is the range checkpoint (`ranges.py:222`). The `cdc_cursor` handoff is `tail_seed` (`hetero.py:2884`) and "slot before snapshot". Offline `IMPORT INTO` versus online `COPY` is a rung of the decision layer (P0). Effort for a stats splitter: S on PostgreSQL (`pg_stats.histogram_bounds`, `most_common_vals` for skew), M on MySQL 8 (histograms only where `ANALYZE TABLE ... UPDATE HISTOGRAM` was run; fall back to `information_schema.STATISTICS` cardinality plus a sample); docker-testable against a skewed key.

### 10.2 Verify

**Mechanism.** Rows read in batches of `--row-batch-size` (20,000) ordered by primary key, `--concurrency` (16) tables at once, compared value by value; summary per table: `num_truth_rows`, `num_success`, `num_conditional_success`, `num_missing`, `num_mismatch`, `num_extraneous`, `num_column_mismatch`, `num_live_retry`. The `--live` mode (re-check mismatched rows before reporting them, counted in `num_live_retry`) and `--continuous` mode (loop over all tables until stopped) existed and were **removed** in a recent release along with fixup mode; the docs now say rows may change between batches and recommend a quiet source. Limits: primary-key STRING columns with different collations fail; geospatial types not compared; auto-increment vs UUID not comparable; MySQL compares one database to `public`.

**migkit.** Verify sends digests, not rows (1.5), so the 20,000-row batches across the link are the cost migkit avoids; the "live" retry is `_resolve_inflight` with a fence rather than a timer (`base.py:3223`), and its proof is written ("the difference was still arriving (fence ...)"); the removed `--continuous` is the `watch` loop with `unchanged.py`. The collation failure is what `_bisect` guards against by construction. Nothing to take.

### 10.3 Replicator

**Mechanism.** A **staging schema in the target** (`--stagingSchema defaultdb._replicator`, created with `--stagingCreateSchema`) holds checkpoints, buffered mutations and resolved timestamps. Modes: **Consistent** (default: per-row order and source transaction atomicity preserved; mutations buffered and flushed at `--flushSize` or `--flushPeriod`, `--parallelism` concurrent transactions), **BestEffort** (atomicity relaxed across tables not joined by foreign keys; `--bestEffortOnly` or auto-entered after `--bestEffortWindow` of lag), **Immediate** (applied as they arrive, no buffering; requires no foreign keys on the target). Forward replication from PostgreSQL/MySQL/Oracle always preserves per-row order and transaction atomicity. Sources: `pglogical` (slot), `mylogical` (GTID, `--defaultGTIDSet`), `oraclelogminer` (`--scn`, `--backfillFromSCN`), CockroachDB changefeeds (`start`, for failback). Resume by rerunning with the same `--stagingSchema` (and the same `--slotName`); `--enableCheckpointStream`. **Userscripts** (TypeScript, `--userscript`): filter tables/rows/columns, route one source table to several targets, transform or add computed columns, in flight. DDL on either side during replication "can cause replication failures".

**migkit.** Lanes by dependency are BestEffort's rule (foreign-key groups stay together, `base.py:3697` `_ordered_tables`) with Consistent's guarantee inside a batch; Immediate is the lanes path with keys off where every parent is in scope (R2.5). The staging schema is the footprint migkit's ladder keeps to one mark table at most. No userscripts, by design; rules/masking are declarative. Gap worth naming: **`--bestEffortWindow`** - a policy that relaxes cross-table atomicity *only while behind* and restores it when caught up; migkit grows batch size while behind (`hetero.py:3030-3036`) but never changes the atomicity rule. Small, and only if a measurement shows the FK-grouped lane is the ceiling.

---

## 11. YugabyteDB Voyager

### 11.1 Export/import, cutover

**Mechanism.** `export schema` -> `analyze schema` -> `export data from source` (snapshot first, then the CDC phase capturing changes into an **event queue on local disk** under the export directory; Debezium-based) -> `import schema` -> `import data to target` (the CSV snapshot split into batches of 20,000 rows / ~200 MB, ingested by `COPY` concurrently across all target nodes; `--parallel-jobs` default adapts to CPU/memory with an upper bound of half the cores, `--adaptive-parallelism balanced|aggressive|disabled`; then the queued events applied) -> `initiate cutover to target` when the export rate drops to 0 (stops export, drains import, `cutover status`) -> `finalize-schema-post-data-import` (NOT VALID constraints, indexes) -> `end migration`. Everything is restartable from `metainfo` (queue segments tracked in `queue_segment_meta`; per-table exported-event stats; the target's `ybvoyager_import_data_event_channels_metainfo`). `get data-migration-report` shows imported events, ingestion rate, remaining events, ETA. DDL is not carried during streaming.

**11.2 Event channels and conflict detection.** Events are routed to N parallel channels by `--cdc-partition-key`: `pk` (hash of the primary key; every channel usable; conflict detection on), custom immutable columns (`--cdc-partition-key-overrides 'schema.table:(col)'`), or `table` (one channel per table; no detection). Conflict detection: an incoming event whose *new* unique value equals an in-flight event's *old* value is held until the earlier one is fully applied (`WaitUntilNoConflict`; logged "conflict detected for table ... waiting for event"); unique keys with `NULLS DISTINCT`/`NOT DISTINCT` handled from the target's index definitions; a table whose unique index covers a STORED generated column (absent from Debezium events) is forced onto the `table` strategy since the detector cannot see that key. Batch retries raised to 50. A recent issue: a partitioned parent with no PK and `--use-partition-root true` applies streamed UPDATE/DELETE to every partition holding the leaf key.

**11.3 Fall-forward and fall-back.** *Fall-forward*: a third database, the **source-replica**, is kept level with the target after cutover (`export data from target` via YugabyteDB CDC -> `import data to source-replica`); `initiate cutover to source-replica` drains and flips to it; triggers and indexes on the replica are created afterwards by hand. *Fall-back*: the *original source* is kept level from the target (`export data from target` -> `import data to source`); the source must have triggers and foreign keys disabled beforehand (superuser below PostgreSQL 15), `ybvoyager` keeps write access; `initiate cutover to source` stops the export when the rate is 0 and drains; re-enable triggers/FKs after. Limits: rows over 4 MB on the target, savepoints before 2024.2.8, tables without primary keys by hand.

**migkit.** Fall-back is `reverse: at_cutover` (8.3), with the keys-off rule of R2.5 for the way back. Fall-forward - a third database following the *target* - is a second hop from target to replica and is not named anywhere; it costs nothing to name in `cutover.md` as the rehearsal shape for heterogeneous moves. Channels by key hash = `_lanes` (`base.py:3609`); Voyager's *hold only the colliding event* is R2.2's union-find, finer than migkit's "a table with a second unique index goes whole to one lane" (`base.py:3613-3619`) - R2.2 is the better design and is not built (M). Cutover by "export rate 0" is a heuristic; `fence_wait` to the frozen position is exact. The **durable event queue** is 4.2's gap. Batch retries with a count (50) versus migkit's lane retry then one-by-one then stop (R2.3) - migkit's is the more honest failure.

---

## 12. ClickHouse ClickPipes for PostgreSQL (PeerDB)

**Mechanism.** Logical decoding on a slot. **Initial load** in parallel: the table is partitioned by `CTID` - a `COUNT(*)` then a window-function query yields CTID ranges of "snapshot number of rows per partition" (100,000 by default; PeerDB OSS 500,000) - and "initial load parallelism" (4) partitions are read at once; PeerDB OSS also has "tables in parallel" (4). A snapshot connection is held for the duration to keep the load consistent with the slot *(the per-partition `SET TRANSACTION SNAPSHOT` detail is inferred from PeerDB's design, not on the page)*. Tables land as `ReplacingMergeTree(_peerdb_version) ORDER BY <pk>` with `_peerdb_synced_at`, `_peerdb_is_deleted Int8`, `_peerdb_version Int64`: an UPDATE is an INSERT with a higher version, a DELETE an INSERT with `_peerdb_is_deleted=1`; deduplication is asynchronous in merges, so reads use `FINAL` and filter `_peerdb_is_deleted = 0` until merged. A `clickhousectl` issue (Sept 2026) shows the CLI defaulting to plain `MergeTree`, which keeps old versions and rejects `FINAL` - set the engine explicitly. Latency "as low as 10 s"; the slot is consumed without reconnecting; pause/resume is safe.

**migkit.** PostgreSQL `position_spans` builds ctid page ranges from `relpages`/`reltuples` in the catalogue (`postgres.py:714-745`) - no `COUNT(*)` and no window-function scan, which is cheaper than PeerDB's partitioning for the same result; one exported snapshot for all workers (`postgres.py:6448`). migkit's ClickHouse engine (`migkit/engines/clickhouse.py`, 611 lines) does not use a version column or `ReplacingMergeTree` (grep: none); the research note `docs/research/clickhouse-opensearch-tools-2026-09-27.md` covers the target side. Gap M if ClickHouse is a change target: **version + tombstone columns and `ReplacingMergeTree`**, since a MergeTree target cannot take an update at all; the verify would then need `FINAL` and the deleted filter.

---

## 13. SingleStore CDC-in from MySQL

**Mechanism.** `CREATE LINK ... AS MYSQL CONFIG '{database.hostname, database.port, database.ssl.mode, database.exclude.list}' CREDENTIALS '{...}'` (primary only, not a replica), `CREATE TABLES AS INFER PIPELINE` generates tables, `AGGREGATOR PIPELINE`s with `REPLACE KEY(...) INTO PROCEDURE ..._apply_changes FORMAT AVRO (__operation <- ...)` and the apply procedures; `START ALL PIPELINES`. The extractor is Debezium (config keys `snapshot.mode`, `table.include.list`; Java binary path required; `REPLICATION SLAVE, REPLICATION CLIENT, SELECT` and `mysql_native_password`). Snapshot modes: default - binlog position captured, full snapshot, then CDC, and *a restart during the snapshot restarts it from the beginning*; `incremental` - incremental snapshot in parallel with CDC (Debezium's watermark snapshot), slower but resumable. Limits: 16 CDC-in pipelines total; no `ALTER TABLE` on the source once started; auto-increment, defaults, indexes and non-primary keys not inferred; an unsupported column type turns *every* column of the table into TEXT; every table needs a primary key; `SHOW CDC EXTRACTOR POOL` for status; new work is steered to "SingleStore Flow".

**migkit.** No SingleStore engine. The mechanism to take is the one already in R19.8: Debezium's incremental snapshot (chunks interleaved with the change stream by watermark), which is also Vitess's `lastpk` rule and makes the "restart snapshot from the beginning" failure impossible - migkit's ranged copy already resumes; R19.8 removes the wasted applies.

---

## 14. Spanner migration tool (formerly HarbourBridge)

**Mechanism.** Schema: SMT converts the source schema (dump or live) to Spanner, the operator edits it in the UI, and every decision is serialised to **`session.json`** - the mapping the data pipeline consumes (`--session`; `sessionFilePath` for the Dataflow job; sessions can also live in a `spannermigrationtool_metadata` Spanner database). Minimal-downtime: Datastream backfills and streams changes to GCS -> Pub/Sub notifications -> the **Datastream-to-Spanner Dataflow template** writes to Spanner. Ordering and idempotence: Datastream does not guarantee order, so the template creates a **shadow table** per table (`shadow_<table>`, `shouldCreateShadowTables=true`) holding, per primary key, the sequencing fields of the last applied event - `timestamp` and `lsn` for PostgreSQL (`CREATE TABLE "shadow_singers" ("singer_id" bigint NOT NULL, "timestamp" bigint, "lsn" character varying(2621440), PRIMARY KEY ("singer_id"))`), `timestamp`, `log_file`, `log_position` for MySQL, LSN for SQL Server - and an incoming event older than the shadow row is skipped; "data consistency is guaranteed only at the end of migration". Errors go to a GCS dead-letter queue split into retryable (retried every `dlqRetryMinutes=10` up to `dlqMaxRetryCount=500` - e.g. a child of an interleaved table arriving before its parent) and severe; `runMode regular|retryDLQ|retryAllDLQ`. Shadow tables are kept for validation. Reverse replication (`spanner-to-sourcedb`) uses `rev_shadow_` tables holding the Spanner commit timestamp per key, consistent per key, not across keys or tables. No DDL propagation; foreign keys applied with the schema in GoogleSQL dialect; sharded migrations MySQL only; Dataflow workers tunable.

**migkit.** The tail applies in log order, so per-row "last applied position" is not needed for ordering. It would give something else: an exact `update_origin_differs` without a before image (compare the target row's *last applied position* with the change's origin position instead of comparing values) - a footprint-bearing rung for the two-way ladder where a source cannot keep full row images. Note it in R3 as a bottom rung; effort M; not built. The DLQ-with-retry for "parent not yet there" is R2.3's "constraint error: lane rolled back and rows applied one by one, then stop" - migkit stops rather than parks, which is the right default for a migration and the wrong one for a pipeline.

---

## 15. Neon and Supabase

**Neon.** *Import Data Assistant* (console): checks Postgres version, region and extensions, creates a branch in the target project, and generates pre-populated `pg_dump`/`pg_restore` commands; under 10 GB, a brief write pause, only reads from the source; not for Supabase/Heroku sources (extensions) or IPv6. `@neondatabase/pg-import` - an experimental CLI, last published two years ago. The Aug 2026 Labs assistant picks one of three paths by size: managed import (<10 GB), `pg_dump`/`pg_restore` (10-200 GB, unpooled connection, one database at a time since `pg_dumpall` is unsupported), or logical replication (copies the schema first, creates publication and subscription, enables `wal_level=logical` on the source with a restart, tracks table sync state, measures lag as source WAL position minus the slot's confirmed position); not recommended for production yet. Rules: unpooled connection strings, `--no-owner`, `--no-tablespaces`, no large objects, no piping for large databases.

**Supabase.** Three methods: a Colab notebook that scripts `pg_dump`/`psql` (pgloader for MySQL sources), manual dump/restore (run from a VM in the same region; lower `-j` on the dump against production, full `-j` on the restore; `VACUUM VERBOSE ANALYZE` after; 18+ `--with-statistics`), and logical replication (Postgres 10+, replica identity on every table taking UPDATE/DELETE, a schema freeze, sequences exported with `pg_dump --data-only --table='*_seq'` after the source is set `default_transaction_read_only = true`, LOBs by hand). Roles and RLS state are not migrated. Use Supavisor *session* mode.

**migkit.** Nothing mechanical to take: dump/restore paths (`movers.py:925` `pgdump_move`), sequences carried (`movers.py:1364` `_pg_carry_sequences`), users and grants (`migkit/users.py`), the freeze (`freeze.py`), slot lag in `watch`. Two checklist items are worth confirming in `assess`: extension and major-version compatibility named before anything moves (Neon refuses on both) and the pooled-connection warning (a pooler in transaction mode breaks `SET TRANSACTION SNAPSHOT` and COPY) - both are one query.

---

## 16. What the products converge on, and where migkit stands

Everyone with a change stream converges on: key-range chunks planned once and checkpointed; a position taken before the copy and the copy overlapped with the stream; hot-row collapse per batch; upsert-style idempotence on replay (DM's safe mode, Tencent's REPLACE rewrite, Alibaba's overwrite-after-restart); a marker of the tool's own writes for two-way (Alibaba's `dts` database, migkit's `migkit_origin`); a conflict vocabulary of insert-exists / update-missing / delete-missing with error, ignore, overwrite and a compare-a-column rule; forward-only DDL in two-way; a fenced same-point comparison (VDiff, Tencent's binlog checksum, migkit's fence); a reverse stream created at the moment of the switch (MoveTables, Voyager fall-back, migkit `reverse: at_cutover`).

What migkit has that none of them document: the measured rejection of XOR folds; the range fingerprint that refuses stale partials; digests before rows so the link carries numbers; a fence that *proves* an in-flight difference instead of retrying on a timer; a freeze derived from the catalogue per role; the revert generated with the repair; counters that add on both sides; and a decision layer that composes strategy per table from facts (P0) where every product above ships one fixed way per feature.

What the products have that migkit does not, by gap size:

1. **Statistics-based chunk edges** (MOLT `pg_stats` shards up to 200, sync-diff buckets, TiDB regions) instead of a full key walk. S on PostgreSQL, M on MySQL.
2. **Bisect on non-integer keys** (sync-diff's `OFFSET count/2` midpoint) - falls out of 1. S once 1 exists.
3. **Changes not applied to ranges not yet copied** (Vitess `lastpk`, Debezium/SingleStore incremental snapshot) - R19.8. S-M.
4. **Exact batches as the default rung for every one-way tail** and a mark row per keyless span in the same transaction (DM loader, Alibaba Exactly-Once) - R3/R19.11. S-M.
5. **Decode change events under the schema as of their position** (DM's `table_info`) so a DDL under a stopped tail does not stop the tail's first batch. M.
6. **A durable local log buffer** for MySQL sources with short retention (DM relay, Voyager queue, DTS local store). M.
7. **Finer lane conflicts** - hold only the colliding event (Voyager) rather than one lane per table with a second unique index - R2.2 union-find. M.
8. **Many-to-one with a DDL owner** (DM pessimistic lock) - R3 topologies. M-L.
9. **Per-table conflict policy overrides and an operator on the compared column** (Alibaba independent policies, UseMax/UseMin; Tencent ConditionCover). S.
10. **Per-table re-snapshot with a maximum hold** (VDiff `--max-diff-duration`) - backlog "snapshot age". S.
11. **Cutover refusals and a cutover dry run** (`--max-replication-lag-allowed`, `--dry-run`). S.
12. **Hotspot readout in `watch`** (Alibaba's conflict depth). S.
13. **Assess additions**: PostgreSQL replica identity per table in scope, extension/version parity, pooled-connection detection, a stated retry budget. S each.
14. **ClickHouse as a change target**: `ReplacingMergeTree` with version and tombstone columns. M, only if that target is wanted.

Deliberately not taken: writing on the source (`__tencentdb__`), stamp/soft-delete columns and temporary accounts on the target (DRS), statement rewrites to `REPLACE INTO` (Tencent, DM), sampling as a verdict (Alibaba, Tencent, DRS), script hooks (MOLT userscripts), a staging schema of buffered mutations in the target (MOLT), a proxy for routing (Vitess, PlanetScale).

---

## 17. Table

| mechanism | product | what it gives | migkit status | how migkit builds it better | effort | testable in docker |
|---|---|---|---|---|---|---|
| Table slicing by PK into fixed slices; source/sink thread pools | Alibaba DTS | parallel full load without a global lock | built: `ranges.py:222` plan, `:330` bounds_sql (key walk), `postgres.py:714` ctid spans, `:260` keyless spans | edges from statistics (`pg_stats`, histograms) so no scan precedes the copy; workers auto-sized by stress (`throttle.py:70`) | S (PG) / M (MySQL) | yes |
| Hot-row merge (`trans.hot.merge.enable`) + hotspot readout | Alibaba DTS | one write per hot key per batch; which keys are hot | built: `base.py:3917` `_collapsed`, `:3656`, `:3609` lanes; readout missing | report top collapsed keys and depth in `watch_sample` (`hetero.py:3145`) | S | yes |
| Loop prevention via `dts` database in both destinations | Alibaba DTS | the reverse task skips its own transactions | built and beyond: `twoway.py:44` table rung; R3 ladder (origin, logical message, tagged GTID, comment) | rung chosen per side by measurement, proved by a probe, no server setting changed | M (ladder rungs) | yes |
| Conflict detection insert/update/delete + TaskFailed/Ignore/Overwrite, per-table UseMax/UseMin | Alibaba DTS | decided two-way rows | built: `twoway.py:217` resolve, `:354` policies incl. last_update_wins, source_priority, delta | add per-table overrides and a column operator; log `delete_missing` explicitly | S | yes |
| Forward-only DDL, direction switch | Alibaba DTS | no DDL loop | partial: `hetero.py:2808` shape gate stops the tail; no forwarding | document the gate as the rule for both directions in `cutover.md` | S | yes |
| Exactly-Once write: transactional tables in a `dts` schema, GTID required, keyless tables locked | Alibaba DTS | no duplicates on keyless tables | built differently: `ranges.py:260` count-settle; `twoway.py:134-160` exact batches (counters/two-way only) | make `exact` the default rung for one-way tails; a mark row per keyless span in the same transaction, no source lock | S-M | yes |
| Full verify by row sampling 10-100%, row-count-only, incremental verify, RPS/MBps caps | Alibaba DTS | cheaper verify | built without sampling: `mysql.py:1720`, `postgres.py:6313`, `checkpoint.py:51`; `delta_verify` `hetero.py:1934`; `unchanged.py` | whole-table digest costs less than a sample and proves; keyless tables not skipped at 10k rows | - | yes |
| `__tencentdb__.Checksums` written on the source and replicated through the binlog | Tencent DTS | per-chunk same-point check with no coordination, works on native replicas | not built, by design (no source writes); fence gives table-grain exactness `hetero.py:1902`, `base.py:3194` | optional bottom rung for native-replica targets where the hop allows a footprint | M | yes (MySQL replica pair) |
| Independent check: full / sampling / row count, slices, 10-min query cap, keyless >50k skipped, N rechecks at an interval | Tencent DTS | verify of a heterogeneous pair | built: slices + digests + fenced rechecks (`base.py:3223`); `Rate.chunk_rows` targets seconds (`checkpoint.py:193`) | rechecks proven against the position rather than counted | - | yes |
| REPLICA IDENTITY FULL warning at precheck | Tencent DTS / Alibaba DTS | UPDATE/DELETE carried on keyless PG tables | partial: blind mode said once (`twoway.py:299`) | assess names tables whose identity will refuse writes or blind the tail | S | yes |
| ReportError / Ignore (`INSERT IGNORE`) / Cover (`REPLACE INTO`, `DELETE+REPLACE`) / ConditionCover (column, operator, side priority) | Tencent DTS | conflict policies by statement rewrite | built without rewrites: upserts `mysql.py:293`, `postgres.py:1116`; `last_update_wins` = ConditionCover(>) | add the operator; never REPLACE (triggers, cascades, auto-increment) | S | yes |
| Auto-retry window 5-720 min; timed start; source-side resumable export | Tencent DTS | unattended recovery | built in parts: `schedule.py`, `lease.py`, `stall.py`, ranged resume | one `retry_for` option that names the budget and stops honestly | S | yes |
| Lock-free full phase, keyless tables locked, 8 threads, CPU 18-45% | Tencent DTS | predictable source load | built: no locks at all; throttle by measured stress | - | - | yes |
| Static vs dynamic value comparison, row-then-value, Review re-check, 60-min cap, stamp columns, temp accounts | Huawei DRS | live comparison | built: `delta_verify` in watch, `fenced_recheck`, `check_counts` first; stamps/temp accounts deliberately not | no duration cap: ranges checkpointed (`checkpoint.py`) | - | yes |
| Pessimistic shard-DDL lock (owner executes, others skip) | TiDB DM | many-to-one merge with DDL | not built (R3 topologies) | coordinator owning a lock, one tail per source, shared shape file | M-L | yes (2 shards -> 1) |
| Optimistic shard-DDL joined schema, strict mode | TiDB DM | merge continues through compatible DDL | not built | only with the pessimistic rung proved first; strict by default | L | yes |
| Relay log (local binlog copy, purge policy, GTID re-pull) | TiDB DM | survives upstream purge; shares one pull | not built; retention measured in assess (`mysql.py:1013-1028`); PG slot holds WAL | durable local buffer only where retention < expected stop, chosen by the decision layer | M | yes (`binlog_expire_logs_seconds` low) |
| Safe mode: INSERT->REPLACE, UPDATE->DELETE+REPLACE for 2 checkpoint intervals | TiDB DM | idempotent replay window | built better: always-upsert `base.py:3990`, exact marks `twoway.py:125` | no window, no key churn | - | yes |
| Checkpoint tables in the downstream (global + per-table, `table_info`, `exit_safe`), loader position in the batch's transaction | TiDB DM | exact resume; decode old events under their schema | partial: local files, `tail-shape.json` (`hetero.py:2830`), exact only for counters/two-way | decode under the position's schema; position with the rows for keyless spans | M / S | yes |
| Physical import: sorted KV engines, SST ingest, import mode, scatter | TiDB Lightning | 100-500 GiB/h | not built (R19.1 physical rung: basebackup/CLONE/snapshots) | physical only with same major version + privilege, tail fast-forwarded to the backup's end position | L | partly (PG basebackup yes; RDS snapshots no) |
| Checksum triple (KV count, bytes, XOR of CRC-64) vs `ADMIN CHECKSUM TABLE` | TiDB Lightning | proof the load landed | built as count + summed hashes; XOR rejected by measurement (`checkpoint.py:118`) | sum fold catches duplicate rows XOR cancels | - | yes |
| Conflict strategy none/error/replace, precheck before import, `conflict_error_v3` | TiDB Lightning | duplicates found before ingest | built on the source side: duplicate hunt `base.py:989-1045`; `_collapsed` keeps latest | - | - | yes |
| Checkpoints with error-destroy / error-ignore / remove | TiDB Lightning | resume; also a data-loss foot-gun | built: fingerprinted partials refuse a changed plan (`checkpoint.py:88`) | no "ignore the error" switch exists | - | yes |
| Bucket / random / limit splitters, `CalculateChunkSize` (50k or rows/10k) | sync-diff-inspector | stats-driven chunks | not built (time-sized ranges `checkpoint.py:172`; key walk) | stats edges + time sizing together | S-M | yes |
| `COUNT(*) + BIT_XOR(md5 halves)` with `ISNULL` flags, float rounding | sync-diff-inspector | one query per chunk | built with a sum fold and `rowtext` encoding | sum instead of XOR; geometry canonicalised (`postgres.py:6313`) | - | yes |
| Bisect at `SplitThreshold=1000` via `OFFSET count/2`, stop when both halves differ, then merge-compare and fix SQL | sync-diff-inspector | rows found in few reads | built for single integer keys: `hetero.py:462` `_bisect`, leaf 2000; repair + revert (`revert.py`) | extend to text/composite keys via stats edges; revert generated with the fix | S after splitter | yes (`test_a_large_table_is_bisected_across_engines.py`) |
| Consistency modes flush/snapshot/lock/none/auto; `-r` split on INT/STRING first key; metadata Log/Pos | Dumpling | consistent dump | built: PG exported snapshot `postgres.py:6448`; MySQL Shell for bulk; own copier needs no global snapshot | mode chosen per engine by the decision layer | - | yes |
| VReplication copy/catchup/fast-forward with `lastpk` and `copy_state` in the batch's transaction | Vitess | consistent copy without a long snapshot; changes for uncopied ranges skipped | built except the skip: `ranges.py`, `hetero.py:2884`, interleave test; R19.8 not built | skip changes to uncopied ranges (S-M); position with the rows | S-M | yes |
| VDiff v2: same-GTID snapshots both sides, streaming merge compare, resume by last PK, auto-retry, `--max-diff-duration`, extra-rows re-compare | Vitess | restartable exact diff of a running workflow | built: fence + exported snapshot, ranged checkpoint, digests before rows | per-table re-snapshot with a maximum hold (backlog "snapshot age") | S | yes |
| SwitchTraffic: DeniedTables, wait to position, reverse workflow, routing rules, `--max-replication-lag-allowed`, `--dry-run` | Vitess / PlanetScale | safe switch with a way back | built: `freeze.py`, `fence_wait`, `reverse: at_cutover` (`cli.py:1821`); no routing (no proxy) | lag threshold that refuses to start the freeze; cutover dry run from the planner | S | yes |
| Online DDL cut-over: buffer, puncture rename, mark GTID, drain, rename back | Vitess | atomic-looking swap | same shape at the database grain in `cutover.md`; not a schema tool | - | - | - |
| Deferred secondary indexes, verify (VDiff) before switch | PlanetScale | faster copy | built for PG `movers.py:819`; refused on MySQL by measurement | re-measure bulk `ADD INDEX` after key-ordered load on InnoDB | S | yes |
| Stats-based shards (up to 200, any key type) vs range shards; continuation tokens per table/file; `cdc_cursor` | MOLT Fetch | even parallel export, resume at file grain | built: ranged checkpoint, `tail_seed`; stats shards not built | see splitter row | S-M | yes |
| Row batches of 20k across the link; `--live` retry and `--continuous` (removed) | MOLT Verify | live verify | built better: digests, fenced proof of in-flight, watch loop | - | - | yes |
| Staging schema in target (checkpoints, buffered mutations); Consistent / BestEffort(`--bestEffortWindow`) / Immediate; userscripts | MOLT Replicator | apply modes and transforms | built: lanes by FK groups (`base.py:3697`), keys off where parents in scope (R2.5); no userscripts, one mark table at most | relax cross-table atomicity only while behind, if measured as the ceiling | S | yes |
| Local event queue segments; channels by PK hash; hold colliding events (`WaitUntilNoConflict`); batch retry 50 | YugabyteDB Voyager | parallel apply with unique-key safety | partial: lanes send unique-index tables whole (`base.py:3613`); R2.2 union-find not built | hold only the colliding row, everything else parallel | M | yes |
| Fall-forward (source-replica) and fall-back (original source) after cutover; cutover when export rate = 0 | YugabyteDB Voyager | a way back and a way sideways | fall-back built (`reverse: at_cutover`); fall-forward = second hop, unnamed; cutover by fence not rate | name the rehearsal topology in `cutover.md` | S | yes |
| CTID partitions via COUNT + window function, 100k rows, 4 parallel; `ReplacingMergeTree(_peerdb_version)` + `_peerdb_is_deleted`, `FINAL` reads | ClickPipes / PeerDB | parallel snapshot; updates on an append store | ctid spans from the catalogue `postgres.py:714` (cheaper); ClickHouse engine has no version/tombstone | version + tombstone + Replacing engine if ClickHouse becomes a change target | M | yes |
| Debezium link, `INFER PIPELINE`, snapshot restarts from zero unless `incremental` | SingleStore | CDC-in | no engine; incremental snapshot = R19.8 | - | - | - |
| Shadow tables per PK (timestamp + log_file/log_position or lsn), skip older events, DLQ retry 10 min x 500, `session.json` | Spanner migration tool | idempotent out-of-order apply | not needed for ordering (log order); not built | per-row last-applied position as a footprint rung for `update_origin_differs` without full row images | M | yes |
| Import Data Assistant (version/extension/region checks, branch, generated dump/restore); Labs assistant for logical replication; unpooled connections | Neon | guided small migrations | dump/restore, sequences, users, slot lag all built | assess: extension/version parity, pooled-connection detection | S | yes |
| Colab notebook, dump/restore from a same-region VM, sequences exported after read-only, RLS/roles not migrated | Supabase | guided migration | built (`movers.py:925`, `:1364`; `users.py`; `freeze.py`) | - | - | yes |

---

## Sources

Alibaba Cloud DTS
- https://www.alibabacloud.com/blog/alibaba-cloud-dts-experience-sharing-series-%7C-guide-to-accelerating-full-data-migration_601706
- https://www.alibabacloud.com/help/en/dts/product-overview/system-architecture-and-design-concepts
- https://www.alibabacloud.com/help/en/dts/support/faq
- https://www.alibabacloud.com/help/en/dts/support/performance-white-paper
- https://www.alibabacloud.com/help/en/dts/user-guide/view-hot-data
- https://www.alibabacloud.com/help/en/dts/user-guide/configure-two-way-data-synchronization-between-mysql-instances
- https://help.aliyun.com/zh/dts/user-guide/configure-a-conflict-resolution-policy
- https://www.alibabacloud.com/help/en/dts/user-guide/switch-the-direction-of-a-two-way-synchronization-instance
- https://www.alibabacloud.com/help/en/dts/user-guide/synchronize-tables-without-primary-keys-and-unique-constraints-from-the-source-database
- https://www.alibabacloud.com/help/en/dts/user-guide/enable-data-verification
- https://www.alibabacloud.com/help/en/dts/user-guide/view-data-verification-details
- https://www.alibabacloud.com/help/en/dts/user-guide/configure-etl-in-dts-tasks
- https://www.alibabacloud.com/help/en/dts/user-guide/configure-an-etl-task-in-dag-mode-1
- https://static-aliyun-doc.oss-cn-hangzhou.aliyuncs.com/download/pdf/176062/Introduction_intl_en-US.pdf

Tencent Cloud DTS
- https://www.tencentcloud.com/document/product/571/42724 (consistency check, `__tencentdb__` mechanism)
- https://www.tencentcloud.com/document/product/571/42645 (MySQL migration: full phase, retry, timed start, privileges)
- https://cloud.tencent.com/document/faq/571/62986
- https://www.tencentcloud.com/document/product/571/49935 and https://cloud.tencent.com/document/product/571/78653 (conflict policies)
- https://www.tencentcloud.com/document/api/571/51823 (`ConflictHandleOption` fields)
- https://cloud.tencent.com.cn/document/product/571/92847 (`ModifySyncJobConfig`)
- https://staticintl.cloudcachetci.com/doc/pdf/product/pdf/409_59721_en.pdf (PostgreSQL migration guide, replica identity)
- https://cloud.tencent.com/document/product/571/96723 (tuning guide)

Huawei DRS
- https://support.huaweicloud.com/intl/en-us/realtimesyn-drs/drs_10_0012.html
- https://support.huaweicloud.com/intl/en-us/realtimemig-drs/drs_02_0007.html
- https://support.huaweicloud.com/intl/en-us/realtimedr-drs/drs_02_0033.html
- https://support.huaweicloud.com/intl/en-us/realtimesyn-drs/drs_06_0005.html
- https://support.huaweicloud.com/intl/en-us/drs_faq/drs_16_1130.html
- https://support.huaweicloud.com/intl/en-us/eu-west-0-usermanual-drs/Data%20Replication%20Service%20User%20Guide-pdf.pdf

TiDB DM, Lightning, sync-diff-inspector, Dumpling
- https://docs.pingcap.com/tidb/stable/dm-shard-merge
- https://docs.pingcap.com/tidb/stable/feature-shard-merge-pessimistic/
- https://docs.pingcap.com/tidb/stable/feature-shard-merge-optimistic/
- https://docs.pingcap.com/tidb/stable/relay-log
- https://docs.pingcap.com/tidb/stable/dm-safe-mode
- https://docs.pingcap.com/tidb/stable/dm-manage-schema/
- https://pingcap.com/blog-cn/dm-source-code-reading-9/ and https://cn.pingcap.com/blog/dm-source-code-reading-4/
- https://github.com/pingcap/tiflow/blob/master/dm/syncer/checkpoint_test.go
- https://docs.pingcap.com/tidb/stable/tidb-lightning-physical-import-mode
- https://docs.pingcap.com/tidb/stable/tidb-lightning-physical-import-mode-usage/
- https://docs.pingcap.com/tidb/stable/tidb-lightning-checkpoints/
- https://docs.pingcap.com/tidb/stable/tidb-lightning-glossary/
- https://docs.pingcap.com/tidb/stable/tidb-lightning-faq/
- https://docs.pingcap.com/tidb/stable/sync-diff-inspector-overview
- https://cn.pingcap.com/blog/optimisation-of-the-sync-diff-inspector/ (English: https://segmentfault.com/a/1190000041010820/en)
- https://raw.githubusercontent.com/pingcap/tidb-tools/master/sync_diff_inspector/utils/utils.go
- https://raw.githubusercontent.com/pingcap/tidb-tools/master/sync_diff_inspector/splitter/bucket.go
- https://raw.githubusercontent.com/pingcap/tidb-tools/master/sync_diff_inspector/splitter/splitter.go
- https://raw.githubusercontent.com/pingcap/tidb-tools/master/sync_diff_inspector/diff.go
- https://docs.pingcap.com/tidb/stable/dumpling-overview
- https://github.com/pingcap/dumpling/pull/305 and https://github.com/pingcap/dumpling/issues/104

Vitess and PlanetScale
- https://vitess.io/docs/25.0/reference/vreplication/internal/life-of-a-stream/
- https://vitess.io/docs/21.0/reference/vreplication/vreplication/
- https://vitess.io/blog/2022-11-22-vdiff-v2/
- https://vitess.io/docs/21.0/reference/vreplication/vdiff/
- https://vitess.io/docs/21.0/reference/programs/vtctldclient/vtctldclient_vdiff/vtctldclient_vdiff_create/
- https://vitess.io/docs/21.0/reference/vreplication/movetables/
- https://vitess.io/docs/21.0/reference/vreplication/internal/cutover/
- https://vitess.io/blog/2022-04-06-online-ddl-vitess-cut-over/
- https://vitess.io/docs/archive/22.0/user-guides/schema-changes/ddl-strategies/
- https://github.com/github/gh-ost/blob/master/doc/cut-over.md
- https://github.com/vitessio/vitess/issues/13136 (atomic copy RFC)
- https://planetscale.com/docs/vitess/imports/database-imports
- https://planetscale.com/blog/import-your-mysql-data-to-planetscale

CockroachDB MOLT
- https://docs.cockroachlabs.com/docs/molt/molt-fetch
- https://docs.cockroachlabs.com/docs/molt/molt-verify
- https://docs.cockroachlabs.com/docs/molt/molt-replicator
- https://www.cockroachlabs.com/docs/molt/migrate-load-replicate
- https://www.cockroachlabs.com/docs/molt/migrate-resume-replication
- https://www.cockroachlabs.com/docs/releases/molt
- https://www.cockroachlabs.com/blog/data-integrity-molt-verify-migrations/

YugabyteDB Voyager
- https://docs.yugabyte.com/preview/yugabyte-voyager/migrate/live-migrate/
- https://docs.yugabyte.com/preview/yugabyte-voyager/migrate/live-fall-forward/
- https://docs.yugabyte.com/preview/yugabyte-voyager/migrate/live-fall-back/
- https://docs.yugabyte.com/stable/yugabyte-voyager/reference/performance/
- https://pkg.go.dev/github.com/yugabyte/yb-voyager/yb-voyager/src/metadb and .../src/tgtdb
- https://github.com/yugabyte/yb-voyager/pull/3824, https://github.com/yugabyte/yb-voyager/pull/3741, https://github.com/yugabyte/yb-voyager/issues/3834
- https://docs.yugabyte.com/stable/yugabyte-voyager/release-notes/

ClickHouse ClickPipes / PeerDB, SingleStore
- https://clickhouse.com/docs/integrations/clickpipes/postgres/parallel_initial_load
- https://clickhouse.com/docs/integrations/clickpipes/postgres/faq
- https://clickhouse.com/blog/postgres-to-clickhouse-data-modeling-tips-v2
- https://blog.peerdb.io/parallelized-initial-load-for-cdc-based-streaming-from-postgres
- https://docs.peerdb.io/mirror/cdc-pg-clickhouse
- https://github.com/ClickHouse/clickhousectl/issues/1009
- https://docs.singlestore.com/db/v9.0/load-data/data-sources/replicate-data-from-mysql/
- https://docs.singlestore.com/cloud/reference/sql-reference/security-management-commands/create-link/
- https://www.singlestore.com/blog/steps-to-migrate-from-mysql-to-singlestore/

Spanner migration tool
- https://googlecloudplatform.github.io/spanner-migration-tool/minimal
- https://googlecloudplatform.github.io/spanner-migration-tool/ui/schema-conv/session-manage.html
- https://googlecloudplatform.github.io/spanner-migration-tool/troubleshoot/minimal.html
- https://docs.cloud.google.com/dataflow/docs/guides/templates/provided/datastream-to-cloud-spanner
- https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/README_Cloud_Datastream_to_Spanner.md
- https://github.com/GoogleCloudPlatform/DataflowTemplates/pull/4276 (PostgreSQL shadow table DDL)
- https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-to-sourcedb/README_Spanner_to_SourceDb.md

Neon and Supabase
- https://neon.com/docs/import/import-data-assistant
- https://neon.com/docs/import/migrate-intro
- https://neon.com/docs/import/migrate-from-postgres
- https://neon.com/blog/introducing-neon-labs
- https://www.npmjs.com/package/@neondatabase/pg-import
- https://supabase.com/docs/guides/platform/migrating-to-supabase/postgres
- https://supabase.com/docs/guides/platform/migrating-within-supabase/backup-restore
